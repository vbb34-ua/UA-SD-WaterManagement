import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Acceso a la BD SQLite de CENTRAL.
 * Tablas:
 *  - watering_stations(id, location, status, bloqueada)
 *  - operarios(id, nombre)
 * Todos los métodos son synchronized para evitar escrituras concurrentes (SQLITE_BUSY).
 */
public class DatabaseManager {

    // Estrategia 6: la ruta de la BD se puede definir con DB_PATH (ej. /app/data/water_management.db en Railway)
    private static String getUrl() {
        String dbPath = System.getenv("DB_PATH");
        if (dbPath == null || dbPath.trim().isEmpty()) {
            dbPath = "water_management.db";
        } else {
            dbPath = dbPath.replace("\"", "").trim();
        }

        File file = new File(dbPath);
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        return "jdbc:sqlite:" + dbPath;
    }

    // Caché en memoria: estado y bloqueo de cada estación
    private static final ConcurrentHashMap<String, String> estados = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> bloqueadas = new ConcurrentHashMap<>();

    // Operarios dados de alta por defecto
    private static final String[] OPERARIOS_INICIALES = {
        "FO_01", "FO_02", "FO_03", "FO_04", "FO_05", "FO_06", "FO_07", "FO_08", "FO_09", "FO_10"
    };

    public static synchronized void inicializarYcargarEstaciones() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            System.err.println("Error: No se encontró el driver JDBC de SQLite.");
        }

        try (Connection conn = DriverManager.getConnection(getUrl())) {
            System.out.println("BD: Conectado a SQLite correctamente en (" + getUrl() + ").");

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE IF NOT EXISTS watering_stations ("
                        + " id TEXT PRIMARY KEY,"
                        + " location TEXT NOT NULL,"
                        + " status TEXT NOT NULL,"
                        + " bloqueada INTEGER NOT NULL DEFAULT 0"
                        + ");");
                stmt.execute("CREATE TABLE IF NOT EXISTS operarios ("
                        + " id TEXT PRIMARY KEY,"
                        + " nombre TEXT"
                        + ");");
            }

            // Migración: BDs antiguas no tenían la columna 'bloqueada'
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("ALTER TABLE watering_stations ADD COLUMN bloqueada INTEGER NOT NULL DEFAULT 0");
            } catch (SQLException ignored) {
                // La columna ya existe
            }

            try (PreparedStatement ps = conn.prepareStatement("INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?)")) {
                for (String op : OPERARIOS_INICIALES) {
                    ps.setString(1, op);
                    ps.setString(2, "Operario " + op);
                    ps.executeUpdate();
                }
            }

            // Tras un reinicio no conocemos el estado real: todas DESCONECTADA hasta que conecten
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE watering_stations SET status = 'DESCONECTADA'");
            }

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT id, location, bloqueada FROM watering_stations")) {

                System.out.println("--- Estaciones de Riego Registradas ---");
                boolean hayEstaciones = false;
                while (rs.next()) {
                    hayEstaciones = true;
                    String id = rs.getString("id");
                    String location = rs.getString("location");
                    boolean bloq = rs.getInt("bloqueada") == 1;

                    estados.put(id, "DESCONECTADA");
                    bloqueadas.put(id, bloq);
                    Dashboard.cargarEstacion(id, location, Dashboard.Estado.DESCONECTADA);
                    System.out.println("ID: " + id + " | Ubicación: " + location + " | Estado: DESCONECTADA"
                            + (bloq ? " | BLOQUEADA" : ""));
                }
                if (!hayEstaciones) {
                    System.out.println("No hay estaciones registradas en la base de datos.");
                }
                System.out.println("---------------------------------------");
            }
        } catch (SQLException e) {
            System.err.println("Error grave de base de datos: " + e.getMessage());
        }
    }

    /**
     * Da de alta (o actualiza) una estación. Devuelve el estado con el que queda:
     * FUERA_SERVICIO si estaba bloqueada por CENTRAL, DISPONIBLE en otro caso. Null si falla.
     */
    public static synchronized String registrarEstacion(String id, String location) {
        boolean bloq = esBloqueada(id);
        String estado = bloq ? "FUERA_SERVICIO" : "DISPONIBLE";
        String sql = "INSERT INTO watering_stations (id, location, status, bloqueada) VALUES (?, ?, ?, 0) "
                + "ON CONFLICT(id) DO UPDATE SET location=excluded.location, status=excluded.status";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            pstmt.setString(2, location);
            pstmt.setString(3, estado);
            pstmt.executeUpdate();

            estados.put(id, estado);
            bloqueadas.putIfAbsent(id, false);
            return estado;
        } catch (SQLException e) {
            System.err.println("Error registrando estación: " + e.getMessage());
            return null;
        }
    }

    public static String obtenerEstado(String id) {
        return estados.getOrDefault(id, "DESCONECTADA");
    }

    public static boolean existeEstacion(String id) {
        return estados.containsKey(id);
    }

    public static List<String> listarIdsEstaciones() {
        return new ArrayList<>(estados.keySet());
    }

    public static synchronized boolean actualizarEstado(String id, String estado) {
        String sql = "UPDATE watering_stations SET status = ? WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, estado);
            pstmt.setString(2, id);
            pstmt.executeUpdate();

            if (estados.containsKey(id)) estados.put(id, estado);
            return true;
        } catch (SQLException e) {
            System.err.println("Error actualizando estado de estación [" + id + "]: " + e.getMessage());
            return false;
        }
    }

    public static boolean esBloqueada(String id) {
        return bloqueadas.getOrDefault(id, false);
    }

    public static synchronized void setBloqueada(String id, boolean bloq) {
        String sql = "UPDATE watering_stations SET bloqueada = ? WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, bloq ? 1 : 0);
            pstmt.setString(2, id);
            pstmt.executeUpdate();
            if (estados.containsKey(id)) bloqueadas.put(id, bloq);
        } catch (SQLException e) {
            System.err.println("Error actualizando bloqueo de [" + id + "]: " + e.getMessage());
        }
    }

    public static synchronized boolean existeOperario(String id) {
        String sql = "SELECT 1 FROM operarios WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id);
            try (ResultSet rs = pstmt.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            System.err.println("Error consultando operario [" + id + "]: " + e.getMessage());
            return false;
        }
    }
}
