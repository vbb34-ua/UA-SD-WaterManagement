package com.watermanagement.central.datos;

import com.watermanagement.central.panel.Dashboard;

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
 * Acceso a la BD SQLite de CENTRAL. Tablas:
 * <ul>
 *   <li>{@code watering_stations(id, location, status, bloqueada)}: estaciones dadas de alta</li>
 *   <li>{@code operarios(id, nombre)}: operarios dados de alta</li>
 *   <li>{@code riegos(id, ws, operario, duracion, estado, volumen, duracion_real, motivo_fin, inicio, fin)}:
 *       historial; permite cerrar un riego aunque CENTRAL se haya reiniciado</li>
 *   <li>{@code mensajes_pendientes(id, operario, texto, creado)}: buzón de operarios desconectados</li>
 * </ul>
 *
 * <p>El estado y el bloqueo de cada estación se guardan también en memoria para consultarlos
 * sin acceder a disco. Los métodos que escriben son {@code synchronized} para evitar
 * escrituras concurrentes (SQLITE_BUSY).
 */
public class DatabaseManager {

    /**
     * @return URL JDBC de la BD. La ruta se toma de DB_PATH (en Railway, un volumen montado en
     *         /app/data para que la BD sobreviva a los redespliegues) o es water_management.db
     *         en el directorio actual. Crea la carpeta si no existe.
     */
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

    /** Estado de cada estación dada de alta (sus claves son la lista de estaciones existentes). */
    private static final ConcurrentHashMap<String, String> estados = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Boolean> bloqueadas = new ConcurrentHashMap<>();

    /** Operarios que se dan de alta al crear la BD. */
    private static final String[] OPERARIOS_INICIALES = {
        "FO_01", "FO_02", "FO_03", "FO_04", "FO_05", "FO_06", "FO_07", "FO_08", "FO_09", "FO_10"
    };

    /** Estaciones que se dan de alta al crear la BD: {id, ubicación}. */
    private static final String[][] ESTACIONES_INICIALES = {
        {"WS_01", "Parque Central"}, {"WS_02", "Jardines del Río"}, {"WS_03", "Parque del Río"},
        {"WS_04", "Rotonda Norte"},  {"WS_05", "Parque Central"},   {"WS_06", "Jardines del Río"},
        {"WS_07", "Parque del Río"}, {"WS_08", "Rotonda Norte"},    {"WS_09", "Parque Central"},
        {"WS_10", "Jardines del Río"}
    };

    /**
     * Crea las tablas si no existen, da de alta las estaciones y operarios iniciales y carga
     * las estaciones en memoria y en el panel. Todas empiezan DESCONECTADAS hasta que su Monitor se conecte.
     */
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
                stmt.execute("CREATE TABLE IF NOT EXISTS riegos ("
                        + " id TEXT PRIMARY KEY,"
                        + " ws TEXT NOT NULL,"
                        + " operario TEXT NOT NULL,"
                        + " duracion INTEGER NOT NULL,"
                        + " estado TEXT NOT NULL,"          // ACTIVO | INTERRUMPIDO | FINALIZADO
                        + " volumen TEXT,"
                        + " duracion_real TEXT,"
                        + " motivo_fin TEXT,"
                        + " inicio TEXT DEFAULT CURRENT_TIMESTAMP,"
                        + " fin TEXT"
                        + ");");
                stmt.execute("CREATE TABLE IF NOT EXISTS mensajes_pendientes ("
                        + " id INTEGER PRIMARY KEY AUTOINCREMENT,"
                        + " operario TEXT NOT NULL,"
                        + " texto TEXT NOT NULL,"
                        + " creado TEXT DEFAULT CURRENT_TIMESTAMP"
                        + ");");
            }

            // Las BDs creadas antes de añadir el bloqueo no tienen la columna 'bloqueada'
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
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT OR IGNORE INTO watering_stations (id, location, status, bloqueada) VALUES (?, ?, 'DESCONECTADA', 0)")) {
                for (String[] ws : ESTACIONES_INICIALES) {
                    ps.setString(1, ws[0]);
                    ps.setString(2, ws[1]);
                    ps.executeUpdate();
                }
            }

            // Tras un reinicio no se conoce el estado real de las estaciones hasta que se conecten
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
     * Marca como conectada una estación ya dada de alta.
     *
     * @param location nueva ubicación, o null para mantener la guardada
     * @return el estado con el que queda (FUERA_SERVICIO si está bloqueada, DISPONIBLE si no),
     *         o null si la estación no existe o falla la BD
     */
    public static synchronized String registrarEstacion(String id, String location) {
        boolean bloq = esBloqueada(id);
        String estado = bloq ? "FUERA_SERVICIO" : "DISPONIBLE";
        String sql = location != null
                ? "UPDATE watering_stations SET status = ?, location = ? WHERE id = ?"
                : "UPDATE watering_stations SET status = ? WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, estado);
            if (location != null) {
                pstmt.setString(2, location);
                pstmt.setString(3, id);
            } else {
                pstmt.setString(2, id);
            }
            if (pstmt.executeUpdate() == 0) return null;

            estados.put(id, estado);
            bloqueadas.putIfAbsent(id, false);
            return estado;
        } catch (SQLException e) {
            System.err.println("Error registrando estación: " + e.getMessage());
            return null;
        }
    }

    /** @return la ubicación guardada de la estación, o null si no existe */
    public static synchronized String obtenerUbicacion(String id) {
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement("SELECT location FROM watering_stations WHERE id = ?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        } catch (SQLException e) {
            return null;
        }
    }

    /**
     * Da de alta una estación nueva (queda DESCONECTADA y sin bloquear).
     *
     * @return false si ya existía o falla la BD
     */
    public static synchronized boolean altaEstacion(String id, String location) {
        String sql = "INSERT OR IGNORE INTO watering_stations (id, location, status, bloqueada) VALUES (?, ?, 'DESCONECTADA', 0)";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, location);
            if (ps.executeUpdate() == 0) return false;
            estados.put(id, "DESCONECTADA");
            bloqueadas.put(id, false);
            return true;
        } catch (SQLException e) {
            System.err.println("Error dando de alta la estación [" + id + "]: " + e.getMessage());
            return false;
        }
    }

    /**
     * Da de alta un operario nuevo.
     *
     * @return false si ya existía o falla la BD
     */
    public static synchronized boolean altaOperario(String id) {
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement("INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?)")) {
            ps.setString(1, id);
            ps.setString(2, "Operario " + id);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            System.err.println("Error dando de alta el operario [" + id + "]: " + e.getMessage());
            return false;
        }
    }

    /** @return true si el operario está dado de alta (o false si falla la BD) */
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

    /** @return el estado actual de la estación (DESCONECTADA si no existe) */
    public static String obtenerEstado(String id) {
        return estados.getOrDefault(id, "DESCONECTADA");
    }

    /** @return true si la estación está dada de alta */
    public static boolean existeEstacion(String id) {
        return estados.containsKey(id);
    }

    /** @return los ids de todas las estaciones dadas de alta */
    public static List<String> listarIdsEstaciones() {
        return new ArrayList<>(estados.keySet());
    }

    /**
     * Guarda el nuevo estado de la estación.
     *
     * @return false si falla la BD
     */
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

    /** @return true si CENTRAL ha bloqueado la estación */
    public static boolean esBloqueada(String id) {
        return bloqueadas.getOrDefault(id, false);
    }

    /** Bloquea o desbloquea la estación. Se guarda en la BD, así que se mantiene tras un reinicio. */
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

    // ---------------------------------------------------------------- RIEGOS

    /** @return {@code {id, ws, operario, duracion, estado}} del riego, o null si no existe */
    public static synchronized String[] obtenerRiego(String id) {
        String sql = "SELECT id, ws, operario, duracion, estado FROM riegos WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new String[] { rs.getString(1), rs.getString(2), rs.getString(3),
                        String.valueOf(rs.getInt(4)), rs.getString(5) };
            }
        } catch (SQLException e) {
            System.err.println("Error consultando riego [" + id + "]: " + e.getMessage());
            return null;
        }
    }

    /**
     * Crea el riego o, si ya existe, solo actualiza su estado.
     *
     * @param estado ACTIVO, INTERRUMPIDO o FINALIZADO
     */
    public static synchronized void guardarRiego(String id, String ws, String operario, int duracion, String estado) {
        String sql = "INSERT INTO riegos (id, ws, operario, duracion, estado) VALUES (?, ?, ?, ?, ?) "
                + "ON CONFLICT(id) DO UPDATE SET estado = excluded.estado";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, id);
            ps.setString(2, ws);
            ps.setString(3, operario);
            ps.setInt(4, duracion);
            ps.setString(5, estado);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error guardando riego [" + id + "]: " + e.getMessage());
        }
    }

    /** @param estado ACTIVO, INTERRUMPIDO o FINALIZADO */
    public static synchronized void actualizarEstadoRiego(String id, String estado) {
        String sql = "UPDATE riegos SET estado = ? WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, estado);
            ps.setString(2, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error actualizando riego [" + id + "]: " + e.getMessage());
        }
    }

    /** Marca el riego como FINALIZADO con el volumen, la duración real y el motivo que envía la estación. */
    public static synchronized void finalizarRiego(String id, String volumen, String duracionReal, String motivo) {
        String sql = "UPDATE riegos SET estado = 'FINALIZADO', volumen = ?, duracion_real = ?, motivo_fin = ?, "
                + "fin = CURRENT_TIMESTAMP WHERE id = ?";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, volumen);
            ps.setString(2, duracionReal);
            ps.setString(3, motivo);
            ps.setString(4, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error finalizando riego [" + id + "]: " + e.getMessage());
        }
    }

    /** @return los riegos que la BD tiene como ACTIVOS en la estación, como {@code {id, operario}} */
    public static synchronized List<String[]> riegosActivosDe(String ws) {
        List<String[]> lista = new ArrayList<>();
        String sql = "SELECT id, operario FROM riegos WHERE ws = ? AND estado = 'ACTIVO'";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, ws);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) lista.add(new String[] { rs.getString(1), rs.getString(2) });
            }
        } catch (SQLException e) {
            System.err.println("Error consultando riegos activos de [" + ws + "]: " + e.getMessage());
        }
        return lista;
    }

    // ---------------------------------------------------------------- BUZÓN DE OPERARIOS

    /** Guarda un mensaje en el buzón del operario para entregárselo cuando se conecte. */
    public static synchronized void guardarMensajePendiente(String operario, String texto) {
        String sql = "INSERT INTO mensajes_pendientes (operario, texto) VALUES (?, ?)";
        try (Connection conn = DriverManager.getConnection(getUrl());
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, operario);
            ps.setString(2, texto);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("Error guardando mensaje para [" + operario + "]: " + e.getMessage());
        }
    }

    /** @return los mensajes del buzón del operario en orden de llegada; el buzón queda vacío */
    public static synchronized List<String> extraerMensajesPendientes(String operario) {
        List<String> mensajes = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(getUrl())) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT texto FROM mensajes_pendientes WHERE operario = ? ORDER BY id")) {
                ps.setString(1, operario);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) mensajes.add(rs.getString(1));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM mensajes_pendientes WHERE operario = ?")) {
                ps.setString(1, operario);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("Error leyendo mensajes de [" + operario + "]: " + e.getMessage());
        }
        return mensajes;
    }
}
