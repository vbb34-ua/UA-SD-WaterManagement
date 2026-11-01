import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Estado en memoria del panel de CENTRAL. Cada cambio se imprime en consola
 * y se envía al Dashboard Web (SSE).
 */
public class Dashboard {

    public enum Estado {
        DISPONIBLE, REGANDO, FUGA, FUERA_SERVICIO, DESCONECTADA
    }

    public static class WSInfo {
        public String id;
        public String location;
        public Estado estado;
        public String caudal = "0.0";
        public String volumen = "0.0";
        public String operario = "-";

        public WSInfo(String id, String location, Estado estado) {
            this.id = id;
            this.location = location;
            this.estado = estado;
        }
    }

    private static final Map<String, WSInfo> estaciones = new ConcurrentHashMap<>();
    private static final LinkedList<String> logs = new LinkedList<>();
    private static final int MAX_LOGS = 15;

    public static void iniciar() {
        log("Sistema CENTRAL iniciado. La interfaz gráfica está en el Dashboard Web.");
    }

    public static synchronized void registrarEstacion(String id, String location, Estado estado) {
        estaciones.put(id, new WSInfo(id, location, estado));
        log("Alta de WS exitosa: [" + id + "] en [" + location + "] -> " + estado.name());
        WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
    }

    public static synchronized void cargarEstacion(String id, String location, Estado estado) {
        estaciones.put(id, new WSInfo(id, location, estado));
        WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
    }

    public static synchronized void actualizarEstado(String id, Estado estado) {
        WSInfo ws = estaciones.get(id);
        if (ws == null) {
            ws = new WSInfo(id, "Desconocida", estado);
            estaciones.put(id, ws);
        }
        ws.estado = estado;
        if (estado != Estado.REGANDO) {
            ws.caudal = "0.0";
            ws.operario = "-";
        }
        WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
    }

    public static synchronized Estado getEstado(String id) {
        WSInfo ws = estaciones.get(id);
        return ws == null ? null : ws.estado;
    }

    /** Solo se llama mientras la estación está REGANDO (lo controla GestorRiegos). */
    public static synchronized void actualizarTelemetria(String id, String caudal, String volumen) {
        WSInfo ws = estaciones.get(id);
        if (ws != null) {
            ws.caudal = caudal;
            ws.volumen = volumen;
            WebDashboard.broadcast("TELEMETRIA", "{\"id\":\"" + esc(id) + "\",\"caudal\":\"" + esc(caudal)
                    + "\",\"volumen\":\"" + esc(volumen) + "\"}");
        }
    }

    /** Al iniciar un riego: asigna operario y reinicia los contadores. */
    public static synchronized void asignarOperario(String id, String operario) {
        WSInfo ws = estaciones.get(id);
        if (ws != null) {
            ws.operario = operario;
            ws.caudal = "0.0";
            ws.volumen = "0.0";
            WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
        }
    }

    public static synchronized void log(String mensaje) {
        String time = new SimpleDateFormat("HH:mm:ss").format(new Date());
        String finalMsg = "[" + time + "] " + mensaje;

        logs.add(finalMsg);
        if (logs.size() > MAX_LOGS) {
            logs.removeFirst();
        }

        if (mensaje.contains("¡ALERTA!") || mensaje.contains("FUGA") || mensaje.contains("ABORTADO")) {
            System.err.println(finalMsg);
        } else {
            System.out.println(finalMsg);
        }

        WebDashboard.broadcast("NUEVO_LOG", "{\"mensaje\":\"" + esc(finalMsg) + "\"}");
    }

    public static synchronized String obtenerEstadoJSON() {
        StringBuilder json = new StringBuilder("[");
        boolean primero = true;
        for (WSInfo ws : estaciones.values()) {
            if (!primero) json.append(",");
            json.append(String.format("{\"id\":\"%s\",\"location\":\"%s\",\"estado\":\"%s\",\"caudal\":\"%s\",\"volumen\":\"%s\",\"operario\":\"%s\"}",
                    esc(ws.id), esc(ws.location), ws.estado.name(), esc(ws.caudal), esc(ws.volumen), esc(ws.operario)));
            primero = false;
        }
        json.append("]");
        return json.toString();
    }

    public static synchronized String obtenerListadoEstaciones() {
        if (estaciones.isEmpty()) {
            return "No hay estaciones registradas actualmente.";
        }
        StringBuilder sb = new StringBuilder();
        for (WSInfo ws : estaciones.values()) {
            sb.append("\n  - Estación: ").append(ws.id)
              .append(" | Ubicación: ").append(ws.location)
              .append(" | Estado: ").append(ws.estado.name());
            if (ws.estado == Estado.REGANDO) {
                sb.append(" [Caudal: ").append(ws.caudal).append(" L/min | Volumen: ").append(ws.volumen)
                  .append(" L | Operario: ").append(ws.operario).append("]");
            }
        }
        return sb.toString();
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
