package com.watermanagement.central.panel;

import java.util.concurrent.ConcurrentHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Estado en tiempo real de la red que muestra el panel: estado, caudal, volumen y operario de
 * cada estación, más el historial de eventos. Cada cambio se envía al panel web por SSE
 * ({@link WebDashboard#broadcast}) y cada evento se imprime también en consola.
 */
public class Dashboard {

    /** Estados posibles de una estación. */
    public enum Estado {
        DISPONIBLE, REGANDO, FUGA, FUERA_SERVICIO, DESCONECTADA
    }

    /** Datos de una estación tal como los muestra el panel. */
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
    /** Últimos eventos, para enviárselos a los navegadores que se conectan. */
    private static final LinkedList<String> logs = new LinkedList<>();
    private static final int MAX_LOGS = 200;

    public static void iniciar() {
        log("Sistema CENTRAL iniciado. La interfaz gráfica está en el Dashboard Web.");
    }

    /** Una estación se ha conectado: la muestra con su estado y lo registra en el historial. */
    public static synchronized void registrarEstacion(String id, String location, Estado estado) {
        estaciones.put(id, new WSInfo(id, location, estado));
        log("Alta de WS exitosa: [" + id + "] en [" + location + "] -> " + estado.name());
        WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
    }

    /** Añade una estación al panel sin registrar ningún evento (al cargar la BD o al darla de alta). */
    public static synchronized void cargarEstacion(String id, String location, Estado estado) {
        estaciones.put(id, new WSInfo(id, location, estado));
        WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
    }

    /** Cambia el estado de una estación; si deja de regar, se ponen a cero su caudal y su operario. */
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

    /** @return el estado de la estación, o null si no existe */
    public static synchronized Estado getEstado(String id) {
        WSInfo ws = estaciones.get(id);
        return ws == null ? null : ws.estado;
    }

    /**
     * Actualiza el caudal y el volumen de un riego en curso. Se envía como evento TELEMETRIA
     * (solo esa estación) en lugar del estado completo, porque llega cada segundo.
     */
    public static synchronized void actualizarTelemetria(String id, String caudal, String volumen) {
        WSInfo ws = estaciones.get(id);
        if (ws != null) {
            ws.caudal = caudal;
            ws.volumen = volumen;
            WebDashboard.broadcast("TELEMETRIA", "{\"id\":\"" + esc(id) + "\",\"caudal\":\"" + esc(caudal)
                    + "\",\"volumen\":\"" + esc(volumen) + "\"}");
        }
    }

    /** Al empezar un riego: asigna el operario a la estación y pone a cero caudal y volumen. */
    public static synchronized void asignarOperario(String id, String operario) {
        WSInfo ws = estaciones.get(id);
        if (ws != null) {
            ws.operario = operario;
            ws.caudal = "0.0";
            ws.volumen = "0.0";
            WebDashboard.broadcast("ESTADO_ACTUALIZADO", obtenerEstadoJSON());
        }
    }

    /**
     * Registra un evento con la hora: lo guarda en el historial, lo imprime en consola (las
     * alertas por la salida de error) y lo envía al panel.
     */
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

    /** @return todas las estaciones como array JSON: {@code [{"id":..,"location":..,"estado":..,...}]} */
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

    /** @return el historial de eventos como array JSON: {@code ["[hh:mm:ss] mensaje", ...]} */
    public static synchronized String obtenerLogsJSON() {
        StringBuilder json = new StringBuilder("[");
        boolean primero = true;
        for (String l : logs) {
            if (!primero) json.append(",");
            json.append("\"").append(esc(l)).append("\"");
            primero = false;
        }
        return json.append("]").toString();
    }

    /** @return listado de estaciones en texto, para la respuesta a {@code LISTAR_WS} de un operario */
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

    /** Escapa un texto para incluirlo entre comillas en JSON. */
    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
    }
}
