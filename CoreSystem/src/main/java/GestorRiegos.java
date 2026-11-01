import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lógica de negocio de CENTRAL: alta de estaciones/operarios, ciclo de vida de un riego,
 * averías y órdenes de CENTRAL (iniciar / bloquear / activar).
 *
 * La usan tanto los hilos de sockets (WM_Central_Thread) como el Dashboard Web.
 * Los métodos son synchronized para que las comprobaciones de estado y los cambios
 * sean atómicos (ej. dos operarios pidiendo la misma estación a la vez).
 */
public class GestorRiegos {

    public static final int DURACION_DEFECTO_S = 15;
    public static final int DURACION_MAX_S = 3600;
    /** Id de operario usado cuando el riego se pide desde el menú de la propia estación. */
    public static final String OPERARIO_LOCAL = "LOCAL";
    /** Id de operario usado cuando el riego lo inicia CENTRAL desde su panel. */
    public static final String OPERARIO_CENTRAL = "CENTRAL";

    private static final long TIMEOUT_SOLICITUD_MS = 10000;
    /** Una conexión con actividad más reciente que esto se considera viva (para detectar IDs duplicados). */
    private static final long CONEXION_VIVA_MS = 8000;

    static class Riego {
        final String id;
        final String ws;
        final String operario;
        final int duracion;
        final long creado = System.currentTimeMillis();

        Riego(String id, String ws, String operario, int duracion) {
            this.id = id;
            this.ws = ws;
            this.operario = operario;
            this.duracion = duracion;
        }
    }

    private static final ConcurrentHashMap<String, Riego> pendientes = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Riego> activos = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, WM_Central_Thread> estacionesConectadas = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, WM_Central_Thread> operariosConectados = new ConcurrentHashMap<>();
    private static long contadorRiegos = 0;

    // ---------------------------------------------------------------- ALTAS

    /** Devuelve null si el alta es correcta o el motivo del rechazo. */
    public static synchronized String registrarEstacion(String id, String location, WM_Central_Thread hilo) {
        if (!idValido(id)) return "ID_INVALIDO";

        WM_Central_Thread anterior = estacionesConectadas.get(id);
        if (anterior != null && anterior != hilo) {
            if (anterior.msDesdeUltimaActividad() < CONEXION_VIVA_MS) {
                Dashboard.log("Alta rechazada: ya hay una WS [" + id + "] conectada y activa.");
                return "ID_EN_USO";
            }
            // Conexión antigua "zombi" (half-open): la nueva la sustituye
            Dashboard.log("WS [" + id + "] se reconecta. Se descarta la conexión anterior.");
            estacionesConectadas.remove(id, anterior);
            anterior.cerrar();
        }

        String estado = DatabaseManager.registrarEstacion(id, location);
        if (estado == null) return "ERROR_BD";

        estacionesConectadas.put(id, hilo);
        Dashboard.registrarEstacion(id, location, Dashboard.Estado.valueOf(estado));
        return null;
    }

    public static synchronized String registrarOperario(String id, WM_Central_Thread hilo) {
        if (!idValido(id)) return "ID_INVALIDO";
        if (!DatabaseManager.existeOperario(id)) {
            Dashboard.log("Operario [" + id + "] rechazado: no está dado de alta en CENTRAL.");
            return "OPERARIO_NO_REGISTRADO";
        }
        WM_Central_Thread anterior = operariosConectados.get(id);
        if (anterior != null && anterior != hilo) {
            if (anterior.msDesdeUltimaActividad() < CONEXION_VIVA_MS) {
                Dashboard.log("Operario [" + id + "] rechazado: ya hay una sesión activa con ese ID.");
                return "ID_EN_USO";
            }
            operariosConectados.remove(id, anterior);
            anterior.cerrar();
        }
        operariosConectados.put(id, hilo);
        Dashboard.log("Operario [" + id + "] conectado.");
        return null;
    }

    public static synchronized void operarioDesconectado(String id, WM_Central_Thread hilo) {
        if (operariosConectados.remove(id, hilo)) {
            Dashboard.log("Operario [" + id + "] desconectado.");
        }
    }

    /**
     * Estrategias 2 y 3: la estación ha caído (timeout de 15 s, EOF o error de socket).
     * Si estaba regando se aborta el riego de emergencia.
     */
    public static synchronized void estacionDesconectada(String id, WM_Central_Thread hilo) {
        // remove(id, hilo): si la estación ya se reconectó con otro socket, no tocamos su estado
        if (!estacionesConectadas.remove(id, hilo)) return;

        Dashboard.log("Caída/Timeout detectado en la conexión de WS [" + id + "].");
        abortarRiego(id, "La estación [" + id + "] ha perdido la conexión durante el riego", false);
        cambiarEstado(id, Dashboard.Estado.DESCONECTADA);
    }

    // ---------------------------------------------------------------- RIEGO

    /** Paso 4 de la mecánica: valida la estación y pide autorización a su Monitor. */
    public static synchronized void solicitarRiego(String ws, String operario, int duracion) {
        duracion = normalizarDuracion(duracion);
        Dashboard.log("Petición de activación para WS [" + ws + "] por [" + operario + "] (" + duracion + " s).");
        notificarOperario(operario, ws, "PROCESANDO: Comprobando el estado de la estación " + ws + "...");

        if (!DatabaseManager.existeEstacion(ws)) {
            denegar(ws, operario, "La estación " + ws + " no existe en CENTRAL");
            return;
        }
        String estado = DatabaseManager.obtenerEstado(ws);
        if (!"DISPONIBLE".equals(estado)) {
            denegar(ws, operario, "La estación " + ws + " se encuentra en estado " + estado);
            return;
        }
        WM_Central_Thread hiloWS = estacionesConectadas.get(ws);
        if (hiloWS == null) {
            denegar(ws, operario, "La estación " + ws + " está desconectada");
            return;
        }
        Riego pendiente = pendientes.get(ws);
        if (pendiente != null && System.currentTimeMillis() - pendiente.creado < TIMEOUT_SOLICITUD_MS) {
            denegar(ws, operario, "La estación " + ws + " ya tiene otra solicitud en curso");
            return;
        }

        Riego r = new Riego("R" + System.currentTimeMillis() + "_" + (++contadorRiegos), ws, operario, duracion);
        pendientes.put(ws, r);
        notificarOperario(operario, ws, "PROCESANDO: Estación " + ws + " disponible. Solicitando autorización a la estación...");
        hiloWS.enviarTrama("SOLICITUD_RIEGO#" + r.id + "#" + operario + "#" + duracion);
    }

    public static synchronized void riegoAutorizado(String ws, String riegoId) {
        Riego r = pendientes.get(ws);
        if (r == null || !r.id.equals(riegoId)) {
            Dashboard.log("Autorización de WS [" + ws + "] ignorada: solicitud " + riegoId + " desconocida o caducada.");
            enviarAEstacion(ws, "ABORTAR_RIEGO#Solicitud caducada");
            return;
        }
        pendientes.remove(ws);

        // Mientras se esperaba la respuesta la estación pudo bloquearse o averiarse
        String estado = DatabaseManager.obtenerEstado(ws);
        if (!"DISPONIBLE".equals(estado)) {
            enviarAEstacion(ws, "ABORTAR_RIEGO#La estación ya no está disponible");
            denegar(ws, r.operario, "La estación " + ws + " ha pasado a estado " + estado);
            return;
        }

        activos.put(ws, r);
        cambiarEstado(ws, Dashboard.Estado.REGANDO);
        Dashboard.asignarOperario(ws, r.operario);
        Dashboard.log("WS [" + ws + "] AUTORIZA el riego de [" + r.operario + "]. Riego en curso (" + r.duracion + " s).");

        // Estrategia 5: el Engine abre la electroválvula al consumir este evento de Kafka
        KafkaManager.publicarEvento("INICIO_RIEGO", ws + "#" + r.operario + "#" + r.duracion + "#" + r.id);
        notificarOperario(r.operario, ws, "AUTORIZADO: El riego ha comenzado en " + ws + " (duración " + r.duracion + " s)");
    }

    public static synchronized void riegoDenegado(String ws, String riegoId, String motivo) {
        Riego r = pendientes.get(ws);
        if (r == null || !r.id.equals(riegoId)) return;
        pendientes.remove(ws);
        denegar(ws, r.operario, "La estación " + ws + " ha rechazado el riego: " + motivo);
    }

    public static synchronized void telemetria(String ws, String caudal, String volumen) {
        Riego r = activos.get(ws);
        if (r == null) return; // Telemetría tardía de un riego que ya terminó o se abortó
        Dashboard.actualizarTelemetria(ws, caudal, volumen);
        // Los operarios reciben la telemetría de su WS por Kafka
        KafkaManager.publicarEvento("TELEMETRIA", ws + "#" + r.operario + "#" + caudal + "#" + volumen);
    }

    /** Paso 8: la WS informa del fin (tiempo agotado u opción de menú). */
    public static synchronized void finRiego(String ws, String volumenTotal, String duracionS) {
        Riego r = activos.remove(ws);
        if (r == null) {
            Dashboard.log("FIN_RIEGO de WS [" + ws + "] ignorado: no había riego activo.");
            return;
        }
        String caudalMedio = "0.00";
        try {
            double v = Double.parseDouble(volumenTotal);
            double d = Double.parseDouble(duracionS);
            if (d > 0) caudalMedio = String.format(Locale.US, "%.2f", v / (d / 60.0));
        } catch (NumberFormatException ignored) {}

        String resumen = String.format("Estación %s | Volumen consumido: %s L | Duración: %s s | Caudal medio: %s L/min",
                ws, volumenTotal, duracionS, caudalMedio);
        Dashboard.log("Riego finalizado en WS [" + ws + "]. " + resumen);

        cambiarEstado(ws, DatabaseManager.esBloqueada(ws) ? Dashboard.Estado.FUERA_SERVICIO : Dashboard.Estado.DISPONIBLE);
        KafkaManager.publicarEvento("FIN_RIEGO", ws + "#" + volumenTotal + "#" + duracionS);
        notificarOperario(r.operario, ws, "RESUMEN_FIN_RIEGO: " + resumen);
    }

    // ---------------------------------------------------------------- AVERÍAS

    public static synchronized void averia(String ws) {
        Dashboard.log("¡ALERTA! Avería/FUGA detectada en WS [" + ws + "].");
        abortarRiego(ws, "Fuga detectada en la estación [" + ws + "]", true);
        cambiarEstado(ws, Dashboard.Estado.FUGA);
        KafkaManager.publicarEvento("AVERIA", ws);
    }

    public static synchronized void averiaResuelta(String ws) {
        if (!"FUGA".equals(DatabaseManager.obtenerEstado(ws))) return;
        boolean bloq = DatabaseManager.esBloqueada(ws);
        Dashboard.log("Avería resuelta en WS [" + ws + "]." + (bloq ? " Sigue bloqueada por CENTRAL." : ""));
        cambiarEstado(ws, bloq ? Dashboard.Estado.FUERA_SERVICIO : Dashboard.Estado.DISPONIBLE);
        KafkaManager.publicarEvento("RESUELTA", ws);
    }

    // ---------------------------------------------------------------- ÓRDENES DE CENTRAL (punto 11)

    public static synchronized void iniciarDesdeCentral(String ws, int duracion) {
        solicitarRiego(ws, OPERARIO_CENTRAL, duracion);
    }

    public static synchronized void bloquear(String ws) {
        if (!DatabaseManager.existeEstacion(ws)) return;
        Dashboard.log("CENTRAL bloquea la estación [" + ws + "].");
        DatabaseManager.setBloqueada(ws, true);
        abortarRiego(ws, "La estación [" + ws + "] ha sido bloqueada por CENTRAL", true);
        String estado = DatabaseManager.obtenerEstado(ws);
        // Si está desconectada o en fuga mantiene ese estado; al conectarse/repararse quedará FUERA_SERVICIO
        if ("DISPONIBLE".equals(estado) || "REGANDO".equals(estado)) {
            cambiarEstado(ws, Dashboard.Estado.FUERA_SERVICIO);
        }
        enviarAEstacion(ws, "AVISO#Estación bloqueada por CENTRAL (Fuera de Servicio)");
        KafkaManager.publicarEvento("BLOQUEO", ws);
    }

    public static synchronized void activar(String ws) {
        if (!DatabaseManager.existeEstacion(ws)) return;
        Dashboard.log("CENTRAL reactiva la estación [" + ws + "].");
        DatabaseManager.setBloqueada(ws, false);
        if ("FUERA_SERVICIO".equals(DatabaseManager.obtenerEstado(ws))) {
            cambiarEstado(ws, Dashboard.Estado.DISPONIBLE);
        }
        enviarAEstacion(ws, "AVISO#Estación reactivada por CENTRAL");
        KafkaManager.publicarEvento("ACTIVACION", ws);
    }

    public static void iniciarTodas(int duracion) {
        for (String ws : DatabaseManager.listarIdsEstaciones()) iniciarDesdeCentral(ws, duracion);
    }

    public static void bloquearTodas() {
        for (String ws : DatabaseManager.listarIdsEstaciones()) bloquear(ws);
    }

    public static void activarTodas() {
        for (String ws : DatabaseManager.listarIdsEstaciones()) activar(ws);
    }

    // ---------------------------------------------------------------- AUXILIARES

    /**
     * Aborta el riego activo (y/o la solicitud pendiente) de una estación:
     * avisa al operario, publica ABORTO_RIEGO en Kafka y, si se pide, ordena al Monitor cerrar.
     */
    private static void abortarRiego(String ws, String motivo, boolean avisarEstacion) {
        Riego pendiente = pendientes.remove(ws);
        if (pendiente != null) {
            denegar(ws, pendiente.operario, motivo);
        }
        Riego r = activos.remove(ws);
        if (r != null) {
            Dashboard.log("ABORTADO: riego de [" + r.operario + "] en WS [" + ws + "]. " + motivo);
            notificarOperario(r.operario, ws, "ABORTADO: " + motivo);
            KafkaManager.publicarEvento("ABORTO_RIEGO", ws + "#" + motivo);
            if (avisarEstacion) enviarAEstacion(ws, "ABORTAR_RIEGO#" + motivo);
        }
    }

    private static void denegar(String ws, String operario, String motivo) {
        Dashboard.log("Riego DENEGADO para [" + operario + "] en WS [" + ws + "]: " + motivo);
        notificarOperario(operario, ws, "DENEGADO: " + motivo);
    }

    private static void cambiarEstado(String ws, Dashboard.Estado estado) {
        DatabaseManager.actualizarEstado(ws, estado.name());
        Dashboard.actualizarEstado(ws, estado);
    }

    /** Envía un texto al operario. Si el riego se pidió desde la propia WS, se le envía como AVISO. */
    private static void notificarOperario(String operario, String ws, String texto) {
        if (OPERARIO_LOCAL.equals(operario)) {
            enviarAEstacion(ws, "AVISO#" + texto);
            return;
        }
        WM_Central_Thread hilo = operariosConectados.get(operario);
        if (hilo != null) hilo.enviarTexto(texto);
    }

    private static void enviarAEstacion(String ws, String trama) {
        WM_Central_Thread hilo = estacionesConectadas.get(ws);
        if (hilo != null) hilo.enviarTrama(trama);
    }

    private static int normalizarDuracion(int d) {
        if (d <= 0) return DURACION_DEFECTO_S;
        return Math.min(d, DURACION_MAX_S);
    }

    private static boolean idValido(String id) {
        return id != null && id.matches("[A-Za-z0-9_\\-]{1,32}");
    }
}
