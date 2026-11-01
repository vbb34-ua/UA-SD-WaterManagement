import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lógica de negocio de CENTRAL: alta de estaciones/operarios, ciclo de vida de un riego,
 * averías y órdenes de CENTRAL (iniciar / bloquear / activar).
 *
 * La usan tanto los hilos de sockets (WM_Central_Thread) como el Dashboard Web.
 * Los métodos son synchronized para que las comprobaciones de estado y los cambios
 * sean atómicos (ej. dos operarios pidiendo la misma estación a la vez).
 *
 * Ninguna transacción se pierde:
 *  - Cada riego se guarda en SQLite (tabla riegos) con un id único. La estación envía
 *    FIN_RIEGO con ese id y CENTRAL lo confirma (CONFIRMACION_FIN); la estación lo
 *    reenvía hasta recibir la confirmación, aunque CENTRAL se haya reiniciado.
 *  - Los mensajes para operarios desconectados se guardan en un buzón (SQLite) y se
 *    entregan cuando el operario vuelve a conectarse.
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

    /**
     * Alta de una estación. Si el Monitor indica que tiene un riego en curso (riegoEnCurso != null,
     * formato {id, operario, duracion}) se recupera: pasa por aquí tras una caída de CENTRAL o de la red.
     * Devuelve null si el alta es correcta o el motivo del rechazo.
     */
    public static synchronized String registrarEstacion(String id, String location, String[] riegoEnCurso,
                                                        WM_Central_Thread hilo) {
        if (!idValido(id)) return "ID_INVALIDO";
        // Autenticación: solo se admiten estaciones dadas de alta en la red (BD de CENTRAL)
        if (!DatabaseManager.existeEstacion(id)) {
            Dashboard.log("Alta DENEGADA: la WS [" + id + "] no está registrada en CENTRAL.");
            return "WS_NO_REGISTRADA";
        }

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

        // location == null: la estación no indica ubicación y se mantiene la de la BD
        String estado = DatabaseManager.registrarEstacion(id, location);
        if (estado == null) return "ERROR_BD";
        if (location == null) location = DatabaseManager.obtenerUbicacion(id);

        estacionesConectadas.put(id, hilo);
        Dashboard.registrarEstacion(id, location, Dashboard.Estado.valueOf(estado));

        if (riegoEnCurso != null) {
            recuperarRiegoEnCurso(id, riegoEnCurso, hilo);
        }
        return null;
    }

    /** La estación sigue regando tras una caída de CENTRAL o de la red: se vuelve a mostrar REGANDO. */
    private static void recuperarRiegoEnCurso(String ws, String[] datos, WM_Central_Thread hilo) {
        String riegoId = datos[0];
        String[] guardado = DatabaseManager.obtenerRiego(riegoId);
        String operario = guardado != null ? guardado[2] : datos[1];
        int duracion = guardado != null ? Integer.parseInt(guardado[3]) : parseEntero(datos[2], DURACION_DEFECTO_S);

        if (DatabaseManager.esBloqueada(ws) || (guardado != null && "FINALIZADO".equals(guardado[4]))) {
            // No se puede continuar: se ordena parar (después del ACK del registro)
            DatabaseManager.guardarRiego(riegoId, ws, operario, duracion, "INTERRUMPIDO");
            hilo.alTerminarRespuesta(() -> hilo.enviarTrama("ABORTAR_RIEGO#La estación está bloqueada por CENTRAL"));
            Dashboard.log("WS [" + ws + "] tenía el riego " + riegoId + " en curso pero está bloqueada: se aborta.");
            return;
        }

        boolean estabaInterrumpido = guardado != null && "INTERRUMPIDO".equals(guardado[4]);
        Riego r = new Riego(riegoId, ws, operario, duracion);
        activos.put(ws, r);
        DatabaseManager.guardarRiego(riegoId, ws, operario, duracion, "ACTIVO");
        cambiarEstado(ws, Dashboard.Estado.REGANDO);
        Dashboard.asignarOperario(ws, operario);
        Dashboard.log("WS [" + ws + "] reconectada con el riego " + riegoId + " de [" + operario + "] en curso. Se recupera.");
        if (estabaInterrumpido) {
            notificarOperario(operario, ws, "REANUDADO: Se ha recuperado la conexión con la estación " + ws
                    + ". El riego continúa.", true);
        }
    }

    /** Alta de una estación nueva en la red desde el panel de CENTRAL. Devuelve null si va bien o el motivo del error. */
    public static synchronized String altaEstacion(String id, String location) {
        if (!idValido(id)) return "ID inválido (solo letras, números, '_' y '-')";
        if (location == null || location.trim().isEmpty()) location = "Sin ubicación";
        location = location.replace("#", " ").trim();
        if (!DatabaseManager.altaEstacion(id, location)) return "La estación " + id + " ya existe";
        Dashboard.cargarEstacion(id, location, Dashboard.Estado.DESCONECTADA);
        Dashboard.log("Alta de nueva WS [" + id + "] en [" + location + "] (Desconectada hasta que conecte).");
        return null;
    }

    /** Alta de un operario nuevo desde el panel de CENTRAL. */
    public static synchronized String altaOperario(String id) {
        if (!idValido(id)) return "ID inválido (solo letras, números, '_' y '-')";
        if (!DatabaseManager.altaOperario(id)) return "El operario " + id + " ya existe";
        Dashboard.log("Alta de nuevo operario [" + id + "].");
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
        // Tras el ACK: mensajes del buzón y servicio en curso
        hilo.alTerminarRespuesta(() -> entregarPendientes(id));
        return null;
    }

    /** Entrega al operario lo que ocurrió mientras estaba desconectado. */
    private static synchronized void entregarPendientes(String operario) {
        WM_Central_Thread hilo = operariosConectados.get(operario);
        if (hilo == null) return;

        List<String> mensajes = DatabaseManager.extraerMensajesPendientes(operario);
        if (!mensajes.isEmpty()) {
            hilo.enviarTexto("AVISO: Tiene " + mensajes.size() + " mensaje(s) recibido(s) mientras estaba desconectado:");
            for (int i = 0; i < mensajes.size(); i++) {
                if (!hilo.enviarTexto(mensajes.get(i))) {
                    // Se ha vuelto a caer: el resto vuelve al buzón
                    for (int j = i; j < mensajes.size(); j++) DatabaseManager.guardarMensajePendiente(operario, mensajes.get(j));
                    return;
                }
            }
            Dashboard.log("Entregados " + mensajes.size() + " mensaje(s) pendiente(s) al operario [" + operario + "].");
        }
        for (Riego r : activos.values()) {
            if (r.operario.equals(operario)) {
                hilo.enviarTexto("SERVICIO_EN_CURSO:" + r.ws);
            }
        }
    }

    public static synchronized void operarioDesconectado(String id, WM_Central_Thread hilo) {
        if (operariosConectados.remove(id, hilo)) {
            Dashboard.log("Operario [" + id + "] desconectado. Sus riegos continúan; recibirá el resultado al reconectar.");
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

    /**
     * La estación ha terminado de reenviar lo que tenía pendiente tras reconectarse.
     * Los riegos que CENTRAL tenía como ACTIVOS y la estación ya no conoce se dan por interrumpidos.
     */
    public static synchronized void estacionSincronizada(String ws) {
        Riego actual = activos.get(ws);
        for (String[] r : DatabaseManager.riegosActivosDe(ws)) {
            if (actual != null && actual.id.equals(r[0])) continue;
            DatabaseManager.actualizarEstadoRiego(r[0], "INTERRUMPIDO");
            Dashboard.log("Riego " + r[0] + " de [" + r[1] + "] en WS [" + ws + "] interrumpido (la estación ya no lo tiene en curso).");
            notificarOperario(r[1], ws, "ABORTADO: El riego en la estación [" + ws
                    + "] se interrumpió mientras CENTRAL no tenía conexión con ella", true);
        }
    }

    // ---------------------------------------------------------------- RIEGO

    /** Paso 4 de la mecánica: valida la estación y pide autorización a su Monitor. */
    public static synchronized String solicitarRiego(String ws, String operario, int duracion) {
        duracion = normalizarDuracion(duracion);
        Dashboard.log("Petición de activación para WS [" + ws + "] por [" + operario + "] (" + duracion + " s).");
        notificarOperario(operario, ws, "PROCESANDO: Comprobando el estado de la estación " + ws + "...", false);

        if (!DatabaseManager.existeEstacion(ws)) {
            return denegar(ws, operario, "La estación " + ws + " no existe en CENTRAL");
        }
        String estado = DatabaseManager.obtenerEstado(ws);
        if (!"DISPONIBLE".equals(estado)) {
            return denegar(ws, operario, "La estación " + ws + " se encuentra en estado " + estado);
        }
        WM_Central_Thread hiloWS = estacionesConectadas.get(ws);
        if (hiloWS == null) {
            return denegar(ws, operario, "La estación " + ws + " está desconectada");
        }
        Riego pendiente = pendientes.get(ws);
        if (pendiente != null && System.currentTimeMillis() - pendiente.creado < TIMEOUT_SOLICITUD_MS) {
            return denegar(ws, operario, "La estación " + ws + " ya tiene otra solicitud en curso");
        }

        Riego r = new Riego("R" + System.currentTimeMillis() + "_" + (++contadorRiegos), ws, operario, duracion);
        pendientes.put(ws, r);
        notificarOperario(operario, ws, "PROCESANDO: Estación " + ws + " disponible. Solicitando autorización a la estación...", false);
        hiloWS.enviarTrama("SOLICITUD_RIEGO#" + r.id + "#" + operario + "#" + duracion);
        return null;
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
        DatabaseManager.guardarRiego(r.id, ws, r.operario, r.duracion, "ACTIVO");
        cambiarEstado(ws, Dashboard.Estado.REGANDO);
        Dashboard.asignarOperario(ws, r.operario);
        Dashboard.log("WS [" + ws + "] AUTORIZA el riego de [" + r.operario + "]. Riego en curso (" + r.duracion + " s).");

        // Estrategia 5: el Engine abre la electroválvula al consumir este evento de Kafka
        KafkaManager.publicarEvento("INICIO_RIEGO", ws + "#" + r.operario + "#" + r.duracion + "#" + r.id);
        notificarOperario(r.operario, ws, "AUTORIZADO: El riego ha comenzado en " + ws + " (duración " + r.duracion + " s)", true);
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

    /**
     * Paso 8: la WS informa del fin de un riego (tiempo, menú, aborto, avería o pérdida de enlace).
     * Es idempotente: la estación lo reenvía hasta recibir CONFIRMACION_FIN.
     */
    public static synchronized void finRiego(String ws, String riegoId, String volumenTotal, String duracionS,
                                             String motivo, WM_Central_Thread hilo) {
        try {
            String operario;
            Riego r = activos.get(ws);
            boolean eraActivo = r != null && r.id.equals(riegoId);

            if (eraActivo) {
                activos.remove(ws);
                operario = r.operario;
            } else {
                String[] guardado = DatabaseManager.obtenerRiego(riegoId);
                if (guardado == null || "FINALIZADO".equals(guardado[4])) {
                    return; // Desconocido o ya procesado (reenvío duplicado)
                }
                operario = guardado[2];
            }

            String caudalMedio = "0.00";
            try {
                double v = Double.parseDouble(volumenTotal);
                double d = Double.parseDouble(duracionS);
                if (d > 0) caudalMedio = String.format(Locale.US, "%.2f", v / (d / 60.0));
            } catch (NumberFormatException ignored) {}

            String resumen = String.format("Estación %s | Volumen consumido: %s L | Duración: %s s | Caudal medio: %s L/min | %s",
                    ws, volumenTotal, duracionS, caudalMedio, textoMotivo(motivo));
            DatabaseManager.finalizarRiego(riegoId, volumenTotal, duracionS, motivo);
            Dashboard.log("Riego " + riegoId + " finalizado en WS [" + ws + "]. " + resumen);

            if (eraActivo && Dashboard.getEstado(ws) == Dashboard.Estado.REGANDO) {
                cambiarEstado(ws, DatabaseManager.esBloqueada(ws) ? Dashboard.Estado.FUERA_SERVICIO : Dashboard.Estado.DISPONIBLE);
            }
            KafkaManager.publicarEvento("FIN_RIEGO", ws + "#" + volumenTotal + "#" + duracionS + "#" + riegoId);
            notificarOperario(operario, ws, "RESUMEN_FIN_RIEGO: " + resumen, true);
        } finally {
            // Siempre se confirma, así la estación deja de reenviarlo
            hilo.enviarTrama("CONFIRMACION_FIN#" + riegoId);
        }
    }

    private static String textoMotivo(String motivo) {
        if (motivo == null) return "Finalizado";
        switch (motivo) {
            case "TIEMPO":   return "Fin: tiempo de riego alcanzado";
            case "MANUAL":   return "Fin: finalizado desde el menú de la estación";
            case "ABORTADO": return "Fin: riego abortado";
            case "AVERIA":   return "Fin: interrumpido por avería (volumen regado hasta la avería)";
            case "ENLACE":   return "Fin: interrumpido por pérdida de enlace entre Monitor y Engine";
            default:         return "Fin: " + motivo;
        }
    }

    // ---------------------------------------------------------------- AVERÍAS

    public static synchronized void averia(String ws) {
        Dashboard.log("¡ALERTA! Avería/FUGA detectada en WS [" + ws + "].");
        abortarRiego(ws, "Avería detectada en la estación [" + ws
                + "]. Recibirá el resumen de lo regado cuando la estación informe", true);
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

    /** Devuelve null si la solicitud se envió a la estación, o el motivo de la denegación. */
    public static synchronized String iniciarDesdeCentral(String ws, int duracion) {
        return solicitarRiego(ws, OPERARIO_CENTRAL, duracion);
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
        enviarAEstacion(ws, "AVISO#Estación bloqueada por CENTRAL (Out of Service)");
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
     * Aborta el riego activo (y/o la solicitud pendiente) de una estación: lo marca como INTERRUMPIDO
     * en la BD, avisa al operario, publica ABORTO_RIEGO en Kafka y, si se pide, ordena al Monitor cerrar.
     * El resumen final (volumen regado) llegará después con el FIN_RIEGO de la estación.
     */
    private static void abortarRiego(String ws, String motivo, boolean avisarEstacion) {
        Riego pendiente = pendientes.remove(ws);
        if (pendiente != null) {
            denegar(ws, pendiente.operario, motivo);
        }
        Riego r = activos.remove(ws);
        if (r != null) {
            DatabaseManager.actualizarEstadoRiego(r.id, "INTERRUMPIDO");
            Dashboard.log("ABORTADO: riego de [" + r.operario + "] en WS [" + ws + "]. " + motivo);
            notificarOperario(r.operario, ws, "ABORTADO: " + motivo, true);
            KafkaManager.publicarEvento("ABORTO_RIEGO", ws + "#" + motivo);
            if (avisarEstacion) enviarAEstacion(ws, "ABORTAR_RIEGO#" + motivo);
        }
    }

    private static String denegar(String ws, String operario, String motivo) {
        Dashboard.log("Riego DENEGADO para [" + operario + "] en WS [" + ws + "]: " + motivo);
        notificarOperario(operario, ws, "DENEGADO: " + motivo, true);
        return motivo;
    }

    private static void cambiarEstado(String ws, Dashboard.Estado estado) {
        DatabaseManager.actualizarEstado(ws, estado.name());
        Dashboard.actualizarEstado(ws, estado);
    }

    /**
     * Envía un texto al operario. Si el riego se pidió desde la propia WS, se le envía como AVISO.
     * Si el operario no está conectado (o el envío falla) y guardarSiOffline, va a su buzón.
     */
    private static void notificarOperario(String operario, String ws, String texto, boolean guardarSiOffline) {
        if (OPERARIO_LOCAL.equals(operario)) {
            enviarAEstacion(ws, "AVISO#" + texto);
            return;
        }
        if (OPERARIO_CENTRAL.equals(operario)) return;

        WM_Central_Thread hilo = operariosConectados.get(operario);
        boolean entregado = hilo != null && hilo.enviarTexto(texto);
        if (!entregado && guardarSiOffline) {
            DatabaseManager.guardarMensajePendiente(operario, texto);
            Dashboard.log("Operario [" + operario + "] no conectado: mensaje guardado en su buzón.");
        }
    }

    private static void enviarAEstacion(String ws, String trama) {
        WM_Central_Thread hilo = estacionesConectadas.get(ws);
        if (hilo != null) hilo.enviarTrama(trama);
    }

    private static int normalizarDuracion(int d) {
        if (d <= 0) return DURACION_DEFECTO_S;
        return Math.min(d, DURACION_MAX_S);
    }

    private static int parseEntero(String s, int defecto) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return defecto; }
    }

    private static boolean idValido(String id) {
        return id != null && id.matches("[A-Za-z0-9_\\-]{1,32}");
    }
}
