package com.watermanagement.central.logica;

import com.watermanagement.central.kafka.KafkaManager;
import com.watermanagement.central.red.WM_Central_Thread;
import com.watermanagement.central.panel.Dashboard;
import com.watermanagement.central.datos.DatabaseManager;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Lógica de negocio de CENTRAL: alta de estaciones y operarios, ciclo de vida de un riego,
 * averías y órdenes de CENTRAL (iniciar, bloquear y activar).
 *
 * <p>La usan los hilos de sockets ({@link WM_Central_Thread}) y el panel web. Los métodos
 * públicos son {@code synchronized} para que comprobar un estado y cambiarlo sea atómico
 * (p. ej. dos operarios pidiendo la misma estación a la vez).
 *
 * <p>Ninguna transacción se pierde:
 * <ul>
 *   <li>Cada riego se guarda en SQLite (tabla {@code riegos}) con un id único. La estación envía
 *       {@code FIN_RIEGO} con ese id y lo reenvía hasta recibir {@code CONFIRMACION_FIN}, aunque
 *       CENTRAL se haya reiniciado entretanto.</li>
 *   <li>Los mensajes para operarios desconectados se guardan en un buzón (SQLite) y se
 *       entregan cuando el operario vuelve a conectarse.</li>
 * </ul>
 */
public class GestorRiegos {

    /** Duración aplicada cuando la solicitud no indica una válida. */
    public static final int DURACION_DEFECTO_S = 15;
    public static final int DURACION_MAX_S = 3600;
    /** Id de operario usado cuando el riego se pide desde el menú de la propia estación. */
    public static final String OPERARIO_LOCAL = "LOCAL";
    /** Id de operario usado cuando el riego lo inicia CENTRAL desde su panel. */
    public static final String OPERARIO_CENTRAL = "CENTRAL";

    /** Tiempo que una solicitud espera la respuesta del Monitor antes de admitir otra en la misma WS. */
    private static final long TIMEOUT_SOLICITUD_MS = 10000;
    /** Una conexión con actividad más reciente que esto se considera viva (para detectar IDs duplicados). */
    private static final long CONEXION_VIVA_MS = 8000;

    /** Un riego solicitado o en curso. */
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

    /** Solicitudes enviadas a un Monitor que aún no ha respondido, por id de WS. */
    private static final ConcurrentHashMap<String, Riego> pendientes = new ConcurrentHashMap<>();
    /** Riegos autorizados y en curso, por id de WS. */
    private static final ConcurrentHashMap<String, Riego> activos = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, WM_Central_Thread> estacionesConectadas = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, WM_Central_Thread> operariosConectados = new ConcurrentHashMap<>();
    private static long contadorRiegos = 0;

    // ---------------------------------------------------------------- ALTAS

    /**
     * Registra la conexión de un Monitor. Solo se admiten estaciones dadas de alta en la BD.
     * Si el Monitor indica un riego en curso (tras una caída de CENTRAL o de la red), se recupera.
     *
     * @param location     ubicación enviada por el Monitor, o null para mantener la de la BD
     * @param riegoEnCurso {@code {riegoId, operario, duracion}} o null si la estación no está regando
     * @return null si el alta es correcta, o el motivo del rechazo ({@code WS_NO_REGISTRADA}, {@code ID_EN_USO}...)
     */
    public static synchronized String registrarEstacion(String id, String location, String[] riegoEnCurso,
                                                        WM_Central_Thread hilo) {
        if (!idValido(id)) return "ID_INVALIDO";
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
            // La conexión anterior lleva tiempo sin actividad (socket semiabierto): la nueva la sustituye
            Dashboard.log("WS [" + id + "] se reconecta. Se descarta la conexión anterior.");
            estacionesConectadas.remove(id, anterior);
            anterior.cerrar();
        }

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

    /**
     * La estación siguió regando durante una caída de CENTRAL o de la red: el riego vuelve a
     * figurar como ACTIVO y la WS como REGANDO. Si entretanto la WS se bloqueó o el riego ya
     * constaba como finalizado, se ordena pararlo.
     *
     * @param datos {@code {riegoId, operario, duracion}} tal como los envía el Monitor; si el riego
     *              está en la BD, el operario y la duración se toman de allí
     */
    private static void recuperarRiegoEnCurso(String ws, String[] datos, WM_Central_Thread hilo) {
        String riegoId = datos[0];
        String[] guardado = DatabaseManager.obtenerRiego(riegoId);
        String operario = guardado != null ? guardado[2] : datos[1];
        int duracion = guardado != null ? Integer.parseInt(guardado[3]) : parseEntero(datos[2], DURACION_DEFECTO_S);

        if (DatabaseManager.esBloqueada(ws) || (guardado != null && "FINALIZADO".equals(guardado[4]))) {
            // La orden de parar se envía después del ACK del registro
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

    /**
     * Da de alta una estación nueva desde el panel de CENTRAL. Queda DESCONECTADA hasta que su Monitor se conecte.
     *
     * @param location ubicación (vacía = "Sin ubicación"); el carácter '#' se elimina porque separa campos en las tramas
     * @return null si el alta es correcta, o el motivo del error para mostrarlo en el panel
     */
    public static synchronized String altaEstacion(String id, String location) {
        if (!idValido(id)) return "ID inválido (solo letras, números, '_' y '-')";
        if (location == null || location.trim().isEmpty()) location = "Sin ubicación";
        location = location.replace("#", " ").trim();
        if (!DatabaseManager.altaEstacion(id, location)) return "La estación " + id + " ya existe";
        Dashboard.cargarEstacion(id, location, Dashboard.Estado.DESCONECTADA);
        Dashboard.log("Alta de nueva WS [" + id + "] en [" + location + "] (Desconectada hasta que conecte).");
        return null;
    }

    /**
     * Da de alta un operario nuevo desde el panel de CENTRAL.
     *
     * @return null si el alta es correcta, o el motivo del error para mostrarlo en el panel
     */
    public static synchronized String altaOperario(String id) {
        if (!idValido(id)) return "ID inválido (solo letras, números, '_' y '-')";
        if (!DatabaseManager.altaOperario(id)) return "El operario " + id + " ya existe";
        Dashboard.log("Alta de nuevo operario [" + id + "].");
        return null;
    }

    /**
     * Registra la conexión de un operario. Solo se admiten operarios dados de alta en la BD.
     * Tras el ACK se le entregan los mensajes de su buzón y los riegos suyos que siguen en curso.
     *
     * @return null si el registro es correcto, o el motivo del rechazo ({@code OPERARIO_NO_REGISTRADO}, {@code ID_EN_USO}...)
     */
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
        hilo.alTerminarRespuesta(() -> entregarPendientes(id));
        return null;
    }

    /**
     * Entrega al operario los mensajes de su buzón y le indica sus riegos en curso
     * ({@code SERVICIO_EN_CURSO}) para que vuelva a seguir su telemetría.
     */
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

    /**
     * El operario se ha desconectado. Sus riegos continúan y los mensajes que no reciba
     * quedan en su buzón.
     *
     * @param hilo conexión que se cierra; si el operario ya se reconectó por otra, no se hace nada
     */
    public static synchronized void operarioDesconectado(String id, WM_Central_Thread hilo) {
        if (operariosConectados.remove(id, hilo)) {
            Dashboard.log("Operario [" + id + "] desconectado. Sus riegos continúan; recibirá el resultado al reconectar.");
        }
    }

    /**
     * La conexión con una estación se ha perdido (timeout de 15 s, EOF o error de socket):
     * pasa a DESCONECTADA y, si estaba regando, el riego se aborta y se avisa al operario.
     *
     * @param hilo conexión que se cierra; si la estación ya se reconectó por otra, no se hace nada
     */
    public static synchronized void estacionDesconectada(String id, WM_Central_Thread hilo) {
        if (!estacionesConectadas.remove(id, hilo)) return;

        Dashboard.log("Caída/Timeout detectado en la conexión de WS [" + id + "].");
        abortarRiego(id, "La estación [" + id + "] ha perdido la conexión durante el riego", false);
        cambiarEstado(id, Dashboard.Estado.DESCONECTADA);
    }

    /**
     * La estación ha terminado de reenviar lo que tenía pendiente tras reconectarse
     * ({@code SINCRONIZADO}). Los riegos que la BD tenía como ACTIVOS y la estación ya no
     * conoce se marcan como INTERRUMPIDOS y se avisa a su operario.
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

    /**
     * Comprueba que la estación existe, está DISPONIBLE y conectada, y pide autorización
     * a su Monitor ({@code SOLICITUD_RIEGO}). El operario recibe cada paso como {@code PROCESANDO:}.
     *
     * @param operario id del operario, {@link #OPERARIO_LOCAL} o {@link #OPERARIO_CENTRAL}
     * @param duracion segundos; si no es válida se usa {@link #DURACION_DEFECTO_S} (máximo {@link #DURACION_MAX_S})
     * @return null si la solicitud se envió al Monitor, o el motivo de la denegación
     */
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

    /**
     * El Monitor autoriza el riego: pasa a ACTIVO en la BD, la WS a REGANDO y se publica
     * {@code INICIO_RIEGO} en Kafka, que es la orden para que el Engine abra la válvula.
     * Si la solicitud ya no es válida (caducada, o la WS cambió de estado), se ordena abortar.
     */
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

        KafkaManager.publicarEvento("INICIO_RIEGO", ws + "#" + r.operario + "#" + r.duracion + "#" + r.id);
        notificarOperario(r.operario, ws, "AUTORIZADO: El riego ha comenzado en " + ws + " (duración " + r.duracion + " s)", true);
    }

    /** El Monitor rechaza la solicitud (p. ej. Engine averiado): se informa al operario. */
    public static synchronized void riegoDenegado(String ws, String riegoId, String motivo) {
        Riego r = pendientes.get(ws);
        if (r == null || !r.id.equals(riegoId)) return;
        pendientes.remove(ws);
        denegar(ws, r.operario, "La estación " + ws + " ha rechazado el riego: " + motivo);
    }

    /**
     * Telemetría de un riego en curso: se muestra en el panel y se publica en Kafka,
     * por donde la recibe el operario. Se ignora si el riego ya terminó o se abortó.
     */
    public static synchronized void telemetria(String ws, String caudal, String volumen) {
        Riego r = activos.get(ws);
        if (r == null) return;
        Dashboard.actualizarTelemetria(ws, caudal, volumen);
        KafkaManager.publicarEvento("TELEMETRIA", ws + "#" + r.operario + "#" + caudal + "#" + volumen);
    }

    /**
     * La estación informa del fin de un riego: se guarda en la BD, la WS vuelve a DISPONIBLE
     * (o FUERA_SERVICIO si está bloqueada) y el operario recibe el resumen. Siempre se responde
     * {@code CONFIRMACION_FIN}; como la estación reenvía hasta recibirla, un fin repetido se ignora.
     *
     * @param motivo TIEMPO, MANUAL, ABORTADO, AVERIA o ENLACE, o null si la trama no lo indica
     * @param hilo   conexión por la que se envía la confirmación
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
                    return; // Desconocido o ya procesado: reenvío duplicado
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
            hilo.enviarTrama("CONFIRMACION_FIN#" + riegoId);
        }
    }

    /** @return el motivo de fin en texto legible para el resumen del operario */
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

    /**
     * El Monitor detecta una avería en el Engine (fuga o Engine caído): la WS pasa a FUGA y
     * su riego se aborta. El volumen regado llegará después con el {@code FIN_RIEGO}.
     */
    public static synchronized void averia(String ws) {
        Dashboard.log("¡ALERTA! Avería/FUGA detectada en WS [" + ws + "].");
        abortarRiego(ws, "Avería detectada en la estación [" + ws
                + "]. Recibirá el resumen de lo regado cuando la estación informe", true);
        cambiarEstado(ws, Dashboard.Estado.FUGA);
        KafkaManager.publicarEvento("AVERIA", ws);
    }

    /** La avería se ha resuelto: la WS vuelve a DISPONIBLE, o a FUERA_SERVICIO si CENTRAL la tiene bloqueada. */
    public static synchronized void averiaResuelta(String ws) {
        if (!"FUGA".equals(DatabaseManager.obtenerEstado(ws))) return;
        boolean bloq = DatabaseManager.esBloqueada(ws);
        Dashboard.log("Avería resuelta en WS [" + ws + "]." + (bloq ? " Sigue bloqueada por CENTRAL." : ""));
        cambiarEstado(ws, bloq ? Dashboard.Estado.FUERA_SERVICIO : Dashboard.Estado.DISPONIBLE);
        KafkaManager.publicarEvento("RESUELTA", ws);
    }

    // ---------------------------------------------------------------- ÓRDENES DE CENTRAL (panel web)

    /**
     * Inicia un riego ordenado desde el panel, con las mismas comprobaciones que el de un operario.
     *
     * @return null si la solicitud se envió a la estación, o el motivo de la denegación
     */
    public static synchronized String iniciarDesdeCentral(String ws, int duracion) {
        return solicitarRiego(ws, OPERARIO_CENTRAL, duracion);
    }

    /**
     * Bloquea la estación (queda bloqueada en la BD aunque CENTRAL se reinicie): aborta su
     * riego y pasa a FUERA_SERVICIO. No admite riegos hasta que se active.
     */
    public static synchronized void bloquear(String ws) {
        if (!DatabaseManager.existeEstacion(ws)) return;
        Dashboard.log("CENTRAL bloquea la estación [" + ws + "].");
        DatabaseManager.setBloqueada(ws, true);
        abortarRiego(ws, "La estación [" + ws + "] ha sido bloqueada por CENTRAL", true);
        String estado = DatabaseManager.obtenerEstado(ws);
        // Si está desconectada o en fuga mantiene ese estado; al conectarse o repararse quedará FUERA_SERVICIO
        if ("DISPONIBLE".equals(estado) || "REGANDO".equals(estado)) {
            cambiarEstado(ws, Dashboard.Estado.FUERA_SERVICIO);
        }
        enviarAEstacion(ws, "AVISO#Estación bloqueada por CENTRAL (Out of Service)");
        KafkaManager.publicarEvento("BLOQUEO", ws);
    }

    /** Desbloquea la estación: si estaba FUERA_SERVICIO vuelve a DISPONIBLE. */
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

    /** Ordena un riego en todas las estaciones; las que no estén disponibles lo deniegan. */
    public static void iniciarTodas(int duracion) {
        for (String ws : DatabaseManager.listarIdsEstaciones()) iniciarDesdeCentral(ws, duracion);
    }

    /** Bloquea todas las estaciones. */
    public static void bloquearTodas() {
        for (String ws : DatabaseManager.listarIdsEstaciones()) bloquear(ws);
    }

    /** Activa todas las estaciones. */
    public static void activarTodas() {
        for (String ws : DatabaseManager.listarIdsEstaciones()) activar(ws);
    }

    // ---------------------------------------------------------------- AUXILIARES

    /**
     * Aborta la solicitud pendiente y el riego activo de una estación: el riego queda INTERRUMPIDO
     * en la BD, se avisa al operario y se publica {@code ABORTO_RIEGO}. El resumen final (volumen
     * regado) llegará después con el {@code FIN_RIEGO} de la estación.
     *
     * @param avisarEstacion true para ordenar al Monitor que cierre la válvula; false si la
     *                       estación no está conectada o ya la ha cerrado ella misma
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

    /** Registra la denegación, avisa al operario y devuelve el motivo (para encadenar con {@code return}). */
    private static String denegar(String ws, String operario, String motivo) {
        Dashboard.log("Riego DENEGADO para [" + operario + "] en WS [" + ws + "]: " + motivo);
        notificarOperario(operario, ws, "DENEGADO: " + motivo, true);
        return motivo;
    }

    /** Cambia el estado de la WS en la BD y en el panel. */
    private static void cambiarEstado(String ws, Dashboard.Estado estado) {
        DatabaseManager.actualizarEstado(ws, estado.name());
        Dashboard.actualizarEstado(ws, estado);
    }

    /**
     * Envía un texto a quien pidió el riego: al operario, al menú de la estación (como {@code AVISO})
     * si se pidió desde ella, o a nadie si lo ordenó CENTRAL.
     *
     * @param guardarSiOffline true para guardar el texto en el buzón del operario si no se le puede entregar
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

    /** @return la duración limitada a 1..{@link #DURACION_MAX_S}, o {@link #DURACION_DEFECTO_S} si no es positiva */
    private static int normalizarDuracion(int d) {
        if (d <= 0) return DURACION_DEFECTO_S;
        return Math.min(d, DURACION_MAX_S);
    }

    private static int parseEntero(String s, int defecto) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return defecto; }
    }

    /** IDs de 1 a 32 caracteres: letras, números, '_' y '-' (sin '#', que separa campos en las tramas). */
    private static boolean idValido(String id) {
        return id != null && id.matches("[A-Za-z0-9_\\-]{1,32}");
    }
}
