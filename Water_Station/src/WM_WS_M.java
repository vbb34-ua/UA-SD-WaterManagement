import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * WM_WS_M - Monitor de la estación de riego.
 *
 *  - Se autentica en WM_Central (REGISTRO#id#ubicacion) y le reporta averías,
 *    telemetría y fin de riego.
 *  - Ofrece un ServerSocket al Engine (WM_WS_E) y le envía HEALTH_CHECK cada segundo.
 *    Si el Engine responde KO o no responde, notifica una FUGA a Central.
 *  - Autoriza o deniega las solicitudes de riego de Central y controla la duración
 *    del riego (al cumplirse, ordena al Engine FINALIZAR_RIEGO).
 *
 * Tolerancia a fallos:
 *  - Si cae WM_Central, el riego en curso continúa hasta terminar. Los FIN_RIEGO se guardan
 *    y se reenvían al reconectar hasta que Central los confirma (CONFIRMACION_FIN).
 *  - Si cae el Engine regando, se guarda lo regado (última telemetría) y se envía a Central
 *    cuando la avería se resuelve.
 *
 * Todos los mensajes por socket usan el protocolo <STX>DATA<ETX><LRC> con respuesta ACK/NACK (clase Protocolo).
 *
 * Uso: java WM_WS_M <puerto_para_Engine> <IP_Central> <puerto_Central> <id_WS> [ubicacion]
 */
public class WM_WS_M {

    private static final long INTERVALO_HEALTH_MS = 1000;
    private static final long TIMEOUT_RESPUESTA_ENGINE_MS = 3000;
    private static final long GRACIA_ARRANQUE_MS = 5000;
    private static final long INTERVALO_PING_MS = 5000;
    /** Central responde ACK a cada PING (cada 5 s). Sin nada en 20 s, el socket está semiabierto. */
    private static final int TIMEOUT_LECTURA_CENTRAL_MS = 20000;
    /** Si tras autorizar no llega telemetría en este tiempo, se envía INICIO_RIEGO por socket. */
    private static final long ESPERA_KAFKA_MS = 6000;

    private final int puertoLocalEngine;
    private final String ipCentral;
    private final int puertoCentral;
    private final String idWS;
    private final String ubicacion;
    private final long arranqueMs = System.currentTimeMillis();

    // Conexión con Central
    private volatile Socket socketCentral;
    private DataOutputStream salidaCentral;
    private DataInputStream entradaCentral;
    private final Object lockCentral = new Object();
    private volatile boolean registrado = false;
    private volatile long ultimoEnvioCentralMs = 0;

    // Conexión con el Engine
    private volatile Socket socketEngine;
    private DataOutputStream salidaEngine;
    private final Object lockEngine = new Object();
    private volatile long ultimaRespuestaEngineMs = 0;
    private volatile String ultimoEstadoEngine = null;
    private volatile boolean engineConectadoAlgunaVez = false;

    // Estado de salud y riego (protegido por lockRiego)
    private volatile boolean enEjecucion = true;
    private volatile boolean averiaActiva = false;
    private final Object lockRiego = new Object();
    private boolean enRiego = false;
    private String riegoId = null;
    private String operarioRiego = null;
    private int duracionS = 0;
    private long autorizadoMs = 0;
    private long inicioTelemetriaMs = 0;
    private String ultimoVolumen = "0.00";
    private boolean respaldoEnviado = false;
    private boolean finSolicitado = false;
    /** FIN construido por el Monitor cuando el Engine cae regando; se envía al resolverse la avería. */
    private String[] finRetenido = null; // {riegoId, trama}
    /** Riegos que el Engine abrió sin autorización del Monitor (se ignoran sus FIN). */
    private final Set<String> riegosDescartados = new HashSet<>();

    /** FIN_RIEGO enviados (o por enviar) a Central y aún no confirmados: riegoId -> trama. */
    private final Map<String, String> finesSinConfirmar = new LinkedHashMap<>();

    public WM_WS_M(int puertoLocalEngine, String ipCentral, int puertoCentral, String idWS, String ubicacion) {
        this.puertoLocalEngine = puertoLocalEngine;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.idWS = idWS;
        this.ubicacion = ubicacion.replace("#", " ");
    }

    public void iniciar() {
        System.out.println("=== ARRANCANDO MONITOR (WM_WS_M) - WS ID: " + idWS + " | Ubicación: "
                + (ubicacion.isEmpty() ? "(la registrada en CENTRAL)" : ubicacion) + " ===");

        // Estrategia 4: ServerSocket del Engine siempre a la escucha
        new Thread(this::bucleAceptarEngine, "Hilo-ServerSocket-Engine").start();

        // Estrategia 1: conexión y reconexión con backoff a Central
        new Thread(this::bucleConexionCentral, "Hilo-Conexion-Central").start();

        // Estrategia 2: latido hacia Central cuando no hay tráfico
        new Thread(this::buclePingCentral, "Hilo-Ping-Central").start();

        bucleSupervisionSalud();
    }

    // ================================================================ CENTRAL

    // Estrategia 1: reconexión con backoff exponencial (2, 4, 8 ... 30 s)
    private void bucleConexionCentral() {
        int esperaMs = 2000;
        final int maxEsperaMs = 30000;
        boolean avisadoCaida = false;

        while (enEjecucion) {
            try {
                Socket s = new Socket(ipCentral, puertoCentral);
                s.setSoTimeout(TIMEOUT_LECTURA_CENTRAL_MS);
                synchronized (lockCentral) {
                    salidaCentral = new DataOutputStream(s.getOutputStream());
                    entradaCentral = new DataInputStream(s.getInputStream());
                    socketCentral = s;
                }

                enviarACentral(tramaRegistro());
                String respuesta = entradaCentral.readUTF();

                if (Protocolo.esAck(respuesta)) {
                    registrado = true;
                    esperaMs = 2000;
                    avisadoCaida = false;
                    System.out.println("[MONITOR] Conectado y autenticado en WM_Central como [" + idWS + "].");
                    enviarAEngine("AVISO#Conexión con WM_Central establecida");
                    sincronizarConCentral();
                    iniciarEscuchaCentral();
                } else {
                    String motivo = respuesta.length() > 1 ? respuesta.substring(1) : "NACK";
                    System.err.println("[MONITOR] Autenticación DENEGADA por WM_Central: " + motivo
                            + ("WS_NO_REGISTRADA".equals(motivo)
                                ? " (la estación " + idWS + " no está dada de alta en CENTRAL)" : ""));
                }
            } catch (IOException e) {
                System.err.println("[MONITOR] Imposible conectar con WM_Central (" + ipCentral + ":" + puertoCentral
                        + "): " + e.getMessage());
            } finally {
                registrado = false;
                cerrarSocketCentral();
            }

            if (!avisadoCaida) {
                avisadoCaida = true;
                boolean regando;
                synchronized (lockRiego) { regando = enRiego; }
                String msg = "Imposible conectar con WM_Central. No se admiten nuevas solicitudes de riego"
                        + (regando ? "; el riego en curso continúa y su resultado se enviará al reconectar." : ".");
                System.err.println("[MONITOR] " + msg);
                enviarAEngine("AVISO#" + msg);
            }

            System.out.println("[MONITOR] Reintentando conexión con WM_Central en " + (esperaMs / 1000) + " s...");
            dormir(esperaMs);
            esperaMs = Math.min(esperaMs * 2, maxEsperaMs);
        }
    }

    /** REGISTRO#id#ubicacion, más el riego en curso si lo hay (para que Central lo recupere). */
    private String tramaRegistro() {
        synchronized (lockRiego) {
            if (enRiego) {
                return "REGISTRO#" + idWS + "#" + ubicacion + "#" + riegoId + "#" + operarioRiego + "#" + duracionS;
            }
        }
        return "REGISTRO#" + idWS + "#" + ubicacion;
    }

    /** Tras registrarse: estado de avería y todos los FIN_RIEGO no confirmados. */
    private void sincronizarConCentral() {
        if (averiaActiva) enviarACentralSeguro("AVERIA#" + idWS);
        List<String> fines;
        synchronized (finesSinConfirmar) {
            fines = new ArrayList<>(finesSinConfirmar.values());
        }
        if (!fines.isEmpty()) {
            System.out.println("[MONITOR] Reenviando " + fines.size() + " fin(es) de riego pendiente(s) a WM_Central.");
        }
        for (String f : fines) enviarACentralSeguro(f);
        enviarACentralSeguro("SINCRONIZADO#" + idWS);
    }

    private void iniciarEscuchaCentral() throws IOException {
        while (enEjecucion) {
            String mensaje = entradaCentral.readUTF();
            if (Protocolo.esAck(mensaje)) continue;          // Respuesta de Central a una trama nuestra
            if (Protocolo.esNack(mensaje)) {
                System.err.println("[MONITOR] WM_Central ha rechazado una trama (NACK"
                        + (mensaje.length() > 1 ? ": " + mensaje.substring(1) : "") + ").");
                continue;
            }
            String datos = Protocolo.desempaquetar(mensaje);
            if (datos == null) {
                System.err.println("[MONITOR] Trama de WM_Central mal formada o con LRC incorrecto. Respondiendo NACK.");
                enviarRawCentral(Protocolo.S_NACK);
                continue;
            }
            enviarRawCentral(Protocolo.S_ACK);

            String[] partes = datos.split("#", -1);
            switch (partes[0]) {
                case "SOLICITUD_RIEGO": // SOLICITUD_RIEGO#riegoId#operario#duracion
                    if (partes.length >= 4) atenderSolicitudRiego(partes[1], partes[2], partes[3]);
                    break;
                case "ABORTAR_RIEGO":
                    abortarRiegoLocal(partes.length > 1 ? partes[1] : "Orden de CENTRAL");
                    break;
                case "CONFIRMACION_FIN":
                    if (partes.length > 1) {
                        synchronized (finesSinConfirmar) { finesSinConfirmar.remove(partes[1]); }
                    }
                    break;
                case "AVISO":
                    String aviso = partes.length > 1 ? partes[1] : "";
                    System.out.println("[MONITOR] [AVISO CENTRAL] " + aviso);
                    enviarAEngine("AVISO#" + aviso);
                    break;
                default:
                    break;
            }
        }
    }

    private void atenderSolicitudRiego(String id, String operario, String duracion) {
        String motivo = null;
        synchronized (lockRiego) {
            if (averiaActiva) motivo = "La estación tiene una avería activa";
            else if (!engineConectado()) motivo = "El Engine de la estación no está conectado";
            else if (enRiego) motivo = "La estación ya está regando";

            if (motivo == null) {
                enRiego = true;
                riegoId = id;
                operarioRiego = operario;
                try { duracionS = Integer.parseInt(duracion); } catch (NumberFormatException e) { duracionS = 15; }
                autorizadoMs = System.currentTimeMillis();
                inicioTelemetriaMs = 0;
                ultimoVolumen = "0.00";
                respaldoEnviado = false;
                finSolicitado = false;
            }
        }

        if (motivo == null) {
            System.out.println("\n[MONITOR] Solicitud de riego " + id + " del operario [" + operario + "] ("
                    + duracionS + " s): AUTORIZADA.");
            enviarACentralSeguro("RIEGO_AUTORIZADO#" + id);
        } else {
            System.err.println("[MONITOR] Solicitud de riego " + id + " del operario [" + operario + "] DENEGADA: " + motivo);
            enviarACentralSeguro("RIEGO_DENEGADO#" + id + "#" + motivo);
        }
    }

    /** Envía DATA a Central empaquetado en trama <STX>DATA<ETX><LRC>. */
    private void enviarACentral(String datos) throws IOException {
        enviarRawCentral(Protocolo.empaquetar(datos));
    }

    /** Escribe tal cual en el socket de Central (tramas ya empaquetadas o ACK/NACK). */
    private void enviarRawCentral(String contenido) throws IOException {
        synchronized (lockCentral) {
            if (salidaCentral == null || socketCentral == null || socketCentral.isClosed()) {
                throw new IOException("Sin conexión con WM_Central");
            }
            salidaCentral.writeUTF(contenido);
            salidaCentral.flush();
            ultimoEnvioCentralMs = System.currentTimeMillis();
        }
    }

    /** Envía a Central si hay conexión. Devuelve false si no fue posible. */
    private boolean enviarACentralSeguro(String datos) {
        if (!registrado && !datos.startsWith("REGISTRO")) return false;
        try {
            enviarACentral(datos);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void cerrarSocketCentral() {
        synchronized (lockCentral) {
            try {
                if (socketCentral != null) socketCentral.close();
            } catch (IOException ignored) {}
            socketCentral = null;
        }
    }

    // Estrategia 2: ping a Central solo cuando no hay tráfico (evita sockets semiabiertos)
    private void buclePingCentral() {
        while (enEjecucion) {
            dormir(1000);
            if (registrado && System.currentTimeMillis() - ultimoEnvioCentralMs >= INTERVALO_PING_MS) {
                enviarACentralSeguro("PING#" + idWS);
            }
        }
    }

    /** Guarda el FIN hasta que Central lo confirme y lo envía si hay conexión. */
    private void registrarFinRiego(String id, String volumen, String duracion, String motivo) {
        String trama = "FIN_RIEGO#" + idWS + "#" + volumen + "#" + duracion + "#" + id + "#" + motivo;
        synchronized (finesSinConfirmar) { finesSinConfirmar.put(id, trama); }
        if (!enviarACentralSeguro(trama)) {
            System.err.println("[MONITOR] Imposible conectar con WM_Central: el fin del riego " + id
                    + " se enviará al reconectar.");
        }
    }

    // ================================================================ ENGINE

    // Estrategia 4: el ServerSocket nunca se cierra; acepta reconexiones del Engine
    private void bucleAceptarEngine() {
        try (ServerSocket serverSocket = new ServerSocket(puertoLocalEngine)) {
            System.out.println("[MONITOR] ServerSocket local abierto en puerto " + puertoLocalEngine + " (Esperando a WM_WS_E)...");
            while (enEjecucion) {
                try {
                    Socket nuevo = serverSocket.accept();
                    synchronized (lockEngine) {
                        if (socketEngine != null) {
                            try { socketEngine.close(); } catch (IOException ignored) {}
                        }
                        salidaEngine = new DataOutputStream(nuevo.getOutputStream());
                        socketEngine = nuevo;
                        ultimaRespuestaEngineMs = System.currentTimeMillis();
                        ultimoEstadoEngine = null;
                        engineConectadoAlgunaVez = true;
                    }
                    System.out.println("[MONITOR] Engine (WM_WS_E) conectado/reconectado. Reanudando HEALTH_CHECK.");
                    if (!registrado) enviarAEngine("AVISO#Imposible conectar con WM_Central. No se admiten nuevas solicitudes de riego.");
                    new Thread(() -> bucleLecturaEngine(nuevo), "Hilo-Lectura-Engine").start();
                } catch (IOException e) {
                    if (enEjecucion) System.err.println("[MONITOR] Error aceptando conexión de Engine: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("[MONITOR] Error fatal en ServerSocket local (puerto " + puertoLocalEngine + "): " + e.getMessage());
            System.exit(1);
        }
    }

    /** Lee todo lo que envía el Engine: respuestas OK/KO, telemetría, fin de riego y peticiones manuales. */
    private void bucleLecturaEngine(Socket s) {
        try {
            DataInputStream entrada = new DataInputStream(s.getInputStream());
            while (enEjecucion) {
                String trama = entrada.readUTF();
                if (Protocolo.esAck(trama)) continue;
                if (Protocolo.esNack(trama)) {
                    System.err.println("[MONITOR] El Engine ha rechazado una trama (NACK).");
                    continue;
                }
                String msg = Protocolo.desempaquetar(trama);
                if (msg == null) {
                    System.err.println("[MONITOR] Trama del Engine mal formada o con LRC incorrecto. Respondiendo NACK.");
                    enviarRawEngine(Protocolo.S_NACK);
                    continue;
                }
                enviarRawEngine(Protocolo.S_ACK);
                String[] p = msg.split("#", -1);
                switch (p[0]) {
                    case "OK":
                    case "KO":
                        ultimoEstadoEngine = p[0];
                        ultimaRespuestaEngineMs = System.currentTimeMillis();
                        break;
                    case "TELEMETRIA": // TELEMETRIA#riegoId#caudal#volumen
                        if (p.length >= 4) procesarTelemetria(p[1], p[2], p[3]);
                        break;
                    case "FIN_RIEGO": // FIN_RIEGO#volumen#duracion#riegoId#motivo
                        if (p.length >= 5) procesarFinRiego(p[3], p[1], p[2], p[4]);
                        break;
                    case "SOLICITUD_MANUAL": // SOLICITUD_MANUAL#duracion
                        if (!registrado) {
                            System.err.println("[MONITOR] Solicitud manual rechazada: imposible conectar con WM_Central.");
                            enviarAEngine("AVISO#Imposible conectar con WM_Central: no se puede solicitar el riego");
                        } else {
                            System.out.println("[MONITOR] Solicitud de riego manual desde la estación. Reenviando a WM_Central...");
                            enviarACentralSeguro("SOLICITUD_MANUAL#" + idWS + "#" + (p.length > 1 ? p[1] : "0"));
                        }
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("[MONITOR] Conexión con el Engine perdida: " + e.getMessage());
        } finally {
            synchronized (lockEngine) {
                if (socketEngine == s) {
                    socketEngine = null;
                    salidaEngine = null;
                }
            }
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    private void procesarTelemetria(String id, String caudal, String volumen) {
        boolean descartar = false;
        synchronized (lockRiego) {
            if (!enRiego || !id.equals(riegoId)) {
                // El Engine riega sin autorización (ej. evento de Kafka antiguo): se le ordena parar
                descartar = riegosDescartados.add(id);
                if (!descartar) return;
            } else {
                if (inicioTelemetriaMs == 0) inicioTelemetriaMs = System.currentTimeMillis();
                ultimoVolumen = volumen;
            }
        }
        if (descartar) {
            System.err.println("[MONITOR] El Engine riega (" + id + ") sin autorización. Ordenando cierre.");
            enviarAEngine("ABORTAR_RIEGO#Riego no autorizado por el Monitor");
            return;
        }
        System.out.println("[MONITOR] Riego en curso | Caudal: " + caudal + " L/min | Volumen: " + volumen + " L");
        enviarACentralSeguro("TELEMETRIA#" + idWS + "#" + caudal + "#" + volumen);
    }

    private void procesarFinRiego(String id, String volumen, String duracion, String motivo) {
        synchronized (lockRiego) {
            if (riegosDescartados.contains(id)) return;
            if (enRiego && id.equals(riegoId)) enRiego = false;
            if (finRetenido != null && finRetenido[0].equals(id)) finRetenido = null; // el Engine trae el dato exacto
        }
        System.out.println("\n[MONITOR] Riego " + id + " finalizado (" + motivo + ") | Volumen total: " + volumen
                + " L | Duración: " + duracion + " s");
        registrarFinRiego(id, volumen, duracion, motivo);
    }

    private boolean engineConectado() {
        Socket s = socketEngine;
        return s != null && !s.isClosed();
    }

    /** Envía DATA al Engine empaquetado en trama <STX>DATA<ETX><LRC>. */
    private void enviarAEngine(String datos) {
        enviarRawEngine(Protocolo.empaquetar(datos));
    }

    /** Escribe tal cual en el socket del Engine (tramas ya empaquetadas o ACK/NACK). */
    private void enviarRawEngine(String contenido) {
        synchronized (lockEngine) {
            if (salidaEngine == null) return;
            try {
                salidaEngine.writeUTF(contenido);
                salidaEngine.flush();
            } catch (IOException e) {
                System.err.println("[MONITOR] Error enviando al Engine: " + e.getMessage());
                try { socketEngine.close(); } catch (Exception ignored) {}
            }
        }
    }

    /** Cancela el riego local (si lo hay) y ordena al Engine cerrar la válvula. El Engine enviará el FIN. */
    private void abortarRiegoLocal(String motivo) {
        synchronized (lockRiego) {
            if (!enRiego) return;
            enRiego = false;
        }
        System.err.println("[MONITOR] RIEGO ABORTADO: " + motivo);
        enviarAEngine("ABORTAR_RIEGO#" + motivo);
    }

    // ================================================================ SUPERVISIÓN

    private void bucleSupervisionSalud() {
        while (enEjecucion) {
            dormir(INTERVALO_HEALTH_MS);
            long ahora = System.currentTimeMillis();

            // 1. HEALTH_CHECK al Engine y evaluación de la última respuesta
            String fallo = null;
            if (!engineConectado()) {
                if (engineConectadoAlgunaVez || ahora - arranqueMs > GRACIA_ARRANQUE_MS) fallo = "Engine no conectado";
            } else {
                enviarAEngine("HEALTH_CHECK");
                if (ahora - ultimaRespuestaEngineMs > TIMEOUT_RESPUESTA_ENGINE_MS) fallo = "Timeout: el Engine no responde";
                else if ("KO".equals(ultimoEstadoEngine)) fallo = "Respuesta KO recibida desde el Engine";
            }

            if (fallo != null) {
                reportarAveria(fallo);
            } else if (averiaActiva && "OK".equals(ultimoEstadoEngine)) {
                averiaActiva = false;
                System.out.println("[MONITOR] Engine recuperado (OK). Notificando a WM_Central que la avería está resuelta.");
                enviarACentralSeguro("RESUELTA#" + idWS);
                enviarFinRetenido();
            }

            // 2. Control del riego en curso (sigue aunque WM_Central no esté disponible)
            boolean enviarRespaldo = false;
            boolean finalizar = false;
            String idRespaldo = null;
            int durRespaldo = 0;
            synchronized (lockRiego) {
                if (enRiego) {
                    if (inicioTelemetriaMs == 0 && !respaldoEnviado && ahora - autorizadoMs > ESPERA_KAFKA_MS) {
                        respaldoEnviado = true;
                        enviarRespaldo = true;
                        idRespaldo = riegoId;
                        durRespaldo = duracionS;
                    }
                    if (inicioTelemetriaMs > 0 && !finSolicitado && ahora - inicioTelemetriaMs >= duracionS * 1000L) {
                        finSolicitado = true;
                        finalizar = true;
                    }
                }
            }
            if (enviarRespaldo) {
                System.out.println("[MONITOR] Kafka no ha entregado INICIO_RIEGO a tiempo. Enviando la orden por socket (respaldo).");
                enviarAEngine("INICIO_RIEGO#" + idRespaldo + "#" + durRespaldo);
            }
            if (finalizar) {
                System.out.println("[MONITOR] Tiempo de riego (" + duracionS + " s) alcanzado. Ordenando al Engine FINALIZAR_RIEGO.");
                enviarAEngine("FINALIZAR_RIEGO");
            }
        }
    }

    private void reportarAveria(String motivo) {
        if (averiaActiva) return;
        averiaActiva = true;
        System.err.println("\n[MONITOR] ¡ALERTA DE AVERÍA! (" + motivo + "). Notificando FUGA a WM_Central...");

        // Si se estaba regando, se conserva lo regado hasta ahora (última telemetría) para informar al recuperarse
        synchronized (lockRiego) {
            if (enRiego) {
                long segundos = inicioTelemetriaMs > 0 ? Math.round((System.currentTimeMillis() - inicioTelemetriaMs) / 1000.0) : 0;
                finRetenido = new String[] { riegoId, ultimoVolumen, String.valueOf(segundos) };
                System.err.println("[MONITOR] Riego " + riegoId + " interrumpido. Regado hasta la avería: " + ultimoVolumen
                        + " L. Se informará a WM_Central cuando se resuelva la avería.");
            }
        }
        abortarRiegoLocal("Avería detectada: " + motivo);
        if (!enviarACentralSeguro("AVERIA#" + idWS)) {
            System.err.println("[MONITOR] Imposible conectar con WM_Central: la avería se notificará al reconectar.");
        }
    }

    /** Al resolverse la avería: estado final del riego interrumpido (si el Engine no lo envió él mismo). */
    private void enviarFinRetenido() {
        String[] fin;
        synchronized (lockRiego) {
            fin = finRetenido;
            finRetenido = null;
        }
        if (fin == null) return;
        System.out.println("[MONITOR] Enviando a WM_Central el estado final del riego interrumpido " + fin[0]
                + " (" + fin[1] + " L, " + fin[2] + " s).");
        registrarFinRiego(fin[0], fin[1], fin[2], "AVERIA");
    }

    private void dormir(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Uso: java WM_WS_M <puerto_para_Engine> <IP_Central> <puerto_Central> <id_WS> [ubicacion]");
            System.exit(1);
        }

        int puertoLocal = Integer.parseInt(args[0]);
        String ipCentral = args[1];
        int puertoCentral = Integer.parseInt(args[2]);
        String idWS = args[3];
        // Sin ubicación: CENTRAL usa la que tiene registrada para esta estación
        String ubicacion = args.length >= 5 ? args[4].trim() : "";

        Locale.setDefault(Locale.US);
        new WM_WS_M(puertoLocal, ipCentral, puertoCentral, idWS, ubicacion).iniciar();
    }
}
