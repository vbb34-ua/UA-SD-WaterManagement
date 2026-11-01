import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

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
 * Uso: java WM_WS_M <puerto_para_Engine> <IP_Central> <puerto_Central> <id_WS> [ubicacion]
 */
public class WM_WS_M {

    private static final char STX = 0x02;
    private static final char ETX = 0x03;
    private static final char ACK = 0x06;

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

    // Estado de salud y riego
    private volatile boolean enEjecucion = true;
    private volatile boolean averiaActiva = false;
    private final Object lockRiego = new Object();
    private boolean enRiego = false;
    private String riegoId = null;
    private int duracionS = 0;
    private long autorizadoMs = 0;
    private long inicioTelemetriaMs = 0;
    private boolean respaldoEnviado = false;
    private boolean finSolicitado = false;

    public WM_WS_M(int puertoLocalEngine, String ipCentral, int puertoCentral, String idWS, String ubicacion) {
        this.puertoLocalEngine = puertoLocalEngine;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.idWS = idWS;
        this.ubicacion = ubicacion.replace("#", " ");
    }

    public void iniciar() {
        System.out.println("=== ARRANCANDO MONITOR (WM_WS_M) - WS ID: " + idWS + " | Ubicación: " + ubicacion + " ===");

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

        while (enEjecucion) {
            try {
                System.out.println("[MONITOR] Conectando a WM_Central (" + ipCentral + ":" + puertoCentral + ")...");
                Socket s = new Socket(ipCentral, puertoCentral);
                s.setSoTimeout(TIMEOUT_LECTURA_CENTRAL_MS);
                synchronized (lockCentral) {
                    salidaCentral = new DataOutputStream(s.getOutputStream());
                    entradaCentral = new DataInputStream(s.getInputStream());
                    socketCentral = s;
                }

                enviarACentral("REGISTRO#" + idWS + "#" + ubicacion);
                String respuesta = entradaCentral.readUTF();

                if (respuesta.equals(String.valueOf(ACK))) {
                    registrado = true;
                    esperaMs = 2000;
                    System.out.println("[MONITOR] Autenticado correctamente en WM_Central como [" + idWS + "].");
                    // Si había una avería sin resolver, Central debe saberlo tras la reconexión
                    if (averiaActiva) enviarACentralSeguro("AVERIA#" + idWS);
                    iniciarEscuchaCentral();
                } else {
                    System.err.println("[MONITOR] Registro rechazado por Central: " + respuesta.substring(1));
                }
            } catch (IOException e) {
                System.err.println("[MONITOR] Error/Pérdida de enlace con WM_Central: " + e.getMessage());
            } finally {
                registrado = false;
                cerrarSocketCentral();
            }

            // Fail-safe: sin Central no hay supervisión, el riego local se aborta
            abortarRiegoLocal("Pérdida de conexión con CENTRAL");

            System.out.println("[MONITOR] Reintentando conexión con Central en " + (esperaMs / 1000) + " s...");
            dormir(esperaMs);
            esperaMs = Math.min(esperaMs * 2, maxEsperaMs);
        }
    }

    private void iniciarEscuchaCentral() throws IOException {
        while (enEjecucion) {
            String mensaje = entradaCentral.readUTF();
            String datos = extraerDatos(mensaje);
            if (datos == null) continue; // ACK / NACK sueltos

            String[] partes = datos.split("#", -1);
            switch (partes[0]) {
                case "SOLICITUD_RIEGO": // SOLICITUD_RIEGO#riegoId#operario#duracion
                    if (partes.length >= 4) atenderSolicitudRiego(partes[1], partes[2], partes[3]);
                    break;
                case "ABORTAR_RIEGO":
                    abortarRiegoLocal(partes.length > 1 ? partes[1] : "Orden de CENTRAL");
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
                try { duracionS = Integer.parseInt(duracion); } catch (NumberFormatException e) { duracionS = 15; }
                autorizadoMs = System.currentTimeMillis();
                inicioTelemetriaMs = 0;
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

    private String extraerDatos(String trama) {
        if (trama == null || trama.length() < 3 || trama.charAt(0) != STX) return null;
        int etx = trama.indexOf(ETX);
        if (etx < 1 || etx != trama.length() - 2) return null;
        char lrc = 0;
        for (int i = 0; i <= etx; i++) lrc ^= trama.charAt(i);
        if (lrc != trama.charAt(trama.length() - 1)) {
            System.err.println("[MONITOR] Trama de Central con LRC incorrecto. Descartada.");
            return null;
        }
        return trama.substring(1, etx);
    }

    private void enviarACentral(String datos) throws IOException {
        synchronized (lockCentral) {
            if (salidaCentral == null || socketCentral == null || socketCentral.isClosed()) {
                throw new IOException("Sin conexión con Central");
            }
            String mensaje = STX + datos + ETX;
            char lrc = 0;
            for (int i = 0; i < mensaje.length(); i++) lrc ^= mensaje.charAt(i);
            salidaCentral.writeUTF(mensaje + lrc);
            salidaCentral.flush();
            ultimoEnvioCentralMs = System.currentTimeMillis();
        }
    }

    private void enviarACentralSeguro(String datos) {
        try {
            enviarACentral(datos);
        } catch (IOException e) {
            System.err.println("[MONITOR] No se pudo enviar '" + datos.split("#")[0] + "' a Central: " + e.getMessage());
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
                String msg = entrada.readUTF();
                String[] p = msg.split("#", -1);
                switch (p[0]) {
                    case "OK":
                    case "KO":
                        ultimoEstadoEngine = p[0];
                        ultimaRespuestaEngineMs = System.currentTimeMillis();
                        break;
                    case "TELEMETRIA": // TELEMETRIA#caudal#volumen
                        if (p.length >= 3) procesarTelemetria(p[1], p[2]);
                        break;
                    case "FIN_RIEGO": // FIN_RIEGO#volumen#duracion
                        if (p.length >= 3) procesarFinRiego(p[1], p[2]);
                        break;
                    case "SOLICITUD_MANUAL": // SOLICITUD_MANUAL#duracion
                        System.out.println("[MONITOR] Solicitud de riego manual desde la estación. Reenviando a Central...");
                        enviarACentralSeguro("SOLICITUD_MANUAL#" + idWS + "#" + (p.length > 1 ? p[1] : "0"));
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

    private void procesarTelemetria(String caudal, String volumen) {
        synchronized (lockRiego) {
            if (!enRiego) return;
            if (inicioTelemetriaMs == 0) inicioTelemetriaMs = System.currentTimeMillis();
        }
        System.out.println("[MONITOR] Riego en curso | Caudal: " + caudal + " L/min | Volumen: " + volumen + " L");
        enviarACentralSeguro("TELEMETRIA#" + idWS + "#" + caudal + "#" + volumen);
    }

    private void procesarFinRiego(String volumen, String duracion) {
        synchronized (lockRiego) {
            if (!enRiego) return;
            enRiego = false;
        }
        System.out.println("\n[MONITOR] Riego finalizado | Volumen total: " + volumen + " L | Duración: " + duracion + " s");
        enviarACentralSeguro("FIN_RIEGO#" + idWS + "#" + volumen + "#" + duracion);
    }

    private boolean engineConectado() {
        Socket s = socketEngine;
        return s != null && !s.isClosed();
    }

    private void enviarAEngine(String mensaje) {
        synchronized (lockEngine) {
            if (salidaEngine == null) return;
            try {
                salidaEngine.writeUTF(mensaje);
                salidaEngine.flush();
            } catch (IOException e) {
                System.err.println("[MONITOR] Error enviando al Engine: " + e.getMessage());
                try { socketEngine.close(); } catch (Exception ignored) {}
            }
        }
    }

    /** Cancela el riego local (si lo hay) y ordena al Engine cerrar la válvula. */
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
                System.out.println("[MONITOR] Engine recuperado (OK). Notificando a Central que la avería está resuelta.");
                enviarACentralSeguro("RESUELTA#" + idWS);
            }

            // 2. Control del riego en curso
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
        System.err.println("\n[MONITOR] ¡ALERTA DE AVERÍA! (" + motivo + "). Notificando FUGA a Central...");
        abortarRiegoLocal("Avería detectada: " + motivo);
        enviarACentralSeguro("AVERIA#" + idWS);
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
        String ubicacion = args.length >= 5 ? args[4] : "Sin ubicación";

        new WM_WS_M(puertoLocal, ipCentral, puertoCentral, idWS, ubicacion).iniciar();
    }
}
