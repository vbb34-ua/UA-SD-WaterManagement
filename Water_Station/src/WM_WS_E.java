import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * WM_WS_E - Engine de la estación de riego.
 * Simula el caudalímetro, el detector de fugas y la electroválvula.
 *
 *  - Socket con su Monitor (WM_WS_M): responde HEALTH_CHECK con OK/KO y le envía
 *    la telemetría (cada segundo) y el fin de riego.
 *  - Kafka (topic wm-eventos): recibe INICIO_RIEGO y ABORTO_RIEGO de su estación.
 *
 * Mensajes con el Monitor en tramas <STX>DATA<ETX><LRC> con respuesta ACK/NACK (clase Protocolo).
 *
 * Uso: java WM_WS_E <ip_broker:puerto> <ip_Monitor> <puerto_Monitor> [id_WS]
 */
public class WM_WS_E {

    private static final String TOPIC_EVENTOS = "wm-eventos";
    /** Eventos de Kafka más antiguos que esto se ignoran (evita reabrir la válvula al reiniciar). */
    private static final long ANTIGUEDAD_MAX_EVENTO_MS = 30000;
    /** Estrategia 3: si el Monitor no envía nada en este tiempo, se da el enlace por perdido. */
    private static final int TIMEOUT_MONITOR_MS = 5000;

    private final String brokerAddress;
    private final String ipMonitor;
    private final int puertoMonitor;
    private final String idWS;

    private volatile Socket socketMonitor;
    private DataOutputStream salidaMonitor;
    private DataInputStream entradaMonitor;
    private final Object lockEnvio = new Object();

    private volatile boolean valvulaAbierta = false;
    private volatile boolean estadoSaludCorrecto = true;
    private volatile boolean enEjecucion = true;

    // Datos del riego en curso
    private volatile String riegoActual = null;
    private volatile String ultimoRiegoAtendido = null;
    private volatile long inicioRiegoMs = 0;
    private volatile double volumenAcumulado = 0;
    private volatile double caudalActual = 0;
    private final Random random = new Random();
    /** FIN_RIEGO que no se pudieron entregar al Monitor (se envían al reconectar). */
    private final List<String> finesPendientes = new ArrayList<>();

    public WM_WS_E(String brokerAddress, String ipMonitor, int puertoMonitor, String idWS) {
        this.brokerAddress = brokerAddress;
        this.ipMonitor = ipMonitor;
        this.puertoMonitor = puertoMonitor;
        this.idWS = idWS;
    }

    public void iniciar() {
        System.out.println("=== ARRANCANDO ENGINE (WM_WS_E) - WS ID: " + idWS + " ===");
        System.out.println("[ENGINE] Broker asignado: " + brokerAddress);

        // Estrategia 1: conexión con reintentos al Monitor local
        new Thread(this::gestorConexionMonitor, "Hilo-Conexion-Monitor").start();

        // Estrategia 5: consumidor real de Kafka
        new Thread(this::hiloConsumidorBroker, "Hilo-Kafka-Consumidor").start();

        // Caudalímetro: envía telemetría cada segundo mientras la válvula está abierta
        new Thread(this::hiloCaudalimetro, "Hilo-Caudalimetro").start();

        hiloConsolaSimulacion();
    }

    // ---------------------------------------------------------------- MONITOR

    // Estrategia 1: reintentos continuos al conectar con el Monitor
    private void gestorConexionMonitor() {
        while (enEjecucion) {
            if (conectarConMonitor()) {
                bucleAtencionMonitor();
            }
            dormir(2000);
        }
    }

    private boolean conectarConMonitor() {
        try {
            System.out.println("[ENGINE] Conectando a Monitor local (" + ipMonitor + ":" + puertoMonitor + ")...");
            Socket s = new Socket(ipMonitor, puertoMonitor);
            s.setSoTimeout(TIMEOUT_MONITOR_MS);
            synchronized (lockEnvio) {
                salidaMonitor = new DataOutputStream(s.getOutputStream());
                entradaMonitor = new DataInputStream(s.getInputStream());
                socketMonitor = s;
            }
            System.out.println("[ENGINE] Conectado exitosamente al Monitor. Estación operativa.");
            enviarFinesPendientes();
            return true;
        } catch (IOException e) {
            System.err.println("[ENGINE] ESTACIÓN AVERIADA: no se puede conectar con el Monitor (" + e.getMessage()
                    + "). No se admiten riegos. Reintentando en 2 s...");
            return false;
        }
    }

    /** Entrega al Monitor los fines de riego que quedaron pendientes por pérdida de enlace. */
    private void enviarFinesPendientes() {
        List<String> copia;
        synchronized (finesPendientes) {
            copia = new ArrayList<>(finesPendientes);
            finesPendientes.clear();
        }
        for (String fin : copia) {
            System.out.println("[ENGINE] Enviando al Monitor el estado final pendiente: " + fin);
            if (!enviarAMonitor(fin)) {
                synchronized (finesPendientes) { finesPendientes.add(fin); }
            }
        }
    }

    private void bucleAtencionMonitor() {
        try {
            while (enEjecucion) {
                String trama = entradaMonitor.readUTF();
                if (Protocolo.esAck(trama)) continue;
                if (Protocolo.esNack(trama)) {
                    System.err.println("[ENGINE] El Monitor ha rechazado una trama (NACK).");
                    continue;
                }
                String mensaje = Protocolo.desempaquetar(trama);
                if (mensaje == null) {
                    System.err.println("[ENGINE] Trama del Monitor mal formada o con LRC incorrecto. Respondiendo NACK.");
                    enviarRawMonitor(Protocolo.S_NACK);
                    continue;
                }
                enviarRawMonitor(Protocolo.S_ACK);
                String[] partes = mensaje.split("#", -1);

                switch (partes[0]) {
                    case "HEALTH_CHECK":
                        enviarAMonitor(estadoSaludCorrecto ? "OK" : "KO");
                        break;
                    case "INICIO_RIEGO": // Canal de respaldo si Kafka no entrega el evento
                        abrirValvula(partes.length > 1 ? partes[1] : null, "Monitor (respaldo por socket)");
                        break;
                    case "ABORTAR_RIEGO":
                        cerrarValvula("ABORTADO: " + (partes.length > 1 ? partes[1] : "orden del Monitor"), "ABORTADO");
                        break;
                    case "FINALIZAR_RIEGO":
                        cerrarValvula("Tiempo de riego alcanzado", "TIEMPO");
                        break;
                    case "AVISO":
                        System.out.println("\n>>> [AVISO CENTRAL] " + (partes.length > 1 ? partes[1] : ""));
                        break;
                    default:
                        break;
                }
            }
        } catch (IOException e) {
            System.err.println("\n[ENGINE] ¡ALERTA! Pérdida de enlace con el Monitor local: " + e.getMessage());
        } finally {
            synchronized (lockEnvio) {
                try {
                    if (socketMonitor != null) socketMonitor.close();
                } catch (IOException ignored) {}
                socketMonitor = null;
            }
            // Estrategia 3: válvula a prueba de fallos (cierre inmediato por seguridad).
            // Lo regado se guarda y se enviará al Monitor cuando vuelva.
            if (valvulaAbierta) {
                cerrarValvula("FAIL-SAFE: cierre de emergencia por pérdida de comunicación con el Monitor", "ENLACE");
            }
            if (enEjecucion) {
                System.err.println("==============================================");
                System.err.println(">>> [ENGINE] ESTACIÓN AVERIADA: Monitor no disponible.");
                System.err.println("    No se admiten riegos (ni manuales) hasta que el Monitor vuelva.");
                System.err.println("==============================================");
            }
        }
    }

    private boolean monitorConectado() {
        Socket s = socketMonitor;
        return s != null && !s.isClosed();
    }

    /** Envía DATA al Monitor empaquetado en trama <STX>DATA<ETX><LRC>. */
    private boolean enviarAMonitor(String datos) {
        return enviarRawMonitor(Protocolo.empaquetar(datos));
    }

    /** Escribe tal cual en el socket del Monitor (tramas ya empaquetadas o ACK/NACK). */
    private boolean enviarRawMonitor(String contenido) {
        synchronized (lockEnvio) {
            if (!monitorConectado()) return false;
            try {
                salidaMonitor.writeUTF(contenido);
                salidaMonitor.flush();
                return true;
            } catch (IOException e) {
                System.err.println("[ENGINE] Error enviando al Monitor: " + e.getMessage());
                return false;
            }
        }
    }

    // ---------------------------------------------------------------- ELECTROVÁLVULA

    private synchronized void abrirValvula(String riegoId, String origen) {
        if (valvulaAbierta) return;
        if (riegoId != null && riegoId.equals(ultimoRiegoAtendido)) return; // Evento duplicado
        if (!estadoSaludCorrecto) {
            System.err.println("[ENGINE] Riego " + riegoId + " ignorado: la estación tiene una avería (KO).");
            return;
        }
        if (!monitorConectado()) {
            System.err.println("[ENGINE] Riego " + riegoId + " ignorado: ESTACIÓN AVERIADA (sin enlace con el Monitor).");
            return;
        }
        riegoActual = riegoId;
        ultimoRiegoAtendido = riegoId;
        volumenAcumulado = 0;
        caudalActual = 0;
        inicioRiegoMs = System.currentTimeMillis();
        valvulaAbierta = true;
        System.out.println("\n==============================================");
        System.out.println(">>> [ENGINE] ELECTROVÁLVULA ABIERTA - Riego " + riegoId);
        System.out.println("    Orden recibida vía: " + origen);
        System.out.println("==============================================");
    }

    /**
     * Cierra la válvula e informa SIEMPRE del estado final del riego (volumen y duración) al Monitor,
     * con su código de motivo: TIEMPO, MANUAL, ABORTADO, AVERIA o ENLACE.
     * Si el Monitor no está disponible, el fin se guarda y se envía al reconectar: lo regado no se pierde.
     */
    private synchronized void cerrarValvula(String motivo, String codigoMotivo) {
        if (!valvulaAbierta) return;
        valvulaAbierta = false;
        long duracionS = Math.round((System.currentTimeMillis() - inicioRiegoMs) / 1000.0);
        String volumen = String.format(Locale.US, "%.2f", volumenAcumulado);
        caudalActual = 0;

        System.out.println("\n==============================================");
        System.out.println(">>> [ENGINE] ELECTROVÁLVULA CERRADA - " + motivo);
        System.out.println("    Volumen total: " + volumen + " L | Duración: " + duracionS + " s");
        System.out.println("==============================================");

        String fin = "FIN_RIEGO#" + volumen + "#" + duracionS + "#" + riegoActual + "#" + codigoMotivo;
        if (!enviarAMonitor(fin)) {
            synchronized (finesPendientes) { finesPendientes.add(fin); }
            System.err.println("[ENGINE] Monitor no disponible: el estado final del riego se enviará al reconectar.");
        }
        riegoActual = null;
    }

    /** Simula el caudalímetro: cada segundo mide el caudal y acumula el volumen. */
    private void hiloCaudalimetro() {
        while (enEjecucion) {
            dormir(1000);
            String telemetria = null;
            synchronized (this) {
                if (valvulaAbierta) {
                    caudalActual = 15 + random.nextDouble() * 10; // 15-25 L/min
                    volumenAcumulado += caudalActual / 60.0;      // litros en 1 segundo
                    telemetria = String.format(Locale.US, "TELEMETRIA#%s#%.2f#%.2f", riegoActual, caudalActual, volumenAcumulado);
                    long t = (System.currentTimeMillis() - inicioRiegoMs) / 1000;
                    System.out.printf(Locale.US, "[ENGINE] Regando %s | Caudal: %.2f L/min | Volumen: %.2f L | t=%d s%n",
                            idWS, caudalActual, volumenAcumulado, t);
                }
            }
            if (telemetria != null) enviarAMonitor(telemetria);
        }
    }

    // ---------------------------------------------------------------- KAFKA

    // Estrategia 5: desacoplamiento con Kafka (grupo persistente ws-engine-<idWS>)
    private void hiloConsumidorBroker() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "ws-engine-" + idWS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        while (enEjecucion) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC_EVENTOS));
                System.out.println("[ENGINE] Consumidor Kafka suscrito a '" + TOPIC_EVENTOS + "' (grupo ws-engine-" + idWS + ").");

                while (enEjecucion) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> record : records) {
                        procesarEventoKafka(record);
                    }
                }
            } catch (Exception e) {
                System.err.println("[ENGINE] Error en el consumidor Kafka: " + e.getMessage() + ". Reintentando en 5 s...");
                dormir(5000);
            }
        }
    }

    private void procesarEventoKafka(ConsumerRecord<String, String> record) {
        if (!idWS.equals(record.key()) || record.value() == null) return; // Solo eventos de esta estación
        if (System.currentTimeMillis() - record.timestamp() > ANTIGUEDAD_MAX_EVENTO_MS) return;

        String[] p = record.value().split("#", -1);
        switch (p[0]) {
            case "INICIO_RIEGO": // INICIO_RIEGO#ws#operario#duracion#riegoId
                abrirValvula(p.length > 4 ? p[4] : null, "Kafka (operario " + (p.length > 2 ? p[2] : "?") + ")");
                break;
            case "ABORTO_RIEGO": // ABORTO_RIEGO#ws#motivo
                cerrarValvula("ABORTADO (Kafka): " + (p.length > 2 ? p[2] : ""), "ABORTADO");
                break;
            default:
                break;
        }
    }

    // ---------------------------------------------------------------- MENÚ DE LA ESTACIÓN

    private void imprimirMenu() {
        System.out.println("\n----------- MENÚ ESTACIÓN " + idWS + " -----------");
        System.out.println(" R [segundos] -> Solicitar riego manual (ej. 'R 20')");
        System.out.println(" F            -> Finalizar el riego en curso");
        System.out.println(" K            -> Simular avería/fuga (el Monitor recibirá KO)");
        System.out.println(" O            -> Restablecer normalidad (OK)");
        System.out.println(" E            -> Ver estado de la estación");
        System.out.println(" H            -> Mostrar este menú");
        System.out.println("---------------------------------------------");
    }

    private void hiloConsolaSimulacion() {
        imprimirMenu();
        try (Scanner scanner = new Scanner(System.in)) {
            while (enEjecucion && scanner.hasNextLine()) {
                String[] cmd = scanner.nextLine().trim().toUpperCase().split("\\s+");
                switch (cmd[0]) {
                    case "K":
                        estadoSaludCorrecto = false;
                        System.err.println("\n>>> [SIMULACIÓN] Fuga detectada. Estado KO: el Monitor reportará FUGA.");
                        // Si estaba regando, el riego termina inmediatamente (se informa de lo regado)
                        cerrarValvula("Fuga detectada por el sensor", "AVERIA");
                        break;
                    case "O":
                        estadoSaludCorrecto = true;
                        System.out.println("\n>>> [SIMULACIÓN] Estado restablecido a OK.");
                        break;
                    case "R":
                        int seg = 0;
                        if (cmd.length > 1) {
                            try { seg = Integer.parseInt(cmd[1]); } catch (NumberFormatException ignored) {}
                        }
                        if (!monitorConectado()) {
                            System.err.println("[ENGINE] ESTACIÓN AVERIADA (Monitor no disponible): no se admiten riegos.");
                        } else if (!estadoSaludCorrecto) {
                            System.err.println("[ENGINE] ESTACIÓN AVERIADA (fuga activa): no se admiten riegos.");
                        } else {
                            enviarAMonitor("SOLICITUD_MANUAL#" + seg);
                            System.out.println("[ENGINE] Solicitud de riego manual enviada a CENTRAL"
                                    + (seg > 0 ? " (" + seg + " s)." : " (duración por defecto)."));
                        }
                        break;
                    case "F":
                        if (valvulaAbierta) cerrarValvula("Riego finalizado manualmente desde el menú", "MANUAL");
                        else System.out.println("[ENGINE] No hay ningún riego en curso.");
                        break;
                    case "E":
                        System.out.printf(Locale.US, "[ENGINE] Estación: %s | Salud: %s | Monitor: %s | Válvula: %s | Volumen: %.2f L%n",
                                (monitorConectado() && estadoSaludCorrecto) ? "OPERATIVA" : "AVERIADA",
                                estadoSaludCorrecto ? "OK" : "KO",
                                monitorConectado() ? "conectado" : "DESCONECTADO",
                                valvulaAbierta ? "ABIERTA (riego " + riegoActual + ")" : "CERRADA",
                                volumenAcumulado);
                        break;
                    case "H":
                        imprimirMenu();
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void dormir(int ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Uso: java WM_WS_E <ip_broker:puerto> <ip_Monitor> <puerto_Monitor> [id_WS]");
            System.exit(1);
        }

        String broker = args[0];
        String ipMonitor = args[1];
        int puertoMonitor = Integer.parseInt(args[2]);
        String idWS = (args.length >= 4) ? args[3] : "WS_01";

        new WM_WS_E(broker, ipMonitor, puertoMonitor, idWS).iniciar();
    }
}
