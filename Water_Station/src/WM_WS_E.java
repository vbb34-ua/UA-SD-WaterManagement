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
 * WM_WS_E: Engine de la estación de riego. Simula la electroválvula, el caudalímetro
 * y el detector de fugas, y ofrece el menú de la estación.
 *
 * <ul>
 *   <li>Socket con su Monitor (WM_WS_M): responde HEALTH_CHECK con OK/KO y le envía
 *       la telemetría (cada segundo) y el fin de riego.</li>
 *   <li>Kafka (topic {@value #TOPIC_EVENTOS}): recibe INICIO_RIEGO y ABORTO_RIEGO de su estación.</li>
 * </ul>
 *
 * <p>Válvula a prueba de fallos: si se pierde el enlace con el Monitor, la válvula se cierra
 * al instante y la estación no admite riegos hasta que el Monitor vuelva.
 *
 * <p>Los mensajes con el Monitor usan tramas {@link Protocolo} con respuesta ACK/NACK.
 *
 * <p>Uso: {@code java WM_WS_E <ip_broker:puerto> <ip_Monitor> <puerto_Monitor> [id_WS]}
 */
public class WM_WS_E {

    private static final String TOPIC_EVENTOS = "wm-eventos";
    /** Eventos de Kafka más antiguos que esto se ignoran (evita reabrir la válvula al reiniciar). */
    private static final long ANTIGUEDAD_MAX_EVENTO_MS = 30000;
    /** El Monitor envía HEALTH_CHECK cada segundo: sin nada en este tiempo, el enlace se da por perdido. */
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
    /** false mientras hay una fuga simulada (tecla K): se responde KO al HEALTH_CHECK. */
    private volatile boolean estadoSaludCorrecto = true;
    private volatile boolean enEjecucion = true;

    // Riego en curso
    private volatile String riegoActual = null;
    /** Último riego abierto: si su INICIO_RIEGO llega dos veces (Kafka y socket), se ignora el repetido. */
    private volatile String ultimoRiegoAtendido = null;
    private volatile long inicioRiegoMs = 0;
    private volatile double volumenAcumulado = 0;
    private volatile double caudalActual = 0;
    private final Random random = new Random();
    /** FIN_RIEGO que no se pudieron entregar al Monitor (se envían al reconectar). */
    private final List<String> finesPendientes = new ArrayList<>();

    /**
     * @param brokerAddress dirección host:puerto de Kafka
     * @param idWS          id de la estación; debe coincidir con el de su Monitor
     */
    public WM_WS_E(String brokerAddress, String ipMonitor, int puertoMonitor, String idWS) {
        this.brokerAddress = brokerAddress;
        this.ipMonitor = ipMonitor;
        this.puertoMonitor = puertoMonitor;
        this.idWS = idWS;
    }

    /**
     * Arranca los hilos de conexión con el Monitor, consumidor de Kafka y caudalímetro, y
     * ejecuta el menú de la estación en el hilo actual.
     */
    public void iniciar() {
        System.out.println("=== ARRANCANDO ENGINE (WM_WS_E) - WS ID: " + idWS + " ===");
        System.out.println("[ENGINE] Broker asignado: " + brokerAddress);

        new Thread(this::gestorConexionMonitor, "Hilo-Conexion-Monitor").start();
        new Thread(this::hiloConsumidorBroker, "Hilo-Kafka-Consumidor").start();
        new Thread(this::hiloCaudalimetro, "Hilo-Caudalimetro").start();

        hiloConsolaSimulacion();
    }

    // ---------------------------------------------------------------- MONITOR

    /** Conecta con el Monitor y atiende sus mensajes; si el enlace se pierde, reintenta cada 2 s. */
    private void gestorConexionMonitor() {
        while (enEjecucion) {
            if (conectarConMonitor()) {
                bucleAtencionMonitor();
            }
            dormir(2000);
        }
    }

    /** @return true si se ha conectado (y entregado los fines pendientes) */
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

    /**
     * Atiende los mensajes del Monitor hasta perder el enlace (error o {@value #TIMEOUT_MONITOR_MS} ms
     * sin recibir nada). Al perderlo cierra la válvula y la estación queda averiada.
     */
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
            // Válvula a prueba de fallos: se cierra al instante; lo regado se enviará al Monitor cuando vuelva
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

    /**
     * Envía los datos al Monitor empaquetados en una trama.
     *
     * @return false si el Monitor no está conectado
     */
    private boolean enviarAMonitor(String datos) {
        return enviarRawMonitor(Protocolo.empaquetar(datos));
    }

    /** Escribe tal cual en el socket del Monitor (una trama ya empaquetada, o un ACK/NACK). */
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

    /**
     * Abre la válvula para el riego indicado, salvo que ya esté abierta, ese riego ya se haya
     * atendido, haya una fuga o no haya enlace con el Monitor.
     *
     * @param origen por dónde llegó la orden (Kafka o el respaldo por socket), solo para mostrarlo
     */
    private synchronized void abrirValvula(String riegoId, String origen) {
        if (valvulaAbierta) return;
        if (riegoId != null && riegoId.equals(ultimoRiegoAtendido)) return;
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
     * Cierra la válvula e informa siempre al Monitor del estado final del riego (FIN_RIEGO con
     * volumen y duración). Si el Monitor no está disponible, el fin se guarda y se envía al
     * reconectar, así que lo regado no se pierde. No hace nada si la válvula ya está cerrada.
     *
     * @param motivo       texto que se muestra en la consola
     * @param codigoMotivo TIEMPO, MANUAL, ABORTADO, AVERIA o ENLACE
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

    /**
     * Simula el caudalímetro: mientras la válvula está abierta, cada segundo genera un caudal
     * de 15 a 25 L/min, acumula el volumen y envía la telemetría al Monitor.
     */
    private void hiloCaudalimetro() {
        while (enEjecucion) {
            dormir(1000);
            String telemetria = null;
            synchronized (this) {
                if (valvulaAbierta) {
                    caudalActual = 15 + random.nextDouble() * 10;
                    volumenAcumulado += caudalActual / 60.0; // litros regados en 1 segundo
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

    /**
     * Consume {@value #TOPIC_EVENTOS} con un grupo propio de la estación ({@code ws-engine-<id>}):
     * cada Engine recibe todos los eventos y se queda con los de su WS. Si Kafka falla,
     * reintenta cada 5 s.
     */
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

    /** Atiende un evento de Kafka si es de esta estación y es reciente. */
    private void procesarEventoKafka(ConsumerRecord<String, String> record) {
        if (!idWS.equals(record.key()) || record.value() == null) return;
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

    /** Lee las órdenes del menú de la estación desde la consola. */
    private void hiloConsolaSimulacion() {
        imprimirMenu();
        try (Scanner scanner = new Scanner(System.in)) {
            while (enEjecucion && scanner.hasNextLine()) {
                String[] cmd = scanner.nextLine().trim().toUpperCase().split("\\s+");
                switch (cmd[0]) {
                    case "K":
                        estadoSaludCorrecto = false;
                        System.err.println("\n>>> [SIMULACIÓN] Fuga detectada. Estado KO: el Monitor reportará FUGA.");
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

    /** @param args {@code <ip_broker:puerto> <ip_Monitor> <puerto_Monitor> [id_WS]} (id por defecto: WS_01) */
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
