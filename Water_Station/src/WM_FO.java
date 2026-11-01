import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * WM_FO - Aplicación del operario de campo.
 *
 *  - Socket con WM_Central: registro, solicitudes de riego (ACTIVAR) y notificaciones.
 *  - Kafka (topic wm-eventos): telemetría en tiempo real de la WS que le está regando.
 *
 * Fichero de servicios: una línea por servicio, "ID_WS" o "ID_WS,segundos" (ej. "WS_01,20").
 * Las líneas vacías o que empiezan por '#' se ignoran.
 *
 * Si se indica un fichero de servicios como último parámetro, se ejecuta automáticamente al arrancar
 * (sin interacción) y después se muestra el menú.
 * Mensajes con Central en tramas <STX>DATA<ETX><LRC> con respuesta ACK/NACK (clase Protocolo).
 *
 * Uso: java WM_FO <ip_broker:puerto> <id_operador> [ip_central] [puerto_central] [fichero_servicios]
 */
public class WM_FO {

    private static final String TOPIC_EVENTOS = "wm-eventos";
    private static final int DURACION_DEFECTO_S = 15;
    private static final long INTERVALO_PING_MS = 5000;
    /** Central responde ACK a cada PING. Sin nada en 20 s, el socket está semiabierto. */
    private static final int TIMEOUT_LECTURA_MS = 20000;

    private final String brokerAddress;
    private final String idOperador;
    private final String ipCentral;
    private final int puertoCentral;

    private volatile Socket socketCentral;
    private DataOutputStream salidaCentral;
    private DataInputStream entradaCentral;
    private final Object lockEnvio = new Object();
    private volatile long ultimoEnvioMs = 0;

    private volatile boolean enEjecucion = true;
    private volatile boolean registradoEnCentral = false;

    /** Estación que está prestando servicio a este operario (para filtrar la telemetría de Kafka). */
    private volatile String wsEnServicio = null;

    private boolean servicioConcluido = false;
    private String ultimoMensajeConclusion = "";
    private final Object lockServicio = new Object();

    /** Fichero de servicios a ejecutar automáticamente al arrancar (null = solo menú). */
    private final String ficheroAutomatico;

    public WM_FO(String brokerAddress, String idOperador, String ipCentral, int puertoCentral, String ficheroAutomatico) {
        this.brokerAddress = brokerAddress;
        this.idOperador = idOperador;
        this.ipCentral = ipCentral;
        this.puertoCentral = puertoCentral;
        this.ficheroAutomatico = ficheroAutomatico;
    }

    public void iniciar() {
        System.out.println("===============================================================");
        System.out.println("   WATER MANAGEMENT - FIELD OPERATOR (WM_FO)");
        System.out.println("   Operador ID: [" + idOperador + "] | Broker: [" + brokerAddress + "]");
        System.out.println("   Central: [" + ipCentral + ":" + puertoCentral + "]");
        System.out.println("===============================================================");

        // Estrategia 1: conexión y reconexión continua a la Central
        new Thread(this::gestorConexionCentral, "Hilo-Conexion-Central").start();

        // Estrategia 2: latido para que Central no cierre la sesión por inactividad
        new Thread(this::buclePing, "Hilo-Ping-Central").start();

        // Telemetría de la WS en servicio a través de Kafka
        new Thread(this::hiloConsumidorBroker, "Hilo-Kafka-Consumidor").start();

        // Modo automático: ejecuta el fichero de servicios sin interacción y después muestra el menú
        if (ficheroAutomatico != null) {
            System.out.println("[MODO AUTOMÁTICO] Esperando conexión con WM_Central para ejecutar '" + ficheroAutomatico + "'...");
            while (enEjecucion && !registradoEnCentral) dormir(500);
            procesarFicheroServicios(ficheroAutomatico);
        }

        menuPrincipal();
    }

    // ---------------------------------------------------------------- CONEXIÓN CON CENTRAL

    // Estrategia 1: reconexión con backoff (2, 4, 8 ... 30 s) y re-autenticación automática
    private void gestorConexionCentral() {
        int esperaMs = 2000;
        final int maxEsperaMs = 30000;

        while (enEjecucion) {
            if (conectarYRegistrarEnCentral()) {
                esperaMs = 2000;
                bucleEscuchaCentral(); // Hasta que se pierda la conexión
            }
            if (!enEjecucion) break;
            System.err.println("[OPERADOR] Reintentando conexión con WM_Central en " + (esperaMs / 1000) + " s...");
            dormir(esperaMs);
            esperaMs = Math.min(esperaMs * 2, maxEsperaMs);
        }
    }

    private boolean conectarYRegistrarEnCentral() {
        try {
            System.out.println("[OPERADOR] Conectando a WM_Central (" + ipCentral + ":" + puertoCentral + ")...");
            Socket s = new Socket(ipCentral, puertoCentral);
            s.setSoTimeout(TIMEOUT_LECTURA_MS);
            synchronized (lockEnvio) {
                salidaCentral = new DataOutputStream(s.getOutputStream());
                entradaCentral = new DataInputStream(s.getInputStream());
                socketCentral = s;
            }

            enviarTrama("REGISTRO_OPERARIO#" + idOperador);
            String respuesta = entradaCentral.readUTF();

            if (Protocolo.esAck(respuesta)) {
                registradoEnCentral = true;
                System.out.println("[OPERADOR] ¡Registro exitoso en WM_Central! (ACK recibido)");
                System.out.print("> ");
                return true;
            }

            String motivo = respuesta.length() > 1 ? respuesta.substring(1) : "NACK";
            cerrarConexion();
            if ("OPERARIO_NO_REGISTRADO".equals(motivo) || "ID_INVALIDO".equals(motivo)) {
                System.err.println("[OPERADOR] CENTRAL rechaza al operario [" + idOperador + "]: no está dado de alta. Saliendo.");
                System.exit(1);
            }
            System.err.println("[OPERADOR] Registro rechazado por Central (" + motivo + ").");
            return false;
        } catch (IOException e) {
            System.err.println("[OPERADOR] Imposible conectar con WM_Central (" + ipCentral + ":" + puertoCentral
                    + "): " + e.getMessage());
            cerrarConexion();
            return false;
        }
    }

    private void bucleEscuchaCentral() {
        try {
            while (enEjecucion) {
                String trama = entradaCentral.readUTF();
                if (Protocolo.esAck(trama)) continue;   // Respuesta de Central a una trama nuestra
                if (Protocolo.esNack(trama)) {
                    System.err.println("[OPERADOR] WM_Central ha rechazado una trama (NACK"
                            + (trama.length() > 1 ? ": " + trama.substring(1) : "") + ").");
                    continue;
                }
                String mensaje = Protocolo.desempaquetar(trama);
                if (mensaje == null) {
                    System.err.println("[OPERADOR] Trama de WM_Central mal formada o con LRC incorrecto. Respondiendo NACK.");
                    enviarRaw(Protocolo.S_NACK);
                    continue;
                }
                enviarRaw(Protocolo.S_ACK);
                if (mensaje.startsWith("SERVICIO_EN_CURSO:")) {
                    // Tras reconectar: Central indica que una WS sigue regando para este operario
                    wsEnServicio = mensaje.substring(mensaje.indexOf(':') + 1).trim();
                    System.out.println("\n>>> [CENTRAL] Su riego en la estación " + wsEnServicio
                            + " sigue en curso. Mostrando su telemetría.");
                    System.out.print("> ");
                    continue;
                }
                imprimirMensajeCentral(mensaje);

                if (esFinDeServicio(mensaje)) {
                    concluirServicio(mensaje);
                }
            }
        } catch (IOException e) {
            if (enEjecucion) {
                System.err.println("\n[OPERADOR] Imposible conectar con WM_Central: conexión perdida (" + e.getMessage() + ").");
                if (wsEnServicio != null) {
                    System.err.println("[OPERADOR] El riego en " + wsEnServicio
                            + " continúa en la estación. Recibirá el resultado al reconectar.");
                }
            }
        } finally {
            registradoEnCentral = false;
            cerrarConexion();
        }
    }

    private void concluirServicio(String mensaje) {
        wsEnServicio = null;
        synchronized (lockServicio) {
            servicioConcluido = true;
            ultimoMensajeConclusion = mensaje;
            lockServicio.notifyAll();
        }
    }

    private boolean esFinDeServicio(String msg) {
        return msg.startsWith("RESUMEN_FIN_RIEGO:") ||
                msg.startsWith("DENEGADO:") ||
                msg.startsWith("ERROR:") ||
                msg.startsWith("ABORTADO:");
    }

    private void imprimirMensajeCentral(String msg) {
        String hora = new SimpleDateFormat("HH:mm:ss").format(new Date());

        if (msg.startsWith("AUTORIZADO:") || msg.startsWith("REANUDADO:")) {
            System.out.println("\n>>> [" + hora + "] [CENTRAL - ÉXITO] " + msg);
        } else if (msg.startsWith("DENEGADO:") || msg.startsWith("ERROR:") || msg.startsWith("ABORTADO:")) {
            System.err.println("\n>>> [" + hora + "] [CENTRAL - AVISO/ERROR] " + msg);
        } else if (msg.startsWith("RESUMEN_FIN_RIEGO:")) {
            System.out.println("\n=======================================================");
            System.out.println(">>> [" + hora + "] [RESUMEN FINAL DE RIEGO]");
            System.out.println("    " + msg.replace("RESUMEN_FIN_RIEGO:", "").trim());
            System.out.println("=======================================================");
        } else if (msg.startsWith("LISTA_ESTACIONES:")) {
            System.out.println("\n--- ESTADO DE ESTACIONES EN CENTRAL ---" + msg.substring(msg.indexOf(':') + 1));
            System.out.println("---------------------------------------");
        } else {
            System.out.println("[" + hora + "] [CENTRAL] " + msg);
        }
        System.out.print("> ");
    }

    /** Envía DATA a Central empaquetado en trama <STX>DATA<ETX><LRC>. */
    private void enviarTrama(String datos) throws IOException {
        synchronized (lockEnvio) {
            if (salidaCentral == null || socketCentral == null || socketCentral.isClosed()) {
                throw new IOException("Socket no disponible");
            }
            salidaCentral.writeUTF(Protocolo.empaquetar(datos));
            salidaCentral.flush();
            ultimoEnvioMs = System.currentTimeMillis();
        }
    }

    /** Envía un ACK/NACK a Central. */
    private void enviarRaw(String contenido) {
        synchronized (lockEnvio) {
            try {
                if (salidaCentral != null && socketCentral != null && !socketCentral.isClosed()) {
                    salidaCentral.writeUTF(contenido);
                    salidaCentral.flush();
                }
            } catch (IOException ignored) {}
        }
    }

    // Estrategia 2: ping a Central cuando no hay tráfico
    private void buclePing() {
        while (enEjecucion) {
            dormir(1000);
            if (registradoEnCentral && System.currentTimeMillis() - ultimoEnvioMs >= INTERVALO_PING_MS) {
                try {
                    enviarTrama("PING#" + idOperador);
                } catch (IOException ignored) {}
            }
        }
    }

    // ---------------------------------------------------------------- KAFKA

    private void hiloConsumidorBroker() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "fo-" + idOperador);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        while (enEjecucion) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(Collections.singletonList(TOPIC_EVENTOS));
                System.out.println("[OPERADOR] Suscrito a Kafka '" + TOPIC_EVENTOS + "' para recibir la telemetría.");
                while (enEjecucion) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<String, String> r : records) {
                        procesarEventoKafka(r);
                    }
                }
            } catch (Exception e) {
                System.err.println("[OPERADOR] Error en el consumidor Kafka: " + e.getMessage() + ". Reintentando en 5 s...");
                dormir(5000);
            }
        }
    }

    private void procesarEventoKafka(ConsumerRecord<String, String> r) {
        if (r.value() == null || System.currentTimeMillis() - r.timestamp() > 10000) return;
        String ws = wsEnServicio;
        if (ws == null || !ws.equals(r.key())) return; // Solo la WS que presta servicio a este operario

        String[] p = r.value().split("#", -1);
        // TELEMETRIA#ws#operario#caudal#volumen
        if ("TELEMETRIA".equals(p[0]) && p.length >= 5 && idOperador.equals(p[2])) {
            System.out.println("[" + new SimpleDateFormat("HH:mm:ss").format(new Date()) + "] [KAFKA] " + ws
                    + " regando | Caudal: " + p[3] + " L/min | Volumen acumulado: " + p[4] + " L");
        }
    }

    // ---------------------------------------------------------------- SERVICIOS

    public boolean solicitarRiego(String idWs, int duracion) {
        try {
            System.out.println("\n[OPERADOR] Enviando solicitud de riego para WS [" + idWs + "] (" + duracion + " s)...");
            synchronized (lockServicio) {
                servicioConcluido = false;
                ultimoMensajeConclusion = "";
            }
            wsEnServicio = idWs;
            enviarTrama("ACTIVAR#" + idWs + "#" + idOperador + "#" + duracion);
            return true;
        } catch (IOException e) {
            wsEnServicio = null;
            System.err.println("[OPERADOR] Imposible conectar con WM_Central: la solicitud de riego no se ha enviado.");
            return false;
        }
    }

    public void procesarFicheroServicios(String rutaFichero) {
        File archivo = new File(rutaFichero);
        if (!archivo.exists() || !archivo.isFile()) {
            System.err.println("[OPERADOR] Error: El archivo '" + rutaFichero + "' no existe.");
            return;
        }

        System.out.println("\n[MODO AUTOMÁTICO] Cargando servicios desde: " + rutaFichero);
        List<String[]> servicios = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(archivo))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                linea = linea.trim();
                if (!linea.isEmpty() && !linea.startsWith("#")) {
                    servicios.add(linea.split("[,;\\s]+"));
                }
            }
        } catch (IOException e) {
            System.err.println("[OPERADOR] Error leyendo fichero: " + e.getMessage());
            return;
        }

        for (int i = 0; i < servicios.size() && enEjecucion; i++) {
            String idWs = servicios.get(i)[0].trim();
            int duracion = DURACION_DEFECTO_S;
            if (servicios.get(i).length > 1) {
                try { duracion = Integer.parseInt(servicios.get(i)[1].trim()); } catch (NumberFormatException ignored) {}
            }
            System.out.println("\n[SERVICIO " + (i + 1) + "/" + servicios.size() + "] Activando estación: " + idWs);

            String resultado;
            if (!solicitarRiego(idWs, duracion)) {
                resultado = "ERROR: sin conexión con CENTRAL";
            } else {
                resultado = esperarConclusion(duracion * 1000L + 60000L);
            }
            System.out.println("[MODO AUTOMÁTICO] Servicio en " + idWs + " concluido: " + resultado);

            if (i < servicios.size() - 1 && enEjecucion) {
                System.out.println("[MODO AUTOMÁTICO] Esperando 4 s antes del siguiente servicio...");
                dormir(4000);
            }
        }
        System.out.println("[MODO AUTOMÁTICO] Todos los servicios del fichero han sido procesados.");
    }

    private String esperarConclusion(long timeoutMs) {
        synchronized (lockServicio) {
            long inicio = System.currentTimeMillis();
            while (!servicioConcluido && enEjecucion) {
                long restante = timeoutMs - (System.currentTimeMillis() - inicio);
                if (restante <= 0) {
                    wsEnServicio = null;
                    return "TIMEOUT esperando la conclusión del servicio";
                }
                try {
                    lockServicio.wait(restante);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            return ultimoMensajeConclusion;
        }
    }

    public void consultarEstacionesDisponibles() {
        try {
            enviarTrama("LISTAR_WS#" + idOperador);
        } catch (IOException e) {
            System.err.println("[OPERADOR] Imposible conectar con WM_Central: no se pueden consultar las estaciones.");
        }
    }

    private void menuPrincipal() {
        try (Scanner scanner = new Scanner(System.in)) {
            while (enEjecucion) {
                System.out.println("\n-------------------------------------------");
                System.out.println("   PANEL DE OPERARIO - ID: " + idOperador
                        + (registradoEnCentral ? " (conectado)" : " (SIN CONEXIÓN)"));
                System.out.println("-------------------------------------------");
                System.out.println("[1] Solicitar activación puntual de riego (Manual)");
                System.out.println("[2] Cargar y ejecutar servicios desde archivo");
                System.out.println("[3] Consultar estaciones de riego (Estados)");
                System.out.println("[4] Salir");
                System.out.print("Seleccione una opción: ");

                if (!scanner.hasNextLine()) break;
                String opcion = scanner.nextLine().trim();

                switch (opcion) {
                    case "1":
                        System.out.print("ID de estación (ej. WS_01): ");
                        String idWs = scanner.nextLine().trim();
                        System.out.print("Duración en segundos [" + DURACION_DEFECTO_S + "]: ");
                        String dur = scanner.nextLine().trim();
                        int duracion = DURACION_DEFECTO_S;
                        try { if (!dur.isEmpty()) duracion = Integer.parseInt(dur); } catch (NumberFormatException ignored) {}
                        if (!idWs.isEmpty()) solicitarRiego(idWs, duracion);
                        break;

                    case "2":
                        System.out.print("Ruta de archivo: ");
                        String ruta = scanner.nextLine().trim();
                        if (!ruta.isEmpty()) procesarFicheroServicios(ruta);
                        break;

                    case "3":
                        consultarEstacionesDisponibles();
                        break;

                    case "4":
                        enEjecucion = false;
                        cerrarConexion();
                        System.exit(0);
                        break;

                    default:
                        System.out.println("Opción no válida.");
                        break;
                }
            }
        }
    }

    private void cerrarConexion() {
        synchronized (lockEnvio) {
            try {
                if (socketCentral != null) socketCentral.close();
            } catch (IOException ignored) {}
            socketCentral = null;
        }
    }

    private void dormir(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Uso: java WM_FO <ip_broker:puerto> <id_operador> [ip_central] [puerto_central] [fichero_servicios]");
            System.exit(1);
        }

        String broker = args[0];
        String idOperador = args[1];
        String ipCentral = "localhost";
        int puertoCentral = 6000;
        String fichero = null;

        if (args.length >= 4) {
            ipCentral = args[2];
            puertoCentral = Integer.parseInt(args[3]);
            if (args.length >= 5 && !args[4].trim().isEmpty()) fichero = args[4].trim();
        } else if (args.length == 3) {
            if (args[2].contains(":")) {
                String[] partes = args[2].split(":");
                ipCentral = partes[0];
                puertoCentral = Integer.parseInt(partes[1]);
            } else {
                ipCentral = args[2];
            }
        }

        new WM_FO(broker, idOperador, ipCentral, puertoCentral, fichero).iniciar();
    }
}
