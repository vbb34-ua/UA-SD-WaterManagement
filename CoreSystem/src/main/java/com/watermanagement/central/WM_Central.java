package com.watermanagement.central;

import com.watermanagement.central.kafka.KafkaManager;
import com.watermanagement.central.red.WM_Central_Thread;
import com.watermanagement.central.panel.Dashboard;
import com.watermanagement.central.datos.DatabaseManager;
import com.watermanagement.central.panel.WebDashboard;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Punto de entrada de WM_Central: carga la BD, arranca el panel web y el productor
 * Kafka y acepta conexiones de Monitores (WM_WS_M) y operarios (WM_FO), cada una
 * atendida en su propio {@link WM_Central_Thread}.
 */
public class WM_Central {

    /**
     * Arranca CENTRAL. Cada parámetro se toma, por orden de prioridad, del argumento,
     * de la variable de entorno o del valor por defecto.
     *
     * @param args {@code [puerto_sockets] [broker_kafka]}; por defecto SOCKET_PORT o 6000
     *             y KAFKA_BROKER o localhost:9092. El puerto web se toma de PORT (Railway) o 8080.
     */
    public static void main(String[] args) {

        // Railway puede guardar las variables entre comillas: se limpian
        String envPortWeb = System.getenv("PORT");
        if (envPortWeb != null) envPortWeb = envPortWeb.replace("\"", "").trim();

        String envPortSocket = System.getenv("SOCKET_PORT");
        if (envPortSocket != null) envPortSocket = envPortSocket.replace("\"", "").trim();

        String envKafkaBroker = System.getenv("KAFKA_BROKER");
        if (envKafkaBroker != null) envKafkaBroker = envKafkaBroker.replace("\"", "").trim();

        int socketPort = 6000;
        if (args.length > 0 && !args[0].isEmpty()) {
            socketPort = Integer.parseInt(args[0].replace("\"", "").trim());
        } else if (envPortSocket != null && !envPortSocket.isEmpty()) {
            socketPort = Integer.parseInt(envPortSocket);
        }

        String kafkaBroker = "localhost:9092";
        if (args.length > 1 && !args[1].isEmpty()) {
            kafkaBroker = args[1].replace("\"", "").trim();
        } else if (envKafkaBroker != null && !envKafkaBroker.isEmpty()) {
            kafkaBroker = envKafkaBroker;
        }

        int webPort = 8080;
        if (envPortWeb != null && !envPortWeb.isEmpty()) {
            int parsedPort = Integer.parseInt(envPortWeb);
            // Railway puede asignar a PORT el puerto de sockets (lo toma del EXPOSE 6000 del Dockerfile)
            if (parsedPort == socketPort) {
                System.out.println("[WARN] El puerto PORT (" + parsedPort + ") coincide con SOCKET_PORT. Asignando 8080 para Web Dashboard.");
                webPort = 8080;
            } else {
                webPort = parsedPort;
            }
        }

        System.out.println("--- INICIANDO CENTRAL ---");
        System.out.println("Puerto Web Dashboard: " + webPort);
        System.out.println("Puerto Sockets TCP:   " + socketPort);
        System.out.println("Kafka Broker:         " + kafkaBroker);
        System.out.println("-------------------------");

        DatabaseManager.inicializarYcargarEstaciones();
        Dashboard.iniciar();
        WebDashboard.iniciar(webPort);
        KafkaManager.inicializar(kafkaBroker);

        // Al apagar la JVM se vacían los eventos pendientes de Kafka
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Dashboard.log("Apagando servidor CENTRAL...");
            KafkaManager.cerrar();
        }));

        // Si el ServerSocket falla (p. ej. puerto ocupado) se vuelve a abrir: CENTRAL no se detiene nunca
        while (true) {
            try (ServerSocket skServidor = new ServerSocket(socketPort)) {
                Dashboard.log("Servidor de SOCKETS iniciado. Escuchando el puerto " + socketPort);
                Dashboard.log("Esperando conexiones de estaciones (WS) y operarios (FO)...");

                while (true) {
                    try {
                        Socket skCliente = skServidor.accept();
                        System.out.println("Nueva conexión Sockets recibida: " + skCliente.getInetAddress().getHostAddress());
                        Thread t = new WM_Central_Thread(skCliente);
                        t.start();
                    } catch (IOException e) {
                        System.err.println("Error al aceptar la conexión de un cliente: " + e.getMessage());
                    } catch (Exception e) {
                        System.err.println("Excepción inesperada en el bucle de clientes: " + e);
                    }
                }
            } catch (BindException e) {
                System.err.println("El puerto " + socketPort + " ya está en uso. Reintentando en 5 segundos...");
                esperar(5000);
            } catch (IOException e) {
                System.err.println("Error grave en el ServerSocket: " + e.getMessage() + ". Reintentando en 5 s...");
                esperar(5000);
            }
        }
    }

    private static void esperar(int milisegundos) {
        try {
            Thread.sleep(milisegundos);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}