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

public class WM_Central {

    public static void main(String[] args) {

        // 1. LEER LAS VARIABLES DE ENTORNO DE RAILWAY (Prioridad nube)
        String envPortWeb = System.getenv("PORT");
        if (envPortWeb != null) envPortWeb = envPortWeb.replace("\"", "").trim();

        String envPortSocket = System.getenv("SOCKET_PORT");
        if (envPortSocket != null) envPortSocket = envPortSocket.replace("\"", "").trim();

        String envKafkaBroker = System.getenv("KAFKA_BROKER");
        if (envKafkaBroker != null) envKafkaBroker = envKafkaBroker.replace("\"", "").trim();

        // 2. ASIGNAR PUERTO DE SOCKETS (Prioridad: args[0] > SOCKET_PORT > 6000 por defecto)
        int socketPort = 6000;
        if (args.length > 0 && !args[0].isEmpty()) {
            socketPort = Integer.parseInt(args[0].replace("\"", "").trim());
        } else if (envPortSocket != null && !envPortSocket.isEmpty()) {
            socketPort = Integer.parseInt(envPortSocket);
        }

        // 3. ASIGNAR BROKER KAFKA (Prioridad: args[1] > KAFKA_BROKER > localhost:9092)
        String kafkaBroker = "localhost:9092";
        if (args.length > 1 && !args[1].isEmpty()) {
            kafkaBroker = args[1].replace("\"", "").trim();
        } else if (envKafkaBroker != null && !envKafkaBroker.isEmpty()) {
            kafkaBroker = envKafkaBroker;
        }

        // 4. ASIGNAR PUERTO WEB (Railway inyecta PORT dinámicamente)
        int webPort = 8080;
        if (envPortWeb != null && !envPortWeb.isEmpty()) {
            int parsedPort = Integer.parseInt(envPortWeb);
            // Evitar colisión si Railway asigna PORT igual al socketPort (ej. si leyó EXPOSE 6000)
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

        // 3. Inicializar sistema consultando la BD
        DatabaseManager.inicializarYcargarEstaciones();

        // 4. Inicializar el Dashboard UI de terminal
        Dashboard.iniciar();
        
        // 5. Iniciar el Dashboard Web pasándole el puerto de Railway
        WebDashboard.iniciar(webPort);

        // 6. Inicializar el productor Kafka pasándole la ruta de Railway
        KafkaManager.inicializar(kafkaBroker);

        // Shutdown hook: cerrar Kafka limpiamente al apagar la JVM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Dashboard.log("Apagando servidor CENTRAL...");
            KafkaManager.cerrar();
        }));

        // Bucle de ejecución continuo para mantener el sistema 24/7 (Sockets)
        while (true) {
            try (ServerSocket skServidor = new ServerSocket(socketPort)) { // Usa la variable de entorno
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

    // Método auxiliar (mantén el que ya tenías)
    private static void esperar(int milisegundos) {
        try {
            Thread.sleep(milisegundos);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}