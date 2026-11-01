import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class WebDashboard {

    private static HttpServer server;
    // Lista de clientes conectados a la página web esperando eventos en tiempo real
    private static final List<HttpExchange> clientesSSE = new CopyOnWriteArrayList<>();

    public static void iniciar(int puerto) {
        try {
            server = HttpServer.create(new InetSocketAddress(puerto), 0);
            
// 1. Endpoint para servir la página web HTML desde el JAR
server.createContext("/", exchange -> {
    // Intentamos leer el archivo dentro del JAR (debe estar en src/main/resources/index.html)
    try (java.io.InputStream is = WebDashboard.class.getResourceAsStream("/index.html")) {
        if (is != null) {
            byte[] bytes = is.readAllBytes();
            exchange.getResponseHeaders().add("Content-Type", "text/html; charset=UTF-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        } else {
            String response = "Error 404: No se encuentra index.html en src/main/resources/";
            exchange.sendResponseHeaders(404, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    } catch (IOException e) {
        System.err.println("[WEB] Error al servir index.html: " + e.getMessage());
    }
});

            // 2. Endpoint SSE (Server-Sent Events) para actualizaciones en tiempo real
            server.createContext("/events", exchange -> {
                exchange.getResponseHeaders().add("Content-Type", "text/event-stream");
                exchange.getResponseHeaders().add("Cache-Control", "no-cache");
                exchange.getResponseHeaders().add("Connection", "keep-alive");
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.sendResponseHeaders(200, 0); // 0 = response de longitud arbitraria
                
                clientesSSE.add(exchange);
                System.out.println("[WEB] Nuevo navegador conectado al Dashboard.");
                
                // Enviamos un evento inicial para forzar que el Dashboard envíe todo su estado
                broadcast("ESTADO_INICIAL", Dashboard.obtenerEstadoJSON());
            });

            // 3. Endpoint API para acciones manuales (Subtarea 6)
            server.createContext("/api/action", exchange -> {
                String query = exchange.getRequestURI().getQuery();
                String response = "OK";
                
                if (query != null) {
                    String ws = null;
                    String action = null;
                    int duracion = GestorRiegos.DURACION_DEFECTO_S;

                    for (String param : query.split("&")) {
                        String[] pair = param.split("=");
                        if (pair.length == 2) {
                            String valor = java.net.URLDecoder.decode(pair[1], java.nio.charset.StandardCharsets.UTF_8);
                            if (pair[0].equals("ws")) ws = valor;
                            if (pair[0].equals("action")) action = valor;
                            if (pair[0].equals("dur")) {
                                try { duracion = Integer.parseInt(valor); } catch (NumberFormatException ignored) {}
                            }
                        }
                    }

                    // Punto 11: órdenes a UNA estación (ws=<id>) o a TODAS (ws=ALL)
                    if (ws != null && action != null) {
                        boolean todas = "ALL".equals(ws);
                        switch (action) {
                            case "START" -> {
                                if (todas) GestorRiegos.iniciarTodas(duracion);
                                else GestorRiegos.iniciarDesdeCentral(ws, duracion);
                            }
                            case "BLOCK" -> {
                                if (todas) GestorRiegos.bloquearTodas();
                                else GestorRiegos.bloquear(ws);
                            }
                            case "ACTIVATE" -> {
                                if (todas) GestorRiegos.activarTodas();
                                else GestorRiegos.activar(ws);
                            }
                            default -> response = "ACCION_DESCONOCIDA";
                        }
                    } else {
                        response = "PARAMETROS_INCOMPLETOS";
                    }
                }
                
                // Respondemos al navegador
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.sendResponseHeaders(200, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            // 4. Endpoint de altas: /api/alta?tipo=ws&id=WS_11&loc=Parque Central  |  /api/alta?tipo=op&id=FO_11
            server.createContext("/api/alta", exchange -> {
                String tipo = null, id = null, loc = null;
                String query = exchange.getRequestURI().getRawQuery();
                if (query != null) {
                    for (String param : query.split("&")) {
                        String[] pair = param.split("=", 2);
                        if (pair.length != 2) continue;
                        String valor = java.net.URLDecoder.decode(pair[1], java.nio.charset.StandardCharsets.UTF_8).trim();
                        if (pair[0].equals("tipo")) tipo = valor;
                        if (pair[0].equals("id")) id = valor;
                        if (pair[0].equals("loc")) loc = valor;
                    }
                }
                String error;
                if ("ws".equals(tipo)) error = GestorRiegos.altaEstacion(id, loc);
                else if ("op".equals(tipo)) error = GestorRiegos.altaOperario(id);
                else error = "Tipo de alta desconocido";

                byte[] resp = (error == null ? "OK" : error).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            });

            server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
            server.start();
            System.out.println("[WEB] Dashboard moderno disponible en: http://localhost:" + puerto);

        } catch (IOException e) {
            System.err.println("[WEB] Error arrancando el servidor HTTP: " + e.getMessage());
        }
    }

    // Método que el antiguo Dashboard llamará para notificar a todos los navegadores
    public static void broadcast(String tipoEvento, String datosJSON) {
        String mensaje = "event: " + tipoEvento + "\ndata: " + datosJSON + "\n\n";
        
        List<HttpExchange> caidos = new ArrayList<>();
        
        for (HttpExchange cliente : clientesSSE) {
            try {
                OutputStream os = cliente.getResponseBody();
                os.write(mensaje.getBytes("UTF-8"));
                os.flush();
            } catch (IOException e) {
                // Si falla (ej. cerraron la pestaña), lo marcamos para borrarlo
                caidos.add(cliente);
            }
        }
        clientesSSE.removeAll(caidos);
    }
}
