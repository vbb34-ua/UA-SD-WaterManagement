package com.watermanagement.central.panel;

import com.watermanagement.central.logica.GestorRiegos;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Servidor HTTP del panel web de CENTRAL:
 * <ul>
 *   <li>{@code /}: ficheros estáticos de {@code src/main/resources/web} (index.html, css, js)</li>
 *   <li>{@code /events}: Server-Sent Events con el estado de la red en tiempo real</li>
 *   <li>{@code /api/action?ws=<id|ALL>&action=<START|BLOCK|ACTIVATE>[&dur=s]}: órdenes de CENTRAL</li>
 *   <li>{@code /api/alta?tipo=<ws|op>&id=<id>[&loc=ubicación]}: alta de estaciones y operarios</li>
 * </ul>
 */
public class WebDashboard {

    private static HttpServer server;
    /** Navegadores conectados a {@code /events}. */
    private static final List<HttpExchange> clientesSSE = new CopyOnWriteArrayList<>();

    private static final Map<String, String> TIPOS_MIME = new HashMap<>();
    static {
        TIPOS_MIME.put("html", "text/html; charset=UTF-8");
        TIPOS_MIME.put("css", "text/css; charset=UTF-8");
        TIPOS_MIME.put("js", "text/javascript; charset=UTF-8");
        TIPOS_MIME.put("svg", "image/svg+xml");
        TIPOS_MIME.put("png", "image/png");
        TIPOS_MIME.put("ico", "image/x-icon");
    }

    /**
     * Arranca el servidor HTTP y el latido de SSE. Si el puerto no está disponible, CENTRAL
     * sigue funcionando sin panel web.
     */
    public static void iniciar(int puerto) {
        try {
            server = HttpServer.create(new InetSocketAddress(puerto), 0);

            server.createContext("/", WebDashboard::servirEstatico);

            server.createContext("/events", exchange -> {
                exchange.getResponseHeaders().add("Content-Type", "text/event-stream; charset=UTF-8");
                exchange.getResponseHeaders().add("Cache-Control", "no-cache");
                exchange.getResponseHeaders().add("Connection", "keep-alive");
                exchange.sendResponseHeaders(200, 0); // 0 = longitud indefinida (la conexión queda abierta)

                // El estado inicial se envía solo a este navegador. Se toman los cerrojos en el mismo
                // orden que Dashboard -> broadcast (Dashboard y luego WebDashboard): así ningún cambio
                // se pierde entre el estado inicial y el alta del cliente, y no hay interbloqueos.
                synchronized (Dashboard.class) {
                    synchronized (WebDashboard.class) {
                        enviar(exchange, "ESTADO_INICIAL", Dashboard.obtenerEstadoJSON());
                        enviar(exchange, "LOGS_INICIALES", Dashboard.obtenerLogsJSON());
                        clientesSSE.add(exchange);
                    }
                }
                System.out.println("[WEB] Nuevo navegador conectado al Dashboard.");
            });

            server.createContext("/api/action", exchange -> {
                Map<String, String> p = parametros(exchange);
                String ws = p.get("ws");
                String action = p.get("action");
                int duracion = GestorRiegos.DURACION_DEFECTO_S;
                try { duracion = Integer.parseInt(p.getOrDefault("dur", "")); } catch (NumberFormatException ignored) {}

                String respuesta = "OK";
                if (ws == null || action == null) {
                    respuesta = "Parámetros incompletos";
                } else {
                    boolean todas = "ALL".equals(ws);
                    switch (action) {
                        case "START" -> {
                            if (todas) GestorRiegos.iniciarTodas(duracion);
                            else {
                                String error = GestorRiegos.iniciarDesdeCentral(ws, duracion);
                                if (error != null) respuesta = error;
                            }
                        }
                        case "BLOCK" -> {
                            if (todas) GestorRiegos.bloquearTodas();
                            else GestorRiegos.bloquear(ws);
                        }
                        case "ACTIVATE" -> {
                            if (todas) GestorRiegos.activarTodas();
                            else GestorRiegos.activar(ws);
                        }
                        default -> respuesta = "Acción desconocida";
                    }
                }
                responderTexto(exchange, respuesta);
            });

            server.createContext("/api/alta", exchange -> {
                Map<String, String> p = parametros(exchange);
                String tipo = p.get("tipo");
                String error;
                if ("ws".equals(tipo)) error = GestorRiegos.altaEstacion(p.get("id"), p.get("loc"));
                else if ("op".equals(tipo)) error = GestorRiegos.altaOperario(p.get("id"));
                else error = "Tipo de alta desconocido";
                responderTexto(exchange, error == null ? "OK" : error);
            });

            server.setExecutor(Executors.newCachedThreadPool());
            server.start();

            // Latido cada 15 s: evita que el proxy de Railway cierre las conexiones SSE inactivas
            // y detecta los navegadores que ya se cerraron
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "Hilo-SSE-Latido");
                t.setDaemon(true);
                return t;
            }).scheduleAtFixedRate(WebDashboard::latido, 15, 15, TimeUnit.SECONDS);

            System.out.println("[WEB] Dashboard disponible en: http://localhost:" + puerto);
        } catch (IOException e) {
            System.err.println("[WEB] Error arrancando el servidor HTTP: " + e.getMessage());
        }
    }

    // ---------------------------------------------------------------- FICHEROS ESTÁTICOS

    /**
     * Sirve un fichero del panel desde el classpath ({@code /web/...}, dentro del JAR).
     * Solo admite rutas simples, sin "..", para no exponer otros ficheros.
     */
    private static void servirEstatico(HttpExchange exchange) throws IOException {
        String ruta = exchange.getRequestURI().getPath();
        if (ruta.equals("/")) ruta = "/index.html";

        if (ruta.contains("..") || !ruta.matches("[A-Za-z0-9_\\-./]+")) {
            responder(exchange, 400, "text/plain; charset=UTF-8", "Ruta no válida".getBytes(StandardCharsets.UTF_8));
            return;
        }

        try (InputStream is = WebDashboard.class.getResourceAsStream("/web" + ruta)) {
            if (is == null) {
                responder(exchange, 404, "text/plain; charset=UTF-8", "404 - No encontrado".getBytes(StandardCharsets.UTF_8));
                return;
            }
            String ext = ruta.substring(ruta.lastIndexOf('.') + 1).toLowerCase();
            exchange.getResponseHeaders().add("Cache-Control", "no-cache");
            responder(exchange, 200, TIPOS_MIME.getOrDefault(ext, "application/octet-stream"), is.readAllBytes());
        }
    }

    // ---------------------------------------------------------------- SSE

    /**
     * Envía un evento SSE a todos los navegadores conectados y descarta los que ya no responden.
     *
     * @param tipoEvento nombre del evento (ESTADO_ACTUALIZADO, TELEMETRIA, NUEVO_LOG...)
     * @param datosJSON  datos del evento en JSON
     */
    public static synchronized void broadcast(String tipoEvento, String datosJSON) {
        List<HttpExchange> caidos = new ArrayList<>();
        for (HttpExchange cliente : clientesSSE) {
            if (!enviar(cliente, tipoEvento, datosJSON)) caidos.add(cliente);
        }
        quitar(caidos);
    }

    /** Envía un comentario SSE ({@code : ping}), que el navegador ignora, a todos los clientes. */
    private static synchronized void latido() {
        List<HttpExchange> caidos = new ArrayList<>();
        for (HttpExchange cliente : clientesSSE) {
            try {
                OutputStream os = cliente.getResponseBody();
                os.write(": ping\n\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
            } catch (IOException e) {
                caidos.add(cliente);
            }
        }
        quitar(caidos);
    }

    /** @return false si el navegador se ha desconectado */
    private static boolean enviar(HttpExchange cliente, String tipoEvento, String datosJSON) {
        try {
            OutputStream os = cliente.getResponseBody();
            os.write(("event: " + tipoEvento + "\ndata: " + datosJSON + "\n\n").getBytes(StandardCharsets.UTF_8));
            os.flush();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private static void quitar(List<HttpExchange> caidos) {
        for (HttpExchange c : caidos) {
            clientesSSE.remove(c);
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    // ---------------------------------------------------------------- AUXILIARES

    /** @return los parámetros de la URL ({@code ?clave=valor&...}) ya decodificados */
    private static Map<String, String> parametros(HttpExchange exchange) {
        Map<String, String> p = new HashMap<>();
        String query = exchange.getRequestURI().getRawQuery();
        if (query == null) return p;
        for (String par : query.split("&")) {
            String[] kv = par.split("=", 2);
            if (kv.length == 2) p.put(kv[0], URLDecoder.decode(kv[1], StandardCharsets.UTF_8).trim());
        }
        return p;
    }

    private static void responderTexto(HttpExchange exchange, String texto) throws IOException {
        responder(exchange, 200, "text/plain; charset=UTF-8", texto.getBytes(StandardCharsets.UTF_8));
    }

    private static void responder(HttpExchange exchange, int codigo, String tipo, byte[] cuerpo) throws IOException {
        exchange.getResponseHeaders().add("Content-Type", tipo);
        exchange.sendResponseHeaders(codigo, cuerpo.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(cuerpo);
        }
    }
}
