package com.watermanagement.central.red;

import com.watermanagement.central.logica.GestorRiegos;
import com.watermanagement.central.panel.Dashboard;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Hilo que atiende una conexión de socket con CENTRAL: un Monitor de WS (WM_WS_M)
 * o una aplicación de operario (WM_FO).
 *
 * Protocolo (en ambos sentidos): <STX>CODIGO#campo1#...#campoN<ETX><LRC>,
 * LRC = XOR de todos los caracteres desde STX hasta ETX. Quien recibe una trama
 * responde <ACK> si es correcta o <NACK>[motivo] si no lo es.
 */
public class WM_Central_Thread extends Thread {

    private enum Tipo { DESCONOCIDO, ESTACION, OPERARIO }

    private static final char STX = 0x02;
    private static final char ETX = 0x03;
    private static final char ACK = 0x06;
    private static final char NACK = 0x15;

    /** Estrategia 2: si en 15 s no llega nada (telemetría, ping...) la conexión se da por caída. */
    private static final int TIMEOUT_LECTURA_MS = 15000;

    private final Socket skCliente;
    private DataInputStream entrada;
    private DataOutputStream salida;

    private Tipo tipo = Tipo.DESCONOCIDO;
    private String idConexion = null;
    private volatile long ultimaActividadMs = System.currentTimeMillis();

    public WM_Central_Thread(Socket p_cliente) {
        this.skCliente = p_cliente;
    }

    // ---------------------------------------------------------------- E/S

    /** Acción a ejecutar justo después de enviar la respuesta (ACK) de la operación actual. */
    private Runnable trasRespuesta = null;

    private synchronized boolean escribeSocket(String datos) {
        try {
            salida.writeUTF(datos);
            salida.flush();
            return true;
        } catch (Exception e) {
            System.out.println("Error escribiendo en socket [" + idConexion + "]: " + e.getMessage());
            return false;
        }
    }

    /** Trama con formato STX/ETX/LRC (hacia los Monitores). */
    public boolean enviarTrama(String datos) {
        String mensaje = STX + datos + ETX;
        return escribeSocket(mensaje + calcularLRC(mensaje));
    }

    /** Mensaje informativo para un operario, también en trama STX/ETX/LRC. Devuelve false si no se pudo enviar. */
    public boolean enviarTexto(String texto) {
        return enviarTrama(texto);
    }

    /**
     * Programa una acción para después del ACK (ej. entregar el buzón tras el registro),
     * para que el cliente no reciba otros mensajes antes de la respuesta a su registro.
     */
    public void alTerminarRespuesta(Runnable accion) {
        this.trasRespuesta = accion;
    }

    public long msDesdeUltimaActividad() {
        return System.currentTimeMillis() - ultimaActividadMs;
    }

    public void cerrar() {
        try { skCliente.close(); } catch (IOException ignored) {}
    }

    private static char calcularLRC(String mensaje) {
        char lrc = 0;
        for (int i = 0; i < mensaje.length(); i++) {
            lrc ^= mensaje.charAt(i);
        }
        return lrc;
    }

    /** Valida STX/ETX/LRC y devuelve el campo DATA, o null si la trama es incorrecta. */
    private String procesarTrama(String trama) {
        if (trama == null || trama.length() < 3) return null;
        if (trama.charAt(0) != STX) return null;

        int etxIndex = trama.indexOf(ETX);
        if (etxIndex == -1 || etxIndex != trama.length() - 2) return null;

        char lrcRecibido = trama.charAt(trama.length() - 1);
        char lrcCalculado = calcularLRC(trama.substring(0, trama.length() - 1));
        if (lrcRecibido != lrcCalculado) {
            System.out.println("SRV: Error de validación LRC en trama de [" + idConexion + "].");
            return null;
        }
        return trama.substring(1, etxIndex);
    }

    // ---------------------------------------------------------------- OPERACIONES

    private static String ack() { return String.valueOf(ACK); }
    private static String nack(String motivo) { return NACK + (motivo == null ? "" : motivo); }

    private static int parseEntero(String s, int defecto) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return defecto; }
    }

    /** Devuelve la respuesta que se envía al cliente (ACK/NACK) o null si no hay que responder. */
    private String ejecutarOperacion(String data) {
        String[] partes = data.split("#", -1);
        String operacion = partes[0];

        switch (operacion) {
            case "PING":
                return ack();

            // ---------------- Mensajes de los MONITORES (WM_WS_M)
            case "REGISTRO": { // REGISTRO#id#ubicacion[#riegoId#operario#duracion si tiene un riego en curso]
                if (partes.length < 3 || tipo == Tipo.OPERARIO) return nack("FORMATO");
                String id = partes[1].trim();
                String location = partes[2].trim().isEmpty() ? null : partes[2].trim(); // vacía: la de la BD
                String[] riegoEnCurso = (partes.length >= 6 && !partes[3].isEmpty())
                        ? new String[] { partes[3], partes[4], partes[5] } : null;
                String error = GestorRiegos.registrarEstacion(id, location, riegoEnCurso, this);
                if (error != null) return nack(error);
                this.tipo = Tipo.ESTACION;
                this.idConexion = id;
                return ack();
            }
            case "RIEGO_AUTORIZADO":
                if (tipo != Tipo.ESTACION || partes.length < 2) return nack("NO_REGISTRADO");
                GestorRiegos.riegoAutorizado(idConexion, partes[1]);
                return ack();

            case "RIEGO_DENEGADO":
                if (tipo != Tipo.ESTACION || partes.length < 2) return nack("NO_REGISTRADO");
                GestorRiegos.riegoDenegado(idConexion, partes[1], partes.length >= 3 ? partes[2] : "sin motivo");
                return ack();

            case "TELEMETRIA":
                if (tipo != Tipo.ESTACION || partes.length < 4) return nack("NO_REGISTRADO");
                GestorRiegos.telemetria(idConexion, partes[2], partes[3]);
                return ack();

            case "FIN_RIEGO": // FIN_RIEGO#ws#volumen#duracion#riegoId#motivo
                if (tipo != Tipo.ESTACION || partes.length < 5) return nack("NO_REGISTRADO");
                GestorRiegos.finRiego(idConexion, partes[4], partes[2], partes[3],
                        partes.length >= 6 ? partes[5] : null, this);
                return ack();

            case "SINCRONIZADO": // La estación ha reenviado todo lo pendiente tras reconectarse
                if (tipo != Tipo.ESTACION) return nack("NO_REGISTRADO");
                GestorRiegos.estacionSincronizada(idConexion);
                return ack();

            case "AVERIA":
                if (tipo != Tipo.ESTACION) return nack("NO_REGISTRADO");
                GestorRiegos.averia(idConexion);
                return ack();

            case "RESUELTA":
                if (tipo != Tipo.ESTACION) return nack("NO_REGISTRADO");
                GestorRiegos.averiaResuelta(idConexion);
                return ack();

            case "SOLICITUD_MANUAL": // Riego pedido desde el menú de la propia estación
                if (tipo != Tipo.ESTACION) return nack("NO_REGISTRADO");
                GestorRiegos.solicitarRiego(idConexion, GestorRiegos.OPERARIO_LOCAL,
                        partes.length >= 3 ? parseEntero(partes[2], 0) : 0);
                return ack();

            // ---------------- Mensajes de los OPERARIOS (WM_FO)
            case "REGISTRO_OPERARIO": {
                if (partes.length < 2 || tipo == Tipo.ESTACION) return nack("FORMATO");
                String idOp = partes[1].trim();
                String error = GestorRiegos.registrarOperario(idOp, this);
                if (error != null) return nack(error);
                this.tipo = Tipo.OPERARIO;
                this.idConexion = idOp;
                return ack();
            }
            case "ACTIVAR":
                if (tipo != Tipo.OPERARIO || partes.length < 2) {
                    enviarTexto("ERROR: Debe registrarse como operario antes de solicitar un riego.");
                    return null;
                }
                // El operario siempre es el de la sesión (no se puede pedir en nombre de otro)
                GestorRiegos.solicitarRiego(partes[1].trim(), idConexion,
                        partes.length >= 4 ? parseEntero(partes[3], 0) : 0);
                return ack();

            case "LISTAR_WS":
                enviarTexto("LISTA_ESTACIONES:" + Dashboard.obtenerListadoEstaciones());
                return ack();
        }

        return nack("OPERACION_DESCONOCIDA");
    }

    // ---------------------------------------------------------------- BUCLE

    @Override
    public void run() {
        try {
            entrada = new DataInputStream(skCliente.getInputStream());
            salida = new DataOutputStream(skCliente.getOutputStream());
            // Estrategia 2: timeout de lectura para detectar caídas e inactividad
            skCliente.setSoTimeout(TIMEOUT_LECTURA_MS);

            while (true) {
                String cadena = entrada.readUTF();
                ultimaActividadMs = System.currentTimeMillis();

                // ACK/NACK del cliente a una trama enviada por CENTRAL: no se responden
                if (cadena.length() == 1 && cadena.charAt(0) == ACK) continue;
                if (cadena.length() >= 1 && cadena.charAt(0) == NACK) {
                    System.err.println("[CENTRAL] [" + idConexion + "] ha rechazado una trama (NACK): LRC o formato incorrecto.");
                    continue;
                }

                String data = procesarTrama(cadena);
                String respuesta = (data != null) ? ejecutarOperacion(data) : nack("TRAMA_INVALIDA");
                if (respuesta != null) {
                    escribeSocket(respuesta);
                }
                if (trasRespuesta != null) {
                    Runnable accion = trasRespuesta;
                    trasRespuesta = null;
                    accion.run();
                }
            }
        } catch (SocketTimeoutException te) {
            System.err.println("[CENTRAL] Timeout de " + (TIMEOUT_LECTURA_MS / 1000) + " s sin actividad en [" + idConexion + "].");
        } catch (EOFException eofe) {
            System.out.println("[CENTRAL] Conexión cerrada por el cliente [" + idConexion + "].");
        } catch (Exception e) {
            System.err.println("[CENTRAL] Conexión perdida con [" + idConexion + "]: " + e.getMessage());
        } finally {
            if (tipo == Tipo.ESTACION) {
                GestorRiegos.estacionDesconectada(idConexion, this);
            } else if (tipo == Tipo.OPERARIO) {
                GestorRiegos.operarioDesconectado(idConexion, this);
            }
            cerrar();
        }
    }
}
