/**
 * Protocolo de tramas común a WM_WS_M, WM_WS_E y WM_FO:
 *
 *   <STX><DATA><ETX><LRC>     DATA = CODIGO#campo1#...#campoN
 *
 * LRC = XOR de todos los caracteres desde STX hasta ETX (incluidos).
 * Quien recibe una trama responde <ACK> si es correcta o <NACK> si no lo es.
 */
public final class Protocolo {

    public static final char STX = 0x02;
    public static final char ETX = 0x03;
    public static final char ACK = 0x06;
    public static final char NACK = 0x15;

    public static final String S_ACK = String.valueOf(ACK);
    public static final String S_NACK = String.valueOf(NACK);

    private Protocolo() {}

    /** Construye la trama completa para DATA. */
    public static String empaquetar(String datos) {
        String mensaje = STX + datos + ETX;
        return mensaje + lrc(mensaje);
    }

    /** Devuelve DATA si la trama está bien formada y su LRC es correcto; null en otro caso. */
    public static String desempaquetar(String trama) {
        if (trama == null || trama.length() < 3 || trama.charAt(0) != STX) return null;
        int etx = trama.indexOf(ETX);
        if (etx < 1 || etx != trama.length() - 2) return null;
        if (lrc(trama.substring(0, etx + 1)) != trama.charAt(trama.length() - 1)) return null;
        return trama.substring(1, etx);
    }

    public static boolean esAck(String msg) {
        return msg != null && msg.length() == 1 && msg.charAt(0) == ACK;
    }

    /** NACK, opcionalmente seguido del motivo (ej. "\u0015WS_NO_REGISTRADA"). */
    public static boolean esNack(String msg) {
        return msg != null && !msg.isEmpty() && msg.charAt(0) == NACK;
    }

    private static char lrc(String mensaje) {
        char lrc = 0;
        for (int i = 0; i < mensaje.length(); i++) lrc ^= mensaje.charAt(i);
        return lrc;
    }
}
