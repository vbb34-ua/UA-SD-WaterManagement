/**
 * @file Llamadas HTTP a CENTRAL (órdenes y altas).
 * Todas devuelven una promesa con el texto de la respuesta: "OK" o el motivo del error.
 */

async function get(url) {
    try {
        const res = await fetch(url);
        return (await res.text()).trim();
    } catch {
        return 'CENTRAL is not reachable';
    }
}

/**
 * Envía una orden de CENTRAL.
 * @param {string} ws id de la estación, o 'ALL' para todas
 * @param {'START'|'BLOCK'|'ACTIVATE'} action
 * @param {number|string} [duration] segundos de riego (solo START)
 */
export function sendAction(ws, action, duration) {
    let url = `/api/action?ws=${encodeURIComponent(ws)}&action=${action}`;
    if (duration) url += `&dur=${encodeURIComponent(duration)}`;
    return get(url);
}

/** Da de alta una estación nueva (queda desconectada hasta que su Monitor se conecte). */
export function registerStation(id, location) {
    return get(`/api/alta?tipo=ws&id=${encodeURIComponent(id)}&loc=${encodeURIComponent(location)}`);
}

/** Da de alta un operario nuevo. */
export function registerOperator(id) {
    return get(`/api/alta?tipo=op&id=${encodeURIComponent(id)}`);
}
