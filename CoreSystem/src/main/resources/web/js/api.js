// =====================================================================
// api.js - Llamadas HTTP a CENTRAL (órdenes y altas).
// Todas devuelven el texto de respuesta: "OK" o el motivo del error.
// =====================================================================

async function get(url) {
    try {
        const res = await fetch(url);
        return (await res.text()).trim();
    } catch {
        return 'CENTRAL is not reachable';
    }
}

/** action: START | BLOCK | ACTIVATE. ws: id de la estación o 'ALL'. */
export function sendAction(ws, action, duration) {
    let url = `/api/action?ws=${encodeURIComponent(ws)}&action=${action}`;
    if (duration) url += `&dur=${encodeURIComponent(duration)}`;
    return get(url);
}

export function registerStation(id, location) {
    return get(`/api/alta?tipo=ws&id=${encodeURIComponent(id)}&loc=${encodeURIComponent(location)}`);
}

export function registerOperator(id) {
    return get(`/api/alta?tipo=op&id=${encodeURIComponent(id)}`);
}
