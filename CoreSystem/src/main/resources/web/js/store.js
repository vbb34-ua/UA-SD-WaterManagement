// =====================================================================
// store.js - Estado del panel y conexión en tiempo real con CENTRAL (SSE).
// Los demás módulos se suscriben con subscribe() y se repintan al cambiar.
// =====================================================================

export const STATUS = {
    DISPONIBLE:     { label: 'Available' },
    REGANDO:        { label: 'Watering' },
    FUGA:           { label: 'Leak' },
    FUERA_SERVICIO: { label: 'Out of Service' },
    DESCONECTADA:   { label: 'Disconnected' },
};

const MAX_LOGS = 300;

const state = {
    stations: {},      // id -> { id, location, estado, caudal, volumen, operario }
    logs: [],          // [{ time, text, level }] (más reciente primero)
    selected: null,    // id de la estación abierta en el panel lateral
    connected: false,
};

const listeners = new Set();

/** fn(state, cambio) con cambio: 'stations' | 'telemetry' | 'logs' | 'selection' | 'connection' */
export function subscribe(fn) {
    listeners.add(fn);
}

function emit(change, extra) {
    listeners.forEach(fn => fn(state, change, extra));
}

export function getState() {
    return state;
}

export function select(id) {
    state.selected = id;
    emit('selection');
}

/** Lista de estaciones ordenada por ID. */
export function stationList() {
    return Object.values(state.stations).sort((a, b) => a.id.localeCompare(b.id, undefined, { numeric: true }));
}

// ---------------------------------------------------------------- LOGS

/** Clasifica un evento de CENTRAL para colorearlo. */
function levelOf(text) {
    if (/ALERTA|FUGA|ABORTADO|DENEGAD|Ca[ií]da|Timeout/i.test(text)) return 'danger';
    if (/bloquea/i.test(text)) return 'warn';
    if (/AUTORIZA|finalizado|resuelta|reactiva|recupera|exitosa|conectado\./i.test(text)) return 'ok';
    return 'info';
}

function parseLog(raw) {
    const m = /^\[(.*?)\] (.*)$/.exec(raw);
    const text = m ? m[2] : raw;
    return { time: m ? m[1] : '', text, level: levelOf(text) };
}

// ---------------------------------------------------------------- SSE

export function connect() {
    const es = new EventSource('/events');

    es.onopen = () => {
        state.connected = true;
        emit('connection');
    };

    es.onerror = () => {
        // EventSource reintenta solo; mientras tanto se avisa al usuario
        state.connected = false;
        emit('connection');
    };

    const setStations = list => {
        const next = {};
        list.forEach(ws => { next[ws.id] = ws; });
        state.stations = next;
        if (state.selected && !next[state.selected]) state.selected = null;
        emit('stations');
    };

    es.addEventListener('ESTADO_INICIAL', e => setStations(JSON.parse(e.data)));
    es.addEventListener('ESTADO_ACTUALIZADO', e => setStations(JSON.parse(e.data)));

    es.addEventListener('TELEMETRIA', e => {
        const t = JSON.parse(e.data);
        const ws = state.stations[t.id];
        if (!ws) return;
        ws.caudal = t.caudal;
        ws.volumen = t.volumen;
        emit('telemetry', t.id);
    });

    es.addEventListener('LOGS_INICIALES', e => {
        state.logs = JSON.parse(e.data).map(parseLog).reverse();
        emit('logs');
    });

    es.addEventListener('NUEVO_LOG', e => {
        const log = parseLog(JSON.parse(e.data).mensaje);
        state.logs.unshift(log);
        if (state.logs.length > MAX_LOGS) state.logs.length = MAX_LOGS;
        emit('logs', log);
    });
}
