// =====================================================================
// detail.js - Panel lateral con el detalle de una estación y sus órdenes
// (iniciar riego, bloquear, activar). Se abre al pinchar una estación.
// =====================================================================

import { STATUS, subscribe, getState, select } from './store.js';
import { sendAction } from './api.js';
import { esc, showResult } from './ui.js';

const $ = id => document.getElementById(id);

function current() {
    const { selected, stations } = getState();
    return selected ? stations[selected] : null;
}

function render() {
    const ws = current();
    const drawer = $('drawer');
    const open = !!ws;
    drawer.classList.toggle('open', open);
    drawer.setAttribute('aria-hidden', String(!open));
    $('drawer-backdrop').hidden = !open;
    if (!ws) return;

    const watering = ws.estado === 'REGANDO';
    $('d-id').textContent = ws.id;
    $('d-loc').textContent = ws.location;
    const status = $('d-status');
    status.className = `pill big s-${ws.estado}`;
    status.innerHTML = `<span class="dot s-${esc(ws.estado)}"></span>${esc(STATUS[ws.estado]?.label ?? ws.estado)}`;
    $('d-flow').textContent = watering ? `${ws.caudal} L/min` : '0.0 L/min';
    $('d-vol').textContent = `${ws.volumen} L`;
    $('d-op').textContent = watering ? ws.operario : '—';

    const leak = ws.estado === 'FUGA';
    const offline = ws.estado === 'DESCONECTADA';
    const check = (ok, text) => `<li><span class="dot s-${ok ? 'DISPONIBLE' : 'FUGA'}"></span>${esc(text)}</li>`;
    $('d-checks').innerHTML = offline
        ? `<li><span class="dot s-DESCONECTADA"></span>No connection with the station's Monitor</li>`
        : check(!leak, leak ? 'Leak detected — irrigation not allowed' : 'Leak detector: no issues')
          + check(true, 'Connected to CENTRAL');

    // Solo se ofrecen las órdenes que tienen sentido en el estado actual
    const blocked = ws.estado === 'FUERA_SERVICIO';
    $('d-dur-field').hidden = ws.estado !== 'DISPONIBLE';
    $('d-start').hidden = ws.estado !== 'DISPONIBLE';
    $('d-block').hidden = blocked;
    $('d-activate').hidden = !blocked;
}

async function act(action) {
    const ws = current();
    if (!ws) return;
    const dur = action === 'START' ? $('d-dur').value : null;
    const result = await sendAction(ws.id, action, dur);
    const messages = {
        START: `Irrigation requested for ${ws.id} (${dur} s)`,
        BLOCK: `${ws.id} blocked (Out of Service)`,
        ACTIVATE: `${ws.id} activated`,
    };
    showResult(result, messages[action]);
}

export function initDetail() {
    $('d-close').addEventListener('click', () => select(null));
    $('drawer-backdrop').addEventListener('click', () => select(null));
    document.addEventListener('keydown', e => { if (e.key === 'Escape') select(null); });

    $('d-start').addEventListener('click', () => act('START'));
    $('d-block').addEventListener('click', () => act('BLOCK'));
    $('d-activate').addEventListener('click', () => act('ACTIVATE'));

    subscribe((state, change, id) => {
        if (change === 'selection' || change === 'stations') render();
        else if (change === 'telemetry' && id === state.selected) render();
    });
}
