// =====================================================================
// control.js - Vista "Control": órdenes a todas las estaciones y altas.
// =====================================================================

import { sendAction, registerStation, registerOperator } from './api.js';
import { showResult, toast } from './ui.js';
import { LOCATION_SUGGESTIONS } from './map.js';

const $ = id => document.getElementById(id);

async function all(action) {
    if (action === 'BLOCK' && !confirm('Block ALL stations? Any irrigation in progress will be stopped.')) return;
    const dur = action === 'START' ? $('dur-all').value : null;
    const result = await sendAction('ALL', action, dur);
    const messages = {
        START: `Irrigation requested on all available stations (${dur} s)`,
        BLOCK: 'All stations blocked (Out of Service)',
        ACTIVATE: 'All stations activated',
    };
    showResult(result, messages[action]);
}

async function newStation() {
    const id = $('new-ws-id').value.trim();
    const loc = $('new-ws-loc').value.trim();
    if (!id) return toast('Enter the station ID', true);
    const result = await registerStation(id, loc);
    showResult(result, `Station ${id} registered`);
    if (result === 'OK') { $('new-ws-id').value = ''; $('new-ws-loc').value = ''; }
}

async function newOperator() {
    const id = $('new-op-id').value.trim();
    if (!id) return toast('Enter the operator ID', true);
    const result = await registerOperator(id);
    showResult(result, `Operator ${id} registered`);
    if (result === 'OK') $('new-op-id').value = '';
}

export function initControl() {
    $('zone-names').innerHTML = LOCATION_SUGGESTIONS.map(n => `<option value="${n}">`).join('');
    $('btn-start-all').addEventListener('click', () => all('START'));
    $('btn-block-all').addEventListener('click', () => all('BLOCK'));
    $('btn-activate-all').addEventListener('click', () => all('ACTIVATE'));
    $('btn-new-ws').addEventListener('click', newStation);
    $('btn-new-op').addEventListener('click', newOperator);
}
