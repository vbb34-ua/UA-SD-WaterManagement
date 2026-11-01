// =====================================================================
// stations.js - Tabla de estaciones (vista "Stations") y listas laterales
// de la vista "Monitoring" (riegos activos y estaciones que requieren atención).
// =====================================================================

import { STATUS, subscribe, getState, stationList, select } from './store.js';
import { esc, statusPill, dot } from './ui.js';

let statusFilter = 'ALL';
let search = '';

// ---------------------------------------------------------------- TABLA

function matches(ws) {
    if (statusFilter !== 'ALL' && ws.estado !== statusFilter) return false;
    if (!search) return true;
    const q = search.toLowerCase();
    return ws.id.toLowerCase().includes(q) || (ws.location || '').toLowerCase().includes(q);
}

function renderTable() {
    const { selected } = getState();
    const rows = stationList().filter(matches);
    document.getElementById('stations-empty').hidden = rows.length > 0;
    document.getElementById('stations-tbody').innerHTML = rows.map(ws => {
        const watering = ws.estado === 'REGANDO';
        const volume = parseFloat(ws.volumen) > 0 ? ws.volumen : null;
        return `
        <tr data-id="${esc(ws.id)}" class="${ws.id === selected ? 'selected' : ''}">
            <td class="id">${esc(ws.id)}</td>
            <td>${esc(ws.location)}</td>
            <td>${statusPill(ws.estado)}</td>
            <td class="num">${watering ? esc(ws.caudal) : '<span class="muted">–</span>'}</td>
            <td class="num">${volume ? esc(volume) : '<span class="muted">–</span>'}</td>
            <td>${watering ? esc(ws.operario) : '<span class="muted">–</span>'}</td>
        </tr>`;
    }).join('');
}

function renderFilterChips() {
    const counts = {};
    stationList().forEach(ws => { counts[ws.estado] = (counts[ws.estado] || 0) + 1; });
    const total = stationList().length;
    const chips = [['ALL', `All (${total})`]]
        .concat(Object.entries(STATUS).map(([k, s]) => [k, `${s.label} (${counts[k] || 0})`]));
    document.getElementById('status-filter').innerHTML = chips.map(([k, label]) =>
        `<button class="chip ${statusFilter === k ? 'active' : ''}" data-status="${k}">
            ${k === 'ALL' ? '' : dot(k)}${esc(label)}
        </button>`).join('');
}

/** Usado por los contadores de la cabecera para abrir la tabla filtrada. */
export function setStatusFilter(status) {
    statusFilter = status;
    renderFilterChips();
    renderTable();
}

// ---------------------------------------------------------------- MONITORING (lateral)

function item(ws, valueHtml, sub) {
    return `
    <div class="item" data-id="${esc(ws.id)}">
        ${dot(ws.estado)}
        <div>
            <div class="item-title">${esc(ws.id)}</div>
            <div class="item-sub">${esc(sub)}</div>
        </div>
        <div class="item-value">${valueHtml}</div>
    </div>`;
}

function renderSideLists() {
    const list = stationList();

    const watering = list.filter(ws => ws.estado === 'REGANDO');
    document.getElementById('active-list').innerHTML = watering.length
        ? watering.map(ws => item(ws,
            `${esc(ws.caudal)} L/min<small>${esc(ws.volumen)} L</small>`,
            `${ws.location} · ${ws.operario}`)).join('')
        : '<div class="empty">No irrigation in progress.</div>';

    const attention = list.filter(ws => ws.estado === 'FUGA' || ws.estado === 'FUERA_SERVICIO');
    document.getElementById('alert-list').innerHTML = attention.length
        ? attention.map(ws => item(ws, statusPill(ws.estado), ws.location)).join('')
        : '<div class="empty">No leaks or blocked stations.</div>';
}

// ---------------------------------------------------------------- INIT

export function initStations() {
    // Clic en una fila o elemento de lista -> abre el detalle
    const openFrom = e => {
        const row = e.target.closest('[data-id]');
        if (row) select(row.dataset.id);
    };
    document.getElementById('stations-tbody').addEventListener('click', openFrom);
    document.getElementById('active-list').addEventListener('click', openFrom);
    document.getElementById('alert-list').addEventListener('click', openFrom);

    document.getElementById('status-filter').addEventListener('click', e => {
        const chip = e.target.closest('[data-status]');
        if (chip) setStatusFilter(chip.dataset.status);
    });
    document.getElementById('station-search').addEventListener('input', e => {
        search = e.target.value.trim();
        renderTable();
    });

    subscribe((state, change) => {
        if (change === 'stations' || change === 'telemetry') {
            renderTable();
            renderSideLists();
            if (change === 'stations') renderFilterChips();
        } else if (change === 'selection') {
            renderTable();
        }
    });
}
