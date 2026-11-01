/**
 * @file Historial de eventos de CENTRAL (vista "Events", con filtros) y últimos eventos
 * en la vista "Monitoring".
 */

import { subscribe, getState } from './store.js';
import { esc } from './ui.js';

/** Filtros por nivel: [nivel, texto del botón]. */
const LEVELS = [['all', 'All'], ['danger', 'Alerts'], ['warn', 'Commands'], ['ok', 'Success'], ['info', 'Info']];
/** Eventos que se muestran en la vista "Monitoring". */
const RECENT = 8;

let level = 'all';
let search = '';

function row(log) {
    return `<div class="event ${log.level}">
        <span class="event-time">${esc(log.time)}</span>
        <span class="dot"></span>
        <span class="event-msg">${esc(log.text)}</span>
    </div>`;
}

function renderRecent() {
    const logs = getState().logs.slice(0, RECENT);
    document.getElementById('recent-events').innerHTML = logs.length
        ? logs.map(row).join('')
        : '<div class="empty">No events yet.</div>';
}

function renderAll() {
    const q = search.toLowerCase();
    const logs = getState().logs.filter(l =>
        (level === 'all' || l.level === level) && (!q || l.text.toLowerCase().includes(q)));
    document.getElementById('event-list').innerHTML = logs.length
        ? logs.map(row).join('')
        : '<div class="empty">No events match the filter.</div>';
}

function renderChips() {
    document.getElementById('event-filter').innerHTML = LEVELS.map(([k, label]) =>
        `<button class="chip ${level === k ? 'active' : ''}" data-level="${k}">${label}</button>`).join('');
}

/** Conecta los filtros y la búsqueda del historial, y lo repinta con cada evento nuevo. */
export function initEvents() {
    renderChips();
    document.getElementById('event-filter').addEventListener('click', e => {
        const chip = e.target.closest('[data-level]');
        if (!chip) return;
        level = chip.dataset.level;
        renderChips();
        renderAll();
    });
    document.getElementById('event-search').addEventListener('input', e => {
        search = e.target.value.trim();
        renderAll();
    });

    subscribe((state, change) => {
        if (change === 'logs') {
            renderRecent();
            renderAll();
        }
    });
}
