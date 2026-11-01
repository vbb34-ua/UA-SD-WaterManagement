// =====================================================================
// app.js - Punto de entrada: pestañas, cabecera (contadores y conexión)
// e inicialización de todos los módulos.
// =====================================================================

import { STATUS, connect, subscribe, stationList } from './store.js';
import { initMap } from './map.js';
import { initStations, setStatusFilter } from './stations.js';
import { initDetail } from './detail.js';
import { initControl } from './control.js';
import { initEvents } from './events.js';
import { esc } from './ui.js';

const VIEWS = ['monitoring', 'stations', 'control', 'events'];
let currentView = 'monitoring';
let unseenAlerts = 0;

// ---------------------------------------------------------------- PESTAÑAS

function showView(view) {
    if (!VIEWS.includes(view)) view = 'monitoring';
    currentView = view;
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.view === view));
    document.querySelectorAll('.view').forEach(v => v.classList.toggle('active', v.id === 'view-' + view));
    if (view === 'events') {
        unseenAlerts = 0;
        renderBadge();
    }
    if (location.hash !== '#' + view) history.replaceState(null, '', '#' + view);
}

function renderBadge() {
    const badge = document.getElementById('events-badge');
    badge.hidden = unseenAlerts === 0;
    badge.textContent = unseenAlerts > 99 ? '99+' : String(unseenAlerts);
}

// ---------------------------------------------------------------- CABECERA

function renderKpis() {
    const counts = {};
    stationList().forEach(ws => { counts[ws.estado] = (counts[ws.estado] || 0) + 1; });
    document.getElementById('kpis').innerHTML = Object.entries(STATUS).map(([k, s]) =>
        `<button class="kpi" data-status="${k}" title="Show ${esc(s.label)} stations">
            <span class="dot s-${k}"></span><strong>${counts[k] || 0}</strong>${esc(s.label)}
        </button>`).join('');
}

function renderConnection(connected) {
    const conn = document.getElementById('conn');
    conn.classList.toggle('ok', connected);
    conn.classList.toggle('down', !connected);
    conn.querySelector('.conn-text').textContent = connected ? 'Live' : 'Disconnected';
    document.getElementById('banner').hidden = connected;
}

// ---------------------------------------------------------------- INIT

document.addEventListener('DOMContentLoaded', () => {
    initMap();
    initStations();
    initDetail();
    initControl();
    initEvents();

    document.querySelectorAll('.tab').forEach(t => t.addEventListener('click', () => showView(t.dataset.view)));
    window.addEventListener('hashchange', () => showView(location.hash.slice(1)));

    // Contador de la cabecera -> tabla filtrada por ese estado
    document.getElementById('kpis').addEventListener('click', e => {
        const kpi = e.target.closest('[data-status]');
        if (!kpi) return;
        setStatusFilter(kpi.dataset.status);
        showView('stations');
    });

    subscribe((state, change, log) => {
        if (change === 'stations') renderKpis();
        if (change === 'connection') renderConnection(state.connected);
        if (change === 'logs' && log && log.level === 'danger' && currentView !== 'events') {
            unseenAlerts++;
            renderBadge();
        }
    });

    renderKpis();
    showView(location.hash.slice(1) || 'monitoring');
    connect();
});
