/** @file Mapa de la ciudad: zonas y estaciones colocadas según su ubicación. */

import { STATUS, subscribe, getState, stationList, select } from './store.js';
import { esc } from './ui.js';

/**
 * Zonas conocidas del mapa.
 * cx/cy: centro como fracción del ancho/alto del mapa. r: radio como fracción del lado menor.
 * aliases: nombres de ubicación que corresponden a la zona, en inglés y en español
 * (se comparan sin tildes, mayúsculas ni espacios).
 */
export const ZONES = [
    { key: 'river-park',        name: 'River Park',        cx: 0.14, cy: 0.30, r: 0.22,
      aliases: ['river park', 'parque del rio', 'parque rio'] },
    { key: 'central-park',      name: 'Central Park',      cx: 0.40, cy: 0.64, r: 0.27,
      aliases: ['central park', 'parque central'] },
    { key: 'riverside-gardens', name: 'Riverside Gardens', cx: 0.68, cy: 0.30, r: 0.24,
      aliases: ['riverside gardens', 'jardines del rio', 'jardines rio', 'jardines de la ribera'] },
    { key: 'north-roundabout',  name: 'North Roundabout',  cx: 0.87, cy: 0.72, r: 0.21,
      aliases: ['north roundabout', 'rotonda norte'] },
];

/** Nombres de ubicación sugeridos al dar de alta una estación. */
export const LOCATION_SUGGESTIONS = ['Parque Central', 'Parque del Río', 'Jardines del Río', 'Rotonda Norte'];

/** Huecos libres del mapa para ubicaciones que no son ninguna de las zonas conocidas. */
const EXTRA_SLOTS = [
    { cx: 0.10, cy: 0.80 }, { cx: 0.64, cy: 0.84 }, { cx: 0.42, cy: 0.16 }, { cx: 0.93, cy: 0.20 },
];
const EXTRA_RADIUS = 0.15;
/** Separación vertical (px) entre las estaciones de una misma zona. */
const ROW_HEIGHT = 22;

/** Zonas creadas para ubicaciones desconocidas, por ubicación normalizada. */
const extraZones = {};
let mapArea;

/** @returns {string} el texto sin tildes, en minúsculas y solo con letras y números */
function normalize(text) {
    return (text || '').normalize('NFD').replace(/[̀-ͯ]/g, '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

const ZONE_BY_ALIAS = {};
ZONES.forEach(z => z.aliases.forEach(a => { ZONE_BY_ALIAS[normalize(a)] = z; }));

/**
 * Busca la zona de una ubicación: por nombre exacto, por nombre contenido (p. ej.
 * "Central Park - Sector B") o, si no es ninguna conocida, una zona extra con su nombre.
 */
function zoneFor(location) {
    const loc = normalize(location);
    if (ZONE_BY_ALIAS[loc]) return ZONE_BY_ALIAS[loc];
    for (const alias in ZONE_BY_ALIAS) {
        if (loc.includes(alias)) return ZONE_BY_ALIAS[alias];
    }
    const key = loc || 'unassigned';
    if (!extraZones[key]) {
        const i = Object.keys(extraZones).length;
        const slot = EXTRA_SLOTS[i % EXTRA_SLOTS.length];
        const name = (!loc || loc === 'sinubicacion' || loc === 'desconocida') ? 'Unassigned' : location;
        extraZones[key] = { key: 'extra-' + i, name, cx: slot.cx, cy: slot.cy, r: EXTRA_RADIUS };
    }
    return extraZones[key];
}

// ---------------------------------------------------------------- RENDER

/** Crea o actualiza el marcador de una estación: su id y, si está regando, el caudal en directo. */
function renderStation(ws) {
    let el = document.getElementById('st-' + ws.id);
    if (!el) {
        el = document.createElement('div');
        el.id = 'st-' + ws.id;
        el.className = 'station';
        el.addEventListener('click', () => select(ws.id));
        mapArea.appendChild(el);
    }
    const watering = ws.estado === 'REGANDO';
    el.title = `${ws.id} · ${ws.location} · ${STATUS[ws.estado]?.label ?? ws.estado}`;
    el.classList.toggle('selected', getState().selected === ws.id);
    el.innerHTML = `
        <span class="dot-wrap">
            ${watering ? '<span class="ripple"></span><span class="ripple"></span>' : ''}
            <span class="dot s-${esc(ws.estado)}"></span>
        </span>
        <span class="station-id">${esc(ws.id)}</span>
        ${watering ? `<span class="station-extra">${esc(ws.caudal)} L/min</span>` : ''}`;
}

/** Dibuja las zonas y coloca cada estación dentro de la zona de su ubicación. */
function layout() {
    const W = mapArea.clientWidth;
    const H = mapArea.clientHeight;
    if (!W || !H) return;
    const side = Math.min(W, H);
    const stations = stationList();

    // Quitar marcadores de estaciones que ya no existen
    const ids = new Set(stations.map(s => 'st-' + s.id));
    mapArea.querySelectorAll('.station').forEach(el => { if (!ids.has(el.id)) el.remove(); });

    // Estaciones agrupadas por zona
    const groups = {};
    stations.forEach(ws => {
        const z = zoneFor(ws.location);
        (groups[z.key] = groups[z.key] || { zone: z, list: [] }).list.push(ws);
        renderStation(ws);
    });

    const zones = ZONES.concat(Object.values(extraZones).filter(z => groups[z.key]));
    zones.forEach(z => {
        let circle = document.getElementById('zone-' + z.key);
        if (!circle) {
            circle = document.createElement('div');
            circle.id = 'zone-' + z.key;
            circle.className = 'zone';
            circle.innerHTML = '<div class="zone-label"></div>';
            mapArea.prepend(circle);
        }
        // El radio se reduce si el círculo se saldría del mapa
        const cx = z.cx * W, cy = z.cy * H;
        const R = Math.max(40, Math.min(z.r * side, cx - 8, W - cx - 8, cy - 8, H - cy - 8));
        z._R = R;
        circle.querySelector('.zone-label').textContent = z.name;
        circle.style.width = circle.style.height = 2 * R + 'px';
        circle.style.left = cx - R + 'px';
        circle.style.top = cy - R + 'px';
        circle.classList.toggle('busy', !!groups[z.key]);
    });

    // Estaciones en columna, centradas en su zona, para que sus etiquetas no se solapen
    Object.values(groups).forEach(({ zone, list }) => {
        const R = zone._R;
        list.forEach((ws, i) => {
            const el = document.getElementById('st-' + ws.id);
            el.style.left = zone.cx * W - R * 0.35 + 'px';
            el.style.top = zone.cy * H + R * 0.12 + (i - (list.length - 1) / 2) * ROW_HEIGHT + 'px';
        });
    });
}

function renderLegend() {
    document.getElementById('legend').innerHTML = Object.entries(STATUS)
        .map(([key, s]) => `<span><span class="dot s-${key}"></span>${s.label}${key === 'REGANDO' ? ' (blinking)' : ''}</span>`)
        .join('');
}

/** Dibuja la leyenda y mantiene el mapa al día con el estado de la red. */
export function initMap() {
    mapArea = document.getElementById('map-area');
    renderLegend();

    subscribe((state, change, id) => {
        if (change === 'stations') layout();
        else if (change === 'telemetry' && state.stations[id]) renderStation(state.stations[id]);
        else if (change === 'selection') stationList().forEach(renderStation);
    });

    // Se recoloca al cambiar el tamaño de la ventana o al volver a la pestaña del mapa
    new ResizeObserver(layout).observe(mapArea);
}
