// =====================================================================
// ui.js - Utilidades de interfaz compartidas (escape HTML, avisos, pills).
// =====================================================================

import { STATUS } from './store.js';

export function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, c =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

/** Etiqueta de estado con su color. */
export function statusPill(estado, extraClass = '') {
    const label = STATUS[estado]?.label ?? estado;
    return `<span class="pill s-${esc(estado)} ${extraClass}"><span class="dot s-${esc(estado)}"></span>${esc(label)}</span>`;
}

export function dot(estado) {
    return `<span class="dot s-${esc(estado)}"></span>`;
}

/** Aviso temporal abajo en el centro. */
export function toast(message, isError = false) {
    const box = document.getElementById('toasts');
    const el = document.createElement('div');
    el.className = 'toast' + (isError ? ' error' : '');
    el.textContent = message;
    box.appendChild(el);
    setTimeout(() => el.remove(), isError ? 6000 : 3500);
}

/** Muestra el resultado de una llamada a la API ("OK" o un error). */
export function showResult(result, okMessage) {
    if (result === 'OK') toast(okMessage);
    else toast(result, true);
}
