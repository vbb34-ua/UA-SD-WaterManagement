/** @file Utilidades de interfaz compartidas: escape de HTML, etiquetas de estado y avisos. */

import { STATUS } from './store.js';

/** @returns {string} el valor como texto seguro para insertarlo en HTML */
export function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, c =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

/** @returns {string} HTML de la etiqueta de un estado, con su color */
export function statusPill(estado, extraClass = '') {
    const label = STATUS[estado]?.label ?? estado;
    return `<span class="pill s-${esc(estado)} ${extraClass}"><span class="dot s-${esc(estado)}"></span>${esc(label)}</span>`;
}

/** @returns {string} HTML del punto de color de un estado */
export function dot(estado) {
    return `<span class="dot s-${esc(estado)}"></span>`;
}

/** Muestra un aviso temporal abajo en el centro (los errores duran más). */
export function toast(message, isError = false) {
    const box = document.getElementById('toasts');
    const el = document.createElement('div');
    el.className = 'toast' + (isError ? ' error' : '');
    el.textContent = message;
    box.appendChild(el);
    setTimeout(() => el.remove(), isError ? 6000 : 3500);
}

/**
 * Muestra el resultado de una llamada a la API.
 * @param {string} result respuesta de CENTRAL: "OK" o el motivo del error
 * @param {string} okMessage aviso que se muestra si la respuesta es "OK"
 */
export function showResult(result, okMessage) {
    if (result === 'OK') toast(okMessage);
    else toast(result, true);
}
