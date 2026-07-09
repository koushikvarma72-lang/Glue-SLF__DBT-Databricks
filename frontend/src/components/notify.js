/**
 * Toast notifications — transient, stacked, auto-dismissing alerts for action outcomes.
 *
 * The migration flow's "alerts" (reconcile pass/fail, build complete, deploy, precheck,
 * conversion ready) previously surfaced only as inline boxes you had to be looking at the
 * right panel to notice. notify() also pops them as corner toasts so a long-running step's
 * result is visible even after you've scrolled away or switched focus.
 *
 * Zero dependencies, self-contained: lazily mounts ONE fixed container on <body> and
 * appends a card per call. Uses the app's existing CSS variables (with literal fallbacks)
 * so it matches the theme without importing any stylesheet.
 *
 *   notify('1/1 tables match the source.', { kind: 'success', title: 'Reconcile passed' });
 *   notify(msg, { kind: 'error' });                 // longer default timeout for errors
 *   notify(msg, { kind: 'info', timeout: 0 });      // 0 = sticky until dismissed
 */
import { esc } from './ui.js';

const KINDS = {
  success: { icon: '✅', accent: 'var(--success,#16a34a)' },
  error:   { icon: '❌', accent: 'var(--danger,#dc2626)' },
  warning: { icon: '⚠️', accent: 'var(--warning,#d97706)' },
  info:    { icon: 'ℹ️', accent: 'var(--accent,#2563eb)' },
};

const MAX_VISIBLE = 5;          // cap the stack; oldest is dropped past this
const DEFAULT_TIMEOUT = 6000;   // success/info auto-dismiss
const ERROR_TIMEOUT = 11000;    // errors linger longer (you triggered them, don't miss them)

let container = null;

function ensureContainer() {
  if (container && document.body.contains(container)) return container;
  container = document.createElement('div');
  container.setAttribute('data-toast-container', '');
  container.setAttribute('aria-live', 'polite');   // announce toasts as they append
  container.setAttribute('aria-atomic', 'false');
  container.style.cssText = [
    'position:fixed', 'top:16px', 'right:16px', 'z-index:99999',
    'display:flex', 'flex-direction:column', 'gap:10px',
    'max-width:min(400px,calc(100vw - 32px))', 'pointer-events:none',
  ].join(';');
  document.body.appendChild(container);
  return container;
}

function dismiss(card) {
  if (!card || card.dataset.leaving) return;
  card.dataset.leaving = '1';
  card.style.opacity = '0';
  card.style.transform = 'translateX(12px)';
  setTimeout(() => card.remove(), 200);
}

/**
 * Show a toast. ``message`` is the body line; ``opts.title`` (optional) is a bold lead.
 * ``opts.kind`` ∈ {success, error, warning, info}. ``opts.timeout`` ms — 0 means sticky;
 * omit to use the kind's default (errors linger longer). Returns the card element.
 */
export function notify(message, { kind = 'info', title = '', timeout } = {}) {
  const k = KINDS[kind] || KINDS.info;
  const root = ensureContainer();

  while (root.children.length >= MAX_VISIBLE) dismiss(root.firstElementChild);

  const card = document.createElement('div');
  // Errors/warnings are assertive (interrupt), success/info are polite.
  card.setAttribute('role', (kind === 'error' || kind === 'warning') ? 'alert' : 'status');
  card.style.cssText = [
    'pointer-events:auto', 'box-sizing:border-box', 'width:100%',
    'background:var(--bg-surface,#ffffff)', 'color:var(--text-primary,#111827)',
    'border:1px solid var(--border,#e5e7eb)', `border-left:4px solid ${k.accent}`,
    'border-radius:8px', 'padding:10px 12px', 'font-size:12px', 'line-height:1.45',
    'box-shadow:0 6px 24px rgba(0,0,0,.18)',
    'display:flex', 'gap:9px', 'align-items:flex-start',
    'opacity:0', 'transform:translateX(12px)', 'transition:opacity .2s ease,transform .2s ease',
  ].join(';');

  const body = document.createElement('div');
  body.style.cssText = 'flex:1 1 auto;min-width:0;overflow-wrap:anywhere';
  body.innerHTML =
    `<span style="margin-right:6px" aria-hidden="true">${k.icon}</span>` +
    (title ? `<strong>${esc(title)}</strong><br>` : '') +
    `<span>${esc(message)}</span>`;

  const close = document.createElement('button');
  close.type = 'button';
  close.textContent = '✕';
  close.setAttribute('aria-label', 'Dismiss notification');
  close.style.cssText = [
    'flex-shrink:0', 'background:none', 'border:none', 'cursor:pointer',
    'color:var(--text-muted,#9ca3af)', 'font-size:13px', 'line-height:1', 'padding:2px 0 0',
  ].join(';');
  close.addEventListener('click', () => dismiss(card));

  card.appendChild(body);
  card.appendChild(close);
  root.appendChild(card);

  // Enter animation on the next frame (so the transition runs from the initial state).
  requestAnimationFrame(() => { card.style.opacity = '1'; card.style.transform = 'translateX(0)'; });

  const ms = timeout === undefined ? (kind === 'error' ? ERROR_TIMEOUT : DEFAULT_TIMEOUT) : timeout;
  if (ms > 0) setTimeout(() => dismiss(card), ms);
  return card;
}
