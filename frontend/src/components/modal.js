/**
 * In-app confirmation dialog — a themed replacement for the native window.confirm()
 * ("localhost:5173 says…"), which looks foreign and can't be styled.
 *
 * Promise-based so it drops into the existing async handlers:
 *
 *   if (!(await confirmModal('This runs CREATE TABLE…', { title: 'Deploy 3 tables?',
 *                                                         confirmLabel: 'Deploy' }))) return;
 *
 * Resolves true on confirm, false on cancel / overlay-click / Escape. Enter confirms.
 * Self-contained: builds DOM with textContent (no injection), themed via the app's CSS
 * variables with literal fallbacks, removes itself on close.
 */

const PRIMARY = 'var(--accent,#2563eb)';
const DANGER = 'var(--danger,#dc2626)';

export function confirmModal(message, {
  title = 'Confirm', confirmLabel = 'OK', cancelLabel = 'Cancel', danger = false,
} = {}) {
  return new Promise((resolve) => {
    const opener = document.activeElement; // restore focus here on close
    const overlay = document.createElement('div');
    overlay.setAttribute('data-confirm-overlay', '');
    overlay.style.cssText = [
      'position:fixed', 'inset:0', 'z-index:100000', 'display:flex',
      'align-items:center', 'justify-content:center', 'padding:20px', 'box-sizing:border-box',
      'background:rgba(0,0,0,.45)',
    ].join(';');

    const card = document.createElement('div');
    card.setAttribute('role', 'dialog');
    card.setAttribute('aria-modal', 'true');
    card.style.cssText = [
      'background:var(--bg-surface,#ffffff)', 'color:var(--text-primary,#111827)',
      'border:1px solid var(--border,#e5e7eb)', 'border-radius:12px',
      'box-shadow:0 16px 48px rgba(0,0,0,.32)', 'max-width:460px', 'width:100%',
      'padding:20px', 'box-sizing:border-box', 'font-size:13px', 'line-height:1.5',
    ].join(';');

    const titleEl = document.createElement('div');
    titleEl.textContent = title;
    titleEl.style.cssText = 'font-weight:700;font-size:15px;margin-bottom:8px';

    const msgEl = document.createElement('div');
    msgEl.textContent = message || '';
    msgEl.style.cssText = 'color:var(--text-secondary,#4b5563);white-space:pre-wrap;margin-bottom:18px';

    const btnRow = document.createElement('div');
    btnRow.style.cssText = 'display:flex;gap:10px;justify-content:flex-end';

    const cancelBtn = document.createElement('button');
    cancelBtn.type = 'button';
    cancelBtn.textContent = cancelLabel;
    cancelBtn.style.cssText = [
      'padding:7px 16px', 'font-size:13px', 'cursor:pointer', 'border-radius:7px',
      'border:1px solid var(--border,#e5e7eb)', 'background:var(--bg-primary,#f9fafb)',
      'color:var(--text-secondary,#4b5563)',
    ].join(';');

    const confirmBtn = document.createElement('button');
    confirmBtn.type = 'button';
    confirmBtn.textContent = confirmLabel;
    confirmBtn.style.cssText = [
      'padding:7px 16px', 'font-size:13px', 'font-weight:600', 'cursor:pointer',
      'border-radius:7px', 'border:1px solid transparent', 'color:#fff',
      `background:${danger ? DANGER : PRIMARY}`,
    ].join(';');

    function close(val) {
      document.removeEventListener('keydown', onKey);
      overlay.remove();
      if (opener && typeof opener.focus === 'function') opener.focus(); // restore focus
      resolve(val);
    }
    function onKey(e) {
      if (e.key === 'Escape') { e.preventDefault(); close(false); }
      // Enter confirms only for non-destructive dialogs — never auto-confirm a danger action.
      else if (e.key === 'Enter' && !danger) { e.preventDefault(); close(true); }
      else if (e.key === 'Tab') {
        // Trap focus between the two buttons.
        e.preventDefault();
        const order = [cancelBtn, confirmBtn];
        const i = order.indexOf(document.activeElement);
        const next = e.shiftKey ? (i <= 0 ? order.length - 1 : i - 1) : (i === order.length - 1 ? 0 : i + 1);
        order[next].focus();
      }
    }
    cancelBtn.addEventListener('click', () => close(false));
    confirmBtn.addEventListener('click', () => close(true));
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(false); });
    document.addEventListener('keydown', onKey);

    btnRow.append(cancelBtn, confirmBtn);
    card.append(titleEl, msgEl, btnRow);
    overlay.appendChild(card);
    document.body.appendChild(overlay);
    // Danger dialogs default focus to Cancel so a stray Enter/click can't destroy.
    (danger ? cancelBtn : confirmBtn).focus();
  });
}
