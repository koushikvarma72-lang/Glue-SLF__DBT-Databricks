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

/**
 * In-app prompt dialog — a themed replacement for the native window.prompt(), which
 * looks foreign, can't be styled, and only supports a single free-text field.
 *
 * Promise-based, mirroring confirmModal's overlay/shell/focus-trap/Escape mechanics:
 *
 *   const res = await promptModal({
 *     title: 'AWS SSO', message: 'Pick an account',
 *     fields: [{ id: 'account', label: 'Account', type: 'select', options: [...] }],
 *     confirmLabel: 'Continue',
 *   });
 *   if (!res) return;            // cancel / Escape / overlay-click
 *   const { account } = res;     // one entry per field id
 *
 * `fields` = [{ id, label, placeholder, value, type }] where type is
 * 'text' | 'password' | 'select'. For 'select', pass `options` as an array of
 * strings or { value, label } objects. Resolves { [id]: value } on confirm, or
 * null on cancel / overlay-click / Escape. Enter submits. Self-contained,
 * builds DOM with textContent (no injection), themed via the app's CSS variables.
 */
export function promptModal({
  title = 'Enter details', message = '', fields = [],
  confirmLabel = 'OK', cancelLabel = 'Cancel',
} = {}) {
  return new Promise((resolve) => {
    const opener = document.activeElement; // restore focus here on close
    const overlay = document.createElement('div');
    overlay.setAttribute('data-prompt-overlay', '');
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
    if (message) {
      msgEl.textContent = message;
      msgEl.style.cssText = 'color:var(--text-secondary,#4b5563);white-space:pre-wrap;margin-bottom:16px';
    }

    const form = document.createElement('form');
    form.style.cssText = 'display:flex;flex-direction:column;gap:12px;margin-bottom:18px';

    const inputStyle = [
      'width:100%', 'box-sizing:border-box', 'padding:7px 10px', 'font-size:13px',
      'border-radius:7px', 'border:1px solid var(--border,#e5e7eb)',
      'background:var(--bg-primary,#f9fafb)', 'color:var(--text-primary,#111827)',
    ].join(';');

    const inputs = {}; // id -> element
    fields.forEach((f, idx) => {
      const row = document.createElement('label');
      row.style.cssText = 'display:flex;flex-direction:column;gap:5px';

      if (f.label) {
        const lbl = document.createElement('span');
        lbl.textContent = f.label;
        lbl.style.cssText = 'font-weight:600;font-size:12px;color:var(--text-secondary,#4b5563)';
        row.appendChild(lbl);
      }

      let input;
      if (f.type === 'select') {
        input = document.createElement('select');
        input.style.cssText = inputStyle;
        (f.options || []).forEach((opt) => {
          const o = document.createElement('option');
          if (opt && typeof opt === 'object') { o.value = String(opt.value); o.textContent = opt.label; }
          else { o.value = String(opt); o.textContent = String(opt); }
          input.appendChild(o);
        });
        if (f.value != null) input.value = String(f.value);
      } else {
        input = document.createElement('input');
        input.type = f.type === 'password' ? 'password' : 'text';
        input.style.cssText = inputStyle;
        if (f.placeholder) input.placeholder = f.placeholder;
        if (f.value != null) input.value = String(f.value);
      }
      input.id = `prompt-field-${f.id}`;
      inputs[f.id] = input;
      row.appendChild(input);
      form.appendChild(row);
      if (idx === 0) input.setAttribute('data-autofocus', '');
    });

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
    confirmBtn.type = 'submit';
    confirmBtn.textContent = confirmLabel;
    confirmBtn.style.cssText = [
      'padding:7px 16px', 'font-size:13px', 'font-weight:600', 'cursor:pointer',
      'border-radius:7px', 'border:1px solid transparent', 'color:#fff',
      `background:${PRIMARY}`,
    ].join(';');

    function collect() {
      const out = {};
      Object.keys(inputs).forEach((id) => { out[id] = inputs[id].value; });
      return out;
    }
    function close(val) {
      document.removeEventListener('keydown', onKey);
      overlay.remove();
      if (opener && typeof opener.focus === 'function') opener.focus(); // restore focus
      resolve(val);
    }
    function submit() { close(collect()); }
    function focusable() {
      return [...Object.values(inputs), cancelBtn, confirmBtn];
    }
    function onKey(e) {
      if (e.key === 'Escape') { e.preventDefault(); close(null); }
      else if (e.key === 'Tab') {
        // Trap focus among the fields + buttons.
        e.preventDefault();
        const order = focusable();
        const i = order.indexOf(document.activeElement);
        const next = e.shiftKey ? (i <= 0 ? order.length - 1 : i - 1) : (i === order.length - 1 ? 0 : i + 1);
        order[next].focus();
      }
    }
    form.addEventListener('submit', (e) => { e.preventDefault(); submit(); });
    cancelBtn.addEventListener('click', () => close(null));
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(null); });
    document.addEventListener('keydown', onKey);

    btnRow.append(cancelBtn, confirmBtn);
    if (message) card.append(titleEl, msgEl, form, btnRow);
    else card.append(titleEl, form, btnRow);
    overlay.appendChild(card);
    document.body.appendChild(overlay);
    // Autofocus the first field (fall back to the confirm button when there are none).
    const first = card.querySelector('[data-autofocus]') || confirmBtn;
    first.focus();
  });
}
