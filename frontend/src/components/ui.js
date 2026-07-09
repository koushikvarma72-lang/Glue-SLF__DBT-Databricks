/**
 * Shared UI primitives — the common "DNA" reused across migration tools.
 *
 * Tools render the same building blocks (form fields, tabs, code artifacts); only
 * the data/config differs. Keeping these here means a new tool reuses the exact
 * look & behaviour instead of re-implementing it.
 */

import { store } from '../store.js';
import { api } from '../api.js';
// notify imports esc from this file; the cycle is safe because both are used at
// runtime (event handlers), long after both modules finish evaluating.
import { notify } from './notify.js';
import { confirmModal } from './modal.js';

export function esc(s) {
  return String(s == null ? '' : s).replace(/[&<>]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

// esc() is only safe for element *text* content. For values placed inside a
// double-quoted HTML attribute we must also escape " (and ') or the first quote
// in the value silently terminates the attribute (corrupting data-* payloads).
export function escAttr(s) {
  return esc(s).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/** A read-only collapsible code block (for review/deploy panes). */
export function codeCard(name, code, badge) {
  return `
    <details style="border:1px solid var(--border);border-radius:8px;margin-bottom:8px">
      <summary style="padding:8px 12px;cursor:pointer;font-size:13px;font-family:monospace">${esc(name)}${badge ? ` <span class="badge" style="font-size:10px">${badge}</span>` : ''}</summary>
      <pre style="margin:0;padding:12px;overflow:auto;background:var(--bg-primary);border-top:1px solid var(--border);font-size:12px;line-height:1.5"><code>${esc(code || '(no code)')}</code></pre>
    </details>`;
}

/** A labelled text/password input. */
export function field(id, label, value, { type = 'text', placeholder = '', hint = '' } = {}) {
  return `
    <label class="ai-settings-field" style="display:flex;flex-direction:column;gap:4px;margin-bottom:10px">
      <span style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.5px">${label}</span>
      <input id="${id}" class="app-input" type="${type}" autocomplete="off" placeholder="${placeholder}"
        value="${String(value || '').replace(/"/g, '&quot;')}" />
      ${hint ? `<span style="font-size:11px;color:var(--text-muted)">${hint}</span>` : ''}
    </label>`;
}

/** A labelled dropdown. ``blankLabel`` is the empty/“none” option text. */
export function selectField(id, label, value, options, { hint = '', blankLabel = '— select —' } = {}) {
  const escAttr = s => String(s || '').replace(/"/g, '&quot;');
  const opts = [...new Set([value, ...options].filter(Boolean))];
  return `
    <label class="ai-settings-field" style="display:flex;flex-direction:column;gap:4px;margin-bottom:10px">
      <span style="font-size:11px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.5px">${label}</span>
      <select id="${id}" class="app-select">
        <option value="">${blankLabel}</option>
        ${opts.map(o => `<option value="${escAttr(o)}" ${o === value ? 'selected' : ''}>${escAttr(o)}</option>`).join('')}
      </select>
      ${hint ? `<span style="font-size:11px;color:var(--text-muted)">${hint}</span>` : ''}
    </label>`;
}

/** A horizontal tab bar. ``tabs`` = [{id, label}]; buttons carry class .ui-tab + data-tab. */
export function renderTabs(tabs, activeId) {
  return tabs.map(({ id, label }) => {
    const active = activeId === id;
    return `<button class="ui-tab" data-tab="${id}" style="appearance:none;background:none;border:none;border-bottom:2px solid ${active ? 'var(--success,#0f766e)' : 'transparent'};color:${active ? 'var(--text-primary)' : 'var(--text-secondary)'};font-weight:${active ? 700 : 500};font-size:13px;padding:8px 4px;cursor:pointer">${label}</button>`;
  }).join('');
}

/**
 * An editable artifact block: view the code, edit it (textarea, persisted), reset,
 * or get an AI explanation. ``akey`` uniquely identifies it; ``edits``/``explains``
 * are the {key: value} maps from state. Wire it with .sfg-artifact / .sfg-code /
 * .sfg-explain / .sfg-reset / .sfg-explanation selectors.
 */
export function codeArtifact(akey, name, code, kind, edits, explains) {
  const current = (edits && akey in edits) ? edits[akey] : code;
  const explanation = explains && explains[akey];
  // Toggle starts in View (read-only); wireArtifacts() flips read-only on Edit.
  const tglOn = 'padding:3px 10px;font-size:11px;border:none;cursor:pointer;background:var(--success,#0f766e);color:#fff';
  const tglOff = 'padding:3px 10px;font-size:11px;border:none;cursor:pointer;background:var(--bg-surface);color:var(--text-secondary)';
  return `
    <details class="sfg-artifact" data-akey="${escAttr(akey)}" data-kind="${escAttr(kind)}" data-name="${escAttr(name)}" style="border:1px solid var(--border);border-radius:8px;margin-bottom:8px">
      <summary style="padding:8px 12px;cursor:pointer;font-size:13px;font-family:monospace">${esc(name)}${(edits && akey in edits) ? ' <span class="badge badge-info" style="font-size:10px">edited</span>' : ''}</summary>
      <div style="border-top:1px solid var(--border);padding:10px 12px">
        <div style="display:flex;gap:8px;margin-bottom:8px;align-items:center">
          <button class="btn btn-secondary sfg-explain" style="padding:3px 10px;font-size:11px"><span aria-hidden="true">💡</span> Explain</button>
          <button class="btn btn-secondary sfg-reset" style="padding:3px 10px;font-size:11px" title="Revert edits"><span aria-hidden="true">↺</span> Reset</button>
          <div class="sfg-toggle" style="margin-left:auto;display:inline-flex;border:1px solid var(--border);border-radius:6px;overflow:hidden">
            <button class="sfg-view" style="${tglOn}"><span aria-hidden="true">👁</span> View</button>
            <button class="sfg-edit" style="${tglOff}"><span aria-hidden="true">✏️</span> Edit</button>
          </div>
        </div>
        <textarea class="sfg-code" spellcheck="false" readonly data-original="${escAttr(code)}"
          style="width:100%;min-height:220px;font-family:monospace;font-size:12px;line-height:1.5;padding:10px;border:1px solid var(--border);border-radius:6px;background:var(--bg-surface);color:var(--text-primary);resize:vertical">${esc(current)}</textarea>
        <div class="sfg-explanation" role="status" aria-live="polite" style="margin-top:8px;font-size:12px;line-height:1.6;white-space:pre-wrap;color:var(--text-secondary);${explanation ? '' : 'display:none'}">${esc(explanation || '')}</div>
      </div>
    </details>`;
}

/**
 * Render a titled group of editable artifacts ({name: code}); keys become "kind:name".
 * The group is a COLLAPSED dropdown (click the header to expand) so a long list — e.g. 14
 * table DDLs — stays a single clean line until opened. ``open`` forces it expanded.
 */
export function artifactGroup(title, obj, kind, edits, explains, { open = false } = {}) {
  const keys = Object.keys(obj || {});
  if (!keys.length) return '';
  return `
    <details class="artifact-group"${open ? ' open' : ''} style="margin:14px 0;border:1px solid var(--border);border-radius:10px;background:var(--bg-surface)">
      <summary style="padding:11px 14px;cursor:pointer;font-size:14px;font-weight:700;color:var(--text-primary)">
        ${esc(title)} <span style="font-size:12px;color:var(--text-muted);font-weight:400">(${keys.length})</span>
      </summary>
      <div style="padding:2px 12px 12px">
        ${keys.map(k => codeArtifact(`${kind}:${k}`, k, obj[k], kind, edits, explains)).join('')}
      </div>
    </details>`;
}

/**
 * Plain-language guidance per review-queue tag, for the NON-TECHNICAL operator who
 * runs the migration. ``plain`` is an operator-readable sentence describing what was
 * flagged; ``action`` is the suggested next step. Engineers only handle the flagged
 * 20%, so the operator needs to know what to confirm vs. what to hand off.
 */
export const REVIEW_TAG_GUIDE = {
  NONDETERMINISTIC: {
    plain: "This step can give different results each run (e.g. it keeps an arbitrary row, or uses the current date/time).",
    action: "Confirm it's safe, or send to an engineer",
  },
  ASSUMPTION: {
    plain: "The tool assumed something it couldn't verify from your data.",
    action: "Check it's correct",
  },
  'MISSING-SCHEMA': {
    plain: "A column or schema this logic needs wasn't available, so it couldn't be fully translated.",
    action: "Provide the schema, or send to an engineer",
  },
  IMPERATIVE: {
    plain: "Row-by-row code with no direct SQL equivalent.",
    action: "Send to an engineer",
  },
  'GLUE-CONSTRUCT': {
    plain: "Uses an AWS Glue-specific feature that needs manual translation.",
    action: "Send to an engineer",
  },
  EXTERNAL: {
    plain: "Calls an outside system or API.",
    action: "Send to an engineer",
  },
  'NEEDS-AI': {
    plain: "This wasn't translated — no AI provider was applied.",
    action: "Configure AI and regenerate",
  },
  TRUNCATED: {
    plain: "The generated output was cut off before it finished.",
    action: "Regenerate",
  },
};

/** Resolve the plain/action guidance for an item; falls back to its raw detail. */
export function reviewItemGuidance(it) {
  const g = REVIEW_TAG_GUIDE[it && it.tag];
  if (g) return { plain: g.plain, action: g.action };
  return { plain: (it && it.detail) || '', action: 'Review' };
}

/** The {name: code} maps inside a conversion that an artifact line can be pulled from. */
function conversionCodeMaps(conv) {
  if (!conv) return {};
  // Later maps win on key collision, but keys are artifact filenames so collisions are rare.
  return {
    ...(conv.notes || {}),
    ...(conv.ddl || {}),
    ...(conv.notebooks || {}),
    ...(conv.dbt_models || {}),
    ...(conv.sources_yml ? { 'sources.yml': conv.sources_yml } : {}),
    ...(conv.schema_yml ? { 'schema.yml': conv.schema_yml } : {}),
    ...(conv.unit_tests_yml ? { 'unit_tests.yml': conv.unit_tests_yml } : {}),
    ...(conv.packages_yml ? { 'packages.yml': conv.packages_yml } : {}),
    ...(conv.governance_md ? { 'GOVERNANCE.md': conv.governance_md } : {}),
  };
}

/** Find the code string for an item's artifact within a conversion (match by name). */
function lookupArtifactCode(conv, artifact) {
  const maps = conversionCodeMaps(conv);
  if (artifact in maps) return maps[artifact];
  // Tolerant match: some keys carry a "kind:name" prefix or differ only by case.
  const want = String(artifact || '').toLowerCase();
  const hit = Object.keys(maps).find(k => {
    const kl = k.toLowerCase();
    return kl === want || kl.split(':').pop() === want;
  });
  return hit != null ? maps[hit] : undefined;
}

/** A ±``radius``-line window of ``code`` around 1-based ``line`` (for handoff snippets). */
function snippetAround(code, line, radius = 6) {
  if (!code || !line) return '';
  const lines = String(code).split('\n');
  const idx = Math.max(1, Math.min(line, lines.length)); // clamp into range
  const from = Math.max(1, idx - radius);
  const to = Math.min(lines.length, idx + radius);
  const out = [];
  for (let n = from; n <= to; n++) {
    const marker = n === idx ? '>' : ' ';
    out.push(`${marker} ${String(n).padStart(4, ' ')} | ${lines[n - 1]}`);
  }
  return out.join('\n');
}

/**
 * Build a Markdown engineer-handoff packet for the review queue. Pure & exported so it
 * is testable/reusable: given the flagged ``items`` and the conversion ``conv`` (which
 * holds the {name: code} maps dbt_models/notebooks/ddl/notes), it returns a Markdown
 * string with a title, a one-line tag summary, and one section per item containing
 * artifact:line, tag, the plain-language description, the technical detail, and — when
 * the artifact code is available — a ±6-line snippet around the flagged line.
 * No DOM, no escaping (Markdown is plain text), no backend call.
 */
export function buildHandoffMarkdown(items, conv) {
  const list = items || [];
  const byTag = {};
  list.forEach(it => { byTag[it.tag] = (byTag[it.tag] || 0) + 1; });
  const summary = Object.entries(byTag).sort((a, b) => b[1] - a[1])
    .map(([tag, n]) => `${n} ${tag}`).join(', ') || 'none';

  const out = [];
  out.push('# Migration review — engineer handoff');
  out.push('');
  out.push(`${list.length} item(s) need human review: ${summary}.`);
  out.push('');
  list.forEach((it, i) => {
    const { plain } = reviewItemGuidance(it);
    const loc = `${it.artifact}${it.line ? ':' + it.line : ''}`;
    out.push(`## ${i + 1}. ${loc}`);
    out.push('');
    out.push(`- **Tag:** ${it.tag}`);
    if (it.kind) out.push(`- **Artifact type:** ${it.kind}`);
    out.push(`- **What this means:** ${plain}`);
    out.push(`- **Technical detail:** ${it.detail || '(none)'}`);
    out.push('');
    const code = lookupArtifactCode(conv, it.artifact);
    const snip = snippetAround(code, it.line, 6);
    if (snip) {
      out.push('```');
      out.push(snip);
      out.push('```');
      out.push('');
    }
  });
  return out.join('\n');
}

/**
 * The human review queue — every construct the converters flagged as untranslatable
 * (the "hard 20%"). ``items`` = [{artifact, kind, tag, line, detail}]. Renders a
 * grouped, collapsible panel; empty → a green "nothing flagged" note. This is half the
 * ship gate ("untranslatable list empty AND reconciliation passes").
 *
 * Each row is written for a NON-TECHNICAL operator: the plain-language sentence and a
 * suggested-action pill are prominent; the original technical ``detail`` is secondary
 * (small/dim). The header carries an "📤 Export for engineer" button — wire it with
 * wireReviewQueue(container, conv).
 */
export function reviewQueuePanel(items, { title = 'Needs human review before shipping' } = {}) {
  const list = items || [];
  if (!list.length) {
    return `<div style="box-sizing:border-box;width:100%;border:1px solid var(--border);border-left:3px solid var(--success,#16a34a);border-radius:10px;background:var(--bg-surface);padding:10px 12px;font-size:12px;color:var(--text-secondary);margin-bottom:12px">
      <span style="color:var(--success,#16a34a);font-weight:600">✓</span> No untranslatable constructs were flagged. Every step was translated — still verify the built tables against the source before shipping.</div>`;
  }
  const byTag = {};
  list.forEach(it => { (byTag[it.tag] = byTag[it.tag] || []).push(it); });
  const tagChips = Object.entries(byTag).sort((a, b) => b[1].length - a[1].length)
    .map(([tag, its]) => `<span class="badge badge-error" style="font-size:10px">${esc(tag)} ${its.length}</span>`).join(' ');
  const rows = list.map(it => {
    const { plain, action } = reviewItemGuidance(it);
    return `
    <div style="box-sizing:border-box;font-size:12px;padding:10px 12px;border-top:1px solid var(--border);display:flex;gap:8px 10px;align-items:baseline;flex-wrap:wrap">
      <span class="badge badge-error" style="font-size:9px;flex-shrink:0">${esc(it.tag)}</span>
      <span style="font-family:monospace;color:var(--text-secondary);flex-shrink:0">${esc(it.artifact)}${it.line ? ':' + esc(it.line) : ''}</span>
      <span style="flex-basis:100%;height:0"></span>
      <span style="color:var(--text-primary);font-weight:600;flex:1 1 240px;min-width:0;overflow-wrap:anywhere">${esc(plain)}</span>
      <span style="margin-left:auto;flex-shrink:0;white-space:normal;max-width:100%;font-size:11px;padding:3px 9px;border-radius:6px;background:var(--bg-surface);color:var(--text-secondary);border:1px solid var(--border)">→ ${esc(action)}</span>
      ${it.detail ? `<span style="flex-basis:100%;color:var(--text-muted);font-size:11px;line-height:1.4;overflow-wrap:anywhere">${esc(it.detail)}</span>` : ''}
    </div>`;
  }).join('');
  return `<details open data-review-queue style="display:block;box-sizing:border-box;width:100%;max-width:100%;border:1px solid var(--border);border-left:3px solid var(--error,#dc2626);border-radius:10px;background:var(--bg-surface);font-size:12px;margin-bottom:12px;overflow:hidden">
    <summary style="box-sizing:border-box;padding:10px 12px;cursor:pointer;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
      <span style="font-weight:700;color:var(--text-primary);font-size:13px">⚠ ${esc(title)}</span>
      <span style="color:var(--text-muted)">— ${list.length} item(s)</span>
      <span style="display:inline-flex;gap:4px;flex-wrap:wrap;min-width:0">${tagChips}</span>
      <button type="button" class="btn btn-secondary review-queue-export" style="margin-left:auto;padding:3px 10px;font-size:11px;font-weight:600;flex-shrink:0" title="Download a Markdown handoff packet for an engineer">📤 Export for engineer</button>
    </summary>
    <div style="background:var(--bg-primary)">${rows}</div>
  </details>`;
}

/**
 * Wire the review queue's "📤 Export for engineer" button inside ``container``: build a
 * Markdown handoff packet from the current queue + ``conv`` and download it client-side
 * (Blob + temporary <a download>). No backend call. ``items`` defaults to
 * ``conv.untranslatable`` (what reviewQueuePanel renders). Safe to call when no queue
 * is present (no-op).
 */
export function wireReviewQueue(container, conv, items) {
  if (!container) return;
  const btn = container.querySelector('.review-queue-export');
  if (!btn) return;
  const list = items || (conv && conv.untranslatable) || [];
  btn.addEventListener('click', (e) => {
    // Inside a <summary>, a click would toggle the <details>; keep it open.
    e.preventDefault();
    e.stopPropagation();
    const md = buildHandoffMarkdown(list, conv);
    const blob = new Blob([md], { type: 'text/markdown' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `migration-review-${list.length}-items.md`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  });
}

/**
 * Render a reconciliation report ([{source, candidate, passed, checks, failures, error}]).
 * The verification gate: shows per-table PASS/FAIL with the row-count, key-integrity and
 * aggregate-fingerprint detail behind a disclosure.
 */
export function reconcileResultsPanel(results) {
  const list = results || [];
  if (!list.length) return '';
  const card = (r) => {
    const ok = r.passed;
    const rc = (r.checks && r.checks.row_counts) || null;
    const head = `${ok ? '✅' : '❌'} <span style="font-family:monospace">${esc(r.source)}</span> → <span style="font-family:monospace">${esc(r.candidate)}</span>`;
    const detail = r.error
      ? `<div style="color:var(--danger,#dc2626);font-size:12px;padding:6px 0">${esc(r.error)}</div>`
      : `
        ${rc ? `<div style="font-size:12px;color:var(--text-secondary)">rows: source ${rc.source} · candidate ${rc.candidate}${rc.delta ? ` (Δ ${rc.delta})` : ''}</div>` : ''}
        ${(r.failures || []).length ? `<ul style="margin:6px 0 0;padding-left:18px;font-size:12px;color:var(--danger,#dc2626)">${r.failures.map(f => `<li>${esc(f)}</li>`).join('')}</ul>` : '<div style="font-size:12px;color:var(--success,#16a34a)">all checks passed</div>'}`;
    return `<details style="border:1px solid var(--border);border-radius:8px;margin-bottom:8px;background:var(--bg-primary)">
      <summary style="padding:8px 12px;cursor:pointer;font-size:13px">${head}</summary>
      <div style="padding:8px 12px;border-top:1px solid var(--border)">${detail}</div>
    </details>`;
  };
  return list.map(card).join('');
}

/**
 * Wire view/edit/reset/explain behaviour for codeArtifact() blocks inside
 * ``container``. Edits persist quietly to sfGlueArtifactEdits (no re-render);
 * Explain calls the AI explain endpoint and caches into sfGlueArtifactExplain.
 * Shared by the Migrate page and the Databricks/DBT Agent pages.
 */
export function wireArtifacts(container) {
  const TGL_ON = 'background:var(--success,#0f766e);color:#fff';
  const TGL_OFF = 'background:var(--bg-surface);color:var(--text-secondary)';
  container.querySelectorAll('.sfg-artifact').forEach(box => {
    const akey = box.dataset.akey;
    const ta = box.querySelector('.sfg-code');
    const explEl = box.querySelector('.sfg-explanation');
    if (!ta) return;

    // View / Edit toggle — View keeps the code read-only (Qlik-style).
    const viewBtn = box.querySelector('.sfg-view');
    const editBtn = box.querySelector('.sfg-edit');
    const setMode = (edit) => {
      ta.readOnly = !edit;
      ta.style.background = edit ? 'var(--bg-primary)' : 'var(--bg-surface)';
      if (viewBtn) { viewBtn.style.cssText += ';' + (edit ? TGL_OFF : TGL_ON); }
      if (editBtn) { editBtn.style.cssText += ';' + (edit ? TGL_ON : TGL_OFF); }
      if (edit) ta.focus();
    };
    viewBtn?.addEventListener('click', () => setMode(false));
    editBtn?.addEventListener('click', () => setMode(true));

    // The 'edited' badge is only in the summary at build time; toggle it live so the
    // persisted-vs-original state is visible without a re-render.
    const summary = box.querySelector('summary');
    const setEditedBadge = (on) => {
      if (!summary) return;
      const has = summary.querySelector('.sfg-edited-badge');
      if (on && !has) {
        const b = document.createElement('span');
        b.className = 'badge badge-info sfg-edited-badge';
        b.style.fontSize = '10px';
        b.textContent = 'edited';
        summary.append(' ', b);
      } else if (!on && has) {
        has.remove();
      }
    };

    ta.addEventListener('input', () => {
      const live = store.get().sfGlueArtifactEdits || (store.get().sfGlueArtifactEdits = {});
      live[akey] = ta.value;
      setEditedBadge(ta.value !== (ta.dataset.original || ''));
    });
    box.querySelector('.sfg-reset')?.addEventListener('click', async () => {
      const live = store.get().sfGlueArtifactEdits || {};
      const hasEdit = akey in live || ta.value !== (ta.dataset.original || '');
      if (!hasEdit) return;
      if (!(await confirmModal('Discard your edits and restore the original code?',
        { title: 'Reset artifact', confirmLabel: 'Discard', danger: true }))) return;
      ta.value = ta.dataset.original || '';
      delete live[akey];
      setEditedBadge(false);
      notify('Reverted to original', { kind: 'info' });
    });
    box.querySelector('.sfg-explain')?.addEventListener('click', async (e) => {
      const btn = e.currentTarget;
      btn.disabled = true; btn.innerHTML = '<span aria-hidden="true">💡</span> Explaining…';
      if (editBtn) editBtn.disabled = true; // stale-snapshot guard while it runs
      explEl.style.display = 'block';
      explEl.textContent = 'Thinking…';
      try {
        const res = await api.explainSnowflakeGlueArtifact({ name: box.dataset.name, code: ta.value, kind: box.dataset.kind });
        explEl.textContent = res.text || '(no explanation)';
        (store.get().sfGlueArtifactExplain || (store.get().sfGlueArtifactExplain = {}))[akey] = res.text || '';
      } catch (err) {
        explEl.textContent = "Couldn't generate an explanation. Check your AI provider settings and try again.";
        notify("Couldn't generate an explanation. Check your AI provider settings and try again.",
          { kind: 'error', title: err && err.message ? err.message : 'Explain failed' });
      } finally {
        btn.disabled = false; btn.innerHTML = '<span aria-hidden="true">💡</span> Explain';
        if (editBtn) editBtn.disabled = false;
      }
    });
  });
}
