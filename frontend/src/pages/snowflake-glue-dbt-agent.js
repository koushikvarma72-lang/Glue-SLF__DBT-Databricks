/**
 * Snowflake/Glue → Databricks/DBT — DBT Agent.
 *
 * The deploy-review for the dbt side of the conversion: the staging/silver/gold
 * models, sources.yml, and the generated schema.yml model tests. This is also the
 * SHIP GATE — it surfaces the untranslatable review queue and runs the cross-engine
 * reconciliation (built Databricks tables vs the legacy Snowflake source). Nothing
 * ships until the review queue is empty AND reconciliation passes.
 */
import { api } from '../api.js';
import { store } from '../store.js';
import {
  esc, artifactGroup, codeArtifact, wireArtifacts,
  reviewQueuePanel, reconcileResultsPanel, wireReviewQueue,
} from '../components/ui.js';
import { notify } from '../components/notify.js';
import { confirmModal } from '../components/modal.js';

// Base table name (last dotted segment), lowercased — for matching keys to tables.
const baseName = s => String(s || '').split('.').pop().toLowerCase();

// ── Local dbt-Core run of the converted models (real `dbt build`, no git/Cloud) ──
// Module-level so an in-flight run survives the page's full re-renders (store.set).
let _sfgRun = { jobId: null, running: false, finished: false, status: '', summary: '',
                error: '', logs: '', logOffset: 0, models: [], cancelling: false };
let _sfgPollTimer = null;

// Models the user sees, with editor edits applied (same key convention as Build:
// "dbt model:<name>"; sources.yml under its own artifact key). Source-agnostic.
function sfgModelsWithEdits() {
  const conv = store.get().sfGlueConversion || {};
  const editsNow = store.get().sfGlueArtifactEdits || {};
  const models = {};
  Object.entries(conv.dbt_models || {}).forEach(([k, v]) => {
    models[k] = (`dbt model:${k}` in editsNow) ? editsNow[`dbt model:${k}`] : v;
  });
  const srcKey = 'sources_yml:sources.yml';
  const sources_yml = (srcKey in editsNow) ? editsNow[srcKey] : (conv.sources_yml || '');
  return { models, sources_yml };
}

// Full artifact set with the user's Review edits applied, for the dbt-project export.
// Source-agnostic: spreads whatever the conversion produced and overlays edited versions.
function sfgArtifactsForExport() {
  const conv = store.get().sfGlueConversion || {};
  const e = store.get().sfGlueArtifactEdits || {};
  const pick = (k, v) => (k in e ? e[k] : v);
  const dbt_models = {};
  Object.entries(conv.dbt_models || {}).forEach(([k, v]) => {
    dbt_models[k] = (`dbt model:${k}` in e) ? e[`dbt model:${k}`] : v;
  });
  return {
    ...conv,
    dbt_models,
    sources_yml: pick('sources_yml:sources.yml', conv.sources_yml),
    schema_yml: pick('schema_yml:schema.yml', conv.schema_yml),
    unit_tests_yml: pick('unit_tests_yml:unit_tests.yml', conv.unit_tests_yml),
    packages_yml: pick('packages_yml:packages.yml', conv.packages_yml),
    governance_md: pick('governance_md:GOVERNANCE.md', conv.governance_md),
  };
}

// Download the full runnable dbt project (.zip) built by /api/sfglue/export.
async function exportSfgDbtProject(container) {
  const conv = store.get().sfGlueConversion || {};
  if (!Object.keys(conv.dbt_models || {}).length && !conv.sources_yml) {
    notify('Generate the conversion first.', { kind: 'warning', title: 'Nothing to export' });
    return;
  }
  const btn = container.querySelector('#sfg-dbt-export');
  const prev = btn ? btn.textContent : '';
  if (btn) { btn.disabled = true; btn.textContent = 'Exporting…'; }
  try {
    const dest = store.get().sfGlueDestination || {};
    const projectName = dest.catalog
      ? `${String(dest.catalog).replace(/[^A-Za-z0-9_]/g, '_')}_dbt` : 'sfglue_migration';
    const blob = await api.exportSnowflakeGlueDbtProject({
      artifacts: sfgArtifactsForExport(), destination: dest, projectName,
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = `${projectName}.zip`;
    document.body.appendChild(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
    notify('Runnable dbt project downloaded.', { kind: 'success', title: 'Export ready' });
  } catch (err) {
    notify(String((err && err.message) || err), { kind: 'error', title: 'Export failed' });
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = prev || '⤓ Export dbt project (.zip)'; }
  }
}

function _sfgIsOk(status) { return /^(success|pass)$/i.test(status || ''); }

function renderSfgRunView() {
  const btn = document.getElementById('sfg-dbt-run');
  const stop = document.getElementById('sfg-dbt-cancel');
  const statusEl = document.getElementById('sfg-dbt-status');
  const modelsEl = document.getElementById('sfg-dbt-models');
  const log = document.getElementById('sfg-dbt-log');
  const dest = store.get().sfGlueDestination || {};
  // Databricks Agent stores the PAT under `token`; accept either key.
  const ready = !!(dest.workspace_url && dest.sql_warehouse_id && (dest.token || dest.personal_access_token));

  if (btn) {
    btn.disabled = _sfgRun.running || !ready;
    btn.textContent = _sfgRun.running ? 'Running…' : (_sfgRun.finished ? '▶ Re-run with dbt' : '▶ Run with dbt');
  }
  if (stop) {
    stop.style.display = _sfgRun.running ? '' : 'none';
    stop.disabled = !!_sfgRun.cancelling;
    stop.textContent = _sfgRun.cancelling ? 'Stopping…' : 'Stop';
  }
  if (statusEl) {
    let html = '';
    if (_sfgRun.error) html = `<span style="color:var(--danger,#dc2626)">⚠ ${esc(_sfgRun.error)}</span>`;
    else if (_sfgRun.running) html = `<span style="color:var(--text-secondary)">${_sfgRun.cancelling ? 'Stopping…' : 'Running dbt (debug → build)…'}</span>`;
    else if (_sfgRun.status === 'success') html = `<span style="color:var(--success,#16a34a)">✅ ${esc(_sfgRun.summary || 'dbt build completed.')}</span>`;
    else if (_sfgRun.status === 'cancelled') html = `<span style="color:var(--text-muted)">Cancelled before finishing.</span>`;
    else if (_sfgRun.status === 'error') html = `<span style="color:var(--danger,#dc2626)">❌ ${esc(_sfgRun.summary || 'dbt build failed — see the log.')}</span>`;
    else if (!ready) html = `<span style="color:var(--text-muted)">Set the Databricks workspace, token &amp; SQL warehouse on the <strong>Databricks Agent</strong> step first.</span>`;
    statusEl.innerHTML = html;
  }
  if (modelsEl) {
    const ms = _sfgRun.models || [];
    if (!ms.length) { modelsEl.innerHTML = ''; }
    else {
      const failed = ms.filter(m => !_sfgIsOk(m.status)).length;
      modelsEl.innerHTML =
        `<div style="font-size:11px;color:var(--text-secondary);margin-bottom:2px">${ms.length} model${ms.length === 1 ? '' : 's'}${failed ? ` · ${failed} failed` : ''}</div>` +
        ms.map(m => {
          const c = _sfgIsOk(m.status) ? 'var(--success,#16a34a)' : 'var(--danger,#dc2626)';
          const t = (typeof m.execution_time === 'number') ? ` · ${m.execution_time.toFixed(1)}s` : '';
          return `<div style="display:flex;align-items:center;gap:8px;padding:2px 0;font-size:12px;border-top:1px solid var(--border)">
            <span style="width:8px;height:8px;border-radius:50%;background:${c};flex:none"></span>
            <code style="flex:1">${esc(m.name)}</code><span style="color:${c}">${esc(m.status)}${t}</span></div>`;
        }).join('');
    }
  }
  if (log) {
    const atBottom = log.scrollHeight - log.scrollTop - log.clientHeight < 40;
    log.textContent = _sfgRun.logs || 'No output yet.';
    if (atBottom) log.scrollTop = log.scrollHeight;
  }
}

function startSfgPolling() {
  if (_sfgPollTimer) clearInterval(_sfgPollTimer);
  _sfgPollTimer = setInterval(async () => {
    if (!_sfgRun.jobId) return;
    try {
      const r = await api.getDbtLocalStatus(_sfgRun.jobId, _sfgRun.logOffset);
      const chunk = r.logs || [];
      if (chunk.length) {
        const prefix = _sfgRun.logs && !_sfgRun.logs.endsWith('\n') ? '\n' : '';
        _sfgRun.logs += prefix + chunk.join('\n') + '\n';
      }
      if (typeof r.logOffset === 'number') _sfgRun.logOffset = r.logOffset;
      _sfgRun.status = r.status || 'running';
      _sfgRun.summary = r.summary || '';
      if (Array.isArray(r.models)) _sfgRun.models = r.models;
      if (r.finished) {
        _sfgRun.running = false; _sfgRun.cancelling = false; _sfgRun.finished = true;
        if (r.status === 'error' && r.error) _sfgRun.error = r.error;
        clearInterval(_sfgPollTimer); _sfgPollTimer = null;
      }
      renderSfgRunView();
    } catch (err) {
      _sfgRun.running = false; _sfgRun.cancelling = false; _sfgRun.status = 'error'; _sfgRun.error = err.message;
      clearInterval(_sfgPollTimer); _sfgPollTimer = null;
      renderSfgRunView();
    }
  }, 2000);
}

async function runSfgDbtLocal() {
  const dest = store.get().sfGlueDestination || {};
  if (!dest.workspace_url || !dest.sql_warehouse_id || !(dest.token || dest.personal_access_token)) {
    _sfgRun.error = 'Set the Databricks workspace URL, token & SQL warehouse on the Databricks Agent step first.';
    renderSfgRunView();
    return;
  }
  const { models, sources_yml } = sfgModelsWithEdits();
  const n = Object.keys(models).length;
  if (!n) { notify('Generate the conversion first.', { kind: 'warning', title: 'Nothing to run' }); return; }
  if (!(await confirmModal(
    `Runs real dbt (debug → build) for ${n} model(s) against ${dest.workspace_url}, creating/overwriting their tables in ${dest.catalog || 'the catalog'}.${dest.personal_access_token ? '' : ''} Continue?`,
    { title: `Run ${n} model(s) with dbt-Core?`, confirmLabel: 'Run dbt', danger: true }))) return;

  _sfgRun = { jobId: null, running: true, finished: false, status: 'running', summary: '',
              error: '', logs: 'Starting dbt run…\n', logOffset: 0, models: [], cancelling: false };
  renderSfgRunView();
  try {
    const res = await api.runDbtLocalSfGlue({
      sessionId: store.get().sfGlueSessionId || store.get().sessionId || 'sfglue',
      models, sources_yml, destination: dest,
    });
    _sfgRun.jobId = res.jobId;
    startSfgPolling();
  } catch (err) {
    _sfgRun.running = false; _sfgRun.status = 'error'; _sfgRun.error = err.message;
    renderSfgRunView();
    notify(err.message, { kind: 'error', title: 'dbt run failed to start' });
  }
}

async function cancelSfgDbtLocal() {
  if (!_sfgRun.jobId || !_sfgRun.running || _sfgRun.cancelling) return;
  _sfgRun.cancelling = true;
  renderSfgRunView();
  try { await api.cancelDbtLocal(_sfgRun.jobId); }
  catch (err) { _sfgRun.cancelling = false; _sfgRun.error = err.message; renderSfgRunView(); }
}

// ── Push converted models to a GitHub repo (the dbt Cloud production path) ──
// Persist only the non-secret repo coordinates; the token stays in the input (memory).
function loadRepoCfg() {
  try { return JSON.parse(localStorage.getItem('qvf_sfglue_repo') || '{}'); } catch { return {}; }
}
function saveRepoCfg(c) {
  try {
    localStorage.setItem('qvf_sfglue_repo', JSON.stringify({
      owner: c.owner || '', repo: c.repo || '', branch: c.branch || '', path: c.path || '' }));
  } catch { /* storage disabled — non-fatal */ }
}

async function pushSfgModelsToRepo(container) {
  const get = id => (container.querySelector(id)?.value || '').trim();
  const github = {
    token: get('#sfg-gh-token'), owner: get('#sfg-gh-owner'), repo: get('#sfg-gh-repo'),
    branch: get('#sfg-gh-branch') || 'main', path: get('#sfg-gh-path') || 'models',
  };
  const resEl = container.querySelector('#sfg-gh-result');
  const setRes = html => { if (resEl) resEl.innerHTML = html; };
  if (!github.token) return setRes('<span style="color:var(--danger,#dc2626)">GitHub token required (repo write scope).</span>');
  if (!github.owner || !github.repo) return setRes('<span style="color:var(--danger,#dc2626)">Owner and repo are required.</span>');
  saveRepoCfg(github);
  const { models, sources_yml } = sfgModelsWithEdits();
  if (!Object.keys(models).length) { notify('Generate the conversion first.', { kind: 'warning', title: 'Nothing to push' }); return; }
  const btn = container.querySelector('#sfg-gh-push');
  if (btn) { btn.disabled = true; btn.textContent = 'Pushing…'; }
  setRes('<span style="color:var(--text-muted)">Pushing…</span>');
  try {
    const r = await api.pushModelsToRepo({ github, models, sources_yml });
    const ok = (r.pushed || []).length, bad = (r.failed || []).length;
    const okRows = (r.pushed || []).map(p => `<div style="font-size:11px"><span style="color:var(--success,#16a34a)">✓</span> <code>${esc(p.path)}</code></div>`).join('');
    const badRows = (r.failed || []).map(p => `<div style="font-size:11px"><span style="color:var(--danger,#dc2626)">✗</span> <code>${esc(p.path)}</code> — ${esc(p.detail)}</div>`).join('');
    setRes(`<div style="font-size:12px;margin-bottom:4px">Pushed <strong>${ok}</strong> file(s) to <code>${esc(r.repo)}@${esc(r.branch)}</code>${bad ? ` · ${bad} failed` : ''}.${(ok && !bad) ? ' Now trigger the dbt Cloud job (DBT Agent run) to build them.' : ''}</div>${okRows}${badRows}`);
    notify(bad ? `Pushed ${ok}, ${bad} failed` : `Pushed ${ok} file(s) to ${r.repo}`, { kind: bad ? 'warning' : 'success', title: 'Push to repo' });
  } catch (e) {
    setRes(`<span style="color:var(--danger,#dc2626)">⚠ ${esc(e.message)}</span>`);
    notify(e.message, { kind: 'error', title: 'Push failed' });
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = '⬆ Push to repo'; }
  }
}

// Snowflake source connection (same derivation as the Review step).
function snowflakeConfig(state) {
  const sf = state.sfGlueSnowflakeConfig || {};
  return (sf.account && (sf.database || (state.sfGlueSnowflakeConnection || {}).success)) ? sf : undefined;
}

// The reconcilable pairs: each Snowflake table/view in scope → its Databricks target,
// with a primary key prefilled from the declared relationships when we have one.
// Exported so the automated Run flow can reuse the exact same pairing logic.
export function reconcilePairs(state) {
  const conv = state.sfGlueConversion || {};
  const targets = ((conv.plan || {}).targets || []).filter(t => t.system === 'snowflake' && t.target);
  const rels = (state.sfGlueReview && state.sfGlueReview.relationships) || [];
  const pkByBase = {};
  rels.forEach(r => { if (r.pk_table && (r.pk_columns || []).length) pkByBase[baseName(r.pk_table)] = r.pk_columns.join(','); });
  const savedKeys = state.sfGlueReconcileKeys || {};
  // Server-suggested keys (from a previous reconcile run) — used to prefill the key
  // input when the operator hasn't typed one and there's no declared relationship key.
  const suggestedByCand = {};
  ((state.sfGlueReconcile || {}).results || []).forEach(r => {
    if ((r.suggested_key || []).length) suggestedByCand[r.candidate] = r.suggested_key.join(',');
  });
  return targets.map(t => ({
    source: t.source,
    target: t.target,
    layer: t.layer,
    key: (t.source in savedKeys) ? savedKeys[t.source]
      : (pkByBase[baseName(t.source)] || suggestedByCand[t.target] || ''),
  }));
}

function renderTestResults(state) {
  const conv = state.sfGlueConversion || {};
  const specs = conv.test_specs || [];
  const run = state.sfGlueTests || null;
  if (!specs.length) {
    return `<div style="font-size:12px;color:var(--text-muted)">No tests generated — declare primary/foreign keys in the lineage to get key/grain/relationship/contract tests.</div>`;
  }
  const summary = `${specs.length} generated test(s)` + (run
    ? ` · <strong style="color:${run.all_passed ? 'var(--success,#16a34a)' : 'var(--danger,#dc2626)'}">${(run.results || []).filter(r => r.passed).length}/${(run.results || []).length} passed</strong>`
    : ' · not run yet');
  const KIND = { not_null: 'not null', unique: 'unique', unique_combo: 'unique (grain)', relationships: 'FK', contract: 'contract' };
  const rows = !run ? '' : (run.results || []).map(r => `
    <tr>
      <td style="padding:3px 8px;font-family:monospace;font-size:11px">${esc(r.model)}</td>
      <td style="padding:3px 8px;font-size:11px">${esc(KIND[r.kind] || r.kind)}${(r.columns || []).length ? ` <span style="color:var(--text-muted)">(${esc((r.columns || []).join(', '))})</span>` : ''}</td>
      <td style="padding:3px 8px;font-size:12px">${r.passed ? '<span style="color:var(--success,#16a34a)">✅</span>' : `<span style="color:var(--danger,#dc2626)" title="${esc(r.detail || '')}">❌ ${esc(r.detail || 'fail')}</span>`}</td>
    </tr>`).join('');
  return `<div style="font-size:12px;color:var(--text-secondary);margin-bottom:6px">${summary}</div>` + (run ? `
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim)">
        <th style="padding:3px 8px">Model</th><th style="padding:3px 8px">Test</th><th style="padding:3px 8px">Result</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>` : '');
}

function renderReconcile(state) {
  const pairs = reconcilePairs(state);
  if (!pairs.length) {
    return `<div style="font-size:12px;color:var(--text-muted)">No Snowflake source tables in scope to verify.</div>`;
  }
  const resByCand = {};
  ((state.sfGlueReconcile || {}).results || []).forEach(r => { resByCand[r.candidate] = r; });
  const rows = pairs.map(p => {
    const r = resByCand[p.target];
    const status = !r ? '<span style="color:var(--text-muted)">—</span>'
      : r.error ? `<span title="${esc(r.error)}" style="color:var(--danger,#dc2626)">⚠ error</span>`
        : r.passed ? '<span style="color:var(--success,#16a34a)">✅ pass</span>'
          : `<span style="color:var(--danger,#dc2626)">❌ ${(r.failures || []).length} issue(s)</span>`;
    // Auto-excluded columns (surrogate/hash keys, run stamps) the operator can't be
    // expected to know about — shown as plain-language chips so they understand why a
    // column was skipped instead of silently dropping it.
    const chips = ((r && r.suggested_exclude) || []).map(x =>
      `<span title="${esc(x.column)} — ${esc(x.reason)}"
        style="display:inline-block;margin:2px 4px 0 0;padding:2px 7px;border:1px solid var(--border);border-radius:10px;background:var(--bg-primary);font-size:10px;color:var(--text-secondary)">
        ${esc(x.column)} — ${esc(x.reason)}</span>`).join('');
    const chipRow = chips
      ? `<tr><td colspan="4" style="padding:0 8px 6px 8px">
          <div style="font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.5px">Auto-excluded (won't match across engines)</div>
          ${chips}</td></tr>`
      : '';
    return `<tr>
      <td style="padding:4px 8px;font-family:monospace;font-size:12px">${esc(p.source)}</td>
      <td style="padding:4px 8px;font-family:monospace;font-size:12px;color:var(--text-secondary)">${esc(p.target)}</td>
      <td style="padding:4px 8px"><input class="rec-key" data-src="${esc(p.source)}" value="${esc(p.key)}" placeholder="primary key(s), comma-sep" aria-label="Reconcile key column(s) for ${esc(p.source)}"
        style="width:170px;padding:3px 7px;border:1px solid var(--border);border-radius:5px;background:var(--bg-primary);color:var(--text-primary);font-size:11px;font-family:monospace" /></td>
      <td style="padding:4px 8px;font-size:12px">${status}</td>
    </tr>${chipRow}`;
  }).join('');
  return `
    <table style="width:100%;border-collapse:collapse">
      <thead><tr style="text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim)">
        <th style="padding:4px 8px">Source (Snowflake)</th><th style="padding:4px 8px">Candidate (Databricks)</th>
        <th style="padding:4px 8px">Primary key</th><th style="padding:4px 8px">Result</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    ${(state.sfGlueReconcile || {}).results ? `<div style="margin-top:10px">${reconcileResultsPanel(state.sfGlueReconcile.results)}</div>` : ''}`;
}

// Summarize a reconcile run as ONE toast: green when every pair matches the source,
// otherwise the first failing table + its headline issue (and a count of any others).
function notifyReconcile(result) {
  const results = (result && result.results) || [];
  if (!results.length) { notify('Verification returned no results.', { kind: 'warning', title: 'Verify' }); return; }
  const failed = results.filter(r => r.error || !r.passed);
  if (!failed.length) {
    notify(`${results.length}/${results.length} table(s) match the source.`, { kind: 'success', title: 'Verification passed' });
    return;
  }
  const f = failed[0];
  const cand = baseName(f.candidate || f.source || '');
  const headline = f.error || (f.failures || [])[0] || 'mismatch';
  const more = failed.length > 1 ? ` · +${failed.length - 1} more` : '';
  notify(`${cand}: ${headline}${more}`, { kind: 'error', title: `Verification failed — ${failed.length}/${results.length}` });
}

export function renderSfGlueDbtAgentPage(container) {
  const state = store.get();
  const conv = state.sfGlueConversion;
  const edits = state.sfGlueArtifactEdits || {};
  const explains = state.sfGlueArtifactExplain || {};

  if (!conv) {
    container.innerHTML = `
      <div class="page" style="padding:24px;width:100%"><div style="max-width:1000px;margin:0 auto">
        <button class="btn btn-secondary" id="dbt-back" style="padding:4px 10px;font-size:11px;margin-bottom:12px">← Databricks Agent</button>
        <div style="color:var(--text-muted);font-size:14px;padding:40px;text-align:center;border:1px dashed var(--border);border-radius:10px">
          Run <strong>⚡ Generate conversion</strong> on the <strong>Review &amp; Edit</strong> step first to produce the dbt models and <code>sources.yml</code>.
        </div>
      </div></div>`;
    container.querySelector('#dbt-back')?.addEventListener('click', () => store.navigate('sfglue-databricks-agent'));
    return;
  }

  const models = conv.dbt_models || {};
  const repoCfg = loadRepoCfg();
  const busyRec = state.isReconcilingSfGlue;
  const busyTest = state.isTestingSfGlue;
  const hasTests = ((conv.test_specs || []).length) > 0;
  // Reconcile compares BUILT Databricks tables against Snowflake — it can't run until the
  // tables exist, so gate the button on a successful Build/deploy from the previous step.
  const built = !!(state.sfGlueBuild && state.sfGlueBuild.success);
  const recTitle = built
    ? 'Verify the built Databricks tables against the Snowflake source.'
    : 'Build the tables on the Databricks Agent step first — verification compares built Databricks tables against Snowflake.';

  container.innerHTML = `
    <div class="page" style="overflow:auto;padding:24px;width:100%">
      <div style="max-width:1000px;margin:0 auto">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
          <button class="btn btn-secondary" id="dbt-back" style="padding:4px 10px">← Databricks Agent</button>
          <h2 style="margin:0">DBT Agent</h2>
        </div>
        <p style="color:var(--text-secondary);margin:0 0 14px">
          dbt models (staging → silver → gold), <code>sources.yml</code>, and generated <code>schema.yml</code> tests. Review, resolve the queue below, then verify against the source.
        </p>

        ${reviewQueuePanel(conv.untranslatable)}

        <!-- RUN: execute the converted models with real dbt-Core (no git / dbt Cloud) -->
        <div style="border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:16px;background:var(--bg-surface)">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
            <strong style="font-size:13px">▶ Run with dbt-Core (local)</strong>
            <span style="font-size:11px;color:var(--text-muted)">Runs these models with real dbt (<code>dbt debug → dbt build</code>) on your Databricks warehouse — the Glue jobs, executed as dbt. No git, no dbt Cloud.</span>
            <button class="btn btn-primary" id="sfg-dbt-run" style="margin-left:auto;padding:4px 12px;font-size:12px">▶ Run with dbt</button>
            <button class="btn btn-secondary" id="sfg-dbt-cancel" style="display:none;padding:4px 12px;font-size:12px">Stop</button>
          </div>
          <div id="sfg-dbt-status" style="font-size:12px;margin-bottom:6px"></div>
          <div id="sfg-dbt-models" style="margin-bottom:6px"></div>
          <pre id="sfg-dbt-log" style="margin:0;padding:10px;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;overflow:auto;max-height:260px;font-size:11px;line-height:1.5;white-space:pre-wrap">No output yet.</pre>
        </div>

        <!-- PUSH: commit the models to the dbt Cloud project's GitHub repo (production path) -->
        <div style="border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:16px;background:var(--bg-surface)">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
            <strong style="font-size:13px">⬆ Push to GitHub repo (dbt Cloud)</strong>
            <span style="font-size:11px;color:var(--text-muted)">Commits these models to the repo your dbt Cloud project is connected to, so the Cloud job runs them. <strong>External GitHub repo only</strong> — dbt's managed repo has no write API.</span>
            <button class="btn btn-secondary" id="sfg-dbt-export" style="margin-left:auto;padding:4px 12px;font-size:12px" title="Download the full runnable dbt project as a .zip — models + sources/schema/unit-tests/packages + dbt_project.yml + profiles.yml + bronze notebooks + reference DDL.">⤓ Export dbt project (.zip)</button>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:8px">
            <input id="sfg-gh-token" type="password" placeholder="GitHub token (repo write)" autocomplete="off" style="flex:2;min-width:200px;padding:5px 9px;border:1px solid var(--border);border-radius:6px;background:var(--bg-primary);color:var(--text-primary);font-size:12px" />
            <input id="sfg-gh-owner" placeholder="owner" value="${esc(repoCfg.owner || '')}" style="flex:1;min-width:110px;padding:5px 9px;border:1px solid var(--border);border-radius:6px;background:var(--bg-primary);color:var(--text-primary);font-size:12px" />
            <input id="sfg-gh-repo" placeholder="repo" value="${esc(repoCfg.repo || '')}" style="flex:1;min-width:110px;padding:5px 9px;border:1px solid var(--border);border-radius:6px;background:var(--bg-primary);color:var(--text-primary);font-size:12px" />
            <input id="sfg-gh-branch" placeholder="branch (main)" value="${esc(repoCfg.branch || '')}" style="width:120px;padding:5px 9px;border:1px solid var(--border);border-radius:6px;background:var(--bg-primary);color:var(--text-primary);font-size:12px" />
            <input id="sfg-gh-path" placeholder="path (models)" value="${esc(repoCfg.path || '')}" style="width:120px;padding:5px 9px;border:1px solid var(--border);border-radius:6px;background:var(--bg-primary);color:var(--text-primary);font-size:12px" />
            <button class="btn btn-secondary" id="sfg-gh-push" style="padding:4px 12px;font-size:12px">⬆ Push to repo</button>
          </div>
          <div id="sfg-gh-result" style="font-size:12px"></div>
        </div>

        <!-- STAGED GATE 1: run the generated dbt tests + enforced contracts on the warehouse -->
        <div style="border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:16px;background:var(--bg-surface)">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
            <strong style="font-size:13px">🧪 Run dbt tests &amp; contracts</strong>
            <span style="font-size:11px;color:var(--text-muted)">Executes the generated key/grain tests, FK relationships &amp; enforced-contract checks as SQL on the built tables — an execution gate BEFORE reconciliation.</span>
            <button class="btn btn-primary" id="test-run" ${(busyTest || !built || !hasTests) ? 'disabled' : ''} title="${built ? (hasTests ? 'Run the generated tests on the built Databricks tables.' : 'No tests generated — declare keys in the lineage.') : 'Build the tables first.'}" style="margin-left:auto;padding:4px 12px;font-size:12px">${busyTest ? 'Running…' : '🧪 Run tests'}</button>
          </div>
          <div id="test-error" style="color:var(--danger,#dc2626);font-size:12px;margin-bottom:6px">${esc(state.sfGlueTestError || '')}</div>
          <div id="test-body">${renderTestResults(state)}</div>
        </div>

        <!-- SHIP GATE: reconcile built Databricks tables against the legacy Snowflake source -->
        <div style="border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:16px;background:var(--bg-surface)">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
            <strong style="font-size:13px">✅ Verify against source</strong>
            <span style="font-size:11px;color:var(--text-muted)">Diffs row counts, key integrity &amp; per-column aggregates (Snowflake vs Databricks). Build/deploy the tables first.</span>
            <button class="btn btn-primary" id="rec-run" ${(busyRec || !built) ? 'disabled' : ''} title="${esc(recTitle)}" style="margin-left:auto;padding:4px 12px;font-size:12px">${busyRec ? 'Verifying…' : '🔬 Verify all'}</button>
          </div>
          <div id="rec-error" style="color:var(--danger,#dc2626);font-size:12px;margin-bottom:6px">${esc(state.sfGlueReconcileError || '')}</div>
          <div id="rec-body">${renderReconcile(state)}</div>
        </div>

        ${artifactGroup('dbt models', models, 'dbt model', edits, explains)}
        ${conv.sources_yml ? `<h3 style="margin:18px 0 8px">dbt sources.yml</h3>${codeArtifact('sources_yml:sources.yml', 'sources.yml', conv.sources_yml, 'dbt sources', edits, explains)}` : ''}
        ${conv.schema_yml ? `<h3 style="margin:18px 0 8px">dbt schema.yml <span style="font-size:12px;color:var(--text-muted)">(key/grain tests + enforced contracts)</span></h3>${codeArtifact('schema_yml:schema.yml', 'schema.yml', conv.schema_yml, 'dbt tests', edits, explains)}` : ''}
        ${conv.unit_tests_yml ? `<h3 style="margin:18px 0 8px">dbt unit_tests.yml <span style="font-size:12px;color:var(--text-muted)">(pre-build logic tests — fill expected rows from a golden fixture)</span></h3>${codeArtifact('unit_tests_yml:unit_tests.yml', 'unit_tests.yml', conv.unit_tests_yml, 'dbt unit tests', edits, explains)}` : ''}
        ${conv.packages_yml ? `<h3 style="margin:18px 0 8px">dbt packages.yml <span style="font-size:12px;color:var(--text-muted)">(dbt_utils — run <code>dbt deps</code>)</span></h3>${codeArtifact('packages_yml:packages.yml', 'packages.yml', conv.packages_yml, 'dbt packages', edits, explains)}` : ''}
        ${conv.governance_md ? `<h3 style="margin:18px 0 8px">Governance checklist <span style="font-size:12px;color:var(--text-muted)">(lineage · secrets · dev/prod · cost)</span></h3>${codeArtifact('governance_md:GOVERNANCE.md', 'GOVERNANCE.md', conv.governance_md, 'governance', edits, explains)}` : ''}
        ${Object.keys(models).length ? '' : '<div style="color:var(--text-muted);font-size:13px">No dbt models in this conversion (no transformation jobs or Snowflake views/tables in scope).</div>'}
      </div>
    </div>`;

  wireArtifacts(container);
  wireReviewQueue(container, store.get().sfGlueConversion);
  container.querySelector('#dbt-back')?.addEventListener('click', () => store.navigate('sfglue-databricks-agent'));

  // Local dbt-Core run controls. State is module-level, so restore the view and resume
  // polling if a run is still in flight after this re-render.
  container.querySelector('#sfg-dbt-run')?.addEventListener('click', runSfgDbtLocal);
  container.querySelector('#sfg-dbt-cancel')?.addEventListener('click', cancelSfgDbtLocal);
  container.querySelector('#sfg-gh-push')?.addEventListener('click', () => pushSfgModelsToRepo(container));
  container.querySelector('#sfg-dbt-export')?.addEventListener('click', () => exportSfgDbtProject(container));
  if (_sfgRun.jobId && _sfgRun.running && !_sfgPollTimer) startSfgPolling();
  renderSfgRunView();

  // Persist edited keys without a re-render (so typing isn't disrupted).
  container.querySelectorAll('.rec-key').forEach(inp => inp.addEventListener('change', () => {
    const keys = store.get().sfGlueReconcileKeys || (store.get().sfGlueReconcileKeys = {});
    keys[inp.dataset.src] = inp.value.trim();
  }));

  container.querySelector('#rec-run')?.addEventListener('click', async () => {
    const s = store.get();
    // Route every error through state — store.set re-renders the page, so the captured
    // `container` is stale after the busy-state re-render below; reading from state is robust.
    const fail = (msg) => store.set({ sfGlueReconcileError: msg, isReconcilingSfGlue: false });
    const snowflake = snowflakeConfig(s);
    if (!snowflake) return fail('Connect Snowflake (the source of truth) on the Connections step first.');
    const destination = s.sfGlueDestination || {};
    if (!destination.workspace_url || !destination.sql_warehouse_id) {
      return fail('Set the Databricks Workspace URL and SQL Warehouse ID on the Databricks Agent step first.');
    }
    // Read the live key inputs so unsaved edits are included.
    const keyBySrc = { ...(s.sfGlueReconcileKeys || {}) };
    container.querySelectorAll('.rec-key').forEach(inp => { keyBySrc[inp.dataset.src] = inp.value.trim(); });
    const pairs = reconcilePairs(s)
      .map(p => ({ source: p.source, candidate: p.target, key: keyBySrc[p.source] || p.key }))
      .filter(p => p.candidate);
    if (!pairs.length) return fail('No tables to verify.');
    if (pairs.every(p => !p.key)) return fail('Set a primary key for at least one table to verify.');
    store.set({ sfGlueReconcileKeys: keyBySrc, sfGlueReconcileError: '', isReconcilingSfGlue: true });
    try {
      const result = await api.reconcileSnowflakeGlueMigration({ snowflake, destination, pairs });
      store.set({ sfGlueReconcile: result, isReconcilingSfGlue: false });
      notifyReconcile(result);
    } catch (e) {
      fail('⚠ ' + e.message);
      notify(e.message, { kind: 'error', title: 'Verification error' });
    }
  });

  container.querySelector('#test-run')?.addEventListener('click', async () => {
    const s = store.get();
    const fail = (msg) => store.set({ sfGlueTestError: msg, isTestingSfGlue: false });
    const destination = s.sfGlueDestination || {};
    const specs = (s.sfGlueConversion || {}).test_specs || [];
    if (!destination.workspace_url || !destination.sql_warehouse_id) {
      return fail('Set the Databricks Workspace URL and SQL Warehouse ID on the Databricks Agent step first.');
    }
    if (!specs.length) return fail('No tests to run.');
    store.set({ sfGlueTestError: '', isTestingSfGlue: true });
    try {
      const result = await api.runSfGlueTests({ destination, test_specs: specs });
      store.set({ sfGlueTests: result, isTestingSfGlue: false });
      notify(result.summary || 'Tests complete', {
        kind: result.all_passed ? 'success' : 'warning',
        title: result.all_passed ? 'All tests passed' : 'Some tests failed',
      });
    } catch (e) {
      fail('⚠ ' + e.message);
      notify(e.message, { kind: 'error', title: 'Test run error' });
    }
  });
}

export function destroySfGlueDbtAgentPage() {
  // Stop the dbt-run poller; the in-memory _sfgRun state is kept so re-entering the
  // page shows the last/finished run.
  if (_sfgPollTimer) { clearInterval(_sfgPollTimer); _sfgPollTimer = null; }
}
