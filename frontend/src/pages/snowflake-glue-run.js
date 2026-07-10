/**
 * Automated end-to-end migration run (Snowflake + Glue → Databricks/dbt).
 *
 * One-click orchestration over the same APIs the 6 manual step pages use, with a
 * SINGLE pre-build checkpoint: it auto-runs lineage → load source → convert, pauses
 * so the user can review the generated code, then on Continue runs precheck → build →
 * reconcile and links to the Report. The manual step pages remain as an advanced view.
 *
 * Results are written straight into the same store keys the step pages/report read
 * (sfGlueLineage, sfGlueReview, sfGlueConversion, sfGluePrecheck, sfGlueBuild,
 * sfGlueReconcile), via direct mutation (no store.set) so the long run doesn't churn
 * the whole app through re-renders; the phase list + checkpoint update the DOM directly.
 */
import { api } from '../api.js';
import { store } from '../store.js';
import { field, esc } from '../components/ui.js';
import { notify } from '../components/notify.js';
import { reconcilePairs } from './snowflake-glue-dbt-agent.js';

const PHASES = [
  { key: 'lineage', label: 'Analyze lineage' },
  { key: 'review', label: 'Load source (Glue scripts + Snowflake SQL)' },
  { key: 'convert', label: 'Generate conversion — dbt models / DDL / notebooks' },
  { key: 'checkpoint', label: 'Review generated code (checkpoint)' },
  { key: 'precheck', label: 'Precheck Databricks (Unity Catalog)' },
  { key: 'build', label: 'Build models into Databricks' },
  { key: 'reconcile', label: 'Reconcile row counts' },
];

// Module-level run state — survives re-renders so returning to the page shows progress.
let phaseState = {};      // key -> { status: pending|running|done|error|skipped, note }
let stage = 'idle';       // idle | running1 | checkpoint | running2 | done
let abortCtl = null;
const resetRun = () => { phaseState = {}; stage = 'idle'; abortCtl = null; };

const ICON = { pending: '○', running: '⏳', done: '✓', error: '✗', skipped: '⤼' };
const COLOR = {
  pending: 'var(--text-dim)', running: 'var(--primary)', done: 'var(--success)',
  error: 'var(--error)', skipped: 'var(--text-muted)',
};

function sourceConfigs(state) {
  const sf = state.sfGlueSnowflakeConfig || {}, gl = state.sfGlueGlueConfig || {};
  const pgOk = !!(state.sfGluePostgresConnection && state.sfGluePostgresConnection.success);
  return {
    snowflake: (sf.account && (sf.database || (state.sfGlueSnowflakeConnection || {}).success)) ? sf : undefined,
    glue: (gl.region && (gl.profile_name || (gl.access_key_id && gl.secret_access_key))) ? gl : undefined,
    postgres: pgOk ? (state.sfGluePostgresConfig || undefined) : undefined,
  };
}

function currentDest() {
  const d = store.get().sfGlueDestination || {};
  return {
    workspace_url: d.workspace_url || '', token: d.token || '', sql_warehouse_id: d.sql_warehouse_id || '',
    catalog: d.catalog || 'lakehouse', bronze_schema: d.bronze_schema || 'bronze',
    silver_schema: d.silver_schema || 'silver', gold_schema: d.gold_schema || 'gold',
    source_catalog: d.source_catalog || '', source_schema: d.source_schema || '',
  };
}

function phaseRow(p) {
  const st = (phaseState[p.key] || {}).status || 'pending';
  const note = (phaseState[p.key] || {}).note || '';
  return `
    <div id="run-phase-${p.key}" style="display:flex;align-items:center;gap:12px;padding:10px 12px;border-bottom:1px solid var(--border)">
      <span class="run-ph-icon" style="width:20px;text-align:center;font-size:14px;color:${COLOR[st]}">${ICON[st]}</span>
      <span style="flex:1;font-size:13px;color:var(--text-primary)">${esc(p.label)}</span>
      <span class="run-ph-note" style="font-size:11px;color:var(--text-muted);max-width:340px;text-align:right">${esc(note)}</span>
    </div>`;
}

function setPhase(key, status, note = '') {
  phaseState[key] = { status, note };
  const row = document.getElementById(`run-phase-${key}`);
  if (!row) return;
  const icon = row.querySelector('.run-ph-icon');
  const noteEl = row.querySelector('.run-ph-note');
  if (icon) { icon.textContent = ICON[status]; icon.style.color = COLOR[status]; }
  if (noteEl) noteEl.textContent = note;
}

export function renderSfGlueRunPage(container) {
  const state = store.get();
  const { snowflake, glue } = sourceConfigs(state);
  const connected = !!(snowflake || glue);
  const dest = currentDest();

  container.innerHTML = `
    <div class="page" style="overflow:auto;padding:24px;width:100%">
      <div style="max-width:900px;margin:0 auto">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px">
          <button class="btn btn-secondary" id="run-back" style="padding:4px 10px">← Connect</button>
          <h2 style="margin:0">Automated migration</h2>
        </div>
        <p style="color:var(--text-secondary);margin:0 0 18px;font-size:13px;line-height:1.6">
          Runs the whole pipeline for you: lineage → convert every table → <strong>one review checkpoint</strong> →
          build into Databricks → reconcile. Use the numbered steps above any time for manual control.
        </p>

        ${connected ? '' : `<div class="badge badge-error" style="display:block;padding:10px;margin-bottom:14px;font-size:12px">Connect Snowflake and/or AWS Glue on the Connect step first.</div>`}

        <!-- Databricks destination (needed to convert with real bronze columns + to build) -->
        <div class="card" style="margin-bottom:16px">
          <div class="card-header"><div class="card-title"><span aria-hidden="true">🧱</span> Databricks destination</div></div>
          <div class="card-body">
            ${field('run-dbx-url', 'Workspace URL', dest.workspace_url, { placeholder: 'https://dbc-xxxx.cloud.databricks.com' })}
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px">
              ${field('run-dbx-token', 'Access token', dest.token, { type: 'password', placeholder: dest.token ? '•••••• (saved)' : 'dapi…' })}
              ${field('run-dbx-warehouse', 'SQL Warehouse ID', dest.sql_warehouse_id, {})}
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px">
              ${field('run-dbx-catalog', 'Catalog', dest.catalog, { placeholder: 'lakehouse' })}
              ${field('run-dbx-bronze', 'Bronze schema', dest.bronze_schema, { placeholder: 'bronze' })}
              ${field('run-dbx-source-catalog', 'Source catalog (opt)', dest.source_catalog, { placeholder: 'raw' })}
            </div>
          </div>
        </div>

        <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
          <button class="btn btn-primary" id="run-start" style="font-weight:700">🚀 Run migration</button>
          <button class="btn btn-secondary" id="run-stop" style="display:none">⏹ Stop</button>
          <span id="run-status" style="font-size:12px;color:var(--text-muted)"></span>
        </div>

        <div style="border:1px solid var(--border);border-radius:10px;overflow:hidden;background:var(--bg-surface)">
          ${PHASES.map(phaseRow).join('')}
        </div>

        <div id="run-checkpoint" style="margin-top:16px"></div>
        <div id="run-error" role="alert" style="color:var(--error);font-size:13px;margin-top:12px"></div>
      </div>
    </div>`;

  // Persist Databricks destination edits (direct mutation, no re-render).
  const readDest = () => ({
    workspace_url: container.querySelector('#run-dbx-url').value.trim(),
    token: container.querySelector('#run-dbx-token').value || (store.get().sfGlueDestination || {}).token || '',
    sql_warehouse_id: container.querySelector('#run-dbx-warehouse').value.trim(),
    catalog: container.querySelector('#run-dbx-catalog').value.trim() || 'lakehouse',
    bronze_schema: container.querySelector('#run-dbx-bronze').value.trim() || 'bronze',
    silver_schema: (store.get().sfGlueDestination || {}).silver_schema || 'silver',
    gold_schema: (store.get().sfGlueDestination || {}).gold_schema || 'gold',
    source_catalog: container.querySelector('#run-dbx-source-catalog').value.trim(),
    source_schema: (store.get().sfGlueDestination || {}).source_schema || '',
  });
  container.querySelectorAll('#run-dbx-url,#run-dbx-token,#run-dbx-warehouse,#run-dbx-catalog,#run-dbx-bronze,#run-dbx-source-catalog')
    .forEach(inp => inp.addEventListener('change', () => { store.get().sfGlueDestination = readDest(); }));

  container.querySelector('#run-back')?.addEventListener('click', () => store.navigate('sfglue-connect'));
  container.querySelector('#run-stop')?.addEventListener('click', () => { if (abortCtl) abortCtl.abort(); });
  container.querySelector('#run-start')?.addEventListener('click', () => startRun(container));

  // If a run is already mid-flight / at the checkpoint / done, restore that view.
  if (stage === 'checkpoint') renderCheckpoint(container, store.get().sfGlueConversion || {});
  else if (stage === 'done') renderDone(container);
}

function guardBeforeStart(container) {
  const state = store.get();
  const { snowflake, glue } = sourceConfigs(state);
  const err = container.querySelector('#run-error');
  if (err) err.textContent = '';
  if (!snowflake && !glue) { if (err) err.textContent = 'Connect a source first.'; return false; }
  store.get().sfGlueDestination = {
    ...(store.get().sfGlueDestination || {}),
    workspace_url: container.querySelector('#run-dbx-url').value.trim(),
    token: container.querySelector('#run-dbx-token').value || (store.get().sfGlueDestination || {}).token || '',
    sql_warehouse_id: container.querySelector('#run-dbx-warehouse').value.trim(),
    catalog: container.querySelector('#run-dbx-catalog').value.trim() || 'lakehouse',
    bronze_schema: container.querySelector('#run-dbx-bronze').value.trim() || 'bronze',
    source_catalog: container.querySelector('#run-dbx-source-catalog').value.trim(),
  };
  const d = store.get().sfGlueDestination;
  if (!d.workspace_url || !d.sql_warehouse_id) {
    if (err) err.textContent = 'Set the Databricks Workspace URL and SQL Warehouse ID first (needed to build).';
    return false;
  }
  return true;
}

function setBusyUI(container, busy) {
  const start = container.querySelector('#run-start');
  const stop = container.querySelector('#run-stop');
  if (start) start.style.display = busy ? 'none' : 'inline-flex';
  if (stop) stop.style.display = busy ? 'inline-flex' : 'none';
}

async function startRun(container) {
  if (!guardBeforeStart(container)) return;
  resetRun();
  PHASES.forEach(p => setPhase(p.key, 'pending'));
  container.querySelector('#run-checkpoint').innerHTML = '';
  stage = 'running1';
  abortCtl = new AbortController();
  setBusyUI(container, true);
  const statusEl = container.querySelector('#run-status');
  if (statusEl) statusEl.textContent = 'Running…';
  try {
    const state = store.get();
    const { snowflake, glue, postgres } = sourceConfigs(state);

    setPhase('lineage', 'running');
    const lin = await api.buildSnowflakeGlueLineage({ snowflake, glue });
    store.get().sfGlueLineage = lin;
    const lineage = lin.lineage || {};
    const selected = (lineage.nodes || []).filter(n => String(n.id).startsWith('sf:')).map(n => n.id);
    store.get().sfGlueSelectedTables = selected;
    store.get().sfGlueReview = null;
    if (!selected.length) throw new Error('No Snowflake tables found in the lineage to migrate.');
    setPhase('lineage', 'done', `${selected.length} table(s)`);

    setPhase('review', 'running');
    const review = await api.reviewSnowflakeGlue({ snowflake, glue });
    store.get().sfGlueReview = review;
    setPhase('review', 'done', `${(review.glue_jobs || []).length} Glue job(s)`);

    setPhase('convert', 'running');
    const glueScripts = {};
    (review.glue_jobs || []).forEach(j => { if (j && j.name) glueScripts[j.name] = j.script; });
    const conv = await api.convertSnowflakeGlueMigration({
      snowflake, glue, postgres, lineage, selectedIds: selected,
      destination: store.get().sfGlueDestination, glueScripts, signal: abortCtl.signal,
    });
    store.get().sfGlueConversion = conv;
    setPhase('convert', 'done', `${Object.keys(conv.dbt_models || {}).length} dbt · ${Object.keys(conv.ddl || {}).length} DDL · ${Object.keys(conv.notebooks || {}).length} notebook(s)`);

    stage = 'checkpoint';
    setPhase('checkpoint', 'running', 'Review, then Continue to build');
    setBusyUI(container, false);
    if (statusEl) statusEl.textContent = 'Paused for review';
    renderCheckpoint(container, conv);
  } catch (err) {
    onRunError(container, err);
  }
}

function renderCheckpoint(container, conv) {
  const box = container.querySelector('#run-checkpoint');
  if (!box) return;
  const counts = [
    ['dbt models', Object.keys(conv.dbt_models || {}).length],
    ['Databricks DDL', Object.keys(conv.ddl || {}).length],
    ['PySpark notebooks', Object.keys(conv.notebooks || {}).length],
  ];
  box.innerHTML = `
    <div class="card" style="border-color:var(--primary)">
      <div class="card-header"><div class="card-title"><span aria-hidden="true">⏸</span> Review checkpoint</div></div>
      <div class="card-body">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:10px">
          Conversion is done. Review the generated code, then continue — <strong>Build writes tables into your Databricks catalog</strong>.
        </div>
        <div style="display:flex;gap:18px;flex-wrap:wrap;margin-bottom:14px">
          ${counts.map(([l, n]) => `<span style="font-size:12px"><strong style="font-size:16px;color:var(--primary)">${n}</strong> ${esc(l)}</span>`).join('')}
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
          <button class="btn btn-secondary" id="run-review-adv">Open in Review &amp; Edit (advanced)</button>
          <button class="btn btn-primary" id="run-continue" style="font-weight:700">✅ Continue → Build &amp; Reconcile</button>
        </div>
      </div>
    </div>`;
  box.querySelector('#run-review-adv')?.addEventListener('click', () => store.navigate('sfglue-review'));
  box.querySelector('#run-continue')?.addEventListener('click', () => continueRun(container));
}

async function continueRun(container) {
  container.querySelector('#run-checkpoint').innerHTML = '';
  setPhase('checkpoint', 'done', 'approved');
  stage = 'running2';
  setBusyUI(container, true);
  const statusEl = container.querySelector('#run-status');
  if (statusEl) statusEl.textContent = 'Building…';
  try {
    const state = store.get();
    const destination = state.sfGlueDestination;
    const lineage = (state.sfGlueLineage || {}).lineage || {};
    const selected = state.sfGlueSelectedTables || [];

    setPhase('precheck', 'running');
    try {
      const pre = await api.precheckSnowflakeGlueMigration({ lineage, selectedIds: selected, destination });
      store.get().sfGluePrecheck = pre;
      setPhase('precheck', 'done');
    } catch (e) {
      setPhase('precheck', 'skipped', `skipped: ${e.message}`);  // precheck is advisory, don't block build
    }

    setPhase('build', 'running');
    const models = {};
    Object.entries((state.sfGlueConversion || {}).dbt_models || {}).forEach(([k, v]) => { models[k] = v; });
    const build = await api.buildSnowflakeGlueMigration({ destination, models, glue: state.sfGlueGlueConfig || {} });
    store.get().sfGlueBuild = build;
    const results = build.results || [];
    const ok = results.filter(r => r.status === 'success' || r.status === 'repaired').length;
    setPhase('build', ok === results.length ? 'done' : 'error', `${ok}/${results.length} model(s) built`);

    setPhase('reconcile', 'running');
    const pairs = reconcilePairs(store.get());
    if (pairs.length && pairs.some(p => p.key)) {
      const { snowflake } = sourceConfigs(store.get());
      const rec = await api.reconcileSnowflakeGlueMigration({ snowflake, destination, pairs });
      store.get().sfGlueReconcile = rec;
      setPhase('reconcile', 'done', `${pairs.filter(p => p.key).length} table(s) checked`);
    } else {
      setPhase('reconcile', 'skipped', 'no primary keys — reconcile in the dbt Agent (advanced)');
    }

    stage = 'done';
    setBusyUI(container, false);
    if (statusEl) statusEl.textContent = 'Done';
    renderDone(container);
  } catch (err) {
    onRunError(container, err);
  }
}

function renderDone(container) {
  const box = container.querySelector('#run-checkpoint');
  if (!box) return;
  box.innerHTML = `
    <div class="card" style="border-color:var(--success)">
      <div class="card-header"><div class="card-title"><span aria-hidden="true">🎉</span> Migration run complete</div></div>
      <div class="card-body">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:12px">
          Conversion built into Databricks. Open the report for the full summary, fidelity grade, and artifacts.
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
          <button class="btn btn-primary" id="run-report" style="font-weight:700">View report →</button>
          <button class="btn btn-secondary" id="run-again">Run again</button>
        </div>
      </div>
    </div>`;
  box.querySelector('#run-report')?.addEventListener('click', () => store.navigate('sfglue-report'));
  box.querySelector('#run-again')?.addEventListener('click', () => { resetRun(); store.navigate('sfglue-run'); });
}

function onRunError(container, err) {
  setBusyUI(container, false);
  const statusEl = container.querySelector('#run-status');
  if (statusEl) statusEl.textContent = 'Stopped';
  // Mark the currently-running phase as failed.
  const running = PHASES.find(p => (phaseState[p.key] || {}).status === 'running');
  if (running) setPhase(running.key, 'error', err && err.name === 'AbortError' ? 'stopped' : (err.message || 'failed'));
  const el = container.querySelector('#run-error');
  if (el && !(err && err.name === 'AbortError')) el.textContent = '⚠ ' + (err.message || 'Run failed');
  if (err && err.name === 'AbortError') notify('Run stopped.', { kind: 'warning', title: 'Stopped' });
  else notify(err.message || 'Run failed', { kind: 'error', title: 'Migration run failed' });
}

export function destroySfGlueRunPage() { /* module-level run state persists intentionally */ }
