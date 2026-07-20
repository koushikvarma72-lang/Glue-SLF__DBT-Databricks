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
import { promptModal } from '../components/modal.js';
import { reconcilePairs } from './snowflake-glue-dbt-agent.js';

const PHASES = [
  { key: 'lineage', label: 'Analyze lineage' },
  { key: 'review', label: 'Load source (Glue scripts + Snowflake SQL)' },
  { key: 'convert', label: 'Generate conversion — dbt models / DDL / notebooks' },
  { key: 'checkpoint', label: 'Review generated code (checkpoint)' },
  { key: 'precheck', label: 'Precheck Databricks (Unity Catalog)' },
  { key: 'build', label: 'Build models into Databricks' },
  { key: 'reconcile', label: 'Reconcile row counts' },
  { key: 'push', label: 'Push notebooks + dbt project to workspace' },
  { key: 'orchestrate', label: 'Deploy orchestration (Glue workflows + Airflow DAGs \u2192 Databricks Jobs)' },
  { key: 'workflow_run', label: 'Run migrated workflow (verification gate)' },
];

// Module-level run state — survives re-renders so returning to the page shows progress.
let phaseState = {};      // key -> { status: pending|running|done|error|skipped, note }
let stage = 'idle';       // idle | running1 | checkpoint | running2 | done
let abortCtl = null;
const resetRun = () => { phaseState = {}; stage = 'idle'; abortCtl = null; };

const ICON = { pending: '○', running: '●', done: '✓', error: '✗', skipped: '–' };
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
        <p class="sfg-lead">
          Lineage → convert → one review checkpoint → build → verify. The numbered steps stay available for manual control.
        </p>

        ${connected ? '' : `<div class="badge badge-error" style="display:block;padding:10px;margin-bottom:14px;font-size:12px">Connect Snowflake and/or AWS Glue on the Connect step first.</div>`}

        <!-- Databricks destination (needed to convert with real bronze columns + to build) -->
        <div class="card" style="margin-bottom:16px">
          <div class="card-header"><div class="card-title">Databricks destination</div></div>
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
            <div style="display:flex;align-items:center;gap:10px;margin-top:10px">
              <button class="btn btn-secondary" id="run-dbx-test">Test Databricks connection</button>
              <span id="run-dbx-test-result" style="font-size:12px;color:var(--text-muted)"></span>
            </div>
          </div>
        </div>

        <!-- Airflow source orchestrator (optional): paste DAG source from the Airflow
             UI's Code tab — no server access needed. Converted alongside Glue workflows. -->
        <div class="card" style="margin-bottom:16px">
          <div class="card-header"><div class="card-title">Airflow DAGs (optional source orchestrator)</div></div>
          <div class="card-body">
            <div style="font-size:12px;color:var(--text-secondary);margin-bottom:8px">
              If Airflow orchestrates the source, paste the DAG (Python source or dag-factory YAML — auto-detected).
            </div>
            <textarea id="run-airflow-dag" spellcheck="false" placeholder="# Paste DAG .py source OR dag-factory .yaml here (leave empty to skip)"
              style="width:100%;min-height:110px;font-family:var(--font-mono,monospace);font-size:11px;background:var(--bg-inset,var(--bg-surface));color:var(--text-primary);border:1px solid var(--border);border-radius:8px;padding:10px;resize:vertical">${esc(localStorage.getItem('qvf_sfglue_airflow_dag') || '')}</textarea>
          </div>
        </div>

        <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
          <button class="btn btn-primary" id="run-start" style="font-weight:700">Run migration</button>
          <button class="btn btn-secondary" id="run-stop" style="display:none">Stop</button>
          <span id="run-status" style="font-size:12px;color:var(--text-muted)"></span>
        </div>
        <label style="display:flex;align-items:center;gap:8px;margin-bottom:16px;font-size:12.5px;color:var(--text-secondary);cursor:pointer">
          <input type="checkbox" id="run-handsfree" ${localStorage.getItem('qvf_sfglue_handsfree') === '1' ? 'checked' : ''}/>
          <span><strong>Hands-free</strong> — skip the checkpoint and run every phase without stopping</span>
        </label>

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
  // Direct mutation skips store.set()'s persistence hook, so ALSO write the
  // localStorage copy here — otherwise the Databricks values vanish on reload.
  const saveDest = () => {
    const d = readDest();
    store.get().sfGlueDestination = d;
    try { localStorage.setItem('qvf_sfglue_destination', JSON.stringify(d)); } catch (_) { /* non-fatal */ }
    return d;
  };
  container.querySelectorAll('#run-dbx-url,#run-dbx-token,#run-dbx-warehouse,#run-dbx-catalog,#run-dbx-bronze,#run-dbx-source-catalog')
    .forEach(inp => inp.addEventListener('change', saveDest));

  // Databricks connection test (token + warehouse + catalog, via REST — no SQL).
  container.querySelector('#run-dbx-test')?.addEventListener('click', async () => {
    const el = container.querySelector('#run-dbx-test-result');
    const dest = saveDest();
    el.textContent = 'testing…'; el.style.color = 'var(--text-muted)';
    try {
      const r = await api.testSfGlueDatabricks(dest);
      const c = r.checks || {};
      const bits = [`✓ ${(c.auth || {}).user || 'authenticated'}`];
      if (c.warehouse) bits.push(`warehouse ${c.warehouse.ok ? c.warehouse.state || 'OK' : '✗ ' + c.warehouse.state}`);
      if (c.catalog) bits.push(`catalog ${c.catalog.ok ? '✓ ' + c.catalog.name : '✗ not found'}`);
      el.textContent = bits.join(' · ');
      el.style.color = 'var(--success)';
    } catch (e) {
      el.textContent = '✗ ' + e.message;
      el.style.color = 'var(--error)';
    }
  });

  // Hands-free toggle (persisted).
  container.querySelector('#run-handsfree')?.addEventListener('change', (e) => {
    if (e.target.checked) localStorage.setItem('qvf_sfglue_handsfree', '1');
    else localStorage.removeItem('qvf_sfglue_handsfree');
  });

  // Persist the pasted Airflow DAG source across reloads (local demo machine).
  container.querySelector('#run-airflow-dag')?.addEventListener('change', (e) => {
    const v = e.target.value || '';
    if (v.trim()) localStorage.setItem('qvf_sfglue_airflow_dag', v);
    else localStorage.removeItem('qvf_sfglue_airflow_dag');
  });

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
  try { localStorage.setItem('qvf_sfglue_destination', JSON.stringify(d)); } catch (_) { /* non-fatal */ }
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

    // Hands-free mode: no pause — auto-approve and roll straight into build.
    if (localStorage.getItem('qvf_sfglue_handsfree') === '1') {
      setPhase('checkpoint', 'done', 'auto-approved (hands-free)');
      stage = 'running2';
      await continueRun(container);
      return;
    }
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
      <div class="card-header"><div class="card-title">Review checkpoint</div></div>
      <div class="card-body">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:10px">
          Conversion is done. Review the generated code, then continue — <strong>Build writes tables into your Databricks catalog</strong>.
        </div>
        <div style="display:flex;gap:18px;flex-wrap:wrap;margin-bottom:14px">
          ${counts.map(([l, n]) => `<span style="font-size:12px"><strong style="font-size:16px;color:var(--primary)">${n}</strong> ${esc(l)}</span>`).join('')}
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap">
          <button class="btn btn-secondary" id="run-review-adv">Open in Review &amp; Edit (advanced)</button>
          <button class="btn btn-primary" id="run-continue" style="font-weight:700">✓ Continue → Build &amp; Reconcile</button>
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
    const ok = results.filter(r => ['success', 'created', 'repaired'].includes(r.status)).length;
    setPhase('build', ok === results.length ? 'done' : 'error', `${ok}/${results.length} model(s) built`);

    setPhase('reconcile', 'running');
    const pairs = reconcilePairs(store.get());
    // No primary key required — verification runs schema + row-count + per-column
    // fingerprints on every table (keys, when present, add the dup/null-key check).
    if (pairs.length) {
      const { snowflake } = sourceConfigs(store.get());
      const rec = await api.reconcileSnowflakeGlueMigration({ snowflake, destination, pairs });
      store.get().sfGlueReconcile = rec;
      const passed = (rec.results || []).filter(r => r.passed).length;
      const total = (rec.results || []).length;
      setPhase('reconcile', total && passed === total ? 'done' : 'error',
               `${passed}/${total || pairs.length} table(s) match source`);
    } else {
      setPhase('reconcile', 'skipped', 'no tables to verify');
    }


    // ── Orchestration automation (gap-plan Phases 1/8): push artifacts, convert +
    // deploy the Glue workflows, then run the migrated Job as the verification gate.
    // Every step here is NON-FATAL: an environment hiccup marks the phase and the run
    // still completes (the report shows what needs attention).
    const conv = store.get().sfGlueConversion || {};
    let pushOk = false;
    setPhase('push', 'running');
    try {
      // If the pasted Airflow definition is YAML (dag-factory style), it is a source
      // CONFIG file \u2014 carry it into the workspace under <root>/conf/ as well.
      const afSrc = (localStorage.getItem('qvf_sfglue_airflow_dag') || '').trim();
      const isYaml = afSrc && !/^\s*(from|import)\s+\w/m.test(afSrc) && afSrc.includes('tasks:');
      const confFiles = isYaml ? { 'pipeline_dags.yaml': afSrc } : {};
      const pushRes = await api.pushSfGlueWorkspace({ destination, artifacts: conv, confFiles });
      pushOk = !!pushRes.success;
      setPhase('push', pushOk ? 'done' : 'error',
               `${pushRes.pushed || 0} file(s) \u2192 ${pushRes.root || '/Shared/sfglue'}` +
               (isYaml ? ' (+conf)' : ''));
    } catch (e) {
      setPhase('push', 'error', e.message);
    }

    let deployedJobs = [];
    setPhase('orchestrate', 'running');
    try {
      const glueCfg = sourceConfigs(store.get()).glue;
      const airflowSrc = (localStorage.getItem('qvf_sfglue_airflow_dag') || '').trim();
      const dagFiles = airflowSrc ? { 'pasted_dag.py': airflowSrc } : null;
      if (!glueCfg && !dagFiles) { setPhase('orchestrate', 'skipped', 'no Glue connection or Airflow DAG'); }
      else {
        // Pass 1 (bare) discovers the legacy task names from BOTH orchestrators so
        // one artifact map covers Glue workflows and Airflow DAGs alike.
        const bareJobs = [];
        if (glueCfg) {
          try { bareJobs.push(...((await api.planSfGlueWorkflows({ glue: glueCfg, destination })).jobs || [])); }
          catch (e) { if (e.status !== 404) throw e; }
        }
        if (dagFiles) bareJobs.push(...((await api.planSfGlueAirflow({ dagFiles, destination })).jobs || []));
        const amap = autoArtifactMap(conv, bareJobs);

        // Pass 2 (mapped) + one combined deploy.
        const plannedJobs = [];
        if (glueCfg) {
          try { plannedJobs.push(...((await api.planSfGlueWorkflows({ glue: glueCfg, destination, artifactMap: amap })).jobs || [])); }
          catch (e) { if (e.status !== 404) throw e; }
        }
        if (dagFiles) plannedJobs.push(...((await api.planSfGlueAirflow({ dagFiles, destination, artifactMap: amap })).jobs || []));
        if (!plannedJobs.length) { setPhase('orchestrate', 'skipped', 'no workflows or DAGs found'); }
        else {
          // Carry the selected pipeline bucket + region so the deployed notebook
          // tasks get real widget parameters (S3_VENDOR_BUCKET etc.).
          const deployDest = {
            ...destination,
            pipeline_bucket: store.get().sfGlueSelectedBucket || '',
            aws_region: (store.get().sfGlueGlueConfig || {}).region || '',
          };
          const dep = await api.deploySfGlueWorkflows({ destination: deployDest, jobs: plannedJobs.map(j => j.job) });
          deployedJobs = (dep.results || []).filter(r => r.success);
          store.get().sfGlueWorkflows = { planned: plannedJobs, deployed: dep.results };
          const nAf = plannedJobs.filter(j => j.source === 'airflow').length;
          setPhase('orchestrate', dep.success ? 'done' : 'error',
                   `${deployedJobs.length}/${(dep.results || []).length} job(s) deployed` +
                   (nAf ? ` (${nAf} from Airflow)` : ''));
        }
      }
    } catch (e) {
      setPhase('orchestrate', e.status === 404 ? 'skipped' : 'error',
               e.status === 404 ? 'no Glue workflows found' : e.message);
    }

    setPhase('workflow_run', 'running', 'running the migrated job\u2026');
    try {
      if (!pushOk || !deployedJobs.length) {
        setPhase('workflow_run', 'skipped', !pushOk ? 'artifacts not pushed' : 'nothing deployed');
      } else {
        // 300s budget: enough for the migrated job's happy path; a hanging run gets
        // reported as TIMEOUT instead of eating 25 minutes of the demo.
        const verdict = await api.runSfGlueWorkflow({
          destination, jobId: deployedJobs[0].job_id, timeoutSeconds: 300 });
        store.get().sfGlueWorkflowRun = verdict;
        const failed = (verdict.tasks || []).filter(t => t.state && t.state !== 'SUCCESS');
        setPhase('workflow_run', verdict.success ? 'done' : 'error',
                 verdict.success ? `run ${verdict.run_id}: all tasks green`
                                 : `${verdict.state}${failed.length ? ' \u2014 ' + failed.map(t => t.task_key).join(', ') : ''}`);
      }
    } catch (e) {
      setPhase('workflow_run', 'error', e.message);
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
      <div class="card-header"><div class="card-title">Migration run complete</div></div>
      <div class="card-body">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:12px">
          Conversion built into Databricks. Open the report for the full summary, fidelity grade, and artifacts.
        </div>
        <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center">
          <button class="btn btn-primary" id="run-report" style="font-weight:700">View report →</button>
          <select id="run-airflow-dbt-src" style="padding:6px 8px;border:1px solid var(--border);border-radius:6px;font-size:12px;background:var(--bg-surface);color:var(--text-primary)">
            <option value="workspace">dbt: workspace (demo)</option>
            <option value="git">dbt: git source</option>
            <option value="dbt_cloud">dbt: dbt Cloud</option>
          </select>
          <button class="btn btn-secondary" id="run-airflow-dag-dl">Download Airflow DAG (target)</button>
          <button class="btn btn-secondary" id="run-again">Run again</button>
        </div>
        <div id="run-airflow-dag-note" style="font-size:11px;color:var(--text-muted);margin-top:8px"></div>
      </div>
    </div>`;
  box.querySelector('#run-report')?.addEventListener('click', () => store.navigate('sfglue-report'));
  box.querySelector('#run-again')?.addEventListener('click', () => { resetRun(); store.navigate('sfglue-run'); });
  box.querySelector('#run-airflow-dag-dl')?.addEventListener('click', async () => {
    const note = box.querySelector('#run-airflow-dag-note');
    note.textContent = 'generating…';
    try {
      const conv = store.get().sfGlueConversion || {};
      const dbtSource = box.querySelector('#run-airflow-dbt-src')?.value || 'workspace';
      let gitUrl, dbtCloudJobId;
      if (dbtSource === 'git') {
        const res = await promptModal({
          title: 'Airflow DAG — git source',
          message: 'Git repo URL for the dbt project:',
          fields: [{
            id: 'gitUrl', label: 'Git repo URL', type: 'text',
            placeholder: 'https://github.com/your-org/cdl-dbt.git',
            value: localStorage.getItem('qvf_dbt_git_url') || 'https://github.com/your-org/cdl-dbt.git',
          }],
          confirmLabel: 'Generate',
        });
        if (!res) { note.textContent = ''; return; }
        gitUrl = (res.gitUrl || '').trim() || undefined;
        if (gitUrl) localStorage.setItem('qvf_dbt_git_url', gitUrl);
      }
      if (dbtSource === 'dbt_cloud') {
        const res = await promptModal({
          title: 'Airflow DAG — dbt Cloud',
          message: 'dbt Cloud job ID to trigger:',
          fields: [{
            id: 'dbtCloudJobId', label: 'dbt Cloud job ID', type: 'text',
            placeholder: 'e.g. 123456',
            value: localStorage.getItem('qvf_dbt_cloud_job_id') || '',
          }],
          confirmLabel: 'Generate',
        });
        if (!res) { note.textContent = ''; return; }
        dbtCloudJobId = (res.dbtCloudJobId || '').trim() || undefined;
        if (dbtCloudJobId) localStorage.setItem('qvf_dbt_cloud_job_id', dbtCloudJobId);
      }
      const out = await api.emitTargetAirflow({ artifacts: conv, destination: currentDest(),
                                                dbtSource, gitUrl, dbtCloudJobId });
      const blob = new Blob([out.yaml], { type: 'text/yaml' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = (out.name || 'cdl_migrated_databricks') + '.yaml';
      a.click();
      URL.revokeObjectURL(a.href);
      note.textContent = `dag-factory YAML for ${out.name} — ${out.tasks.length} tasks, dbt layers: ${(out.layers || []).join(' → ')}. Drop it in your Airflow dags folder.`;
    } catch (e) {
      note.textContent = '✗ ' + e.message;
    }
  });
}

function onRunError(container, err) {
  setBusyUI(container, false);
  const statusEl = container.querySelector('#run-status');
  if (statusEl) statusEl.textContent = 'Stopped';
  // Mark the currently-running phase as failed.
  const running = PHASES.find(p => (phaseState[p.key] || {}).status === 'running');
  if (running) setPhase(running.key, 'error', err && err.name === 'AbortError' ? 'stopped' : (err.message || 'failed'));
  const el = container.querySelector('#run-error');
  if (el && !(err && err.name === 'AbortError')) el.textContent = (err.message || 'Run failed');
  if (err && err.name === 'AbortError') notify('Run stopped.', { kind: 'warning', title: 'Stopped' });
  else notify(err.message || 'Run failed', { kind: 'error', title: 'Migration run failed' });
}


// Derive the workflow task → converted-artifact mapping automatically, so orchestration
// deploys with real targets instead of placeholders. Name-based, source-agnostic:
// batch-control jobs → framework notebooks; jobs with a converted notebook → notebook;
// remaining transformation jobs → the dbt model set.
function autoArtifactMap(conv, plannedJobs) {
  const nbs = Object.keys(conv.notebooks || {});
  const models = Object.keys(conv.dbt_models || {});
  const tJobs = new Set(((conv.plan || {}).transformation_jobs) || []);
  const map = {};
  const legacyNames = new Set();
  (plannedJobs || []).forEach(j => (j.dag && j.dag.tasks || []).forEach(t => legacyNames.add(t.legacy_name)));
  legacyNames.forEach(name => {
    if (!name) return;
    const low = String(name).toLowerCase();
    if (low.includes('batch_open') && nbs.includes('fw_batch_open.py')) { map[name] = { kind: 'framework', notebook: 'fw_batch_open.py' }; return; }
    if (low.includes('batch_close') && nbs.includes('fw_batch_close.py')) { map[name] = { kind: 'framework', notebook: 'fw_batch_close.py' }; return; }
    if (low.includes('load_conf') && nbs.includes('fw_file_audit.py')) { map[name] = { kind: 'framework', notebook: 'fw_file_audit.py' }; return; }
    const nb = nbs.find(n => n === name + '.py') || nbs.find(n => n === name + '__transform.py')
             || nbs.find(n => n.startsWith(name + '.') || n.startsWith(name + '__'));
    if (nb) { map[name] = { kind: 'notebook', path: nb }; return; }
    if (tJobs.has(name) && models.length) { map[name] = { kind: 'dbt', models }; }
  });
  return map;
}

export function destroySfGlueRunPage() { /* module-level run state persists intentionally */ }
