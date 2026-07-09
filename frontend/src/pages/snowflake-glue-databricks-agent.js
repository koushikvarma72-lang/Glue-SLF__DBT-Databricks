/**
 * Snowflake/Glue → Databricks/DBT — Databricks Agent.
 *
 * The single home for everything Databricks: the workspace connection + destination
 * (catalog / schemas), the "Check Databricks" precheck, the reviewed bronze
 * extract/load notebooks + table DDL (+ migration notes), and the "Deploy table DDL"
 * action that creates the tables in Unity Catalog via the SQL Warehouse. The
 * destination's catalog/schema names are what "Generate conversion" on Review & Edit
 * writes into the generated code, so they live here, in one place.
 */
import { api } from '../api.js';
import { store } from '../store.js';
import { esc, artifactGroup, wireArtifacts, field as df } from '../components/ui.js';
import { notify } from '../components/notify.js';
import { confirmModal } from '../components/modal.js';

function renderPrecheck(pre) {
  if (!pre) return '';
  const row = t => `<span style="font-family:monospace">${esc(t.source)}</span> <span class="badge" style="font-size:9px">${esc(t.layer)}</span>`;
  // Make the connection result unambiguous: an empty "already in Databricks" list
  // could mean "connected, nothing there" OR "couldn't connect" — say which.
  const status = pre.introspection_error
    ? `<div style="font-size:12px;color:var(--danger,#dc2626);margin-bottom:6px">⚠ Couldn't read Unity Catalog — check Workspace URL / token / SQL Warehouse. <span style="color:var(--text-muted)">${esc(pre.introspection_error)}</span></div>`
    : `<div style="font-size:12px;color:var(--success,#16a34a);margin-bottom:6px">✓ Connected to Databricks — read the destination catalog successfully.</div>`;
  return `<div style="margin-top:10px;padding:10px;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px">
    ${status}
    <div style="display:flex;gap:18px;flex-wrap:wrap;font-size:12px">
      <span><strong>To migrate:</strong> ${pre.to_migrate.length ? pre.to_migrate.map(row).join(' · ') : 'none'}</span>
      <span style="color:var(--text-muted)"><strong>Already in Databricks:</strong> ${pre.already_present.length ? pre.already_present.map(t => esc(t.source)).join(', ') : 'none'}</span>
    </div>
  </div>`;
}

// The prominent pass/fail signal is now the toast notification; these inline blocks are
// collapsed DETAIL-on-demand so the same outcome isn't shouted twice (badge + toast).
// They auto-expand only on failure (where you need to see which row broke).
function renderDeployResults(dep) {
  if (!dep) return '';
  if (!dep.results) {
    return `<details style="border:1px solid var(--border);border-radius:8px;background:var(--bg-primary)">
      <summary style="padding:8px 12px;cursor:pointer;font-size:12px;color:var(--danger,#dc2626)">⚠ Deploy failed — details</summary>
      <div style="padding:8px 12px;border-top:1px solid var(--border);font-size:12px;white-space:pre-wrap">${esc(dep.error || 'Deploy failed')}</div>
    </details>`;
  }
  const rows = dep.results.map(r =>
    `<div style="font-size:12px;font-family:monospace">${r.success ? '✅' : '❌'} ${esc(r.target)} <span style="color:var(--text-muted)">${esc(r.message || '')}</span></div>`).join('');
  return `<details ${dep.success ? '' : 'open'} style="border:1px solid var(--border);border-radius:8px;background:var(--bg-primary)">
    <summary style="padding:8px 12px;cursor:pointer;font-size:12px;color:var(--text-secondary)">${dep.success ? '✅' : '⚠'} Deploy results — ${esc(dep.summary || `${dep.results.length} table(s)`)}</summary>
    <div style="padding:8px 12px;border-top:1px solid var(--border)">${rows}</div>
  </details>`;
}

function renderSeedResults(sd) {
  if (!sd) return '';
  if (!sd.results) {
    return `<details open style="border:1px solid var(--border);border-radius:8px;background:var(--bg-primary)">
      <summary style="padding:8px 12px;cursor:pointer;font-size:12px;color:var(--danger,#dc2626)">⚠ Seed failed — details</summary>
      <div style="padding:8px 12px;border-top:1px solid var(--border);font-size:12px;white-space:pre-wrap">${esc(sd.error || 'Seed failed')}</div>
    </details>`;
  }
  const rows = sd.results.map(r =>
    `<div style="font-size:12px;font-family:monospace">${r.status === 'ok' ? '✅' : '❌'} ${esc(r.name)}${r.message ? ' <span style="color:var(--text-muted)">: ' + esc(r.message) + '</span>' : ''}</div>`).join('');
  return `<details ${sd.success ? '' : 'open'} style="border:1px solid var(--border);border-radius:8px;background:var(--bg-primary)">
    <summary style="padding:8px 12px;cursor:pointer;font-size:12px;color:var(--text-secondary)">${sd.success ? '✅' : '⚠'} Bronze seed — ${esc(sd.summary || `${sd.results.length} statement(s)`)}</summary>
    <div style="padding:8px 12px;border-top:1px solid var(--border)">${rows}</div>
  </details>`;
}

function renderBuildResults(bld) {
  if (!bld) return '';
  if (!bld.results) {
    return `<details style="border:1px solid var(--border);border-radius:8px;background:var(--bg-primary)">
      <summary style="padding:8px 12px;cursor:pointer;font-size:12px;color:var(--danger,#dc2626)">⚠ Build failed — details</summary>
      <div style="padding:8px 12px;border-top:1px solid var(--border);font-size:12px;white-space:pre-wrap">${esc(bld.error || 'Build failed')}</div>
    </details>`;
  }
  const icon = s => (s === 'created' ? '✅' : (s === 'repaired' ? '🔧' : (s === 'failed' ? '❌' : '⏭')));
  // A model the AI auto-fixed against the real upstream columns then re-ran successfully.
  const repairBadge = r => {
    if (r.status !== 'repaired') return '';
    const n = Number(r.repair_attempts) || 1;
    return ` <span class="badge badge-success" style="font-size:9px">🔧 auto-repaired (${n} attempt${n === 1 ? '' : 's'})</span>`;
  };
  const rows = bld.results.map(r =>
    `<div style="font-size:12px;font-family:monospace">${icon(r.status)} ${esc(r.name)} <span style="color:var(--text-muted)">→ ${esc(r.status)}${r.message ? ': ' + esc(r.message) : ''}</span>${repairBadge(r)}</div>`).join('');
  return `<details ${bld.success ? '' : 'open'} style="border:1px solid var(--border);border-radius:8px;background:var(--bg-primary)">
    <summary style="padding:8px 12px;cursor:pointer;font-size:12px;color:var(--text-secondary)">${bld.success ? '✅' : '⚠'} Build results — ${esc(bld.summary || `${bld.results.length} model(s)`)}</summary>
    <div style="padding:8px 12px;border-top:1px solid var(--border)">${rows}</div>
  </details>`;
}

// ── Action-outcome toasts. Each mirrors what its inline result box shows, so a long step's
// result is visible even after scrolling away. Defensive about result shape (read the same
// keys the render* fns above read, with fallbacks) so a toast can't claim a false success.
function notifyPrecheck(pre) {
  if (!pre) return;
  if (pre.introspection_error) {
    notify("Couldn't read Unity Catalog — check Workspace URL / token / SQL Warehouse ID.",
      { kind: 'error', title: 'Precheck failed' });
    return;
  }
  notify(`Connected — ${(pre.to_migrate || []).length} table(s) to migrate.`, { kind: 'success', title: 'Precheck' });
}
function notifyDeploy(dep, n) {
  if (!dep || !dep.results) { notify((dep && dep.error) || 'Deploy failed', { kind: 'error', title: 'Deploy failed' }); return; }
  // On a hard failure (e.g. schema create failed) the server returns no summary and 0
  // tables were created — don't claim "N deployed". Only the success path knows N landed.
  notify(dep.success ? (dep.summary || `${n} table(s) deployed.`) : (dep.summary || 'Deploy did not complete — see results.'),
    { kind: dep.success ? 'success' : 'warning', title: dep.success ? 'Deploy complete' : 'Deploy partial' });
}
function notifyBuild(bld) {
  if (!bld || !bld.results) { notify((bld && bld.error) || 'Build failed', { kind: 'error', title: 'Build failed' }); return; }
  notify(bld.summary || (bld.success ? 'All models built.' : 'Some models failed to build.'),
    { kind: bld.success ? 'success' : 'warning', title: bld.success ? 'Build complete' : 'Build partial' });
}

export function renderSfGlueDatabricksAgentPage(container) {
  const state = store.get();
  const conv = state.sfGlueConversion;
  const dest = state.sfGlueDestination || {};
  const edits = state.sfGlueArtifactEdits || {};
  const explains = state.sfGlueArtifactExplain || {};
  const busyPre = state.isPrecheckingSfGlue;
  const busyDeploy = state.isDeployingSfGlue;
  const busyBuild = state.isBuildingSfGlue;
  const busySeed = state.isSeedingSfGlue;
  const notebooks = (conv && conv.notebooks) || {};
  const ddl = (conv && conv.ddl) || {};
  const dbtModels = (conv && conv.dbt_models) || {};
  const notes = (conv && conv.notes) || {};
  const artifactCount = Object.keys(notebooks).length + Object.keys(ddl).length + Object.keys(notes).length;

  container.innerHTML = `
    <div class="page" style="overflow:auto;padding:24px;width:100%">
      <div style="max-width:1000px;margin:0 auto">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
          <button class="btn btn-secondary" id="dbx-back" style="padding:4px 10px">← Review & Edit</button>
          <h2 style="margin:0">Databricks Agent</h2>
        </div>
        <p style="color:var(--text-secondary);margin:0 0 14px">
          Connect your Databricks workspace and set the destination, then review the bronze notebooks and table
          DDL. The <strong>Catalog</strong> and <strong>schema</strong> names below are what
          <strong>Generate conversion</strong> (on Review &amp; Edit) writes into the generated code.
        </p>

        <!-- Databricks connection + destination (the single place for Databricks config) -->
        <div style="border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:14px;background:var(--bg-surface)">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px">
            <strong style="font-size:13px">⚙️ Databricks destination${dest.catalog ? ` — ${esc(dest.catalog)}` : ''}</strong>
            <button class="btn btn-secondary" id="dbx-precheck" ${busyPre ? 'disabled' : ''} style="margin-left:auto;padding:4px 10px;font-size:12px">${busyPre ? 'Checking…' : '<span aria-hidden="true">🔎</span> Check Databricks'}</button>
          </div>
          <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px">
            ${df('dbx-dest-url', 'Workspace URL', dest.workspace_url, { placeholder: 'https://dbc-xxxx.cloud.databricks.com' })}
            ${df('dbx-dest-token', 'Access token', dest.token, { type: 'password', placeholder: dest.token ? '•••••• (saved)' : 'dapi…' })}
            ${df('dbx-dest-warehouse', 'SQL Warehouse ID', dest.sql_warehouse_id, {})}
            ${df('dbx-dest-catalog', 'Catalog', dest.catalog, { placeholder: 'lakehouse' })}
            ${df('dbx-dest-bronze', 'Bronze schema', dest.bronze_schema, { placeholder: 'bronze' })}
            ${df('dbx-dest-silver', 'Silver schema', dest.silver_schema, { placeholder: 'silver' })}
            ${df('dbx-dest-gold', 'Gold schema', dest.gold_schema, { placeholder: 'gold' })}
            ${df('dbx-dest-source-catalog', 'Source catalog', dest.source_catalog, { placeholder: 'raw_catalog' })}
            ${df('dbx-dest-source-schema', 'Source schema', dest.source_schema, { placeholder: 'raw' })}
          </div>
          <div style="font-size:11px;color:var(--text-muted);margin-top:6px">
            <strong>Source catalog/schema</strong> is the raw landing location your <code>{{ source('bronze', …) }}</code> models read from. Used by <strong><span aria-hidden="true">🏗</span> Build models</strong>. Leave blank to use the Catalog / Bronze schema above.
          </div>
          <div id="dbx-error" role="status" aria-live="polite" style="color:var(--danger,#dc2626);font-size:12px;margin-top:8px"></div>
          ${state.sfGluePrecheck ? renderPrecheck(state.sfGluePrecheck) : ''}
        </div>

        ${conv && Object.keys(ddl).length ? `
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;flex-wrap:wrap">
            <button class="btn btn-primary" id="dbx-deploy" ${busyDeploy ? 'disabled' : ''} style="padding:6px 14px;font-size:13px;font-weight:700">${busyDeploy ? '<span aria-hidden="true">🚀</span> Deploying…' : `<span aria-hidden="true">🚀</span> Deploy ${Object.keys(ddl).length} table(s) to Databricks`}</button>
            <span style="font-size:12px;color:var(--text-muted)">Creates the table DDL in <code>${esc(dest.catalog || 'lakehouse')}</code> via your SQL Warehouse. Run <strong><span aria-hidden="true">🔎</span> Check Databricks</strong> first to skip ones already there. Bronze notebooks &amp; dbt models are copy/run for now.</span>
          </div>
          <div id="dbx-deploy-results" role="status" aria-live="polite" style="margin-bottom:14px">${state.sfGlueDeploy ? renderDeployResults(state.sfGlueDeploy) : ''}</div>
        ` : `
          <div class="badge badge-info" style="display:block;text-align:left;white-space:normal;padding:10px;margin-bottom:14px;font-size:12px">
            Run <strong><span aria-hidden="true">⚡</span> Generate conversion</strong> on Review &amp; Edit to produce the table DDL, then deploy it here.
          </div>
        `}

        ${conv && Object.keys(dbtModels).length ? `
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;flex-wrap:wrap">
            <button class="btn" id="dbx-seed-bronze" ${busySeed ? 'disabled' : ''} style="padding:6px 14px;font-size:13px;font-weight:700">${busySeed ? '<span aria-hidden="true">🌱</span> Seeding…' : '<span aria-hidden="true">🌱</span> Load sample bronze data'}</button>
            <span style="font-size:12px;color:var(--text-muted)">Lands a small, referentially-consistent sample dataset into the bronze schema (columns derived from your models) so Build can run end-to-end without the real S3 ingestion. Demo/dev only — production uses the Bronze ingestion notebook.</span>
          </div>
          <div id="dbx-seed-results" role="status" aria-live="polite" style="margin-bottom:14px">${state.sfGlueSeedBronze ? renderSeedResults(state.sfGlueSeedBronze) : ''}</div>
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;flex-wrap:wrap">
            <button class="btn btn-primary" id="dbx-build" ${busyBuild ? 'disabled' : ''} style="padding:6px 14px;font-size:13px;font-weight:700">${busyBuild ? '<span aria-hidden="true">🏗</span> Building…' : `<span aria-hidden="true">🏗</span> Build ${Object.keys(dbtModels).length} model(s)`}</button>
            <span style="font-size:12px;color:var(--text-muted)">Populates the migrated tables — runs the dbt models in dependency order on your SQL Warehouse (resolving <code>{{ ref }}</code>/<code>{{ source }}</code> to real tables). Deploy the table DDL first so the targets exist.</span>
          </div>
          <div id="dbx-build-results" role="status" aria-live="polite" style="margin-bottom:14px">${state.sfGlueBuild ? renderBuildResults(state.sfGlueBuild) : ''}</div>
        ` : ''}

        ${conv ? `
          ${artifactGroup('Bronze extract/load notebooks', notebooks, 'notebook', edits, explains)}
          ${artifactGroup('Databricks table DDL', ddl, 'DDL', edits, explains)}
          ${Object.keys(notes).length ? artifactGroup('Migration notes (review before keeping)', notes, 'note', edits, explains) : ''}
          ${artifactCount ? '' : '<div style="color:var(--text-muted);font-size:13px">No Databricks artifacts in this conversion (no ingestion jobs or Snowflake tables in scope).</div>'}
        ` : `
          <div style="color:var(--text-muted);font-size:14px;padding:24px;text-align:center;border:1px dashed var(--border);border-radius:10px">
            Run <strong>⚡ Generate conversion</strong> on the Review &amp; Edit step to produce the bronze notebooks and table DDL.
          </div>
        `}

        <div style="margin-top:22px"><button class="btn btn-primary" id="dbx-to-dbt">DBT Agent →</button></div>
      </div>
    </div>`;

  wireArtifacts(container);
  container.querySelector('#dbx-back')?.addEventListener('click', () => store.navigate('sfglue-review'));
  container.querySelector('#dbx-to-dbt')?.addEventListener('click', () => store.navigate('sfglue-dbt-agent'));

  // Postgres → bronze ingestion is now generated automatically during conversion (when a
  // Postgres source is connected) and appears under "Bronze extract/load notebooks" — no
  // manual button here anymore.

  const readDest = () => ({
    workspace_url: container.querySelector('#dbx-dest-url').value.trim(),
    token: container.querySelector('#dbx-dest-token').value || dest.token || '',
    sql_warehouse_id: container.querySelector('#dbx-dest-warehouse').value.trim(),
    catalog: container.querySelector('#dbx-dest-catalog').value.trim() || 'lakehouse',
    bronze_schema: container.querySelector('#dbx-dest-bronze').value.trim() || 'bronze',
    silver_schema: container.querySelector('#dbx-dest-silver').value.trim() || 'silver',
    gold_schema: container.querySelector('#dbx-dest-gold').value.trim() || 'gold',
    // Raw landing location for {{ source('bronze', …) }}. Empty → server falls back to
    // catalog / bronze_schema, preserving behavior for destinations set before this field.
    source_catalog: container.querySelector('#dbx-dest-source-catalog').value.trim(),
    source_schema: container.querySelector('#dbx-dest-source-schema').value.trim(),
  });

  // Persist destination edits in-session on blur WITHOUT a re-render, so tabbing
  // between fields isn't disrupted. Check Databricks (below) and the Review & Edit
  // "Generate conversion" step both read store.sfGlueDestination.
  container.querySelectorAll('#dbx-dest-url,#dbx-dest-token,#dbx-dest-warehouse,#dbx-dest-catalog,#dbx-dest-bronze,#dbx-dest-silver,#dbx-dest-gold,#dbx-dest-source-catalog,#dbx-dest-source-schema')
    .forEach(inp => inp.addEventListener('change', () => { store.get().sfGlueDestination = readDest(); }));

  container.querySelector('#dbx-precheck')?.addEventListener('click', async () => {
    const lineage = state.sfGlueLineage && state.sfGlueLineage.lineage;
    const sel = store.get().sfGlueSelectedTables || [];
    const err = container.querySelector('#dbx-error');
    if (!sel.length) { if (err) err.textContent = 'Select tables on the Review & Edit step first.'; return; }
    const destination = readDest();
    store.set({ sfGlueDestination: destination, isPrecheckingSfGlue: true });
    try {
      const result = await api.precheckSnowflakeGlueMigration({ lineage, selectedIds: sel, destination });
      store.set({ sfGluePrecheck: result, isPrecheckingSfGlue: false });
      notifyPrecheck(result);
    } catch (e) {
      store.set({ isPrecheckingSfGlue: false });
      const el = container.querySelector('#dbx-error'); if (el) el.textContent = '⚠ ' + e.message;
      notify(e.message, { kind: 'error', title: 'Precheck failed' });
    }
  });

  container.querySelector('#dbx-deploy')?.addEventListener('click', async () => {
    const err = container.querySelector('#dbx-error');
    if (err) err.textContent = '';
    const destination = readDest();
    if (!destination.workspace_url || !destination.sql_warehouse_id) {
      if (err) err.textContent = 'Set the Workspace URL and SQL Warehouse ID before deploying.';
      return;
    }
    // Deploy the DDL the user sees — apply any edits made in the DDL artifact editors.
    const editsNow = store.get().sfGlueArtifactEdits || {};
    const ddlMap = {};
    Object.entries((store.get().sfGlueConversion || {}).ddl || {}).forEach(([k, v]) => {
      ddlMap[k] = (`DDL:${k}` in editsNow) ? editsNow[`DDL:${k}`] : v;
    });
    const n = Object.keys(ddlMap).length;
    if (!n) return;
    if (!(await confirmModal('This runs CREATE TABLE statements on your SQL Warehouse.',
      { title: `Deploy ${n} table(s) into ${destination.catalog || 'lakehouse'}?`, confirmLabel: 'Deploy', danger: true }))) return;
    store.set({ sfGlueDestination: destination, isDeployingSfGlue: true });
    try {
      const result = await api.deploySnowflakeGlueMigration({ destination, ddl: ddlMap });
      store.set({ sfGlueDeploy: result, isDeployingSfGlue: false });
      notifyDeploy(result, n);
    } catch (e) {
      store.set({ sfGlueDeploy: { error: e.message }, isDeployingSfGlue: false });
      notify(e.message, { kind: 'error', title: 'Deploy failed' });
    }
  });

  container.querySelector('#dbx-build')?.addEventListener('click', async () => {
    const err = container.querySelector('#dbx-error');
    if (err) err.textContent = '';
    const destination = readDest();
    if (!destination.workspace_url || !destination.sql_warehouse_id) {
      if (err) err.textContent = 'Set the Workspace URL and SQL Warehouse ID before building.';
      return;
    }
    // Build the models the user sees — apply any edits made in the dbt model editors
    // (same edit-key convention the DBT Agent page uses: "dbt model:<name>").
    const editsNow = store.get().sfGlueArtifactEdits || {};
    const models = {};
    Object.entries((store.get().sfGlueConversion || {}).dbt_models || {}).forEach(([k, v]) => {
      models[k] = (`dbt model:${k}` in editsNow) ? editsNow[`dbt model:${k}`] : v;
    });
    const n = Object.keys(models).length;
    if (!n) return;
    if (!(await confirmModal('Runs CREATE OR REPLACE TABLE/VIEW statements in dependency order on your SQL Warehouse and populates the migrated tables.',
      { title: `Build ${n} model(s) into ${destination.catalog || 'lakehouse'}?`, confirmLabel: 'Build', danger: true }))) return;
    store.set({ sfGlueDestination: destination, isBuildingSfGlue: true });
    try {
      const result = await api.buildSnowflakeGlueMigration({ destination, models, glue: store.get().sfGlueGlueConfig || {} });
      store.set({ sfGlueBuild: result, isBuildingSfGlue: false });
      notifyBuild(result);
    } catch (e) {
      store.set({ sfGlueBuild: { error: e.message }, isBuildingSfGlue: false });
      notify(e.message, { kind: 'error', title: 'Build failed' });
    }
  });

  container.querySelector('#dbx-seed-bronze')?.addEventListener('click', async () => {
    const destination = readDest();
    if (!destination.workspace_url || !destination.sql_warehouse_id) {
      notify('Set the Workspace URL and SQL Warehouse ID first.', { kind: 'warning', title: 'Not configured' });
      return;
    }
    // Same model set Build uses (with editor edits applied) — so the seed columns match
    // exactly what Build will read.
    const editsNow = store.get().sfGlueArtifactEdits || {};
    const models = {};
    Object.entries((store.get().sfGlueConversion || {}).dbt_models || {}).forEach(([k, v]) => {
      models[k] = (`dbt model:${k}` in editsNow) ? editsNow[`dbt model:${k}`] : v;
    });
    if (!Object.keys(models).length) { notify('Generate the conversion first.', { kind: 'warning', title: 'Nothing to seed' }); return; }
    if (!(await confirmModal('Creates the raw bronze tables and inserts a few sample rows on your SQL Warehouse so Build can run end-to-end without the real S3 ingestion. Demo/dev only.',
      { title: 'Load sample bronze data?', confirmLabel: 'Load sample data' }))) return;
    store.set({ sfGlueDestination: destination, isSeedingSfGlue: true });
    try {
      const result = await api.seedBronzeSampleData({ destination, models });
      store.set({ sfGlueSeedBronze: result, isSeedingSfGlue: false });
      notify(result.success ? (result.summary || 'Sample bronze data loaded.') : (result.summary || 'Seed did not complete — see results.'),
        { kind: result.success ? 'success' : 'warning', title: 'Sample bronze' });
    } catch (e) {
      store.set({ sfGlueSeedBronze: { error: e.message }, isSeedingSfGlue: false });
      notify(e.message, { kind: 'error', title: 'Seed bronze failed' });
    }
  });
}

export function destroySfGlueDatabricksAgentPage() {
  /* no graphs/timers */
}
