/**
 * Snowflake/Glue → Databricks/DBT — Review & Edit (single screen, modelled on the
 * Qlik Review & Edit page). Everything happens here:
 *
 *   TOP    — Project Tables (select what to migrate, left) + Dependency Map (right).
 *   BOTTOM — SOURCE (Glue jobs & Snowflake SQL, View/Edit/Explain, left)
 *            ↔ GENERATED Databricks/dbt output in tabs (View/Edit/Explain, right).
 *   Plus the "Generate conversion" (Migrate) button — table selection + Migrate live here.
 *
 * The Databricks *connection/destination* config and "Check Databricks" live on the
 * Databricks Agent step (one home for Databricks connection details); the generated
 * artifacts themselves are previewed here AND on the agent steps.
 *
 * The dependency-map graph and the table list stay mounted across right-pane tab
 * switches and inline selection so the d3 layout / scroll position isn't reset.
 */
import { api } from '../api.js';
import { store } from '../store.js';
import { renderLineageFlow } from '../components/lineageFlow.js';
import { esc, renderTabs, codeArtifact, wireArtifacts, reviewQueuePanel, wireReviewQueue } from '../components/ui.js';
import { notify } from '../components/notify.js';

// Summarize a finished conversion as a toast (it's the longest step — the result should be
// visible even if the operator switched away while it ran).
function notifyConversion(result) {
  if (!result) return;
  const models = Object.keys(result.dbt_models || {}).length;
  const review = (result.untranslatable || []).length;
  const errs = result.errors ? Object.keys(result.errors).length : 0;
  const msg = `${models} dbt model(s)` + (review ? ` · ${review} need review` : '') + (errs ? ` · ${errs} fetch warning(s)` : '');
  notify(msg, { kind: errs ? 'warning' : 'success', title: 'Conversion ready' });
}

// Tabs for the GENERATED-output pane (right side, bottom). The Databricks *connection*
// config lives on the Databricks Agent step; the generated artifacts are previewed here.
let genTab = 'dbt';
// Lets the Stop button abort an in-flight "Generate conversion" request. Module-level so
// the abort survives the store.set re-render that happens when conversion starts.
let _convertAbort = null;
const GEN_TABS = [
  { id: 'dbt', label: '💎 dbt models' },
  { id: 'ddl', label: '🗄️ Databricks DDL' },
  { id: 'notebooks', label: '🔥 PySpark notebooks' },
  { id: 'tests', label: '🧪 Tests & contracts' },
  { id: 'sources', label: '📄 sources.yml' },
  { id: 'notes', label: '📝 Notes' },
];

function destroyGraph() {
  /* Mermaid renders into the page container, cleared by the shell on navigation. */
}

function renderGeneratedBody(state) {
  const conv = state.sfGlueConversion;
  if (!conv) return emptyBox('Pick tables on the left, then click <strong>⚡ Generate conversion</strong> to produce dbt models, Databricks DDL and bronze notebooks here.');
  const edits = state.sfGlueArtifactEdits || {};
  const explains = state.sfGlueArtifactExplain || {};
  // The hard-20% review queue heads the generated pane: nothing ships until it is empty
  // and reconciliation passes (the gate lives on the DBT Agent step).
  const queue = reviewQueuePanel(conv.untranslatable);
  const grp = (obj, kind) => {
    const keys = Object.keys(obj || {});
    return keys.length ? keys.map(k => codeArtifact(`${kind}:${k}`, k, obj[k], kind, edits, explains)).join('') : null;
  };
  if (genTab === 'dbt') return queue + (grp(conv.dbt_models, 'dbt model') || emptyBox('No dbt models in this conversion.'));
  if (genTab === 'ddl') return grp(conv.ddl, 'DDL') || emptyBox('No Databricks DDL in this conversion.');
  if (genTab === 'notebooks') return grp(conv.notebooks, 'notebook') || emptyBox('No PySpark notebooks (no ingestion or procedural-transform jobs in scope).');
  if (genTab === 'notes') return grp(conv.notes, 'note') || emptyBox('No notes. (Publish / reverse-ETL jobs that are obsolete on Databricks are flagged here.)');
  if (genTab === 'tests') {
    const parts = [];
    if (conv.schema_yml) parts.push(codeArtifact('schema_yml:schema.yml', 'schema.yml — key/grain tests + enforced contracts', conv.schema_yml, 'dbt tests', edits, explains));
    if (conv.unit_tests_yml) parts.push(codeArtifact('unit_tests_yml:unit_tests.yml', 'unit_tests.yml — pre-build logic tests (fill expected rows)', conv.unit_tests_yml, 'dbt unit tests', edits, explains));
    if (conv.packages_yml) parts.push(codeArtifact('packages_yml:packages.yml', 'packages.yml — dbt_utils (compound-grain tests)', conv.packages_yml, 'dbt packages', edits, explains));
    if (conv.governance_md) parts.push(codeArtifact('governance_md:GOVERNANCE.md', 'GOVERNANCE.md — lineage / secrets / dev-prod / cost checklist', conv.governance_md, 'governance', edits, explains));
    const gate = conv.gate ? `<div style="margin:0 0 8px;padding:8px 12px;border-radius:8px;font-size:12px;border:1px solid var(--border);background:var(--bg-surface)">
        <strong>${conv.gate.blockers_empty ? '✅' : '⛔'} Ship gate:</strong> ${conv.gate.blocker_count} review-queue blocker(s)${conv.gate.contracts_enforced && conv.gate.contracts_enforced.length ? ` · ${conv.gate.contracts_enforced.length} enforced contract(s)` : ''}. <span style="color:var(--text-muted)">Gate = blockers clear AND reconciliation passes AND tests/contracts build. The AI grade is triage only.</span>
      </div>` : '';
    return gate + (parts.length ? parts.join('') : emptyBox('No tests/contracts generated (declare keys in the lineage to get key/grain tests).'));
  }
  return conv.sources_yml
    ? codeArtifact('sources_yml:sources.yml', 'sources.yml', conv.sources_yml, 'dbt sources', edits, explains)
    : emptyBox('No sources.yml.');
}

function sourceConfigs(state) {
  const sf = state.sfGlueSnowflakeConfig || {}, gl = state.sfGlueGlueConfig || {};
  const pgOk = !!(state.sfGluePostgresConnection && state.sfGluePostgresConnection.success);
  return {
    snowflake: (sf.account && (sf.database || (state.sfGlueSnowflakeConnection || {}).success)) ? sf : undefined,
    glue: (gl.region && (gl.profile_name || (gl.access_key_id && gl.secret_access_key))) ? gl : undefined,
    // When Postgres is connected, convert auto-generates the Postgres → bronze notebook.
    postgres: pgOk ? (state.sfGluePostgresConfig || undefined) : undefined,
  };
}

const emptyBox = msg => `<div style="color:var(--text-muted);font-size:13px;padding:24px;text-align:center;border:1px dashed var(--border);border-radius:10px;margin:8px">${msg}</div>`;

// ── LEFT-TOP: selectable Project Tables ─────────────────────────────────────
function renderTableList(state) {
  const lineage = state.sfGlueLineage && state.sfGlueLineage.lineage;
  const candidates = (lineage?.nodes || [])
    .filter(n => String(n.id).startsWith('sf:'))
    .sort((a, b) => String(a.label).localeCompare(String(b.label)));
  const dupNames = new Set();
  ((state.sfGlueLineage && state.sfGlueLineage.duplicates) || []).forEach(g => (g.members || []).forEach(m => {
    if (m.system === 'snowflake') dupNames.add(String(m.full_name).toLowerCase());
  }));
  const selected = new Set(state.sfGlueSelectedTables || []);
  if (!candidates.length) return emptyBox('No Snowflake tables found. Connect Snowflake on the Lineage step.');
  return candidates.map(c => {
    const isView = c.type === 'gold';
    const isDup = dupNames.has(String(c.label).toLowerCase());
    return `
      <label class="sfg-cand-row" data-name="${esc(String(c.label).toLowerCase())}"
        style="display:flex;align-items:center;gap:8px;padding:6px 10px;border-bottom:1px solid var(--border);cursor:pointer">
        <input type="checkbox" class="sfg-cand" value="${esc(c.id)}" ${selected.has(c.id) ? 'checked' : ''} />
        <span style="flex:1;font-size:12px;font-family:monospace">${esc(c.display || c.label)}</span>
        <span class="badge ${isView ? 'badge-info' : ''}" style="font-size:9px">${isView ? 'VIEW' : 'TABLE'}</span>
        ${isDup ? '<span class="badge badge-error" style="font-size:9px" title="Also exists in Glue/another schema">dup</span>' : ''}
      </label>`;
  }).join('');
}

// ── LEFT-BOTTOM: editable SOURCE (Glue jobs + Snowflake view SQL) ─────────────
function renderSourceBody(state) {
  const review = state.sfGlueReview;
  if (!review) return emptyBox('Click <strong>Load source</strong> above to pull the Glue job code and Snowflake view SQL.');
  const edits = state.sfGlueArtifactEdits || {};
  const explains = state.sfGlueArtifactExplain || {};
  const jobs = review.glue_jobs || [];
  const views = review.views || [];
  const html = jobs.map(j => codeArtifact(`gluejob:${j.name}`, j.name, j.script, 'glue job', edits, explains)).join('')
    + views.map(v => codeArtifact(`sfview:${v.full_name}`, v.full_name, v.sql, 'snowflake view', edits, explains)).join('');
  return html || emptyBox('No Glue jobs or Snowflake views — the base tables have no transformation code to edit.');
}

// ── RIGHT-BOTTOM: editable GENERATED Databricks/dbt output ────────────────────
export function renderSfGlueReviewPage(container) {
  const state = store.get();
  const lineage = state.sfGlueLineage && state.sfGlueLineage.lineage;
  const hasModel = !!(lineage && lineage.nodes && lineage.nodes.length);
  const selectedCount = (state.sfGlueSelectedTables || []).length;
  const review = state.sfGlueReview;
  const busyReview = state.isReviewingSfGlue;
  const busyConvert = state.isConvertingSfGlue;

  if (!hasModel) {
    container.innerHTML = `
      <div class="page" style="padding:24px;width:100%"><div style="max-width:760px;margin:0 auto">
        <button class="btn btn-secondary" id="rv-back" style="padding:4px 10px;font-size:11px;margin-bottom:12px">← Lineage</button>
        ${emptyBox('No data model yet — run <strong>Analyze</strong> on the Lineage step, then come back to review &amp; migrate.')}
      </div></div>`;
    container.querySelector('#rv-back')?.addEventListener('click', () => store.navigate('sfglue-lineage'));
    return;
  }

  container.innerHTML = `
    <div class="page" id="sfg-review-page" style="height:100%;display:flex;flex-direction:column;overflow:hidden;width:100%">
      <!-- Toolbar -->
      <div style="display:flex;align-items:center;gap:10px;padding:12px 24px;border-bottom:1px solid var(--border);flex-wrap:wrap;flex-shrink:0">
        <button class="btn btn-secondary" id="rv-back" style="padding:4px 10px;font-size:11px">← Lineage</button>
        <h2 style="margin:0;font-size:18px">Review &amp; Edit</h2>
        <span id="rv-sel-count" style="font-size:12px;color:var(--text-muted)">${selectedCount} selected</span>
        <div style="margin-left:auto;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
          <button class="btn btn-secondary" id="rv-load" ${busyReview ? 'disabled' : ''} style="padding:5px 11px;font-size:12px">${busyReview ? 'Loading…' : (review ? '↻ Refresh source' : 'Load source')}</button>
          ${busyConvert ? `<button class="btn btn-secondary" id="rv-convert-stop" style="padding:5px 13px;font-size:12px;font-weight:700;color:var(--error);border-color:var(--error)">⏹ Stop</button>` : ''}
          <button class="btn btn-primary" id="rv-convert" ${busyConvert || !selectedCount ? 'disabled' : ''} ${!selectedCount ? 'title="Select at least one table first"' : ''} style="padding:5px 13px;font-size:12px;font-weight:700">${busyConvert ? '⏳ Converting…' : '⚡ Generate conversion'}</button>
        </div>
      </div>

      ${review && review.errors && Object.keys(review.errors).length ? `<div class="badge badge-error" style="display:block;margin:8px 24px;padding:8px;font-size:12px;white-space:normal;text-align:left">⚠ ${Object.entries(review.errors).map(([k, v]) => `${esc(k)}: ${esc(typeof v === 'object' ? JSON.stringify(v) : v)}`).join(' · ')}</div>` : ''}

      <!-- Body: top (tables + graph) / bottom (source + generated). Panes are drag-resizable. -->
      <div id="rv-body" style="flex:1;display:flex;flex-direction:column;min-height:0">
        <!-- TOP -->
        <div id="rv-top" style="flex:0 0 38%;min-height:120px;display:flex;overflow:hidden">
          <div id="rv-tables-pane" style="flex:0 0 330px;min-width:200px;display:flex;flex-direction:column;border-right:1px solid var(--border);overflow:hidden">
            <div style="display:flex;align-items:center;gap:8px;padding:7px 10px;border-bottom:1px solid var(--border);background:var(--bg-surface)">
              <span style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim)">📂 Project Tables</span>
              <input id="rv-search" placeholder="filter…" aria-label="Filter tables" autocomplete="off" style="margin-left:auto;width:110px;padding:3px 7px;border:1px solid var(--border);border-radius:5px;background:var(--bg-primary);color:var(--text-primary);font-size:11px" />
            </div>
            <label style="display:flex;align-items:center;gap:7px;padding:5px 10px;border-bottom:1px solid var(--border);font-size:11px;color:var(--text-secondary)">
              <input type="checkbox" id="rv-select-all" /> Select all (visible)
            </label>
            <div id="rv-table-list" style="overflow:auto;flex:1">${renderTableList(state)}</div>
          </div>
          <div class="output-resizer rv-resizer" data-resize="col" data-target="rv-tables-pane" title="Drag to resize columns"></div>
          <div style="flex:1 1 0;min-width:200px;display:flex;flex-direction:column;overflow:hidden">
            <div style="padding:7px 12px;border-bottom:1px solid var(--border);background:var(--bg-surface);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim)">🔗 Dependency Map</div>
            <div id="rv-graph" style="flex:1;background:var(--bg-surface);overflow:hidden"></div>
          </div>
        </div>

        <!-- Drag to resize TOP ↔ BOTTOM -->
        <div class="output-resizer-h rv-resizer" data-resize="row" title="Drag to resize rows"></div>

        <!-- BOTTOM — SOURCE (left) ↔ GENERATED output (right). Databricks *connection*
             config lives on the Databricks Agent step; the artifacts are previewed here. -->
        <div id="rv-bottom" style="flex:1 1 0;display:flex;min-height:0;overflow:hidden">
          <!-- SOURCE -->
          <div id="rv-src-pane" style="flex:0 0 50%;min-width:220px;display:flex;flex-direction:column;border-right:1px solid var(--border);overflow:hidden">
            <div style="padding:6px 12px;border-bottom:1px solid var(--border);background:var(--bg-surface);font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim)">SOURCE — Glue jobs &amp; Snowflake SQL</div>
            <div id="rv-source-body" style="flex:1;overflow:auto;padding:8px">${renderSourceBody(state)}</div>
          </div>
          <div class="output-resizer rv-resizer" data-resize="col" data-target="rv-src-pane" title="Drag to resize columns"></div>
          <!-- GENERATED -->
          <div style="flex:1 1 0;min-width:220px;display:flex;flex-direction:column;overflow:hidden;background:var(--bg-primary)">
            <div id="rv-gen-tabs" style="display:flex;gap:10px;padding:2px 12px 0;border-bottom:1px solid var(--border);background:var(--bg-surface);overflow-x:auto">${renderTabs(GEN_TABS, genTab)}</div>
            <div id="rv-gen-body" style="flex:1;overflow-y:auto;overflow-x:hidden;padding:8px">${renderGeneratedBody(state)}</div>
          </div>
        </div>
      </div>

      <!-- Footer -->
      <div style="display:flex;align-items:center;gap:12px;padding:10px 24px;border-top:1px solid var(--border);flex-shrink:0">
        <span style="font-size:12px;color:var(--text-muted)">Select tables, edit the source or generated code, then Generate conversion. Set the Databricks connection on the Databricks Agent step.</span>
        <div style="flex:1"></div>
        <button class="btn btn-primary" id="rv-to-dbx" ${state.sfGlueConversion ? '' : 'disabled'} title="${state.sfGlueConversion ? '' : 'Generate a conversion first'}">Databricks Agent →</button>
      </div>
    </div>`;

  // ── Mount the dependency map — the readable interactive Flow view (same as the
  //    Check Lineage step). Selection is via the Project Tables checkboxes on the left. ──
  const graphEl = container.querySelector('#rv-graph');
  if (graphEl) renderLineageFlow(graphEl, lineage, {});
  wireArtifacts(container);
  wireReviewQueue(container, store.get().sfGlueConversion);
  wireResizers(container);

  // ── Helpers ──
  // Destination is configured on the Databricks Agent step now; Generate reads the
  // saved values (falling back to sensible medallion defaults if not set yet).
  const savedDest = () => {
    const d = store.get().sfGlueDestination || {};
    return {
      workspace_url: d.workspace_url || '',
      token: d.token || '',
      sql_warehouse_id: d.sql_warehouse_id || '',
      catalog: d.catalog || 'lakehouse',
      bronze_schema: d.bronze_schema || 'bronze',
      silver_schema: d.silver_schema || 'silver',
      gold_schema: d.gold_schema || 'gold',
      // The raw-source location MUST flow to /convert too, or the convert-time column
      // grounding introspects the wrong schema (bronze) and the AI guesses column names.
      // (The Build step already reads these from the Databricks Agent page; convert didn't.)
      source_catalog: d.source_catalog || '',
      source_schema: d.source_schema || '',
    };
  };
  const updateCount = () => {
    const el = container.querySelector('#rv-sel-count');
    const n = (store.get().sfGlueSelectedTables || []).length;
    if (el) el.textContent = `${n} selected`;
    const convertBtn = container.querySelector('#rv-convert');
    if (convertBtn && !store.get().isConvertingSfGlue) {
      convertBtn.disabled = !n;
      if (n) convertBtn.removeAttribute('title'); else convertBtn.title = 'Select at least one table first';
    }
    syncSelectAll();
  };
  // Reflect "all visible selected" on #rv-select-all (checked / indeterminate / unchecked).
  function syncSelectAll() {
    const all = container.querySelector('#rv-select-all');
    if (!all) return;
    const visible = [...container.querySelectorAll('.sfg-cand-row')].filter(r => r.style.display !== 'none');
    const checked = visible.filter(r => r.querySelector('.sfg-cand')?.checked).length;
    all.checked = visible.length > 0 && checked === visible.length;
    all.indeterminate = checked > 0 && checked < visible.length;
  }
  // Quiet selection updates (no re-render → graph & scroll stay put).
  function setSelected(id, on) {
    const sel = new Set(store.get().sfGlueSelectedTables || []);
    if (on) sel.add(id); else sel.delete(id);
    store.get().sfGlueSelectedTables = [...sel];
    updateCount();
  }

  // ── Table selection ──
  container.querySelectorAll('.sfg-cand').forEach(cb => cb.addEventListener('change', () => setSelected(cb.value, cb.checked)));
  container.querySelector('#rv-select-all')?.addEventListener('change', (e) => {
    // Toggle every VISIBLE row (the filter sets row.style.display directly, so read
    // that — don't match on the serialized style-attribute substring, which is
    // brittle). Collect into one set and write the store once so all rows land,
    // not just the first.
    const on = e.target.checked;
    const sel = new Set(store.get().sfGlueSelectedTables || []);
    container.querySelectorAll('.sfg-cand-row').forEach(row => {
      if (row.style.display === 'none') return;
      const cb = row.querySelector('.sfg-cand');
      if (!cb) return;
      cb.checked = on;
      if (on) sel.add(cb.value); else sel.delete(cb.value);
    });
    store.get().sfGlueSelectedTables = [...sel];
    updateCount();
  });
  container.querySelector('#rv-search')?.addEventListener('input', (e) => {
    const q = e.target.value.trim().toLowerCase();
    container.querySelectorAll('.sfg-cand-row').forEach(row => {
      row.style.display = row.dataset.name.includes(q) ? 'flex' : 'none';
    });
    syncSelectAll();
  });
  syncSelectAll();

  // ── Generated tabs switch in place (graph + source + table list stay mounted) ──
  container.querySelector('#rv-gen-tabs')?.addEventListener('click', (e) => {
    const btn = e.target.closest('.ui-tab');
    if (!btn || genTab === btn.dataset.tab) return;
    genTab = btn.dataset.tab;
    container.querySelector('#rv-gen-tabs').innerHTML = renderTabs(GEN_TABS, genTab);
    container.querySelector('#rv-gen-body').innerHTML = renderGeneratedBody(store.get());
    wireArtifacts(container);
    wireReviewQueue(container, store.get().sfGlueConversion);
  });

  // ── Navigation ──
  container.querySelector('#rv-back')?.addEventListener('click', () => store.navigate('sfglue-lineage'));
  container.querySelector('#rv-to-dbx')?.addEventListener('click', () => store.navigate('sfglue-databricks-agent'));

  // ── Load source ──
  container.querySelector('#rv-load')?.addEventListener('click', async () => {
    const { snowflake, glue } = sourceConfigs(store.get());
    if (!snowflake && !glue) { notify('Connect a source on the Connections step first.', { kind: 'warning', title: 'Not connected' }); return; }
    store.set({ isReviewingSfGlue: true });
    try {
      const result = await api.reviewSnowflakeGlue({ snowflake, glue });
      store.set({ sfGlueReview: result, isReviewingSfGlue: false });
    } catch (err) {
      store.set({ isReviewingSfGlue: false });
      notify(err.message, { kind: 'error', title: 'Load source failed' });
    }
  });

  // ── Generate conversion (Migrate) ──
  container.querySelector('#rv-convert')?.addEventListener('click', async () => {
    const sel = store.get().sfGlueSelectedTables || [];
    if (!sel.length) { notify('Select at least one table to migrate.', { kind: 'warning', title: 'Nothing selected' }); return; }
    const destination = savedDest();
    const { snowflake, glue, postgres } = sourceConfigs(store.get());
    // Send the Glue job scripts captured at the Review step (with any edits the user
    // made here) so conversion doesn't depend on a still-valid live Glue session and
    // honours edited source. Capture BEFORE we clear edits below.
    const reviewState = store.get().sfGlueReview || {};
    const editsNow = store.get().sfGlueArtifactEdits || {};
    const glueScripts = {};
    (reviewState.glue_jobs || []).forEach((j) => {
      if (!j || !j.name) return;
      const k = `gluejob:${j.name}`;
      glueScripts[j.name] = (k in editsNow) ? editsNow[k] : j.script;
    });
    _convertAbort = new AbortController();
    store.set({ sfGlueDestination: destination, isConvertingSfGlue: true, sfGlueArtifactEdits: {}, sfGlueArtifactExplain: {} });
    try {
      const result = await api.convertSnowflakeGlueMigration({ snowflake, glue, postgres, lineage, selectedIds: sel, destination, glueScripts, signal: _convertAbort.signal });
      store.set({ sfGlueConversion: result, isConvertingSfGlue: false });
      notifyConversion(result);
    } catch (err) {
      store.set({ isConvertingSfGlue: false });
      // A user-initiated Stop aborts the fetch — that's not an error, just report it.
      if (err && err.name === 'AbortError') {
        notify('Conversion stopped.', { kind: 'warning', title: 'Stopped' });
      } else {
        notify(err.message, { kind: 'error', title: 'Conversion failed' });
      }
    } finally {
      _convertAbort = null;
    }
  });

  // Stop the in-flight conversion (aborts the request; the UI stops waiting).
  container.querySelector('#rv-convert-stop')?.addEventListener('click', () => {
    if (_convertAbort) _convertAbort.abort();
  });
}

// ── Drag-resizable panes ──────────────────────────────────────────────────
// One vertical splitter (TOP ↔ BOTTOM) and two column splitters (Tables ↔ Map,
// Source ↔ Generated). Each resized pane is switched to a fixed flex-basis while
// its sibling (flex:1) absorbs the rest; drags are clamped to sane minimums and
// pointer-locked via the shared `output-resizing*` body classes.
function wireResizers(container) {
  const body = container.querySelector('#rv-body');
  if (!body) return;
  container.querySelectorAll('.rv-resizer').forEach((bar) => {
    bar.addEventListener('mousedown', (e) => {
      e.preventDefault();
      const isRow = bar.dataset.resize === 'row';
      bar.classList.add('dragging');
      document.body.classList.add(isRow ? 'output-resizing-v' : 'output-resizing');

      let onMove;
      if (isRow) {
        // Vertical drag → resize the TOP pane's height; BOTTOM (flex:1) takes the rest.
        const top = container.querySelector('#rv-top');
        const startY = e.clientY;
        const startH = top.getBoundingClientRect().height;
        onMove = (ev) => {
          const max = body.getBoundingClientRect().height - 140;
          const h = Math.max(120, Math.min(startH + (ev.clientY - startY), max));
          top.style.flex = `0 0 ${h}px`;
        };
      } else {
        // Column drag → resize the target pane's width; its sibling (flex:1) fills.
        const target = container.querySelector('#' + bar.dataset.target);
        const parent = target.parentElement;
        const startX = e.clientX;
        const startW = target.getBoundingClientRect().width;
        onMove = (ev) => {
          const max = parent.getBoundingClientRect().width - 200;
          const w = Math.max(180, Math.min(startW + (ev.clientX - startX), max));
          target.style.flex = `0 0 ${w}px`;
        };
      }
      const onUp = () => {
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup', onUp);
        bar.classList.remove('dragging');
        document.body.classList.remove('output-resizing', 'output-resizing-v');
        // The dependency map (Cytoscape) only re-fits its canvas on a window resize,
        // not on a container resize — nudge it so the graph reflows to the new pane.
        window.dispatchEvent(new Event('resize'));
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup', onUp);
    });
  });
}

export function destroySfGlueReviewPage() {
  destroyGraph();
}
