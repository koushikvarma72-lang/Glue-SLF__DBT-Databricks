/**
 * Snowflake/Glue → Databricks/DBT — Step 2: Check lineage.
 *
 * Builds a source→Snowflake dataflow graph from the Glue catalog + ETL job
 * scripts and Snowflake object dependencies, renders it as an interactive flow
 * (SOURCE→BRONZE→SILVER→GOLD), and surfaces duplicate-table / overlapping-logic
 * findings plus consolidation recommendations (deterministic + AI). Reviewing this
 * is how the user spots duplication before choosing what to migrate.
 */
import { api } from '../api.js';
import { store } from '../store.js';
import { renderLineageFlow } from '../components/lineageFlow.js';
import { renderOpsHeader, augmentLineageWithOps } from '../components/lineageOps.js';
import { esc } from '../components/ui.js';
import { notify } from '../components/notify.js';
import { confirmModal } from '../components/modal.js';

let sfgOpsGraph = null;   // cached operational-lineage graph (fetched lazily, augments the Flow view)

const SEVERITY_COLOR = { high: '#dc2626', medium: '#ca8a04', low: '#16a34a' };
const LEGEND = [
  ['source', '#2563eb', 'Source (S3 / external file)'],
  ['bronze', '#8b5cf6', 'Bronze — raw / landing'],
  ['silver', '#0f766e', 'Silver — cleaned / conformed'],
  ['gold', '#ca8a04', 'Gold — marts / views'],
];

function buildPayloadConfigs(state) {
  const sf = state.sfGlueSnowflakeConfig || {};
  const glue = state.sfGlueGlueConfig || {};
  // Include Snowflake whenever it's connected; a database is needed to list
  // objects, but send it anyway so the backend returns a clear "select a database"
  // message instead of silently showing nothing.
  const sfConnected = !!(state.sfGlueSnowflakeConnection && state.sfGlueSnowflakeConnection.success);
  const includeSf = !!(sf.account && (sf.database || sfConnected));
  const includeGlue = !!(glue.region && (glue.profile_name || (glue.access_key_id && glue.secret_access_key)));
  return {
    snowflake: includeSf ? sf : undefined,
    glue: includeGlue ? glue : undefined,
  };
}

function renderSourceErrors(errors) {
  if (!errors || !Object.keys(errors).length) return '';
  const human = (v) => {
    if (v == null) return '';
    if (typeof v !== 'object') return String(v);
    const msg = v.message || v.error || v.detail;
    if (msg) return v.code ? `[${v.code}] ${msg}` : String(msg);
    return JSON.stringify(v);
  };
  const items = Object.entries(errors)
    .map(([k, v]) => `<li><strong>${esc(k)}</strong>: ${esc(human(v))}</li>`)
    .join('');
  return `<div class="badge badge-error" style="display:block;text-align:left;white-space:normal;padding:10px;margin-bottom:12px;font-size:12px">
    <span aria-hidden="true">⚠</span> Some sources reported issues:<ul style="margin:6px 0 0 16px">${items}</ul></div>`;
}

function renderDuplicates(duplicates) {
  if (!duplicates || !duplicates.length) {
    return `<div style="color:var(--text-muted);font-size:13px">No tables appear in more than one system.</div>`;
  }
  // These lists are routinely 15-20+ tables long. Keep them in a fixed-height
  // scrollable box so the panel stays compact instead of running the page down.
  const cards = duplicates.map(g => `
    <div style="border:1px solid var(--border);border-radius:8px;padding:10px 12px;margin-bottom:8px;background:var(--bg-primary)">
      <div style="display:flex;align-items:center;gap:8px">
        <strong style="font-size:13px">${esc(g.base_name)}</strong>
        ${g.cross_system ? '<span class="badge badge-error" style="font-size:10px">in both systems</span>' : '<span class="badge badge-warning" style="font-size:10px">repeated in one system</span>'}
        ${g.column_overlap != null ? `<span style="font-size:11px;color:var(--text-muted)">column-name match ${Math.round(g.column_overlap * 100)}%</span>` : ''}
      </div>
      <div style="font-size:12px;color:var(--text-secondary);margin-top:4px">
        ${g.members.map(m => `${esc(m.full_name)} <span style="color:var(--text-muted)">[${esc(m.system)}]</span>`).join(' · ')}
      </div>
    </div>`).join('');
  return `
    <div style="font-size:11px;color:var(--text-muted);margin-bottom:8px">
      Same logical table found in two places — usually the Glue/S3 gold layer and the copy loaded into Snowflake.
      “Column-name match” compares schema column names, not the underlying data.
    </div>
    <div style="max-height:420px;overflow:auto;padding-right:4px">${cards}</div>`;
}

function renderRecommendations(recs, aiUsed, aiStatus, aiError) {
  if (!recs || !recs.length) {
    return `<div style="color:var(--text-muted);font-size:13px">No recommendations.</div>`;
  }
  // Honest subtitle: distinguish "no provider" from "AI call failed" (e.g. expired
  // Bedrock SSO) from "AI ran but returned nothing" — instead of always blaming config.
  const status = aiStatus || (aiUsed ? 'ok' : 'no_provider');
  const subtitle = status === 'ok' ? 'AI-assisted + structural analysis'
    : status === 'error' ? `Structural analysis — AI call failed${aiError ? ': ' + esc(aiError) : ''}. If using Bedrock, your SSO may have expired (run \`aws sso login\`); then re-run Analyze lineage.`
    : status === 'empty' ? 'Structural analysis — the AI returned no recommendations. Re-run Analyze lineage to retry.'
    : 'Structural analysis — no AI provider configured. Connect one via ⚙ Settings, then re-run Analyze lineage.';

  const card = (r) => `
    <div style="border-left:3px solid ${SEVERITY_COLOR[r.severity] || '#999'};padding:8px 12px;margin-bottom:8px;background:var(--bg-primary);border-radius:0 6px 6px 0">
      <div style="display:flex;align-items:center;gap:8px">
        <strong style="font-size:13px">${esc(r.title)}</strong>
        <span style="font-size:10px;text-transform:uppercase;color:${SEVERITY_COLOR[r.severity] || '#999'}">${esc(r.severity || '')}</span>
        ${r.source === 'ai' ? '<span class="badge badge-info" style="font-size:10px">AI</span>' : ''}
      </div>
      ${r.detail ? `<div style="font-size:12px;color:var(--text-secondary);margin-top:4px">${esc(r.detail)}</div>` : ''}
      ${(r.members && r.members.length) ? `<div style="font-size:11px;color:var(--text-muted);margin-top:4px">${r.members.map(esc).join(' · ')}</div>` : ''}
    </div>`;

  // The deterministic baseline emits ONE "materialized in both Snowflake and Glue" rec
  // per duplicate table — routinely 15-20+ near-identical HIGH cards that all say the same
  // thing and mirror the "Tables in both systems" list. Collapse them into a single
  // scrollable dropdown so the higher-value AI/other recs above aren't buried.
  const isPerTableDup = (r) => r.source !== 'ai' && /materialized in both/i.test(r.title || '');
  const bulk = recs.filter(isPerTableDup);
  const rest = recs.filter((r) => !isPerTableDup(r));

  const bulkHtml = bulk.length ? `
    <details style="border:1px solid var(--border);border-radius:8px;margin-bottom:8px;background:var(--bg-primary)">
      <summary style="cursor:pointer;padding:9px 12px;font-size:13px;font-weight:600;display:flex;align-items:center;gap:8px;list-style:none">
        <span style="color:var(--text-muted);font-size:11px">▸</span>
        Consolidate ${bulk.length} table${bulk.length === 1 ? '' : 's'} materialized in both Snowflake &amp; Glue
        <span style="font-size:10px;text-transform:uppercase;color:${SEVERITY_COLOR.high}">high</span>
      </summary>
      <div style="max-height:340px;overflow:auto;padding:2px 10px 8px">${bulk.map(card).join('')}</div>
    </details>` : '';

  return `
    <div style="font-size:11px;color:var(--text-muted);margin-bottom:8px">
      ${subtitle}
    </div>
    ${rest.map(card).join('')}
    ${bulkHtml}`;
}

export function renderSfGlueLineagePage(container) {
  const state = store.get();
  const data = state.sfGlueLineage;
  const busy = state.isBuildingSfGlueLineage;
  const legendHtml = LEGEND.map(([, color, label]) =>
    `<span style="display:inline-flex;align-items:center;gap:5px;font-size:11px;color:var(--text-secondary)">
       <span style="width:10px;height:10px;border-radius:50%;background:${color};display:inline-block"></span>${label}</span>`
  ).join('');

  container.innerHTML = `
    <div class="page" style="overflow:auto;padding:24px;width:100%">
      <div style="max-width:1100px;margin:0 auto">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
          <button class="btn btn-secondary" id="sfglue-back" style="padding:4px 10px">← Connections</button>
          <h2 style="margin:0">Lineage & duplication review</h2>
        </div>
        <p style="color:var(--text-secondary);margin:0 0 16px">
          Full dataflow from sources → Snowflake (and the ETL in Glue). Review it to spot tables and business logic
          that live in more than one system — typically the same gold table built in Glue/S3 and copied into
          Snowflake — so you can migrate each once instead of twice.
        </p>

        <div style="display:flex;align-items:center;gap:12px;margin-bottom:${busy ? '8px' : '16px'}">
          <button class="btn btn-primary" id="sfglue-analyze" ${busy ? 'disabled' : ''}>
            ${busy ? 'Analyzing…' : (data ? 'Re-analyze lineage' : 'Analyze lineage')}
          </button>
          ${busy ? '' : (data ? `<span style="font-size:12px;color:var(--text-muted)">${data.summary || ''}</span>` : '')}
        </div>
        ${busy ? `
          <div style="margin-bottom:16px">
            <div class="progress-indeterminate"></div>
            <div style="font-size:12px;color:var(--text-muted);margin-top:6px">
              Reading Snowflake objects and the Glue catalog + ETL scripts… this can take a moment for large accounts.
            </div>
          </div>
        ` : ''}

        <div id="sfglue-error" role="alert" style="color:var(--danger,#dc2626);font-size:13px;margin-bottom:12px"></div>

        ${data ? `
          ${renderSourceErrors(data.errors)}
          ${(data.lineage && data.lineage.nodes && data.lineage.nodes.length) ? `
            <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:8px">
              <div style="display:flex;gap:14px;flex-wrap:wrap">${legendHtml}</div>
            </div>
            <div id="sfglue-ops-header"></div>
            <div id="sfglue-graph" style="border:1px solid var(--border);border-radius:10px;background:var(--bg-surface);padding:14px;height:560px;overflow:hidden"></div>

            <div style="display:grid;grid-template-columns:1fr 1fr;gap:18px;margin-top:20px">
              <div>
                <h3 style="margin:0 0 10px">Tables in both systems</h3>
                ${renderDuplicates(data.duplicates)}
              </div>
              <div>
                <h3 style="margin:0 0 10px">Recommendations</h3>
                ${renderRecommendations(data.recommendations, data.ai_used, data.ai_status, data.ai_error)}
              </div>
            </div>

            <div style="margin-top:22px;display:flex;align-items:center;gap:12px">
              <button class="btn btn-primary" id="sfglue-to-review">Review & Edit →</button>
              <span style="font-size:12px;color:var(--text-muted)">Select tables, edit the Glue/Snowflake source and the generated dbt, then migrate — all on one screen.</span>
            </div>
          ` : `
            <div style="color:var(--text-muted);font-size:14px;padding:30px;text-align:center;border:1px dashed var(--border);border-radius:10px">
              No tables or jobs were discovered. Common causes: no <strong>database</strong> selected, the
              <strong>Schema</strong> doesn't exist in that database (clear Schema to scan all, or set a real one like
              <code>TPCH_SF1000</code>), the role can't read <code>INFORMATION_SCHEMA</code>, or the AWS Glue connection
              failed — see the issues above for the exact reason.
            </div>
          `}
        ` : `
          <div style="color:var(--text-muted);font-size:14px;padding:40px;text-align:center;border:1px dashed var(--border);border-radius:10px">
            Click <strong>Analyze lineage</strong> to read the Glue catalog + ETL scripts and Snowflake dependencies, then render the dataflow.
          </div>
        `}
      </div>

      <div id="sfglue-job-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,0.45);z-index:1000;align-items:center;justify-content:center">
        <div role="dialog" aria-modal="true" aria-labelledby="sfglue-job-modal-title" style="background:var(--bg-surface);border:1px solid var(--border);border-radius:12px;max-width:920px;width:92%;max-height:84vh;display:flex;flex-direction:column;padding:18px">
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
            <h3 id="sfglue-job-modal-title" style="margin:0;font-family:monospace;font-size:15px"><span aria-hidden="true">⚙ </span><span id="sfglue-job-modal-name">Glue job</span></h3>
            <button class="btn btn-secondary" id="sfglue-job-modal-close" style="margin-left:auto;padding:3px 10px">Close</button>
          </div>
          <div id="sfglue-job-modal-body" role="status" aria-live="polite" style="overflow:auto"></div>
        </div>
      </div>
    </div>`;

  // Show a Glue job's script + metadata in a modal.
  let jobModalOpener = null;  // a11y: element to restore focus to on close
  const showJobDetails = (name) => {
    const scripts = (data && data.glue_scripts) || {};
    const meta = ((data && data.jobs) || []).find(j => (j.name || '') === name) || {};
    const code = scripts[name] || '';
    const modal = container.querySelector('#sfglue-job-modal');
    const nameEl = container.querySelector('#sfglue-job-modal-name');
    const body = container.querySelector('#sfglue-job-modal-body');
    if (!modal || !nameEl || !body) return;
    jobModalOpener = document.activeElement;
    nameEl.textContent = name;
    body.innerHTML = `
      <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px">
        ${meta.type ? `type: <code>${esc(meta.type)}</code> · ` : ''}${meta.script_location ? `<code>${esc(meta.script_location)}</code>` : ''}
      </div>
      <pre style="margin:0;padding:12px;background:var(--bg-primary);border:1px solid var(--border);border-radius:8px;overflow:auto;max-height:62vh;font-size:12px;line-height:1.5"><code>${esc(code || '(script not captured — the job may have no inline script, or read access was denied)')}</code></pre>`;
    modal.style.display = 'flex';
    container.querySelector('#sfglue-job-modal-close')?.focus();  // a11y: move focus into the dialog
  };

  // Fetch the operational graph once (Glue Workflow chain + RDS config edges); on
  // arrival re-mount so the Flow view gains the header strips + config-derived edges.
  const ensureOpsGraph = () => {
    if (sfgOpsGraph || ensureOpsGraph._inflight) return;
    const st = store.get();
    const { snowflake, glue } = buildPayloadConfigs(st);
    const pgOk = !!(st.sfGluePostgresConnection && st.sfGluePostgresConnection.success);
    const postgres = pgOk ? (st.sfGluePostgresConfig || undefined) : undefined;
    if (!glue && !postgres) return;
    ensureOpsGraph._inflight = true;
    api.buildOperationalLineage({ glue, glueDatabases: st.sfGlueGlueDatabases, postgres, snowflake })
      .then((g) => { sfgOpsGraph = g; mountLineageGraph(); })
      .catch(() => { /* header simply doesn't render; the base flow still works */ })
      .finally(() => { ensureOpsGraph._inflight = false; });
  };

  const mountLineageGraph = () => {
    const el = container.querySelector('#sfglue-graph');
    const header = container.querySelector('#sfglue-ops-header');
    if (!el) return;
    if (!data || !data.lineage) return;
    if (header) {
      if (sfgOpsGraph) renderOpsHeader(header, sfgOpsGraph, { onJobClick: showJobDetails });
      else header.innerHTML = '';
    }
    el.style.height = '560px';
    el.style.overflow = 'hidden';          // the inner .lf-scroll handles scrolling
    ensureOpsGraph();                      // strips + config edges arrive async, then re-mount
    const flowLineage = sfgOpsGraph ? augmentLineageWithOps(data.lineage, sfgOpsGraph) : data.lineage;
    renderLineageFlow(el, flowLineage, { onJobClick: showJobDetails });
  };
  mountLineageGraph();

  const jobModal = container.querySelector('#sfglue-job-modal');
  const jobModalOpen = () => jobModal && jobModal.style.display !== 'none';
  const closeJobModal = () => {
    if (!jobModal) return;
    jobModal.style.display = 'none';
    // a11y: return focus to whatever opened the dialog.
    if (jobModalOpener && typeof jobModalOpener.focus === 'function') jobModalOpener.focus();
    jobModalOpener = null;
  };
  container.querySelector('#sfglue-job-modal-close')?.addEventListener('click', closeJobModal);
  jobModal?.addEventListener('click', (e) => { if (e.target === jobModal) closeJobModal(); });
  // Escape closes the dialog; Tab is trapped inside it while open. Bound on container
  // (replaced each render) so it doesn't leak.
  container.addEventListener('keydown', (e) => {
    if (!jobModalOpen()) return;
    if (e.key === 'Escape') { closeJobModal(); return; }
    if (e.key !== 'Tab') return;
    // a11y: trap Tab within the dialog so focus can't reach controls behind the dimmer.
    const focusables = jobModal.querySelectorAll('a[href],button:not([disabled]),input,select,textarea,[tabindex]:not([tabindex="-1"])');
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  });

  container.querySelector('#sfglue-back')?.addEventListener('click', () => store.navigate('sfglue-connect'));
  container.querySelector('#sfglue-to-review')?.addEventListener('click', () => store.navigate('sfglue-review'));

  container.querySelector('#sfglue-analyze')?.addEventListener('click', async () => {
    const st = store.get();
    const { snowflake, glue } = buildPayloadConfigs(st);
    if (!snowflake && !glue) {
      const e = container.querySelector('#sfglue-error');
      if (e) e.textContent = 'No connected source. Go back and connect Snowflake and/or AWS Glue.';
      return;
    }
    // Re-analyzing rebuilds the graph and clears the Review step. Warn first if the
    // user has already selected tables or made review edits — those go stale otherwise.
    const hasSelection = !!(st.sfGlueSelectedTables && st.sfGlueSelectedTables.length);
    const hasReview = !!st.sfGlueReview;
    if (st.sfGlueLineage && (hasSelection || hasReview)) {
      const ok = await confirmModal(
        'This will rebuild lineage and clear your current selection/review. Continue?',
        { title: 'Re-analyze lineage?', confirmLabel: 'Re-analyze', danger: true });
      if (!ok) return;
    }
    const e = container.querySelector('#sfglue-error');
    if (e) e.textContent = '';
    sfgOpsGraph = null;   // fresh source → rebuild the ops graph on next Ops view
    store.set({ isBuildingSfGlueLineage: true });
    try {
      const result = await api.buildSnowflakeGlueLineage({ snowflake, glue });
      // Re-analyzing invalidates a previously-loaded review (it would otherwise
      // keep showing a stale/empty table list on the Review step).
      store.set({ sfGlueLineage: result, sfGlueReview: null, sfGlueSelectedTables: [], isBuildingSfGlueLineage: false });
    } catch (err) {
      store.set({ isBuildingSfGlueLineage: false });
      const errEl = container.querySelector('#sfglue-error');
      if (errEl) errEl.textContent = err.message;
      notify(err.message, { kind: 'error', title: 'Lineage analysis failed' });
    }
  });
}

export function destroySfGlueLineagePage() {
  // The flow view is plain DOM in the container; nothing to tear down.
}
