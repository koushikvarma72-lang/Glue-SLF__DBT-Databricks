import { store } from '../../store.js';
import { agentState } from './state.js';
import { bindFormCache, restoreCachedForm } from './form-mode.js';
import { renderAgentStatus } from './status-mode.js';
import { runAgent, cancelRun, testConnection, updateConnectionView, bindConnectionControls, syncRunButtons } from './cloud-mode.js';
import { bindLocalControls, localTarget } from './local-mode.js';
import { bindPreviewActions } from './preview-mode.js';
import { escapeHtml } from '../../utils.js';
import { confirmModal } from '../../components/modal.js';
import { notify } from '../../components/notify.js';

// Shared icon set, matching the Databricks Agent page's visual language so the
// two agent pages look like one product.
const S = (inner, opts = '') =>
  `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false" ${opts}>${inner}</svg>`;
const ICON = {
  link: S('<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>'),
  layers: S('<polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/>'),
  play: S('<polygon points="6 4 20 12 6 20 6 4"/>', 'fill="currentColor" stroke="none"'),
  database: S('<ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 5v6c0 1.66-4 3-9 3s-9-1.34-9-3V5"/><path d="M3 11v6c0 1.66 4 3 9 3s9-1.34 9-3v-6"/>'),
  check: S('<polyline points="20 6 9 17 4 12"/>'),
};

export function renderAgentPage(container) {
  const state = store.get();
  const structured = state.regeneration || {};
  const sqlOutput = structured.sql || state.regeneratedSql || '';

  // Generated SQL is a router-guard prerequisite (main.js PAGE_GUARDS.agent),
  // so sqlOutput is guaranteed non-empty by the time this page renders.

  const modelName = 'migration_output';
  const defaultCommands = `dbt seed --full-refresh\ndbt run --select ${modelName}\ndbt test --select ${modelName}`;
  const lineCount = sqlOutput.split('\n').length;
  const sourceName = state.filename || 'Uploaded Qlik setup';

  container.innerHTML = `
    <div class="page dbx-page" id="agent-page">
      <main class="dbx-main">
        <header class="dbx-header">
          <div class="dbx-header-text">
            <div class="dbx-eyebrow">dbt · Run</div>
            <h1 class="dbx-title"><span class="dbx-title-mark">${ICON.database}</span> dbt Agent</h1>
            <p class="dbx-subtitle">Build the migrated model in your warehouse — run dbt Core directly on the connected Databricks workspace, or trigger a dbt Cloud job.</p>
          </div>
          ${statusPill()}
        </header>

        ${stepper()}

        <section class="dbx-card">
          ${renderLocalRunCard()}
        </section>

        <details class="dbx-card" id="dbt-cloud-section">
          <summary style="cursor:pointer;font-weight:600;list-style:revert">Or deploy via dbt Cloud instead</summary>
          <div style="margin-top:16px">
            ${renderConnectionCard()}
          </div>
          <div style="margin-top:16px">
            ${renderRunCard(defaultCommands)}
          </div>
        </details>

        <section class="dbx-card">
          ${renderPreviewCard({ lineCount, modelName, sqlOutput, sourceName })}
        </section>
      </main>

      <div class="review-footer">
        <button class="btn btn-secondary" id="back-to-output">← Back to Output</button>
        <div style="flex:1"></div>
        <button class="btn btn-secondary" id="download-agent-package">Download dbt Package</button>
        <button class="btn btn-primary" id="new-agent-upload">New Upload</button>
      </div>
    </div>
  `;

  restoreCachedForm();
  bindAgentActions(sqlOutput);
}

function statusPill() {
  const cls = agentState.connected ? 'ok' : 'idle';
  const label = agentState.connected ? 'Connected' : 'Not connected';
  return `<div class="dbx-status-pill dbx-status-${cls}" id="dbt-conn-badge"><span class="dbx-status-dot" aria-hidden="true"></span>${label}</div>`;
}

function stepper() {
  const steps = [
    { label: 'Connect', icon: ICON.link, done: agentState.connected },
    { label: 'Configure', icon: ICON.layers, done: !!agentState.runId },
    { label: 'Run', icon: ICON.play, done: !!agentState.runId },
  ];
  const activeIdx = steps.findIndex(s => !s.done);
  return `
    <div class="dbx-steps">
      ${steps.map((s, i) => {
        const stateCls = s.done ? 'done' : (i === activeIdx ? 'active' : 'todo');
        const stateWord = s.done ? 'done' : (i === activeIdx ? 'current' : 'to do');
        return `
          <div class="dbx-step dbx-step-${stateCls}" aria-label="${escapeHtml(s.label)} — ${stateWord}">
            <div class="dbx-step-num">${s.done ? ICON.check : s.icon}</div>
            <div class="dbx-step-label">${escapeHtml(s.label)}</div>
          </div>
          ${i < steps.length - 1 ? `<div class="dbx-step-bar ${s.done ? 'done' : ''}"></div>` : ''}
        `;
      }).join('')}
    </div>
  `;
}

function renderConnectionCard() {
  return `
    <div class="dbx-card-head">
      <div class="dbx-card-headline">
        <span class="dbx-card-icon">${ICON.link}</span>
        <div>
          <h3 class="dbx-card-title">dbt Cloud Connection</h3>
          <p class="dbx-card-desc">Credentials are kept on this device only. Create a service token under <em>Account Settings → Service tokens</em>.</p>
        </div>
      </div>
      <div class="dbx-card-actions">
        <button class="btn btn-primary" id="test-dbt-btn">Test Login</button>
      </div>
    </div>

    <!-- Connect form: token + account ID + API URL on one row (3 equal columns) -->
    <div id="dbt-connect-form" class="dbx-field-grid" style="grid-template-columns:repeat(3,minmax(0,1fr))">
      <label class="dbx-field">
        <span>Service token</span>
        <input class="dbx-input" id="dbt-token" type="password" autocomplete="off" placeholder="dbtc_…">
      </label>
      <label class="dbx-field">
        <span>Account ID</span>
        <input class="dbx-input" id="dbt-account-id" inputmode="numeric" placeholder="12345">
      </label>
      <label class="dbx-field">
        <span>dbt Cloud API URL</span>
        <input class="dbx-input" id="dbt-base-url" value="https://cloud.getdbt.com/api/v2" placeholder="https://cloud.getdbt.com/api/v2">
      </label>
    </div>

    <!-- After connecting, collapses to this summary + auto-filled pickers -->
    <div id="dbt-connected-summary" class="dbx-field-grid" style="display:none">
      <div style="grid-column:1/-1;display:flex;align-items:center;gap:8px;padding:10px 12px;border:1px solid var(--border);border-radius:6px;background:var(--bg-secondary)">
        <span style="color:var(--success,#16a34a);font-weight:600"><span aria-hidden="true">✓ </span>Connected</span>
        <span id="dbt-conn-account" style="color:var(--text-secondary);font-size:12px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"></span>
        <button class="btn btn-secondary" id="dbt-edit-conn" style="padding:2px 10px;font-size:12px">Edit</button>
      </div>
      <label class="dbx-field">
        <span>Project</span>
        <select class="dbx-input" id="dbt-project-id"></select>
      </label>
      <label class="dbx-field">
        <span>Job</span>
        <select class="dbx-input" id="dbt-job-id"></select>
      </label>
    </div>

    <!-- Inline connection-test error (e.g. "Invalid token"), shown right here by
         the Test Login button instead of in the distant Run card. -->
    <div id="dbt-conn-error" class="dbx-conn-error" style="display:none"></div>
  `;
}

function renderLocalRunCard() {
  const { cfg, ready } = localTarget();
  const host = (cfg.workspace_url || '').replace(/^https?:\/\//, '').replace(/\/$/, '');
  const target = ready
    ? `Target: <strong>${escapeHtml(host)}</strong> · <strong>${escapeHtml(cfg.catalog)}.${escapeHtml(cfg.schema)}</strong> · warehouse <code>${escapeHtml(cfg.sql_warehouse_id)}</code>`
    : '';
  return `
    <div class="dbx-card-head">
      <div class="dbx-card-headline">
        <span class="dbx-card-icon">${ICON.play}</span>
        <div>
          <h3 class="dbx-card-title">Run on Databricks (dbt Core)</h3>
          <p class="dbx-card-desc">Builds the generated dbt project on the Databricks workspace you connected on the previous step — no manual download or git commit. Runs <code>dbt debug</code> then <code>dbt build</code>.</p>
        </div>
      </div>
      <div class="dbx-card-actions">
        <button class="btn btn-success" id="run-dbt-local-btn" ${ready ? '' : 'disabled'}>Run on Databricks</button>
        <button class="btn btn-secondary" id="cancel-dbt-local-btn" style="display:none">Stop</button>
      </div>
    </div>
    ${target ? `<p class="dbx-card-desc" style="margin:0 0 8px">${target}</p>` : ''}
    <div id="dbt-local-status" aria-live="polite" style="margin:4px 0 10px;font-size:13px"></div>
    <div id="dbt-local-models" style="margin:0 0 10px"></div>
    <pre id="dbt-local-log" class="output-sql-content" style="margin:0;max-height:320px;overflow:auto;white-space:pre-wrap">No output yet.</pre>
  `;
}

function renderRunCard(defaultCommands) {
  return `
    <div class="dbx-card-head">
      <div class="dbx-card-headline">
        <span class="dbx-card-icon">${ICON.layers}</span>
        <div>
          <h3 class="dbx-card-title">Commands &amp; Run</h3>
          <p class="dbx-card-desc">These dbt commands run in the selected dbt Cloud job. Connect first to enable the run.</p>
        </div>
      </div>
      <div class="dbx-card-actions">
        <button class="btn btn-secondary" id="cancel-dbt-btn" style="display:none">Stop run</button>
        <button class="btn btn-success" id="run-dbt-btn" disabled>Run Agent</button>
      </div>
    </div>
    <label class="dbx-field dbx-field-full">
      <span>Commands to run in dbt Cloud</span>
      <textarea class="dbx-input" id="dbt-commands" rows="4" spellcheck="false">${escapeHtml(defaultCommands)}</textarea>
    </label>
    <div class="agent-status ${agentState.error ? 'error' : ''}" id="agent-status" style="margin-top:12px">
      ${renderAgentStatus()}
    </div>
  `;
}

function renderPreviewCard({ lineCount, modelName, sqlOutput, sourceName }) {
  return `
    <div class="dbx-card-head">
      <div class="dbx-card-headline">
        <span class="dbx-card-icon">${ICON.database}</span>
        <div>
          <h3 class="dbx-card-title">Generated dbt SQL</h3>
          <p class="dbx-card-desc">models/${escapeHtml(modelName)}.sql · ${lineCount} lines · source: ${escapeHtml(sourceName)}</p>
        </div>
      </div>
      <div class="dbx-card-actions">
        <button class="copy-btn" id="copy-agent-sql">Copy SQL</button>
      </div>
    </div>
    <pre class="output-sql-content animate-fade-in" style="margin:0;max-height:360px;overflow:auto">${escapeHtml(sqlOutput)}</pre>
  `;
}

function bindAgentActions(sqlOutput) {
  document.getElementById('back-to-output')?.addEventListener('click', () => store.navigate('output'));
  document.getElementById('new-agent-upload')?.addEventListener('click', async () => {
    const ok = await confirmModal(
      'This clears the current migration and all edits, validation, and chat history. This cannot be undone.',
      { title: 'Start a new migration?', confirmLabel: 'Start new', danger: true },
    );
    if (!ok) return;
    store.reset();
    store.navigate('upload');
  });
  document.getElementById('download-agent-package')?.addEventListener('click', async (e) => {
    const btn = e.currentTarget;
    const sessionId = store.get().sessionId;
    if (!sessionId) { notify('No active session to download.', { kind: 'error' }); return; }
    const original = btn.textContent;
    btn.disabled = true; btn.textContent = 'Preparing…';
    try {
      const res = await fetch(`/api/download/${sessionId}`);
      if (!res.ok) throw new Error(`Download failed (${res.status})`);
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url; a.download = `dbt-package-${sessionId.slice(0, 8)}.zip`;
      document.body.appendChild(a); a.click(); a.remove();
      URL.revokeObjectURL(url);
    } catch (err) {
      notify(err.message || 'Could not download the package.', { kind: 'error', title: 'Download failed' });
    } finally {
      btn.disabled = false; btn.textContent = original;
    }
  });
  bindPreviewActions(sqlOutput);
  bindFormCache();
  document.getElementById('test-dbt-btn')?.addEventListener('click', testConnection);
  document.getElementById('run-dbt-btn')?.addEventListener('click', runAgent);
  document.getElementById('cancel-dbt-btn')?.addEventListener('click', cancelRun);
  bindLocalControls();
  bindConnectionControls();
  // Reflect any already-verified connection from this session (agentState persists).
  updateConnectionView();
  // Reflect an in-flight run (e.g. when returning to the page): show Stop, lock Run.
  syncRunButtons();
}
