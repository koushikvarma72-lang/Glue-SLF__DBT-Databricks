/**
 * Local dbt-Core run mode.
 *
 * Runs the freshly generated dbt project on the Databricks workspace the user
 * already connected on the Databricks Agent step — no manual ZIP download or git
 * commit. The backend assembles the project, writes a profiles.yml from that
 * connection, and runs `dbt debug` then `dbt build` in a background job; we poll
 * its status (fetching only new log lines each time), stream the logs here, show
 * per-model results, and can stop an in-flight run.
 */
import { api } from '../../api.js';
import { recordDeployment, fixInReview } from '../../components/deploy-feedback.js';
import { store } from '../../store.js';
import { agentState } from './state.js';
import { setBusy } from './utils.js';
import { confirmModal } from '../../components/modal.js';

let localPollTimer = null;

/** The Databricks connection captured on the previous (Databricks Agent) step. */
export function localTarget() {
  const cfg = store.get().dbxAgentConfig || {};
  const ready = !!(
    (cfg.workspace_url || '').trim() &&
    (cfg.personal_access_token || '').trim() &&
    (cfg.sql_warehouse_id || '').trim() &&
    (cfg.catalog || '').trim() &&
    (cfg.schema || '').trim()
  );
  return { cfg, ready };
}

function setLogText(text) {
  const pre = document.getElementById('dbt-local-log');
  if (!pre) return;
  const atBottom = pre.scrollHeight - pre.scrollTop - pre.clientHeight < 40;
  pre.textContent = text || 'No output yet.';
  if (atBottom) pre.scrollTop = pre.scrollHeight;
}

function escapeText(s) {
  return String(s == null ? '' : s).replace(/[&<>"']/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function isOk(status) {
  return /^(success|pass)$/i.test(status || '');
}

/** Per-model pass/fail from dbt's run_results.json. */
function renderModels() {
  const el = document.getElementById('dbt-local-models');
  if (!el) return;
  const models = agentState.localModels || [];
  if (!models.length) {
    el.innerHTML = '';
    return;
  }
  const failed = models.filter(mdl => !isOk(mdl.status)).length;
  const head = `<div style="font-size:12px;color:var(--text-secondary);margin-bottom:4px">${models.length} model${models.length === 1 ? '' : 's'}${failed ? ` · ${failed} failed` : ''}</div>`;
  const rows = models.map(mdl => {
    const color = isOk(mdl.status) ? 'var(--success, #16a34a)' : 'var(--danger, #dc2626)';
    const t = (typeof mdl.execution_time === 'number') ? ` · ${mdl.execution_time.toFixed(1)}s` : '';
    return `<div style="display:flex;align-items:center;gap:8px;padding:3px 0;font-size:12px;border-top:1px solid var(--border)">
        <span style="width:8px;height:8px;border-radius:50%;background:${color};flex:none"></span>
        <code style="flex:1">${escapeText(mdl.name)}</code>
        <span style="color:${color}">${escapeText(mdl.status)}${t}</span>
      </div>`;
  }).join('');
  el.innerHTML = head + rows;
}

/** Reflect agentState's local-run fields into the DOM (buttons + status + log + models). */
export function renderLocalView() {
  const btn = document.getElementById('run-dbt-local-btn');
  const stopBtn = document.getElementById('cancel-dbt-local-btn');
  const statusEl = document.getElementById('dbt-local-status');
  const { ready } = localTarget();

  if (btn) {
    if (agentState.localRunning) {
      btn.disabled = true;
      btn.textContent = 'Running…';
    } else {
      btn.disabled = !ready;
      btn.textContent = agentState.localFinished ? 'Re-run on Databricks' : 'Run on Databricks';
    }
  }

  if (stopBtn) {
    stopBtn.style.display = agentState.localRunning ? '' : 'none';
    stopBtn.disabled = !!agentState.localCancelling;
    stopBtn.textContent = agentState.localCancelling ? 'Stopping…' : 'Stop';
  }

  if (statusEl) {
    let html = '';
    if (agentState.localError) {
      html = `<span class="dbx-status-pill dbx-status-fail"><span class="dbx-status-dot"></span>Error</span> <span style="color:var(--text-secondary)">Couldn't run dbt on Databricks: ${escapeText(agentState.localError)}</span>`;
    } else if (agentState.localRunning) {
      const label = agentState.localCancelling ? 'Stopping…' : 'Running dbt…';
      html = `<span class="dbx-status-pill dbx-status-idle"><span class="dbx-status-dot"></span>${label}</span>`;
    } else if (agentState.localStatus === 'success') {
      html = `<span class="dbx-status-pill dbx-status-ok"><span class="dbx-status-dot"></span>Success</span> <span style="color:var(--text-secondary)">${escapeText(agentState.localSummary || 'dbt build completed.')}</span>`;
    } else if (agentState.localStatus === 'cancelled') {
      html = `<span class="dbx-status-pill dbx-status-idle"><span class="dbx-status-dot"></span>Cancelled</span> <span style="color:var(--text-secondary)">Run stopped before it finished.</span>`;
    } else if (agentState.localStatus === 'error') {
      html = `<span class="dbx-status-pill dbx-status-fail"><span class="dbx-status-dot"></span>Failed</span> <span style="color:var(--text-secondary)">${escapeText(agentState.localSummary || 'dbt build failed — see the log.')}</span> <button class="btn btn-secondary btn-sm" id="dbt-local-fix-review" style="margin-left:8px">Fix in Review →</button>`;
    } else if (!ready) {
      html = `<span style="color:var(--text-secondary)">Connect Databricks on the <strong>Databricks Agent</strong> step first (workspace, token, warehouse, catalog, schema).</span>`;
    }
    statusEl.innerHTML = html;
    // Failed run → jump to Review with the failure preloaded in the Refine chat.
    document.getElementById('dbt-local-fix-review')?.addEventListener('click', () => {
      const logTail = (agentState.localLogs || '').split('\n').slice(-30).join('\n');
      fixInReview(
        `The dbt run on Databricks failed: ${agentState.localSummary || agentState.localError || 'see log'}\n\n`
        + `Recent dbt log:\n${logTail}\n\n`
        + 'Fix the generated SQL so this run succeeds.'
      );
    });
  }

  renderModels();
  setLogText(agentState.localLogs);
}

export async function runDbtLocalBuild() {
  const btn = document.getElementById('run-dbt-local-btn');
  if (btn && btn.disabled) return;
  const { cfg, ready } = localTarget();
  if (!ready) {
    agentState.localError = 'Databricks connection is incomplete. Connect on the Databricks Agent step first.';
    renderLocalView();
    return;
  }

  if (!(await confirmModal(
    `This runs dbt build against ${cfg.workspace_url || 'the connected workspace'} and creates/overwrites the models' tables there. Continue?`,
    { title: 'Run dbt on Databricks?', confirmLabel: 'Run', danger: true },
  ))) return;

  agentState.localRunning = true;
  agentState.localFinished = false;
  agentState.localStatus = 'running';
  agentState.localSummary = '';
  agentState.localError = '';
  agentState.localLogs = 'Starting dbt run…\n';
  agentState.localLogOffset = 0;
  agentState.localModels = [];
  agentState.localCancelling = false;
  setBusy(btn, true, 'Starting…');
  renderLocalView();

  try {
    const result = await api.runDbtLocal({
      sessionId: store.get().sessionId,
      workspace_url: cfg.workspace_url,
      personal_access_token: cfg.personal_access_token,
      sql_warehouse_id: cfg.sql_warehouse_id,
      catalog: cfg.catalog,
      schema: cfg.schema,
    });
    agentState.localJobId = result.jobId;
    startLocalPolling();
  } catch (err) {
    agentState.localRunning = false;
    agentState.localStatus = 'error';
    agentState.localError = err.message;
    renderLocalView();
  }
}

export async function cancelLocalRun() {
  if (!agentState.localJobId || !agentState.localRunning || agentState.localCancelling) return;
  agentState.localCancelling = true;
  renderLocalView();
  try {
    await api.cancelDbtLocal(agentState.localJobId);
    // The poller picks up status 'cancelled' once the job actually stops.
  } catch (err) {
    agentState.localCancelling = false;
    agentState.localError = err.message;
    renderLocalView();
  }
}

function startLocalPolling() {
  if (localPollTimer) clearInterval(localPollTimer);
  localPollTimer = setInterval(async () => {
    if (!agentState.localJobId) return;
    try {
      const result = await api.getDbtLocalStatus(agentState.localJobId, agentState.localLogOffset);
      const chunk = result.logs || [];
      if (chunk.length) {
        const prefix = agentState.localLogs && !agentState.localLogs.endsWith('\n') ? '\n' : '';
        agentState.localLogs += prefix + chunk.join('\n') + '\n';
      }
      if (typeof result.logOffset === 'number') agentState.localLogOffset = result.logOffset;
      agentState.localStatus = result.status || 'running';
      agentState.localSummary = result.summary || '';
      if (Array.isArray(result.models)) agentState.localModels = result.models;
      if (result.finished) {
        agentState.localRunning = false;
        agentState.localCancelling = false;
        agentState.localFinished = true;
        if (result.status === 'error' && result.error) agentState.localError = result.error;
        // Record the outcome for the Report page's "Deployed" stage.
        if (result.status === 'success' || result.status === 'error') {
          recordDeployment(
            'dbt on Databricks',
            result.status === 'success' ? 'success' : 'failed',
            agentState.localSummary || result.error || '',
          );
        }
        clearInterval(localPollTimer);
        localPollTimer = null;
      }
      renderLocalView();
    } catch (err) {
      agentState.localRunning = false;
      agentState.localCancelling = false;
      agentState.localStatus = 'error';
      agentState.localError = err.message;
      clearInterval(localPollTimer);
      localPollTimer = null;
      renderLocalView();
    }
  }, 2000);
}

export function bindLocalControls() {
  document.getElementById('run-dbt-local-btn')?.addEventListener('click', runDbtLocalBuild);
  document.getElementById('cancel-dbt-local-btn')?.addEventListener('click', cancelLocalRun);
  // Restore the in-flight / finished view when the page re-renders.
  if (agentState.localJobId && agentState.localRunning && !localPollTimer) startLocalPolling();
  renderLocalView();
}

export function destroyLocalRun() {
  if (localPollTimer) clearInterval(localPollTimer);
  localPollTimer = null;
}
