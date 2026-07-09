import { api } from '../../api.js';
import { store } from '../../store.js';
import { readConfig } from './form-mode.js';
import { agentState } from './state.js';
import { updateStatus } from './status-mode.js';
import { destroyLocalRun } from './local-mode.js';
import { setBusy, escapeHtml } from './utils.js';

let pollTimer = null;

export async function testConnection() {
  const btn = document.getElementById('test-dbt-btn');
  const errEl = document.getElementById('dbt-conn-error');
  setBusy(btn, true, 'Testing...');
  agentState.error = '';
  if (errEl) {
    errEl.style.display = 'none';
    errEl.textContent = '';
  }
  try {
    const result = await api.testDbtCloudConnection(readConfig());
    agentState.connected = true;
    agentState.account = result.account || null;
    agentState.projects = result.projects || [];
    agentState.jobs = result.jobs || [];
  } catch (err) {
    agentState.connected = false;
    // Surface the connection error inline, next to the Test Login button, rather
    // than in the (distant) Run card status, so it's obvious the test failed.
    if (errEl) {
      // role="alert" so screen readers announce the failure; wrap the raw backend
      // message in a human sentence with what to check.
      errEl.setAttribute('role', 'alert');
      errEl.textContent = `Could not connect: ${err.message}. Check the token, account ID, and API URL.`;
      errEl.style.display = '';
    }
  } finally {
    setBusy(btn, false, 'Test Login');
    updateConnectionView();
    updateStatus();
  }
}

// ─── Connection view: collapse the credential form into a compact summary ──────
// once connected, and turn the Account/Project/Job IDs into auto-filled pickers.

export function updateConnectionView() {
  const form = document.getElementById('dbt-connect-form');
  const summary = document.getElementById('dbt-connected-summary');
  const runBtn = document.getElementById('run-dbt-btn');
  const badge = document.getElementById('dbt-conn-badge');
  if (!form || !summary) return;

  if (agentState.connected) {
    form.style.display = 'none';
    summary.style.display = '';
    if (badge) {
      badge.className = 'dbx-status-pill dbx-status-ok';
      badge.innerHTML = '<span class="dbx-status-dot"></span>Connected';
    }

    const accId = document.getElementById('dbt-account-id')?.value || '';
    const accName = agentState.account && (agentState.account.name || agentState.account.account_name);
    const accEl = document.getElementById('dbt-conn-account');
    if (accEl) accEl.textContent = accName ? `${accName} · account ${accId}` : `account ${accId}`;

    const projSel = document.getElementById('dbt-project-id');
    if (projSel) {
      // Start empty: the user must explicitly pick a project (and then a job).
      // A disabled placeholder keeps the field "nil" until they choose.
      projSel.innerHTML = '<option value="" disabled selected>Select a project…</option>'
        + agentState.projects.map(p => `<option value="${p.id}">${escapeHtml(p.name || ('Project ' + p.id))}</option>`).join('');
      projSel.value = '';
    }
    populateJobs(); // also sets the Run button state from the selected project/job
  } else {
    form.style.display = '';
    summary.style.display = 'none';
    if (badge) {
      badge.className = 'dbx-status-pill dbx-status-idle';
      badge.innerHTML = '<span class="dbx-status-dot"></span>Not connected';
    }
    if (runBtn) runBtn.disabled = true;
  }
}

function populateJobs() {
  const jobSel = document.getElementById('dbt-job-id');
  if (!jobSel) return;
  const selectedProject = document.getElementById('dbt-project-id')?.value || '';
  if (!selectedProject) {
    // A job can't be chosen until a project is selected.
    jobSel.innerHTML = '<option value="" disabled selected>Select a project first…</option>';
    jobSel.value = '';
    syncRunButtons();
    return;
  }
  const jobs = agentState.jobs.filter(j => String(j.projectId) === String(selectedProject));
  // Start empty: the user must explicitly pick a job from this project.
  jobSel.innerHTML = '<option value="" disabled selected>Select a job…</option>'
    + (jobs.length
        ? jobs.map(j => `<option value="${j.id}">${escapeHtml(j.name || ('Job ' + j.id))}</option>`).join('')
        : '<option value="" disabled>No jobs in this project</option>');
  jobSel.value = '';
  // Run stays disabled until both a project and a job are chosen (syncRunButtons).
  syncRunButtons();
}

export function bindConnectionControls() {
  // "Edit" reveals the credential form again without dropping the session.
  document.getElementById('dbt-edit-conn')?.addEventListener('click', () => {
    const form = document.getElementById('dbt-connect-form');
    const summary = document.getElementById('dbt-connected-summary');
    if (form) form.style.display = '';
    if (summary) summary.style.display = 'none';
  });
  // Narrow the Job picker to the chosen Project (and reset the job selection).
  document.getElementById('dbt-project-id')?.addEventListener('change', populateJobs);
  // Picking a job is what finally enables Run.
  document.getElementById('dbt-job-id')?.addEventListener('change', syncRunButtons);
}

// A run is "active" while it is queued/starting/running — keep the Run button
// disabled for that whole window (not just the trigger request) so repeated
// clicks can't queue duplicate runs. Re-enables once the run reaches a terminal
// state or errors.
const TERMINAL_STATUSES = ['Success', 'Error', 'Cancelled'];

export function syncRunButtons() {
  const runBtn = document.getElementById('run-dbt-btn');
  const cancelBtn = document.getElementById('cancel-dbt-btn');
  const active = !!agentState.runId && !agentState.error && !TERMINAL_STATUSES.includes(agentState.status);
  if (runBtn) {
    if (active) {
      runBtn.disabled = true;
      runBtn.textContent = `Running… (${agentState.status || 'Queued'})`;
      runBtn.removeAttribute('title');
    } else {
      // Not running: Run requires a verified connection AND an explicit project
      // and job selection — without both, the agent cannot run.
      const projSel = document.getElementById('dbt-project-id');
      const jobSel = document.getElementById('dbt-job-id');
      const ready = agentState.connected && projSel && projSel.value && jobSel && jobSel.value;
      runBtn.disabled = !ready;
      // After a terminal run, label it "Re-run" to match the local card.
      const finished = !!agentState.runId && TERMINAL_STATUSES.includes(agentState.status);
      runBtn.textContent = finished ? 'Re-run Agent' : 'Run Agent';
      // Tell the user why Run is gated when connected but selections are missing.
      if (agentState.connected && !ready) {
        runBtn.title = 'Select a project and job to enable Run';
      } else {
        runBtn.removeAttribute('title');
      }
    }
  }
  // The Stop button only exists/enables while a run is in-flight.
  if (cancelBtn) {
    cancelBtn.style.display = active ? '' : 'none';
    if (active && cancelBtn.dataset.busy !== '1') {
      cancelBtn.disabled = false;
      cancelBtn.textContent = 'Stop run';
    }
  }
}

export async function cancelRun() {
  const btn = document.getElementById('cancel-dbt-btn');
  if (!agentState.runId || (btn && btn.disabled)) return;
  if (btn) {
    btn.dataset.busy = '1';
    setBusy(btn, true, 'Stopping…');
  }
  try {
    const result = await api.cancelDbtCloudRun({
      ...readConfig(),
      runId: agentState.runId,
    });
    // Reflect the cancel immediately; the poller will converge to "Cancelled".
    agentState.status = result.statusHumanized || result.status || 'Cancelled';
    agentState.statusDetail = 'Cancellation requested.';
  } catch (err) {
    agentState.error = err.message;
  } finally {
    if (btn) {
      btn.dataset.busy = '';
      btn.disabled = false;
      btn.textContent = 'Stop run';
    }
    updateStatus();
    syncRunButtons();
  }
}

export async function runAgent() {
  const btn = document.getElementById('run-dbt-btn');
  if (btn && btn.disabled) return; // guard against a double-fire while a run is active
  setBusy(btn, true, 'Starting…');
  agentState.error = '';
  try {
    const result = await api.runDbtCloudJob({
      ...readConfig(),
      sessionId: store.get().sessionId,
    });
    agentState.connected = true;
    agentState.runId = result.runId;
    agentState.status = result.statusHumanized || result.status || 'Queued';
    agentState.runHref = result.href || '';
    agentState.statusDetail = '';
    startPolling();
  } catch (err) {
    agentState.error = err.message;
  } finally {
    updateStatus();
    syncRunButtons();
  }
}

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    if (!agentState.runId) return;
    try {
      const result = await api.getDbtCloudRunStatus({
        ...readConfig(),
        runId: agentState.runId,
      });
      agentState.status = result.statusHumanized || result.status || '';
      agentState.runHref = result.href || agentState.runHref;
      const finished = result.finishedAt ? `finished ${result.finishedAt}` : '';
      // Surface dbt's reason for a cancelled/errored run (e.g. superseded, no run slots).
      agentState.statusDetail = result.statusMessage
        ? `${result.statusMessage}${finished ? ` — ${finished}` : ''}`
        : finished;
      updateStatus();
      syncRunButtons();
      if (TERMINAL_STATUSES.includes(agentState.status)) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
    } catch (err) {
      agentState.error = err.message;
      updateStatus();
      syncRunButtons();
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }, 5000);
}

export function destroyAgentPage() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = null;
  destroyLocalRun();
}
