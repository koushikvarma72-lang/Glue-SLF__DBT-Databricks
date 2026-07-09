import { agentState } from './state.js';
import { escapeHtml } from '../../utils.js';

// Map a dbt Cloud run status to one of the existing pill colour variants.
function statusPillClass(status) {
  const s = String(status || '').toLowerCase();
  if (s === 'success') return 'dbx-status-ok';
  if (s === 'error' || s === 'cancelled') return 'dbx-status-fail';
  return 'dbx-status-run';
}

export function renderAgentStatus() {
  if (agentState.error) {
    const msg = escapeHtml(agentState.error);
    // A poll failure leaves runId set: the run may still be live in dbt Cloud, so
    // keep the last known run link and warn rather than collapsing to a bare error.
    if (agentState.runId) {
      const idTxt = `#${escapeHtml(String(agentState.runId))}`;
      const link = agentState.runHref
        ? `<a class="dbx-run-link" href="${escapeHtml(agentState.runHref)}" target="_blank" rel="noopener">Open run ${idTxt} in dbt Cloud <span aria-hidden="true">↗</span></a>`
        : `<span>Run ${idTxt}</span>`;
      return `<div class="dbx-run-status"><strong>Lost connection to dbt Cloud</strong> — the run may still be in progress. ${link}</div><div class="dbx-run-detail">${msg}</div>`;
    }
    return `<strong>Run failed.</strong> ${msg}`;
  }
  if (agentState.runId) {
    const status = agentState.status || 'Queued';
    const pill = `<span class="dbx-status-pill ${statusPillClass(status)}"><span class="dbx-status-dot" aria-hidden="true"></span>${escapeHtml(status)}</span>`;
    const idTxt = `#${escapeHtml(String(agentState.runId))}`;
    const link = agentState.runHref
      ? `<a class="dbx-run-link" href="${escapeHtml(agentState.runHref)}" target="_blank" rel="noopener">Open run ${idTxt} in dbt Cloud <span aria-hidden="true">↗</span></a>`
      : `<span>Run ${idTxt}</span>`;
    const detail = agentState.statusDetail ? `<div class="dbx-run-detail">${escapeHtml(agentState.statusDetail)}</div>` : '';
    return `<div class="dbx-run-status">${pill}${link}</div>${detail}`;
  }
  if (agentState.connected) {
    const projectCount = agentState.projects.length;
    const jobCount = agentState.jobs.length;
    return `Connection verified. Found ${projectCount} project${projectCount === 1 ? '' : 's'} and ${jobCount} job${jobCount === 1 ? '' : 's'}.`;
  }
  return 'Enter your dbt Cloud token, account ID, and API URL, then Test Login to pick a project and job. The token is only sent for this request.';
}

export function updateStatus() {
  const status = document.getElementById('agent-status');
  if (!status) return;
  status.classList.toggle('error', !!agentState.error);
  // Announce errors assertively, live run status politely.
  status.setAttribute('role', agentState.error ? 'alert' : 'status');
  status.setAttribute('aria-live', agentState.error ? 'assertive' : 'polite');
  status.innerHTML = renderAgentStatus();
}
