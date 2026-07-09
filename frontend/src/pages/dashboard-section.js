/**
 * Portable AI/BI Dashboard section (vanilla JS).
 *
 * Renders the "AI/BI Dashboards" card: Detect Charts -> preview the mapping ->
 * tick which charts to migrate -> Deploy to Databricks. Mirrors the host app's
 * store/api conventions; adjust the two imports below to your project.
 *
 * Host contract (see README.md for the full data shapes):
 *  - `store`: { get(): state, set(partial), subscribe(fn) } and a full re-render
 *    fires on every `set` (so checkbox toggles update the DOM directly here to
 *    avoid resetting the chart list's scroll position).
 *  - `api`: object you extend with the two methods at the bottom of this file.
 *  - `escapeHtml(str)`: HTML-escape helper.
 *  - State fields used: sessionId, dbxAgentConfig, dbxAgentDashboardPreview,
 *    dbxAgentDeployDashboardResult, isPreviewingDbxAgentDashboard,
 *    isDeployingDbxAgentDashboard.
 *  - `collectConfig()`: returns the Databricks connection config object the
 *    backend's `config_from_payload` understands
 *    ({ workspace_url, personal_access_token, sql_warehouse_id, catalog, schema }).
 */

// ⬇️ Adjust these imports to your project's paths.
import { store } from '../store.js';
import { api } from '../api.js';
import { escapeHtml } from '../utils.js';

// Minimal inline icons (or import your own ICON map and delete this).
const S = (inner, opts = '') => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" ${opts}>${inner}</svg>`;
const ICON = {
  chart: S('<line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/>'),
  check: S('<polyline points="20 6 9 17 4 12"/>'),
  x: S('<line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/>'),
  external: S('<path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/>'),
};

// ── Render ───────────────────────────────────────────────────────────────────

export function renderDashboardSection(state) {
  const preview = state.dbxAgentDashboardPreview;
  const deploy = state.dbxAgentDeployDashboardResult;
  const summary = preview && !preview.error ? preview.summary : null;
  const canDeploy = !!(summary && summary.mapped > 0);

  const statusBadge = (status) => status === 'mapped'
    ? `<span class="dbx-badge dbx-badge-ok">mapped</span>`
    : `<span class="dbx-badge dbx-badge-skip">skipped</span>`;

  return `
    <div class="dbx-card-head">
      <div class="dbx-card-headline">
        <span class="dbx-card-icon">${ICON.chart}</span>
        <div>
          <h3 class="dbx-card-title">AI/BI Dashboards</h3>
          <p class="dbx-card-desc">Rebuild the QVF's Qlik charts as a native Databricks AI/BI dashboard on your migrated tables. Needs a SQL Warehouse.</p>
        </div>
      </div>
      <div class="dbx-card-actions">
        <button class="btn btn-primary" id="dbx-agent-preview-dashboard-btn" ${state.isPreviewingDbxAgentDashboard ? 'disabled' : ''}>
          ${state.isPreviewingDbxAgentDashboard ? '<span class="spinner"></span> Detecting…' : 'Detect Charts'}
        </button>
        ${canDeploy ? `
          <button class="btn btn-success" id="dbx-agent-deploy-dashboard-btn" ${state.isDeployingDbxAgentDashboard ? 'disabled' : ''}>
            ${state.isDeployingDbxAgentDashboard ? '<span class="spinner"></span> Deploying…' : 'Deploy to Databricks'}
          </button>
        ` : ''}
      </div>
    </div>

    ${!preview ? `
      <div class="dbx-empty">Click <strong>Detect Charts</strong> to read the charts extracted from the QVF and map them to AI/BI widgets. The target catalog/schema are used for the dataset queries.</div>
    ` : ''}
    ${preview?.error ? `<div class="dbx-alert dbx-alert-error">${escapeHtml(preview.error)}</div>` : ''}

    ${summary ? `
      <div class="dbx-alert ${preview.validatedAgainstLiveSchema ? 'dbx-alert-success' : 'dbx-alert-info'}" style="margin-top:0">
        ${preview.validatedAgainstLiveSchema
          ? '✓ Built against your live Unity Catalog schema — datasets target the real (migrated) column names.'
          : 'Built from extracted metadata. Add a SQL Warehouse + token and re-detect so datasets target the real migrated columns.'}
      </div>
      <div class="dbx-checks">
        <div class="dbx-check dbx-check-neutral"><span class="dbx-check-dot"></span>${summary.total_charts} chart${summary.total_charts === 1 ? '' : 's'} found</div>
        <div class="dbx-check dbx-check-${summary.mapped ? 'ok' : 'fail'}"><span class="dbx-check-dot">${summary.mapped ? ICON.check : ICON.x}</span>${summary.mapped} mapped</div>
        ${summary.skipped ? `<div class="dbx-check dbx-check-fail"><span class="dbx-check-dot">${ICON.x}</span>${summary.skipped} skipped</div>` : ''}
        <div class="dbx-check dbx-check-neutral"><span class="dbx-check-dot"></span>${summary.pages} page${summary.pages === 1 ? '' : 's'} · ${summary.datasets} dataset${summary.datasets === 1 ? '' : 's'}</div>
      </div>
    ` : ''}

    ${(preview?.warnings || []).map(w => `<div class="dbx-alert dbx-alert-warn">${escapeHtml(w)}</div>`).join('')}

    ${preview?.charts?.length ? `
      <div class="dbx-chart-select-bar">
        <label class="dbx-chart-select-all">
          <input type="checkbox" id="dbx-chart-select-all" ${summary && summary.mapped > 0 ? 'checked' : ''} ${summary && summary.mapped > 0 ? '' : 'disabled'} />
          Select all mappable
        </label>
        <span class="dbx-chart-select-count" id="dbx-chart-select-count"></span>
      </div>
      <div class="dbx-chart-scroll">
      <div class="dbx-ddl-list">
        ${preview.charts.map((c, i) => {
          const cid = c.id != null ? String(c.id) : `viz_${i}`;
          const selectable = c.status === 'mapped';
          return `
          <div class="dbx-ddl-item ${selectable ? '' : 'dbx-ddl-item-skip'}">
            <div class="dbx-ddl-item-head">
              <input type="checkbox" class="dbx-chart-select" data-chart-id="${escapeHtml(cid)}"
                ${selectable ? 'checked' : 'disabled'}
                title="${selectable ? 'Include this chart in the dashboard' : 'Skipped charts cannot be deployed'}" />
              <code class="dbx-ddl-src">${escapeHtml(c.qlikType || 'chart')}</code>
              <span class="dbx-ddl-arrow">&rarr;</span>
              <span class="dbx-ddl-target">${escapeHtml(c.widgetType || '')}</span>
              ${statusBadge(c.status)}
              ${c.title ? `<span class="dbx-ddl-title">${escapeHtml(c.title)}</span>` : ''}
            </div>
            <div class="dbx-chart-meta">
              ${c.table ? `<span>table <strong>${escapeHtml(c.table)}</strong></span>` : ''}
              ${(c.dimensions || []).length ? `<span>dims: ${escapeHtml((c.dimensions || []).join(', '))}</span>` : ''}
              ${(c.measures || []).length ? `<span>measures: ${escapeHtml((c.measures || []).join(', '))}</span>` : ''}
            </div>
            ${c.reason ? `<div class="dbx-alert dbx-alert-warn" style="margin:4px 0 0">${escapeHtml(c.reason)}</div>` : ''}
            ${(c.warnings || []).map(w => `<div class="dbx-chart-warn">⚠ ${escapeHtml(w)}</div>`).join('')}
          </div>
        `;}).join('')}
      </div>
      </div>
    ` : ''}

    ${deploy ? `
      <div class="dbx-alert ${deploy.success ? 'dbx-alert-success' : 'dbx-alert-error'}">
        ${deploy.success
          ? `Dashboard <strong>${escapeHtml(deploy.display_name || '')}</strong> ${deploy.updated ? 'updated' : 'created'}${deploy.summary ? ` with ${deploy.summary.mapped} widget${deploy.summary.mapped === 1 ? '' : 's'}` : ''}.`
          : escapeHtml(deploy.error || deploy.message || 'Dashboard deployment failed.')}
        ${deploy.success && deploy.dashboard_url
          ? `<div style="margin-top:6px"><a class="dbx-check dbx-check-link" href="${escapeHtml(deploy.dashboard_url)}" target="_blank" rel="noopener"><span class="dbx-check-dot">${ICON.external}</span>Open in Databricks</a></div>`
          : ''}
      </div>
    ` : ''}
  `;
}

// ── Wiring ─────────────────────────────────────────────────────────────────
// Call this after each render (it no-ops when the chart list isn't present).

export function setupDashboardHandlers(collectConfig) {
  document.getElementById('dbx-agent-preview-dashboard-btn')
    ?.addEventListener('click', () => handlePreviewDashboard(collectConfig));
  document.getElementById('dbx-agent-deploy-dashboard-btn')
    ?.addEventListener('click', () => handleDeployDashboard(collectConfig));
  setupDashboardChartSelection();
}

// Read the chart ids the user ticked. Returns null when the selection UI isn't
// present (deploy everything — back-compat).
function selectedDashboardChartIds() {
  const boxes = document.querySelectorAll('.dbx-chart-select');
  if (!boxes.length) return null;
  return Array.from(boxes).filter(b => b.checked).map(b => b.dataset.chartId);
}

// Update count + deploy button in place (no store.set -> no re-render -> the
// chart list keeps its scroll position while ticking boxes).
function setupDashboardChartSelection() {
  const boxes = Array.from(document.querySelectorAll('.dbx-chart-select'));
  if (!boxes.length) return;
  const selectAll = document.getElementById('dbx-chart-select-all');
  const countLabel = document.getElementById('dbx-chart-select-count');
  const selectable = boxes.filter(b => !b.disabled);

  const refresh = () => {
    const checked = selectable.filter(b => b.checked).length;
    if (countLabel) countLabel.textContent = `${checked} of ${selectable.length} selected`;
    if (selectAll) {
      selectAll.checked = checked > 0 && checked === selectable.length;
      selectAll.indeterminate = checked > 0 && checked < selectable.length;
    }
    const deployBtn = document.getElementById('dbx-agent-deploy-dashboard-btn');
    if (deployBtn && !store.get().isDeployingDbxAgentDashboard) deployBtn.disabled = checked === 0;
  };

  selectable.forEach(b => b.addEventListener('change', refresh));
  selectAll?.addEventListener('change', () => {
    selectable.forEach(b => { b.checked = selectAll.checked; });
    refresh();
  });
  refresh();
}

async function handlePreviewDashboard(collectConfig) {
  const state = store.get();
  const config = collectConfig();
  store.set({ isPreviewingDbxAgentDashboard: true, dbxAgentConfig: config });
  try {
    const result = await api.previewDatabricksDashboard(state.sessionId, config);
    store.set({ isPreviewingDbxAgentDashboard: false, dbxAgentDashboardPreview: result, dbxAgentDeployDashboardResult: null });
  } catch (err) {
    store.set({ isPreviewingDbxAgentDashboard: false, dbxAgentDashboardPreview: { error: err.message, charts: [] } });
  }
}

async function handleDeployDashboard(collectConfig) {
  const state = store.get();
  const config = collectConfig();
  if (!config.sql_warehouse_id) {
    store.set({ dbxAgentDeployDashboardResult: { success: false, error: 'A SQL Warehouse ID is required to deploy an AI/BI dashboard.' } });
    return;
  }
  const selectedChartIds = selectedDashboardChartIds();
  if (selectedChartIds && selectedChartIds.length === 0) {
    store.set({ dbxAgentDeployDashboardResult: { success: false, error: 'Select at least one chart to deploy.' } });
    return;
  }
  store.set({ isDeployingDbxAgentDashboard: true, dbxAgentConfig: config });
  try {
    const result = await api.deployDatabricksDashboard(state.sessionId, config, selectedChartIds || undefined);
    store.set({ isDeployingDbxAgentDashboard: false, dbxAgentDeployDashboardResult: result });
  } catch (err) {
    store.set({ isDeployingDbxAgentDashboard: false, dbxAgentDeployDashboardResult: { success: false, error: err.message } });
  }
}

/* ── Add these two methods to your `api` object ───────────────────────────────
   (API_BASE = your backend's /api root; route_prefix defaults to
   /databricks-agent — keep it in sync with register_dashboard_routes.)

  async previewDatabricksDashboard(sessionId, config) {
    const res = await fetch(`${API_BASE}/databricks-agent/dashboard-preview/${encodeURIComponent(sessionId)}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return res.json();
  },

  async deployDatabricksDashboard(sessionId, config, selectedChartIds) {
    const body = { ...(config || {}) };
    if (Array.isArray(selectedChartIds)) body.selected_chart_ids = selectedChartIds;
    const res = await fetch(`${API_BASE}/databricks-agent/deploy-dashboard/${encodeURIComponent(sessionId)}`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return res.json();
  },
─────────────────────────────────────────────────────────────────────────── */
