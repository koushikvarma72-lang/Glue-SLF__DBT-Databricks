import { store } from '../../store.js';

const FORM_CACHE_KEY = 'qvf_dbt_agent_form';
// Project/Job are now auto-populated dropdowns (filled after Test Login), so they
// are not cached as free text — only the credential/URL/commands fields are.
const FORM_FIELD_IDS = [
  'dbt-base-url',
  'dbt-account-id',
  'dbt-commands',
];

export function readConfig() {
  cacheForm();
  const config = {
    baseUrl: document.getElementById('dbt-base-url')?.value.trim(),
    token: document.getElementById('dbt-token')?.value.trim(),
    accountId: document.getElementById('dbt-account-id')?.value.trim(),
    projectId: document.getElementById('dbt-project-id')?.value.trim(),
    jobId: document.getElementById('dbt-job-id')?.value.trim(),
    commands: (document.getElementById('dbt-commands')?.value || '')
      .split('\n')
      .map(line => line.trim())
      .filter(Boolean),
  };
  persistDbtCloudConfig(config);
  return config;
}

// Mirror the dbt Cloud credentials/IDs so they persist across page reloads.
// IMPORTANT: do NOT use store.set() here. The app subscribes to every store
// change with a full renderApp() (main.js), which rebuilds the entire DOM and
// destroys this page (incl. its poll timer). Since readConfig() runs at the
// start of Test Login / Run Agent (and on every keystroke via bindFormCache),
// going through store.set() would tear the page down mid-click — the user sees
// a "reload" and loses the run progress. Instead we update the live store state
// in place and write localStorage directly (same key store.js uses), which
// persists across reloads without notifying subscribers.
function persistDbtCloudConfig(config) {
  const dbtCloudConfig = {
    baseUrl: config.baseUrl || '',
    token: config.token || '',
    accountId: config.accountId || '',
    projectId: config.projectId || '',
    jobId: config.jobId || '',
  };
  store.get().dbtCloudConfig = dbtCloudConfig;
  try {
    localStorage.setItem('qvf_dbt_cloud_config', JSON.stringify(dbtCloudConfig));
  } catch (_) {
    /* storage unavailable — non-fatal */
  }
}

export function cacheForm() {
  const form = {};
  FORM_FIELD_IDS.forEach(id => {
    const el = document.getElementById(id);
    if (el) form[id] = el.value;
  });
  sessionStorage.setItem(FORM_CACHE_KEY, JSON.stringify(form));
}

export function restoreCachedForm() {
  // First seed inputs from the persisted dbt Cloud config so credentials and
  // IDs survive a full page reload (sessionStorage does not).
  const saved = store.get().dbtCloudConfig || {};
  const fieldMap = {
    'dbt-base-url': saved.baseUrl,
    'dbt-token': saved.token,
    'dbt-account-id': saved.accountId,
    'dbt-project-id': saved.projectId,
    'dbt-job-id': saved.jobId,
  };
  Object.entries(fieldMap).forEach(([id, value]) => {
    const el = document.getElementById(id);
    // Don't clobber the base-url default ("…/api/v2") with an empty saved value.
    if (el && value) el.value = value;
  });

  // Then overlay any same-session cached free-text edits.
  const cached = JSON.parse(sessionStorage.getItem(FORM_CACHE_KEY) || '{}');
  Object.entries(cached).forEach(([id, value]) => {
    const el = document.getElementById(id);
    if (el) el.value = value;
  });
}

export function bindFormCache() {
  document.querySelectorAll('#agent-page input, #agent-page textarea').forEach(input => {
    input.addEventListener('input', () => {
      cacheForm();
      // Keep the persisted store in sync as the user types so a reload restores
      // exactly what they last entered (token, account/project/job, base URL).
      persistDbtCloudConfig({
        baseUrl: document.getElementById('dbt-base-url')?.value.trim(),
        token: document.getElementById('dbt-token')?.value.trim(),
        accountId: document.getElementById('dbt-account-id')?.value.trim(),
        projectId: document.getElementById('dbt-project-id')?.value.trim(),
        jobId: document.getElementById('dbt-job-id')?.value.trim(),
      });
    });
  });
}
