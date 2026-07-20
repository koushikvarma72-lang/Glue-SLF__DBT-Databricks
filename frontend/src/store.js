/**
 * sfglue — Global State Store
 * Simple reactive state management for the Snowflake + AWS Glue → Databricks/dbt flow.
 *
 * (Trimmed from the multi-tool BI app: the Qlik/QVD/Cognos/Tableau/dbxAgent state
 * families and the QVF per-file review subsystem were removed — this app hosts only
 * the sfglue flow.)
 */

const state = {
  currentPage: 'sfglue-home',
  sessionId: null,
  filename: null,

  // Data surfaced by the lineage/review pages
  graph: { nodes: [], edges: [] },
  tables: [],
  metadata: null,
  script: '',
  description: '',

  dialect: 'databricks',
  uploadMode: 'snowflake_glue',
  // Post-deploy reconciliation result — Report page "Reconciled against source" stage.
  reconciliation: null,

  // ─── Snowflake/Glue → Databricks/DBT flow ────────────────────────────────────
  sfGlueSnowflakeConfig: {
    account: '', user: '', password: '', role: '', warehouse: '', database: '', schema: '', authenticator: '',
  },
  sfGlueGlueConfig: {
    region: '', access_key_id: '', secret_access_key: '', session_token: '', profile_name: '', catalog_id: '',
  },
  sfGlueSnowflakeConnection: null,   // { success, identity, warehouses, databases } from test-connection
  sfGlueSnowflakeSchemas: [],        // schemas of the selected database (for the Schema picker)
  sfGlueGlueConnection: null,        // { success, identity } from test-connection
  sfGlueSelectedBucket: '',          // S3 bucket chosen in the bucket picker (persisted)
  sfGlueLineage: null,               // { lineage, duplicates, recommendations, summary, ai_used, jobs }
  sfGlueSelectedTables: [],          // node ids (sf:...) the user chose to migrate
  sfGlueDestination: {
    workspace_url: '', token: '', sql_warehouse_id: '',
    catalog: 'lakehouse', bronze_schema: 'bronze', silver_schema: 'silver', gold_schema: 'gold',
  },
  sfGlueReview: null,                // { tables, views(+sql), glue_tables, glue_jobs(+script), business_logic }
  sfGluePrecheck: null,              // { targets, already_present, to_migrate, required_not_selected }
  sfGlueConversion: null,            // { plan, notebooks, dbt_models, ddl, sources_yml }
  sfGlueArtifactEdits: {},           // { artifactKey: editedCode } edits on the Migrate page
  sfGlueArtifactExplain: {},         // { artifactKey: explanationText }
  sfGlueColumnEdits: {},             // { full_name: "col TYPE\n..." } edited source columns
  // ── Downstream run results (deploy/build/seed/reconcile/tests/grade). Declared
  //    here so they're never implicit globals, and cleared by sfGlueResultDefaults()
  //    on re-analyze / reset (previously they lingered and a stale "Deploy complete"
  //    could show against a new selection). ──
  sfGlueDeploy: null, isDeployingSfGlue: false,
  sfGlueBuild: null, isBuildingSfGlue: false,
  sfGlueSeedBronze: null, isSeedingSfGlue: false,
  sfGlueReconcile: null, sfGlueReconcileKeys: {}, sfGlueReconcileError: '', isReconcilingSfGlue: false,
  sfGlueTests: null, sfGlueTestError: '', isTestingSfGlue: false,
  sfGlueQualityGrade: null,
  // Optional Postgres source (auto-ingestion helper on the Connect step).
  sfGluePostgresConfig: { host: '', port: '5432', database: '', user: '', password: '', schema: '' },
  sfGluePostgresConnection: null, isTestingPostgres: false,

  // sfGlue step loading flags
  isTestingSnowflake: false,
  isTestingGlue: false,
  isBuildingSfGlueLineage: false,
  isReviewingSfGlue: false,
  isPrecheckingSfGlue: false,
  isConvertingSfGlue: false,

  // Listeners
  _listeners: [],
};

// Re-entrancy-safe notification. The single subscriber (main.js) re-renders the
// whole current page on every change, and some pages call store.set/navigate during
// render (redirect guards). Without protection that re-enters the renderer and can
// recurse without bound. _notify() runs listeners synchronously (so the DOM is ready
// right after set(), as callers expect); a re-entrant set/navigate is coalesced into
// one extra pass instead of recursing, and a hard cap guarantees no render-loop hang.
let _notifying = false;
let _renderQueued = false;
const _MAX_NOTIFY_PASSES = 50;

// Volatile Snowflake/Glue downstream-run results — cleared on re-analyze and reset
// so a new selection never inherits a prior run's deploy/build/reconcile/test outcome.
function sfGlueResultDefaults() {
  return {
    sfGlueDeploy: null, isDeployingSfGlue: false,
    sfGlueBuild: null, isBuildingSfGlue: false,
    sfGlueSeedBronze: null, isSeedingSfGlue: false,
    sfGlueReconcile: null, sfGlueReconcileKeys: {}, sfGlueReconcileError: '', isReconcilingSfGlue: false,
    sfGlueTests: null, sfGlueTestError: '', isTestingSfGlue: false,
    sfGlueQualityGrade: null,
  };
}

// NOTE: connection configs — including credentials — ARE persisted to localStorage
// so a restart on this local dev machine doesn't require retyping. This is an
// intentional local-dev convenience; do not enable it in a hosted/shared build.

export const store = {
  get() {
    return state;
  },

  _notify() {
    if (_notifying) { _renderQueued = true; return; }
    _notifying = true;
    try {
      let passes = 0;
      do {
        _renderQueued = false;
        const listeners = state._listeners.slice();
        for (const fn of listeners) fn(state);
        if (++passes >= _MAX_NOTIFY_PASSES) {
          _renderQueued = false;
          console.warn('[store] render loop exceeded ' + _MAX_NOTIFY_PASSES + ' passes — breaking to avoid a hang. A render-time store.set/navigate is likely oscillating.');
          break;
        }
      } while (_renderQueued);
    } finally {
      _notifying = false;
    }
  },

  set(updates) {
    Object.assign(state, updates);
    // Persist sessionId so a page refresh can restore the session
    if (updates.sessionId !== undefined) {
      if (updates.sessionId) {
        localStorage.setItem('qvf_session_id', updates.sessionId);
      } else {
        localStorage.removeItem('qvf_session_id');
      }
    }
    // Persist the active tool (uploadMode) so a refresh restores the right flow.
    if (updates.uploadMode !== undefined) {
      try {
        if (updates.uploadMode) localStorage.setItem('qvf_upload_mode', state.uploadMode);
        else localStorage.removeItem('qvf_upload_mode');
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    // Persist the selected dialect (dbt vs PySpark) so a page refresh doesn't
    // silently bounce the user back to the dbt flow mid-migration.
    if (updates.dialect !== undefined) {
      try {
        localStorage.setItem('qvf_dialect', state.dialect || 'databricks');
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    // Source connection configs are persisted at the user's request (local demo
    // machine) so a restart doesn't require retyping. NOTE: this stores secrets
    // (Snowflake password, AWS keys, Postgres password) in browser localStorage —
    // acceptable for a local demo, not for shared machines.
    if (updates.sfGlueSnowflakeConfig !== undefined) {
      try {
        localStorage.setItem('qvf_sfglue_snowflake_config', JSON.stringify((state.sfGlueSnowflakeConfig || {})));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    if (updates.sfGlueGlueConfig !== undefined) {
      try {
        localStorage.setItem('qvf_sfglue_glue_config', JSON.stringify((state.sfGlueGlueConfig || {})));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    if (updates.sfGluePostgresConfig !== undefined) {
      try {
        localStorage.setItem('qvf_sfglue_postgres_config', JSON.stringify((state.sfGluePostgresConfig || {})));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    if (updates.sfGlueSelectedBucket !== undefined) {
      try {
        localStorage.setItem('qvf_sfglue_bucket', state.sfGlueSelectedBucket || '');
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    if (updates.sfGlueDestination !== undefined) {
      try {
        localStorage.setItem('qvf_sfglue_destination', JSON.stringify((state.sfGlueDestination || {})));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    this._notify();
  },

  subscribe(fn) {
    state._listeners.push(fn);
    return () => {
      state._listeners = state._listeners.filter(l => l !== fn);
    };
  },

  navigate(page) {
    state.currentPage = page;
    window.location.hash = page;
    this._notify();
  },

  // Merge a partial into a Snowflake/Glue source config in memory WITHOUT
  // re-rendering, so changing a dropdown (e.g. Database) doesn't wipe unsaved text
  // in the other form. Credentials are session-only and never persisted to storage.
  // invalidateLineage=true drops stale lineage/selection so a database change forces
  // a fresh analysis instead of showing the old graph.
  patchSfGlueConfig(which, partial, { invalidateLineage = false } = {}) {
    const key = which === 'glue' ? 'sfGlueGlueConfig' : 'sfGlueSnowflakeConfig';
    state[key] = { ...state[key], ...partial };
    if (invalidateLineage) {
      state.sfGlueLineage = null;
      state.sfGlueReview = null;
      state.sfGlueSelectedTables = [];
      state.sfGluePrecheck = null;
      state.sfGlueConversion = null;
      state.sfGlueArtifactEdits = {};
      state.sfGlueArtifactExplain = {};
      // Downstream results are tied to the old lineage — drop them too so
      // Deploy/Build/Reconcile panels don't show stale outcomes.
      Object.assign(state, sfGlueResultDefaults());
    }
  },

  reset() {
    Object.assign(state, {
      sessionId: null,
      filename: null,
      graph: { nodes: [], edges: [] },
      tables: [],
      metadata: null,
      script: '',
      description: '',
      dialect: 'databricks',
      uploadMode: 'snowflake_glue',
      reconciliation: null,
      // Snowflake/Glue flow — connections, lineage, artifacts, and run results.
      sfGlueSnowflakeConnection: null, sfGlueGlueConnection: null, sfGlueSnowflakeSchemas: [],
      sfGlueLineage: null, sfGlueReview: null, sfGlueSelectedTables: [],
      sfGluePrecheck: null, sfGlueConversion: null, sfGlueArtifactEdits: {},
      sfGlueArtifactExplain: {}, sfGlueColumnEdits: {},
      sfGluePostgresConnection: null, isTestingPostgres: false,
      ...sfGlueResultDefaults(),
    });
    localStorage.removeItem('qvf_session_id');
    localStorage.removeItem('qvf_upload_mode');
    localStorage.removeItem('qvf_dialect');
    localStorage.removeItem('qvf_sfglue_destination');
    localStorage.removeItem('qvf_sfglue_snowflake_config');
    localStorage.removeItem('qvf_sfglue_glue_config');
    localStorage.removeItem('qvf_sfglue_postgres_config');
    localStorage.removeItem('qvf_sfglue_bucket');
    this._notify();
  },
};

// Initialize from URL hash. main.js's validNavPage() is the authoritative gate, so
// keep this permissive (any sfglue-* page) rather than a drifting hard-coded list.
const hash = window.location.hash.slice(1);
if (/^sfglue-[a-z-]+$/.test(hash)) {
  state.currentPage = hash;
}

// Restore sessionId + active tool from localStorage so a page refresh reconnects to
// the active session and the correct flow.
const _storedSessionId = localStorage.getItem('qvf_session_id');
if (_storedSessionId) {
  state.sessionId = _storedSessionId;
}
const _storedUploadMode = localStorage.getItem('qvf_upload_mode');
if (['snowflake_glue'].includes(_storedUploadMode)) {
  state.uploadMode = _storedUploadMode;
}

// Restore the selected dialect (dbt vs PySpark) so a page refresh keeps the
// user on the flow they were using instead of resetting to the dbt default.
const _storedDialect = localStorage.getItem('qvf_dialect');
if (['pyspark', 'databricks', 'snowflake'].includes(_storedDialect)) {
  state.dialect = _storedDialect;
}

// Restore the persisted source connection configs (user-requested convenience on a
// local demo machine — includes secrets; see the matching note in set()).
try {
  for (const [key, stateKey] of [
    ['qvf_sfglue_snowflake_config', 'sfGlueSnowflakeConfig'],
    ['qvf_sfglue_glue_config', 'sfGlueGlueConfig'],
    ['qvf_sfglue_postgres_config', 'sfGluePostgresConfig'],
  ]) {
    const stored = localStorage.getItem(key);
    if (stored) state[stateKey] = { ...state[stateKey], ...JSON.parse(stored) };
  }
  const _storedBucket = localStorage.getItem('qvf_sfglue_bucket');
  if (_storedBucket) state.sfGlueSelectedBucket = _storedBucket;
  const _storedDest = localStorage.getItem('qvf_sfglue_destination');
  if (_storedDest) {
    state.sfGlueDestination = { ...state.sfGlueDestination, ...JSON.parse(_storedDest) };
  }
} catch (_) { /* ignore malformed/unavailable storage */ }
