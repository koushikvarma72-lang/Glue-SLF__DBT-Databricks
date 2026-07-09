/**
 * QVF Decoder â€” Global State Store
 * Simple reactive state management
 */

const state = {
  currentPage: 'upload',      // 'upload' | 'business' | 'inspect' | 'review' | 'output' | 'deploy' | 'agent' | 'databricks-agent'
  sessionId: null,
  currentFileId: null,
  fileId: null,
  filename: null,

  // Data from upload
  graph: { nodes: [], edges: [] },
  tables: [],
  associations: [],
  metadata: null,
  script: '',
  sqlSections: [],
  description: '',
  generationPlan: [],
  generationPlanText: '',

  // Edited data (Page 2)
  editedSql: '',
  editedText: '',
  editMode: false,
  rightEditMode: false,
  activeRightTab: 'sql',

  // Regenerated output (Page 3)
  regeneratedSql: '',
  regeneratedText: '',
  regeneratedLineage: '',
  regeneration: null,
  regenerationHistory: [],
  outputTableSchema: null,
  isGeneratingOutputTableSchema: false,
  validationResult: null,
  isValidatingMigration: false,
  validationMode: 'quick',
  validationProgressMessage: '',

  // Review state, keyed by fileId
  reviewStateByFile: {},

  // UI State
  isUploading: false,
  isProcessing: false,
  uploadingFilename: null,
  isGenerating: false,
  uploadProgress: 0,
  dialect: 'databricks',
  migrationSource: 'qlik',          // landing: source platform
  migrationDestination: 'databricks', // landing: destination — seeds the default dialect/framework
  uploadMode: 'qvf',
  // Last deploy outcome (dbt local / Databricks notebook) — Report page stage.
  lastDeployment: null,
  // Post-deploy reconciliation result (schema/key/non-empty vs the deployed
  // Databricks tables) — Report page "Reconciled against source" stage.
  reconciliation: null,
  // Error text handed from a failed deploy to the Review chat ("Fix in Review").
  reviewChatPrefill: null,

  // ─── Snowflake/Glue → Databricks/DBT flow (uploadMode === 'snowflake_glue') ──
  sfGlueSnowflakeConfig: {
    account: '', user: '', password: '', role: '', warehouse: '', database: '', schema: '', authenticator: '',
  },
  sfGlueGlueConfig: {
    region: '', access_key_id: '', secret_access_key: '', session_token: '', profile_name: '', catalog_id: '',
  },
  sfGlueSnowflakeConnection: null,   // { success, identity, warehouses, databases } from test-connection
  sfGlueSnowflakeSchemas: [],        // schemas of the selected database (for the Schema picker)
  sfGlueGlueConnection: null,        // { success, identity } from test-connection
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
  //    here so they're never implicit globals, and cleared by SFGLUE_RESULT_KEYS
  //    below on re-analyze / source-switch / reset (previously they lingered and
  //    a stale "Deploy complete" could show against a new selection). ──
  sfGlueDeploy: null, isDeployingSfGlue: false,
  sfGlueBuild: null, isBuildingSfGlue: false,
  sfGlueSeedBronze: null, isSeedingSfGlue: false,
  sfGlueReconcile: null, sfGlueReconcileKeys: {}, sfGlueReconcileError: '', isReconcilingSfGlue: false,
  sfGlueTests: null, sfGlueTestError: '', isTestingSfGlue: false,
  sfGlueQualityGrade: null,
  // Optional Postgres source (auto-ingestion helper on the Connect step).
  sfGluePostgresConfig: { host: '', port: '5432', database: '', user: '', password: '', schema: '' },
  sfGluePostgresConnection: null, isTestingPostgres: false,

  // ─── Cognos/Qlik → Power BI (PBIP) flow (uploadMode === 'cognos_powerbi') ───
  cognosModel: null,          // full model from /api/cognos/model/<sid>: {graph, calculations, calcGraph, description, script, ...}
  cognosCalcs: [],            // calculations/measures list (mirror of cognosModel.calculations)
  cognosDaxByCalc: {},        // { calcId: {sql, dax, confidence, status, notes} } from convert-measure/dax-convert
  cognosSelectedCalcId: null, // active measure in the workbench
  cognosValidation: null,     // /api/cognos/validate-relationships result
  cognosDataSources: null,    // /api/cognos/table-data-status (embedded CSV/Excel data)
  cognosArtifactEdits: {},    // { calcId: editedDax } manual DAX edits
  cognosModelLoadError: null, // store-backed error so the Inspect lazy-fetch can't loop
  cognosAgentLog: [],         // DAX-Agent activity log, persisted so it survives re-render
  isUploadingCognos: false,
  isConvertingCognosDax: false,
  isValidatingCognosRel: false,
  isGeneratingCognosPbip: false,

  isTestingSnowflake: false,
  isTestingGlue: false,
  isBuildingSfGlueLineage: false,
  isReviewingSfGlue: false,
  isPrecheckingSfGlue: false,
  isConvertingSfGlue: false,

  // ─── Tableau → Power BI flow (uploadMode === 'tableau_powerbi') ──────────────
  tabPbiTableauConfig: {       // source: Tableau Server/Cloud PAT (session-only, NOT persisted)
    server_url: '', site: '', token_name: '', token_value: '', api_version: '3.19',
  },
  tabPbiTableauConnection: null,  // { success, identity } from Tableau test-connection
  tabPbiContent: null,            // { workbooks, datasources } listed from Tableau Server
  tabPbiSiteAnalysis: null,       // { summary, projects, workbooks, duplicates, stale } — migration-triage dashboard
  tabPbiAnalyzeError: null,       // error string from Analyze-site / migrate-from-dashboard (kept in state so it survives re-render)
  tabPbiTableauWorkbookId: null,  // live Tableau workbook id (set when migrating from a server workbook) — enables real render/PDF export
  isAnalyzingTableau: false,
  tabPbiMetadata: null,           // parsed tableau_metadata — held client-side, replayed to later steps
  tabPbiLineage: null,            // { lineage, duplicates, recommendations, summary, ai_used }
  tabPbiSelected: [],             // node ids (ds:...) the user chose to migrate
  tabPbiDestination: {            // destination: Power BI workspace (optional; persisted)
    workspace_id: '', dataset_name: '', tenant_id: '', client_id: '', client_secret: '', access_token: '',
  },
  tabPbiPowerBIConnection: null,  // { success, identity } from Power BI test-connection
  tabPbiReview: null,             // { datasources, worksheets, dashboards, parameters, business_logic }
  tabPbiPrecheck: null,           // { targets, already_present, to_migrate, required_not_selected }
  tabPbiConversion: null,         // { tmdl_tables, dax_measures, m_queries, model_tmdl, database_tmdl, pbip_files, report_notes, assumptions }
  tabPbiArtifactEdits: {},        // { artifactKey: editedCode }
  tabPbiArtifactExplain: {},      // { artifactKey: explanationText }
  isTestingTableau: false,
  isParsingTableau: false,
  isTestingPowerBI: false,
  isBuildingTabPbiLineage: false,
  isReviewingTabPbi: false,
  isPrecheckingTabPbi: false,
  isConvertingTabPbi: false,

  qvdInspection: null,
  qvdSchemaSuggestion: null,
  qvdBusinessAnalysis: null,
  qvdKpiCatalog: null,
  qvdLineageReconciliation: null,
  qvdAiExplanation: null,
  qvdEditableMapping: [],
  qvdApprovedMapping: null,
  qvdDdlGeneration: null,
  qvdRowPreviews: {},
  qvdColumnProfiles: {},
  qvdParquetConversions: {},
  qvdParquetValidations: {},
  qvdDatabricksLoadScripts: {},
  qvdMigrationPackages: {},
  qvdDatabricksConfig: {
    workspace_url: '',
    personal_access_token: '',
    sql_warehouse_id: '',
    catalog: 'main',
    schema: 'qvd_raw',
    volume: '',
    volume_path: '',
    cloud_storage_path: '',
  },
  qvdDatabricksWarehouses: [],
  qvdDatabricksCatalogs: [],
  qvdDatabricksSchemas: [],
  qvdDatabricksVolumes: [],
  qvdDatabricksUpload: null,
  qvdDatabricksConnection: null,
  qvdDatabricksPrecheck: null,
  qvdDatabricksExecution: null,
  qvdExecutionMode: 'generate_sql_only',
  qvdMappingValidationErrors: [],
  qvdSelectedFiles: [],
  isSuggestingQvdSchema: false,
  isDiscoveringQvdBusinessEntities: false,
  isGeneratingQvdKpiCatalog: false,
  isGeneratingQvdLineageReconciliation: false,
  isGeneratingQvdAiExplanation: false,
  isSavingQvdMapping: false,
  isGeneratingQvdDdl: false,
  qvdPreviewLoadingByFile: {},
  qvdProfileLoadingByFile: {},
  qvdParquetLoadingByFile: {},
  qvdParquetValidationLoadingByFile: {},
  qvdDatabricksLoadLoadingByFile: {},
  qvdMigrationPackageLoadingByFile: {},
  isSavingDatabricksConfig: false,
  isTestingDatabricksConnection: false,
  isDiscoveringDatabricksWarehouses: false,
  isDiscoveringDatabricksCatalogs: false,
  isDiscoveringDatabricksSchemas: false,
  isDiscoveringDatabricksVolumes: false,
  isPreparingDatabricksTarget: false,
  isUploadingDatabricksParquet: false,
  isRunningDatabricksPrecheck: false,
  isExecutingDatabricksMigration: false,

  // Databricks Agent (QVF flow)
  dbxAgentConfig: {
    workspace_url: '',
    personal_access_token: '',
    oauth_refresh_token: '',
    oauth_expires_at: 0,
    sql_warehouse_id: '',
    cluster_id: '',
    catalog: 'main',
    schema: 'default',
  },
  dbxAgentConnection: null,
  dbxAgentWarehouses: [],
  dbxAgentCatalogs: [],
  dbxAgentSchemas: [],
  dbxAgentSourceTables: null,
  dbxAgentCreateResult: null,
  dbxAgentNotebookPath: '',
  dbxAgentDeployResult: null,
  dbxAgentRunResult: null,
  dbxAgentRunStatus: null,
  // AI/BI dashboard (post-deploy): two-question flow + preview/deploy state.
  dbxAgentWantDashboard: null,          // null (unasked) | true | false
  dbxAgentDashboardSampleData: null,    // null | 'with' | 'without'
  dbxAgentSampleDataResult: null,
  dbxAgentDashboardPreview: null,
  dbxAgentDeployDashboardResult: null,
  isTestingDbxAgentConnection: false,
  isConnectingDbxAgentOAuth: false,
  isDiscoveringDbxAgentWarehouses: false,
  isDiscoveringDbxAgentCatalogs: false,
  isDiscoveringDbxAgentSchemas: false,
  isGeneratingDbxAgentDdl: false,
  isCreatingDbxAgentTables: false,
  isDeployingDbxAgentNotebook: false,
  isRunningDbxAgentNotebook: false,
  isCheckingDbxAgentRunStatus: false,
  isSeedingDbxAgentSampleData: false,
  isPreviewingDbxAgentDashboard: false,
  isDeployingDbxAgentDashboard: false,

  // Qlik connector ("Connect to Qlik" upload step)
  qlikConnection: {
    mode: 'cloud', // 'cloud' (Qlik Cloud / SaaS) | 'enterprise' (Qlik Sense Enterprise)
    base_url: '',
    api_key: '',
    user_directory: '',
    user_id: '',
  },
  // dbt Cloud Agent connection config (persisted; mirrors dbxAgentConfig/qlikConnection).
  // baseUrl default '' so the backend's default API URL applies when left blank.
  dbtCloudConfig: {
    baseUrl: '',
    token: '',
    accountId: '',
    projectId: '',
    jobId: '',
  },

  qlikSourceMode: false, // true when the user chose "Connect to Qlik" instead of a manual file upload
  qlikConnected: false,
  qlikIdentity: null,
  qlikApps: [],
  qlikSelectedAppId: null,
  qlikShowAppBrowser: true, // true while the main panel should show the connect form / app picker
  qlikConnectionError: null,
  isTestingQlikConnection: false,
  isLoadingQlikApps: false,
  isMigratingQlikApp: false,
  qlikMigratingAppId: null,

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

// Volatile Snowflake/Glue downstream-run results — cleared on re-analyze,
// source switch, and reset so a new selection never inherits a prior run's
// deploy/build/reconcile/test outcome.
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

export const store = {
  get() {
    return state;
  },

  setQuiet(updates) {
    // Update state WITHOUT notifying the global subscriber, so an intermediate
    // step in a multi-step flow (e.g. migration generation) doesn't tear down and
    // rebuild the whole app shell + current page (CodeMirror editors, lineage
    // graph). The flow renders intentionally at its start and end instead.
    Object.assign(state, updates);
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
    // Persist the active tool (uploadMode) so a refresh restores the RIGHT flow —
    // e.g. a Cognos session must rehydrate via getCognosModel, not the Qlik route.
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
    // Persist the Databricks connection config (incl. token) so a page refresh
    // doesn't wipe it and force the user to re-authenticate every time.
    if (updates.dbxAgentConfig !== undefined) {
      try {
        localStorage.setItem('qvf_dbx_agent_config', JSON.stringify(state.dbxAgentConfig || {}));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    // Persist the Qlik connection config (incl. API key) so a page refresh
    // doesn't wipe it and force the user to reconnect every time.
    if (updates.qlikConnection !== undefined) {
      try {
        localStorage.setItem('qvf_qlik_connection', JSON.stringify(state.qlikConnection || {}));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    // Persist the dbt Cloud connection config (incl. service token) so a page
    // refresh doesn't wipe it and force the user to re-enter it every time.
    if (updates.dbtCloudConfig !== undefined) {
      try {
        localStorage.setItem('qvf_dbt_cloud_config', JSON.stringify(state.dbtCloudConfig || {}));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    // NOTE: the Snowflake + Glue source connection configs (Snowflake password, AWS
    // secret/session keys) are intentionally NOT persisted — they live in memory for
    // the session only and are cleared on restart, so credentials never touch storage.
    if (updates.sfGlueDestination !== undefined) {
      try {
        localStorage.setItem('qvf_sfglue_destination', JSON.stringify(state.sfGlueDestination || {}));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    // Persist the Power BI workspace (destination) config so a page refresh keeps it.
    // The Tableau source PAT is intentionally NOT persisted (session-only secret).
    if (updates.tabPbiDestination !== undefined) {
      try {
        localStorage.setItem('qvf_tabpbi_destination', JSON.stringify(state.tabPbiDestination || {}));
      } catch (_) { /* storage unavailable — non-fatal */ }
    }
    this._notify();
  },

  ensureFileReviewState(fileId, fallback = {}) {
    if (!fileId) return null;

    if (!state.reviewStateByFile[fileId]) {
      state.reviewStateByFile[fileId] = {
        editMode: false,
        rightEditMode: false,
        activeRightTab: 'sql',
        editedSql: fallback.editedSql || fallback.script || '',
        editedText: fallback.editedText || fallback.description || '',
        regeneratedSql: fallback.regeneratedSql || '',
        regeneratedText: fallback.regeneratedText || '',
        regeneratedLineage: fallback.regeneratedLineage || '',
        regeneration: fallback.regeneration || null,
        regenerationHistory: fallback.regenerationHistory || [],
        generationPlan: fallback.generationPlan || [],
        generationPlanText: fallback.generationPlanText || '',
        outputTableSchema: fallback.outputTableSchema || null,
        validationResult: fallback.validationResult || null,
        baseline: fallback.baseline || null,
      };
    } else if (fallback && Object.keys(fallback).length > 0) {
      // Merge carefully: don't let an empty/falsy fallback value clobber a
      // previously populated value for fields that represent generated
      // migration output. Without this, re-rendering the review page (e.g.
      // after navigating away to another page and back, or switching
      // between file tabs and back) can wipe out a finished migration's
      // generated code/description because the top-level state mirror was
      // momentarily out of sync.
      const protectedKeys = [
        'editedSql', 'editedText',
        'regeneratedSql', 'regeneratedText', 'regeneration',
        'generationPlan', 'generationPlanText', 'regenerationHistory',
      ];
      const existing = state.reviewStateByFile[fileId];
      const safeUpdate = { ...fallback };
      protectedKeys.forEach((key) => {
        const incoming = safeUpdate[key];
        const incomingEmpty = incoming === undefined || incoming === null || incoming === ''
          || (Array.isArray(incoming) && incoming.length === 0);
        const existingValue = existing[key];
        const existingPopulated = existingValue !== undefined && existingValue !== null && existingValue !== ''
          && !(Array.isArray(existingValue) && existingValue.length === 0);
        if (incomingEmpty && existingPopulated) {
          delete safeUpdate[key];
        }
      });
      Object.assign(existing, safeUpdate);
    }

    return state.reviewStateByFile[fileId];
  },

  getFileReviewState(fileId) {
    if (!fileId) return null;
    return state.reviewStateByFile[fileId] || null;
  },

  setFileReviewState(fileId, updates) {
    if (!fileId) return null;
    const current = this.ensureFileReviewState(fileId);
    Object.assign(current, updates);
    this._syncCurrentFileMirror(fileId);
    return current;
  },

  setFileReviewBaseline(fileId, baseline) {
    if (!fileId) return null;
    const current = this.ensureFileReviewState(fileId);
    current.baseline = { ...baseline };
    this._syncCurrentFileMirror(fileId);
    return current;
  },

  isFileReviewDirty(fileId, snapshot) {
    const current = state.reviewStateByFile[fileId];
    if (!current || !current.baseline) return true;

    const live = snapshot || {
      sourceSql: current.editedSql || '',
      regenSql: current.regeneratedSql || '',
      regenText: current.regeneratedText || '',
    };

    return (
      live.sourceSql !== (current.baseline.sourceSql || '') ||
      live.regenSql !== (current.baseline.regenSql || '') ||
      live.regenText !== (current.baseline.regenText || '')
    );
  },

  setCurrentFile(fileId, fallback = {}) {
    const current = this.ensureFileReviewState(fileId, fallback);
    state.currentFileId = fileId;
    state.fileId = fileId;
    if (current) this._syncCurrentFileMirror(fileId);
    return current;
  },

  _syncCurrentFileMirror(fileId) {
    const current = state.reviewStateByFile[fileId];
    if (!current) return;
    state.editMode = !!current.editMode;
    state.rightEditMode = !!current.rightEditMode;
    state.activeRightTab = current.activeRightTab || 'sql';
    state.editedSql = current.editedSql || '';
    state.editedText = current.editedText || '';
    state.regeneratedSql = current.regeneratedSql || '';
    state.regeneratedText = current.regeneratedText || '';
    state.regeneratedLineage = current.regeneratedLineage || '';
    state.regeneration = current.regeneration || null;
    state.regenerationHistory = current.regenerationHistory || [];
    state.generationPlan = current.generationPlan || [];
    state.generationPlanText = current.generationPlanText || '';
    state.outputTableSchema = current.outputTableSchema || null;
    state.validationResult = current.validationResult || null;
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

  // Merge a partial into a Tableau→Power BI config in memory WITHOUT re-rendering
  // (mirrors patchSfGlueConfig). which='powerbi' patches the destination workspace;
  // otherwise the Tableau source PAT (session-only, never persisted). invalidateLineage
  // drops stale lineage/selection/conversion so a source change forces a fresh parse.
  patchTabPbiConfig(which, partial, { invalidateLineage = false } = {}) {
    const key = which === 'powerbi' ? 'tabPbiDestination' : 'tabPbiTableauConfig';
    state[key] = { ...state[key], ...partial };
    if (invalidateLineage) {
      state.tabPbiLineage = null;
      state.tabPbiReview = null;
      state.tabPbiSelected = [];
      state.tabPbiPrecheck = null;
      state.tabPbiConversion = null;
      state.tabPbiArtifactEdits = {};
      state.tabPbiArtifactExplain = {};
    }
  },

  // Clear loaded data for BOTH source flows (Qlik/QVD and Snowflake/Glue) when the
  // user switches migration tool/source, so no stale reference (e.g. a QVF
  // filename) leaks into the newly-selected flow. Saved connection configs and
  // destination credentials are intentionally preserved.
  clearWorkspaceForSourceSwitch() {
    Object.assign(state, {
      sessionId: null, currentFileId: null, fileId: null, filename: null,
      graph: { nodes: [], edges: [] }, tables: [], associations: [], metadata: null,
      script: '', description: '', sqlSections: [],
      regeneratedSql: '', regeneratedText: '', regeneratedLineage: '', regeneration: null,
      regenerationHistory: [], reviewStateByFile: {}, sessionStats: null,
      qvdInspection: null, qvdSchemaSuggestion: null, qvdBusinessAnalysis: null,
      qvdKpiCatalog: null, qvdLineageReconciliation: null, qvdAiExplanation: null,
      qvdDdlGeneration: null, qvdSelectedFiles: [],
      sfGlueSnowflakeConnection: null, sfGlueGlueConnection: null, sfGlueLineage: null,
      sfGlueReview: null, sfGlueSelectedTables: [], sfGluePrecheck: null, sfGlueConversion: null,
      sfGlueArtifactEdits: {}, sfGlueArtifactExplain: {},
      sfGluePostgresConnection: null, isTestingPostgres: false,
      ...sfGlueResultDefaults(),
      tabPbiTableauConnection: null, tabPbiPowerBIConnection: null, tabPbiContent: null,
      tabPbiSiteAnalysis: null, tabPbiAnalyzeError: null, tabPbiTableauWorkbookId: null, isAnalyzingTableau: false,
      tabPbiMetadata: null, tabPbiLineage: null, tabPbiSelected: [], tabPbiReview: null,
      tabPbiPrecheck: null, tabPbiConversion: null, tabPbiArtifactEdits: {}, tabPbiArtifactExplain: {},
      cognosModel: null, cognosCalcs: [], cognosDaxByCalc: {}, cognosSelectedCalcId: null,
      cognosValidation: null, cognosDataSources: null, cognosArtifactEdits: {},
      cognosModelLoadError: null, cognosAgentLog: [],
    });
    localStorage.removeItem('qvf_session_id');  // don't restore the old session on reload
    this._notify();
  },

  reset() {
    Object.assign(state, {
      sessionId: null,
      currentFileId: null,
      fileId: null,
      filename: null,
      graph: { nodes: [], edges: [] },
      tables: [],
      associations: [],
      metadata: null,
      script: '',
      sqlSections: [],
      description: '',
      generationPlan: [],
      generationPlanText: '',
      editedSql: '',
      editedText: '',
      editMode: false,
      rightEditMode: false,
      activeRightTab: 'sql',
      regeneratedSql: '',
      regeneratedText: '',
      regeneratedLineage: '',
      regeneration: null,
      regenerationHistory: [],
      outputTableSchema: null,
      isGeneratingOutputTableSchema: false,
      validationResult: null,
      isValidatingMigration: false,
      validationMode: 'quick',
      validationProgressMessage: '',
      reviewStateByFile: {},
      // Snowflake/Glue flow — connections, lineage, artifacts, and run results.
      sfGlueSnowflakeConnection: null, sfGlueGlueConnection: null, sfGlueSnowflakeSchemas: [],
      sfGlueLineage: null, sfGlueReview: null, sfGlueSelectedTables: [],
      sfGluePrecheck: null, sfGlueConversion: null, sfGlueArtifactEdits: {},
      sfGlueArtifactExplain: {}, sfGlueColumnEdits: {},
      sfGluePostgresConnection: null, isTestingPostgres: false,
      ...sfGlueResultDefaults(),
      isUploading: false,
      isProcessing: false,
      uploadingFilename: null,
      isGenerating: false,
      dialect: 'databricks',
      migrationSource: 'qlik',
      migrationDestination: 'databricks',
      uploadMode: 'qvf',
      lastDeployment: null,
      reconciliation: null,
      reviewChatPrefill: null,
      qvdInspection: null,
      qvdSchemaSuggestion: null,
      qvdBusinessAnalysis: null,
      qvdKpiCatalog: null,
      qvdLineageReconciliation: null,
      qvdAiExplanation: null,
      qvdEditableMapping: [],
      qvdApprovedMapping: null,
      qvdDdlGeneration: null,
      qvdRowPreviews: {},
      qvdColumnProfiles: {},
      qvdParquetConversions: {},
      qvdParquetValidations: {},
      qvdDatabricksLoadScripts: {},
      qvdMigrationPackages: {},
      qvdDatabricksConfig: {
        workspace_url: '',
        personal_access_token: '',
        sql_warehouse_id: '',
        catalog: 'main',
        schema: 'qvd_raw',
        volume: '',
        volume_path: '',
        cloud_storage_path: '',
      },
      qvdDatabricksWarehouses: [],
      qvdDatabricksCatalogs: [],
      qvdDatabricksSchemas: [],
      qvdDatabricksVolumes: [],
      qvdDatabricksUpload: null,
      qvdDatabricksConnection: null,
      qvdDatabricksPrecheck: null,
      qvdDatabricksExecution: null,
      qvdExecutionMode: 'generate_sql_only',
      qvdMappingValidationErrors: [],
      qvdSelectedFiles: [],
      isSuggestingQvdSchema: false,
      isDiscoveringQvdBusinessEntities: false,
      isGeneratingQvdKpiCatalog: false,
      isGeneratingQvdLineageReconciliation: false,
      isGeneratingQvdAiExplanation: false,
      isGeneratingQvdDdl: false,
      isSavingQvdMapping: false,
      qvdPreviewLoadingByFile: {},
      qvdProfileLoadingByFile: {},
      qvdParquetLoadingByFile: {},
      qvdParquetValidationLoadingByFile: {},
      qvdDatabricksLoadLoadingByFile: {},
      qvdMigrationPackageLoadingByFile: {},
      isSavingDatabricksConfig: false,
      isTestingDatabricksConnection: false,
      isDiscoveringDatabricksWarehouses: false,
      isDiscoveringDatabricksCatalogs: false,
      isDiscoveringDatabricksSchemas: false,
      isDiscoveringDatabricksVolumes: false,
      isPreparingDatabricksTarget: false,
      isUploadingDatabricksParquet: false,
      isRunningDatabricksPrecheck: false,
      isExecutingDatabricksMigration: false,
      dbxAgentConfig: {
        workspace_url: '',
        personal_access_token: '',
        oauth_refresh_token: '',
        oauth_expires_at: 0,
        sql_warehouse_id: '',
        cluster_id: '',
        catalog: 'main',
        schema: 'default',
      },
      dbxAgentConnection: null,
      dbxAgentWarehouses: [],
      dbxAgentCatalogs: [],
      dbxAgentSchemas: [],
      dbxAgentSourceTables: null,
      dbxAgentCreateResult: null,
      dbxAgentNotebookPath: '',
      dbxAgentDeployResult: null,
      dbxAgentRunResult: null,
      dbxAgentRunStatus: null,
      dbxAgentWantDashboard: null,
      dbxAgentDashboardSampleData: null,
      dbxAgentSampleDataResult: null,
      dbxAgentDashboardPreview: null,
      dbxAgentDeployDashboardResult: null,
      isTestingDbxAgentConnection: false,
  isConnectingDbxAgentOAuth: false,
      isDiscoveringDbxAgentWarehouses: false,
      isDiscoveringDbxAgentCatalogs: false,
      isDiscoveringDbxAgentSchemas: false,
      isGeneratingDbxAgentDdl: false,
      isCreatingDbxAgentTables: false,
      isDeployingDbxAgentNotebook: false,
      isRunningDbxAgentNotebook: false,
      isCheckingDbxAgentRunStatus: false,
      isSeedingDbxAgentSampleData: false,
      isPreviewingDbxAgentDashboard: false,
      isDeployingDbxAgentDashboard: false,
      dbtCloudConfig: {
        baseUrl: '',
        token: '',
        accountId: '',
        projectId: '',
        jobId: '',
      },
      qlikSourceMode: false,
      // Reset the in-memory credentials as well — reset() already removes the
      // localStorage copy below, so keeping the key in memory was inconsistent.
      qlikConnection: {
        mode: 'cloud',
        base_url: '',
        api_key: '',
        user_directory: '',
        user_id: '',
      },
      qlikConnected: false,
      qlikIdentity: null,
      qlikApps: [],
      qlikSelectedAppId: null,
      qlikShowAppBrowser: true,
      qlikConnectionError: null,
      isTestingQlikConnection: false,
      isLoadingQlikApps: false,
      isMigratingQlikApp: false,
      qlikMigratingAppId: null,
      // Tableau → Power BI
      tabPbiTableauConfig: {
        server_url: '', site: '', token_name: '', token_value: '', api_version: '3.19',
      },
      tabPbiTableauConnection: null,
      tabPbiContent: null,
      tabPbiSiteAnalysis: null,
      tabPbiAnalyzeError: null,
      tabPbiTableauWorkbookId: null,
      isAnalyzingTableau: false,
      tabPbiMetadata: null,
      tabPbiLineage: null,
      tabPbiSelected: [],
      tabPbiDestination: {
        workspace_id: '', dataset_name: '', tenant_id: '', client_id: '', client_secret: '', access_token: '',
      },
      tabPbiPowerBIConnection: null,
      tabPbiReview: null,
      tabPbiPrecheck: null,
      tabPbiConversion: null,
      tabPbiArtifactEdits: {},
      tabPbiArtifactExplain: {},
      isTestingTableau: false,
      isParsingTableau: false,
      isTestingPowerBI: false,
      isBuildingTabPbiLineage: false,
      isReviewingTabPbi: false,
      isPrecheckingTabPbi: false,
      isConvertingTabPbi: false,
      // Cognos/Qlik → Power BI (PBIP)
      cognosModel: null,
      cognosCalcs: [],
      cognosDaxByCalc: {},
      cognosSelectedCalcId: null,
      cognosValidation: null,
      cognosDataSources: null,
      cognosArtifactEdits: {},
      cognosModelLoadError: null,
      cognosAgentLog: [],
      isUploadingCognos: false,
      isConvertingCognosDax: false,
      isValidatingCognosRel: false,
      isGeneratingCognosPbip: false,
    });
    localStorage.removeItem('qvf_session_id');
    localStorage.removeItem('qvf_upload_mode');
    localStorage.removeItem('qvf_dbx_agent_config');
    localStorage.removeItem('qvf_dbt_cloud_config');
    localStorage.removeItem('qvf_dialect');
    localStorage.removeItem('qvf_qlik_connection');
    localStorage.removeItem('qvf_sfglue_destination');
    localStorage.removeItem('qvf_tabpbi_destination');
    this._notify();
  },
};

// Initialize from URL hash. Accept the legacy pages, the 'report' page, and any
// registry tool page (cognos-*/sfglue-* etc.); main.js's validNavPage() is the
// authoritative gate, so keep this permissive rather than a drifting hard-coded list.
const hash = window.location.hash.slice(1);
const _LEGACY_PAGES = ['upload', 'business', 'inspect', 'review', 'output', 'deploy', 'agent', 'databricks-agent', 'report'];
if (_LEGACY_PAGES.includes(hash) || /^(cognos|sfglue|tabpbi)-[a-z-]+$/.test(hash)) {
  state.currentPage = hash;
}

// Restore sessionId + active tool from localStorage so a page refresh reconnects to
// the active session AND the correct flow. main.js rehydrates full state via the
// tool-appropriate endpoint (getCognosModel for Cognos, getModel for Qlik/QVF).
const _storedSessionId = localStorage.getItem('qvf_session_id');
if (_storedSessionId) {
  state.sessionId = _storedSessionId;
}
const _storedUploadMode = localStorage.getItem('qvf_upload_mode');
if (['qvf', 'qvd', 'snowflake_glue', 'cognos_powerbi', 'tableau_powerbi'].includes(_storedUploadMode)) {
  state.uploadMode = _storedUploadMode;
}

// Restore the selected dialect (dbt vs PySpark) so a page refresh keeps the
// user on the flow they were using instead of resetting to the dbt default.
const _storedDialect = localStorage.getItem('qvf_dialect');
if (['pyspark', 'databricks', 'snowflake'].includes(_storedDialect)) {
  state.dialect = _storedDialect;
}

// Restore the persisted Databricks connection config so the token/workspace URL
// survive a page refresh.
try {
  const _storedDbxConfig = localStorage.getItem('qvf_dbx_agent_config');
  if (_storedDbxConfig) {
    state.dbxAgentConfig = { ...state.dbxAgentConfig, ...JSON.parse(_storedDbxConfig) };
  }
} catch (_) { /* ignore malformed/unavailable storage */ }

// Restore the persisted Qlik connection config so the API key/base URL
// survive a page refresh.
try {
  const _storedQlikConfig = localStorage.getItem('qvf_qlik_connection');
  if (_storedQlikConfig) {
    state.qlikConnection = { ...state.qlikConnection, ...JSON.parse(_storedQlikConfig) };
  }
} catch (_) { /* ignore malformed/unavailable storage */ }

// Restore the persisted dbt Cloud connection config so the service token,
// account/project/job IDs, and API URL survive a page refresh.
try {
  const _storedDbtCloudConfig = localStorage.getItem('qvf_dbt_cloud_config');
  if (_storedDbtCloudConfig) {
    state.dbtCloudConfig = { ...state.dbtCloudConfig, ...JSON.parse(_storedDbtCloudConfig) };
  }
} catch (_) { /* ignore malformed/unavailable storage */ }

// Source connection configs (Snowflake/Glue) are NOT restored — they're session-only.
// Purge any previously-stored credentials from older builds so secrets don't linger
// in localStorage across a restart.
try {
  localStorage.removeItem('qvf_sfglue_snowflake_config');
  localStorage.removeItem('qvf_sfglue_glue_config');
  const _storedDest = localStorage.getItem('qvf_sfglue_destination');
  if (_storedDest) {
    state.sfGlueDestination = { ...state.sfGlueDestination, ...JSON.parse(_storedDest) };
  }
} catch (_) { /* ignore malformed/unavailable storage */ }

// Restore the persisted Power BI workspace (destination) config for the Tableau flow.
// The Tableau source PAT is session-only and never restored.
try {
  const _storedTabPbiDest = localStorage.getItem('qvf_tabpbi_destination');
  if (_storedTabPbiDest) {
    state.tabPbiDestination = { ...state.tabPbiDestination, ...JSON.parse(_storedTabPbiDest) };
  }
} catch (_) { /* ignore malformed/unavailable storage */ }

