/**
 * QVF Decoder â€” API Client
 * Handles all communication with the Flask backend
 */

const API_BASE = '/api';

function friendlyMessage(payload, fallbackMessage, statusMessage) {
  const raw = String(payload.error || (payload.errors || [])[0] || '').trim();
  if (raw.includes('<!doctype html>') || raw.includes('Method Not Allowed')) {
    return `${fallbackMessage}: this action is not available from the current backend route. Restart the backend and try again.`;
  }
  if (/approved mapping artifact not found/i.test(raw)) {
    return 'Save the approved mapping before running this step.';
  }
  if (/KPI catalog artifact not found/i.test(raw)) {
    return 'Generate the KPI Catalog & Documentation before running this step.';
  }
  if (/Parquet validation report not found/i.test(raw)) {
    return 'Validate the Parquet output before running Databricks load or deployment steps.';
  }
  if (/Parquet path is local/i.test(raw)) {
    return 'The Parquet output is on your local machine. Configure a Databricks Volume Path or cloud storage path before loading data.';
  }
  if (/SQL Warehouse ID is required/i.test(raw)) {
    return 'Enter a SQL Warehouse ID before testing or executing Databricks deployment.';
  }
  if (/valid Databricks Workspace URL|Workspace URL is required/i.test(raw)) {
    return 'Enter a valid Databricks Workspace URL, for example https://dbc-xxxx.cloud.databricks.com.';
  }
  return raw || `${fallbackMessage}: ${statusMessage}`;
}

async function parseApiResponse(res, fallbackMessage) {
  const text = await res.text();
  let payload = {};

  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { error: text };
    }
  }

  if (!res.ok) {
    const statusMessage = `${res.status}${res.statusText ? ` ${res.statusText}` : ''}`;
    const error = new Error(friendlyMessage(payload, fallbackMessage, statusMessage));
    Object.assign(error, payload);
    throw error;
  }

  return payload;
}

export const api = {
  /**
   * Upload a QVF file
   * @param {File} file - The QVF file to upload
   * @param {string} sessionId - Optional session ID
   * @returns {Promise<Object>} Upload result with graph data
   */
  async uploadFile(file, sessionId = null) {
    const formData = new FormData();
    formData.append('file', file);
    if (sessionId) {
      formData.append('session_id', sessionId);
    }

    const res = await fetch(`${API_BASE}/upload`, {
      method: 'POST',
      body: formData,
    });

    return parseApiResponse(res, 'Upload failed');
  },

  async uploadInspectQvd(files, sessionId = null) {
    const formData = new FormData();
    Array.from(files || []).forEach(file => {
      formData.append('files', file);
    });
    if (sessionId) {
      formData.append('session_id', sessionId);
    }

    const res = await fetch(`${API_BASE}/qvd/upload-inspect`, {
      method: 'POST',
      body: formData,
    });

    return parseApiResponse(res, 'QVD inspection failed');
  },

  async suggestQvdSchema(sessionId) {
    const res = await fetch(`${API_BASE}/qvd/suggest-schema/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
    });

    return parseApiResponse(res, 'QVD schema suggestion failed');
  },

  async discoverQvdBusinessEntities(sessionId) {
    const res = await fetch(`${API_BASE}/qvd/business-analysis/entities/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    return parseApiResponse(res, 'QVD business entity discovery failed');
  },

  async generateQvdKpiCatalog(sessionId) {
    const res = await fetch(`${API_BASE}/qvd/business-analysis/kpis/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    return parseApiResponse(res, 'QVD KPI catalog generation failed');
  },

  async generateQvdLineageReconciliation(sessionId) {
    const res = await fetch(`${API_BASE}/qvd/business-analysis/lineage-reconciliation/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    return parseApiResponse(res, 'QVD lineage and reconciliation generation failed');
  },

  async generateQvdAiExplanation(sessionId) {
    const res = await fetch(`${API_BASE}/qvd/business-analysis/ai-explain/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    return parseApiResponse(res, 'QVD AI business explanation failed');
  },

  async saveApprovedQvdMapping(sessionId, mappingRows) {
    const res = await fetch(`${API_BASE}/qvd/save-approved-mapping/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mapping_rows: mappingRows }),
    });

    return parseApiResponse(res, 'Approved QVD mapping save failed');
  },

  async generateQvdDdl(sessionId) {
    const res = await fetch(`${API_BASE}/qvd/generate-ddl/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    });

    return parseApiResponse(res, 'QVD DDL generation failed');
  },

  async previewQvdRows(sessionId, fileName, limit = 100) {
    const res = await fetch(`${API_BASE}/qvd/preview-rows/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file_name: fileName, limit }),
    });

    return parseApiResponse(res, 'QVD row preview failed');
  },

  async profileQvdColumns(sessionId, fileName, limit = 10000) {
    const res = await fetch(`${API_BASE}/qvd/profile-columns/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file_name: fileName, limit }),
    });

    return parseApiResponse(res, 'QVD column profiling failed');
  },

  async convertQvdToParquet(sessionId, fileName, batchId = null) {
    const body = { file_name: fileName };
    if (batchId) body.batch_id = batchId;
    const res = await fetch(`${API_BASE}/qvd/convert-parquet/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    return parseApiResponse(res, 'QVD to Parquet conversion failed');
  },

  async validateQvdParquet(sessionId, targetTable) {
    const res = await fetch(`${API_BASE}/qvd/validate-parquet/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target_table: targetTable }),
    });

    return parseApiResponse(res, 'QVD Parquet validation failed');
  },

  async generateQvdDatabricksLoad(sessionId, targetTable, parquetPath = null) {
    const body = { target_table: targetTable };
    if (parquetPath) body.parquet_path = parquetPath;
    const res = await fetch(`${API_BASE}/qvd/generate-databricks-load/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    return parseApiResponse(res, 'QVD Databricks load script generation failed');
  },

  async generateQvdMigrationPackage(sessionId, targetTable, fileName = null) {
    const body = { target_table: targetTable };
    if (fileName) body.file_name = fileName;
    const res = await fetch(`${API_BASE}/qvd/generate-migration-package/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    return parseApiResponse(res, 'QVD migration package generation failed');
  },

  async saveQvdDatabricksConfig(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/save-config/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });

    return parseApiResponse(res, 'Databricks configuration save failed');
  },

  async testQvdDatabricksConnection(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/test-connection/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });

    return parseApiResponse(res, 'Databricks connection test failed');
  },

  async discoverQvdDatabricksWarehouses(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/warehouses/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks warehouse discovery failed');
  },

  async discoverQvdDatabricksCatalogs(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/catalogs/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks catalog discovery failed');
  },

  async discoverQvdDatabricksSchemas(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/schemas/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks schema discovery failed');
  },

  async discoverQvdDatabricksVolumes(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/volumes/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks volume discovery failed');
  },

  async createQvdDatabricksSchema(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/create-schema/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks schema creation failed');
  },

  async createQvdDatabricksVolume(sessionId, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/create-volume/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks volume creation failed');
  },

  async uploadQvdParquetToDatabricksVolume(sessionId, targetTable, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/upload-parquet/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), target_table: targetTable }),
    });
    return parseApiResponse(res, 'Databricks volume upload failed');
  },

  async executeQvdDatabricksMigration(sessionId, targetTable, executionMode, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/execute/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ...(config || {}),
        target_table: targetTable,
        execution_mode: executionMode,
      }),
    });

    return parseApiResponse(res, 'Databricks migration execution failed');
  },

  async precheckQvdDatabricksDeployment(sessionId, targetTable, executionMode, config) {
    const res = await fetch(`${API_BASE}/qvd/databricks/precheck/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        ...(config || {}),
        target_table: targetTable,
        execution_mode: executionMode,
      }),
    });

    return parseApiResponse(res, 'Databricks deployment precheck failed');
  },

  /**
   * Get full data model for a session
   * @param {string} sessionId
   * @returns {Promise<Object>} Full model data
   */
  async getModel(sessionId) {
    const res = await fetch(`${API_BASE}/model/${sessionId}`);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Failed to load model');
    }
    return res.json();
  },

  /**
   * Regenerate SQL and description from edits
   * @param {string} sessionId
   * @param {string} editedSql
   * @param {string} editedText
   * @returns {Promise<Object>} Regenerated output
   */
  async regenerate(sessionId, editedSql, editedText, triggerMigration = false, regeneratedSql = '', regeneratedText = '', dialect = 'databricks', generationMode = 'auto') {
    const res = await fetch(`${API_BASE}/regenerate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ 
        sessionId, 
        editedSql, 
        editedText,
        triggerMigration,
        regeneratedSql,
        regeneratedText,
        dialect,
        generationMode
      }),
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Regeneration failed');
    }

    return res.json();
  },

  async getRegenerationStatus(jobId) {
    const res = await fetch(`${API_BASE}/regenerate/status/${jobId}`);
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.error || 'Failed to load regeneration status');
    }
    return res.json();
  },

  async explain(sessionId, code) {
    const res = await fetch(`${API_BASE}/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, code }),
    });
    if (!res.ok) throw new Error('Failed to explain code');
    return res.json();
  },

  /** Grade the generated SQL's fidelity to the source script (read-only). */
  async gradeMigration(sessionId, sql, dialect = 'databricks') {
    const res = await fetch(`${API_BASE}/grade-migration`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, sql, dialect }),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(payload.error || 'Grade failed');
    return payload;
  },

  /** Grade Snowflake/Glue conversion fidelity vs the original source (read-only). */
  async gradeSfGlue(payload) {
    const res = await fetch(`${API_BASE}/sfglue/grade`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const out = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(out.error || 'Grade failed');
    return out;
  },

  /**
   * Kick off a background job that validates the generated dbt SQL / PySpark
   * code against the original Qlik script using AI-simulated sample-data
   * execution, with an iterative auto-fix loop.
   * @param {string} mode - 'quick' (1 iteration) or 'pro' (up to 5 iterations)
   */
  async validateMigration(sessionId, code, description, dialect, mode = 'quick') {
    const res = await fetch(`${API_BASE}/validate-migration`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, code, description, dialect, mode }),
    });
    return parseApiResponse(res, 'Failed to start migration validation');
  },

  async getValidationStatus(jobId) {
    const res = await fetch(`${API_BASE}/validate-migration/status/${jobId}`);
    return parseApiResponse(res, 'Failed to load validation status');
  },

  /**
   * Post-deploy reconciliation: diff each deployed Databricks table against
   * what the migration plan expects (schema parity + key integrity +
   * non-emptiness). `destination` is the Databricks connection (dbxAgentConfig);
   * `tables` optionally pins the exact deployed table names + keys.
   */
  async reconcileDeployment(sessionId, destination, tables = null) {
    const res = await fetch(`${API_BASE}/reconcile-deployment/${sessionId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, ...(tables ? { tables } : {}) }),
    });
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(payload.error || 'Reconciliation failed');
    return payload;
  },

  /**
   * Ask the AI to derive the output Databricks table(s) — name, columns, and
   * data types — from the generated dbt SQL or PySpark code.
   */
  async generateOutputTableSchema(code, description, dialect) {
    const res = await fetch(`${API_BASE}/output-table-schema`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ code, description, dialect }),
    });
    return parseApiResponse(res, 'Failed to detect output tables');
  },

  /**
   * Send a natural-language refinement instruction for the current SQL draft.
   * @param {string} sessionId
   * @param {string} message  - e.g. "add a filter for active customers only"
   * @param {string} currentSql
   * @param {string} currentDesc
   * @param {string} dialect
   */
  async chat(sessionId, message, currentSql = '', currentDesc = '', dialect = 'databricks') {
    const res = await fetch(`${API_BASE}/chat`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, message, currentSql, currentDesc, dialect }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Chat refinement failed');
    return payload;
  },

  /**
   * Open an SSE stream for a regeneration job.
   * @param {string} jobId
   * @param {Object} callbacks - { onToken(text), onProgress(msg), onDone(data), onError(err) }
   * @returns {EventSource} — caller can call .close() to abort
   */
  streamJob(jobId, callbacks = {}) {
    const evtSource = new EventSource(`${API_BASE}/stream/${jobId}`);
    evtSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.type === 'token' && callbacks.onToken) callbacks.onToken(data.content);
        else if (data.type === 'progress' && callbacks.onProgress) callbacks.onProgress(data.message);
        else if (data.type === 'done') { evtSource.close(); if (callbacks.onDone) callbacks.onDone(data); }
        else if (data.type === 'error') { evtSource.close(); if (callbacks.onError) callbacks.onError(new Error(data.message)); }
        // heartbeat — ignore
      } catch (e) { /* ignore parse errors */ }
    };
    evtSource.onerror = () => {
      evtSource.close();
      if (callbacks.onError) callbacks.onError(new Error('Stream connection failed'));
    };
    return evtSource;
  },

  /**
   * Streaming chat refinement via fetch + ReadableStream.
   * POST is not supported by EventSource, so we use fetch.
   * @param {string} sessionId
   * @param {string} message
   * @param {string} currentSql
   * @param {string} currentDesc
   * @param {string} dialect
   * @param {Object} callbacks - { onToken(text), onDone(data), onError(err) }
   * @returns {Promise<void>}
   */
  async chatStream(sessionId, message, currentSql = '', currentDesc = '', dialect = 'databricks', callbacks = {}) {
    const res = await fetch(`${API_BASE}/chat/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, message, currentSql, currentDesc, dialect }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Chat stream failed' }));
      throw new Error(err.error || 'Chat stream failed');
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop(); // keep incomplete line in buffer
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const data = JSON.parse(line.slice(6));
          if (data.type === 'token' && callbacks.onToken) callbacks.onToken(data.content);
          else if (data.type === 'done' && callbacks.onDone) callbacks.onDone(data);
          else if (data.type === 'error') {
            if (callbacks.onError) callbacks.onError(new Error(data.message));
            return;
          }
        } catch (e) { /* ignore parse errors */ }
      }
    }
  },

  async getAiSettings() {
    const res = await fetch(`${API_BASE}/settings/ai`);
    return parseApiResponse(res, 'Could not load AI settings');
  },

  async setAiSettings(config) {
    const res = await fetch(`${API_BASE}/settings/ai`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Could not save AI settings');
  },

  async testDbtCloudConnection(config) {
    const res = await fetch(`${API_BASE}/dbt-cloud/test`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to connect to dbt Cloud');
    return payload;
  },

  async runDbtCloudJob(config) {
    const res = await fetch(`${API_BASE}/dbt-cloud/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to trigger dbt Cloud job');
    return payload;
  },

  async getDbtCloudRunStatus(config) {
    const res = await fetch(`${API_BASE}/dbt-cloud/status`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to load dbt Cloud run status');
    return payload;
  },

  async cancelDbtCloudRun(config) {
    const res = await fetch(`${API_BASE}/dbt-cloud/cancel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to cancel dbt Cloud run');
    return payload;
  },

  // ─── Local dbt Core run (against the connected Databricks workspace) ───────

  async runDbtLocal(config) {
    const res = await fetch(`${API_BASE}/dbt-local/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to start the local dbt run');
    return payload;
  },

  // Run the Snowflake/Glue converted models with real dbt-Core locally. Status/cancel
  // reuse getDbtLocalStatus/cancelDbtLocal (shared job registry).
  async runDbtLocalSfGlue({ sessionId, models, sources_yml, destination }) {
    const res = await fetch(`${API_BASE}/dbt-local/run-sfglue`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, models, sources_yml, destination }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to start the local dbt run');
    return payload;
  },

  // ─── Reference CDL environment tools (Connect page) ────────────────────────

  async listAwsBuckets({ glue } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/aws/buckets`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ glue }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Could not list buckets');
    return payload;
  },

  async referenceIni({ glue, bucket, key, section, action, target_bucket } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/reference/ini`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ glue, bucket, key, section, action, target_bucket }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'INI operation failed');
    return payload;
  },

  async seedControlSchema({ postgres } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/reference/seed-control`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postgres }),
    });
    const payload = await res.json();
    if (!res.ok && !payload.results) throw new Error(payload.error || 'Seed failed');
    return payload;
  },

  async configPathReport({ postgres } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/reference/config-paths`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postgres }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Config-path report failed');
    return payload;
  },

  async repointConfigPaths({ postgres, old_bucket, new_bucket } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/reference/repoint-config-paths`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postgres, old_bucket, new_bucket }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Config-path repoint failed');
    return payload;
  },

  // ─── Orchestration + workspace automation (gap-plan Phases 1/8) ────────────

  // Operational lineage: fused Glue-Workflow chain + RDS control rows + catalog graph.
  async buildOperationalLineage({ glue, glueDatabases, postgres, snowflake, jobFlags } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/lineage/operational`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ glue, glue_databases: glueDatabases, postgres, snowflake,
                             job_flags: jobFlags || {} }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Operational lineage failed');
    return payload;
  },

  // Emit a TARGET Airflow DAG (dag-factory YAML) that orchestrates the migrated
  // Databricks + dbt pipeline — the mirror of planSfGlueAirflow.
  async emitTargetAirflow({ artifacts, destination, dagId, schedule, fileArrivalPath,
                            dbtSource, gitUrl, dbtCloudJobId } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/airflow/emit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ artifacts, destination, dag_id: dagId, schedule,
                             file_arrival_path: fileArrivalPath, dbt_source: dbtSource,
                             git_url: gitUrl, dbt_cloud_job_id: dbtCloudJobId }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Airflow DAG emit failed');
    return payload;
  },

  // Convert Glue Workflows into Databricks Jobs (plan only — nothing deployed).
  async planSfGlueWorkflows({ glue, destination, artifactMap } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/workflows/plan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ glue, destination, artifact_map: artifactMap || {} }),
    });
    const payload = await res.json();
    if (!res.ok) { const e = new Error(payload.error || 'Workflow plan failed'); e.status = res.status; throw e; }
    return payload;
  },

  // Validate the Databricks destination (token, warehouse, catalog) — no SQL run.
  async testSfGlueDatabricks(destination) {
    const res = await fetch(`${API_BASE}/sfglue/databricks/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Databricks connection failed');
    return payload;
  },

  // Convert Airflow DAGs into Databricks Jobs (plan only) — the Airflow twin of
  // planSfGlueWorkflows. dagFiles: { 'my_dag.py': '<source>' } (paste from the
  // Airflow UI's Code tab), or baseUrl+credentials for live REST introspection.
  async planSfGlueAirflow({ dagFiles, baseUrl, username, password, token, destination, artifactMap } = {}) {
    const airflow = {};
    if (dagFiles && Object.keys(dagFiles).length) airflow.dag_files = dagFiles;
    if (baseUrl) { airflow.base_url = baseUrl; airflow.username = username || ''; airflow.password = password || ''; airflow.token = token || ''; }
    const res = await fetch(`${API_BASE}/sfglue/airflow/plan`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ airflow, destination, artifact_map: artifactMap || {} }),
    });
    const payload = await res.json();
    if (!res.ok) { const e = new Error(payload.error || 'Airflow plan failed'); e.status = res.status; throw e; }
    return payload;
  },

  // Create/update the planned Jobs in the workspace (idempotent by tag).
  async deploySfGlueWorkflows({ destination, jobs } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/workflows/deploy`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, jobs }),
    });
    const payload = await res.json();
    if (!res.ok && !payload.results) throw new Error(payload.error || 'Workflow deploy failed');
    return payload;
  },

  // Push the converted notebooks + dbt project into the workspace so Jobs can run.
  // confFiles: {name: text} — source YAML/config files, landed at <root>/conf/.
  async pushSfGlueWorkspace({ destination, artifacts, root, confFiles } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/workspace/push`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, artifacts, root, conf_files: confFiles || {} }),
    });
    const payload = await res.json();
    if (!res.ok && !payload.results) throw new Error(payload.error || 'Workspace push failed');
    return payload;
  },

  // Trigger a deployed Job and wait for the verdict (the workflow dry-run gate).
  async runSfGlueWorkflow({ destination, jobId, timeoutSeconds } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/workflows/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, job_id: jobId, timeout_seconds: timeoutSeconds }),
    });
    const payload = await res.json();
    if (!res.ok && !payload.state && !payload.run_id) throw new Error(payload.error || 'Workflow run failed');
    return payload;
  },

  // Push the converted models (+ sources.yml) to a GitHub repo so a dbt Cloud job
  // connected to that repo runs them. Returns {success, pushed[], failed[], repo, branch}.
  async pushModelsToRepo({ github, models, sources_yml }) {
    const res = await fetch(`${API_BASE}/dbt-cloud/push-models`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ github, models, sources_yml }),
    });
    const payload = await res.json();
    if (!res.ok && !payload.pushed) throw new Error(payload.error || 'Failed to push models to the repo');
    return payload;
  },

  async getDbtLocalStatus(jobId, since = 0) {
    const res = await fetch(`${API_BASE}/dbt-local/status/${encodeURIComponent(jobId)}?since=${since}`);
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to load the dbt run status');
    return payload;
  },

  async cancelDbtLocal(jobId) {
    const res = await fetch(`${API_BASE}/dbt-local/cancel/${encodeURIComponent(jobId)}`, { method: 'POST' });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Failed to cancel the local dbt run');
    return payload;
  },

  // ─── Databricks Agent (QVF flow) ──────────────────────────────────────────

  async testDatabricksAgentConnection(config) {
    const res = await fetch(`${API_BASE}/databricks-agent/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks connection test failed');
  },

  async startDatabricksOAuth(workspaceUrl) {
    const res = await fetch(`${API_BASE}/databricks-oauth/start`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ workspace_url: workspaceUrl }),
    });
    return parseApiResponse(res, 'Could not start Databricks sign-in');
  },

  async consumeDatabricksOAuthToken(state) {
    const res = await fetch(`${API_BASE}/databricks-oauth/token`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ state }),
    });
    return parseApiResponse(res, 'Could not retrieve Databricks token');
  },

  async refreshDatabricksOAuth(workspaceUrl, refreshToken) {
    const res = await fetch(`${API_BASE}/databricks-oauth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ workspace_url: workspaceUrl, refresh_token: refreshToken }),
    });
    return parseApiResponse(res, 'Could not refresh Databricks session');
  },

  async discoverDatabricksAgentWarehouses(config) {
    const res = await fetch(`${API_BASE}/databricks-agent/warehouses`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks warehouse discovery failed');
  },

  async discoverDatabricksAgentCatalogs(config) {
    const res = await fetch(`${API_BASE}/databricks-agent/catalogs`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks catalog discovery failed');
  },

  async discoverDatabricksAgentSchemas(config) {
    const res = await fetch(`${API_BASE}/databricks-agent/schemas`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Databricks schema discovery failed');
  },

  async uploadDatabricksAgentSource(sessionId, file, config) {
    const form = new FormData();
    form.append('file', file);
    // Flatten the connection config into form fields (multipart, not JSON).
    Object.entries(config || {}).forEach(([k, v]) => {
      if (v !== undefined && v !== null) form.append(k, v);
    });
    const res = await fetch(`${API_BASE}/databricks-agent/upload-source/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      body: form, // no Content-Type header — the browser sets the multipart boundary
    });
    return parseApiResponse(res, 'CSV upload to Databricks failed');
  },

  async generateDatabricksSourceTableDdl(sessionId, config) {
    const res = await fetch(`${API_BASE}/databricks-agent/source-tables/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Source table DDL generation failed');
  },

  async createDatabricksSourceTables(sessionId, config, statements) {
    const res = await fetch(`${API_BASE}/databricks-agent/create-tables/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), statements: statements || [] }),
    });
    return parseApiResponse(res, 'Source table creation failed');
  },

  async deployPysparkNotebook(sessionId, config, code, notebookPath) {
    const res = await fetch(`${API_BASE}/databricks-agent/deploy-notebook/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), code, notebook_path: notebookPath }),
    });
    return parseApiResponse(res, 'Notebook deployment failed');
  },

  async runPysparkNotebook(sessionId, config, notebookPath, clusterId) {
    const res = await fetch(`${API_BASE}/databricks-agent/run-notebook/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), notebook_path: notebookPath, cluster_id: clusterId }),
    });
    return parseApiResponse(res, 'Notebook run failed');
  },

  async getPysparkRunStatus(sessionId, config, runId) {
    const res = await fetch(`${API_BASE}/databricks-agent/run-status/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), run_id: runId }),
    });
    return parseApiResponse(res, 'Failed to fetch run status');
  },

  // ─── AI/BI Dashboards (rebuild Qlik charts as a Lakeview dashboard) ───────

  async previewDatabricksDashboard(sessionId, config) {
    const res = await fetch(`${API_BASE}/databricks-agent/dashboard-preview/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Failed to detect charts');
  },

  async deployDatabricksDashboard(sessionId, config, selectedChartIds) {
    const body = { ...(config || {}) };
    if (Array.isArray(selectedChartIds)) body.selected_chart_ids = selectedChartIds;
    const res = await fetch(`${API_BASE}/databricks-agent/deploy-dashboard/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    return parseApiResponse(res, 'Dashboard deployment failed');
  },

  async seedDashboardSampleData(sessionId, config, tables) {
    const res = await fetch(`${API_BASE}/databricks-agent/seed-sample-data/${encodeURIComponent(sessionId)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), tables: tables || [] }),
    });
    return parseApiResponse(res, 'Sample data insertion failed');
  },

  // ─── Qlik Connector (Connect to Qlik upload step) ─────────────────────────

  async testQlikConnection(config) {
    const res = await fetch(`${API_BASE}/qlik/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config || {}),
    });
    return parseApiResponse(res, 'Qlik connection test failed');
  },

  async listQlikApps(config, sessionId) {
    const res = await fetch(`${API_BASE}/qlik/apps`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), session_id: sessionId }),
    });
    return parseApiResponse(res, 'Failed to load Qlik apps');
  },

  async migrateQlikApp(config, sessionId, appId, appName) {
    const res = await fetch(`${API_BASE}/qlik/apps/${encodeURIComponent(appId)}/migrate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ...(config || {}), session_id: sessionId, name: appName }),
    });
    return parseApiResponse(res, 'Qlik app migration failed');
  },

  // ─── Snowflake/Glue → Databricks/DBT flow ─────────────────────────────────

  async testSnowflakeConnection(config) {
    const res = await fetch(`${API_BASE}/sfglue/snowflake/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake: config || {} }),
    });
    return parseApiResponse(res, 'Snowflake connection test failed');
  },

  async listSnowflakeSchemas(config) {
    const res = await fetch(`${API_BASE}/sfglue/snowflake/schemas`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake: config || {} }),
    });
    return parseApiResponse(res, 'Failed to load Snowflake schemas');
  },

  async testGlueConnection(config) {
    const res = await fetch(`${API_BASE}/sfglue/glue/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ glue: config || {} }),
    });
    return parseApiResponse(res, 'AWS Glue connection test failed');
  },

  async testPostgresConnection(config) {
    const res = await fetch(`${API_BASE}/sfglue/postgres/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postgres: config || {} }),
    });
    return parseApiResponse(res, 'Postgres connection test failed');
  },

  async introspectPostgres({ postgres, snowflake } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/postgres/introspect`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ postgres: postgres || {}, snowflake: snowflake || null }),
    });
    return parseApiResponse(res, 'Postgres introspection failed');
  },

  async generatePostgresIngestion({ tables, destination, secret_scope } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/postgres/generate-ingestion`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tables, destination, secret_scope }),
    });
    return parseApiResponse(res, 'Postgres ingestion generation failed');
  },

  async introspectSnowflakeGlue({ snowflake, glue, glueDatabases } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/introspect`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake, glue, glue_databases: glueDatabases }),
    });
    return parseApiResponse(res, 'Failed to introspect Snowflake/Glue');
  },

  async buildSnowflakeGlueLineage({ snowflake, glue, glueDatabases } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/lineage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake, glue, glue_databases: glueDatabases }),
    });
    return parseApiResponse(res, 'Failed to build lineage');
  },

  async reviewSnowflakeGlue({ snowflake, glue, glueDatabases } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake, glue, glue_databases: glueDatabases }),
    });
    return parseApiResponse(res, 'Failed to build review');
  },

  async explainSnowflakeGlueArtifact({ name, code, kind, glue } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, code, kind, glue }),
    });
    return parseApiResponse(res, 'Failed to explain');
  },

  async precheckSnowflakeGlueMigration({ lineage, selectedIds, destination } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/precheck`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lineage, selected_ids: selectedIds, destination }),
    });
    return parseApiResponse(res, 'Precheck failed');
  },

  async convertSnowflakeGlueMigration({ snowflake, glue, postgres, lineage, selectedIds, destination, glueScripts, signal } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/convert`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // `postgres` (optional) lets convert auto-generate the Postgres → bronze JDBC landing
      // notebook when a Postgres source is connected — no separate manual step.
      body: JSON.stringify({ snowflake, glue, postgres, lineage, selected_ids: selectedIds, destination, glue_scripts: glueScripts }),
      signal,
    });
    return parseApiResponse(res, 'Conversion failed');
  },

  // Returns a zip Blob — a runnable dbt project assembled from the convert artifacts.
  // Source-agnostic: the server builds the project purely from `artifacts` + `destination`,
  // so it works for any converted Glue+Snowflake flow. Caller creates an object URL to download.
  async exportSnowflakeGlueDbtProject({ artifacts, destination, projectName = 'sfglue_migration' } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/export`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ artifacts, destination, project_name: projectName }),
    });
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      throw new Error(t || 'dbt project export failed');
    }
    return res.blob();
  },

  async deploySnowflakeGlueMigration({ destination, ddl } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/deploy`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, ddl }),
    });
    return parseApiResponse(res, 'Deploy failed');
  },

  // Build (populate) the migrated tables by running the dbt models in dependency order
  // on the SQL Warehouse. `models` is { name: sql } (conv.dbt_models with edits applied);
  // the server resolves {{ ref }}/{{ source }} to real tables at build time.
  async buildSnowflakeGlueMigration({ destination, models, glue } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/build`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, models, glue }),
    });
    return parseApiResponse(res, 'Build failed');
  },

  // Seed a small, referentially-consistent SAMPLE dataset into the bronze schema so Build
  // can run end-to-end without the real S3 ingestion. Schema is derived from the models.
  async seedBronzeSampleData({ destination, models, rows } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/seed-bronze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, models, rows }),
    });
    return parseApiResponse(res, 'Seed bronze failed');
  },

  async reconcileSnowflakeGlueMigration({ snowflake, destination, pairs, floatTol } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/reconcile`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake, destination, pairs, float_tol: floatTol }),
    });
    return parseApiResponse(res, 'Reconciliation failed');
  },

  async runSfGlueTests({ destination, test_specs } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/run-tests`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ destination, test_specs }),
    });
    return parseApiResponse(res, 'Test run failed');
  },

  // ─── Cognos/Qlik → Power BI (PBIP) flow ───────────────────────────────────
  // All routes namespaced under /api/cognos/* (+ /api/export/*). The tool uses its
  // own isolated backend session/DB; sessionId is threaded through bodies/paths.

  async uploadCognos(file, sessionId = null) {
    const fd = new FormData();
    fd.append('file', file);
    if (sessionId) fd.append('session_id', sessionId);
    const res = await fetch(`${API_BASE}/cognos/upload`, { method: 'POST', body: fd });
    return parseApiResponse(res, 'Cognos upload failed');
  },

  async getCognosModel(sessionId) {
    const res = await fetch(`${API_BASE}/cognos/model/${encodeURIComponent(sessionId)}`);
    return parseApiResponse(res, 'Failed to load Cognos model');
  },

  async resetCognos() {
    const res = await fetch(`${API_BASE}/cognos/reset`, { method: 'POST' });
    return parseApiResponse(res, 'Failed to reset Cognos session');
  },

  async explainCognos(sessionId, code) {
    const res = await fetch(`${API_BASE}/cognos/explain`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, code }),
    });
    return parseApiResponse(res, 'Failed to explain code');
  },

  // Blocking single-measure DAX conversion (non-streaming fallback path).
  async convertCognosMeasure(payload) {
    const res = await fetch(`${API_BASE}/cognos/dax-convert`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload || {}),
    });
    return parseApiResponse(res, 'DAX conversion failed');
  },

  async saveCognosDax(sessionId, daxResults) {
    const res = await fetch(`${API_BASE}/cognos/save-dax`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, daxResults }),
    });
    return parseApiResponse(res, 'Failed to save DAX');
  },

  async validateCognosRelationships(sessionId, projectName = 'Cognos_Migration') {
    const res = await fetch(`${API_BASE}/cognos/validate-relationships`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, projectName }),
    });
    return parseApiResponse(res, 'Relationship validation failed');
  },

  // Returns a zip Blob (PBIP project). Caller creates an object URL to download.
  async generateCognosPbip(sessionId, projectName = 'Cognos_Migration') {
    const res = await fetch(`${API_BASE}/cognos/generate-pbip`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sessionId, projectName }),
    });
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      throw new Error(t || 'PBIP generation failed');
    }
    return res.blob();
  },

  // Attach a data source (CSV bundle / Excel / CSV) so the PBIP embeds real rows.
  async uploadCognosDataSource(sessionId, file) {
    const fd = new FormData();
    fd.append('sessionId', sessionId);
    fd.append('file', file);
    const res = await fetch(`${API_BASE}/cognos/upload-data-source`, { method: 'POST', body: fd });
    return parseApiResponse(res, 'Data source upload failed');
  },

  async getCognosTableDataStatus(sessionId) {
    const res = await fetch(`${API_BASE}/cognos/table-data-status?sessionId=${encodeURIComponent(sessionId)}`);
    return parseApiResponse(res, 'Failed to load data-source status');
  },

  // Export routes are namespaced under /api/cognos/export/* (same tool boundary).
  cognosMigrationReportUrl(sessionId) {
    return `${API_BASE}/cognos/export/migration-report?sessionId=${encodeURIComponent(sessionId)}`;
  },

  /**
   * Per-measure DAX conversion stream (pseudo-SSE over fetch + ReadableStream —
   * POST body, so not native EventSource). Events carry
   * { phase: 'sql'|'dax'|'confidence'|'done'|'error', step, status, complete, result }.
   * @param {Object} measure - { cognos_expression|expression, semantic_class, name, table, additive_type, regularAggregate, sessionId }
   * @param {Object} callbacks - { onEvent(ev), onDone(ev), onError(err) }
   */
  async streamCognosMeasure(measure = {}, callbacks = {}) {
    const res = await fetch(`${API_BASE}/cognos/convert-measure/stream`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        expression: measure.cognos_expression ?? measure.expression ?? '',
        semanticClass: measure.semantic_class || 'DERIVED',
        fieldName: measure.name || '',
        tableName: measure.table || '',
        additiveType: measure.additive_type || '',
        regularAggregate: measure.regularAggregate || '',
        sessionId: measure.sessionId || '',
      }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        try {
          const ev = JSON.parse(trimmed.slice(trimmed.indexOf(':') + 1).trim());
          if (ev.phase === 'error') { callbacks.onError?.(new Error(ev.error || 'stream error')); return; }
          if (ev.phase === 'done') callbacks.onDone?.(ev);
          else callbacks.onEvent?.(ev);
        } catch (_) { /* ignore partial/parse errors */ }
      }
    }
  },

  // ─── Tableau → Power BI flow ──────────────────────────────────────────────

  async testTableauConnection(config) {
    const res = await fetch(`${API_BASE}/tabpbi/tableau/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tableau: config || {} }),
    });
    return parseApiResponse(res, 'Tableau connection test failed');
  },

  async listTableauContent(config) {
    const res = await fetch(`${API_BASE}/tabpbi/tableau/content`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tableau: config || {} }),
    });
    return parseApiResponse(res, 'Failed to list Tableau content');
  },

  /**
   * Analyze a whole Tableau site for migration triage: projects, workbooks with
   * usage/last-modified/size/owner, duplicate workbooks and stale content.
   * Returns { success, summary, projects, workbooks, duplicates, stale }.
   */
  async analyzeTableauSite(config) {
    const res = await fetch(`${API_BASE}/tabpbi/tableau/analyze`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tableau: config || {} }),
    });
    return parseApiResponse(res, 'Failed to analyze Tableau site');
  },

  async testPowerBIConnection(config) {
    const res = await fetch(`${API_BASE}/tabpbi/powerbi/test-connection`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ powerbi: config || {} }),
    });
    return parseApiResponse(res, 'Power BI connection test failed');
  },

  /** Download the actual rendered workbook as a PDF from Tableau (base64 in JSON). */
  async fetchTableauWorkbookPdf(config, workbookId) {
    const res = await fetch(`${API_BASE}/tabpbi/tableau/workbook-pdf`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tableau: config || {}, workbook_id: workbookId }),
    });
    return parseApiResponse(res, 'Failed to download Tableau PDF');
  },

  /** Fetch a single view's actual rendered PNG (by sheet name) from Tableau (base64 in JSON). */
  async fetchTableauViewImage(config, workbookId, sheet) {
    const res = await fetch(`${API_BASE}/tabpbi/tableau/view-image`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tableau: config || {}, workbook_id: workbookId, sheet }),
    });
    return parseApiResponse(res, 'Failed to fetch Tableau view image');
  },

  /**
   * Parse a Tableau workbook into `tableau_metadata`.
   * Pass a File/Blob (drag-drop or picker) for the upload path, OR
   * { tableau, workbookId } to download+parse from Tableau Server.
   */
  async parseTableau(fileOrConfig) {
    let res;
    if (fileOrConfig instanceof File || fileOrConfig instanceof Blob) {
      const formData = new FormData();
      formData.append('file', fileOrConfig, fileOrConfig.name || 'workbook.twbx');
      res = await fetch(`${API_BASE}/tabpbi/parse`, { method: 'POST', body: formData });
    } else {
      const { tableau, workbookId } = fileOrConfig || {};
      res = await fetch(`${API_BASE}/tabpbi/parse`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ tableau, workbook_id: workbookId }),
      });
    }
    return parseApiResponse(res, 'Failed to parse Tableau workbook');
  },

  async buildTableauPowerBILineage({ metadata } = {}) {
    const res = await fetch(`${API_BASE}/tabpbi/lineage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ metadata }),
    });
    return parseApiResponse(res, 'Failed to build lineage');
  },

  async reviewTableauPowerBI({ metadata } = {}) {
    const res = await fetch(`${API_BASE}/tabpbi/review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ metadata }),
    });
    return parseApiResponse(res, 'Failed to build review');
  },

  async explainTabPbiArtifact({ name, code, kind } = {}) {
    const res = await fetch(`${API_BASE}/tabpbi/explain`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, code, kind }),
    });
    return parseApiResponse(res, 'Failed to explain');
  },

  async precheckTableauPowerBI({ lineage, selectedIds, destination } = {}) {
    const res = await fetch(`${API_BASE}/tabpbi/precheck`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lineage, selected_ids: selectedIds, destination }),
    });
    return parseApiResponse(res, 'Precheck failed');
  },

  async convertTableauPowerBI({ metadata, lineage, selectedIds, destination } = {}) {
    const res = await fetch(`${API_BASE}/tabpbi/convert`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ metadata, lineage, selected_ids: selectedIds, destination }),
    });
    return parseApiResponse(res, 'Conversion failed');
  },

  /**
   * Clear all data for a fresh start
   */
  async reset(sessionId) {
    const res = await fetch(`${API_BASE}/reset`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // With a sessionId the backend only clears that session; without one it
      // performs a full wipe (legacy single-user behavior).
      body: JSON.stringify(sessionId ? { sessionId } : {}),
    });
    if (!res.ok) throw new Error('Failed to reset session');
    return res.json();
  },
};

