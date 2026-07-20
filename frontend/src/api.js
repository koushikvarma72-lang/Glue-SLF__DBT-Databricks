/**
 * Migration Studio — API Client
 * Snowflake + AWS Glue → Databricks / dbt migration.
 *
 * Thin wrapper over the Flask backend. Almost every route lives under
 * /api/sfglue/* (introspection, lineage, review, convert, deploy, build,
 * reconcile, workflow/Airflow orchestration, workspace push), plus the AWS
 * SSO device-flow login (/api/aws/sso/*) and the local dbt-Core run endpoints
 * (/api/dbt-local/*) used by the Snowflake/Glue flow.
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
  // ─── Connections (Connect page) ────────────────────────────────────────────

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

  // ── AWS SSO device-flow login ("Sign in with AWS") ─────────────────────────
  async awsSsoStart({ startUrl, region } = {}) {
    const res = await fetch(`${API_BASE}/aws/sso/start`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start_url: startUrl, region }),
    });
    const p = await res.json();
    if (!res.ok) throw new Error(p.error || 'SSO start failed');
    return p;
  },
  async awsSsoPoll(sessionId) {
    const res = await fetch(`${API_BASE}/aws/sso/poll`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId }),
    });
    const p = await res.json();
    if (!res.ok) throw new Error(p.error || 'SSO poll failed');
    return p;
  },
  async awsSsoAccounts(sessionId) {
    const res = await fetch(`${API_BASE}/aws/sso/accounts`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId }),
    });
    const p = await res.json();
    if (!res.ok) throw new Error(p.error || 'SSO accounts failed');
    return p;
  },
  async awsSsoCredentials({ sessionId, accountId, roleName } = {}) {
    const res = await fetch(`${API_BASE}/aws/sso/credentials`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: sessionId, account_id: accountId, role_name: roleName }),
    });
    const p = await res.json();
    if (!res.ok) throw new Error(p.error || 'SSO credentials failed');
    return p;
  },

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

  // ─── Lineage / review ──────────────────────────────────────────────────────

  async buildSnowflakeGlueLineage({ snowflake, glue, glueDatabases } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/lineage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ snowflake, glue, glue_databases: glueDatabases }),
    });
    return parseApiResponse(res, 'Failed to build lineage');
  },

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

  // ─── Migration: precheck → convert → export/deploy → build → reconcile ──────

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

  // ─── Orchestration: Glue Workflows / Airflow → Databricks Jobs ──────────────

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

  // Emit a TARGET Airflow DAG (dag-factory YAML) that orchestrates the migrated
  // Databricks + dbt pipeline — the mirror of planSfGlueAirflow.
  async emitTargetAirflow({ artifacts, destination, dagId, schedule, fileArrivalPath,
                            dbtSource, gitUrl, dbtCloudJobId, providerFree } = {}) {
    const res = await fetch(`${API_BASE}/sfglue/airflow/emit`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      // provider_free=true emits a BashOperator DAG (via run_databricks_task.py) that
      // runs on an Airflow WITHOUT the databricks provider (the reference 2.10.5 setup).
      body: JSON.stringify({ artifacts, destination, dag_id: dagId, schedule,
                             file_arrival_path: fileArrivalPath, dbt_source: dbtSource,
                             git_url: gitUrl, dbt_cloud_job_id: dbtCloudJobId,
                             provider_free: !!providerFree }),
    });
    const payload = await res.json();
    if (!res.ok) throw new Error(payload.error || 'Airflow DAG emit failed');
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

  // ─── Local dbt-Core run (Snowflake/Glue converted models) ───────────────────

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
};
