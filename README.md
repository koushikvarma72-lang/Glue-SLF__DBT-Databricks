# sfglue — Snowflake + AWS Glue → Databricks + dbt

Standalone migration app, split out of the combined BI Migration Tool (`qvf_decoder 2`). It hosts
**only** the sfglue flow: connect Snowflake/Glue → check lineage → review → Databricks agent →
dbt agent → report, with AI conversion + a reconciliation gate + a runnable-dbt-project export.

Fully self-contained — no imports from `qvf_decoder 2`.

```
sfglue_app/
  backend/
    app.py            Flask app (port 5060) + SPA static serving
    call_ai.py        multi-provider AI dispatch — Bedrock by default (Anthropic / OpenAI opt-in)
    integrations/     sfglue engine, provider clients, + a tiny databricks-introspect shim
    migration/        dialect_normalizer + duckdb_execution (only what the engine needs)
  qvd_to_databricks/  Databricks connectivity toolkit (databricks_connection/executor, …)
  frontend/           sfglue-only SPA (Vite); proxy → :5060
  server.py, requirements.txt
```

## Run

**Backend** (port 5060):
```bash
cd sfglue_app
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# AI works on Amazon Bedrock by default — just have active AWS creds + Bedrock model access:
aws sso login --profile Cognitive-Tech-495688866359
export AWS_PROFILE=Cognitive-Tech-495688866359
export AWS_REGION=us-west-2
python server.py                        # http://localhost:5060
```

**Frontend** (Vite dev, proxies /api → :5060):
```bash
cd sfglue_app/frontend
npm install
npm run dev                             # http://localhost:5173
# or: npm run build   → backend serves frontend/dist at :5060
```

## Gap-plan Phases 1–7 (see docs/GAP_IMPLEMENTATION_PLAN.md)
- **Phase 1 — orchestration**: `POST /api/sfglue/workflows/plan` converts Glue Workflows
  (graphs + trigger predicates + cron schedules) into Databricks Jobs (JSON + DAB YAML,
  warnings for non-representable predicates); `POST /api/sfglue/workflows/deploy` creates/
  updates them idempotently (matched by `tags.sfglue_source`). Engine:
  `orchestration_migration.py` — deterministic, no AI.
  Airflow as source orchestrator: `POST /api/sfglue/airflow/plan` accepts either pasted DAG
  source files (`airflow.dag_files` — copy from the Airflow UI's Code tab, no server access
  needed) or a live `airflow.base_url` + credentials (REST API). DAGs are AST-parsed into the
  same normalized model (GlueJobOperator `job_name` → `legacy_name`, so the same artifact map
  applies; sensors → file-arrival-trigger warnings; `@daily`-style presets → Quartz) and feed
  the same deploy path. Engine: `airflow_migration.py`.
  dag-factory YAML is accepted too: pasted files are auto-detected (Python vs YAML) and
  `parse_dag_factory_yaml` handles `{dag_id: {tasks: {...}, schedule_interval}}` shapes.
  Pasted YAML configs are additionally carried into the workspace at `<root>/conf/` so the
  config-driven pattern survives the migration.
  Airflow on the TARGET side: `POST /api/sfglue/airflow/emit` (or the "Download Airflow DAG"
  button on the run page) emits a dag-factory YAML that orchestrates the MIGRATED pipeline —
  Databricks notebook tasks + per-layer dbt tasks (`dbt build --select staging|intermediate|marts`)
  — never the retired Glue jobs. The emitted DAG round-trips through the same parser sfglue uses
  to ingest source DAGs, so the tool covers Airflow on both ends. Engine: `emit_target_airflow_yaml`.
- **Operational lineage** (`POST /api/sfglue/lineage/operational`, "🔧 Ops flow" view): fuses
  the Glue Workflow job chain + the RDS control-table rows + the catalog into one laned graph —
  control plane (RDS) on top, the Glue job execution chain, medallion data columns
  (source→bronze→silver→gold→Snowflake), and a source-health panel. Clicking a job shows its
  reads/writes, control tables, extracted config SQL, and review flags. The per-table data edges
  come from the config rows (query_configuration/stitching_configuration), so config-driven
  pipelines connect properly instead of showing floating boxes. Fully generic — nothing about any
  pipeline is hardcoded (roles by `canonical`, columns by candidate lists, edges by evidence).
  Engine: `operational_lineage.build_operational_lineage`.
- **Layered dbt output**: generated models are organized `models/staging/` (`stg_*`),
  `models/intermediate/` (`int_*`), `models/marts/` (`dim_*`/`fct_*`/…). ALL layers
  materialize as tables — staging+intermediate in the silver schema, marts in gold —
  via layer configs in `dbt_project.yml` plus a `generate_schema_name` macro so custom
  schemas apply verbatim (no `silver_gold` concatenation).
- **Phase 2 — control plane**: `POST /api/sfglue/convert` with a `postgres` payload now also
  migrates the metadata framework: control-schema Delta DDL for the detected framework
  tables, **query_configuration SQL rows → dbt models** (AI + scaffold fallback), and
  templated `fw_batch_open/close` + `fw_file_audit` runtime notebooks. Engine:
  `control_plane_migration.py`.
- **Phase 3 — DQ + alerting**: `dq_rules` rows compile to dbt tests (`dq_schema.yml`),
  `<model>__rejects` quarantine models, and bronze notebook checks; `message_template`
  rows become Jobs `email_notifications`. Engine: `dq_migration.py`.
- **Phase 4 — event ingestion**: `workflows/plan` accepts `file_arrival_url` to emit a
  file-arrival trigger instead of a schedule (EVENT triggers are flagged for this).
- **Phase 5 — Snowflake pipeline objects**: introspect with `include: ["pipeline"]`
  returns tasks/streams/pipes/procedures/stages; `consumption_inventory.snowflake_pipeline_notes`
  emits per-object dispositions (tasks → Job tasks, streams → Delta CDF, pipes → Auto Loader).
- **Phase 6 — outbound kit**: `POST /api/sfglue/outbound/kit` builds the cutover checklist
  (+ best-effort consumer inventory from ACCESS_HISTORY).
- **Phase 7 — governance**: `POST /api/sfglue/governance/plan` maps Lake Formation
  permissions to a UC GRANT script — **diff-only**, with an unmapped-principal worksheet.
- Bundle export picks all of it up: `resources/workflow_*.yml`, `dbt/models/dq_schema.yml`,
  `governance/grants.sql`, `OUTBOUND_CUTOVER.md`.
- **Full-flow automation**: the Automated migration page now runs three more phases after
  reconcile — `POST /api/sfglue/workspace/push` (notebooks + dbt project → `/Shared/sfglue`
  via the Workspace API), workflow plan+deploy with an **auto-derived artifact map**
  (batch-control jobs → framework notebooks, converted jobs → their notebooks, transform
  jobs → the dbt model set), and `POST /api/sfglue/workflows/run` (run-now + poll — the
  Phase-1 verification gate). All three are non-fatal: a failure marks the phase and the
  run still completes.
- Tests: `python -m unittest backend.tests.test_phase0 backend.tests.test_gap_phases -v`

## Gap-plan Phase 0 (see docs/GAP_IMPLEMENTATION_PLAN.md)
- **Orchestration introspection**: `POST /api/sfglue/introspect` accepts
  `include: ["workflows","triggers","crawlers"]` and returns the Glue Workflows (with graphs),
  Triggers (schedules + predicates), and Crawlers alongside tables/jobs.
- **Control-framework introspection**: `POST /api/sfglue/postgres/framework` detects the RDS
  control tables (configuration_master, dq_rules, message_template, …) by name or column
  fingerprint and returns their rows (capped, secret columns masked). Force-include missed
  tables via `tables: [...]`.
- **Asset-bundle export**: `POST /api/sfglue/export` with `format: "bundle"` emits a Databricks
  Asset Bundle (databricks.yml with dev/test/prod targets, resources/ job skeleton,
  src/notebooks/, nested dbt/ project, sfglue_state.json manifest). Default remains the plain
  dbt-project zip.
- **Versioned artifact registry**: `POST /api/sfglue/convert` responses now carry
  `schema_version` + `artifact_registry` (typed inventory of every generated artifact).
- Tests: `python -m unittest backend.tests.test_phase0 -v`

## Notes
- **AI defaults to Amazon Bedrock** (same as the BI Migration Tool) — no keys or provider env
  needed. With active AWS credentials (`AWS_PROFILE` + `AWS_REGION`, or any boto3 credential source)
  and Bedrock model access enabled, it works out of the box. Model tiers match the BI app:
  conversion runs on `us.anthropic.claude-sonnet-4-6` (override with `BEDROCK_MODEL` /
  `BEDROCK_MODEL_STANDARD`).
- **To use an API key instead**, set `ANTHROPIC_API_KEY` (+ optional `ANTHROPIC_MODEL`) or
  `OPENAI_API_KEY` (+ `OPENAI_BASE_URL`/`OPENAI_MODEL`) — a key auto-selects that provider. Force a
  specific provider with `SFGLUE_AI_PROVIDER=bedrock|anthropic|openai`.
- Each migration step runs a ~1-token preflight probe; if the provider isn't reachable (e.g. an
  expired SSO token) the route returns `needsAiConfig` with an actionable message
  (`aws sso login …`) instead of emitting placeholder output.
- Databricks/Snowflake/Glue credentials are supplied per-request from the UI (the `destination` /
  connection payloads) — nothing is baked in. Source-agnostic: works for any Glue+Snowflake flow.
