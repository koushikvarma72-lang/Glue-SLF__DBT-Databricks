# Reference CDL pipeline — replica kit

Recreates the **reference commercial-data-lake architecture** (the diagrams this migration
takes as its source-of-truth) in a real AWS account, wired around the demo's existing
assets. This is the "original pipeline" the sfglue migration tool migrates — with it in
place, the demo story is: *reference architecture → introspect → convert → Databricks,
gate-verified end to end.*

## Diagram → replica mapping

| Diagram component | Replica | Script |
|---|---|---|
| SFTP landing | simulated: upload to the landing prefix (`drop_sample_file.py`); AWS Transfer Family optional, see below | — |
| S3 zones: landing / raw / curated / publish (+ reject prefixes) | one bucket, zone prefixes | `setup_s3_zones.py` |
| Glue job chain: load_config → parent_batch_open → landing_to_raw → raw_to_curated → curated_to_publish → publish_to_snowflake → parent_batch_close | **Glue Workflow + triggers** wiring your existing 7 jobs | `setup_glue_workflow.py` |
| RDS config/audit framework | local Postgres `control` DB, completed with batch/audit tables | `seed_control_db.sql` |
| Snowflake publish layer | your existing `VEEVA_CRM.PUB.*` schema | (already in place) |
| Job orchestration / auditing | workflow run + control-table rows | `check_run.py` |

Out of scope for the core path (documented, not built): DQ reject routing,
message_template alerting, Informatica/Nginx outbound, Lake Formation. The migration
tool's Phase 3/6/7 features cover their target-side equivalents.

## Run order

```bash
export AWS_PROFILE=<profile>  AWS_REGION=us-west-2

# 1. S3 zone layout (idempotent)
python setup_s3_zones.py --bucket <your-cdl-bucket>

# 2. Complete the control DB (adds parent_batch_process, file_process_log,
#    dl_ingestion_log — additive, never touches your existing 3 tables' data)
psql -h localhost -d control -f seed_control_db.sql

# 3. Wire the Glue Workflow around your existing jobs (idempotent; prints the graph)
python setup_glue_workflow.py            # auto-discovers job names, or:
python setup_glue_workflow.py --jobs load_confiq,parent_batch_open,landing_to_raw,raw_to_curated,curated_to_publish,publish_to_snowflake,parent_batch_close

# 4. Simulate an SFTP file arrival + start a run
python drop_sample_file.py --bucket <your-cdl-bucket> --start-workflow

# 5. Watch the run + audit rows
python check_run.py --workflow cdl_ingest
```

After a green run, the sfglue app's **introspect with `include: ["workflows","triggers"]`**
sees the workflow, and `/api/sfglue/workflows/plan` converts it to the Databricks Job —
the full reference→target orchestration story.

## Run the MIGRATED pipeline as an Airflow DAG (target side → Databricks)

Once the artifacts are pushed to the workspace (`POST /api/sfglue/workspace/push` →
`/Shared/sfglue`), Airflow orchestrates the migrated pipeline on Databricks. The DAG is
**provider-free**: it uses only `BashOperator` + the stdlib `run_databricks_task.py`
helper (Databricks Jobs `runs/submit` REST API), so it imports and runs on the reference
Airflow 2.10.5 with **no** `apache-airflow-providers-databricks` (that provider forces an
Airflow 3.x upgrade and breaks the setup).

Two DAG shapes, both provider-free:

- **Per-task** (the full pipeline; no pre-built Databricks Job needed) — one task per
  migrated notebook + per dbt layer, discovered from the pushed workspace:

  ```bash
  # in the airflow venv, with AIRFLOW_HOME set and `airflow standalone` running:
  pip install dag-factory     # one-time
  python setup_airflow_databricks.py --per-task \
      --databricks-host https://dbc-xxxx.cloud.databricks.com \
      --databricks-token dapi... \
      --catalog workspace --warehouse <SQL_WAREHOUSE_ID> \
      --airflow-password <admin-pw> --trigger --watch
  ```
  Writes `~/airflow/dags/cdl_migrated_databricks.yaml`, registers it, unpauses, triggers,
  and watches to completion (exits 0 green / 1 red).

- **Single-task** (triggers a pre-deployed sfglue Databricks Job via `run-now`) — the
  original one-task variant; drop `--per-task` and it auto-discovers the Job by
  `tags.sfglue_source`.

The app can also emit the per-task YAML directly:
`POST /api/sfglue/airflow/emit {"provider_free": true, "destination": {...}}`. The emitted
file reads `DATABRICKS_HOST` / `DATABRICKS_TOKEN` / `CDL_HELPER` from the Airflow env
(no secret baked into the YAML) — export those before triggering. The committed
`cdl_migrated_databricks.yaml` is a regenerated reference copy of that output.

## Optional: real SFTP (AWS Transfer Family)

The core-path boundary is the landing bucket. If the demo must show a real SFTP drop:
create a Transfer Family SFTP server pointed at the bucket's `landing/` prefix
(Console → Transfer Family → Create server → S3). ~$0.30/hr while running — create it
for the demo, delete after.
