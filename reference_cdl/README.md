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

## Optional: real SFTP (AWS Transfer Family)

The core-path boundary is the landing bucket. If the demo must show a real SFTP drop:
create a Transfer Family SFTP server pointed at the bucket's `landing/` prefix
(Console → Transfer Family → Create server → S3). ~$0.30/hr while running — create it
for the demo, delete after.
