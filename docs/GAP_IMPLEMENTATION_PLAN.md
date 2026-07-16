# Gap Implementation Plan — sfglue Migration Tool

**Goal:** evolve sfglue from a *table/SQL converter* into a *full-platform migration product* capable of
migrating the reference CDL architecture (SFTP → S3 landing/raw/curated/publish → Glue → Snowflake, with an
RDS config/audit framework, DQ engine, and Informatica/API outbound) to Databricks + dbt, like-for-like.

**Author:** Architecture plan, v1.0 — 2026-07-11
**Scope:** all 11 identified gaps, phased. Each phase maps to concrete modules in this repo.

> **Implementation status (2026-07-11):** Phases 0–7 backend engines implemented and
> unit-tested (see README "Gap-plan" sections for the endpoints). Delivered: orchestration
> converter + deploy (`orchestration_migration.py`), control-plane migration incl.
> query_configuration→dbt (`control_plane_migration.py`), DQ compiler + alerting
> (`dq_migration.py`), Snowflake pipeline-object introspection (`snowflake_client.py`) +
> dispositions, outbound kit (`consumption_inventory.py`), LF→UC grants diff-only
> (`governance_migration.py`), bundle export for all artifact classes. Remaining:
> frontend review-UI surfaces for the new artifact classes, workflow dry-run gate,
> DQ replay gate, incremental dbt_build compilation (Phase 5.3), and the Phase 8
> pilot/parallel-run — these need live-environment iteration.

---

## 1. Target end-state

A user connects Snowflake + Glue + the RDS config DB, and the tool produces a **complete, deployable
Databricks workspace bundle**:

| Legacy component | Migrated artifact |
|---|---|
| Glue ETL jobs (ingestion) | Bronze PySpark notebooks (exists today) |
| Glue ETL jobs (transform) + Snowflake views/SQL | dbt models silver/gold (exists today) |
| Snowflake tables | Delta DDL + staging models (exists today) |
| Glue Workflows / Triggers (parent-batch orchestration) | **Databricks Workflows (Jobs) via asset bundle** |
| RDS config/audit framework (configuration_master, file_process_log, …) | **UC control schema + framework notebooks** |
| dq_rules + reject buckets | **dbt tests + DLT-style expectations + quarantine tables** |
| SFTP / S3 event ingestion | **Auto Loader + file-arrival triggers** |
| Glue Crawlers | **Auto Loader schema inference / UC external tables** |
| Snowflake tasks/streams/pipes/procs | **Workflows / Delta CDF / Auto Loader / notebooks or dbt** |
| Informatica/Nginx outbound, Reltio, Power BI | **Consumption repoint kit (inventory + Delta Sharing + SQL endpoints)** |
| Lake Formation permissions | **Unity Catalog GRANT script** |
| message_template alerts | **Workflow notifications + alert framework notebook** |
| Dev/Test/Prod × US/EU | **Databricks Asset Bundle (DAB) targets** |

**Product-level principle:** every generated artifact remains subject to the existing verification gates
(precheck → deploy → reconcile → dbt tests → report). New artifact classes get their own gates (dry-run
workflow validation, DQ-rule replay, grant diff).

## 2. Guiding architecture principles (reuse what works)

1. **Engine/route split** stays: pure, unit-testable engines in `backend/integrations/*` +
   `backend/migration/*`; Flask routes in `*_routes.py` only marshal payloads. New engines follow suit.
2. **AI with deterministic fallback**: every AI converter degrades to an annotated scaffold
   (pattern in `snowflake_glue_migration.py`). Orchestration/DQ/grants conversion must ALSO work fully
   deterministically — AI only enriches (naming, comments, ambiguous mapping).
3. **Nothing deploys unverified**: extend the reconcile/precheck gate philosophy to workflows (dry-run),
   DQ rules (replay on sample), and grants (diff-only mode first).
4. **Everything exportable**: the dbt-project export becomes a **Databricks Asset Bundle export**
   (`databricks.yml` + resources + notebooks + dbt project) so the output is deployable via
   `databricks bundle deploy` outside the tool.
5. **Per-request credentials** stay (no stored secrets); new connectors (Workflows API, LF API) reuse the
   existing `*ConnectionConfig.from_payload` pattern.

## 3. Phasing overview

| Phase | Gaps | Theme | Depends on | Est. effort |
|---|---|---|---|---|
| 0 | — | Introspection foundation + bundle export skeleton | — | 2 wks |
| 1 | G1, G5 | Orchestration: workflows, triggers, crawlers → Databricks Jobs | 0 | 4–5 wks |
| 2 | G2 | RDS config/audit framework → UC control plane | 0 | 4 wks |
| 3 | G3, G9 | DQ engine, reject routing, alerting | 2 | 3–4 wks |
| 4 | G4 | Event-driven ingestion (SFTP/S3 events → Auto Loader) | 1 | 2–3 wks |
| 5 | G6, G10 | Snowflake advanced objects + true incremental models | 0 | 4 wks |
| 6 | G7 | Outbound/consumption repoint kit | 1–3 | 3 wks |
| 7 | G8, G11 | Governance (LF→UC) + multi-env asset-bundle targets | all | 3 wks |
| 8 | — | Hardening, E2E pilot on reference CDL, GA | all | 3 wks |

Phases 1–2 can run in parallel with 5 (different engineers, disjoint modules). Critical path:
0 → 1 → 4 → 8 and 0 → 2 → 3 → 8. **Total ≈ 5–6 months with 3 engineers; ~4 months with 4.**

---

## Phase 0 — Foundations (2 wks)

**0.1 Introspection expansion (`glue_client.py`)**
Add `list_glue_workflows()`, `list_glue_triggers()`, `list_glue_crawlers()` (boto3 paginators:
`get_workflows`/`get_workflow`, `get_triggers`, `get_crawlers`), returning the same masked/dict shape as
`list_glue_jobs`. Wire into `/api/sfglue/introspect` behind a `include: ["workflows","triggers","crawlers"]`
payload flag so existing clients are unaffected.

**0.2 RDS framework introspection (`postgres_client.py`)**
Add `introspect_framework_tables()`: detect the known control tables (`configuration_master`,
`file_process_log`, `parent_batch_process`, `cdl_ingestion_log`, `query_configuration`, `dq_rules`,
`message_template`, `cdl_ds_snowflake_replicate`) by name heuristics + column-shape fingerprints, and pull
**row contents** (config rows are the actual migration input, not just schema). Cap rows, mask secrets-like
columns.

**0.3 Databricks Asset Bundle (DAB) export skeleton**
New module `backend/integrations/bundle_export.py`: emits `databricks.yml`, `resources/*.yml`,
`notebooks/`, `dbt/` from the migration state. Phase 0 delivers the skeleton with today's artifacts
(notebooks + dbt project); later phases add resource types. Extend `/api/sfglue/export` with
`format: "bundle"`.

**0.4 Migration-state schema versioning**
The review/convert state (see `frontend/src/pages/review-state.js` and the routes' state payloads) gets a
`schemaVersion` and typed artifact registry (`artifactType: notebook|dbt_model|ddl|workflow|dq_rule|grant|…`)
so new artifact classes flow through review → report without page rewrites.

**Acceptance:** introspect returns workflows/triggers/crawlers + framework-table rows for the reference
CDL account; `export?format=bundle` produces a bundle that `databricks bundle validate` passes.

---

## Phase 1 — Orchestration migration (Gaps 1, 5) — the blocker (4–5 wks)

**Problem:** the legacy pipeline is sequenced by Glue Workflows/Triggers implementing
`Trigger → Load Config → Parent batch open → landing→raw → raw→curated → curated→publish →
publish→snowflake → parent batch close`, plus a second outbound workflow. None of this is read or converted.

**1.1 Orchestration model (new `backend/integrations/orchestration_migration.py`, pure)**
- Parse Glue workflow graphs: nodes = jobs/crawlers, edges = trigger predicates (ON_DEMAND, SCHEDULED cron,
  CONDITIONAL on job SUCCEEDED/FAILED). Build a normalized DAG:
  `{tasks: [{id, kind, legacyRef, dependsOn[], schedule?, retry?, timeout?}]}`.
- Map each node to its **converted artifact** from the existing migration output (job → notebook or dbt
  model set) via `snowflake_glue_lineage.classify_job` / `migration_layer`. Crawler nodes map to
  Auto Loader/`CREATE EXTERNAL TABLE` setup tasks (see 1.3).
- Emit **Databricks Jobs**: one Workflow per Glue workflow — `notebook_task` for bronze notebooks,
  `dbt_task` (or `sql_task` using `dbt_build.py` compiled SQL as fallback) for model groups,
  `depends_on` from trigger predicates, `schedule` from SCHEDULED triggers (cron translation incl.
  Glue's day-of-week quirks), `email_notifications` stub (Phase 3 fills from message_template),
  retry/timeout defaults + per-task overrides.
- Parent-batch open/close tasks are re-pointed at the Phase 2 control-plane notebooks; until Phase 2 lands
  they become no-op logging tasks with a TODO marker (deterministic scaffold rule).
- Output: Jobs API 2.1 JSON **and** DAB `resources/jobs/*.yml` (same model, two serializers).

**1.2 Deploy + validation gate (`databricks_agent_routes.py` + `qvd_to_databricks/databricks_executor.py`)**
- New routes: `/api/sfglue/workflows/plan` (convert + diff vs existing jobs in the workspace — reuse the
  precheck philosophy), `/api/sfglue/workflows/deploy` (Jobs API create/update, idempotent by
  `tags.sfglue_source`), `/api/sfglue/workflows/dry-run` (run with all tasks in a skip/echo mode or
  `run_now` on a limit-0 parameter set where safe).
- Gate: a workflow is "verified" when a dry run completes and every task resolved its artifact.

**1.3 Crawler replacement (Gap 5)**
Deterministic mapping per crawler target: S3 path + format → either (a) an Auto Loader bronze notebook
snippet (`cloudFiles.schemaEvolutionMode`, `schemaLocation`) merged into the ingestion notebook for that
feed, or (b) `CREATE EXTERNAL TABLE ... LOCATION` in UC for query-in-place tables. The choice is surfaced
in review as an editable decision (default: Auto Loader if a Glue job reads the crawled table, external
table otherwise).

**1.4 Frontend**
New review tab "Orchestration" (extend `snowflake-glue-review.js` + a `workflowGraph` component reusing
`lineageFlow.js` rendering): shows legacy workflow DAG side-by-side with the proposed Databricks Jobs DAG,
per-task mapping table, editable schedules/retries. Databricks-agent page gets Plan/Deploy/Dry-run cards.

**Testing:** unit — DAG parser on fixture workflows (fan-in, conditional-on-failure, scheduled+conditional
mix); cron translation table; serializer golden files. Integration — deploy to a scratch workspace, dry-run.

**Acceptance:** the reference CDL's two workflows (ingestion chain + outbound chain) convert to two
Databricks Jobs whose dry-runs pass, exported in the bundle.

---

## Phase 2 — RDS config/audit framework → UC control plane (Gap 2) (4 wks)

**Problem:** the platform is *metadata-driven*: `configuration_master` defines feeds,
`query_configuration` parameterizes SQL, `parent_batch_process`/`file_process_log`/`cdl_ingestion_log`
provide batch tracking + audit. Migrating jobs without this framework changes operational behavior.

**Decision (architectural):** re-platform the control schema into a UC schema (`<catalog>._sfglue_control`)
as Delta tables, and ship a small, generic **framework runtime** as versioned notebooks — NOT a rewrite of
each stored config row into code. Config stays data; behavior stays config-driven. Rationale: preserves the
ops model (add a feed = insert a config row), minimizes AI surface, and keeps parity provable.

**2.1 Control-schema migration (new `backend/integrations/control_plane_migration.py`)**
- DDL translation for the 8 framework tables (reuse `snowflake_type_to_databricks`-style mapping for
  Postgres types — extract shared type-mapper into `backend/migration/type_mapping.py`).
- Data seeding via the existing seed-bronze path (`/api/sfglue/seed-bronze` generalized to
  `/api/sfglue/seed`, source = Postgres runner) with **config-row transformation rules**: S3 URIs →
  target paths/volumes, Glue job names → Databricks task keys, Snowflake identifiers → UC identifiers.
  Deterministic rewrite table generated from the Phase 1 mapping; unresolved rows flagged for review.
- `query_configuration` rows containing SQL run through the existing AI SQL converter
  (`snowflake_glue_migration` convert path) with the sql_guard applied.

**2.2 Framework runtime notebooks (generated, templated — not AI)**
Four parameterized notebooks emitted into the bundle: `fw_batch_open`, `fw_batch_close`,
`fw_file_audit` (writes file_process_log/cdl_ingestion_log equivalents), `fw_config_reader` (Python lib
cell shared via `%run`). Phase 1 workflow tasks re-point to these. Audit writes use Delta `MERGE` keyed on
batch/file id — idempotent on retry.

**2.3 Parity gate**
Extend `reconcile.py` usage: after a pilot run, reconcile legacy `file_process_log` vs new audit table on
count + key integrity for the same input files. This is the framework's fidelity proof.

**Frontend:** "Control plane" section in review (framework tables, rewrite decisions, unresolved rows) +
seed/verify actions on the Databricks-agent page.

**Acceptance:** a feed configured only in `configuration_master` flows end-to-end on Databricks with
batch open/close + audit rows written, no code change.

---

## Phase 3 — DQ engine, reject routing, alerting (Gaps 3, 9) (3–4 wks)

**3.1 DQ-rule compiler (new `backend/integrations/dq_migration.py`, pure)**
Input: `dq_rules` rows (from Phase 0 introspection). Classify each rule:
- **Column-shape rules** (not-null, unique, accepted values, ranges, referential) → **dbt tests** in
  `schema.yml` (native + `dbt_utils`/`dbt_expectations`), attached to the staging/silver model of the feed.
- **File-validation rules** (header/count/format checks at landing) → checks in the bronze Auto Loader
  notebook (badRecordsPath / manual assertion cells) — these run before dbt exists in the flow.
- **Row-quarantine rules** (reject rows, continue load) → generated quarantine pattern: model splits into
  `stg_X` + `stg_X__rejects` (`WHERE NOT (rule)` inverted), mirroring `landing_reject/raw_reject` buckets
  as `_rejects` Delta tables + a `dq_reject_log` control-plane table. dbt tests assert reject rates.
- Unclassifiable rules → AI-assisted proposal with the standard scaffold fallback, flagged for review.

**3.2 Rule replay gate**
Before accepting a compiled rule: run legacy rule and compiled rule against the same sampled data
(Snowflake side via existing runner, Databricks side post-seed) and compare pass/fail row counts.
Reuses the reconcile runner-injection pattern; new route `/api/sfglue/dq/replay`.

**3.3 Alerting (Gap 9)**
`message_template` rows → (a) `email_notifications`/`webhook_notifications` on the corresponding
Databricks Job (failure/success wiring from trigger predicates), (b) a `fw_notify` framework notebook for
in-pipeline templated alerts (file validation failure, DQ threshold breach) rendering the migrated
templates from the control schema. SNS targets map to webhooks; recipients carried as-is.

**Frontend:** "Data Quality" review tab — rule table (legacy rule → compiled artifact → replay status),
editable severity (fail/warn/quarantine).

**Acceptance:** ≥90% of reference `dq_rules` compile deterministically; replay pass on sampled data;
reject rows land in `_rejects` tables; failure alert fires on a forced bad file.

---

## Phase 4 — Event-driven ingestion (Gap 4) (2–3 wks)

- **SFTP landing stays on AWS Transfer Family** (moving managed SFTP is out of scope and unnecessary);
  the migration boundary is the S3 landing bucket.
- Bronze notebooks for file feeds are upgraded to **Auto Loader** (`cloudFiles`) reading the landing
  bucket path from `configuration_master` (Phase 2), replacing the S3-data-event → Glue trigger with either
  **file-arrival triggers** on the Databricks Job (default) or continuous Auto Loader for high-frequency
  feeds — a per-feed review decision.
- New converter rules in `snowflake_glue_migration.py`'s ingestion path: detect S3-event-driven jobs
  (from Phase 1 trigger metadata) and emit the file-arrival trigger in the Jobs resource + checkpoint/
  schema-location convention (`/Volumes/<catalog>/_sfglue_control/checkpoints/<feed>`).
- **External locations/credentials prerequisite** (UC storage credential + external location for the
  landing bucket) emitted as a documented setup step in the precheck: `/api/sfglue/precheck` gains a
  "storage access" section that verifies `LIST` on the landing path and fails actionably.

**Acceptance:** dropping a file in landing triggers the Databricks ingestion Job end-to-end for a pilot feed.

---

## Phase 5 — Snowflake advanced objects + incremental (Gaps 6, 10) (4 wks)

**5.1 Introspection (`snowflake_client.py`)**
Add `SHOW PROCEDURES / TASKS / STREAMS / PIPES / STAGES / FILE FORMATS` (+ `GET_DDL` per object), same
privilege-tolerant SHOW→INFORMATION_SCHEMA fallback as tables/views. Feed into lineage
(`snowflake_glue_lineage.py`): tasks/streams/pipes become graph nodes so the review shows what consumes what.

**5.2 Conversion rules (extend `snowflake_glue_migration.py`)**
| Object | Target |
|---|---|
| Task (scheduled SQL) | task in the Phase-1 Databricks Job (sql/dbt task), cron translated |
| Task DAG (AFTER chains) | depends_on edges in the same Job |
| Stream on table | Delta **Change Data Feed** on the migrated table + `readChangeFeed` in consumer; if consumed by simple insert-into pattern → dbt incremental model instead |
| Snowpipe | Auto Loader (folds into Phase 4 machinery) |
| Stored procedure (SQL script) | AI → notebook (procedural) or dbt model (single-statement body); sql_guard applied; scaffold fallback |
| Stage / file format | UC external location note + Auto Loader read options |
| Publish extracts/aggregates/semantic views | already views/tables — ensure gold-layer classification (`_GOLD_NAME_RE`) covers extract/semantic naming; add rules |

**5.3 True incremental models (Gap 10)**
Lift the documented v1 limitation:
- Converter: when a legacy job/task is detectably incremental (MERGE, stream consumer, batch-window
  predicate from `query_configuration`), emit dbt `materialized='incremental'` with
  `incremental_strategy='merge'`, `unique_key` from PK metadata (already introspected for reconcile),
  and an `is_incremental()` window predicate.
- `dbt_build.py`: implement incremental compilation — first build = CTAS, subsequent = `MERGE INTO`
  wrapper; drop the "always full refresh" caveat. `partition_by` passthrough to Delta while here.
- Reconcile gate extension: run reconcile after an incremental batch, not only full loads.

**Acceptance:** reference Snowflake tasks/streams/pipes appear in lineage and convert; an incremental
model processes a second batch without full rebuild and passes reconcile.

---

## Phase 6 — Outbound/consumption repoint kit (Gap 7) (3 wks)

**Position:** the tool does not migrate Informatica mappings or the Nginx layer (third-party systems);
it delivers everything those systems need to **repoint from Snowflake to Databricks**, plus native
replacements where cheap.

- **Consumption inventory** (new `backend/integrations/consumption_inventory.py`): from Snowflake
  `ACCOUNT_USAGE.ACCESS_HISTORY`/`QUERY_HISTORY` (privilege permitting), identify external consumers per
  published object (Informatica service user, Power BI, Reltio, API layer) → a per-consumer object list
  with the new UC fully-qualified names. This is the outbound cutover checklist.
- **Repoint artifacts** generated into the bundle: Databricks SQL Warehouse connection sheet (hostname,
  http_path, OAuth/service-principal guidance) per consumer; Power BI: partner-connect/DSN instructions +
  the semantic-view equivalents as SQL views; Informatica IICS: Databricks Delta connector mapping notes
  keyed by the inventory; optional **Delta Sharing** share definition for file-based consumers (Porzio-EU
  style file drops) as SQL `CREATE SHARE` scripts.
- **Reltio loop:** mastered-data return feed = an ingestion feed like any other → route it through the
  standard feed config (Phase 2) reading Reltio's S3 drop; document only the Reltio-side URL change.
- Report page gains an "Outbound cutover" section (consumer × objects × status).

**Acceptance:** for the reference system, the report lists every outbound consumer with its object list and
generated repoint artifact; one pilot consumer (Power BI) validated against a SQL Warehouse.

---

## Phase 7 — Governance + multi-env (Gaps 8, 11) (3 wks)

**7.1 Lake Formation → Unity Catalog (new `backend/integrations/governance_migration.py`)**
- Introspect: `lakeformation.list_permissions` (+ Glue resource policies) via the existing Glue session
  plumbing; also Snowflake `SHOW GRANTS` for the warehouse side.
- Deterministic mapping: LF database/table/column grants → UC `GRANT SELECT/MODIFY ON catalog/schema/table`;
  LF data filters / column-level → UC row filters + column masks (generate `CREATE FUNCTION` masks +
  `ALTER TABLE ... SET ROW FILTER`); principals (IAM roles/users) → account groups via an **editable
  principal-mapping table** in review (this mapping is inherently human — never auto-applied).
- Gate: **diff-only mode first** — emit the grant script + a diff vs current UC grants; apply is a separate
  explicit action. Grants are also emitted into the bundle as SQL for env promotion.

**7.2 Multi-env / region (DAB targets)**
- `bundle_export.py` emits `targets: {dev, test, prod}` (× region where applicable) with per-target
  catalog/schema prefixes, workspace hosts, and variable-substituted storage paths — mirroring the
  Dev/Test/Prod × US/EU matrix. The tool itself keeps deploying to ONE workspace per session (per-request
  creds); promotion happens via the bundle in CI (`databricks bundle deploy -t prod`), which is the
  correct enterprise posture. Ship a sample GitHub Actions/Azure DevOps pipeline in the export.

**Acceptance:** grant diff for the reference account reviewed and applied to dev; the same bundle deploys
to a second (test) workspace changing only the target flag.

---

## Phase 8 — Hardening + pilot + GA (3 wks)

- **E2E pilot on the reference CDL**: one full vertical slice (one high-value feed: SFTP file → Auto Loader
  bronze → DQ + quarantine → silver/gold dbt → workflow schedule → reconcile → outbound repoint) run for
  ≥1 week in parallel with legacy; daily reconcile as the parallel-run report.
- Performance: introspection pagination/caching for large catalogs (hundreds of jobs/rules); AI-call
  budget telemetry per phase (extend `_map_concurrent` metrics).
- Failure-mode review: expired-SSO paths (`needsAiConfig` pattern) extended to Jobs API/LF API calls;
  partial-deploy rollback for workflows (delete-by-tag).
- Docs: operator runbook, cutover checklist (freeze → final reconcile → repoint outbound → decommission),
  and per-gap "what is NOT migrated" honesty list (e.g., Informatica mapping logic, SAS workspace —
  the data-science workspace is intentionally out of scope; Databricks notebooks/Repos are the landing
  zone for R/Python users, SAS remains external).

---

## 4. Cross-cutting workstreams

- **Testing strategy:** every new engine is pure with fixture-driven unit tests (pattern:
  `reconcile.py --self-test`); golden-file tests for all serializers (Jobs JSON, DAB YAML, grant SQL);
  one integration suite gated on scratch-workspace creds; frontend: extend `api.test.js` per new route.
- **Security:** no new credential persistence; Jobs/LF/UC calls use per-request tokens; generated bundles
  must never embed secrets (profiles reference env vars — pattern already in dbt export).
- **AI usage policy:** AI allowed for SQL/procedural code translation and naming only; orchestration
  graphs, DQ classification, grants, and config rewrites are deterministic. Keeps fidelity provable and
  cost bounded.
- **Team shape:** 3–4 engineers — (a) orchestration/Databricks platform, (b) control-plane/DQ,
  (c) Snowflake objects/dbt/reconcile, (d, optional) frontend + bundle/CI. Weekly architecture review;
  each phase exits only on its acceptance criteria.

## 5. Key risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Glue trigger predicates richer than Jobs `depends_on` (e.g., ANY-of, EventBridge) | Wrong sequencing | DAG parser flags non-representable predicates for review; synthesize sentinel tasks where needed |
| Framework tables diverge from the 8 known shapes at other clients | Phase 2 misses tables | Fingerprint detection + manual "mark as control table" in review UI |
| `ACCESS_HISTORY` not licensed (needs Enterprise ed.) | No consumption inventory | Fallback: QUERY_HISTORY parse + manual consumer entry |
| Incremental-detection false positives | Data loss on merge | Reconcile-after-increment gate is mandatory before marking verified |
| LF principal→UC group mapping errors | Access outage | Diff-only first; apply gated behind explicit review sign-off |
| AI provider limits at scale (100s of objects) | Slow/failed runs | Existing bounded `_map_concurrent` + per-phase budget telemetry + resumable state |

## 6. Definition of done (product)

1. Reference CDL migrates end-to-end with every diagram component either **migrated** (artifact in the
   bundle, gate passed) or **explicitly dispositioned** (repoint kit / documented out-of-scope).
2. `databricks bundle deploy` of the export stands up jobs, notebooks, dbt, grants, and control schema in
   a clean workspace.
3. Parallel-run week: daily reconcile green on all pilot feeds; audit-log parity proven.
4. Report page presents the full disposition matrix — the artifact a migration lead hands to sign-off.
