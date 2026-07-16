"""Conversion engine for the Snowflake/Glue → Databricks/DBT flow (Phase 3).

Implements the migration plan's core principle — the **E+L vs T split at the
bronze boundary**:

  * ingestion Glue jobs  → Databricks extract/load **notebooks** landing **bronze**
  * transformation Glue jobs + Snowflake views/SQL → **dbt models** (silver/gold)
  * Snowflake tables      → Databricks DDL (type-translated) + a dbt staging model
                            reading the bronze source

Plus deterministic scaffolding the plan calls for: Snowflake→Delta type mapping,
``sources.yml`` with freshness, and the precheck that compares planned targets to
what already exists in Databricks (Unity Catalog) so nothing is duplicated and
the user is told which required tables are still missing.

Pure/deterministic functions are unit-tested; the AI converters take ``call_ai``
and fall back to an annotated scaffold (with the original code embedded) when AI
isn't configured, so they always return usable output.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from backend.integrations.snowflake_glue_lineage import (
    _base_name,
    _glue_layer,
    canonical_layer,
    _looks_like_path,
    _norm,
    classify_job,
    migration_layer,
    upstream_subgraph,
)

logger = logging.getLogger(__name__)

# Per-table model translation is one AI call each; a multi-output job is dozens of
# independent, I/O-bound calls. Run them concurrently (bounded, to stay under the
# provider's rate limit) instead of serially. Tune with SFGLUE_AI_WORKERS.
try:
    _AI_WORKERS = max(1, int(os.environ.get("SFGLUE_AI_WORKERS", "4")))
except ValueError:
    _AI_WORKERS = 4


from backend.migration.dialect_normalizer import finalize_sfglue_model_sql


def _map_concurrent(items, fn):
    """Apply ``fn`` to each item concurrently (bounded by _AI_WORKERS), preserving
    input order. Falls back to serial for 0/1 items. ``fn`` should handle its own
    errors (the AI converters degrade to a scaffold), so a slow/failed call never
    sinks the batch."""
    items = list(items)
    if len(items) <= 1:
        return [fn(x) for x in items]
    with ThreadPoolExecutor(max_workers=min(_AI_WORKERS, len(items))) as ex:
        return list(ex.map(fn, items))


# ─── Snowflake → Databricks type translation ─────────────────────────────────

def snowflake_type_to_databricks(sf_type: str) -> str:
    """Map a Snowflake column type to its Databricks/Delta equivalent.

    Covers the cases the plan calls out: NUMBER(p,s)→DECIMAL(p,s), VARIANT/OBJECT/
    ARRAY→STRING, timestamp variants, and the common scalar families.
    """
    t = str(sf_type or "").strip().upper()
    if not t:
        return "STRING"
    m = re.match(r"([A-Z_]+)\s*(\(([^)]*)\))?", t)
    base = m.group(1) if m else t
    args = (m.group(3) or "").strip() if m else ""

    if base in ("NUMBER", "NUMERIC", "DECIMAL"):
        return f"DECIMAL({args})" if args else "DECIMAL(38,0)"
    if base in ("INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT"):
        return "BIGINT"
    if base in ("FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"):
        return "DOUBLE"
    if base in ("VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT", "NVARCHAR", "NVARCHAR2", "NCHAR"):
        return "STRING"
    if base == "BOOLEAN":
        return "BOOLEAN"
    if base == "DATE":
        return "DATE"
    if base in ("TIMESTAMP_NTZ", "DATETIME"):
        return "TIMESTAMP_NTZ"
    if base in ("TIMESTAMP", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"):
        return "TIMESTAMP"
    if base in ("VARIANT", "OBJECT"):
        return "STRING"     # store semi-structured as JSON string (cast on read)
    if base == "ARRAY":
        return "ARRAY<STRING>"
    if base in ("BINARY", "VARBINARY"):
        return "BINARY"
    if base in ("GEOGRAPHY", "GEOMETRY", "TIME"):
        return "STRING"     # no native Databricks equivalent
    return "STRING"


def _ident(name: str) -> str:
    return f"`{name}`" if name else name


def generate_databricks_ddl(target_full_name: str, columns: list, *, source_full_name: str = "",
                            foreign_keys: list | None = None) -> str:
    """Build a CREATE TABLE for Databricks from Snowflake-style columns.

    ``foreign_keys`` (optional) = [{columns:[...], ref_table: <target full name>,
    ref_columns:[...]}] — emitted as informational FOREIGN KEY constraints so the
    relationships from Snowflake carry forward into Unity Catalog.
    """
    lines = []
    for c in columns or []:
        col = c.get("name")
        if not col:
            continue
        lines.append(f"  {_ident(col)} {snowflake_type_to_databricks(c.get('type'))}")
    for fk in foreign_keys or []:
        cols = ", ".join(_ident(c) for c in (fk.get("columns") or []) if c)
        ref_cols = ", ".join(_ident(c) for c in (fk.get("ref_columns") or []) if c)
        if cols and fk.get("ref_table") and ref_cols:
            lines.append(f"  FOREIGN KEY ({cols}) REFERENCES {fk['ref_table']} ({ref_cols})")
    body = ",\n".join(lines) if lines else "  -- no columns introspected"
    header = f"-- migrated from {source_full_name}\n" if source_full_name else ""
    return f"{header}CREATE TABLE IF NOT EXISTS {target_full_name} (\n{body}\n) USING DELTA;"


# ─── dbt sources.yml (bronze) with freshness ─────────────────────────────────

_LOADED_AT_CANDIDATES = ("_loaded_at", "loaded_at", "_load_ts", "load_ts", "_ingested_at",
                        "ingested_at", "dl_load_ts", "_batch_ts", "_ingest_ts", "ingest_ts")


def _detect_loaded_at_field(bronze_columns: dict | None) -> str | None:
    """A bronze audit load-timestamp column to drive dbt source freshness, or None.

    Source-agnostic: freshness is declared ONLY when the landed data actually carries such a
    column — otherwise ``dbt source freshness`` errors on a missing ``loaded_at_field``. We
    never assume a specific audit-column name; we only recognize common ones if present.
    """
    seen = {str(c).lower() for cols in (bronze_columns or {}).values() for c in (cols or [])}
    return next((c for c in _LOADED_AT_CANDIDATES if c in seen), None)


def generate_sources_yml(catalog: str, bronze_schema: str, bronze_tables: list,
                         source_catalog: str | None = None, source_schema: str | None = None,
                         loaded_at_field: str | None = None) -> str:
    """Generate a dbt sources.yml declaring the bronze tables (+ a freshness gate IFF a
    load-timestamp column is known).

    ``source_catalog``/``source_schema`` are the real raw landing location the
    ``{{ source('bronze', …) }}`` refs resolve to; they default to ``catalog``/``bronze_schema``.
    ``loaded_at_field`` drives the optional freshness block — pass one only when the bronze
    tables actually carry it (see ``_detect_loaded_at_field``). When None, no freshness is
    emitted (a dbt source is valid without it), so this never assumes an audit column that a
    given pipeline's landing step didn't produce.
    """
    db = source_catalog or catalog
    schema = source_schema or bronze_schema
    rows = "\n".join(f"      - name: {t}" for t in sorted(set(bronze_tables))) or "      []"
    freshness = (
        f"    loaded_at_field: {loaded_at_field}\n"
        "    freshness:\n"
        "      warn_after: {count: 24, period: hour}\n"
        "      error_after: {count: 48, period: hour}\n"
    ) if loaded_at_field else ""
    return (
        "version: 2\n\n"
        "sources:\n"
        "  - name: bronze\n"
        f"    database: {db}\n"
        f"    schema: {schema}\n"
        f"{freshness}"
        "    tables:\n"
        f"{rows}\n"
    )


# ─── dbt project scaffolding (runnable-project export) ───────────────────────

def _dbt_project_name(project_name: str | None) -> str:
    name = re.sub(r"[^a-z0-9_]", "_", (project_name or "sfglue_migration").lower()).strip("_")
    return name or "sfglue_migration"


def generate_dbt_project_yml(destination: dict, project_name: str | None = None) -> str:
    """A dbt_project.yml for the converted models, organized in the standard dbt layer
    layout — staging / intermediate / marts. Every layer materializes as TABLES (the
    intermediate tables are first-class deliverables, not throwaway views): staging +
    intermediate land in the silver schema, marts in gold. Per-model config() can still
    override, but the project default is the persisted-tables contract."""
    name = _dbt_project_name(project_name)
    d = destination or {}
    silver = d.get("silver_schema") or "silver"
    gold = d.get("gold_schema") or "gold"
    return (
        f"name: '{name}'\n"
        "version: '1.0.0'\n"
        "config-version: 2\n"
        f"profile: '{name}'\n\n"
        'model-paths: ["models"]\n'
        'seed-paths: ["seeds"]\n'
        'test-paths: ["tests"]\n'
        'macro-paths: ["macros"]\n\n'
        "models:\n"
        f"  {name}:\n"
        "    +materialized: table\n"
        "    staging:\n"
        "      +materialized: table\n"
        f"      +schema: {silver}\n"
        "    intermediate:\n"
        "      +materialized: table\n"
        f"      +schema: {silver}\n"
        "    marts:\n"
        "      +materialized: table\n"
        f"      +schema: {gold}\n"
    )


# generate_schema_name override: dbt's default CONCATENATES target schema with the custom
# schema ("silver" + "gold" → "silver_gold"). We want the custom schema verbatim so the
# layer configs above land exactly in <catalog>.silver / <catalog>.gold.
_GENERATE_SCHEMA_NAME_MACRO = (
    "{% macro generate_schema_name(custom_schema_name, node) -%}\n"
    "    {%- if custom_schema_name is none -%}\n"
    "        {{ target.schema }}\n"
    "    {%- else -%}\n"
    "        {{ custom_schema_name | trim }}\n"
    "    {%- endif -%}\n"
    "{%- endmacro %}\n"
)

_MART_PREFIXES = ("dim_", "fct_", "fact_", "mart_", "pub_", "agg_")


def dbt_layer_for_model(fname: str, sql: str = "") -> str:
    """staging | intermediate | marts — by naming convention, falling back to the
    schema the model's own config() targets (gold ⇒ marts)."""
    base = str(fname or "").lower()
    if base.startswith("stg_"):
        return "staging"
    if base.startswith("int_"):
        return "intermediate"
    if base.startswith(_MART_PREFIXES):
        return "marts"
    if re.search(r"schema\s*=\s*['\"]gold['\"]", sql or ""):
        return "marts"
    return "staging"


def generate_profiles_yml(destination: dict, project_name: str | None = None) -> str:
    """A dbt-databricks profiles.yml. Catalog/schema come from the ``destination`` payload;
    host/http_path/token are ``env_var`` references — NO secrets are baked in. Fully
    source-agnostic (nothing about any one pipeline)."""
    d = destination or {}
    name = _dbt_project_name(project_name)
    catalog = d.get("catalog") or "main"
    schema = d.get("silver_schema") or d.get("gold_schema") or d.get("bronze_schema") or "default"
    return (
        f"{name}:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: databricks\n"
        f"      catalog: {catalog}\n"
        f"      schema: {schema}\n"
        "      host: \"{{ env_var('DATABRICKS_HOST') }}\"\n"
        "      http_path: \"{{ env_var('DATABRICKS_HTTP_PATH') }}\"\n"
        "      token: \"{{ env_var('DATABRICKS_TOKEN') }}\"\n"
        "      threads: 4\n"
    )


def build_dbt_project_files(conversion: dict, destination: dict,
                            project_name: str | None = None) -> dict:
    """Assemble a runnable dbt project as ``{relative_path: text}`` from a run_conversion result.

    Layout: dbt_project.yml / profiles.yml / packages.yml at root; models/*.sql + sources.yml
    + schema.yml + unit_tests.yml under models/; reference DDL (ddl/) and bronze ingest
    notebooks (notebooks/) alongside (bronze ingestion runs outside dbt). Source-agnostic —
    everything derives from the conversion output and the ``destination`` payload."""
    c = conversion or {}
    name = _dbt_project_name(project_name)
    files: dict = {
        "dbt_project.yml": generate_dbt_project_yml(destination, name),
        "profiles.yml": generate_profiles_yml(destination, name),
    }
    if c.get("packages_yml"):
        files["packages.yml"] = c["packages_yml"]
    if c.get("sources_yml"):
        files["models/sources.yml"] = c["sources_yml"]
    if c.get("schema_yml"):
        files["models/schema.yml"] = c["schema_yml"]
    if c.get("unit_tests_yml"):
        files["models/unit_tests.yml"] = c["unit_tests_yml"]
    files["macros/generate_schema_name.sql"] = _GENERATE_SCHEMA_NAME_MACRO
    for fname, sql in (c.get("dbt_models") or {}).items():
        layer = dbt_layer_for_model(fname, sql if isinstance(sql, str) else "")
        # Persisted-tables contract: middle tables are deliverables, so a per-model
        # materialized='view' from generation is upgraded to 'table' here.
        text = sql if isinstance(sql, str) else str(sql)
        text = re.sub(r"materialized\s*=\s*['\"]view['\"]", "materialized='table'", text)
        files[f"models/{layer}/{fname}"] = text
    for tname, sql in (c.get("ddl") or {}).items():
        files[f"ddl/{tname}.sql"] = sql
    for nbname, code in (c.get("notebooks") or {}).items():
        files[f"notebooks/{nbname}"] = code
    for cfname, text in (c.get("conf_files") or {}).items():
        files[f"conf/{cfname}"] = text if isinstance(text, str) else str(text)
    if c.get("governance_md"):
        files["GOVERNANCE.md"] = c["governance_md"]
    files["README.md"] = (
        f"# {name}\n\n"
        "Generated dbt project (Snowflake + AWS Glue → Databricks + dbt).\n\n"
        "## Run\n"
        "```bash\n"
        "export DATABRICKS_HOST=...  DATABRICKS_HTTP_PATH=...  DATABRICKS_TOKEN=...\n"
        "dbt deps\n"
        "dbt build     # builds models + runs tests/contracts\n"
        "```\n\n"
        "`notebooks/` holds the bronze ingestion (run on Databricks with S3 access) that lands\n"
        "the raw tables the models read via `{{ source('bronze', ...) }}`. `ddl/` is reference\n"
        "DDL for tables materialized outside dbt.\n"
    )
    return files


# ─── destination target naming ───────────────────────────────────────────────

def _schema_for_layer(layer: str, destination: dict) -> str | None:
    d = destination or {}
    return {
        "bronze": d.get("bronze_schema") or "bronze",
        "silver": d.get("silver_schema") or "silver",
        "gold": d.get("gold_schema") or "gold",
    }.get(layer)


def target_table_name(node_label: str, layer: str, destination: dict) -> str | None:
    schema = _schema_for_layer(layer, destination)
    if not schema:
        return None
    catalog = (destination or {}).get("catalog") or "main"
    return f"{catalog}.{schema}.{_base_name(node_label)}"


# Dimensional / serving naming → a table belongs in the gold layer even when its
# source system can't tell us so. Snowflake reports every physical table as a plain
# "table" (lineage types it 'silver'), so a star-schema DIM_/FACT_/MART_ would land
# in silver without this refinement.
_GOLD_NAME_RE = re.compile(r"^(dim|dimension|fact|fct|mart|agg|aggregate|rpt|report|kpi|summary|metrics?)[_]", re.I)


# Generic ways a Spark/Glue job names a table it writes — used to detect whether a
# transformation job has MULTIPLE outputs (so it should become several dbt models, one
# per table) rather than a single model. Intentionally source-agnostic: it matches the
# common idioms, not any one pipeline's helper names.
_OUTPUT_TABLE_RES = (
    re.compile(r"\.saveAsTable\(\s*['\"]([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)*)['\"]", re.I),   # .saveAsTable("db.t")
    re.compile(r"\.insertInto\(\s*['\"]([A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)*)['\"]", re.I),    # .insertInto("db.t")
    re.compile(r"catalogTableName\s*=\s*['\"]([A-Za-z_][\w]*)['\"]", re.I),                   # Glue catalog sink
    re.compile(r"\b\w*(?:write|save|sink|persist|materiali[sz]e)\w*\s*\(\s*[^,()'\"]+,\s*['\"]([A-Za-z_][\w]*)['\"]", re.I),  # write_x(df, "name")
)


def _path_layer_token(path: str) -> str | None:
    """Medallion layer named in an S3 path token, normalized to bronze|silver|gold.

    Recognizes both conventions via the canonical synonym map, so a
    ``s3://.../curated/...`` path resolves to silver and ``s3://.../publish/...`` to
    gold (previously only literal gold/silver/bronze tokens were understood).
    """
    return canonical_layer(path)


def catalog_outputs_for_job(io: dict, lineage: dict) -> list[str]:
    """The catalog tables a transformation job produces — its medallion-layer outputs.

    A transform job's output layer is the curated layer of its write paths (e.g. a job
    writing ``s3://.../gold/...`` produces the gold layer). The Glue catalog already
    lists every table in that layer, so this returns a COMPLETE output list without
    parsing the (often loop/helper-driven) script — the most reliable basis for a
    per-table dbt split. Empty when the job's output layer or catalog can't be resolved.
    """
    writes = (io or {}).get("writes", []) or []
    reads = (io or {}).get("reads", []) or []
    write_layers = {_path_layer_token(w) for w in writes if _looks_like_path(w)}
    out_layer = next((lyr for lyr in ("gold", "silver") if lyr in write_layers), None)
    if not out_layer:
        # Fallback: a transform reads one layer and writes the next one up. If the write
        # path didn't resolve a layer, infer the output from the highest read layer
        # (bronze→silver, silver→gold). Covers jobs whose write target is a templated
        # path the parser couldn't tag.
        read_layers = {_path_layer_token(r) for r in reads if _looks_like_path(r)}
        out_layer = ({"silver": "gold", "bronze": "silver"}.get("silver") if "silver" in read_layers
                     else {"bronze": "silver"}.get("bronze") if "bronze" in read_layers else None)
    if not out_layer:
        return []
    seen: list[str] = []
    for n in (lineage or {}).get("nodes", []) or []:
        if str(n.get("id", "")).startswith("glue:") and _glue_layer(n["label"]) == out_layer:
            base = _base_name(n["label"])
            if base and base not in seen:
                seen.append(base)
    return seen


def detect_job_output_tables(script: str) -> list[str]:
    """Best-effort, source-agnostic list of the table names a transform job writes.

    Catches ``saveAsTable``/``insertInto`` literals, Glue catalog sinks, and the common
    ``write_helper(df, "name")`` idiom. It cannot resolve names passed as variables
    (e.g. a loop var or a helper's parameter), so the result is a LOWER BOUND — which is
    why splitting a job into per-table models is gated on a complete enumeration (AI),
    and this is used only as a signal/annotation in the no-AI path.
    """
    text = script or ""
    found: list[str] = []
    for rx in _OUTPUT_TABLE_RES:
        for m in rx.findall(text):
            base = str(m).split(".")[-1]
            if base and "{" not in base and base not in found:
                found.append(base)
    return found


def _is_warehouse_load_job(script: str) -> bool:
    """True for a job that WRITES to an external warehouse (Snowflake / JDBC / Redshift
    / BigQuery) via a connector — a 'publish' / reverse-ETL load, not a lakehouse
    transform. On Databricks the gold layer is served directly, so these are usually
    obsolete and should not become dbt models or bronze notebooks.
    """
    text = (script or "").lower()
    has_connector = (
        "net.snowflake.spark.snowflake" in text
        or 'format("snowflake")' in text or "format('snowflake')" in text
        or 'format("jdbc")' in text or "format('jdbc')" in text
        or "redshift" in text or "bigquery" in text
    )
    has_warehouse_write = (
        (".save(" in text or ".write" in text)
        and ("dbtable" in text or "sftable" in text or "preactions" in text)
    )
    return has_connector and has_warehouse_write


def _node_layer(node: dict) -> str:
    """Migration target layer for a node.

    Starts from the lineage type (source/bronze/silver/gold) and refines a Snowflake
    *table* to gold when its name is dimensional/mart-shaped — so the star schema and
    marts land in the gold schema instead of silver. Glue catalog tables already carry
    their layer from the schema name, and Snowflake views are already typed gold.
    """
    default = migration_layer(node.get("type"))
    if node.get("system") == "snowflake" and node.get("type") != "gold":
        base = _base_name(node.get("label", ""))
        if _GOLD_NAME_RE.match(base) or "mart" in base:
            return "gold"
    return default


def _migratable_nodes(scoped: dict):
    """Nodes that become target tables (Snowflake + Glue catalog), with layer."""
    out = []
    for n in scoped.get("nodes", []):
        nid = str(n.get("id", ""))
        if nid.startswith("sf:") or nid.startswith("glue:"):
            out.append((n, _node_layer(n)))
    return out


# ─── precheck: planned targets vs what already exists in Databricks ──────────

def build_precheck(lineage: dict, selected_ids, existing_basenames, destination: dict) -> dict:
    """Compare planned target tables to tables already present in Databricks.

    ``existing_basenames`` is the set (lowercased) of table base names found in the
    destination schemas via introspection. Returns target statuses, the already-
    present vs to-migrate split, and the required upstream Snowflake tables that
    the user has NOT selected (the "you still need these" partial-list warning).
    """
    selected = set(selected_ids or [])
    scoped = upstream_subgraph(lineage or {}, selected)
    existing = {str(b).lower() for b in (existing_basenames or [])}

    targets, already_present, to_migrate = [], [], []
    for node, layer in _migratable_nodes(scoped):
        base = _base_name(node["label"])
        target = target_table_name(node["label"], layer, destination)
        exists = base.lower() in existing
        entry = {
            "source": node["label"],
            "layer": layer,
            "target": target,
            "exists_in_databricks": exists,
            "selected": node["id"] in selected,
        }
        targets.append(entry)
        (already_present if exists else to_migrate).append(entry)

    required_not_selected = [
        n["label"] for n in scoped.get("nodes", [])
        if str(n.get("id", "")).startswith("sf:") and n["id"] not in selected
    ]
    return {
        "targets": targets,
        "already_present": already_present,
        "to_migrate": to_migrate,
        "required_not_selected": required_not_selected,
    }


# ─── migration plan: classify jobs in scope, route to converters ─────────────

def _jobs_in_scope(scoped: dict, jobs_io: dict):
    """Job names whose write targets fall inside the scoped subgraph."""
    scoped_bases = {_base_name(n["label"]) for n in scoped.get("nodes", [])}
    in_scope = []
    for name, io in (jobs_io or {}).items():
        writes = (io or {}).get("writes", []) or []
        if any(_base_name(w) in scoped_bases for w in writes):
            in_scope.append(name)
    return in_scope


def build_migration_plan(lineage: dict, selected_ids, jobs_io: dict, destination: dict,
                         glue_scripts: dict | None = None) -> dict:
    """Classify Glue jobs and lay out what each target becomes.

    The E/L vs T split (per the migration plan):
      * ingestion jobs (E/L: external/S3/file → land in bronze) → Databricks notebooks
      * transformation jobs (T: warehouse table → warehouse table) → dbt models
      * publish jobs (gold → external warehouse via a connector) → set aside; on
        Databricks the gold layer is served directly, so these are usually obsolete.

    Transformation jobs are scoped to the selection (their write target must feed a
    selected table). Ingestion jobs are FOUNDATIONAL — they populate the bronze layer
    everything else builds on — so an ingestion/landing job is included when it feeds
    the selection OR when it brings external data in but its bronze target names can't
    be matched to the warehouse selection (landing names like ``call_expenses`` rarely
    equal the warehouse ``CALL_EXPENSE``). This is why a pure S3→bronze ingest still
    produces a notebook even if nothing it lands is named exactly like a picked table.

    ``glue_scripts`` (optional) lets the planner detect publish/reverse-ETL jobs from
    their connector usage; without it, such jobs fall through to the transform path.
    """
    lineage = lineage or {}
    glue_scripts = glue_scripts or {}
    scoped = upstream_subgraph(lineage, selected_ids or [])
    known_norms = {
        _norm(n["label"]) for n in lineage.get("nodes", [])
        if str(n.get("id", "")).startswith(("sf:", "glue:"))
    }
    scoped_bases = {_base_name(n["label"]) for n in scoped.get("nodes", [])}

    ingestion_jobs, transformation_jobs, publish_jobs = [], [], []
    for name, io in (jobs_io or {}).items():
        io = io or {}
        # A gold→external-warehouse load (Snowflake/JDBC) is a publish step, not a
        # lakehouse transform — set it aside instead of forcing it into dbt/bronze.
        if _is_warehouse_load_job(glue_scripts.get(name, "")):
            publish_jobs.append(name)
            continue
        writes = io.get("writes", []) or []
        table_writes = [w for w in writes if not _looks_like_path(w)]
        in_scope = any(_base_name(w) in scoped_bases for w in writes)
        kind = classify_job(io, known_norms)
        if kind == "ingestion":
            # Include when it feeds the selection, or when it's a foundational landing
            # job (external source, no warehouse-named target to scope it against).
            if in_scope or io.get("external") or not table_writes:
                ingestion_jobs.append(name)
        elif in_scope or not table_writes:
            # Transformation/load jobs feeding the selection. Loop-driven jobs that
            # write via f-string saveAsTable (e.g. silver→gold over many tables) expose
            # no scopable table target — include them too; they're foundational to the
            # medallion build the selection depends on.
            transformation_jobs.append(name)

    targets = []
    for node, layer in _migratable_nodes(scoped):
        targets.append({
            "id": node["id"],
            "system": node.get("system"),
            "source": node["label"],
            "type": node.get("type"),
            "layer": layer,
            "target": target_table_name(node["label"], layer, destination),
        })

    # Bronze tables = what the bronze ingest notebooks actually land: the named
    # outputs of the ingestion jobs, plus the RAW entities the lake catalogs at its
    # lowest curated layer (silver is built 1:1 from bronze, so its base names ARE
    # the bronze tables). Gold/mart catalog tables are deliberately EXCLUDED — they
    # are derived downstream by the transform jobs, not landed in bronze. (Listing
    # them as bronze sources is what made staging models point at tables that don't
    # exist.)
    bronze_tables = set()
    for name in ingestion_jobs:
        for w in (jobs_io.get(name, {}) or {}).get("writes", []) or []:
            if not _looks_like_path(w):
                bronze_tables.add(_base_name(w))
    for n in lineage.get("nodes", []):
        if str(n.get("id", "")).startswith("glue:") and _glue_layer(n["label"]) == "silver":
            bronze_tables.add(_base_name(n["label"]))

    return {
        "targets": targets,
        "ingestion_jobs": ingestion_jobs,
        "transformation_jobs": transformation_jobs,
        "publish_jobs": publish_jobs,
        "bronze_tables": sorted(b for b in bronze_tables if b),
    }


# ─── AI converters (with deterministic fallback) ─────────────────────────────

# Per-thread record of WHY the last _ai_text call fell back, so the scaffold a converter
# emits can state the true reason. A configured-but-throttled provider must NOT show
# "configure a provider" — that sends the user chasing the wrong fix. _map_concurrent runs
# each converter (and its scaffold builder) on one worker thread, so thread-local isolation
# is exactly right: the reason a thread records is the reason that thread's scaffold reads.
_AI_FAIL = threading.local()

# Substrings that mark a retryable, transient provider error (throttling / rate limiting /
# capacity). These warrant a backoff-and-retry; anything else (bad request, auth) is fatal
# and retrying just wastes time, so we fail fast and record the reason.
_RETRYABLE_HINTS = (
    "throttl", "rate exceed", "rate limit", "too many request", "429",
    "slow down", "serviceunavailable", "service unavailable", "503",
    "provisionedthroughput", "modelnotready", "capacity", "timeout", "timed out",
)
try:
    _AI_MAX_ATTEMPTS = max(1, int(os.environ.get("SFGLUE_AI_MAX_ATTEMPTS", "4")))
except ValueError:
    _AI_MAX_ATTEMPTS = 4


def _set_ai_failure(reason):
    _AI_FAIL.reason = reason


def _last_ai_failure_reason():
    """The reason the most recent _ai_text on THIS thread fell back, or None on success/
    no-provider. Read by scaffold builders immediately after the failed call."""
    return getattr(_AI_FAIL, "reason", None)


def _scaffold_reason_comment(call_ai):
    """The fallback-cause line a scaffold embeds. When no provider is wired the user really
    does need to configure one; when a provider IS wired but failed, surface the actual
    error (e.g. a ThrottlingException) so the fix is obvious and the review queue is honest."""
    if not call_ai:
        return "no AI provider is configured — configure a provider, then re-run conversion"
    reason = _last_ai_failure_reason()
    if reason:
        return f"AI translation failed (provider is configured): {reason} — retry conversion"
    return "no AI translation was applied — retry conversion or hand-translate"


def _ai_text(call_ai, prompt, system_prompt, task):
    """Call AI for a code string; return None on failure (caller falls back to a scaffold).

    Retries transient/throttling errors with exponential backoff + jitter — a burst of
    concurrent per-model calls routinely trips a provider's rate limit, and without retry
    every throttled model would silently degrade to an un-translated scaffold. On final
    failure the reason is recorded (thread-local) so the scaffold can state it."""
    _set_ai_failure(None)
    if not call_ai:
        return None
    last_exc = None
    for attempt in range(1, _AI_MAX_ATTEMPTS + 1):
        try:
            # 8000 (raised from 3000): a full notebook/dbt model can exceed 3000 output tokens,
            # which silently truncated the artifact at stop_reason=max_tokens.
            out = call_ai(prompt, system_prompt=system_prompt, max_tokens=8000, temperature=0, task=task)
            out = out if isinstance(out, str) else ""
            out = re.sub(r"^```[a-zA-Z]*\n?|```$", "", out.strip()).strip()  # strip code fences
            return out or None
        except Exception as exc:  # noqa: BLE001 — AI best-effort
            last_exc = exc
            retryable = any(h in str(exc).lower() for h in _RETRYABLE_HINTS)
            if retryable and attempt < _AI_MAX_ATTEMPTS:
                # Exponential backoff (1s, 2s, 4s, …) + jitter to de-correlate the burst so
                # the retries don't all land together and throttle again.
                delay = min(8.0, 2.0 ** (attempt - 1)) + random.uniform(0, 0.75)
                logger.warning("AI conversion throttled (%s), attempt %d/%d; retrying in %.1fs: %s",
                               task, attempt, _AI_MAX_ATTEMPTS, delay, exc)
                time.sleep(delay)
                continue
            break
    logger.warning("AI conversion failed (%s) after %d attempt(s); using scaffold: %s",
                   task, _AI_MAX_ATTEMPTS, last_exc)
    _set_ai_failure(str(last_exc) if last_exc else "unknown error")
    return None


# The "hard 20%" contract: a 80%-accurate translation is only safe if the model is
# REQUIRED to flag what it cannot translate instead of silently inventing logic. We
# instruct every converter to leave an inline `-- TODO[<TAG>]` (or `# TODO[<TAG>]` in a
# notebook) marker at the exact spot, which scan_untranslatable() below harvests into a
# review queue. A flagged gap is correct behaviour; a confident wrong guess is the
# failure mode we are preventing. (Mirrors the lineage-aware Glue→dbt conversion prompt.)
TODO_TAGS = ("IMPERATIVE", "GLUE-CONSTRUCT", "EXTERNAL", "NONDETERMINISTIC", "ASSUMPTION", "MISSING-SCHEMA")
_TODO_RULES = (
    " Do NOT invent columns or business logic. Use ONLY columns that appear in the schema "
    "context below (or in the source code). When you hit logic that has no clean set-based "
    "SQL equivalent, do NOT guess — leave an inline comment marker `-- TODO[<TAG>]: <short "
    "reason>` at the exact spot (`# TODO[<TAG>]:` inside a Python notebook), using one of these "
    "TAGs: IMPERATIVE (row-by-row Python with no SQL equivalent), GLUE-CONSTRUCT (DynamicFrame / "
    "resolveChoice / relationalize / job bookmarks), EXTERNAL (API / ML inference / arbitrary "
    "UDF calls), NONDETERMINISTIC (random or current-time-dependent branching), ASSUMPTION "
    "(ordering or schema you cannot confirm), MISSING-SCHEMA (a referenced column not in the "
    "provided schema). A flagged gap is correct; a confident wrong guess is not."
)
# Marks in deterministic (no-AI) scaffolds that mean "this was NOT translated, a human must
# act" — surfaced in the review queue under the NEEDS-AI tag so an un-converted passthrough
# never looks finished.
_SCAFFOLD_MARKERS = ("scaffold-reason:", "generated scaffold", "configure an AI provider", "hand-translate")
# Pulls the embedded fallback cause a scaffold states on its `SCAFFOLD-REASON:` line, so the
# review queue tells the user the TRUE reason (e.g. a throttling error) instead of a generic
# "no AI was applied".
_SCAFFOLD_REASON_RE = re.compile(r"(?:--|#)\s*SCAFFOLD-REASON:\s*(.+)")
_TODO_RE = re.compile(r"(?:--|#)\s*(?:TODO|FIXME)\s*(?:\[\s*([A-Za-z][A-Za-z-]*)\s*\])?\s*:?\s*(.*)")


def _columns_context(columns, *, label="Schema") -> str:
    """A one-line `name:type, ...` schema block to ground the model (so it can't guess
    columns). Accepts [{name,type}] dicts or plain name strings; empty → no block."""
    parts = []
    for c in columns or []:
        if isinstance(c, dict):
            name, typ = c.get("name"), c.get("type")
        else:
            name, typ = c, None
        if name:
            parts.append(f"{name}:{typ}" if typ else str(name))
    return f"\n\n{label} (use only these columns): {', '.join(parts)}." if parts else ""


def _bronze_columns_context(bronze_columns, *, tables=None) -> str:
    """Ground the model in the REAL columns of the raw source (bronze) tables it reads.

    ``bronze_columns`` is ``{table_base_lower: [colname|{name,type}, ...]}`` introspected
    live from the configured Databricks source location (so the AI uses the actual
    normalized column names instead of guessing them from the Glue script). ``tables``
    optionally narrows the block to the source tables in scope for one model; without it,
    every introspected table is listed. Returns an empty string when nothing is known, so
    the converter degrades to its prior behavior. Source-agnostic — names come only from
    introspection, never baked in."""
    cols = bronze_columns or {}
    if not cols:
        return ""
    if tables:
        wanted = {str(t).lower() for t in tables if t}
        cols = {k: v for k, v in cols.items() if str(k).lower() in wanted}
    blocks = []
    for name in sorted(cols):
        listed = _columns_context(cols[name], label=f"The source table `{name}` has EXACTLY these columns")
        if listed:
            # `_columns_context` ends with " (use only these columns): a, b, c." — append the
            # stronger no-guessing instruction so the model can't invent column names.
            blocks.append(listed.rstrip(".") + ". Use ONLY these names; do not assume any other column exists.")
    if not blocks:
        return ""
    # The legacy Glue script above often references the SOURCE SYSTEM's raw column names
    # (e.g. pre-landing names), but the table actually landed in the lake may have been
    # renamed/normalized. The introspected lists below are the GROUND TRUTH — they OVERRIDE
    # any column name seen in the script; map the script's logic onto these real names.
    preamble = ("\n\nCRITICAL — REAL SOURCE COLUMNS (introspected live; these OVERRIDE any column "
                "names in the Glue script above — if a script column isn't in this list, find its "
                "renamed equivalent here, never read a column that isn't listed):")
    return preamble + "".join(blocks)


def _unresolved_cte_columns(sql: str) -> list[str]:
    """Columns a CTE references that its (fully-known) upstream CTEs never output — the
    "renamed a column then referenced the old name" class (e.g. a `decoded` CTE emits
    `is_parent_call` from `is_parent_call_vod__c`, and a later CTE wrongly references
    `is_parent_call_vod__c`). Such SQL fails at build with UNRESOLVED_COLUMN and cascades
    'skipped' to every downstream model, so we catch it deterministically before shipping.

    CONSERVATIVE BY DESIGN — to avoid false positives it only checks a scope when EVERY
    source it reads is a sibling CTE with an explicit (non-star) projection. Any scope that
    reads a physical table or a `SELECT *` CTE has unknown columns and is skipped, so a
    legitimately-unknown raw/source column is never flagged. Best-effort: any parser/scope
    error returns [] (never blocks conversion). Returns de-duplicated `col` / `tbl.col`."""
    try:
        import sqlglot
        from sqlglot import expressions as _E
        from sqlglot.optimizer.scope import traverse_scope, Scope
    except Exception:  # noqa: BLE001 — sqlglot optional; lint is best-effort
        return []
    try:
        # The model is Jinja-templated dbt SQL; normalise the tags so sqlglot can parse it.
        # {{ config(...) }} is a STATEMENT (not a table) — drop it, or it leaves a stray token
        # before WITH and the parse fails. ref()/source() are table refs — swap each for a
        # placeholder identifier (its real columns are unknown anyway, so the scope check
        # treats it as an unknown source and skips it).
        bare = re.sub(r"{{\s*config\(.*?\)\s*}}", "", sql or "", flags=re.S)
        bare = re.sub(r"{{.*?}}", "_src_tbl", bare, flags=re.S)
        expr = sqlglot.parse_one(bare, read="databricks")
    except Exception:  # noqa: BLE001
        return []
    bad: list[str] = []
    try:
        for scope in traverse_scope(expr):
            known: set[str] = set()
            unknown_src = False
            for _name, (_node, src) in scope.selected_sources.items():
                if isinstance(src, Scope):
                    sel = src.expression.selects
                    if any(p.is_star for p in sel):
                        unknown_src = True
                    known.update(p.alias_or_name.lower() for p in sel
                                 if p.alias_or_name and not p.is_star)
                else:
                    unknown_src = True  # physical table → columns unknown
            if unknown_src or not known:
                continue
            src_names = {k.lower() for k in scope.selected_sources}
            for col in scope.columns:
                tbl = (col.table or "").lower()
                if tbl and tbl not in src_names:
                    continue  # qualified to a source outside this scope's knowledge
                nm = col.name.lower()
                if nm and nm not in known:
                    ref = f"{col.table}.{col.name}" if col.table else col.name
                    if ref not in bad:
                        bad.append(ref)
    except Exception:  # noqa: BLE001 — scope analysis is best-effort
        return []
    return bad


# ─── Demo bronze seeding ──────────────────────────────────────────────────────
# Build runs the silver models, which read {{ source('bronze', X) }}. In a fresh
# Databricks workspace those raw tables don't exist (the DDL deploy only makes the gold
# shells), so Build fails. For a demo/dev run we land a small, referentially-consistent
# sample dataset whose schema is DERIVED from the models themselves — so it always matches
# what the models read — and whose values are type/decode-aware so the casts and picklist
# decodes actually fire. (Production path remains the real Bronze ingestion notebook.)

_SEED_AUDIT = {"_source_file": "s3://raw/{t}/2024-01.csv", "_batch_id": "BATCH_2024_01",
               "_load_ts": "2024-01-15 02:00:00"}


def _bronze_reads_for_model(model_sql: str) -> dict:
    """{source_table: [raw cols]} a model reads from each ``{{ source('bronze', X) }}``.

    Handles BOTH shapes the converters emit: a ``SELECT * FROM source(...)`` CTE (capture
    the columns the outer query pulls from it) AND a direct read with explicit columns
    (``SELECT e.id, e.amount FROM source(...) e``). Scope-based so a column qualified to a
    bronze alias is captured wherever it appears; unqualified columns are attributed to the
    bronze source only when it is the scope's sole source (no ambiguity). Best-effort: {}
    on any failure."""
    try:
        import sqlglot
        from sqlglot import expressions as _E
        from sqlglot.optimizer.scope import traverse_scope, Scope
    except Exception:  # noqa: BLE001
        return {}
    try:
        sql = re.sub(r"{{\s*config\(.*?\)\s*}}", "", model_sql or "", flags=re.S)
        # Accept single OR double quotes in the Jinja tags (the AI emits either).
        sql = re.sub(r"""{{\s*source\(\s*['"]bronze['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*}}""", r"BRONZE__\1", sql)
        sql = re.sub(r"""{{\s*ref\(\s*['"]([^'"]+)['"]\s*\)\s*}}""", r"ref_\1", sql)
        expr = sqlglot.parse_one(sql, read="databricks")
    except Exception:  # noqa: BLE001
        return {}
    out: dict = {}

    def _add(base, name):
        nm = (name or "").lower()
        if nm and nm != "*" and nm not in out.setdefault(base, []):
            out[base].append(nm)

    try:
        for scope in traverse_scope(expr):
            direct, star_cte = {}, {}   # alias -> bronze base
            for _alias, (_node, src) in scope.selected_sources.items():
                if isinstance(src, _E.Table) and src.name.startswith("BRONZE__"):
                    direct[_alias] = src.name.replace("BRONZE__", "")
                    out.setdefault(direct[_alias], [])               # ensure the table is seeded even if no cols resolve
                elif isinstance(src, Scope) and any(p.is_star for p in src.expression.selects):
                    for t in src.expression.find_all(_E.Table):
                        if t.name.startswith("BRONZE__"):
                            star_cte[_alias] = t.name.replace("BRONZE__", "")
                            out.setdefault(star_cte[_alias], [])
            if not direct and not star_cte:
                continue
            # Unqualified columns are unambiguous only when this scope has exactly one source.
            sole = None
            if len(scope.selected_sources) == 1:
                sole = (list(direct.values()) + list(star_cte.values()))[0]
            for c in scope.columns:
                qual = c.table  # alias qualifier ('' if unqualified)
                if qual in direct:
                    _add(direct[qual], c.name)
                elif qual in star_cte:
                    _add(star_cte[qual], c.name)
                elif not qual and sole:
                    _add(sole, c.name)
    except Exception:  # noqa: BLE001
        return {}
    return out


def _seed_fk_target(col: str, tables: list) -> str | None:
    """Which source table a FK-looking column points at (so child rows link to real
    parents). Source-agnostic: strips a CRM custom-field marker (Salesforce-style trailing
    ``__c`` with an optional vendor token, e.g. ``_vod__c``), common FK suffixes
    (``_id``/``_key``/``_fk``/``_sk``), and trailing digits, then name-matches."""
    b = col.lower()
    b = re.sub(r"(_[a-z0-9]+)?__c$", "", b)   # …__c custom-field marker (vendor-agnostic)
    b = re.sub(r"_(id|key|fk|sk)$", "", b)     # common foreign-key suffixes
    b = b.rstrip("0123456789").strip("_")
    for t in tables:
        if t == col.lower():
            continue
        if b and (b == t or b == t.rstrip("s") or b + "s" == t):
            return t
    return None


def _seed_decode_literal(col: str, model_sql: str) -> str | None:
    """A real picklist literal the model decodes for this column, so the CASE fires."""
    for pat in (rf"case\s+{re.escape(col)}\s+when\s+'([^']+)'",
                rf"when\s+{re.escape(col)}\s*=\s*'([^']+)'"):
        m = re.search(pat, model_sql, re.I)
        if m:
            return m.group(1)
    return None


def _seed_value(table: str, col: str, i: int, model_sql: str, tables: list, pk: str):
    """A sample value for (table, col, row i): audit defaults, FK linkage (row i -> parent
    row i), picklist decode literals, and type-safe values for date/timestamp/numeric casts
    (detected by explicit cast OR column name, since the cast is often on a renamed alias)."""
    c = col.lower()
    if c in _SEED_AUDIT:
        return _SEED_AUDIT[c].format(t=table)
    fk = _seed_fk_target(col, tables)
    if fk:
        return f"{fk}_{i}"
    if c == "id":
        return pk
    lit = _seed_decode_literal(col, model_sql)
    if lit:
        return lit
    if re.search(rf"to_timestamp\(\s*{re.escape(col)}\b", model_sql, re.I) or "time" in c:
        return f"2024-0{(i % 9) + 1}-15 10:00:00"
    if re.search(rf"to_date\(\s*{re.escape(col)}\b", model_sql, re.I) or "date" in c:
        return f"2024-0{(i % 9) + 1}-15"
    if re.search(rf"{re.escape(col)}\b[^()]*?\)?\s*as\s+(?:int|bigint|decimal|number|double)", model_sql, re.I) \
            or any(k in c for k in ("amount", "priority", "order", "duration", "qty")):
        return str(100 + i) if "amount" in c else str(i)
    if c.startswith("is_") or c.startswith("has_") or c.endswith("_flag"):
        return "1" if i % 2 else "0"
    return f"{col}_{i}"


def build_bronze_seed_statements(models: dict, *, catalog: str, bronze_schema: str = "bronze",
                                 rows: int = 4) -> list[str]:
    """Deterministic CREATE+INSERT statements that land a referentially-consistent sample
    dataset into ``<catalog>.<bronze_schema>``, with columns derived from what the models
    actually read. Returns [] if no bronze reads could be derived (e.g. sqlglot missing)."""
    reads: dict = {}
    model_for: dict = {}
    for _name, sql in (models or {}).items():
        if not isinstance(sql, str):
            continue
        for tbl, cols in _bronze_reads_for_model(sql).items():
            reads.setdefault(tbl, [])
            for c in cols:
                if c not in reads[tbl]:
                    reads[tbl].append(c)
            model_for[tbl] = sql
    if not reads:
        return []
    tables = list(reads)
    stmts = [f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{bronze_schema}`"]
    for tbl, cols in reads.items():
        coldefs = ", ".join(f"`{c}` STRING" for c in cols)
        stmts.append(f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{bronze_schema}`.`{tbl}` ({coldefs}) USING DELTA")
        rowvals = []
        for i in range(1, rows + 1):
            pk = f"{tbl}_{i}"
            vals = []
            for c in cols:
                v = _seed_value(tbl, c, i, model_for[tbl], tables, pk)
                vals.append("NULL" if v is None else "'" + str(v).replace("'", "''") + "'")
            rowvals.append("(" + ", ".join(vals) + ")")
        collist = ", ".join(f"`{c}`" for c in cols)
        stmts.append(f"INSERT INTO `{catalog}`.`{bronze_schema}`.`{tbl}` ({collist}) VALUES\n  "
                     + ",\n  ".join(rowvals))
    return stmts


def scan_untranslatable(artifacts: dict) -> list[dict]:
    """Harvest the human review queue from the generated artifacts — every TODO/FIXME
    marker the converters left, plus any deterministic scaffold that was emitted without
    an AI translation. ``artifacts`` is {kind: {name: code}}.

    Returns a list of {artifact, kind, tag, line, detail} — the "nothing ships until this
    is empty" gate the migration kit calls for. Deterministic, so it works with or without
    AI and is unit-testable.
    """
    # Snowflake-only constructs the deterministic finaliser can't auto-translate to
    # Databricks — surface them for human review rather than ship invalid SQL.
    _residue = [
        (re.compile(r'\bLATERAL\s+FLATTEN\b|\bFLATTEN\s*\(', re.I), 'FLATTEN (use LATERAL VIEW explode / posexplode)'),
        (re.compile(r'\bPARSE_JSON\b|\bTO_VARIANT\b|\bTO_OBJECT\b|\bOBJECT_CONSTRUCT\b|\bARRAY_CONSTRUCT\b', re.I), 'Snowflake VARIANT/semi-structured function (use from_json/named_struct/array)'),
        (re.compile(r'\bLISTAGG\b|\bGROUP_CONCAT\b', re.I), 'LISTAGG/GROUP_CONCAT (use array_join(collect_list(...)))'),
        (re.compile(r'\bTRY_TO_(DATE|TIMESTAMP|NUMBER|DECIMAL)\b|\bTO_CHAR\b', re.I), 'residual Snowflake function not translated'),
    ]
    queue: list[dict] = []
    for kind, group in (artifacts or {}).items():
        if not isinstance(group, dict):
            continue
        for name, code in group.items():
            text = code if isinstance(code, str) else ""
            # A deterministic scaffold is wholesale un-translated, so flag it ONCE as
            # NEEDS-AI and don't also list the boilerplate TODOs it embeds (which would
            # make one un-converted artifact look like several distinct issues).
            if any(mk in text.lower() for mk in _SCAFFOLD_MARKERS):
                m = _SCAFFOLD_REASON_RE.search(text)
                detail = ("Emitted as a scaffold with the original code embedded — " +
                          (m.group(1).strip() if m
                           else "no AI translation was applied; translate or configure a provider."))
                queue.append({"artifact": name, "kind": kind, "tag": "NEEDS-AI", "line": 0,
                              "detail": detail})
                continue
            for i, ln in enumerate(text.splitlines(), start=1):
                for m in _TODO_RE.finditer(ln):  # capture every marker on the line, not just the first
                    tag = (m.group(1) or "TODO").upper()
                    detail = (m.group(2) or "").strip()
                    queue.append({"artifact": name, "kind": kind, "tag": tag,
                                  "line": i, "detail": detail or ln.strip()})
            # SQL dialect residue (skip PySpark/notebook artifacts).
            if 'notebook' not in str(kind).lower() and 'pyspark' not in str(kind).lower():
                for rx, why in _residue:
                    if rx.search(text):
                        queue.append({"artifact": name, "kind": kind, "tag": "DIALECT",
                                      "line": 0, "detail": why})
                # Column-resolution check: a reference to a column an upstream CTE renamed
                # away fails at build (UNRESOLVED_COLUMN) and cascades to every dependent
                # model — flag it here so it can't ship looking finished.
                for col in _unresolved_cte_columns(text):
                    queue.append({"artifact": name, "kind": kind, "tag": "UNRESOLVED-COLUMN",
                                  "line": 0, "detail": (
                                      f"`{col}` is referenced but no upstream CTE outputs it "
                                      "(likely a column that was renamed/derived earlier — use the "
                                      "renamed output name). This fails at build with UNRESOLVED_COLUMN.")})
    return queue


# ─── retry-before-queue (validate a conversion, retry with feedback, then queue) ──

_SFGLUE_RETRY_ROUNDS = 1  # extra AI rounds after the first attempt (cheap; Google retries 3×)


def _model_scan_flags(text: str, *, is_pyspark: bool = False) -> list[str]:
    """Build-breaking issues in one generated model (residual Snowflake dialect, an
    unresolved CTE column, a leftover TODO) — reuses the deterministic review-queue scanner
    so 'what a retry must fix' is exactly 'what would otherwise be queued'. Excludes the
    NEEDS-AI scaffold marker (only relevant to no-AI fallbacks, not to AI output)."""
    kind = "pyspark model" if is_pyspark else "dbt model"
    q = scan_untranslatable({kind: {"model": text}})
    return [f"{it['tag']}: {it['detail']}" for it in q if it.get("tag") != "NEEDS-AI"]


def _ai_model_with_retry(call_ai, prompt: str, system: str, task: str, *, is_pyspark: bool,
                         finalize=None) -> str | None:
    """Generate a model; if the output would land in the review queue (residual dialect,
    unresolved column, TODO), feed those SPECIFIC issues back and retry — up to
    ``_SFGLUE_RETRY_ROUNDS`` extra rounds — before giving up so the caller can queue a
    scaffold. This squeezes more clean auto-conversions out without lowering the bar (the
    validator is the same deterministic gate). Returns the cleanest attempt, or None if the
    AI produced nothing (→ caller emits its deterministic scaffold)."""
    best, best_flags, feedback = None, None, ""
    attempts = 1 + (_SFGLUE_RETRY_ROUNDS if call_ai else 0)
    for attempt in range(attempts):
        ai = _ai_text(call_ai, prompt + feedback, system, task)
        if not ai:
            break
        out = finalize(ai) if finalize else ai
        flags = _model_scan_flags(out, is_pyspark=is_pyspark)
        if not flags:
            return out  # clean — ship it, no queue item
        if best is None or len(flags) < len(best_flags):
            best, best_flags = out, flags
        if attempt + 1 < attempts:
            logger.info("sfglue retry: %d issue(s) remain, regenerating: %s", len(flags), flags[:3])
            feedback = ("\n\nYOUR PREVIOUS OUTPUT still has these issues that FAIL to build on "
                        "Databricks/dbt — fix ALL of them and return ONLY the corrected dbt model:\n"
                        + "\n".join(f"- {f}" for f in flags))
    return best


# ─── dbt schema.yml (model tests) ─────────────────────────────────────────────

def _model_alias_keys(model: str) -> set:
    """Lookup keys a model answers to: its own base, plus the bare name for a stg_ model
    (so a key declared on ``account`` resolves the ``stg_account`` staging model)."""
    low = model.lower()
    keys = {low}
    if low.startswith("stg_"):
        keys.add(low[4:])
    return keys


def generate_models_schema_yml(model_files, relationships, columns_by_model=None) -> str:
    """Build a dbt ``schema.yml`` of model tests + enforced contracts.

    Three best-practice guards, in one file:
      • **Key tests** — ``unique`` + ``not_null`` on a single-column grain, ``not_null``
        on foreign keys, and ``relationships`` to the parent model. These catch the wrong
        join / dropped-or-duplicated row an automated translation leaves behind.
      • **Grain-aware compound keys** — a multi-column grain gets ONE model-level
        ``dbt_utils.unique_combination_of_columns`` test (not a wrong per-column ``unique``,
        which would falsely assert each column is independently unique).
      • **Enforced contracts** — for every model whose full column list + Databricks types
        are known (``columns_by_model``), emit ``config.contract.enforced: true`` and a typed
        ``columns:`` block so dbt fails the build if the output schema drifts. Models with an
        AI-derived (unknown) output schema get tests only, never a half-declared contract.

    ``columns_by_model`` = ``{model_base_lower: [{"name", "data_type"}, ...]}`` (data_type
    already translated to Databricks). Returns a guiding stub when nothing can be emitted.
    """
    columns_by_model = columns_by_model or {}
    models = sorted({k[:-4] for k in (model_files or []) if isinstance(k, str) and k.endswith(".sql")})

    # A key declared on base name N resolves to model N or stg_N.
    model_by_key = {}
    for m in models:
        for k in _model_alias_keys(m):
            model_by_key.setdefault(k, m)

    pk_by_base, fk_by_base = {}, {}
    for rel in relationships or []:
        pk_b, fk_b = _base_name(rel.get("pk_table", "")), _base_name(rel.get("fk_table", ""))
        pk_cols = [c for c in (rel.get("pk_columns") or []) if c]
        fk_cols = [c for c in (rel.get("fk_columns") or []) if c]
        if pk_b and pk_cols:
            pk_by_base.setdefault(pk_b.lower(), pk_cols)
        if fk_b and fk_cols and pk_b:
            fk_by_base.setdefault(fk_b.lower(), []).append(
                {"columns": fk_cols, "to": pk_b, "field": (pk_cols or [fk_cols[0]])[0]})

    def _emit_rel(dst, col, rel_tests, indent):
        relt = rel_tests.get(col)
        if not relt:
            return
        dst.append(f"{indent}- relationships:")
        dst.append(f"{indent}    to: ref('{relt['to']}')")
        dst.append(f"{indent}    field: {relt['field']}")

    lines = ["version: 2", "", "models:"]
    emitted = 0
    for model in models:
        keys = _model_alias_keys(model)
        pks = next((pk_by_base[k] for k in keys if k in pk_by_base), [])
        fks = [fk for k in keys for fk in fk_by_base.get(k, [])]
        cols_meta = next((columns_by_model[k] for k in keys if k in columns_by_model), None)

        # Column-level tests. Single-col grain → unique+not_null; compound grain → not_null
        # on each part + a model-level combination test (below). FKs → not_null + relationships.
        col_tests: dict[str, list[str]] = {}
        rel_tests: dict[str, dict] = {}
        compound_pk = list(pks) if len(pks) > 1 else []
        if len(pks) == 1:
            col_tests.setdefault(pks[0], [])
            for t in ("unique", "not_null"):
                if t not in col_tests[pks[0]]:
                    col_tests[pks[0]].append(t)
        else:
            for c in compound_pk:
                col_tests.setdefault(c, [])
                if "not_null" not in col_tests[c]:
                    col_tests[c].append("not_null")
        for fk in fks:
            for col in fk["columns"]:
                col_tests.setdefault(col, [])
                if "not_null" not in col_tests[col]:
                    col_tests[col].append("not_null")
                ref_model = model_by_key.get(fk["to"].lower())
                if ref_model:  # only when the parent is in scope, else ref() dangles
                    rel_tests[col] = {"to": ref_model, "field": fk["field"]}

        has_contract = bool(cols_meta)
        if not has_contract and not col_tests and not compound_pk:
            continue
        emitted += 1
        lines.append(f"  - name: {model}")
        if has_contract:
            lines.append("    config:")
            lines.append("      contract:")
            lines.append("        enforced: true")
        if compound_pk:
            lines.append("    data_tests:")
            lines.append("      - dbt_utils.unique_combination_of_columns:")
            lines.append("          combination_of_columns: [" + ", ".join(compound_pk) + "]")

        if has_contract:
            # Contract enforcement requires EVERY output column declared with its type.
            lines.append("    columns:")
            for cm in cols_meta:
                cname = cm.get("name")
                if not cname:
                    continue
                lines.append(f"      - name: {cname}")
                dt = cm.get("data_type")
                if dt:
                    lines.append(f"        data_type: {dt}")
                tests = col_tests.get(cname, [])
                if tests or cname in rel_tests:
                    lines.append("        data_tests:")
                    for t in tests:
                        lines.append(f"          - {t}")
                    _emit_rel(lines, cname, rel_tests, "          ")
        elif col_tests:
            lines.append("    columns:")
            for col, tests in col_tests.items():
                lines.append(f"      - name: {col}")
                lines.append("        data_tests:")
                for t in tests:
                    lines.append(f"          - {t}")
                _emit_rel(lines, col, rel_tests, "          ")

    if not emitted:
        return ("version: 2\n\n"
                "models:\n"
                "  # No primary/foreign keys were declared in the source lineage, so no tests\n"
                "  # could be generated automatically. Add unique/not_null on each model's grain\n"
                "  # key and relationships() for foreign keys — those catch wrong joins and\n"
                "  # dropped/duplicated rows, the errors an automated translation leaves behind.\n")
    return "\n".join(lines) + "\n"


# ─── executable test specs (the runnable twin of schema.yml) ──────────────────

def build_test_specs(relationships, contracts=None) -> list[dict]:
    """Structured, executable test specs derived from the SAME declared keys + contracts as
    ``generate_models_schema_yml`` — so "what we run on the warehouse" equals "what the dbt
    tests assert". Each spec is one of:
      • ``{model, kind:'not_null', columns:[c]}``
      • ``{model, kind:'unique', columns:[c]}``            (single-column grain)
      • ``{model, kind:'unique_combo', columns:[...]}``    (compound grain)
      • ``{model, kind:'relationships', columns:[...], parent, parent_columns:[...]}``
      • ``{model, kind:'contract', expected_columns:[{name,data_type}]}``

    The ``/api/sfglue/run-tests`` route resolves each ``model`` to its deployed Databricks
    table and turns the spec into a violating-row query (0 rows = pass). ``contracts`` is the
    ``{model_lower: [{name,data_type}]}`` map (same as ``columns_by_model``).
    """
    contracts = contracts or {}
    specs: list[dict] = []
    pk_by_base, fk_by_base = {}, {}
    for rel in relationships or []:
        pk_b, fk_b = _base_name(rel.get("pk_table", "")), _base_name(rel.get("fk_table", ""))
        pk_cols = [c for c in (rel.get("pk_columns") or []) if c]
        fk_cols = [c for c in (rel.get("fk_columns") or []) if c]
        if pk_b and pk_cols:
            pk_by_base.setdefault(pk_b.lower(), (pk_b, pk_cols))
        if fk_b and fk_cols and pk_b:
            fk_by_base.setdefault(fk_b.lower(), []).append((fk_b, fk_cols, pk_b, pk_cols))
    for _low, (model, pks) in pk_by_base.items():
        if len(pks) == 1:
            specs.append({"model": model, "kind": "unique", "columns": pks})
            specs.append({"model": model, "kind": "not_null", "columns": pks})
        else:
            specs.append({"model": model, "kind": "unique_combo", "columns": list(pks)})
            for c in pks:
                specs.append({"model": model, "kind": "not_null", "columns": [c]})
    for _low, fks in fk_by_base.items():
        for (model, fk_cols, parent, parent_cols) in fks:
            for c in fk_cols:
                specs.append({"model": model, "kind": "not_null", "columns": [c]})
            specs.append({"model": model, "kind": "relationships", "columns": list(fk_cols),
                          "parent": parent, "parent_columns": list(parent_cols)})
    for model_low, cols in (contracts or {}).items():
        if cols:
            specs.append({"model": model_low, "kind": "contract", "expected_columns": list(cols)})
    return specs


# ─── dbt packages.yml (dbt_utils, for grain/combination tests) ────────────────

def generate_packages_yml() -> str:
    """dbt ``packages.yml`` — pulls in dbt_utils, which provides
    ``unique_combination_of_columns`` (used for compound-grain tests) and other helpers."""
    return (
        "packages:\n"
        "  - package: dbt-labs/dbt_utils\n"
        "    version: [\">=1.1.0\", \"<2.0.0\"]\n"
    )


# ─── governance / lineage / security / cost (checklist + config stubs) ─────────

def generate_governance_md(destination: dict, plan: dict | None = None) -> str:
    """A governance checklist + config stubs for the migrated pipeline (Unity Catalog
    lineage, secrets posture, dev/prod isolation, cost/perf guardrails).

    Grounded in the ACTUAL target (catalog + medallion schemas) and the secret scopes the
    generated bronze notebooks read (``aws_s3`` for S3, ``jdbc`` for external DB sources),
    so the operator applies concrete settings, not generic advice. This is dimension 4 of
    the best-practices gap analysis, which the tool can't verify automatically.
    """
    d = destination or {}
    plan = plan or {}
    catalog = d.get("catalog", "main")
    bronze = d.get("bronze_schema", "bronze")
    silver = d.get("silver_schema", "silver")
    gold = d.get("gold_schema", "gold")
    src_cat = d.get("source_catalog") or catalog
    src_sch = d.get("source_schema") or bronze
    n_bronze = len(plan.get("bronze_tables") or [])
    n_ingest = len(plan.get("ingestion_jobs") or [])
    return f"""# Governance, lineage, security & cost — checklist + config stubs

*Dimension 4 of the migration best-practices gap analysis. The tool wires the pipeline; these
controls are workspace policy the operator must apply. Target: catalog `{catalog}`
(bronze=`{bronze}`, silver=`{silver}`, gold=`{gold}`); raw landing `{src_cat}.{src_sch}`.*

## 1. Lineage (Unity Catalog — automatic, verify it)
- [ ] Confirm **column-level lineage** shows in Catalog Explorer for the gold models (UC captures
      it automatically for tables written through SQL/dbt on a UC-enabled warehouse).
- [ ] Tag the gold/mart tables so lineage is discoverable: `ALTER TABLE {catalog}.{gold}.<t> SET TAGS ('domain'='<domain>')`.
- [ ] Keep dbt `sources.yml`/`ref()` intact — dbt's own DAG is the model-level lineage of record.

## 2. Secrets (nothing hardcoded — the generated notebooks already use these scopes)
- [ ] Create secret scope **`aws_s3`** with `aws_access_key_id`, `aws_secret_access_key` (+ optional `aws_session_token`) — used by the {n_ingest} bronze ingestion notebook(s) for S3 reads.
- [ ] Create secret scope **`jdbc`** with `username`, `password` — used when a Glue job's logic/data comes from an external DB (JDBC) redirected to bronze.
- [ ] Grant `READ` on the scopes only to the job's service principal; never to all-users.
- [ ] Audit: `databricks secrets list-scopes` — confirm no credential literals in any notebook.

## 3. Dev / prod isolation (one catalog per environment)
- [ ] Use a catalog per environment: `dev_{catalog}` / `{catalog}` (prod). Point dbt targets at each.
- [ ] Grants: prod write only to the pipeline service principal; analysts get `SELECT` on `{gold}` only.

```yaml
# profiles.yml (dbt) — dev vs prod targets, same models, different catalog
sfglue:
  target: dev
  outputs:
    dev:  {{type: databricks, catalog: dev_{catalog}, schema: {silver}, threads: 4, http_path: "{{{{ env_var('DBX_HTTP_PATH') }}}}", token: "{{{{ env_var('DBX_TOKEN') }}}}"}}
    prod: {{type: databricks, catalog: {catalog},      schema: {silver}, threads: 8, http_path: "{{{{ env_var('DBX_HTTP_PATH') }}}}", token: "{{{{ env_var('DBX_TOKEN') }}}}"}}
```

## 4. Cost & performance guardrails
- [ ] **SQL Warehouse**: enable Serverless + **Auto-stop ≤ 10 min**; size to the smallest that meets SLA; enable result cache.
- [ ] **Bronze ingestion** ({n_bronze} table(s)): idempotent `overwrite`/`MERGE` (already generated) — schedule off-peak; use job clusters (auto-terminate), not all-purpose.
- [ ] **Delta layout**: `OPTIMIZE`/Z-ORDER the gold marts on their common filter/join keys; enable Predictive Optimization if available.
- [ ] **Photon** on for the SQL Warehouse (vectorized — cheaper per query on aggregations/joins).
- [ ] Set **budget alerts** and tag jobs/warehouses for cost attribution: tag key `cost_center`.

## 5. Data quality gates (already generated — wire into CI)
- [ ] `dbt deps && dbt build` runs the enforced **contracts** + key/grain/relationship **tests** (see schema.yml).
- [ ] Fill the **unit_tests.yml** expected rows from a golden fixture, then `dbt test`.
- [ ] Run **reconciliation** (Verify against source) as the final gate before promoting dev → prod.
"""


# ─── Postgres → Databricks bronze (JDBC ingestion) ────────────────────────────

def generate_postgres_bronze_ingestion(tables, destination: dict, *, secret_scope: str = "jdbc") -> str:
    """Deterministic Databricks notebook that lands Postgres tables into Delta **bronze** via
    Spark JDBC — so a table that originated in Postgres (and was shipped to Snowflake) is read
    straight from its LIVE source, not frozen from a Snowflake snapshot.

    ``tables`` = [{schema, name}, ...] (Postgres objects). Reads each via
    ``spark.read.format('jdbc')`` and writes it as its OWN Delta table into the bronze target
    (``source_catalog.source_schema`` — where the dbt ``source('bronze', …)`` refs resolve).
    Host/port/db are widgets; user/password come from Databricks secrets (nothing hardcoded).
    Idempotent ``overwrite`` — a re-run never duplicates rows. No AI needed (deterministic).
    """
    d = destination or {}
    catalog = d.get("source_catalog") or d.get("catalog") or "main"
    schema = d.get("source_schema") or d.get("bronze_schema") or "bronze"
    # Build (postgres_dbtable, bronze_target) pairs. Landing EVERY Postgres table can surface
    # same-named tables in different schemas (e.g. public.account + staging.account) that would
    # otherwise overwrite each other in bronze — so schema-qualify only the colliding base
    # names; unique ones keep their base name so dbt source('bronze', …) refs still resolve.
    from collections import Counter
    prelim = []
    for t in (tables or []):
        if not isinstance(t, dict):
            continue
        name = (t.get("name") or "").strip()
        if not name:
            continue
        src_schema = (t.get("schema") or "public").strip()
        prelim.append((src_schema, name, _base_name(name)))
    base_counts = Counter(base for _, _, base in prelim)
    rows = []
    for src_schema, name, base in prelim:
        target = base if base_counts[base] == 1 else f"{src_schema}_{base}"
        # dbtable: fully-qualified Postgres source; bronze target: base name (schema-qualified on collision).
        rows.append((f'{src_schema}.{name}', target))
    if not rows:
        return ("# No Postgres tables selected for ingestion.\n"
                "# Connect Postgres and pick the tables that originated there, then regenerate.\n")

    table_list_py = "TABLES = [\n" + "".join(
        f'    ("{dbtable}", "{target}"),\n' for dbtable, target in rows
    ) + "]\n"
    return (
        "# Databricks BRONZE ingestion — Postgres → Delta (JDBC), generated (deterministic).\n"
        "# Lands each Postgres table as its own Delta table in the bronze target so the\n"
        "# downstream dbt source('bronze', ...) / ref() reads it from the lakehouse. Postgres\n"
        "# stays the LIVE source (no Snowflake snapshot). Idempotent overwrite — safe to re-run.\n"
        "#\n"
        "# Requires the Postgres JDBC driver on the cluster (org.postgresql.Driver — included in\n"
        "# most Databricks runtimes; else add Maven coord org.postgresql:postgresql:42.7.3).\n\n"
        "# --- Connection parameters — widgets (never hardcode host/db) ---\n"
        'dbutils.widgets.text("PG_HOST", "")            # Postgres host\n'
        'dbutils.widgets.text("PG_PORT", "5432")        # Postgres port\n'
        'dbutils.widgets.text("PG_DATABASE", "")        # Postgres database\n'
        f'dbutils.widgets.text("JDBC_SECRET_SCOPE", "{secret_scope}")  # secret scope with username/password\n'
        f'dbutils.widgets.text("target_catalog", "{catalog}")\n'
        f'dbutils.widgets.text("target_schema", "{schema}")\n\n'
        'PG_HOST = dbutils.widgets.get("PG_HOST")\n'
        'PG_PORT = dbutils.widgets.get("PG_PORT") or "5432"\n'
        'PG_DATABASE = dbutils.widgets.get("PG_DATABASE")\n'
        'SCOPE = dbutils.widgets.get("JDBC_SECRET_SCOPE")\n'
        'target_catalog = dbutils.widgets.get("target_catalog")\n'
        'target_schema = dbutils.widgets.get("target_schema")\n\n'
        "# Credentials — read DIRECTLY from Databricks secrets (fail fast if missing).\n"
        'PG_USER = dbutils.secrets.get(SCOPE, "username")\n'
        'PG_PASSWORD = dbutils.secrets.get(SCOPE, "password")\n\n'
        'JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"\n'
        'JDBC_PROPS = {"user": PG_USER, "password": PG_PASSWORD, "driver": "org.postgresql.Driver"}\n\n'
        "spark.sql(f\"CREATE SCHEMA IF NOT EXISTS `{target_catalog}`.`{target_schema}`\")\n\n"
        "# (postgres_dbtable, bronze_target_table)\n"
        + table_list_py +
        "\nfor dbtable, target in TABLES:\n"
        "    df = spark.read.jdbc(url=JDBC_URL, table=dbtable, properties=JDBC_PROPS)\n"
        "    (df.write.format(\"delta\").mode(\"overwrite\").option(\"overwriteSchema\", \"true\")\n"
        "        .saveAsTable(f\"`{target_catalog}`.`{target_schema}`.`{target}`\"))\n"
        "    print(f\"bronze <- postgres: {dbtable} -> {target_catalog}.{target_schema}.{target}\")\n"
    )


# ─── dbt unit tests (pre-build logic validation) ──────────────────────────────

_REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
_SRC_RE = re.compile(r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")


def _is_passthrough_model(sql: str) -> bool:
    """A trivial ``select * from source(...)`` staging model carries no converted logic
    worth a unit test."""
    body = re.sub(r"\{\{.*?\}\}", "", sql or "", flags=re.S)
    body = re.sub(r"--.*", "", body)
    body = re.sub(r"\s+", " ", body).strip().lower()
    return body in ("select *", "select * from", "") or body.startswith("select * from")


def generate_unit_tests_yml(dbt_models) -> str:
    """Scaffold dbt **unit tests** for every model that carries converted (AI-derived) logic.

    Unit tests validate SQL logic on static given→expect rows BEFORE the model builds —
    the most direct guard that a converted CASE/DIVIDE/date-math/segmentation still returns
    the same values. We can auto-derive each model's INPUTS (its ``ref()``/``source()``
    dependencies) but not the business-correct OUTPUT, so ``given`` rows and ``expect`` rows
    are emitted as clearly-marked ``FILL_EXPECTED`` placeholders for the reviewer to complete
    (ideally from a golden/regression fixture). Passthrough staging models are skipped.

    Returns a single ``unit_tests.yml`` body, or a guiding stub when there's nothing to test.
    """
    logic_models = []
    for fname, sql in sorted((dbt_models or {}).items()):
        if not isinstance(fname, str) or not fname.endswith(".sql"):
            continue
        base = fname[:-4]
        if base.lower().startswith("stg_") or _is_passthrough_model(sql):
            continue
        refs = sorted(set(_REF_RE.findall(sql or "")))
        srcs = sorted(set(_SRC_RE.findall(sql or "")))  # [(source_name, table)]
        logic_models.append((base, refs, srcs))

    if not logic_models:
        return ("unit_tests:\n"
                "  # No models with converted business logic were produced (only passthrough\n"
                "  # staging), so no unit-test scaffolds were generated. When a model carries\n"
                "  # nontrivial logic (CASE/DIVIDE/date math/segmentation), add a unit test that\n"
                "  # feeds static input rows and asserts the expected output rows.\n")

    lines = ["unit_tests:"]
    for base, refs, srcs in logic_models:
        lines.append(f"  - name: test_{base}_logic")
        lines.append(f"    model: {base}")
        lines.append("    # FILL_EXPECTED: replace the placeholder rows below with a small golden")
        lines.append("    # fixture — representative inputs and the business-correct output — then")
        lines.append("    # run `dbt test --select test_" + base + "_logic` to prove the converted logic.")
        lines.append("    given:")
        inputs = [("ref", r) for r in refs] + [("source", f"{s[0]}, {s[1]}") for s in srcs]
        if not inputs:
            lines.append("      # (no ref()/source() detected — declare this model's inputs here)")
        for kind, ident in inputs:
            call = f"ref('{ident}')" if kind == "ref" else f"source('{ident.split(', ')[0]}', '{ident.split(', ')[1]}')"
            lines.append(f"      - input: {call}")
            lines.append("        rows:")
            lines.append("          - {}  # FILL_EXPECTED: one or more input rows")
        lines.append("    expect:")
        lines.append("      rows:")
        lines.append("        - {}  # FILL_EXPECTED: the expected output row(s) for the given inputs")
    return "\n".join(lines) + "\n"



# ─── Config-driven-pipeline detection + control-plane artifacts ───────────────
# Metadata-driven pipelines (e.g. the CDP flow) don't hardcode per-object logic —
# they loop over a control/config table (configuration_master + batch/ingestion
# logs) that lives in a relational store (RDS/Postgres) or the warehouse. Detecting
# this lets the migration reproduce the SAME config-driven design on Databricks.

_CONFIG_COL_SIGNALS = (
    # configuration_master (ingestion + medallion layout)
    "source_object_name", "curated_tablename", "publish_tablename", "configuration_id",
    "full_or_incremental_load", "parent_batch_id", "active_flag", "source_system",
    "pattern_name", "primary_key", "s3_landing_path", "s3_raw_path", "s3_curated_path",
    "s3_publish_path", "curated_database", "publish_database", "file_format",
    # query_configuration (SQL-driven transforms)
    "sql_query", "source_tablename", "target_tablename", "priority_column",
    # cdl_ds_snowflake_replicate (publish/replication)
    "truncate_load_flag", "partition_column", "data_load_flag", "reporting_cluster",
)
_CONFIG_TABLE_DENY = {"hadoopconfiguration", "configuration", "sparkconfiguration"}
_CONFIG_TABLE_RE = re.compile(
    r"\b(\w*(?:configuration|config_master|ingestion_log|file_process_log|"
    r"batch_process|control_table|metadata_table|_replicate|_manifest)\w*|"
    r"\w+_config|\w+_control)\b",
    re.I,
)


def _config_source_of(text: str) -> str:
    low = text.lower()
    if "jdbc:postgresql" in low or "postgres" in low or ("jdbc" in low and "5432" in low):
        return "postgres"
    if "net.snowflake" in low or 'format("snowflake")' in low or "format('snowflake')" in low:
        return "snowflake"
    if "jdbc" in low or "rds" in low or "get_secret_manager" in low:
        return "jdbc"
    return "unknown"


def detect_config_driven_pipeline(scripts) -> dict:
    """Detect whether the pipeline is metadata/config-table driven.

    ``scripts`` is a mapping ``{job_name: source}``. Returns a JSON-serializable
    summary: is_config_driven, config table names, the store the config is read
    from (postgres/snowflake/jdbc), the driver columns matched, and per-job
    evidence. Pure/deterministic — safe to unit test and call without an AI provider.
    """
    scripts = scripts or {}
    tables, columns, sources, evidence = set(), set(), set(), {}
    for name, script in scripts.items():
        text = script or ""
        low = text.lower()
        sig = []
        for t in _CONFIG_TABLE_RE.findall(text):
            tl = t.lower()
            # Drop obvious non-tables the broad regex can catch (Spark's
            # hadoopConfiguration, column/var names, query-string handles).
            if tl in _CONFIG_TABLE_DENY or tl.endswith(
                    ("_id", "_name", "_qry", "_query", "_available", "_df", "_sequence")):
                continue
            tables.add(tl); sig.append("table:" + tl)
        for c in _CONFIG_COL_SIGNALS:
            if c in low:
                columns.add(c); sig.append("col:" + c)
        if sig:
            sources.add(_config_source_of(text))
            evidence[name] = sorted(set(sig))
    is_cd = bool(tables) and len(columns) >= 2
    src = "unknown"
    for pref in ("postgres", "snowflake", "jdbc"):
        if pref in sources:
            src = pref
            break
    return {
        "is_config_driven": is_cd,
        "config_tables": sorted(tables),
        "config_source": src,
        "driver_columns": sorted(columns),
        "evidence": evidence,
    }


def generate_control_plane_artifacts(detection: dict, destination: dict = None) -> dict:
    """Control-plane artifacts for a config-driven migration -> {filename: content}.

    (1) Databricks DDL for the control schema (configuration_master + batch_log +
    ingestion_log), and (2) a config-read helper that keeps the config authoritative
    in Postgres/RDS and reads it over JDBC via widgets + secrets (swap the JDBC url
    from local Postgres to RDS later with no code change). {} when not config-driven.
    """
    if not detection or not detection.get("is_config_driven"):
        return {}
    d = destination or {}
    catalog = d.get("catalog", "lakehouse")
    control_schema = d.get("control_schema", "control")
    src = detection.get("config_source", "postgres")

    setup_sql = (
        "-- Control-plane setup (config-driven pipeline detected).\n"
        "-- Recreate the control plane so the migrated flow keeps its config-driven design.\n"
        "CREATE SCHEMA IF NOT EXISTS " + catalog + "." + control_schema + ";\n\n"
        "-- Option A (recommended while iterating): keep configuration_master authoritative\n"
        "-- in Postgres/RDS and read it over JDBC (see read_config helper).\n"
        "-- Option B: materialize a Databricks copy for offline runs:\n"
        "CREATE TABLE IF NOT EXISTS " + catalog + "." + control_schema + ".configuration_master (\n"
        "  source_system STRING, source_object_name STRING, landing_path STRING,\n"
        "  bronze_tablename STRING, curated_tablename STRING, publish_tablename STRING,\n"
        "  domain STRING, sub_domain STRING, primary_key STRING, parent_key STRING,\n"
        "  full_or_incremental_load STRING, active_flag STRING\n"
        ") USING DELTA;\n\n"
        "CREATE TABLE IF NOT EXISTS " + catalog + "." + control_schema + ".batch_log (\n"
        "  batch_id STRING, source_system STRING, status STRING,\n"
        "  started_at TIMESTAMP, completed_at TIMESTAMP, message STRING\n"
        ") USING DELTA;\n\n"
        "CREATE TABLE IF NOT EXISTS " + catalog + "." + control_schema + ".ingestion_log (\n"
        "  batch_id STRING, source_system STRING, source_object_name STRING, layer STRING,\n"
        "  row_count BIGINT, status STRING, logged_at TIMESTAMP, message STRING\n"
        ") USING DELTA;\n"
    )

    read_helper = (
        "# Read the pipeline config table (keeps it authoritative in Postgres/RDS).\n"
        "# The original pipeline read its config from " + src + ". This reads it over JDBC\n"
        "# via widgets + Databricks secrets — point config_jdbc_url at LOCAL Postgres now,\n"
        "# swap to RDS later with no code change (same secret scope).\n"
        'dbutils.widgets.text("config_secret_scope", "control_db")\n'
        'dbutils.widgets.text("config_jdbc_url", "jdbc:postgresql://localhost:5432/control")\n'
        'dbutils.widgets.text("config_table", "configuration_master")\n\n'
        'scope = dbutils.widgets.get("config_secret_scope")\n'
        'url = dbutils.widgets.get("config_jdbc_url")\n'
        'table = dbutils.widgets.get("config_table")\n\n'
        'config_df = (spark.read.format("jdbc")\n'
        '    .option("url", url)\n'
        '    .option("dbtable", table)\n'
        '    .option("user", dbutils.secrets.get(scope, "username"))\n'
        '    .option("password", dbutils.secrets.get(scope, "password"))\n'
        '    .option("driver", "org.postgresql.Driver")\n'
        '    .load()\n'
        '    .where("active_flag = \'A\'"))\n\n'
        "# Drive the pipeline from config rows (source_object_name -> curated/publish targets):\n"
        "config_rows = [r.asDict() for r in config_df.collect()]\n"
    )
    return {"control_setup.sql": setup_sql, "read_config.py": read_helper}



def convert_ingestion_job(call_ai, job_name: str, script: str, *, target_catalog: str,
                          target_schema: str, bronze_tables: list | None = None) -> str:
    """Glue ingestion job → Databricks bronze ingestion notebook.

    The migration REPOINTS the pipeline at the same source — it never reads or moves the
    data. So the generated notebook must (1) preserve the source read EXACTLY (same storage
    location, same file format incl. any per-file sheet/table selection, same file→table
    mapping), (2) reference that location via PARAMETERS (widgets), never a hardcoded path
    — mirroring the Glue job's own job parameters — and (3) repoint ONLY the write: one Delta
    table per source table into ``target_catalog.target_schema`` (the location the dbt
    ``source('bronze', …)`` refs resolve to), so bronze→silver→gold lines up.

    ``bronze_tables`` (optional) is the raw entity vocabulary from the plan — passed as a
    naming hint so the written table names match the dbt source() names exactly.
    """
    system = (
        "You convert an AWS Glue ingestion job into a Databricks BRONZE ingestion notebook. "
        "Output ONLY Python (no prose). Rules, in order:\n"
        "1. PRESERVE THE SOURCE READ. Keep the same storage location, the same file format, and "
        "any per-file sheet/table selection and file→table mapping the Glue job uses. Do NOT swap "
        "the reader: if it reads Excel (.xlsx) workbooks by sheet, read the SAME workbooks by the "
        "SAME sheet (pandas.read_excel via openpyxl — add `%pip install openpyxl` — then "
        "spark.createDataFrame). NEVER replace an Excel / boto3+pandas read with Auto Loader / "
        "cloudFiles / read_files() — those cannot read .xlsx or select a sheet.\n"
        "2. REFERENCE THE SOURCE VIA PARAMETERS, never hardcode it. Mirror the Glue job's job "
        "parameters as notebook widgets (dbutils.widgets.text(...) then .get(...)) — e.g. the "
        "bucket, the raw prefix, the batch id. No bucket/path literal may appear inline.\n"
        "3. REPOINT ONLY THE WRITE. Write each ingested logical table as its OWN Delta table via "
        "saveAsTable into the Databricks bronze location given below (one table per source table, "
        "using the job's own table/file/sheet mapping). Expose target catalog+schema as widgets "
        "defaulting to the values below. Do NOT write Parquet back to an S3 path. Make the write "
        "IDEMPOTENT — a re-run must NOT duplicate rows: use mode('overwrite') for a full reload "
        "(bronze = current copy of source) or MERGE on the natural key; NEVER plain append.\n"
        "4. Keep the lineage/audit columns the Glue job stamps (e.g. _source_file/_batch_id/_load_ts).\n"
        "5. NULL FIDELITY across the pandas→Spark bridge: convert missing/NaN to None BEFORE "
        "spark.createDataFrame (e.g. df = df.astype(object).where(df.notnull(), None)), so empty "
        "cells become SQL NULL, not the string 'nan' — otherwise null counts diverge from the source "
        "and the downstream trim/blank-to-null can't neutralize them.\n"
        "6. Do NOT emit CREATE CATALOG (it usually needs admin rights and the catalog already exists); "
        "CREATE SCHEMA IF NOT EXISTS at most.\n"
        "7. AWS CREDENTIALS for boto3 — emit RIGHT AFTER the widgets, before the first S3 read: add "
        "widgets SECRET_SCOPE (default 'aws_s3') and AWS_REGION (NO default region literal), then build "
        "_session = boto3.session.Session(...) with aws_access_key_id and aws_secret_access_key read "
        "DIRECTLY from dbutils.secrets.get(SECRET_SCOPE, ...) — do NOT wrap those two in try/except; they "
        "MUST raise immediately if the scope/keys are missing (fail fast). Set region_name ONLY when "
        "AWS_REGION is non-empty; OMIT it entirely when blank (never pass region_name=''). Add "
        "aws_session_token ONLY if present, guarded so an absent STS secret does not error (the ONLY "
        "guarded lookup). Use _session.client('s3') for every S3 call — never the bare boto3 client. Do "
        "NOT hardcode any key, secret, token, or region literal. Keep it runnable on a standard "
        "Databricks cluster.\n"
        "8. EXTERNAL DATABASE SOURCES (JDBC — Postgres/MySQL/Oracle/SQL Server/Redshift). If the Glue "
        "job reads its data OR its transformation logic/rules/config/lookups from an external database "
        "(any spark.read.format('jdbc'), .option('dbtable', …), a JDBC url, or a table holding "
        "mapping/rule rows the job applies), REDIRECT the migrated job to connect to that SAME external "
        "database and LAND it to bronze — do NOT inline, hardcode, or copy-freeze the external rows "
        "(they must stay authoritative at the source). Read with spark.read.format('jdbc') using "
        "url/dbtable/user/password/driver supplied via widgets + Databricks secrets: add widgets "
        "JDBC_SECRET_SCOPE (default 'jdbc'), JDBC_URL, JDBC_DBTABLE, JDBC_DRIVER; read the username via "
        "dbutils.secrets.get(JDBC_SECRET_SCOPE,'username') and the password via "
        "dbutils.secrets.get(JDBC_SECRET_SCOPE,'password') — NEVER a literal host, connection string, "
        "user, or password. Write each pulled external table as its OWN Delta table in the bronze "
        "target (idempotent overwrite/MERGE) so the downstream dbt source('bronze', …)/ref() reads it "
        "from the lakehouse. Preserve any incremental/watermark predicate as a widget. This keeps the "
        "external logic live at its source (no stale snapshot) while decoupling the transform onto the "
        "lakehouse." + _TODO_RULES
    )
    tables_hint = (("\nThe bronze tables in scope (write the ones this job ingests, using these exact "
                    "names so they match the dbt source() vocabulary): " + ", ".join(bronze_tables) + ".")
                   if bronze_tables else "")
    prompt = (
        f"Databricks bronze target: catalog `{target_catalog}`, schema `{target_schema}` — write "
        f"each table as `{target_catalog}.{target_schema}.<table>` (Delta).{tables_hint}\n"
        f"Original AWS Glue job '{job_name}':\n```python\n{script}\n```\n"
        "Produce the equivalent Databricks notebook: preserve the source read + parameters, "
        "repoint only the write."
    )
    ai = _ai_text(call_ai, prompt, system, "sfglue_migration")
    if ai:
        return ai
    # Deterministic fallback (no AI): a parameterized skeleton that still references the source
    # via widgets and targets the correct bronze location — clearly marked as a scaffold so the
    # review queue flags it for hand-translation rather than letting it look finished.
    tables_line = (("# Write targets — one Delta table per source file/sheet:\n"
                    + "".join(f"#   {target_catalog}.{target_schema}.{t}\n" for t in bronze_tables))
                   if bronze_tables else "")
    return (
        f"# Databricks bronze ingestion notebook — generated scaffold for Glue job '{job_name}'\n"
        f"# SCAFFOLD-REASON: {_scaffold_reason_comment(call_ai)}\n"
        f"# TARGET: Delta tables in {target_catalog}.{target_schema} (one per source table)\n"
        "# TODO[GLUE-CONSTRUCT]: hand-translate the source read from the original Glue job below. "
        "Preserve its file format (e.g. .xlsx read per sheet) and read the SAME location — but via "
        "the widgets below, never a hardcoded path. Write each table as Delta with saveAsTable; do "
        "NOT write Parquet back to S3.\n\n"
        "# Source location — PARAMETERS, never hardcoded (mirror the Glue job's --BUCKET/--RAW_PREFIX).\n"
        'dbutils.widgets.text("bucket", "")          # required: source bucket\n'
        'dbutils.widgets.text("raw_prefix", "raw")   # source prefix / folder\n'
        'dbutils.widgets.text("batch_id", "")        # audit batch id\n'
        f'dbutils.widgets.text("target_catalog", "{target_catalog}")\n'
        f'dbutils.widgets.text("target_schema", "{target_schema}")\n'
        'bucket = dbutils.widgets.get("bucket")\n'
        'raw_prefix = dbutils.widgets.get("raw_prefix")\n'
        'target_catalog = dbutils.widgets.get("target_catalog")\n'
        'target_schema = dbutils.widgets.get("target_schema")\n\n'
        '# --- AWS credentials for boto3 — from a Databricks secret scope; nothing hardcoded ---\n'
        'dbutils.widgets.text("SECRET_SCOPE", "aws_s3")   # secret scope holding the AWS creds\n'
        'dbutils.widgets.text("AWS_REGION", "")           # bucket region; leave blank to let boto3 resolve\n'
        'SECRET_SCOPE = dbutils.widgets.get("SECRET_SCOPE")\n'
        'AWS_REGION = dbutils.widgets.get("AWS_REGION")\n'
        'import boto3\n'
        '# Required creds — read DIRECTLY (no try/except): these MUST raise here if missing (fail fast).\n'
        '_aws_kw = {\n'
        '    "aws_access_key_id": dbutils.secrets.get(SECRET_SCOPE, "aws_access_key_id"),\n'
        '    "aws_secret_access_key": dbutils.secrets.get(SECRET_SCOPE, "aws_secret_access_key"),\n'
        '}\n'
        'if AWS_REGION:                       # set region_name ONLY when provided (never region_name="")\n'
        '    _aws_kw["region_name"] = AWS_REGION\n'
        'try:                                 # optional STS token — the ONLY guarded lookup\n'
        '    _aws_kw["aws_session_token"] = dbutils.secrets.get(SECRET_SCOPE, "aws_session_token")\n'
        'except Exception:\n'
        '    pass\n'
        '_session = boto3.session.Session(**_aws_kw)\n'
        '# Use _session.client("s3") for every S3 call (never the bare boto3 client).\n\n'
        + tables_line +
        "# 1) Extract — translate the Glue read below; keep the SAME format/sheet, reading from the\n"
        "#    PARAMETERIZED location (e.g. s3://{bucket}/{raw_prefix}/<file>.xlsx via pandas+openpyxl).\n"
        "#    Download via _session.client(\"s3\").download_file(...), then pandas.read_excel(sheet_name=...).\n"
        "#    Convert NaN->None before spark.createDataFrame so blank cells land as NULL, not 'nan'.\n"
        "# 2) Load — write each table as Delta (overwrite = idempotent re-runs; never plain append):\n"
        "#    df.write.mode('overwrite').saveAsTable(f'{target_catalog}.{target_schema}.<table>')\n\n"
        "# ─── original AWS Glue job for reference ───\n"
        + "".join(f"# {ln}\n" for ln in (script or "").splitlines())
    )


def _dbt_model(call_ai, name: str, sql_or_script: str, *, materialized: str, is_pyspark: bool,
               columns: list | None = None, bronze_columns: dict | None = None) -> str:
    kind = "AWS Glue PySpark transformation job" if is_pyspark else "Snowflake SQL/view"
    system = (
        f"You convert a {kind} into a Databricks dbt model. Output ONLY the dbt model SQL (no prose). "
        "Translate Snowflake SQL to Spark SQL: QUALIFY → window filter, FLATTEN/LATERAL → explode, "
        "VARIANT access → get_json_object/from_json, Snowflake date funcs → Spark equivalents, MERGE semantics "
        "→ incremental config. Reference upstream tables with {{ ref('...') }} or {{ source('bronze','...') }}. "
        f"Start the model with {{{{ config(materialized='{materialized}') }}}}."
        + ' CTE COLUMN DISCIPLINE: when a CTE renames or derives a column (e.g. `src_id AS entity_id`, or a raw picklist column `status_raw` decoded to `status`), every LATER CTE must reference the NEW output name and must NOT reference the original/raw column name or any column an upstream CTE did not select. Wrong: a `decoded` CTE outputs `status`, then a later CTE writes `(... OR cast(status_raw as int)=1)` — `status_raw` no longer exists and the build fails. Right: reference `status`.' + _TODO_RULES
    )
    prompt = (f"Model name: {name}\nSource logic:\n```\n{sql_or_script}\n```"
              f"{_columns_context(columns, label='Model output columns')}"
              f"{_bronze_columns_context(bronze_columns)}\nProduce the dbt model.")
    # Deterministically finalise Snowflake->Databricks dialect for SQL models (no-op for
    # PySpark). Retry-before-queue: a flagged output is regenerated with feedback before the
    # caller falls back to a scaffold.
    out = _ai_model_with_retry(call_ai, prompt, system, "sfglue_migration",
                               is_pyspark=is_pyspark,
                               finalize=None if is_pyspark else finalize_sfglue_model_sql)
    if out is not None:
        return out
    fence = "python" if is_pyspark else "sql"
    cmt = "#" if is_pyspark else "--"
    return (
        f"{cmt} dbt model '{name}' — generated scaffold ({'transformation job' if is_pyspark else 'Snowflake view'})\n"
        f"{cmt} SCAFFOLD-REASON: {_scaffold_reason_comment(call_ai)}\n"
        f"{cmt} TODO: translate Snowflake→Spark SQL dialect and replace refs with {{{{ ref()/source() }}}}.\n"
        f"{{{{ config(materialized='{materialized}') }}}}\n\n"
        "/* ─── original source logic for reference ───\n"
        f"```{fence}\n{sql_or_script}\n```\n*/\n"
    )


def convert_view_to_dbt(call_ai, view_full_name: str, view_sql: str, materialized: str = "view",
                        columns: list | None = None, bronze_columns: dict | None = None) -> str:
    return _dbt_model(call_ai, _base_name(view_full_name), view_sql, materialized=materialized,
                      is_pyspark=False, columns=columns, bronze_columns=bronze_columns)


def convert_transformation_job_to_dbt(call_ai, job_name: str, script: str) -> str:
    return _dbt_model(call_ai, job_name, script, materialized="table", is_pyspark=True)


def _parse_str_list(text) -> list:
    """Tolerantly pull a JSON array of strings out of an LLM reply — handles ```json
    fences, surrounding prose, and single-quoted arrays (some providers emit those)."""
    if not isinstance(text, str):
        return []
    t = re.sub(r"^```[a-zA-Z]*\n?|```$", "", text.strip()).strip()
    start, end = t.find("["), t.rfind("]")
    if start == -1 or end <= start:
        return []
    chunk = t[start:end + 1]
    for candidate in (chunk, chunk.replace("'", '"')):
        try:
            data = json.loads(candidate)
            return data if isinstance(data, list) else []
        except Exception:  # noqa: BLE001
            continue
    return []


def _enumerate_output_tables_via_ai(call_ai, job_name: str, script: str) -> list[str]:
    """Ask the model to list EVERY table a transform job writes. Complete (it reads the
    whole script, resolving loops/helpers a regex can't), so its result is safe to split
    on. Returns [] on any failure so the caller can fall back."""
    if not call_ai:
        return []
    try:
        prompt = (
            "List every output table this AWS Glue / PySpark job writes — count "
            "saveAsTable / insertInto / catalog sinks / connector writes / helper "
            f"functions, including those written inside loops or helpers. Job '{job_name}':\n"
            f"```python\n{(script or '')[:9000]}\n```\n"
            'Respond with ONLY a JSON array of lowercase table base names (no schema/db '
            'prefix), e.g. ["dim_entity","fact_event"].'
        )
        out = call_ai(prompt, system_prompt="You analyze ETL code and enumerate its output tables precisely.",
                      max_tokens=500, temperature=0, task="enumerate_outputs")
        data = _parse_str_list(out)
    except Exception as exc:  # noqa: BLE001 — AI best-effort
        logger.warning("Output-table enumeration failed for %s: %s", job_name, exc)
        return []
    seen: list[str] = []
    for t in data if isinstance(data, list) else []:
        base = str(t).split(".")[-1].strip().lower()
        if base and "{" not in base and base.isidentifier() and base not in seen:
            seen.append(base)
    return seen


def _dbt_model_for_table(call_ai, job_name: str, script: str, table: str, materialized: str,
                         available_refs: list | None = None, bronze_sources: list | None = None,
                         table_columns: list | None = None, bronze_columns: dict | None = None) -> str:
    """One dbt model reproducing just ``table`` out of a multi-output transform job."""
    system = (
        "You convert ONE table out of a multi-table AWS Glue PySpark job into a single Databricks dbt model. "
        "Output ONLY the dbt model SQL (no prose). Reproduce THAT table's logic exactly — renames, picklist "
        "decodes, type casts, window/dedup (use QUALIFY row_number() ... = 1), joins (same keys, LEFT where the "
        "source uses left), aggregations, DQ flags, and surrogate keys (abs(xxhash64(col))). "
        "NEVER read S3 paths or call saveAsTable. "
        f"Start the model with {{{{ config(materialized='{materialized}') }}}}."
        + ' CTE COLUMN DISCIPLINE: when a CTE renames or derives a column (e.g. `src_id AS entity_id`, or a raw picklist column `status_raw` decoded to `status`), every LATER CTE must reference the NEW output name and must NOT reference the original/raw column name or any column an upstream CTE did not select. Wrong: a `decoded` CTE outputs `status`, then a later CTE writes `(... OR cast(status_raw as int)=1)` — `status_raw` no longer exists and the build fails. Right: reference `status`.' + _TODO_RULES
    )
    # Hand the model the EXACT upstream vocabulary so it doesn't guess (which produced
    # broken refs — invented sources, reading bronze for silver-derived columns, etc.).
    refs = sorted({r for r in (available_refs or []) if r and r != table})
    srcs = sorted({s for s in (bronze_sources or []) if s})
    vocab = ""
    if refs:
        vocab += ("\n\nUpstream tables BUILT BY OTHER dbt models — reference each with "
                  "{{ ref('<name>') }} (these are produced by sibling models, NOT raw): "
                  + ", ".join(refs) + ".")
    if srcs:
        vocab += ("\nRAW landing tables — reference ONLY these with {{ source('bronze','<name>') }}: "
                  + ", ".join(srcs) + ".")
    vocab += (
        "\nRules: use {{ ref('X') }} for any table another model builds; use "
        "{{ source('bronze','X') }} ONLY for the raw landing tables listed above; NEVER invent any other "
        "source() name (e.g. no source('silver',...) / source('medaffairs_silver',...)). When a name is both a "
        "raw source and a built model, a silver/staging model reads its OWN raw input via source('bronze',...) "
        "and references other tables via ref(); a gold model reads ALL its inputs via ref(). "
        f"The target table `{table}` must NOT appear in its own FROM."
    )
    # Ground the model in the REAL columns of the raw sources it may read (the bronze
    # tables listed above), so it uses the actual landed names instead of guessing from
    # the Glue script. Limited to the bronze sources in scope for this job.
    prompt = (f"AWS Glue job '{job_name}'. Produce the dbt model for ONLY the `{table}` table:\n"
              f"```python\n{script}\n```{vocab}"
              f"{_columns_context(table_columns, label=f'`{table}` output columns')}"
              # Ground on the bronze sources this job reads. The caller passes bronze_columns
              # ONLY for jobs that actually read bronze (silver-build) — gold/fact jobs read
              # ref()'d silver outputs, so they get no bronze_columns and aren't (wrongly)
              # told silver-produced columns are "missing from bronze".
              f"{_bronze_columns_context(bronze_columns, tables=srcs)}")
    # This output is dbt SQL (not PySpark), so finalise the dialect and validate/retry it
    # the same way single-model conversions do, before falling back to a scaffold.
    out = _ai_model_with_retry(call_ai, prompt, system, "sfglue_migration",
                               is_pyspark=False, finalize=finalize_sfglue_model_sql)
    if out is not None:
        return out
    return (
        f"-- dbt model for '{table}' — decomposed from transformation job '{job_name}'\n"
        f"-- SCAFFOLD-REASON: {_scaffold_reason_comment(call_ai)}\n"
        f"-- TODO: translate the `{table}` logic below into Spark SQL, replacing reads with\n"
        f"--       {{{{ ref()/source() }}}}.\n"
        f"{{{{ config(materialized='{materialized}') }}}}\n\n"
        f"/* ─── original transformation job (build the `{table}` table from this) ───\n"
        f"```python\n{script}\n```\n*/\n"
    )



# ─── Procedural transform detection + PySpark porting ─────────────────────────
# Some transformation jobs are NOT expressible as declarative dbt SQL: they read
# Excel/zip, loop over a config table building frames, call UDFs/RDD ops, or drive
# dynamic per-source dispatch. Flattening those into dbt SQL loses logic. For them
# the faithful target is a Databricks PySpark model/notebook that ports the actual
# Spark logic 1:1; only genuinely relational transforms become dbt SQL.

_PYSPARK_TRANSFORM_SIGNALS = (
    re.compile(r"\b(pandas|openpyxl|read_excel|\.xlsx)\b", re.I),          # spreadsheet reads
    re.compile(r"\b(zipfile|tarfile|gzip|BytesIO|deflate64)\b", re.I),      # (de)compression
    re.compile(r"\bboto3\b|from_options|getSink|DynamicFrame|resolveChoice|relationalize", re.I),  # Glue/S3 object ops
    re.compile(r"\b(ThreadPool|multiprocessing|Pool)\b"),                  # concurrency
    re.compile(r"\.rdd\b|mapPartitions|flatMap|\bpandas_udf\b|\budf\s*\(|F\.udf\b"),  # UDF / RDD
    re.compile(r"http\.client|requests\.|urllib|\.api\b"),                # external API calls
    re.compile(r"createDataFrame\s*\("),                                   # frames from python structures
    re.compile(r"exec\s*\(|eval\s*\(|getattr\s*\([^)]*,\s*[\'\"]"),   # dynamic dispatch
)
# A Python for-loop that also writes/unions frames = imperative frame-building.
_LOOP_BUILD = re.compile(r"for\s+\w+\s+in\b[\s\S]{0,600}?(\.union\(|saveAsTable|\.write\b|insertInto)", re.I)


def transform_needs_pyspark(script: str) -> bool:
    """True when a transformation job uses constructs dbt SQL cannot faithfully
    express, so it should be ported as a PySpark model instead of flattened to SQL.
    Conservative: only fires on clearly non-relational signals; a plain
    table-to-table SQL transform still routes to dbt.
    """
    text = script or ""
    if not text.strip():
        return False
    if any(rx.search(text) for rx in _PYSPARK_TRANSFORM_SIGNALS):
        return True
    return bool(_LOOP_BUILD.search(text))


def convert_transformation_job_to_pyspark(call_ai, job_name: str, script: str, *,
                                          target_catalog: str, target_schema: str,
                                          output_tables: list | None = None,
                                          available_refs: list | None = None,
                                          bronze_sources: list | None = None) -> str:
    """Procedural Glue transform job → Databricks PySpark notebook (logic-preserving).

    Used when the job cannot be faithfully expressed as dbt SQL. Ports ALL the Spark
    logic, reads upstream tables from the lakehouse (bronze source() vocab / sibling
    refs), and writes each output as Delta into ``target_catalog.target_schema``.
    Mirrors the ingestion converter's contract: preserve logic, parameterize via
    widgets, idempotent writes, keep audit columns, flag anything ambiguous with TODO.
    """
    refs_hint = (("\nUpstream tables you may read (bronze raw + sibling models), read via "
                  "spark.table('<catalog>.<schema>.<name>'): " + ", ".join(
                      sorted(set((bronze_sources or []) + (available_refs or []))))) 
                 if (bronze_sources or available_refs) else "")
    outs_hint = (("\nOutput tables this job builds (write each as its OWN Delta table): "
                  + ", ".join(output_tables)) if output_tables else "")
    system = (
        "You convert an AWS Glue TRANSFORMATION job into a Databricks PySpark notebook, "
        "used precisely because this job is NOT expressible as declarative dbt SQL. "
        "Output ONLY Python (no prose). Rules, in order:\n"
        "1. PRESERVE ALL LOGIC. Port every transformation the Glue job performs — loops, "
        "per-source/config-driven branches, Excel/zip handling, joins, filters, casts, "
        "dedup, business rules, null handling — faithfully. Do NOT drop, simplify, or "
        "approximate steps. Keep the same output tables and columns.\n"
        "2. READ UPSTREAM FROM THE LAKEHOUSE. Replace the Glue job's warehouse/catalog "
        "reads with spark.table('<catalog>.<schema>.<table>') against the bronze/silver "
        "tables given below (parameterize catalog/schema via widgets). If it reads Excel/"
        "zip/S3 objects or an external DB for rules/config, KEEP that read live at its "
        "source (same location/format), referenced via widgets + Databricks secrets — "
        "never hardcode a path, host, key, or copy-freeze external rows.\n"
        "3. WRITE EACH OUTPUT AS DELTA via saveAsTable into the target catalog.schema "
        "below (one table per output). Make writes IDEMPOTENT (overwrite for full, MERGE "
        "on the natural key for incremental) — NEVER plain append. Keep audit columns.\n"
        "4. PARAMETERIZE via dbutils.widgets (catalog, schema, batch_id, and any bucket/"
        "prefix the job used) — no path/bucket/credential literal inline.\n"
        "5. Do NOT emit CREATE CATALOG; CREATE SCHEMA IF NOT EXISTS at most.\n"
        "6. This is a dbt-python-compatible notebook: it may be run standalone or wrapped "
        "as a dbt python model. Keep it runnable on a standard Databricks cluster." + _TODO_RULES
    )
    prompt = (
        f"Databricks target: catalog `{target_catalog}`, schema `{target_schema}` — write "
        f"each output as `{target_catalog}.{target_schema}.<table>` (Delta).{refs_hint}{outs_hint}\n"
        f"Original AWS Glue transformation job '{job_name}':\n```python\n{script}\n```\n"
        "Produce the equivalent Databricks PySpark notebook: preserve ALL logic, read "
        "upstream from the lakehouse, repoint only the writes."
    )
    ai = _ai_text(call_ai, prompt, system, "sfglue_migration")
    if ai:
        return ai
    # Deterministic scaffold (no AI) — clearly marked so the review queue flags it for
    # hand-translation rather than looking finished.
    outs_line = "".join(f"#   {target_catalog}.{target_schema}.{t}\n" for t in (output_tables or []))
    return (
        f"# Databricks PySpark transform notebook — scaffold for Glue job '{job_name}'\n"
        f"# SCAFFOLD-REASON: {_scaffold_reason_comment(call_ai)}\n"
        "# TODO[IMPERATIVE]: hand-port the procedural transform logic from the Glue job "
        "below. This job was routed to PySpark (not dbt SQL) because it uses constructs "
        "SQL cannot express (e.g. Excel/zip reads, config loops, UDFs).\n"
        f"# Output Delta tables:\n{outs_line}\n"
        'dbutils.widgets.text("catalog", "%s")\n'
        'dbutils.widgets.text("schema", "%s")\n'
        'dbutils.widgets.text("batch_id", "")\n'
        % (target_catalog, target_schema)
    )


def convert_transformation_job_to_dbt_models(call_ai, job_name: str, script: str, materialized: str = "table",
                                             output_tables: list | None = None,
                                             available_refs: list | None = None,
                                             bronze_sources: list | None = None,
                                             output_columns: dict | None = None,
                                             bronze_columns: dict | None = None) -> dict:
    """Decompose a (possibly multi-output) transformation job into dbt models — one per
    output table — returning {filename: sql}.

    dbt is one-model-per-table, but Glue/Spark transform jobs routinely build many
    tables in one script. To split reliably we need a COMPLETE list of the job's
    outputs. We source that, most-reliable first:
      1. ``output_tables`` supplied by the caller — e.g. the medallion-layer tables the
         catalog says this job produces. The catalog is authoritative and complete, so
         this doesn't depend on parsing a monolithic script or on AI enumeration.
      2. AI enumeration of the script (reads loops/helpers a regex can't), floored with
         the static ``saveAsTable``/``write_*`` scan.
    If we get >=2 outputs we emit one model per table (AI translates each); otherwise we
    keep a single job-level model (embedding the whole job) so nothing is dropped. Fully
    source-agnostic — no pipeline-specific table names anywhere.
    """
    catalog_tables = [t for t in (output_tables or []) if t and isinstance(t, str)]

    if call_ai:
        tables = list(dict.fromkeys(catalog_tables))  # prefer the catalog's complete list
        if len(tables) < 2:                            # fall back to in-script enumeration
            tables = _enumerate_output_tables_via_ai(call_ai, job_name, script)
            if tables:
                for t in detect_job_output_tables(script):
                    if t not in tables:
                        tables.append(t)
        if len(tables) >= 2:
            # Every table this job builds is a valid {{ ref() }} for its siblings; add
            # the caller-provided refs (e.g. tables from OTHER transform jobs) too.
            refs = list(dict.fromkeys(tables + [r for r in (available_refs or []) if r]))
            cols_by_base = {str(k).lower(): v for k, v in (output_columns or {}).items()}
            sqls = _map_concurrent(
                tables,
                lambda t: _dbt_model_for_table(call_ai, job_name, script, t, materialized,
                                               available_refs=refs, bronze_sources=bronze_sources,
                                               table_columns=cols_by_base.get(str(t).lower()),
                                               bronze_columns=bronze_columns),
            )
            return {f"{t}.sql": sql for t, sql in zip(tables, sqls)}
        return {f"{job_name}.sql": convert_transformation_job_to_dbt(call_ai, job_name, script)}

    # No AI: keep one model with the whole job (don't hand-translate), but flag the
    # multi-output so it's clear the job must be split once an AI provider is configured.
    single = convert_transformation_job_to_dbt(call_ai, job_name, script)
    detected = list(dict.fromkeys(catalog_tables)) or detect_job_output_tables(script)
    if len(detected) >= 2:
        single = (
            f"-- NOTE: this job builds multiple tables ({', '.join(detected)}). dbt is one\n"
            "-- model per table — configure an AI provider to auto-split into per-table models,\n"
            "-- or split this manually (one model each).\n" + single
        )
    return {f"{job_name}.sql": single}


def publish_job_note(job_name: str, script: str) -> str:
    """Markdown note for a gold→warehouse publish job that's usually obsolete on
    Databricks (gold is served directly). Keeps the original script for reference in
    case true reverse-ETL back to the warehouse is still required."""
    return (
        f"# `{job_name}` — publish / reverse-ETL step (review before keeping)\n\n"
        "This Glue job loads the **gold** layer into an external warehouse "
        "(Snowflake / JDBC). On Databricks the gold tables are served **directly** from "
        "Unity Catalog, so this step is normally **obsolete** — drop it unless you must "
        "keep syncing data back to the warehouse (true reverse-ETL).\n\n"
        "If you do need it, re-implement it as a scheduled Databricks job that reads the "
        "migrated gold tables and writes out via the Snowflake connector / Lakehouse "
        "Federation, rather than re-running this Glue script.\n\n"
        "Original Glue job, for reference:\n\n```python\n" + (script or "") + "\n```\n"
    )


def staging_model_for_table(table_full_name: str) -> str:
    """A simple dbt staging model that reads the table's bronze source."""
    base = _base_name(table_full_name)
    return (
        f"-- staging model for {table_full_name}\n"
        "{{ config(materialized='view') }}\n\n"
        f"select * from {{{{ source('bronze', '{base}') }}}}\n"
    )


def explain_artifact(call_ai, name: str, code: str, kind: str = "code") -> dict:
    """Plain-English explanation of a generated artifact (notebook / dbt model / DDL).

    Returns {text, ai_used}.
    """
    if not call_ai:
        return {"text": "AI provider not configured — set one in Settings to get explanations.", "ai_used": False}
    try:
        prompt = (
            f"Explain this {kind} ('{name}') in plain English for a data engineer reviewing a migration: "
            "what it does, its inputs and outputs, and anything to double-check before running it. Be concise.\n\n"
            f"```\n{(code or '')[:6000]}\n```"
        )
        text = call_ai(prompt, system_prompt="You explain data-pipeline code clearly and concisely.",
                       max_tokens=1200, task="explain", temperature=0)
        return {"text": text.strip() if isinstance(text, str) else "", "ai_used": True}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Artifact explanation failed: %s", exc)
        return {"text": f"Explanation failed: {exc}", "ai_used": False}


def _parse_grade_object(text):
    """Tolerantly pull one JSON object out of an LLM reply (fence → whole → first {…})."""
    if not isinstance(text, str):
        return None
    candidates = []
    m = re.search(r"```(?:json)?\s*([\s\S]*?)```", text, re.IGNORECASE)
    if m:
        candidates.append(m.group(1).strip())
    candidates.append(text.strip())
    start = text.find("{")
    if start != -1:
        depth, in_str, esc = 0, False, False
        for i in range(start, len(text)):
            ch = text[i]
            if in_str:
                esc = (ch == "\\" and not esc)
                if ch == '"' and not esc:
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidates.append(text[start:i + 1])
                    break
    for candidate in candidates:
        try:
            obj = json.loads(candidate)
        except Exception:  # noqa: BLE001
            continue
        if isinstance(obj, dict):
            return obj
    return None


def grade_migration_fidelity(call_ai, *, original, converted, dialect="databricks",
                             max_chars_each=70000, max_prompt_chars=160000):
    """Grade how faithfully the converted Databricks/dbt artifacts implement the
    original Snowflake views/tables + Glue scripts + declared relationships.

    ``original``/``converted`` are pre-assembled text blobs. Returns
    {overall, dimensions:[{name,score,note}], summary, ai_used}.

    ``max_chars_each`` caps each blob (source / converted) BEFORE the prompt is built,
    and ``max_prompt_chars`` is forwarded to the AI client so the full grading prompt is
    NOT silently clipped by the global 35 000-char default. These matter: a real migration
    is ~20 models + 2 multi-thousand-line Glue jobs, which blows past a 16 000-char slice.
    Clipping ``converted`` mid-model made the grader believe most models were MISSING and
    that the last visible model was "truncated / non-runnable" — flooring completeness even
    on a faithful, complete conversion. If a blob still exceeds its cap we say so explicitly
    so the grader treats the cut as "not shown", never as "missing / broken".
    """
    if not call_ai:
        return {"overall": 0, "dimensions": [], "summary": "AI provider not configured.", "ai_used": False}

    def _clip(text, cap, what):
        text = text or ""
        if len(text) <= cap:
            return text or "(none)"
        return (text[:cap] +
                f"\n\n[NOTE: {what} was long and only the first {cap:,} of {len(text):,} "
                "characters are shown here. Everything above is shown IN FULL — do NOT treat "
                "the cut-off tail as missing or truncated logic; grade only what is visible.]")

    system_prompt = (
        "You are a meticulous data-migration reviewer. Grade how faithfully the CONVERTED "
        f"{dialect}/dbt artifacts reproduce the ORIGINAL source (Snowflake views/tables + AWS "
        "Glue job logic + declared relationships + the stated business logic) — judge whether "
        "the migrated pipeline would produce the same data, not its style.\n"
        "Grade against the source material that is ACTUALLY provided. A source may legitimately "
        "have NO Snowflake views (all transformation logic lives in the Glue jobs) or NO Glue "
        "jobs (all logic in views). When an artifact TYPE is absent, do NOT penalize for it and "
        "do NOT ask for more context — grade completeness by whether every PROVIDED source "
        "transformation (each Glue job's catalog outputs, each view) and every in-scope source "
        "table is represented among the converted artifacts.\n"
        "Score four dimensions 0-100:\n"
        "- completeness: every provided source transformation/output and in-scope table is "
        "represented in the converted artifacts.\n"
        "- correctness: transformations, filters, calculations, joins and SQL semantics are "
        "faithful (Snowflake/Glue→Databricks dialect differences handled correctly).\n"
        "- joins_grain: joins, keys, and row grain match the source (declared relationships "
        "respected; no fan-out / dropped rows).\n"
        "- idiomatic: valid, runnable Databricks SQL / dbt / PySpark.\n"
        "Be critical about ACTUAL defects — a genuinely missing output, a wrong join, or invalid "
        "SQL must lower the score. But do NOT lower a score merely because a source artifact type "
        "is absent, because you would prefer more context, or because a long blob was cut off for "
        "display: grade what the provided artifacts demonstrably do. The overall is a weighted "
        "view (completeness and correctness matter most).\n\n"
        "Respond with JSON ONLY, no commentary, in exactly this shape:\n"
        '{"overall": <0-100>, "dimensions": [{"name": "completeness", "score": <0-100>, '
        '"note": "<short>"}, ...4 total], "summary": "<one or two sentences>"}'
    )
    prompt = (
        f"--- ORIGINAL source (Snowflake views/tables + Glue scripts + relationships) ---\n"
        f"{_clip(original, max_chars_each, 'the source')}\n\n"
        f"--- CONVERTED {dialect} artifacts (dbt models / notebooks / DDL) ---\n"
        f"{_clip(converted, max_chars_each, 'the converted artifact set')}\n\n"
        "Grade the converted artifacts against the original and return the JSON."
    )
    try:
        raw = call_ai(prompt, system_prompt=system_prompt, max_tokens=1500, task="grade",
                      temperature=0, max_prompt_chars=max_prompt_chars)
    except Exception as exc:  # noqa: BLE001
        logger.warning("sfglue grade failed: %s", exc)
        return {"overall": 0, "dimensions": [], "summary": f"Grading failed: {exc}", "ai_used": False}

    parsed = _parse_grade_object(raw)
    if not parsed or "overall" not in parsed:
        return {"overall": 0, "dimensions": [], "summary": "Could not grade the migration.", "ai_used": False}

    def _clamp(v):
        try:
            return max(0, min(100, int(round(float(v)))))
        except (TypeError, ValueError):
            return 0

    dims = []
    for dim in (parsed.get("dimensions") or [])[:4]:
        if isinstance(dim, dict):
            dims.append({
                "name": str(dim.get("name") or "dimension").strip(),
                "score": _clamp(dim.get("score", 0)),
                "note": str(dim.get("note") or "").strip()[:200],
            })
    return {
        "overall": _clamp(parsed.get("overall", 0)),
        "dimensions": dims,
        "summary": str(parsed.get("summary") or "").strip()[:600],
        "ai_used": True,
    }


# ─── orchestrate the full conversion ─────────────────────────────────────────

def run_conversion(call_ai, lineage, selected_ids, *, jobs_io, glue_scripts,
                   snowflake_ddl, snowflake_columns, destination, relationships=None,
                   bronze_columns=None) -> dict:
    """Build the plan and produce all conversion artifacts for the scoped selection.

    Returns {plan, notebooks, dbt_models, ddl, notes, sources_yml} — each a {name:
    code} map. Ingestion jobs → bronze notebooks; transformation jobs + Snowflake views
    → dbt models; Snowflake tables → Databricks DDL (in their resolved layer — gold for
    dimensional/mart names) plus a bronze-reading staging model ONLY when the table is
    genuinely landed in bronze; publish/reverse-ETL jobs → notes (obsolete on
    Databricks). sources_yml declares the real bronze raw entities. Declared foreign
    keys are carried forward as FOREIGN KEY constraints in the table DDL.

    ``bronze_columns`` (optional) is ``{table_base_lower: [colname|{name,type}, ...]}``
    introspected LIVE from the configured Databricks source (bronze) location. It grounds
    the model-generation prompts in the REAL landed column names so the AI uses them
    instead of guessing from the Glue script (the MISSING-SCHEMA failure class). Defaults
    to ``None``/empty → the converters degrade to their prior behavior.
    """
    lineage = lineage or {}
    relationships = relationships or []
    bronze_columns = bronze_columns or {}
    plan = build_migration_plan(lineage, selected_ids, jobs_io, destination, glue_scripts=glue_scripts)
    scoped = upstream_subgraph(lineage, selected_ids or [])

    notebooks, dbt_models, ddl, notes = {}, {}, {}, {}
    # {model_base_lower: [{name, data_type}]} for models whose complete output schema is
    # known (Snowflake-typed) — the source for enforced dbt contracts. Left empty for
    # AI-derived transform models (unknown output schema → tests only, no half-contract).
    columns_by_model = {}

    def _contract_cols(full_name):
        cols = snowflake_columns.get(full_name) or []
        return [{"name": c.get("name"), "data_type": snowflake_type_to_databricks(c.get("type"))}
                for c in cols if isinstance(c, dict) and c.get("name")]

    # Map each scoped Snowflake table to its Databricks target name, then group the
    # foreign keys by source table so the DDL can reference the migrated targets.
    target_by_full = {}
    for node, layer in _migratable_nodes(scoped):
        if str(node["id"]).startswith("sf:"):
            target_by_full[_norm(node["label"])] = target_table_name(node["label"], layer, destination)
    fks_by_table = {}
    for rel in relationships:
        ref_target = target_by_full.get(_norm(rel.get("pk_table", ""))) \
            or target_table_name(rel.get("pk_table", ""), "silver", destination)
        fks_by_table.setdefault(_norm(rel.get("fk_table", "")), []).append({
            "columns": rel.get("fk_columns", []),
            "ref_table": ref_target,
            "ref_columns": rel.get("pk_columns", []),
        })

    # Ingestion jobs → bronze notebooks. Each lands the raw source entities as Delta tables
    # in the SAME location the dbt source('bronze', …) refs resolve to — source_catalog/
    # source_schema (falling back to catalog/bronze_schema, mirroring the build + introspect
    # routes) — so bronze→silver→gold lines up. The job's S3 source is preserved and
    # referenced via parameters; the tool never reads or moves the data. The bronze table
    # vocabulary (plan["bronze_tables"]) is passed so written names match the source() names.
    catalog = (destination or {}).get("catalog", "main")
    bronze_schema = (destination or {}).get("bronze_schema", "bronze")
    silver_schema = (destination or {}).get("silver_schema", "silver")
    gold_schema = (destination or {}).get("gold_schema", "gold")
    source_catalog = (destination or {}).get("source_catalog") or catalog
    source_schema = (destination or {}).get("source_schema") or bronze_schema
    bronze_tables = plan.get("bronze_tables") or None
    # Each job's conversion is one independent 60-90s AI call — run them concurrently
    # (bounded by _AI_WORKERS) instead of queueing them back to back.
    for name, code in _map_concurrent(
            plan["ingestion_jobs"],
            lambda name: (name, convert_ingestion_job(
                call_ai, name, glue_scripts.get(name, ""),
                target_catalog=source_catalog, target_schema=source_schema,
                bronze_tables=bronze_tables))):
        notebooks[f"{name}.py"] = code

    # Transformation jobs → dbt models. Each job is DECOMPOSED into one model per output
    # table (dbt is one-model-per-table). The output list comes from the catalog (the
    # medallion-layer tables the job writes) — complete and deterministic — so the split
    # doesn't depend on parsing a monolithic script or on AI enumeration. Generic: no
    # pipeline-specific table names.
    # Every table any transform job builds is a valid {{ ref() }} target for the others
    # (silver models feed gold models, etc.); the bronze raw entities are the only valid
    # {{ source('bronze',…) }}. Handing this exact vocabulary to each per-table
    # translation stops it guessing refs (which produced broken source() names and
    # bronze reads of silver-derived columns).
    job_outputs = {name: catalog_outputs_for_job(jobs_io.get(name, {}), lineage)
                   for name in plan["transformation_jobs"]}
    all_refs = sorted({t for outs in job_outputs.values() for t in outs})
    bronze_sources = plan["bronze_tables"]
    def _convert_one_transform(name):
        """Convert one transformation job — pure w.r.t. shared state so the jobs can
        run concurrently. Returns a result dict merged (in input order) below."""
        outs = job_outputs[name]
        io = jobs_io.get(name, {}) or {}
        script = glue_scripts.get(name, "")
        write_layers = {_path_layer_token(w) for w in (io.get("writes") or []) if _looks_like_path(w)}
        # Procedural transforms (Excel/zip reads, config loops, UDFs, dynamic dispatch)
        # cannot be faithfully expressed as dbt SQL — port them as a PySpark notebook so
        # ALL the logic is preserved. Only genuinely relational transforms become dbt SQL.
        if transform_needs_pyspark(script):
            tgt_schema = gold_schema if "gold" in write_layers else silver_schema
            code = convert_transformation_job_to_pyspark(
                call_ai, name, script, target_catalog=catalog, target_schema=tgt_schema,
                output_tables=outs, available_refs=all_refs, bronze_sources=bronze_sources)
            logger.info("sfglue: job=%s routed to PySpark transform (non-relational logic)", name)
            note = (
                f"# {name} — ported to PySpark (not dbt SQL)\n\n"
                "This transformation job uses constructs dbt SQL cannot express "
                "(e.g. Excel/zip reads, config-driven loops, UDFs, or dynamic dispatch), "
                "so it was migrated as a Databricks PySpark notebook to preserve all logic. "
                f"See `{name}__transform.py` in the notebooks tab. It can be run standalone "
                "or wrapped as a dbt python model.\n"
            )
            return {"name": name, "kind": "pyspark", "notebook": code, "note": note}
        # Bronze column grounding applies ONLY to jobs that read bronze (bronze→silver
        # builds). A job that writes the GOLD layer reads silver ref() outputs, not bronze.
        job_bronze_cols = {} if "gold" in write_layers else bronze_columns
        models = convert_transformation_job_to_dbt_models(
            call_ai, name, script, output_tables=outs,
            available_refs=all_refs, bronze_sources=bronze_sources, bronze_columns=job_bronze_cols)
        logger.info(
            "sfglue decompose: job=%s writes=%s reads=%s catalog_outputs=%d -> %d model(s): %s",
            name, io.get("writes"), io.get("reads"), len(outs), len(models), sorted(models)[:6],
        )
        return {"name": name, "kind": "dbt", "models": models}

    # Jobs are independent of each other (the shared ref vocabulary was precomputed
    # above), so convert them concurrently; merge preserves input order → deterministic.
    pyspark_transform_jobs = []
    for res in _map_concurrent(plan["transformation_jobs"], _convert_one_transform):
        if res["kind"] == "pyspark":
            notebooks[f"{res['name']}__transform.py"] = res["notebook"]
            notes[f"{res['name']}__transform.md"] = res["note"]
            pyspark_transform_jobs.append(res["name"])
        else:
            dbt_models.update(res["models"])
    plan["pyspark_transform_jobs"] = pyspark_transform_jobs

    # Config-driven pipeline: detect the control/metadata layer and emit the
    # control-plane artifacts (schema DDL + JDBC config-read helper) so the migrated
    # flow keeps its config-driven design instead of losing the metadata layer.
    config_detection = detect_config_driven_pipeline(glue_scripts)
    plan["config_driven"] = config_detection
    if config_detection.get("is_config_driven"):
        for fname, content in generate_control_plane_artifacts(config_detection, destination).items():
            notes["_control__" + fname] = content
        logger.info("sfglue: config-driven pipeline detected (tables=%s source=%s)",
                    config_detection.get("config_tables"), config_detection.get("config_source"))

    # Publish/reverse-ETL jobs (gold → external warehouse) are set aside as notes, not
    # forced into a bronze notebook or a passthrough dbt model — they're usually
    # obsolete on Databricks where gold is served directly.
    for name in plan.get("publish_jobs", []):
        notes[f"{name}.md"] = publish_job_note(name, glue_scripts.get(name, ""))

    # Snowflake nodes in scope → views become dbt models; tables become Databricks DDL.
    # A passthrough staging model is emitted ONLY for a table that is genuinely landed
    # in bronze (its name is one of the bronze raw entities). A DERIVED table (a star-
    # schema dim/fact/mart built by the transform jobs) gets DDL as its schema contract
    # but NO bronze-passthrough staging model — there is no 1:1 bronze source to read,
    # so emitting `select * from source('bronze', 'dim_account')` would dangle.
    bronze_set = set(plan["bronze_tables"])
    # Views are one AI call each — batch them for concurrent conversion; tables (pure
    # deterministic DDL) are handled inline below.
    view_items = [(node, layer) for node, layer in _migratable_nodes(scoped)
                  if str(node["id"]).startswith("sf:") and node.get("type") == "gold"]
    for (node, layer), sql in zip(view_items, _map_concurrent(
            view_items,
            lambda nl: convert_view_to_dbt(
                call_ai, nl[0]["label"], snowflake_ddl.get(nl[0]["label"], ""),
                materialized="table" if nl[1] == "gold" else "view",
                columns=snowflake_columns.get(nl[0]["label"]), bronze_columns=bronze_columns))):
        full = node["label"]
        base = _base_name(full)
        dbt_models[f"{base}.sql"] = sql
        # A view's output columns are the Snowflake view's typed columns → enforce a contract.
        cc = _contract_cols(full)
        if cc:
            columns_by_model[base.lower()] = cc
    for node, layer in _migratable_nodes(scoped):
        if not str(node["id"]).startswith("sf:"):
            continue
        full = node["label"]
        base = _base_name(full)
        if node.get("type") == "gold":
            continue  # views handled (concurrently) above
        else:  # Snowflake table → Databricks DDL (with carried-forward FKs)
            ddl[base] = generate_databricks_ddl(
                target_table_name(full, layer, destination), snowflake_columns.get(full, []),
                source_full_name=full, foreign_keys=fks_by_table.get(_norm(full), []))
            if base in bronze_set:  # landed in bronze → a real staging model is valid
                dbt_models[f"stg_{base}.sql"] = staging_model_for_table(full)
                # A select-* staging model's output is the table's columns → enforce a contract.
                cc = _contract_cols(full)
                if cc:
                    columns_by_model[f"stg_{base}".lower()] = cc

    sources_yml = generate_sources_yml(
        (destination or {}).get("catalog", "main"),
        (destination or {}).get("bronze_schema", "bronze"),
        plan["bronze_tables"],
        source_catalog=(destination or {}).get("source_catalog"),
        source_schema=(destination or {}).get("source_schema"),
        loaded_at_field=_detect_loaded_at_field(bronze_columns),
    )
    # dbt model tests (unique/not_null/relationships) + enforced contracts + grain-aware
    # compound-key tests — catches a wrong join, a dropped/duplicated row, or a drifted
    # output schema. Contracts are emitted only for models with a known typed column list.
    schema_yml = generate_models_schema_yml(dbt_models.keys(), relationships, columns_by_model)
    # dbt unit tests (pre-build logic validation) — scaffolds seeded with each logic model's
    # real inputs; the reviewer fills the expected rows from a golden fixture. And packages.yml
    # for dbt_utils (the compound-grain combination test).
    unit_tests_yml = generate_unit_tests_yml(dbt_models)
    packages_yml = generate_packages_yml()
    # Executable twin of the schema.yml tests — run on the SQL Warehouse via /run-tests as a
    # staged gate BEFORE the reconciliation gate.
    test_specs = build_test_specs(relationships, columns_by_model)
    # Governance/lineage/security/cost checklist + config stubs (dimension 4).
    governance_md = generate_governance_md(destination, plan)
    # The human review queue: every TODO the converters flagged + any un-translated scaffold.
    untranslatable = scan_untranslatable(
        {"notebook": notebooks, "dbt model": dbt_models, "DDL": ddl, "note": notes})

    return {
        "plan": plan,
        "notebooks": notebooks,
        "dbt_models": dbt_models,
        "ddl": ddl,
        "notes": notes,
        "sources_yml": sources_yml,
        "schema_yml": schema_yml,
        "unit_tests_yml": unit_tests_yml,
        "packages_yml": packages_yml,
        # dbt project scaffolding so the artifacts are a runnable project (see /api/sfglue/export).
        "dbt_project_yml": generate_dbt_project_yml(destination),
        "profiles_yml": generate_profiles_yml(destination),
        "governance_md": governance_md,
        "contracts": columns_by_model,
        "test_specs": test_specs,
        "untranslatable": untranslatable,
        # Independent ship gate (NOT the AI grade): the deterministic content check is the
        # first pillar. The reconcile route contributes the second. AI fidelity grade is
        # triage only. "blockers" = untranslatable review-queue items that must be resolved.
        "gate": {
            "blockers_empty": len(untranslatable) == 0,
            "blocker_count": len(untranslatable),
            "contracts_enforced": sorted(columns_by_model.keys()),
            "notes": "Ship gate = blockers_empty AND reconciliation passes AND tests/contracts build. "
                     "The AI fidelity grade is a triage signal only, never the gate.",
        },
    }
