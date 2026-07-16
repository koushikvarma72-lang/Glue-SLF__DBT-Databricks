"""Control-plane migration (Phase 2 of the gap plan).

Metadata-driven pipelines keep their behavior in RDS control tables. This module
re-platforms that layer onto Databricks WITHOUT rewriting config into code:

  1. ``generate_control_schema_ddl``  — the framework tables as Delta DDL in a
     dedicated control schema (config stays data; ops model survives).
  2. ``convert_query_configuration_rows`` — the *actual transform SQL* stored in
     ``query_configuration`` rows becomes dbt models (AI-translated with the
     standard annotated-scaffold fallback). This is where pipelines whose Glue
     jobs are generic config-loop runners get their dbt models from.
  3. ``generate_framework_notebooks`` — templated (NOT AI) batch open/close +
     file-audit runtime notebooks that the Phase-1 workflows re-point to.

Pure: no connections, no I/O. The routes layer supplies introspected framework
tables (see ``postgres_client.introspect_framework_tables``) and ``call_ai``.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# ─── Postgres → Delta type mapping ───────────────────────────────────────────

_PG_TYPE_MAP = [
    (re.compile(r"^(small|big)?serial", re.I), "BIGINT"),
    (re.compile(r"^bigint", re.I), "BIGINT"),
    (re.compile(r"^(integer|int4?|smallint|int2)", re.I), "INT"),
    (re.compile(r"^(numeric|decimal)\s*\((\d+)\s*,\s*(\d+)\)", re.I), None),  # keep (p,s)
    (re.compile(r"^(numeric|decimal)", re.I), "DECIMAL(38,6)"),
    (re.compile(r"^(double precision|float8|real|float4?)", re.I), "DOUBLE"),
    (re.compile(r"^bool", re.I), "BOOLEAN"),
    (re.compile(r"^timestamp", re.I), "TIMESTAMP"),
    (re.compile(r"^date$", re.I), "DATE"),
    (re.compile(r"^(json|jsonb|uuid|text|character|varchar|char|citext|bytea)", re.I), "STRING"),
]


def postgres_type_to_databricks(pg_type: str) -> str:
    t = str(pg_type or "").strip()
    if not t:
        return "STRING"
    for rx, target in _PG_TYPE_MAP:
        m = rx.match(t)
        if m:
            if target is None:  # NUMERIC(p,s) — carry precision through
                return f"DECIMAL({m.group(2)},{m.group(3)})"
            return target
    return "STRING"


def control_schema_name(destination: dict) -> str:
    return (destination or {}).get("control_schema") or "control"


def generate_control_schema_ddl(framework_tables: list[dict], destination: dict) -> dict:
    """Delta DDL for each detected framework table → {ddl_name: sql}.

    Named ``control__<table>`` so they sit apart from the data-layer DDL in the
    review UI and the export.
    """
    d = destination or {}
    catalog = d.get("catalog") or "main"
    schema = control_schema_name(d)
    out: dict = {}
    for t in framework_tables or []:
        cols = [c for c in (t.get("columns") or []) if c.get("name")]
        if not cols:
            continue
        body = ",\n".join(
            f"  `{c['name']}` {postgres_type_to_databricks(c.get('type'))}" for c in cols)
        out[f"control__{t['name']}"] = (
            f"-- control-plane table migrated from {t.get('schema','public')}.{t['name']}"
            f" (canonical: {t.get('canonical', t['name'])})\n"
            f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`{t['name']}` (\n{body}\n"
            ") USING DELTA;"
        )
    return out


# ─── query_configuration rows → dbt models ───────────────────────────────────

_SQL_COL_CANDIDATES = ("query_text", "query_sql", "sql_text", "sql_query", "query", "sql",
                       "statement", "transformation_sql")
_TARGET_COL_CANDIDATES = ("target_table", "target_tablename", "target_object", "target",
                          "publish_tablename", "curated_tablename", "output_table",
                          "table_name", "model_name")
_ACTIVE_COL_CANDIDATES = ("is_active", "active_flag", "active", "enabled")
_ORDER_COL_CANDIDATES = ("execution_order", "step_number", "sequence", "seq_no", "run_order")

_ACTIVE_TRUE = {"y", "yes", "true", "1", "a", "active", "t"}


def _rows_as_dicts(entry: dict) -> list[dict]:
    cols = [c.get("name") for c in entry.get("columns") or []]
    return [dict(zip(cols, r)) for r in entry.get("rows") or []]


def _pick_col(cols: set, candidates) -> str | None:
    low = {str(c).lower(): c for c in cols}
    return next((low[c] for c in candidates if c in low), None)


def _model_name(target: str, idx: int) -> str:
    base = re.sub(r"[^a-zA-Z0-9_]+", "_", str(target or "").split(".")[-1]).strip("_").lower()
    return base or f"query_config_{idx}"


_QC_SYSTEM = (
    "You translate warehouse SQL (Snowflake/Spark dialect) from a metadata-driven pipeline's "
    "query_configuration table into a Databricks dbt model. Return ONLY the SQL for one model. "
    "Rules: reference raw landed tables via {{ source('bronze', '<table>') }} and sibling "
    "models via {{ ref('<model>') }} — the valid vocabularies are provided; convert Snowflake "
    "functions to Databricks equivalents; keep ALL business logic; resolve config placeholders "
    "like ${schema}/{{params}} to the given catalog/schema; flag anything you cannot verify "
    "with an inline `-- TODO[ASSUMPTION]: ...` comment. RUNTIME placeholders such as "
    "{batch_id}, {file_id}, {file_name}, {raw_filename}: DROP any WHERE/AND filter comparing "
    "a column to one of them (these models are full-refresh), and emit metadata columns as "
    "cast(null as string) — never leave a bare {placeholder} or {{ var(...) }} in the SQL. "
    "No prose, no code fences."
)

# Deterministic scrub for runtime placeholders the AI may still leave behind — the app
# executes these models as RAW SQL on the warehouse (no dbt var resolution), so any
# surviving {token} / {{ var(...) }} is a guaranteed parse error.
_PH_FENCE = re.compile(r"^\s*```.*$", re.M)
_PH_FILTER = re.compile(
    r"(?i)\b(where|and)\s+[\w.`]+\s*=\s*'?(\{\{[^}]*\}\}|\{[A-Za-z_]\w*\})'?")
_PH_QUOTED = re.compile(r"'(\{\{[^}]*\}\}|\{[A-Za-z_]\w*\})'")
_PH_VAR = re.compile(r"\{\{\s*var\([^)]*\)\s*\}\}")
_PH_BARE = re.compile(r"\{[A-Za-z_]\w*\}")


def scrub_runtime_placeholders(sql: str) -> str:
    """Make qc-derived model SQL executable as raw SQL: strip code fences, drop
    batch/file runtime filters (full-refresh semantics), and null out remaining
    placeholder tokens. Real dbt jinja (source/ref/config) is left untouched."""
    s = _PH_FENCE.sub("", sql or "")
    s = _PH_FILTER.sub(lambda m: "where 1=1" if m.group(1).lower() == "where" else "", s)
    s = _PH_QUOTED.sub("cast(null as string)", s)
    s = _PH_VAR.sub("null", s)
    s = _PH_BARE.sub("null", s)
    return s


def _qc_scaffold(name: str, sql: str, reason: str) -> str:
    return (
        f"-- dbt model scaffold for query_configuration target `{name}`\n"
        f"-- TODO[EXTERNAL]: {reason}\n"
        "-- The ORIGINAL config-table SQL is embedded below; translate to Databricks SQL\n"
        "-- with {{ source('bronze', ...) }} / {{ ref(...) }} references.\n"
        "{{ config(materialized='table') }}\n\n"
        "/* original query_configuration SQL:\n" + (sql or "").replace("*/", "* /") + "\n*/\n"
        "select 1 as placeholder -- replace with the translated query\n"
    )


def convert_query_configuration_rows(call_ai, qc_entry: dict, *, destination: dict,
                                     bronze_sources: list | None = None,
                                     available_refs: list | None = None,
                                     map_concurrent=None) -> dict:
    """Turn query_configuration rows into dbt models → {"models": {fname: sql},
    "skipped": [...], "columns": {...meta}}.

    Deterministic frame, AI for the SQL translation only; a failed/absent AI
    yields an annotated scaffold embedding the original SQL (review-queue item).
    """
    rows = _rows_as_dicts(qc_entry or {})
    if not rows:
        return {"models": {}, "skipped": [], "meta": {}}
    cols = set(rows[0].keys())
    sql_col = _pick_col(cols, _SQL_COL_CANDIDATES)
    target_col = _pick_col(cols, _TARGET_COL_CANDIDATES)
    active_col = _pick_col(cols, _ACTIVE_COL_CANDIDATES)
    order_col = _pick_col(cols, _ORDER_COL_CANDIDATES)
    if not sql_col:
        return {"models": {}, "skipped": ["no SQL column recognized in query_configuration"],
                "meta": {"columns": sorted(cols)}}

    d = destination or {}
    catalog, gold = d.get("catalog") or "main", d.get("gold_schema") or "gold"
    work, skipped = [], []
    for i, row in enumerate(rows):
        sql = str(row.get(sql_col) or "").strip()
        if not sql:
            skipped.append(f"row {i}: empty SQL")
            continue
        if active_col is not None:
            flag = str(row.get(active_col) if row.get(active_col) is not None else "y").strip().lower()
            if flag not in _ACTIVE_TRUE:
                skipped.append(f"row {i}: inactive ({active_col}={flag!r})")
                continue
        name = _model_name(row.get(target_col) if target_col else "", i)
        order = row.get(order_col) if order_col else i
        work.append((name, sql, order))
    work.sort(key=lambda w: (str(w[2]), w[0]))

    refs = sorted({r for r in (available_refs or []) if r} | {n for n, _, _ in work})
    src = ", ".join(sorted(bronze_sources or [])) or "(none introspected)"

    def _one(item):
        name, sql, _ = item
        prompt = (
            f"Target model: {name}\nTarget catalog/schema: {catalog}.{gold}\n"
            f"Valid {{{{ source('bronze', ...) }}}} tables: {src}\n"
            f"Valid {{{{ ref(...) }}}} models: {', '.join(refs)}\n\n"
            f"query_configuration SQL to translate:\n```sql\n{sql}\n```"
        )
        out = None
        if call_ai:
            try:
                out = call_ai(prompt, system_prompt=_QC_SYSTEM, max_tokens=8000,
                              temperature=0, task="qc_to_dbt")
                out = re.sub(r"^```[a-zA-Z]*\n?|```$", "", (out or "").strip()).strip() or None
            except Exception as exc:  # noqa: BLE001 — scaffold fallback below
                logger.warning("query_configuration AI translation failed (%s): %s", name, exc)
        if not out:
            reason = ("no AI provider configured" if not call_ai
                      else "AI translation failed — retry conversion")
            return f"{name}.sql", _qc_scaffold(name, sql, reason)
        out = scrub_runtime_placeholders(out)
        if "config(" not in out:
            out = "{{ config(materialized='table') }}\n\n" + out
        try:  # deterministic dialect cleanup, best-effort
            from backend.migration.dialect_normalizer import finalize_sfglue_model_sql
            out = finalize_sfglue_model_sql(out) or out
        except Exception:  # noqa: BLE001
            pass
        return f"{name}.sql", out

    runner = map_concurrent or (lambda items, fn: [fn(x) for x in items])
    models = dict(runner(work, _one))
    return {"models": models, "skipped": skipped,
            "meta": {"sql_col": sql_col, "target_col": target_col, "rows": len(rows)}}


# ─── framework runtime notebooks (templated, deterministic) ──────────────────

def generate_framework_notebooks(destination: dict) -> dict:
    """Batch open/close + file-audit notebooks writing to the control schema.

    Idempotent (MERGE on batch/file id); parameterized via widgets so the same
    notebooks serve every feed. These are the tasks the Phase-1 workflow points
    its parent_batch_open/close nodes at.
    """
    d = destination or {}
    catalog, schema = d.get("catalog") or "main", control_schema_name(d)
    fq = f"`{catalog}`.`{schema}`"
    common = (
        "# Framework runtime — generated by sfglue (deterministic template, not AI).\n"
        "# Widgets let one notebook serve every feed/workflow.\n"
        "from datetime import datetime, timezone\n"
        "import uuid\n\n"
        'dbutils.widgets.text("batch_id", "")\n'
        'dbutils.widgets.text("source_system", "")\n'
        'batch_id = dbutils.widgets.get("batch_id") or str(uuid.uuid4())\n'
        'source_system = dbutils.widgets.get("source_system")\n'
        "now = datetime.now(timezone.utc).isoformat()\n\n"
    )
    open_nb = common + (
        f'spark.sql(f"""\nMERGE INTO {fq}.`batch_log` t\n'
        "USING (SELECT '{batch_id}' batch_id) s ON t.batch_id = s.batch_id\n"
        "WHEN NOT MATCHED THEN INSERT (batch_id, source_system, status, started_at)\n"
        "VALUES ('{batch_id}', '{source_system}', 'RUNNING', '{now}')\n"
        '""")\n'
        'dbutils.jobs.taskValues.set(key="batch_id", value=batch_id)\n'
        'print(f"batch {batch_id} opened")\n'
    )
    close_nb = common + (
        'dbutils.widgets.text("status", "SUCCEEDED")\n'
        'status = dbutils.widgets.get("status") or "SUCCEEDED"\n'
        f'spark.sql(f"""\nUPDATE {fq}.`batch_log`\n'
        "SET status = '{status}', completed_at = '{now}'\n"
        "WHERE batch_id = '{batch_id}'\n"
        '""")\n'
        'print(f"batch {batch_id} closed: {status}")\n'
    )
    audit_nb = common + (
        'dbutils.widgets.text("object_name", "")\n'
        'dbutils.widgets.text("layer", "bronze")\n'
        'dbutils.widgets.text("row_count", "0")\n'
        'dbutils.widgets.text("status", "SUCCEEDED")\n'
        'obj = dbutils.widgets.get("object_name")\n'
        f'spark.sql(f"""\nINSERT INTO {fq}.`ingestion_log`\n'
        "(batch_id, source_system, source_object_name, layer, row_count, status, logged_at)\n"
        "VALUES ('{batch_id}', '{source_system}', '{obj}', "
        "'{dbutils.widgets.get(\"layer\")}', {int(dbutils.widgets.get(\"row_count\") or 0)}, "
        "'{dbutils.widgets.get(\"status\")}', '{now}')\n"
        '""")\n'
        'print(f"audit row written for {obj}")\n'
    )
    return {
        "fw_batch_open.py": open_nb,
        "fw_batch_close.py": close_nb,
        "fw_file_audit.py": audit_nb,
    }
