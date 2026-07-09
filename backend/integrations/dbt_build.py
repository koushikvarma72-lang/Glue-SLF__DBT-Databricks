"""Compile dbt models into runnable Databricks SQL for the one-click "Build" step.

The migration tool generates portable dbt models that reference upstream tables via
``{{ ref('X') }}`` and the raw landing tables via ``{{ source('bronze','Y') }}``. Those
models stay portable — this module does NOT mutate the stored ``.sql`` files. Instead,
at *build time* it resolves the refs/sources to fully-qualified Databricks tables and
wraps each model body in a ``CREATE OR REPLACE TABLE/VIEW`` so a non-technical operator
can populate the migrated tables with one click (no hand-run SQL).

This module is deterministic and holds NO live connections, so it is fully unit-testable.

v1 limitations (intentional, documented):
  * ``partition_by`` in a model's ``{{ config(...) }}`` is DROPPED. Delta tables work
    without an explicit partition; partitioning is a later optimization, not a
    correctness requirement.
  * ``incremental`` materializations are treated as a FULL REFRESH (``CREATE OR REPLACE
    TABLE``). v1 has no state to do an incremental merge; a full rebuild is always
    correct, just not incremental.
  * ``ephemeral`` / other exotic materializations fall back to ``table``.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

# Gold-layer naming (dimensional / serving). Kept in sync with the same idea in
# snowflake_glue_migration; re-derived here so this module is self-contained and has no
# import-time dependency on the migration module (it stays a pure, isolated unit).
_GOLD_NAME_RE = re.compile(
    r"^(dim|dimension|fact|fct|mart|agg|aggregate|rpt|report|kpi|summary|metrics?)[_]", re.I)

# {{ ref('X') }} / {{ ref("X") }} — optional whitespace, either quote style.
_REF_RE = re.compile(r"""\{\{\s*ref\s*\(\s*(['"])(?P<name>[^'"]+)\1\s*\)\s*\}\}""")
# {{ source('bronze','Y') }} — first arg is the source name (we resolve any source name
# to the configured source catalog/schema; bronze is the only one the generator emits),
# second arg is the table.
_SOURCE_RE = re.compile(
    r"""\{\{\s*source\s*\(\s*(['"])(?P<src>[^'"]+)\1\s*,\s*(['"])(?P<table>[^'"]+)\3\s*\)\s*\}\}""")
# {{ config(...) }} — the whole block, including the materialized=... we extract first.
_CONFIG_RE = re.compile(r"""\{\{\s*config\s*\((?P<args>.*?)\)\s*\}\}""", re.S)
# materialized='...' / materialized="..." inside a config block.
_MATERIALIZED_RE = re.compile(r"""materialized\s*=\s*(['"])(?P<mat>[^'"]+)\1""")
# partition_by=... inside a config block (v1: detect so we can note it was dropped).
_PARTITION_RE = re.compile(r"""partition_by\s*=""")


def _classify_layer(name: str) -> str:
    """Layer for a model name: 'gold' for dimensional/serving names, 'staging' for
    stg_ models, else 'silver'."""
    if _GOLD_NAME_RE.match(name):
        return "gold"
    if name.lower().startswith("stg_"):
        return "staging"
    return "silver"


def _schema_for_layer(layer: str, *, silver_schema: str, gold_schema: str) -> str:
    """Target schema for a layer. Staging and silver both live in the silver schema;
    only gold goes to the gold schema."""
    return gold_schema if layer == "gold" else silver_schema


def _extract_materialization(body: str) -> tuple[str, bool, str]:
    """Pull the materialization out of the first ``{{ config(...) }}`` block, then strip
    EVERY config block from the body.

    Returns ``(materialization, dropped_partition_by, stripped_body)``:
      * ``materialization`` — 'view' / 'table' / 'incremental' (default 'table').
      * ``dropped_partition_by`` — True if a ``partition_by=`` was present (dropped in v1).
      * ``stripped_body`` — the body with all ``{{ config(...) }}`` blocks removed and
        leading blank lines trimmed.
    """
    materialization = "table"
    dropped_partition_by = False
    m = _CONFIG_RE.search(body)
    if m:
        args = m.group("args")
        mm = _MATERIALIZED_RE.search(args)
        if mm:
            materialization = mm.group("mat").strip().lower() or "table"
        if _PARTITION_RE.search(args):
            dropped_partition_by = True
    stripped = _CONFIG_RE.sub("", body)
    # Trim leading whitespace/newlines left where the config block used to be.
    stripped = stripped.lstrip("\n")
    stripped = re.sub(r"^\s*\n", "", stripped)
    return materialization, dropped_partition_by, stripped.strip()


def _resolve_refs(body: str, name_to_target: dict, *, target_catalog: str, silver_schema: str) -> tuple[str, list]:
    """Resolve ``{{ ref('X') }}`` tags to their mapped target tables.

    Unknown refs fall back to ``{target_catalog}.{silver_schema}.X``. Returns the
    resolved body and the list of referenced model names (the DAG edges)."""
    refs: list = []

    def repl(m):
        ref_name = m.group("name").strip()
        refs.append(ref_name)
        target = name_to_target.get(ref_name) or f"{target_catalog}.{silver_schema}.{ref_name}"
        return target

    return _REF_RE.sub(repl, body), refs


def _resolve_sources(body: str, *, source_catalog: str, source_schema: str) -> str:
    """Resolve ``{{ source('bronze','Y') }}`` tags to ``{source_catalog}.{source_schema}.Y``."""

    def repl(m):
        table = m.group("table").strip()
        return f"{source_catalog}.{source_schema}.{table}"

    return _SOURCE_RE.sub(repl, body)


def _topo_sort(names: list, edges: dict) -> tuple[list, bool]:
    """Kahn topological sort. ``edges[name]`` = the set of models ``name`` depends on
    (its refs, restricted to known models). Returns ``(ordered_names, has_cycle)``.

    Models with no internal ref dependencies (source-only) come first. On a cycle, falls
    back to the original input order and flags ``has_cycle=True``."""
    # In-degree = number of dependencies still unresolved for each node.
    indeg = {n: 0 for n in names}
    dependents: dict = {n: [] for n in names}
    for n in names:
        for dep in edges.get(n, ()):
            if dep in indeg and dep != n:
                indeg[n] += 1
                dependents[dep].append(n)
    # Seed with zero-dependency nodes, preserving input order for stability.
    queue = [n for n in names if indeg[n] == 0]
    ordered: list = []
    while queue:
        node = queue.pop(0)
        ordered.append(node)
        for dep in dependents[node]:
            indeg[dep] -= 1
            if indeg[dep] == 0:
                queue.append(dep)
    if len(ordered) != len(names):  # cycle → not all nodes drained
        return list(names), True
    return ordered, False


def compile_models(
    dbt_models: dict,
    *,
    target_catalog: str,
    silver_schema: str,
    gold_schema: str,
    source_catalog: str,
    source_schema: str,
) -> list[dict]:
    """Compile portable dbt models into runnable Databricks CREATE statements.

    ``dbt_models`` is ``{"<name>.sql": sql_text}`` (the model name is the filename minus
    the ``.sql`` suffix). Returns a list of model dicts in DEPENDENCY ORDER, each::

        {
          "name": <model name>,
          "layer": "gold" | "silver" | "staging",
          "materialization": "table" | "view" | "incremental",
          "target_table": "<catalog>.<schema>.<name>",
          "statement": "CREATE OR REPLACE TABLE/VIEW <target> AS\\n<resolved body>",
          "refs": [<names this model depends on>],       # all refs (incl. unknown)
          "depends_on": [<known model names this depends on>],
          "dropped_partition_by": bool,                   # v1: partition_by dropped
        }

    The returned list also carries a ``warnings`` list on the FIRST dict's
    ``_warnings`` key when there's a dependency cycle (input order is used as a fallback).
    Resolution is build-time only — the input model text is never mutated.
    """
    # Parse names + classify layers; build the name→target_table map first so refs can
    # resolve to any model regardless of declaration order.
    parsed: list = []
    name_to_target: dict = {}
    name_to_layer: dict = {}
    for filename, sql_text in (dbt_models or {}).items():
        name = filename[:-4] if filename.lower().endswith(".sql") else filename
        layer = _classify_layer(name)
        schema = _schema_for_layer(layer, silver_schema=silver_schema, gold_schema=gold_schema)
        target_table = f"{target_catalog}.{schema}.{name}"
        name_to_target[name] = target_table
        name_to_layer[name] = layer
        parsed.append((name, layer, target_table, str(sql_text or "")))

    # Compile each model body: extract+strip config, resolve refs/sources, wrap.
    compiled: dict = {}
    edges: dict = {}
    names_in_order: list = []
    for name, layer, target_table, sql_text in parsed:
        names_in_order.append(name)
        materialization, dropped_partition_by, body = _extract_materialization(sql_text)
        body, refs = _resolve_refs(
            body, name_to_target, target_catalog=target_catalog, silver_schema=silver_schema)
        body = _resolve_sources(body, source_catalog=source_catalog, source_schema=source_schema)
        # Edges for the DAG: only refs that name a known model (others are external/raw).
        depends_on = [r for r in dict.fromkeys(refs) if r in name_to_target and r != name]
        edges[name] = set(depends_on)
        keyword = "VIEW" if materialization == "view" else "TABLE"
        statement = f"CREATE OR REPLACE {keyword} {target_table} AS\n{body}"
        compiled[name] = {
            "name": name,
            "layer": layer,
            "materialization": materialization,
            "target_table": target_table,
            "statement": statement,
            "refs": list(dict.fromkeys(refs)),
            "depends_on": depends_on,
            "dropped_partition_by": dropped_partition_by,
        }

    ordered_names, has_cycle = _topo_sort(names_in_order, edges)
    result = [compiled[n] for n in ordered_names]
    if result:
        warnings: list = []
        if has_cycle:
            warnings.append(
                "Dependency cycle detected among models — falling back to input order; "
                "build order may be incorrect.")
        if any(m["dropped_partition_by"] for m in result):
            warnings.append("partition_by config was dropped (v1: Delta tables build without it).")
        result[0]["_warnings"] = warnings
    return result


# ─── Build-time auto-repair ──────────────────────────────────────────────────
#
# A compiled model can still FAIL at run time even though its refs/sources resolved
# cleanly: the AI converter may have referenced a column the real upstream table
# doesn't actually expose (e.g. `select <renamed>` where the built table exposes the
# column under a different name).
# Because models run in DEPENDENCY ORDER, every upstream table a failing model reads
# is ALREADY BUILT — so we can introspect its real columns from Databricks and ask the
# AI to rewrite the statement against the truth, then retry. This is the self-healing
# loop the build route drives.
#
# Everything below is deterministic and holds NO live connections — the SQL runner,
# the column introspector, and the AI are all injected — so it is fully unit-testable.

# A fully-qualified 3-part dotted table name following FROM/JOIN. We capture the whole
# `catalog.schema.table` (optionally backtick-quoted per part) so we can introspect it.
# Trailing/leading whitespace and the join flavor (inner/left/right/full/cross) are
# tolerated; the table name is matched up to the next whitespace/paren/comma/semicolon.
_FROM_JOIN_TABLE_RE = re.compile(
    r"""(?ix)            # case-insensitive, verbose
    \b(?:from|join)\s+   # a FROM or JOIN keyword
    (`?[\w$]+`?          # catalog
     \.`?[\w$]+`?        # .schema
     \.`?[\w$]+`?)       # .table   (3-part dotted name)
    """)

# The fence-stripping the migration module's _ai_text uses — reproduced so this module
# stays self-contained (no import-time dependency on snowflake_glue_migration).
_FENCE_RE = re.compile(r"^```[a-zA-Z]*\n?|```$")

# System prompt for the repair call. Spelled out per the build-autorepair contract.
REPAIR_SYSTEM_PROMPT = (
    "You fix a Databricks Spark SQL statement that failed because it referenced columns "
    "that don't exist. Output ONLY the corrected SQL, no prose, no markdown fences. Keep "
    "the same CREATE OR REPLACE TARGET and the same intent; use ONLY the real columns "
    "listed; map renamed columns, drop references to columns that genuinely don't exist."
)


def extract_upstream_tables(statement: str) -> list:
    """Pull the fully-qualified upstream tables a statement READS — the 3-part dotted
    ``catalog.schema.table`` names following a ``FROM``/``JOIN`` — deduped, order-preserved.

    Backtick quoting on any part is stripped so the names are ready to feed an
    information_schema lookup. The CREATE OR REPLACE TARGET (which follows ``TABLE``/``VIEW``,
    not ``FROM``/``JOIN``) is therefore never picked up as an upstream read."""
    tables: list = []
    seen = set()
    for m in _FROM_JOIN_TABLE_RE.finditer(statement or ""):
        raw = m.group(1).replace("`", "")
        if raw and raw not in seen:
            seen.add(raw)
            tables.append(raw)
    return tables


def _strip_fences(text: str) -> str:
    """Strip leading/trailing markdown code fences, mirroring migration._ai_text."""
    return _FENCE_RE.sub("", (text or "").strip()).strip()


def _build_repair_prompt(statement: str, error: str, table_columns: dict) -> str:
    """Compose the repair user-prompt: the failing SQL, the exact Databricks error, and a
    per-table ``Table <fq> has columns: a, b, c`` block for every introspected upstream."""
    lines = [
        "The following Databricks Spark SQL statement failed because it referenced "
        "columns that don't exist.",
        "",
        "Failing SQL:",
        statement or "",
        "",
        "Databricks error:",
        (error or "").strip() or "(no error message provided)",
    ]
    if table_columns:
        lines.append("")
        lines.append("Real columns of the upstream tables it reads:")
        for fq, cols in table_columns.items():
            col_list = ", ".join(cols) if cols else "(could not introspect — leave its usage unchanged)"
            lines.append(f"Table {fq} has columns: {col_list}")
    lines.append("")
    lines.append("Return the corrected SQL only.")
    return "\n".join(lines)


def attempt_build_with_repair(run_sql, call_ai, statement, introspect_cols, *, max_attempts=2):
    """Run a compiled model statement, auto-repairing resolvable schema errors.

    Pure-ish and injected for testability — no Flask/Databricks coupling:
      * ``run_sql(sql) -> dict``    runs a statement; returns the same shape
        ``execute_sql_statement`` does (``{"success": bool, "message"/"error": ...}``).
      * ``call_ai(prompt, system_prompt=, max_tokens=, temperature=, task=) -> str``
        the AI rewrite. When ``None``, NO repair is attempted (degrade to plain run).
      * ``statement``              the compiled ``CREATE OR REPLACE ... AS <body>``.
      * ``introspect_cols(fq) -> list[str]``  real column names of an upstream table
        (best-effort; may return ``[]`` for a table it can't introspect).
      * ``max_attempts``           number of REPAIR attempts after the initial run (default 2).

    Returns a dict::

        {"status": "created" | "repaired" | "failed",
         "message": <last runner message/error>,
         "statement": <the SQL that actually ran successfully, when repaired>,
         "repair_attempts": <int>}

    Behavior:
      * Initial run succeeds → ``created`` (no AI touched).
      * ``call_ai is None`` or introspection/repair can't fix it → ``failed`` (caller
        cascades ``skipped`` to dependents, exactly as before).
      * A repair run succeeds → ``repaired`` with the corrected ``statement`` and the
        attempt count, so the UI can surface what changed (and the caller can persist it).
      * Each attempt RE-INTROSPECTS the upstreams and RE-PROMPTS with the LATEST error.
    """
    res = run_sql(statement) or {}
    if res.get("success"):
        return {"status": "created", "message": res.get("message") or "created",
                "statement": statement, "repair_attempts": 0}

    last_error = res.get("message") or res.get("error") or "failed"
    # No AI → can't self-heal. Degrade to today's behavior: report the failure as-is.
    if not call_ai:
        return {"status": "failed", "message": last_error, "repair_attempts": 0}

    current_sql = statement
    for attempt in range(1, max_attempts + 1):
        # Re-introspect the upstreams from the LATEST SQL (a prior repair may have changed
        # which tables it reads), then re-prompt with the most recent error.
        table_columns: dict = {}
        for fq in extract_upstream_tables(current_sql):
            try:
                cols = introspect_cols(fq)
            except Exception as exc:  # noqa: BLE001 — best-effort; skip tables that error
                logger.warning("build auto-repair: column introspection failed for %s: %s", fq, exc)
                cols = []
            table_columns[fq] = [c for c in (cols or []) if c]

        prompt = _build_repair_prompt(current_sql, last_error, table_columns)
        try:
            raw = call_ai(prompt, system_prompt=REPAIR_SYSTEM_PROMPT, max_tokens=8000,
                          temperature=0, task="migration")
        except Exception as exc:  # noqa: BLE001 — AI best-effort; treat as un-repairable
            logger.warning("build auto-repair: AI repair call failed on attempt %d: %s", attempt, exc)
            return {"status": "failed", "message": last_error, "repair_attempts": attempt - 1}

        repaired_sql = _strip_fences(raw if isinstance(raw, str) else "")
        if not repaired_sql or repaired_sql == current_sql:
            # AI gave nothing usable / no change → stop; nothing left to try.
            logger.info("build auto-repair: attempt %d produced no usable change; giving up", attempt)
            return {"status": "failed", "message": last_error, "repair_attempts": attempt - 1}

        rerun = run_sql(repaired_sql) or {}
        if rerun.get("success"):
            return {"status": "repaired", "message": rerun.get("message") or "created (auto-repaired)",
                    "statement": repaired_sql, "repair_attempts": attempt}

        # Still failing — carry the new SQL + new error into the next attempt.
        current_sql = repaired_sql
        last_error = rerun.get("message") or rerun.get("error") or last_error

    return {"status": "failed", "message": last_error, "repair_attempts": max_attempts}
