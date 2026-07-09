"""Operator-friendly reconciliation hints (deterministic, no live connections).

A non-technical operator runs the reconciliation gate without knowing data-engineering
details. They do NOT know that a surrogate key like ``account_sk = abs(xxhash64(...))``
will never match across engines (Snowflake ``HASH`` ≠ Spark ``xxhash64``), nor that
run-stamp columns (``_load_ts``, ``dwh_batch_id``) differ every run. This module looks at
the candidate columns (plus, when available, the source columns and declared FK
relationships) and suggests, in plain English:

  * which columns to EXCLUDE from the diff (and why), and
  * a best-effort PRIMARY KEY (with where the guess came from).

It is pure and side-effect-free — name-based, case-insensitive, and source-agnostic (no
pipeline-specific column names) — so it is fully unit-testable without a warehouse.
"""

from __future__ import annotations

# Plain-English reasons, kept as constants so the route, UI text, and tests agree.
_REASON_SURROGATE = "surrogate/hash key — generated differently per engine, won't match"
_REASON_RUNSTAMP = "load/run metadata — changes every run"

# Surrogate / hash key suffixes (case-insensitive). A column ending in one of these,
# or containing 'xxhash'/'surrogate', is engine-generated and won't match cross-engine.
_SURROGATE_SUFFIXES = ("_sk", "_skey", "_surrogate", "_hash")
_SURROGATE_SUBSTRINGS = ("xxhash", "surrogate")

# Exact run-stamp / load-metadata names (case-insensitive). These differ every run.
_RUNSTAMP_NAMES = {
    "_load_ts", "_loaded_at", "loaded_at", "load_timestamp", "load_date",
    "_batch_id", "batch_id", "dwh_batch_id", "_source_file",
    "_etl_run_id", "etl_run_id", "inserted_at", "updated_at", "_ingested_at",
}

# Table-name prefixes stripped before deriving the "<base>_id" business key.
_TABLE_PREFIXES = ("dim_", "fact_", "fct_", "mart_")


def _base_name(identifier) -> str:
    """Last dotted segment of a (possibly schema-qualified) name, lowercased.

    Local copy of snowflake_glue_lineage._base_name kept tiny so this module stays
    dependency-free and easy to unit-test.
    """
    norm = str(identifier or "").strip().lower()
    return norm.split(".")[-1] if norm else ""


def _col_names(columns) -> list[str]:
    """[{name,type}] | [name] → [original-cased name]. Drops blanks."""
    out = []
    for c in columns or []:
        name = c.get("name") if isinstance(c, dict) else c
        if name:
            out.append(str(name))
    return out


def _is_surrogate(lower_name: str) -> bool:
    if any(lower_name.endswith(s) for s in _SURROGATE_SUFFIXES):
        return True
    return any(sub in lower_name for sub in _SURROGATE_SUBSTRINGS)


def _is_runstamp(lower_name: str) -> bool:
    if lower_name in _RUNSTAMP_NAMES:
        return True
    # dwh_*_ts: any name starting with 'dwh_' and ending '_ts'.
    if lower_name.startswith("dwh_") and lower_name.endswith("_ts"):
        return True
    # *_run_id: any run-id column (etl_run_id is already exact, but covers job_run_id etc.).
    if lower_name.endswith("_run_id"):
        return True
    return False


def _suggest_exclude(columns) -> list[dict]:
    """Return [{column, reason}] for every candidate column that should be excluded.

    Name-based and case-insensitive; ordinary business keys (account_id, call_id,
    product_id) are deliberately left in.
    """
    excludes = []
    for name in _col_names(columns):
        lower = name.lower()
        if _is_surrogate(lower):
            excludes.append({"column": name, "reason": _REASON_SURROGATE})
        elif _is_runstamp(lower):
            excludes.append({"column": name, "reason": _REASON_RUNSTAMP})
    return excludes


def _suggest_primary_key(candidate_columns, *, source_columns, relationships, table):
    """Best-effort primary key + where the guess came from. See module/spec for rules.

    Returns (primary_key: list[str], key_source: str).
    """
    table_base = _base_name(table)
    lower_to_orig = {n.lower(): n for n in _col_names(candidate_columns)}

    # 1) Declared FK relationship whose pk_table base-name matches this table.
    if table_base:
        for rel in relationships or []:
            if _base_name((rel or {}).get("pk_table")) == table_base:
                pk_cols = [str(c) for c in ((rel or {}).get("pk_columns") or []) if c]
                if pk_cols:
                    return pk_cols, "declared_relationship"

    # 2) A candidate column named "<table_base>_id" (after stripping dim_/fact_/... prefix).
    if table_base:
        stripped = table_base
        for prefix in _TABLE_PREFIXES:
            if stripped.startswith(prefix):
                stripped = stripped[len(prefix):]
                break
        guess = f"{stripped}_id"
        if guess in lower_to_orig:
            return [lower_to_orig[guess]], "name_match"

    # 3) A plain "id" column.
    if "id" in lower_to_orig:
        return [lower_to_orig["id"]], "id_column"

    # 4) Nothing inferable — operator must choose.
    return [], "none"


def suggest_reconcile_settings(candidate_columns, *, source_columns=None,
                               relationships=None, table=None) -> dict:
    """Suggest a primary key and exclude-list for reconciling ``table``.

    Args:
        candidate_columns: [{name,type}] | [name] of the Databricks candidate table.
        source_columns: optional [{name,type}] of the Snowflake source (reserved for
            future use; suggestions today are candidate-name based).
        relationships: optional [{pk_table, pk_columns, fk_table, ...}] declared FKs.
        table: the source/candidate table name (used to infer the key by name).

    Returns:
        {"primary_key": [str], "key_source": str,
         "exclude": [{"column": str, "reason": str}]}  — all plain-English.
    """
    primary_key, key_source = _suggest_primary_key(
        candidate_columns, source_columns=source_columns,
        relationships=relationships, table=table)
    return {
        "primary_key": primary_key,
        "key_source": key_source,
        "exclude": _suggest_exclude(candidate_columns),
    }
