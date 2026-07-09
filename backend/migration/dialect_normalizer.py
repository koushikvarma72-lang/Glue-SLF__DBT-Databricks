"""Deterministic dialect normalisation for generated migration SQL.

Prompting the model to target one dialect is best-effort; the fast model tier
still emits Snowflake-only functions on a Databricks target. This module
mechanically rewrites those functions to their Databricks/Spark equivalents in
the finalised SQL, so the output is correct-dialect regardless of the model.

Only genuinely Snowflake-only constructs are rewritten — Databricks SQL natively
supports `::` casts and `LATERAL VIEW EXPLODE`, so those are left untouched. dbt
Jinja ({{ ref/source }}) is naturally preserved because only specific named
function calls are rewritten.
"""

import re

from .duckdb_execution import _rewrite_calls  # balanced-paren function rewriter (reused)


def _to_java_date_format(fmt):
    """Snowflake TO_CHAR/TO_DATE tokens -> Java SimpleDateFormat (Databricks)."""
    f = fmt.strip().strip("'\"")
    # order matters: longer tokens first
    for a, b in (('YYYY', 'yyyy'), ('HH24', 'HH'), ('MON', 'MMM'), ('Mon', 'MMM'),
                 ('MI', 'mm'), ('SS', 'ss'), ('DD', 'dd')):
        f = f.replace(a, b)
    # MM (month) and HH already Java-compatible; leave as-is.
    return f


def normalize_to_databricks(sql):
    if not sql:
        return sql

    def _to_date(a):
        if len(a) >= 2 and a[1].strip().startswith(("'", '"')):
            return f"to_date({a[0]}, '{_to_java_date_format(a[1])}')"
        return f"try_cast({a[0]} as date)" if a else 'NULL'

    def _to_char(a):
        if len(a) >= 2 and a[1].strip().startswith(("'", '"')):
            return f"date_format({a[0]}, '{_to_java_date_format(a[1])}')"
        return f"cast({a[0]} as string)" if a else 'NULL'

    def _dateadd(a):
        if len(a) >= 3:
            unit = a[0].strip().strip("'\"").lower()
            n, d = a[1], a[2]
            if unit.startswith('day'):
                return f"date_add({d}, {n})"
            if unit.startswith('week'):
                return f"date_add({d}, ({n}) * 7)"
            if unit.startswith('month') or unit in ('mm', 'mon'):
                return f"add_months({d}, {n})"
            if unit.startswith('year') or unit in ('yy', 'yyyy'):
                return f"add_months({d}, ({n}) * 12)"
        return f"dateadd({', '.join(a)})"  # unknown unit: leave Databricks 3-arg form

    def _datediff(a):
        if len(a) >= 3:
            unit = a[0].strip().strip("'\"").lower()
            x, y = a[1], a[2]
            if unit.startswith('day'):
                return f"datediff({y}, {x})"
            if unit.startswith('month') or unit in ('mm', 'mon'):
                return f"cast(months_between({y}, {x}) as int)"
            if unit.startswith('year') or unit in ('yy', 'yyyy'):
                return f"cast(months_between({y}, {x}) / 12 as int)"
        return f"datediff({', '.join(a)})"

    sql = _rewrite_calls(sql, 'TRY_TO_DATE', _to_date)
    sql = _rewrite_calls(sql, 'TO_DATE', _to_date)
    sql = _rewrite_calls(sql, 'TO_CHAR', _to_char)
    sql = _rewrite_calls(sql, 'DATEADD', _dateadd)
    sql = _rewrite_calls(sql, 'DATEDIFF', _datediff)
    sql = _rewrite_calls(sql, 'TRY_TO_NUMBER', lambda a: f"try_cast({a[0]} as double)" if a else 'NULL')
    sql = _rewrite_calls(sql, 'TRY_TO_DECIMAL', lambda a: f"try_cast({a[0]} as double)" if a else 'NULL')
    sql = _rewrite_calls(sql, 'IFF', lambda a: f"if({a[0]}, {a[1]}, {a[2]})" if len(a) >= 3 else (a[0] if a else 'NULL'))
    sql = _rewrite_calls(sql, 'ZEROIFNULL', lambda a: f"coalesce({a[0]}, 0)" if a else 'NULL')
    sql = _rewrite_calls(sql, 'NULLIFZERO', lambda a: f"nullif({a[0]}, 0)" if a else 'NULL')
    return sql


def normalize_sql_dialect(sql, dialect):
    """Rewrite generated SQL to the target dialect's functions. Currently handles
    Databricks/Spark targets; other dialects are returned unchanged."""
    d = (dialect or '').strip().lower()
    if d in {'databricks', 'spark'}:  # 'dbt' is the Snowflake-shaped generic alias — leave it
        return normalize_to_databricks(sql)
    return sql


# ─── Snowflake → Databricks finalisation for the SF+Glue dbt models ──────────
# The SF/Glue flow converts pure-Snowflake views into Databricks dbt models. The
# AI does most of it, but leaves Snowflake-isms; this deterministically cleans the
# output. Source is pure Snowflake, so sqlglot's snowflake->databricks transpile
# does the structural work (QUALIFY, ::, IFF, FLATTEN, TRY_TO_DATE); a second pass
# of normalize_to_databricks fixes residuals sqlglot leaves (TO_CHAR, DATEADD).

_JINJA_RE = re.compile(r'\{\{.*?\}\}|\{%.*?%\}', re.DOTALL)
_CONFIG_RE = re.compile(r'\{\{\s*config\([^}]*\)\s*\}\}', re.IGNORECASE | re.DOTALL)


def _protect_jinja(sql):
    """Replace dbt Jinja with placeholder identifiers so a SQL parser can read the
    body. Returns (config_block_or_None, body_with_placeholders, {placeholder: jinja})."""
    config = None
    m = _CONFIG_RE.search(sql)
    if m:
        config = m.group(0)
        sql = sql[:m.start()] + sql[m.end():]
    phs = {}

    def _repl(mm):
        key = f"jinja_ph_{len(phs)}"
        phs[key] = mm.group(0)
        return key

    body = _JINJA_RE.sub(_repl, sql)
    return config, body, phs


def _restore_jinja(sql, config, phs):
    for key, val in phs.items():
        sql = sql.replace(key, val)
    return (config + "\n" + sql.lstrip("\n")) if config else sql


def finalize_sfglue_model_sql(sql):
    """Deterministically finalise a generated SF→Databricks dbt model's SQL.

    Best-effort and safe: on any failure it falls back to the targeted
    normalize_to_databricks (which is Jinja-safe), then to the original text.
    """
    if not sql or not sql.strip():
        return sql
    try:
        import sqlglot
    except Exception:
        return normalize_to_databricks(sql)
    try:
        config, body, phs = _protect_jinja(sql)
        transpiled = sqlglot.transpile(body, read='snowflake', write='databricks', pretty=True)[0]
        restored = _restore_jinja(transpiled, config, phs)
        # second pass cleans residual Snowflake-isms sqlglot leaves (TO_CHAR, DATEADD…)
        return normalize_to_databricks(restored)
    except Exception:
        return normalize_to_databricks(sql)
