"""Rebuild the Snowflake SOURCE estate in a fresh account from the migrated
Databricks estate (trial expired → new trial). Everything is copied back:

  * gold tables (dim_*, fact_*, mart_*, dim_date) → VEEVA_CRM.PUB tables + rows
  * analytics views (vw_*) → recreated from the workspace dbt models, with the
    three role-filter views REPLACED by hand-corrected SQL that restores the
    original CASE(owner_role_code) → function_area derivation the source had.

Usage (from the project .venv — needs snowflake-connector-python):
    python3 rebuild_snowflake_source.py \
        --sf-account <new_acct>.<region> --sf-user KOUSHIK --sf-password '...' \
        --dbx-host https://dbc-xxxx.cloud.databricks.com --dbx-token dapiXXXX \
        --dbx-warehouse 66b30eb900bcd97a
Optional: --database VEEVA_CRM --schema PUB --sf-warehouse COMPUTE_WH
          --sf-role SYSADMIN --catalog workspace --skip-views --skip-data
"""

import argparse
import base64
import json
import re
import time
import urllib.parse
import urllib.request

# ── Databricks helpers (SQL Statement API + workspace export) ────────────────

def _dbx(host, token, path, body=None, params=""):
    url = f"{host.rstrip('/')}{path}{params}"
    req = urllib.request.Request(url, data=json.dumps(body).encode() if body else None,
                                 method="POST" if body else "GET")
    req.add_header("Authorization", f"Bearer {token}")
    if body:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def dbx_sql(host, token, warehouse, statement, catalog):
    out = _dbx(host, token, "/api/2.0/sql/statements",
               {"statement": statement, "warehouse_id": warehouse,
                "catalog": catalog, "wait_timeout": "30s"})
    while out.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        out = _dbx(host, token, f"/api/2.0/sql/statements/{out['statement_id']}")
    if out.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(out.get("status", {}).get("error", {}).get("message", "query failed"))
    cols = [c["name"] for c in out.get("manifest", {}).get("schema", {}).get("columns", [])]
    return cols, out.get("result", {}).get("data_array") or []


# ── Type mapping: Databricks → Snowflake ─────────────────────────────────────

def sf_type(dbx_type: str) -> str:
    t = (dbx_type or "STRING").upper()
    if t.startswith("DECIMAL") or t.startswith("NUMERIC"):
        return t.replace("DECIMAL", "NUMBER").replace("NUMERIC", "NUMBER")
    return {"STRING": "VARCHAR", "DOUBLE": "FLOAT", "FLOAT": "FLOAT",
            "INT": "NUMBER(38,0)", "BIGINT": "NUMBER(38,0)", "SMALLINT": "NUMBER(38,0)",
            "TINYINT": "NUMBER(38,0)", "BOOLEAN": "BOOLEAN", "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP_NTZ", "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
            "BINARY": "BINARY"}.get(t, "VARCHAR")


# ── Corrected analytics views (built from the ACTUAL curated columns) ────────
# The migrated dbt models dropped the CASE derivation (literal role filters that
# match nothing). The SOURCE views carried it — restore the real logic here,
# using whatever the curated `call`/`account` tables really call their columns.
BASE_TABLES = ["account", "call", "call_detail", "call_discussion",
               "call_expense", "call_followup"]


def _pick(cols, *cands):
    low = {c.lower(): c for c in cols}
    for c in cands:
        if c in low:
            return low[c]
    return None


def build_corrected_views(call_cols, acct_cols, sch):
    """Return ({view_name: sql}, note). Empty dict if key columns can't be found."""
    role = _pick(call_cols, "owner_role__c", "owner_role_code", "owner_role", "user_role__c")
    ck   = _pick(call_cols, "account_vod__c", "account_id", "account2_vod__c", "account__c")
    cid  = _pick(call_cols, "id", "call_id")
    cdt  = _pick(call_cols, "call_date_vod__c", "call_date", "call_datetime_vod__c")
    ctp  = _pick(call_cols, "call_type_vod__c", "call_type")
    cst  = _pick(call_cols, "status_vod__c", "call_status", "status")
    cow  = _pick(call_cols, "ownerid", "owner_id", "owner_vod__c", "createdbyid")
    aid  = _pick(acct_cols, "account_id", "id")
    anm  = _pick(acct_cols, "account_name", "name")
    asp  = _pick(acct_cols, "specialty", "specialty_1_vod__c")
    if not (role and ck and cid and aid):
        return {}, (f"could not detect key columns on call/account "
                    f"(role={role}, acct_key={ck}, call_id={cid}, acct_id={aid})")
    fa = (f"case when upper(c.{role}) in ('MSL', 'MEDICAL', 'MEDICAL AFFAIRS') then 'Medical Affairs' "
          f"when upper(c.{role}) in ('REP', 'SALES', 'FIELD', 'FIELD SALES') then 'Field Sales' "
          f"else 'Other' end")
    extra = "".join(f", c.{x}" for x in (cdt, ctp, cst, cow) if x)
    activity = (f"select c.{cid} as call_id, c.{ck} as account_id, a.{anm} as account_name"
                + (f", a.{asp} as specialty" if asp else "")
                + f"{extra}, c.{role} as owner_role, {fa} as function_area\n"
                f"from {sch}.call c\nleft join {sch}.account a on a.{aid} = c.{ck}\n")
    owner_expr = f"c.{cow}" if cow else f"c.{role}"
    views = {
        "vw_field_call_activity":
            f"create or replace view {sch}.vw_field_call_activity as\n"
            f"{activity}where {fa} = 'Field Sales'",
        "vw_med_affairs_activity":
            f"create or replace view {sch}.vw_med_affairs_activity as\n"
            f"{activity}where {fa} = 'Medical Affairs'",
        "vw_field_reach_frequency":
            f"create or replace view {sch}.vw_field_reach_frequency as\n"
            f"with calls as (\n"
            f"  select c.{ck} as account_id, {owner_expr} as owner_id, c.{cid} as call_id\n"
            f"  from {sch}.call c\n  where {fa} = 'Field Sales'\n)\n"
            f"select account_id, count(distinct owner_id) as reach, count(call_id) as frequency\n"
            f"from calls group by account_id",
    }
    note = f"detected: role={role}, acct_key={ck}, call_id={cid}, acct_id={aid}"
    return views, note


# ── dbt model → Snowflake view translation (best-effort) ─────────────────────

_SOURCE_RE = re.compile(r"\{\{\s*source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)\s*\}\}")
_REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"](\w+)['\"]\s*\)\s*\}\}")


def extract_sources(sql: str) -> set[tuple[str, str]]:
    """All distinct (source_name, table_name) pairs a model's source() calls use."""
    return set(_SOURCE_RE.findall(sql))


# A few Spark/Databricks SQL functions the migrated view models use that Snowflake
# either lacks or types differently — not a general dialect translator, just the
# patterns actually seen in this project's models:
#   * YEAR()/MONTH()/QUARTER()/DAY() on a column that's really a string in the
#     raw bronze copy (Snowflake's EXTRACT-family refuses VARCHAR — wrap with
#     TRY_TO_DATE; harmless no-op if the arg is already a DATE/TIMESTAMP).
#   * DATE_FORMAT(expr, 'spark_pattern') — unknown function in Snowflake; the
#     equivalent is TO_CHAR(expr, 'snowflake_pattern').
_YMD_FUNCS = ("YEAR", "MONTH", "QUARTER", "DAY")
_SPARK_TO_SF_DATE_TOKENS = [   # longest tokens first so nothing gets partially clobbered
    ("yyyy", "YYYY"), ("MMMM", "MONTH"), ("EEEE", "DY"), ("MMM", "MON"), ("EEE", "DY"),
    ("HH", "HH24"), ("hh", "HH12"), ("yy", "YY"), ("MM", "MM"), ("dd", "DD"),
    ("mm", "MI"), ("ss", "SS"),
]


def _split_top_level_args(args: str) -> list[str]:
    """Split a function-call argument string on top-level commas only."""
    parts, depth, cur = [], 0, []
    for ch in args:
        if ch == "(":
            depth += 1; cur.append(ch)
        elif ch == ")":
            depth -= 1; cur.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(cur)); cur = []
        else:
            cur.append(ch)
    parts.append("".join(cur))
    return parts


def _rewrite_calls(sql: str, name: str, make_replacement) -> str:
    """Replace every top-level call ``name(args)`` (case-insensitive, paren-balanced)
    with ``make_replacement(args)``. Assumes calls to `name` don't nest inside
    each other, which holds for the functions this is used for."""
    pattern = re.compile(rf"\b{name}\s*\(", re.IGNORECASE)
    out, pos = [], 0
    for m in pattern.finditer(sql):
        if m.start() < pos:
            continue
        out.append(sql[pos:m.start()])
        i = m.end(); depth = 1; j = i
        while j < len(sql) and depth > 0:
            if sql[j] == "(":
                depth += 1
            elif sql[j] == ")":
                depth -= 1
            j += 1
        args = sql[i:j - 1]
        out.append(make_replacement(args))
        pos = j
    out.append(sql[pos:])
    return "".join(out)


def _translate_date_format_pattern(fmt: str) -> str:
    out = fmt
    for src, dst in _SPARK_TO_SF_DATE_TOKENS:
        out = out.replace(src, dst)
    return out


def spark_to_snowflake_functions(sql: str) -> str:
    for fn in _YMD_FUNCS:
        sql = _rewrite_calls(sql, fn, lambda args, fn=fn: f"{fn}(TRY_TO_DATE({args.strip()}))")

    def _date_format_repl(args: str) -> str:
        parts = _split_top_level_args(args)
        if len(parts) != 2:
            return f"DATE_FORMAT({args})"  # unexpected shape — leave visible, not silently wrong
        expr, fmt_lit = parts[0].strip(), parts[1].strip()
        sf_fmt = _translate_date_format_pattern(fmt_lit.strip("'\" "))
        quote = "'" if "'" in fmt_lit else '"'
        return f"TO_CHAR(TRY_TO_DATE({expr}), {quote}{sf_fmt}{quote})"

    return _rewrite_calls(sql, "DATE_FORMAT", _date_format_repl)


def translate_model_sql(sql: str, curated_schema: str, source_schema_map: dict) -> str:
    """source_schema_map: {source_name: fully-qualified target schema}. ref() always
    resolves to curated_schema (the finished PUB tables); source() is routed per
    source name — bronze sources need the raw layer, not the curated one, since
    these models read original (un-renamed) columns."""
    sql = re.sub(r"\{\{\s*config\([^)]*\)\s*\}\}", "", sql)              # strip config blocks
    sql = _REF_RE.sub(lambda m: f"{curated_schema}.{m.group(1)}", sql)
    sql = _SOURCE_RE.sub(
        lambda m: f"{source_schema_map.get(m.group(1), curated_schema)}.{m.group(2)}", sql)
    sql = sql.replace("`", "")                                            # backticks → bare
    sql = spark_to_snowflake_functions(sql)
    return sql.strip().rstrip(";")


def fetch_view_models(host, token, root="/Shared/sfglue/dbt/models"):
    """Return {model_name: sql} for every vw_* model in the workspace dbt project."""
    out, stack = {}, [root]
    while stack:
        path = stack.pop()
        try:
            listing = _dbx(host, token, "/api/2.0/workspace/list",
                           params=f"?path={urllib.parse.quote(path)}")
        except Exception:
            continue
        for obj in listing.get("objects", []):
            if obj.get("object_type") == "DIRECTORY":
                stack.append(obj["path"])
            elif obj["path"].rsplit("/", 1)[-1].startswith("vw_"):
                name = obj["path"].rsplit("/", 1)[-1].removesuffix(".sql")
                exp = _dbx(host, token, "/api/2.0/workspace/export",
                           params=f"?path={urllib.parse.quote(obj['path'])}&format=SOURCE")
                out[name] = base64.b64decode(exp.get("content", "")).decode("utf-8", "replace")
    return out


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf-account", required=True)
    ap.add_argument("--sf-user", required=True)
    ap.add_argument("--sf-password", required=True)
    ap.add_argument("--sf-role", default="SYSADMIN")
    ap.add_argument("--sf-warehouse", default="COMPUTE_WH")
    ap.add_argument("--database", default="VEEVA_CRM")
    ap.add_argument("--schema", default="PUB")
    ap.add_argument("--dbx-host", required=True)
    ap.add_argument("--dbx-token", required=True)
    ap.add_argument("--dbx-warehouse", required=True)
    ap.add_argument("--catalog", default="workspace")
    ap.add_argument("--gold-schema", default="gold")
    ap.add_argument("--silver-schema", default="silver")
    ap.add_argument("--bronze-schema", default="bronze")
    ap.add_argument("--raw-schema", default="RAW", help="Snowflake schema to hold "
                    "un-renamed bronze copies (needed by views that read source('bronze', ...))")
    ap.add_argument("--skip-data", action="store_true")
    ap.add_argument("--skip-views", action="store_true")
    a = ap.parse_args()

    import snowflake.connector
    con = snowflake.connector.connect(account=a.sf_account, user=a.sf_user,
                                      password=a.sf_password, role=a.sf_role or None)
    cur = con.cursor()
    sch = f"{a.database}.{a.schema}"

    print(f"== Snowflake bootstrap ({a.sf_account})")
    cur.execute(f"create warehouse if not exists {a.sf_warehouse} "
                f"warehouse_size='XSMALL' auto_suspend=60 initially_suspended=true")
    cur.execute(f"use warehouse {a.sf_warehouse}")
    cur.execute(f"create database if not exists {a.database}")
    cur.execute(f"create schema if not exists {sch}")
    cur.execute(f"use schema {sch}")
    print(f"  ✓ warehouse {a.sf_warehouse}, database {a.database}, schema {a.schema}")

    col_cache = {}

    def copy_table(src_schema, tname, dest_sch=None):
        dest_sch = dest_sch or sch
        _, cols = dbx_sql(a.dbx_host, a.dbx_token, a.dbx_warehouse,
                          f"select column_name, full_data_type from {a.catalog}.information_schema.columns "
                          f"where table_schema='{src_schema}' and table_name='{tname}' "
                          f"order by ordinal_position", a.catalog)
        if not cols:
            print(f"  – {tname:<32} not found in {src_schema}, skipped")
            return False
        if dest_sch == sch:
            col_cache[tname] = [c for c, _ in cols]
        ddl_cols = ", ".join(f'"{c.upper()}" {sf_type(t)}' for c, t in cols)
        cur.execute(f"create or replace table {dest_sch}.{tname} ({ddl_cols})")
        n = 0
        if not a.skip_data:
            _, rows = dbx_sql(a.dbx_host, a.dbx_token, a.dbx_warehouse,
                              f"select * from {a.catalog}.{src_schema}.`{tname}`", a.catalog)
            if rows:
                ph = ", ".join(["%s"] * len(cols))
                cur.executemany(f"insert into {dest_sch}.{tname} values ({ph})", rows)
                n = len(rows)
        print(f"  ✓ {tname:<32} {len(cols)} col(s), {n} row(s)  → {dest_sch}")
        return True

    print(f"\n== Copying {a.catalog}.{a.gold_schema} tables → {sch}")
    _, tabs = dbx_sql(a.dbx_host, a.dbx_token, a.dbx_warehouse,
                      f"select table_name from {a.catalog}.information_schema.tables "
                      f"where table_schema='{a.gold_schema}' order by table_name", a.catalog)
    made = sum(copy_table(a.gold_schema, t) for (t,) in tabs)

    # The vw_* views read the CURATED layer (call, call_expense, …) — copy those
    # base tables from silver so the views have something to stand on.
    print(f"\n== Copying curated base tables ({a.silver_schema}) → {sch}")
    made += sum(copy_table(a.silver_schema, t) for t in BASE_TABLES)

    if not a.skip_views:
        print(f"\n== Recreating analytics views in {sch}")
        corrected, note = build_corrected_views(col_cache.get("call", []),
                                                col_cache.get("account", []), sch)
        print(f"  ({note})" if note else "")
        models = {}
        try:
            models = fetch_view_models(a.dbx_host, a.dbx_token)
        except Exception as exc:  # noqa: BLE001
            print(f"  (workspace model fetch failed: {exc} — using corrected views only)")

        # Some views read RAW bronze columns via source('bronze', X) instead of the
        # curated layer — discover every such table across all models and copy it
        # (untouched columns) into its own raw schema before creating those views.
        bronze_tables = sorted({tbl for m in models.values()
                                for src, tbl in extract_sources(m) if src == "bronze"})
        source_schema_map = {"silver": sch, "gold": sch}
        if bronze_tables:
            raw_sch = f"{a.database}.{a.raw_schema}"
            print(f"\n== Copying raw bronze tables referenced by view models → {raw_sch}")
            cur.execute(f"create schema if not exists {raw_sch}")
            for t in bronze_tables:
                copy_table(a.bronze_schema, t, dest_sch=raw_sch)
            source_schema_map["bronze"] = raw_sch

        # corrected versions win over the (defective) migrated models
        names = sorted(set(models) | set(corrected))
        for name in names:
            if name in corrected:
                sql = corrected[name]
                tag = "corrected source logic"
            else:
                sql = f"create or replace view {sch}.{name} as\n" + \
                      translate_model_sql(models[name], sch, source_schema_map)
                tag = "from dbt model"
            try:
                cur.execute(sql)
                print(f"  ✓ {name:<32} ({tag})")
            except Exception as exc:  # noqa: BLE001
                print(f"  ✗ {name}: {str(exc)[:200]}")
                print(f"      --- full SQL for {name} ---\n{sql}\n      --- end ---")

    print(f"\n== done — {made} table(s) in {sch}. Point the app's Connect page at the "
          f"new account and re-test the Snowflake connection.")
    cur.close(); con.close()


if __name__ == "__main__":
    main()
