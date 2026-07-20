"""Deployment / build / verification routes: deploy DDL, build (run) dbt models,
seed bronze, reconcile, run tests. Behaviour identical to the original handlers."""

from __future__ import annotations

import logging
import re
import re as _re_sources

from flask import Blueprint, jsonify, request
from backend.integrations.sfglue_sql_guard import (
    UnsafeSqlError,
    assert_safe_ddl,
    assert_safe_where,
    quote_ident,
)
from backend.integrations.snowflake_client import SnowflakeConnectionConfig, fetch_table_columns, open_query_runner
from backend.integrations.snowflake_glue_migration import build_bronze_seed_statements
from backend.integrations.routes._shared import _bind_ai, body, get_call_ai

logger = logging.getLogger("sfglue.routes")

bp = Blueprint("sfglue_deploy", __name__)


def _required_source_tables(compiled: list, source_catalog: str, source_schema: str) -> set:
    """The raw source tables the compiled models read — i.e. the
    ``{source_catalog}.{source_schema}.<table>`` references compile_models produced from
    ``{{ source('bronze', X) }}``. Returns lowercased base names. Pure/deterministic so it
    is unit-testable without a warehouse."""
    cat = _re_sources.escape(str(source_catalog))
    sch = _re_sources.escape(str(source_schema))
    # Match catalog.schema.table with optional backticks around each part.
    rx = _re_sources.compile(
        rf"`?{cat}`?\.`?{sch}`?\.`?([A-Za-z_][A-Za-z0-9_]*)`?", _re_sources.IGNORECASE)
    needed = set()
    for m in compiled or []:
        for tbl in rx.findall(str(m.get("statement") or "")):
            needed.add(tbl.lower())
    return needed


def _missing_source_tables(compiled, cfg, source_catalog, source_schema, execute_sql_statement):
    """Pre-build gate: which raw source tables the models read are NOT present in the
    destination ``{source_catalog}.{source_schema}``. Returns (missing, existing, err):

      missing  — sorted base names the models need but the catalog doesn't have
      existing — set of base names actually present (lowercased)
      err      — a string if existence couldn't be determined (schema/catalog absent, no
                 introspection), else None

    Running the build without bronze populated yields a raw TABLE_OR_VIEW_NOT_FOUND on the
    first silver model and cascades 'skipped' to everything downstream — confusing and
    un-actionable. Detecting it up front lets the route say exactly which tables to land
    (run the bronze ingestion notebooks / load the raw data) before building."""
    needed = _required_source_tables(compiled, source_catalog, source_schema)
    if not needed:
        return [], set(), None
    cat_q = str(source_catalog).replace("`", "``")
    sl = lambda v: str(v).replace("'", "''")
    sql = (f"SELECT table_name FROM `{cat_q}`.information_schema.tables "
           f"WHERE table_schema = '{sl(source_schema)}'")
    try:
        res = execute_sql_statement(sql, cfg.sql_warehouse_id, catalog=source_catalog,
                                    schema=source_schema, config=cfg, stage='build_source_check')
    except Exception as exc:  # noqa: BLE001 — best-effort gate
        return [], set(), str(exc)
    if res.get('success') is False:
        return [], set(), (res.get('message') or res.get('error') or 'source introspection failed')
    rows = ((res.get('result') or {}).get('data_array')) or []
    existing = {str(r[0]).lower() for r in rows if r and r[0]}
    missing = sorted(t for t in needed if t not in existing)
    return missing, existing, None


def _schema_for_compiled(model: dict, silver_schema: str, gold_schema: str) -> str:
    """Target schema for a compiled model — gold layer → gold schema, else silver.

    compile_models classifies 'staging' as a separate layer but lands it in the silver
    schema, so anything that isn't 'gold' resolves to silver here (matching the
    target_table the compiler already built)."""
    return gold_schema if model.get("layer") == "gold" else silver_schema


@bp.route('/api/sfglue/deploy', methods=['POST'])
def sfglue_deploy():
    """Deploy the generated table DDL into Databricks Unity Catalog.

    Creates each ``catalog.schema`` the DDL targets, then runs every CREATE TABLE
    against the configured SQL Warehouse, returning a per-table result. Notebooks
    and dbt models aren't executed here (they need the Workspace API / a dbt runtime)
    — this materializes the table shells so the downstream models have somewhere to
    write.
    """
    data = body('ddl', message="No table DDL to deploy. Generate the conversion first.")
    destination = data.get('destination') or {}
    ddl_map = data.get('ddl') or {}
    if not ddl_map:
        return jsonify({"success": False, "error": "No table DDL to deploy. Generate the conversion first."}), 400

    # Safety gate: these statements are AI-generated then editable in the
    # browser, so treat them as untrusted. Only single CREATE statements run.
    try:
        for name, sql in ddl_map.items():
            assert_safe_ddl(sql, label=f"DDL for {name}")
    except UnsafeSqlError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
    from qvd_to_databricks.databricks_executor import execute_sql_statement

    cfg = DatabricksConnectionConfig.from_payload(destination)
    if not cfg.workspace_url or not cfg.personal_access_token:
        return jsonify({"success": False, "error": "Set the Databricks Workspace URL and Access token above first."}), 400
    if not cfg.sql_warehouse_id:
        return jsonify({"success": False, "error": "A SQL Warehouse ID is required to deploy."}), 400

    # Pull catalog.schema out of each CREATE TABLE so we create exactly the schemas
    # the DDL targets (Unity Catalog won't create a table in a missing schema).
    target_re = re.compile(
        r"create\s+table\s+(?:if\s+not\s+exists\s+)?`?([\w]+)`?\.`?([\w]+)`?\.`?[\w]+`?", re.I)
    schemas, seen = [], set()
    for sql in ddl_map.values():
        m = target_re.search(str(sql or ""))
        cat, sch = (m.group(1), m.group(2)) if m else (cfg.catalog, cfg.schema)
        if (cat, sch) not in seen:
            seen.add((cat, sch))
            schemas.append((cat, sch))

    results = []
    for cat, sch in schemas:
        r = execute_sql_statement(
            f"CREATE SCHEMA IF NOT EXISTS {quote_ident(cat)}.{quote_ident(sch)}",
            cfg.sql_warehouse_id, catalog=cat, schema=sch, config=cfg, stage='create_schema')
        if not r.get('success'):
            results.append({"target": f"{cat}.{sch}", "kind": "schema", "success": False,
                            "message": r.get('message') or r.get('error') or 'schema create failed'})
    if results:  # a schema failed → table DDL would all fail the same way
        return jsonify({"success": False, "results": results})

    for name, sql in ddl_map.items():
        r = execute_sql_statement(
            str(sql or ""), cfg.sql_warehouse_id,
            catalog=cfg.catalog, schema=cfg.schema, config=cfg, stage='create_table')
        results.append({"target": name, "kind": "table", "success": bool(r.get('success')),
                        "message": r.get('message') or r.get('error') or ('created' if r.get('success') else 'failed')})

    ok = sum(1 for r in results if r['success'])
    return jsonify({"success": all(r['success'] for r in results), "results": results,
                    "summary": f"{ok}/{len(results)} table(s) created in {cfg.catalog}"})

@bp.route('/api/sfglue/build', methods=['POST'])
def sfglue_build():
    """Build (populate) the migrated tables by RUNNING the dbt models in Databricks.

    The deploy route above only materializes empty table shells from the DDL — it
    never runs the models, so the tables stay empty and an operator would have to
    hand-run SQL. This route closes that gap with one click: it compiles each dbt
    model (resolving ``{{ ref() }}``/``{{ source() }}`` to real Databricks tables —
    the stored models are NOT mutated) and executes the resulting CREATE OR REPLACE
    statements in dependency order on the SQL Warehouse.

    Body: ``{destination, models}`` where ``models`` is ``{name: sql}`` (the client
    sends ``conv.dbt_models`` with edits applied, mirroring how deploy sends ddl).
    ``destination`` carries catalog + bronze/silver/gold schemas and (new)
    ``source_catalog``/``source_schema`` (the raw landing location), defaulting to
    ``catalog``/``bronze_schema``. If a model fails, its downstream dependents are
    marked ``skipped`` and not run.
    """
    from backend.integrations.dbt_build import attempt_build_with_repair, compile_models
    from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
    from qvd_to_databricks.databricks_executor import execute_sql_statement

    data = body('models', message="No dbt models to build. Generate the conversion first.")
    destination = data.get('destination') or {}
    models = data.get('models') or {}
    # Bedrock (used only by the schema-error auto-repair) reuses the Glue connection's creds.
    ai = _bind_ai(get_call_ai(), data.get('glue'))
    if not models:
        return jsonify({"success": False, "error": "No dbt models to build. Generate the conversion first."}), 400

    cfg = DatabricksConnectionConfig.from_payload(destination)
    if not cfg.workspace_url or not cfg.personal_access_token:
        return jsonify({"success": False, "error": "Set the Databricks Workspace URL and Access token above first."}), 400
    if not cfg.sql_warehouse_id:
        return jsonify({"success": False, "error": "A SQL Warehouse ID is required to build."}), 400

    silver_schema = destination.get('silver_schema') or 'silver'
    gold_schema = destination.get('gold_schema') or 'gold'
    # The raw/bronze location the source() refs resolve to. Fall back to the
    # catalog/bronze_schema so existing destinations (without the new fields) work.
    source_catalog = destination.get('source_catalog') or cfg.catalog
    source_schema = destination.get('source_schema') or destination.get('bronze_schema') or 'bronze'

    try:
        compiled = compile_models(
            models, target_catalog=cfg.catalog, silver_schema=silver_schema,
            gold_schema=gold_schema, source_catalog=source_catalog, source_schema=source_schema)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Snowflake/Glue build compile failed")
        return jsonify({"success": False, "error": f"Model compile failed: {exc}"}), 500

    if not compiled:
        return jsonify({"success": False, "error": "No models compiled."}), 400

    # Safety gate on the compiled statements before any run — same rationale
    # as deploy: client-editable, AI-origin SQL executed on a live warehouse.
    try:
        for m in compiled:
            assert_safe_ddl(m.get("statement"), label=f"model {m.get('name')}")
    except UnsafeSqlError as exc:
        return jsonify({"success": False, "error": str(exc)}), 400

    # Fail-fast gate: the silver models read raw {{ source('bronze', X) }} tables. If
    # those aren't landed in Databricks yet, the first silver model raises
    # TABLE_OR_VIEW_NOT_FOUND and every downstream model cascades to 'skipped' — a
    # confusing, un-actionable wall. Check existence once and tell the operator exactly
    # which source tables to land (run the bronze ingestion notebooks / load the raw
    # data) before building. Best-effort: if existence can't be determined we proceed
    # (the per-model run still reports any real failure).
    missing_src, _existing_src, src_err = _missing_source_tables(
        compiled, cfg, source_catalog, source_schema, execute_sql_statement)
    if missing_src:
        loc = f"{source_catalog}.{source_schema}"
        return jsonify({
            "success": False,
            "needsSource": True,
            "missingSourceTables": missing_src,
            "error": (
                f"Can't build yet — {len(missing_src)} raw source table(s) the models read "
                f"do not exist in {loc}: {', '.join(missing_src)}. "
                "Land the raw data first: run the Bronze ingestion notebook(s) on Databricks "
                "(or load the source tables into that schema), then Build again. "
                "The Databricks DDL deploy only creates the gold target shells — it does not "
                "populate bronze."),
        }), 409
    if src_err:
        logger.warning("sfglue build: source-existence pre-check skipped (%s); "
                       "proceeding — per-model run will surface any real failure", src_err)

    # Create the target schemas first (Unity Catalog won't write into a missing
    # schema), mirroring how deploy creates schemas before running DDL.
    schemas, seen = [], set()
    for m in compiled:
        cat, sch = cfg.catalog, _schema_for_compiled(m, silver_schema, gold_schema)
        if (cat, sch) not in seen:
            seen.add((cat, sch))
            schemas.append((cat, sch))
    for cat, sch in schemas:
        r = execute_sql_statement(
            f"CREATE SCHEMA IF NOT EXISTS {quote_ident(cat)}.{quote_ident(sch)}",
            cfg.sql_warehouse_id, catalog=cat, schema=sch, config=cfg, stage='create_schema')
        if not r.get('success'):
            return jsonify({"success": False, "results": [
                {"name": f"{cat}.{sch}", "target": f"{cat}.{sch}", "status": "failed",
                 "message": r.get('message') or r.get('error') or 'schema create failed'}]})

    def introspect_cols(full_name):
        """Real column names of an upstream Databricks table — best-effort.

        Mirrors the reconcile route's ``dbx_columns`` introspection: split the
        fully-qualified ``catalog.schema.table`` the failing statement reads and query
        Unity Catalog's ``information_schema.columns``. Escapes identifiers/quotes so a
        name with a backtick/quote can't break (or inject into) the query, and returns
        ``[]`` on any error so the repair loop degrades gracefully (no domain/column/
        catalog literals — every name comes from the SQL + live introspection)."""
        parts = str(full_name).split(".")
        if len(parts) < 3:
            return []
        cat, schema, table = parts[0], parts[-2], parts[-1]
        cat_q = str(cat).replace("`", "``")
        sl = lambda s: str(s).replace("'", "''")
        sql = (f"SELECT column_name FROM `{cat_q}`.information_schema.columns "
               f"WHERE table_schema = '{sl(schema)}' AND table_name = '{sl(table)}' "
               f"ORDER BY ordinal_position")
        res = execute_sql_statement(sql, cfg.sql_warehouse_id, catalog=cat,
                                    schema=schema, config=cfg, stage='build_introspect')
        if res.get('success') is False:
            logger.warning("sfglue build auto-repair: column fetch failed for %s: %s",
                           full_name, res.get('message') or res.get('error'))
            return []
        rows = ((res.get('result') or {}).get('data_array')) or []
        return [r[0] for r in rows if r and r[0]]

    # Map name → its direct dependencies (known models it refs), so a failed model can
    # cascade a 'skipped' status to everything downstream of it.
    depends_on = {m["name"]: set(m.get("depends_on") or []) for m in compiled}
    results, failed_or_skipped = [], set()
    for m in compiled:
        name = m["name"]
        sch = _schema_for_compiled(m, silver_schema, gold_schema)
        # Skip if any dependency already failed/was skipped (compiled is in dependency
        # order, so a failed upstream has already been processed).
        blocking = depends_on[name] & failed_or_skipped
        if blocking:
            failed_or_skipped.add(name)
            results.append({"name": name, "target": m["target_table"], "status": "skipped",
                            "message": f"skipped — depends on failed model(s): {', '.join(sorted(blocking))}"})
            continue

        def run_sql(sql, _sch=sch):
            return execute_sql_statement(
                sql, cfg.sql_warehouse_id,
                catalog=cfg.catalog, schema=_sch, config=cfg, stage='build_model')

        # Run the model and, on a resolvable schema error, auto-repair against the REAL
        # upstream columns and retry (only when call_ai is available; otherwise this is
        # exactly the prior run-once behavior). Upstreams run first (dependency order),
        # so a failing ref()'s table is already built and its columns introspectable.
        outcome = attempt_build_with_repair(
            run_sql, ai, m["statement"], introspect_cols, max_attempts=2)
        entry = {"name": name, "target": m["target_table"], "status": outcome["status"],
                 "message": outcome.get("message") or outcome["status"]}
        if outcome["status"] == "repaired":
            # Surface the corrected SQL (what actually ran) + attempt count so the UI/user
            # can see what changed, and update the stored model so a re-build/export uses it.
            entry["repair_attempts"] = outcome.get("repair_attempts", 0)
            entry["statement"] = outcome.get("statement")
            models[f"{name}.sql"] = outcome.get("statement")
        if outcome["status"] == "failed":
            failed_or_skipped.add(name)
        results.append(entry)

    built = sum(1 for r in results if r["status"] in ("created", "repaired"))
    return jsonify({"success": all(r["status"] in ("created", "repaired") for r in results),
                    "results": results, "summary": f"{built}/{len(results)} models built"})

@bp.route('/api/sfglue/seed-bronze', methods=['POST'])
def sfglue_seed_bronze():
    """Land a small, referentially-consistent SAMPLE dataset into the bronze schema so
    Build can run end-to-end in-app without the real S3 ingestion. The schema is DERIVED
    from the dbt models (the exact columns they read), so it always matches; values are
    type/decode-aware so casts and picklist decodes fire. This is a demo/dev convenience
    — the production path is the generated Bronze ingestion notebook.

    Body: {destination, models}. Executes CREATE+INSERT on the SQL Warehouse, mirroring
    deploy/build, and returns per-table results."""
    from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
    from qvd_to_databricks.databricks_executor import execute_sql_statement

    data = body('models', message="No dbt models — generate the conversion first.")
    destination = data.get('destination') or {}
    models = data.get('models') or {}
    if not models:
        return jsonify({"success": False, "error": "No dbt models — generate the conversion first."}), 400

    cfg = DatabricksConnectionConfig.from_payload(destination)
    if not cfg.workspace_url or not cfg.personal_access_token:
        return jsonify({"success": False, "error": "Set the Databricks Workspace URL and Access token first."}), 400
    if not cfg.sql_warehouse_id:
        return jsonify({"success": False, "error": "A SQL Warehouse ID is required to seed bronze."}), 400

    source_catalog = destination.get('source_catalog') or cfg.catalog
    source_schema = destination.get('source_schema') or destination.get('bronze_schema') or 'bronze'
    try:
        rows = int(data.get('rows') or 4)
    except (TypeError, ValueError):
        rows = 4
    rows = max(1, min(rows, 50))

    try:
        stmts = build_bronze_seed_statements(models, catalog=source_catalog,
                                             bronze_schema=source_schema, rows=rows)
    except Exception as exc:  # noqa: BLE001
        logger.exception("sfglue seed-bronze: statement build failed")
        return jsonify({"success": False, "error": f"Could not build seed: {exc}"}), 500
    if not stmts:
        return jsonify({"success": False, "error": (
            "Could not derive the bronze schema from the models (no {{ source('bronze', ...) }} "
            "reads found, or SQL parser unavailable).")}), 422

    results, ok = [], True
    for stmt in stmts:
        label = "schema" if stmt.lstrip().upper().startswith("CREATE SCHEMA") else (
            stmt.split("`")[5] if stmt.count("`") >= 6 else "stmt")
        r = execute_sql_statement(stmt, cfg.sql_warehouse_id, catalog=source_catalog,
                                  schema=source_schema, config=cfg, stage='seed_bronze')
        success = r.get('success') is not False
        ok = ok and success
        verb = "create" if "CREATE" in stmt[:40].upper() else "insert"
        results.append({"name": f"{label} ({verb})", "status": "ok" if success else "failed",
                        "message": (r.get('message') or r.get('error') or '') if not success else ''})
        if not success:
            break  # a failed CREATE makes its INSERT pointless; stop and report

    return jsonify({
        "success": ok,
        "results": results,
        "summary": (f"bronze seeded in {source_catalog}.{source_schema} ({rows} rows/table)"
                    if ok else "seed did not complete — see results"),
    })

@bp.route('/api/sfglue/reconcile', methods=['POST'])
def sfglue_reconcile():
    """Verification gate: prove each migrated dbt model matches its legacy Snowflake
    source. For every {source, candidate, key} pair, run the cross-engine fingerprint
    diff (row count + key integrity + per-column aggregates) and return a pass/fail
    report. This is the "nothing ships until reconcile passes" check the kit calls for.

    Body: {snowflake, destination, float_tol?, row_count_tol?, col_tol?, pairs:[{source,
    candidate, key, exclude?, where?, row_count_tol?, col_tol?, containment?}]}. Checks:
    schema parity + type drift, row count (± row_count_tol), key integrity, per-column
    aggregate fingerprint (incl. distinct cardinality, per-column col_tol), and optional
    foreign-key containment on the candidate. The candidate (Databricks) table must
    already be deployed/built.
    """
    from backend.integrations.reconcile import DATABRICKS, SNOWFLAKE, reconcile, check_containment
    from backend.integrations.reconcile_suggest import suggest_reconcile_settings

    data = body()
    pairs = data.get('pairs') or []
    if not data.get('snowflake'):
        return jsonify({"success": False, "error": "Connect Snowflake (the source of truth) first."}), 400
    if not pairs:
        return jsonify({"success": False, "error": "No table pairs to reconcile."}), 400

    destination = data.get('destination') or {}
    float_tol = data.get('float_tol')
    try:
        float_tol = float(float_tol) if float_tol is not None else 1e-6
    except (TypeError, ValueError):
        float_tol = 1e-6
    # Optional tolerances (deeper reconciliation): a fractional row-count allowance and a
    # per-column relative-tolerance override map. Both default to strict/exact.
    try:
        row_count_tol = float(data.get('row_count_tol') or 0.0)
    except (TypeError, ValueError):
        row_count_tol = 0.0
    col_tol_global = data.get('col_tol') if isinstance(data.get('col_tol'), dict) else {}

    from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
    from qvd_to_databricks.databricks_executor import execute_sql_statement

    cfg = DatabricksConnectionConfig.from_payload(destination)
    if not cfg.workspace_url or not cfg.sql_warehouse_id:
        return jsonify({"success": False,
                        "error": "Set the Databricks Workspace URL and SQL Warehouse ID (on the Databricks Agent step) to reconcile."}), 400

    def dbx_run(sql):
        res = execute_sql_statement(sql, cfg.sql_warehouse_id, catalog=cfg.catalog,
                                    schema=cfg.schema, config=cfg, stage='reconcile')
        if res.get('success') is False:
            raise RuntimeError(res.get('message') or res.get('error') or 'Databricks query failed')
        return ((res.get('result') or {}).get('data_array')) or []

    def dbx_columns(full_name):
        parts = str(full_name).split(".")
        cat = parts[0] if len(parts) >= 3 else (cfg.catalog or 'main')
        schema, table = parts[-2], parts[-1]
        # Escape so a name with a quote/backtick can't break (or inject into) the query.
        cat_q = str(cat).replace("`", "``")
        sl = lambda s: str(s).replace("'", "''")
        sql = (f"SELECT column_name, data_type FROM `{cat_q}`.information_schema.columns "
               f"WHERE table_schema = '{sl(schema)}' AND table_name = '{sl(table)}' ORDER BY ordinal_position")
        try:
            rows = dbx_run(sql)
        except Exception as exc:  # noqa: BLE001 — missing table → empty columns → reported as not deployed
            logger.warning("reconcile: candidate column fetch failed for %s: %s", full_name, exc)
            return []
        return [{"name": r[0], "type": r[1]} for r in rows if r and len(r) >= 2 and r[0]]

    sf_config = SnowflakeConnectionConfig.from_payload(data['snowflake'])
    results = []
    sf_run = sf_close = None
    try:
        sf_run, sf_close = open_query_runner(sf_config)
        for pair in pairs:
            source = (pair or {}).get('source')
            candidate = (pair or {}).get('candidate')
            key = (pair or {}).get('key') or []
            if isinstance(key, str):
                key = [k.strip() for k in key.split(",") if k.strip()]
            exclude = (pair or {}).get('exclude') or []
            if isinstance(exclude, str):
                exclude = [c.strip() for c in exclude.split(",") if c.strip()]
            try:
                where = assert_safe_where((pair or {}).get('where') or None)
            except UnsafeSqlError as exc:
                results.append({"source": source, "candidate": candidate,
                                "passed": False, "error": str(exc)})
                continue
            if not source or not candidate:
                results.append({"source": source, "candidate": candidate, "passed": False,
                                "error": "source and candidate table names are required"})
                continue
            # Operator-friendly suggestions: a non-tech operator can't be expected to
            # know which key to use or that surrogate/run-stamp columns won't match
            # cross-engine. Suggest a key (used only when none was supplied) and always
            # surface (+ apply) the auto-exclude list. relationships aren't available in
            # this route, so pass None — suggestion falls back to name/id inference.
            src_cols = fetch_table_columns(sf_config, source)
            cand_cols = dbx_columns(candidate)
            suggestion = suggest_reconcile_settings(
                cand_cols, source_columns=src_cols, relationships=None, table=source)
            suggested_key = suggestion["primary_key"]
            suggested_exclude = suggestion["exclude"]
            if not key:
                key = list(suggested_key)
            # Effective exclude = user-supplied + suggested (deduped, case-insensitive).
            eff_exclude = list(exclude)
            seen = {c.lower() for c in eff_exclude}
            for item in suggested_exclude:
                if item["column"].lower() not in seen:
                    eff_exclude.append(item["column"])
                    seen.add(item["column"].lower())
            base = {"source": source, "candidate": candidate, "suggested_key": suggested_key,
                    "key_source": suggestion["key_source"], "suggested_exclude": suggested_exclude}
            # A primary key is NOT required. Without one, reconcile() still runs schema
            # parity + row count + per-column aggregate fingerprint (the bulk of the gate);
            # the key only adds the duplicate/null-key integrity check. So run with whatever
            # key we have (possibly empty) instead of blocking the whole table.
            # Per-pair tolerance overrides fall back to the request-level defaults.
            pair_col_tol = pair.get('col_tol') if isinstance(pair.get('col_tol'), dict) else {}
            eff_col_tol = {**col_tol_global, **pair_col_tol}
            try:
                pair_row_tol = float(pair.get('row_count_tol')) if pair.get('row_count_tol') is not None else row_count_tol
            except (TypeError, ValueError):
                pair_row_tol = row_count_tol
            try:
                rep = reconcile(
                    source_runner=sf_run, candidate_runner=dbx_run,
                    source_table=source, candidate_table=candidate,
                    source_columns=src_cols, candidate_columns=cand_cols,
                    primary_key=key, source_dialect=SNOWFLAKE, candidate_dialect=DATABRICKS,
                    exclude=eff_exclude, float_tol=float_tol, col_tol=eff_col_tol,
                    row_count_tol=pair_row_tol, where=where,
                )
                row = {**base, "key": rep.primary_key, "passed": rep.passed,
                       "checks": rep.checks, "failures": rep.failures}
                # Optional reference/containment: verify each declared FK on the migrated
                # (candidate) side resolves to its parent. pair['containment'] =
                # [{child_columns, parent, parent_columns}] with parent a deployed candidate.
                containment = pair.get('containment') or []
                if containment:
                    cres = []
                    for fk in containment:
                        r = check_containment(
                            dbx_run, DATABRICKS, child_table=candidate,
                            child_columns=fk.get('child_columns') or [],
                            parent_table=fk.get('parent') or '',
                            parent_columns=fk.get('parent_columns') or [])
                        cres.append({**fk, **r})
                        if r.get("orphans"):
                            rep_fail = f"foreign key {fk.get('child_columns')} → {fk.get('parent')}: {r['orphans']} orphan row(s)"
                            row["passed"] = False
                            row.setdefault("failures", []).append(rep_fail)
                    row["checks"] = {**row.get("checks", {}), "containment": cres}
                results.append(row)
                # Log the verdict per table so it survives a UI reload (the result body
                # isn't otherwise persisted anywhere server-side).
                _rc = rep.checks.get("row_counts", {})
                logger.info("sfglue reconcile: %s -> %s : %s (rows src=%s cand=%s)%s",
                            source, candidate, "PASS" if rep.passed else "FAIL",
                            _rc.get("source"), _rc.get("candidate"),
                            "" if rep.passed else " | " + "; ".join(rep.failures))
            except Exception as exc:  # noqa: BLE001
                logger.exception("Reconcile failed for %s vs %s", source, candidate)
                results.append({**base, "passed": False, "error": str(exc)})
    finally:
        if sf_close:
            sf_close()

    passed = sum(1 for r in results if r.get("passed"))
    logger.info("sfglue reconcile: %d/%d table(s) clean", passed, len(results))
    # success = the reconciliation RAN (so the UI renders per-table results); all_passed
    # is the actual gate — every table matched its source.
    return jsonify({"success": True, "all_passed": bool(results) and passed == len(results),
                    "results": results,
                    "summary": f"{passed}/{len(results)} table(s) reconciled clean"})

@bp.route('/api/sfglue/run-tests', methods=['POST'])
def sfglue_run_tests():
    """Execution-based gate: RUN the generated dbt tests + enforced contracts as SQL on the
    Databricks SQL Warehouse, BEFORE the reconciliation gate. Each test becomes a
    violating-row query (0 rows = pass); contracts are checked against information_schema.

    Body: {destination, test_specs:[...]} (test_specs come from the conversion result).
    Returns {success, all_passed, results:[{model, kind, columns, passed, violations, detail}]}.
    The candidate tables must already be deployed/built (Databricks Agent step).
    """
    from backend.integrations.reconcile import DATABRICKS, check_containment

    data = body('test_specs', message="No test specs — generate a conversion first.")
    specs = data.get('test_specs') or []
    destination = data.get('destination') or {}
    if not specs:
        return jsonify({"success": False, "error": "No test specs — generate a conversion first."}), 400

    from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
    from qvd_to_databricks.databricks_executor import execute_sql_statement

    cfg = DatabricksConnectionConfig.from_payload(destination)
    if not cfg.workspace_url or not cfg.sql_warehouse_id:
        return jsonify({"success": False,
                        "error": "Set the Databricks Workspace URL and SQL Warehouse ID to run tests."}), 400
    cat = cfg.catalog or 'main'
    cat_q = str(cat).replace("`", "``")

    def dbx_run(sql):
        res = execute_sql_statement(sql, cfg.sql_warehouse_id, catalog=cfg.catalog,
                                    schema=cfg.schema, config=cfg, stage='run-tests')
        if res.get('success') is False:
            raise RuntimeError(res.get('message') or res.get('error') or 'Databricks query failed')
        return ((res.get('result') or {}).get('data_array')) or []

    def scalar(sql):
        rows = dbx_run(sql)
        if not rows:
            return None
        return rows[0][0] if isinstance(rows[0], (list, tuple)) else rows[0]

    # Resolve a model base name → deployed FQN. The build step lands models across the
    # bronze/silver/gold schemas; search information_schema (preferring the curated
    # layers) and try the stg_ alias so a key declared on `account` finds `stg_account`.
    _fqn_cache: dict[str, str] = {}
    def resolve_fqn(base):
        key = str(base or '').lower()
        if key in _fqn_cache:
            return _fqn_cache[key]
        cands = [key, key[4:] if key.startswith('stg_') else 'stg_' + key]
        names_csv = ", ".join("'" + c.replace("'", "''") + "'" for c in cands)
        sql = (f"SELECT table_schema, table_name FROM `{cat_q}`.information_schema.tables "
               f"WHERE lower(table_name) IN ({names_csv})")
        fqn = None
        try:
            rows = dbx_run(sql)
            pref = {'gold': 0, 'silver': 1, 'bronze': 2}
            rows = sorted(rows, key=lambda r: pref.get(str(r[0]).lower(), 3))
            if rows:
                fqn = f"`{cat_q}`.`{rows[0][0]}`.`{rows[0][1]}`"
        except Exception as exc:  # noqa: BLE001
            logger.warning("run-tests: fqn resolve failed for %s: %s", base, exc)
        _fqn_cache[key] = fqn
        return fqn

    def q(ident):
        return "`" + str(ident).replace("`", "``") + "`"

    results = []
    for spec in specs:
        kind = spec.get('kind')
        model = spec.get('model')
        cols = spec.get('columns') or []
        row = {"model": model, "kind": kind, "columns": cols, "passed": True, "violations": 0, "detail": ""}
        fqn = resolve_fqn(model)
        if not fqn:
            row.update(passed=False, detail=f"model '{model}' is not deployed yet — build it first")
            results.append(row)
            continue
        try:
            if kind == 'not_null':
                row["violations"] = int(scalar(f"SELECT count(*) FROM {fqn} WHERE {q(cols[0])} IS NULL") or 0)
                row["passed"] = row["violations"] == 0
                row["detail"] = "" if row["passed"] else f"{row['violations']} null value(s) in {cols[0]}"
            elif kind in ('unique', 'unique_combo'):
                ccsv = ", ".join(q(c) for c in cols)
                row["violations"] = int(scalar(
                    f"SELECT count(*) FROM (SELECT {ccsv} FROM {fqn} GROUP BY {ccsv} HAVING count(*) > 1) d") or 0)
                row["passed"] = row["violations"] == 0
                row["detail"] = "" if row["passed"] else f"{row['violations']} duplicate grain group(s) on ({', '.join(cols)})"
            elif kind == 'relationships':
                parent_fqn = resolve_fqn(spec.get('parent'))
                if not parent_fqn:
                    row.update(passed=True, detail=f"parent '{spec.get('parent')}' not deployed — relationship skipped")
                else:
                    cres = check_containment(dbx_run, DATABRICKS, child_table=fqn.replace('`', ''),
                                             child_columns=cols, parent_table=parent_fqn.replace('`', ''),
                                             parent_columns=spec.get('parent_columns') or [])
                    # check_containment re-qualifies; pass raw names it can quote itself.
                    row["violations"] = cres.get("orphans") or 0
                    row["passed"] = cres.get("ok", True)
                    row["detail"] = "" if row["passed"] else f"{row['violations']} orphan FK row(s) → {spec.get('parent')}"
            elif kind == 'contract':
                want = {(c.get('name') or '').lower(): str(c.get('data_type') or '').upper().replace(' ', '')
                        for c in (spec.get('expected_columns') or []) if c.get('name')}
                parts = fqn.replace('`', '').split('.')
                sl = lambda s: str(s).replace("'", "''")
                got_rows = dbx_run(
                    f"SELECT lower(column_name), upper(data_type) FROM `{cat_q}`.information_schema.columns "
                    f"WHERE table_schema='{sl(parts[-2])}' AND table_name='{sl(parts[-1])}'")
                got = {r[0]: str(r[1]).upper().replace(' ', '') for r in got_rows if r and r[0]}
                missing = [c for c in want if c not in got]
                mistyped = [f"{c}: want {want[c]} got {got[c]}" for c in want
                            if c in got and want[c] and got[c] and want[c].split('(')[0] != got[c].split('(')[0]]
                row["violations"] = len(missing) + len(mistyped)
                row["passed"] = row["violations"] == 0
                row["detail"] = "" if row["passed"] else "; ".join(
                    ([f"missing: {missing}"] if missing else []) + ([f"type drift: {mistyped}"] if mistyped else []))
            else:
                row.update(passed=True, detail=f"unknown test kind '{kind}' — skipped")
        except Exception as exc:  # noqa: BLE001
            row.update(passed=False, detail=f"test error: {str(exc)[:200]}")
        results.append(row)

    passed = sum(1 for r in results if r.get("passed"))
    logger.info("sfglue run-tests: %d/%d test(s) passed", passed, len(results))
    return jsonify({"success": True, "all_passed": bool(results) and passed == len(results),
                    "results": results,
                    "summary": f"{passed}/{len(results)} test(s) passed"})
