"""Routes for the Snowflake/Glue → Databricks/DBT migration flow (Phase 1).

Endpoints:
  POST /api/sfglue/snowflake/test-connection  — validate Snowflake creds
  POST /api/sfglue/glue/test-connection       — validate AWS Glue creds
  POST /api/sfglue/introspect                 — list Snowflake objects + Glue catalog/jobs
  POST /api/sfglue/lineage                    — build dataflow graph + duplicate findings
                                                + (AI) consolidation recommendations

Connection details are accepted per-request (not persisted server-side); the
frontend caches them in localStorage like the Qlik/Databricks connectors.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor

from flask import Response, jsonify, request

from backend.integrations.glue_client import (
    GlueConnectionConfig,
    fetch_glue_job_scripts,
    list_glue_catalog,
    list_glue_jobs,
    test_glue_connection,
)
from backend.integrations.snowflake_client import (
    SnowflakeConnectionConfig,
    fetch_object_ddl,
    fetch_table_columns,
    list_snowflake_objects,
    list_snowflake_relationships,
    list_snowflake_schemas,
    open_query_runner,
    test_snowflake_connection,
)
from backend.integrations.sfglue_sql_guard import (
    UnsafeSqlError,
    assert_safe_ddl,
    assert_safe_where,
    quote_ident,
)
from backend.integrations.snowflake_glue_lineage import (
    build_lineage,
    detect_duplicates,
    explain_business_logic,
    parse_pyspark_io,
    recommend,
)
from backend.integrations.snowflake_glue_migration import (
    build_bronze_seed_statements,
    build_dbt_project_files,
    build_precheck,
    explain_artifact,
    generate_postgres_bronze_ingestion,
    grade_migration_fidelity,
    run_conversion,
)
from backend.integrations.postgres_client import (
    PostgresConnectionConfig,
    list_postgres_objects,
    test_postgres_connection,
)

logger = logging.getLogger(__name__)


def _introspect_source_columns(destination: dict) -> dict:
    """Introspect the REAL columns of the raw source (bronze) tables from Databricks.

    The model converters guess source columns from the legacy Glue script otherwise (the
    MISSING-SCHEMA failure: a model reads a column the landed table doesn't actually
    have under that name). To ground them in the truth, run ONE Unity Catalog query against
    the source location the ``{{ source('bronze', X) }}`` refs resolve to — its
    ``source_catalog``/``source_schema`` (falling back to ``catalog``/``bronze_schema``,
    mirroring the build route) — and return ``{table_base_lower: [{name,type}, ...]}``.

    Best-effort and additive: when Databricks isn't configured/reachable or the query
    returns nothing, returns ``{}`` so the conversion DEGRADES to its prior behavior
    without crashing. No table/column/location literals are baked in — every name comes
    from config (the location) and live introspection (the columns).
    """
    destination = destination or {}
    try:
        from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
        from qvd_to_databricks.databricks_executor import execute_sql_statement
    except Exception as exc:  # noqa: BLE001 — introspection is best-effort
        logger.warning("sfglue convert: source-column introspection unavailable: %s", exc)
        return {}

    cfg = DatabricksConnectionConfig.from_payload(destination)
    if not cfg.workspace_url or not cfg.personal_access_token or not cfg.sql_warehouse_id:
        return {}  # no destination creds / warehouse → nothing to introspect against

    source_catalog = destination.get('source_catalog') or cfg.catalog
    source_schema = (destination.get('source_schema') or destination.get('bronze_schema')
                     or cfg.schema or 'bronze')
    if not source_catalog or not source_schema:
        return {}

    # Escape like the reconcile route: a catalog with a backtick can't break the
    # identifier, a schema with a quote can't break (or inject into) the WHERE literal.
    cat_q = str(source_catalog).replace("`", "``")
    schema_q = str(source_schema).replace("'", "''")
    sql = (f"SELECT table_name, column_name FROM `{cat_q}`.information_schema.columns "
           f"WHERE table_schema = '{schema_q}' ORDER BY table_name, ordinal_position")
    try:
        res = execute_sql_statement(sql, cfg.sql_warehouse_id, catalog=source_catalog,
                                    schema=source_schema, config=cfg, stage='introspect_source')
        if res.get('success') is False:
            raise RuntimeError(res.get('message') or res.get('error') or 'source introspection failed')
        rows = ((res.get('result') or {}).get('data_array')) or []
    except Exception as exc:  # noqa: BLE001 — degrade to no grounding
        logger.warning("sfglue convert: source-column introspection failed for %s.%s: %s",
                       source_catalog, source_schema, exc)
        return {}

    bronze_columns: dict = {}
    for r in rows:
        if not r or len(r) < 2 or not r[0] or not r[1]:
            continue
        table_base = str(r[0]).split(".")[-1].lower()
        bronze_columns.setdefault(table_base, []).append(r[1])
    logger.info(
        "sfglue convert: introspected %d source table(s) from %s.%s%s",
        len(bronze_columns), source_catalog, source_schema,
        (": " + ", ".join(sorted(bronze_columns)[:12])) if bronze_columns
        else " (EMPTY — model grounding will degrade; check Source catalog/schema points at your real tables)",
    )
    return bronze_columns


import re as _re_sources


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


def _ai_preflight(call_ai):
    """Is an LLM actually reachable right now? Returns (ok, reason).

    A configured provider can still be unusable at request time — the classic case is an
    expired AWS SSO token (config looks fine; every call 401s). We probe with one tiny call
    so the migration steps can refuse up front with an actionable notice instead of emitting
    deterministic scaffolds/placeholders that look like real output. Cheap: ~1 token."""
    if not call_ai:
        return False, ("No AI/LLM provider is connected. Connect a provider in Settings, "
                       "then retry — this step needs an LLM to translate the source logic.")
    try:
        # NB: do NOT pass a tiny max_tokens — call_ai enforces a minimum output-token budget
        # (MIN_REQUIRED_OUTPUT_TOKENS) and rejects anything below it, which would make a
        # HEALTHY provider look unreachable. max_tokens is only a ceiling; a "reply ok" prompt
        # generates ~2 tokens and stops, so omitting it (use the app default) stays cheap.
        call_ai("Reply with the single word: ok", system_prompt="Reply with exactly: ok",
                temperature=0, task="health")
        return True, None
    except Exception as exc:  # noqa: BLE001 — probe is best-effort
        logger.warning("sfglue AI preflight failed: %s", exc)
        return False, (f"The configured LLM provider isn't reachable right now: {exc}. "
                       "Fix the connection (for AWS Bedrock, refresh your SSO login: "
                       "`aws sso login`), then retry.")


def _aws_creds_from_glue(glue):
    """Pull AWS credentials out of a Glue connection payload so Bedrock can reuse them.

    Returns a dict {region, profile?, access_key_id?, secret_access_key?, session_token?}
    or None if the payload has nothing usable. A named profile wins over explicit keys
    (matching the Glue client's own precedence)."""
    if not isinstance(glue, dict):
        return None
    pick = lambda *ks: next((glue[k] for k in ks if glue.get(k)), None)
    region = pick("region", "aws_region", "awsRegion")
    profile = pick("profile_name", "profile", "profileName")
    ak = pick("access_key_id", "accessKeyId", "aws_access_key_id")
    sk = pick("secret_access_key", "secretAccessKey", "aws_secret_access_key")
    st = pick("session_token", "sessionToken", "aws_session_token")
    if profile:
        return {"region": region, "profile": profile}
    if ak and sk:
        creds = {"region": region, "access_key_id": ak, "secret_access_key": sk}
        if st:
            creds["session_token"] = st
        return creds
    return {"region": region} if region else None


def _bind_ai(call_ai, glue):
    """Wrap call_ai so Bedrock uses the Glue connection's AWS creds (if any), letting the
    AI work without separately configuring the server environment. No-op when there are no
    creds or no call_ai — returns the original callable."""
    if not call_ai:
        return call_ai
    creds = _aws_creds_from_glue(glue)
    if not creds:
        return call_ai

    def bound(prompt, *args, **kwargs):
        kwargs.setdefault("aws_creds", creds)
        return call_ai(prompt, *args, **kwargs)
    return bound


def register_snowflake_glue_routes(app, call_ai=None):
    @app.route('/api/sfglue/snowflake/test-connection', methods=['POST'])
    def sfglue_snowflake_test():
        data = request.get_json(silent=True) or {}
        config = SnowflakeConnectionConfig.from_payload(data.get('snowflake') or data)
        result = test_snowflake_connection(config)
        return jsonify(result), (200 if result.get('success') else 400)

    @app.route('/api/sfglue/snowflake/schemas', methods=['POST'])
    def sfglue_snowflake_schemas():
        data = request.get_json(silent=True) or {}
        config = SnowflakeConnectionConfig.from_payload(data.get('snowflake') or data)
        result = list_snowflake_schemas(config)
        return jsonify(result), (200 if result.get('success') else 400)

    @app.route('/api/sfglue/glue/test-connection', methods=['POST'])
    def sfglue_glue_test():
        data = request.get_json(silent=True) or {}
        config = GlueConnectionConfig.from_payload(data.get('glue') or data)
        result = test_glue_connection(config)
        return jsonify(result), (200 if result.get('success') else 400)

    @app.route('/api/sfglue/postgres/test-connection', methods=['POST'])
    def sfglue_postgres_test():
        data = request.get_json(silent=True) or {}
        config = PostgresConnectionConfig.from_payload(data.get('postgres') or data)
        result = test_postgres_connection(config)
        return jsonify(result), (200 if result.get('success') else 400)

    @app.route('/api/sfglue/postgres/introspect', methods=['POST'])
    def sfglue_postgres_introspect():
        """List Postgres tables (with columns), and flag which of them also exist in the
        connected Snowflake (i.e. were shipped there) so the UI can mark external origin.

        Body: {postgres, snowflake?}. Returns {success, tables, shipped_to_snowflake, errors}.
        """
        data = request.get_json(silent=True) or {}
        pg_config = PostgresConnectionConfig.from_payload(data.get('postgres') or data)
        pg = list_postgres_objects(pg_config)
        if not pg.get('success'):
            return jsonify({"success": False, "error": pg.get('error'), "tables": []}), 400
        pg_tables = pg.get('tables') or []
        shipped = []
        # Best-effort cross-check against Snowflake so the UI can show "also in Snowflake".
        if data.get('snowflake'):
            try:
                sf = list_snowflake_objects(SnowflakeConnectionConfig.from_payload(data['snowflake']))
                if sf.get('success'):
                    sf_names = {str(t.get('name') or '').lower() for t in (sf.get('tables') or [])}
                    shipped = sorted({str(t.get('name') or '').lower() for t in pg_tables
                                      if str(t.get('name') or '').lower() in sf_names})
            except Exception as exc:  # noqa: BLE001 — cross-check is advisory
                logger.info("postgres introspect: snowflake cross-check skipped: %s", exc)
        return jsonify({"success": True, "tables": pg_tables, "shipped_to_snowflake": shipped, "errors": {}})

    @app.route('/api/sfglue/postgres/generate-ingestion', methods=['POST'])
    def sfglue_postgres_generate_ingestion():
        """Generate a Databricks bronze ingestion notebook that reads the selected Postgres
        tables straight into Delta bronze via JDBC (deterministic — no AI, no live warehouse).

        Body: {tables:[{schema,name}], destination, secret_scope?}. Returns
        {success, notebooks:{name: code}, table_count}.
        """
        data = request.get_json(silent=True) or {}
        tables = data.get('tables') or []
        if not tables:
            return jsonify({"success": False, "error": "No Postgres tables selected."}), 400
        destination = data.get('destination') or {}
        scope = (data.get('secret_scope') or 'jdbc').strip() or 'jdbc'
        code = generate_postgres_bronze_ingestion(tables, destination, secret_scope=scope)
        return jsonify({"success": True,
                        "notebooks": {"postgres_bronze_ingest.py": code},
                        "table_count": len(tables)})

    @app.route('/api/sfglue/introspect', methods=['POST'])
    def sfglue_introspect():
        """List Snowflake objects + Glue catalog tables + Glue jobs.

        Each source is optional — pass only what's connected. Per-source errors
        are reported without failing the whole call.
        """
        data = request.get_json(silent=True) or {}
        out = {"success": True, "snowflake": None, "glue": None, "errors": {}}

        if data.get('snowflake'):
            sf_config = SnowflakeConnectionConfig.from_payload(data['snowflake'])
            sf = list_snowflake_objects(sf_config)
            if sf.get('success'):
                out['snowflake'] = {"tables": sf['tables'], "views": sf['views']}
            else:
                out['errors']['snowflake'] = sf.get('error')

        if data.get('glue'):
            glue_config = GlueConnectionConfig.from_payload(data['glue'])
            cat = list_glue_catalog(glue_config, databases=data.get('glue_databases'))
            jobs = list_glue_jobs(glue_config)
            glue_block = {"tables": [], "jobs": []}
            if cat.get('success'):
                glue_block['tables'] = cat['tables']
                glue_block['databases'] = cat.get('databases', [])
            else:
                out['errors']['glue_catalog'] = cat.get('error')
            if jobs.get('success'):
                glue_block['jobs'] = jobs['jobs']
            else:
                out['errors']['glue_jobs'] = jobs.get('error')
            out['glue'] = glue_block

        if out['snowflake'] is None and out['glue'] is None:
            return jsonify({"success": False, "error": "Connect Snowflake and/or AWS Glue first."}), 400
        return jsonify(out)

    @app.route('/api/sfglue/lineage', methods=['POST'])
    def sfglue_lineage():
        """Build the source→Snowflake lineage graph + duplicate findings + recommendations."""
        data = request.get_json(silent=True) or {}
        errors = {}
        # Reuse the Glue connection's AWS creds for Bedrock so recommendations work without
        # separately configuring the server environment.
        ai = _bind_ai(call_ai, data.get('glue'))

        snowflake_objects = {"tables": [], "views": []}
        snowflake_ddl = {}
        relationships = []
        if data.get('snowflake'):
            sf_config = SnowflakeConnectionConfig.from_payload(data['snowflake'])
            sf = list_snowflake_objects(sf_config)
            if sf.get('success'):
                snowflake_objects = {"tables": sf['tables'], "views": sf['views']}
                if not (sf['tables'] or sf['views']):
                    loc = sf_config.database + (f".{sf_config.schema}" if sf_config.schema else "")
                    errors['snowflake'] = (
                        f"No tables or views found in {loc or '(no database selected)'}. "
                        "Check the database/schema name (it's case-insensitive), or clear the Schema field to scan all schemas."
                    )
                ddl = fetch_object_ddl(sf_config)
                if ddl.get('success'):
                    snowflake_ddl = ddl['ddl']
                elif sf['views']:
                    errors['snowflake_ddl'] = ddl.get('error')
                # Declared foreign keys → relationship edges between base tables.
                rels = list_snowflake_relationships(sf_config)
                if rels.get('success'):
                    relationships = rels['relationships']
            else:
                errors['snowflake'] = sf.get('error')

        glue_tables, glue_jobs, glue_scripts = [], [], {}
        if data.get('glue'):
            glue_config = GlueConnectionConfig.from_payload(data['glue'])
            cat = list_glue_catalog(glue_config, databases=data.get('glue_databases'))
            if cat.get('success'):
                glue_tables = cat['tables']
            else:
                errors['glue_catalog'] = cat.get('error')
            jobs = list_glue_jobs(glue_config)
            if jobs.get('success'):
                glue_jobs = jobs['jobs']
                scripts = fetch_glue_job_scripts(glue_config, glue_jobs)
                if scripts.get('success'):
                    glue_scripts = scripts['scripts']
                    if scripts.get('errors'):
                        errors['glue_scripts'] = scripts['errors']
                else:
                    errors['glue_scripts'] = scripts.get('error')
            else:
                errors['glue_jobs'] = jobs.get('error')

        # Only hard-fail when no source was even attempted. If a source WAS queried
        # but returned nothing, return success with an empty graph + the per-source
        # notes above, so the UI explains why instead of showing a generic error.
        if not (data.get('snowflake') or data.get('glue')):
            return jsonify({"success": False, "error": "Connect Snowflake and/or AWS Glue first."}), 400

        try:
            lineage = build_lineage(snowflake_objects, snowflake_ddl, glue_tables, glue_jobs, glue_scripts,
                                    relationships=relationships)
            duplicates = detect_duplicates(snowflake_objects, glue_tables)
            for _g in duplicates:
                logger.info("DUP_GROUP base=%s cross_system=%s overlap=%s members=%s",
                            _g.get("base_name"), _g.get("cross_system"), _g.get("column_overlap"),
                            [f"{_m.get('full_name')}[{_m.get('system')}]" for _m in _g.get("members", [])])
            analysis = recommend(ai, lineage, duplicates, snowflake_objects, glue_jobs)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Snowflake/Glue lineage build failed")
            return jsonify({"success": False, "error": f"Lineage analysis failed: {exc}"}), 500

        # Recommendations are an AI surface. When no LLM produced them, don't pass off the
        # deterministic structural baseline as "recommendations" — replace the cards with a
        # single notice telling the user to connect an LLM. The lineage graph and the
        # duplicate-table detection are pure analysis and stay (they need no LLM).
        # recommend() already attempted an AI pass, so ai_used tells us if it worked — no
        # need for a second probe on the happy path. Only when it didn't do we probe (one
        # round-trip) to get the actionable reason (not configured vs unreachable).
        ai_ok = bool(analysis.get("ai_used"))
        if not ai_ok:
            # Give the RIGHT reason, not a blanket "connect a provider". recommend()
            # reports ai_status: no_provider | error | empty. Only probe when we still
            # need the actionable reason (no_provider / reachability confirmation).
            status = analysis.get("ai_status")
            ai_err = analysis.get("ai_error")
            if status == "error":
                title = "AI recommendations unavailable — provider call failed"
                detail = (f"The AI call failed: {ai_err}. If using AWS Bedrock, refresh your SSO "
                          "(`aws sso login`), then re-run Analyze lineage.") if ai_err else \
                         "The AI call failed. Check the provider/connection and re-run Analyze lineage."
            elif status == "empty":
                ok, why = _ai_preflight(ai)
                if ok:
                    title = "AI recommendations unavailable — no usable output"
                    detail = "The AI provider is connected but returned no usable recommendations. Re-run Analyze lineage to retry."
                else:
                    title = "AI recommendations unavailable"
                    detail = why or "The AI provider isn't reachable right now — check Settings and retry."
            else:  # no_provider (or unknown → probe for the actionable reason)
                _, ai_why = _ai_preflight(ai)
                title = "Connect an LLM to get migration recommendations"
                detail = ai_why or "No AI provider is connected. Connect one in Settings, then re-run Analyze lineage."
            analysis["recommendations"] = [{
                "title": title, "detail": detail, "severity": "high", "members": [], "source": "notice",
            }]
            analysis["ai_used"] = False

        return jsonify({
            "success": True,
            "needsAiConfig": (not ai_ok),
            "lineage": lineage,
            "duplicates": analysis["duplicates"],
            "recommendations": analysis["recommendations"],
            "summary": analysis["summary"],
            "ai_used": analysis["ai_used"],
            "ai_status": analysis.get("ai_status"),
            "ai_error": analysis.get("ai_error"),
            "jobs": glue_jobs,
            "glue_scripts": glue_scripts,   # so the lineage view can show a job's code on click
            "relationships": relationships,
            "errors": errors,
        })

    @app.route('/api/sfglue/review', methods=['POST'])
    def sfglue_review():
        """Rich review payload: table list (with columns), Glue jobs + their ETL code,
        Snowflake view SQL, and an AI plain-English business-logic overview."""
        data = request.get_json(silent=True) or {}
        errors = {}
        snowflake_objects = {"tables": [], "views": []}
        snowflake_ddl = {}
        relationships = []
        if data.get('snowflake'):
            sf_config = SnowflakeConnectionConfig.from_payload(data['snowflake'])
            sf = list_snowflake_objects(sf_config)
            if sf.get('success'):
                snowflake_objects = {"tables": sf['tables'], "views": sf['views']}
                ddl = fetch_object_ddl(sf_config)
                if ddl.get('success'):
                    snowflake_ddl = ddl['ddl']
                rels = list_snowflake_relationships(sf_config)
                if rels.get('success'):
                    relationships = rels['relationships']
            else:
                errors['snowflake'] = sf.get('error')

        glue_tables, glue_jobs, glue_scripts = [], [], {}
        if data.get('glue'):
            glue_config = GlueConnectionConfig.from_payload(data['glue'])
            cat = list_glue_catalog(glue_config, databases=data.get('glue_databases'))
            if cat.get('success'):
                glue_tables = cat['tables']
            else:
                errors['glue_catalog'] = cat.get('error')
            jobs = list_glue_jobs(glue_config)
            if jobs.get('success'):
                glue_jobs = jobs['jobs']
                scripts = fetch_glue_job_scripts(glue_config, glue_jobs)
                glue_scripts = scripts.get('scripts', {}) if scripts.get('success') else {}
            else:
                errors['glue_jobs'] = jobs.get('error')

        if not (data.get('snowflake') or data.get('glue')):
            return jsonify({"success": False, "error": "Connect Snowflake and/or AWS Glue first."}), 400

        # Attach view SQL to each view, and job scripts to each job, for the UI.
        views = [{**v, "sql": snowflake_ddl.get(v["full_name"], "")} for v in snowflake_objects["views"]]
        jobs_out = [{**j, "script": glue_scripts.get(j.get("name"), "")} for j in glue_jobs]
        business = explain_business_logic(call_ai, snowflake_objects, snowflake_ddl, glue_jobs, glue_scripts)

        return jsonify({
            "success": True,
            "tables": snowflake_objects["tables"],
            "views": views,
            "glue_tables": glue_tables,
            "glue_jobs": jobs_out,
            "relationships": relationships,
            "business_logic": business["text"],
            "business_logic_ai": business["ai_used"],
            "errors": errors,
        })

    @app.route('/api/sfglue/explain', methods=['POST'])
    def sfglue_explain():
        """Explain a single generated artifact (notebook / dbt model / DDL) in plain English."""
        data = request.get_json(silent=True) or {}
        out = explain_artifact(call_ai, data.get('name', ''), data.get('code', ''), data.get('kind', 'code'))
        return jsonify({"success": True, **out})

    @app.route('/api/sfglue/grade', methods=['POST'])
    def sfglue_grade():
        """Grade how faithfully the converted artifacts match the original source.

        Body: {tables:[{full_name,columns}], relationships:[{fk_table,fk_columns,pk_table,
               pk_columns}], views:[{name,sql}], glue_jobs:[{name,script}], business_logic,
               dbt_models:{name:code}, notebooks:{name:code}, ddl:{name:code}, dialect}.
        Returns {overall, dimensions, summary} — read-only, no source re-fetch.
        """
        data = request.get_json(silent=True) or {}

        def _join_named(items, key):
            out = []
            for it in (items or []):
                name = (it.get('name') or it.get('full_name') or '?') if isinstance(it, dict) else '?'
                code = (it.get(key) or '') if isinstance(it, dict) else ''
                if code:
                    out.append(f"-- {name}\n{code}")
            return "\n\n".join(out)

        def _join_map(m):
            return "\n\n".join(f"-- {n}\n{c}" for n, c in (m or {}).items() if c)

        def _tables_block(tables):
            """The in-scope Snowflake source tables + their columns. When a source has NO
            views (all logic in Glue jobs) these table schemas ARE the source of truth the
            models were derived from — feeding them lets the grader verify completeness
            (every table represented) and grain (keys) instead of seeing 'no source material'."""
            lines = []
            for t in (tables or []):
                if not isinstance(t, dict):
                    continue
                name = t.get('full_name') or t.get('name')
                if not name:
                    continue
                cols = t.get('columns') or []
                col_str = ", ".join(
                    (f"{c.get('name')}:{c.get('type')}" if isinstance(c, dict) and c.get('type')
                     else str(c.get('name') if isinstance(c, dict) else c))
                    for c in cols if (c.get('name') if isinstance(c, dict) else c)
                )
                lines.append(f"- {name}" + (f" ({col_str})" if col_str else ""))
            return "\n".join(lines)

        def _rels_block(rels):
            lines = []
            for r in (rels or []):
                if not isinstance(r, dict):
                    continue
                fk_t, pk_t = r.get('fk_table'), r.get('pk_table')
                if not (fk_t and pk_t):
                    continue
                fk_c = ", ".join(r.get('fk_columns') or [])
                pk_c = ", ".join(r.get('pk_columns') or [])
                lines.append(f"- {fk_t}({fk_c}) -> {pk_t}({pk_c})")
            return "\n".join(lines)

        tables_block = _tables_block(data.get('tables'))
        rels_block = _rels_block(data.get('relationships'))
        original = "\n\n".join(filter(None, [
            ("## Snowflake source tables (in scope)\n" + tables_block) if tables_block else '',
            ("## Declared relationships (FK -> PK)\n" + rels_block) if rels_block else '',
            "## Snowflake views\n" + _join_named(data.get('views'), 'sql'),
            "## Glue job scripts\n" + _join_named(data.get('glue_jobs'), 'script'),
            ("## Business logic\n" + data.get('business_logic')) if data.get('business_logic') else '',
        ]))
        converted = "\n\n".join(filter(None, [
            "## dbt models\n" + _join_map(data.get('dbt_models')),
            "## Bronze notebooks\n" + _join_map(data.get('notebooks')),
            "## DDL\n" + _join_map(data.get('ddl')),
        ]))

        if not converted.strip():
            return jsonify({"error": "Nothing converted yet to grade."}), 400

        out = grade_migration_fidelity(
            call_ai, original=original, converted=converted, dialect=data.get('dialect', 'databricks'),
        )
        # The model grading its OWN output is systematically overconfident, so this is a
        # TRIAGE signal (prioritise review), never the ship gate. The gate is the
        # deterministic review queue + reconciliation. Label the payload so no consumer
        # can mistake it for a pass/fail verdict.
        if isinstance(out, dict):
            out.setdefault("role", "triage")
            out.setdefault("advisory",
                           "AI self-grade — triage/prioritisation only, not a ship criterion. "
                           "Ship gate = review queue empty AND reconciliation passes AND tests/contracts build.")
        return jsonify(out)

    @app.route('/api/sfglue/precheck', methods=['POST'])
    def sfglue_precheck():
        """Compare planned target tables against what already exists in Databricks.

        Uses the lineage graph the frontend already holds (no source re-fetch) and
        introspects the destination catalog/schemas via Unity Catalog.
        """
        data = request.get_json(silent=True) or {}
        lineage = data.get('lineage') or {}
        selected = data.get('selected_ids') or data.get('selected') or []
        destination = data.get('destination') or {}
        if not lineage.get('nodes'):
            return jsonify({"success": False, "error": "No lineage available. Build lineage first."}), 400
        if not selected:
            return jsonify({"success": False, "error": "Select at least one table to migrate."}), 400

        existing, introspection_error = set(), None
        try:
            from qvd_to_databricks.databricks_connection import DatabricksConnectionConfig
            from backend.integrations.databricks_agent_routes import introspect_schema_tables

            cfg = DatabricksConnectionConfig.from_payload(destination)
            schemas = {destination.get(k) for k in ('bronze_schema', 'silver_schema', 'gold_schema') if destination.get(k)}
            for schema in schemas or {cfg.schema}:
                tables, err = introspect_schema_tables(cfg, cfg.catalog, schema)
                if err:
                    introspection_error = err.get('message') if isinstance(err, dict) else str(err)
                    continue
                for t in tables or []:
                    if t.get('name'):
                        existing.add(str(t['name']).lower())
        except Exception as exc:  # noqa: BLE001 — introspection is best-effort
            introspection_error = str(exc)

        result = build_precheck(lineage, selected, existing, destination)
        # connected=True only when introspection actually succeeded — distinguishes
        # "connected, nothing there yet" from "couldn't reach Databricks" (both of which
        # otherwise show an empty 'already in Databricks' list).
        result.update({"success": True, "introspection_error": introspection_error,
                       "connected": introspection_error is None})
        return jsonify(result)

    @app.route('/api/sfglue/deploy', methods=['POST'])
    def sfglue_deploy():
        """Deploy the generated table DDL into Databricks Unity Catalog.

        Creates each ``catalog.schema`` the DDL targets, then runs every CREATE TABLE
        against the configured SQL Warehouse, returning a per-table result. Notebooks
        and dbt models aren't executed here (they need the Workspace API / a dbt runtime)
        — this materializes the table shells so the downstream models have somewhere to
        write.
        """
        data = request.get_json(silent=True) or {}
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

    @app.route('/api/sfglue/build', methods=['POST'])
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

        data = request.get_json(silent=True) or {}
        destination = data.get('destination') or {}
        models = data.get('models') or {}
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
                run_sql, call_ai, m["statement"], introspect_cols, max_attempts=2)
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

    @app.route('/api/sfglue/convert', methods=['POST'])
    def sfglue_convert():
        """Generate the migration artifacts for the scoped selection.

        Re-fetches Glue job scripts + Snowflake columns/DDL (needed for the actual
        code translation), classifies ingestion vs transformation, and converts:
        ingestion → Databricks notebooks (bronze); transformation + views → dbt
        models (silver/gold); tables → Databricks DDL + bronze-reading staging.
        """
        data = request.get_json(silent=True) or {}
        lineage = data.get('lineage') or {}
        selected = data.get('selected_ids') or data.get('selected') or []
        destination = data.get('destination') or {}
        if not lineage.get('nodes') or not selected:
            return jsonify({"success": False, "error": "Build lineage and select tables first."}), 400

        # Reuse the Glue connection's AWS creds for Bedrock so conversion works without
        # separately configuring the server environment.
        ai = _bind_ai(call_ai, data.get('glue'))

        # No LLM, no conversion. The model translation REQUIRES an LLM; running without one
        # only emits un-translated scaffolds that look finished but aren't. Refuse up front
        # with an actionable notice instead. (The deterministic DDL/sources.yml are still
        # available via their own steps; this gate is specifically the AI-dependent convert.)
        ai_ok, ai_why = _ai_preflight(ai)
        if not ai_ok:
            return jsonify({"success": False, "needsAiConfig": True, "error": ai_why}), 503

        # ── Input gather. The Snowflake metadata, the Glue scripts, and the Databricks
        # source-column introspection are three INDEPENDENT cloud round-trips (different
        # providers, no data dependency between them). Running them serially made convert
        # pay the SUM of three cold connects; we run them concurrently so it pays only the
        # MAX. Each group owns its error handling and RETURNS its results — no shared
        # mutation across threads. Per-phase timings are logged so the wall-clock is
        # attributable instead of hidden inside one opaque total.
        def _fetch_snowflake():
            """Snowflake metadata: object columns + DDL + declared relationships."""
            cols, ddl_map, rels, errs = {}, {}, [], {}
            if data.get('snowflake'):
                sf_config = SnowflakeConnectionConfig.from_payload(data['snowflake'])
                sf = list_snowflake_objects(sf_config)
                if sf.get('success'):
                    for obj in (sf['tables'] + sf['views']):
                        cols[obj['full_name']] = obj.get('columns', [])
                    ddl = fetch_object_ddl(sf_config)
                    if ddl.get('success'):
                        ddl_map = ddl['ddl']
                    else:
                        errs['snowflake_ddl'] = ddl.get('error')
                    rel_res = list_snowflake_relationships(sf_config)
                    if rel_res.get('success'):
                        rels = rel_res['relationships']
                else:
                    errs['snowflake'] = sf.get('error')
            return cols, ddl_map, rels, errs

        def _fetch_glue():
            """Glue job scripts. Prefer the scripts the client already captured at the
            Lineage/Review step (they reflect the user's Review edits and don't depend on a
            still-valid live Glue session). Fall back to a fresh live fetch for any job the
            client didn't send — keeps conversion working even if a temporary SSO/STS Glue
            session has since expired (previously that silently produced no artifacts)."""
            scripts_map, errs = {}, {}
            if data.get('glue'):
                glue_config = GlueConnectionConfig.from_payload(data['glue'])
                jobs = list_glue_jobs(glue_config)
                if jobs.get('success'):
                    scripts = fetch_glue_job_scripts(glue_config, jobs['jobs'])
                    if scripts.get('success'):
                        scripts_map.update(scripts.get('scripts', {}) or {})
                else:
                    errs['glue_jobs'] = jobs.get('error')
            return scripts_map, errs

        def _fetch_postgres():
            """When a Postgres source is connected, choose the tables to auto-land in bronze:
            the Postgres tables also present in Snowflake (i.e. shipped there — the ones in
            this migration), falling back to every Postgres table if the cross-check finds
            none. The generated JDBC notebook is added to the conversion below so a
            Postgres-origin source lands its data without a manual step. Best-effort: any
            failure returns ([], {error}) and the conversion proceeds unaffected."""
            if not data.get('postgres'):
                return [], {}
            try:
                pg = list_postgres_objects(PostgresConnectionConfig.from_payload(data['postgres']))
            except Exception as exc:  # noqa: BLE001 — Postgres ingestion is additive
                return [], {"postgres": str(exc)}
            if not pg.get('success'):
                return [], {"postgres": pg.get('error')}
            pg_tables = pg.get('tables') or []
            chosen = pg_tables
            if data.get('snowflake'):
                try:
                    sf = list_snowflake_objects(SnowflakeConnectionConfig.from_payload(data['snowflake']))
                    if sf.get('success'):
                        sf_names = {str(t.get('name') or '').lower() for t in (sf.get('tables') or [])}
                        shipped = [t for t in pg_tables
                                   if str(t.get('name') or '').lower() in sf_names]
                        if shipped:
                            chosen = shipped
                except Exception as exc:  # noqa: BLE001 — cross-check is advisory
                    logger.info("sfglue convert: postgres shipped cross-check skipped: %s", exc)
            return chosen, {}

        def _timed(label, fn):
            t = time.perf_counter()
            try:
                return fn()
            finally:
                logger.info("sfglue convert: phase %s took %.1fs", label, time.perf_counter() - t)

        # _introspect_source_columns grounds the converters in the REAL source-table
        # columns (the location {{ source('bronze', X) }} resolves to). Best-effort: any
        # failure leaves bronze_columns empty and the converters degrade gracefully.
        t_fetch = time.perf_counter()
        with ThreadPoolExecutor(max_workers=4) as ex:
            f_sf = ex.submit(_timed, "snowflake_meta", _fetch_snowflake)
            f_glue = ex.submit(_timed, "glue_scripts", _fetch_glue)
            f_bronze = ex.submit(_timed, "databricks_introspect",
                                 lambda: _introspect_source_columns(destination))
            f_pg = ex.submit(_timed, "postgres_introspect", _fetch_postgres)
            snowflake_columns, snowflake_ddl, relationships, sf_errs = f_sf.result()
            glue_scripts, glue_errs = f_glue.result()
            bronze_columns = f_bronze.result()
            postgres_tables, pg_errs = f_pg.result()
        logger.info("sfglue convert: input gather (4 providers, parallel) took %.1fs",
                    time.perf_counter() - t_fetch)
        errors = {**sf_errs, **glue_errs, **pg_errs}

        # Honesty signal: if the bronze (raw landing) schema has no tables, the converters
        # couldn't ground column names on the REAL landed schema and inferred them from the
        # Glue script instead — so generated refs may not match what actually lands. Only
        # the destination is introspected here, so surface it as a non-fatal warning (the
        # convert still produces artifacts) rather than failing or staying silent.
        if not bronze_columns:
            _src_schema = (destination.get('source_schema') or destination.get('bronze_schema') or 'bronze')
            _src_catalog = (destination.get('source_catalog') or destination.get('catalog') or '')
            _loc = f"{_src_catalog}.{_src_schema}".strip('.')
            errors.setdefault("source_grounding", (
                f"No tables found in {_loc or 'the bronze schema'} — column names in the generated "
                "models were inferred from the Glue script, not the real landed schema, so some "
                "references may not match. Land the raw data into bronze and regenerate for exact "
                "column grounding."))

        # Client-supplied scripts win (they carry the user's Review-screen edits).
        client_scripts = data.get('glue_scripts') or {}
        for name, script in client_scripts.items():
            if isinstance(script, str) and script.strip():
                glue_scripts[name] = script
        jobs_io = {name: parse_pyspark_io(script) for name, script in glue_scripts.items()}

        try:
            t_conv = time.perf_counter()
            artifacts = run_conversion(
                ai, lineage, selected,
                jobs_io=jobs_io, glue_scripts=glue_scripts,
                snowflake_ddl=snowflake_ddl, snowflake_columns=snowflake_columns,
                destination=destination, relationships=relationships,
                bronze_columns=bronze_columns,
            )
            logger.info("sfglue convert: phase run_conversion (AI) took %.1fs",
                        time.perf_counter() - t_conv)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Snowflake/Glue conversion failed")
            return jsonify({"success": False, "error": f"Conversion failed: {exc}"}), 500

        # Postgres → bronze: when a Postgres source is connected, auto-attach the JDBC landing
        # notebook so it lands its data in Delta bronze without a separate manual step — it just
        # appears alongside the other bronze notebooks. Deterministic (no AI, no live warehouse).
        if postgres_tables:
            try:
                code = generate_postgres_bronze_ingestion(
                    [{"schema": t.get("schema"), "name": t.get("name")} for t in postgres_tables],
                    destination, secret_scope=(data.get('postgres_secret_scope') or 'jdbc'))
                artifacts.setdefault("notebooks", {})["postgres_bronze_ingest.py"] = code
                logger.info("sfglue convert: auto-generated postgres bronze ingestion for %d table(s)",
                            len(postgres_tables))
            except Exception as exc:  # noqa: BLE001 — ingestion notebook is additive
                logger.warning("sfglue convert: postgres ingestion generation failed: %s", exc)
                errors.setdefault("postgres_ingestion", str(exc))

        artifacts.update({"success": True, "errors": errors})
        return jsonify(artifacts)

    @app.route('/api/sfglue/export', methods=['POST'])
    def sfglue_export():
        """Package the converted artifacts into a downloadable, runnable dbt project (.zip).

        Stateless: the client posts the ``convert`` result it already holds + the
        ``destination``. Source-agnostic — the project layout/config derive entirely from the
        artifacts and the destination payload, so this works for ANY converted Glue+Snowflake
        flow, not one demo's."""
        data = request.get_json(silent=True) or {}
        artifacts = data.get('artifacts') or data
        destination = data.get('destination') or artifacts.get('destination') or {}
        if not (artifacts.get('dbt_models') or artifacts.get('sources_yml')):
            return jsonify({"success": False,
                            "error": "No conversion artifacts to export — run Convert first."}), 400
        project_name = data.get('project_name') or 'sfglue_migration'
        try:
            files = build_dbt_project_files(artifacts, destination, project_name)
            import io
            import zipfile
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
                for path, content in files.items():
                    zf.writestr(f"{project_name}/{path}",
                                content if isinstance(content, str) else str(content))
            buf.seek(0)
        except Exception as exc:  # noqa: BLE001
            logger.exception("sfglue export failed")
            return jsonify({"success": False, "error": f"Export failed: {exc}"}), 500
        return Response(buf.getvalue(), mimetype='application/zip',
                        headers={'Content-Disposition': f'attachment; filename="{project_name}.zip"'})

    @app.route('/api/sfglue/seed-bronze', methods=['POST'])
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

        data = request.get_json(silent=True) or {}
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

    @app.route('/api/sfglue/reconcile', methods=['POST'])
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

        data = request.get_json(silent=True) or {}
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
                if not key:
                    results.append({**base, "passed": False,
                                    "error": "a primary key is required to reconcile (none declared and none could be inferred) — set one"})
                    continue
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

    @app.route('/api/sfglue/run-tests', methods=['POST'])
    def sfglue_run_tests():
        """Execution-based gate: RUN the generated dbt tests + enforced contracts as SQL on the
        Databricks SQL Warehouse, BEFORE the reconciliation gate. Each test becomes a
        violating-row query (0 rows = pass); contracts are checked against information_schema.

        Body: {destination, test_specs:[...]} (test_specs come from the conversion result).
        Returns {success, all_passed, results:[{model, kind, columns, passed, violations, detail}]}.
        The candidate tables must already be deployed/built (Databricks Agent step).
        """
        from backend.integrations.reconcile import DATABRICKS, check_containment

        data = request.get_json(silent=True) or {}
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
