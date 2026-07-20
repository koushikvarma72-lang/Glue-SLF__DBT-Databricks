"""Conversion routes: precheck (target diff), convert (generate artifacts),
export (zip). Behaviour identical to the original handlers."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor

from flask import Blueprint, Response, jsonify
from backend.integrations.bundle_export import SCHEMA_VERSION, build_artifact_registry, build_bundle_files
from backend.integrations.control_plane_migration import convert_query_configuration_rows, generate_control_schema_ddl, generate_framework_notebooks
from backend.integrations.dq_migration import compile_dq_rules, compile_notifications
from backend.integrations.glue_client import GlueConnectionConfig, fetch_glue_job_scripts, list_glue_jobs
from backend.integrations.postgres_client import PostgresConnectionConfig, introspect_framework_tables, list_postgres_objects
from backend.integrations.snowflake_client import (
    SnowflakeConnectionConfig,
    fetch_object_ddl,
    list_snowflake_objects,
    list_snowflake_relationships,
)
from backend.integrations.snowflake_glue_lineage import parse_pyspark_io
from backend.integrations.snowflake_glue_migration import (
    _map_concurrent,
    build_dbt_project_files,
    build_precheck,
    generate_postgres_bronze_ingestion,
    run_conversion,
)
from backend.integrations.routes._shared import _ai_preflight, _bind_ai, body, get_call_ai

logger = logging.getLogger("sfglue.routes")

bp = Blueprint("sfglue_convert", __name__)


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


@bp.route('/api/sfglue/precheck', methods=['POST'])
def sfglue_precheck():
    """Compare planned target tables against what already exists in Databricks.

    Uses the lineage graph the frontend already holds (no source re-fetch) and
    introspects the destination catalog/schemas via Unity Catalog.
    """
    data = body()
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

@bp.route('/api/sfglue/convert', methods=['POST'])
def sfglue_convert():
    """Generate the migration artifacts for the scoped selection.

    Re-fetches Glue job scripts + Snowflake columns/DDL (needed for the actual
    code translation), classifies ingestion vs transformation, and converts:
    ingestion → Databricks notebooks (bronze); transformation + views → dbt
    models (silver/gold); tables → Databricks DDL + bronze-reading staging.
    """
    data = body()
    lineage = data.get('lineage') or {}
    selected = data.get('selected_ids') or data.get('selected') or []
    destination = data.get('destination') or {}
    if not lineage.get('nodes') or not selected:
        return jsonify({"success": False, "error": "Build lineage and select tables first."}), 400

    # Reuse the Glue connection's AWS creds for Bedrock so conversion works without
    # separately configuring the server environment.
    ai = _bind_ai(get_call_ai(), data.get('glue'))

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
        """When a Postgres source is connected, land EVERY Postgres table in bronze as its
        own Delta table (requirement: migrate all Postgres tables to Delta, not only the
        subset that ships to Snowflake). Introspects Postgres and returns all tables across
        all non-system schemas; the generated JDBC notebook is added to the conversion
        below so a Postgres-origin source lands its data without a manual step. Best-effort:
        any failure returns ([], {error}) and the conversion proceeds unaffected."""
        if not data.get('postgres'):
            return [], {}
        try:
            pg = list_postgres_objects(PostgresConnectionConfig.from_payload(data['postgres']))
        except Exception as exc:  # noqa: BLE001 — Postgres ingestion is additive
            return [], {"postgres": str(exc)}
        if not pg.get('success'):
            return [], {"postgres": pg.get('error')}
        return pg.get('tables') or [], {}

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

    # Phase 2+3 (gap plan): when a Postgres control DB is connected, migrate the
    # metadata framework itself — control-schema DDL, the transform SQL stored in
    # query_configuration → dbt models, framework runtime notebooks, dq_rules →
    # dbt tests/quarantine models, message_template → notification config. All
    # additive and best-effort: a control-plane failure never sinks the conversion.
    if data.get('postgres'):
        try:
            fw = introspect_framework_tables(
                PostgresConnectionConfig.from_payload(data['postgres']))
            if fw.get('success') and fw.get('framework_tables'):
                entries = {e['canonical']: e for e in fw['framework_tables']
                           if 'error' not in e}
                artifacts['control_plane'] = {
                    "tables": sorted(entries.keys()),
                    "skipped": [], "dq_summary": {},
                }
                # 2a. control schema DDL (framework tables as Delta)
                artifacts.setdefault('ddl', {}).update(
                    generate_control_schema_ddl(list(entries.values()), destination))
                # 2b. framework runtime notebooks (templated, deterministic)
                artifacts.setdefault('notebooks', {}).update(
                    generate_framework_notebooks(destination))
                # 2c. query_configuration rows → dbt models (the real transform SQL
                # in config-driven pipelines lives HERE, not in the Glue scripts)
                qc = entries.get('query_configuration')
                if qc:
                    bronze = sorted((artifacts.get('plan') or {}).get('bronze_tables') or [])
                    refs = [f[:-4] for f in (artifacts.get('dbt_models') or {})]
                    res = convert_query_configuration_rows(
                        ai, qc, destination=destination, bronze_sources=bronze,
                        available_refs=refs, map_concurrent=_map_concurrent)
                    artifacts.setdefault('dbt_models', {}).update(res['models'])
                    artifacts['control_plane']['skipped'] = res['skipped']
                    logger.info("sfglue convert: query_configuration → %d dbt model(s), "
                                "%d skipped", len(res['models']), len(res['skipped']))
                # 3. dq_rules → dbt tests + quarantine models + notebook checks
                dq = entries.get('dq_rules')
                if dq:
                    compiled = compile_dq_rules(
                        dq, known_models=[f[:-4] for f in (artifacts.get('dbt_models') or {})])
                    if compiled['dq_schema_yml']:
                        artifacts['dq_schema_yml'] = compiled['dq_schema_yml']
                    artifacts.setdefault('dbt_models', {}).update(compiled['quarantine_models'])
                    if compiled['notebook_checks'] or compiled['unclassified']:
                        artifacts.setdefault('notes', {})['_dq__review.md'] = (
                            "# DQ rules needing review\n\n"
                            f"File-level checks (run in the bronze notebooks): "
                            f"{compiled['notebook_checks']}\n\n"
                            f"Unclassified rules (translate manually): "
                            f"{compiled['unclassified']}\n")
                    artifacts['control_plane']['dq_summary'] = compiled['summary']
                    logger.info("sfglue convert: dq_rules compiled: %s", compiled['summary'])
                # 3b. message_template → Jobs notification block
                mt = entries.get('message_template')
                if mt:
                    artifacts['notifications'] = compile_notifications(mt)
            elif fw.get('success'):
                # Connected fine but found NO control tables — almost always the wrong
                # database (source DB vs control DB). Say so loudly instead of silently
                # skipping: this exact silence cost a debugging round.
                pg_db = (data['postgres'] or {}).get('database') or '(unknown)'
                hint = (f"No control-framework tables (configuration_master, "
                        f"query_configuration, dq_rules, …) found in Postgres database "
                        f"'{pg_db}'. If the config tables live in a different database "
                        "(e.g. 'control'), point the Postgres connection there and "
                        "re-run the conversion.")
                detected = (artifacts.get('plan') or {}).get('config_driven') or {}
                if detected.get('config_tables'):
                    hint += (" The Glue scripts reference these config tables: "
                             + ", ".join(detected['config_tables'][:8]) + ".")
                errors.setdefault('control_plane', hint)
                logger.info("sfglue convert: control-plane skipped — %s", hint)
            else:
                errors.setdefault('control_plane', fw.get('error'))
        except Exception as exc:  # noqa: BLE001 — control plane is additive
            logger.warning("sfglue convert: control-plane migration failed: %s", exc)
            errors.setdefault('control_plane', str(exc))

    artifacts.update({
        "success": True,
        "errors": errors,
        # Phase 0.4: schema-versioned typed artifact inventory — additive, so
        # existing frontend code is unaffected; new artifact classes (workflows,
        # DQ rules, grants) flow through review/report via this registry.
        "schema_version": SCHEMA_VERSION,
        "artifact_registry": build_artifact_registry(artifacts),
    })
    return jsonify(artifacts)

@bp.route('/api/sfglue/export', methods=['POST'])
def sfglue_export():
    """Package the converted artifacts into a downloadable, runnable dbt project (.zip).

    Stateless: the client posts the ``convert`` result it already holds + the
    ``destination``. Source-agnostic — the project layout/config derive entirely from the
    artifacts and the destination payload, so this works for ANY converted Glue+Snowflake
    flow, not one demo's."""
    data = body()
    artifacts = data.get('artifacts') or data
    destination = data.get('destination') or artifacts.get('destination') or {}
    if not (artifacts.get('dbt_models') or artifacts.get('sources_yml')):
        return jsonify({"success": False,
                        "error": "No conversion artifacts to export — run Convert first."}), 400
    project_name = data.get('project_name') or 'sfglue_migration'
    export_format = (data.get('format') or '').strip().lower()
    try:
        files = build_dbt_project_files(artifacts, destination, project_name)
        if export_format in ('bundle', 'dab'):
            # Databricks Asset Bundle export (Phase 0.3): wraps the dbt project
            # + notebooks in a deployable bundle with dev/test/prod targets.
            files = build_bundle_files(artifacts, destination, project_name,
                                       dbt_files=files)
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
