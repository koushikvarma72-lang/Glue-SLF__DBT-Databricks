"""Lineage routes: the source -> Snowflake analytical lineage graph and the
operational (Glue workflow + control-DB) lineage. Behaviour identical."""

from __future__ import annotations

import logging

from flask import Blueprint, jsonify
from backend.integrations.glue_client import (
    GlueConnectionConfig,
    fetch_glue_job_scripts,
    list_glue_catalog,
    list_glue_jobs,
    list_glue_triggers,
    list_glue_workflows,
)
from backend.integrations.orchestration_migration import parse_workflow_dag
from backend.integrations.postgres_client import PostgresConnectionConfig, introspect_framework_tables
from backend.integrations.snowflake_client import (
    SnowflakeConnectionConfig,
    fetch_object_ddl,
    list_snowflake_objects,
    list_snowflake_relationships,
)
from backend.integrations.snowflake_glue_lineage import build_lineage, detect_duplicates, recommend
from backend.integrations.routes._shared import _ai_preflight, _bind_ai, body, get_call_ai

logger = logging.getLogger("sfglue.routes")

bp = Blueprint("sfglue_lineage", __name__)


@bp.route('/api/sfglue/lineage', methods=['POST'])
def sfglue_lineage():
    """Build the source→Snowflake lineage graph + duplicate findings + recommendations."""
    data = body()
    errors = {}
    # Reuse the Glue connection's AWS creds for Bedrock so recommendations work without
    # separately configuring the server environment.
    ai = _bind_ai(get_call_ai(), data.get('glue'))

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

@bp.route('/api/sfglue/lineage/operational', methods=['POST'])
def sfglue_lineage_operational():
    """Operational lineage: fuse the Glue Workflow chain + RDS control rows + catalog
    into one laned graph (jobs, control tables, data tables; execution/control/data
    edges) with per-job logic drilldown and generic source-health checks. Everything
    is derived from introspection — nothing about any one pipeline is hardcoded.

    Body: {glue?, glue_databases?, postgres?, snowflake?, job_flags?}.
    """
    from backend.integrations.operational_lineage import build_operational_lineage
    data = body()
    if not (data.get('glue') or data.get('postgres')):
        return jsonify({"success": False,
                        "error": "Connect AWS Glue and/or the Postgres control DB first."}), 400
    errors = {}
    workflow_dag, glue_tables, glue_jobs = {}, [], []
    if data.get('glue'):
        gc = GlueConnectionConfig.from_payload(data['glue'])
        cat = list_glue_catalog(gc, databases=data.get('glue_databases'))
        if cat.get('success'):
            glue_tables = cat['tables']
        else:
            errors['glue_catalog'] = cat.get('error')
        jl = list_glue_jobs(gc)
        glue_jobs = jl.get('jobs', []) if jl.get('success') else []
        wf = list_glue_workflows(gc)
        trg = list_glue_triggers(gc)
        triggers = trg.get('triggers', []) if trg.get('success') else []
        wflows = wf.get('workflows', []) if wf.get('success') else []
        if wflows:
            # fuse all workflows' tasks into one chain view (usually one workflow)
            merged = {"name": wflows[0].get('name', 'workflow'), "tasks": []}
            for w in wflows:
                merged['tasks'].extend(parse_workflow_dag(w, triggers).get('tasks', []))
            workflow_dag = merged
        elif glue_jobs:
            # no workflow — still surface the jobs as un-chained nodes
            workflow_dag = {"tasks": [{"key": j['name'], "legacy_name": j['name'],
                                       "kind": "glue_job", "depends_on": []}
                                      for j in glue_jobs]}

    framework_tables = []
    if data.get('postgres'):
        pg = PostgresConnectionConfig.from_payload(data['postgres'])
        fw = introspect_framework_tables(pg)
        if fw.get('success'):
            framework_tables = fw.get('framework_tables', [])
        else:
            errors['postgres'] = fw.get('error')

    snowflake_objects = {}
    if data.get('snowflake'):
        sc = SnowflakeConnectionConfig.from_payload(data['snowflake'])
        sf = list_snowflake_objects(sc)
        if sf.get('success'):
            snowflake_objects = {"tables": sf['tables'], "views": sf['views']}

    graph = build_operational_lineage(
        workflow_dag=workflow_dag, framework_tables=framework_tables,
        glue_tables=glue_tables, snowflake_objects=snowflake_objects,
        job_flags=data.get('job_flags') or {})
    return jsonify({"success": True, "errors": errors, **graph})
