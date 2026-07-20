"""Review / explain / grade routes -- the AI-assisted read-only inspection
surfaces. Behaviour identical to the original handlers."""

from __future__ import annotations

from flask import Blueprint, jsonify
from backend.integrations.glue_client import (
    GlueConnectionConfig,
    fetch_glue_job_scripts,
    list_glue_catalog,
    list_glue_jobs,
)
from backend.integrations.snowflake_client import (
    SnowflakeConnectionConfig,
    fetch_object_ddl,
    list_snowflake_objects,
    list_snowflake_relationships,
)
from backend.integrations.snowflake_glue_lineage import explain_business_logic
from backend.integrations.snowflake_glue_migration import explain_artifact, grade_migration_fidelity
from backend.integrations.routes._shared import _bind_ai, body, get_call_ai

bp = Blueprint("sfglue_review", __name__)


@bp.route('/api/sfglue/review', methods=['POST'])
def sfglue_review():
    """Rich review payload: table list (with columns), Glue jobs + their ETL code,
    Snowflake view SQL, and an AI plain-English business-logic overview."""
    data = body()
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
    ai = _bind_ai(get_call_ai(), data.get('glue'))  # reuse the Glue connection's AWS creds for Bedrock
    business = explain_business_logic(ai, snowflake_objects, snowflake_ddl, glue_jobs, glue_scripts)

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

@bp.route('/api/sfglue/explain', methods=['POST'])
def sfglue_explain():
    """Explain a single generated artifact (notebook / dbt model / DDL) in plain English."""
    data = body()
    ai = _bind_ai(get_call_ai(), data.get('glue'))
    out = explain_artifact(ai, data.get('name', ''), data.get('code', ''), data.get('kind', 'code'))
    return jsonify({"success": True, **out})

@bp.route('/api/sfglue/grade', methods=['POST'])
def sfglue_grade():
    """Grade how faithfully the converted artifacts match the original source.

    Body: {tables:[{full_name,columns}], relationships:[{fk_table,fk_columns,pk_table,
           pk_columns}], views:[{name,sql}], glue_jobs:[{name,script}], business_logic,
           dbt_models:{name:code}, notebooks:{name:code}, ddl:{name:code}, dialect}.
    Returns {overall, dimensions, summary} — read-only, no source re-fetch.
    """
    data = body()

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
        _bind_ai(get_call_ai(), data.get('glue')), original=original, converted=converted,
        dialect=data.get('dialect', 'databricks'),
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
