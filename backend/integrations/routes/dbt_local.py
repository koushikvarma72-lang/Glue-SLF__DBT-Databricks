"""Local dbt-Core run routes (/api/dbt-local/*) used by the Snowflake/Glue flow.
Behaviour identical to the original handlers."""

from __future__ import annotations

from flask import Blueprint, jsonify, request
from backend.integrations.routes._shared import body

bp = Blueprint("sfglue_dbt_local", __name__)


@bp.route('/api/dbt-local/run-sfglue', methods=['POST'])
def sfglue_dbt_local_run():
    """Run the converted models with real dbt-Core against the Databricks warehouse.

    Body: {sessionId?, models: {fname: sql}, sources_yml?, destination}. → {jobId}.
    (These /api/dbt-local routes lived in the combined BI tool and were not split
    out with the app — the DBT Agent page depends on them.)
    """
    from backend.integrations.dbt_local import start_dbt_run
    data = body()
    result = start_dbt_run(data.get('models') or {}, data.get('sources_yml') or '',
                           data.get('destination') or {},
                           session_id=str(data.get('sessionId') or 'sfglue'))
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/dbt-local/status/<job_id>', methods=['GET'])
def sfglue_dbt_local_status(job_id):
    from backend.integrations.dbt_local import get_status
    try:
        since = int(request.args.get('since') or 0)
    except ValueError:
        since = 0
    result = get_status(job_id, since)
    return jsonify(result), (200 if result.get('success') else 404)

@bp.route('/api/dbt-local/cancel/<job_id>', methods=['POST'])
def sfglue_dbt_local_cancel(job_id):
    from backend.integrations.dbt_local import cancel
    result = cancel(job_id)
    return jsonify(result), (200 if result.get('success') else 404)
