"""Connection-test + source-picker routes (Snowflake / Glue / Postgres /
Databricks validation, Snowflake schema listing, S3 bucket picker). Behaviour
identical to the original monolithic handlers."""

from __future__ import annotations

from flask import Blueprint, jsonify
from backend.integrations.glue_client import GlueConnectionConfig, test_glue_connection
from backend.integrations.postgres_client import PostgresConnectionConfig, test_postgres_connection
from backend.integrations.snowflake_client import SnowflakeConnectionConfig, list_snowflake_schemas, test_snowflake_connection
from backend.integrations.routes._shared import body

bp = Blueprint("sfglue_connections", __name__)


@bp.route('/api/sfglue/snowflake/test-connection', methods=['POST'])
def sfglue_snowflake_test():
    data = body()
    config = SnowflakeConnectionConfig.from_payload(data.get('snowflake') or data)
    result = test_snowflake_connection(config)
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/sfglue/snowflake/schemas', methods=['POST'])
def sfglue_snowflake_schemas():
    data = body()
    config = SnowflakeConnectionConfig.from_payload(data.get('snowflake') or data)
    result = list_snowflake_schemas(config)
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/sfglue/glue/test-connection', methods=['POST'])
def sfglue_glue_test():
    data = body()
    config = GlueConnectionConfig.from_payload(data.get('glue') or data)
    result = test_glue_connection(config)
    return jsonify(result), (200 if result.get('success') else 400)

# ── AWS SSO device-flow login ("Sign in with AWS") ──────────────────────

@bp.route('/api/sfglue/postgres/test-connection', methods=['POST'])
def sfglue_postgres_test():
    data = body()
    config = PostgresConnectionConfig.from_payload(data.get('postgres') or data)
    result = test_postgres_connection(config)
    return jsonify(result), (200 if result.get('success') else 400)

@bp.route('/api/sfglue/databricks/test-connection', methods=['POST'])
def sfglue_databricks_test():
    """Validate the Databricks destination: token (SCIM me), SQL warehouse
    (exists + state), and catalog (Unity Catalog lookup). Pure REST, no SQL run."""
    import requests as _rq
    data = body()
    d = data.get('destination') or data
    host = str(d.get('workspace_url') or '').rstrip('/')
    token = d.get('token') or ''
    wh = d.get('sql_warehouse_id') or ''
    catalog = d.get('catalog') or ''
    if not host or not token:
        return jsonify({'success': False, 'error': 'workspace_url and token are required'}), 400
    hdrs = {'Authorization': f'Bearer {token}'}
    out = {'success': True, 'checks': {}}
    try:
        r = _rq.get(f'{host}/api/2.0/preview/scim/v2/Me', headers=hdrs, timeout=20)
        if r.status_code == 200:
            out['checks']['auth'] = {'ok': True, 'user': (r.json() or {}).get('userName', '')}
        else:
            return jsonify({'success': False, 'error': f'token rejected (HTTP {r.status_code})'}), 400
        if wh:
            r = _rq.get(f'{host}/api/2.0/sql/warehouses/{wh}', headers=hdrs, timeout=20)
            ok = r.status_code == 200
            out['checks']['warehouse'] = {'ok': ok,
                'state': (r.json() or {}).get('state', '') if ok else f'HTTP {r.status_code}'}
            if not ok:
                out['success'] = False
        if catalog:
            r = _rq.get(f'{host}/api/2.1/unity-catalog/catalogs/{catalog}', headers=hdrs, timeout=20)
            ok = r.status_code == 200
            out['checks']['catalog'] = {'ok': ok, 'name': catalog}
            if not ok:
                out['success'] = False
                out['error'] = f"catalog '{catalog}' not found"
    except Exception as exc:  # noqa: BLE001
        return jsonify({'success': False, 'error': f'Databricks unreachable: {exc}'}), 400
    return jsonify(out), (200 if out['success'] else 400)

@bp.route('/api/sfglue/aws/buckets', methods=['POST'])
def sfglue_aws_buckets():
    """List S3 buckets visible to the connected AWS credentials (bucket picker)."""
    from backend.integrations.reference_cdl_tools import list_s3_buckets
    data = body('glue', message="Connect AWS Glue first.")
    if not data.get('glue'):
        return jsonify({"success": False, "error": "Connect AWS Glue first."}), 400
    result = list_s3_buckets(GlueConnectionConfig.from_payload(data['glue']))
    return jsonify(result), (200 if result.get('success') else 400)
