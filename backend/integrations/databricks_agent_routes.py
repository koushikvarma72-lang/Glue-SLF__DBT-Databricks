"""Lean shim for the standalone sfglue app.

The sfglue routes lazy-import exactly ONE symbol from the BI app's Databricks agent —
``introspect_schema_tables`` (live column introspection for source-column grounding). We
reproduce just that here so this app doesn't carry the full 1,400-line BI databricks agent.
Same import path (``backend.integrations.databricks_agent_routes``) so the copied routes
resolve it unchanged.
"""
from qvd_to_databricks.databricks_executor import execute_sql_statement


def introspect_schema_tables(config, catalog, schema):
    """Read the live column list for every table in ``catalog.schema``.

    Returns ``(tables, error)`` where ``tables`` is ``[{name, fields:[col, ...]}]`` (or
    ``None`` with a structured ``error``)."""
    catalog = (catalog or config.catalog or 'main').strip()
    schema = (schema or config.schema or 'default').strip()
    if not config.sql_warehouse_id:
        return None, {'message': 'A SQL Warehouse ID is required to read the live schema.'}
    sql = (
        f"SELECT table_name, column_name "
        f"FROM `{catalog}`.information_schema.columns "
        f"WHERE table_schema = '{schema}' "
        f"ORDER BY table_name, ordinal_position"
    )
    result = execute_sql_statement(
        sql, config.sql_warehouse_id, catalog=catalog, schema=schema,
        config=config, stage='introspect_schema')
    if result.get('success') is False:
        return None, {
            'message': result.get('message') or result.get('error') or 'Schema introspection failed.',
            'error_code': result.get('error_code'),
        }
    rows = ((result.get('result') or {}).get('data_array')) or []
    by_table = {}
    for row in rows:
        if not row or len(row) < 2:
            continue
        table_name, column_name = row[0], row[1]
        if table_name and column_name:
            by_table.setdefault(table_name, []).append(column_name)
    return [{'name': name, 'fields': fields} for name, fields in by_table.items()], None
