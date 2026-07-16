"""Snowflake source connector for the Snowflake/Glue → Databricks/DBT flow.

Collects connection details, tests connectivity, lists tables/views, and pulls
object DDL (view/table definitions) from INFORMATION_SCHEMA so the lineage engine
can derive table-to-table dependencies by parsing those definitions.

The ``snowflake-connector-python`` package is imported lazily (like boto3 for
Bedrock) so the rest of the app runs even when it isn't installed; callers get a
clean, actionable error instead of an ImportError at startup.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_SIMPLE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _ident(name) -> str:
    """Render an identifier for SQL. Simple names are left UNQUOTED so Snowflake
    normalizes case (matching the typical uppercase-stored names) rather than
    forcing an exact-case match; names with special chars are safely quoted."""
    name = str(name or "")
    if _SIMPLE_IDENT.match(name):
        return name
    return '"' + name.replace('"', '""') + '"'


def _sf_type_from_show(data_type) -> str:
    """Turn a SHOW COLUMNS ``data_type`` (a JSON blob) into a Snowflake type string.

    e.g. {"type":"FIXED","precision":38,"scale":0} -> NUMBER(38,0);
    {"type":"TEXT","length":255} -> VARCHAR(255); REAL -> FLOAT; others pass through.
    """
    try:
        d = json.loads(data_type) if isinstance(data_type, str) else (data_type or {})
    except (ValueError, TypeError):
        return str(data_type or "TEXT")
    if not isinstance(d, dict):
        return str(data_type or "TEXT")
    t = str(d.get("type") or "TEXT").upper()
    if t == "FIXED":
        return f"NUMBER({d.get('precision', 38)},{d.get('scale', 0)})"
    if t == "TEXT":
        ln = d.get("length")
        return f"VARCHAR({ln})" if ln else "VARCHAR"
    if t == "REAL":
        return "FLOAT"
    return t


@dataclass
class SnowflakeConnectionConfig:
    account: str = ""          # e.g. ab12345.us-east-1
    user: str = ""
    password: str = ""
    role: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""           # optional; blank = all schemas in the database
    authenticator: str = ""    # optional, e.g. 'externalbrowser' / 'snowflake' (default)

    @classmethod
    def from_payload(cls, payload: dict) -> "SnowflakeConnectionConfig":
        p = payload or {}

        def pick(*keys):
            for k in keys:
                v = p.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            return ""

        return cls(
            account=pick("account", "snowflake_account", "snowflakeAccount"),
            user=pick("user", "username", "snowflake_user"),
            password=pick("password", "snowflake_password"),
            role=pick("role"),
            warehouse=pick("warehouse"),
            database=pick("database", "db"),
            schema=pick("schema"),
            authenticator=pick("authenticator").lower(),
        )

    def masked(self) -> dict:
        data = asdict(self)
        data["password"] = ""
        data["password_present"] = bool(self.password)
        return data

    def public_persisted(self) -> dict:
        data = self.masked()
        data["last_saved_at"] = datetime.now(timezone.utc).isoformat()
        return data


def validate_config(config: SnowflakeConnectionConfig) -> list[str]:
    errors = []
    if not config.account:
        errors.append("Snowflake account identifier is required (e.g. ab12345.us-east-1).")
    if not config.user:
        errors.append("Snowflake user is required.")
    if not config.password and config.authenticator != "externalbrowser":
        errors.append("Snowflake password is required (or use the externalbrowser authenticator).")
    # Database is intentionally NOT required here: the user connects first, then
    # picks a database from the SHOW DATABASES list. Object listing enforces it.
    return errors


def _connect(config: SnowflakeConnectionConfig):
    """Open a Snowflake connection. Lazy-imports the connector with a clear error."""
    try:
        import snowflake.connector as sf  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Snowflake support requires the 'snowflake-connector-python' package. "
            "Install it in the server environment:  pip install \"snowflake-connector-python>=3.12\""
        ) from exc

    kwargs = {
        "account": config.account,
        "user": config.user,
        "login_timeout": 20,
        "network_timeout": 30,
    }
    if config.password:
        kwargs["password"] = config.password
    if config.authenticator:
        kwargs["authenticator"] = config.authenticator
    if config.role:
        kwargs["role"] = config.role
    if config.warehouse:
        kwargs["warehouse"] = config.warehouse
    if config.database:
        kwargs["database"] = config.database
    if config.schema:
        kwargs["schema"] = config.schema
    return sf.connect(**kwargs)


def _rows(cursor) -> list[dict]:
    cols = [c[0].lower() for c in (cursor.description or [])]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def test_snowflake_connection(config: SnowflakeConnectionConfig) -> dict:
    """Validate config and run a trivial identity query. Returns {success, ...}."""
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        cur.execute("select current_account(), current_user(), current_role(), current_warehouse()")
        account, user, role, warehouse = cur.fetchone()
        # Preload warehouses + databases the role can see so the UI can offer
        # pickers. Best-effort: SHOW may be denied for the role — don't fail the test.
        warehouses, databases = [], []
        try:
            cur.execute("SHOW WAREHOUSES")
            warehouses = [r.get("name") for r in _rows(cur) if r.get("name")]
        except Exception as exc:  # noqa: BLE001
            logger.info("Could not list Snowflake warehouses (non-fatal): %s", exc)
        try:
            cur.execute("SHOW DATABASES")
            databases = [r.get("name") for r in _rows(cur) if r.get("name")]
        except Exception as exc:  # noqa: BLE001
            logger.info("Could not list Snowflake databases (non-fatal): %s", exc)
        return {
            "success": True,
            "identity": {
                "account": account,
                "user": user,
                "role": role,
                "warehouse": warehouse,
                "database": config.database,
            },
            "warehouses": warehouses,
            "databases": databases,
        }
    except RuntimeError as exc:  # missing package
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 — surface connector errors cleanly
        logger.warning("Snowflake connection test failed: %s", exc)
        return {"success": False, "error": f"Snowflake connection failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def list_snowflake_schemas(config: SnowflakeConnectionConfig) -> dict:
    """List the schemas in the configured database (so the UI can offer a picker)."""
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    if not config.database:
        return {"success": False, "error": "Select a Snowflake database first."}
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS IN DATABASE {_ident(config.database)}")
        schemas = sorted(
            r.get("name") for r in _rows(cur)
            if r.get("name") and r.get("name") != "INFORMATION_SCHEMA"
        )
        return {"success": True, "schemas": schemas}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Snowflake schema listing failed: %s", exc)
        return {"success": False, "error": f"Snowflake schema listing failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def list_snowflake_pipeline_objects(config: SnowflakeConnectionConfig) -> dict:
    """List pipeline-layer objects (Phase 5 of the gap plan): tasks, streams,
    pipes, procedures, stages. Each SHOW is best-effort (privilege-tolerant) —
    a denied SHOW yields an empty list + a per-kind error, never a hard failure.

    Returns {success, tasks, streams, pipes, procedures, stages, errors}.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    if not config.database:
        return {"success": False, "error": "Select a Snowflake database first."}

    scope = f"IN DATABASE {_ident(config.database)}"
    if config.schema:
        scope = f"IN SCHEMA {_ident(config.database)}.{_ident(config.schema)}"

    def _show(cur, kind, mapper):
        try:
            cur.execute(f"SHOW {kind} {scope}")
            return [mapper(r) for r in _rows(cur)], None
        except Exception as exc:  # noqa: BLE001 — per-kind, privilege-tolerant
            return [], str(exc)

    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        out, errs = {}, {}
        out["tasks"], errs["tasks"] = _show(cur, "TASKS", lambda r: {
            "name": r.get("name"), "schema": r.get("schema_name"),
            "schedule": r.get("schedule") or "", "state": r.get("state") or "",
            "predecessors": r.get("predecessors") or "", "definition": r.get("definition") or "",
            "warehouse": r.get("warehouse") or ""})
        out["streams"], errs["streams"] = _show(cur, "STREAMS", lambda r: {
            "name": r.get("name"), "schema": r.get("schema_name"),
            "table_name": r.get("table_name") or "", "mode": r.get("mode") or "",
            "stale": r.get("stale") or ""})
        out["pipes"], errs["pipes"] = _show(cur, "PIPES", lambda r: {
            "name": r.get("name"), "schema": r.get("schema_name"),
            "definition": r.get("definition") or "",
            "notification_channel": r.get("notification_channel") or ""})
        out["procedures"], errs["procedures"] = _show(cur, "PROCEDURES", lambda r: {
            "name": r.get("name"), "schema": r.get("schema_name"),
            "arguments": r.get("arguments") or ""})
        out["stages"], errs["stages"] = _show(cur, "STAGES", lambda r: {
            "name": r.get("name"), "schema": r.get("schema_name"),
            "url": r.get("url") or "", "type": r.get("type") or ""})
        return {"success": True, **out,
                "errors": {k: v for k, v in errs.items() if v}}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Snowflake pipeline-object listing failed: %s", exc)
        return {"success": False, "error": f"Snowflake pipeline listing failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def list_snowflake_objects(config: SnowflakeConnectionConfig) -> dict:
    """List base tables and views (with columns) in the configured database/schema.

    Returns {success, tables: [...], views: [...]} where each item is
    {database, schema, name, full_name, kind, columns: [{name, type}]}.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    if not config.database:
        return {"success": False, "error": "Select a Snowflake database first."}

    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        # Prefer SHOW (privilege-aware, matches Snowsight). If it errors — e.g.
        # the role lacks schema USAGE or the identifier won't resolve — fall back
        # to INFORMATION_SCHEMA (case-insensitive) before giving up.
        try:
            result = _list_objects_via_show(cur, config)
        except Exception as show_exc:  # noqa: BLE001
            logger.info("SHOW listing failed (%s); trying INFORMATION_SCHEMA", show_exc)
            try:
                result = _list_objects_via_information_schema(cur, config)
            except Exception as is_exc:  # noqa: BLE001
                loc = config.database + (f".{config.schema}" if config.schema else "")
                return {"success": False, "error": (
                    f"Could not list objects in {loc}: {is_exc}. The connecting role may lack USAGE on the "
                    "database/schema, or the name/case is wrong — try a role that can see it in Snowsight."
                )}
        return {"success": True, **result}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Snowflake object listing failed: %s", exc)
        return {"success": False, "error": f"Snowflake listing failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def _list_objects_via_show(cur, config) -> dict:
    if config.schema:
        scope = f"IN SCHEMA {_ident(config.database)}.{_ident(config.schema)}"
    else:
        scope = f"IN DATABASE {_ident(config.database)}"

    cols_by_obj: dict[tuple, list] = {}
    try:
        cur.execute(f"SHOW COLUMNS {scope}")
        for r in _rows(cur):
            cols_by_obj.setdefault((r.get("schema_name"), r.get("table_name")), []).append(
                {"name": r.get("column_name"), "type": _sf_type_from_show(r.get("data_type"))}
            )
    except Exception as exc:  # noqa: BLE001 — columns are best-effort
        logger.info("SHOW COLUMNS failed (non-fatal): %s", exc)

    def _objs(rows, kind):
        out = []
        for r in rows:
            schema, name = r.get("schema_name"), r.get("name")
            if not name:
                continue
            # Skip Snowflake's built-in metadata schema — its hundreds of system
            # views aren't user objects (only relevant when no schema is selected,
            # so SHOW ... IN DATABASE doesn't flood the list with INFORMATION_SCHEMA).
            if str(schema or "").upper() == "INFORMATION_SCHEMA":
                continue
            out.append({
                "database": config.database, "schema": schema, "name": name,
                "full_name": f"{config.database}.{schema}.{name}", "kind": kind,
                "columns": cols_by_obj.get((schema, name), []),
            })
        return out

    cur.execute(f"SHOW TABLES {scope}")  # raises → caller falls back
    tables = _objs(_rows(cur), "table")
    views = []
    try:
        cur.execute(f"SHOW VIEWS {scope}")
        views = _objs(_rows(cur), "view")
    except Exception as exc:  # noqa: BLE001
        logger.info("SHOW VIEWS failed (non-fatal): %s", exc)
    return {"tables": tables, "views": views}


def _list_objects_via_information_schema(cur, config) -> dict:
    schema_filter = "and upper(table_schema) = upper(%(schema)s)" if config.schema else ""
    params = {"schema": config.schema} if config.schema else {}
    db = _ident(config.database)
    cur.execute(
        f"select table_schema, table_name, table_type from {db}.information_schema.tables "
        f"where table_schema <> 'INFORMATION_SCHEMA' {schema_filter} order by table_schema, table_name",
        params,
    )
    table_meta = _rows(cur)
    cur.execute(
        f"select table_schema, table_name, column_name, data_type, ordinal_position "
        f"from {db}.information_schema.columns where table_schema <> 'INFORMATION_SCHEMA' {schema_filter} "
        f"order by table_schema, table_name, ordinal_position",
        params,
    )
    cols_by_obj: dict[tuple, list] = {}
    for r in _rows(cur):
        cols_by_obj.setdefault((r["table_schema"], r["table_name"]), []).append(
            {"name": r["column_name"], "type": r["data_type"]}
        )
    tables, views = [], []
    for r in table_meta:
        schema, name = r["table_schema"], r["table_name"]
        is_view = "VIEW" in str(r.get("table_type") or "").upper()
        obj = {
            "database": config.database, "schema": schema, "name": name,
            "full_name": f"{config.database}.{schema}.{name}",
            "kind": "view" if is_view else "table",
            "columns": cols_by_obj.get((schema, name), []),
        }
        (views if is_view else tables).append(obj)
    return {"tables": tables, "views": views}


def list_snowflake_relationships(config: SnowflakeConnectionConfig) -> dict:
    """List declared foreign-key relationships so they can be shown in the lineage
    and carried forward as Databricks constraints.

    Uses ``SHOW IMPORTED KEYS`` (FK → referenced PK). Returns {success,
    relationships: [{constraint, fk_table, fk_columns, pk_table, pk_columns}]}
    with full ``db.schema.table`` names. Best-effort: empty list if none/denied.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    if not config.database:
        return {"success": False, "error": "Select a Snowflake database first."}

    if config.schema:
        scope = f"IN SCHEMA {_ident(config.database)}.{_ident(config.schema)}"
    else:
        scope = f"IN DATABASE {_ident(config.database)}"
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        cur.execute(f"SHOW IMPORTED KEYS {scope}")
        rows = _rows(cur)
        groups = {}
        for r in rows:
            fk_full = f"{r.get('fk_database_name')}.{r.get('fk_schema_name')}.{r.get('fk_table_name')}"
            pk_full = f"{r.get('pk_database_name')}.{r.get('pk_schema_name')}.{r.get('pk_table_name')}"
            key = r.get("fk_name") or f"{fk_full}->{pk_full}"
            g = groups.setdefault(key, {
                "constraint": r.get("fk_name") or "",
                "fk_table": fk_full, "pk_table": pk_full,
                "_cols": [],
            })
            try:
                seq = int(r.get("key_sequence") or 0)
            except (TypeError, ValueError):
                seq = 0
            g["_cols"].append((seq, r.get("fk_column_name"), r.get("pk_column_name")))
        relationships = []
        for g in groups.values():
            ordered = sorted(g["_cols"], key=lambda c: c[0])
            relationships.append({
                "constraint": g["constraint"],
                "fk_table": g["fk_table"],
                "fk_columns": [c[1] for c in ordered if c[1]],
                "pk_table": g["pk_table"],
                "pk_columns": [c[2] for c in ordered if c[2]],
            })
        return {"success": True, "relationships": relationships}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Snowflake relationship listing failed: %s", exc)
        return {"success": False, "error": f"Snowflake relationship listing failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def open_query_runner(config: SnowflakeConnectionConfig):
    """Open ONE Snowflake connection and return ``(run, close)`` where ``run(sql)``
    executes a query and returns its rows as positional tuples.

    For the reconciliation harness, which runs several aggregate/count queries against the
    same source table — reusing one connection avoids a connect per query. The caller MUST
    call ``close()`` (use try/finally). Raises RuntimeError if the connector is missing.
    """
    conn = _connect(config)

    def run(sql: str):
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass

    def close():
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    return run, close


def fetch_table_columns(config: SnowflakeConnectionConfig, full_name: str) -> list[dict]:
    """Live column list ``[{name, type}]`` for one ``db.schema.table`` (information_schema).

    Used by reconciliation to ground the per-table fingerprint in the real source columns.
    Returns [] on any failure (the caller reports "no readable columns")."""
    parts = str(full_name or "").split(".")
    if len(parts) < 2:
        return []
    schema, table = parts[-2], parts[-1]
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        cur.execute(
            "select column_name, data_type from information_schema.columns "
            "where upper(table_schema) = upper(%(s)s) and upper(table_name) = upper(%(t)s) "
            "order by ordinal_position",
            {"s": schema, "t": table},
        )
        return [{"name": r["column_name"], "type": r["data_type"]} for r in _rows(cur)]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Snowflake column fetch failed for %s: %s", full_name, exc)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def fetch_object_ddl(config: SnowflakeConnectionConfig) -> dict:
    """Fetch view definitions (and as much DDL as available) for dependency parsing.

    Returns {success, ddl: {full_name: definition_sql}}. View definitions come
    straight from INFORMATION_SCHEMA.VIEWS.view_definition (one bulk query, no
    per-object round trips).
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    if not config.database:
        return {"success": False, "error": "Select a Snowflake database first."}

    schema_filter = "and upper(table_schema) = upper(%(schema)s)" if config.schema else ""
    params = {"schema": config.schema} if config.schema else {}
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        cur.execute(
            f"""
            select table_schema, table_name, view_definition
            from information_schema.views
            where table_schema <> 'INFORMATION_SCHEMA' {schema_filter}
            """,
            params,
        )
        ddl = {}
        for r in _rows(cur):
            full = f"{config.database}.{r['table_schema']}.{r['table_name']}"
            ddl[full] = r.get("view_definition") or ""
        return {"success": True, "ddl": ddl}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Snowflake DDL fetch failed: %s", exc)
        return {"success": False, "error": f"Snowflake DDL fetch failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
