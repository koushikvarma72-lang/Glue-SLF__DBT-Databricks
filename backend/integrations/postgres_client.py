"""Postgres source client for the Snowflake+Glue → Databricks migration.

Some pipelines originate data in **Postgres** (or read transform logic/lookups from it) and
ship it to Snowflake. When migrating to Databricks we want two things:
  1. See those tables in the lineage and know they came from Postgres (external origin).
  2. REDIRECT the migrated pipeline to read them straight from Postgres into Delta bronze via
     JDBC — keeping Postgres as the live source instead of freezing a Snowflake snapshot.

This module only does (1)'s introspection + a connection test. The actual bronze ingestion
runs on Databricks (Spark JDBC) — see ``generate_postgres_bronze_ingestion`` in
``snowflake_glue_migration`` — so it needs no Postgres driver at runtime; this tool only needs
a lightweight driver to LIST tables/columns. We use **pg8000** (pure-Python — no build step,
works on any Python) so introspection has no native-wheel dependency.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Postgres system schemas that hold no user tables — never list these.
_SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}


@dataclass
class PostgresConnectionConfig:
    host: str = ""
    port: str = "5432"
    database: str = ""
    user: str = ""
    password: str = ""
    schema: str = ""       # optional; blank = all non-system schemas
    sslmode: str = ""      # optional: 'require' etc.

    @classmethod
    def from_payload(cls, payload: dict) -> "PostgresConnectionConfig":
        p = payload or {}

        def pick(*keys, default=""):
            for k in keys:
                v = p.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            return default

        return cls(
            host=pick("host", "postgres_host", "pg_host"),
            port=pick("port", "postgres_port", "pg_port", default="5432"),
            database=pick("database", "db", "postgres_database"),
            user=pick("user", "username", "postgres_user"),
            password=pick("password", "postgres_password"),
            schema=pick("schema", "postgres_schema"),
            sslmode=pick("sslmode").lower(),
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


def validate_config(config: PostgresConnectionConfig) -> list[str]:
    errors = []
    if not config.host:
        errors.append("Postgres host is required.")
    if not config.database:
        errors.append("Postgres database is required.")
    if not config.user:
        errors.append("Postgres user is required.")
    # Password is NOT required: a local Postgres may use trust/peer auth (no password).
    # If the server does demand one, the connection simply fails with a clear auth error.
    return errors


def _connect(config: PostgresConnectionConfig):
    """Open a Postgres connection via pg8000. Lazy-import with a clear error."""
    try:
        import pg8000.dbapi as pg  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "Postgres support requires the 'pg8000' package (pure-Python). "
            "Install it in the server environment:  pip install \"pg8000>=1.30\""
        ) from exc
    try:
        port = int(str(config.port or "5432").strip())
    except (TypeError, ValueError):
        port = 5432
    kwargs = dict(host=config.host, port=port, database=config.database,
                  user=config.user, password=config.password, timeout=20)
    if config.sslmode and config.sslmode != "disable":
        import ssl
        kwargs["ssl_context"] = ssl.create_default_context()
        # Origin DBs are often self-signed; don't hard-fail the demo on cert verification.
        kwargs["ssl_context"].check_hostname = False
        kwargs["ssl_context"].verify_mode = ssl.CERT_NONE
    return pg.connect(**kwargs)


def test_postgres_connection(config: PostgresConnectionConfig) -> dict:
    """Validate config and run a trivial identity query. Returns {success, ...}."""
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user, version()")
        row = cur.fetchone() or []
        return {"success": True, "database": row[0] if row else config.database,
                "user": row[1] if len(row) > 1 else config.user,
                "version": (str(row[2])[:80] if len(row) > 2 else "")}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Postgres connection test failed: %s", exc)
        return {"success": False, "error": f"Postgres connection failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


# Map a Postgres type to a coarse label consistent with the Snowflake client's output, so
# lineage/matching treats a column the same regardless of which engine reported it.
def _pg_type(t: str) -> str:
    return str(t or "").strip().upper() or "TEXT"


def list_postgres_objects(config: PostgresConnectionConfig) -> dict:
    """List base tables (with columns) in the configured database (optionally one schema).

    Returns {success, tables: [{database, schema, name, full_name, kind, columns:[{name,type}]}]}.
    Views are listed too (kind='view') so a Postgres view shipped to Snowflake is still visible.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    conn = None
    try:
        conn = _connect(config)
        cur = conn.cursor()
        params: list = []
        schema_filter = ""
        if config.schema:
            schema_filter = " AND c.table_schema = %s"
            params.append(config.schema)
        else:
            schema_filter = " AND c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast')"
        sql = (
            "SELECT c.table_schema, c.table_name, c.column_name, c.data_type, t.table_type "
            "FROM information_schema.columns c "
            "JOIN information_schema.tables t "
            "  ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
            "WHERE t.table_type IN ('BASE TABLE','VIEW')" + schema_filter +
            " ORDER BY c.table_schema, c.table_name, c.ordinal_position"
        )
        cur.execute(sql, params)
        by_obj: dict[tuple, dict] = {}
        for r in cur.fetchall():
            sch, name, col, dtype, ttype = r[0], r[1], r[2], r[3], r[4]
            if str(sch) in _SYSTEM_SCHEMAS:
                continue
            key = (sch, name)
            obj = by_obj.get(key)
            if obj is None:
                obj = {
                    "database": config.database, "schema": sch, "name": name,
                    "full_name": f"{config.database}.{sch}.{name}",
                    "kind": "view" if "VIEW" in str(ttype).upper() else "table",
                    "columns": [],
                }
                by_obj[key] = obj
            if col:
                obj["columns"].append({"name": col, "type": _pg_type(dtype)})
        tables = list(by_obj.values())
        return {"success": True, "tables": tables}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Postgres object listing failed: %s", exc)
        return {"success": False, "error": f"Postgres listing failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
