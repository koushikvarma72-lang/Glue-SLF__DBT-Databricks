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
import re
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


# ─── config/audit framework introspection (Phase 0.2 of the gap plan) ────────
#
# Metadata-driven platforms keep their behavior in RDS control tables
# (configuration_master, dq_rules, message_template, …). For migration those
# ROWS are the input — not just the schema — so this section detects the known
# control tables (by name, falling back to a column fingerprint) and pulls
# their contents, capped and with secret-looking columns masked.

# canonical table → (name regexes, column-hint set). A table is classified by
# the FIRST name-pattern match; otherwise by ≥3 column hints (fingerprint).
_FRAMEWORK_TABLES: dict[str, dict] = {
    "configuration_master": {
        "names": [r"^config(uration)?_master$", r"^cdl_config(uration)?(_master)?$"],
        "columns": {"source_system", "file_pattern", "file_name_pattern", "target_table",
                    "frequency", "is_active", "active_flag", "landing_path"},
    },
    "file_process_log": {
        "names": [r"^file_process_log$", r"^file_processing_log$"],
        "columns": {"file_name", "file_status", "batch_id", "record_count",
                    "processed_date", "process_status", "file_received_date"},
    },
    "parent_batch_process": {
        "names": [r"^parent_batch(_process)?$", r"^batch_process(_master)?$"],
        "columns": {"batch_id", "batch_status", "batch_start_time", "batch_end_time",
                    "batch_date", "parent_batch_id"},
    },
    "cdl_ingestion_log": {
        "names": [r"^(cdl_)?ingestion_log$"],
        "columns": {"table_name", "ingestion_status", "batch_id", "row_count",
                    "ingestion_start", "ingestion_end", "layer"},
    },
    "query_configuration": {
        "names": [r"^query_config(uration)?$"],
        "columns": {"query_text", "query_sql", "sql_text", "execution_order",
                    "step_number", "target_table", "query_type"},
    },
    "dq_rules": {
        "names": [r"^dq_rules?$", r"^data_quality_rules?$"],
        "columns": {"rule_name", "rule_type", "rule_expression", "column_name",
                    "table_name", "severity", "threshold", "is_active"},
    },
    "message_template": {
        "names": [r"^message_templates?$", r"^notification_templates?$"],
        "columns": {"template_name", "template_body", "message_body", "subject",
                    "recipients", "notification_type"},
    },
    "cdl_ds_snowflake_replicate": {
        "names": [r"^(cdl_|dl_)?(ds_)?snowflake_replicat(e|ion)$"],
        "columns": {"snowflake_table", "snowflake_schema", "replicate_flag",
                    "source_table", "sync_status", "last_sync_time"},
    },
    # Revealed by the reference CDL's actual Glue job source (raw_to_curated /
    # curated_to_publish / parent_batch_close read these):
    "dq_rules_master": {
        "names": [r"^dq_rules_master$"],
        "columns": {"rule_name", "sql_query", "is_active"},
    },
    "outbound_logs": {
        "names": [r"^outbound_logs?$"],
        "columns": {"output_status", "batch_id", "source_system"},
    },
    "stitching_configuration": {
        "names": [r"^stitching_config(uration)?$"],
        "columns": {"stitching_type", "target_table_name", "source_table_name",
                    "primary_keys", "record_load_key", "dl_source"},
    },
    "outbound_query_configuration": {
        "names": [r"^outbound_query_config(uration)?$"],
        "columns": {"target_location_path", "source_system", "sql_query"},
    },
}

# Columns whose VALUES must never leave the source DB unmasked.
_SECRET_COL_RE = re.compile(r"(password|passwd|secret|token|api_?key|credential|private_?key)", re.I)

_MAX_FRAMEWORK_ROWS = 500  # hard cap regardless of the requested row_cap


def _norm_table_name(name: str) -> str:
    """Lowercase, strip quotes/schema, collapse non-alphanumerics to '_'."""
    base = str(name or "").strip().strip('"').split(".")[-1]
    return re.sub(r"[^a-z0-9]+", "_", base.lower()).strip("_")


def classify_framework_table(name: str, columns: list[str]) -> dict | None:
    """Classify a table as one of the known control tables.

    Returns {canonical, matched_by: 'name'|'columns'} or None. Pure — unit-testable.
    """
    norm = _norm_table_name(name)
    cols = {_norm_table_name(c) for c in columns or []}
    for canonical, spec in _FRAMEWORK_TABLES.items():
        if any(re.match(pat, norm) for pat in spec["names"]):
            return {"canonical": canonical, "matched_by": "name"}
    best, best_hits = None, 0
    for canonical, spec in _FRAMEWORK_TABLES.items():
        hits = len(cols & spec["columns"])
        if hits >= 3 and hits > best_hits:
            best, best_hits = canonical, hits
    if best:
        return {"canonical": best, "matched_by": "columns"}
    return None


def mask_row(columns: list[str], row: list) -> list:
    """Replace values of secret-looking columns with '***'. Pure."""
    return ["***" if _SECRET_COL_RE.search(str(col or "")) and val not in (None, "")
            else val for col, val in zip(columns, row)]


def _quote_ident(name: str) -> str:
    return '"' + str(name or "").replace('"', '""') + '"'


def introspect_framework_tables(config: PostgresConnectionConfig,
                                extra_tables: list[str] | None = None,
                                row_cap: int = 200) -> dict:
    """Find the control-framework tables and pull their rows (capped, masked).

    ``extra_tables`` lets the user force-include tables the heuristics missed
    (review-UI "mark as control table"). Returns {success, framework_tables:
    [{name, schema, canonical, matched_by, columns, rows, row_count, truncated}]}.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    row_cap = max(1, min(int(row_cap or 200), _MAX_FRAMEWORK_ROWS))
    forced = {_norm_table_name(t) for t in extra_tables or []}

    listing = list_postgres_objects(config)
    if not listing.get("success"):
        return listing
    candidates = []
    for obj in listing.get("tables", []):
        if obj.get("kind") != "table":
            continue
        col_names = [c["name"] for c in obj.get("columns", [])]
        cls = classify_framework_table(obj["name"], col_names)
        if cls is None and _norm_table_name(obj["name"]) in forced:
            cls = {"canonical": _norm_table_name(obj["name"]), "matched_by": "forced"}
        if cls:
            candidates.append((obj, col_names, cls))

    conn = None
    out = []
    try:
        conn = _connect(config)
        cur = conn.cursor()
        for obj, col_names, cls in candidates:
            fq = f'{_quote_ident(obj["schema"])}.{_quote_ident(obj["name"])}'
            entry = {"name": obj["name"], "schema": obj["schema"],
                     "canonical": cls["canonical"], "matched_by": cls["matched_by"],
                     "columns": obj.get("columns", []), "rows": [], "row_count": 0,
                     "truncated": False}
            try:
                cur.execute(f"SELECT COUNT(*) FROM {fq}")
                total = int((cur.fetchone() or [0])[0])
                cur.execute(f"SELECT * FROM {fq} LIMIT {row_cap}")
                fetched_cols = [d[0] for d in cur.description or []] or col_names
                rows = [mask_row(fetched_cols, [_jsonable(v) for v in r]) for r in cur.fetchall()]
                entry.update({"rows": rows, "row_count": total,
                              "columns": [{"name": c} for c in fetched_cols]
                              if fetched_cols != col_names else entry["columns"],
                              "truncated": total > len(rows)})
            except Exception as exc:  # noqa: BLE001 — per-table, don't sink the batch
                entry["error"] = str(exc)
            out.append(entry)
        return {"success": True, "framework_tables": out,
                "detected": len([e for e in out if "error" not in e])}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Postgres framework introspection failed: %s", exc)
        return {"success": False, "error": f"Framework introspection failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def _jsonable(v):
    """Coerce driver values (datetime/Decimal/bytes/…) to JSON-safe primitives."""
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


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
