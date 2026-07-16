"""In-app reference-environment tools.

The reference CDL pipeline is config-driven: bucket names live in an INI file on
S3 (read by load_config into workflow run-properties) and path templates live in
``configuration_master``. These helpers let the app do what previously needed
aws-cli/psql by hand:

  * list S3 buckets (the Connect page's bucket picker)
  * inspect / repoint the INI's four bucket keys (with an S3 backup copy)
  * apply the control-schema seed (reference_cdl/seed_control_db.sql) to Postgres
  * report configuration_master's s3_*_path templates for review

Reuses the Glue connection's AWS session and the Postgres connection config —
no new credential surfaces.
"""

from __future__ import annotations

import configparser
import io
import logging
import os
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_BUCKET_KEYS = ("s3_vendor_bucket", "s3_dl_bucket", "s3_dl_curated_bucket", "s3_dl_publish_bucket")

_SEED_SQL_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                              "reference_cdl", "seed_control_db.sql")


def list_s3_buckets(glue_config) -> dict:
    """All buckets visible to the connected AWS credentials."""
    from backend.integrations.glue_client import _aws_session, validate_config
    errors = validate_config(glue_config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(glue_config) as session:
            s3 = session.client("s3")
        resp = s3.list_buckets()
        return {"success": True,
                "buckets": sorted(b["Name"] for b in resp.get("Buckets", []))}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Could not list buckets: {exc}"}


def read_pipeline_ini(glue_config, bucket: str, key: str) -> dict:
    """Fetch + parse the pipeline INI; return sections and their bucket keys."""
    from backend.integrations.glue_client import _aws_session
    try:
        with _aws_session(glue_config) as session:
            s3 = session.client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        cp = configparser.ConfigParser()
        cp.read_string(body)
        sections = {}
        for sec in cp.sections():
            sections[sec] = {k: cp.get(sec, k) for k in _BUCKET_KEYS if cp.has_option(sec, k)}
        return {"success": True, "sections": sections, "raw_length": len(body)}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Could not read s3://{bucket}/{key}: {exc}"}


def repoint_pipeline_ini(glue_config, bucket: str, key: str, section: str,
                         target_bucket: str) -> dict:
    """Set the four bucket keys in ``section`` to ``target_bucket``.

    A timestamped backup of the original is written next to it first
    (<key>.bak-YYYYmmddTHHMMSS). Returns the before/after values.
    """
    from backend.integrations.glue_client import _aws_session
    if not target_bucket:
        return {"success": False, "error": "target bucket is required"}
    try:
        with _aws_session(glue_config) as session:
            s3 = session.client("s3")
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        cp = configparser.ConfigParser()
        cp.read_string(body)
        if section not in cp.sections():
            return {"success": False,
                    "error": f"Section {section!r} not in INI (has: {cp.sections()})"}
        before = {k: cp.get(section, k) for k in _BUCKET_KEYS if cp.has_option(section, k)}
        for k in _BUCKET_KEYS:
            cp.set(section, k, target_bucket)
        out = io.StringIO()
        cp.write(out)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        backup_key = f"{key}.bak-{stamp}"
        s3.put_object(Bucket=bucket, Key=backup_key, Body=body.encode("utf-8"))
        s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue().encode("utf-8"))
        after = {k: target_bucket for k in _BUCKET_KEYS}
        logger.info("reference: repointed %s [%s] -> %s (backup %s)",
                    key, section, target_bucket, backup_key)
        return {"success": True, "before": before, "after": after,
                "backup_key": backup_key}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Repoint failed: {exc}"}


def _split_sql(text: str) -> list[str]:
    """Split the seed file into statements (comment-aware, no ; inside literals here)."""
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def seed_control_schema(pg_config) -> dict:
    """Apply reference_cdl/seed_control_db.sql to the connected control DB."""
    from backend.integrations.postgres_client import _connect, validate_config
    errors = validate_config(pg_config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with open(os.path.normpath(_SEED_SQL_PATH), encoding="utf-8") as f:
            sql_text = f.read()
    except OSError as exc:
        return {"success": False, "error": f"Seed file not found: {exc}"}
    statements = _split_sql(sql_text)
    conn = None
    results = []
    try:
        conn = _connect(pg_config)
        cur = conn.cursor()
        for stmt in statements:
            label = " ".join(stmt.split()[:4])
            try:
                cur.execute(stmt)
                results.append({"statement": label, "status": "ok"})
            except Exception as exc:  # noqa: BLE001 — report per-statement, keep going
                conn.rollback()
                results.append({"statement": label, "status": "failed", "error": str(exc)[:200]})
                continue
            conn.commit()
        ok = sum(1 for r in results if r["status"] == "ok")
        return {"success": ok == len(results), "applied": ok,
                "total": len(results), "results": results}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Seed failed: {exc}", "results": results}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


_PATH_COLUMNS = ("s3_landing_path", "s3_archive_path", "s3_raw_path",
                 "s3_curated_path", "s3_publish_path", "s3_reject_path")


def repoint_config_paths(pg_config, old_bucket: str, new_bucket: str = "",
                         mode: str = "template") -> dict:
    """Rewrite hardcoded ``s3://<old_bucket>/`` prefixes in configuration_master's
    path columns.

    mode='template' (default, recommended): replace with ``s3://{bucket_name}/``
    — the jobs already ``.format(bucket_name=<INI value>)`` every path column, so
    this makes the rows genuinely config-driven: future bucket moves are an INI
    edit only. mode='hardcode': replace with ``s3://<new_bucket>/`` (old behavior,
    trades one baked-in bucket for another).
    """
    from backend.integrations.postgres_client import _connect, validate_config
    errors = validate_config(pg_config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    if not old_bucket:
        return {"success": False, "error": "old bucket name is required"}
    if mode == "hardcode" and not new_bucket:
        return {"success": False, "error": "new bucket name is required for hardcode mode"}
    old_prefix = f"s3://{old_bucket}/"
    new_prefix = "s3://{bucket_name}/" if mode == "template" else f"s3://{new_bucket}/"
    conn = None
    try:
        conn = _connect(pg_config)
        cur = conn.cursor()
        counts = {}
        for col in _PATH_COLUMNS:
            try:
                cur.execute(
                    f"UPDATE configuration_master SET {col} = replace({col}, %s, %s) "
                    f"WHERE {col} LIKE %s", (old_prefix, new_prefix, old_prefix + "%"))
                counts[col] = cur.rowcount
            except Exception as exc:  # noqa: BLE001 — column may not exist in every schema
                conn.rollback()
                counts[col] = f"skipped: {str(exc)[:80]}"
                continue
            conn.commit()
        updated = sum(v for v in counts.values() if isinstance(v, int))
        logger.info("reference: config paths %s: %s -> %s (%d updates)",
                    mode, old_bucket, new_prefix, updated)
        note = ("Paths are now {bucket_name} templates — the INI alone controls the "
                "bucket from here on (repoint the INI, nothing else)."
                if mode == "template" else
                "Paths now hardcode the new bucket; re-run this for any future move, "
                "or switch to template mode for config-driven paths.")
        return {"success": True, "updated": updated, "mode": mode,
                "per_column": counts, "note": note}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Config-path repoint failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


def config_path_report(pg_config, expected_zones=("landing", "raw", "curated", "publish")) -> dict:
    """configuration_master's s3_*_path templates + whether each looks aligned
    with the kit's zone layout ({bucket_name} placeholder + zone prefix)."""
    from backend.integrations.postgres_client import _connect, validate_config
    errors = validate_config(pg_config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    conn = None
    try:
        conn = _connect(pg_config)
        cur = conn.cursor()
        cur.execute("SELECT source_system, source_object_name, s3_landing_path, s3_raw_path, "
                    "s3_curated_path, s3_publish_path FROM configuration_master "
                    "WHERE active_flag = 'A'")
        rows = cur.fetchall()
        report = []
        for r in rows:
            entry = {"source_system": r[0], "source_object_name": r[1], "paths": {}}
            for zone, val in zip(("landing", "raw", "curated", "publish"), r[2:6]):
                val = str(val or "")
                entry["paths"][zone] = {
                    "template": val,
                    "has_bucket_placeholder": "{bucket_name}" in val,
                    "mentions_zone": bool(re.search(rf"\b{zone}", val)),
                }
            report.append(entry)
        return {"success": True, "rows": report}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Config-path report failed: {exc}"}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
