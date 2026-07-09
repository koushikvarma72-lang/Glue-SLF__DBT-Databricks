"""AWS Glue source connector for the Snowflake/Glue → Databricks/DBT flow.

Reads the Glue Data Catalog (databases/tables/columns) and Glue jobs (definitions
+ their ETL scripts, fetched from the script's S3 location) so the lineage engine
can show the real ETL logic and derive source→target lineage from it.

Uses boto3 (already a project dependency). boto3 is imported lazily so a missing
install surfaces as a clean error rather than failing app startup.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class GlueConnectionConfig:
    region: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    session_token: str = ""
    profile_name: str = ""     # optional named profile (overrides explicit keys if set)
    catalog_id: str = ""       # optional AWS account id for the Glue catalog

    @classmethod
    def from_payload(cls, payload: dict) -> "GlueConnectionConfig":
        p = payload or {}

        def pick(*keys):
            for k in keys:
                v = p.get(k)
                if v is not None and str(v).strip():
                    return str(v).strip()
            return ""

        return cls(
            region=pick("region", "aws_region", "awsRegion"),
            access_key_id=pick("access_key_id", "accessKeyId", "aws_access_key_id"),
            secret_access_key=pick("secret_access_key", "secretAccessKey", "aws_secret_access_key"),
            session_token=pick("session_token", "sessionToken", "aws_session_token"),
            profile_name=pick("profile_name", "profile", "profileName"),
            catalog_id=pick("catalog_id", "catalogId"),
        )

    def masked(self) -> dict:
        data = asdict(self)
        data["secret_access_key"] = ""
        data["session_token"] = ""
        data["secret_present"] = bool(self.secret_access_key)
        return data

    def public_persisted(self) -> dict:
        data = self.masked()
        data["last_saved_at"] = datetime.now(timezone.utc).isoformat()
        return data


def validate_config(config: GlueConnectionConfig) -> list[str]:
    errors = []
    if not config.region:
        errors.append("AWS region is required (e.g. us-east-1).")
    if not config.profile_name and not (config.access_key_id and config.secret_access_key):
        errors.append("Provide either an AWS profile name or an access key id + secret access key.")
    return errors


def _build_session(config: GlueConnectionConfig):
    try:
        import boto3  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "AWS Glue support requires boto3. Install the project requirements and restart the server."
        ) from exc

    kwargs = {"region_name": config.region} if config.region else {}
    if config.profile_name:
        kwargs["profile_name"] = config.profile_name
    elif config.access_key_id and config.secret_access_key:
        kwargs["aws_access_key_id"] = config.access_key_id
        kwargs["aws_secret_access_key"] = config.secret_access_key
        if config.session_token:
            kwargs["aws_session_token"] = config.session_token
    return boto3.Session(**kwargs)


@contextmanager
def _aws_session(config: GlueConnectionConfig):
    """Yield a boto3 Session, isolating explicit-key auth from the environment.

    When the user supplies access keys (and no profile), an ambient
    ``AWS_PROFILE``/``AWS_DEFAULT_PROFILE`` (e.g. from .env) would otherwise make
    botocore try to load that profile at client-creation time and fail with
    ProfileNotFound — even though valid keys were given. We temporarily remove
    those vars for the lifetime of the session/clients, then restore them. boto3
    resolves the profile lazily (on .client()), so the scrub must wrap all client
    use, not just Session construction.
    """
    use_explicit_keys = bool(not config.profile_name and config.access_key_id and config.secret_access_key)
    saved = {}
    if use_explicit_keys:
        for var in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
            if var in os.environ:
                saved[var] = os.environ.pop(var)
    try:
        yield _build_session(config)
    finally:
        os.environ.update(saved)


def _catalog_kwargs(config: GlueConnectionConfig) -> dict:
    return {"CatalogId": config.catalog_id} if config.catalog_id else {}


def test_glue_connection(config: GlueConnectionConfig) -> dict:
    """Validate config and confirm Glue access. Returns {success, ...}."""
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(config) as session:
            sts = session.client("sts")
            glue = session.client("glue")
        identity = sts.get_caller_identity()
        # A tiny Glue call proves the credentials actually reach Glue (not just STS).
        glue.get_databases(MaxResults=1, **_catalog_kwargs(config))
        return {
            "success": True,
            "identity": {"account": identity.get("Account"), "arn": identity.get("Arn"), "region": config.region},
        }
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 — surface boto/Glue errors cleanly
        logger.warning("AWS Glue connection test failed: %s", exc)
        return {"success": False, "error": f"AWS Glue connection failed: {exc}"}


def list_glue_catalog(config: GlueConnectionConfig, databases: list[str] | None = None) -> dict:
    """List catalog tables (with columns + storage location) for the given databases.

    Returns {success, databases: [...], tables: [{database, name, full_name,
    columns:[{name,type}], location, classification}]}.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(config) as session:
            glue = session.client("glue")
        ck = _catalog_kwargs(config)

        db_names = list(databases or [])
        if not db_names:
            paginator = glue.get_paginator("get_databases")
            for page in paginator.paginate(**ck):
                db_names.extend(d["Name"] for d in page.get("DatabaseList", []))

        tables = []
        for db in db_names:
            paginator = glue.get_paginator("get_tables")
            for page in paginator.paginate(DatabaseName=db, **ck):
                for t in page.get("TableList", []):
                    sd = t.get("StorageDescriptor", {}) or {}
                    cols = [{"name": c.get("Name"), "type": c.get("Type")} for c in sd.get("Columns", []) or []]
                    params = t.get("Parameters", {}) or {}
                    tables.append({
                        "database": db,
                        "name": t.get("Name"),
                        "full_name": f"{db}.{t.get('Name')}",
                        "columns": cols,
                        "location": sd.get("Location") or "",
                        "classification": params.get("classification") or "",
                    })
        return {"success": True, "databases": db_names, "tables": tables}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("AWS Glue catalog listing failed: %s", exc)
        return {"success": False, "error": f"AWS Glue catalog listing failed: {exc}"}


def list_glue_jobs(config: GlueConnectionConfig) -> dict:
    """List Glue jobs with their command metadata (script location + job type)."""
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(config) as session:
            glue = session.client("glue")
        jobs = []
        paginator = glue.get_paginator("get_jobs")
        for page in paginator.paginate():
            for j in page.get("Jobs", []):
                cmd = j.get("Command", {}) or {}
                jobs.append({
                    "name": j.get("Name"),
                    "type": cmd.get("Name") or "",            # 'glueetl' | 'pythonshell' | 'gluestreaming'
                    "script_location": cmd.get("ScriptLocation") or "",
                    "python_version": cmd.get("PythonVersion") or "",
                    "role": j.get("Role") or "",
                    "glue_version": j.get("GlueVersion") or "",
                })
        return {"success": True, "jobs": jobs}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("AWS Glue job listing failed: %s", exc)
        return {"success": False, "error": f"AWS Glue job listing failed: {exc}"}


def fetch_glue_job_scripts(config: GlueConnectionConfig, jobs: list[dict]) -> dict:
    """Fetch each job's ETL script text from its S3 ScriptLocation.

    ``jobs`` is the list returned by list_glue_jobs (or any dicts carrying
    ``name`` + ``script_location``). Returns {success, scripts: {job_name: text},
    errors: {job_name: message}}.
    """
    try:
        with _aws_session(config) as session:
            s3 = session.client("s3")
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"AWS session failed: {exc}"}

    scripts, errors = {}, {}
    for job in jobs or []:
        name = job.get("name")
        loc = job.get("script_location") or ""
        if not name or not loc.startswith("s3://"):
            errors[name or "(unknown)"] = "No S3 script location on this job."
            continue
        parsed = urlparse(loc)
        bucket, key = parsed.netloc, parsed.path.lstrip("/")
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            scripts[name] = obj["Body"].read().decode("utf-8", errors="replace")
        except Exception as exc:  # noqa: BLE001
            errors[name] = f"Could not read {loc}: {exc}"
    return {"success": True, "scripts": scripts, "errors": errors}
