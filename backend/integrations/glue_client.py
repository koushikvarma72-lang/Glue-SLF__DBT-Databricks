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


# ─── orchestration surface: workflows / triggers / crawlers ─────────────────
#
# Phase 0 of the gap plan: read the pieces that sequence the pipeline (Glue
# Workflows + Triggers) and the pieces that infer schemas (Crawlers) so the
# Phase-1 orchestration converter has real input. Normalizers are pure
# functions (unit-testable without AWS); the list_* wrappers follow the same
# error-contract as list_glue_jobs.

def _normalize_trigger(t: dict) -> dict:
    """Flatten a Glue Trigger API dict to the shape the orchestration engine consumes."""
    t = t or {}
    actions = [{"job_name": a.get("JobName") or "", "crawler_name": a.get("CrawlerName") or ""}
               for a in t.get("Actions") or []]
    pred = t.get("Predicate") or {}
    conditions = [{
        "logical_operator": c.get("LogicalOperator") or "EQUALS",
        "job_name": c.get("JobName") or "",
        "state": c.get("State") or "",
        "crawler_name": c.get("CrawlerName") or "",
        "crawl_state": c.get("CrawlState") or "",
    } for c in pred.get("Conditions") or []]
    return {
        "name": t.get("Name"),
        "workflow_name": t.get("WorkflowName") or "",
        "type": t.get("Type") or "",                     # SCHEDULED | CONDITIONAL | ON_DEMAND | EVENT
        "state": t.get("State") or "",
        "schedule": t.get("Schedule") or "",             # cron(...) for SCHEDULED
        "actions": actions,
        "predicate_logical": pred.get("Logical") or ("AND" if conditions else ""),
        "conditions": conditions,
    }


def _normalize_crawler(c: dict) -> dict:
    """Flatten a Glue Crawler API dict (targets + schedule are what migration needs)."""
    c = c or {}
    raw_targets = c.get("Targets") or {}
    targets: list[dict] = []
    for s3t in raw_targets.get("S3Targets") or []:
        targets.append({"kind": "s3", "path": s3t.get("Path") or ""})
    for jt in raw_targets.get("JdbcTargets") or []:
        targets.append({"kind": "jdbc", "path": jt.get("Path") or "",
                        "connection": jt.get("ConnectionName") or ""})
    for ct in raw_targets.get("CatalogTargets") or []:
        targets.append({"kind": "catalog", "database": ct.get("DatabaseName") or "",
                        "tables": list(ct.get("Tables") or [])})
    for dt in raw_targets.get("DeltaTargets") or []:
        targets.append({"kind": "delta", "paths": list(dt.get("DeltaTables") or [])})
    sched = c.get("Schedule") or {}
    if isinstance(sched, str):
        sched = {"ScheduleExpression": sched}
    return {
        "name": c.get("Name"),
        "database": c.get("DatabaseName") or "",
        "table_prefix": c.get("TablePrefix") or "",
        "schedule": sched.get("ScheduleExpression") or "",
        "schedule_state": sched.get("State") or "",
        "targets": targets,
    }


def _normalize_workflow(w: dict) -> dict:
    """Flatten a Glue Workflow (with IncludeGraph) into {name, nodes, edges}.

    Node ``type`` is lowercased ('trigger'|'job'|'crawler'); edges reference node ids.
    """
    w = w or {}
    graph = w.get("Graph") or {}
    nodes = [{"id": n.get("UniqueId"), "type": (n.get("Type") or "").lower(),
              "name": n.get("Name")} for n in graph.get("Nodes") or []]
    edges = [{"source": e.get("SourceId"), "target": e.get("DestinationId")}
             for e in graph.get("Edges") or []]
    return {
        "name": w.get("Name"),
        "description": w.get("Description") or "",
        "default_run_properties": w.get("DefaultRunProperties") or {},
        "nodes": nodes,
        "edges": edges,
    }


def list_glue_workflows(config: GlueConnectionConfig) -> dict:
    """List Glue Workflows with their full trigger/job/crawler graphs.

    Returns {success, workflows: [{name, description, nodes, edges, ...}]}.
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(config) as session:
            glue = session.client("glue")
        names: list[str] = []
        token = None
        while True:  # list_workflows has no paginator in older botocore — manual NextToken loop
            kwargs = {"NextToken": token} if token else {}
            page = glue.list_workflows(**kwargs)
            names.extend(page.get("Workflows", []))
            token = page.get("NextToken")
            if not token:
                break
        workflows = []
        for i in range(0, len(names), 25):  # batch_get_workflows caps at 25 names
            batch = glue.batch_get_workflows(Names=names[i:i + 25], IncludeGraph=True)
            workflows.extend(_normalize_workflow(w) for w in batch.get("Workflows", []))
        return {"success": True, "workflows": workflows}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("AWS Glue workflow listing failed: %s", exc)
        return {"success": False, "error": f"AWS Glue workflow listing failed: {exc}"}


def list_glue_triggers(config: GlueConnectionConfig) -> dict:
    """List all Glue Triggers (schedules + conditional predicates).

    Standalone triggers matter too — not every client wires triggers into a Workflow.
    Returns {success, triggers: [...]} (see _normalize_trigger for the shape).
    """
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(config) as session:
            glue = session.client("glue")
        triggers = []
        paginator = glue.get_paginator("get_triggers")
        for page in paginator.paginate():
            triggers.extend(_normalize_trigger(t) for t in page.get("Triggers", []))
        return {"success": True, "triggers": triggers}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("AWS Glue trigger listing failed: %s", exc)
        return {"success": False, "error": f"AWS Glue trigger listing failed: {exc}"}


def list_glue_crawlers(config: GlueConnectionConfig) -> dict:
    """List Glue Crawlers (name, target paths, schedule, output database/prefix)."""
    errors = validate_config(config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(config) as session:
            glue = session.client("glue")
        crawlers = []
        paginator = glue.get_paginator("get_crawlers")
        for page in paginator.paginate():
            crawlers.extend(_normalize_crawler(c) for c in page.get("Crawlers", []))
        return {"success": True, "crawlers": crawlers}
    except RuntimeError as exc:
        return {"success": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001
        logger.warning("AWS Glue crawler listing failed: %s", exc)
        return {"success": False, "error": f"AWS Glue crawler listing failed: {exc}"}


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
