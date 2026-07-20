"""Local dbt-Core runner for the sfglue flow.

The DBT Agent page's "Run with dbt" button was wired to ``/api/dbt-local/*``
routes that lived in the combined BI Migration Tool and were never split out —
this module supplies them for the standalone app.

Design: one background thread per run. The converted models + sources.yml are
written to a temp dir as a full dbt project (reusing ``build_dbt_project_files``
so the on-disk layout matches the export exactly), credentials are passed via
environment variables (profiles.yml only holds env_var references — no secrets
on disk), and ``dbt debug`` → ``dbt deps`` (if needed) → ``dbt build`` runs as a
subprocess with stdout streamed into an in-memory log the status endpoint pages
through. Per-model results come from ``target/run_results.json``.

Frontend contract (see frontend/src/api.js):
  POST /api/dbt-local/run-sfglue {sessionId, models, sources_yml, destination} → {jobId}
  GET  /api/dbt-local/status/<jobId>?since=N →
       {status, summary, models[], logs[], logOffset, finished, error?}
  POST /api/dbt-local/cancel/<jobId> → {success}
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import threading
import time
import uuid

logger = logging.getLogger(__name__)

_JOBS: dict = {}
_JOBS_LOCK = threading.Lock()
_MAX_LOG_LINES = 5000
_MAX_JOBS = 20
_JOB_TTL_SECS = 30 * 60  # drop finished jobs whose end-time is older than this


def _prune_jobs() -> None:
    """Evict old finished jobs. Caller MUST hold _JOBS_LOCK."""
    now = time.time()
    stale = [jid for jid, j in _JOBS.items()
             if j.get("finished") and (now - j.get("ended_at", now)) > _JOB_TTL_SECS]
    for jid in stale:
        _JOBS.pop(jid, None)
    if len(_JOBS) > _MAX_JOBS:
        finished = sorted(
            (jid for jid, j in _JOBS.items() if j.get("finished")),
            key=lambda jid: _JOBS[jid].get("ended_at", 0))
        for jid in finished:
            if len(_JOBS) <= _MAX_JOBS:
                break
            _JOBS.pop(jid, None)


def _job(job_id: str):
    with _JOBS_LOCK:
        return _JOBS.get(job_id)


def _append_log(job: dict, line: str) -> None:
    with job["lock"]:
        job["logs"].append(line.rstrip("\n"))
        if len(job["logs"]) > _MAX_LOG_LINES:  # keep memory bounded on chatty runs
            del job["logs"][: len(job["logs"]) - _MAX_LOG_LINES]


def _find_dbt() -> str | None:
    """The dbt executable: SFGLUE_DBT_BIN wins, then PATH.

    The override matters because dbt-core does not (yet) run on Python 3.14 —
    a backend on 3.14 can point at a dbt installed in a separate 3.12/3.13 venv.
    """
    override = os.environ.get("SFGLUE_DBT_BIN", "").strip()
    if override:
        path = os.path.expanduser(override)
        if os.path.isfile(path) and os.access(path, os.X_OK):
            return path
        logger.warning("SFGLUE_DBT_BIN=%r is not an executable file — falling back to PATH", override)
    return shutil.which("dbt")


def _dest_env(destination: dict) -> dict | None:
    """Env vars the generated profiles.yml expects. None if incomplete."""
    d = destination or {}
    host = (d.get("workspace_url") or d.get("workspaceUrl") or "").replace("https://", "").rstrip("/")
    token = d.get("token") or d.get("personal_access_token") or d.get("access_token") or ""
    wh = d.get("sql_warehouse_id") or d.get("sqlWarehouseId") or ""
    if not (host and token and wh):
        return None
    return {
        "DATABRICKS_HOST": host,
        "DATABRICKS_HTTP_PATH": f"/sql/1.0/warehouses/{wh}",
        "DATABRICKS_TOKEN": token,
    }


def _parse_run_results(project_dir: str) -> list[dict]:
    """target/run_results.json → [{name, status, execution_time}] (best-effort)."""
    path = os.path.join(project_dir, "target", "run_results.json")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        out = []
        for r in data.get("results") or []:
            uid = str(r.get("unique_id") or "")
            if not uid.startswith("model."):
                continue
            out.append({
                "name": uid.split(".")[-1],
                "status": r.get("status") or "unknown",
                "execution_time": r.get("execution_time"),
            })
        return out
    except Exception:  # noqa: BLE001 — absent/partial file on early failure
        return []


def _run_and_cleanup(job: dict, project_dir: str, env: dict) -> None:
    """Run the dbt steps, then always remove the temp project dir (success/error/cancel)."""
    try:
        _run_steps(job, project_dir, env)
    finally:
        job["ended_at"] = time.time()
        shutil.rmtree(project_dir, ignore_errors=True)


def _run_steps(job: dict, project_dir: str, env: dict) -> None:
    """Worker thread: dbt debug → (deps) → build, streaming output."""
    dbt = _find_dbt()
    if not dbt:
        job.update(status="error", finished=True,
                   error=("dbt executable not found in this server's environment. Install it "
                          "next to the backend:  pip install dbt-databricks"))
        _append_log(job, "ERROR: dbt not found — pip install dbt-databricks")
        return

    steps = [("debug", [dbt, "debug", "--no-use-colors"])]
    if os.path.exists(os.path.join(project_dir, "packages.yml")):
        steps.append(("deps", [dbt, "deps", "--no-use-colors"]))
    steps.append(("build", [dbt, "build", "--no-use-colors"]))

    full_env = {**os.environ, **env,
                "DBT_PROFILES_DIR": project_dir, "DBT_SEND_ANONYMOUS_USAGE_STATS": "0"}
    for step_name, cmd in steps:
        if job.get("cancelled"):
            job.update(status="cancelled", finished=True, summary="Cancelled before finishing.")
            return
        _append_log(job, f"── dbt {step_name} ──")
        try:
            proc = subprocess.Popen(cmd, cwd=project_dir, env=full_env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, bufsize=1)
        except Exception as exc:  # noqa: BLE001
            job.update(status="error", finished=True, error=f"Could not start dbt: {exc}")
            return
        with job["lock"]:
            job["proc"] = proc
        for line in proc.stdout or []:
            _append_log(job, line)
        proc.wait()
        with job["lock"]:
            job["proc"] = None
        if job.get("cancelled"):
            job.update(status="cancelled", finished=True, summary="Cancelled before finishing.")
            return
        if proc.returncode != 0:
            job["models"] = _parse_run_results(project_dir)
            job.update(status="error", finished=True,
                       summary=f"dbt {step_name} failed (exit {proc.returncode}) — see the log.")
            return

    job["models"] = _parse_run_results(project_dir)
    failed = [m for m in job["models"] if str(m.get("status", "")).lower() not in ("success", "pass")]
    if failed:
        job.update(status="error", finished=True,
                   summary=f"dbt build finished with {len(failed)} failed node(s).")
    else:
        n = len(job["models"])
        job.update(status="success", finished=True,
                   summary=f"dbt build completed — {n} model(s) built successfully.")


def start_dbt_run(models: dict, sources_yml: str, destination: dict,
                  session_id: str = "sfglue") -> dict:
    """Start a run; returns {success, jobId} or {success: False, error}."""
    from backend.integrations.snowflake_glue_migration import build_dbt_project_files

    if not models:
        return {"success": False, "error": "No dbt models to run — generate the conversion first."}
    env = _dest_env(destination)
    if env is None:
        return {"success": False, "error": ("Set the Databricks workspace URL, access token and "
                                            "SQL Warehouse ID on the Databricks Agent step first.")}

    project_dir = tempfile.mkdtemp(prefix=f"sfglue_dbt_{session_id}_")
    files = build_dbt_project_files(
        {"dbt_models": models, "sources_yml": sources_yml or ""}, destination, "sfglue_local_run")
    for rel, content in files.items():
        path = os.path.join(project_dir, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content if isinstance(content, str) else str(content))

    job_id = uuid.uuid4().hex[:12]
    job = {"id": job_id, "status": "running", "summary": "", "error": "",
           "logs": [], "models": [], "finished": False, "cancelled": False,
           "proc": None, "dir": project_dir, "lock": threading.Lock()}
    with _JOBS_LOCK:
        _prune_jobs()
        _JOBS[job_id] = job
    threading.Thread(target=_run_and_cleanup, args=(job, project_dir, env),
                     name=f"dbt-local-{job_id}", daemon=True).start()
    logger.info("dbt-local: started run %s (%d model(s)) in %s", job_id, len(models), project_dir)
    return {"success": True, "jobId": job_id}


def get_status(job_id: str, since: int = 0) -> dict:
    job = _job(job_id)
    if not job:
        return {"success": False, "error": f"Unknown dbt run {job_id!r}."}
    with job["lock"]:
        logs = list(job["logs"])
    since = max(0, int(since or 0))
    return {
        "success": True,
        "status": job["status"],
        "summary": job["summary"],
        "error": job.get("error") or "",
        "models": job["models"],
        "logs": logs[since:],
        "logOffset": len(logs),
        "finished": job["finished"],
    }


def cancel(job_id: str) -> dict:
    job = _job(job_id)
    if not job:
        return {"success": False, "error": f"Unknown dbt run {job_id!r}."}
    job["cancelled"] = True
    with job["lock"]:
        proc = job.get("proc")
    if proc is not None:
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
    shutil.rmtree(job.get("dir") or "", ignore_errors=True)
    return {"success": True}
