"""Push converted artifacts into the Databricks workspace (Workspace API 2.0).

Automates the manual `databricks workspace import-dir` step so the deployed
orchestration Job can actually run: notebooks land as workspace NOTEBOOKS
(format SOURCE) and dbt project files as workspace FILES (format AUTO), under
the same root the deploy route wires into the job (default ``/Shared/sfglue``,
layout matching the asset-bundle export: ``src/notebooks/`` + ``dbt/``).

Idempotent: everything is imported with ``overwrite=true``.
"""

from __future__ import annotations

import base64
import logging
import posixpath

logger = logging.getLogger(__name__)

_NOTEBOOK_EXTS = {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R"}


def _is_notebook(rel_path: str) -> bool:
    # Only files under a notebooks/ directory become NOTEBOOK objects; dbt .sql/.py
    # files must stay plain workspace FILES or dbt can't read them.
    parts = rel_path.replace("\\", "/").split("/")
    return "notebooks" in parts[:-1]


def build_push_plan(artifacts: dict, dbt_files: dict | None, root: str,
                    conf_files: dict | None = None) -> list[dict]:
    """Pure: the list of imports to perform → [{path, kind, language?, content}]."""
    root = (root or "/Shared/sfglue").rstrip("/")
    plan: list[dict] = []
    for name, code in (artifacts.get("notebooks") or {}).items():
        ext = posixpath.splitext(name)[1].lower()
        plan.append({
            "path": f"{root}/src/notebooks/{name}",
            "kind": "notebook",
            "language": _NOTEBOOK_EXTS.get(ext, "PYTHON"),
            "content": code if isinstance(code, str) else str(code),
        })
    for rel, content in (dbt_files or {}).items():
        plan.append({
            "path": f"{root}/dbt/{rel}",
            "kind": "file",
            "content": content if isinstance(content, str) else str(content),
        })
    # Source YAML/config files, carried into ONE workspace folder so the
    # config-driven pattern survives the migration (read from
    # /Workspace<root>/conf/<name> by the ingestion notebooks).
    for name, content in (conf_files or {}).items():
        plan.append({
            "path": f"{root}/conf/{posixpath.basename(str(name))}",
            "kind": "file",
            "content": content if isinstance(content, str) else str(content),
        })
    return plan


def push_to_workspace(plan: list[dict], *, workspace_url: str, token: str,
                      timeout: int = 60) -> dict:
    """Execute the push plan against the Workspace API. Returns per-item results."""
    import requests

    base = str(workspace_url or "").rstrip("/")
    if not base or not token:
        return {"success": False, "error": "workspace_url and access token are required"}
    hdrs = {"Authorization": f"Bearer {token}"}

    made_dirs: set = set()
    results, ok = [], True
    for item in plan or []:
        path = item["path"]
        parent = posixpath.dirname(path)
        try:
            if parent not in made_dirs:
                r = requests.post(f"{base}/api/2.0/workspace/mkdirs",
                                  json={"path": parent}, headers=hdrs, timeout=timeout)
                r.raise_for_status()
                made_dirs.add(parent)
            payload = {
                "path": path,
                "overwrite": True,
                "content": base64.b64encode(item["content"].encode("utf-8")).decode(),
            }
            if item["kind"] == "notebook":
                payload.update(format="SOURCE", language=item.get("language", "PYTHON"))
            else:
                payload.update(format="AUTO")
            r = requests.post(f"{base}/api/2.0/workspace/import",
                              json=payload, headers=hdrs, timeout=timeout)
            if r.status_code == 400 and "RESOURCE_ALREADY_EXISTS" in (r.text or ""):
                # overwrite=true can't replace a node of a DIFFERENT type (e.g. a
                # notebook left by an earlier push where a file now goes). Delete
                # the conflicting node and import again — still idempotent.
                requests.post(f"{base}/api/2.0/workspace/delete",
                              json={"path": path, "recursive": False},
                              headers=hdrs, timeout=timeout)
                r = requests.post(f"{base}/api/2.0/workspace/import",
                                  json=payload, headers=hdrs, timeout=timeout)
            r.raise_for_status()
            results.append({"path": path, "kind": item["kind"], "status": "ok"})
        except Exception as exc:  # noqa: BLE001 — per-file, report and continue
            ok = False
            detail = getattr(getattr(exc, "response", None), "text", "")[:200]
            results.append({"path": path, "kind": item["kind"], "status": "failed",
                            "error": f"{exc} {detail}".strip()})
    return {"success": ok, "pushed": sum(1 for r in results if r["status"] == "ok"),
            "results": results}


# ─── run-now + poll: the workflow dry-run gate ───────────────────────────────

def run_job_and_wait(job_id: int, *, workspace_url: str, token: str,
                     poll_seconds: int = 10, timeout_seconds: int = 1800) -> dict:
    """Trigger the job and poll until it finishes (bounded). Returns the verdict:
    {success, run_id, state, tasks: [{task_key, state, error?}], run_page_url}.
    """
    import time

    import requests

    base = str(workspace_url or "").rstrip("/")
    if not base or not token:
        return {"success": False, "error": "workspace_url and access token are required"}
    hdrs = {"Authorization": f"Bearer {token}"}
    try:
        r = requests.post(f"{base}/api/2.1/jobs/run-now",
                          json={"job_id": int(job_id)}, headers=hdrs, timeout=60)
        r.raise_for_status()
        run_id = r.json().get("run_id")
    except Exception as exc:  # noqa: BLE001
        detail = getattr(getattr(exc, "response", None), "text", "")[:300]
        return {"success": False, "error": f"run-now failed: {exc} {detail}".strip()}

    deadline = time.monotonic() + timeout_seconds
    state, tasks, page = "PENDING", [], ""
    while time.monotonic() < deadline:
        try:
            r = requests.get(f"{base}/api/2.1/jobs/runs/get",
                             params={"run_id": run_id}, headers=hdrs, timeout=60)
            r.raise_for_status()
            run = r.json()
        except Exception as exc:  # noqa: BLE001
            return {"success": False, "run_id": run_id, "error": f"poll failed: {exc}"}
        page = run.get("run_page_url") or page
        st = run.get("state") or {}
        state = st.get("life_cycle_state") or "PENDING"
        tasks = [{
            "task_key": t.get("task_key"),
            "state": ((t.get("state") or {}).get("result_state")
                      or (t.get("state") or {}).get("life_cycle_state") or ""),
            "error": (t.get("state") or {}).get("state_message") or "",
        } for t in run.get("tasks") or []]
        if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            result = st.get("result_state") or state
            return {"success": result == "SUCCESS", "run_id": run_id, "state": result,
                    "tasks": tasks, "run_page_url": page,
                    "message": st.get("state_message") or ""}
        time.sleep(poll_seconds)
    return {"success": False, "run_id": run_id, "state": "TIMEOUT", "tasks": tasks,
            "run_page_url": page,
            "error": f"run still going after {timeout_seconds}s — watch it at {page}"}
