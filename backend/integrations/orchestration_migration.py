"""Orchestration migration (Phase 1 of the gap plan) — Glue Workflows → Databricks Jobs.

Pure, deterministic engine (NO AI — a wrong schedule or dependency is a silent
outage, so this must be provable). Input is the normalized output of
``glue_client.list_glue_workflows`` / ``list_glue_triggers``; output is a
Databricks Jobs 2.1 payload + a DAB ``resources/jobs`` YAML for the bundle.

Glue's model: TRIGGER nodes activate JOB/CRAWLER nodes. Edges run
``condition-source → trigger`` and ``trigger → action``. So job B depends on
job A iff an edge A→T exists and T→B exists for some CONDITIONAL trigger T.
Non-representable predicates (ANY-of logic, on-FAILURE conditions, EVENT
triggers) are surfaced as ``warnings`` for the review queue — never dropped.

Task targets come from an ``artifact_map`` (job name → converted artifact):
  {"kind": "notebook",  "path": "<file>"}     → notebook_task
  {"kind": "dbt",       "models": [...]}      → dbt CLI task placeholder (run via dbt_build)
  {"kind": "framework", "notebook": "<fw_*>"} → control-plane runtime notebook
Unmapped jobs get a clearly-marked placeholder notebook task (review item).
"""

from __future__ import annotations

import json
import re

# ─── Glue cron → Quartz (Databricks) ─────────────────────────────────────────
# Glue: cron(Minutes Hours Day-of-month Month Day-of-week Year)  — 6 fields, no seconds
# Databricks (Quartz): Seconds Minutes Hours Day-of-month Month Day-of-week [Year]


def glue_cron_to_quartz(expr: str) -> str | None:
    """Translate a Glue schedule expression to Databricks Quartz. None if not cron."""
    text = str(expr or "").strip()
    m = re.match(r"cron\s*\((.+)\)$", text, re.I)
    if m:
        inner = m.group(1)
    elif re.fullmatch(r"[\d*/,?LW#a-zA-Z-]+(\s+[\d*/,?LW#a-zA-Z-]+){4,6}", text):
        inner = text  # bare cron fields (no cron() wrapper)
    else:
        return None
    def _quartz_dom_dow(dom: str, dow: str) -> tuple[str, str]:
        # Quartz requires exactly one of day-of-month / day-of-week to be '?'.
        # Unix cron '*'/'*' means "any day" → Quartz 'dom=*, dow=?'.
        if dom != "?" and dow != "?":
            if dow in ("*",):
                dow = "?"
            elif dom in ("*",):
                dom = "?"
        return dom, dow

    fields = inner.split()
    if len(fields) == 6:
        minutes, hours, dom, month, dow, year = fields
        dom, dow = _quartz_dom_dow(dom, dow)
        out = ["0", minutes, hours, dom, month, dow]
        if year and year != "*":
            out.append(year)
        return " ".join(out)
    if len(fields) == 5:  # plain unix-style cron — pad seconds, no year
        minutes, hours, dom, month, dow = fields
        dom, dow = _quartz_dom_dow(dom, dow)
        return " ".join(["0", minutes, hours, dom, month, dow])
    return None


# ─── workflow graph → normalized task DAG ────────────────────────────────────

def parse_workflow_dag(workflow: dict, triggers: list[dict] | None = None) -> dict:
    """Normalize one Glue workflow into {name, tasks, schedule, warnings}.

    tasks: [{key, kind ('job'|'crawler'), legacy_name, depends_on: [keys]}]
    schedule: quartz cron of the workflow's SCHEDULED trigger (if any)
    """
    wf = workflow or {}
    warnings: list[str] = []
    nodes = {n["id"]: n for n in wf.get("nodes") or [] if n.get("id")}
    by_name = {}
    for t in triggers or []:
        if t.get("name"):
            by_name[t["name"]] = t

    # id → normalized task key (job/crawler nodes only)
    def _key(node):
        return re.sub(r"[^a-zA-Z0-9_]+", "_", str(node.get("name") or node["id"])).strip("_")[:100]

    action_nodes = {nid: n for nid, n in nodes.items() if n.get("type") in ("job", "crawler")}
    trigger_nodes = {nid: n for nid, n in nodes.items() if n.get("type") == "trigger"}

    incoming: dict = {}
    outgoing: dict = {}
    for e in wf.get("edges") or []:
        outgoing.setdefault(e.get("source"), []).append(e.get("target"))
        incoming.setdefault(e.get("target"), []).append(e.get("source"))

    schedule = None
    depends: dict = {nid: set() for nid in action_nodes}
    for tid, tnode in trigger_nodes.items():
        tmeta = by_name.get(tnode.get("name")) or {}
        ttype = (tmeta.get("type") or "").upper()
        preds = [nid for nid in incoming.get(tid, []) if nid in action_nodes]
        succs = [nid for nid in outgoing.get(tid, []) if nid in action_nodes]
        if ttype == "SCHEDULED" and tmeta.get("schedule"):
            q = glue_cron_to_quartz(tmeta["schedule"])
            if q and schedule is None:
                schedule = q
            elif not q:
                warnings.append(f"trigger {tnode.get('name')}: unparseable schedule "
                                f"{tmeta.get('schedule')!r} — set the Job schedule manually")
        if ttype == "EVENT":
            warnings.append(f"trigger {tnode.get('name')}: EVENT trigger — map to a "
                            "file-arrival trigger on the ingestion job (see Phase 4 notes)")
        if (tmeta.get("predicate_logical") or "").upper() == "ANY" and len(preds) > 1:
            warnings.append(f"trigger {tnode.get('name')}: ANY-of predicate — Databricks "
                            "depends_on is ALL-of; review the dependency fan-in")
        for c in tmeta.get("conditions") or []:
            state = (c.get("state") or c.get("crawl_state") or "").upper()
            if state and state != "SUCCEEDED":
                warnings.append(f"trigger {tnode.get('name')}: condition on state {state} — "
                                "Databricks has no on-failure dependency; model it as an "
                                "on_failure notification or a final cleanup task")
        for s in succs:
            depends[s].update(preds)

    tasks = []
    for nid, node in action_nodes.items():
        tasks.append({
            "key": _key(node),
            "kind": node.get("type"),
            "legacy_name": node.get("name"),
            "depends_on": sorted(_key(action_nodes[p]) for p in depends.get(nid, ())
                                 if p in action_nodes),
        })
    tasks.sort(key=lambda t: (len(t["depends_on"]), t["key"]))
    return {"name": wf.get("name") or "workflow", "tasks": tasks,
            "schedule": schedule, "warnings": warnings}


# ─── DAG + artifacts → Databricks Job ────────────────────────────────────────

def _placeholder_note(job_name: str) -> str:
    return (f"# TODO[EXTERNAL]: no converted artifact found for Glue job/crawler "
            f"'{job_name}'.\n# Run the conversion for it (or map it manually), then "
            "re-plan the workflow.\nprint('placeholder task — see TODO above')\n")


def build_databricks_job(dag: dict, *, artifact_map: dict | None = None,
                         destination: dict | None = None,
                         email_notifications: dict | None = None,
                         file_arrival_url: str | None = None) -> dict:
    """One parsed DAG → {job (Jobs 2.1 JSON), placeholders {fname: content}, warnings}.

    ``file_arrival_url`` (Phase 4): a storage location that should trigger the
    job on file arrival (from an EVENT trigger / S3-event legacy pattern) —
    emitted as the Jobs ``trigger.file_arrival`` block instead of a schedule.
    """
    amap = artifact_map or {}
    d = destination or {}
    warnings = list(dag.get("warnings") or [])
    placeholders: dict = {}

    # Some source tasks are NOT runnable/needed as Databricks tasks:
    #   * sensors + cross-DAG triggers → become a Job file-arrival trigger, not a task;
    #   * unmapped "glue-around" ops (python/bash notifications, empty markers) → there
    #     is nothing converted to run, and a phantom placeholder notebook would just
    #     fail the whole Job at runtime.
    # Drop them and rewire dependents to the dropped node's own upstreams so the DAG
    # stays connected. Data tasks with no mapping (glue_job/databricks) still get a
    # visible placeholder — those you WANT surfaced for review, not silently dropped.
    all_tasks = dag.get("tasks") or []

    def _is_dropped(t):
        if t.get("kind") in ("sensor", "trigger_dag"):
            return True
        mapped = bool((artifact_map or {}).get(t["legacy_name"]) or (artifact_map or {}).get(t["key"]))
        return (not mapped) and t.get("kind") in ("python", "bash", "empty")

    dropped = {t["key"] for t in all_tasks if _is_dropped(t)}
    upstream_of = {t["key"]: list(t.get("depends_on") or []) for t in all_tasks}

    def _resolve(deps):
        out = []
        for k in deps:
            if k in dropped:
                out.extend(_resolve(upstream_of.get(k, [])))  # skip past the dropped node
            else:
                out.append(k)
        # de-dup preserving order
        seen, uniq = set(), []
        for k in out:
            if k not in seen:
                seen.add(k); uniq.append(k)
        return uniq

    tasks = []
    for t in all_tasks:
        if t["key"] in dropped:
            warnings.append(f"task {t['key']}: {t.get('operator') or t.get('kind')} dropped "
                            "— model as a Job file-arrival trigger, not a task")
            continue
        art = amap.get(t["legacy_name"]) or amap.get(t["key"]) or {}
        task: dict = {"task_key": t["key"]}
        deps = _resolve(t["depends_on"]) if t.get("depends_on") else []
        if deps:
            task["depends_on"] = [{"task_key": k} for k in deps]
        kind = art.get("kind")
        if kind == "dbt":
            # dbt models are built via the compiled-SQL Build step (dbt_build) — emit a
            # SQL-file task placeholder the bundle wires to the exported dbt project.
            task["dbt_task"] = {
                "commands": ["dbt deps", "dbt build --select " + " ".join(
                    m.replace(".sql", "") for m in art.get("models") or ["*"])],
                "project_directory": "dbt",
            }
        elif kind in ("notebook", "framework"):
            path = art.get("path") or art.get("notebook") or ""
            task["notebook_task"] = {
                "notebook_path": f"src/notebooks/{path}",
                "base_parameters": {"catalog": d.get("catalog") or "main"},
            }
        else:
            fname = f"placeholder__{t['key']}.py"
            placeholders[fname] = _placeholder_note(t["legacy_name"])
            task["notebook_task"] = {"notebook_path": f"src/notebooks/{fname}"}
            warnings.append(f"task {t['key']}: no converted artifact — placeholder emitted")
        tasks.append(task)

    job: dict = {
        "name": f"sfglue — {dag.get('name')}",
        "tags": {"sfglue_source": dag.get("name") or "workflow", "generator": "sfglue"},
        "max_concurrent_runs": 1,
        "tasks": tasks,
    }
    if file_arrival_url:
        job["trigger"] = {"file_arrival": {"url": file_arrival_url},
                          "pause_status": "UNPAUSED"}
    elif dag.get("schedule"):
        job["schedule"] = {"quartz_cron_expression": dag["schedule"],
                           "timezone_id": "UTC", "pause_status": "PAUSED"}
    if email_notifications:
        job["email_notifications"] = email_notifications
    return {"job": job, "placeholders": placeholders, "warnings": warnings}


# ─── serializers ─────────────────────────────────────────────────────────────

def job_to_dab_yaml(job: dict, resource_key: str) -> str:
    """Jobs-JSON → DAB resources/jobs YAML (string templates, no yaml dep)."""

    def _emit(obj, indent):
        pad = "  " * indent
        lines = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)) and v:
                    lines.append(f"{pad}{k}:")
                    lines.extend(_emit(v, indent + 1))
                elif isinstance(v, (dict, list)):
                    continue
                else:
                    lines.append(f"{pad}{k}: {json.dumps(v)}")
        elif isinstance(obj, list):
            for item in obj:
                sub = _emit(item, indent + 1)
                if sub:
                    first = sub[0].lstrip()
                    lines.append(f"{pad}- {first}")
                    lines.extend(sub[1:])
        return lines

    key = re.sub(r"[^a-zA-Z0-9_]+", "_", resource_key).strip("_").lower()
    body = _emit(job, 3)
    return "\n".join([
        "# Generated by sfglue from the legacy Glue workflow — Phase 1 orchestration.",
        "resources:",
        "  jobs:",
        f"    {key}:",
    ] + body) + "\n"


# ─── deploy (Jobs API 2.1, idempotent by tag) ────────────────────────────────

def deploy_job(job: dict, *, workspace_url: str, token: str, timeout: int = 60) -> dict:
    """Create-or-update the job in the workspace, matched by tags.sfglue_source.

    Returns {success, job_id, action ('created'|'updated')} or {success: False, error}.
    """
    import requests

    base = str(workspace_url or "").rstrip("/")
    if not base or not token:
        return {"success": False, "error": "workspace_url and access token are required"}
    hdrs = {"Authorization": f"Bearer {token}"}
    tag = (job.get("tags") or {}).get("sfglue_source")
    try:
        existing_id = None
        if tag:
            r = requests.get(f"{base}/api/2.1/jobs/list",
                             params={"name": job.get("name")}, headers=hdrs, timeout=timeout)
            r.raise_for_status()
            for j in (r.json().get("jobs") or []):
                settings = j.get("settings") or {}
                if (settings.get("tags") or {}).get("sfglue_source") == tag:
                    existing_id = j.get("job_id")
                    break
        if existing_id:
            r = requests.post(f"{base}/api/2.1/jobs/reset",
                              json={"job_id": existing_id, "new_settings": job},
                              headers=hdrs, timeout=timeout)
            r.raise_for_status()
            return {"success": True, "job_id": existing_id, "action": "updated"}
        r = requests.post(f"{base}/api/2.1/jobs/create", json=job, headers=hdrs, timeout=timeout)
        r.raise_for_status()
        return {"success": True, "job_id": r.json().get("job_id"), "action": "created"}
    except Exception as exc:  # noqa: BLE001 — surface API errors cleanly
        detail = ""
        resp = getattr(exc, "response", None)
        if resp is not None:
            detail = f" — {getattr(resp, 'text', '')[:300]}"
        return {"success": False, "error": f"{exc}{detail}"}
