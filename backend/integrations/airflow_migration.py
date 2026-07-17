"""Airflow source-orchestrator support — DAGs → the Phase-1 normalized DAG model.

Airflow entered the reference picture as the orchestrator sitting above the Glue
jobs. This module turns Airflow DAGs into the SAME normalized shape
``orchestration_migration.parse_workflow_dag`` produces, so the entire existing
pipeline (``build_databricks_job`` → DAB YAML → deploy → run gate) works
unchanged. Two introspection paths, because the deployment may be unknown:

  * ``parse_dag_source(name, source)`` — static AST parse of a DAG .py file
    (copy it from the Airflow UI's Code tab; no server access needed).
  * ``fetch_airflow_dags(base_url, ...)`` — Airflow 2.x REST API
    (``/api/v1/dags`` + ``/dags/{id}/tasks``, which returns downstream ids).

Operator mapping notes:
  * GlueJobOperator's ``job_name`` becomes ``legacy_name`` — so a DAG that
    orchestrates the same Glue jobs converts with the SAME artifact map the
    Glue-workflow path uses.
  * Sensors (S3KeySensor etc.) become warnings suggesting a file-arrival
    trigger; TriggerDagRunOperator becomes a cross-job warning.

Deterministic, pure (the REST fetch is the only I/O), unit-tested.
"""

from __future__ import annotations

import ast
import json
import logging
import re

logger = logging.getLogger(__name__)

_PRESETS = {
    "@once": None,
    "@hourly": "0 0 * * * ?",
    "@daily": "0 0 0 * * ?",
    "@midnight": "0 0 0 * * ?",
    "@weekly": "0 0 0 ? * 1",
    "@monthly": "0 0 0 1 * ?",
    "@yearly": "0 0 0 1 1 ?",
    "@annually": "0 0 0 1 1 ?",
}

_SENSOR_RE = re.compile(r"sensor", re.I)


def airflow_schedule_to_quartz(schedule) -> str | None:
    """Airflow schedule_interval → Databricks Quartz cron (None if not mappable)."""
    from backend.integrations.orchestration_migration import glue_cron_to_quartz
    if schedule is None:
        return None
    s = str(schedule).strip()
    if s in _PRESETS:
        return _PRESETS[s]
    return glue_cron_to_quartz(s)  # handles bare 5-field cron


# ─── static AST parsing ──────────────────────────────────────────────────────

def _const(node):
    return node.value if isinstance(node, ast.Constant) else None


def _kw(call: ast.Call, name: str):
    for kw in call.keywords:
        if kw.arg == name:
            return _const(kw.value) if isinstance(kw.value, ast.Constant) else kw.value
    return None


def _call_name(call: ast.Call) -> str:
    f = call.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def parse_dag_source(name: str, source: str) -> dict:
    """Parse one DAG file into {name, tasks, schedule, warnings}. Pure.

    Handles: operator assignments (``t = XOperator(task_id=...)``), ``>>``/``<<``
    chains (including lists), ``set_downstream``/``set_upstream``, DAG(...) or
    ``with DAG(...)`` schedule, and Glue job name extraction.
    """
    warnings: list[str] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return {"name": name, "tasks": [], "schedule": None,
                "warnings": [f"DAG file does not parse: {exc}"]}

    dag_id, schedule = name, None
    tasks: dict[str, dict] = {}          # var name -> task
    by_task_id: dict[str, str] = {}      # task_id -> var name
    deps: list[tuple[str, str]] = []     # (upstream var, downstream var)

    def _op_from_call(var: str, call: ast.Call):
        cls = _call_name(call)
        if not (cls.endswith("Operator") or _SENSOR_RE.search(cls) or cls in ("EmptyOperator", "DummyOperator")):
            return
        task_id = _kw(call, "task_id")
        if not isinstance(task_id, str):
            task_id = var
        kind = "other"
        legacy = task_id
        if "Glue" in cls and "Job" in cls:
            kind = "glue_job"
            jn = _kw(call, "job_name")
            if isinstance(jn, str):
                legacy = jn
        elif "Databricks" in cls:
            kind = "databricks"
        elif cls in ("PythonOperator", "PythonVirtualenvOperator", "BranchPythonOperator"):
            kind = "python"
            pc = _kw(call, "python_callable")
            if isinstance(pc, ast.Name):
                legacy = pc.id
        elif cls == "BashOperator":
            kind = "bash"
        elif _SENSOR_RE.search(cls):
            kind = "sensor"
            warnings.append(f"task {task_id}: {cls} — Databricks has no in-job sensors; "
                            "map it to a file-arrival trigger on the Job (Phase 4) and drop the task")
        elif cls == "TriggerDagRunOperator":
            kind = "trigger_dag"
            warnings.append(f"task {task_id}: TriggerDagRunOperator — cross-DAG trigger; "
                            "model as a run_job_task pointing at the other migrated Job")
        elif cls in ("EmptyOperator", "DummyOperator"):
            kind = "empty"
        tasks[var] = {"var": var, "task_id": task_id, "kind": kind,
                      "legacy_name": legacy, "operator": cls}
        by_task_id[task_id] = var

    def _operands(node) -> list[str]:
        """Names referenced by one side of a >> expression (Name or list of Names)."""
        if isinstance(node, ast.Name):
            return [node.id]
        if isinstance(node, (ast.List, ast.Tuple)):
            return [e.id for e in node.elts if isinstance(e, ast.Name)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            return _chain(node)[-1]
        return []

    def _chain(binop: ast.BinOp) -> list[list[str]]:
        """Flatten a >>/<< chain into ordered groups (already left-to-right for >>)."""
        left = (_chain(binop.left) if isinstance(binop.left, ast.BinOp)
                and isinstance(binop.left.op, (ast.RShift, ast.LShift))
                else [_operands(binop.left)])
        right = [_operands(binop.right)]
        groups = left + right
        if isinstance(binop.op, ast.LShift):
            groups = list(reversed(groups))
        return groups

    for node in ast.walk(tree):
        # t = SomeOperator(...)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
            for tgt in node.targets:
                if isinstance(tgt, ast.Name):
                    _op_from_call(tgt.id, node.value)
        # DAG(...) — direct or in `with`
        if isinstance(node, ast.Call) and _call_name(node) == "DAG":
            did = _kw(node, "dag_id")
            if not isinstance(did, str) and node.args:
                did = _const(node.args[0])
            if isinstance(did, str):
                dag_id = did
            for key in ("schedule_interval", "schedule"):
                sc = _kw(node, key)
                if isinstance(sc, str):
                    schedule = sc
                elif sc is not None and not isinstance(sc, str) and not isinstance(sc, ast.AST):
                    schedule = sc

    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp) \
                and isinstance(node.value.op, (ast.RShift, ast.LShift)):
            groups = _chain(node.value)
            for up_group, down_group in zip(groups, groups[1:]):
                for u in up_group:
                    for d in down_group:
                        deps.append((u, d))
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr in ("set_downstream", "set_upstream"):
                src = call.func.value
                if isinstance(src, ast.Name) and call.args:
                    for other in _operands(call.args[0]):
                        if call.func.attr == "set_downstream":
                            deps.append((src.id, other))
                        else:
                            deps.append((other, src.id))

    # var-level deps → task list keyed like the Glue path
    def _key(t):
        return re.sub(r"[^a-zA-Z0-9_]+", "_", t["task_id"]).strip("_")[:100]

    depends: dict[str, set] = {v: set() for v in tasks}
    for up, down in deps:
        if up in tasks and down in tasks:
            depends[down].add(up)

    out_tasks = []
    for var, t in tasks.items():
        out_tasks.append({
            "key": _key(t),
            "kind": t["kind"],
            "legacy_name": t["legacy_name"],
            "operator": t["operator"],
            "depends_on": sorted(_key(tasks[u]) for u in depends.get(var, ())),
        })
    out_tasks.sort(key=lambda t: (len(t["depends_on"]), t["key"]))

    quartz = airflow_schedule_to_quartz(schedule) if isinstance(schedule, str) else None
    if isinstance(schedule, str) and quartz is None and schedule != "@once":
        warnings.append(f"schedule {schedule!r} not auto-translatable — set the Job schedule manually")
    return {"name": dag_id, "tasks": out_tasks, "schedule": quartz, "warnings": warnings}


# ─── YAML DAG definitions (dag-factory style) ────────────────────────────────

def _yaml_task_kind(op: str, task_id: str, warnings: list) -> tuple[str, str | None]:
    """Map an operator class path to our task kind. Returns (kind, warning_added)."""
    cls = str(op or "").rsplit(".", 1)[-1]
    if "Glue" in cls and "Job" in cls:
        return "glue_job", cls
    if "Databricks" in cls:
        return "databricks", cls
    if cls in ("PythonOperator", "PythonVirtualenvOperator", "BranchPythonOperator"):
        return "python", cls
    if cls == "BashOperator":
        return "bash", cls
    if _SENSOR_RE.search(cls):
        warnings.append(f"task {task_id}: {cls} — Databricks has no in-job sensors; "
                        "map it to a file-arrival trigger on the Job (Phase 4) and drop the task")
        return "sensor", cls
    if cls == "TriggerDagRunOperator":
        warnings.append(f"task {task_id}: TriggerDagRunOperator — cross-DAG trigger; "
                        "model as a run_job_task pointing at the other migrated Job")
        return "trigger_dag", cls
    if cls in ("EmptyOperator", "DummyOperator"):
        return "empty", cls
    return "other", cls


def parse_dag_factory_yaml(name: str, text: str) -> list[dict]:
    """Parse dag-factory-style YAML into the normalized DAG model. Pure.

    Accepted shapes (tolerant):
      <dag_id>:                       # one or many top-level DAG keys
        schedule_interval: "0 2 * * *"   # or `schedule:`
        tasks:                            # mapping {task_id: {...}} OR list [{task_id,...}]
          load_config:
            operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator
            job_name: load_confiq
            dependencies: [wait_for_files]   # or upstream:/depends_on:

    Returns a LIST of DAGs ({name, tasks, schedule, warnings}) — a file may define several.
    """
    import yaml
    try:
        doc = yaml.safe_load(text)
    except Exception as exc:  # noqa: BLE001
        return [{"name": name, "tasks": [], "schedule": None,
                 "warnings": [f"YAML does not parse: {exc}"]}]
    if not isinstance(doc, dict):
        return [{"name": name, "tasks": [], "schedule": None,
                 "warnings": ["YAML is not a mapping — expected {dag_id: {tasks: ...}}"]}]

    # Optional wrapper key `dags:`
    dag_map = doc.get("dags") if isinstance(doc.get("dags"), dict) else doc
    out = []
    for dag_id, spec in dag_map.items():
        if not isinstance(spec, dict) or "tasks" not in spec:
            continue  # skip default: blocks etc.
        warnings: list[str] = []
        raw_tasks = spec.get("tasks")
        # normalize to {task_id: cfg}
        if isinstance(raw_tasks, list):
            raw_tasks = {str(t.get("task_id") or t.get("name") or f"task_{i}"): t
                         for i, t in enumerate(raw_tasks) if isinstance(t, dict)}
        if not isinstance(raw_tasks, dict):
            out.append({"name": dag_id, "tasks": [], "schedule": None,
                        "warnings": ["tasks is neither a mapping nor a list"]})
            continue
        tasks = []
        for task_id, cfg in raw_tasks.items():
            cfg = cfg if isinstance(cfg, dict) else {}
            op = cfg.get("operator") or cfg.get("operator_class") or ""
            kind, _cls = _yaml_task_kind(op, task_id, warnings)
            legacy = task_id
            if kind == "glue_job":
                legacy = str(cfg.get("job_name") or task_id)
            elif kind == "python" and cfg.get("python_callable_name"):
                legacy = str(cfg["python_callable_name"])
            deps = (cfg.get("dependencies") or cfg.get("upstream")
                    or cfg.get("depends_on") or [])
            if isinstance(deps, str):
                deps = [deps]
            tasks.append({
                "key": re.sub(r"[^a-zA-Z0-9_]+", "_", str(task_id)).strip("_")[:100],
                "kind": kind,
                "legacy_name": legacy,
                "operator": str(op).rsplit(".", 1)[-1],
                "depends_on": sorted(re.sub(r"[^a-zA-Z0-9_]+", "_", str(d)).strip("_")[:100]
                                     for d in deps),
            })
        tasks.sort(key=lambda t: (len(t["depends_on"]), t["key"]))
        sched = spec.get("schedule_interval", spec.get("schedule"))
        quartz = airflow_schedule_to_quartz(sched) if isinstance(sched, str) else None
        if isinstance(sched, str) and quartz is None and sched != "@once":
            warnings.append(f"schedule {sched!r} not auto-translatable — set the Job schedule manually")
        out.append({"name": str(dag_id), "tasks": tasks, "schedule": quartz, "warnings": warnings})
    if not out:
        out.append({"name": name, "tasks": [], "schedule": None,
                    "warnings": ["no DAG definitions found in YAML (need {dag_id: {tasks: ...}})"]})
    return out


def looks_like_yaml_dag(filename: str, text: str) -> bool:
    """Heuristic: route a pasted file to the YAML parser vs the Python AST parser."""
    fn = str(filename or "").lower()
    if fn.endswith((".yml", ".yaml")):
        return True
    if fn.endswith(".py"):
        return False
    t = (text or "").lstrip()
    # Python tells: imports / DAG( calls; YAML tells: top-level `key:` with tasks:
    if re.search(r"^\s*(from|import)\s+\w", t, re.M):
        return False
    return bool(re.search(r"^\S[^\n:]*:\s*$", t, re.M) and "tasks:" in t)


# ─── TARGET emitter: Airflow DAG that orchestrates the MIGRATED pipeline ─────
# Airflow on the target side drives Databricks + dbt (never the retired Glue jobs).
# Output is dag-factory YAML — the same shape sfglue INGESTS as a source — so the
# customer's Airflow runs it as-is, and the tool round-trips Airflow on both ends.

_INGEST_HINTS = ("landing", "raw", "ingest", "load", "bronze", "audit")
_FRAMEWORK_OPEN = ("fw_batch_open",)
_FRAMEWORK_CLOSE = ("fw_batch_close",)


def _yaml_quote(v) -> str:
    return '"' + str(v).replace('"', '\\"') + '"'


def emit_target_airflow_yaml(conversion: dict, destination: dict, *,
                             dag_id: str = "cdl_migrated_databricks",
                             schedule: str = "0 2 * * *",
                             databricks_conn_id: str = "databricks_default",
                             notebook_root: str = "/Shared/sfglue",
                             file_arrival_path: str | None = None,
                             owner: str = "cdl",
                             dbt_source: str = "workspace",
                             git_url: str | None = None,
                             git_branch: str = "main",
                             git_provider: str = "gitHub",
                             dbt_cloud_conn_id: str = "dbt_cloud_default",
                             dbt_cloud_job_id: str | None = None) -> dict:
    """Build a dag-factory YAML DAG that orchestrates the MIGRATED pipeline:

        [wait_for_files] -> [batch_open] -> ingest notebooks
                         -> dbt (per-layer or dbt-Cloud job)
                         -> [batch_close] -> notify

    Notebook steps are DatabricksSubmitRunOperator notebook_tasks against the pushed
    /Shared/sfglue notebooks. The dbt stage depends on ``dbt_source``:

      * "workspace" (default) — per-layer DatabricksSubmitRunOperator dbt_tasks reading
        the project pushed to <root>/dbt (no repo needed; ideal for demos).
      * "git" — same per-layer dbt_tasks but source=GIT with a git_source block
        (git_url/branch/provider) — the project lives in the customer's repo.
      * "dbt_cloud" — a single DbtCloudRunJobOperator that triggers the customer's
        dbt Cloud job (dbt Cloud owns the project + layer ordering).

    Pure — returns {"name", "yaml", "tasks", "layers", "dbt_source"}.
    """
    c = conversion or {}
    d = destination or {}
    catalog = d.get("catalog") or "main"
    wh = d.get("sql_warehouse_id") or "<SQL_WAREHOUSE_ID>"
    nb_root = (notebook_root or "/Shared/sfglue").rstrip("/")
    dbt_dir = f"{nb_root}/dbt"

    notebooks = list((c.get("notebooks") or {}).keys())
    models = list((c.get("dbt_models") or {}).keys())
    layers_present = []
    for layer in ("staging", "intermediate", "marts"):
        from backend.integrations.snowflake_glue_migration import dbt_layer_for_model
        if any(dbt_layer_for_model(m, (c.get("dbt_models") or {}).get(m, "")) == layer for m in models):
            layers_present.append(layer)
    if not layers_present and models:
        layers_present = ["staging"]

    open_nb = next((n for n in notebooks if any(h in n.lower() for h in _FRAMEWORK_OPEN)), None)
    close_nb = next((n for n in notebooks if any(h in n.lower() for h in _FRAMEWORK_CLOSE)), None)
    framework = {open_nb, close_nb}
    ingest_nbs = [n for n in notebooks
                  if n not in framework and any(h in n.lower() for h in _INGEST_HINTS)]
    if not ingest_nbs:  # fall back to any non-framework notebook
        ingest_nbs = [n for n in notebooks if n not in framework]

    def _nb_op(path):
        return {
            "operator": "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
            "databricks_conn_id": databricks_conn_id,
            "notebook_task": {"notebook_path": f"{nb_root}/src/notebooks/{path.rsplit('.',1)[0]}"},
        }

    def _dbt_op(layer):
        dbt_task = {
            "project_directory": dbt_dir if dbt_source == "workspace" else "dbt",
            "commands": ["dbt deps", f"dbt build --select {layer}"],
            "catalog": catalog,
            "warehouse_id": wh,
            "source": "GIT" if dbt_source == "git" else "WORKSPACE",
        }
        if dbt_source == "git":
            # named dbt_task + git_source satisfies the operator's validation
            return {
                "operator": "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
                "databricks_conn_id": databricks_conn_id,
                "dbt_task": dbt_task,
                "git_source": {
                    "git_url": git_url or "<GIT_REPO_URL>",
                    "git_provider": git_provider,
                    "git_branch": git_branch,
                },
            }
        # workspace mode: DatabricksSubmitRunOperator rejects a named dbt_task without
        # git_source (its validation predates workspace-sourced dbt projects), so pass
        # the raw runs/submit payload via ``json`` instead — a multi-task submit with a
        # serverless environment carrying dbt-databricks. Same API, no operator veto.
        return {
            "operator": "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
            "databricks_conn_id": databricks_conn_id,
            "json": {
                "run_name": f"{dag_id}.dbt_{layer}",
                "tasks": [{
                    "task_key": f"dbt_{layer}",
                    "dbt_task": dbt_task,
                    "environment_key": "dbt_env",
                }],
                "environments": [{
                    "environment_key": "dbt_env",
                    "spec": {"client": "1", "dependencies": ["dbt-databricks"]},
                }],
            },
        }

    def _dbt_cloud_op():
        return {
            "operator": "airflow.providers.dbt.cloud.operators.dbt.DbtCloudRunJobOperator",
            "dbt_cloud_conn_id": dbt_cloud_conn_id,
            "job_id": dbt_cloud_job_id or "<DBT_CLOUD_JOB_ID>",
            "check_interval": 60,
            "timeout": 3600,
        }

    tasks: list[tuple[str, dict, list]] = []  # (task_id, spec, deps)
    prev = None
    if file_arrival_path:
        tasks.append(("wait_for_files", {
            "operator": "airflow.providers.amazon.aws.sensors.s3.S3KeySensor",
            "bucket_key": file_arrival_path, "wildcard_match": True,
            "poke_interval": 300, "timeout": 3600,
        }, []))
        prev = "wait_for_files"
    if open_nb:
        tasks.append(("batch_open", _nb_op(open_nb), [prev] if prev else []))
        prev = "batch_open"
    for i, nb in enumerate(ingest_nbs):
        tid = "ingest" if len(ingest_nbs) == 1 else f"ingest_{i+1}"
        tasks.append((tid, _nb_op(nb), [prev] if prev else []))
        prev = tid
    if dbt_source == "dbt_cloud":
        # dbt Cloud owns the project + layer ordering — one triggering task.
        tasks.append(("dbt_cloud_run", _dbt_cloud_op(), [prev] if prev else []))
        prev = "dbt_cloud_run"
    else:
        for layer in layers_present:
            tid = f"dbt_{layer}"
            tasks.append((tid, _dbt_op(layer), [prev] if prev else []))
            prev = tid
    if close_nb:
        tasks.append(("batch_close", _nb_op(close_nb), [prev] if prev else []))
        prev = "batch_close"
    tasks.append(("notify", {
        "operator": "airflow.operators.bash.BashOperator",
        "bash_command": f"echo {dag_id} completed",
    }, [prev] if prev else []))

    # render dag-factory YAML
    L = [
        f"{dag_id}:",
        "  default_args:",
        f"    owner: {owner}",
        "    start_date: 2024-01-01",
        "    retries: 0",
        f'  schedule: "{schedule}"',
        "  catchup: false",
        "  max_active_runs: 1",
        f'  description: "Migrated CDL pipeline on Databricks + dbt, orchestrated by Airflow"',
        "  tasks:",
    ]
    for tid, spec, deps in tasks:
        L.append(f"    {tid}:")
        for k, v in spec.items():
            if isinstance(v, dict) and any(isinstance(x, (dict, list)) for x in v.values()):
                # deep payloads (e.g. a raw runs/submit ``json``) — emit as a JSON
                # flow mapping, which is valid YAML and survives any nesting depth
                L.append(f"      {k}: {json.dumps(v)}")
                continue
            if isinstance(v, dict):
                L.append(f"      {k}:")
                for kk, vv in v.items():
                    if isinstance(vv, list):
                        items = ", ".join(_yaml_quote(x) for x in vv)
                        L.append(f"        {kk}: [{items}]")
                    else:
                        L.append(f"        {kk}: {_yaml_quote(vv) if not str(vv).startswith(('/','{')) else vv}")
            elif isinstance(v, bool):
                L.append(f"      {k}: {str(v).lower()}")
            elif isinstance(v, (int,)):
                L.append(f"      {k}: {v}")
            else:
                L.append(f"      {k}: {v if k in ('operator',) else _yaml_quote(v)}")
        if deps:
            L.append(f"      dependencies: [{', '.join(deps)}]")
    L.append("")
    return {"name": dag_id, "yaml": "\n".join(L),
            "tasks": [t[0] for t in tasks], "layers": layers_present}


# ─── REST API introspection (Airflow 2.x stable API) ─────────────────────────

def fetch_airflow_dags(base_url: str, username: str = "", password: str = "",
                       token: str = "", timeout: int = 30) -> dict:
    """List DAGs + tasks via the Airflow REST API. Returns normalized DAGs."""
    import requests

    base = str(base_url or "").rstrip("/")
    if not base:
        return {"success": False, "error": "Airflow base_url is required"}
    auth = (username, password) if username else None
    hdrs = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        r = requests.get(f"{base}/api/v1/dags", params={"limit": 100},
                         auth=auth, headers=hdrs, timeout=timeout)
        r.raise_for_status()
        dags_meta = r.json().get("dags", [])
        out = []
        for meta in dags_meta:
            dag_id = meta.get("dag_id")
            warnings: list[str] = []
            rt = r2 = None
            r2 = requests.get(f"{base}/api/v1/dags/{dag_id}/tasks",
                              auth=auth, headers=hdrs, timeout=timeout)
            r2.raise_for_status()
            api_tasks = r2.json().get("tasks", [])
            # API gives downstream ids; invert to depends_on.
            depends: dict[str, set] = {t.get("task_id"): set() for t in api_tasks}
            for t in api_tasks:
                for d in t.get("downstream_task_ids", []) or []:
                    if d in depends:
                        depends[d].add(t.get("task_id"))
            tasks = []
            for t in api_tasks:
                cls = ((t.get("class_ref") or {}).get("class_name")) or ""
                kind = ("glue_job" if "Glue" in cls and "Job" in cls else
                        "sensor" if _SENSOR_RE.search(cls) else
                        "databricks" if "Databricks" in cls else "other")
                if kind == "sensor":
                    warnings.append(f"task {t.get('task_id')}: {cls} — map to a file-arrival trigger")
                tasks.append({
                    "key": re.sub(r"[^a-zA-Z0-9_]+", "_", t.get("task_id", ""))[:100],
                    "kind": kind, "legacy_name": t.get("task_id"), "operator": cls,
                    "depends_on": sorted(depends.get(t.get("task_id"), ())),
                })
            tasks.sort(key=lambda x: (len(x["depends_on"]), x["key"]))
            sched = meta.get("schedule_interval") or {}
            sched_val = sched.get("value") if isinstance(sched, dict) else sched
            out.append({"name": dag_id, "tasks": tasks,
                        "schedule": airflow_schedule_to_quartz(sched_val),
                        "warnings": warnings, "is_paused": meta.get("is_paused")})
        return {"success": True, "dags": out}
    except Exception as exc:  # noqa: BLE001
        detail = getattr(getattr(exc, "response", None), "text", "")[:200]
        return {"success": False, "error": f"Airflow API failed: {exc} {detail}".strip()}
