"""Airflow orchestrates the MIGRATED pipeline — Airflow DAG → Databricks dbt Job.

This is the target-side twin of the (now-retired) Airflow→Glue script. Airflow does
NOT touch Glue here; the original pipeline stays as the Glue Workflow. On the migrated
side the Glue jobs are dbt models (staging / intermediate / marts) on Databricks, and
Airflow — defined by the SAME dag-factory YAML style — triggers the deployed Databricks
Job that runs them.

What it automates:
  1. writes a dag-factory loader + a cdl_migrated YAML DAG whose task is a
     DatabricksRunNowOperator pointing at the sfglue-deployed Databricks Job;
  2. creates/updates the Airflow `databricks_default` connection via the REST API;
  3. auto-discovers the deployed Job by name (tags.sfglue_source) if --job-id omitted;
  4. registers the DAG, unpauses, triggers, and watches to completion.

Prereqs (one-time, in the airflow venv):
    pip install apache-airflow-providers-databricks
    # that pip install may reinstall the native setproctitle — re-apply the shim:
    pip uninstall -y setproctitle && \
      printf 'def setproctitle(t,*a,**k):pass\\ndef getproctitle():return ""\\n' \
      > ~/airflow-venv/lib/python3.12/site-packages/setproctitle.py
    # restart `airflow standalone` after installing the provider.

Usage:
    python setup_airflow_databricks.py \
        --databricks-host https://dbc-xxxx.cloud.databricks.com \
        --databricks-token dapi... \
        --airflow-password <admin-pw> --trigger --watch
"""

import argparse
import base64
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

DAG_ID = "cdl_migrated"

LOADER = '''"""dag-factory loader — every *.yaml here becomes an airflow DAG automatically.
The word "airflow" must appear so safe-mode DAG discovery parses this file.
"""
from pathlib import Path

try:
    from dagfactory import load_yaml_dags
    load_yaml_dags(globals_dict=globals(), dags_folder=str(Path(__file__).parent))
except ImportError:
    import dagfactory
    for _y in sorted(Path(__file__).parent.glob("*.yaml")):
        _f = dagfactory.DagFactory(str(_y))
        _f.clean_dags(globals()); _f.generate_dags(globals())
'''


def render_yaml(job_id: int, schedule: str, host: str, token: str, helper: str) -> str:
    """Single-task DAG: one BashOperator triggers a PRE-DEPLOYED Databricks Job
    (jobs/run-now) via the stdlib trigger_databricks_job.py helper.

    Provider-free (no apache-airflow-providers-databricks, which would force Airflow
    3.x and break the 2.10.5 setup). "airflow" appears in the operator path so
    safe-mode DAG discovery parses this file. helper path may contain spaces/parens
    (e.g. 'USB Demo ( ... )') — single-quote it.
    """
    cmd = f"python3 '{helper}' --host {host} --token {token} --job-id {job_id}"
    return "\n".join([
        f"{DAG_ID}:",
        "  default_args:",
        "    owner: cdl",
        "    start_date: 2026-01-01",
        "    retries: 0",
        f'  schedule: "{schedule}"',
        "  catchup: false",
        "  max_active_runs: 1",
        '  description: "MIGRATED: Airflow orchestrates the dbt models on Databricks"',
        "  tasks:",
        "    run_migrated_pipeline:",
        "      operator: airflow.operators.bash.BashOperator",
        f'      bash_command: "{cmd}"',
        "    notify:",
        "      operator: airflow.operators.bash.BashOperator",
        '      bash_command: "echo migrated Databricks pipeline completed"',
        "      dependencies: [run_migrated_pipeline]",
        "",
    ])


def discover_notebooks(host: str, token: str, notebook_root: str) -> list:
    """List the migrated notebooks pushed to <root>/src/notebooks (Workspace API).

    Generic — nothing about any pipeline is hardcoded; whatever the sfglue push
    landed becomes an ingest task, in name order. Returns bare object names
    (no .py), or [] if the dir is absent/unreachable (the DAG then runs dbt only).
    """
    path = f"{notebook_root.rstrip('/')}/src/notebooks"
    url = host.rstrip("/") + "/api/2.0/workspace/list?path=" + urllib.parse.quote(path)
    try:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            objs = json.load(r).get("objects", [])
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not list {path}: {exc} — DAG will run dbt layers only)")
        return []
    names = [o["path"].rsplit("/", 1)[-1] for o in objs if o.get("object_type") == "NOTEBOOK"]
    return sorted(names)


def render_per_task_yaml(dag_id: str, schedule: str, host: str, token: str, helper: str,
                         notebooks: list, layers: list, notebook_root: str,
                         catalog: str, warehouse: str) -> str:
    """Full per-task provider-free DAG: one BashOperator per migrated notebook +
    per dbt layer, each shelling out to run_databricks_task.py (jobs/runs/submit).

    No databricks provider needed. Framework open/close notebooks bracket the
    ingest notebooks; dbt layers run after ingest; a notify task closes the DAG.
    Creds are inlined (local demo, ~/airflow/dags) exactly like the single-task
    variant — the helper path is single-quoted for spaces/parens.
    """
    root = notebook_root.rstrip("/")
    def _nb_cmd(name):
        nb = f"{root}/src/notebooks/{name.rsplit('.', 1)[0]}"
        c = f"python3 '{helper}' --host {host} --token {token} --notebook {nb}"
        return c + (f" --catalog {catalog}" if catalog else "")
    def _dbt_cmd(layer):
        c = (f"python3 '{helper}' --host {host} --token {token} "
             f"--dbt-project {root}/dbt --dbt-select {layer}")
        c += (f" --catalog {catalog}" if catalog else "")
        c += (f" --warehouse {warehouse}" if warehouse else "")
        return c

    open_nb = next((n for n in notebooks if "batch_open" in n.lower()), None)
    close_nb = next((n for n in notebooks if "batch_close" in n.lower()), None)
    ingest = [n for n in notebooks if n not in (open_nb, close_nb)]

    L = [f"{dag_id}:", "  default_args:", "    owner: cdl",
         "    start_date: 2026-01-01", "    retries: 0",
         f'  schedule: "{schedule}"', "  catchup: false", "  max_active_runs: 1",
         '  description: "MIGRATED: Airflow runs the Databricks + dbt pipeline per-task (provider-free)"',
         "  tasks:"]
    ordered, prev = [], None
    def _add(tid, cmd, dep):
        L.append(f"    {tid}:")
        L.append("      operator: airflow.operators.bash.BashOperator")
        L.append(f'      bash_command: "{cmd}"')
        if dep:
            L.append(f"      dependencies: [{dep}]")
    if open_nb:
        _add("batch_open", _nb_cmd(open_nb), prev); prev = "batch_open"
    for i, nb in enumerate(ingest):
        tid = "ingest" if len(ingest) == 1 else f"ingest_{i+1}"
        _add(tid, _nb_cmd(nb), prev); prev = tid
    for layer in layers:
        tid = f"dbt_{layer}"
        _add(tid, _dbt_cmd(layer), prev); prev = tid
    if close_nb:
        _add("batch_close", _nb_cmd(close_nb), prev); prev = "batch_close"
    _add("notify", f"echo {dag_id} completed", prev)
    L.append("")
    return "\n".join(L)


# ─── Airflow REST helpers ─────────────────────────────────────────────────────

def _af(a, method, path, body=None):
    url = a.airflow_url.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    tok = base64.b64encode(f"{a.airflow_user}:{a.airflow_password}".encode()).decode()
    req.add_header("Authorization", f"Basic {tok}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r) if r.length != 0 else {}


# ─── Databricks Jobs API (auto-discover the deployed job) ─────────────────────

def discover_job_id(a):
    """Find the sfglue-deployed Databricks Job (tags.sfglue_source or name prefix)."""
    import urllib.request as u
    url = a.databricks_host.rstrip("/") + "/api/2.1/jobs/list?limit=100"
    req = u.Request(url, headers={"Authorization": f"Bearer {a.databricks_token}"})
    with u.urlopen(req, timeout=30) as r:
        jobs = json.load(r).get("jobs", [])
    for j in jobs:
        s = j.get("settings", {})
        if "sfglue_source" in (s.get("tags") or {}) or str(s.get("name", "")).startswith("sfglue"):
            print(f"discovered job: {s.get('name')} (id {j['job_id']})")
            return j["job_id"]
    raise SystemExit("No sfglue-deployed Databricks Job found — deploy one from the app "
                     "first, or pass --job-id explicitly.")


# ─── DAG selection (connect to an existing DAG instead of creating one) ───────

def dag_exists(a, dag_id=None):
    """Is a DAG with this id already registered in Airflow? (selection probe)."""
    try:
        _af(a, "GET", f"/api/v1/dags/{dag_id or DAG_ID}")
        return True
    except Exception:
        return False


def list_dags(a):
    """Print the DAGs Airflow currently knows about, so the operator can pick one
    to --select. Migrated-pipeline DAGs (cdl_migrated*) are flagged."""
    try:
        dags = _af(a, "GET", "/api/v1/dags?limit=100").get("dags", [])
    except Exception as exc:  # noqa: BLE001
        print(f"could not list DAGs: {exc}"); return
    if not dags:
        print("no DAGs registered yet."); return
    print(f"{len(dags)} DAG(s) registered:")
    for d in dags:
        did = d.get("dag_id", "")
        tag = "  <- migrated pipeline" if did.startswith("cdl_migrated") else ""
        paused = " (paused)" if d.get("is_paused") else ""
        print(f"  - {did}{paused}{tag}")


# ─── register / unpause / trigger / watch (same as the Glue script) ───────────

def wait_for_dag(a, timeout=120):
    print(f"waiting for {DAG_ID} to register", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _af(a, "GET", f"/api/v1/dags/{DAG_ID}")
            print(" — registered ✓"); return True
        except Exception:
            print(".", end="", flush=True); time.sleep(5)
    print("\nDAG never appeared. Import errors:")
    try:
        for e in _af(a, "GET", "/api/v1/importErrors").get("import_errors", []):
            print(f"  {e.get('filename')}: {str(e.get('stack_trace'))[:300]}")
    except Exception:
        pass
    return False


def trigger_and_watch(a):
    _af(a, "PATCH", f"/api/v1/dags/{DAG_ID}", {"is_paused": False})
    print("unpaused ✓")
    if not a.trigger:
        return
    time.sleep(8)
    active = _af(a, "GET", f"/api/v1/dags/{DAG_ID}/dagRuns?state=running&state=queued").get("dag_runs", [])
    if active:
        run_id = active[0]["dag_run_id"]; print(f"adopting active run {run_id}")
    else:
        run_id = _af(a, "POST", f"/api/v1/dags/{DAG_ID}/dagRuns", {"conf": {}})["dag_run_id"]
        print(f"triggered run {run_id}")
    if not a.watch:
        return
    while True:
        time.sleep(15)
        dr = _af(a, "GET", f"/api/v1/dags/{DAG_ID}/dagRuns/{run_id}")
        tis = _af(a, "GET", f"/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
        st = {t["task_id"]: t["state"] for t in tis.get("task_instances", [])}
        ok = sum(1 for s in st.values() if s == "success")
        bad = sum(1 for s in st.values() if s in ("failed", "upstream_failed"))
        run = sum(1 for s in st.values() if s in ("running", "queued"))
        print(f"run {run_id}: {dr['state'].upper()}  ({ok} ok, {bad} failed, {run} running)")
        if dr["state"] in ("success", "failed"):
            for tid, s in st.items():
                mark = "✓" if s == "success" else ("✗" if s in ("failed", "upstream_failed") else "·")
                print(f"  {mark} {tid}: {s}")
            sys.exit(0 if dr["state"] == "success" else 1)


def main():
    global DAG_ID
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dags-dir", default="~/airflow/dags")
    ap.add_argument("--airflow-url", default="http://localhost:8080")
    ap.add_argument("--airflow-user", default="admin")
    ap.add_argument("--airflow-password", default="")
    # Databricks creds are only needed when CREATING a DAG (per-task submits / single
    # Job trigger). Selecting an already-registered DAG or listing DAGs needs Airflow only.
    ap.add_argument("--databricks-host", default="")
    ap.add_argument("--databricks-token", default="")
    ap.add_argument("--job-id", type=int, default=0, help="deployed Databricks Job id (auto-discover if omitted)")
    ap.add_argument("--schedule", default="0 3 * * *")
    ap.add_argument("--trigger", action="store_true")
    ap.add_argument("--watch", action="store_true")
    # ── Per-task provider-free DAG (the full migrated pipeline, no pre-built Job) ──
    ap.add_argument("--per-task", action="store_true",
                    help="deploy a per-task DAG (one BashOperator per migrated "
                         "notebook + dbt layer via run_databricks_task.py) instead "
                         "of a single task that triggers a pre-deployed Databricks Job")
    ap.add_argument("--notebook-root", default="/Shared/sfglue",
                    help="workspace root the notebooks + dbt project were pushed to")
    ap.add_argument("--catalog", default="", help="Unity Catalog for the tasks (per-task mode)")
    ap.add_argument("--warehouse", default="", help="SQL warehouse id for the dbt layers (per-task mode)")
    ap.add_argument("--layers", default="staging,intermediate,marts",
                    help="comma-separated dbt layers to build in order (per-task mode)")
    # ── DAG creation vs selection ─────────────────────────────────────────────
    ap.add_argument("--dag-id", default="",
                    help="target DAG id (default: cdl_migrated_databricks for --per-task, "
                         "else cdl_migrated)")
    ap.add_argument("--list-dags", action="store_true",
                    help="list the DAGs already registered in Airflow and exit "
                         "(pick one to pass to --select)")
    ap.add_argument("--select", action="store_true",
                    help="connect to an ALREADY-registered DAG (--dag-id) and run it — "
                         "do NOT write/overwrite any DAG file")
    ap.add_argument("--recreate", action="store_true",
                    help="overwrite the DAG file even if that DAG already exists "
                         "(default: reuse the existing one — selection)")
    a = ap.parse_args()

    # Resolve the target DAG id (explicit wins; else mode default).
    DAG_ID = a.dag_id or ("cdl_migrated_databricks" if a.per_task else "cdl_migrated")

    # --list-dags: just show what's registered so the operator can pick one.
    if a.list_dags:
        list_dags(a); return

    dags = Path(a.dags_dir).expanduser()

    # Decide create vs select. We can only probe/adopt when we can reach Airflow
    # (needs --airflow-password); without it we always create the file(s).
    exists = bool(a.airflow_password) and dag_exists(a, DAG_ID)
    select = a.select or (exists and not a.recreate)

    if a.select and not exists:
        print(f"--select given but DAG '{DAG_ID}' is not registered in Airflow.")
        if a.airflow_password:
            list_dags(a)
        raise SystemExit(f"Nothing to select. Re-run without --select to create '{DAG_ID}', "
                         "or pick an id from the list above.")

    if select:
        # SELECTION: reuse the existing DAG as-is — no file writes.
        print(f"selected existing DAG '{DAG_ID}' — reusing it (no files written). "
              "Pass --recreate to overwrite instead.")
    else:
        # CREATION: write the loader + the DAG YAML.
        if not a.databricks_host or not a.databricks_token:
            raise SystemExit("--databricks-host and --databricks-token are required to "
                             "CREATE a DAG (omit them only with --select on an existing DAG).")
        dags.mkdir(parents=True, exist_ok=True)
        (dags / "load_yaml_dags.py").write_text(LOADER)
        if a.per_task:
            # Full per-task pipeline: NO pre-deployed Job required — each notebook / dbt
            # layer is submitted directly via run_databricks_task.py (jobs/runs/submit).
            helper = str((Path(__file__).parent / "run_databricks_task.py").resolve())
            notebooks = discover_notebooks(a.databricks_host, a.databricks_token, a.notebook_root)
            layers = [x.strip() for x in a.layers.split(",") if x.strip()]
            (dags / f"{DAG_ID}.yaml").write_text(
                render_per_task_yaml(DAG_ID, a.schedule, a.databricks_host, a.databricks_token,
                                     helper, notebooks, layers, a.notebook_root,
                                     a.catalog, a.warehouse))
            print(f"{'recreated' if exists else 'wrote'} {dags}/{DAG_ID}.yaml "
                  f"({len(notebooks)} notebook task(s) + {len(layers)} dbt layer(s))")
        else:
            job_id = a.job_id or discover_job_id(a)
            helper = str((Path(__file__).parent / "trigger_databricks_job.py").resolve())
            (dags / f"{DAG_ID}.yaml").write_text(
                render_yaml(job_id, a.schedule, a.databricks_host, a.databricks_token, helper))
            print(f"{'recreated' if exists else 'wrote'} {dags}/{DAG_ID}.yaml (job_id {job_id})")

    if not a.airflow_password:
        print("no --airflow-password — files written; trigger skipped.")
        return
    if wait_for_dag(a):
        trigger_and_watch(a)


if __name__ == "__main__":
    main()
