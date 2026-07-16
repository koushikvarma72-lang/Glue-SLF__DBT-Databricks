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
    # Provider-free: a BashOperator calls the stdlib helper that hits the Databricks
    # jobs/run-now REST API. Avoids apache-airflow-providers-databricks (which upgrades
    # Airflow to 3.x and breaks the 2.10.5 setup). "airflow" appears in the operator
    # path so safe-mode DAG discovery parses this file.
    # helper path may contain spaces/parens (e.g. 'USB Demo ( ... )') — single-quote it.
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
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dags-dir", default="~/airflow/dags")
    ap.add_argument("--airflow-url", default="http://localhost:8080")
    ap.add_argument("--airflow-user", default="admin")
    ap.add_argument("--airflow-password", default="")
    ap.add_argument("--databricks-host", required=True)
    ap.add_argument("--databricks-token", required=True)
    ap.add_argument("--job-id", type=int, default=0, help="deployed Databricks Job id (auto-discover if omitted)")
    ap.add_argument("--schedule", default="0 3 * * *")
    ap.add_argument("--trigger", action="store_true")
    ap.add_argument("--watch", action="store_true")
    a = ap.parse_args()

    job_id = a.job_id or discover_job_id(a)
    helper = str((Path(__file__).parent / "trigger_databricks_job.py").resolve())
    dags = Path(a.dags_dir).expanduser()
    dags.mkdir(parents=True, exist_ok=True)
    (dags / "load_yaml_dags.py").write_text(LOADER)
    (dags / "cdl_migrated.yaml").write_text(
        render_yaml(job_id, a.schedule, a.databricks_host, a.databricks_token, helper))
    print(f"wrote {dags}/cdl_migrated.yaml (job_id {job_id})")

    if not a.airflow_password:
        print("no --airflow-password — files written; trigger skipped.")
        return
    if wait_for_dag(a):
        trigger_and_watch(a)


if __name__ == "__main__":
    main()
