"""Zero-click Airflow orchestration of the reference CDL pipeline.

Automates the whole source-side Airflow story:
  1. writes a dag-factory loader + a cdl_ingest_yaml DAG definition (pure YAML)
     into your Airflow dags folder — the DAG "creates itself" on the next parse;
  2. ensures the REST API accepts basic auth (patches airflow.cfg once);
  3. waits for the DAG to register, unpauses it, triggers a run, and watches it
     to completion (the Airflow twin of check_run.py).

The generated YAML passes CONTROL_* to every Glue job via script_args, so the
jobs reach your local Postgres through the bore tunnel WITHOUT Glue Workflow
run-properties (this DAG runs the jobs directly).

The SAME YAML is valid input for the sfglue migration app (paste it into the
Airflow box) — the file that runs the original pipeline is the file that
migrates it.

Prereqs (one-time, inside the airflow venv):
    pip install dag-factory

Usage:
    source ~/airflow-venv/bin/activate && export AIRFLOW_HOME=~/airflow
    # terminal 1: airflow standalone     # terminal 2: bore local 5432 --to bore.pub
    python setup_airflow_pipeline.py --control-port <BORE_PORT> \
        --airflow-password <ADMIN_PASSWORD> --trigger --watch
"""

import argparse
import base64
import configparser
import json
import sys
import time
import urllib.request
from pathlib import Path

DAG_ID = "cdl_ingest_yaml"
GLUE_CHAIN = ["load_confiq", "parent_batch_open", "landing_to_raw",
              "raw_to_curated", "curated_to_publish", "parent_batch_close"]
TASK_IDS = ["load_config", "batch_open", "landing_to_raw",
            "raw_to_curated", "curated_to_publish", "batch_close"]

LOADER = '''"""dag-factory loader — every *.yaml in this folder becomes a DAG automatically.

Handles both dag-factory APIs: 1.x exposes load_yaml_dags(); 0.x exposes DagFactory.
NOTE: the word "airflow" must appear in this file — Airflow's safe-mode DAG discovery
only parses .py files containing both "airflow" and "dag".
"""
from pathlib import Path

try:  # dag-factory >= 1.0
    from dagfactory import load_yaml_dags
    load_yaml_dags(globals_dict=globals(),
                   dags_folder=str(Path(__file__).parent))
except ImportError:  # dag-factory 0.x
    import dagfactory
    for _yaml in sorted(Path(__file__).parent.glob("*.yaml")):
        _factory = dagfactory.DagFactory(str(_yaml))
        _factory.clean_dags(globals())
        _factory.generate_dags(globals())
'''


def render_yaml(a) -> str:
    """The DAG as dag-factory YAML. script_args carry the control-DB coordinates."""
    ctl = {
        # The job scripts declare WORKFLOW_NAME/RUN_ID as REQUIRED args (they were
        # written for Glue Workflow runs). Airflow starts them directly, so we pass
        # stand-ins; control_connection.py prefers the CONTROL_* args below and
        # skips the workflow run-properties lookup entirely.
        "--WORKFLOW_NAME": "airflow_direct",
        "--WORKFLOW_RUN_ID": "airflow_direct",
        "--CONTROL_TARGET": a.control_target,
        "--CONTROL_HOST": a.control_host,
        "--CONTROL_PORT": str(a.control_port),
        "--CONTROL_DB": a.control_db,
        "--CONTROL_USER": a.control_user,
        "--CONTROL_PASSWORD": a.control_password,
    }
    args_yaml = "\n".join(f'        "{k}": "{v}"' for k, v in ctl.items())
    out = [
        f"{DAG_ID}:",
        "  default_args:",
        "    owner: cdl",
        "    start_date: 2026-01-01",
        "    retries: 0",
        f'  schedule: "{a.schedule}"',  # dag-factory >= 1.x key (was schedule_interval)
        "  catchup: false",
        "  max_active_runs: 1",  # Glue jobs allow 1 concurrent run — never race two DAG runs
        '  description: "CDL medaffairs ingestion (YAML-defined): landing -> raw -> curated -> publish"',
        "  tasks:",
    ]
    prev = None
    for task_id, job in zip(TASK_IDS, GLUE_CHAIN):
        out += [
            f"    {task_id}:",
            "      operator: airflow.providers.amazon.aws.operators.glue.GlueJobOperator",
            f"      job_name: {job}",
            f"      region_name: {a.region}",
            "      wait_for_completion: true",
            "      script_args:",
            args_yaml,
        ]
        if prev:
            out += [f"      dependencies: [{prev}]"]
        prev = task_id
    out += [
        "    notify:",
        "      operator: airflow.operators.bash.BashOperator",
        '      bash_command: "echo cdl_ingest pipeline completed"',
        f"      dependencies: [{prev}]",
        "",
    ]
    return "\n".join(out)


# ─── Airflow REST helpers ─────────────────────────────────────────────────────

def _req(a, method, path, body=None):
    url = a.airflow_url.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    tok = base64.b64encode(f"{a.airflow_user}:{a.airflow_password}".encode()).decode()
    req.add_header("Authorization", f"Basic {tok}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def ensure_basic_auth(a) -> bool:
    """Patch [api] auth_backends in airflow.cfg to include basic_auth. Returns True if changed."""
    cfg_path = Path(a.airflow_home).expanduser() / "airflow.cfg"
    if not cfg_path.exists():
        return False
    cp = configparser.ConfigParser()
    cp.read(cfg_path)
    want = "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    cur = cp.get("api", "auth_backends", fallback="")
    if "basic_auth" in cur:
        return False
    cp.set("api", "auth_backends", want)
    with open(cfg_path, "w") as fh:
        cp.write(fh)
    return True


def wait_for_dag(a, timeout=120):
    print(f"waiting for {DAG_ID} to register", end="", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _req(a, "GET", f"/api/v1/dags/{DAG_ID}")
            print(" — registered ✓")
            return True
        except Exception:
            print(".", end="", flush=True)
            time.sleep(5)
    print("\nDAG never appeared. Checking import errors…")
    try:
        errs = _req(a, "GET", "/api/v1/importErrors").get("import_errors", [])
        for e in errs:
            print(f"  {e.get('filename')}: {str(e.get('stack_trace'))[:300]}")
        if not errs:
            print("  none reported — is `pip install dag-factory` done in the airflow venv?")
    except Exception as exc:  # noqa: BLE001
        print(f"  (could not read import errors: {exc})")
    return False


def trigger_and_watch(a):
    _req(a, "PATCH", f"/api/v1/dags/{DAG_ID}", {"is_paused": False})
    print("unpaused ✓")
    if not a.trigger:
        return
    # Unpausing a DAG with a past start_date creates one scheduled catch-up run.
    # Adopt any already-active run instead of racing a second one against it
    # (the Glue jobs only allow one concurrent run each).
    time.sleep(8)
    active = _req(a, "GET",
                  f"/api/v1/dags/{DAG_ID}/dagRuns?state=running&state=queued").get("dag_runs", [])
    if active:
        run_id = active[0]["dag_run_id"]
        print(f"adopting already-active run {run_id}")
    else:
        run = _req(a, "POST", f"/api/v1/dags/{DAG_ID}/dagRuns", {"conf": {}})
        run_id = run["dag_run_id"]
        print(f"triggered run {run_id}")
    if not a.watch:
        return
    n = len(TASK_IDS) + 1
    while True:
        time.sleep(15)
        dr = _req(a, "GET", f"/api/v1/dags/{DAG_ID}/dagRuns/{run_id}")
        tis = _req(a, "GET", f"/api/v1/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
        states = {t["task_id"]: t["state"] for t in tis.get("task_instances", [])}
        ok = sum(1 for s in states.values() if s == "success")
        bad = sum(1 for s in states.values() if s in ("failed", "upstream_failed"))
        running = sum(1 for s in states.values() if s in ("running", "queued"))
        print(f"run {run_id}: {dr['state'].upper()}  ({ok}/{n} ok, {bad} failed, {running} running)")
        if dr["state"] in ("success", "failed"):
            for tid, st in states.items():
                mark = "✓" if st == "success" else ("✗" if st in ("failed", "upstream_failed") else "·")
                print(f"  {mark} {tid}: {st}")
            sys.exit(0 if dr["state"] == "success" else 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dags-dir", default="~/airflow/dags")
    ap.add_argument("--airflow-home", default="~/airflow")
    ap.add_argument("--airflow-url", default="http://localhost:8080")
    ap.add_argument("--airflow-user", default="admin")
    ap.add_argument("--airflow-password", default="")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--schedule", default="0 2 * * *")
    ap.add_argument("--control-target", default="local_postgres")
    ap.add_argument("--control-host", default="bore.pub")
    ap.add_argument("--control-port", required=True, help="current bore.pub port")
    ap.add_argument("--control-db", default="control")
    ap.add_argument("--control-user", default="venkatajayakrishnakoushikvarma")
    ap.add_argument("--control-password", default="")
    ap.add_argument("--trigger", action="store_true", help="trigger a run after registering")
    ap.add_argument("--watch", action="store_true", help="poll the run to completion")
    a = ap.parse_args()

    dags = Path(a.dags_dir).expanduser()
    dags.mkdir(parents=True, exist_ok=True)
    (dags / "load_yaml_dags.py").write_text(LOADER)
    (dags / "cdl_ingest.yaml").write_text(render_yaml(a))
    print(f"wrote {dags}/load_yaml_dags.py and {dags}/cdl_ingest.yaml")

    if ensure_basic_auth(a):
        print("airflow.cfg patched for REST basic auth — RESTART `airflow standalone`, then rerun this script.")
        sys.exit(2)
    if not a.airflow_password:
        print("no --airflow-password given — files written; unpause/trigger skipped.")
        return
    if wait_for_dag(a):
        trigger_and_watch(a)


if __name__ == "__main__":
    main()
