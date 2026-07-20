"""Run ONE Databricks task (a workspace notebook or a dbt build) and poll to
completion — pure stdlib.

Called by the provider-free target Airflow DAG's BashOperator tasks, so the DAG
needs NO apache-airflow-providers-databricks (which drags Airflow to 3.x and
breaks the 2.10.5 reference setup). Uses the one-shot Jobs API `runs/submit`
(no pre-created Job needed) + `runs/get` polling. Exits 0 on SUCCESS, 1 on
failure — so Airflow marks the task accordingly.

Credentials come from the process env by default (DATABRICKS_HOST /
DATABRICKS_TOKEN) so nothing secret is written into the DAG YAML; --host/--token
override for local runs.

Examples:
    # a workspace notebook
    python3 run_databricks_task.py --notebook /Shared/sfglue/src/notebooks/landing_to_raw \\
        --catalog workspace

    # a dbt layer against a workspace-resident project
    python3 run_databricks_task.py --dbt-project /Shared/sfglue/dbt --dbt-select staging \\
        --catalog workspace --warehouse 66b30eb900bcd97a
"""
import argparse
import json
import os
import sys
import time
import urllib.request

POLL_SECONDS = 15
TERMINAL = ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")


def _api(host, token, path, body=None):
    url = host.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def _build_task(a):
    """Assemble the single-task runs/submit payload from the CLI args.

    A notebook task and a dbt task are mutually exclusive; exactly one must be
    given. base_parameters/env carry the runtime widgets the migrated notebooks
    read (catalog + S3 buckets) so an empty bucket name never reaches boto3.
    """
    base_params = {}
    if a.catalog:
        base_params["catalog"] = a.catalog
    if a.pipeline_bucket:
        base_params["S3_VENDOR_BUCKET"] = a.pipeline_bucket
        base_params["S3_DL_BUCKET"] = a.pipeline_bucket
    if a.aws_region:
        base_params["AWS_REGION"] = a.aws_region

    if a.notebook:
        task = {
            "task_key": a.task_key or "notebook",
            "notebook_task": {"notebook_path": a.notebook,
                              "base_parameters": base_params},
        }
        # a notebook needs compute: serverless (default) or an existing cluster
        if a.existing_cluster_id:
            task["existing_cluster_id"] = a.existing_cluster_id
        return task, None

    if a.dbt_project:
        dbt_task = {
            "project_directory": a.dbt_project,
            "commands": ["dbt deps", f"dbt build --select {a.dbt_select}"],
            "catalog": a.catalog or "main",
            "source": "GIT" if a.git_url else "WORKSPACE",
        }
        if a.warehouse:
            dbt_task["warehouse_id"] = a.warehouse
        if a.dbt_schema:
            dbt_task["schema"] = a.dbt_schema
        task = {"task_key": a.task_key or f"dbt_{a.dbt_select}",
                "dbt_task": dbt_task, "environment_key": "dbt_env"}
        env = [{"environment_key": "dbt_env",
                "spec": {"client": "1", "dependencies": ["dbt-databricks"]}}]
        git_source = None
        if a.git_url:
            git_source = {"git_url": a.git_url, "git_provider": a.git_provider,
                          "git_branch": a.git_branch}
        return task, (env, git_source)

    raise SystemExit("give exactly one of --notebook or --dbt-project")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default=os.environ.get("DATABRICKS_HOST", ""))
    ap.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""))
    ap.add_argument("--task-key", default="")
    ap.add_argument("--notebook", default="")
    ap.add_argument("--existing-cluster-id", default=os.environ.get("DATABRICKS_CLUSTER_ID", ""))
    ap.add_argument("--dbt-project", default="")
    ap.add_argument("--dbt-select", default="staging")
    ap.add_argument("--dbt-schema", default="")
    ap.add_argument("--warehouse", default=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))
    ap.add_argument("--catalog", default=os.environ.get("DATABRICKS_CATALOG", ""))
    ap.add_argument("--pipeline-bucket", default=os.environ.get("S3_PIPELINE_BUCKET", ""))
    ap.add_argument("--aws-region", default=os.environ.get("AWS_REGION", ""))
    ap.add_argument("--git-url", default="")
    ap.add_argument("--git-branch", default="main")
    ap.add_argument("--git-provider", default="gitHub")
    a = ap.parse_args()

    if not a.host or not a.token:
        raise SystemExit("Databricks host/token missing — set DATABRICKS_HOST + "
                         "DATABRICKS_TOKEN in the Airflow env (or pass --host/--token).")

    task, extras = _build_task(a)
    run_name = a.task_key or task["task_key"]
    payload = {"run_name": f"cdl_migrated.{run_name}", "tasks": [task]}
    if extras:
        env, git_source = extras
        payload["environments"] = env
        if git_source:
            payload["git_source"] = git_source

    submitted = _api(a.host, a.token, "/api/2.1/jobs/runs/submit", payload)
    run_id = submitted.get("run_id")
    if not run_id:
        print(f"runs/submit returned no run_id: {submitted}", flush=True)
        sys.exit(1)
    print(f"submitted Databricks run {run_id} ({run_name})", flush=True)

    while True:
        time.sleep(POLL_SECONDS)
        info = _api(a.host, a.token, f"/api/2.1/jobs/runs/get?run_id={run_id}")
        state = info.get("state", {})
        life, result = state.get("life_cycle_state"), state.get("result_state")
        print(f"  run {run_id}: {life} {result or ''}".rstrip(), flush=True)
        if life in TERMINAL:
            if result == "SUCCESS":
                print("Databricks task SUCCEEDED", flush=True)
                sys.exit(0)
            print(f"Databricks task FAILED: {result} — {state.get('state_message', '')}",
                  flush=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
