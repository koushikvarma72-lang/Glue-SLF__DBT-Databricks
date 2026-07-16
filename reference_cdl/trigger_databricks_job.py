"""Trigger a Databricks Job and poll to completion — pure stdlib.

Called by the Airflow BashOperator task so the DAG needs NO databricks provider
(which drags apache-airflow to 3.x and breaks the 2.10.5 setup). Exits 0 on
success, 1 on failure — so Airflow marks the task accordingly.

    python3 trigger_databricks_job.py --host https://dbc-xxx.cloud.databricks.com \
        --token dapi... --job-id 123456789
"""
import argparse
import json
import sys
import time
import urllib.request


def _api(host, token, path, body=None):
    url = host.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data,
                                 headers={"Authorization": f"Bearer {token}",
                                          "Content-Type": "application/json"},
                                 method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--job-id", type=int, required=True)
    a = ap.parse_args()

    run = _api(a.host, a.token, "/api/2.1/jobs/run-now", {"job_id": a.job_id})
    run_id = run["run_id"]
    print(f"triggered Databricks run {run_id} for job {a.job_id}", flush=True)

    while True:
        time.sleep(15)
        info = _api(a.host, a.token, f"/api/2.1/jobs/runs/get?run_id={run_id}")
        state = info.get("state", {})
        life = state.get("life_cycle_state")
        result = state.get("result_state")
        print(f"  run {run_id}: {life} {result or ''}".rstrip(), flush=True)
        if life in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if result == "SUCCESS":
                print("Databricks job SUCCEEDED", flush=True)
                sys.exit(0)
            print(f"Databricks job FAILED: {result} — {state.get('state_message','')}", flush=True)
            sys.exit(1)


if __name__ == "__main__":
    main()
