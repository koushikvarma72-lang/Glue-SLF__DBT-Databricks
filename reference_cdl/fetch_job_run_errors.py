"""Print per-task errors for the latest run of the deployed sfglue Databricks Job(s).

Usage:
    python fetch_job_run_errors.py --host https://dbc-xxxx.cloud.databricks.com \
        --token dapiXXXX [--match sfglue]
"""

import argparse
import json
import urllib.request


def _get(host, token, path, params=""):
    req = urllib.request.Request(f"{host.rstrip('/')}{path}{params}")
    req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--match", default="sfglue", help="job-name substring filter")
    a = ap.parse_args()

    jobs = _get(a.host, a.token, "/api/2.1/jobs/list", "?limit=25").get("jobs", [])
    jobs = [j for j in jobs if a.match.lower() in (j.get("settings", {}).get("name", "")).lower()]
    if not jobs:
        print(f"no jobs matching '{a.match}'")
        return
    for j in jobs:
        name = j["settings"]["name"]
        runs = _get(a.host, a.token, "/api/2.1/jobs/runs/list",
                    f"?job_id={j['job_id']}&limit=1").get("runs", [])
        if not runs:
            print(f"\n== {name}: no runs yet")
            continue
        run = _get(a.host, a.token, "/api/2.1/jobs/runs/get", f"?run_id={runs[0]['run_id']}")
        state = run.get("state", {})
        print(f"\n== {name} — run {runs[0]['run_id']}: "
              f"{state.get('result_state') or state.get('life_cycle_state')}")
        for t in run.get("tasks", []):
            ts = t.get("state", {})
            result = ts.get("result_state") or ts.get("life_cycle_state")
            mark = "✓" if result == "SUCCESS" else "✗"
            print(f"  {mark} {t.get('task_key')}: {result}")
            if result not in ("SUCCESS", None):
                # task-level message, then the run-output error for the juicy detail
                msg = ts.get("state_message", "")
                if msg:
                    print(f"      {msg[:300]}")
                try:
                    out = _get(a.host, a.token, "/api/2.1/jobs/runs/get-output",
                               f"?run_id={t['run_id']}")
                    err = out.get("error", "") or (out.get("error_trace", "")[:500])
                    if err:
                        print(f"      {err[:500]}")
                except Exception:  # noqa: BLE001 — output not always retrievable
                    pass


if __name__ == "__main__":
    main()
