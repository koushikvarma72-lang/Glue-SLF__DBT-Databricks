#!/usr/bin/env python3
"""Watch the latest CDL workflow run + show the control-DB audit trail.

Usage:
  python check_run.py --workflow cdl_ingest [--watch]
  python check_run.py --workflow cdl_ingest --pg-dsn "host=localhost dbname=control user=..."
"""

import argparse
import sys
import time


def _workflow_status(glue, name):
    wf = glue.get_workflow(Name=name, IncludeGraph=False).get("Workflow") or {}
    run = wf.get("LastRun") or {}
    stats = run.get("Statistics") or {}
    return {
        "run_id": run.get("WorkflowRunId"),
        "status": run.get("Status"),
        "succeeded": stats.get("SucceededActions", 0),
        "failed": stats.get("FailedActions", 0),
        "running": stats.get("RunningActions", 0),
        "total": stats.get("TotalActions", 0),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", default="cdl_ingest")
    ap.add_argument("--watch", action="store_true", help="poll every 15s until finished")
    ap.add_argument("--pg-dsn", help="optional psycopg/pg8000 DSN for the control DB audit query")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    import boto3
    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    glue = session.client("glue")

    while True:
        st = _workflow_status(glue, args.workflow)
        print(f"run {st['run_id']}: {st['status']}  "
              f"({st['succeeded']}/{st['total']} ok, {st['failed']} failed, {st['running']} running)")
        if not args.watch or st["status"] not in ("RUNNING", None):
            break
        time.sleep(15)

    if args.pg_dsn:
        try:
            import pg8000.dbapi as pg
            parts = dict(p.split("=", 1) for p in args.pg_dsn.split())
            conn = pg.connect(host=parts.get("host", "localhost"),
                              database=parts.get("dbname", "control"),
                              user=parts.get("user", ""), password=parts.get("password", ""))
            cur = conn.cursor()
            cur.execute("SELECT batch_id, batch_status, batch_start_time, batch_end_time "
                        "FROM parent_batch_process ORDER BY batch_start_time DESC LIMIT 5")
            print("\nlatest batches:")
            for r in cur.fetchall():
                print("  ", " | ".join(str(x) for x in r))
            conn.close()
        except Exception as exc:  # noqa: BLE001
            print(f"(control-DB audit query skipped: {exc})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
