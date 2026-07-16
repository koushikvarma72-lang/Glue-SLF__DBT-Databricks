#!/usr/bin/env python3
"""Wire the existing Glue jobs into the reference CDL Workflow (idempotent).

Creates workflow ``cdl_ingest`` with the diagram's chain:

  START (on-demand) → load_config → parent_batch_open → landing_to_raw
        → raw_to_curated → curated_to_publish → publish_to_snowflake
        → parent_batch_close

Each hop is a CONDITIONAL trigger on the previous job's SUCCEEDED state — exactly
the structure the sfglue orchestration converter reads (introspect with
include:["workflows","triggers"], then /api/sfglue/workflows/plan).

Usage:
  python setup_glue_workflow.py                       # auto-discover job names
  python setup_glue_workflow.py --jobs a,b,c,d,e,f,g  # explicit, in chain order
  python setup_glue_workflow.py --schedule "cron(0 2 * * ? *)"   # scheduled start
"""

import argparse
import sys

# Chain order + name patterns for auto-discovery (first match wins, fuzzy on _/typos —
# the demo account's job is literally named 'load_confiq').
CHAIN = [
    ("load_config", ("load_config", "load_confiq", "loadconfig")),
    ("parent_batch_open", ("parent_batch_open", "batch_open")),
    ("landing_to_raw", ("landing_to_raw",)),
    ("raw_to_curated", ("raw_to_curated",)),
    ("curated_to_publish", ("curated_to_publish",)),
    ("publish_to_snowflake", ("publish_to_snowflake", "publish_snowflake")),
    ("parent_batch_close", ("parent_batch_close", "batch_close")),
]
WORKFLOW = "cdl_ingest"


def _discover(glue) -> list[str]:
    names, token = [], None
    while True:
        page = glue.list_jobs(**({"NextToken": token} if token else {}))
        names.extend(page.get("JobNames", []))
        token = page.get("NextToken")
        if not token:
            break
    low = {n.lower(): n for n in names}
    chain = []
    for role, patterns in CHAIN:
        hit = next((low[k] for p in patterns for k in low if p in k), None)
        if not hit:
            sys.exit(f"ERROR: no Glue job matching {role!r} (patterns {patterns}). "
                     f"Pass --jobs explicitly. Found jobs: {sorted(names)}")
        chain.append(hit)
    return chain


def _recreate_trigger(glue, name, **kwargs):
    try:
        glue.delete_trigger(Name=name)
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_trigger(Name=name, **kwargs)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", help="7 job names in chain order, comma-separated")
    ap.add_argument("--schedule", help="Glue cron for the start trigger, e.g. 'cron(0 2 * * ? *)'. "
                                       "Default: on-demand.")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    import boto3
    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    glue = session.client("glue")

    chain = ([j.strip() for j in args.jobs.split(",")] if args.jobs else _discover(glue))
    if len(chain) != len(CHAIN):
        sys.exit(f"ERROR: expected {len(CHAIN)} jobs in order, got {len(chain)}")
    print("chain:", " → ".join(chain))

    try:
        glue.create_workflow(Name=WORKFLOW, Description="Reference CDL ingestion chain "
                             "(created by sfglue reference_cdl kit)")
        print(f"created workflow {WORKFLOW}")
    except glue.exceptions.AlreadyExistsException:
        print(f"workflow {WORKFLOW} exists — rewiring triggers")

    # Start trigger → first job.
    start_kwargs = dict(WorkflowName=WORKFLOW, Actions=[{"JobName": chain[0]}])
    if args.schedule:
        start_kwargs.update(Type="SCHEDULED", Schedule=args.schedule, StartOnCreation=True)
    else:
        start_kwargs.update(Type="ON_DEMAND")
    _recreate_trigger(glue, f"{WORKFLOW}__start", **start_kwargs)
    print(f"  trigger {WORKFLOW}__start → {chain[0]}"
          + (f"  [{args.schedule}]" if args.schedule else "  [on-demand]"))

    # Conditional hops: after job i SUCCEEDED → job i+1.
    for i in range(len(chain) - 1):
        _recreate_trigger(
            glue, f"{WORKFLOW}__after_{chain[i]}"[:255],
            WorkflowName=WORKFLOW, Type="CONDITIONAL", StartOnCreation=True,
            Predicate={"Logical": "AND", "Conditions": [{
                "LogicalOperator": "EQUALS", "JobName": chain[i], "State": "SUCCEEDED"}]},
            Actions=[{"JobName": chain[i + 1]}],
        )
        print(f"  trigger after {chain[i]} SUCCEEDED → {chain[i + 1]}")

    print(f"\ndone. Start a run:  aws glue start-workflow-run --name {WORKFLOW}")
    print("Then introspect it in sfglue with include:[\"workflows\",\"triggers\"] and "
          "convert via /api/sfglue/workflows/plan.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
