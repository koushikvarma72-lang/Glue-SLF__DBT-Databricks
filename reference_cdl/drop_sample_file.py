#!/usr/bin/env python3
"""Simulate an SFTP file arrival: upload a sample CSV to the landing zone and
(optionally) start the CDL workflow — the diagram's 'S3 data event' moment.

Usage:
  python drop_sample_file.py --bucket gmb-cdl-dev [--file my.csv] [--start-workflow]
"""

import argparse
import io
import sys
from datetime import datetime, timezone

SAMPLE = (
    "ID,NAME,ACCOUNT_TYPE,DATA_SRC_NM\n"
    "ACC-9001,Reference Clinic,HCO,reference_cdl\n"
    "ACC-9002,Dr Reference,HCP,reference_cdl\n"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--file", help="local file to drop (default: a generated sample CSV)")
    ap.add_argument("--prefix", default="landing/commercial/")
    ap.add_argument("--start-workflow", action="store_true")
    ap.add_argument("--workflow", default="cdl_ingest")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    import boto3
    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3 = session.client("s3")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if args.file:
        key = f"{args.prefix}{stamp}_{args.file.split('/')[-1]}"
        s3.upload_file(args.file, args.bucket, key)
    else:
        key = f"{args.prefix}account_{stamp}.csv"
        s3.upload_fileobj(io.BytesIO(SAMPLE.encode()), args.bucket, key)
    print(f"landed s3://{args.bucket}/{key}")

    if args.start_workflow:
        glue = session.client("glue")
        run = glue.start_workflow_run(Name=args.workflow)
        print(f"started workflow {args.workflow} run {run['RunId']}")
        print(f"watch it:  python check_run.py --workflow {args.workflow}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
