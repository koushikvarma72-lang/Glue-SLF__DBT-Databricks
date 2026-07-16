#!/usr/bin/env python3
"""Create the CDL S3 zone layout (idempotent).

One bucket, zone prefixes matching the reference architecture:
  landing/commercial/   landing_reject/commercial/
  raw/commercial/       raw_reject/commercial/
  curated/commercial/   publish/commercial/

Usage:  python setup_s3_zones.py --bucket gmb-cdl-dev [--region us-west-2]
Credentials: standard boto3 chain (AWS_PROFILE / env / SSO).
"""

import argparse
import sys

ZONES = [
    "landing/commercial/", "landing_reject/commercial/",
    "raw/commercial/", "raw_reject/commercial/",
    "curated/commercial/", "publish/commercial/",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    import boto3
    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    s3 = session.client("s3")
    region = args.region or session.region_name or "us-east-1"

    try:
        s3.head_bucket(Bucket=args.bucket)
        print(f"bucket s3://{args.bucket} exists")
    except Exception:
        kwargs = {} if region == "us-east-1" else {
            "CreateBucketConfiguration": {"LocationConstraint": region}}
        s3.create_bucket(Bucket=args.bucket, **kwargs)
        print(f"created bucket s3://{args.bucket} ({region})")

    for zone in ZONES:
        s3.put_object(Bucket=args.bucket, Key=zone)  # zero-byte prefix marker
        print(f"  zone s3://{args.bucket}/{zone}")
    print("done — zone layout matches the reference diagram.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
