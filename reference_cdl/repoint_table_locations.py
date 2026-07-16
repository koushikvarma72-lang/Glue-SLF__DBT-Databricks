"""Repoint Glue catalog table locations to the demo bucket's zones.

silver tables -> s3://<bucket>/curated/<table>
gold tables   -> s3://<bucket>/publish/<table>

Usage:
    python repoint_table_locations.py            # dry run (prints plan)
    python repoint_table_locations.py --apply    # actually update
"""

import sys

import boto3

BUCKET = "cdl-demo-495688866359"
ZONES = {"medaffairs_silver": "curated", "medaffairs_gold": "publish"}
# update_table accepts only TableInput keys, not the read-only ones get_table returns
_TI_KEYS = ("Name", "Description", "Owner", "Retention", "StorageDescriptor",
            "PartitionKeys", "ViewOriginalText", "ViewExpandedText",
            "TableType", "Parameters")


def main(apply: bool) -> None:
    glue = boto3.client("glue")
    for db, zone in ZONES.items():
        paginator = glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=db):
            for tbl in page["TableList"]:
                name = tbl["Name"]
                old = (tbl.get("StorageDescriptor") or {}).get("Location") or ""
                new = f"s3://{BUCKET}/{zone}/{name}"
                if old == new:
                    print(f"  ok      {db}.{name}  (already {new})")
                    continue
                print(f"  repoint {db}.{name}: {old or '<none>'} -> {new}")
                if apply:
                    ti = {k: tbl[k] for k in _TI_KEYS if k in tbl}
                    ti["StorageDescriptor"]["Location"] = new
                    glue.update_table(DatabaseName=db, TableInput=ti)
    print("\nAPPLIED" if apply else "\nDRY RUN — rerun with --apply to update")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
