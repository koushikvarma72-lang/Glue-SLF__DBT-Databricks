"""Strip embedded spark.sql.sources.* properties from the medaffairs catalog tables.

When a Glue table carries spark.sql.sources.provider / spark.sql.sources.schema.*
parameters, Spark uses THAT embedded schema and ignores the catalog PartitionKeys —
producing "X is not a valid partition column" even when the keys are correct.
Removing the properties makes Spark fall back to Hive-style metadata.

Usage:
    python strip_spark_table_params.py            # dry run
    python strip_spark_table_params.py --apply
"""

import sys

import boto3

DBS = ("medaffairs_silver", "medaffairs_gold")
_TI_KEYS = ("Name", "Description", "Owner", "Retention", "StorageDescriptor",
            "PartitionKeys", "ViewOriginalText", "ViewExpandedText",
            "TableType", "Parameters")


def main(apply: bool) -> None:
    glue = boto3.client("glue")
    for db in DBS:
        for page in glue.get_paginator("get_tables").paginate(DatabaseName=db):
            for tbl in page["TableList"]:
                params = tbl.get("Parameters") or {}
                doomed = [k for k in params if k.startswith("spark.sql.")]
                if not doomed:
                    print(f"  ok    {db}.{tbl['Name']}")
                    continue
                print(f"  strip {db}.{tbl['Name']}: {doomed}")
                if apply:
                    ti = {k: tbl[k] for k in _TI_KEYS if k in tbl}
                    ti["Parameters"] = {k: v for k, v in params.items()
                                        if not k.startswith("spark.sql.")}
                    glue.update_table(DatabaseName=db, TableInput=ti)
    print("\nAPPLIED" if apply else "\nDRY RUN — rerun with --apply")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
