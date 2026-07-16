#!/usr/bin/env python3
"""Align the reference CDL's control config with the REAL Glue catalog + job contract.

What the investigation found (2026-07-13):
  * query_configuration rows had interface='csv' + key names in priority_column —
    the Glue jobs filter interface='raw_to_curated'/'curated_to_publish' and need
    integer priorities, so every file logged No_Query_Configuration_Available.
  * configuration_master pointed at databases 'silver'/'gold'; the actual Glue
    catalog databases are 'medaffairs_silver'/'medaffairs_gold'.
  * stitching_configuration was empty (curated_to_publish needs 'full' rows).
  * The jobs INSERT OVERWRITE ... PARTITION(batch_id, pt_file_id) into silver and
    PARTITION(data_src_nm) into gold — catalog tables must carry those partition keys.

This script does two things:
  1. (--apply) Repartitions the 12 catalog tables (recreate with the same columns/
     location, moving the partition columns into PartitionKeys).
  2. Always: reads the LIVE table schemas and generates ``seed_pipeline_config.sql``
     — configuration_master fixes, query_configuration rows for both interfaces
     (xlsx headers → silver columns via the known rename rules; silver → gold with
     literals for data_src_nm/audit columns), stitching 'full' rows, and a clean
     reset of the batch/file/ingestion ledgers.

Usage:
  python align_reference_config.py            # plan only: prints actions + writes the SQL
  python align_reference_config.py --apply    # also applies the catalog changes
  psql -h localhost -d control -f seed_pipeline_config.sql
"""

import argparse
import re
import sys

SILVER_DB, GOLD_DB = "medaffairs_silver", "medaffairs_gold"
OBJECTS = {  # source_object_name -> (silver table, gold table)
    "account": ("account", "dim_account"),
    "call": ("call", "fact_call"),
    "call_detail": ("call_detail", "fact_call_detail"),
    "call_discussion": ("call_discussion", "fact_call_discussion"),
    "call_expense": ("call_expense", "fact_call_expense"),
    "call_followup": ("call_followup", "fact_call_followup"),
}
SILVER_PARTS = [("batch_id", "string"), ("pt_file_id", "string")]
GOLD_PARTS = [("data_src_nm", "string")]

# xlsx header -> generic snake mapping (Veeva-style). Special cases first.
_SPECIAL = {"id": "{obj}_id", "name": "{obj}_name"}


def _snake(header: str) -> str:
    h = re.sub(r"_vod__c$|__c$", "", header, flags=re.I)
    # split camelCase only at lower/digit→Upper boundaries so 'ID' stays 'id'
    h = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", h).lower()
    return re.sub(r"__+", "_", h).strip("_")


def _source_expr(silver_col: str, obj: str, headers_by_snake: dict) -> str:
    """The SELECT expression that produces one silver column from the raw view."""
    c = silver_col.lower()
    if c == f"{obj}_id" and "id" in headers_by_snake:
        return f"`{headers_by_snake['id']}` as {silver_col}"
    if c == f"{obj}_name" and "name" in headers_by_snake:
        return f"`{headers_by_snake['name']}` as {silver_col}"
    if c == "full_name":
        f_, l_ = headers_by_snake.get("first_name"), headers_by_snake.get("last_name")
        if f_ and l_:
            return f"concat_ws(' ', `{f_}`, `{l_}`) as {silver_col}"
    if c in headers_by_snake:
        return f"`{headers_by_snake[c]}` as {silver_col}"
    # audit / dq / dwh columns
    if c == "_source_file":
        return f"'{{file_name}}' as {silver_col}"
    if c in ("_batch_id", "dwh_batch_id"):
        return f"'{{batch_id}}' as {silver_col}"
    if c in ("_load_ts", "dwh_silver_ts"):
        return f"cast(current_timestamp() as string) as {silver_col}"
    if c.startswith("dq_"):
        return f"'N' as {silver_col}"
    # variant matches: call_id vs call2, account_id vs account, specialty vs specialty_1
    base = c[:-3] if c.endswith("_id") else c
    for cand in (base, base.rstrip("s"), f"{base}2"):
        if cand in headers_by_snake:
            return f"`{headers_by_snake[cand]}` as {silver_col}"
    for k, h in headers_by_snake.items():
        if k.startswith(c + "_") or c.startswith(k + "_"):
            return f"`{h}` as {silver_col}"
    return f"cast(null as string) as {silver_col}"


def _gold_expr(gold_col: str, silver_cols: set, obj: str) -> str:
    c = gold_col.lower()
    if c in silver_cols:
        return c
    if c == "data_src_nm":
        return f"'{obj}' as data_src_nm"
    if c == "batch_id":
        return "'{batch_id}' as batch_id"
    if c.startswith("dwh_") and c.endswith("_ts"):
        return f"cast(current_timestamp() as string) as {c}"
    if c == "dwh_batch_id":
        return "'{batch_id}' as dwh_batch_id"
    return f"cast(null as string) as {c}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="apply catalog repartitioning")
    ap.add_argument("--out", default="seed_pipeline_config.sql")
    args = ap.parse_args()

    import boto3
    glue = boto3.client("glue")

    def get_cols(db, name):
        t = glue.get_table(DatabaseName=db, Name=name)["Table"]
        sd = t["StorageDescriptor"]
        cols = [(c["Name"], c.get("Type", "string")) for c in sd.get("Columns", [])]
        parts = [(c["Name"], c.get("Type", "string")) for c in t.get("PartitionKeys", [])]
        return t, cols, parts

    def ensure_partitions(db, name, wanted):
        t, cols, parts = get_cols(db, name)
        if [p[0] for p in parts] == [w[0] for w in wanted]:
            print(f"  {db}.{name}: partitions OK {[w[0] for w in wanted]}")
            return cols, parts
        data_cols = [c for c in cols if c[0] not in {w[0] for w in wanted}]
        sd = dict(t["StorageDescriptor"])
        sd["Columns"] = [{"Name": n, "Type": ty} for n, ty in data_cols]
        table_input = {
            "Name": name, "StorageDescriptor": sd,
            "PartitionKeys": [{"Name": n, "Type": ty} for n, ty in wanted],
            "TableType": t.get("TableType", "EXTERNAL_TABLE"),
            "Parameters": t.get("Parameters", {}),
        }
        if args.apply:
            glue.delete_table(DatabaseName=db, Name=name)
            glue.create_table(DatabaseName=db, TableInput=table_input)
            print(f"  {db}.{name}: RECREATED with partitions {[w[0] for w in wanted]}")
        else:
            print(f"  {db}.{name}: WOULD recreate with partitions {[w[0] for w in wanted]} (use --apply)")
        return data_cols, wanted

    # xlsx headers per object — from the landing workbooks (matches the generated
    # stg_* models' source columns).
    HEADERS = {
        "account": ["ID", "Name", "Account_Type_vod__c", "FirstName", "LastName",
                     "Specialty_1_vod__c", "Credentials_vod__c", "Primary_Parent_vod__c"],
        "call": ["Id", "Name", "Account_vod__c", "Call_Date_vod__c", "Status_vod__c",
                  "Call_Channel__c", "Call_Type_vod__c", "Owner_Name", "Territory_vod__c",
                  "Duration_vod__c"],
        "call_detail": ["ID", "Name", "Call2_vod__c", "Product_vod__c",
                         "Product_Name_vod__c", "Detail_Priority_vod__c"],
        "call_discussion": ["ID", "Name", "Call2_vod__c", "Product_vod__c",
                             "Topic_vod__c", "Reaction_vod__c"],
        "call_expense": ["Id", "Name", "Call2_vod__c", "Expense_Type_vod__c",
                          "Amount_vod__c", "Expense_Date_vod__c"],
        "call_followup": ["ID", "Name", "Call2_vod__c", "Activity_Type_vod__c",
                           "Due_date_vod__c", "Product_vod__c"],
    }

    print("== Catalog partition alignment ==")
    silver_schemas, gold_schemas = {}, {}
    for obj, (s_tbl, g_tbl) in OBJECTS.items():
        silver_schemas[obj] = ensure_partitions(SILVER_DB, s_tbl, SILVER_PARTS)
        gold_schemas[obj] = ensure_partitions(GOLD_DB, g_tbl, GOLD_PARTS)

    print("== Generating", args.out, "==")
    sql = ["-- Generated by align_reference_config.py — control config aligned with the",
           "-- live Glue catalog + the Glue jobs' contract. Review, then:",
           "--   psql -h localhost -d control -f seed_pipeline_config.sql",
           "BEGIN;",
           "",
           "-- 1. configuration_master → real catalog names",
           f"UPDATE configuration_master SET curated_database='{SILVER_DB}', publish_database='{GOLD_DB}';"]
    for obj, (s_tbl, g_tbl) in OBJECTS.items():
        sql.append(f"UPDATE configuration_master SET curated_tablename='{s_tbl}', "
                   f"publish_tablename='{g_tbl}' WHERE source_object_name='{obj}';")

    sql += ["", "-- 2. query_configuration — the two interfaces the jobs expect",
            "DELETE FROM query_configuration WHERE interface IN ('raw_to_curated','curated_to_publish');"]
    for obj, (s_tbl, g_tbl) in OBJECTS.items():
        headers = HEADERS[obj]
        hb = {}
        for h in headers:
            hb.setdefault(_snake(h), h)
        s_cols, s_parts = silver_schemas[obj]
        select_silver = ",\n  ".join(
            [_source_expr(n, obj, hb) for n, _ in s_cols]
            + ["'{batch_id}' as batch_id", "cast({file_id} as string) as pt_file_id"])
        q1 = f"select\n  {select_silver}\nfrom {{raw_filename}}"
        sql.append("INSERT INTO query_configuration (source_system, sql_query, domain, sub_domain, "
                   "file_name, interface, source_tablename, target_tablename, priority_column) VALUES "
                   f"('veeva_crm', $qq${q1}$qq$, 'commercial', 'crm', '*.xlsx', 'raw_to_curated', "
                   f"'raw_{obj}', '{s_tbl}', '1');")

        g_cols, g_parts = gold_schemas[obj]
        silver_names = {n.lower() for n, _ in s_cols} | {p[0] for p in SILVER_PARTS}
        select_gold = ",\n  ".join(
            [_gold_expr(n, silver_names, obj) for n, _ in g_cols]
            + [_gold_expr(p[0], silver_names, obj) for p in GOLD_PARTS])
        q2 = (f"select\n  {select_gold}\nfrom {{curated_db}}.{{curated_tbl_name}}\n"
              "where batch_id = '{batch_id}'")
        sql.append("INSERT INTO query_configuration (source_system, sql_query, domain, sub_domain, "
                   "file_name, interface, source_tablename, target_tablename, priority_column) VALUES "
                   f"('veeva_crm', $qq${q2}$qq$, 'commercial', 'crm', '*.xlsx', 'curated_to_publish', "
                   f"'{s_tbl}', '{g_tbl}', '1');")

    sql += ["", "-- 3. stitching_configuration — 'full' overwrite per table",
            "DELETE FROM stitching_configuration WHERE dl_source='veeva_crm';"]
    for obj, (s_tbl, g_tbl) in OBJECTS.items():
        sql.append("INSERT INTO stitching_configuration (dl_source, pt_pattern_name, pattern_name, "
                   "source_database_name, source_table_name, target_database_name, target_table_name, "
                   "stitching_type, primary_keys, order_key, record_load_key, active_flag) VALUES "
                   f"('veeva_crm', '{obj}', '{obj}', '{SILVER_DB}', '{s_tbl}', '{GOLD_DB}', '{g_tbl}', "
                   "'full', '', '', '', 'A');")

    sql += ["", "-- 4. clean ledger so the next run processes from scratch",
            "DELETE FROM dl_ingestion_log;",
            "DELETE FROM file_process_log;",
            "DELETE FROM parent_batch_process WHERE source_system='veeva_crm';",
            "COMMIT;"]

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(sql) + "\n")
    print(f"wrote {args.out} ({len(sql)} statements). Review it, then:")
    print(f"  psql -h localhost -d control -f {args.out}")
    if not args.apply:
        print("NOTE: catalog repartitioning was PLANNED only — rerun with --apply to execute.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
