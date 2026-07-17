"""Validate the migrated estate: list every table in bronze/silver/gold with row
counts, so you can see at a glance whether the right tables exist and hold data.

Usage:
    python3 validate_migration.py --host https://dbc-xxxx.cloud.databricks.com \
        --token dapiXXXX --warehouse 66b30eb900bcd97a [--catalog workspace]
"""

import argparse
import json
import time
import urllib.request


def _sql(host, token, warehouse, statement, catalog):
    body = {"statement": statement, "warehouse_id": warehouse,
            "catalog": catalog, "wait_timeout": "30s"}
    req = urllib.request.Request(f"{host.rstrip('/')}/api/2.0/sql/statements",
                                 data=json.dumps(body).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as r:
        out = json.load(r)
    # poll if still running
    while out.get("status", {}).get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        req2 = urllib.request.Request(
            f"{host.rstrip('/')}/api/2.0/sql/statements/{out['statement_id']}")
        req2.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(req2, timeout=60) as r:
            out = json.load(r)
    if out.get("status", {}).get("state") != "SUCCEEDED":
        raise RuntimeError(out.get("status", {}).get("error", {}).get("message", "query failed"))
    return [row for row in (out.get("result", {}).get("data_array") or [])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--token", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--catalog", default="workspace")
    a = ap.parse_args()

    grand = {}
    for schema in ("bronze", "silver", "gold"):
        try:
            tables = _sql(a.host, a.token, a.warehouse,
                          f"SELECT table_name FROM {a.catalog}.information_schema.tables "
                          f"WHERE table_schema = '{schema}' ORDER BY table_name", a.catalog)
        except Exception as exc:  # noqa: BLE001
            print(f"\n== {a.catalog}.{schema}: {exc}")
            continue
        print(f"\n== {a.catalog}.{schema} — {len(tables)} table(s)")
        total = 0
        for (name,) in tables:
            try:
                cnt = int(_sql(a.host, a.token, a.warehouse,
                               f"SELECT COUNT(*) FROM {a.catalog}.{schema}.`{name}`",
                               a.catalog)[0][0])
            except Exception:  # noqa: BLE001
                cnt = -1
            total += max(cnt, 0)
            flag = "" if cnt > 0 else ("  ⚠ EMPTY" if cnt == 0 else "  ⚠ UNREADABLE")
            print(f"  {name:<42} {cnt:>10,}{flag}" if cnt >= 0
                  else f"  {name:<42} {'?':>10}{flag}")
        grand[schema] = (len(tables), total)
    print("\n== summary")
    for schema, (n, rows) in grand.items():
        print(f"  {schema:<8} {n:>3} table(s)   {rows:>12,} row(s)")


if __name__ == "__main__":
    main()
