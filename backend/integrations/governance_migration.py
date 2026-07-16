"""Governance migration (Phase 7 of the gap plan) — Lake Formation → Unity Catalog.

Deterministic mapping, DIFF-ONLY by design: this module produces the GRANT
script + a principal-mapping worksheet; it never applies grants. Applying is a
separate, explicitly-confirmed action (an access outage is worse than a manual
step). Principal mapping (IAM role/user → UC account group) is inherently a
human decision, so unmapped principals are emitted as commented-out grants.
"""

from __future__ import annotations

import re

# LF permission → UC privilege(s) on tables/schemas.
_PERM_MAP = {
    "SELECT": ["SELECT"],
    "DESCRIBE": ["BROWSE"],
    "INSERT": ["MODIFY"],
    "DELETE": ["MODIFY"],
    "ALTER": ["MODIFY"],
    "DROP": [],           # destructive — never auto-granted; surfaced as a review line
    "ALL": ["ALL PRIVILEGES"],
    "SUPER": [],          # admin — review line
    "CREATE_TABLE": ["CREATE TABLE"],
    "CREATE_DATABASE": ["CREATE SCHEMA"],
    "DATA_LOCATION_ACCESS": [],  # storage credential concern — review line
}


def list_lakeformation_permissions(glue_config) -> dict:
    """Pull LF permissions with the existing Glue session plumbing (boto3).

    Returns {success, permissions: [{principal, resource_type, database, table,
    columns, permissions[]}]}. Live call — kept thin; mapping is pure below.
    """
    from backend.integrations.glue_client import _aws_session, validate_config
    errors = validate_config(glue_config)
    if errors:
        return {"success": False, "error": " ".join(errors)}
    try:
        with _aws_session(glue_config) as session:
            lf = session.client("lakeformation")
        perms, token = [], None
        while True:
            kwargs = {"NextToken": token} if token else {}
            page = lf.list_permissions(**kwargs)
            for p in page.get("PrincipalResourcePermissions", []):
                res = p.get("Resource") or {}
                table = res.get("Table") or res.get("TableWithColumns") or {}
                db = res.get("Database") or {}
                perms.append({
                    "principal": (p.get("Principal") or {}).get("DataLakePrincipalIdentifier", ""),
                    "resource_type": ("table" if table else "database" if db else "other"),
                    "database": table.get("DatabaseName") or db.get("Name") or "",
                    "table": table.get("Name") or ("*" if table.get("TableWildcard") is not None else ""),
                    "columns": (res.get("TableWithColumns") or {}).get("ColumnNames") or [],
                    "permissions": p.get("Permissions") or [],
                })
            token = page.get("NextToken")
            if not token:
                break
        return {"success": True, "permissions": perms}
    except Exception as exc:  # noqa: BLE001
        return {"success": False, "error": f"Lake Formation listing failed: {exc}"}


def _uc_name(part: str) -> str:
    return "`" + re.sub(r"[^\w]", "_", str(part or "")) + "`"


def map_permissions_to_uc_grants(permissions: list[dict], *, catalog: str,
                                 principal_map: dict | None = None) -> dict:
    """LF permissions → UC GRANT SQL. Pure.

    ``principal_map``: {IAM arn/name → UC group/user}. Unmapped principals produce
    COMMENTED grants + an entry in ``unmapped_principals`` (the review worksheet).
    Column-restricted LF grants become full-table grants + a review line (UC
    column masks are a manual design decision, not an auto-translation).

    Returns {grants_sql, unmapped_principals, review_lines, stats}.
    """
    pmap = principal_map or {}
    lines, review, unmapped = [], [], {}
    granted = 0
    for p in permissions or []:
        principal = p.get("principal") or ""
        uc_principal = pmap.get(principal)
        target = (f"{_uc_name(catalog)}.{_uc_name(p['database'])}"
                  if p.get("resource_type") == "database" or p.get("table") in ("", "*")
                  else f"{_uc_name(catalog)}.{_uc_name(p['database'])}.{_uc_name(p['table'])}")
        kind = "SCHEMA" if ".`" not in target[len(_uc_name(catalog)) + 1:] else "TABLE"
        kind = "SCHEMA" if p.get("resource_type") == "database" or p.get("table") in ("", "*") else "TABLE"
        if p.get("columns"):
            review.append(f"-- REVIEW: LF column-level grant on {target} "
                          f"(columns: {', '.join(p['columns'])}) for {principal} — "
                          "translated to a full-table grant; add a UC column mask if needed.")
        for lf_perm in p.get("permissions") or []:
            uc_privs = _PERM_MAP.get(str(lf_perm).upper())
            if uc_privs is None:
                review.append(f"-- REVIEW: unknown LF permission {lf_perm!r} on {target} "
                              f"for {principal}")
                continue
            if not uc_privs:
                review.append(f"-- REVIEW: LF {lf_perm} on {target} for {principal} — "
                              "no safe UC auto-translation (destructive/admin/storage).")
                continue
            for priv in uc_privs:
                stmt = f"GRANT {priv} ON {kind} {target} TO {_uc_name(uc_principal)};" \
                    if uc_principal else \
                    f"-- UNMAPPED PRINCIPAL: GRANT {priv} ON {kind} {target} TO `<map: {principal}>`;"
                lines.append(stmt)
                if uc_principal:
                    granted += 1
                else:
                    unmapped.setdefault(principal, 0)
                    unmapped[principal] += 1

    header = [
        "-- Unity Catalog grants generated from Lake Formation permissions (sfglue Phase 7).",
        "-- DIFF-ONLY: review, map principals, then apply manually or via CI.",
        "-- Unmapped principals are commented out — fill the principal map and re-plan.",
        "",
    ]
    return {
        "grants_sql": "\n".join(header + sorted(set(review)) + [""] + lines) + "\n",
        "unmapped_principals": unmapped,
        "review_lines": review,
        "stats": {"grants": granted, "commented": sum(unmapped.values()),
                  "review_items": len(review)},
    }
