"""DQ-rule compiler (Phase 3 of the gap plan) — deterministic, pure.

Input: ``dq_rules`` rows introspected from the RDS control DB. Each rule is
classified and compiled to the strongest artifact that can host it:

  column-shape rules  → dbt tests (not_null / unique / accepted_values /
                        relationships / range via dbt_utils expression)
  row-quarantine      → a ``<model>__rejects`` dbt model (the rule inverted),
                        mirroring the legacy landing_reject/raw_reject buckets
  file-level checks   → notebook check specs (they run before dbt exists)
  unclassifiable      → review-queue items (never silently dropped)

Also compiles ``message_template`` rows into Databricks Jobs notification
settings (Phase 3 alerting).
"""

from __future__ import annotations

import re

# rule_type synonyms → canonical classification
_TYPE_SYNONYMS = {
    "not_null": "not_null", "notnull": "not_null", "null_check": "not_null",
    "mandatory": "not_null", "required": "not_null",
    "unique": "unique", "uniqueness": "unique", "duplicate_check": "unique",
    "pk_check": "unique", "primary_key": "unique",
    "accepted_values": "accepted_values", "domain": "accepted_values",
    "value_list": "accepted_values", "picklist": "accepted_values", "lov": "accepted_values",
    "referential": "relationships", "foreign_key": "relationships", "fk_check": "relationships",
    "lookup": "relationships",
    "range": "range", "range_check": "range", "between": "range",
    "min_max": "range", "threshold": "range",
    "regex": "regex", "pattern": "regex", "format": "regex",
    "row_count": "file_check", "record_count": "file_check", "count_check": "file_check",
    "header": "file_check", "header_check": "file_check", "file_format": "file_check",
    "filename": "file_check", "schema_check": "file_check", "freshness": "file_check",
    "custom_sql": "expression", "expression": "expression", "sql": "expression",
    "business_rule": "expression",
}


def _norm(s) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", str(s or "").strip().lower()).strip("_")


def _rows_as_dicts(entry: dict) -> list[dict]:
    cols = [c.get("name") for c in (entry or {}).get("columns") or []]
    return [dict(zip(cols, r)) for r in (entry or {}).get("rows") or []]


def _get(row: dict, *keys):
    low = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        v = low.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def classify_dq_rule(row: dict) -> dict:
    """Classify one dq_rules row. Pure; returns
    {kind, table, column, severity, params, source_row}."""
    rtype = _norm(_get(row, "rule_type", "rule_name", "check_type", "dq_type"))
    kind = _TYPE_SYNONYMS.get(rtype)
    expression = _get(row, "rule_expression", "expression", "rule_sql", "check_sql", "condition")
    if kind is None:
        # fall back on the expression text; a pure comparison → expression rule
        kind = "expression" if expression else "unclassified"
    table = _norm(_get(row, "table_name", "target_table", "object_name", "entity"))
    column = _get(row, "column_name", "column", "field_name", "attribute")
    severity = (_get(row, "severity", "criticality", "action") or "error").lower()
    severity = {"critical": "error", "fail": "error", "hard": "error",
                "warn": "warn", "warning": "warn", "soft": "warn",
                "quarantine": "quarantine", "reject": "quarantine"}.get(severity, severity)
    params: dict = {"expression": expression}
    if kind == "accepted_values":
        vals = _get(row, "accepted_values", "value_list", "values", "domain_values") or expression
        params["values"] = [v.strip() for v in re.split(r"[,;|]", vals) if v.strip()]
    if kind == "range":
        params["min"] = _get(row, "min_value", "minimum", "lower_bound")
        params["max"] = _get(row, "max_value", "maximum", "upper_bound")
    if kind == "relationships":
        params["ref_table"] = _norm(_get(row, "ref_table", "reference_table", "lookup_table",
                                         "parent_table"))
        params["ref_column"] = _get(row, "ref_column", "reference_column", "lookup_column",
                                    "parent_column")
    return {"kind": kind, "table": table, "column": column, "severity": severity,
            "params": params, "source_row": row}


def _dbt_test_for(rule: dict):
    """The dbt test dict (or None) for a classified column rule."""
    kind, col, p, sev = rule["kind"], rule["column"], rule["params"], rule["severity"]
    cfg = {"severity": "warn"} if sev == "warn" else {}
    if kind == "not_null":
        return {"not_null": {"config": cfg}} if cfg else "not_null"
    if kind == "unique":
        return {"unique": {"config": cfg}} if cfg else "unique"
    if kind == "accepted_values" and p.get("values"):
        t = {"accepted_values": {"values": p["values"]}}
        if cfg:
            t["accepted_values"]["config"] = cfg
        return t
    if kind == "relationships" and p.get("ref_table") and p.get("ref_column"):
        t = {"relationships": {"to": f"ref('{p['ref_table']}')", "field": p["ref_column"]}}
        if cfg:
            t["relationships"]["config"] = cfg
        return t
    if kind == "range" and (p.get("min") or p.get("max")):
        parts = []
        if p.get("min"):
            parts.append(f">= {p['min']}")
        if p.get("max"):
            parts.append(f"<= {p['max']}")
        t = {"dbt_utils.accepted_range": {}}
        if p.get("min"):
            t["dbt_utils.accepted_range"]["min_value"] = p["min"]
        if p.get("max"):
            t["dbt_utils.accepted_range"]["max_value"] = p["max"]
        if cfg:
            t["dbt_utils.accepted_range"]["config"] = cfg
        return t
    if kind == "regex" and p.get("expression"):
        t = {"dbt_utils.not_empty_string": {}} if not p["expression"] else None
        # regex → expression_is_true on RLIKE (portable to Databricks SQL)
        return {"dbt_utils.expression_is_true": {
            "expression": f"{col} RLIKE '{p['expression']}'", **({"config": cfg} if cfg else {})}}
    return None


def _yaml_test(t, indent: str) -> list[str]:
    """Render one dbt test (str or single-key dict) as YAML lines."""
    if isinstance(t, str):
        return [f"{indent}- {t}"]
    (name, body), = t.items()
    lines = [f"{indent}- {name}:"]
    for k, v in body.items():
        if isinstance(v, list):
            lines.append(f"{indent}    {k}: [{', '.join(repr(x) for x in v)}]")
        elif isinstance(v, dict):
            lines.append(f"{indent}    {k}:")
            lines.extend(f"{indent}      {kk}: {vv}" for kk, vv in v.items())
        else:
            lines.append(f"{indent}    {k}: {v}")
    return lines


def compile_dq_rules(dq_entry: dict, *, known_models: list | None = None) -> dict:
    """Compile all dq_rules rows.

    Returns {dq_schema_yml, quarantine_models: {fname: sql}, notebook_checks: [...],
    unclassified: [...], summary: {...}}. ``known_models`` (base names) lets table
    references resolve to ref() vs source('bronze', …).
    """
    rules = [classify_dq_rule(r) for r in _rows_as_dicts(dq_entry)]
    known = {_norm(m) for m in known_models or []}

    by_model: dict = {}
    quarantine: dict = {}
    notebook_checks: list = []
    unclassified: list = []

    for rule in rules:
        tbl = rule["table"]
        if rule["kind"] == "file_check":
            notebook_checks.append({
                "table": tbl, "check": _get(rule["source_row"], "rule_type", "rule_name"),
                "expression": rule["params"].get("expression", ""),
                "severity": rule["severity"],
            })
            continue
        if rule["kind"] == "expression" or rule["severity"] == "quarantine":
            expr = rule["params"].get("expression")
            if tbl and expr:
                ref = (f"{{{{ ref('{tbl}') }}}}" if tbl in known
                       else f"{{{{ source('bronze', '{tbl}') }}}}")
                quarantine[f"{tbl}__rejects.sql"] = (
                    f"-- quarantine model generated from dq_rules (legacy reject-bucket pattern)\n"
                    "{{ config(materialized='table') }}\n\n"
                    f"-- rows FAILING the rule: {expr}\n"
                    f"select *, current_timestamp() as _dq_rejected_at,\n"
                    f"       '{_get(rule['source_row'], 'rule_name', 'rule_type') or 'dq_rule'}' as _dq_rule\n"
                    f"from {ref}\nwhere not ({expr})\n"
                )
            else:
                unclassified.append(rule["source_row"])
            continue
        if rule["kind"] == "unclassified" or not (tbl and rule["column"]):
            unclassified.append(rule["source_row"])
            continue
        test = _dbt_test_for(rule)
        if test is None:
            unclassified.append(rule["source_row"])
            continue
        by_model.setdefault(tbl, {}).setdefault(rule["column"], []).append(test)

    lines = ["# dq_schema.yml — dbt tests compiled from the legacy dq_rules control table.",
             "# Review, then merge into models/schema.yml (dbt allows one properties entry",
             "# per model, so these are emitted separately for the reviewer to fold in).",
             "version: 2", "", "models:"]
    for model in sorted(by_model):
        lines.append(f"  - name: {model}")
        lines.append("    columns:")
        for col in sorted(by_model[model]):
            lines.append(f"      - name: {col}")
            lines.append("        tests:")
            for t in by_model[model][col]:
                lines.extend(_yaml_test(t, "          "))
    dq_yaml = "\n".join(lines) + "\n" if by_model else ""

    return {
        "dq_schema_yml": dq_yaml,
        "quarantine_models": quarantine,
        "notebook_checks": notebook_checks,
        "unclassified": unclassified,
        "summary": {
            "total": len(rules),
            "dbt_tests": sum(len(ts) for cols in by_model.values() for ts in cols.values()),
            "quarantine_models": len(quarantine),
            "notebook_checks": len(notebook_checks),
            "unclassified": len(unclassified),
        },
    }


# ─── message_template → Databricks Jobs notifications (Phase 3 alerting) ─────

def compile_notifications(mt_entry: dict) -> dict:
    """message_template rows → {email_notifications, templates, skipped}.

    Emails found in recipient-ish columns land in the Jobs API
    ``email_notifications`` block; the template bodies are preserved (they move
    into the control schema with the rest of the framework data).
    """
    rows = _rows_as_dicts(mt_entry)
    emails: set = set()
    templates: list = []
    for row in rows:
        rec = _get(row, "recipients", "recipient", "to_address", "email", "notify_to", "dl_list")
        found = re.findall(r"[\w.+-]+@[\w-]+\.[\w.]+", rec)
        emails.update(found)
        templates.append({
            "name": _get(row, "template_name", "name", "notification_type") or "template",
            "subject": _get(row, "subject", "email_subject"),
            "recipients": found,
        })
    return {
        "email_notifications": {
            "on_failure": sorted(emails),
            "on_success": [],
        } if emails else {},
        "templates": templates,
    }
