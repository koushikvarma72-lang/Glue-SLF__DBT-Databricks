"""Operational lineage — fuse orchestration + control-plane + catalog into ONE graph.

The plain dataflow lineage (``snowflake_glue_lineage.build_lineage``) parses Glue job
*scripts*. For a config-driven pipeline the scripts are generic engines, so script
parsing yields almost no per-table edges — the real mapping lives in the RDS control
rows. This builder fuses four already-introspected sources into a typed, laned graph:

  * orchestration — the job chain + dependencies (``parse_workflow_dag`` output);
  * control plane — the framework tables + rows (``introspect_framework_tables``);
  * catalog       — Glue catalog + Snowflake tables (the data-table nodes);
  * (optional) review flags per job.

EVERYTHING is derived generically. No job name, table name, or chain is hardcoded:
control tables are referenced by their ``canonical`` role; source/target/interface
columns are picked from candidate lists; data + control edges are drawn only where a
config-row value actually matches a discovered job or catalog table (evidence-based —
never invents an edge). Works for any config-driven Glue estate, not just the demo.

Pure and unit-tested.
"""

from __future__ import annotations

import re

# Column-candidate lists (role-based, not instance-specific).
_SRC_COLS = ("source_tablename", "source_table_name", "source_table", "src_table",
             "source_object_name", "curated_tablename", "raw_tablename", "from_table")
_TGT_COLS = ("target_tablename", "target_table_name", "target_table", "tgt_table",
             "publish_tablename", "curated_tablename", "to_table")
_IFACE_COLS = ("interface", "job", "job_name", "step", "step_name", "stage", "phase")
_SQL_COLS = ("sql_query", "query_text", "query_sql", "sql_text", "query")
_SNFK_COLS = ("snowflake_table", "target_table", "snowflake_schema")

# canonical roles that are "ledger / audit" (written during a run) vs "config" (read).
_LEDGER_ROLES = ("batch", "log", "ingestion", "audit")


def _base(name: str) -> str:
    """Last dotted segment, lowercased, non-alnum → '_' (matches postgres_client)."""
    s = str(name or "").strip().strip('"').split(".")[-1]
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def _pick(cols: list[str], candidates) -> str | None:
    low = {c.lower(): c for c in cols}
    for cand in candidates:
        if cand in low:
            return low[cand]
    # loose contains-match as a fallback
    for cand in candidates:
        for lc, orig in low.items():
            if cand in lc:
                return orig
    return None


def _row_cells(entry: dict) -> list[dict]:
    cols = [c.get("name") if isinstance(c, dict) else c for c in entry.get("columns", [])]
    out = []
    for r in entry.get("rows", []) or []:
        out.append({cols[i]: r[i] for i in range(min(len(cols), len(r)))})
    return out


def build_operational_lineage(workflow_dag: dict | None = None,
                              framework_tables: list[dict] | None = None,
                              glue_tables: list[dict] | None = None,
                              snowflake_objects: dict | None = None,
                              job_flags: dict | None = None) -> dict:
    """Fuse orchestration + control + catalog → {nodes, edges, jobs, health, lanes}.

    Node types: ``source`` | ``bronze`` | ``silver`` | ``gold`` (data tables),
    ``job`` (Glue job), ``control`` (RDS framework table).
    Edge kinds: ``execution`` (job→job), ``control`` (job↔control table),
    ``data`` (table→job→table), ``replicate`` (gold→Snowflake).
    """
    dag = workflow_dag or {}
    framework_tables = framework_tables or []
    glue_tables = glue_tables or []
    snowflake_objects = snowflake_objects or {}
    job_flags = job_flags or {}

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    by_base: dict[str, list[str]] = {}

    def add(node_id, label, ntype, **extra):
        if node_id not in nodes:
            nodes[node_id] = {"id": node_id, "label": label, "type": ntype, **extra}
            by_base.setdefault(_base(label), []).append(node_id)
        return node_id

    def add_edge(frm, to, kind, label=""):
        if frm and to and frm != to:
            edges.append({"from": frm, "to": to, "kind": kind, "label": label})

    def _layer(full_name: str) -> str:
        f = full_name.lower()
        if any(k in f for k in ("gold", "publish", "mart", "_dim", ".dim", "_fct", ".fct", "fact")):
            return "gold"
        if any(k in f for k in ("silver", "curated", "cleansed", "conform")):
            return "silver"
        if any(k in f for k in ("bronze", "raw", "landing", "stage", "staging")):
            return "bronze"
        return "silver"

    # ── catalog data-table nodes (Glue + Snowflake) ──────────────────────────
    for t in glue_tables:
        fn = t.get("full_name") or t.get("name") or ""
        if fn:
            add(f"tbl:{_base(fn)}", fn, _layer(fn), system="glue",
                columns=len(t.get("columns") or []), location=t.get("location") or "")
    for t in (snowflake_objects.get("tables", []) or []):
        fn = t.get("full_name") or ""
        if fn:
            add(f"sf:{_base(fn)}", fn, "gold", system="snowflake",
                columns=len(t.get("columns") or []))

    def _infer_layer_from_name(name: str, fallback: str) -> str:
        """Layer a config-referenced (not-in-catalog) table by its name, so raw_*/
        *.xlsx land in bronze/source and stg_* in silver — not the caller's default."""
        n = str(name or "").lower()
        if n.endswith((".xlsx", ".xls", ".csv", ".json", ".parquet")) or "landing" in n:
            return "source"
        if n.startswith(("raw_", "raw.")) or "/raw" in n or "_raw" in n or "bronze" in n:
            return "bronze"
        if n.startswith("stg_") or "staging" in n or "curated" in n or "silver" in n:
            return "silver"
        if n.startswith(("dim_", "fct_", "fact_", "mart_", "pub_")) or "gold" in n or "publish" in n:
            return "gold"
        return fallback

    def resolve_table(name, *, create_layer=None):
        """Map a config value to an existing catalog node by base name (evidence-based).
        Creates a node only when create_layer is given (for genuinely-referenced tables)."""
        if not name:
            return None
        b = _base(name)
        cands = by_base.get(b, [])
        # prefer a glue/data node over a job/control node of the same base
        for nid in cands:
            if nodes[nid]["type"] in ("bronze", "silver", "gold", "source"):
                return nid
        if create_layer:
            layer = _infer_layer_from_name(name, create_layer)
            return add(f"ref:{b}", name, layer, system="config-ref", inferred=True)
        return None

    # ── jobs from the orchestration DAG (generic chain) ──────────────────────
    tasks = dag.get("tasks") or []
    job_ids: dict[str, str] = {}   # task key -> node id
    job_by_legacy: dict[str, str] = {}
    for t in tasks:
        key = t.get("key") or t.get("legacy_name") or ""
        if not key:
            continue
        legacy = t.get("legacy_name") or key
        jid = add(f"job:{_base(legacy)}", legacy, "job", kind=t.get("kind") or "",
                  task_key=key, flags=list(job_flags.get(legacy, []) or job_flags.get(key, [])))
        job_ids[key] = jid
        job_by_legacy[_base(legacy)] = jid
    # execution edges (depends_on → task)
    for t in tasks:
        jid = job_ids.get(t.get("key"))
        for up in t.get("depends_on") or []:
            add_edge(job_ids.get(up), jid, "execution")

    def match_job(value) -> str | None:
        """Map a config cell value to a discovered job node, generically."""
        if not value:
            return None
        b = _base(value)
        if b in job_by_legacy:
            return job_by_legacy[b]
        for lb, jid in job_by_legacy.items():   # containment either way
            if lb and (lb in b or b in lb):
                return jid
        return None

    # ── control-plane nodes + evidence-based control/data edges ──────────────
    jobs_detail: dict[str, dict] = {
        nid: {"id": nid, "label": nodes[nid]["label"], "reads": set(), "writes": set(),
              "control_tables": set(), "config_samples": [],
              "flags": nodes[nid].get("flags", [])}
        for nid in job_ids.values()
    }
    health: list[dict] = []

    for entry in framework_tables:
        canon = entry.get("canonical") or _base(entry.get("name", ""))
        cid = add(f"ctl:{_base(entry.get('name',''))}", entry.get("name", canon),
                  "control", canonical=canon, rows=entry.get("row_count", 0))
        cells = _row_cells(entry)
        cols = [c.get("name") if isinstance(c, dict) else c for c in entry.get("columns", [])]
        src_c, tgt_c = _pick(cols, _SRC_COLS), _pick(cols, _TGT_COLS)
        iface_c, sql_c = _pick(cols, _IFACE_COLS), _pick(cols, _SQL_COLS)
        is_ledger = any(k in canon for k in _LEDGER_ROLES)

        # data edges from config rows: source → [job] → target
        for row in cells:
            job_nid = match_job(row.get(iface_c)) if iface_c else None
            s_nid = resolve_table(row.get(src_c), create_layer="silver") if src_c else None
            t_nid = resolve_table(row.get(tgt_c), create_layer="gold") if tgt_c else None
            if s_nid and t_nid:
                if job_nid:
                    add_edge(s_nid, job_nid, "data")
                    add_edge(job_nid, t_nid, "data")
                    jobs_detail[job_nid]["reads"].add(nodes[s_nid]["label"])
                    jobs_detail[job_nid]["writes"].add(nodes[t_nid]["label"])
                    if sql_c and row.get(sql_c) and len(jobs_detail[job_nid]["config_samples"]) < 3:
                        jobs_detail[job_nid]["config_samples"].append(
                            {"table": entry.get("name"), "source": row.get(src_c),
                             "target": row.get(tgt_c), "sql": str(row.get(sql_c))[:400]})
                else:
                    add_edge(s_nid, t_nid, "data", label=canon)
            # health: referenced table with no catalog match
            for col, val in ((src_c, row.get(src_c) if src_c else None),
                             (tgt_c, row.get(tgt_c) if tgt_c else None)):
                if val and resolve_table(val) is None:
                    health.append({"severity": "warn", "kind": "missing_table",
                                   "detail": f"{entry.get('name')} references '{val}' — no matching catalog table"})

        # control edges: a job whose name appears in this table's rows uses it.
        linked = set()
        for row in cells:
            for v in row.values():
                jid = match_job(v)
                if jid:
                    linked.add(jid)
        for jid in linked:
            add_edge(jid, cid, "control", label="reads" if not is_ledger else "writes")
            jobs_detail[jid]["control_tables"].add(canon)

        # replication edges (snowflake_replicate-style tables)
        if "snowflake" in canon or "replicat" in canon:
            snfk_c = _pick(cols, _SNFK_COLS)
            for row in cells:
                s_nid = resolve_table(row.get(src_c)) if src_c else None
                sf_name = row.get(snfk_c) if snfk_c else None
                sf_nid = resolve_table(sf_name) if sf_name else None
                if s_nid and sf_nid:
                    add_edge(s_nid, sf_nid, "replicate", label="→ snowflake")

    # ── generic health checks ────────────────────────────────────────────────
    # orphan data tables: exist but neither produced (data-in) nor read (data-out)
    touched = set()
    for e in edges:
        if e["kind"] in ("data", "replicate"):
            touched.add(e["from"]); touched.add(e["to"])
    for nid, n in nodes.items():
        if n["type"] in ("bronze", "silver", "gold") and nid not in touched:
            health.append({"severity": "info", "kind": "orphan_table",
                           "detail": f"{n['label']} — in catalog but no config row connects it"})
    # jobs with no logic found (no data + no control link)
    for jid, det in jobs_detail.items():
        if not det["reads"] and not det["writes"] and not det["control_tables"]:
            health.append({"severity": "info", "kind": "job_no_logic",
                           "detail": f"{det['label']} — no config rows or catalog I/O attributed"})
    # broken chain: a depends_on referencing an unknown task
    known = {t.get("key") for t in tasks}
    for t in tasks:
        for up in t.get("depends_on") or []:
            if up not in known:
                health.append({"severity": "warn", "kind": "broken_chain",
                               "detail": f"{t.get('key')} depends on unknown task '{up}'"})

    jobs_out = []
    for jid, det in jobs_detail.items():
        jobs_out.append({
            "id": jid, "label": det["label"],
            "reads": sorted(det["reads"]), "writes": sorted(det["writes"]),
            "control_tables": sorted(det["control_tables"]),
            "config_samples": det["config_samples"], "flags": det["flags"],
        })
    jobs_out.sort(key=lambda j: j["label"])

    uniq = {(e["from"], e["to"], e["kind"], e["label"]): e for e in edges}
    return {
        "nodes": list(nodes.values()),
        "edges": list(uniq.values()),
        "jobs": jobs_out,
        "health": health,
        "lanes": {
            "control": [n["id"] for n in nodes.values() if n["type"] == "control"],
            "jobs": [n["id"] for n in nodes.values() if n["type"] == "job"],
            "data": [n["id"] for n in nodes.values() if n["type"] in ("source", "bronze", "silver", "gold")],
        },
        "counts": {
            "jobs": len(job_ids), "control_tables": len(framework_tables),
            "data_tables": sum(1 for n in nodes.values() if n["type"] in ("bronze", "silver", "gold")),
            "edges": len(uniq), "health": len(health),
        },
    }
