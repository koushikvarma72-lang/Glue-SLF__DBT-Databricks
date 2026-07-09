"""Lineage engine for the Snowflake/Glue → Databricks/DBT flow.

Pure, dependency-light functions (no live connections) that turn introspected
metadata into:

  * a unified source→Snowflake dataflow graph in the {nodes, edges} shape the
    frontend LineageGraph component already renders, and
  * duplicate-table / overlapping-business-logic findings plus consolidation
    recommendations (deterministic baseline, optionally enriched by call_ai).

Node ``type`` values reuse LineageGraph's medallion palette:
  source = external input (S3/file)        bronze = Glue catalog table
  silver = Snowflake base table            gold   = Snowflake view

sqlglot is used to parse Snowflake view definitions when available, with a regex
fallback so the module works even if sqlglot isn't installed.
"""

from __future__ import annotations

import json
import logging
import re

logger = logging.getLogger(__name__)


# ─── identifier helpers ──────────────────────────────────────────────────────

def _strip_quotes(token: str) -> str:
    return (token or "").strip().strip('"').strip("`").strip("'").strip()


def _norm(identifier: str) -> str:
    """Normalize a dotted identifier: strip quotes per-part, lowercase."""
    parts = [_strip_quotes(p) for p in str(identifier or "").split(".") if _strip_quotes(p)]
    return ".".join(parts).lower()


def _base_name(identifier: str) -> str:
    """Last dotted segment, normalized — used to match the 'same' table across systems."""
    norm = _norm(identifier)
    return norm.split(".")[-1] if norm else ""


def _short_label(identifier: str) -> str:
    """A compact display label for a graph node — the original-cased last 1-2 parts.

    The full database-qualified name (e.g. SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.CUSTOMER)
    is too long for a node box and all such nodes look identical when truncated. This
    keeps the identifying tail (schema.table / db.table), or the trailing path segment.
    """
    text = str(identifier or "").strip()
    if text.startswith("s3://") or text.startswith("s3a://"):
        parts = [p for p in text.rstrip("/").split("/") if p]
        return parts[-1] if parts else text
    parts = [p for p in text.split(".") if p]
    return ".".join(parts[-2:]) if len(parts) >= 2 else (parts[-1] if parts else text)


# ─── SQL dependency parsing (Snowflake view definitions) ─────────────────────

_FROM_JOIN_RE = re.compile(
    r"\b(?:from|join)\s+([a-zA-Z_][\w$]*(?:\.[a-zA-Z_\"`][\w$\"`]*){0,2})",
    re.IGNORECASE,
)
# CTE names introduced by a WITH clause (`name AS (`) — excluded from deps in the
# regex fallback so a CTE alias isn't mistaken for a source table.
_CTE_NAME_RE = re.compile(r"([a-zA-Z_][\w$]*)\s+as\s*\(", re.IGNORECASE)


def parse_sql_dependencies(sql: str) -> list[str]:
    """Return the source tables/views referenced by a SQL statement.

    Tries sqlglot first (accurate, ignores CTE aliases and string literals); falls
    back to a FROM/JOIN regex when sqlglot isn't importable or fails to parse.
    """
    sql = sql or ""
    if not sql.strip():
        return []
    try:
        import sqlglot  # type: ignore
        from sqlglot import exp  # type: ignore

        parsed = sqlglot.parse_one(sql, read="snowflake")
        cte_names = {_norm(c.alias_or_name) for c in parsed.find_all(exp.CTE)}
        deps = []
        seen = set()
        for tbl in parsed.find_all(exp.Table):
            name = ".".join(p for p in [tbl.catalog, tbl.db, tbl.name] if p)
            norm = _norm(name)
            if not norm or norm in cte_names or norm in seen:
                continue
            seen.add(norm)
            deps.append(norm)
        return deps
    except Exception:  # noqa: BLE001 — any sqlglot import/parse issue → regex fallback
        cte_names = {_norm(m) for m in _CTE_NAME_RE.findall(sql)} if re.search(r"\bwith\b", sql, re.IGNORECASE) else set()
        deps, seen = [], set()
        for m in _FROM_JOIN_RE.finditer(sql):
            norm = _norm(m.group(1))
            if norm and norm not in seen and norm not in cte_names:
                seen.add(norm)
                deps.append(norm)
        return deps


# ─── Glue PySpark / Spark-SQL I/O parsing ────────────────────────────────────

# Glue DynamicFrame catalog read:  create_dynamic_frame.from_catalog(database="d", table_name="t")
_GLUE_CATALOG_RE = re.compile(
    r"database\s*=\s*['\"]([^'\"]+)['\"][^)]*?table_name\s*=\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE | re.DOTALL,
)
# Glue catalog sink:  .setCatalogInfo(catalogDatabase="d", catalogTableName="t")
_GLUE_SINK_RE = re.compile(
    r"catalogDatabase\s*=\s*['\"]([^'\"]+)['\"][^)]*?catalogTableName\s*=\s*['\"]([^'\"]+)['\"]",
    re.IGNORECASE | re.DOTALL,
)
_SPARK_READ_TABLE_RE = re.compile(r"\.(?:read\s*\.\s*)?table\(\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
_SPARK_SAVE_AS_RE = re.compile(r"\.saveAsTable\(\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
_SPARK_INSERT_INTO_RE = re.compile(r"\.insertInto\(\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
_LOAD_PATH_RE = re.compile(r"\.(?:load|parquet|csv|json|orc)\(\s*['\"](s3[a-z]?://[^'\"]+)['\"]", re.IGNORECASE)
_SAVE_PATH_RE = re.compile(r"\.save\(\s*['\"](s3[a-z]?://[^'\"]+)['\"]", re.IGNORECASE)
_SPARK_SQL_RE = re.compile(r"(?:spark|glueContext\.spark_session|sqlContext)\.sql\(\s*['\"]{1,3}(.+?)['\"]{1,3}\s*\)", re.IGNORECASE | re.DOTALL)
_SQL_WRITE_RE = re.compile(r"\b(?:insert\s+into|insert\s+overwrite(?:\s+table)?|create\s+(?:or\s+replace\s+)?table(?:\s+if\s+not\s+exists)?)\s+([a-zA-Z_][\w$]*(?:\.[a-zA-Z_\"`][\w$\"`]*){0,2})", re.IGNORECASE)
# Snowflake / JDBC connector table reference: .option("dbtable","CRM.CALL"),
# connection_options={"dbtable":"CRM.CALL"}, .options(dbtable="CRM.CALL"), sfTable, etc.
# This is how Glue jobs read from / write to a Snowflake table (the destination).
_DBTABLE_RE = re.compile(r"(?:dbtable|sftable|sffulltablename)\s*['\"]?\s*[,:=]\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)
# Any S3 path string literal (incl. inside f-strings, e.g. f"s3://{bucket}/landing/").
_S3_LITERAL_RE = re.compile(r"['\"](s3a?://[^'\"]+)['\"]", re.IGNORECASE)
# A file-ingestion read with a non-literal path the other regexes miss: pandas
# read_*, Spark format().load/csv/json/parquet/excel, Glue from_options (S3/file).
_FILE_READ_RE = re.compile(
    r"(?:\bpd\b|\bpandas\b)\s*\.\s*read_\w+\s*\(|create_dynamic_frame\.from_options\s*\(|"
    r"spark\s*\.\s*read[\s\S]{0,80}?\.\s*(?:load|csv|json|parquet|orc|text|excel)\s*\(",
    re.IGNORECASE)
# Any DataFrame/DynamicFrame write op (the path/target may be a variable).
_WRITE_OP_RE = re.compile(r"\.write\s*(?:Stream)?\s*\.|\.write\b|write_dynamic_frame", re.IGNORECASE)
# The "map source files to target table names" ingestion idiom:
#   {"Accounts.xlsx": "account", "Calls.csv": "call_detail", ...}
# The dict VALUES are the bronze tables the job lands.
_FILE_TABLE_MAP_RE = re.compile(
    r"['\"][^'\"]+\.(?:xlsx|xls|csv|parquet|json|txt|tsv|avro)['\"]\s*:\s*['\"]([A-Za-z_][\w]*)['\"]",
    re.IGNORECASE)
# Path keywords that mark an S3 location as an output (write) vs an input (read).
_OUT_PATH_KW = ("processed", "output", "curated", "stage", "staging", "bronze",
                "silver", "gold", "refined", "conformed", "clean", "target", "sink", "dest")
_IN_PATH_KW = ("landing", "raw", "input", "inbound", "source", "incoming", "drop", "ingest", "src")
# Layer keywords used to collapse a *templated* S3 path (e.g. f"s3://{b}/{silver}/{t}/")
# down to a single layer node (s3://silver) so loop-driven jobs still connect.
_S3_LAYER_KW = ("landing", "raw", "bronze", "staging", "stage", "silver", "conformed",
                "gold", "mart", "curated", "processed", "refined", "serving",
                "publish", "published", "presentation")
# Internal *curated* lakehouse layers. A job that reads or writes one of these is
# deriving conformed/serving data from the lake itself — a transformation/load, not
# raw ingestion — even when its S3 path looks "external" to the path heuristics.
# (Reading bronze is handled separately: consuming bronze ⇒ building silver.)
_CURATED_LAYER_KW = ("silver", "gold", "mart", "curated", "refined", "serving", "conformed",
                     "publish", "published", "presentation")


def _s3_layer_token(path: str) -> str:
    low = path.lower()
    for kw in _S3_LAYER_KW:
        if kw in low:
            return kw
    return ""


def parse_pyspark_io(script_text: str) -> dict:
    """Extract {reads, writes, external} table/path identifiers from a Glue/PySpark script.

    Covers the common idioms: Glue DynamicFrame from_catalog / catalog sinks,
    spark.read.table / saveAsTable / insertInto, format().load/save on S3 paths,
    Snowflake/JDBC connector tables (the ``dbtable`` option — read vs write decided
    by a nearby ``write``/``sink``), embedded spark.sql(...) statements
    (INSERT/CREATE → write, FROM/JOIN → read), plus file-ingestion jobs that read
    files (pandas/Spark/Glue) and write Parquet/Delta to variable S3 paths — their
    bronze targets are recovered from a file→table-name map when present. ``external``
    is True when the job pulls from outside the warehouse (S3/file) — i.e. it's an
    ingestion/landing job that populates bronze. Returns sorted lists. Heuristic.
    """
    text = script_text or ""
    reads: set[str] = set()
    writes: set[str] = set()
    out_paths: set[str] = set()

    for db, tbl in _GLUE_CATALOG_RE.findall(text):
        reads.add(_norm(f"{db}.{tbl}"))
    for db, tbl in _GLUE_SINK_RE.findall(text):
        writes.add(_norm(f"{db}.{tbl}"))
    for m in _SPARK_READ_TABLE_RE.findall(text):
        reads.add(_norm(m))
    for m in _LOAD_PATH_RE.findall(text):
        reads.add(m.strip().lower())
    for m in _SPARK_SAVE_AS_RE.findall(text):
        writes.add(_norm(m))
    for m in _SPARK_INSERT_INTO_RE.findall(text):
        writes.add(_norm(m))
    for m in _SAVE_PATH_RE.findall(text):
        writes.add(m.strip().lower())

    # Snowflake/JDBC connector tables (the source→Snowflake-destination flow). A
    # dbtable inside a write chain is the destination table; otherwise it's a read.
    for m in _DBTABLE_RE.finditer(text):
        tbl = _norm(m.group(1))
        if not tbl:
            continue
        preceding = text[max(0, m.start() - 240):m.start()].lower()
        if "write" in preceding or "sink" in preceding:
            writes.add(tbl)
        else:
            reads.add(tbl)

    for sql in _SPARK_SQL_RE.findall(text):
        for dep in parse_sql_dependencies(sql):
            reads.add(dep)
        for w in _SQL_WRITE_RE.findall(sql):
            writes.add(_norm(w))

    has_write_op = bool(_WRITE_OP_RE.search(text))
    has_file_read = bool(_FILE_READ_RE.search(text))

    # S3 path literals assigned/used anywhere (e.g. landing_path = f"s3://.../landing/").
    # Classify each as a read (input) or a write (output) by path keyword / nearby
    # write context so ingestion jobs that use variable paths still register I/O.
    # Skip unresolved f-string templates (e.g. f"s3://{bucket}/{gold}/{table}/") —
    # they're not concrete datasets and would otherwise add junk nodes like "{table}".
    saw_s3_template = False
    for m in _S3_LITERAL_RE.finditer(text):
        raw = m.group(1).strip()
        low = raw.lower()
        pre = text[max(0, m.start() - 80):m.start()].lower()
        if "{" in raw or "}" in raw:
            # Templated loop path — collapse to its layer node so the job still
            # connects (e.g. f"s3://{bucket}/{silver}/{table}/" → s3://silver).
            layer = _s3_layer_token(low)
            if not layer:
                saw_s3_template = True
                continue
            target = f"s3://{layer}"
        else:
            target = low
        # Read vs write: prefer nearby call context, then a path keyword.
        if "read" in pre and "write" not in pre and "save" not in pre:
            reads.add(target)
        elif "write" in pre or "save" in pre or "sink" in pre:
            out_paths.add(target)
        elif any(k in low for k in _OUT_PATH_KW):
            out_paths.add(target)
        else:
            reads.add(target)

    # File→table-name map: the dict values are the bronze tables the job produces.
    table_map_writes = {_norm(t) for t in _FILE_TABLE_MAP_RE.findall(text)} if has_write_op else set()
    writes |= table_map_writes
    # Use raw output paths only when we couldn't recover named bronze tables, so a
    # well-named ingest doesn't also spawn a redundant ".../processed/" path node.
    if not table_map_writes:
        writes |= out_paths

    # Catch-all: drop any unresolved f-string template path (e.g. captured by the
    # .parquet("s3://{bucket}/{t}/") matcher) — concrete dataset names never contain
    # braces, and these would otherwise show up as junk "{t}"/"{table}" nodes.
    braced = [x for x in (reads | writes) if "{" in x or "}" in x]
    if braced:
        reads = {r for r in reads if "{" not in r and "}" not in r}
        writes = {w for w in writes if "{" not in w and "}" not in w}

    # A target that's also read is a write (it's the job's output).
    reads -= writes
    external = (
        has_file_read
        or bool(out_paths)
        or saw_s3_template
        or bool(braced)
        or any(_looks_like_path(r) for r in reads)
    )
    return {"reads": sorted(reads), "writes": sorted(writes), "external": external}


# ─── unified lineage graph ───────────────────────────────────────────────────

def _looks_like_path(identifier: str) -> bool:
    return identifier.startswith("s3://") or identifier.startswith("s3a://")


# Medallion layer synonyms across naming conventions. Supports both the
# bronze/silver/gold vocabulary and the landing/raw/curated/publish vocabulary
# (raw->bronze, curated->silver, publish->gold). Checked GOLD-first so a serving
# token wins over an intermediate one in a mixed path.
_LAYER_SYNONYMS = (
    ("gold",   ("gold", "publish", "published", "serving", "presentation", "mart", "semantic")),
    ("silver", ("silver", "curated", "conformed", "cleansed", "refined", "enriched", "processed")),
    ("bronze", ("bronze", "landing", "raw", "staging", "stage", "ingest")),
)


def canonical_layer(text: str) -> str | None:
    """Map any layer token / schema / path fragment to bronze|silver|gold, or None.

    One source of truth for layer inference so the bronze/silver/gold and the
    landing/raw/curated/publish conventions classify identically everywhere.
    """
    low = (text or "").lower()
    for layer, kws in _LAYER_SYNONYMS:
        if any(k in low for k in kws):
            return layer
    return None


def _glue_layer(full_name: str) -> str:
    """Infer a Glue catalog table's medallion layer from its schema/database name.

    Glue catalogs are often already medallion-organized (e.g. ``medaffairs_gold``,
    ``raw_db``, ``crm_curated``, ``crm_publish``). Uses the canonical synonym map so
    both naming conventions resolve correctly: ``curated`` -> silver and
    ``publish``/``published`` -> gold (previously ``curated`` was mislabelled gold and
    ``publish`` fell through to bronze).
    """
    schema = _norm(full_name).split(".")[0]
    return canonical_layer(schema) or "bronze"  # raw / landing / staging / unknown


def build_lineage(
    snowflake_objects: dict | None = None,
    snowflake_ddl: dict | None = None,
    glue_tables: list | None = None,
    glue_jobs: list | None = None,
    glue_scripts: dict | None = None,
    relationships: list | None = None,
) -> dict:
    """Merge Snowflake + Glue metadata into a {nodes, edges} dataflow graph.

    Edges come from: Glue job reads→writes (the ETL logic), Snowflake view
    definitions (dep→view), declared foreign keys between tables (fk_table →
    referenced table), and cross-system name matches (a 'load' edge). The
    relationship edges are what connect base tables (e.g. TPCH) that have no views
    or ETL between them.
    """
    snowflake_objects = snowflake_objects or {}
    snowflake_ddl = snowflake_ddl or {}
    glue_tables = glue_tables or []
    glue_jobs = glue_jobs or []
    glue_scripts = glue_scripts or {}
    relationships = relationships or []

    nodes: dict[str, dict] = {}
    edges: list[dict] = []
    # base-name → node id, to resolve un-qualified references and cross-system matches.
    by_base: dict[str, list[str]] = {}

    def add_node(node_id, label, ntype, **extra):
        if node_id not in nodes:
            nodes[node_id] = {"id": node_id, "label": label, "display": _short_label(label), "type": ntype, **extra}
            base = _base_name(label)
            by_base.setdefault(base, []).append(node_id)
        return node_id

    def resolve(identifier, as_target=False):
        """Map a parsed reference to an existing node id, else create a new node.

        Unknown reads become 'source' nodes; unknown write targets become 'silver'
        (a produced table). Ambiguous base-name matches are broken by path-part
        overlap (e.g. 'analytics.orders' prefers sf 'ANALYTICS.PUBLIC.ORDERS' over
        glue 'raw_db.orders').
        """
        norm = identifier if _looks_like_path(identifier) else _norm(identifier)
        # exact full-name match
        for nid, n in nodes.items():
            if _norm(n["label"]) == norm:
                return nid
        candidates = by_base.get(_base_name(norm), [])
        if len(candidates) == 1:
            return candidates[0]
        if len(candidates) > 1:
            ref_parts = set(norm.split("."))
            scored = sorted(
                candidates,
                key=lambda nid: len(ref_parts & set(_norm(nodes[nid]["label"]).split("."))),
                reverse=True,
            )
            top = scored[0]
            top_score = len(ref_parts & set(_norm(nodes[top]["label"]).split(".")))
            runner_score = len(ref_parts & set(_norm(nodes[scored[1]]["label"]).split("."))) if len(scored) > 1 else -1
            if top_score > runner_score:
                return top
        # nothing matched — create a node typed by role
        ntype = "silver" if (as_target and not _looks_like_path(norm)) else "source"
        return add_node(f"{'tgt' if as_target else 'ext'}:{norm}", norm, ntype)

    # Snowflake nodes
    for t in snowflake_objects.get("tables", []) or []:
        add_node(f"sf:{_norm(t['full_name'])}", t["full_name"], "silver",
                 system="snowflake", column_count=len(t.get("columns") or []))
    for v in snowflake_objects.get("views", []) or []:
        add_node(f"sf:{_norm(v['full_name'])}", v["full_name"], "gold",
                 system="snowflake", column_count=len(v.get("columns") or []))

    # Glue catalog table nodes — layer inferred from the schema (gold/silver/bronze)
    # so a catalog that's already medallion-organized reads correctly.
    for t in glue_tables:
        add_node(f"glue:{_norm(t['full_name'])}", t["full_name"], _glue_layer(t["full_name"]),
                 system="glue", column_count=len(t.get("columns") or []), location=t.get("location") or "")

    # Snowflake view dependency edges
    for full_name, ddl in snowflake_ddl.items():
        view_id = f"sf:{_norm(full_name)}"
        if view_id not in nodes:
            add_node(view_id, full_name, "gold", system="snowflake")
        for dep in parse_sql_dependencies(ddl):
            src_id = resolve(dep)
            if src_id != view_id:
                edges.append({"from": src_id, "to": view_id, "label": "view"})

    # Glue jobs become first-class nodes: source(s) → [job] → destination(s), so the
    # flow reads as "source → Glue job → destination" and the job node is clickable.
    for job in glue_jobs:
        name = job.get("name") or "glue_job"
        io = parse_pyspark_io(glue_scripts.get(name, ""))
        reads_io, writes_io = io.get("reads", []), io.get("writes", [])
        if not reads_io and not writes_io:
            continue
        job_id = f"job:{_norm(name)}"
        add_node(job_id, name, "job", system="glue", job_type=job.get("type") or "",
                 script_location=job.get("script_location") or "")
        for r in reads_io:
            r_id = resolve(r)
            if r_id != job_id:
                edges.append({"from": r_id, "to": job_id, "label": ""})
        for w in writes_io:
            w_id = resolve(w, as_target=True)
            if w_id != job_id:
                edges.append({"from": job_id, "to": w_id, "label": ""})

    # Cross-system edges: Glue catalog table ↔ Snowflake table, same base name.
    # A raw/bronze Glue table feeding a warehouse table is a real "load"; a Glue
    # table in a silver/gold schema that mirrors a Snowflake table is the same
    # logical table materialized in both systems (a "copy" — what the cross-system
    # panel flags), not a distinct dataflow step.
    for t in glue_tables:
        glue_id = f"glue:{_norm(t['full_name'])}"
        label = "load" if _glue_layer(t["full_name"]) == "bronze" else "copy"
        for cand in by_base.get(_base_name(t["full_name"]), []):
            if cand.startswith("sf:") and cand != glue_id:
                edges.append({"from": glue_id, "to": cand, "label": label})

    # Foreign-key relationship edges (fk_table → referenced pk_table). Only connect
    # tables that already exist as nodes — never invent a node from a relationship.
    def _find_existing(full_name):
        norm = _norm(full_name)
        for nid, n in nodes.items():
            if _norm(n["label"]) == norm:
                return nid
        return None

    for rel in relationships:
        fk_id = _find_existing(rel.get("fk_table"))
        pk_id = _find_existing(rel.get("pk_table"))
        if fk_id and pk_id and fk_id != pk_id:
            edges.append({"from": fk_id, "to": pk_id, "label": "fk"})

    # De-dup edges
    unique = {(e["from"], e["to"], e["label"]): e for e in edges}
    return {"nodes": list(nodes.values()), "edges": list(unique.values())}


# ─── medallion layer + job classification ───────────────────────────────────

# Lineage node type → medallion layer. 'source' nodes are external inputs, not a
# migrated table layer.
LAYER_BY_TYPE = {"source": "external", "bronze": "bronze", "silver": "silver", "gold": "gold"}


def migration_layer(node_type: str) -> str:
    return LAYER_BY_TYPE.get(node_type or "", "silver")


def classify_job(io: dict, known_table_norms) -> str:
    """Classify a Glue job as 'ingestion' (E+L) or 'transformation' (T).

    The plan splits responsibilities at the bronze boundary: ingestion jobs bring
    EXTERNAL data in (S3/file/JDBC/API → land it), transformation jobs derive new
    tables from existing warehouse/catalog tables. Heuristic: reads that are S3
    paths or unknown identifiers are 'external'; reads that match a known catalog/
    warehouse table are 'known'. External-dominant (or no captured reads, e.g. a
    JDBC pull) → ingestion; purely table-to-table → transformation.

    Exception, checked first: a job whose I/O paths point at the lake's own curated
    layers (``s3://.../silver|gold/``) or that consumes ``bronze`` is a
    transformation/load — silver→gold, gold→Snowflake, etc. Those read templated S3
    paths so the path heuristics flag them 'external', but they are NOT raw ingestion.
    """
    known = set(known_table_norms or [])
    reads = (io or {}).get("reads", []) or []
    writes = (io or {}).get("writes", []) or []
    known_reads = [r for r in reads if not _looks_like_path(r) and _norm(r) in known]

    # Curated-layer I/O (or a bronze read) means the job derives data from the lake,
    # not from outside it → transformation/load. Checked before the 'external' test
    # because these jobs read templated S3 paths that look external.
    io_layers = {_s3_layer_token(p) for p in (reads + writes) if _looks_like_path(p)}
    reads_bronze = any(_looks_like_path(r) and _s3_layer_token(r) == "bronze" for r in reads)
    if (io_layers & set(_CURATED_LAYER_KW)) or reads_bronze:
        return "transformation"

    # A job that pulls from outside the warehouse (S3/file/pandas/Glue from_options)
    # and doesn't derive purely from known warehouse tables is an ingestion job.
    if (io or {}).get("external") and not known_reads:
        return "ingestion"
    if not reads:
        return "ingestion"  # pulls from a source not captured as a table (JDBC/API)
    external = [r for r in reads if _looks_like_path(r) or _norm(r) not in known]
    if known_reads and not external:
        return "transformation"
    if external and not known_reads:
        return "ingestion"
    return "ingestion" if len(external) >= len(known_reads) else "transformation"


# ─── scoped (upstream) lineage ───────────────────────────────────────────────

def upstream_subgraph(lineage: dict, target_ids) -> dict:
    """Return the sub-graph of ``target_ids`` plus all of their upstream ancestors.

    Walks edges backwards (to → from) from each target so the result is exactly
    "what feeds the selected tables" — the scoped lineage shown after the user
    picks tables to migrate, and the set Phase 3 will convert. Unknown target ids
    are ignored. Edges are kept only when both endpoints are in the result.
    """
    lineage = lineage or {}
    nodes = lineage.get("nodes", []) or []
    edges = lineage.get("edges", []) or []
    node_ids = {n["id"] for n in nodes}

    incoming: dict[str, list[str]] = {}
    for e in edges:
        incoming.setdefault(e["to"], []).append(e["from"])

    included = {tid for tid in (target_ids or []) if tid in node_ids}
    frontier = list(included)
    while frontier:
        cur = frontier.pop()
        for src in incoming.get(cur, []):
            if src not in included:
                included.add(src)
                frontier.append(src)

    sub_nodes = [n for n in nodes if n["id"] in included]
    sub_edges = [e for e in edges if e["from"] in included and e["to"] in included]
    return {"nodes": sub_nodes, "edges": sub_edges}


# ─── duplicate detection ─────────────────────────────────────────────────────

def detect_duplicates(snowflake_objects: dict | None = None, glue_tables: list | None = None) -> list[dict]:
    """Flag the same logical table appearing in more than one place.

    A group is raised when the same base table name appears in more than one
    system (or twice within one). Each group reports its members and a
    column-name overlap ratio (Jaccard on column-name sets — a name match, not a
    data comparison) so the UI can rank how closely the schemas align.
    """
    snowflake_objects = snowflake_objects or {}
    glue_tables = glue_tables or []

    members: dict[str, list[dict]] = {}

    def add(full_name, system, columns):
        base = _base_name(full_name)
        if not base:
            return
        members.setdefault(base, []).append({
            "full_name": full_name,
            "system": system,
            "columns": [c.get("name") for c in (columns or []) if c.get("name")],
        })

    for t in snowflake_objects.get("tables", []) or []:
        add(t["full_name"], "snowflake", t.get("columns"))
    for v in snowflake_objects.get("views", []) or []:
        add(v["full_name"], "snowflake", v.get("columns"))
    for t in glue_tables:
        add(t["full_name"], "glue", t.get("columns"))

    groups = []
    for base, items in members.items():
        if len(items) < 2:
            continue
        # column overlap across the group (min pairwise Jaccard, lowercased)
        colsets = [{c.lower() for c in it["columns"]} for it in items if it["columns"]]
        overlap = None
        if len(colsets) >= 2:
            ratios = []
            for i in range(len(colsets)):
                for j in range(i + 1, len(colsets)):
                    union = colsets[i] | colsets[j]
                    ratios.append(len(colsets[i] & colsets[j]) / len(union) if union else 0.0)
            overlap = round(min(ratios), 2) if ratios else None
        cross_system = len({it["system"] for it in items}) > 1
        groups.append({
            "base_name": base,
            "members": items,
            "cross_system": cross_system,
            "column_overlap": overlap,
        })
    # Most suspicious first: cross-system, then higher column overlap.
    groups.sort(key=lambda g: (g["cross_system"], g["column_overlap"] or 0), reverse=True)
    return groups


# ─── recommendations (deterministic baseline + optional AI) ──────────────────

def _deterministic_recommendations(duplicates: list[dict]) -> list[dict]:
    recs = []
    for g in duplicates:
        names = ", ".join(m["full_name"] for m in g["members"])
        if g["cross_system"]:
            recs.append({
                "title": f"Consolidate '{g['base_name']}' — materialized in both Snowflake and Glue",
                "detail": (
                    f"'{g['base_name']}' is the same logical table held in two systems ({names}) — typically the "
                    "Glue/S3 gold layer and the copy loaded into Snowflake. In Databricks, land it once as a single "
                    "governed table and point both consumers at it, rather than recreating both copies."
                ),
                "severity": "high" if (g["column_overlap"] or 0) >= 0.6 else "medium",
                "members": [m["full_name"] for m in g["members"]],
            })
        else:
            recs.append({
                "title": f"Review repeated '{g['base_name']}' definitions",
                "detail": f"'{g['base_name']}' is defined more than once in the same system ({names}). Confirm whether these are intentionally separate before migrating.",
                "severity": "low",
                "members": [m["full_name"] for m in g["members"]],
            })
    return recs


def _extract_json(text: str):
    """Pull the first JSON object/array out of a possibly chatty LLM reply."""
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:  # noqa: BLE001
        pass
    for open_c, close_c in (("{", "}"), ("[", "]")):
        start = text.find(open_c)
        end = text.rfind(close_c)
        if start != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except Exception:  # noqa: BLE001
                continue
    return None


def explain_business_logic(call_ai, snowflake_objects=None, snowflake_ddl=None,
                           glue_jobs=None, glue_scripts=None) -> dict:
    """Produce a plain-English overview of the source's business logic.

    Returns {text, ai_used}. Deterministic fallback (counts + object names) when
    call_ai isn't configured or fails.
    """
    snowflake_objects = snowflake_objects or {}
    snowflake_ddl = snowflake_ddl or {}
    glue_jobs = glue_jobs or []
    glue_scripts = glue_scripts or {}
    tables = snowflake_objects.get("tables", []) or []
    views = snowflake_objects.get("views", []) or []

    fallback_lines = [
        f"This source has **{len(tables)} table(s)**, **{len(views)} view(s)** in Snowflake "
        f"and **{len(glue_jobs)} Glue job(s)**.",
    ]
    if tables:
        fallback_lines.append("Tables: " + ", ".join(t["full_name"] for t in tables[:40]))
    if views:
        fallback_lines.append("Views: " + ", ".join(v["full_name"] for v in views[:40]))
    if glue_jobs:
        fallback_lines.append("Glue jobs: " + ", ".join(j.get("name", "") for j in glue_jobs[:40]))
    fallback = "\n\n".join(fallback_lines)

    if not call_ai:
        return {"text": fallback, "ai_used": False}
    try:
        view_snips = "\n\n".join(f"-- {fn}\n{(sql or '')[:700]}" for fn, sql in list(snowflake_ddl.items())[:12])
        job_snips = "\n\n".join(f"# {name}\n{(script or '')[:700]}" for name, script in list(glue_scripts.items())[:12])
        prompt = (
            "In plain business English (Markdown), explain what this data platform does: the key entities/tables, "
            "what the Snowflake views and AWS Glue ETL jobs compute, and the overall data flow. Be concise and concrete.\n\n"
            f"Tables: {', '.join(t['full_name'] for t in tables[:60])}\n"
            f"Views: {', '.join(v['full_name'] for v in views[:60])}\n\n"
            f"View definitions:\n{view_snips or '(none)'}\n\nGlue job scripts:\n{job_snips or '(none)'}"
        )
        text = call_ai(prompt, system_prompt="You are a data analyst writing a clear plain-English overview.",
                       max_tokens=1500, task="business_explanation", temperature=0)
        text = text.strip() if isinstance(text, str) else ""
        return {"text": text or fallback, "ai_used": bool(text)}
    except Exception as exc:  # noqa: BLE001 — AI best-effort
        logger.warning("Business-logic explanation failed; using fallback: %s", exc)
        return {"text": fallback, "ai_used": False}


def recommend(call_ai, lineage: dict, duplicates: list[dict],
              snowflake_objects: dict | None = None, glue_jobs: list | None = None) -> dict:
    """Produce {summary, recommendations, duplicates}.

    Always returns the deterministic baseline. When call_ai is provided, an AI
    pass adds business-logic-level duplication findings and consolidation advice;
    any AI failure degrades gracefully to the baseline.
    """
    snowflake_objects = snowflake_objects or {}
    glue_jobs = glue_jobs or []
    base_recs = _deterministic_recommendations(duplicates)
    n_tables = len(snowflake_objects.get("tables", []) or [])
    n_views = len(snowflake_objects.get("views", []) or [])
    n_cross = sum(1 for g in duplicates if g.get("cross_system"))
    summary = (
        f"{n_tables} Snowflake table(s), {n_views} view(s), {len(glue_jobs)} Glue job(s); "
        f"{len(lineage.get('nodes', []))} lineage node(s), {n_cross} table(s) materialized in both systems."
    )
    # ai_status distinguishes WHY AI recs may be absent so the UI can show an honest
    # message: 'no_provider' (none configured) vs 'error' (call failed — e.g. expired
    # Bedrock SSO) vs 'empty' (ran but returned nothing) vs 'ok'.
    result = {"summary": summary, "recommendations": base_recs, "duplicates": duplicates,
              "ai_used": False, "ai_status": "no_provider" if not call_ai else "empty"}

    if not call_ai:
        return result

    try:
        dup_lines = "\n".join(
            f"- {g['base_name']}: {', '.join(m['full_name']+' ['+m['system']+']' for m in g['members'])}"
            f" (column_overlap={g['column_overlap']})"
            for g in duplicates[:40]
        ) or "(none detected structurally)"
        job_lines = "\n".join(f"- {j.get('name')} ({j.get('type')})" for j in glue_jobs[:40]) or "(none)"
        prompt = (
            "You are a data platform architect planning a migration of Snowflake tables and AWS Glue ETL "
            "jobs onto Databricks/dbt. The goal is to AVOID DUPLICATION in the destination. Given the "
            "structurally-detected duplicate table groups and the Glue jobs below, identify any overlapping "
            "BUSINESS LOGIC (jobs/views computing the same thing) and recommend how to consolidate in Databricks.\n\n"
            f"Duplicate table groups:\n{dup_lines}\n\nGlue jobs:\n{job_lines}\n\n"
            "Respond with ONLY a JSON object: "
            '{"recommendations":[{"title":str,"detail":str,"severity":"high|medium|low","members":[str]}]}'
        )
        reply = call_ai(prompt, system_prompt="Return only valid JSON.", max_tokens=1500,
                        task="business_explanation", temperature=0)
        parsed = _extract_json(reply if isinstance(reply, str) else "")
        ai_recs = (parsed or {}).get("recommendations") if isinstance(parsed, dict) else None
        if isinstance(ai_recs, list) and ai_recs:
            cleaned = []
            for r in ai_recs:
                if not isinstance(r, dict) or not r.get("title"):
                    continue
                cleaned.append({
                    "title": str(r.get("title")),
                    "detail": str(r.get("detail") or ""),
                    "severity": str(r.get("severity") or "medium").lower(),
                    "members": [str(m) for m in (r.get("members") or []) if m],
                    "source": "ai",
                })
            if cleaned:
                result["recommendations"] = cleaned + base_recs
                result["ai_used"] = True
                result["ai_status"] = "ok"
    except Exception as exc:  # noqa: BLE001 — AI is best-effort
        logger.warning("AI recommendation pass failed; using deterministic baseline: %s", exc)
        result["ai_status"] = "error"
        result["ai_error"] = str(exc)[:200]

    return result
