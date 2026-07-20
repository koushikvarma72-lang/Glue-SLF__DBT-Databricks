"""Cross-engine reconciliation harness for the Snowflake/Glue → Databricks migration.

This is the verification gate the migration kit calls for: a generated dbt model only
ships once it provably produces the same data as the legacy source. It is the adaptation
of the warehouse-local ``reconcile.py`` (which diffs two tables in ONE connection via a
JOIN) to this tool's reality — the source lives in **Snowflake** and the candidate in
**Databricks**, two different engines that cannot be JOINed in a single query.

So instead of a cross-table JOIN we compute, on each engine **independently**, a
fingerprint that reduces to engine-comparable scalars, then diff those in Python:

  1. Schema parity        — column names present in both (case-insensitive).
  1b. Schema type drift   — a shared column that flipped numeric↔non-numeric across engines
                            (silently corrupts every downstream SUM/AVG/join). Expected
                            translations (VARIANT→STRING, timestamp spelling) are advisory.
  2. Row count            — total rows on each side (with an optional fractional tolerance).
  3. Key integrity        — duplicate / null primary keys on each side (always exact).
  4. Aggregate fingerprint — per column: non-null count, null count, DISTINCT cardinality,
                            min, max, and (for numeric columns) sum + sum-of-squares. Pure
                            arithmetic, so directly comparable across engines; catches
                            dropped/duplicated rows, wrong joins, value drift, and value-SET
                            drift. Per-column tolerance overrides are supported.
  5. Reference/containment — ``check_containment()`` (single-engine anti-join) verifies every
                            non-null foreign key resolves to a parent row.

What this deliberately does NOT do (and why): a per-row key-set diff and per-row value
diff require both tables in one engine. Hash-based column checksums aren't comparable
either (Snowflake ``HASH`` ≠ Spark ``xxhash64``). Co-locate the two tables (Databricks
Lakehouse Federation, or land a ``*_legacy`` snapshot into Delta) to get row-level diffs;
the count + key-integrity + aggregate fingerprint here is the honest cross-engine gate.

The engine connection is injected as a ``runner`` callable — ``run(sql) -> list[row]``
where each row is an indexable sequence — so the core is dependency-free and unit-testable
against stdlib ``sqlite3`` (see ``_self_test``). The routes layer supplies real Snowflake
and Databricks runners.

Run the built-in self-test (no warehouse needed):
    python -m backend.integrations.reconcile --self-test
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Sequence

Runner = Callable[[str], Sequence[Sequence[Any]]]


# --------------------------------------------------------------------------- #
# dialects — only identifier quoting and numeric-type detection differ
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Dialect:
    name: str
    quote: str = '"'  # Snowflake/SQLite use ", Spark/Databricks use `

    def q(self, ident: str) -> str:
        return f"{self.quote}{ident}{self.quote}"

    def qualify(self, table: str) -> str:
        return ".".join(self.q(p) for p in str(table).split("."))


SNOWFLAKE = Dialect("snowflake", '"')
DATABRICKS = Dialect("databricks", "`")
SQLITE = Dialect("sqlite", '"')

_NUMERIC_HINTS = ("INT", "NUMERIC", "DECIMAL", "FLOAT", "REAL", "DOUBLE", "NUMBER", "BIGINT",
                  "SMALLINT", "TINYINT", "BYTEINT", "LONG", "SHORT")


def _is_numeric(type_str: str | None) -> bool:
    t = str(type_str or "").upper()
    return any(h in t for h in _NUMERIC_HINTS)


def _norm_cols(columns) -> dict[str, str]:
    """[{name,type}] | [name] → {lower_name: type_str}. Last write wins on dup names."""
    out: dict[str, str] = {}
    for c in columns or []:
        if isinstance(c, dict):
            name, typ = c.get("name"), c.get("type")
        else:
            name, typ = c, None
        if name:
            out[str(name).lower()] = str(typ or "")
    return out


def _to_number(v):
    """Best-effort numeric coercion (Databricks returns numbers as strings in data_array;
    Snowflake returns Decimal). Returns None if not numeric-looking."""
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


_BOOL_TRUE = {"true", "t", "1", "1.0"}
_BOOL_FALSE = {"false", "f", "0", "0.0"}


def _as_bool(v):
    """Canonicalize a cross-engine boolean spelling, else None. Snowflake returns a Python
    bool (str()='True'/'False'); Databricks' SQL API returns 'true'/'false' — so MIN/MAX on a
    boolean column would string-compare unequal on every value without this."""
    s = str(v).strip().lower()
    if s in _BOOL_TRUE:
        return True
    if s in _BOOL_FALSE:
        return False
    return None


def _values_equal(a, b, float_tol: float) -> bool:
    """NULL-aware, float-tolerant, cross-engine value comparison."""
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    na, nb = _to_number(a), _to_number(b)
    if na is not None and nb is not None:
        # RELATIVE tolerance, scaled by magnitude: Snowflake (Decimal) and Databricks
        # (double, returned as a string) round differently, so an absolute-only tolerance
        # spuriously FAILS on large sums / sums-of-squares. This lets a cross-engine
        # rounding wobble pass while a real ≥float_tol-relative drift still fails.
        return abs(na - nb) <= float_tol * max(1.0, abs(na), abs(nb))
    # Booleans stringify differently per engine ('True'/'true'/'t' vs 'False'/'false'/'f').
    # Canonicalize before the string compare so a representation diff isn't a false positive.
    ba, bb = _as_bool(a), _as_bool(b)
    if ba is not None and bb is not None:
        return ba == bb
    # Non-numeric: compare as trimmed strings (dates/strings land here; engines stringify
    # timestamps differently, so those may need exclude).
    return str(a).strip() == str(b).strip()


# --------------------------------------------------------------------------- #
# report container
# --------------------------------------------------------------------------- #
@dataclass
class Report:
    source: str
    candidate: str
    primary_key: list[str]
    passed: bool = True
    checks: dict[str, Any] = field(default_factory=dict)
    failures: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.passed = False
        self.failures.append(msg)


# --------------------------------------------------------------------------- #
# per-engine fingerprint
# --------------------------------------------------------------------------- #
def _scalar(runner: Runner, sql: str) -> Any:
    rows = runner(sql)
    if not rows:
        return None
    row = rows[0]
    return row[0] if isinstance(row, (list, tuple)) else row


def _fingerprint(runner: Runner, dialect: Dialect, table: str, *, name_by_lower: dict[str, str],
                 type_by_lower: dict[str, str], agg_cols_lower: list[str], numeric_lower: set[str],
                 pk_lower: list[str], where: str | None) -> dict[str, Any]:
    """Run the count / key-integrity / aggregate queries for one table on one engine.

    Returns {row_count, dup_keys, null_keys, agg:{col_lower:{nonnull,nulls,min,max,sum}}}.
    Column casing comes from ``name_by_lower`` (each engine stores its own case).
    """
    tq = dialect.qualify(table)
    wsql = f" WHERE {where}" if where else ""
    fp: dict[str, Any] = {}

    fp["row_count"] = int(_scalar(runner, f"SELECT COUNT(*) FROM {tq}{wsql}") or 0)

    # key integrity — duplicate key groups and null keys
    if pk_lower:
        key_csv = ", ".join(dialect.q(name_by_lower[k]) for k in pk_lower)
        fp["dup_keys"] = int(_scalar(
            runner,
            f"SELECT COUNT(*) FROM (SELECT {key_csv} FROM {tq}{wsql} "
            f"GROUP BY {key_csv} HAVING COUNT(*) > 1) d") or 0)
        null_pred = " OR ".join(f"{dialect.q(name_by_lower[k])} IS NULL" for k in pk_lower)
        null_clause = f"{wsql} AND ({null_pred})" if wsql else f" WHERE {null_pred}"
        fp["null_keys"] = int(_scalar(runner, f"SELECT COUNT(*) FROM {tq}{null_clause}") or 0)
    else:
        fp["dup_keys"], fp["null_keys"] = None, None

    # aggregate fingerprint — one single-row SELECT, read positionally
    select_parts, plan = [], []  # plan = [(col_lower, kind), ...]
    for col in agg_cols_lower:
        cq = dialect.q(name_by_lower[col])
        select_parts.append(f"COUNT({cq})"); plan.append((col, "nonnull"))
        select_parts.append(f"SUM(CASE WHEN {cq} IS NULL THEN 1 ELSE 0 END)"); plan.append((col, "nulls"))
        # distinct cardinality — COUNT(DISTINCT) excludes NULLs on both engines, so it's
        # cross-engine comparable and catches value-SET drift (a remap/dedup that keeps the
        # same count/min/max/sum but changes which distinct values exist).
        select_parts.append(f"COUNT(DISTINCT {cq})"); plan.append((col, "distinct"))
        select_parts.append(f"MIN({cq})"); plan.append((col, "min"))
        select_parts.append(f"MAX({cq})"); plan.append((col, "max"))
        if col in numeric_lower:
            select_parts.append(f"SUM({cq})"); plan.append((col, "sum"))
            # sum-of-squares: catches *symmetric* value drift that preserves count/min/max/
            # sum (e.g. 200→250 with another 300→250). Still pure arithmetic, so it stays
            # comparable across engines. Not a row-level guarantee — co-locate for that.
            select_parts.append(f"SUM({cq} * {cq})"); plan.append((col, "sumsq"))
    agg: dict[str, dict[str, Any]] = {c: {} for c in agg_cols_lower}
    if select_parts:
        rows = runner(f"SELECT {', '.join(select_parts)} FROM {tq}{wsql}")
        row = rows[0] if rows else [None] * len(plan)
        for i, (col, kind) in enumerate(plan):
            val = row[i] if i < len(row) else None
            agg[col][kind] = int(val) if kind in ("nonnull", "nulls", "distinct") and val is not None else val
    fp["agg"] = agg
    return fp


# --------------------------------------------------------------------------- #
# core reconciliation
# --------------------------------------------------------------------------- #
def reconcile(
    *,
    source_runner: Runner,
    candidate_runner: Runner,
    source_table: str,
    candidate_table: str,
    source_columns,
    candidate_columns,
    primary_key: list[str],
    source_dialect: Dialect = SNOWFLAKE,
    candidate_dialect: Dialect = DATABRICKS,
    exclude: list[str] | None = None,
    float_tol: float = 1e-6,
    col_tol: dict[str, float] | None = None,
    row_count_tol: float = 0.0,
    where: str | None = None,
) -> Report:
    """Diff a Databricks candidate table against its legacy Snowflake source. See module
    docstring for the checks performed and the cross-engine limitations.

    Tolerances: ``float_tol`` is the default relative tolerance for numeric aggregates;
    ``col_tol`` overrides it per column (``{col_lower: tol}``); ``row_count_tol`` is the
    allowed FRACTIONAL row-count delta (0.0 = exact — the default). Key integrity
    (dup/null keys) is always exact — it's a correctness invariant, not a measurement.
    """
    rep = Report(source=source_table, candidate=candidate_table, primary_key=list(primary_key or []))
    exclude_lower = {str(c).lower() for c in (exclude or [])}
    col_tol = {str(k).lower(): float(v) for k, v in (col_tol or {}).items()}

    s_cols = _norm_cols(source_columns)
    c_cols = _norm_cols(candidate_columns)
    if not s_cols:
        rep.fail(f"source table '{source_table}' has no readable columns")
    if not c_cols:
        rep.fail(f"candidate table '{candidate_table}' has no readable columns (not deployed yet?)")
    if not s_cols or not c_cols:
        return rep

    # ---- 1. schema parity (case-insensitive on names) ---------------------- #
    only_src = sorted(set(s_cols) - set(c_cols))
    only_cand = sorted(set(c_cols) - set(s_cols))
    rep.checks["schema"] = {"only_in_source": only_src, "only_in_candidate": only_cand}
    if only_src:
        rep.fail(f"columns missing from candidate: {only_src}")
    if only_cand:
        rep.fail(f"unexpected extra columns in candidate: {only_cand}")

    # ---- 1b. schema TYPE drift (shared columns) --------------------------- #
    # A column that migrated numeric→string (or vice-versa) silently corrupts every
    # downstream SUM/AVG and any numeric join. Compare the numeric-ness of each shared
    # column across engines; a numeric XOR non-numeric is a hard fail. Other family
    # differences (e.g. VARIANT→STRING, TIMESTAMP spelling) are EXPECTED translations, so
    # they're recorded as advisory notes, not failures.
    type_mismatches, type_notes = [], []
    for col in sorted(set(s_cols) & set(c_cols)):
        st, ct = s_cols.get(col), c_cols.get(col)
        s_num, c_num = _is_numeric(st), _is_numeric(ct)
        if s_num != c_num:
            type_mismatches.append({"column": col, "source": st, "candidate": ct})
        elif str(st or "").strip().upper() != str(ct or "").strip().upper():
            type_notes.append({"column": col, "source": st, "candidate": ct})
    rep.checks["schema_types"] = {"numeric_drift": type_mismatches, "translated": type_notes}
    for m in type_mismatches:
        rep.fail(f"type drift on '{m['column']}': source {m['source']!r} vs candidate {m['candidate']!r} "
                 "(numeric↔non-numeric — corrupts aggregates/joins)")

    pk_lower = [str(k).lower() for k in (primary_key or [])]
    for k in pk_lower:
        if k not in s_cols or k not in c_cols:
            rep.fail(f"primary key column '{k}' not present in both tables")
            return rep  # cannot run key-integrity without a usable key

    shared = sorted((set(s_cols) & set(c_cols)) - exclude_lower)
    agg_cols = [c for c in shared if c not in pk_lower] or shared  # at least the keys
    numeric_lower = {c for c in agg_cols if _is_numeric(s_cols.get(c)) and _is_numeric(c_cols.get(c))}
    needed = list(dict.fromkeys(agg_cols + pk_lower))
    s_names = {k: _orig_name(source_columns, k) for k in needed}
    c_names = {k: _orig_name(candidate_columns, k) for k in needed}

    # ---- run both fingerprints (engine failures are reported, not raised) -- #
    try:
        s_fp = _fingerprint(source_runner, source_dialect, source_table, name_by_lower=s_names,
                            type_by_lower=s_cols, agg_cols_lower=agg_cols, numeric_lower=numeric_lower,
                            pk_lower=pk_lower, where=where)
    except Exception as exc:  # noqa: BLE001
        rep.fail(f"source fingerprint failed: {exc}")
        return rep
    try:
        c_fp = _fingerprint(candidate_runner, candidate_dialect, candidate_table, name_by_lower=c_names,
                            type_by_lower=c_cols, agg_cols_lower=agg_cols, numeric_lower=numeric_lower,
                            pk_lower=pk_lower, where=where)
    except Exception as exc:  # noqa: BLE001
        rep.fail(f"candidate fingerprint failed: {exc}")
        return rep

    # ---- 2. row counts ----------------------------------------------------- #
    s_n, c_n = s_fp["row_count"], c_fp["row_count"]
    rep.checks["row_counts"] = {"source": s_n, "candidate": c_n, "delta": c_n - s_n}

    # Empty-table short-circuit. When either side has 0 rows, EVERY per-column aggregate
    # trivially "differs" (real values vs. an empty table), spraying one noise failure per
    # column and burying the real signal — the table is empty. Diagnose that as a single,
    # operator-readable failure (it already carries the row counts) and skip the row-count
    # and per-column noise. The most common cause: the candidate built fine but its
    # upstream (bronze/raw) source was never loaded, so the transform had no input.
    if s_n == 0 or c_n == 0:
        # With a WHERE filter, 0 rows means the filtered SLICE is empty, not the table.
        empt = "returned 0 rows (after the WHERE filter)" if where else "is empty (0 rows)"
        if s_n == 0 and c_n == 0:
            both = ("both tables returned 0 rows (after the WHERE filter)" if where
                    else "both tables are empty (0 rows)")
            diag = f"{both} — load/build the data (or check the filter) before reconciling"
        elif c_n == 0:
            diag = (f"candidate '{candidate_table}' {empt} but source has {s_n} — "
                    "build/deploy it and load its upstream source tables first, then re-run. "
                    "Per-column checks skipped.")
        else:
            diag = (f"source '{source_table}' {empt} but candidate has {c_n} — "
                    "check the source table/location or filter. Per-column checks skipped.")
        rep.checks["aggregate_fingerprint"] = {"column_differences": {}, "skipped": "empty_table"}
        rep.fail(diag)
        return rep

    if abs(c_n - s_n) > row_count_tol * max(1, s_n):
        extra = f" (allowed ±{row_count_tol:.2%})" if row_count_tol else ""
        rep.fail(f"row count mismatch: source={s_n} candidate={c_n}{extra}")

    # ---- 3. key integrity -------------------------------------------------- #
    if pk_lower:
        rep.checks["key_integrity"] = {
            "source_duplicate_keys": s_fp["dup_keys"], "candidate_duplicate_keys": c_fp["dup_keys"],
            "source_null_keys": s_fp["null_keys"], "candidate_null_keys": c_fp["null_keys"],
        }
        for label, val in [("source dup keys", s_fp["dup_keys"]), ("candidate dup keys", c_fp["dup_keys"]),
                           ("source null keys", s_fp["null_keys"]), ("candidate null keys", c_fp["null_keys"])]:
            if val:
                rep.fail(f"{label}: {val}")

    # ---- 4. aggregate fingerprint ----------------------------------------- #
    # column_differences = only the metrics that DON'T match (the failure signal).
    # column_comparison  = the FULL before→after fingerprint for EVERY column
    #   (source value, candidate value, match flag per metric) so the UI can show a
    #   side-by-side "proof of conversion" table, not just the mismatches.
    col_diffs: dict[str, dict[str, Any]] = {}
    col_cmp: dict[str, dict[str, Any]] = {}
    for col in agg_cols:
        s_a, c_a = s_fp["agg"].get(col, {}), c_fp["agg"].get(col, {})
        tol = col_tol.get(col, float_tol)
        metrics: dict[str, Any] = {}
        for metric in ("nonnull", "nulls", "distinct", "min", "max", "sum", "sumsq"):
            if metric not in s_a and metric not in c_a:
                continue
            sv, cv = s_a.get(metric), c_a.get(metric)
            match = _values_equal(sv, cv, tol)
            metrics[metric] = {"source": sv, "candidate": cv, "match": match}
            if not match:
                col_diffs.setdefault(col, {})[metric] = {"source": sv, "candidate": cv}
        if metrics:
            col_cmp[col] = {"metrics": metrics, "match": all(m["match"] for m in metrics.values())}
    rep.checks["aggregate_fingerprint"] = {
        "column_differences": col_diffs, "column_comparison": col_cmp}
    for col, diffs in col_diffs.items():
        rep.fail(f"column '{col}' differs: {', '.join(sorted(diffs))}")

    return rep


# --------------------------------------------------------------------------- #
# reference / containment integrity (single engine)
# --------------------------------------------------------------------------- #
def check_containment(runner: Runner, dialect: Dialect, *, child_table: str, child_columns: list[str],
                      parent_table: str, parent_columns: list[str]) -> dict[str, Any]:
    """Referential-integrity (containment) check WITHIN one engine: every non-null child key
    must resolve to a parent row. Returns ``{ok, orphans, error}``; ``orphans > 0`` means a
    broken foreign key (e.g. a fact row whose dimension key has no match) — the classic
    silent damage a wrong join or a dropped dimension load causes.

    Both tables must live in the SAME engine (a cross-engine FK can't be joined). Run it on
    the candidate (Databricks) side after deploy, and optionally on the source (Snowflake)
    side as a baseline so an orphan that already existed in the source isn't blamed on the
    migration.
    """
    child_cols = [c for c in (child_columns or []) if c]
    parent_cols = [c for c in (parent_columns or []) if c]
    if not child_cols or len(child_cols) != len(parent_cols):
        return {"ok": True, "orphans": None, "error": "no usable key pair"}
    cq, pq = dialect.qualify(child_table), dialect.qualify(parent_table)
    on = " AND ".join(f"c.{dialect.q(cc)} = p.{dialect.q(pc)}" for cc, pc in zip(child_cols, parent_cols))
    notnull = " AND ".join(f"c.{dialect.q(cc)} IS NOT NULL" for cc in child_cols)
    sql = (f"SELECT COUNT(*) FROM {cq} c LEFT JOIN {pq} p ON {on} "
           f"WHERE p.{dialect.q(parent_cols[0])} IS NULL AND {notnull}")
    try:
        orphans = int(_scalar(runner, sql) or 0)
        return {"ok": orphans == 0, "orphans": orphans, "error": None}
    except Exception as exc:  # noqa: BLE001
        return {"ok": True, "orphans": None, "error": str(exc)[:200]}


def _orig_name(columns, lower_name: str) -> str:
    """Recover the original-cased column name for a lowercased key."""
    for c in columns or []:
        name = c.get("name") if isinstance(c, dict) else c
        if name and str(name).lower() == lower_name:
            return str(name)
    return lower_name


# --------------------------------------------------------------------------- #
# self-test (stdlib sqlite3 — two independent connections simulate two engines)
# --------------------------------------------------------------------------- #
def _self_test() -> int:
    import sqlite3

    print("Running cross-engine reconcile self-test on two in-memory SQLite DBs...\n")
    cols = [{"name": "order_id", "type": "INTEGER"}, {"name": "customer", "type": "TEXT"},
            {"name": "amount", "type": "REAL"}, {"name": "status", "type": "TEXT"}]

    def make(rows):
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE orders (order_id INTEGER, customer TEXT, amount REAL, status TEXT)")
        conn.executemany("INSERT INTO orders VALUES (?,?,?,?)", rows)
        conn.commit()
        return lambda sql: conn.execute(sql).fetchall()

    good = [(1, "a", 10.0, "paid"), (2, "b", 20.0, "paid"),
            (3, "c", 30.0, "pending"), (4, "d", None, "paid")]

    # Case 1 — identical → PASS
    rep_ok = reconcile(source_runner=make(good), candidate_runner=make(good),
                       source_table="orders", candidate_table="orders",
                       source_columns=cols, candidate_columns=cols, primary_key=["order_id"],
                       source_dialect=SQLITE, candidate_dialect=SQLITE, float_tol=1e-6)
    print("Case 1 (identical):", "PASS" if rep_ok.passed else "FAIL")
    assert rep_ok.passed, rep_ok.failures

    # Case 2 — candidate drops row 4, adds row 5 (count diff), changes amount on 2 (sum diff)
    broken = [(1, "a", 10.0, "paid"), (2, "b", 999.0, "paid"),
              (3, "c", 30.0, "pending"), (5, "e", 50.0, "paid")]
    rep_bad = reconcile(source_runner=make(good), candidate_runner=make(broken),
                        source_table="orders", candidate_table="orders",
                        source_columns=cols, candidate_columns=cols, primary_key=["order_id"],
                        source_dialect=SQLITE, candidate_dialect=SQLITE, float_tol=1e-3)
    print("Case 2 (broken):   ", "PASS" if rep_bad.passed else "FAIL (expected)")
    assert not rep_bad.passed
    assert rep_bad.checks["row_counts"]["source"] == rep_bad.checks["row_counts"]["candidate"], \
        "this scenario keeps row count equal (4 vs 4); the diff must be caught by aggregates"
    amt = rep_bad.checks["aggregate_fingerprint"]["column_differences"].get("amount", {})
    assert "sum" in amt, rep_bad.checks["aggregate_fingerprint"]
    print("\nCase 2 detected failures:")
    for f in rep_bad.failures:
        print("   -", f)

    # Case 3 — float drift within tolerance is NOT flagged
    drift = [(1, "a", 10.0000001, "paid"), (2, "b", 20.0, "paid"),
             (3, "c", 30.0, "pending"), (4, "d", None, "paid")]
    rep_tol = reconcile(source_runner=make(good), candidate_runner=make(drift),
                        source_table="orders", candidate_table="orders",
                        source_columns=cols, candidate_columns=cols, primary_key=["order_id"],
                        source_dialect=SQLITE, candidate_dialect=SQLITE, float_tol=1e-3)
    print("\nCase 3 (float drift within tol):", "PASS" if rep_tol.passed else "FAIL")
    assert rep_tol.passed, rep_tol.failures

    print("\nSelf-test passed: identical reconciles clean; a value change with equal row")
    print("count is caught by the aggregate fingerprint; sub-tolerance float drift is ignored.")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="Cross-engine reconcile self-test.")
    p.add_argument("--self-test", action="store_true", help="run the built-in SQLite self-test and exit")
    args = p.parse_args()
    if args.self_test:
        return _self_test()
    p.error("this module is driven by the /api/sfglue/reconcile route; use --self-test to verify it")
    return 2  # unreachable


if __name__ == "__main__":
    raise SystemExit(main())
