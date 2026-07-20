"""Best-effort REAL execution of generated SQL on sample data via DuckDB.

Used (behind the QVF_DUCKDB_VALIDATION flag) by the migration validation flow to
obtain a ground-truth "actual" output for the generated dbt/SQL model instead of
asking the LLM to *simulate* execution. The LLM is still used for the Qlik
("expected") side, which isn't SQL.

Design: pure, optional, and defensive. Any problem (duckdb missing, Jinja we
can't resolve, a SQL feature DuckDB doesn't support, a source table the SQL
references that isn't in the sample) returns None so the caller transparently
falls back to LLM simulation. No web/app dependency.
"""

import logging
import re

logger = logging.getLogger(__name__)

_JINJA_CONFIG = re.compile(r'\{\{\s*config\s*\([^}]*\)\s*\}\}', re.IGNORECASE | re.DOTALL)
_JINJA_REF = re.compile(r"""\{\{\s*ref\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}""", re.IGNORECASE)
_JINJA_SOURCE = re.compile(
    r"""\{\{\s*source\(\s*['"][^'"]+['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}""", re.IGNORECASE
)
_JINJA_THIS = re.compile(r'\{\{\s*this\s*\}\}', re.IGNORECASE)
_DBT_TAG = re.compile(r'\{%.*?%\}', re.DOTALL)       # {% ... %} control tags
_JINJA_ANY = re.compile(r'\{\{.*?\}\}', re.DOTALL)   # any leftover {{ ... }}


def _duckdb():
    try:
        import duckdb
        return duckdb
    except Exception:
        return None


def _q(ident):
    return '"' + str(ident).replace('"', '') + '"'


def strip_dbt_jinja(sql, output_name='migration_output'):
    """Reduce a dbt model to plain SQL DuckDB can run.

    Drops {{ config(...) }} and {% ... %}, rewrites {{ ref('x') }} /
    {{ source('s','x') }} to the bare relation name, and {{ this }} to the
    output name. Any leftover {{ ... }} is removed.
    """
    s = sql or ''
    s = _JINJA_CONFIG.sub('', s)
    s = _DBT_TAG.sub('', s)
    s = _JINJA_REF.sub(lambda m: _q(m.group(1)), s)
    s = _JINJA_SOURCE.sub(lambda m: _q(m.group(1)), s)
    s = _JINJA_THIS.sub(_q(output_name), s)
    s = _JINJA_ANY.sub('', s)
    return s.strip()


def _literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'TRUE' if value else 'FALSE'
    if isinstance(value, (int, float)):
        return repr(value)
    text = str(value)
    # Promote clean numeric strings to numbers so SUM/AVG etc. work.
    cleaned = re.sub(r'[,$\s]', '', text)
    if re.fullmatch(r'-?\d+(\.\d+)?', cleaned):
        return cleaned
    return "'" + text.replace("'", "''") + "'"


def _create_sample_table(con, name, columns, rows):
    if not columns:
        return
    col_idents = ', '.join(_q(c) for c in columns)
    if rows:
        value_rows = []
        for row in rows:
            cells = [_literal(row[i]) if i < len(row) else 'NULL' for i in range(len(columns))]
            value_rows.append('(' + ', '.join(cells) + ')')
        con.execute(
            f'CREATE OR REPLACE TABLE {_q(name)} AS '
            f'SELECT * FROM (VALUES {", ".join(value_rows)}) AS t({col_idents})'
        )
    else:
        cols = ', '.join(f'{_q(c)} VARCHAR' for c in columns)
        con.execute(f'CREATE OR REPLACE TABLE {_q(name)} ({cols})')


def _split_statements(sql):
    """Split on top-level semicolons (ignoring those inside quotes)."""
    out, buf, quote = [], [], None
    for ch in sql:
        if quote:
            buf.append(ch)
            if ch == quote:
                quote = None
        elif ch in ("'", '"'):
            quote = ch
            buf.append(ch)
        elif ch == ';':
            stmt = ''.join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
        else:
            buf.append(ch)
    tail = ''.join(buf).strip()
    if tail:
        out.append(tail)
    return out


def _jsonable(v):
    import datetime
    import decimal
    if isinstance(v, decimal.Decimal):
        f = float(v)
        return int(f) if f == int(f) else f
    if isinstance(v, (datetime.date, datetime.datetime)):
        return v.isoformat()
    return v



def _split_args(argstr):
    """Split a function arg list on top-level commas (respecting parens/quotes)."""
    args, buf, depth, q = [], [], 0, None
    for ch in argstr:
        if q:
            buf.append(ch)
            if ch == q:
                q = None
        elif ch in ("'", '"'):
            q = ch
            buf.append(ch)
        elif ch == '(':
            depth += 1
            buf.append(ch)
        elif ch == ')':
            depth -= 1
            buf.append(ch)
        elif ch == ',' and depth == 0:
            args.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        args.append(''.join(buf).strip())
    return args


def _rewrite_calls(sql, func_name, handler):
    """Replace every ``func_name(...)`` call using balanced-paren arg extraction."""
    pat = re.compile(r'\b' + re.escape(func_name) + r'\s*\(', re.IGNORECASE)
    out, i, n = [], 0, len(sql)
    while True:
        m = pat.search(sql, i)
        if not m:
            out.append(sql[i:])
            break
        out.append(sql[i:m.start()])
        depth, j, q = 0, m.end() - 1, None
        while j < n:
            ch = sql[j]
            if q:
                if ch == q:
                    q = None
            elif ch in ("'", '"'):
                q = ch
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    break
            j += 1
        inner = sql[m.end():j]
        try:
            out.append(handler(_split_args(inner)))
        except Exception:
            out.append(sql[m.start():j + 1])  # leave as-is on any trouble
        i = j + 1
    return ''.join(out)


def _translate_date_format(fmt):
    """Snowflake/Spark date format -> DuckDB strftime tokens (best effort)."""
    f = fmt.strip().strip("'\"")
    repl = [('yyyy', '%Y'), ('YYYY', '%Y'), ('Mon', '%b'), ('MON', '%b'), ('Month', '%B'),
            ('MM', '%m'), ('mm', '%m'), ('DD', '%d'), ('dd', '%d'), ('HH24', '%H'),
            ('HH', '%H'), ('MI', '%M'), ('SS', '%S')]
    for a, b in repl:
        f = f.replace(a, b)
    return f


def _prenormalize_dialect(sql):
    """Rewrite cross-dialect (mostly Snowflake) functions sqlglot won't convert
    cleanly to DuckDB, so a dialect-mixed generated model can still execute."""
    def _try_to_date(args):
        return f"TRY_CAST({args[0]} AS DATE)" if args else "NULL"

    def _to_char(args):
        if len(args) >= 2 and args[1].strip().startswith(("'", '"')):
            return f"strftime({args[0]}, '{_translate_date_format(args[1])}')"
        return f"CAST({args[0]} AS VARCHAR)" if args else "NULL"

    # More Snowflake-only scalar functions DuckDB / sqlglot-databricks don't convert.
    sql = _rewrite_calls(sql, 'IFF', lambda a: f"CASE WHEN {a[0]} THEN {a[1]} ELSE {a[2]} END" if len(a) >= 3 else (a[0] if a else 'NULL'))
    sql = _rewrite_calls(sql, 'ZEROIFNULL', lambda a: f"COALESCE({a[0]}, 0)" if a else 'NULL')
    sql = _rewrite_calls(sql, 'NULLIFZERO', lambda a: f"NULLIF({a[0]}, 0)" if a else 'NULL')
    sql = _rewrite_calls(sql, 'TRY_TO_NUMBER', lambda a: f"TRY_CAST({a[0]} AS DOUBLE)" if a else 'NULL')
    sql = _rewrite_calls(sql, 'TRY_TO_DECIMAL', lambda a: f"TRY_CAST({a[0]} AS DOUBLE)" if a else 'NULL')
    # add_months is Databricks/Spark; DuckDB uses interval arithmetic.
    sql = _rewrite_calls(sql, 'add_months', lambda a: f"(CAST({a[0]} AS DATE) + ({a[1]}) * INTERVAL 1 MONTH)" if len(a) >= 2 else (a[0] if a else 'NULL'))
    sql = _rewrite_calls(sql, 'TRY_TO_DATE', _try_to_date)
    sql = _rewrite_calls(sql, 'TO_DATE', _try_to_date)
    sql = _rewrite_calls(sql, 'TO_CHAR', _to_char)
    sql = re.sub(r'CURRENT_DATE\s*\(\s*\)', 'CURRENT_DATE', sql, flags=re.IGNORECASE)
    sql = _rewrite_calls(sql, 'CAST',
                         lambda a: f"CAST({a[0]})" if a and a[0].upper().endswith(' AS STRING')
                         else f"CAST({', '.join(a)})")
    sql = re.sub(r'\bAS\s+STRING\b', 'AS VARCHAR', sql, flags=re.IGNORECASE)
    return sql


def _read_dialects(dialect):
    d = (dialect or '').strip().lower()
    if d == 'snowflake':
        return ['snowflake', 'databricks', 'spark']
    return ['databricks', 'snowflake', 'spark']


def _to_duckdb_sql(sql, dialect):
    """Transpile dialect SQL to DuckDB statements via sqlglot (after pre-normalising
    Snowflake-isms). Returns a list of statements, or None if sqlglot is missing
    or can't parse under any candidate read dialect."""
    try:
        import sqlglot
    except Exception:
        return None
    pre = _prenormalize_dialect(sql)
    for read in _read_dialects(dialect):
        try:
            stmts = sqlglot.transpile(pre, read=read, write='duckdb', error_level=None)
            if stmts:
                return stmts
        except Exception:
            continue
    return None


def execute_sql_on_sample(code, sample_data, dialect='databricks', output_name='migration_output', max_rows=50):
    """Run ``code`` against ``sample_data`` in an in-memory DuckDB and return the
    final result as {"tables": {output_name: {"columns": [...], "rows": [...]}}},
    or None on any failure (caller falls back to LLM simulation).
    """
    duckdb = _duckdb()
    if not duckdb or not code:
        return None
    tables = (sample_data or {}).get('tables') or {}
    if not tables:
        return None

    con = None
    try:
        con = duckdb.connect(':memory:')
        for name, table in tables.items():
            _create_sample_table(con, name, table.get('columns') or [], table.get('rows') or [])

        sql = strip_dbt_jinja(code, output_name=output_name)
        statements = _to_duckdb_sql(sql, dialect) or _split_statements(sql)
        if not statements:
            return None

        # Execute all statements; the last one that returns rows is the output.
        last_result = None
        for stmt in statements:
            res = con.execute(stmt)
            if res.description:
                last_result = res
        if last_result is None or not last_result.description:
            return None

        columns = [d[0] for d in last_result.description]
        rows = [[_jsonable(c) for c in row] for row in last_result.fetchmany(max_rows)]
        return {'tables': {output_name: {'columns': columns, 'rows': rows}}}
    except Exception:
        return None
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                logger.warning(
                    "duckdb_execution: failed to close DuckDB connection", exc_info=True)
