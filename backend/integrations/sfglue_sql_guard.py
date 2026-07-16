"""Pre-execution SQL safety gate for the Snowflake+Glue → Databricks flow.

The ``/api/sfglue/deploy`` and ``/api/sfglue/build`` routes execute SQL that
originates from AI generation and is then editable in the browser before being
sent back — i.e. fully client-controlled by the time it reaches the warehouse.
Unlike the QVF flow (which runs ``validate_migration_sql`` before shipping),
these routes previously executed whatever arrived. This module is the missing
gate: it enforces that each statement is a single, CREATE-leading statement, so
a ``DROP``/``TRUNCATE``/``DELETE``/``GRANT`` — or a piggy-backed second
statement (``CREATE …; DROP …``) — is rejected before it runs.

``assert_safe_where`` guards the reconcile route's freeform ``WHERE`` filter,
which is spliced into live queries on both engines.
"""

from __future__ import annotations

import re

__all__ = ['UnsafeSqlError', 'assert_safe_ddl', 'assert_safe_where', 'quote_ident']


class UnsafeSqlError(ValueError):
    """Raised when a client-supplied statement fails the safety gate.

    ValueError subclass so the routes' broad handlers surface it as a 400.
    """


def quote_ident(ident: str) -> str:
    """Backtick-quote a Databricks identifier, escaping embedded backticks.

    Mirrors the escaping already used in reconcile.py / qvd_routes.py so a name
    containing a backtick can't break out of the intended identifier.
    """
    return '`' + str(ident or '').replace('`', '``') + '`'


def _mask(sql: str) -> str:
    """Blank comments and string literals, preserving length/offsets."""
    def blank(m: re.Match) -> str:
        return ' ' * len(m.group(0))

    masked = re.sub(r'/\*[\s\S]*?\*/', blank, sql)
    masked = re.sub(r'--[^\n\r]*', blank, masked)
    masked = re.sub(r"'(?:''|[^'])*'", blank, masked)
    return masked


def _split_statements(masked: str, original: str) -> list[tuple[str, str]]:
    """Split on top-level ``;`` using masked text; return (original, masked) slices.

    Filtering and keyword checks must use the MASKED slice: a slice that is only
    comments/whitespace is not a statement (so ``CREATE …; -- trailing note`` is
    one statement, not two), and a leading ``-- provenance comment`` line —
    which every generated DDL carries — must not hide the CREATE verb.
    """
    parts, start = [], 0
    for i, ch in enumerate(masked):
        if ch == ';':
            parts.append((original[start:i], masked[start:i]))
            start = i + 1
    parts.append((original[start:], masked[start:]))
    return [p for p in parts if p[1].strip()]


def assert_safe_ddl(sql: str, *, label: str = 'statement') -> None:
    """Reject anything that isn't a single CREATE-leading statement.

    Allowed: exactly one statement (a trailing ``;`` is fine) whose first
    keyword is CREATE — covers ``CREATE TABLE``, ``CREATE OR REPLACE TABLE/VIEW``,
    ``CREATE SCHEMA``. Everything else (DROP/TRUNCATE/DELETE/ALTER/GRANT/INSERT/
    MERGE/CALL, multiple statements, empty input) raises :class:`UnsafeSqlError`.
    Sub-selects/CTEs inside the single CREATE are fine — they aren't separate
    statements.
    """
    text = str(sql or '')
    masked = _mask(text)
    statements = _split_statements(masked, text)
    if not statements:
        raise UnsafeSqlError(f'Empty {label} — nothing to execute.')
    if len(statements) > 1:
        raise UnsafeSqlError(
            f'{label} contains {len(statements)} statements; only a single '
            'CREATE statement may be executed here.'
        )
    # Verb check on the MASKED slice: comments are blanked to spaces there, so a
    # leading '-- migrated from …' header can't shadow (or spoof) the keyword.
    lead = re.match(r'\s*([A-Za-z_]+)', statements[0][1])
    verb = (lead.group(1) if lead else '').upper()
    if verb != 'CREATE':
        raise UnsafeSqlError(
            f'{label} must be a CREATE statement (got {verb or "empty"!r}). '
            'DROP/TRUNCATE/DELETE/ALTER/GRANT and similar are not permitted.'
        )


# Statement terminators / dangerous verbs that must never appear in a filter.
_WHERE_FORBIDDEN = re.compile(
    r';|--|/\*|\b(DROP|TRUNCATE|DELETE|ALTER|GRANT|REVOKE|INSERT|UPDATE|MERGE|'
    r'CREATE|CALL|EXECUTE|COPY|MOVE)\b',
    re.IGNORECASE,
)


def assert_safe_where(where: str | None) -> str | None:
    """Validate a freeform reconcile WHERE filter (spliced into live queries).

    A boolean predicate is legitimately freeform, so this can't be fully
    parameterized; instead it blocks statement terminators, comment starters,
    and DDL/DML verbs, so the clause can only ever be a row filter. Returns the
    trimmed clause (or None); raises :class:`UnsafeSqlError` on rejection.
    """
    if where is None:
        return None
    clause = str(where).strip()
    if not clause:
        return None
    if _WHERE_FORBIDDEN.search(clause):
        raise UnsafeSqlError(
            'Reconcile filter may only be a row-filter predicate — statement '
            'separators, comments, and DDL/DML keywords are not allowed.'
        )
    return clause
