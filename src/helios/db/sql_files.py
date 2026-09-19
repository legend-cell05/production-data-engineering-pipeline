"""Loading and rendering of the ``.sql`` files.

SQL lives in ``sql/`` as plain files so it stays readable, diffable, runnable
from ``psql`` and reviewable by someone who does not write Python.

The only templating is the substitution of the four schema placeholders --
``${RAW}``, ``${CORE}``, ``${MART}``, ``${META}``. Those come from ``Settings``,
where they are validated as plain identifiers, so the substitution cannot be
used to inject SQL. Every *value* is passed as a bound parameter.
"""

from __future__ import annotations

import os
from pathlib import Path

from helios.config import PROJECT_ROOT, Settings, get_settings
from helios.exceptions import ConfigurationError

#: Root of the SQL library. ``HELIOS_SQL_DIR`` overrides it for deployments
#: that ship the SQL somewhere other than next to the project root.
SQL_ROOT: Path = (
    Path(os.environ["HELIOS_SQL_DIR"]).resolve()
    if os.environ.get("HELIOS_SQL_DIR")
    else PROJECT_ROOT / "sql"
)

_SAFE_IDENTIFIER_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_")


def load_sql(relative_path: str) -> str:
    """Read a SQL file from ``sql/``.

    Raises:
        ConfigurationError: If the file is missing or resolves outside ``sql/``.
    """
    path = (SQL_ROOT / relative_path).resolve()
    if not path.is_relative_to(SQL_ROOT.resolve()):
        raise ConfigurationError(f"SQL path escapes the sql/ directory: {relative_path}")
    if not path.is_file():
        raise ConfigurationError(f"SQL file not found: {path}")
    return path.read_text(encoding="utf-8")


def render_sql(sql: str, settings: Settings | None = None) -> str:
    """Substitute the schema placeholders.

    A plain ``str.replace`` rather than ``string.Template`` so PostgreSQL
    dollar-quoting (``$fn$ ... $fn$``) survives untouched.
    """
    cfg = settings or get_settings()
    mapping = {
        "${RAW}": cfg.raw_schema,
        "${CORE}": cfg.core_schema,
        "${MART}": cfg.mart_schema,
        "${META}": cfg.meta_schema,
    }
    for name in mapping.values():
        if not name or not set(name) <= _SAFE_IDENTIFIER_CHARS:
            raise ConfigurationError(f"unsafe schema identifier: {name!r}")

    for placeholder, value in mapping.items():
        sql = sql.replace(placeholder, value)
    return sql


def load_rendered_sql(relative_path: str, settings: Settings | None = None) -> str:
    """Read a SQL file and substitute the schema names."""
    return render_sql(load_sql(relative_path), settings)


def split_statements(sql: str) -> list[str]:
    """Split a SQL script into individual statements.

    Needed because a *parameterised* script cannot be sent to PostgreSQL as one
    multi-command string: the extended query protocol used for bound parameters
    accepts a single command per prepared statement. Scripts without parameters
    are sent whole.

    The splitter tracks single quotes, escaped quotes, dollar-quoted blocks and
    ``--`` comments, so a semicolon inside ``'a;b'`` or inside a function body
    does not split a statement in two.
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_single_quote = False
    in_line_comment = False
    dollar_tag: str | None = None
    index = 0

    while index < len(sql):
        char = sql[index]
        nxt = sql[index + 1] if index + 1 < len(sql) else ""

        if in_line_comment:
            buffer.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue

        if dollar_tag is not None:
            if sql.startswith(dollar_tag, index):
                buffer.append(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
            else:
                buffer.append(char)
                index += 1
            continue

        if in_single_quote:
            buffer.append(char)
            if char == "'":
                if nxt == "'":  # '' is an escaped quote, not the end
                    buffer.append(nxt)
                    index += 2
                    continue
                in_single_quote = False
            index += 1
            continue

        if char == "-" and nxt == "-":
            in_line_comment = True
            buffer.append(char)
            index += 1
            continue

        if char == "'":
            in_single_quote = True
            buffer.append(char)
            index += 1
            continue

        if char == "$":
            end = sql.find("$", index + 1)
            candidate = sql[index : end + 1] if end != -1 else ""
            if candidate and all(c.isalnum() or c == "_" for c in candidate[1:-1]):
                dollar_tag = candidate
                buffer.append(candidate)
                index += len(candidate)
                continue

        if char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            index += 1
            continue

        buffer.append(char)
        index += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)

    # Drop fragments that are only comments or whitespace.
    return [
        s
        for s in statements
        if any(line.strip() and not line.strip().startswith("--") for line in s.splitlines())
    ]


def list_sql_files(subdirectory: str) -> list[Path]:
    """Return the ``.sql`` files of a subdirectory, sorted by name.

    File names are numbered so this lexicographic sort is also the correct
    execution order.
    """
    directory = SQL_ROOT / subdirectory
    if not directory.is_dir():
        raise ConfigurationError(f"SQL directory not found: {directory}")
    return sorted(directory.glob("*.sql"))
