"""Exceptions, laid out as PEP 249 requires so the DB-API layer can raise them as they are."""

import json
import re


class Warning(Exception):  # noqa: A001 (PEP 249 name)
    pass


class Error(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    """The gateway couldn't be reached or answered with an HTTP error, or a wait timed out."""


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


class HTTPError(OperationalError):
    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


class QueryError(DatabaseError):
    """A statement ended in ERROR_STATE. `message` is the engine's error, `raw` Kyuubi's full one."""

    def __init__(self, message: str, raw: str, sqlstate: str | None = None):
        super().__init__(message)
        self.message = message
        self.raw = raw
        self.sqlstate = sqlstate


class QuerySyntaxError(QueryError, ProgrammingError):
    """A QueryError with SQLSTATE class 42 (syntax error or access rule violation)."""


# Kyuubi wraps the engine's error as "org.apache.kyuubi.KyuubiSQLException: Error operating
# ExecuteStatement: java.sql.SQLException: Parser Error: ...". Strip the Java class names and the
# operation prefix; engine messages such as "Parser Error:" have no dots, so they stay.
_WRAPPER = re.compile(r"^\s*(?:(?:[a-z][\w$]*\.)+[A-Z][\w$]*:|Error operating \w+:)\s*")
_STACK = re.compile(r"\n\s+at [\w$.<>]+\(.*", re.S)
_PYTHON = "Interpret error:"


def _python_error(message: str) -> str | None:
    """Python statements on Spark fail with "Interpret error:" and the worker's JSON reply; return
    its traceback as Python prints it."""
    if not message.startswith(_PYTHON):
        return None
    try:
        reply, _ = json.JSONDecoder().raw_decode(message[len(_PYTHON):].lstrip())
        content = reply["response"]["content"]
    except (ValueError, KeyError, TypeError):
        return None
    return "".join(content.get("traceback") or []).rstrip() or f"{content.get('ename')}: {content.get('evalue')}"


def query_error(exception: dict | str | None) -> QueryError:
    if isinstance(exception, dict):
        raw = exception.get("message") or ""
        sqlstate = exception.get("sqlstate") or exception.get("SQLState")
    else:
        raw, sqlstate = exception or "", None
    message = _STACK.sub("", raw)
    while (stripped := _WRAPPER.sub("", message, count=1)) != message:
        message = stripped
    message = _python_error(message) or message.strip() or "statement failed"
    cls = QuerySyntaxError if (sqlstate or "").startswith("42") else QueryError
    return cls(message, raw, sqlstate)
