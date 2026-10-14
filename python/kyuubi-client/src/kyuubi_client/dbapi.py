"""PEP 249 (DB-API 2.0) over the REST client: a Connection is one Kyuubi session, a cursor's
execute() one operation.

    conn = connect("http://localhost:10099", user="alice", engine="duckdb")
    cur = conn.cursor()
    cur.execute("SELECT * FROM range(10) t(i) WHERE i > %s", (5,))
    print(cur.fetchall())

Kyuubi's REST API has no bind parameters, so parameters are quoted into the SQL client-side
(paramstyle "pyformat": %s or %(name)s; with parameters, a literal % is written %%). Statements
autocommit; commit() is a no-op and rollback() isn't supported (use BEGIN/ROLLBACK statements).
"""

import math
from datetime import date, datetime, time
from decimal import Decimal

from . import errors
from .client import KyuubiClient, Operation, Session
from .errors import (  # noqa: F401 (PEP 249 module attributes)
    DatabaseError, DataError, Error, IntegrityError, InterfaceError, InternalError,
    NotSupportedError, OperationalError, ProgrammingError, Warning,
)

apilevel = "2.0"
threadsafety = 1  # threads may share the module, not connections
paramstyle = "pyformat"


def connect(
    url: str,
    user: str,
    password: str = "",
    engine: str | None = None,
    conf: dict | None = None,
    timeout: float | None = None,
    verify: bool | str = True,
) -> "Connection":
    """Opens a Kyuubi session. engine: "duckdb", "spark", or None for the server default.
    timeout: per-statement limit in seconds (None waits indefinitely). verify: see KyuubiClient."""
    client = KyuubiClient(url, user, password, verify=verify)
    try:
        session = client.open_session(engine, conf)
    except Exception:
        client.close()
        raise
    return Connection(client, session, timeout)


class Connection:
    def __init__(self, client: KyuubiClient, session: Session, timeout: float | None = None):
        self.client = client
        self.session = session
        self.timeout = timeout
        self.closed = False

    def cursor(self) -> "Cursor":
        self._check()
        return Cursor(self)

    def commit(self):
        self._check()

    def rollback(self):
        raise NotSupportedError("statements autocommit; use BEGIN/ROLLBACK statements")

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.session.close()
            finally:
                self.client.close()

    def _check(self):
        if self.closed:
            raise InterfaceError("connection is closed")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Cursor:
    arraysize = 1
    page_size = 1000  # rows per REST fetch

    def __init__(self, connection: Connection):
        self.connection = connection
        self.operation: Operation | None = None
        self.description = None
        self.rowcount = -1
        self._buffer: list[tuple] = []
        self._exhausted = True
        self.closed = False

    def execute(self, operation: str, parameters=None):
        self._check()
        self._close_operation()
        sql = render(operation, parameters, self.connection.session.engine)
        self.operation = self.connection.session.execute(sql, timeout=self.connection.timeout)
        self.description = self.operation.description or None
        self._exhausted = self.description is None
        return self

    def executemany(self, operation: str, seq_of_parameters):
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)
        return self

    def fetchone(self):
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int | None = None) -> list[tuple]:
        size = self.arraysize if size is None else size
        while len(self._buffer) < size and self._fill():
            pass
        rows, self._buffer = self._buffer[:size], self._buffer[size:]
        return rows

    def fetchall(self) -> list[tuple]:
        while self._fill():
            pass
        rows, self._buffer = self._buffer, []
        return rows

    def __iter__(self):
        while (row := self.fetchone()) is not None:
            yield row

    def _fill(self) -> bool:
        if self.operation is None:
            raise ProgrammingError("no statement has been executed")
        if self._exhausted:
            return False
        page = self.operation.fetch(self.page_size)
        self._buffer.extend(page)
        self._exhausted = len(page) == 0
        return bool(page)

    def setinputsizes(self, sizes):
        pass

    def setoutputsize(self, size, column=None):
        pass

    def close(self):
        self.closed = True
        self._close_operation()

    def _close_operation(self):
        if self.operation is not None:
            self.operation.close()
        self.operation, self.description, self._buffer, self._exhausted = None, None, [], True

    def _check(self):
        if self.closed:
            raise InterfaceError("cursor is closed")
        self.connection._check()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def render(operation: str, parameters, engine: str | None) -> str:
    if parameters is None:
        return operation
    if isinstance(parameters, dict):
        return operation % {k: quote(v, engine) for k, v in parameters.items()}
    return operation % tuple(quote(v, engine) for v in parameters)


def quote(value, engine: str | None = None) -> str:
    """A SQL literal for `value`. Spark SQL treats backslash as an escape in string literals;
    DuckDB (the default engine) doesn't, and escapes a quote by doubling it."""
    spark = engine == "spark"
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise NotSupportedError(f"can't quote {value!r}")
        return repr(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise NotSupportedError(f"can't quote {value!r}")
        return str(value)
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, time):
        if spark:
            raise NotSupportedError("Spark SQL has no TIME type")
        return f"TIME '{value.isoformat()}'"
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        return f"X'{data.hex()}'" if spark else "'" + "".join(f"\\x{b:02X}" for b in data) + "'::BLOB"
    if isinstance(value, str):
        if spark:
            return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"
        return "'" + value.replace("'", "''") + "'"
    raise NotSupportedError(f"can't quote a {type(value).__name__}")
