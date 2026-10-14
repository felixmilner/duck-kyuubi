"""Client for Apache Kyuubi's REST API (v1), with the duck-kyuubi launcher. See ../README.md."""

from .client import KyuubiClient, Operation, Session
from .credentials import lookup_password
from .dbapi import Connection, Cursor, apilevel, connect, paramstyle, threadsafety
from .launcher import Launcher, RunFailed
from .errors import (
    DatabaseError, DataError, Error, HTTPError, IntegrityError, InterfaceError, InternalError,
    NotSupportedError, OperationalError, ProgrammingError, QueryError, QuerySyntaxError, Warning,
)

__all__ = [
    "KyuubiClient", "Session", "Operation", "Launcher", "RunFailed", "Connection", "Cursor", "connect",
    "apilevel", "paramstyle", "threadsafety", "lookup_password",
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError",
    "HTTPError", "QueryError", "QuerySyntaxError",
]
