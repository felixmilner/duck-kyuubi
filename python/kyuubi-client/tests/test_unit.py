"""Offline tests: value conversion, error cleanup, quoting and the cursor's paging."""

import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from kyuubi_client import dbapi, errors, types


@pytest.mark.parametrize(
    "kind, value, expected",
    [
        ("INT_TYPE", 1, 1),
        ("DECIMAL_TYPE", "1.25", Decimal("1.25")),
        ("DATE_TYPE", "2026-10-03", date(2026, 10, 3)),
        ("TIMESTAMP_TYPE", "2026-10-03 01:02:03.5", datetime(2026, 10, 3, 1, 2, 3, 500000)),
        ("TIMESTAMP_TYPE", "2026-10-03 01:02:03", datetime(2026, 10, 3, 1, 2, 3)),
        ("TIMESTAMP_TYPE", "2026-10-03T01:02:03.123456789", datetime(2026, 10, 3, 1, 2, 3, 123456)),
        ("TIMESTAMP_TYPE", "2026-10-03T01:02:03.1Z", datetime(2026, 10, 3, 1, 2, 3, 100000, timezone.utc)),
        ("TIMESTAMP_TYPE", "2026-10-03 01:02:03-05:30",
         datetime(2026, 10, 3, 1, 2, 3, tzinfo=timezone(-timedelta(hours=5, minutes=30)))),
        ("TIMESTAMP_TYPE", "infinity", "infinity"),
        ("STRING_TYPE", "[1, 2]", "[1, 2]"),
        ("DECIMAL_TYPE", None, None),
    ],
)
def test_convert(kind, value, expected):
    assert types.convert(kind, value) == expected


def test_error_message_is_unwrapped():
    raw = ("org.apache.kyuubi.KyuubiSQLException: Error operating ExecuteStatement: "
           "java.sql.SQLException: Parser Error: syntax error at or near \"nope\"\n"
           "\tat org.apache.kyuubi.Foo.bar(Foo.scala:1)\n\tat x.y.Z.q(Z.java:2)")
    e = errors.query_error({"message": raw})
    assert type(e) is errors.QueryError
    assert e.message == 'Parser Error: syntax error at or near "nope"'
    assert e.raw == raw


def test_python_error_is_its_traceback():
    reply = {"code": "1/0", "response": {"msg_type": "execute_reply", "content": {
        "data": {}, "ename": "ZeroDivisionError", "evalue": "division by zero", "status": "error",
        "traceback": ["Traceback (most recent call last):\n", "ZeroDivisionError: division by zero\n"]}}}
    raw = ("org.apache.kyuubi.KyuubiSQLException: Error operating ExecutePython: "
           "org.apache.kyuubi.KyuubiSQLException: Interpret error:\n" + json.dumps(reply, indent=2)
           + "\n\tat org.apache.kyuubi.Foo.bar(Foo.scala:1)")
    e = errors.query_error({"message": raw})
    assert e.message == "Traceback (most recent call last):\nZeroDivisionError: division by zero"


def test_error_sqlstate_42_is_programming_error():
    e = errors.query_error({"message": "org.apache.spark.sql.AnalysisException: [X] y", "sqlstate": "42P01"})
    assert isinstance(e, errors.ProgrammingError) and e.message == "[X] y"


@pytest.mark.parametrize(
    "value, duckdb, spark",
    [
        (None, "NULL", "NULL"),
        (True, "TRUE", "TRUE"),
        (3, "3", "3"),
        (1.5, "1.5", "1.5"),
        (Decimal("1.10"), "1.10", "1.10"),
        ("it's", "'it''s'", "'it\\'s'"),
        ("a\\b", "'a\\b'", "'a\\\\b'"),
        (date(2026, 10, 3), "DATE '2026-10-03'", "DATE '2026-10-03'"),
        (datetime(2026, 10, 3, 1, 2, 3, 500), "TIMESTAMP '2026-10-03 01:02:03.000500'",
         "TIMESTAMP '2026-10-03 01:02:03.000500'"),
        (b"\x00\xff", "'\\x00\\xFF'::BLOB", "X'00ff'"),
    ],
)
def test_quote(value, duckdb, spark):
    assert dbapi.quote(value, "duckdb") == duckdb
    assert dbapi.quote(value, None) == duckdb
    assert dbapi.quote(value, "spark") == spark


def test_quote_rejects():
    for value in (float("nan"), Decimal("Infinity"), [1], object()):
        with pytest.raises(errors.NotSupportedError):
            dbapi.quote(value)
    with pytest.raises(errors.NotSupportedError):
        dbapi.quote(time(1, 2), "spark")


def test_render():
    assert dbapi.render("SELECT 100%", None, None) == "SELECT 100%"
    assert dbapi.render("SELECT %s, 100%%", ("x",), None) == "SELECT 'x', 100%"
    assert dbapi.render("SELECT %(a)s", {"a": 1}, None) == "SELECT 1"


class FakeOperation:
    def __init__(self, rows, page_cap=3):
        self.rows, self.page_cap, self.fetches, self.closed = list(rows), page_cap, 0, False
        self.description = [("x", "INT", None, None, None, None, None)]

    def fetch(self, max_rows):
        self.fetches += 1
        n = min(max_rows, self.page_cap)
        page, self.rows = self.rows[:n], self.rows[n:]
        return page

    def close(self):
        self.closed = True


class FakeSession:
    engine = None

    def __init__(self, rows):
        self.rows, self.statements = rows, []

    def execute(self, sql, timeout=None):
        self.statements.append(sql)
        return FakeOperation(self.rows)


def cursor(rows):
    session = FakeSession(rows)
    return dbapi.Connection(client=None, session=session).cursor(), session


def test_cursor_pages_through_all_rows():
    cur, session = cursor([(i,) for i in range(8)])
    cur.execute("SELECT %s", ("it's",))
    assert session.statements == ["SELECT 'it''s'"]
    assert cur.description[0][0] == "x"
    assert cur.fetchone() == (0,)
    assert cur.fetchmany(4) == [(1,), (2,), (3,), (4,)]
    assert cur.fetchall() == [(5,), (6,), (7,)]
    assert cur.fetchone() is None and cur.fetchall() == []


def test_cursor_closes_previous_operation():
    cur, _ = cursor([(1,)])
    cur.execute("SELECT 1")
    first = cur.operation
    cur.execute("SELECT 1")
    assert first.closed and not cur.operation.closed
    cur.close()
    with pytest.raises(errors.InterfaceError):
        cur.execute("SELECT 1")


def test_fetch_before_execute():
    cur, _ = cursor([])
    with pytest.raises(errors.ProgrammingError):
        cur.fetchall()


def test_failed_login_message(monkeypatch):
    from kyuubi_client import KyuubiClient

    class Response:
        status_code, ok, content = 403, False, b"<html>...</html>"

    client = KyuubiClient("http://kyuubi", user="alice", password="wrong")
    monkeypatch.setattr(client.http, "request", lambda *a, **k: Response())
    with pytest.raises(errors.HTTPError, match="login failed for user 'alice'") as e:
        client.request("POST", "/sessions", json={})
    assert e.value.status == 403
