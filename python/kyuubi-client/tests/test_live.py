"""Against a running Kyuubi (pytest --live; conftest.py has the settings and markers). The SQL tests run
on the duckdb engine, and on Spark if you select it; each engine's tests share one session."""

from datetime import date, datetime
from decimal import Decimal

import pytest

from kyuubi_client import Error, HTTPError, KyuubiClient, Launcher, OperationalError, QueryError, RunFailed, connect

pytestmark = pytest.mark.live

ENGINES = [pytest.param("duckdb", marks=pytest.mark.quack), pytest.param("spark", marks=pytest.mark.spark)]
TRICKY = ["it's", "back\\slash", "100%", "line\nbreak", "ünïcødé ✓", "", "'; SELECT 1; --"]


@pytest.fixture(scope="module", params=ENGINES)
def conn(request, kyuubi_url, user, password, verify):
    with connect(kyuubi_url, user=user, password=password, engine=request.param, timeout=600,
                 verify=verify) as c:
        yield c


def test_types(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT CAST(1 AS INT) AS i, CAST(2 AS BIGINT) AS b, CAST(1.5 AS DOUBLE) AS d,"
        " CAST(1.25 AS DECIMAL(10,2)) AS dec, 'x' AS s, true AS t, CAST(NULL AS STRING) AS n,"
        " DATE '2026-10-03' AS dt, TIMESTAMP '2026-10-03 01:02:03.5' AS ts"
        if conn.session.engine == "spark" else
        "SELECT 1::INT AS i, 2::BIGINT AS b, 1.5::DOUBLE AS d, 1.25::DECIMAL(10,2) AS dec, 'x' AS s,"
        " true AS t, NULL::VARCHAR AS n, DATE '2026-10-03' AS dt, TIMESTAMP '2026-10-03 01:02:03.5' AS ts"
    )
    assert [d[0] for d in cur.description] == ["i", "b", "d", "dec", "s", "t", "n", "dt", "ts"]
    assert cur.description[3][1:] == ("DECIMAL", None, None, 10, 2, None)
    assert cur.fetchall() == [
        (1, 2, 1.5, Decimal("1.25"), "x", True, None, date(2026, 10, 3), datetime(2026, 10, 3, 1, 2, 3, 500000))
    ]


def test_paging(conn):
    cur = conn.cursor()
    cur.page_size = 1000
    cur.execute("SELECT id FROM range(2500) AS t(id) ORDER BY id")
    assert cur.fetchone() == (0,)
    rows = cur.fetchall()
    assert len(rows) == 2499 and rows[-1] == (2499,)


@pytest.mark.parametrize("text", TRICKY)
def test_parameters_round_trip(conn, text):
    cur = conn.cursor()
    cur.execute("SELECT %s AS s, %s AS n, %s AS d", (text, 42, date(2026, 10, 3)))
    assert cur.fetchall() == [(text, 42, date(2026, 10, 3))]


def test_bytes_parameter(conn):
    cur = conn.cursor()
    cur.execute("SELECT octet_length(%s) AS n", (b"\x00\xffhi",))
    assert cur.fetchone() == (4,)


def test_error_message(conn):
    cur = conn.cursor()
    with pytest.raises(QueryError) as e:
        cur.execute("SELEC nope")
    assert "org.apache" not in e.value.message
    assert "syntax" in e.value.message.lower()
    cur.execute("SELECT 1 AS still_works")
    assert cur.fetchone() == (1,)


def test_session_state(conn):
    cur = conn.cursor()
    if conn.session.engine == "spark":
        cur.execute("CREATE OR REPLACE TEMPORARY VIEW client_state AS SELECT 7 AS x")
    else:
        cur.execute("CREATE OR REPLACE TEMP TABLE client_state AS SELECT 7 AS x")
    cur.execute("SELECT x FROM client_state")
    assert cur.fetchall() == [(7,)]


@pytest.mark.quack
def test_timeout_cancels(kyuubi_url, user, password, verify):
    with KyuubiClient(kyuubi_url, user=user, password=password, verify=verify) as client, client.open_session("duckdb") as session:
        session.execute("SELECT 1")  # engine and ATTACH up first
        op = session.execute("SELECT count(*) FROM range(1000000000000)", wait=False)
        with pytest.raises(OperationalError, match="cancelled"):
            op.wait(timeout=3)
        assert op.status() in ("CANCELED_STATE", "CLOSED_STATE", "ERROR_STATE")
        assert session.execute("SELECT 2").fetch() == [(2,)]


@pytest.mark.quack
def test_logs_and_metadata(kyuubi_url, user, password, verify):
    with KyuubiClient(kyuubi_url, user=user, password=password, verify=verify) as client, client.open_session("duckdb") as session:
        op = session.execute("SELECT 1")
        assert any("operation" in line.lower() for line in op.logs())
        assert session.catalogs().fetch()  # the engine-local DuckDB's catalogs, see client.py


@pytest.mark.spark
def test_python(kyuubi_url, user, password, verify):
    """PySpark in the driver: one result row with output and status; state carries over."""
    with KyuubiClient(kyuubi_url, user=user, password=password, verify=verify) as client, client.open_session("spark") as session:
        assert session.execute("x = 21", language="python", timeout=600).fetch()[0][:2] == ("", "ok")
        assert session.execute("x * 2", language="python").fetch()[0][:2] == ("42", "ok")
        assert session.execute("spark.range(5).count() == 5", language="python").fetch()[0][0] == "True"
        with pytest.raises(QueryError) as e:
            session.execute("1/0", language="python")
        assert e.value.message.endswith("ZeroDivisionError: division by zero")


@pytest.mark.parametrize("conf", [{"spark.kubernetes.namespace": "kube-system"},
                                  {"kubernetes.namespace": "kube-system"},
                                  {"kyuubi.engine.jdbc.connection.url": "jdbc:duckdb:/tmp/mine.db"},
                                  {"kyuubi.engine.type": "TRINO"},
                                  {"kyuubi.engine.jdbc.type": "postgresql"}])
def test_session_conf_policy(kyuubi_url, user, password, verify, conf):
    """The session policy (kyuubi-session-policy): config outside the allowlist fails the session."""
    with pytest.raises(HTTPError, match="isn't allowed"):
        connect(kyuubi_url, user=user, password=password, conf=conf, verify=verify)


def test_closed_connection(kyuubi_url, user, password, verify):
    conn = connect(kyuubi_url, user=user, password=password, verify=verify)
    cur = conn.cursor()
    conn.close()
    from kyuubi_client import InterfaceError
    with pytest.raises(InterfaceError):
        cur.execute("SELECT 1")


@pytest.mark.auth
@pytest.mark.parametrize("password", ["", "wrong"])
def test_login_fails(kyuubi_url, user, password, verify):
    with pytest.raises(HTTPError, match="login failed") as e:
        connect(kyuubi_url, user=user, password=password, verify=verify)
    assert e.value.status in (401, 403)


# ---------------------------------------------------------------- the launcher (examples/jobs/catalog.json)

@pytest.fixture
def jobs(kyuubi_url, user, password, verify):
    with KyuubiClient(kyuubi_url, user=user, password=password, verify=verify) as client, Launcher(client) as jobs:
        yield jobs


def test_launcher_lists_the_catalog(jobs):
    catalog = {j["job"]: j for j in jobs.list()}
    assert "hello" in catalog and '"seconds"' in catalog["hello"]["params"]


@pytest.mark.parametrize("statement, message", [
    ("SELECT 42", "launcher sessions accept only"),
    ("RUN nope", "no job nope in the catalog"),
    ("RUN hello image=evil", "unknown parameter image"),
    ("RUN hello seconds=100000", "must be between"),
])
def test_launcher_refuses(jobs, statement, message):
    with pytest.raises(Error, match=message):
        jobs._query(statement)


@pytest.mark.k8s
def test_launcher_runs_a_job(jobs):
    lines = []
    status = jobs.run("hello", seconds="3", name="live-test", on_line=lines.append, timeout=300)
    assert status["phase"] == "Succeeded" and status["exit_code"] == 0
    assert '"hello": "live-test"' in status["summary"]
    assert "hello, live-test" in lines and "tick 3 of 3" in lines


@pytest.mark.k8s
def test_launcher_reports_a_failed_job(jobs):
    with pytest.raises(RunFailed) as e:
        jobs.run("hello", seconds="1", exit="4", timeout=300)
    assert e.value.status["exit_code"] == 4
    assert e.value.status["message"] == "the exit code you asked for"
