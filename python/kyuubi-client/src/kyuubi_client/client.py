"""Kyuubi REST API (v1): transport, sessions and operations.

    with KyuubiClient("http://localhost:10099", user="alice") as client:
        with client.open_session(engine="spark") as session:
            op = session.execute("SELECT 1 AS x")
            print(op.description, op.fetch())
"""

import time
from collections.abc import Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import errors, types

# engine name -> session config; None keeps the server default (its kyuubi.engine.type)
ENGINES = {
    "duckdb": {"kyuubi.engine.type": "JDBC"},
    "spark": {"kyuubi.engine.type": "SPARK_SQL"},
}
LANGUAGES = {"sql": "SQL", "python": "PYTHON", "scala": "SCALA"}

FINISHED = "FINISHED_STATE"
TERMINAL = {FINISHED, "ERROR_STATE", "CANCELED_STATE", "CLOSED_STATE", "TIMEOUT_STATE", "UNKNOWN_STATE"}


class KyuubiClient:
    """HTTP transport. Only GETs are retried (connection errors, 502/503/504), since a repeated POST
    could start a second statement or session. verify: True (system CAs), a CA file (e.g. your
    cluster's CA certificate), or False."""

    def __init__(
        self,
        url: str,
        user: str,
        password: str = "",
        timeout: float = 60,
        retries: int = 3,
        verify: bool | str = True,
    ):
        self.base = url.rstrip("/") + "/api/v1"
        self.user = user
        self.timeout = timeout
        self.http = requests.Session()
        self.http.auth = (user, password)
        self.http.verify = verify
        retry = Retry(
            total=retries,
            backoff_factor=0.5,
            status_forcelist=(502, 503, 504),
            allowed_methods={"GET"},
            raise_on_status=False,
        )
        self.http.mount("http://", HTTPAdapter(max_retries=retry))
        self.http.mount("https://", HTTPAdapter(max_retries=retry))

    def request(self, method: str, path: str, json=None, params=None):
        try:
            response = self.http.request(
                method, self.base + path, json=json, params=params, timeout=self.timeout
            )
        except requests.RequestException as e:
            raise errors.OperationalError(f"Kyuubi unreachable at {self.base}: {e}") from e
        if response.status_code in (401, 403):   # Jetty's HTML error page; nothing more to say
            raise errors.HTTPError(response.status_code, f"login failed for user {self.user!r}")
        if not response.ok:
            try:
                message = response.json().get("message") or response.text
            except ValueError:
                message = response.text
            raise errors.HTTPError(response.status_code, message.split("\n\tat ")[0].strip())
        return response.json() if response.content else None

    def ping(self) -> bool:
        """True if the server answers, even if the login fails (open_session reports that)."""
        try:
            return self.http.get(self.base + "/ping", timeout=self.timeout).status_code < 500
        except requests.RequestException:
            return False

    def open_session(self, engine: str | None = None, conf: dict | None = None) -> "Session":
        configs = dict(ENGINES[engine]) if engine else {}
        configs.update(conf or {})
        body = self.request("POST", "/sessions", json={"configs": configs})
        return Session(self, body["identifier"], engine)

    def close(self):
        self.http.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Session:
    """One Kyuubi session: one engine connection, so state (temp tables, USE, transactions) carries
    over between statements. The engine itself starts on the first statement, not on open."""

    def __init__(self, client: KyuubiClient, identifier: str, engine: str | None):
        self.client = client
        self.id = identifier
        self.engine = engine
        self.closed = False

    def execute(
        self,
        statement: str,
        language: str = "sql",
        conf: dict | None = None,
        wait: bool = True,
        timeout: float | None = None,
    ) -> "Operation":
        """Runs `statement` (SQL, or Python/Scala on Spark). With wait=True, returns once it
        finished and raises QueryError if it failed."""
        overlay = dict(conf or {})
        if language != "sql":
            overlay["kyuubi.operation.language"] = LANGUAGES[language]
        body = {"statement": statement, "runAsync": True}
        if overlay:
            body["confOverlay"] = overlay
        op = self._operation("statement", body, statement)
        if wait:
            op.wait(timeout)
        return op

    # Metadata calls. With the duckdb dialect these still describe the engine's local DuckDB, not
    # the Quack server; query information_schema instead.
    def catalogs(self) -> "Operation":
        return self._operation("catalogs", {}).wait()

    def schemas(self, catalog: str | None = None, schema: str | None = None) -> "Operation":
        return self._operation("schemas", {"catalogName": catalog, "schemaName": schema}).wait()

    def tables(self, catalog=None, schema=None, table=None, table_types: list[str] | None = None):
        body = {"catalogName": catalog, "schemaName": schema, "tableName": table, "tableTypes": table_types}
        return self._operation("tables", body).wait()

    def columns(self, catalog=None, schema=None, table=None, column=None) -> "Operation":
        body = {"catalogName": catalog, "schemaName": schema, "tableName": table, "columnName": column}
        return self._operation("columns", body).wait()

    def _operation(self, kind: str, body: dict, statement: str | None = None) -> "Operation":
        if self.closed:
            raise errors.InterfaceError("session is closed")
        body = {k: v for k, v in body.items() if v is not None}
        result = self.client.request("POST", f"/sessions/{self.id}/operations/{kind}", json=body)
        return Operation(self.client, result["identifier"], statement)

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.client.request("DELETE", f"/sessions/{self.id}")
            except errors.HTTPError as e:
                if e.status != 404:  # already gone (server restart, idle timeout)
                    raise

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Operation:
    def __init__(self, client: KyuubiClient, identifier: str, statement: str | None = None):
        self.client = client
        self.id = identifier
        self.statement = statement
        self.state: str | None = None
        self.event: dict = {}
        self._columns: list[dict] | None = None
        self.closed = False

    def status(self) -> str:
        self.event = self.client.request("GET", f"/operations/{self.id}/event")
        self.state = self.event["state"]
        return self.state

    def wait(self, timeout: float | None = None, poll: float = 0.1, max_poll: float = 1.0) -> "Operation":
        """Polls until the operation ends. Ctrl-C or the timeout cancels it on the server."""
        deadline = None if timeout is None else time.monotonic() + timeout
        try:
            while self.status() not in TERMINAL:
                if deadline is not None and time.monotonic() >= deadline:
                    self.cancel()
                    raise errors.OperationalError(f"statement did not finish within {timeout}s; cancelled")
                time.sleep(poll)
                poll = min(poll * 1.5, max_poll)
        except KeyboardInterrupt:
            self.cancel()
            raise
        if self.state == "ERROR_STATE":
            raise errors.query_error(self.event.get("exception"))
        if self.state != FINISHED:
            raise errors.OperationalError(f"statement ended in {self.state}")
        return self

    @property
    def columns(self) -> list[dict]:
        """resultsetmetadata columns: columnName, dataType ("INT_TYPE"...), precision, scale."""
        if self._columns is None:
            self._columns = self.client.request("GET", f"/operations/{self.id}/resultsetmetadata")["columns"]
        return self._columns

    @property
    def description(self) -> list[tuple]:
        """PEP 249 description: (name, type_code, display_size, internal_size, precision, scale,
        null_ok), type_code being the Kyuubi type without "_TYPE", e.g. "DECIMAL"."""
        return [
            (c["columnName"], types.type_name(c["dataType"]), None, None,
             c.get("precision") or None, c.get("scale") or None, None)
            for c in self.columns
        ]

    def fetch(self, max_rows: int = 1000) -> list[tuple]:
        """The next page of rows as tuples; [] once all rows are read."""
        page = self.client.request("GET", f"/operations/{self.id}/rowset", params={"maxrows": max_rows})
        kinds = [c["dataType"] for c in self.columns]
        return [
            tuple(types.convert(kind, field["value"]) for kind, field in zip(kinds, row["fields"]))
            for row in page["rows"]
        ]

    def rows(self, page_size: int = 1000) -> Iterator[tuple]:
        while page := self.fetch(page_size):
            yield from page

    def logs(self, max_rows: int = 1000) -> list[str]:
        """The next lines of the operation log."""
        return self.client.request("GET", f"/operations/{self.id}/log", params={"maxrows": max_rows})["logRowSet"]

    def cancel(self):
        self.client.request("PUT", f"/operations/{self.id}", json={"action": "cancel"})

    def close(self):
        if not self.closed:
            self.closed = True
            try:
                self.client.request("PUT", f"/operations/{self.id}", json={"action": "close"})
            except errors.HTTPError:
                pass  # closed with its session, or the server restarted

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
