"""KyuubiJobOperator against a scripted launcher: result, failure, skip, cancel on kill."""

import contextlib

import pytest

pytest.importorskip("airflow")
try:   # Airflow 3, else 2
    from airflow.sdk.exceptions import AirflowException, AirflowSkipException   # noqa: E402
except ImportError:
    from airflow.exceptions import AirflowException, AirflowSkipException   # noqa: E402

from airflow_kyuubi import operators                                      # noqa: E402
from airflow_kyuubi.operators import KyuubiJobOperator                    # noqa: E402
from kyuubi_client import RunFailed, errors                               # noqa: E402


class FakeLauncher:
    def __init__(self, client, script):
        self.script, self.submitted, self.cancelled = script, [], []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def submit(self, job, **params):
        self.submitted.append((job, params))
        return "r-1"

    def follow(self, run, on_line, poll, timeout):
        for line in self.script.get("lines", []):
            on_line(line)
        if "raises" in self.script:
            raise self.script["raises"]
        return self.script["status"]

    def cancel(self, run):
        self.cancelled.append(run)


@pytest.fixture
def operator(monkeypatch):
    holder = {}

    def make(script, **kw):
        launcher = FakeLauncher(None, script)
        holder["launcher"] = launcher
        monkeypatch.setattr(operators, "Launcher", lambda client: launcher)
        op = KyuubiJobOperator(task_id="t", job="hello", job_params={"tables": "a", "shard": 0}, **kw)
        monkeypatch.setattr(op, "_client", lambda: contextlib.nullcontext(object()))
        return op, launcher
    return make


class Ti:
    def __init__(self):
        self.pushed = {}

    def xcom_push(self, key, value):
        self.pushed[key] = value


def test_returns_the_run_summary_and_passes_parameters_as_strings(operator):
    op, launcher = operator({"lines": ["a", "b"], "status": {"phase": "Succeeded", "seconds": 7.0,
                                                           "summary": '{"tables_ok": 1, "files": 300}'}})
    ti = Ti()
    assert op.execute({"ti": ti}) == {"tables_ok": 1, "files": 300}
    assert launcher.submitted == [("hello", {"tables": "a", "shard": "0"})]
    assert ti.pushed == {"kyuubi_run": "r-1"}


def test_a_failed_run_fails_the_task_with_its_exit_code(operator):
    status = {"run": "r-1", "phase": "Failed", "exit_code": 1, "message": "a table failed or a balance control broke"}
    op, _ = operator({"raises": RunFailed(status)})
    with pytest.raises(AirflowException, match="exit code 1.*balance control"):
        op.execute({})


def test_chosen_exit_codes_skip_the_task(operator):
    op, _ = operator({"raises": RunFailed({"run": "r-1", "phase": "Failed", "exit_code": 4})}, skip_on_exit_codes=[4])
    with pytest.raises(AirflowSkipException):
        op.execute({})


def test_giving_up_waiting_cancels_the_run(operator):
    op, launcher = operator({"raises": errors.OperationalError("still Running after 5s")}, run_timeout=5)
    with pytest.raises(errors.OperationalError):
        op.execute({})
    assert launcher.cancelled == ["r-1"]


def test_killing_the_task_cancels_the_run(operator):
    op, launcher = operator({"status": {"phase": "Succeeded", "seconds": 1, "summary": ""}})
    op.execute({})
    launcher.cancelled.clear()
    op.on_kill()
    assert launcher.cancelled == ["r-1"]
    op2, l2 = operator({"status": {"phase": "Succeeded", "seconds": 1, "summary": ""}}, cancel_on_kill=False)
    op2.execute({})
    op2.on_kill()
    assert l2.cancelled == []


def test_a_summary_that_is_not_json_is_returned_as_text(operator):
    op, _ = operator({"status": {"phase": "Succeeded", "seconds": 1, "summary": "done"}})
    assert op.execute({}) == {"summary": "done"}


def test_job_and_params_are_templated():
    assert {"job", "job_params"} <= set(KyuubiJobOperator.template_fields)
