"""Launcher: statement building, log following and failure handling, against a scripted gateway."""

import pytest

from kyuubi_client import errors
from kyuubi_client.launcher import Launcher, RunFailed


class Fake(Launcher):
    """A Launcher whose statements are answered by a script instead of a gateway."""

    def __init__(self, answers):
        super().__init__(client=None)
        self.answers, self.sent = list(answers), []

    def _query(self, statement, timeout=600):
        self.sent.append(statement)
        answer = self.answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def status(phase, exit_code=-1):
    return [{"run": "r1", "job": "j", "phase": phase, "exit_code": exit_code, "seconds": 1.0,
             "summary": "", "message": ""}]


def lines(*texts, start=1):
    return [{"n": start + i, "line": t} for i, t in enumerate(texts)]


def test_submit_builds_an_async_run_statement():
    jobs = Fake([[{"run": "hello-1a2b", "job": "hello", "status": "Submitted"}]])
    assert jobs.submit("hello", name="world", cpu="2", memory="2Gi") == "hello-1a2b"
    assert jobs.sent == ["RUN hello async=true name=world cpu=2 memory=2Gi"]


@pytest.mark.parametrize("job, params", [
    ("Hello", {}), ("hel lo", {}), ("hello", {"cpu": "2; DROP"}), ("hello", {"Cpu": "2"}),
    ("hello", {"name": "a b"}), ("hello", {"x": ""}),
])
def test_submit_refuses_what_the_launcher_would(job, params):
    with pytest.raises(errors.InterfaceError):
        Fake([]).submit(job, **params)


def test_follow_prints_each_line_once_and_returns_the_final_status():
    jobs = Fake([
        status("Pending"), lines(),                                   # nothing yet
        status("Running"), lines("a", "b"),
        status("Running"), lines("c", start=3),
        status("Succeeded", 0), lines("d", start=4), lines(start=5),   # one more line, then the closing read
    ])
    seen = []
    final = jobs.follow("r1", seen.append, poll=0)
    assert seen == ["a", "b", "c", "d"] and final["phase"] == "Succeeded"
    assert "LOGS r1 from=5" in jobs.sent[-1] or "LOGS r1 from=5" in jobs.sent[-2]


def test_follow_raises_runfailed_with_the_status_and_still_prints_the_tail():
    jobs = Fake([status("Failed", 4), lines("boom"), lines(start=2)])
    seen = []
    with pytest.raises(RunFailed) as e:
        jobs.follow("r1", seen.append, poll=0)
    assert seen == ["boom"] and e.value.status["exit_code"] == 4


def test_follow_reports_a_cancelled_run_when_its_pod_is_gone():
    jobs = Fake([errors.QueryError("launcher: Kubernetes API GET /api/v1/...: HTTP 404: pods \"r1\" not found", "raw")])
    with pytest.raises(RunFailed) as e:
        jobs.follow("r1", poll=0)
    assert e.value.status["phase"] == "cancelled"


def test_follow_gives_up_waiting_but_leaves_the_run_going():
    jobs = Fake([status("Running"), lines(), status("Running"), lines()])
    with pytest.raises(errors.OperationalError):
        jobs.follow("r1", poll=0, timeout=0)
    assert not any(s.startswith("CANCEL") for s in jobs.sent)


def test_run_cancels_when_waiting_times_out():
    jobs = Fake([[{"run": "r1", "job": "j", "status": "Submitted"}], status("Running"), lines(), []])
    with pytest.raises(errors.OperationalError):
        jobs.run("j", poll=0, timeout=0)
    assert jobs.sent[-1] == "CANCEL r1"
