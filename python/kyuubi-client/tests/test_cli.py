"""The kyuubi command: statement splitting, settings and output, offline; one live run of each
command."""

import io
import json
from datetime import date
from decimal import Decimal

import pytest

from kyuubi_client import cli, errors


@pytest.mark.parametrize(
    "text, backslash, statements, rest",
    [
        ("SELECT 1; SELECT 2;", False, ["SELECT 1", "SELECT 2"], ""),
        ("SELECT 1; SELECT", False, ["SELECT 1"], " SELECT"),
        ("SELECT 'a;b'; SELECT \"c;d\"", False, ["SELECT 'a;b'"], " SELECT \"c;d\""),
        ("SELECT 'it''s;'; ", False, ["SELECT 'it''s;'"], ""),
        ("SELECT 'a\\';b';", True, ["SELECT 'a\\';b'"], ""),
        ("SELECT 'a\\'; SELECT 2;", False, ["SELECT 'a\\'", "SELECT 2"], ""),
        ("SELECT 1 -- not ; this\n;", False, ["SELECT 1 -- not ; this"], ""),
        ("/* ; */ SELECT 1;", False, ["/* ; */ SELECT 1"], ""),
        ("SELECT 1; /* still", False, ["SELECT 1"], " /* still"),
        ("-- only a comment\n", False, [], ""),
        (";;", False, [], ""),
        ("SELECT 'open;", False, [], "SELECT 'open;"),
    ],
)
def test_split_sql(text, backslash, statements, rest):
    assert cli.split_sql(text, backslash) == (statements, rest)


def args(**kw):
    return cli.build_parser().parse_args(["sessions"] + [x for k, v in kw.items() for x in (f"--{k}", v)])


@pytest.fixture
def profile(tmp_path, monkeypatch):
    for key in ("KYUUBI_URL", "KYUUBI_USER", "KYUUBI_CA", "KYUUBI_ENGINE", "KYUUBI_PROFILE"):
        monkeypatch.delenv(key, raising=False)
    path = tmp_path / "config.ini"
    monkeypatch.setenv("KYUUBI_CONFIG", str(path))
    return path


def test_settings_order(profile, monkeypatch):
    profile.write_text("[default]\nurl = https://a\nuser = ann\n[other]\nurl = https://b\nengine = spark\n")
    assert cli.load_settings(args()) == {"url": "https://a", "user": "ann", "ca": None, "engine": None}
    monkeypatch.setenv("KYUUBI_URL", "https://env")
    assert cli.load_settings(args())["url"] == "https://env"
    assert cli.load_settings(args(url="https://flag"))["url"] == "https://flag"
    other = cli.load_settings(args(profile="other"))
    assert other["engine"] == "spark" and other["url"] == "https://env"


def test_settings_defaults(profile):
    cfg = cli.load_settings(args())
    assert cfg["url"] == cli.DEFAULT_URL and cfg["user"]


@pytest.mark.parametrize("content, match", [
    ("[default]\npassword = x\n", "password out of the profile"),
    ("[default]\nurll = x\n", "unknown setting urll"),
    ("[default]\nengine = trino\n", "unknown engine"),
])
def test_settings_rejects(profile, content, match):
    profile.write_text(content)
    with pytest.raises(errors.InterfaceError, match=match):
        cli.load_settings(args())


def test_settings_missing_profile(profile):
    with pytest.raises(errors.InterfaceError, match="no profile file"):
        cli.load_settings(args(profile="nope"))
    profile.write_text("[default]\n")
    with pytest.raises(errors.InterfaceError, match=r"no profile \[nope\]"):
        cli.load_settings(args(profile="nope"))


def test_session_conf():
    assert cli.session_conf(["a=1", "b=x=y"]) == {"a": "1", "b": "x=y"}
    with pytest.raises(errors.InterfaceError):
        cli.session_conf(["novalue"])


class FakeOperation:
    def __init__(self, names, rows):
        self.columns = [{"columnName": n} for n in names]
        self.description = [(n,) for n in names]
        self._rows = rows

    def rows(self):
        return iter(self._rows)


ROWS = (["n", "s", "d", "dt"], [(1, "a\nb", Decimal("2.50"), date(2026, 10, 3)), (10, None, None, None)])


def render(fmt):
    out = io.StringIO()
    cli.print_rows(FakeOperation(*ROWS), fmt, out)
    return out.getvalue()


def test_table():
    assert render("table").splitlines() == [
        "n  | s    | d    | dt",
        "---+------+------+-----------",
        " 1 | a\\nb | 2.50 | 2026-10-03",
        "10 | NULL | NULL | NULL",
        "(2 rows)",
    ]


def test_csv_and_json():
    assert render("csv").splitlines() == ["n,s,d,dt", '1,"a', 'b",2.50,2026-10-03', "10,,,"]
    assert [json.loads(line) for line in render("json").splitlines()] == [
        {"n": 1, "s": "a\nb", "d": "2.50", "dt": "2026-10-03"}, {"n": 10, "s": None, "d": None, "dt": None}]


def test_no_result_set_prints_nothing():
    out = io.StringIO()
    cli.print_rows(FakeOperation([], []), "table", out)
    assert out.getvalue() == ""


@pytest.fixture
def run(kyuubi_url, user, verify, capsys, monkeypatch):
    monkeypatch.setenv("KYUUBI_CONFIG", "/nonexistent")
    flags = ["--url", kyuubi_url, "--user", user] + (["--ca", verify] if isinstance(verify, str) else [])

    def run(command, *rest):
        code = cli.main([command, *flags, *rest])
        out, err = capsys.readouterr()
        return code, out, err
    return run


@pytest.mark.live
@pytest.mark.quack
def test_live_sql(run):
    code, out, _ = run("sql", "--format", "csv", "SELECT 1 AS a; SELECT 'x;y' AS b")
    assert (code, out.splitlines()) == (0, ["a", "1", "b", "x;y"])
    code, _, err = run("sql", "SELEC nope")
    assert code == 1 and "syntax error" in err
    code, out, _ = run("sessions")
    assert code == 0 and out.startswith("id ")


@pytest.mark.live
@pytest.mark.spark
def test_live_python(run):
    code, out, _ = run("python", "--timeout", "600", "print(6 * 7)")
    assert (code, out) == (0, "42\n")
    code, _, err = run("python", "1/0")
    assert code == 1 and err.rstrip().endswith("ZeroDivisionError: division by zero")
