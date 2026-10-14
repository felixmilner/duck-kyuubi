"""Where the password comes from: $KYUUBI_PASSWORD (even empty), else a prompt on a terminal."""

import io

import pytest

from kyuubi_client import credentials, errors


class Tty(io.StringIO):
    def isatty(self):
        return True


def test_environment_wins(monkeypatch):
    monkeypatch.setenv("KYUUBI_PASSWORD", "s3cret")
    monkeypatch.setattr(credentials.getpass, "getpass", lambda prompt: pytest.fail("prompted"))
    assert credentials.lookup_password("alice") == "s3cret"


def test_empty_is_a_password(monkeypatch):
    """A server without authentication takes any password, including none."""
    monkeypatch.setenv("KYUUBI_PASSWORD", "")
    assert credentials.lookup_password("alice") == ""


def test_prompts_on_a_terminal(monkeypatch):
    monkeypatch.delenv("KYUUBI_PASSWORD", raising=False)
    monkeypatch.setattr(credentials.sys, "stdin", Tty())
    prompts = []
    monkeypatch.setattr(credentials.getpass, "getpass", lambda prompt: prompts.append(prompt) or "typed")
    assert credentials.lookup_password("alice") == "typed"
    assert prompts == ["Kyuubi password for alice: "]


@pytest.mark.parametrize("stdin", [io.StringIO(), None])
def test_without_a_terminal_it_says_what_to_set(monkeypatch, stdin):
    monkeypatch.delenv("KYUUBI_PASSWORD", raising=False)
    monkeypatch.setattr(credentials.sys, "stdin", stdin)
    with pytest.raises(errors.InterfaceError, match="set KYUUBI_PASSWORD"):
        credentials.lookup_password("alice")
