"""Where a user's Kyuubi password comes from: $KYUUBI_PASSWORD (empty is allowed, e.g. for a server
without authentication), else a prompt when there's a terminal to ask on."""

import getpass
import os
import sys

from . import errors


def lookup_password(user: str) -> str:
    if "KYUUBI_PASSWORD" in os.environ:
        return os.environ["KYUUBI_PASSWORD"]
    if sys.stdin is not None and sys.stdin.isatty():
        return getpass.getpass(f"Kyuubi password for {user}: ")
    raise errors.InterfaceError(f"no password for {user!r}: set KYUUBI_PASSWORD")
