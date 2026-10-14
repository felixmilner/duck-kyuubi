"""Live tests (pytest --live) run against the Kyuubi at $KYUUBI_URL, as $KYUUBI_USER (default: your
login name) with $KYUUBI_PASSWORD (default empty); $KYUUBI_CA is the CA file for an https URL.

Markers say what else a live test needs, so you can deselect what your setup lacks (-m "not spark"):
  quack  the duckdb engine with a reachable Quack server
  k8s    the launcher able to start pods (Kyuubi running in Kubernetes)
  spark  a Spark engine
  auth   a server that checks passwords
"""

import getpass
import os

import pytest

from kyuubi_client import KyuubiClient


def pytest_addoption(parser):
    parser.addoption("--live", action="store_true", help="also run the tests against $KYUUBI_URL")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--live"):
        skip = pytest.mark.skip(reason="needs --live")
        for item in items:
            if "live" in item.keywords:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def kyuubi_url():
    url = os.environ.get("KYUUBI_URL")
    if not url:
        pytest.fail("--live needs KYUUBI_URL, e.g. http://localhost:10099")
    if not KyuubiClient(url, user="ping", timeout=5).ping():
        pytest.fail(f"Kyuubi's REST API isn't reachable at {url}")
    return url


@pytest.fixture(scope="session")
def verify():
    return os.environ.get("KYUUBI_CA") or True


@pytest.fixture(scope="session")
def user():
    return os.environ.get("KYUUBI_USER", getpass.getuser())


@pytest.fixture(scope="session")
def password():
    return os.environ.get("KYUUBI_PASSWORD", "")
