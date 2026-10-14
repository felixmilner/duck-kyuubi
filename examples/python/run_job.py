"""Jobs through Kyuubi's launcher engine, with the client's Launcher (python/kyuubi-client).

    pip install -e python/kyuubi-client
    KYUUBI_URL=http://localhost:10099 KYUUBI_USER=alice KYUUBI_PASSWORD= python examples/python/run_job.py
    ... python examples/python/run_job.py duckdb-demo millions=50 memory=2Gi

Lists the catalog (examples/jobs/catalog.json), then runs a job (default: hello) as a pod in the user's
namespace, printing its log as it goes and its summary at the end. Ctrl-C cancels the run.
"""

import json
import os
import sys

from kyuubi_client import KyuubiClient, Launcher, RunFailed

url = os.environ.get("KYUUBI_URL", "http://localhost:10099")
user = os.environ.get("KYUUBI_USER", "alice")
password = os.environ.get("KYUUBI_PASSWORD", "")
job = sys.argv[1] if len(sys.argv) > 1 else "hello"
params = dict(arg.split("=", 1) for arg in sys.argv[2:]) or ({"seconds": "5", "name": user} if job == "hello" else {})

with KyuubiClient(url, user=user, password=password) as client, Launcher(client) as jobs:
    for entry in jobs.list():
        print(f"{entry['job']:<12} {entry['description']}")
    print()
    try:
        # submits with RUN ... async=true, then follows STATUS and LOGS until the run ends
        status = jobs.run(job, on_line=print, **params)
    except RunFailed as e:
        print(f"failed: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"\n{status['run']}: {status['phase']} in {status['seconds']} s")
    print("summary:", json.dumps(json.loads(status["summary"] or "{}"), indent=2))
