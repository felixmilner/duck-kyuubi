# airflow-kyuubi

An Airflow operator that runs jobs through the duck-kyuubi launcher ([docs/launcher.md](../../docs/launcher.md))
and waits for them, so Airflow schedules the work while Kyuubi stays the control plane: login, the user's
namespace and quota, the Secrets the catalog gives each job, cancel and audit.

```python
from airflow_kyuubi import KyuubiJobOperator

KyuubiJobOperator(task_id="crunch", job="duckdb-demo",
                  job_params={"millions": "50", "memory": "2Gi"},
                  skip_on_exit_codes=[4])      # e.g. a job that exits 4 when another run holds its lock
```

- Connection `kyuubi_default`: host = the Kyuubi URL, login and password of the Kyuubi user, and for https
  Extra `{"verify": "/path/to/ca.crt"}`. Each task runs as that user.
- The task submits the job, follows its log into the task log, and returns the run summary (the job's last
  JSON line) as its XCom; the run id is XCom `kyuubi_run`.
- A failed run fails the task with its exit code and the catalog's explanation. Killing the task cancels
  the run. Whether a retry is safe is up to the job.
- `job_params` is templated, so mapped tasks fan a job out (the argument isn't `params`: Airflow reserves
  that name). See [examples/dag_jobs.py](examples/dag_jobs.py).
- No long-lived HTTP request: the job runs asynchronously and the task polls (`poll_interval`).

```bash
pip install -e python/kyuubi-client -e "python/airflow-kyuubi[airflow]"
pytest python/airflow-kyuubi
```

To try the example DAG against a running gateway, with the connection in an environment variable and the
DAG file in your DAGs folder:

```bash
# host with its scheme: without one, the operator assumes https
export AIRFLOW_CONN_KYUUBI_DEFAULT='{"conn_type": "http", "host": "http://localhost:10099", "login": "alice", "password": ""}'
airflow dags test duck_kyuubi_jobs
```

It imports from Airflow 3 (`airflow.sdk`) or falls back to Airflow 2; the tests last ran on Airflow 3.3.2.
