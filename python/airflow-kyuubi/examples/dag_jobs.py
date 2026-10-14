"""Example DAG: launcher jobs from examples/jobs/catalog.json through Kyuubi.

Connection `kyuubi_default` (Admin > Connections): type HTTP, host http://kyuubi.duck-kyuubi:10099 (or your
URL), login/password of the Kyuubi user, and for https Extra {"verify": "/path/to/ca.crt"}.
"""

from datetime import datetime

from airflow import DAG
from airflow_kyuubi import KyuubiJobOperator

with DAG("duck_kyuubi_jobs", start_date=datetime(2026, 10, 1), schedule=None, catchup=False,
         default_args={"retries": 0}) as dag:
    hello = KyuubiJobOperator(task_id="hello", job="hello", job_params={"name": "airflow", "seconds": "5"})
    # mapped tasks: one run per element, in parallel (within the user's quota)
    crunch = KyuubiJobOperator.partial(task_id="crunch", job="duckdb-demo").expand(
        job_params=[{"millions": "10"}, {"millions": "20", "memory": "2Gi"}])
    # exit code 4 marks the task skipped instead of failed
    maybe = KyuubiJobOperator(task_id="maybe", job="hello", job_params={"exit": "4"}, skip_on_exit_codes=[4])
    hello >> crunch >> maybe
