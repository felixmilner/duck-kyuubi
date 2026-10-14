# A launcher job image: Python + DuckDB + job.py. The launcher runs every job as non-root
# (runAsNonRoot), which Kubernetes can only check against a numeric USER.
#   docker build -t duck-kyuubi/duckdb-job:0.1.0 examples/jobs/duckdb-job
FROM python:3.12-slim
ARG DUCKDB_VERSION=1.5.6
RUN pip install --no-cache-dir "duckdb==${DUCKDB_VERSION}"
COPY job.py /app/job.py
USER 10001
ENTRYPOINT ["python", "-u", "/app/job.py"]
