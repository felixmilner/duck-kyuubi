"""Airflow operator for Kyuubi's duck-kyuubi launcher. See ../README.md."""

from .operators import KyuubiJobOperator

__all__ = ["KyuubiJobOperator"]
