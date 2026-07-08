"""SparkSession access — never creates a session off-cluster; returns None
so pure-Python paths (sqlite tests, local tooling) keep working."""

from __future__ import annotations

from typing import Optional


def get_spark():
    """Active SparkSession or None (never raises when pyspark is absent)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        return None
    return SparkSession.getActiveSession()


def get_dbutils(spark=None):
    """Databricks dbutils when running on a cluster, else None."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        spark = spark or get_spark()
        return DBUtils(spark) if spark else None
    except Exception:
        try:
            import IPython  # noqa: F401
            ip = IPython.get_ipython()
            return ip.user_ns.get("dbutils") if ip else None
        except Exception:
            return None
