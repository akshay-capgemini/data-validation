"""Dialect registry — lazy imports so optional engines never block startup."""

from __future__ import annotations

from importlib import import_module

from dvf.core.exceptions import ConfigurationError
from dvf.dialects.base import CanonOptions, SQLDialect, TypeCat  # re-export

#: dialect name -> "module:Class" (lazy)
_DIALECTS: dict[str, str] = {
    "sqlite": "dvf.dialects.sqlite:SqliteDialect",
    "spark": "dvf.dialects.spark:SparkDialect",
    "snowflake": "dvf.dialects.snowflake:SnowflakeDialect",
    "sqlserver": "dvf.dialects.sqlserver:SqlServerDialect",
    "oracle": "dvf.dialects.oracle:OracleDialect",
    "postgres": "dvf.dialects.postgres:PostgresDialect",
    "mysql": "dvf.dialects.mysql:MySqlDialect",
    "db2_luw": "dvf.dialects.db2:Db2LuwDialect",
    "db2_zos": "dvf.dialects.db2:Db2ZosDialect",
    "teradata": "dvf.dialects.teradata:TeradataDialect",
}

#: connection_type -> dialect name (FILE_* all speak Spark SQL over temp views)
_CONNECTION_TYPE_DIALECT: dict[str, str] = {
    "SQLSERVER": "sqlserver",
    "DB2_LUW": "db2_luw",
    "DB2_ZOS": "db2_zos",
    "ORACLE": "oracle",
    "POSTGRES": "postgres",
    "MYSQL": "mysql",
    "TERADATA": "teradata",
    "SNOWFLAKE": "snowflake",
    "DELTA": "spark",
    "SQLITE": "sqlite",
}

_instances: dict[str, SQLDialect] = {}


def register_dialect(name: str, target: str, connection_types: list[str] | None = None) -> None:
    """Extension hook: add a dialect without touching framework code."""
    _DIALECTS[name] = target
    for ct in connection_types or []:
        _CONNECTION_TYPE_DIALECT[ct.upper()] = name


def map_connection_type(connection_type: str, dialect_name: str) -> None:
    """Extension hook: point a new connection type at an EXISTING dialect
    (e.g. a custom connector that still speaks sqlite/spark SQL)."""
    _CONNECTION_TYPE_DIALECT[connection_type.upper()] = dialect_name.lower()


def get_dialect(name: str) -> SQLDialect:
    key = name.lower()
    if key not in _instances:
        target = _DIALECTS.get(key)
        if not target:
            raise ConfigurationError(f"Unknown SQL dialect '{name}'. Known: {sorted(_DIALECTS)}")
        module_name, _, class_name = target.partition(":")
        cls = getattr(import_module(module_name), class_name)
        _instances[key] = cls()
    return _instances[key]


def dialect_for_connection_type(connection_type: str) -> SQLDialect:
    ct = (connection_type or "").upper()
    if ct.startswith("FILE_"):
        return get_dialect("spark")
    name = _CONNECTION_TYPE_DIALECT.get(ct)
    if not name:
        raise ConfigurationError(f"No dialect mapped for connection type '{connection_type}'")
    return get_dialect(name)


__all__ = [
    "SQLDialect", "CanonOptions", "TypeCat",
    "get_dialect", "dialect_for_connection_type", "register_dialect",
    "map_connection_type",
]
