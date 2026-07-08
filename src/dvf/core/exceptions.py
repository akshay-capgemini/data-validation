"""DVF exception hierarchy.

Retry policy is driven by :func:`is_transient` — the engine retries a validation
attempt only when the failure is classified transient (network blips, deadlocks,
lock timeouts). Assertion-style failures (bad SQL, missing objects, config errors)
never retry.
"""

from __future__ import annotations


class DVFError(Exception):
    """Base class for all framework errors."""


class ConfigurationError(DVFError):
    """Invalid or missing framework/environment configuration."""


class MetadataError(DVFError):
    """Invalid or missing validation metadata (connections, configs, rules)."""


class SecretResolutionError(DVFError):
    """Credentials could not be resolved from the configured secret store."""


class ConnectorError(DVFError):
    """Base class for connector-layer failures."""

    def __init__(self, message: str, *, connection_name: str | None = None,
                 sqlstate: str | None = None, vendor_code: int | None = None):
        super().__init__(message)
        self.connection_name = connection_name
        self.sqlstate = sqlstate
        self.vendor_code = vendor_code


class TransientError(ConnectorError):
    """Marker for retryable failures (network, deadlock, lock timeout)."""


class ConnectionFailedError(TransientError):
    """Could not establish a connection — treated as retryable."""


class QueryTimeoutError(TransientError):
    """Pushdown query exceeded its timeout — retryable once resources free up."""


class QueryExecutionError(ConnectorError):
    """Pushdown query failed. Transience decided by SQLSTATE via is_transient()."""


class NotSupportedError(ConnectorError):
    """Operation not supported by this connector (e.g. file_exists on an RDBMS)."""


class ValidationExecutionError(DVFError):
    """A validator failed in an unexpected (non-data) way."""


class ComparisonError(DVFError):
    """Comparison engine failure (tier selection, drill-down, differ)."""


class ReportError(DVFError):
    """Report generation/delivery failure."""


class UnsupportedHashError(DVFError):
    """Dialect has no usable server-side hash function; caller must use fallback."""


#: SQLSTATE classes/values that indicate a retryable condition across engines.
#: 08xxx = connection exceptions, 40001 = serialization failure/deadlock,
#: 40P01 = Postgres deadlock, HYT00/HYT01 = ODBC timeouts, 57033 = DB2 deadlock/timeout,
#: 51002/57014-adjacent DB2 resource states are handled by prefix '57'.
_TRANSIENT_SQLSTATE_PREFIXES = ("08",)
_TRANSIENT_SQLSTATES = {"40001", "40P01", "HYT00", "HYT01", "57033", "57011", "57P03"}

#: Vendor codes commonly seen for deadlock/timeout: SQL Server 1205 (deadlock),
#: -2 (timeout); DB2 -911/-913 (deadlock/timeout rollback); Oracle 60 (deadlock),
#: 12170 (connect timeout); MySQL 1213 (deadlock), 1205 (lock wait timeout).
_TRANSIENT_VENDOR_CODES = {1205, -2, -911, -913, 60, 12170, 1213}


def is_transient(exc: BaseException) -> bool:
    """Classify an exception as retryable or not."""
    if isinstance(exc, TransientError):
        return True
    if isinstance(exc, ConnectorError):
        state = (exc.sqlstate or "").upper()
        if state in _TRANSIENT_SQLSTATES:
            return True
        if any(state.startswith(p) for p in _TRANSIENT_SQLSTATE_PREFIXES):
            return True
        if exc.vendor_code in _TRANSIENT_VENDOR_CODES:
            return True
    return False
