# Connector Developer Guide

Adding a system = one connector class (+ one dialect if it speaks a new SQL
flavor). Zero engine changes.

## 1. Implement BaseConnector

```python
from dvf.connectors.base import BaseConnector

class HanaConnector(BaseConnector):
    connection_types = ("HANA",)

    def connect(self) -> None: ...          # open; raise ConnectionFailedError
    def disconnect(self) -> None: ...
    def execute_query(self, sql, timeout_s=None) -> list[tuple]: ...
    def get_schema(self, schema, obj) -> list[ColumnDef]: ...
    def table_exists(self, schema, obj) -> bool: ...
    # optional: get_primary_keys/get_indexes/get_constraints/get_statistics,
    # list_objects (discovery), read_dataframe (Spark bulk path),
    # resolve_object (view indirection — see file connectors)
```

Rules:
* Control path (`execute_query`) returns **small** result sets only — the
  engine never fetches raw tables through it.
* Raise the framework exceptions (`ConnectionFailedError`, `QueryTimeoutError`,
  `QueryExecutionError` with `sqlstate`/`vendor_code`) — retry classification
  (`core/exceptions.is_transient`) depends on them.
* Never store credentials: resolve via `self.secrets.resolve(self.conn)` at
  connect time.
* JDBC engines: subclass `dvf.connectors.jdbc.JdbcConnector` — you only write
  `jdbc_url()` (+ `driver_class`, `default_port`, `metadata_case`); metadata
  introspection via DatabaseMetaData is inherited.
* File formats: subclass `dvf.connectors.files.FileConnectorBase` — implement
  `_read(path) -> DataFrame`; temp-view mechanics, GDG resolution, existence
  checks are inherited.

## 2. Implement SQLDialect (if a new SQL flavor)

```python
from dvf.dialects.base import SQLDialect

class HanaDialect(SQLDialect):
    name = "hana"
    supports_hash = True          # server-side MD5 available?
    supports_hash_slices = True   # hex→BIGINT parsing for bucket signatures?
    supports_except = True
    def md5_hex_lower(self, expr): return f"LOWER(HASH_MD5({expr}))"
    def canon_decimal(self, expr, scale): ...
    def canon_date(self, expr): ...
    def canon_timestamp(self, expr, precision): ...
    # override quote chars, limit_clause, tablesample_clause,
    # regexp_predicate, hex_slice_to_bigint as needed
```

### The canonicalization contract (MUST hold)
Your dialect must render byte-identical canonical strings to every other
dialect for equal logical values:
* strings RTRIMmed; UPPER only under case_insensitive
* integers and DECIMAL(p,0): plain digits, no point
* decimals: fixed scale, zero-padded (`123.500000`), leading zero (`0.500000`)
* dates `YYYY-MM-DD`; timestamps `YYYY-MM-DD HH:MI:SS.FFFFFF` (respect the
  precision argument)
* booleans `1`/`0`; binary lowercase hex; MD5 lowercase hex
* NULL handling comes from the base class (COALESCE sentinel) — your
  primitives must propagate NULL, never mask it

Capability degradation: `supports_hash=False` → MEDIUM fetches canonical
strings (client-side hash); `supports_hash_slices=False` → LARGE/XLARGE uses
aggregate signatures. Per-connection override: `extra_properties.supports_hash`.

## 3. Register

```python
from dvf.connectors.factory import register_connector
from dvf.dialects import register_dialect  # or map_connection_type

register_connector("HANA", "myext.hana:HanaConnector")
register_dialect("hana", "myext.hana_dialect:HanaDialect",
                 connection_types=["HANA"])
```

## 4. Checklist before shipping
1. **Golden tests** — add the dialect to `tests/unit/dialects/test_golden.py`
   `DIALECTS`, run `pytest --update-golden`, review every generated `.sql`
   under `tests/golden/<name>/` line by line, commit them. Any future change
   to your dialect shows as a reviewable SQL diff.
2. `test_scale_zero_decimal_canonicalizes_as_integer` and
   `test_hash_expressions_lowercase_hex_everywhere` must pass.
3. Validate live: QUICK profile against a real table pair, then FULL on a
   table with a KNOWN planted diff — confirm the mismatch surfaces with correct
   column attribution.
4. Document `extra_properties` keys in docs/metadata-reference.md.
