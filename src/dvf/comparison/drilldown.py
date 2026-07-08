"""DrillDownComparator — LARGE/XLARGE tiers via bucket signatures.

Both sides run ONE aggregate query per level: bucket_id, COUNT(*), and two
independent signature sums. Equal buckets are proven equal without moving a
row; only diverging buckets are drilled into — recursively for XLARGE
(``bucket_expr(depth d) = MOD(FLOOR(key_hash / N^d), N)``: an independent digit
per level, no recomputation) — until a leaf is small enough for a scoped
row-hash comparison. Cost: matched subtrees = one aggregate query per level per
side; mismatch lineage recorded as bucket_path ("17/3/9").

Signature strategy (decided once per validation, SAME on both sides):
* hash slices  — SUM of two disjoint 8-hex-char slices of the row MD5 as BIGINT
* aggregates   — engines that cannot hash/slice (DB2, Teradata): COUNT + typed
  per-column aggregates (SUM numeric / SUM(LENGTH(RTRIM)) string / null-aware
  boolean sums / COUNT of temporal columns)
"""

from __future__ import annotations

from dvf.audit.logger import get_logger
from dvf.comparison.differ import DiffResult, diff_rows_driver
from dvf.comparison.plan import ComparisonPlan
from dvf.core.exceptions import ComparisonError
from dvf.dialects import TypeCat
from dvf.metadata.models import ComparisonTiers

_log = get_logger("comparison.drilldown")

_SIG_COLUMN_CAP = 8       # aggregate-signature strategy: first N value columns
_FLOAT_TOLERANCE = 1e-4   # aggregate signatures compare after rounding


class DrillDownComparator:
    def __init__(self, engine, plan: ComparisonPlan, tier: str):
        self.e = engine
        self.plan = plan
        self.tier = tier
        cfg = engine.v.config
        eng = engine.settings.engine
        self.bucket_count = int(cfg.partition_buckets or eng.bucket_count)
        self.leaf_threshold = eng.leaf_threshold
        self.max_depth = eng.max_depth if tier == ComparisonTiers.XLARGE else 0
        self.use_slices = (
            engine.supports_hash(engine.v.source)
            and engine.supports_hash(engine.v.target)
            and engine.v.source.dialect.supports_hash_slices
            and engine.v.target.dialect.supports_hash_slices)
        self.result = DiffResult()
        self.columns_diff: dict[str, int] = {}
        self.key_paths: dict[tuple, str] = {}
        self.first_sql: tuple[str, str] | None = None
        self.capped = False

    # ------------------------------------------------------------------ #
    def run(self):
        self._validate_key_strategy()
        self._compare_level(None, None, depth=0, path="")
        self.e.ctx.shared["_cmp_key_paths"] = self.key_paths
        return (self.result, self.columns_diff, self.capped,
                self.first_sql or ("", ""))

    # ------------------------------------------------------------------ #
    def _validate_key_strategy(self) -> None:
        for side in ("source", "target"):
            try:
                self._key_hash_int_expr(side)
            except ComparisonError:
                raise

    def _key_hash_int_expr(self, side: str) -> str:
        d = self.e._connector(side).dialect
        pairs = self.plan.key_pairs
        if len(pairs) == 1:
            col = self.e._side_col(pairs[0], side)
            cat = d.type_category(col.native_type)
            if cat == TypeCat.INTEGER or (cat == TypeCat.DECIMAL and col.scale == 0):
                return f"ABS(CAST({d.quote(col.name)} AS BIGINT))"
        if self.e.supports_hash(self.e._connector(side)) and d.supports_hash_slices:
            key_cols = [self.e._side_col(p, side) for p in pairs]
            key_hash = d.md5_hex_lower(d.row_canonical_expr(key_cols, self.plan.canon))
            return d.hex_slice_to_bigint(key_hash, 1, 15)
        raise ComparisonError(
            f"{side} dialect '{d.name}' cannot bucket a non-numeric comparison "
            "key for LARGE/XLARGE — use a numeric key column or MEDIUM tier")

    # ------------------------------------------------------------------ #
    def _compare_level(self, pred_s: str | None, pred_t: str | None,
                       depth: int, path: str) -> None:
        sig_s, sql_s = self._signatures("source", pred_s, depth)
        sig_t, sql_t = self._signatures("target", pred_t, depth)
        if self.first_sql is None:
            self.first_sql = (sql_s, sql_t)

        for bucket in sorted(set(sig_s) | set(sig_t)):
            s, t = sig_s.get(bucket), sig_t.get(bucket)
            if s is not None and t is not None and self._sig_equal(s, t):
                self.result.matched += s[0]
                continue
            cnt = max(s[0] if s else 0, t[0] if t else 0)
            child_path = f"{path}/{bucket}" if path else str(bucket)
            child_s = self._and(pred_s, self._bucket_predicate("source", depth, bucket))
            child_t = self._and(pred_t, self._bucket_predicate("target", depth, bucket))
            if cnt <= self.leaf_threshold or depth >= self.max_depth:
                self._leaf(child_s, child_t, child_path)
            else:
                self._compare_level(child_s, child_t, depth + 1, child_path)

    @staticmethod
    def _and(base: str | None, extra: str) -> str:
        return f"({base}) AND ({extra})" if base else extra

    def _bucket_predicate(self, side: str, depth: int, bucket: int) -> str:
        d = self.e._connector(side).dialect
        expr = d.bucket_expr(self._key_hash_int_expr(side), self.bucket_count, depth)
        return f"{expr} = {bucket}"

    # ------------------------------------------------------------------ #
    def _signatures(self, side: str, pred: str | None,
                    depth: int) -> tuple[dict[int, tuple], str]:
        connector = self.e._connector(side)
        d = connector.dialect
        bucket_expr = d.bucket_expr(self._key_hash_int_expr(side),
                                    self.bucket_count, depth)
        if self.use_slices:
            value_cols = [self.e._side_col(p, side) for p in self.plan.value_pairs]
            hash_expr = d.row_hash_expr(value_cols, self.plan.canon) \
                if value_cols else d.md5_hex_lower(d.literal("-"))
            sigs = [{"expr": d.hash_slice_sum(hash_expr, 1, 8), "alias": "sig1"},
                    {"expr": d.hash_slice_sum(hash_expr, 9, 8), "alias": "sig2"}]
        else:
            sigs = self._aggregate_sigs(d, side)
        extras = (pred,) if pred else ()
        ref = (self.e.v.source_ref(extras) if side == "source"
               else self.e.v.target_ref(extras))
        sql = self.e.sqlgen.render(d, "bucket_signature.sql.j2", ref=ref,
                                   bucket_expr=bucket_expr, sigs=sigs)
        rows = connector.execute_query(
            sql, timeout_s=self.e.settings.engine.query_timeout_s)
        out: dict[int, tuple] = {}
        for row in rows:
            bucket = int(row[0])
            cnt = int(row[1] or 0)
            out[bucket] = (cnt,) + tuple(self._norm(v) for v in row[2:])
        return out, sql

    def _aggregate_sigs(self, d, side: str) -> list[dict]:
        sigs = []
        for i, pair in enumerate(self.plan.value_pairs[:_SIG_COLUMN_CAP]):
            col = self.e._side_col(pair, side)
            q = d.quote(col.name)
            cat = d.type_category(col.native_type)
            if cat in (TypeCat.INTEGER, TypeCat.DECIMAL, TypeCat.FLOAT):
                expr = f"COALESCE(SUM(CAST({q} AS DECIMAL(38,6))), 0)"
            elif cat == TypeCat.STRING:
                expr = f"COALESCE(SUM(LENGTH(RTRIM({q}))), 0)"
            elif cat == TypeCat.BOOLEAN:
                expr = (f"COALESCE(SUM(CASE WHEN {q} IS NULL THEN 3 "
                        f"WHEN {q} = 0 THEN 1 ELSE 2 END), 0)")
            else:
                expr = f"COUNT({q})"
            sigs.append({"expr": expr, "alias": f"sig{i}"})
        return sigs

    @staticmethod
    def _norm(value):
        if value is None:
            return None
        try:
            return round(float(value), 4)
        except (TypeError, ValueError):
            return str(value)

    def _sig_equal(self, s: tuple, t: tuple) -> bool:
        if s[0] != t[0]:                          # counts exact
            return False
        for a, b in zip(s[1:], t[1:]):
            if a is None or b is None:
                if a != b:
                    return False
            elif isinstance(a, float) and isinstance(b, float):
                scale = max(abs(a), abs(b), 1.0)
                if abs(a - b) / scale > _FLOAT_TOLERANCE:
                    return False
            elif a != b:
                return False
        return True

    # ------------------------------------------------------------------ #
    def _leaf(self, pred_s: str, pred_t: str, path: str) -> None:
        """Scoped row-hash comparison of one diverging bucket."""
        timeout = self.e.settings.engine.query_timeout_s
        key_len = len(self.plan.key_pairs)
        sql_s, client_s = self.e.hash_select("source", self.plan,
                                             extra_predicate=pred_s)
        sql_t, client_t = self.e.hash_select("target", self.plan,
                                             extra_predicate=pred_t)
        rows_s = self.e.v.source.execute_query(sql_s, timeout_s=timeout)
        rows_t = self.e.v.target.execute_query(sql_t, timeout_s=timeout)
        if client_s:
            rows_s = self.e.client_hash_rows(rows_s, key_len)
        if client_t:
            rows_t = self.e.client_hash_rows(rows_t, key_len)

        budget = max(self.e._sample_cap() - len(self.key_paths), 0)
        leaf, _, _ = diff_rows_driver(rows_s, rows_t, key_len, budget)

        self.result.matched += leaf.matched
        self.result.mismatched += leaf.mismatched
        self.result.missing += leaf.missing
        self.result.extra += leaf.extra
        self.result.duplicate_source_keys += leaf.duplicate_source_keys
        self.result.duplicate_target_keys += leaf.duplicate_target_keys
        for bucket_keys, into in ((leaf.mismatched_keys, self.result.mismatched_keys),
                                  (leaf.missing_keys, self.result.missing_keys),
                                  (leaf.extra_keys, self.result.extra_keys)):
            for key in bucket_keys:
                into.append(key)
                self.key_paths[key] = path
        _log.debug("leaf %s: %d mismatched / %d missing / %d extra",
                   path, leaf.mismatched, leaf.missing, leaf.extra)
