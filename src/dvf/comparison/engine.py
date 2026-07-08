"""ComparisonEngine — tiered record-level reconciliation.

Runs ONCE per validation (outcome cached in ctx.shared) no matter how many
reconciliation rule types consume it.

Tiers:
* SMALL  same connection : pushdown EXCEPT both directions — only diffs return
                           (falls back to cross path when the engine lacks EXCEPT)
* SMALL  cross engine    : full canonical fetch both sides, exact driver diff
* MEDIUM                 : pushdown (keys, row_hash) both sides; diff in driver
                           or via Spark full-outer join; samples re-fetched by key.
                           No-hash engines (DB2 z/OS, Teradata) ship the canonical
                           row string instead and are hashed client-side.
* LARGE/XLARGE           : bucket signatures + recursive drill-down (drilldown.py)

All values that travel are CANONICAL strings — the same representation both
engines hashed — so a sample's source/target values are directly comparable.
"""

from __future__ import annotations

import hashlib
import json

from dvf.audit.logger import get_logger
from dvf.audit.models import MismatchRecord, MismatchTypes
from dvf.comparison.differ import DiffResult, diff_hash_spark, diff_rows_driver
from dvf.comparison.plan import ComparisonPlan, build_plan
from dvf.core.exceptions import ComparisonError
from dvf.metadata.models import ComparisonTiers
from dvf.utils.timeutil import new_id, utcnow
from dvf.validators.base import ValidationContext

_log = get_logger("comparison.engine")

_ROW_JSON_CAP = 4096


class ComparisonEngine:
    def __init__(self, validator):
        self.v = validator
        self.ctx = validator.ctx
        self.settings = validator.settings
        self.sqlgen = validator.sqlgen

    # ------------------------------------------------------------------ #
    # Public entrypoint (cached)
    # ------------------------------------------------------------------ #
    def outcome(self) -> dict:
        cached = self.ctx.shared.get(ValidationContext.KEY_COMPARISON)
        if cached is None:
            cached = self._compare()
            self.ctx.shared[ValidationContext.KEY_COMPARISON] = cached
        return cached

    # ------------------------------------------------------------------ #
    def _compare(self) -> dict:
        plan = build_plan(self.v)
        src_count, tgt_count = self._row_counts()
        tier = self._select_tier(max(src_count, tgt_count))
        _log.info("comparison tier=%s (src=%d rows, tgt=%d rows, keys=%s)",
                  tier, src_count, tgt_count, plan.key_names)

        if tier == ComparisonTiers.SMALL:
            same_conn = (self.v.config.source_connection
                         == self.v.config.target_connection)
            if same_conn and self.v.source.dialect.supports_except:
                result, columns_diff, capped, sql_pair = self._small_same_engine(plan)
            else:
                result, columns_diff, capped, sql_pair = self._small_cross(plan)
        elif tier in (ComparisonTiers.LARGE, ComparisonTiers.XLARGE):
            from dvf.comparison.drilldown import DrillDownComparator
            result, columns_diff, capped, sql_pair = \
                DrillDownComparator(self, plan, tier).run()
        else:
            result, columns_diff, capped, sql_pair = self._medium(plan)

        samples, sampled_columns = self._emit_samples(plan, result)
        if not columns_diff:
            columns_diff = sampled_columns
        return {
            "tier": tier,
            "rows_compared": result.compared,
            "rows_matched": result.matched,
            "rows_mismatched": result.mismatched,
            "rows_missing": result.missing,
            "rows_extra": result.extra,
            "duplicate_source_keys": result.duplicate_source_keys,
            "duplicate_target_keys": result.duplicate_target_keys,
            "column_mismatch_counts": columns_diff,
            "counts_capped": capped,
            "samples_captured": samples,
            "source_sql": sql_pair[0],
            "target_sql": sql_pair[1],
            "key_columns": plan.key_names,
            "value_columns": plan.value_names,
        }

    # ------------------------------------------------------------------ #
    # Tier selection
    # ------------------------------------------------------------------ #
    def _row_counts(self) -> tuple[int, int]:
        shared = self.ctx.shared
        src = shared.get(ValidationContext.KEY_SOURCE_COUNT)
        tgt = shared.get(ValidationContext.KEY_TARGET_COUNT)
        timeout = self.settings.engine.query_timeout_s
        if src is None:
            sql = self.sqlgen.render(self.v.source.dialect, "record_count.sql.j2",
                                     ref=self.v.source_ref())
            src = int(self.v.source.execute_scalar(sql, timeout_s=timeout) or 0)
            shared[ValidationContext.KEY_SOURCE_COUNT] = src
        if tgt is None:
            sql = self.sqlgen.render(self.v.target.dialect, "record_count.sql.j2",
                                     ref=self.v.target_ref())
            tgt = int(self.v.target.execute_scalar(sql, timeout_s=timeout) or 0)
            shared[ValidationContext.KEY_TARGET_COUNT] = tgt
        return src, tgt

    def _select_tier(self, n: int) -> str:
        configured = (self.v.config.comparison_tier or ComparisonTiers.AUTO).upper()
        t = self.settings.engine.tier_thresholds
        if configured != ComparisonTiers.AUTO:
            tier = configured
        elif n <= t.small:
            tier = ComparisonTiers.SMALL
        elif n <= t.medium:
            tier = ComparisonTiers.MEDIUM
        elif n <= t.large:
            tier = ComparisonTiers.LARGE
        else:
            tier = ComparisonTiers.XLARGE

        if tier == ComparisonTiers.SMALL and n > self.settings.engine.small_tier_max_fetch:
            _log.warning("SMALL tier exceeds small_tier_max_fetch (%d rows) — "
                         "escalating to MEDIUM", n)
            tier = ComparisonTiers.MEDIUM
        if tier == ComparisonTiers.MEDIUM and \
                n > self.settings.engine.driver_diff_max_rows and self.ctx.spark is None:
            raise ComparisonError(
                f"MEDIUM tier over {n} rows needs Spark for the hash join "
                f"(driver_diff_max_rows={self.settings.engine.driver_diff_max_rows})")
        return tier

    def supports_hash(self, connector) -> bool:
        override = connector.conn.prop("supports_hash")
        if override is not None:
            return str(override).lower() == "true"
        return connector.dialect.supports_hash

    # ------------------------------------------------------------------ #
    # SQL builders
    # ------------------------------------------------------------------ #
    def _connector(self, side: str):
        return self.v.source if side == "source" else self.v.target

    def _side_col(self, pair, side: str):
        return pair.source if side == "source" else pair.target

    def _select_cols(self, side: str, plan: ComparisonPlan, *,
                     include_values: bool, hash_mode: str | None) -> list[dict]:
        """hash_mode: None | 'hash' (pushdown md5) | 'canon' (raw canonical string,
        hashed client-side)."""
        d = self._connector(side).dialect
        cols = []
        for i, pair in enumerate(plan.key_pairs):
            cols.append({"expr": d.canonical_column(self._side_col(pair, side),
                                                    plan.canon),
                         "alias": f"k{i}"})
        if include_values:
            for i, pair in enumerate(plan.value_pairs):
                cols.append({"expr": d.canonical_column(self._side_col(pair, side),
                                                        plan.canon),
                             "alias": f"c{i}"})
        if hash_mode:
            value_cols = [self._side_col(p, side) for p in plan.value_pairs]
            if not value_cols:
                expr = d.literal("-")
            elif hash_mode == "hash":
                expr = d.row_hash_expr(value_cols, plan.canon)
            else:
                expr = d.row_canonical_expr(value_cols, plan.canon)
            cols.append({"expr": expr, "alias": "row_hash"})
        return cols

    def render_select(self, side: str, plan: ComparisonPlan, *,
                      include_values: bool, hash_mode: str | None = None,
                      extra_predicate: str | None = None) -> str:
        connector = self._connector(side)
        extras = (extra_predicate,) if extra_predicate else ()
        ref = self.v.source_ref(extras) if side == "source" else self.v.target_ref(extras)
        return self.sqlgen.render(
            connector.dialect, "canonical_select.sql.j2", ref=ref,
            select_cols=self._select_cols(side, plan, include_values=include_values,
                                          hash_mode=hash_mode),
            order_by=None)

    def hash_select(self, side: str, plan: ComparisonPlan,
                    extra_predicate: str | None = None) -> tuple[str, bool]:
        """(sql, needs_client_hash) — no-hash engines return the canonical string."""
        if self.supports_hash(self._connector(side)):
            return self.render_select(side, plan, include_values=False,
                                      hash_mode="hash",
                                      extra_predicate=extra_predicate), False
        return self.render_select(side, plan, include_values=False,
                                  hash_mode="canon",
                                  extra_predicate=extra_predicate), True

    @staticmethod
    def client_hash_rows(rows: list[tuple], key_len: int) -> list[tuple]:
        """Replace the trailing canonical-string payload with its md5 hex."""
        hashed = []
        for row in rows:
            payload = row[key_len]
            digest = hashlib.md5(str(payload).encode("utf-8")).hexdigest() \
                if payload is not None else None
            hashed.append(row[:key_len] + (digest,))
        return hashed

    # ------------------------------------------------------------------ #
    # SMALL tier
    # ------------------------------------------------------------------ #
    def _small_same_engine(self, plan: ComparisonPlan):
        """Both objects on ONE connection: EXCEPT runs entirely pushdown."""
        connector = self.v.source
        d = connector.dialect
        cap = self.settings.engine.small_tier_max_fetch
        timeout = self.settings.engine.query_timeout_s
        src_sql = self.render_select("source", plan, include_values=True)
        tgt_sql = self.render_select("target", plan, include_values=True)
        only_src_sql = d.limit_clause(
            f"{src_sql}\n{d.except_keyword}\n{tgt_sql}", cap)
        only_tgt_sql = d.limit_clause(
            f"{tgt_sql}\n{d.except_keyword}\n{src_sql}", cap)
        only_src = connector.execute_query(only_src_sql, timeout_s=timeout)
        only_tgt = connector.execute_query(only_tgt_sql, timeout_s=timeout)
        capped = len(only_src) >= cap or len(only_tgt) >= cap

        key_len = len(plan.key_pairs)
        result, src_map, tgt_map = diff_rows_driver(
            only_src, only_tgt, key_len, self._sample_cap())
        # EXCEPT returns only differing rows; matched = total - source-side diffs
        src_count = self.ctx.shared.get(ValidationContext.KEY_SOURCE_COUNT, 0)
        result.matched = max(src_count - result.missing - result.mismatched, 0)

        columns_diff = self._column_diff_from_maps(plan, result.mismatched_keys,
                                                   src_map, tgt_map)
        self.ctx.shared["_cmp_payload_maps"] = (src_map, tgt_map)
        return result, columns_diff, capped, (only_src_sql, only_tgt_sql)

    def _small_cross(self, plan: ComparisonPlan):
        """Cross-engine small volumes: full canonical rows both sides, exact diff."""
        timeout = self.settings.engine.query_timeout_s
        src_sql = self.render_select("source", plan, include_values=True)
        tgt_sql = self.render_select("target", plan, include_values=True)
        src_rows = self.v.source.execute_query(src_sql, timeout_s=timeout)
        tgt_rows = self.v.target.execute_query(tgt_sql, timeout_s=timeout)
        result, src_map, tgt_map = diff_rows_driver(
            src_rows, tgt_rows, len(plan.key_pairs), self._sample_cap())
        columns_diff = self._column_diff_from_maps(plan, result.mismatched_keys,
                                                   src_map, tgt_map,
                                                   count_all=True)
        self.ctx.shared["_cmp_payload_maps"] = (src_map, tgt_map)
        return result, columns_diff, False, (src_sql, tgt_sql)

    # ------------------------------------------------------------------ #
    # MEDIUM tier
    # ------------------------------------------------------------------ #
    def _medium(self, plan: ComparisonPlan):
        timeout = self.settings.engine.query_timeout_s
        src_sql, src_client = self.hash_select("source", plan)
        tgt_sql, tgt_client = self.hash_select("target", plan)
        n = max(self.ctx.shared.get(ValidationContext.KEY_SOURCE_COUNT, 0),
                self.ctx.shared.get(ValidationContext.KEY_TARGET_COUNT, 0))
        use_spark = (self.ctx.spark is not None
                     and n > self.settings.engine.driver_diff_max_rows)
        key_len = len(plan.key_pairs)
        if use_spark:
            from pyspark.sql import functions as F
            df_s = self.v.source.read_dataframe(src_sql)
            df_t = self.v.target.read_dataframe(tgt_sql)
            if src_client:
                df_s = df_s.withColumn("row_hash", F.md5(F.col("row_hash")))
            if tgt_client:
                df_t = df_t.withColumn("row_hash", F.md5(F.col("row_hash")))
            key_aliases = [f"k{i}" for i in range(key_len)]
            result = diff_hash_spark(df_s, df_t, key_aliases, self._sample_cap())
        else:
            src_rows = self.v.source.execute_query(src_sql, timeout_s=timeout)
            tgt_rows = self.v.target.execute_query(tgt_sql, timeout_s=timeout)
            if src_client:
                src_rows = self.client_hash_rows(src_rows, key_len)
            if tgt_client:
                tgt_rows = self.client_hash_rows(tgt_rows, key_len)
            result, _, _ = diff_rows_driver(src_rows, tgt_rows, key_len,
                                            self._sample_cap())
        # column detail comes from sampled row fetches in _emit_samples
        return result, {}, False, (src_sql, tgt_sql)

    # ------------------------------------------------------------------ #
    # Samples & mismatch emission
    # ------------------------------------------------------------------ #
    def _sample_cap(self) -> int:
        return int(self.v.config.max_mismatch_samples
                   or self.settings.engine.max_mismatch_samples)

    def _column_diff_from_maps(self, plan: ComparisonPlan, mismatched_keys,
                               src_map, tgt_map, count_all: bool = False) -> dict:
        counts: dict[str, int] = {}
        keys = (src_map.keys() & tgt_map.keys()) if count_all else mismatched_keys
        for key in keys:
            s_payload, t_payload = src_map.get(key), tgt_map.get(key)
            if s_payload is None or t_payload is None or s_payload == t_payload:
                continue
            for i, name in enumerate(plan.value_names):
                if s_payload[i] != t_payload[i]:
                    counts[name] = counts.get(name, 0) + 1
        return counts

    def _emit_samples(self, plan: ComparisonPlan,
                      result: DiffResult) -> tuple[int, dict]:
        emitted = 0
        sampled_columns: dict[str, int] = {}
        key_paths: dict[tuple, str] = self.ctx.shared.pop("_cmp_key_paths", {})
        maps = self.ctx.shared.pop("_cmp_payload_maps", None)
        if maps is not None:
            src_map, tgt_map = maps
        else:
            src_map, tgt_map = self._fetch_sample_rows(plan, result)

        for key in result.missing_keys:
            self._emit(plan, MismatchTypes.MISSING_IN_TARGET, key,
                       source_row=self._row_json(plan, src_map.get(key)),
                       bucket_path=key_paths.get(key))
            emitted += 1
        for key in result.extra_keys:
            self._emit(plan, MismatchTypes.EXTRA_IN_TARGET, key,
                       target_row=self._row_json(plan, tgt_map.get(key)),
                       bucket_path=key_paths.get(key))
            emitted += 1
        for key in result.mismatched_keys:
            s_payload, t_payload = src_map.get(key), tgt_map.get(key)
            if s_payload is None or t_payload is None:
                self._emit(plan, MismatchTypes.COLUMN_MISMATCH, key,
                           bucket_path=key_paths.get(key))
                emitted += 1
                continue
            src_json = self._row_json(plan, s_payload)
            tgt_json = self._row_json(plan, t_payload)
            for i, name in enumerate(plan.value_names):
                if i < len(s_payload) and i < len(t_payload) and \
                        s_payload[i] != t_payload[i]:
                    self._emit(plan, MismatchTypes.COLUMN_MISMATCH, key,
                               column_name=name,
                               source_value=_s(s_payload[i]),
                               target_value=_s(t_payload[i]),
                               source_row=src_json, target_row=tgt_json,
                               bucket_path=key_paths.get(key))
                    emitted += 1
                    sampled_columns[name] = sampled_columns.get(name, 0) + 1
        return emitted, sampled_columns

    def _fetch_sample_rows(self, plan: ComparisonPlan, result: DiffResult):
        """MEDIUM/LARGE tiers: pushdown-fetch full canonical rows for sampled keys."""
        src_keys = list(result.missing_keys) + list(result.mismatched_keys)
        tgt_keys = list(result.extra_keys) + list(result.mismatched_keys)
        return (self._fetch_side("source", plan, src_keys),
                self._fetch_side("target", plan, tgt_keys))

    def _fetch_side(self, side: str, plan: ComparisonPlan,
                    keys: list[tuple]) -> dict[tuple, tuple]:
        if not keys:
            return {}
        connector = self._connector(side)
        d = connector.dialect
        key_exprs = [d.canonical_column(self._side_col(p, side), plan.canon)
                     for p in plan.key_pairs]
        timeout = self.settings.engine.query_timeout_s
        batch_size = self.settings.engine.in_list_batch_size
        rows: dict[tuple, tuple] = {}
        key_len = len(plan.key_pairs)
        for start in range(0, len(keys), batch_size):
            batch = keys[start:start + batch_size]
            predicate = d.in_list_predicate(key_exprs, batch)
            sql = self.render_select(side, plan, include_values=True,
                                     extra_predicate=predicate)
            for row in connector.execute_query(sql, timeout_s=timeout):
                rows[tuple(row[:key_len])] = tuple(row[key_len:])
        return rows

    def _emit(self, plan: ComparisonPlan, mismatch_type: str, key: tuple,
              column_name: str | None = None, source_value: str | None = None,
              target_value: str | None = None, source_row: str | None = None,
              target_row: str | None = None, bucket_path: str | None = None) -> None:
        self.ctx.emit_mismatch(MismatchRecord(
            mismatch_id=new_id("MM"),
            run_id=self.ctx.run_id,
            execution_id=self.ctx.execution_id,
            validation_id=self.v.config.validation_id,
            mismatch_type=mismatch_type,
            key_values={name: _s(value) for name, value
                        in zip(plan.key_names, key)},
            column_name=column_name,
            source_value=source_value,
            target_value=target_value,
            source_row=source_row,
            target_row=target_row,
            bucket_path=bucket_path,
            captured_ts=utcnow(),
        ))

    def _row_json(self, plan: ComparisonPlan, payload: tuple | None) -> str | None:
        if payload is None:
            return None
        data = {name: _s(payload[i]) if i < len(payload) else None
                for i, name in enumerate(plan.value_names)}
        text = json.dumps(data, default=str)
        return text[:_ROW_JSON_CAP]


def _s(value) -> str | None:
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)
