"""Differ — classifies key sets into matched / mismatched / missing / extra.

Two execution paths with identical semantics:
* driver path — small/medium volumes (or no Spark): plain-Python dict diff
* spark path  — large hash sets: full outer join, only counts + sample keys
  ever reach the driver
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence


@dataclass
class DiffResult:
    matched: int = 0
    mismatched: int = 0
    missing: int = 0                      # in source, not in target
    extra: int = 0                        # in target, not in source
    mismatched_keys: list[tuple] = field(default_factory=list)   # sampled
    missing_keys: list[tuple] = field(default_factory=list)
    extra_keys: list[tuple] = field(default_factory=list)
    duplicate_source_keys: int = 0
    duplicate_target_keys: int = 0

    @property
    def compared(self) -> int:            # union of keys on both sides
        return self.matched + self.mismatched + self.missing + self.extra

    @property
    def total_differences(self) -> int:
        return self.mismatched + self.missing + self.extra


# --------------------------------------------------------------------------- #
def diff_rows_driver(source_rows: Sequence[tuple], target_rows: Sequence[tuple],
                     key_len: int, sample_cap: int
                     ) -> tuple["DiffResult", dict[tuple, tuple], dict[tuple, tuple]]:
    """Rows are (k0..k{key_len-1}, payload...) — payload compared as a tuple.
    Works for hash rows (payload = row_hash) AND full canonical rows (payload =
    every value column, giving exact column-level diffs in the SMALL tier).
    Returns (result, source_key->payload, target_key->payload) so callers can
    inspect sampled payloads without re-fetching."""
    src: dict[tuple, tuple] = {}
    tgt: dict[tuple, tuple] = {}
    dup_src = dup_tgt = 0
    for row in source_rows:
        key = tuple(row[:key_len])
        if key in src:
            dup_src += 1
        src[key] = tuple(row[key_len:])
    for row in target_rows:
        key = tuple(row[:key_len])
        if key in tgt:
            dup_tgt += 1
        tgt[key] = tuple(row[key_len:])

    result = DiffResult(duplicate_source_keys=dup_src, duplicate_target_keys=dup_tgt)
    for key, payload in src.items():
        t_payload = tgt.get(key)
        if t_payload is None:
            result.missing += 1
            if len(result.missing_keys) < sample_cap:
                result.missing_keys.append(key)
        elif t_payload == payload:
            result.matched += 1
        else:
            result.mismatched += 1
            if len(result.mismatched_keys) < sample_cap:
                result.mismatched_keys.append(key)
    for key in tgt:
        if key not in src:
            result.extra += 1
            if len(result.extra_keys) < sample_cap:
                result.extra_keys.append(key)
    return result, src, tgt


# --------------------------------------------------------------------------- #
def diff_hash_spark(df_source, df_target, key_aliases: list[str],
                    sample_cap: int) -> DiffResult:
    """Full outer join on key aliases; hash column alias must be 'row_hash'.
    Only aggregate counts and capped sample keys are collected to the driver."""
    from pyspark.sql import functions as F

    s = _normalize(df_source).select(*key_aliases,
                                     F.col("row_hash").alias("src_hash"))
    t = _normalize(df_target).select(*key_aliases,
                                     F.col("row_hash").alias("tgt_hash"))
    joined = s.join(t, on=key_aliases, how="full_outer").select(
        *key_aliases,
        F.when(F.col("src_hash").isNull(), F.lit("EXTRA"))
         .when(F.col("tgt_hash").isNull(), F.lit("MISSING"))
         .when(F.col("src_hash") == F.col("tgt_hash"), F.lit("MATCHED"))
         .otherwise(F.lit("MISMATCHED")).alias("cls"))
    joined = joined.persist()
    try:
        counts = {r["cls"]: r["cnt"] for r in
                  joined.groupBy("cls").agg(F.count("*").alias("cnt")).collect()}
        result = DiffResult(
            matched=int(counts.get("MATCHED", 0)),
            mismatched=int(counts.get("MISMATCHED", 0)),
            missing=int(counts.get("MISSING", 0)),
            extra=int(counts.get("EXTRA", 0)))
        for cls, bucket in (("MISMATCHED", result.mismatched_keys),
                            ("MISSING", result.missing_keys),
                            ("EXTRA", result.extra_keys)):
            if counts.get(cls):
                rows = (joined.filter(F.col("cls") == cls)
                        .select(*key_aliases).limit(sample_cap).collect())
                bucket.extend(tuple(row[a] for a in key_aliases) for row in rows)
        return result
    finally:
        joined.unpersist()


def _normalize(df):
    """JDBC sources (Oracle, DB2) may uppercase aliases — normalize to lower."""
    return df.toDF(*[c.lower() for c in df.columns])
