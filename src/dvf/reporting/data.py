"""ReportDataProvider — the ONLY data source for every report format.

Reads audit/result tables through the storage backend; never recomputes and
never touches source/target systems, so any past run re-renders byte-stable.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from dvf.core.exceptions import ReportError
from dvf.metadata.backend import StorageBackend
from dvf.utils.timeutil import iso, utcnow

_MISMATCH_REPORT_CAP = 500


@dataclass
class ReportData:
    run: dict
    validations: list[dict] = field(default_factory=list)     # + config context
    results: list[dict] = field(default_factory=list)          # result_detail rows
    mismatches: list[dict] = field(default_factory=list)       # capped sample
    mismatch_total: int = 0
    generated_ts: str = ""

    def results_for(self, validation_id: str) -> list[dict]:
        return [r for r in self.results if r["validation_id"] == validation_id]

    def mismatches_for(self, validation_id: str) -> list[dict]:
        return [m for m in self.mismatches if m["validation_id"] == validation_id]


class ReportDataProvider:
    def __init__(self, backend: StorageBackend):
        self.backend = backend

    def fetch(self, run_id: str) -> ReportData:
        runs = self.backend.read_rows("audit", "run_audit", where_eq={"run_id": run_id})
        if not runs:
            raise ReportError(f"Run '{run_id}' not found in audit.run_audit")

        validations = self.backend.read_rows("audit", "validation_run_audit",
                                             where_eq={"run_id": run_id})
        validations.sort(key=lambda v: (v.get("validation_id") or "", v.get("attempt") or 0))

        config_ids = sorted({v["validation_id"] for v in validations})
        configs = {c["validation_id"]: c for c in self.backend.read_rows(
            "meta", "validation_config",
            where_in={"validation_id": config_ids})} if config_ids else {}
        for v in validations:
            cfg = configs.get(v["validation_id"], {})
            v["execution_group"] = cfg.get("execution_group")
            v["description"] = cfg.get("description")
            v["validation_profile"] = cfg.get("validation_profile")
            v["source_label"] = _endpoint(cfg, "source")
            v["target_label"] = _endpoint(cfg, "target")

        results = self.backend.read_rows("results", "result_detail",
                                         where_eq={"run_id": run_id})
        for r in results:
            r["details_parsed"] = _parse_json(r.get("details"))
        results.sort(key=lambda r: (r.get("validation_id") or "",
                                    str(r.get("start_ts") or ""),
                                    r.get("rule_type") or ""))

        mismatches = self.backend.read_rows("results", "mismatch_detail",
                                            where_eq={"run_id": run_id})
        total = len(mismatches)
        for m in mismatches:
            m["key_values_parsed"] = _parse_json(m.get("key_values"))
        mismatches.sort(key=lambda m: (m.get("validation_id") or "",
                                       m.get("mismatch_type") or ""))

        return ReportData(run=runs[0], validations=validations, results=results,
                          mismatches=mismatches[:_MISMATCH_REPORT_CAP],
                          mismatch_total=total, generated_ts=iso(utcnow()))


def _endpoint(cfg: dict, side: str) -> str:
    if not cfg:
        return "?"
    conn = cfg.get(f"{side}_connection") or "?"
    schema = cfg.get(f"{side}_schema")
    obj = cfg.get(f"{side}_object")
    if cfg.get(f"{side}_query"):
        return f"{conn} (query)"
    dotted = ".".join(p for p in (schema, obj) if p)
    return f"{conn}:{dotted}" if dotted else conn


def _parse_json(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except json.JSONDecodeError:
            return {"raw": value}
    return {}
