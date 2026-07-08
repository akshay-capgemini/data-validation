"""CSV & JSON report exports — machine-consumable for downstream QA gates."""

from __future__ import annotations

import csv
import io
import json

from dvf.reporting.base import ReportGenerator
from dvf.reporting.data import ReportData
from dvf.utils.io import join_path, write_text

_VALIDATION_COLS = [
    "validation_id", "status", "attempt", "execution_group", "validation_profile",
    "source_label", "target_label", "rules_passed", "rules_total",
    "source_row_count", "target_row_count", "rows_compared", "rows_matched",
    "rows_mismatched", "rows_missing", "rows_extra", "comparison_tier_used",
    "duration_sec", "error_message",
]
_RESULT_COLS = [
    "validation_id", "rule_type", "column_name", "status", "source_value",
    "target_value", "difference", "difference_pct", "tolerance_applied",
    "duration_sec",
]
_MISMATCH_COLS = [
    "validation_id", "mismatch_type", "key_values", "column_name",
    "source_value", "target_value", "bucket_path",
]


def _csv_text(columns: list[str], rows: list[dict]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([_cell(row.get(c)) for c in columns])
    return buffer.getvalue()


def _cell(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


class CsvReportGenerator(ReportGenerator):
    format_name = "csv"

    def generate(self, data: ReportData, output_dir: str) -> str:
        run_id = data.run["run_id"]
        base = join_path(output_dir, f"dvf_report_{run_id}")
        write_text(f"{base}_validations.csv",
                   _csv_text(_VALIDATION_COLS, data.validations))
        write_text(f"{base}_rule_results.csv",
                   _csv_text(_RESULT_COLS, data.results))
        write_text(f"{base}_mismatches.csv",
                   _csv_text(_MISMATCH_COLS, data.mismatches))
        summary = _csv_text(list(data.run.keys()), [data.run])
        return write_text(f"{base}_summary.csv", summary)


class JsonReportGenerator(ReportGenerator):
    format_name = "json"

    def generate(self, data: ReportData, output_dir: str) -> str:
        document = {
            "run": data.run,
            "validations": data.validations,
            "rule_results": data.results,
            "mismatches": data.mismatches,
            "mismatch_total": data.mismatch_total,
            "generated_ts": data.generated_ts,
        }
        path = join_path(output_dir, f"dvf_report_{data.run['run_id']}.json")
        return write_text(path, json.dumps(document, indent=2, default=str))
