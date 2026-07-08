"""Report manager — renders every configured format for a run from audit data."""

from __future__ import annotations

from importlib import import_module

from dvf.audit.logger import get_logger
from dvf.config.settings import FrameworkSettings
from dvf.metadata.backend import StorageBackend
from dvf.reporting.data import ReportDataProvider
from dvf.utils.io import join_path

_log = get_logger("reporting.manager")

#: format -> "module:Class" (lazy; formats land across phases)
_GENERATORS: dict[str, str] = {
    "html": "dvf.reporting.html:HtmlReportGenerator",
    "excel": "dvf.reporting.excel:ExcelReportGenerator",
    "csv": "dvf.reporting.csv_json:CsvReportGenerator",
    "json": "dvf.reporting.csv_json:JsonReportGenerator",
    "pdf": "dvf.reporting.pdf:PdfReportGenerator",
}


def register_report_format(name: str, target: str) -> None:
    _GENERATORS[name.lower()] = target


def generate_reports(settings: FrameworkSettings, backend: StorageBackend,
                     run_id: str, formats: list[str] | None = None,
                     output_dir: str | None = None) -> dict[str, str]:
    """Render reports; returns {format: written_path}. A format that is not
    available yet (or fails) is logged and skipped — reporting never fails a run."""
    data = ReportDataProvider(backend).fetch(run_id)
    outdir = output_dir or join_path(settings.reporting.output_path, run_id)
    written: dict[str, str] = {}
    for fmt in [f.lower() for f in (formats or settings.reporting.formats)]:
        target = _GENERATORS.get(fmt)
        if not target:
            _log.warning("unknown report format '%s' — skipped", fmt)
            continue
        if fmt == "pdf" and not settings.reporting.pdf_enabled:
            continue
        module_name, _, class_name = target.partition(":")
        try:
            cls = getattr(import_module(module_name), class_name)
        except (ImportError, AttributeError) as exc:
            _log.warning("report format '%s' unavailable (%s) — skipped", fmt, exc)
            continue
        try:
            written[fmt] = cls(settings).generate(data, outdir)
            _log.info("report [%s] written: %s", fmt, written[fmt])
        except Exception as exc:  # noqa: BLE001 — reporting must not fail the run
            _log.error("report format '%s' failed: %s", fmt, exc, exc_info=True)
    return written
