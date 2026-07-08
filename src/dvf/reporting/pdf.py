"""PDF report — xhtml2pdf rendering of the HTML template (pure-Python pip
dependency; no wkhtmltopdf binary needed on locked-down clusters). Optional:
enable with reporting.pdf_enabled + `pip install 'dvf[pdf]'`."""

from __future__ import annotations

import io

from dvf.core.exceptions import ReportError
from dvf.reporting.base import ReportGenerator
from dvf.reporting.data import ReportData
from dvf.reporting.html import HtmlReportGenerator
from dvf.utils.io import join_path, write_bytes


class PdfReportGenerator(ReportGenerator):
    format_name = "pdf"

    def generate(self, data: ReportData, output_dir: str) -> str:
        try:
            from xhtml2pdf import pisa  # lazy: optional dependency
        except ImportError as exc:
            raise ReportError(
                "PDF format needs xhtml2pdf (pip install 'dvf[pdf]')") from exc
        html = HtmlReportGenerator(self.settings).render(data)
        buffer = io.BytesIO()
        status = pisa.CreatePDF(src=html, dest=buffer, encoding="utf-8")
        if status.err:
            raise ReportError(f"PDF rendering failed with {status.err} error(s)")
        path = join_path(output_dir, f"dvf_report_{data.run['run_id']}.pdf")
        return write_bytes(path, buffer.getvalue())
