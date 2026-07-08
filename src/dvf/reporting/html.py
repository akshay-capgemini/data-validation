"""HTML report — one self-contained file (inline CSS, <details> accordions, no JS,
no CDN) so it renders in locked-down environments and via displayHTML."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

from dvf.reporting.base import ReportGenerator, status_color
from dvf.reporting.data import ReportData
from dvf.utils.io import join_path, write_text

_TEMPLATES = Path(__file__).resolve().parent / "templates"


class HtmlReportGenerator(ReportGenerator):
    format_name = "html"

    def __init__(self, settings):
        super().__init__(settings)
        self._env = Environment(
            loader=FileSystemLoader(str(_TEMPLATES)),
            autoescape=select_autoescape(["html"]),
            trim_blocks=True, lstrip_blocks=True)

    # ------------------------------------------------------------------ #
    def render(self, data: ReportData) -> str:
        run = data.run
        cards = [
            {"label": "Validations", "value": run.get("total_validations") or 0,
             "color": "#0f172a"},
            {"label": "Passed", "value": run.get("passed") or 0,
             "color": status_color("PASSED")},
            {"label": "Failed", "value": run.get("failed") or 0,
             "color": status_color("FAILED")},
            {"label": "Warnings", "value": run.get("warnings") or 0,
             "color": status_color("WARNING")},
            {"label": "Errors", "value": run.get("errors") or 0,
             "color": status_color("ERROR")},
            {"label": "Skipped", "value": run.get("skipped") or 0,
             "color": status_color("SKIPPED")},
        ]
        validations = []
        for v in data.validations:
            v = dict(v)
            v["color"] = status_color(v.get("status"))
            v["open"] = v.get("status") not in ("PASSED", "SKIPPED")
            v["rule_rows"] = [dict(r, color=status_color(r.get("status")))
                              for r in data.results_for(v["validation_id"])]
            v["mismatch_rows"] = data.mismatches_for(v["validation_id"])
            validations.append(v)
        template = self._env.get_template("report.html.j2")
        return template.render(run=run, cards=cards, validations=validations,
                               generated_ts=data.generated_ts)

    def generate(self, data: ReportData, output_dir: str) -> str:
        html = self.render(data)
        path = join_path(output_dir, f"dvf_report_{data.run['run_id']}.html")
        return write_text(path, html)
