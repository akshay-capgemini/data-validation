"""Report generator contract + shared status styling."""

from __future__ import annotations

from abc import ABC, abstractmethod

from dvf.config.settings import FrameworkSettings
from dvf.reporting.data import ReportData

#: status -> hex color, shared by every visual format
STATUS_COLORS = {
    "PASSED": "#16a34a", "PASS": "#16a34a", "SUCCEEDED": "#16a34a",
    "FAILED": "#dc2626", "FAIL": "#dc2626",
    "WARNING": "#d97706", "WARN": "#d97706", "PARTIAL": "#d97706",
    "ERROR": "#7f1d1d", "TIMEOUT": "#7f1d1d",
    "SKIPPED": "#6b7280", "SKIP": "#6b7280",
    "RUNNING": "#2563eb", "PENDING": "#6b7280", "CANCELLED": "#6b7280",
}


def status_color(status: str | None) -> str:
    return STATUS_COLORS.get((status or "").upper(), "#6b7280")


class ReportGenerator(ABC):
    format_name: str = ""

    def __init__(self, settings: FrameworkSettings):
        self.settings = settings

    @abstractmethod
    def generate(self, data: ReportData, output_dir: str) -> str:
        """Render the report and return the written path."""
