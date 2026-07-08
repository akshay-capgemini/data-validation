"""Email delivery — run summary inline (HTML) + optional Excel attachment.

AWS-native path: SES via boto3 (send_raw_email). Fallback: SMTP relay.
Recipients resolve per execution_group (recipients_by_group) plus the default
list. Message building is separated from sending so it is unit-testable.
"""

from __future__ import annotations

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dvf.audit.logger import get_logger
from dvf.config.settings import EmailSettings, FrameworkSettings
from dvf.core.exceptions import ReportError
from dvf.reporting.base import status_color
from dvf.reporting.data import ReportData

_log = get_logger("reporting.email")


def resolve_recipients(email: EmailSettings, groups: list[str]) -> list[str]:
    recipients = list(email.recipients)
    for group in groups:
        recipients.extend(email.recipients_by_group.get(group, []))
    return sorted(set(r for r in recipients if r))


def build_summary_html(data: ReportData) -> str:
    run = data.run
    color = status_color(run.get("status"))
    rows = "".join(
        f"<tr><td style='padding:4px 10px'>{v['validation_id']}</td>"
        f"<td style='padding:4px 10px;color:{status_color(v.get('status'))};"
        f"font-weight:bold'>{v.get('status')}</td>"
        f"<td style='padding:4px 10px'>{v.get('rules_passed') or 0}/"
        f"{v.get('rules_total') or 0}</td>"
        f"<td style='padding:4px 10px'>{v.get('error_message') or ''}</td></tr>"
        for v in data.validations[:50])
    return f"""
    <div style="font-family:Segoe UI,Arial,sans-serif;font-size:14px">
      <h2 style="margin:0 0 4px">Data Validation Report</h2>
      <p style="margin:0 0 12px;color:#555">Run <b>{run['run_id']}</b> ·
         status <b style="color:{color}">{run.get('status')}</b> ·
         {run.get('passed') or 0} passed / {run.get('failed') or 0} failed /
         {run.get('errors') or 0} errors / {run.get('warnings') or 0} warnings</p>
      <table style="border-collapse:collapse;border:1px solid #ddd">
        <tr style="background:#0f172a;color:#fff">
          <th style="padding:6px 10px;text-align:left">Validation</th>
          <th style="padding:6px 10px;text-align:left">Status</th>
          <th style="padding:6px 10px;text-align:left">Rules</th>
          <th style="padding:6px 10px;text-align:left">Error</th></tr>
        {rows}
      </table>
      <p style="color:#888;font-size:12px">Generated {data.generated_ts} by DVF.</p>
    </div>"""


def build_message(settings: FrameworkSettings, data: ReportData,
                  recipients: list[str],
                  attachments: list[str] | None = None) -> MIMEMultipart:
    run = data.run
    msg = MIMEMultipart("mixed")
    msg["Subject"] = (f"[DVF] {run.get('status')} — run {run['run_id']} "
                      f"({run.get('failed') or 0} failed / "
                      f"{run.get('total_validations') or 0} total)")
    msg["From"] = settings.reporting.email.from_address
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(build_summary_html(data), "html", "utf-8"))
    for path in attachments or []:
        p = Path(path)
        if not p.exists():
            _log.warning("attachment missing, skipped: %s", path)
            continue
        part = MIMEApplication(p.read_bytes())
        part.add_header("Content-Disposition", "attachment", filename=p.name)
        msg.attach(part)
    return msg


def send_report_email(settings: FrameworkSettings, data: ReportData,
                      groups: list[str] | None = None,
                      attachments: list[str] | None = None) -> bool:
    email = settings.reporting.email
    if not email.enabled:
        return False
    recipients = resolve_recipients(email, groups or [])
    if not recipients:
        _log.warning("email enabled but no recipients resolved — skipped")
        return False
    msg = build_message(settings, data, recipients, attachments)

    if email.method.lower() == "ses":
        try:
            import boto3  # lazy: optional dependency
        except ImportError as exc:
            raise ReportError("SES email needs boto3 (pip install 'dvf[aws]')") from exc
        client = boto3.client("ses", region_name=email.region)
        client.send_raw_email(Source=email.from_address,
                              Destinations=recipients,
                              RawMessage={"Data": msg.as_string()})
    else:
        import smtplib
        if not email.smtp_host:
            raise ReportError("SMTP email needs reporting.email.smtp_host")
        with smtplib.SMTP(email.smtp_host, email.smtp_port, timeout=60) as smtp:
            smtp.sendmail(email.from_address, recipients, msg.as_string())
    _log.info("report email sent to %s", ", ".join(recipients))
    return True
