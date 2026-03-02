"""
Email Alert Service
====================
Sends automatic email alerts when high severity anomalies are detected.
Uses Python's built-in smtplib — no external services needed.

Supports:
- Gmail (most common)
- Outlook/Hotmail
- Any SMTP server
"""

import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Dict, Any
from datetime import datetime
from app.core.config import settings

logger = logging.getLogger(__name__)


class EmailAlertService:

    def __init__(self):
        self.smtp_host = settings.SMTP_HOST
        self.smtp_port = settings.SMTP_PORT
        self.smtp_user = settings.SMTP_USER
        self.smtp_password = settings.SMTP_PASSWORD
        self.alert_recipient = settings.ALERT_EMAIL
        self.enabled = all([
            settings.SMTP_USER,
            settings.SMTP_PASSWORD,
            settings.ALERT_EMAIL
        ])

    def send_anomaly_alert(
        self,
        dataset_name: str,
        total_anomalies: int,
        high_anomalies: List[Dict[str, Any]],
        severity_breakdown: Dict
    ):
        """
        Sends an email alert summarizing high severity anomalies found in a dataset.
        Call this after every file upload that produces high severity results.
        """
        if not self.enabled:
            logger.warning("Email alerts not configured — skipping. Add SMTP settings to .env to enable.")
            return False

        if not high_anomalies:
            logger.info("No high severity anomalies — skipping email alert.")
            return False

        try:
            subject = f"🚨 [{len(high_anomalies)} High Severity] ERP Anomalies Detected — {dataset_name}"
            html_body = self._build_email_html(dataset_name, total_anomalies, high_anomalies, severity_breakdown)
            text_body = self._build_email_text(dataset_name, total_anomalies, high_anomalies, severity_breakdown)

            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"ERP Anomaly Platform <{self.smtp_user}>"
            msg["To"] = self.alert_recipient

            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.smtp_user, self.alert_recipient, msg.as_string())

            logger.info(f"Alert email sent to {self.alert_recipient}")
            return True

        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False

    def _build_email_html(self, dataset_name, total_anomalies, high_anomalies, severity_breakdown):
        """Builds a clean, professional HTML email."""

        rows_html = ""
        for a in high_anomalies[:10]:  # Max 10 in email
            record = a.get("record_data", {})
            invoice_id = record.get("invoice_id", f"Row {a.get('row_index', '?')}")
            vendor = record.get("vendor", "Unknown")
            amount = record.get("amount", "—")
            amount_str = f"${float(amount):,.2f}" if amount and amount != "—" else "—"

            rows_html += f"""
            <tr>
                <td style="padding:10px 12px;border-bottom:1px solid #eee;font-family:monospace;font-size:13px">{invoice_id}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #eee;font-size:13px">{vendor}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #eee;font-size:13px;font-weight:600">{amount_str}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #eee;font-size:12px;color:#666">{a.get('anomaly_type','').replace('_',' ').title()}</td>
                <td style="padding:10px 12px;border-bottom:1px solid #eee;font-size:12px;color:#555;max-width:300px">{a.get('explanation','')[:120]}...</td>
            </tr>"""

        more_text = f"<p style='color:#666;font-size:13px;margin-top:8px'>... and {len(high_anomalies) - 10} more high severity anomalies. Log in to the platform to view all.</p>" if len(high_anomalies) > 10 else ""

        return f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f7fa;font-family:'Helvetica Neue',Arial,sans-serif">
  <div style="max-width:680px;margin:32px auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.08)">

    <!-- Header -->
    <div style="background:#0a0e1a;padding:28px 32px">
      <div style="font-size:11px;color:#00d4ff;letter-spacing:2px;text-transform:uppercase;margin-bottom:8px">ERP ANOMALY DETECTION PLATFORM</div>
      <div style="font-size:22px;font-weight:700;color:#fff">🚨 High Severity Anomalies Detected</div>
      <div style="font-size:13px;color:#5a6a85;margin-top:6px">{datetime.now().strftime('%B %d, %Y at %I:%M %p')}</div>
    </div>

    <!-- Summary bar -->
    <div style="background:#fff8f0;border-left:4px solid #ff3b5c;padding:16px 32px;display:flex;gap:32px">
      <div>
        <div style="font-size:11px;color:#999;text-transform:uppercase;letter-spacing:1px">Dataset</div>
        <div style="font-size:16px;font-weight:600;color:#111;margin-top:2px">{dataset_name}</div>
      </div>
      <div style="margin-left:32px">
        <div style="font-size:11px;color:#999;text-transform:uppercase;letter-spacing:1px">Total Anomalies</div>
        <div style="font-size:16px;font-weight:600;color:#ff3b5c;margin-top:2px">{total_anomalies}</div>
      </div>
      <div style="margin-left:32px">
        <div style="font-size:11px;color:#999;text-transform:uppercase;letter-spacing:1px">High Severity</div>
        <div style="font-size:16px;font-weight:600;color:#ff3b5c;margin-top:2px">{severity_breakdown.get('high', 0)}</div>
      </div>
      <div style="margin-left:32px">
        <div style="font-size:11px;color:#999;text-transform:uppercase;letter-spacing:1px">Medium</div>
        <div style="font-size:16px;font-weight:600;color:#ffb800;margin-top:2px">{severity_breakdown.get('medium', 0)}</div>
      </div>
    </div>

    <!-- Table -->
    <div style="padding:24px 32px">
      <div style="font-size:13px;font-weight:600;color:#333;margin-bottom:14px;text-transform:uppercase;letter-spacing:1px">High Severity Invoices Requiring Immediate Review</div>
      <table style="width:100%;border-collapse:collapse;background:#fafafa;border-radius:8px;overflow:hidden;border:1px solid #eee">
        <thead>
          <tr style="background:#f0f0f0">
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px">Invoice ID</th>
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px">Vendor</th>
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px">Amount</th>
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px">Type</th>
            <th style="padding:10px 12px;text-align:left;font-size:11px;color:#666;text-transform:uppercase;letter-spacing:1px">Explanation</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      {more_text}
    </div>

    <!-- CTA -->
    <div style="padding:0 32px 28px">
      <a href="http://localhost:3000/dashboard.html" style="display:inline-block;background:#00d4ff;color:#000;font-weight:700;font-size:13px;padding:12px 24px;border-radius:8px;text-decoration:none">
        View Full Report on Dashboard →
      </a>
    </div>

    <!-- Footer -->
    <div style="background:#f5f7fa;padding:16px 32px;font-size:11px;color:#999;border-top:1px solid #eee">
      This alert was generated automatically by your ERP Anomaly Detection Platform. 
      You are receiving this because high severity anomalies were found in your uploaded invoice data.
    </div>

  </div>
</body>
</html>"""

    def _build_email_text(self, dataset_name, total_anomalies, high_anomalies, severity_breakdown):
        """Plain text fallback for email clients that don't render HTML."""
        lines = [
            "ERP ANOMALY DETECTION PLATFORM — ALERT",
            "=" * 50,
            f"Dataset: {dataset_name}",
            f"Time: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}",
            f"Total Anomalies: {total_anomalies}",
            f"High Severity: {severity_breakdown.get('high', 0)}",
            f"Medium Severity: {severity_breakdown.get('medium', 0)}",
            "",
            "HIGH SEVERITY INVOICES REQUIRING REVIEW:",
            "-" * 50,
        ]
        for a in high_anomalies[:10]:
            record = a.get("record_data", {})
            lines.append(f"Invoice: {record.get('invoice_id', '?')} | Vendor: {record.get('vendor', '?')} | Amount: {record.get('amount', '?')}")
            lines.append(f"Type: {a.get('anomaly_type', '').replace('_', ' ').title()}")
            lines.append(f"Reason: {a.get('explanation', '')}")
            lines.append("")

        lines.append("Log in to your dashboard to view all anomalies and take action.")
        return "\n".join(lines)


# Singleton instance
email_service = EmailAlertService()