"""
Leads AI — Session Store & Lead Manager  v3.0
==================================================
SessionStore  — in-memory widget chat sessions (swap for Redis in multi-worker)
LeadManager   — in-memory lead cache + SMTP email dispatch

All config (SMTP) from .env via os.getenv — no config.py dependency.
"""
from __future__ import annotations

import html as _html
import os
import smtplib
import time
import uuid
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()


# ═════════════════════════════════════════════════════════════════════════════
# SESSION STORE
# ═════════════════════════════════════════════════════════════════════════════

class SessionStore:
    """
    In-memory session store for active widget chat sessions.
    For multi-worker deployments, swap with a Redis-backed implementation.
    """

    def __init__(self):
        self._sessions: Dict[str, Dict] = {}

    async def get_or_create(self, session_id: str, tenant_id: str) -> Dict:
        if session_id not in self._sessions:
            self._sessions[session_id] = {
                "id":              session_id,
                "tenant_id":       tenant_id,
                "history":         [],
                "message_count":   0,
                "pii_collected":   False,
                "pii":             None,
                "language":        "en",
                "behavior_events": [],
                "started_at":      time.time(),
                "last_active":     time.time(),
            }
        self._sessions[session_id]["last_active"] = time.time()
        return self._sessions[session_id]

    async def save(self, session_id: str, session: Dict) -> None:
        session["last_active"] = time.time()
        self._sessions[session_id] = session

    async def get(self, session_id: str) -> Optional[Dict]:
        return self._sessions.get(session_id)

    async def delete(self, session_id: str) -> None:
        self._sessions.pop(session_id, None)

    async def update_tenant(self, tenant_id: str, updates: Dict) -> Dict:
        # Tenant config lives in PostgreSQL — this is a no-op for in-memory store
        return updates

    async def cleanup_expired(self, max_age_hours: int = 24) -> int:
        cutoff  = time.time() - (max_age_hours * 3600)
        expired = [s for s, d in self._sessions.items()
                   if d.get("last_active", 0) < cutoff]
        for s in expired:
            self._sessions.pop(s, None)
        return len(expired)


# ═════════════════════════════════════════════════════════════════════════════
# LEAD MANAGER
# ═════════════════════════════════════════════════════════════════════════════

class LeadManager:
    """
    Owns lead email notification logic.
    Persistent lead storage is in PostgreSQL (main.py /v1/leads routes).
    """

    def __init__(self):
        self._leads: Dict[str, List[Dict]] = {}

    async def save(self, tenant_id: str, session_id: str, pii: Dict[str, Any]) -> str:
        lead_id = str(uuid.uuid4())[:12]
        self._leads.setdefault(tenant_id, []).append({
            "id":          lead_id,
            "session_id":  session_id,
            "pii":         pii,
            "captured_at": datetime.now(timezone.utc).isoformat(),
        })
        return lead_id

    async def send_brief(
        self,
        tenant:     Dict,
        session_id: str,
        pii:        Dict,
        intent:     Dict,
        history:    List[Dict],
    ) -> None:
        recipient = tenant.get("notification_email") or tenant.get("email", "")
        if not recipient:
            return
        html = _build_email(
            business_name=tenant.get("name", "Your Business"),
            session_id=session_id,
            pii=pii,
            intent=intent,
            history=history,
        )
        await send_email(
            to=recipient,
            subject=(
                f"\U0001f525 New Lead — {intent.get('lead_quality','warm').upper()} | "
                f"{tenant.get('name','Winssoft BMA')}"
            ),
            html=html,
        )

    async def send_otp_email(self, email: str, otp: str, purpose: str) -> None:
        """Send a 6-digit OTP for auth purposes."""
        safe_purpose = _html.escape(purpose)
        subject = f"Wins Soft - Leads AI — Your OTP for {purpose}"
        html = f"""
        <div style="font-family:sans-serif; max-width:500px; margin:auto; padding:20px; border:1px solid #eee; border-radius:10px;">
            <h2 style="color:#2952e3;">Wins Soft - Leads AI Security</h2>
            <p>You requested an OTP for <strong>{safe_purpose}</strong>.</p>
            <div style="background:#f1f5f9; padding:20px; text-align:center; border-radius:8px; margin:20px 0;">
                <span style="font-size:32px; font-weight:bold; letter-spacing:5px; color:#1e293b;">{_html.escape(otp)}</span>
            </div>
            <p style="font-size:12px; color:#64748b;">This code will expire in 15 minutes. If you did not request this, please ignore this email.</p>
            <hr style="border:none; border-top:1px solid #eee; margin:20px 0;">
            <p style="font-size:11px; color:#94a3b8; text-align:center;">&copy; Leads AI — Conversational Intelligence</p>
        </div>
        """
        await send_email(to=email, subject=subject, html=html)

    async def send_sales_inquiry(self, inquiry: Dict) -> None:
        """Send sales inquiry to configured queries email."""
        subject = f"Sales Inquiry from {inquiry.get('company', 'Unknown')}"
        html = f"""
        <div style="font-family:sans-serif; max-width:600px; padding:20px;">
            <h2 style="color:#2952e3;">New Sales Inquiry</h2>
            <table style="width:100%; text-align:left; border-collapse:collapse; margin-top:20px;">
                <tr><th style="padding:8px; border-bottom:1px solid #eee; width:150px;">Name</th>
                    <td style="padding:8px; border-bottom:1px solid #eee;">{_html.escape(inquiry.get('name', ''))}</td></tr>
                <tr><th style="padding:8px; border-bottom:1px solid #eee;">Email</th>
                    <td style="padding:8px; border-bottom:1px solid #eee;">{_html.escape(inquiry.get('email', ''))}</td></tr>
                <tr><th style="padding:8px; border-bottom:1px solid #eee;">Company</th>
                    <td style="padding:8px; border-bottom:1px solid #eee;">{_html.escape(inquiry.get('company', ''))}</td></tr>
                <tr><th style="padding:8px; border-bottom:1px solid #eee;">Phone</th>
                    <td style="padding:8px; border-bottom:1px solid #eee;">{_html.escape(inquiry.get('phone', ''))}</td></tr>
                <tr><th style="padding:8px; border-bottom:1px solid #eee;">Current Sessions</th>
                    <td style="padding:8px; border-bottom:1px solid #eee;">{_html.escape(str(inquiry.get('expected_volume', '')))}</td></tr>
            </table>
            <h3 style="margin-top:20px;">Message:</h3>
            <p style="background:#f8fafc; padding:15px; border-radius:8px;">{_html.escape(inquiry.get('message', ''))}</p>
        </div>
        """
        # Check platform_settings for custom queries email
        from db import get_pool
        admin_email = os.getenv("SMTP_USER", "sales@winssoft.com")
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT value FROM platform_settings WHERE key='queries_email'"
                )
                if row and row["value"] and row["value"].strip():
                    admin_email = row["value"].strip()
        except Exception:
            pass  # fallback to env var
        await send_email(to=admin_email, subject=subject, html=html)

    async def send_security_alert(self, email: str, action: str) -> None:
        """Send a notification about a security-sensitive action."""
        safe_action = _html.escape(action)
        subject = f"Wins Soft - Leads AI — Security Alert: {action}"
        html = f"""
        <div style="font-family:sans-serif; max-width:500px; margin:auto; padding:20px; border:1px solid #eee; border-radius:10px;">
            <h2 style="color:#ef4444;">Security Notification</h2>
            <p>This is to inform you that your <strong>{safe_action}</strong> has been successfully completed.</p>
            <p>If you did not perform this action, please contact support immediately.</p>
            <div style="padding:10px; border-left:4px solid #ef4444; background:#fef2f2; margin:20px 0;">
                <span style="font-size:14px; font-weight:bold;">Action: {safe_action}</span><br>
                <span style="font-size:12px; color:#64748b;">Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC</span>
            </div>
            <hr style="border:none; border-top:1px solid #eee; margin:20px 0;">
            <p style="font-size:11px; color:#94a3b8; text-align:center;">&copy; Wins Soft - Leads AI — Security Team</p>
        </div>
        """
        await send_email(to=email, subject=subject, html=html)

    async def get_analytics(self, tenant_id: str, days: int = 30) -> Dict:
        leads  = self._leads.get(tenant_id, [])
        recent = [l for l in leads if _is_recent(l["captured_at"], days)]
        return {
            "total_leads":       len(leads),
            "leads_last_n_days": len(recent),
            "recent_leads":      leads[-10:][::-1],
            "conversion_rate":   round(len(leads) / max(1, len(leads) + 5) * 100, 1),
        }

    async def get_leads(self, tenant_id: str, limit: int = 50) -> List[Dict]:
        return (self._leads.get(tenant_id, []) or [])[-limit:][::-1]


# ═════════════════════════════════════════════════════════════════════════════
# EMAIL BUILDER
# ═════════════════════════════════════════════════════════════════════════════

def _build_email(
    business_name: str,
    session_id:    str,
    pii:           Dict,
    intent:        Dict,
    history:       List[Dict],
) -> str:
    quality = intent.get("lead_quality", "warm")
    color   = {"hot": "#ef4444", "warm": "#f59e0b", "cold": "#6b7280"}.get(quality, "#f59e0b")
    products = ", ".join(intent.get("products_interested", [])) or "Not specified"

    rows = "".join(
        f'<div style="margin:5px 0;padding:8px 12px;border-radius:8px;'
        f'background:{"#eef2ff" if m["role"]=="user" else "#f0fdf4"}">' 
        f'<b style="color:{"#4f46e5" if m["role"]=="user" else "#059669"}">'
        f'{_html.escape(m["role"].title())}:</b> {_html.escape(str(m["content"]))}</div>'
        for m in history[-10:]
    )

    return f"""<!DOCTYPE html><html>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:auto;
             padding:20px;color:#1a1a2e;background:#f8fafc">
  <div style="background:linear-gradient(135deg,#2952e3,#00aac5);
              padding:24px;border-radius:12px;margin-bottom:20px">
    <h1 style="color:white;margin:0;font-size:21px">Leads AI — New Lead</h1>
    <p style="color:rgba(255,255,255,.8);margin:6px 0 0">{business_name}</p>
  </div>
  <span style="background:{color};color:white;padding:4px 14px;
               border-radius:20px;font-weight:700;font-size:13px">
    {quality.upper()} LEAD
  </span>
  <span style="background:#f1f5f9;padding:4px 14px;border-radius:20px;
               font-size:13px;color:#64748b;margin-left:8px">
    {intent.get("intent","enquiry").title()}
  </span>
  <div style="background:white;border-radius:12px;padding:18px;
              margin:16px 0;border:1px solid #e2e8f0">
    <b style="color:#2952e3">Contact</b><br><br>
    Name: {_html.escape(str(pii.get("name","—")))}<br>
    Email: <a href="mailto:{_html.escape(str(pii.get('email','')))}\">{_html.escape(str(pii.get("email","—")))}</a><br>
    Phone: {_html.escape(str(pii.get("phone","—")))}<br>
    <span style="font-size:11px;color:#94a3b8">Session: {session_id}</span>
  </div>
  <div style="background:white;border-radius:12px;padding:18px;
              margin-bottom:16px;border:1px solid #e2e8f0">
    <b style="color:#2952e3">AI Analysis</b><br><br>
    {intent.get("summary","N/A")}<br><br>
    Products: {products}<br>
    Sentiment: {intent.get("sentiment","neutral").title()}<br>
    <span style="color:#059669"><b>Action:</b>
      {intent.get("recommended_followup","Follow up within 24h")}
    </span>
  </div>
  <div style="background:white;border-radius:12px;padding:18px;
              margin-bottom:16px;border:1px solid #e2e8f0">
    <b style="color:#2952e3">Conversation</b><br><br>
    {rows or '<span style="color:#94a3b8">No messages.</span>'}
  </div>
  <p style="text-align:center;color:#94a3b8;font-size:11px">
    Powered by Wins Soft - Leads AI
  </p>
</body></html>"""


async def send_email(to: str, subject: str, html: str) -> None:
    """Dispatch HTML email via SMTP in a thread to avoid blocking the event loop."""
    import logging
    import asyncio
    log = logging.getLogger(__name__)

    host = os.getenv("SMTP_HOST", "")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER", "")
    pwd  = os.getenv("SMTP_PASS", "")
    frm  = os.getenv("SMTP_FROM", "noreply@winssoft.com")

    if not host:
        log.info(f"[EMAIL no-op] To={to} | {subject}")
        return

    def _send_sync():
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = frm
        msg["To"]      = to
        msg.attach(MIMEText(html, "html", "utf-8"))
        with smtplib.SMTP(host, port, timeout=15) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(user, pwd)
            srv.sendmail(frm, [to], msg.as_string())

    try:
        await asyncio.to_thread(_send_sync)
        log.info(f"[EMAIL sent] To={to}")
    except Exception as e:
        log.error(f"[EMAIL failed] To={to}: {e}")


def _is_recent(iso: str, days: int) -> bool:
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(iso)).days <= days
    except Exception:
        return False
