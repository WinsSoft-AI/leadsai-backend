"""
Winssoft BMA — Main Application  v3.1  (PostgreSQL)
====================================================
All config from .env — no config.py.

COMPLETE ROUTE MAP
──────────────────
PUBLIC
  GET  /health
  GET  /v1/system
  GET  /v1/plans

AUTH  (auth.py router)
  POST /auth/login
  POST /auth/logout
  GET  /auth/me
  POST /auth/register/request-otp
  POST /auth/register
  POST /auth/forgot-password
  POST /auth/reset-password
  POST /auth/change-password

WIDGET  (Origin-based domain auth)
  POST /v1/chat
  POST /v1/cv-search
  POST /v1/stt
  POST /v1/behavior
  POST /v1/lead
  POST /v1/session/close
  POST /v1/ingest
  GET  /v1/ingest/{job_id}
  WS   /ws/voice/{session_id}

USER DASHBOARD  (JWT — owner|member)
  GET  /v1/analytics
  GET  /v1/leads
  GET  /v1/leads/{lead_id}/conversation
  GET  /v1/knowledge/list
  DELETE /v1/knowledge/{doc_id}
  GET  /v1/kb/company
  PUT  /v1/kb/company
  GET  /v1/kb/products
  POST /v1/kb/products
  PUT  /v1/kb/products/{product_id}
  DELETE /v1/kb/products/{product_id}
  POST /v1/kb/products/upload-image
  POST /v1/kb/products/scrape-url
  POST /v1/kb/sync
  GET  /v1/widget-config
  PUT  /v1/widget-config
  POST /v1/verify-domain
  GET  /v1/widget-embed
  GET  /v1/domains
  POST /v1/domains
  DELETE /v1/domains/{domain_id}
  GET  /v1/usage
  GET  /v1/subscription
  GET  /v1/payment/history
  POST /v1/payment/create-order
  POST /v1/payment/verify
  GET  /v1/settings
  PUT  /v1/settings
  GET  /v1/tickets
  POST /v1/tickets
  GET  /v1/tickets/{ticket_id}
  GET  /v1/tickets/{ticket_id}/messages
  POST /v1/tickets/{ticket_id}/messages
  POST /v1/tickets/{ticket_id}/close
  POST /v1/tickets/{ticket_id}/mark-read

ADMIN  (JWT — admin|superadmin)
  GET  /admin/clients
  GET  /admin/clients/{client_id}
  PUT  /admin/clients/{client_id}
  GET  /admin/clients/{client_id}/domains
  GET  /admin/clients/{client_id}/usage
  GET  /admin/clients/{client_id}/tickets
  POST /admin/clients/{client_id}/add-domain
  DELETE /admin/clients/{client_id}/domains/{domain_id}
  POST /admin/clients/{client_id}/extend-plan
  POST /admin/clients/{client_id}/change-plan
  POST /admin/clients/{client_id}/suspend
  POST /admin/clients/{client_id}/activate
  GET  /admin/tickets
  GET  /admin/tickets/{ticket_id}
  GET  /admin/tickets/{ticket_id}/messages
  POST /admin/tickets/{ticket_id}/messages
  POST /admin/tickets/{ticket_id}/claim
  POST /admin/tickets/{ticket_id}/resolve
  POST /admin/tickets/{ticket_id}/mark-read
  POST /admin/tickets/{ticket_id}/assign   ← superadmin only
  GET  /admin/analytics

SUPERADMIN  (JWT — superadmin)
  GET  /superadmin/stats
  GET  /superadmin/clients
  GET  /superadmin/clients/{client_id}
  POST /superadmin/clients/{client_id}/ticket-limit
  POST /superadmin/clients/{client_id}/force-logout
  POST /superadmin/clients/{client_id}/toggle-domain-verified
  POST /superadmin/clients/{client_id}/request-email-change
  POST /superadmin/clients/{client_id}/confirm-email-change
  DELETE /superadmin/clients/{client_id}
  GET  /superadmin/admins
  POST /superadmin/admins
  PUT  /superadmin/admins/{admin_id}
  DELETE /superadmin/admins/{admin_id}
  POST /superadmin/admins/{admin_id}/ticket-limit
  GET  /superadmin/tickets
  POST /superadmin/tickets/{ticket_id}/change-priority
  GET  /superadmin/settings
  PUT  /superadmin/settings
  GET  /superadmin/audit-log
"""

from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import json
import logging
import os
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import (
    Body, Depends, Request, FastAPI, File, Form, Header, HTTPException,
    Query, UploadFile, WebSocket, WebSocketDisconnect, status, BackgroundTasks
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, EmailStr, Field
from fastapi.responses import FileResponse, RedirectResponse

from auth import (
    get_current_user, require_admin, require_superadmin,
    router as auth_router,
)
from db_init import (
    close_pool, get_pool, hash_password,
    init_db, _audit,
)
from payments import get_plan, RazorpayService
from ai_proxy import AIProxy
from lead_manager import LeadManager, SessionStore   # SessionStore lives in lead_manager.py
from models import (
    BehaviorEvent, ChatRequest, ChatResponse, KnowledgeQACreate, LeadCapture,
    PlanCreate, PlanUpdate, CompanyDataUpdate, ProductCreate, ProductUpdate,
)
from s3 import create_storage_service, validate_image

load_dotenv(override=True)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ── Directory setup ────────────────────────────────────────────────────────────

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
WIDGET_DIR = Path("widget")
WIDGET_DIR.mkdir(parents=True, exist_ok=True)
MAX_UPLOAD_BYTES = 5 * 1024 * 1024  # 5 MB
PRODUCT_UPLOAD_DIR = UPLOAD_DIR / "products"
PRODUCT_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
LOGO_UPLOAD_DIR = UPLOAD_DIR / "logos"
LOGO_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ═════════════════════════════════════════════════════════════════════════════
# BACKGROUND TASK — Auto-suspend expired trial tenants
# ═════════════════════════════════════════════════════════════════════════════

async def _check_expired_trials():
    """Background task: checks every 6h for trial tenants past their trial period."""
    await asyncio.sleep(10)  # Initial delay to let app fully start
    while True:
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                trial_days = await conn.fetchval(
                    "SELECT value FROM platform_settings WHERE key='trial_duration_days'"
                )
                trial_days = int(trial_days or 7)

                expired = await conn.fetch(
                    "SELECT id, email, name FROM tenants"
                    " WHERE plan='trial' AND status='active'"
                    "   AND created_at < NOW() - ($1 || ' days')::INTERVAL",
                    str(trial_days),
                )

                for t in expired:
                    await conn.execute(
                        "UPDATE tenants SET status='suspended',"
                        " suspension_reason='trial_expired', updated_at=NOW()"
                        " WHERE id=$1",
                        t["id"],
                    )
                    # Also suspend user_auth rows so JWT recheck catches it
                    await conn.execute(
                        "UPDATE user_auth SET status='suspended', updated_at=NOW()"
                        " WHERE tenant_id=$1",
                        t["id"],
                    )
                    logger.info(f"⏰ Trial expired: tenant={t['id']} email={t['email']}")

                if expired:
                    logger.info(f"⏰ Auto-suspended {len(expired)} expired trial tenants")

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Trial expiry check failed: {e}")

        await asyncio.sleep(6 * 3600)  # Run every 6 hours


# ═════════════════════════════════════════════════════════════════════════════
# LIFESPAN
# ═════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Winssoft BMA starting…")

    # ── Security startup checks ───────────────────────────────────────────
    _secret = os.environ.get("SECRET_KEY", "change-me")
    if _secret == "change-me":
        logger.warning("⚠️  SECRET_KEY is set to the default 'change-me'. Set a strong random value in .env!")
    if os.getenv("DEBUG", "false").lower() == "true" and os.getenv("ENVIRONMENT", "").lower() == "production":
        raise RuntimeError("DEBUG=true is not allowed in production environment")

    await init_db(reset=False, seed=False)
    app.state.payments = RazorpayService()
    app.state.ai       = AIProxy()
    await app.state.ai.system_info()
    app.state.leads    = LeadManager()
    app.state.sessions = SessionStore()
    app.state.s3       = create_storage_service()

    # Phase 8: Trial expiry background task
    trial_expiry_task = asyncio.create_task(_check_expired_trials())

    yield
    logger.info("💤 Shutting down…")
    trial_expiry_task.cancel()
    await app.state.ai.close()
    await close_pool()


app = FastAPI(
    title="Leads AI API",
    version="3.1.0",
    description="Multilingual AI Sales Agent — complete backend",
    lifespan=lifespan,
)

_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins if _origins else ["*"],
    allow_credentials=bool(_origins),  # only True when explicit origins are set
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
if os.getenv("DEV_MODE", "false").lower() == "true":
    app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")
app.mount("/widget", StaticFiles(directory=str(WIDGET_DIR)), name="widget")


# ═════════════════════════════════════════════════════════════════════════════
# TOKEN METERING HELPER
# ═════════════════════════════════════════════════════════════════════════════

async def _meter_tokens(tenant_id: str, input_tokens: int, output_tokens: int):
    """Increment token counters on the active billing_cycle for a tenant."""
    if input_tokens <= 0 and output_tokens <= 0:
        return
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE billing_cycles SET"
                "  input_tokens_used  = input_tokens_used  + $1,"
                "  output_tokens_used = output_tokens_used + $2,"
                "  updated_at = NOW()"
                " WHERE tenant_id = $3 AND status = 'active'",
                input_tokens, output_tokens, tenant_id,
            )
    except Exception as e:
        logger.warning(f"Token metering failed for tenant {tenant_id}: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# DOMAIN-BASED AUTH  (widget endpoints)
# ═════════════════════════════════════════════════════════════════════════════

# Dashboard origins that bypass domain validation (for test-chat, etc.)
_DASHBOARD_ORIGINS = [
    o.strip() for o in
    os.getenv("DASHBOARD_ORIGINS", "http://localhost:5173,http://localhost:3000,http://localhost:8000").split(",")
    if o.strip()
]


def _extract_domain(request: Request) -> str:
    """
    Extract the bare domain from Origin or Referer header.
    e.g. 'https://app.winssoft.com:443/path' → 'app.winssoft.com'
    """
    origin = request.headers.get("origin") or request.headers.get("referer") or ""
    if not origin:
        return ""
    # Strip protocol and path, extract base domain
    origin = origin.split("://")[-1].split("/")[0]
    # Strip port if present
    return origin.split(":")[0].lower()


# Known secondary TLD components (appear before the country code)
_SUB_TLDS = {"co", "com", "net", "org", "ac", "gov", "edu", "gen", "firm", "ind", "nic", "res"}

def _extract_sld(domain: str) -> str:
    """
    Extract the second-level domain (brand/company name) from a full domain.
    Handles subdomains and compound TLDs like .co.in, .co.uk.

    Examples:
        winssoft.com        → winssoft
        winssoft.in         → winssoft
        winssoft.co.in      → winssoft
        app.winssoft.com    → winssoft
        shop.winssoft.co.uk → winssoft
        jaielectronics.com  → jaielectronics
    """
    parts = domain.lower().split(".")
    if len(parts) < 2:
        return parts[0] if parts else ""
    # Check for compound TLD: e.g. co.in, co.uk, com.au
    if len(parts) >= 3 and parts[-2] in _SUB_TLDS and len(parts[-1]) <= 3:
        return parts[-3] if len(parts) >= 3 else parts[0]
    return parts[-2]


def _domain_matches(registered_domain: str, request_origin: str) -> bool:
    """
    Check if a request origin belongs to the same brand as the registered domain.
    Allows different TLDs (.com, .in, .net) and subdomains (app.*, shop.*).
    Blocks completely different brands.

    Examples (registered = 'winssoft.com'):
        winssoft.in          → True  (same brand, different TLD)
        app.winssoft.com     → True  (subdomain)
        shop.winssoft.co.uk  → True  (subdomain + compound TLD)
        jaielectronics.com   → False (different brand)
        winssoft-fake.com    → False (different SLD)
    """
    reg_sld = _extract_sld(registered_domain)
    req_sld = _extract_sld(request_origin)
    if not reg_sld or not req_sld:
        return False
    return reg_sld == req_sld



import jwt as pyjwt
from datetime import datetime, timedelta, timezone

async def verify_widget_jwt(request: Request) -> Dict:
    """
    JWT-based authorization for widget endpoints.
    Accepts a Bearer token issued by /v1/widget/token to verify the tenant.
    Dashboard test chatbot works by passing the user JWT; this handles both.
    """
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing or invalid token")
    token = auth_header[7:]

    # 1. Try decoding as dashboard JWT (for dashboard preview)
    try:
        from auth import _decode_token
        payload = _decode_token(token)
        tenant_id = payload.get("tenant_id")
        if tenant_id:
            return await _get_tenant_config(tenant_id)
    except Exception:
        pass  # Not a dashboard JWT

    # 2. Two-step decode with per-tenant widget_secret
    # Step A: Decode WITHOUT verification to extract tenant_id
    try:
        unverified = pyjwt.decode(token, options={"verify_signature": False})
        if unverified.get("type") != "widget":
            raise HTTPException(status_code=401, detail="Invalid token type")
        tenant_id = unverified.get("tid")
        if not tenant_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")
    except pyjwt.DecodeError:
        raise HTTPException(status_code=401, detail="Malformed token")

    # Step B: Fetch tenant's widget_secret from DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT widget_secret, domain FROM tenants WHERE id=$1 AND status='active'",
            tenant_id,
        )
    if not row or not row["widget_secret"]:
        raise HTTPException(status_code=401, detail="Tenant not found or inactive")

    # Step C: Verify signature with tenant's own secret
    try:
        payload = pyjwt.decode(token, row["widget_secret"], algorithms=["HS256"])
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except pyjwt.InvalidSignatureError:
        raise HTTPException(status_code=401, detail="Invalid token signature — secret may have been rotated")
    except pyjwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    # Domain check on every request (SLD match)
    tenant_config = await _get_tenant_config(tenant_id)

    request_origin = _extract_domain(request)
    tenant_domain = (tenant_config.get("domain") or "").lower()
    if tenant_domain:
        tenant_domain = tenant_domain.split("://")[-1].split("/")[0].split(":")[0]

    if request_origin and request_origin != "localhost" and request_origin != "127.0.0.1":
        if tenant_domain and not _domain_matches(tenant_domain, request_origin):
            is_dashboard_origin = any(request_origin in o for o in _DASHBOARD_ORIGINS)
            if not is_dashboard_origin:
                raise HTTPException(status_code=403, detail=f"CORS Policy: Origin '{request_origin}' is not authorized for this widget.")

    return tenant_config

async def _get_tenant_config(tenant_id: str) -> Dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT t.id AS tenant_id,
                   t.name, t.plan, t.status, t.email, t.domain,
                   wc.greeting, wc.primary_color, wc.accent_color,
                   wc.secondary_color, wc.text_color, wc.bg_image_url,
                   wc.position, wc.proactive_enabled,
                   wc.tts_enabled, wc.stt_enabled, wc.cv_search_enabled,
                   wc.notification_email, wc.languages, wc.pii_after_messages,
                   wc.logo_url, wc.bot_text_color, wc.user_text_color,
                   wc.name AS widget_name, wc.proactive_delay_s,
                   wc.proactive_message
              FROM tenants t
              LEFT JOIN widget_configs wc ON wc.tenant_id = t.id
             WHERE t.id = $1 AND t.status = 'active'
            """,
            tenant_id
        )
        if not row:
            raise HTTPException(status_code=403, detail="Account activation issue. Please contact support.")
            
        return {
            "id":                 row["tenant_id"],
            "name":               row["name"],
            "plan":               row["plan"],
            "email":              row["email"],
            "domain":             row["domain"],
            "greeting":           row["greeting"]           or "Hi! How can I help?",
            "primary_color":      row["primary_color"]      or "#2952e3",
            "accent_color":       row["accent_color"]       or "#00d4f5",
            "secondary_color":    row["secondary_color"]    or "#ffffff",
            "text_color":         row["text_color"]         or "#000000",
            "bg_image_url":       row["bg_image_url"],
            "proactive_enabled":  bool(row["proactive_enabled"]),
            "tts_enabled":        bool(row["tts_enabled"]),
            "stt_enabled":        bool(row["stt_enabled"]),
            "cv_search_enabled":  bool(row["cv_search_enabled"]),
            "notification_email": row["notification_email"] or row["email"],
            "languages":          row["languages"]          or "en",
            "pii_after_messages": row["pii_after_messages"] or 3,
            "logo_url":           row.get("logo_url"),
            "bot_text_color":     row.get("bot_text_color") or "#ffffff",
            "user_text_color":    row.get("user_text_color") or "#ffffff",
            "widget_name":        row.get("widget_name"),
            "position":           row.get("position") or "bottom-right",
            "proactive_delay_s":  row.get("proactive_delay_s") or 5,
            "proactive_message":  row.get("proactive_message") or "Hi there! Need help? Chat with us!",
        }


# ── Widget secret rotation interval (days) ────────────────────────────────────
_WIDGET_SECRET_ROTATION_DAYS = int(os.getenv("WIDGET_SECRET_ROTATION_DAYS", "30"))


async def _rotate_widget_secret_bg(tenant_id: str):
    """Background task: rotate a tenant's widget_secret without blocking the response."""
    try:
        new_secret = secrets.token_hex(32)
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE tenants SET widget_secret=$1, widget_secret_rotated_at=NOW(), updated_at=NOW() WHERE id=$2",
                new_secret, tenant_id,
            )
        logger.info(f"🔄 Widget secret rotated for tenant {tenant_id}")
    except Exception as e:
        logger.error(f"Widget secret rotation failed for {tenant_id}: {e}")


async def _alert_bypass_attempt(slug: str, origin: str, tenant_id: str, tenant_domain: str):
    """Background task: email superadmin about a widget bypass attempt."""
    try:
        from lead_manager import send_email
        pool = await get_pool()
        async with pool.acquire() as conn:
            admins = await conn.fetch(
                "SELECT email FROM admin_users WHERE role='superadmin' AND status='active'"
            )
            tenant = await conn.fetchrow(
                "SELECT company, email, domain FROM tenants WHERE id=$1", tenant_id
            )

        company = tenant["company"] if tenant else "Unknown"
        t_email = tenant["email"] if tenant else "Unknown"

        html = f"""
        <div style="font-family:sans-serif;max-width:600px;margin:auto;padding:20px">
            <h2 style="color:#ef4444">⚠️ Widget Bypass Attempt Detected</h2>
            <p>An unauthorized origin attempted to use a tenant's widget embed script.</p>
            <table style="width:100%;border-collapse:collapse;margin:16px 0">
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold">Company</td>
                    <td style="padding:8px;border:1px solid #ddd">{company}</td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold">Tenant Email</td>
                    <td style="padding:8px;border:1px solid #ddd">{t_email}</td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold">Widget Slug</td>
                    <td style="padding:8px;border:1px solid #ddd"><code>{slug}</code></td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold">Registered Domain</td>
                    <td style="padding:8px;border:1px solid #ddd">{tenant_domain}</td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold;color:#ef4444">Unauthorized Origin</td>
                    <td style="padding:8px;border:1px solid #ddd;color:#ef4444"><strong>{origin}</strong></td></tr>
                <tr><td style="padding:8px;border:1px solid #ddd;font-weight:bold">Timestamp</td>
                    <td style="padding:8px;border:1px solid #ddd">{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}</td></tr>
            </table>
            <p style="color:#666">This script may have been leaked or copied. Please investigate.</p>
        </div>
        """
        for admin in admins:
            await send_email(admin["email"], f"🚨 Widget Bypass Detected — {company} ({slug})", html)
        logger.warning(f"🚨 Bypass alert sent: slug={slug}, origin={origin}, tenant={tenant_id}")
    except Exception as e:
        logger.error(f"Bypass alert failed: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# WIDGET  (JWT Auth via slug)
# ═════════════════════════════════════════════════════════════════════════════

class WidgetTokenReq(BaseModel):
    company_slug: str

@app.post("/v1/widget/token", tags=["Widget"])
async def get_widget_token(req: WidgetTokenReq, request: Request, bg_tasks: BackgroundTasks):
    """Public endpoint: exchanges a company slug for a short-lived Widget JWT.
    Uses per-tenant widget_secret for signing. Auto-rotates secret in background."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status, domain, widget_secret, widget_secret_rotated_at, widget_slug"
            " FROM tenants WHERE widget_slug=$1 AND status='active'",
            req.company_slug
        )
        if not row:
            raise HTTPException(status_code=403, detail="Account activation issue. Please contact support.")

        tenant_id = row["id"]

        # Enforce CORS Domain Policy (SLD match)
        request_origin = _extract_domain(request)
        tenant_domain = (row["domain"] or "").lower()
        if tenant_domain:
            tenant_domain = tenant_domain.split("://")[-1].split("/")[0].split(":")[0]

        if request_origin and request_origin != "localhost" and request_origin != "127.0.0.1":
            if tenant_domain and not _domain_matches(tenant_domain, request_origin):
                is_dashboard_origin = any(request_origin in o for o in _DASHBOARD_ORIGINS)
                if not is_dashboard_origin:
                    # Bypass detected — send alert in background, don't slow down the 403
                    bg_tasks.add_task(
                        _alert_bypass_attempt,
                        req.company_slug, request_origin, tenant_id, tenant_domain
                    )
                    raise HTTPException(status_code=403, detail=f"CORS Policy: Origin '{request_origin}' is not authorized for this widget.")

        # Use tenant's own widget_secret for signing
        tenant_secret = row["widget_secret"]
        if not tenant_secret:
            # First time — generate a secret
            tenant_secret = secrets.token_hex(32)
            await conn.execute(
                "UPDATE tenants SET widget_secret=$1, widget_secret_rotated_at=NOW() WHERE id=$2",
                tenant_secret, tenant_id,
            )

        # Check if rotation is needed — respond first, rotate in background
        rotated_at = row["widget_secret_rotated_at"]
        if rotated_at and (datetime.now(timezone.utc) - rotated_at).days >= _WIDGET_SECRET_ROTATION_DAYS:
            bg_tasks.add_task(_rotate_widget_secret_bg, tenant_id)

        expires_min = int(os.getenv("WIDGET_JWT_EXPIRE_MINUTES", "15"))

        payload = {
            "tid": tenant_id,
            "type": "widget",
            "exp": datetime.utcnow() + timedelta(minutes=expires_min),
            "iat": datetime.utcnow()
        }
        token = pyjwt.encode(payload, tenant_secret, algorithm="HS256")
        return {"access_token": token, "type": "bearer", "expires_in": expires_min * 60}

@app.get("/v1/widget/config", tags=["Widget"])
async def get_widget_config(request: Request, slug: str = None):
    tenant_id = None
    if slug:
        pool = await get_pool()
        async with pool.acquire() as conn:
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE widget_slug=$1 AND status='active'", slug
            )
            if not tenant_id:
                raise HTTPException(status_code=403, detail="Company not found or inactive")
    else:
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            try:
                from auth import _decode_token
                payload = _decode_token(auth[7:])
                tenant_id = payload.get("tenant_id")
            except Exception:
                pass
    if not tenant_id:
        raise HTTPException(status_code=400, detail="Provide slug param or Bearer token")
    cfg = await _get_tenant_config(tenant_id)
    return cfg


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _must_be_user(current: dict) -> None:
    if current.get("account_type") != "user":
        raise HTTPException(status_code=403, detail="Requires a tenant account")


def _must_be_owner(current: dict) -> None:
    """Owner-only: widget config, embed, settings, billing."""
    _must_be_user(current)
    if current.get("role") != "owner":
        raise HTTPException(status_code=403, detail="Owner access required")


def _must_be_owner_or_member(current: dict) -> None:
    """Owner + member: knowledge base, tickets, ingest, leads follow-up."""
    _must_be_user(current)
    if current.get("role") not in ("owner", "member"):
        raise HTTPException(status_code=403, detail="Owner or member access required")


def _must_not_be_suspended(current: dict) -> None:
    """Block dashboard mutations for suspended tenants. Ticket + billing routes are exempt."""
    if current.get("tenant_status") == "suspended":
        raise HTTPException(
            status_code=403,
            detail="Your account is suspended. Please upgrade or contact support.",
        )


def _must_not_be_trial(current: dict, feature: str = "") -> None:
    """Block certain mutations for trial plan users."""
    if current.get("plan") == "trial":
        msg = f"Upgrade your plan to use {feature}." if feature else "This feature requires a paid plan."
        raise HTTPException(status_code=403, detail=msg)




def _build_where(filters: list[tuple[Any, str]]) -> tuple[str, list]:
    """
    Build a WHERE clause from (value, fragment) pairs.
    Fragment should contain a placeholder like $N where N will be filled in.
    Returns (clause_string, params_list).
    """
    conds:  list[str] = ["TRUE"]
    params: list      = []
    for val, frag in filters:
        if val:
            params.append(val)
            conds.append(frag.replace("$N", f"${len(params)}"))
    return " AND ".join(conds), params


# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health", tags=["System"])
async def health():
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return {
        "status":    "ok" if db_ok else "degraded",
        "db":        "ok" if db_ok else "error",
        "version":   "3.1.0",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
@app.get("/favicon.ico")
async def favicon():
    return FileResponse("favicon.ico")

@app.get("/v1/system", tags=["System"])
async def system_info():
    ai_info = await app.state.ai.system_info()
    return {
        "status":  "ok",
        "version": "3.1.0",
        "ai_backend": ai_info,
    }


@app.get("/v1/plans", tags=["Payments"])
async def list_plans():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, onboarding_fee_rupee, base_fee_rupee, currency,"
            " input_token_rate, output_token_rate, description, discount_type, discount_value,"
            " ticket_limit, domain_limit"
            " FROM plans ORDER BY onboarding_fee_rupee ASC"
        )
        return {
            row["id"]: {
                "name":                 row["name"],
                "onboarding_fee_rupee": row["onboarding_fee_rupee"],
                "base_fee_rupee":       row["base_fee_rupee"],
                "currency":             row["currency"],
                "input_token_rate":     float(row["input_token_rate"]),
                "output_token_rate":    float(row["output_token_rate"]),
                "description":          row["description"],
                "discount_type":        row["discount_type"],
                "discount_value":       row["discount_value"],
                "ticket_limit":         row["ticket_limit"],
                "domain_limit":         row["domain_limit"],
            }
            for row in rows
        }


# ═════════════════════════════════════════════════════════════════════════════
# WIDGET  (JWT Auth via slug)
# ═════════════════════════════════════════════════════════════════════════════

class WidgetTokenReq(BaseModel):
    company_slug: str

@app.post("/v1/widget/token", tags=["Widget"])
async def get_widget_token(req: WidgetTokenReq, request: Request):
    """Public endpoint: exchanges a company slug for a short-lived Widget JWT."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, status, domain FROM tenants WHERE widget_slug=$1 AND status='active'",
            req.company_slug
        )
        if not row:
            raise HTTPException(status_code=403, detail="Account activation issue. Please contact support.")

        # Enforce CORS Domain Policy
        request_origin = _extract_domain(request)
        tenant_domain = (row["domain"] or "").lower()
        if tenant_domain:
            tenant_domain = tenant_domain.split("://")[-1].split("/")[0].split(":")[0]

        # Ignore if missing request origin (e.g. direct curl), or if testing from dashboard
        if request_origin and request_origin != "localhost" and request_origin != "127.0.0.1":
            if tenant_domain and not _domain_matches(tenant_domain, request_origin):
                is_dashboard_origin = any(request_origin in o for o in _DASHBOARD_ORIGINS)
                if not is_dashboard_origin:
                    raise HTTPException(status_code=403, detail=f"CORS Policy: Origin '{request_origin}' is not authorized for this widget.")
        
        secret = os.getenv("WIDGET_JWT_SECRET")
        if not secret:
            raise HTTPException(status_code=500, detail="WIDGET_JWT_SECRET not configured")
            
        expires_min = int(os.getenv("WIDGET_JWT_EXPIRE_MINUTES", "60"))
        
        payload = {
            "tid": row["id"],
            "type": "widget",
            "exp": datetime.utcnow() + timedelta(minutes=expires_min),
            "iat": datetime.utcnow()
        }
        token = pyjwt.encode(payload, secret, algorithm="HS256")
        return {"access_token": token, "type": "bearer", "expires_in": expires_min * 60}

@app.get("/v1/widget/config", tags=["Widget"])
async def get_widget_config(request: Request, slug: str = None):
    tenant_id = None
    if slug:
        pool = await get_pool()
        async with pool.acquire() as conn:
            tenant_id = await conn.fetchval(
                "SELECT id FROM tenants WHERE widget_slug=$1 AND status='active'", slug
            )
            if not tenant_id:
                raise HTTPException(status_code=403, detail="Company not found or inactive")
    else:
        # Dashboard preview — extract tenant_id from JWT
        try:
            tenant = await verify_widget_jwt(request)
            tenant_id = tenant.get("tenant_id") or tenant.get("id")
        except Exception:
            raise HTTPException(status_code=401, detail="Missing slug or valid token")

    tenant = await _get_tenant_config(tenant_id)

    return {
        "name":               tenant.get("name"),
        "greeting":           tenant.get("greeting"),
        "primary_color":      tenant.get("primary_color"),
        "accent_color":       tenant.get("accent_color"),
        "secondary_color":    tenant.get("secondary_color"),
        "text_color":         tenant.get("text_color"),
        "bg_image_url":       app.state.s3.resolve_url(tenant.get("bg_image_url")),
        "position":           tenant.get("position"),
        "proactive_enabled":  tenant.get("proactive_enabled"),
        "tts_enabled":        tenant.get("tts_enabled"),
        "stt_enabled":        tenant.get("stt_enabled"),
        "cv_search_enabled":  tenant.get("cv_search_enabled"),
        "languages":          tenant.get("languages"),
        "logo_url":           app.state.s3.resolve_url(tenant.get("logo_url")),
        "bot_text_color":     tenant.get("bot_text_color"),
        "user_text_color":    tenant.get("user_text_color"),
        "business_name":      tenant.get("widget_name") or tenant.get("name"),
        "pii_after_messages": tenant.get("pii_after_messages"),
        "proactive_delay_s":  tenant.get("proactive_delay_s"),
        "proactive_message":  tenant.get("proactive_message"),
    }


# ── Session metadata helpers ─────────────────────────────────────────────────
import re as _re

def _parse_user_agent(ua: str) -> dict:
    """Light regex-based UA parser — no extra deps needed."""
    browser = browser_ver = os_name = os_ver = device_type = ""

    # Browser detection
    if m := _re.search(r"Edg(?:e)?/(\S+)", ua):
        browser, browser_ver = "Edge", m.group(1)
    elif m := _re.search(r"OPR/(\S+)", ua):
        browser, browser_ver = "Opera", m.group(1)
    elif m := _re.search(r"Chrome/(\S+)", ua):
        browser, browser_ver = "Chrome", m.group(1)
    elif m := _re.search(r"Firefox/(\S+)", ua):
        browser, browser_ver = "Firefox", m.group(1)
    elif m := _re.search(r"Safari/(\S+)", ua):
        browser, browser_ver = "Safari", m.group(1)
        if mv := _re.search(r"Version/(\S+)", ua):
            browser_ver = mv.group(1)
    else:
        browser = "Other"

    # OS detection
    if "Windows" in ua:
        os_name = "Windows"
        if m := _re.search(r"Windows NT (\d+\.\d+)", ua):
            ver_map = {"10.0": "10/11", "6.3": "8.1", "6.2": "8", "6.1": "7"}
            os_ver = ver_map.get(m.group(1), m.group(1))
    elif "Mac OS X" in ua:
        os_name = "macOS"
        if m := _re.search(r"Mac OS X [\s_]*([\d_.]+)", ua):
            os_ver = m.group(1).replace("_", ".")
    elif "Android" in ua:
        os_name = "Android"
        if m := _re.search(r"Android\s*([\d.]+)", ua):
            os_ver = m.group(1)
    elif "iPhone" in ua or "iPad" in ua:
        os_name = "iOS"
        if m := _re.search(r"OS ([\d_]+)", ua):
            os_ver = m.group(1).replace("_", ".")
    elif "Linux" in ua:
        os_name = "Linux"
    else:
        os_name = "Other"

    # Device type
    if any(k in ua for k in ("Mobile", "Android", "iPhone")):
        device_type = "mobile"
    elif "iPad" in ua or "Tablet" in ua:
        device_type = "tablet"
    else:
        device_type = "desktop"

    return {
        "browser": browser, "browser_ver": browser_ver,
        "os": os_name, "os_ver": os_ver,
        "device_type": device_type,
    }


async def _geolocate_ip(ip: str) -> dict:
    """Best-effort IP geolocation via ip-api.com (free, no key needed)."""
    geo = {"country": None, "city": None, "region": None, "timezone": None}
    if not ip or ip in ("127.0.0.1", "::1", "localhost", "testclient"):
        return geo
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"http://ip-api.com/json/{ip}?fields=status,country,regionName,city,timezone")
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "success":
                    geo["country"] = data.get("country")
                    geo["city"] = data.get("city")
                    geo["region"] = data.get("regionName")
                    geo["timezone"] = data.get("timezone")
    except Exception:
        pass  # non-critical, skip silently
    return geo


async def _capture_session_metadata(
    session_id: str, tenant_id: str,
    request: Request, req: "ChatRequest",
):
    """Insert session_metadata row on the very first message of a session.
    Runs in background — failures are logged but never block chat."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Skip if already captured
            exists = await conn.fetchval(
                "SELECT 1 FROM session_metadata WHERE session_id=$1", session_id
            )
            if exists:
                return

            # Extract data
            ip = request.client.host if request.client else None
            ua_str = request.headers.get("user-agent", "")
            ua = _parse_user_agent(ua_str)
            lang = request.headers.get("accept-language", "")
            if lang:
                lang = lang.split(",")[0].strip()  # e.g. "en-US"
            referrer = request.headers.get("referer", "") or ""
            geo = await _geolocate_ip(ip)

            await conn.execute(
                "INSERT INTO session_metadata "
                "(session_id, tenant_id, visitor_name, ip_address, country, city, region, "
                "timezone, user_agent, browser, browser_ver, os, os_ver, device_type, "
                "screen_res, language, referrer, page_url) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) "
                "ON CONFLICT (session_id) DO NOTHING",
                session_id, tenant_id, "Guest", ip,
                geo["country"], geo["city"], geo["region"],
                req.client_timezone or geo["timezone"],
                ua_str, ua["browser"], ua["browser_ver"],
                ua["os"], ua["os_ver"], ua["device_type"],
                req.screen_resolution, lang, referrer, req.page_url,
            )
    except Exception as e:
        logger.warning(f"Session metadata capture failed: {e}")


@app.post("/v1/chat", response_model=ChatResponse, tags=["Widget"])
async def chat(req: ChatRequest, request: Request, bg_tasks: BackgroundTasks, tenant=Depends(verify_widget_jwt)):
    session        = await app.state.sessions.get_or_create(req.session_id, tenant["id"])

    # Capture metadata on first message (non-blocking background task)
    if session.get("message_count", 0) == 0:
        bg_tasks.add_task(_capture_session_metadata, req.session_id, tenant["id"], request, req)
    
    q = req.message.strip().lower()
    fast_reply = None
    if q in {"hi", "hello", "hey", "good morning", "good afternoon", "good evening"}:
        fast_reply = "Hello! How can I help you today?"
    elif q in {"thank you", "thanks", "thx", "appreciate it"}:
        fast_reply = "You're very welcome!"
        
    if fast_reply:
        history = session.get("history", [])
        history.append({"role": "user", "content": req.message, "ts": time.time()})
        history.append({"role": "assistant", "content": fast_reply, "ts": time.time()})
        session["history"] = history
        session["message_count"] = session.get("message_count", 0) + 1
        await app.state.sessions.save(session["id"], session)
        return ChatResponse(session_id=session["id"], message=fast_reply, sources=[])

    context_chunks = await app.state.ai.rag_retrieve(query=req.message, tenant_id=tenant["id"], top_k=8)
    history        = session.get("history", [])
    result         = await app.state.ai.gemini_chat(
        message=req.message, history=history,
        context_chunks=context_chunks, tenant_config=tenant, language=req.language,
    )
    # Extract real token usage from Gemini response
    _usage   = result.get("usage", {})
    usage_in  = _usage.get("promptTokenCount", 0)
    usage_out = _usage.get("candidatesTokenCount", 0)
    history.append({"role": "user",      "content": req.message,   "ts": time.time()})
    history.append({"role": "assistant", "content": result["text"], "ts": time.time()})
    session["history"]       = history
    session["message_count"] = session.get("message_count", 0) + 1
    await app.state.sessions.save(session["id"], session)

    audio_url = None
    if req.tts_enabled and tenant.get("tts_enabled"):
        try:
            audio_url = await app.state.ai.tts_synthesize(
                text=result["text"], language=req.language, session_id=session["id"]
            )
        except Exception as e:
            logger.warning(f"TTS failed: {e}")
    needs_pii = (session["message_count"] >= tenant["pii_after_messages"]
                 and not session.get("pii_collected"))

    # ── Extract matching products from RAG sources (Only if NOT a fallback response) ──
    products = []
    response_text_lower = result.get("text", "").lower()
    is_fallback = "don't have that information" in response_text_lower or "do not have that information" in response_text_lower
    
    try:
        product_sources = [
            c for c in context_chunks
            if isinstance(c, dict) and str(c.get("source", "")).startswith("Product")
        ]
        if product_sources and not is_fallback:
            pool = await get_pool()
            async with pool.acquire() as conn:
                for src in product_sources[:4]:
                    src_name = str(src.get("source", ""))
                    prod_name = src_name.replace("Product - ", "").replace("Product -", "").strip()
                    if prod_name:
                        row = await conn.fetchrow(
                            "SELECT name, category, description, pricing, image_url "
                            "FROM kb_products WHERE tenant_id=$1 AND name ILIKE $2 LIMIT 1",
                            tenant["id"], f"%{prod_name}%",
                        )
                        if row:
                            products.append({
                                "name": row["name"],
                                "category": row["category"],
                                "description": (row["description"] or "")[:100],
                                "pricing": row["pricing"],
                                "image_url": row.get("image_url") or "",
                            })
    except Exception as e:
        logger.warning(f"Product extraction from RAG failed: {e}")

    await app.state.payments.log_usage(
        tenant_id=tenant["id"], domain=tenant.get("domain", ""),
        session_id=session["id"], event_type="chat",
        tokens_in=usage_in, tokens_out=usage_out,
    )

    # ── Meter tokens for billing ──
    await _meter_tokens(tenant["id"], usage_in, usage_out)

    # ── Persist session to DB (incremental upsert) ──
    try:
        pool = await get_pool()
        visitor_id = req.session_id.split("_")[0] if "_" in req.session_id else "visitor"
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sessions (id, tenant_id, visitor_id, language, message_count, "
                "pii_collected, chat_history, last_active) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, NOW()) "
                "ON CONFLICT (id) DO UPDATE SET "
                "message_count=$5, pii_collected=$6, chat_history=$7::jsonb, last_active=NOW()",
                session["id"], tenant["id"], visitor_id,
                str(req.language), session["message_count"],
                session.get("pii_collected", False),
                json.dumps(session.get("history", [])),
            )
    except Exception as e:
        logger.warning(f"Session upsert failed: {e}")

    return ChatResponse(
        session_id=session["id"],
        message=result["text"],
        audio_url=audio_url,
        sources=result.get("sources", []),
        needs_pii_prompt=needs_pii,
        language=result.get("detected_language", req.language),
        suggested_products=products,
    )


class _TestChatReq(BaseModel):
    message: str
    session_id: str = ""


@app.post("/v1/chat/test", tags=["Dashboard"])
async def chat_test(req: _TestChatReq, current: dict = Depends(get_current_user)):
    """Dashboard test chat — JWT auth, uses tenant's own RAG + Gemini config."""
    _must_be_user(current)
    tid = current["tenant_id"]

    # Load tenant config for Gemini prompt context
    pool = await get_pool()
    async with pool.acquire() as conn:
        tenant_row = await conn.fetchrow(
            "SELECT t.*, wc.greeting, wc.primary_color, wc.name AS widget_name "
            "FROM tenants t LEFT JOIN widget_configs wc ON wc.tenant_id = t.id "
            "WHERE t.id=$1", tid,
        )
    if not tenant_row:
        raise HTTPException(status_code=404, detail="Tenant not found")

    tenant_cfg = dict(tenant_row)
    tenant_cfg["id"] = tid

    session_id = req.session_id or f"_test_{tid}_{secrets.token_hex(4)}"
    session = await app.state.sessions.get_or_create(session_id, tid)

    context_chunks = await app.state.ai.rag_retrieve(query=req.message, tenant_id=tid, top_k=8)
    history = session.get("history", [])

    result = await app.state.ai.gemini_chat(
        message=req.message, history=history,
        context_chunks=context_chunks, tenant_config=tenant_cfg, language="auto",
    )   
    
    bot_message = result.get("text") or result.get("response") or "No response from AI"

    history.append({"role": "user",      "content": req.message,   "ts": time.time()})
    history.append({"role": "assistant", "content": bot_message,   "ts": time.time()})

    session["history"]       = history[-20:]
    session["message_count"] = session.get("message_count", 0) + 1
    await app.state.sessions.save(session_id, session)

    return {
        "session_id": session_id,
        "message": bot_message,
        "sources": result.get("sources", []),
    }


@app.post("/v1/cv-search", tags=["Widget"])
async def cv_search(
    file: UploadFile = File(...),
    session_id: Optional[str] = None,
    tenant=Depends(verify_widget_jwt),
):
    img_bytes = await file.read()
    matches   = await app.state.ai.cv_search(
        image_bytes=img_bytes, tenant_id=tenant["id"], top_k=3
    )
    return {"matches": matches, "session_id": session_id}


@app.post("/v1/behavior", tags=["Widget"])
async def track_behavior(event: BehaviorEvent, tenant=Depends(verify_widget_jwt)):
    return await app.state.ai.behavior_process(
        event_dict=event.dict(), tenant_id=tenant["id"]
    )


@app.post("/v1/lead", tags=["Widget"])
async def capture_lead(lead: LeadCapture, tenant=Depends(verify_widget_jwt)):
    session = await app.state.sessions.get_or_create(lead.session_id, tenant["id"])
    session["pii_collected"] = True
    session["pii"] = {"name": lead.name, "email": lead.email, "phone": lead.phone}
    await app.state.sessions.save(lead.session_id, session)

    # Persist PII flag to DB session row
    try:
        pool = await get_pool()
        visitor_id = lead.session_id.split("_")[0] if "_" in lead.session_id else "visitor"
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sessions (id, tenant_id, visitor_id, pii_collected, last_active) "
                "VALUES ($1, $2, $3, TRUE, NOW()) "
                "ON CONFLICT (id) DO UPDATE SET pii_collected=TRUE, last_active=NOW()",
                lead.session_id, tenant["id"], visitor_id,
            )
    except Exception as e:
        logger.warning(f"Session PII update failed: {e}")

    lead_id = await app.state.leads.save(
        tenant_id=tenant["id"], session_id=lead.session_id,
        pii={"name": lead.name, "email": lead.email, "phone": lead.phone},
    )
    return {"lead_id": lead_id, "status": "captured"}


async def _process_closed_session(session_id: str, session: dict, tenant: dict, history: list):
    # 1. Analyze intent via Gemini
    intent = {}
    try:
        intent = await app.state.ai.gemini_analyze_intent(history=history, tenant_config=tenant)
    except Exception as e:
        logger.warning(f"Intent analysis failed: {e}")
        intent = {
            "intent": "enquiry", "sentiment": "neutral",
            "summary": "Customer had a conversation.",
            "lead_quality": "warm", "recommended_followup": "Follow up within 24h",
            "products_interested": [],
        }

    pii = session.get("pii") or {}

    # 2. Persist full session to DB
    try:
        pool = await get_pool()
        visitor_id = session_id.split("_")[0] if "_" in session_id else "visitor"
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO sessions (id, tenant_id, visitor_id, language, message_count, "
                    "pii_collected, chat_history, intent, sentiment, notes, ended_at, last_active) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, NOW(), NOW()) "
                    "ON CONFLICT (id) DO UPDATE SET "
                    "chat_history=$7::jsonb, intent=$8, sentiment=$9, notes=$10, ended_at=NOW(), last_active=NOW()",
                    session_id, tenant["id"], visitor_id,
                    session.get("language", "en"), session.get("message_count", 0),
                    session.get("pii_collected", False),
                    json.dumps(history),
                    intent.get("intent", "enquiry"),
                    intent.get("sentiment", "neutral"),
                    f"{intent.get('summary', '')}\n\nRecommended: {intent.get('recommended_followup', '')}".strip()
                )

                # 3. Create or merge lead
                has_pii = bool(pii.get("phone") or pii.get("email"))
                
                lead_name = pii.get("name", "Guest") if has_pii else "Guest"
                lead_email = pii.get("email", "").strip().lower() if pii.get("email") else None
                lead_phone = pii.get("phone", "").strip() if pii.get("phone") else None
                
                quality = intent.get("lead_quality", "warm") if has_pii else "anon"
                product_interest = ", ".join(intent.get("products_interested", []))
                product_quantities = json.dumps(intent.get("product_quantities", {}))
                sentiment = intent.get("sentiment", "neutral")
                notes = f"{intent.get('summary', '')}\n\nRecommended: {intent.get('recommended_followup', '')}".strip()
                
                if not has_pii:
                    lead_id = secrets.token_hex(8)
                    await conn.execute(
                        "INSERT INTO leads (id, tenant_id, session_id, name, email, phone, "
                        "intent, product_interest, product_quantities, quality, sentiment, notes, "
                        "canonical_lead_id, is_merged) "
                        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12, NULL, FALSE)",
                        lead_id, tenant["id"], session_id,
                        lead_name, lead_email, lead_phone,
                        intent.get("intent", "enquiry"), product_interest, product_quantities,
                        quality, sentiment, notes
                    )
                    logger.info(f"✅ Created guest lead {lead_id}")
                else:
                    # Always match by phone number for lead deduplication
                    if not lead_phone:
                        # No phone — create a new standalone lead (can't merge without phone)
                        new_lead_id = secrets.token_hex(8)
                        await conn.execute(
                            "INSERT INTO leads (id, tenant_id, session_id, name, email, phone, "
                            "intent, product_interest, product_quantities, quality, sentiment, notes, "
                            "canonical_lead_id, is_merged) "
                            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12, NULL, FALSE)",
                            new_lead_id, tenant["id"], session_id,
                            lead_name, lead_email, lead_phone,
                            intent.get("intent", "enquiry"), product_interest, product_quantities,
                            quality, sentiment, notes
                        )
                        lead_id = new_lead_id
                        logger.info(f"✅ Created new lead {lead_id} (no phone for merge)")
                    else:
                        # Prevent concurrent identical leads with Advisory Lock
                        lock_str = f"{tenant['id']}:{lead_phone}"
                        lock_id = int(hashlib.md5(lock_str.encode()).hexdigest()[:15], 16)
                        await conn.execute("SELECT pg_advisory_xact_lock($1)", lock_id)
                    
                        canonical = await conn.fetchrow(
                            "SELECT id, name, notes FROM leads "
                            "WHERE tenant_id=$1 AND phone=$2 "
                            "AND DATE(created_at AT TIME ZONE 'UTC') = DATE(NOW() AT TIME ZONE 'UTC') "
                            "ORDER BY created_at DESC LIMIT 1",
                            tenant["id"], lead_phone
                        )
                    
                        if canonical:
                            new_notes = (canonical["notes"] + f"\n\n--- Session ---\n{notes}").strip() if canonical["notes"] else notes
                            # Track different names (case-insensitive)
                            name_update_sql = ""
                            if lead_name and lead_name.lower() != (canonical["name"] or "").lower():
                                name_update_sql = ", merged_names = array_append(merged_names, $4)"
                            await conn.execute(
                                "UPDATE leads SET is_merged=TRUE, merge_count=merge_count+1, "
                                "merged_session_ids = array_append(merged_session_ids, $1), "
                                f"notes=$2{name_update_sql} WHERE id=$3",
                                session_id, new_notes, canonical["id"],
                                *([lead_name] if name_update_sql else [])
                            )
                            lead_id = canonical["id"]
                            logger.info(f"🔀 Merged return lead into {lead_id} (same day, phone match)")
                        else:
                            new_lead_id = secrets.token_hex(8)
                            await conn.execute(
                                "INSERT INTO leads (id, tenant_id, session_id, name, email, phone, "
                                "intent, product_interest, product_quantities, quality, sentiment, notes, "
                                "canonical_lead_id, is_merged) "
                                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12, NULL, FALSE)",
                                new_lead_id, tenant["id"], session_id,
                                lead_name, lead_email, lead_phone,
                                intent.get("intent", "enquiry"), product_interest, product_quantities,
                                quality, sentiment, notes
                            )
                            lead_id = new_lead_id
                            logger.info(f"✅ Created new lead {lead_id}")

                await conn.execute(
                    "UPDATE sessions SET lead_id=$1 WHERE id=$2",
                    lead_id, session_id,
                )
    except Exception as e:
        logger.error(f"Session/lead persistence failed: {e}")

    # 3b. Update session_metadata visitor_name if PII was collected
    try:
        visitor_name = pii.get("name", "").strip() if pii else ""
        if visitor_name:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE session_metadata SET visitor_name=$1 WHERE session_id=$2",
                    visitor_name, session_id,
                )
    except Exception as e:
        logger.warning(f"Session metadata visitor_name update failed: {e}")

    # 4. Send email brief
    try:
        await app.state.leads.send_brief(
            tenant=tenant, session_id=session_id,
            pii=pii, intent=intent, history=history,
        )
    except Exception as e:
        logger.warning(f"Lead brief email failed: {e}")

    await app.state.sessions.delete(session_id)


@app.post("/v1/session/close", tags=["Widget"])
async def close_session(session_id: str, bg_tasks: BackgroundTasks, tenant=Depends(verify_widget_jwt)):
    session = await app.state.sessions.get_or_create(session_id, tenant["id"])
    history = session.get("history", [])
    if not history:
        return {"status": "empty_session"}

    # Discard non-blocking heavy ops to background
    bg_tasks.add_task(_process_closed_session, session_id, session, tenant, history)
    return {"status": "queued"}


@app.post("/v1/ingest", tags=["Widget"])
async def ingest_document(file: UploadFile = File(...), current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    if file.size and file.size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large")
    content = await file.read()
    job_id  = await app.state.ai.rag_ingest(
        content=content, filename=file.filename,
        content_type=file.content_type, tenant_id=current["tenant_id"]
    )
    return {"job_id": job_id, "status": "processing", "filename": file.filename}


@app.get("/v1/ingest/{job_id}", tags=["Widget"])
async def ingest_status(job_id: str, current: dict = Depends(get_current_user)):
    _must_be_user(current)
    return await app.state.ai.rag_get_job_status(job_id)


# ── WebSocket Managers ────────────────────────────────────────────────────────
class TicketConnectionManager:
    def __init__(self):
        # ticket_id -> list of active websockets
        self.active_connections: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, ticket_id: str):
        await websocket.accept()
        if ticket_id not in self.active_connections:
            self.active_connections[ticket_id] = []
        self.active_connections[ticket_id].append(websocket)

    def disconnect(self, websocket: WebSocket, ticket_id: str):
        if ticket_id in self.active_connections:
            if websocket in self.active_connections[ticket_id]:
                self.active_connections[ticket_id].remove(websocket)
            if not self.active_connections[ticket_id]:
                del self.active_connections[ticket_id]

    async def broadcast(self, ticket_id: str, message: dict):
        if ticket_id in self.active_connections:
            # Create a copy of the list to iterate to avoid issues if a socket disconnects mid-loop
            connections = list(self.active_connections[ticket_id])
            for connection in connections:
                try:
                    await connection.send_json(message)
                except Exception as e:
                    logger.error(f"Error broadcasting to WS: {e}")
                    self.disconnect(connection, ticket_id)

ticket_manager = TicketConnectionManager()


# ── Short-lived WS token endpoints ────────────────────────────────────────────
import jwt as pyjwt
_WS_SECRET = os.environ.get("SECRET_KEY", "change-me")
_WS_TOKEN_TTL = 30  # seconds


@app.post("/v1/ws/voice-token", tags=["Widget"])
async def create_voice_ws_token(tenant=Depends(verify_widget_jwt)):
    """Mint a short-lived token (30s) for WebSocket voice connection."""
    payload = {
        "sub": tenant["id"],
        "type": "ws_voice",
        "plan": tenant.get("plan"),
        "name": tenant.get("name"),
        "exp": int(time.time()) + _WS_TOKEN_TTL,
    }
    token = pyjwt.encode(payload, _WS_SECRET, algorithm="HS256")
    return {"ws_token": token}


@app.post("/v1/ws/ticket-token", tags=["Tickets"])
async def create_ticket_ws_token(current: dict = Depends(get_current_user)):
    """Mint a short-lived token (30s) for WebSocket ticket connection."""
    payload = {
        "sub": current["id"],
        "role": current.get("role"),
        "account_type": current.get("account_type"),
        "tenant_id": current.get("tenant_id"),
        "type": "ws_ticket",
        "exp": int(time.time()) + _WS_TOKEN_TTL,
    }
    token = pyjwt.encode(payload, _WS_SECRET, algorithm="HS256")
    return {"ws_token": token}


@app.websocket("/ws/voice/{session_id}")
async def voice_ws(websocket: WebSocket, session_id: str, token: str = Query(...)):
    """Voice WebSocket — accepts a short-lived ws_voice token (from /v1/ws/voice-token)."""
    # Validate short-lived token
    try:
        payload = pyjwt.decode(token, _WS_SECRET, algorithms=["HS256"])
        if payload.get("type") != "ws_voice":
            await websocket.close(code=4001)
            return
        tenant_id = payload.get("sub")
    except pyjwt.PyJWTError:
        await websocket.close(code=4001)
        return

    tenant  = {"id": tenant_id, "name": payload.get("name", ""), "plan": payload.get("plan", "")}
    session = await app.state.sessions.get_or_create(session_id, tenant["id"])
    await websocket.accept()

    greeting_audio = await app.state.ai.tts_synthesize(text="Hello! How can I help you today?", language="en")
    await websocket.send_json({"type": "greeting", "audio_url": greeting_audio})

    transcript_buffer: list[str] = []
    try:
        while True:
            data = await asyncio.wait_for(websocket.receive(), timeout=60.0)
            if "bytes" in data:
                partial = await app.state.ai.stt_process(
                    audio_chunk=data["bytes"],
                    session_id=session_id,
                    language=session.get("language", "en"),
                )
                if partial.get("text"):
                    transcript_buffer.append(partial["text"])
                    await websocket.send_json({"type": "partial_transcript",
                                               "text": " ".join(transcript_buffer)})
            elif "text" in data:
                msg = json.loads(data["text"])
                if msg.get("type") == "silence_gap" and transcript_buffer:
                    final_text = " ".join(transcript_buffer)
                    transcript_buffer.clear()
                    context = await app.state.ai.rag_retrieve(query=final_text, tenant_id=tenant["id"], top_k=8)
                    history = session.get("history", [])
                    result  = await app.state.ai.gemini_chat(
                        message=final_text, history=history,
                        context_chunks=context, tenant_config=tenant,
                        language=session.get("language", "en"),
                    )
                    history.append({"role": "user",      "content": final_text,   "ts": time.time()})
                    history.append({"role": "assistant", "content": result["text"], "ts": time.time()})
                    session["history"]       = history[-20:]
                    session["message_count"] = session.get("message_count", 0) + 1
                    await app.state.sessions.save(session_id, session)
                    audio_url = await app.state.ai.tts_synthesize(
                        text=result["text"],
                        language=session.get("language", "en"),
                        session_id=session_id,
                    )
                    await websocket.send_json({
                        "type": "response", "text": result["text"], "audio_url": audio_url,
                        "needs_pii_prompt": (session["message_count"] >= 3
                                             and not session.get("pii_collected")),
                    })
                elif msg.get("type") == "set_language":
                    session["language"] = msg["language"]
                    await app.state.sessions.save(session_id, session)
                elif msg.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass
    except asyncio.TimeoutError:
        await websocket.close(code=1001)


@app.post("/v1/stt", tags=["Widget"])
async def stt_transcribe(body: dict, tenant=Depends(verify_widget_jwt)):
    """
    Speech-to-Text endpoint for the widget microphone button.
    Accepts base64-encoded audio, returns the transcript text.
    The WebSocket /ws/voice/{session_id} is a separate streaming-voice mode
    (full-duplex, token-gated) — this REST endpoint is the simpler
    push-to-transcribe path used by the mic button in the widget.
    """
    audio_b64 = body.get("audio_b64", "")
    session_id = body.get("session_id", "")
    language   = body.get("language", "auto")
    if not audio_b64:
        raise HTTPException(status_code=400, detail="audio_b64 is required")
    try:
        audio_bytes = base64.b64decode(audio_b64)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid base64 audio data")
    try:
        result = await app.state.ai.stt_process(
            audio_chunk=audio_bytes,
            session_id=session_id or "stt_anon",
            language=language if language != "auto" else "en",
        )
        return {"text": result.get("text", "")}
    except Exception as e:
        logger.warning(f"STT failed: {e}")
        raise HTTPException(status_code=502, detail=f"STT service error: {e}")
# ═════════════════════════════════════════════════════════════════════════════
# USER DASHBOARD  (JWT — owner | member)

@app.get("/v1/analytics", tags=["Dashboard"])
async def get_analytics(
    days: int = 30, 
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current: dict = Depends(get_current_user)
):
    _must_be_user(current)
    tid = current["tenant_id"]
    
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # make end date inclusive of the whole day
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1, seconds=-1)
    else:
        start_dt = now - timedelta(days=days)
        end_dt = now

    pool = await get_pool()
    async with pool.acquire() as conn:
        daily = await conn.fetch(
            "SELECT DATE(s.started_at) AS day,"
            "       COUNT(DISTINCT s.id) AS sessions,"
            "       COUNT(DISTINCT l.id) AS leads,"
            "       COUNT(DISTINCT CASE WHEN l.quality='hot' THEN l.id END) AS hot_leads,"
            "       COUNT(DISTINCT CASE WHEN l.quality='warm' THEN l.id END) AS warm_leads,"
            "       COUNT(DISTINCT CASE WHEN l.quality='cold' THEN l.id END) AS cold_leads,"
            "       COUNT(DISTINCT CASE WHEN l.sentiment='positive' THEN l.id END) AS positive_sent,"
            "       COUNT(DISTINCT CASE WHEN l.sentiment='neutral' THEN l.id END) AS neutral_sent,"
            "       COUNT(DISTINCT CASE WHEN l.sentiment='negative' THEN l.id END) AS negative_sent"
            " FROM sessions s LEFT JOIN leads l ON l.session_id = s.id"
            " WHERE s.tenant_id=$1 AND s.started_at >= $2 AND s.started_at <= $3"
            " GROUP BY DATE(s.started_at) ORDER BY day",
            tid, start_dt, end_dt,
        )
        totals = await conn.fetchrow(
            "SELECT COUNT(*) AS total_sessions FROM sessions"
            " WHERE tenant_id=$1 AND started_at >= $2 AND started_at <= $3", 
            tid, start_dt, end_dt,
        )
        leads_row = await conn.fetchrow(
            "SELECT COUNT(*) AS total_leads,"
            "       COUNT(CASE WHEN quality='hot' THEN 1 END) AS hot_leads,"
            "       COUNT(CASE WHEN quality='anon' THEN 1 END) AS guest_leads,"
            "       COUNT(CASE WHEN quality!='anon' THEN 1 END) AS named_leads"
            " FROM leads WHERE tenant_id=$1 AND created_at >= $2 AND created_at <= $3", 
            tid, start_dt, end_dt,
        )
        intents = await conn.fetch(
            "SELECT intent, COUNT(*) AS cnt FROM sessions"
            " WHERE tenant_id=$1 AND intent IS NOT NULL AND started_at >= $2 AND started_at <= $3"
            " GROUP BY intent ORDER BY cnt DESC LIMIT 5", 
            tid, start_dt, end_dt,
        )
        langs = await conn.fetch(
            "SELECT language, COUNT(*) AS cnt FROM sessions"
            " WHERE tenant_id=$1 AND started_at >= $2 AND started_at <= $3"
            " GROUP BY language ORDER BY cnt DESC", 
            tid, start_dt, end_dt,
        )
        recent_leads = await conn.fetch(
            "SELECT id, name, intent, product_interest AS product, quality,"
            "       to_char(created_at,'HH12:MI AM') AS time"
            " FROM leads WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 8", tid,
        )
        tenant_row = await conn.fetchrow("SELECT company FROM tenants WHERE id=$1", tid)

    total_s = totals["total_sessions"]  if totals    else 0
    total_l = leads_row["total_leads"]  if leads_row else 0
    hot_l   = leads_row["hot_leads"]    if leads_row else 0
    guest_l = leads_row["guest_leads"]  if leads_row else 0
    named_l = leads_row["named_leads"]  if leads_row else 0
    total_i = sum(r["cnt"] for r in intents) or 1

    return {
        "store_name":        (tenant_row["company"] if tenant_row else "") or "",
        "total_sessions":    total_s,
        "total_leads":       total_l,
        "hot_leads":         hot_l,
        "guest_leads":       guest_l,
        "conversion_rate":   round(named_l / total_s * 100, 1) if total_s else 0,
        "sessions_change":   0,
        "leads_change":      0,
        "hot_change":        0,
        "conversion_change": 0,
        "sessions_by_day":   [dict(r) for r in daily],
        "top_intents":       [{"intent": r["intent"],
                               "pct": round(r["cnt"] / total_i * 100)} for r in intents],
        "language_breakdown":{r["language"]: r["cnt"] for r in langs},
        "recent_leads":      [dict(r) for r in recent_leads],
    }


@app.get("/v1/leads/intents", tags=["Dashboard"])
async def get_lead_intents(current: dict = Depends(get_current_user)):
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT intent FROM leads WHERE tenant_id=$1 AND intent IS NOT NULL ORDER BY intent",
            current["tenant_id"]
        )
    return [r["intent"] for r in rows]


@app.get("/v1/leads", tags=["Dashboard"])
async def get_leads(
    filter: str = "all",
    intent: str = "all",
    country: str = "all",
    date_start: str = None,
    date_end: str = None,
    page:   int = 1,
    limit:  int = 50,
    current: dict = Depends(get_current_user),
):
    _must_be_user(current)
    pool   = await get_pool()
    offset = (page - 1) * limit
    async with pool.acquire() as conn:
        conds:  list[str] = ["l.tenant_id=$1"]
        params: list      = [current["tenant_id"]]
        if filter == "anon":
            conds.append("l.quality='anon'")
        elif filter in ("hot", "warm", "cold"):
            params.append(filter)
            conds.append(f"l.quality=${len(params)}")
        if intent != "all":
            params.append(intent)
            conds.append(f"l.intent=${len(params)}")
        if country != "all" and country:
            params.append(f"{country} %")
            conds.append(f"l.phone LIKE ${len(params)}")
        if date_start:
            params.append(datetime.fromisoformat(date_start.replace("Z", "+00:00")) if "T" in date_start else datetime.fromisoformat(date_start + "T00:00:00+00:00"))
            conds.append(f"l.created_at >= ${len(params)}")
        if date_end:
            params.append(datetime.fromisoformat(date_end.replace("Z", "+00:00")) if "T" in date_end else datetime.fromisoformat(date_end + "T23:59:59+00:00"))
            conds.append(f"l.created_at <= ${len(params)}")
        where = " AND ".join(conds)
        rows  = await conn.fetch(
            f"SELECT l.*, "
            f"(SELECT COUNT(*) FROM leads r WHERE r.canonical_lead_id = l.id) AS return_visit_count "
            f"FROM leads l WHERE {where} "
            f"ORDER BY l.created_at DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
            *params, limit, offset,
        )
        total = await conn.fetchval(f"SELECT COUNT(*) FROM leads l WHERE {where}", *params)
    return {"leads": [dict(r) for r in rows], "total": total, "page": page}


@app.get("/v1/leads/{lead_id}/conversation", tags=["Dashboard"])
async def lead_conversation(lead_id: str, current: dict = Depends(get_current_user)):
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM leads WHERE id=$1 AND tenant_id=$2",
            lead_id, current["tenant_id"],
        )
        if not row:
            raise HTTPException(status_code=404, detail="Lead not found")
        lead = dict(row)

        # Fetch linked session's chat history
        session_data = None
        messages = []
        if lead.get("session_id"):
            sess_row = await conn.fetchrow(
                "SELECT id, language, message_count, chat_history, intent, sentiment, "
                "started_at, ended_at, last_active "
                "FROM sessions WHERE id=$1 AND tenant_id=$2",
                lead["session_id"], current["tenant_id"],
            )
            if sess_row:
                session_data = {
                    "id": sess_row["id"],
                    "language": sess_row["language"],
                    "message_count": sess_row["message_count"],
                    "intent": sess_row["intent"],
                    "sentiment": sess_row["sentiment"],
                    "started_at": sess_row["started_at"].isoformat() if sess_row["started_at"] else None,
                    "ended_at": sess_row["ended_at"].isoformat() if sess_row["ended_at"] else None,
                    "last_active": sess_row["last_active"].isoformat() if sess_row["last_active"] else None,
                }
                raw_history = sess_row.get("chat_history")
                if raw_history:
                    import json as _json
                    if isinstance(raw_history, str):
                        messages = _json.loads(raw_history)
                    elif isinstance(raw_history, list):
                        messages = raw_history
                    else:
                        messages = list(raw_history)

        # Also fetch merged sessions' histories if this is a merged lead
        merged_sessions = []
        if lead.get("is_merged") and lead.get("merged_session_ids"):
            for sid in lead["merged_session_ids"]:
                ms_row = await conn.fetchrow(
                    "SELECT id, chat_history, started_at, ended_at, notes FROM sessions WHERE id=$1 AND tenant_id=$2",
                    sid, current["tenant_id"],
                )
                if ms_row:
                    ms_history = ms_row.get("chat_history") or []
                    if isinstance(ms_history, str):
                        ms_history = _json.loads(ms_history)
                    merged_sessions.append({
                        "session_id": ms_row["id"],
                        "started_at": ms_row["started_at"].isoformat() if ms_row["started_at"] else None,
                        "ended_at": ms_row["ended_at"].isoformat() if ms_row["ended_at"] else None,
                        "notes": ms_row["notes"],
                        "messages": list(ms_history),
                    })

    return {
        "lead": lead,
        "session": session_data,
        "messages": messages,
        "merged_sessions": merged_sessions,
    }


@app.get("/v1/knowledge/list", tags=["Dashboard"])
async def list_knowledge(current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM ingest_jobs WHERE tenant_id=$1 ORDER BY created_at DESC",
            current["tenant_id"],
        )
    return {"docs": [dict(r) for r in rows]}


@app.get("/v1/knowledge/status", tags=["Dashboard"])
async def get_knowledge_status(current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]
    pool = await get_pool()
    
    # 1. Check SQL metadata
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*) as doc_count, SUM(chunks_indexed) as sql_chunks "
            "FROM ingest_jobs WHERE tenant_id=$1 AND status='completed'",
            tid
        )
    sql_chunks = row["sql_chunks"] or 0
    doc_count  = row["doc_count"]  or 0
    
    # 2. Check Vector DB via Proxy
    proxy_res = await app.state.ai.rag_get_stats(tid)
    vector_count = proxy_res.get("count", 0)
    ai_status    = proxy_res.get("status", "offline")
    
    # 3. Determine overall status
    status = "online"
    reason = "Vector DB is healthy"
    
    if ai_status == "offline" or ai_status == "error":
        status = "offline"
        reason = "AI Backend unreachable"
    elif sql_chunks > 0 and vector_count == 0:
        status = "issue"
        reason = "Embeddings missing in vector store"
    elif sql_chunks > 0 and vector_count < (sql_chunks * 0.8): # Significant mismatch
        status = "issue"
        reason = f"Sync mismatch: SQL={sql_chunks}, Vector={vector_count}"
        
    return {
        "status": status,
        "reason": reason,
        "doc_count": doc_count,
        "sql_chunks": sql_chunks,
        "vector_chunks": vector_count,
        "backend_status": ai_status
    }


@app.get("/v1/knowledge/qa", tags=["Dashboard"])
async def list_qa(current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM knowledge_qa WHERE tenant_id=$1 ORDER BY created_at DESC",
            current["tenant_id"],
        )
    return {"qa": [dict(r) for r in rows]}


@app.post("/v1/knowledge/qa", tags=["Dashboard"])
async def add_qa(req: KnowledgeQACreate, current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    pool = await get_pool()
    qa_id = secrets.token_hex(8)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO knowledge_qa (id, tenant_id, question, answer) VALUES ($1, $2, $3, $4)",
                qa_id, current["tenant_id"], req.question, req.answer,
            )
            await _audit(conn, current, "create", "knowledge_qa", qa_id, {"question": req.question[:50]}, tenant_id=current["tenant_id"])

    # Sync to RAG engine (non-blocking — don't fail the API if AI_Backend is down)
    try:
        await app.state.ai.rag_ingest_qa(
            qa_id=qa_id, question=req.question,
            answer=req.answer, tenant_id=current["tenant_id"],
        )
    except Exception as e:
        logger.warning(f"Q/A RAG sync failed (qa_id={qa_id}): {e}")

    return {"id": qa_id, "status": "created"}


@app.delete("/v1/knowledge/qa/{qa_id}", tags=["Dashboard"])
async def delete_qa(qa_id: str, current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        # verify ownership
        row = await conn.fetchrow(
            "SELECT id FROM knowledge_qa WHERE id=$1 AND tenant_id=$2",
            qa_id, current["tenant_id"]
        )
        if not row:
            raise HTTPException(status_code=404, detail="Q/A not found")
            
        async with conn.transaction():
            await conn.execute("DELETE FROM knowledge_qa WHERE id=$1", qa_id)
            await _audit(conn, current, "delete", "knowledge_qa", qa_id, {}, tenant_id=current["tenant_id"])

    # Remove from RAG engine (non-blocking)
    try:
        await app.state.ai.rag_delete_qa(qa_id=qa_id, tenant_id=current["tenant_id"])
    except Exception as e:
        logger.warning(f"Q/A RAG delete failed (qa_id={qa_id}): {e}")

    return {"status": "deleted"}


@app.delete("/v1/knowledge/{doc_id}", tags=["Dashboard"])
async def delete_document(doc_id: str, current: dict = Depends(get_current_user)):
    _must_be_owner_or_member(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM ingest_jobs WHERE id=$1 AND tenant_id=$2",
            doc_id, current["tenant_id"],
        )
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        await conn.execute("DELETE FROM ingest_jobs WHERE id=$1", doc_id)
        await _audit(conn, current, "delete", "ingest_job", doc_id, {}, tenant_id=current["tenant_id"])

    # Remove from RAG engine
    try:
        await app.state.ai.rag_delete_doc(doc_id=doc_id, tenant_id=current["tenant_id"])
    except Exception as e:
        logger.warning(f"Doc RAG delete failed (doc_id={doc_id}): {e}")

    return {"status": "deleted", "doc_id": doc_id}


# ── Web Scraping ───────────────────────────────────────────────────────────────

import ipaddress as _ipaddress
import socket as _socket
from urllib.parse import urlparse as _urlparse

_BLOCKED_PORTS = {22, 3306, 5432, 6379, 27017}


def _validate_domain(domain: str) -> str:
    """Reject private/loopback/reserved IPs (SSRF protection) for tenant domain field."""
    url = domain.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    parsed = _urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(400, "Invalid domain")
    if parsed.port and parsed.port in _BLOCKED_PORTS:
        raise HTTPException(400, "Blocked port in domain")
    # Allow localhost in development mode
    _dev = os.getenv("DEV_MODE", "").lower() in ("1", "true", "yes")
    if _dev and hostname in ("localhost", "127.0.0.1", "::1"):
        return domain.strip()
    try:
        for res in _socket.getaddrinfo(hostname, None):
            ip = _ipaddress.ip_address(res[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                raise HTTPException(400, "Domain resolves to a private/internal address")
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(400, "Could not resolve domain hostname")
    return domain.strip()


# ── Structured Knowledge Base ──────────────────────────────────────────────────
# Company data + Product catalog + Vector sync

# Field definitions for company data sections (ordered by priority)
_KB_SECTIONS = {
    "introduction": [
        ("company_name", "Company Name"),
        ("tagline", "Tagline"),
        ("about_us", "About Us"),
        ("mission", "Mission"),
        ("vision", "Vision"),
    ],
    "company_details": [
        ("industry", "Industry"),
        ("founded_year", "Founded Year"),
        ("team_size", "Team Size"),
        ("headquarters", "Headquarters"),
        ("website_url", "Website URL"),
        ("social_links", "Social Links"),
    ],
    "contact": [
        ("email", "Email"),
        ("phone", "Phone"),
        ("whatsapp", "WhatsApp"),
        ("address", "Address"),
        ("city", "City"),
        ("state", "State"),
        ("country", "Country"),
        ("pincode", "Pincode"),
        ("working_hours", "Working Hours"),
        ("map_url", "Map URL"),
    ],
}


@app.get("/v1/kb/sections", tags=["Dashboard"])
async def kb_section_definitions():
    """Return the field definitions for company data sections (used by frontend)."""
    return {"sections": _KB_SECTIONS}


@app.get("/v1/kb/company", tags=["Dashboard"])
async def kb_get_company(current: dict = Depends(get_current_user)):
    """Fetch all company data fields for the current tenant."""
    _must_be_user(current)
    tid = current["tenant_id"]
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, section, field_key, field_value, display_order "
            "FROM kb_company_data WHERE tenant_id=$1 ORDER BY section, display_order",
            tid,
        )
    return {"data": [dict(r) for r in rows], "sections": _KB_SECTIONS}


@app.put("/v1/kb/company", tags=["Dashboard"])
async def kb_update_company(req: CompanyDataUpdate, current: dict = Depends(get_current_user)):
    """Batch upsert company data fields. Each item: {section, field_key, field_value}."""
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]
    pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            for idx, item in enumerate(req.data):
                # Validate section
                if item.section not in _KB_SECTIONS:
                    continue
                row_id = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO kb_company_data (id, tenant_id, section, field_key, field_value, display_order, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, NOW()) "
                    "ON CONFLICT (tenant_id, section, field_key) "
                    "DO UPDATE SET field_value=$5, display_order=$6, updated_at=NOW()",
                    row_id, tid, item.section, item.field_key, item.field_value, idx,
                )
    return {"status": "saved", "count": len(req.data)}


@app.get("/v1/kb/product-template", tags=["Dashboard"])
async def kb_get_product_template(current: dict = Depends(get_current_user)):
    """Return the product import template column definitions."""
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT value FROM platform_settings WHERE key='product_import_template'"
        )
    import json as _json
    default = [
        {"key": "name", "label": "Name", "required": True},
        {"key": "category", "label": "Category", "required": True},
        {"key": "sub_category", "label": "Sub Category", "required": False},
        {"key": "description", "label": "Description", "required": False},
        {"key": "pricing", "label": "Pricing", "required": False},
        {"key": "min_order_qty", "label": "Min Order Qty", "required": False},
        {"key": "image_url", "label": "Image URL", "required": False},
        {"key": "source_url", "label": "Source URL", "required": False},
    ]
    try:
        columns = _json.loads(val) if val else default
    except Exception:
        columns = default
    return {"columns": columns}


@app.get("/v1/kb/products", tags=["Dashboard"])
async def kb_list_products(current: dict = Depends(get_current_user)):
    """List all products for the current tenant."""
    _must_be_user(current)
    tid = current["tenant_id"]
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM kb_products WHERE tenant_id=$1 ORDER BY category, name", tid,
        )
    products = [dict(r) for r in rows]
    for p in products:
        if p.get("image_url"):
            p["image_url"] = app.state.s3.resolve_url(p["image_url"])
    return {"products": products}


@app.post("/v1/kb/products", status_code=201, tags=["Dashboard"])
async def kb_create_product(req: ProductCreate, current: dict = Depends(get_current_user)):
    """Create a new product entry."""
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]
    pid = secrets.token_hex(8)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO kb_products (id, tenant_id, category, sub_category, name, description, "
            "image_url, pricing, min_order_qty, source_url) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            pid, tid, req.category, req.sub_category, req.name, req.description,
            req.image_url, req.pricing, req.min_order_qty, req.source_url,
        )
    return {"id": pid, "status": "created"}


@app.put("/v1/kb/products/{product_id}", tags=["Dashboard"])
async def kb_update_product(product_id: str, req: ProductUpdate, current: dict = Depends(get_current_user)):
    """Update an existing product."""
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]
    pool = await get_pool()

    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id, image_url FROM kb_products WHERE id=$1 AND tenant_id=$2", product_id, tid,
        )
        if not existing:
            raise HTTPException(404, "Product not found")

        # Build dynamic update
        updates, params, idx = [], [product_id, tid], 3
        for field in ("category", "sub_category", "name", "description", "image_url", "pricing", "min_order_qty", "source_url"):
            val = getattr(req, field, None)
            if val is not None:
                updates.append(f"{field}=${idx}")
                params.append(val)
                idx += 1
        if not updates:
            return {"status": "no_changes"}
            
        # Delete old image if it's being replaced with a new one
        if getattr(req, "image_url", None) is not None and existing["image_url"] != getattr(req, "image_url"):
            old_url = existing.get("image_url")
            if old_url and "/v1/assets?key=" in old_url:
                key = old_url.split("/v1/assets?key=")[-1]
                await app.state.s3.delete(key)
                
        updates.append("updated_at=NOW()")
        sql = f"UPDATE kb_products SET {', '.join(updates)} WHERE id=$1 AND tenant_id=$2"
        await conn.execute(sql, *params)

    return {"status": "updated"}


@app.delete("/v1/kb/products/{product_id}", tags=["Dashboard"])
async def kb_delete_product(product_id: str, current: dict = Depends(get_current_user)):
    """Delete a product."""
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, image_url FROM kb_products WHERE id=$1 AND tenant_id=$2", product_id, tid,
        )
        if not row:
            raise HTTPException(404, "Product not found")
        await conn.execute("DELETE FROM kb_products WHERE id=$1", product_id)
        if row.get("image_url") and "/v1/assets?key=" in row["image_url"]:
            key = row["image_url"].split("/v1/assets?key=")[-1]
            await app.state.s3.delete(key)
    return {"status": "deleted"}


@app.post("/v1/kb/products/upload-image", tags=["Dashboard"])
async def kb_upload_product_image(
    file: UploadFile = File(...),
    current: dict = Depends(get_current_user),
):
    """Upload a product image. Returns the full S3 URL."""
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Image too large (max 5 MB)")

    try:
        mime_type = validate_image(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    ext = Path(file.filename or "image.jpg").suffix or ".jpg"
    key = f"tenants/{tid}/products/{secrets.token_hex(8)}{ext}"

    actual_key = await app.state.s3.upload(
        key=key,
        content=content,
        content_type=mime_type,
        cache_control="public, max-age=31536000, immutable"
    )

    return {"image_url": app.state.s3.resolve_url(actual_key)}
@app.post("/v1/kb/products/scrape-url", tags=["Dashboard"])
async def kb_scrape_product_url(
    url: str = Body(..., embed=True),
    current: dict = Depends(get_current_user),
):
    """Scrape a single URL and use Gemini to extract structured product fields."""
    _must_be_owner_or_member(current)
    _must_not_be_suspended(current)
    _must_not_be_trial(current, "the Web Scraper")

    # Step 1: Fetch and extract page text
    try:
        page_data = await app.state.ai.scrape_single_page(url)
    except Exception as e:
        raise HTTPException(502, f"Failed to fetch page: {e}")

    text = page_data.get("text", "")
    if not text.strip():
        raise HTTPException(400, "No extractable text found on the page")

    # Step 2: Use Gemini to extract structured data
    try:
        enriched = await app.state.ai.enrich_product_from_text(text=text, url=url)
    except Exception as e:
        raise HTTPException(502, f"Enrichment failed: {e}")

    return {
        "status": "enriched",
        "page_title": page_data.get("title", ""),
        "product": enriched,
    }


@app.post("/v1/kb/sync", tags=["Dashboard"])
async def kb_sync_vectors(current: dict = Depends(get_current_user)):
    """Trigger a vector DB rebuild for the tenant's structured KB data."""
    _must_be_owner_or_member(current)
    tid = current["tenant_id"]
    pool = await get_pool()

    import hashlib
    chunks = []

    async with pool.acquire() as conn:
        # 1. Company data → one chunk per section
        company_rows = await conn.fetch(
            "SELECT section, field_key, field_value FROM kb_company_data "
            "WHERE tenant_id=$1 ORDER BY section, display_order", tid,
        )
        sections: Dict[str, list] = {}
        for row in company_rows:
            sec = row["section"]
            if sec not in sections:
                sections[sec] = []
            if row["field_value"].strip():
                sections[sec].append(f"{row['field_key'].replace('_', ' ').title()}: {row['field_value']}")

        for sec_name, lines in sections.items():
            if lines:
                text = f"{sec_name.replace('_', ' ').title()}\n" + "\n".join(lines)
                chunk_id = f"kb_company_{hashlib.md5(f'{tid}:{sec_name}'.encode()).hexdigest()[:12]}"
                chunks.append({"id": chunk_id, "text": text, "source": f"Company Data - {sec_name.replace('_', ' ').title()}", "chunk_index": 0, "word_count": len(text.split())})

        # 2. Products → one chunk per product
        products = await conn.fetch(
            "SELECT * FROM kb_products WHERE tenant_id=$1 ORDER BY category, name", tid,
        )
        for prod in products:
            parts = [f"Product: {prod['name']}"]
            if prod["category"]: parts.append(f"Category: {prod['category']}")
            if prod["sub_category"]: parts.append(f"Sub-category: {prod['sub_category']}")
            if prod["description"]: parts.append(f"Description: {prod['description']}")
            if prod["pricing"]: parts.append(f"Pricing: {prod['pricing']}")
            if prod["min_order_qty"]: parts.append(f"Minimum Order Quantity: {prod['min_order_qty']}")
            text = "\n".join(parts)
            chunks.append({"id": f"kb_product_{prod['id']}", "text": text, "source": f"Product - {prod['name']}", "chunk_index": 0, "word_count": len(text.split())})

        # 3. Custom Q/A → one chunk per Q/A pair
        qas = await conn.fetch(
            "SELECT id, question, answer FROM knowledge_qa WHERE tenant_id=$1", tid,
        )
        for qa in qas:
            text = f"Question: {qa['question']}\nAnswer: {qa['answer']}"
            chunks.append({"id": f"qa_{qa['id']}", "text": text, "source": "Custom Q/A", "chunk_index": 0, "word_count": len(text.split())})

    # Rebuild via AI_Backend
    try:
        result = await app.state.ai.rag_rebuild_structured(tenant_id=tid, chunks=chunks)
        return {"status": "synced", **result}
    except Exception as e:
        raise HTTPException(502, f"Vector sync failed: {e}")


# ── Domain verification ───────────────────────────────────────────────────────

@app.post("/v1/verify-domain", tags=["Dashboard"])
async def verify_domain(current: dict = Depends(get_current_user)):
    """Check the tenant's website for the meta verification tag."""
    _must_be_owner(current)
    tid = current["tenant_id"]

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT domain, verification_token, domain_verified FROM tenants WHERE id=$1", tid
        )

    if not row or not row["domain"]:
        raise HTTPException(400, "No website URL configured")
    if row["domain_verified"]:
        return {"status": "already_verified"}

    token = row["verification_token"]
    if not token:
        token = secrets.token_hex(16)
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE tenants SET verification_token=$1 WHERE id=$2", token, tid
            )

    # Fetch website and search for <meta name="leadsai-verify" content="TOKEN">
    try:
        page = await app.state.ai.scrape_single_page(row["domain"])
        html_content = page.get("html", "") or page.get("content", "") or page.get("text", "")
        match = re.search(
            rf'<meta\s+name=["\']leadsai-verify["\']\s+content=["\']({re.escape(token)})["\']',
            html_content, re.IGNORECASE
        )
        if match:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE tenants SET domain_verified=TRUE, updated_at=NOW() WHERE id=$1", tid
                )
            return {"status": "verified"}
        else:
            return {"status": "not_found", "token": token}
    except Exception as e:
        return {"status": "error", "message": str(e), "token": token}



# ── Widget config ──────────────────────────────────────────────────────────────
class _WidgetUpdate(BaseModel):
    name:               Optional[str]  = None
    greeting:           Optional[str]  = None
    primary_color:      Optional[str]  = None
    accent_color:       Optional[str]  = None
    secondary_color:    Optional[str]  = None
    text_color:         Optional[str]  = None
    bot_text_color:     Optional[str]  = None
    user_text_color:    Optional[str]  = None
    logo_url:           Optional[str]  = None
    bg_image_url:       Optional[str]  = None
    position:           Optional[str]  = None
    proactive_enabled:  Optional[bool] = None
    proactive_delay_s:  Optional[int]  = None
    proactive_message:  Optional[str]  = None
    pii_after_messages: Optional[int]  = None
    tts_enabled:        Optional[bool] = None
    stt_enabled:        Optional[bool] = None
    cv_search_enabled:  Optional[bool] = None
    notification_email: Optional[str]  = None
    languages:          Optional[str]  = None


@app.get("/v1/widget-config", tags=["Dashboard"])
async def get_widget_config_dashboard(current: dict = Depends(get_current_user)):
    _must_be_owner(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT wc.* FROM widget_configs wc WHERE wc.tenant_id=$1",
            current["tenant_id"],
        )
    result = dict(row) if row else {}
    if "logo_url" in result:
        result["logo_url"] = app.state.s3.resolve_url(result["logo_url"])
    if "bg_image_url" in result:
        result["bg_image_url"] = app.state.s3.resolve_url(result["bg_image_url"])
    return result


@app.put("/v1/widget-config", tags=["Dashboard"])
async def update_widget_config(body: _WidgetUpdate, current: dict = Depends(get_current_user)):
    _must_be_owner(current)
    _must_not_be_suspended(current)
    _must_not_be_trial(current, "Widget Customization")
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided")
        
    pool   = await get_pool()
    async with pool.acquire() as conn:
        # Check old URLs to delete if replaced or cleared
        if "logo_url" in updates or "bg_image_url" in updates:
            existing = await conn.fetchrow(
                "SELECT logo_url, bg_image_url FROM widget_configs WHERE tenant_id=$1", 
                current["tenant_id"]
            )
            if existing:
                for field in ("logo_url", "bg_image_url"):
                    if field in updates and updates[field] != existing[field]:
                        old_url = existing[field]
                        if old_url and "/v1/assets?key=" in old_url:
                            key = old_url.split("/v1/assets?key=")[-1]
                            await app.state.s3.delete(key)
    
        # Build SET clause: col1=$1, col2=$2 … tenant_id=$N+1
        keys   = list(updates.keys())
        vals   = list(updates.values())
        set_   = ", ".join(f"{k}=${i+1}" for i, k in enumerate(keys))
        
        await conn.execute(
            f"UPDATE widget_configs SET {set_}, updated_at=NOW()"
            f" WHERE tenant_id=${len(keys)+1}",
            *vals, current["tenant_id"],
        )
        await _audit(conn, current, "update_widget_config", "widget_config", current["tenant_id"],
                     {"fields": keys}, tenant_id=current["tenant_id"])
    return {"status": "updated", "fields": keys}

# ── Uploads & Assets ───────────────────────────────────────────────────────────

@app.get("/v1/assets", tags=["Assets"])
async def get_asset(strkey: str = Query(..., alias="key")):
    """Redirect to the presigned S3 URL or serve locally generated URL."""
    # The S3StorageService.get_url will generate a presigned URL using boto3
    url = app.state.s3.get_url(strkey)
    # We issue a 302 redirect so the browser fetches from S3
    return RedirectResponse(url, status_code=302)

@app.post("/v1/upload", tags=["Dashboard"])
async def upload_file(
    file: UploadFile = File(...),
    current: dict = Depends(get_current_user)
):
    """Upload a generic file (e.g. background image) and return its public URL."""
    _must_be_owner(current)

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large")

    try:
        mime_type = validate_image(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    raw_name = file.filename or "upload"
    ext = Path(raw_name).suffix or ".png"
    key = f"tenants/{current['tenant_id']}/widget/bg_{secrets.token_hex(8)}{ext}"

    actual_key = await app.state.s3.upload(
        key=key,
        content=content,
        content_type=mime_type,
        cache_control="public, max-age=31536000, immutable"
    )

    return {"url": app.state.s3.resolve_url(actual_key)}

# ── Widget embed snippet ───────────────────────────────────────────────────────

@app.get("/v1/widget-embed", tags=["Dashboard"])
async def get_widget_embed(request: Request, current: dict = Depends(get_current_user)):
    """
    Returns a ready-to-paste <script> embed snippet for the tenant's website.
    The snippet uses the data-company attribute to identify the tenant.
    """
    _must_be_owner(current)
    tid  = current["tenant_id"]
    pool = await get_pool()

    async with pool.acquire() as conn:
        cfg = await conn.fetchrow(
            "SELECT wc.*, t.name AS business_name, t.plan, t.widget_slug"
            " FROM widget_configs wc"
            " JOIN tenants t ON t.id = wc.tenant_id"
            " WHERE wc.tenant_id = $1",
            tid,
        )
        if not cfg:
            raise HTTPException(status_code=404, detail="Widget config not found")

    cfg  = dict(cfg)

    # Derive the CDN base URL from env — falls back to actual request origin for dev
    cdn_base = os.getenv("WIDGET_CDN_URL", "").rstrip("/")
    if not cdn_base:
        cdn_base = str(request.base_url).rstrip("/") + "/widget"

    domain_snippet = (
        f"<!-- Leads AI Widget -->\n"
        f"<script\n"
        f'  src="{cdn_base}/leadsai.js"\n'
        f'  data-company="{cfg["widget_slug"]}"\n'
        f"  defer\n"
        f"></script>"
    )
    snippets = [{
        "domain":  "Global Snippet",
        "label":   "Universal Embed snippet",
        "snippet": domain_snippet,
    }]

    install_guide = {
        "step_1": "Copy the embed snippet below.",
        "step_2": "Paste the <script> tag just before the closing </body> tag on your website.",
        "note":   "The widget authenticates automatically using the data-company attribute.",
        "cdn_base":       cdn_base,
        "docs_url":       "https://docs.leadsai.winssoft.com/widget/installation",
    }

    return {
        "business_name":     cfg["business_name"],
        "plan":              cfg["plan"],
        "snippets":          snippets,
        "widget_config": {
            "primary_color":      cfg.get("primary_color"),
            "accent_color":       cfg.get("accent_color"),
            "position":           cfg.get("position"),
            "greeting":           cfg.get("greeting"),
            "languages":          cfg.get("languages"),
            "tts_enabled":        cfg.get("tts_enabled"),
            "stt_enabled":        cfg.get("stt_enabled"),
            "cv_search_enabled":  cfg.get("cv_search_enabled"),
            "proactive_enabled":  cfg.get("proactive_enabled"),
            "proactive_delay_s":  cfg.get("proactive_delay_s"),
            "proactive_message":  cfg.get("proactive_message"),
        },
        "install_guide":      install_guide,
    }



# ── Usage / subscription / payments ───────────────────────────────────────────
@app.get("/v1/usage", tags=["Dashboard"])
async def get_usage(days: int = 30, current: dict = Depends(get_current_user)):
    _must_be_user(current)
    analytics = await app.state.payments.get_analytics(current["tenant_id"], days)
    usage     = await app.state.payments.check_usage(current["tenant_id"])
    return {**usage, "analytics": analytics}


@app.get("/v1/subscription", tags=["Dashboard"])
async def get_subscription(current: dict = Depends(get_current_user)):
    _must_be_user(current)
    return await app.state.payments.check_usage(current["tenant_id"])


@app.get("/v1/payment/history", tags=["Dashboard"])
async def payment_history(current: dict = Depends(get_current_user)):
    _must_be_user(current)
    return {"payments": await app.state.payments.get_payment_history(current["tenant_id"])}


class _OrderReq(BaseModel):
    name:    str
    email:   EmailStr
    company: str = ""
    phone:   str = ""
    plan:    str = "pro"


class _VerifyReq(BaseModel):
    razorpay_order_id:   str
    razorpay_payment_id: str
    razorpay_signature:  str
    tenant_id:           str
    plan:                str


@app.post("/v1/payment/create-order", tags=["Payments"])
async def create_order(req: _OrderReq):
    cfg = await get_plan(req.plan)
    if not cfg:
        raise HTTPException(status_code=400, detail=f"Unknown plan: {req.plan}")
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM tenants WHERE email=$1", req.email)
        if row:
            tid = row["id"]
        else:
            tid = secrets.token_hex(8)
            async with conn.transaction():
                await conn.execute(
                    "INSERT INTO tenants (id,name,email,company,phone,plan,status,ticket_limit)"
                    " VALUES ($1,$2,$3,$4,$5,$6,'pending',$7)",
                    tid, req.name, req.email, req.company or req.name,
                    req.phone or "", req.plan, cfg.get("ticket_limit", 2),
                )
                await conn.execute(
                    "INSERT INTO widget_configs (tenant_id,name,notification_email)"
                    " VALUES ($1,$2,$3)",
                    tid, req.company or req.name, req.email,
                )
    order = await app.state.payments.create_order(tid, req.plan)
    order["tenant_id"] = tid
    return order


@app.post("/v1/payment/verify", tags=["Payments"])
async def verify_payment(req: _VerifyReq):
    return await app.state.payments.verify_payment(
        razorpay_order_id=req.razorpay_order_id,
        razorpay_payment_id=req.razorpay_payment_id,
        razorpay_signature=req.razorpay_signature,
        tenant_id=req.tenant_id,
        plan=req.plan,
    )
    # Note: audit for payment_verified is logged inside payments.verify_payment


# ── Settings ──────────────────────────────────────────────────────────────────
class _SettingsUpdate(BaseModel):
    name:                Optional[str] = None
    notification_email:  Optional[str] = None
    company:             Optional[str] = None
    phone:               Optional[str] = None
    country_code:        Optional[str] = None
    domain:              Optional[str] = None
    logo_url:            Optional[str] = None
    notification_emails: Optional[list[str]] = None
    user_phone:          Optional[str] = None

# In-memory email-change OTP store: { "user_id": {"otp": "123456", "email": "new@x.com", "expires": float} }
_email_otps: dict[str, dict] = {}


@app.get("/v1/settings", tags=["Dashboard"])
async def get_settings(current: dict = Depends(get_current_user)):
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow(
            "SELECT name, email, company, phone, country_code, domain, logo_url, plan, status,"
            " notification_emails, created_at FROM tenants WHERE id=$1",
            current["tenant_id"],
        )
        ua = await conn.fetchrow(
            "SELECT name, email, phone, role, created_at FROM user_auth WHERE id=$1",
            current["id"],
        )
        wc = await conn.fetchrow(
            "SELECT notification_email FROM widget_configs WHERE tenant_id=$1",
            current["tenant_id"],
        )
        sub = await conn.fetchrow(
            "SELECT current_period_end, status FROM subscriptions"
            " WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 1",
            current["tenant_id"],
        )
    t = dict(tenant) if tenant else {}
    # Parse notification_emails from comma-separated string
    raw_ne = t.get("notification_emails", "")
    ne_list = [e.strip() for e in str(raw_ne).split(",") if e.strip()] if raw_ne else []
    from typing import Any
    t_dict: dict[str, Any] = dict(t)
    t_dict["notification_emails"] = ne_list
    if t.get("created_at"):
        t_dict["created_at"] = str(t.get("created_at"))
    if t_dict.get("logo_url"):
        t_dict["logo_url"] = app.state.s3.resolve_url(t_dict["logo_url"])
    
    u = dict(ua) if ua else {}
    u_dict: dict[str, Any] = dict(u)
    if u.get("created_at"):
        u_dict["created_at"] = str(u.get("created_at"))
    
    return {
        "tenant": t_dict,
        "user":   u_dict,
        "widget": {"notification_email": wc["notification_email"] if wc else ""},
        "subscription": {
            "period_end": sub["current_period_end"].isoformat() if sub and sub["current_period_end"] else None,
            "status":     sub["status"] if sub else None,
        } if sub else None,
    }


@app.put("/v1/settings", tags=["Dashboard"])
async def update_settings(body: _SettingsUpdate, current: dict = Depends(get_current_user)):
    _must_be_user(current)
    _must_not_be_suspended(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Update user name (any role can update their own name)
            if body.name:
                await conn.execute(
                    "UPDATE user_auth SET name=$1, updated_at=NOW() WHERE id=$2",
                    body.name, current["id"],
                )
            # Update user phone (any role)
            if body.user_phone is not None:
                await conn.execute(
                    "UPDATE user_auth SET phone=$1, updated_at=NOW() WHERE id=$2",
                    body.user_phone, current["id"],
                )
            # Owner-only fields
            if current.get("role") == "owner":
                # Update tenant fields
                if body.domain is not None:
                    body.domain = _validate_domain(body.domain)  # SSRF guard
                tenant_updates = {k: v for k, v in {
                    "company": body.company,
                    "phone":   body.phone,
                    "country_code": body.country_code,
                    "domain":  body.domain,
                    "logo_url": body.logo_url,
                }.items() if v is not None}
                if tenant_updates:
                    keys = list(tenant_updates.keys())
                    vals = list(tenant_updates.values())
                    set_ = ", ".join(f"{k}=${i+1}" for i, k in enumerate(keys))
                    await conn.execute(
                        f"UPDATE tenants SET {set_}, updated_at=NOW() WHERE id=${len(keys)+1}",
                        *vals, current["tenant_id"],
                    )
                # Update notification email on widget_configs
                if body.notification_email:
                    await conn.execute(
                        "UPDATE widget_configs SET notification_email=$1, updated_at=NOW()"
                        " WHERE tenant_id=$2",
                        body.notification_email, current["tenant_id"],
                    )
                # Update notification_emails on tenants (max 3)
                if body.notification_emails is not None:
                    # Trial users can only keep their owner email
                    if current.get("plan") == "trial":
                        raise HTTPException(
                            status_code=403,
                            detail="Upgrade your plan to add additional notification emails.",
                        )
                    emails = body.notification_emails[:3]  # enforce max 3
                    csv = ",".join(e.strip() for e in emails if e.strip())
                    await conn.execute(
                        "UPDATE tenants SET notification_emails=$1, updated_at=NOW() WHERE id=$2",
                        csv, current["tenant_id"],
                    )
            await _audit(conn, current, "update_settings", "tenant", current["tenant_id"],
                         body.model_dump(exclude_none=True), tenant_id=current["tenant_id"])
    return {"status": "updated"}


@app.post("/v1/settings/logo", tags=["Dashboard"])
async def upload_tenant_logo(
    file: UploadFile = File(...),
    current: dict = Depends(get_current_user),
):
    """Upload tenant logo image. Saves to S3 and updates tenant + widget_configs."""
    _must_be_owner(current)

    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=400, detail="File too large (max 5 MB)")

    try:
        mime_type = validate_image(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    ext = Path(file.filename or "logo.png").suffix or ".png"
    key = f"tenants/{current['tenant_id']}/logo{ext}"

    # Delete existing logo with different prefix to keep it clean (cascade)
    await app.state.s3.delete_prefix(f"tenants/{current['tenant_id']}/logo.")

    actual_key = await app.state.s3.upload(
        key=key,
        content=content,
        content_type=mime_type,
        cache_control="public, max-age=300"
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE tenants SET logo_url=$1, updated_at=NOW() WHERE id=$2",
                actual_key, current["tenant_id"],
            )
            await conn.execute(
                "UPDATE widget_configs SET logo_url=$1, updated_at=NOW() WHERE tenant_id=$2",
                actual_key, current["tenant_id"],
            )
            
    return {"logo_url": app.state.s3.resolve_url(actual_key)}


class _EmailOTPReq(BaseModel):
    new_email: str


class _EmailOTPVerify(BaseModel):
    otp: str


@app.post("/v1/settings/request-email-otp", tags=["Dashboard"])
async def request_email_otp(body: _EmailOTPReq, current: dict = Depends(get_current_user)):
    """Send a 6-digit OTP to verify a new email address before changing it."""
    _must_be_user(current)
    import random, time
    otp = f"{random.randint(100000, 999999)}"
    _email_otps[current["id"]] = {
        "otp": otp,
        "email": body.new_email,
        "expires": time.time() + 600,  # 10 min
    }
    logger.info(f"Email OTP for user {current['id']}: {otp} (dev mode)")
    return {"message": "OTP sent to new email"}


@app.post("/v1/settings/verify-email-otp", tags=["Dashboard"])
async def verify_email_otp(body: _EmailOTPVerify, current: dict = Depends(get_current_user)):
    """Verify OTP and update user email."""
    _must_be_user(current)
    import time
    stored = _email_otps.get(current["id"])
    if not stored:
        raise HTTPException(status_code=400, detail="No pending OTP")
    if time.time() > stored["expires"]:
        _email_otps.pop(current["id"], None)
        raise HTTPException(status_code=400, detail="OTP expired")
    if stored["otp"] != body.otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    new_email = stored["email"]
    _email_otps.pop(current["id"], None)

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check uniqueness
        existing = await conn.fetchrow(
            "SELECT id FROM user_auth WHERE email=$1 AND id!=$2", new_email, current["id"]
        )
        if existing:
            raise HTTPException(status_code=409, detail="Email already in use")
        await conn.execute(
            "UPDATE user_auth SET email=$1, updated_at=NOW() WHERE id=$2",
            new_email, current["id"],
        )
    return {"status": "email_updated", "new_email": new_email}


# ═════════════════════════════════════════════════════════════════════════════
# MEMBERS — TEAM MANAGEMENT
# ═════════════════════════════════════════════════════════════════════════════
MAX_OWNERS = 3  # max 3 owners per tenant

# In-memory OTP store: { "tenant:member_id": {"otp": "123456", "expires": float} }
_owner_otps: dict[str, dict] = {}


class _MemberCreate(BaseModel):
    name:     str
    email:    EmailStr
    password: str
    phone:    str = ""
    country_code: str = "+91"
    role:     str = "member"  # member


class _MemberUpdate(BaseModel):
    role:   Optional[str] = None   # owner | member
    status: Optional[str] = None   # active | inactive


@app.get("/v1/members", tags=["Members"])
async def list_members(current: dict = Depends(get_current_user)):
    _must_be_user(current)
    if current.get("role") not in ("owner", "member"):
        raise HTTPException(status_code=403, detail="Owner or member access required")
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, email, role, status, last_active, created_at"
            " FROM user_auth WHERE tenant_id=$1 ORDER BY created_at",
            current["tenant_id"],
        )
    members = [dict(r) for r in rows]
    # Members see limited info (no status management)
    if current.get("role") == "member":
        for m in members:
            m.pop("status", None)
            m.pop("last_active", None)
    return {"members": members, "max_owners": MAX_OWNERS}


@app.post("/v1/members", status_code=201, tags=["Members"])
async def add_member(body: _MemberCreate, current: dict = Depends(get_current_user)):
    _must_be_owner(current)
    _must_not_be_suspended(current)
    _must_not_be_trial(current, "Team Management")
    if body.role != "member":
        raise HTTPException(status_code=400, detail="Can only add members directly")
    pool = await get_pool()
    async with pool.acquire() as conn:
        exists = await conn.fetchval(
            "SELECT 1 FROM user_auth WHERE email=$1", body.email
        )
        if exists:
            raise HTTPException(status_code=409, detail="Email already registered")
        mid = secrets.token_hex(8)
        await conn.execute(
            "INSERT INTO user_auth (id, tenant_id, name, email, phone, country_code, password_hash, role)"
            " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            mid, current["tenant_id"], body.name, body.email,
            body.phone, body.country_code,
            hash_password(body.password), body.role,
        )
    return {"id": mid, "name": body.name, "email": body.email, "role": body.role,
            "phone": body.phone, "country_code": body.country_code}


@app.put("/v1/members/{member_id}", tags=["Members"])
async def update_member(member_id: str, body: _MemberUpdate,
                        current: dict = Depends(get_current_user)):
    _must_be_owner(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, role FROM user_auth WHERE id=$1 AND tenant_id=$2",
            member_id, current["tenant_id"],
        )
        if not row:
            raise HTTPException(status_code=404, detail="Member not found")

        if body.role:
            if body.role not in ("owner", "member"):
                raise HTTPException(status_code=400, detail="Invalid role")
            # Owner promotion requires OTP flow
            if body.role == "owner":
                raise HTTPException(
                    status_code=400,
                    detail="Use /v1/members/{id}/request-owner-otp to promote to owner",
                )
            await conn.execute(
                "UPDATE user_auth SET role=$1, updated_at=NOW() WHERE id=$2",
                body.role, member_id,
            )

        if body.status:
            if body.status not in ("active", "inactive"):
                raise HTTPException(status_code=400, detail="Invalid status")
            await conn.execute(
                "UPDATE user_auth SET status=$1, updated_at=NOW() WHERE id=$2",
                body.status, member_id,
            )

    return {"status": "updated", "member_id": member_id}

class ContactSalesRequest(BaseModel):
    name: str = Field(..., example="John Doe")
    email: str = Field(..., example="john@example.com")
    company: str = Field(..., example="Acme Corp")
    phone: str = Field(..., example="+1234567890")
    expected_volume: str = Field(..., example="50,000 sessions/mo")
    message: str = Field(..., example="We need custom integrations.")

@app.post("/v1/contact-sales", tags=["User Dashboard"])
async def contact_sales(body: ContactSalesRequest, current: dict = Depends(get_current_user)):
    """Send an Enterprise sales inquiry to admin."""
    _must_be_user(current)
    try:
        leads_service = app.state.leads
        await leads_service.send_sales_inquiry({
            "name": body.name,
            "email": body.email,
            "company": body.company,
            "phone": body.phone,
            "expected_volume": body.expected_volume,
            "message": body.message
        })
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Contact sales error: {e}")
        raise HTTPException(status_code=500, detail="Failed to send inquiry.")


@app.post("/v1/members/{member_id}/request-owner-otp", tags=["Members"])
async def request_owner_otp(member_id: str, current: dict = Depends(get_current_user)):
    """Send OTP to current owner's email to confirm owner promotion."""
    _must_be_owner(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Verify member exists in same tenant
        target = await conn.fetchrow(
            "SELECT id, name, role FROM user_auth WHERE id=$1 AND tenant_id=$2",
            member_id, current["tenant_id"],
        )
        if not target:
            raise HTTPException(status_code=404, detail="Member not found")
        if target["role"] == "owner":
            raise HTTPException(status_code=400, detail="Already an owner")
        # Check max owners
        owner_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_auth WHERE tenant_id=$1 AND role='owner'",
            current["tenant_id"],
        )
        if owner_count >= MAX_OWNERS:
            raise HTTPException(
                status_code=403,
                detail=f"Maximum {MAX_OWNERS} owners allowed per tenant",
            )

    # Generate 6-digit OTP
    import random
    otp = f"{random.randint(100000, 999999)}"
    _owner_otps[f"{current['tenant_id']}:{member_id}"] = {
        "otp": otp,
        "expires": time.time() + 600,  # 10 min
    }
    # Send OTP email to current owner
    from lead_manager import _smtp_send
    await _smtp_send(
        to=current["email"],
        subject=f"Wins Soft - Leads AI — Owner Promotion OTP",
        html=(
            f"<p>Your OTP to promote <b>{target['name']}</b> to Owner is:</p>"
            f"<h1 style='letter-spacing:8px;text-align:center'>{otp}</h1>"
            f"<p>This OTP expires in 10 minutes.</p>"
        ),
    )
    return {"status": "otp_sent", "message": "OTP sent to your email"}


@app.post("/v1/members/{member_id}/confirm-owner", tags=["Members"])
async def confirm_owner_promotion(
    member_id: str,
    otp: str = Body(..., embed=True),
    current: dict = Depends(get_current_user),
):
    """Verify OTP and promote member to owner."""
    _must_be_owner(current)
    key = f"{current['tenant_id']}:{member_id}"
    entry = _owner_otps.get(key)
    if not entry:
        raise HTTPException(status_code=400, detail="No OTP requested for this member")
    if time.time() > entry["expires"]:
        _owner_otps.pop(key, None)
        raise HTTPException(status_code=400, detail="OTP expired — request a new one")
    if entry["otp"] != otp:
        raise HTTPException(status_code=400, detail="Invalid OTP")

    _owner_otps.pop(key, None)
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Double-check max owners
        owner_count = await conn.fetchval(
            "SELECT COUNT(*) FROM user_auth WHERE tenant_id=$1 AND role='owner'",
            current["tenant_id"],
        )
        if owner_count >= MAX_OWNERS:
            raise HTTPException(
                status_code=403,
                detail=f"Maximum {MAX_OWNERS} owners reached",
            )
        await conn.execute(
            "UPDATE user_auth SET role='owner', updated_at=NOW() WHERE id=$1 AND tenant_id=$2",
            member_id, current["tenant_id"],
        )
    return {"status": "promoted", "member_id": member_id, "role": "owner"}


@app.delete("/v1/members/{member_id}", tags=["Members"])
async def remove_member(member_id: str, current: dict = Depends(get_current_user)):
    _must_be_owner(current)
    if member_id == current["id"]:
        raise HTTPException(status_code=400, detail="Cannot remove yourself")
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            "DELETE FROM user_auth WHERE id=$1 AND tenant_id=$2",
            member_id, current["tenant_id"],
        )
    if res == "DELETE 0":
        raise HTTPException(status_code=404, detail="Member not found")
    return {"status": "removed", "member_id": member_id}


# ═════════════════════════════════════════════════════════════════════════════
# TICKETS — USER SIDE
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/v1/tickets", tags=["Tickets"])
async def user_list_tickets(
    status: str = "",
    page:   int = 1,
    limit:  int = 50,
    current: dict = Depends(get_current_user),
):
    _must_be_owner_or_member(current)
    offset = (page - 1) * limit
    pool   = await get_pool()
    async with pool.acquire() as conn:
        conds:  list[str] = ["t.tenant_id=$1"]
        params: list      = [current["tenant_id"]]
        # Members see only their own tickets; owners see all
        if current.get("role") == "member":
            params.append(current["id"])
            conds.append(f"t.user_id=${len(params)}")
        if status:
            params.append(status)
            conds.append(f"t.status=${len(params)}")
        where = " AND ".join(conds)
        rows = await conn.fetch(
            f"SELECT t.*,"
            f"       a.name AS assigned_to_name,"
            f"       c.name AS claimed_by_name,"
            f"       (SELECT COUNT(*) FROM ticket_attachments WHERE ticket_id=t.id) AS attach_count"
            f" FROM tickets t"
            f" LEFT JOIN admin_users a ON a.id=t.assigned_to"
            f" LEFT JOIN admin_users c ON c.id=t.claimed_by"
            f" WHERE {where}"
            f" ORDER BY t.created_at DESC"
            f" LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
            *params, limit, offset,
        )
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM tickets t WHERE {where}", *params
        )
    return {"tickets": [dict(r) for r in rows], "total": total, "page": page}


@app.post("/v1/tickets", status_code=201, tags=["Tickets"])
async def user_create_ticket(
    heading:  str             = Form(...),
    context:  str             = Form(...),
    type:     str             = Form(default="issue"),
    priority: str             = Form(default="medium"),
    files:    List[UploadFile] = File(default=[]),
    current:  dict            = Depends(get_current_user),
):
    _must_be_owner_or_member(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        ticket_limit = await conn.fetchval(
            "SELECT ticket_limit FROM tenants WHERE id=$1", current["tenant_id"]
        )
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM tickets WHERE tenant_id=$1 AND status IN ('open','claimed')",
            current["tenant_id"],
        )
        if open_count >= (ticket_limit or 2):
            plan = await conn.fetchval("SELECT plan FROM tenants WHERE id=$1", current["tenant_id"])
            raise HTTPException(
                status_code=403,
                detail=f"Open ticket limit reached for your {plan} plan. "
                       "Close or resolve a ticket first.",
            )
        async with conn.transaction():
            tid = secrets.token_hex(10)
            await conn.execute(
                "INSERT INTO tickets (id,tenant_id,user_id,heading,context,type,priority)"
                " VALUES ($1,$2,$3,$4,$5,$6,$7)",
                tid, current["tenant_id"], current["id"],
                heading, context, type, priority,
            )
            for f in (files or []):
                if not f.filename:
                    continue
                data  = await f.read()
                mime = f.content_type or "application/octet-stream"
                key = f"tickets/{tid}/{secrets.token_hex(4)}_{f.filename}"
                
                actual_key = await app.state.s3.upload(
                    key=key,
                    content=data,
                    content_type=mime,
                    cache_control="private, max-age=31536000"
                )

                await conn.execute(
                    "INSERT INTO ticket_attachments"
                    " (id,ticket_id,filename,file_path,file_size,mime_type)"
                    " VALUES ($1,$2,$3,$4,$5,$6)",
                    secrets.token_hex(8), tid, f.filename,
                    actual_key, len(data), mime,
                )
            await conn.execute(
                "INSERT INTO ticket_status_log"
                " (ticket_id,changed_by,changer_role,from_status,to_status,note)"
                " VALUES ($1,$2,'user','none','open','Ticket created')",
                tid, current["id"],
            )
            await _audit(conn, current, "create_ticket", "ticket", tid,
                         {"heading": heading, "type": type, "priority": priority},
                         tenant_id=current["tenant_id"])
    return {"ticket_id": tid, "status": "open"}


@app.get("/v1/tickets/{ticket_id}", tags=["Tickets"])
async def user_get_ticket(ticket_id: str, current: dict = Depends(get_current_user)):
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT t.*, a.name AS assigned_to_name, c.name AS claimed_by_name"
            " FROM tickets t"
            " LEFT JOIN admin_users a ON a.id=t.assigned_to"
            " LEFT JOIN admin_users c ON c.id=t.claimed_by"
            " WHERE t.id=$1 AND t.tenant_id=$2",
            ticket_id, current["tenant_id"],
        )
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found")
        attachments = await conn.fetch(
            "SELECT id, filename, file_path, file_size, mime_type"
            " FROM ticket_attachments WHERE ticket_id=$1", ticket_id,
        )
        log = await conn.fetch(
            "SELECT * FROM ticket_status_log WHERE ticket_id=$1 ORDER BY created_at",
            ticket_id,
        )
    t = dict(row)
    atts = []
    api_url = os.getenv("VITE_API_URL", "").rstrip("/")
    for a in attachments:
        ad = dict(a)
        ad["url"] = f"{api_url}/v1/tickets/{ticket_id}/attachments/{a['id']}/download"
        atts.append(ad)
    t["attachments"] = atts
    t["status_log"]  = [dict(l) for l in log]
    return t

@app.get("/v1/tickets/{ticket_id}/attachments/{attachment_id}/download", tags=["Tickets"])
async def download_ticket_attachment(ticket_id: str, attachment_id: str, current: dict = Depends(get_current_user)):
    """Securely download a ticket attachment."""
    # We allow user, admin, or superadmin, but if user, they must own the tenant.
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT t.tenant_id, a.file_path, a.filename FROM ticket_attachments a "
            "JOIN tickets t ON t.id = a.ticket_id "
            "WHERE a.id=$1 AND a.ticket_id=$2",
            attachment_id, ticket_id
        )
        if not row:
            raise HTTPException(404, "Attachment not found")
        if current.get("account_type") == "user" and current.get("tenant_id") != row["tenant_id"]:
            raise HTTPException(403, "Access denied")
        
        # Get secure short-lived presigned URL (1 hour)
        url = app.state.s3.get_url(row["file_path"], expires_in=3600)
        return RedirectResponse(url, status_code=302)


@app.get("/v1/tickets/{ticket_id}/messages", tags=["Tickets"])
async def user_get_messages(ticket_id: str, current: dict = Depends(get_current_user)):
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if not await conn.fetchrow(
            "SELECT id FROM tickets WHERE id=$1 AND tenant_id=$2",
            ticket_id, current["tenant_id"],
        ):
            raise HTTPException(status_code=404, detail="Ticket not found")
        msgs = await conn.fetch(
            "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at",
            ticket_id,
        )
        await conn.execute("UPDATE tickets SET unread_user=0 WHERE id=$1", ticket_id)
        await conn.execute(
            "UPDATE ticket_messages SET read_at=NOW()"
            " WHERE ticket_id=$1 AND sender_role='admin' AND read_at IS NULL",
            ticket_id,
        )
    return {"messages": [dict(m) for m in msgs]}


@app.websocket("/ws/tickets/{ticket_id}")
async def ticket_ws(websocket: WebSocket, ticket_id: str, token: str = Query(...)):
    """Ticket WebSocket — accepts a short-lived ws_ticket token (from /v1/ws/ticket-token)."""

    # 1. Authenticate using short-lived WS token
    try:
        payload = pyjwt.decode(token, _WS_SECRET, algorithms=["HS256"])
        if payload.get("type") != "ws_ticket":
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        user_id = payload.get("sub")
        role = payload.get("role")
        account_type = payload.get("account_type")
        tenant_id = payload.get("tenant_id")
        
        # Admins don't have tenant_id
        if not user_id:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        if account_type != "admin" and not tenant_id:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
    except pyjwt.PyJWTError:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # 2. Verify ticket access
    pool = await get_pool()
    async with pool.acquire() as conn:
        if account_type == "admin":
            # Admins can access any ticket
            ticket = await conn.fetchrow("SELECT id, status FROM tickets WHERE id=$1", ticket_id)
        else:
            # Users must be in the same tenant
            ticket = await conn.fetchrow(
                "SELECT id, status FROM tickets WHERE id=$1 AND tenant_id=$2",
                ticket_id, tenant_id
            )
            
        if not ticket:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return

        # 3. Connect and send history
        await ticket_manager.connect(websocket, ticket_id)
        try:
            # Send initial message load
            msgs = await conn.fetch(
                "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at",
                ticket_id
            )
            await websocket.send_json({
                "type": "messages",
                "payload": [dict(m) for m in msgs]
            })

            # Mark as read based on whoever is connecting
            if account_type == "user":
                await conn.execute("UPDATE tickets SET unread_user=0 WHERE id=$1", ticket_id)
                await conn.execute(
                    "UPDATE ticket_messages SET read_at=NOW() WHERE ticket_id=$1 AND sender_role='admin' AND read_at IS NULL",
                    ticket_id
                )
            else:
                await conn.execute("UPDATE tickets SET unread_admin=0 WHERE id=$1", ticket_id)
                await conn.execute(
                    "UPDATE ticket_messages SET read_at=NOW() WHERE ticket_id=$1 AND sender_role='user' AND read_at IS NULL",
                    ticket_id
                )

            # 4. Listen for incoming messages (with rate limiting)
            _MAX_MSG_LEN = 5000
            _RATE_WINDOW = 10   # seconds
            _RATE_LIMIT  = 10   # messages per window
            msg_timestamps: list[float] = []

            while True:
                data = await websocket.receive_text()
                try:
                    msg_data = json.loads(data)
                    if msg_data.get("type") == "message" and msg_data.get("message"):
                        text = msg_data["message"].strip()[:_MAX_MSG_LEN]
                        if not text:
                            continue

                        # Rate limiting
                        now_ts = time.time()
                        msg_timestamps = [t for t in msg_timestamps if now_ts - t < _RATE_WINDOW]
                        if len(msg_timestamps) >= _RATE_LIMIT:
                            await websocket.send_json({"type": "error", "message": "Rate limit exceeded, please slow down."})
                            continue
                        msg_timestamps.append(now_ts)

                        # Check ticket status
                        current_ticket = await conn.fetchrow("SELECT status FROM tickets WHERE id=$1", ticket_id)
                        if not current_ticket or current_ticket['status'] not in ('claimed', 'solved'):
                            await websocket.send_json({"type": "error", "message": "Chat is disabled for this ticket."})
                            continue

                        # Fetch sender name
                        sender_name = "User"
                        if account_type == "admin" or role == "superadmin":
                            admin_user = await conn.fetchrow("SELECT name FROM admin_users WHERE id=$1", user_id)
                            if admin_user:
                                sender_name = admin_user['name']
                            else:
                                sender_name = "Support Agent"
                        else:
                            tenant_user = await conn.fetchrow("SELECT name FROM user_auth WHERE id=$1", user_id)
                            if tenant_user:
                                sender_name = tenant_user['name']

                        # Insert message
                        new_msg_id = os.urandom(8).hex()
                        sender_role = "admin" if (account_type == "admin" or role == "superadmin") else "user"
                        
                        await conn.execute(
                            "INSERT INTO ticket_messages (id, ticket_id, sender_id, sender_role, sender_name, message)"
                            " VALUES ($1, $2, $3, $4, $5, $6)",
                            new_msg_id, ticket_id, user_id, sender_role, sender_name, text
                        )

                        # Update counters
                        if sender_role == "user":
                            await conn.execute("UPDATE tickets SET unread_admin = unread_admin + 1 WHERE id=$1", ticket_id)
                        else:
                            await conn.execute("UPDATE tickets SET unread_user = unread_user + 1 WHERE id=$1", ticket_id)

                        # Fetch the newly inserted message to broadcast
                        new_msg = await conn.fetchrow(
                            "SELECT * FROM ticket_messages WHERE id=$1", new_msg_id
                        )

                        # Broadcast to all connected clients
                        await ticket_manager.broadcast(ticket_id, {
                            "type": "new_message",
                            "payload": dict(new_msg)
                        })

                except json.JSONDecodeError:
                    pass
        except WebSocketDisconnect:
            ticket_manager.disconnect(websocket, ticket_id)
        except Exception as e:
            logger.error(f"WebSocket error in ticket {ticket_id}: {e}")
            ticket_manager.disconnect(websocket, ticket_id)


class _MsgBody(BaseModel):
    message: str


@app.post("/v1/tickets/{ticket_id}/messages", status_code=201, tags=["Tickets"])
async def user_send_message(
    ticket_id: str, body: _MsgBody, current: dict = Depends(get_current_user)
):
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        ticket = await conn.fetchrow(
            "SELECT status FROM tickets WHERE id=$1 AND tenant_id=$2",
            ticket_id, current["tenant_id"],
        )
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        if ticket["status"] in ("solved", "closed"):
            raise HTTPException(status_code=400, detail="Cannot message on a closed/solved ticket")
        if ticket["status"] == "open":
            raise HTTPException(
                status_code=403,
                detail="Chat is enabled once a support agent claims this ticket",
            )
        ua  = await conn.fetchrow("SELECT name FROM user_auth WHERE id=$1", current["id"])
        mid = secrets.token_hex(10)
        await conn.execute(
            "INSERT INTO ticket_messages"
            " (id,ticket_id,sender_id,sender_name,sender_role,message)"
            " VALUES ($1,$2,$3,$4,'user',$5)",
            mid, ticket_id, current["id"], ua["name"], body.message,
        )
        await conn.execute(
            "UPDATE tickets SET unread_admin=unread_admin+1, updated_at=NOW() WHERE id=$1",
            ticket_id,
        )
        await _audit(conn, current, "send_message", "ticket_message", mid,
                     {"ticket_id": ticket_id}, tenant_id=current["tenant_id"])
    return {"message_id": mid, "status": "sent"}


class _CloseBody(BaseModel):
    note: str


@app.post("/v1/tickets/{ticket_id}/close", tags=["Tickets"])
async def user_close_ticket(
    ticket_id: str, body: _CloseBody, current: dict = Depends(get_current_user)
):
    _must_be_user(current)
    if not body.note.strip():
        raise HTTPException(status_code=400, detail="Closure note is required")
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status FROM tickets WHERE id=$1 AND tenant_id=$2",
            ticket_id, current["tenant_id"],
        )
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found")
        if row["status"] in ("solved", "closed"):
            raise HTTPException(status_code=400, detail="Ticket is already closed/solved")
        prev = row["status"]
        await conn.execute(
            "UPDATE tickets SET status='closed', closure_note=$1, updated_at=NOW() WHERE id=$2",
            body.note, ticket_id,
        )
        await conn.execute(
            "INSERT INTO ticket_status_log"
            " (ticket_id,changed_by,changer_role,from_status,to_status,note)"
            " VALUES ($1,$2,'user',$3,'closed',$4)",
            ticket_id, current["id"], prev, body.note,
        )
        await _audit(conn, current, "close_ticket", "ticket", ticket_id,
                     {"from_status": prev, "closure_note": body.note[:100]},
                     tenant_id=current["tenant_id"])
    return {"status": "closed"}


@app.post("/v1/tickets/{ticket_id}/mark-read", tags=["Tickets"])
async def user_mark_read(ticket_id: str, current: dict = Depends(get_current_user)):
    """Mark all admin messages in this ticket as read from the user's side."""
    _must_be_user(current)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if not await conn.fetchrow(
            "SELECT id FROM tickets WHERE id=$1 AND tenant_id=$2",
            ticket_id, current["tenant_id"],
        ):
            raise HTTPException(status_code=404, detail="Ticket not found")
        await conn.execute("UPDATE tickets SET unread_user=0 WHERE id=$1", ticket_id)
        await conn.execute(
            "UPDATE ticket_messages SET read_at=NOW()"
            " WHERE ticket_id=$1 AND sender_role='admin' AND read_at IS NULL",
            ticket_id,
        )
    return {"status": "marked_read"}


# ═════════════════════════════════════════════════════════════════════════════
# ADMIN  (JWT — admin | superadmin)
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/admin/clients", tags=["Admin"])
async def admin_list_clients(
    page:   int = 1,
    search: str = "",
    plan:   str = "",
    status: str = "",
    current: dict = Depends(require_admin),
):
    limit  = 50
    offset = (page - 1) * limit
    pool   = await get_pool()
    async with pool.acquire() as conn:
        conds:  list[str] = ["TRUE"]
        params: list      = []
        if search:
            params.append(f"%{search}%")
            n = len(params)
            conds.append(
                f"(t.name ILIKE ${n} OR t.email ILIKE ${n} OR t.company ILIKE ${n})"
            )
        if plan:
            params.append(plan);   conds.append(f"t.plan=${len(params)}")
        if status:
            params.append(status); conds.append(f"t.status=${len(params)}")
        where = " AND ".join(conds)
        rows = await conn.fetch(
            f"""
            SELECT t.id, t.name, t.email, t.company, t.plan, t.status,
                   t.created_at, t.ticket_limit,
                   (SELECT COUNT(*) FROM sessions s WHERE s.tenant_id=t.id
                      AND s.started_at >= NOW()-INTERVAL '30 days') AS sessions_month,
                   (SELECT COUNT(*) FROM leads l WHERE l.tenant_id=t.id
                      AND l.created_at >= NOW()-INTERVAL '30 days') AS leads_month,
                   (SELECT COUNT(*) FROM tickets tk WHERE tk.tenant_id=t.id
                      AND tk.status IN ('open','claimed')) AS open_tickets,
                   (SELECT MAX(ua.last_active) FROM user_auth ua
                    WHERE ua.tenant_id=t.id) AS last_active
              FROM tenants t WHERE {where}
             ORDER BY t.created_at DESC
             LIMIT ${len(params)+1} OFFSET ${len(params)+2}
            """,
            *params, limit, offset,
        )
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM tenants t WHERE {where}", *params
        )
    return {"clients": [dict(r) for r in rows], "total": total, "page": page}


@app.get("/admin/clients/{client_id}", tags=["Admin"])
async def admin_get_client(client_id: str, current: dict = Depends(require_admin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tenants WHERE id=$1", client_id)
        if not row:
            raise HTTPException(status_code=404, detail="Client not found")
        sub   = await conn.fetchrow(
            "SELECT * FROM subscriptions WHERE tenant_id=$1 AND status='active'"
            " ORDER BY created_at DESC LIMIT 1", client_id,
        )
        usage = await conn.fetchrow(
            "SELECT COUNT(DISTINCT session_id) AS sessions, COUNT(*) AS requests"
            " FROM usage_events WHERE tenant_id=$1 AND created_at>=NOW()-INTERVAL '30 days'",
            client_id,
        )
    return {
        "tenant":       dict(row),
        "subscription": dict(sub) if sub else None,
        "usage_30d":    dict(usage) if usage else {},
    }


class _ClientUpdate(BaseModel):
    name:    Optional[str] = None
    company: Optional[str] = None
    phone:   Optional[str] = None
    domain:  Optional[str] = None


@app.put("/admin/clients/{client_id}", tags=["Admin"])
async def admin_update_client(
    client_id: str, body: _ClientUpdate, current: dict = Depends(require_admin)
):
    if body.domain is not None:
        body.domain = _validate_domain(body.domain)  # SSRF guard
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided")
    keys = list(updates.keys());  vals = list(updates.values())
    set_ = ", ".join(f"{k}=${i+1}" for i, k in enumerate(keys))
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE tenants SET {set_}, updated_at=NOW() WHERE id=${len(keys)+1}",
            *vals, client_id,
        )
        await _audit(conn, current, "update_client", "tenant", client_id, updates, tenant_id=client_id)
    return {"status": "updated"}





@app.get("/admin/clients/{client_id}/usage", tags=["Admin"])
async def admin_client_usage(
    client_id: str,
    days:      int = 30,
    current:   dict = Depends(require_admin),
):
    pool = await get_pool()
    iv   = f"{days} days"
    async with pool.acquire() as conn:
        daily = await conn.fetch(
            "SELECT DATE(created_at) AS day, COUNT(*) AS requests,"
            "       COALESCE(SUM(tokens_in+tokens_out),0) AS tokens,"
            "       ROUND(AVG(latency_ms)) AS avg_latency"
            " FROM usage_events WHERE tenant_id=$1 AND created_at>=NOW()-$2::INTERVAL"
            " GROUP BY DATE(created_at) ORDER BY day",
            client_id, iv,
        )
        totals = await conn.fetchrow(
            "SELECT COUNT(*) AS total_requests,"
            "       COALESCE(SUM(tokens_in+tokens_out),0) AS total_tokens,"
            "       COUNT(DISTINCT session_id) AS total_sessions"
            " FROM usage_events WHERE tenant_id=$1 AND created_at>=NOW()-$2::INTERVAL",
            client_id, iv,
        )
    return {
        "daily":   [dict(r) for r in daily],
        "totals":  dict(totals) if totals else {},
        "days":    days,
    }


@app.get("/admin/clients/{client_id}/tickets", tags=["Admin"])
async def admin_client_tickets(
    client_id: str,
    status:    str = "",
    current:   dict = Depends(require_admin),
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        conds:  list[str] = ["t.tenant_id=$1"]
        params: list      = [client_id]
        if status:
            params.append(status); conds.append(f"t.status=${len(params)}")
        where = " AND ".join(conds)
        rows = await conn.fetch(
            f"SELECT t.*, a.name AS claimed_by_name"
            f" FROM tickets t LEFT JOIN admin_users a ON a.id=t.claimed_by"
            f" WHERE {where} ORDER BY t.created_at DESC",
            *params,
        )
    return {"tickets": [dict(r) for r in rows]}


class _ExtendPlan(BaseModel):
    days: int




@app.post("/admin/clients/{client_id}/extend-plan", tags=["Admin"])
async def admin_extend_plan(
    client_id: str, body: _ExtendPlan, current: dict = Depends(require_admin)
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE subscriptions"
            " SET current_period_end = current_period_end + ($1 || ' days')::INTERVAL,"
            "     updated_at = NOW()"
            " WHERE tenant_id=$2 AND status='active'",
            str(body.days), client_id,
        )
        await _audit(conn, current, "extend_plan", "subscription", client_id, {"days": body.days}, tenant_id=client_id)
    return {"status": "extended", "days_added": body.days}


class _ChangePlan(BaseModel):
    plan: str


@app.post("/admin/clients/{client_id}/change-plan", tags=["Admin"])
async def admin_change_plan(
    client_id: str, body: _ChangePlan, current: dict = Depends(require_admin)
):
    cfg = await get_plan(body.plan)
    if not cfg:
        raise HTTPException(status_code=400, detail="Invalid plan")
    pool = await get_pool()
    async with pool.acquire() as conn:
        old_plan = await conn.fetchval("SELECT plan FROM tenants WHERE id=$1", client_id)
        await conn.execute(
            "UPDATE tenants SET plan=$1, ticket_limit=$2, updated_at=NOW() WHERE id=$3",
            body.plan, cfg.get("ticket_limit", 2), client_id,
        )
        await conn.execute(
            "UPDATE subscriptions"
            " SET plan=$1, amount_rupee=$2, updated_at=NOW()"
            " WHERE tenant_id=$3 AND status='active'",
            body.plan, cfg["onboarding_fee_rupee"] + cfg["base_fee_rupee"], client_id,
        )
        await _audit(conn, current, "change_plan", "tenant", client_id,
                     {"from": old_plan, "to": body.plan}, tenant_id=client_id)
    return {"status": "changed", "plan": body.plan}


@app.post("/admin/clients/{client_id}/suspend", tags=["Admin"])
async def admin_suspend(client_id: str, current: dict = Depends(require_admin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tenants SET status='suspended', updated_at=NOW() WHERE id=$1", client_id
        )
        await _audit(conn, current, "suspend_tenant", "tenant", client_id, {}, tenant_id=client_id)
    return {"status": "suspended"}


@app.post("/admin/clients/{client_id}/activate", tags=["Admin"])
async def admin_activate(client_id: str, current: dict = Depends(require_admin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tenants SET status='active', updated_at=NOW() WHERE id=$1", client_id
        )
        await _audit(conn, current, "activate_tenant", "tenant", client_id, {}, tenant_id=client_id)
    return {"status": "active"}


# ── Admin tickets ──────────────────────────────────────────────────────────────
@app.get("/admin/tickets", tags=["Admin"])
async def admin_list_tickets(
    status:   str = "",
    assigned: str = "",
    current:  dict = Depends(require_admin),
):
    pool   = await get_pool()
    conds:  list[str] = ["TRUE"]
    params: list      = []
    if status:
        params.append(status); conds.append(f"t.status=${len(params)}")
    if assigned == "me":
        params.append(current["id"]); conds.append(f"t.claimed_by=${len(params)}")
    elif assigned == "unassigned":
        conds.append("t.claimed_by IS NULL AND t.status='open'")
    where = " AND ".join(conds)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT t.*, tn.name AS client_name, tn.plan AS client_plan,
                   a.name AS claimed_by_name,
                   (SELECT COUNT(*) FROM ticket_messages m
                     WHERE m.ticket_id=t.id AND m.read_at IS NULL
                       AND m.sender_role='user') AS unread_count
              FROM tickets t
              JOIN tenants tn ON tn.id = t.tenant_id
              LEFT JOIN admin_users a ON a.id = t.claimed_by
             WHERE {where}
             ORDER BY
               CASE t.priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                                WHEN 'medium'  THEN 3 ELSE 4 END,
               t.created_at
            """,
            *params,
        )
    return {"tickets": [dict(r) for r in rows]}


@app.get("/admin/tickets/{ticket_id}", tags=["Admin"])
async def admin_get_ticket(ticket_id: str, current: dict = Depends(require_admin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT t.*, tn.name AS client_name, tn.email AS client_email,"
            "       tn.plan AS client_plan,"
            "       a.name AS claimed_by_name, ua.name AS user_name"
            " FROM tickets t"
            " JOIN tenants tn ON tn.id=t.tenant_id"
            " LEFT JOIN admin_users a  ON a.id=t.claimed_by"
            " LEFT JOIN user_auth   ua ON ua.id=t.user_id"
            " WHERE t.id=$1",
            ticket_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found")
        attachments = await conn.fetch(
            "SELECT id, filename, file_path, mime_type"
            " FROM ticket_attachments WHERE ticket_id=$1", ticket_id,
        )
        log = await conn.fetch(
            "SELECT * FROM ticket_status_log WHERE ticket_id=$1 ORDER BY created_at",
            ticket_id,
        )
    t = dict(row)
    atts = []
    api_url = os.getenv("VITE_API_URL", "").rstrip("/")
    for a in attachments:
        ad = dict(a)
        ad["url"] = f"{api_url}/v1/tickets/{ticket_id}/attachments/{a['id']}/download"
        atts.append(ad)
    t["attachments"] = atts
    t["status_log"]  = [dict(l) for l in log]
    return t


@app.get("/admin/tickets/{ticket_id}/messages", tags=["Admin"])
async def admin_get_messages(ticket_id: str, current: dict = Depends(require_admin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        msgs = await conn.fetch(
            "SELECT * FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at",
            ticket_id,
        )
        await conn.execute("UPDATE tickets SET unread_admin=0 WHERE id=$1", ticket_id)
        await conn.execute(
            "UPDATE ticket_messages SET read_at=NOW()"
            " WHERE ticket_id=$1 AND sender_role='user' AND read_at IS NULL",
            ticket_id,
        )
    return {"messages": [dict(m) for m in msgs]}


@app.post("/admin/tickets/{ticket_id}/messages", status_code=201, tags=["Admin"])
async def admin_send_message(
    ticket_id: str, body: _MsgBody, current: dict = Depends(require_admin)
):
    pool = await get_pool()
    async with pool.acquire() as conn:
        ticket = await conn.fetchrow(
            "SELECT status, claimed_by FROM tickets WHERE id=$1", ticket_id
        )
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        if ticket["status"] not in ("claimed",):
            raise HTTPException(status_code=400, detail="Ticket must be claimed before messaging")
        if ticket["claimed_by"] != current["id"]:
            raise HTTPException(status_code=403, detail="Only the claiming admin can send messages")
        adm = await conn.fetchrow("SELECT name FROM admin_users WHERE id=$1", current["id"])
        mid = secrets.token_hex(10)
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO ticket_messages"
                " (id,ticket_id,sender_id,sender_name,sender_role,message)"
                " VALUES ($1,$2,$3,$4,'admin',$5)",
                mid, ticket_id, current["id"], adm["name"], body.message,
            )
            await conn.execute(
                "UPDATE tickets SET unread_user=unread_user+1, updated_at=NOW() WHERE id=$1",
                ticket_id,
            )
            await _audit(conn, current, "send_message", "ticket_message", mid,
                         {"ticket_id": ticket_id})
    return {"message_id": mid, "status": "sent"}


@app.post("/admin/tickets/{ticket_id}/claim", tags=["Admin"])
async def admin_claim_ticket(ticket_id: str, current: dict = Depends(require_admin)):
    if current["role"] == "superadmin":
        raise HTTPException(status_code=403, detail="Superadmin cannot claim tickets")
    pool = await get_pool()
    async with pool.acquire() as conn:
        limit      = await conn.fetchval(
            "SELECT ticket_limit FROM admin_users WHERE id=$1", current["id"]
        )
        open_count = await conn.fetchval(
            "SELECT COUNT(*) FROM tickets WHERE claimed_by=$1 AND status='claimed'",
            current["id"],
        )
        if limit and open_count >= limit:
            raise HTTPException(
                status_code=403,
                detail=f"You have reached your limit of {limit} claimed tickets.",
            )
        ticket = await conn.fetchrow(
            "SELECT status, claimed_by FROM tickets WHERE id=$1", ticket_id
        )
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        if ticket["status"] != "open":
            raise HTTPException(status_code=400, detail="Ticket is not in 'open' state")
        if ticket["claimed_by"]:
            raise HTTPException(status_code=409, detail="Ticket already claimed")
        async with conn.transaction():
            await conn.execute(
                "UPDATE tickets SET status='claimed', claimed_by=$1, updated_at=NOW() WHERE id=$2",
                current["id"], ticket_id,
            )
            await conn.execute(
                "INSERT INTO ticket_status_log"
                " (ticket_id,changed_by,changer_role,from_status,to_status)"
                " VALUES ($1,$2,'admin','open','claimed')",
                ticket_id, current["id"],
            )
            await _audit(conn, current, "claim_ticket", "ticket", ticket_id, {})
    return {"status": "claimed"}


class _ResolveBody(BaseModel):
    note: str


@app.post("/admin/tickets/{ticket_id}/resolve", tags=["Admin"])
async def admin_resolve_ticket(
    ticket_id: str, body: _ResolveBody, current: dict = Depends(require_admin)
):
    if not body.note.strip():
        raise HTTPException(status_code=400, detail="Resolution note is required")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ticket = await conn.fetchrow(
            "SELECT status, claimed_by FROM tickets WHERE id=$1", ticket_id
        )
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket not found")
        if ticket["claimed_by"] != current["id"]:
            raise HTTPException(status_code=403, detail="Only the claiming admin can resolve")
        if ticket["status"] != "claimed":
            raise HTTPException(status_code=400, detail="Ticket must be 'claimed' to resolve")
        async with conn.transaction():
            await conn.execute(
                "UPDATE tickets SET status='solved', resolution_note=$1, updated_at=NOW() WHERE id=$2",
                body.note, ticket_id,
            )
            await conn.execute(
                "INSERT INTO ticket_status_log"
                " (ticket_id,changed_by,changer_role,from_status,to_status,note)"
                " VALUES ($1,$2,'admin','claimed','solved',$3)",
                ticket_id, current["id"], body.note,
            )
            await _audit(conn, current, "resolve_ticket", "ticket", ticket_id,
                         {"note": body.note[:100]})
    return {"status": "solved"}


@app.post("/admin/tickets/{ticket_id}/mark-read", tags=["Admin"])
async def admin_mark_read(ticket_id: str, current: dict = Depends(require_admin)):
    """Mark all user messages in this ticket as read from the admin's side."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if not await conn.fetchrow("SELECT id FROM tickets WHERE id=$1", ticket_id):
            raise HTTPException(status_code=404, detail="Ticket not found")
        await conn.execute("UPDATE tickets SET unread_admin=0 WHERE id=$1", ticket_id)
        await conn.execute(
            "UPDATE ticket_messages SET read_at=NOW()"
            " WHERE ticket_id=$1 AND sender_role='user' AND read_at IS NULL",
            ticket_id,
        )
    return {"status": "marked_read"}


@app.post("/admin/tickets/{ticket_id}/assign", tags=["Admin"])
async def admin_assign_ticket(
    ticket_id: str, body: dict, current: dict = Depends(require_superadmin)
):
    """Superadmin-only — assign a ticket to any admin."""
    admin_id = body.get("admin_id", "")
    if not admin_id:
        raise HTTPException(status_code=400, detail="admin_id required")
    pool = await get_pool()
    async with pool.acquire() as conn:
        target = await conn.fetchrow(
            "SELECT ticket_limit,"
            " (SELECT COUNT(*) FROM tickets WHERE claimed_by=admin_users.id"
            "   AND status='claimed') AS open"
            " FROM admin_users WHERE id=$1 AND status='active'",
            admin_id,
        )
        if not target:
            raise HTTPException(status_code=404, detail="Admin not found or inactive")
        limit = target["ticket_limit"]
        if limit is None:
            limit = await conn.fetchval("SELECT value FROM platform_settings WHERE key='admin_ticket_limit'")
            limit = int(limit or 10)
        if target["open"] >= limit:
            raise HTTPException(status_code=400, detail="Target admin has reached their limit")
        await conn.execute(
            "UPDATE tickets SET assigned_to=$1, claimed_by=$1, status='claimed',"
            " updated_at=NOW() WHERE id=$2",
            admin_id, ticket_id,
        )
        await conn.execute(
            "INSERT INTO ticket_status_log"
            " (ticket_id,changed_by,changer_role,from_status,to_status,note)"
            " VALUES ($1,$2,'superadmin','open','claimed','Assigned by superadmin')",
            ticket_id, current["id"],
        )
        await _audit(conn, current, "assign_ticket", "ticket", ticket_id,
                     {"admin_id": admin_id})
    return {"status": "assigned", "admin_id": admin_id}


@app.get("/admin/analytics", tags=["Admin"])
async def admin_analytics(days: int = 30, current: dict = Depends(require_admin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Top Level KPIs
        total_clients = await conn.fetchval("SELECT COUNT(*) FROM tenants")
        active_clients = await conn.fetchval("SELECT COUNT(*) FROM tenants WHERE status='active'")
        mrr = await conn.fetchval("""
            SELECT SUM(s.amount_rupee) 
            FROM subscriptions s 
            JOIN tenants t ON s.tenant_id = t.id 
            WHERE t.status='active' AND s.status='active'
            AND s.amount_rupee > 0
            AND EXISTS (SELECT 1 FROM payments p WHERE p.tenant_id = t.id AND p.status='captured')
        """) or 0
        
        sessions_today = await conn.fetchval("SELECT COUNT(*) FROM sessions WHERE started_at >= CURRENT_DATE")
        leads_today = await conn.fetchval("SELECT COUNT(*) FROM leads WHERE created_at >= CURRENT_DATE")
        open_tickets = await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='open' OR status='new'")

        from datetime import timedelta, datetime, timezone
        td = timedelta(days=days)
        
        # Trends
        daily_sessions_rows = await conn.fetch("""
            SELECT TO_CHAR(DATE_TRUNC('day', started_at), 'YYYY-MM-DD') as day, COUNT(*) as count
            FROM sessions
            WHERE started_at >= NOW() - $1::interval
            GROUP BY day ORDER BY day
        """, td)
        
        daily_revenue_rows = await conn.fetch("""
            SELECT TO_CHAR(DATE_TRUNC('day', created_at), 'YYYY-MM-DD') as day, SUM(amount_rupee) as amount
            FROM payments
            WHERE status='captured' AND created_at >= NOW() - $1::interval
            GROUP BY day ORDER BY day
        """, td)
        
        daily_tickets_rows = await conn.fetch("""
            SELECT TO_CHAR(DATE_TRUNC('day', created_at), 'YYYY-MM-DD') as day, COUNT(*) as count
            FROM tickets
            WHERE created_at >= NOW() - $1::interval
            GROUP BY day ORDER BY day
        """, td)

        # Plan Distribution
        plan_dist_rows = await conn.fetch("""
            SELECT p.name as plan, COUNT(t.id) as count
            FROM tenants t
            JOIN plans p ON t.plan = p.id
            WHERE t.status='active'
            GROUP BY p.name
        """)

    total_active_plan = sum(r["count"] for r in plan_dist_rows)
    plan_distribution = [
        {
            "plan": r["plan"],
            "count": r["count"],
            "pct": round((r["count"] / total_active_plan * 100)) if total_active_plan > 0 else 0
        }
        for r in plan_dist_rows
    ]

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - td
    date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days + 1)]
    
    sessions_dict = {r["day"]: r["count"] for r in daily_sessions_rows}
    revenue_dict = {r["day"]: r["amount"] for r in daily_revenue_rows}
    tickets_dict = {r["day"]: r["count"] for r in daily_tickets_rows}
    
    filled_sessions = [{"day": d, "sessions": sessions_dict.get(d, 0)} for d in date_list]
    filled_revenue = [{"day": d, "revenue": revenue_dict.get(d, 0)} for d in date_list]
    filled_tickets = [{"day": d, "tickets": tickets_dict.get(d, 0)} for d in date_list]

    return {
        "kpis": {
            "total_clients": total_clients,
            "active_clients": active_clients,
            "mrr": mrr,
            "sessions_today": sessions_today,
            "leads_today": leads_today,
            "open_tickets": open_tickets
        },
        "trends": {
            "sessions": filled_sessions,
            "revenue": filled_revenue,
            "tickets": filled_tickets
        },
        "plan_distribution": plan_distribution
    }



# ═════════════════════════════════════════════════════════════════════════════
# SUPERADMIN  (JWT — superadmin)
# ═════════════════════════════════════════════════════════════════════════════


@app.get("/superadmin/clients", tags=["SuperAdmin"])

async def sa_list_clients(
    page:   int = 1,
    search: str = "",
    plan:   str = "",
    status: str = "",
    current: dict = Depends(require_superadmin),
):
    """Same as admin client list but superadmin-scoped with audit info."""
    limit  = 50
    offset = (page - 1) * limit
    pool   = await get_pool()
    async with pool.acquire() as conn:
        conds:  list[str] = ["TRUE"]
        params: list      = []
        if search:
            params.append(f"%{search}%")
            n = len(params)
            conds.append(
                f"(t.name ILIKE ${n} OR t.email ILIKE ${n} OR t.company ILIKE ${n})"
            )
        if plan:
            params.append(plan);   conds.append(f"t.plan=${len(params)}")
        if status:
            params.append(status); conds.append(f"t.status=${len(params)}")
        where = " AND ".join(conds)
        rows = await conn.fetch(
            f"""
            SELECT t.*,
                   s.plan AS sub_plan, s.status AS sub_status,
                   s.current_period_end,
                   (SELECT COUNT(*) FROM tickets tk WHERE tk.tenant_id=t.id
                      AND tk.status IN ('open','claimed')) AS open_tickets,
                   (SELECT MAX(ua.last_active) FROM user_auth ua
                    WHERE ua.tenant_id=t.id) AS last_active
              FROM tenants t
              LEFT JOIN subscriptions s ON s.tenant_id=t.id AND s.status='active'
             WHERE {where}
             ORDER BY t.created_at DESC
             LIMIT ${len(params)+1} OFFSET ${len(params)+2}
            """,
            *params, limit, offset,
        )
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM tenants t WHERE {where}", *params
        )
    return {"clients": [dict(r) for r in rows], "total": total, "page": page}


@app.get("/superadmin/clients/{client_id}", tags=["SuperAdmin"])
async def sa_get_client(client_id: str, current: dict = Depends(require_superadmin)):
    """Full client detail including audit log — superadmin only."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tenants WHERE id=$1", client_id)
        if not row:
            raise HTTPException(status_code=404, detail="Client not found")
        sub  = await conn.fetchrow(
            "SELECT * FROM subscriptions WHERE tenant_id=$1 AND status='active'"
            " ORDER BY created_at DESC LIMIT 1", client_id,
        )

        users = await conn.fetch(
            "SELECT id,name,email,role,status,last_active"
            " FROM user_auth WHERE tenant_id=$1", client_id,
        )
        audit = await conn.fetch(
            """
            SELECT a.*, 
                   COALESCE(u.name, adm.name) as actor_name, 
                   COALESCE(u.email, adm.email) as actor_email,
                   t.name as tenant_name
              FROM audit_log a
              LEFT JOIN user_auth u ON a.actor_id = u.id AND a.actor_role IN ('owner', 'member')
              LEFT JOIN admin_users adm ON a.actor_id = adm.id AND a.actor_role IN ('admin', 'superadmin')
              LEFT JOIN tenants t ON a.tenant_id = t.id
             WHERE a.tenant_id=$1
             ORDER BY a.created_at DESC LIMIT 50
            """, client_id,
        )
        tkts = await conn.fetch(
            "SELECT id,heading,status,priority,created_at FROM tickets"
            " WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT 20", client_id,
        )
    tenant_dict = dict(row)
    # Ensure new fields are in the response
    tenant_dict.setdefault("domain_verified", False)
    tenant_dict.setdefault("verification_token", None)
    tenant_dict.setdefault("suspension_reason", None)
    return {
        "tenant":       tenant_dict,
        "subscription": dict(sub) if sub else None,
        "users":        [dict(u) for u in users],
        "tickets":      [dict(t) for t in tkts],
        "audit_log":    [dict(a) for a in audit],
    }


@app.post("/superadmin/clients/{client_id}/toggle-domain-verified", tags=["SuperAdmin"])
async def sa_toggle_domain_verified(
    client_id: str, body: dict = Body(...),
    current: dict = Depends(require_superadmin),
):
    """Superadmin: force-verify or revoke domain verification for a client."""
    verified = body.get("verified", False)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tenants SET domain_verified=$1, updated_at=NOW() WHERE id=$2",
            verified, client_id,
        )
        await _audit(conn, current, "toggle_domain_verified", "tenant", client_id,
                     {"domain_verified": verified})
    return {"status": "ok", "domain_verified": verified}


@app.post("/superadmin/clients/{client_id}/ticket-limit", tags=["SuperAdmin"])
async def sa_set_client_ticket_limit(
    client_id: str, body: dict, current: dict = Depends(require_superadmin)
):
    limit = int(body.get("limit", 2))
    pool  = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tenants SET ticket_limit=$1, updated_at=NOW() WHERE id=$2",
            limit, client_id,
        )
        await _audit(conn, current, "set_ticket_limit", "tenant", client_id, {"limit": limit}, tenant_id=client_id)
    return {"status": "updated", "limit": limit}


@app.post("/superadmin/clients/{client_id}/force-logout", tags=["SuperAdmin"])
async def sa_force_logout(client_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _audit(conn, current, "force_logout", "tenant", client_id, {}, tenant_id=client_id)
    # Stateless JWT: client must discard token. Add token-blacklist table for hard enforcement.
    return {"status": "force_logout_recorded"}


@app.delete("/superadmin/clients/{client_id}", tags=["SuperAdmin"])
async def sa_delete_client(client_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM tenants WHERE id=$1", client_id)
        await _audit(conn, current, "delete_tenant", "tenant", client_id, {}, tenant_id=client_id)
    await app.state.s3.delete_prefix(f"tenants/{client_id}/")
    return {"status": "deleted"}


# ── Superadmin: Change Tenant Email (OTP-verified) ────────────────────────

class _EmailChangeReq(BaseModel):
    new_email: str

class _EmailChangeConfirm(BaseModel):
    new_email: str
    otp: str

@app.post("/superadmin/clients/{client_id}/request-email-change", tags=["SuperAdmin"])
async def sa_request_email_change(
    client_id: str, body: _EmailChangeReq, current: dict = Depends(require_superadmin)
):
    """Send OTP to the new email for verification. Client shares OTP with superadmin."""
    import hashlib, json as _json
    pool = await get_pool()
    async with pool.acquire() as conn:
        tenant = await conn.fetchrow("SELECT email, company FROM tenants WHERE id=$1", client_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")
        if tenant["email"] == body.new_email:
            raise HTTPException(status_code=400, detail="New email is same as current email")

        # Check if new email is already in use
        if await conn.fetchrow("SELECT id FROM tenants WHERE email=$1", body.new_email):
            raise HTTPException(status_code=409, detail="Email already in use by another tenant")
        if await conn.fetchrow("SELECT id FROM user_auth WHERE email=$1", body.new_email):
            raise HTTPException(status_code=409, detail="Email already in use by another user")

        # Generate OTP and store
        otp_code = f"{secrets.randbelow(900000) + 100000}"
        otp_hash = hashlib.sha256(otp_code.encode()).hexdigest()
        otp_id = secrets.token_hex(8)
        expires_at = datetime.utcnow() + timedelta(minutes=10)

        await conn.execute(
            "INSERT INTO otps (id, email, otp_hash, purpose, payload, expires_at)"
            " VALUES ($1, $2, $3, 'sa_email_change', $4, $5)",
            otp_id, body.new_email, otp_hash,
            _json.dumps({"tenant_id": client_id, "old_email": tenant["email"]}),
            expires_at,
        )

    # Send OTP to new email
    from lead_manager import send_email
    html = f"""
    <div style="font-family:sans-serif;max-width:500px;margin:auto;padding:24px;">
        <h2 style="color:#2952e3;">Email Change Verification</h2>
        <p>A request has been made to change the primary email for
        <strong>{tenant['company'] or 'your account'}</strong> to this address.</p>
        <p style="font-size:32px;font-weight:bold;letter-spacing:8px;text-align:center;
                  background:#f3f4f6;padding:16px;border-radius:8px;color:#2952e3;">
            {otp_code}
        </p>
        <p>Please share this code with your account administrator to complete the change.</p>
        <p style="color:#999;font-size:12px;">This code expires in 10 minutes.</p>
    </div>
    """
    try:
        await send_email(body.new_email, "Email Change Verification - OTP", html)
    except Exception as e:
        logger.error(f"Failed to send email change OTP: {e}")
        raise HTTPException(status_code=500, detail="Failed to send OTP email")

    return {"status": "otp_sent", "message": f"OTP sent to {body.new_email}"}


@app.post("/superadmin/clients/{client_id}/confirm-email-change", tags=["SuperAdmin"])
async def sa_confirm_email_change(
    client_id: str, body: _EmailChangeConfirm, current: dict = Depends(require_superadmin)
):
    """Verify OTP and update tenant email across all tables."""
    import hashlib, json as _json
    otp_hash = hashlib.sha256(body.otp.encode()).hexdigest()

    pool = await get_pool()
    async with pool.acquire() as conn:
        # Verify OTP
        otp_row = await conn.fetchrow(
            "SELECT id, payload FROM otps"
            " WHERE email=$1 AND otp_hash=$2 AND purpose='sa_email_change'"
            "   AND used=FALSE AND expires_at > NOW()"
            " ORDER BY created_at DESC LIMIT 1",
            body.new_email, otp_hash,
        )
        if not otp_row:
            raise HTTPException(status_code=400, detail="Invalid or expired OTP")

        payload = otp_row["payload"] if isinstance(otp_row["payload"], dict) else _json.loads(otp_row["payload"])
        if payload.get("tenant_id") != client_id:
            raise HTTPException(status_code=400, detail="OTP does not match this tenant")

        old_email = payload.get("old_email")

        async with conn.transaction():
            # Update tenant email
            await conn.execute(
                "UPDATE tenants SET email=$1, updated_at=NOW() WHERE id=$2",
                body.new_email, client_id,
            )
            # Update owner user_auth email
            await conn.execute(
                "UPDATE user_auth SET email=$1, updated_at=NOW()"
                " WHERE tenant_id=$2 AND role='owner' AND email=$3",
                body.new_email, client_id, old_email,
            )
            # Update widget notification email
            await conn.execute(
                "UPDATE widget_configs SET notification_email=$1, updated_at=NOW()"
                " WHERE tenant_id=$2",
                body.new_email, client_id,
            )
            # Mark OTP as used
            await conn.execute("UPDATE otps SET used=TRUE WHERE id=$1", otp_row["id"])

            await _audit(conn, current, "change_email", "tenant", client_id,
                         {"old_email": old_email, "new_email": body.new_email},
                         tenant_id=client_id)

    return {"status": "email_changed", "new_email": body.new_email}


# ── Superadmin Knowledge Base Manager ───────────────────────────────────────

@app.get("/superadmin/clients/{client_id}/knowledge/list", tags=["SuperAdmin"])
async def sa_list_knowledge(client_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        docs = await conn.fetch("SELECT * FROM ingest_jobs WHERE tenant_id=$1 ORDER BY created_at DESC", client_id)
        qa = await conn.fetch("SELECT * FROM knowledge_qa WHERE tenant_id=$1 ORDER BY created_at DESC", client_id)
    return {
        "docs": [dict(r) for r in docs],
        "qa": [dict(r) for r in qa]
    }

@app.post("/superadmin/clients/{client_id}/knowledge/ingest", tags=["SuperAdmin"])
async def sa_ingest_document(client_id: str, file: UploadFile = File(...), current: dict = Depends(require_superadmin)):
    if file.size and file.size > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="File too large")
    content = await file.read()
    job_id  = await app.state.ai.rag_ingest(
        content=content, filename=file.filename,
        content_type=file.content_type, tenant_id=client_id
    )
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _audit(conn, current, "ingest", "document", job_id, {"filename": file.filename, "target_tenant": client_id}, tenant_id=client_id)

    return {"job_id": job_id, "status": "processing", "filename": file.filename}

@app.post("/superadmin/clients/{client_id}/knowledge/qa", tags=["SuperAdmin"])
async def sa_add_qa(client_id: str, req: KnowledgeQACreate, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    qa_id = secrets.token_hex(8)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "INSERT INTO knowledge_qa (id, tenant_id, question, answer) VALUES ($1, $2, $3, $4)",
                qa_id, client_id, req.question, req.answer,
            )
            await _audit(conn, current, "create", "knowledge_qa", qa_id, {"question": req.question[:50], "target_tenant": client_id}, tenant_id=client_id)

    try:
        await app.state.ai.rag_ingest_qa(
            qa_id=qa_id, question=req.question,
            answer=req.answer, tenant_id=client_id,
        )
    except Exception as e:
        logger.warning(f"SA Q/A RAG sync failed (qa_id={qa_id}): {e}")

    return {"id": qa_id, "status": "created"}

@app.delete("/superadmin/clients/{client_id}/knowledge/qa/{qa_id}", tags=["SuperAdmin"])
async def sa_delete_qa(client_id: str, qa_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM knowledge_qa WHERE id=$1 AND tenant_id=$2", qa_id, client_id)
        if not row:
            raise HTTPException(status_code=404, detail="Q/A not found")
            
        async with conn.transaction():
            await conn.execute("DELETE FROM knowledge_qa WHERE id=$1", qa_id)
            await _audit(conn, current, "delete", "knowledge_qa", qa_id, {"target_tenant": client_id}, tenant_id=client_id)

    try:
        await app.state.ai.rag_delete_qa(qa_id=qa_id, tenant_id=client_id)
    except Exception as e:
        logger.warning(f"SA Q/A RAG delete failed: {e}")

    return {"status": "deleted"}

@app.delete("/superadmin/clients/{client_id}/knowledge/{doc_id}", tags=["SuperAdmin"])
async def sa_delete_document(client_id: str, doc_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM ingest_jobs WHERE id=$1 AND tenant_id=$2", doc_id, client_id)
        if not row:
            raise HTTPException(status_code=404, detail="Document not found")
        await conn.execute("DELETE FROM ingest_jobs WHERE id=$1", doc_id)
        await _audit(conn, current, "delete", "ingest_job", doc_id, {"target_tenant": client_id}, tenant_id=client_id)

    try:
        await app.state.ai.rag_delete_doc(doc_id=doc_id, tenant_id=client_id)
    except Exception as e:
        logger.warning(f"SA Doc RAG delete failed (doc_id={doc_id}): {e}")

    return {"status": "deleted", "doc_id": doc_id}


# ── Superadmin Structured KB (mirrors tenant KB screen) ─────────────────────

@app.get("/superadmin/clients/{client_id}/kb/company", tags=["SuperAdmin"])
async def sa_kb_get_company(client_id: str, current: dict = Depends(require_superadmin)):
    """Fetch all company data fields for a client — superadmin."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, section, field_key, field_value, display_order "
            "FROM kb_company_data WHERE tenant_id=$1 ORDER BY section, display_order",
            client_id,
        )
    return {"data": [dict(r) for r in rows], "sections": _KB_SECTIONS}


@app.put("/superadmin/clients/{client_id}/kb/company", tags=["SuperAdmin"])
async def sa_kb_update_company(client_id: str, req: CompanyDataUpdate, current: dict = Depends(require_superadmin)):
    """Batch upsert company data for a client — superadmin."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for idx, item in enumerate(req.data):
                if item.section not in _KB_SECTIONS:
                    continue
                row_id = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO kb_company_data (id, tenant_id, section, field_key, field_value, display_order, updated_at) "
                    "VALUES ($1, $2, $3, $4, $5, $6, NOW()) "
                    "ON CONFLICT (tenant_id, section, field_key) "
                    "DO UPDATE SET field_value=$5, display_order=$6, updated_at=NOW()",
                    row_id, client_id, item.section, item.field_key, item.field_value, idx,
                )
            await _audit(conn, current, "update", "kb_company_data", client_id, {"count": len(req.data)}, tenant_id=client_id)
    return {"status": "saved", "count": len(req.data)}


@app.get("/superadmin/clients/{client_id}/kb/products", tags=["SuperAdmin"])
async def sa_kb_list_products(client_id: str, current: dict = Depends(require_superadmin)):
    """List all products for a client — superadmin."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM kb_products WHERE tenant_id=$1 ORDER BY category, name", client_id,
        )
    return {"products": [dict(r) for r in rows]}


@app.post("/superadmin/clients/{client_id}/kb/products", status_code=201, tags=["SuperAdmin"])
async def sa_kb_create_product(client_id: str, req: ProductCreate, current: dict = Depends(require_superadmin)):
    """Create a product for a client — superadmin."""
    pid = secrets.token_hex(8)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO kb_products (id, tenant_id, category, sub_category, name, description, "
            "image_url, pricing, min_order_qty, source_url) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            pid, client_id, req.category, req.sub_category, req.name, req.description,
            req.image_url, req.pricing, req.min_order_qty, req.source_url,
        )
        await _audit(conn, current, "create", "kb_product", pid, {"name": req.name, "target_tenant": client_id}, tenant_id=client_id)
    return {"id": pid, "status": "created"}


@app.put("/superadmin/clients/{client_id}/kb/products/{product_id}", tags=["SuperAdmin"])
async def sa_kb_update_product(client_id: str, product_id: str, req: ProductUpdate, current: dict = Depends(require_superadmin)):
    """Update a product for a client — superadmin."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetchrow(
            "SELECT id FROM kb_products WHERE id=$1 AND tenant_id=$2", product_id, client_id,
        )
        if not existing:
            raise HTTPException(404, "Product not found")
        updates, params, idx = [], [product_id, client_id], 3
        for field in ("category", "sub_category", "name", "description", "image_url", "pricing", "min_order_qty", "source_url"):
            val = getattr(req, field, None)
            if val is not None:
                updates.append(f"{field}=${idx}")
                params.append(val)
                idx += 1
        if not updates:
            return {"status": "no_changes"}
        updates.append("updated_at=NOW()")
        sql = f"UPDATE kb_products SET {', '.join(updates)} WHERE id=$1 AND tenant_id=$2"
        await conn.execute(sql, *params)
        await _audit(conn, current, "update", "kb_product", product_id, {"target_tenant": client_id}, tenant_id=client_id)
    return {"status": "updated"}


@app.delete("/superadmin/clients/{client_id}/kb/products/{product_id}", tags=["SuperAdmin"])
async def sa_kb_delete_product(client_id: str, product_id: str, current: dict = Depends(require_superadmin)):
    """Delete a product for a client — superadmin."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM kb_products WHERE id=$1 AND tenant_id=$2", product_id, client_id,
        )
        if not row:
            raise HTTPException(404, "Product not found")
        await conn.execute("DELETE FROM kb_products WHERE id=$1", product_id)
        await _audit(conn, current, "delete", "kb_product", product_id, {"target_tenant": client_id}, tenant_id=client_id)
    return {"status": "deleted"}


@app.post("/superadmin/clients/{client_id}/kb/products/upload-image", tags=["SuperAdmin"])
async def sa_kb_upload_product_image(
    client_id: str, file: UploadFile = File(...), current: dict = Depends(require_superadmin),
):
    """Upload a product image for a client — superadmin."""
    content = await file.read()
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Image too large (max 5 MB)")

    try:
        mime_type = validate_image(content)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    ext = Path(file.filename or "image.jpg").suffix or ".jpg"
    key = f"tenants/{client_id}/products/{secrets.token_hex(8)}{ext}"

    actual_key = await app.state.s3.upload(
        key=key,
        content=content,
        content_type=mime_type,
        cache_control="public, max-age=31536000, immutable"
    )

    return {"image_url": app.state.s3.resolve_url(actual_key)}


@app.post("/superadmin/clients/{client_id}/kb/products/scrape-url", tags=["SuperAdmin"])
async def sa_kb_scrape_product_url(
    client_id: str, url: str = Body(..., embed=True), current: dict = Depends(require_superadmin),
):
    """Scrape a single URL for product enrichment — superadmin."""
    try:
        page_data = await app.state.ai.scrape_single_page(url)
    except Exception as e:
        raise HTTPException(502, f"Failed to fetch page: {e}")
    text = page_data.get("text", "")
    if not text.strip():
        raise HTTPException(400, "No extractable text found on the page")
    try:
        enriched = await app.state.ai.enrich_product_from_text(text=text, url=url)
    except Exception as e:
        raise HTTPException(502, f"Enrichment failed: {e}")
    return {"status": "enriched", "page_title": page_data.get("title", ""), "product": enriched}


@app.post("/superadmin/clients/{client_id}/kb/sync", tags=["SuperAdmin"])
async def sa_kb_sync_vectors(client_id: str, current: dict = Depends(require_superadmin)):
    """Trigger vector DB rebuild for a client — superadmin."""
    pool = await get_pool()
    import hashlib
    chunks = []
    async with pool.acquire() as conn:
        company_rows = await conn.fetch(
            "SELECT section, field_key, field_value FROM kb_company_data "
            "WHERE tenant_id=$1 ORDER BY section, display_order", client_id,
        )
        sections: Dict[str, list] = {}
        for row in company_rows:
            sec = row["section"]
            if sec not in sections:
                sections[sec] = []
            if row["field_value"].strip():
                sections[sec].append(f"{row['field_key'].replace('_', ' ').title()}: {row['field_value']}")
        for sec_name, lines in sections.items():
            if lines:
                text = f"{sec_name.replace('_', ' ').title()}\n" + "\n".join(lines)
                chunk_id = f"kb_company_{hashlib.md5(f'{client_id}:{sec_name}'.encode()).hexdigest()[:12]}"
                chunks.append({"id": chunk_id, "text": text, "source": f"Company Data - {sec_name.replace('_', ' ').title()}", "chunk_index": 0, "word_count": len(text.split())})

        products = await conn.fetch("SELECT * FROM kb_products WHERE tenant_id=$1 ORDER BY category, name", client_id)
        for prod in products:
            parts = [f"Product: {prod['name']}"]
            if prod["category"]: parts.append(f"Category: {prod['category']}")
            if prod["sub_category"]: parts.append(f"Sub-category: {prod['sub_category']}")
            if prod["description"]: parts.append(f"Description: {prod['description']}")
            if prod["pricing"]: parts.append(f"Pricing: {prod['pricing']}")
            if prod["min_order_qty"]: parts.append(f"Minimum Order Quantity: {prod['min_order_qty']}")
            text = "\n".join(parts)
            chunks.append({"id": f"kb_product_{prod['id']}", "text": text, "source": f"Product - {prod['name']}", "chunk_index": 0, "word_count": len(text.split())})

        qas = await conn.fetch("SELECT id, question, answer FROM knowledge_qa WHERE tenant_id=$1", client_id)
        for qa in qas:
            text = f"Question: {qa['question']}\nAnswer: {qa['answer']}"
            chunks.append({"id": f"qa_{qa['id']}", "text": text, "source": "Custom Q/A", "chunk_index": 0, "word_count": len(text.split())})

        await _audit(conn, current, "kb_sync", "tenant", client_id, {"chunks": len(chunks)}, tenant_id=client_id)

    try:
        result = await app.state.ai.rag_rebuild_structured(tenant_id=client_id, chunks=chunks)
        return {"status": "synced", **result}
    except Exception as e:
        raise HTTPException(502, f"Vector sync failed: {e}")



# ── Admin management ───────────────────────────────────────────────────────────
@app.get("/superadmin/admins", tags=["SuperAdmin"])
async def sa_list_admins(current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, email, role, status, ticket_limit, last_active, created_at,"
            "       (SELECT COUNT(*) FROM tickets"
            "         WHERE claimed_by=admin_users.id AND status='claimed') AS open_tickets,"
            "       (SELECT COUNT(*) FROM tickets"
            "         WHERE claimed_by=admin_users.id AND status='solved'"
            "           AND updated_at>=DATE_TRUNC('month',NOW())) AS resolved_this_month"
            " FROM admin_users ORDER BY role DESC, created_at"
        )
    return {"admins": [dict(r) for r in rows]}


class _NewAdmin(BaseModel):
    name:     str
    email:    EmailStr
    password: str


class _EditAdmin(BaseModel):
    name:         Optional[str] = None
    ticket_limit: Optional[int] = None
    status:       Optional[str] = None
    role:         Optional[str] = None


@app.post("/superadmin/admins", status_code=201, tags=["SuperAdmin"])
async def sa_create_admin(body: _NewAdmin, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if await conn.fetchrow("SELECT id FROM admin_users WHERE email=$1", body.email):
            raise HTTPException(status_code=409, detail="Email already registered")
        aid = secrets.token_hex(8)
        limit = await conn.fetchval("SELECT value FROM platform_settings WHERE key='admin_ticket_limit'")
        limit = int(limit or 10)
        await conn.execute(
            "INSERT INTO admin_users (id,name,email,password_hash,role,ticket_limit)"
            " VALUES ($1,$2,$3,$4,'admin',$5)",
            aid, body.name, body.email, hash_password(body.password), limit
        )
        await _audit(conn, current, "create_admin", "admin_user", aid,
                     {"email": body.email, "name": body.name})
    return {"admin_id": aid, "status": "created"}


@app.put("/superadmin/admins/{admin_id}", tags=["SuperAdmin"])
async def sa_edit_admin(
    admin_id: str, body: _EditAdmin, current: dict = Depends(require_superadmin)
):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided")
    if "status" in updates and updates.get("status") not in ("active", "inactive"):
        raise HTTPException(status_code=400, detail="status must be active or inactive")
    if "role" in updates and updates.get("role") not in ("admin", "superadmin"):
        raise HTTPException(status_code=400, detail="role must be admin or superadmin")
    keys = list(updates.keys());  vals = list(updates.values())
    set_ = ", ".join(f"{k}=${i+1}" for i, k in enumerate(keys))
    pool = await get_pool()
    async with pool.acquire() as conn:
        if "role" in updates and admin_id == current["id"]:
             raise HTTPException(status_code=400, detail="Cannot change your own role")
        res = await conn.execute(
            f"UPDATE admin_users SET {set_}, updated_at=NOW()"
            f" WHERE id=${len(keys)+1}",
            *vals, admin_id,
        )
        if res == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Admin not found")
        await _audit(conn, current, "edit_admin", "admin_user", admin_id, updates)
    return {"status": "updated", "fields": keys}


# In-memory store for delete OTPs
ADMIN_DELETE_OTPS = {}

@app.post("/superadmin/admins/{admin_id}/request-delete", tags=["SuperAdmin"])
async def sa_request_delete_admin(admin_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        target = await conn.fetchrow("SELECT role, email FROM admin_users WHERE id=$1", admin_id)
        if not target:
            raise HTTPException(status_code=404, detail="Admin not found")
        if target["role"] == "superadmin":
             raise HTTPException(status_code=400, detail="Cannot delete a superadmin. Downgrade to admin first.")
        if admin_id == current["id"]:
             raise HTTPException(status_code=400, detail="Cannot delete your own account")
             
        otp = str(secrets.randbelow(1_000_000)).zfill(6)
        ADMIN_DELETE_OTPS[admin_id] = otp
        
        superadmins = await conn.fetch("SELECT email FROM admin_users WHERE role='superadmin'")
        sa_emails = [r["email"] for r in superadmins]
        
        # Send actual emails via SMTP here
        for email in sa_emails:
            await app.state.leads.send_otp_email(email, otp, f"Admin Deletion ({target['email']})")
        logger.info(f"OTP to delete {target['email']}: {otp} (Sent to: {', '.join(sa_emails)})")
        
    return {"status": "otp_sent", "message": f"OTP sent to all superadmins"}


@app.delete("/superadmin/admins/{admin_id}", tags=["SuperAdmin"])
async def sa_remove_admin(admin_id: str, otp: str, current: dict = Depends(require_superadmin)):
    if ADMIN_DELETE_OTPS.get(admin_id) != otp:
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")
        
    pool = await get_pool()
    async with pool.acquire() as conn:
        target = await conn.fetchrow("SELECT role FROM admin_users WHERE id=$1", admin_id)
        if not target:
             raise HTTPException(status_code=404, detail="Admin not found")
        if target["role"] == "superadmin":
             raise HTTPException(status_code=400, detail="Cannot delete a superadmin")
             
        await conn.execute("DELETE FROM admin_users WHERE id=$1 AND role='admin'", admin_id)
        await _audit(conn, current, "delete_admin", "admin_user", admin_id, {})
    
    ADMIN_DELETE_OTPS.pop(admin_id, None)
    return {"status": "deleted"}


@app.post("/superadmin/admins/{admin_id}/ticket-limit", tags=["SuperAdmin"])
async def sa_set_admin_ticket_limit(
    admin_id: str, body: dict, current: dict = Depends(require_superadmin)
):
    limit = int(body.get("limit", 10))
    pool  = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE admin_users SET ticket_limit=$1, updated_at=NOW() WHERE id=$2",
            limit, admin_id,
        )
        await _audit(conn, current, "set_ticket_limit", "admin_user", admin_id, {"limit": limit})
    return {"status": "updated", "limit": limit}


# ── Superadmin ticket monitor ──────────────────────────────────────────────────
@app.get("/superadmin/tickets", tags=["SuperAdmin"])
async def sa_list_tickets(
    status:   str = "",
    priority: str = "",
    assigned: str = "",
    page:     int = 1,
    current:  dict = Depends(require_superadmin),
):
    limit  = 50
    offset = (page - 1) * limit
    pool   = await get_pool()
    conds:  list[str] = ["TRUE"]
    params: list      = []
    if status:
        params.append(status);   conds.append(f"t.status=${len(params)}")
    if priority:
        params.append(priority); conds.append(f"t.priority=${len(params)}")
    if assigned:
        params.append(assigned); conds.append(f"t.claimed_by=${len(params)}")
    where = " AND ".join(conds)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT t.*, tn.name AS client_name, tn.plan AS client_plan,
                   a.name AS claimed_by_name
              FROM tickets t
              JOIN tenants tn ON tn.id=t.tenant_id
              LEFT JOIN admin_users a ON a.id=t.claimed_by
             WHERE {where}
             ORDER BY
               CASE t.priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                                WHEN 'medium'  THEN 3 ELSE 4 END,
               t.created_at
             LIMIT ${len(params)+1} OFFSET ${len(params)+2}
            """,
            *params, limit, offset,
        )
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM tickets t WHERE {where}", *params
        )
    return {"tickets": [dict(r) for r in rows], "total": total, "page": page}


class _PriorityBody(BaseModel):
    priority: str


@app.post("/superadmin/tickets/{ticket_id}/change-priority", tags=["SuperAdmin"])
async def sa_change_ticket_priority(
    ticket_id: str, body: _PriorityBody, current: dict = Depends(require_superadmin)
):
    if body.priority not in ("low", "medium", "high", "critical"):
        raise HTTPException(status_code=400, detail="Invalid priority")
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE tickets SET priority=$1, updated_at=NOW() WHERE id=$2",
            body.priority, ticket_id,
        )
        if res == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Ticket not found")
        await _audit(conn, current, "change_priority", "ticket", ticket_id,
                     {"priority": body.priority})
    return {"status": "updated", "priority": body.priority}


# ── Platform settings ──────────────────────────────────────────────────────────
@app.get("/superadmin/settings", tags=["SuperAdmin"])
async def sa_get_settings(current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM platform_settings ORDER BY key")
    return {r["key"]: r["value"] for r in rows}


@app.put("/superadmin/settings", tags=["SuperAdmin"])
async def sa_update_settings(body: dict, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for key, val in body.items():
                if key == "admin_ticket_limit":
                    old_val = await conn.fetchval("SELECT value FROM platform_settings WHERE key='admin_ticket_limit'")
                    if old_val:
                        # Propagate change to all admins who are currently on the old default
                        await conn.execute(
                            "UPDATE admin_users SET ticket_limit=$1 WHERE ticket_limit=$2",
                            int(val), int(old_val)
                        )

                await conn.execute(
                    "INSERT INTO platform_settings (key,value,updated_by,updated_at)"
                    " VALUES ($1,$2,$3,NOW())"
                    " ON CONFLICT (key) DO UPDATE"
                    "   SET value=$2, updated_by=$3, updated_at=NOW()",
                    str(key), str(val), current["id"],
                )
            await _audit(conn, current, "update_settings", "platform", "settings", body)
    return {"status": "updated", "fields": list(body.keys())}


# ── Audit log ──────────────────────────────────────────────────────────────────
@app.get("/superadmin/audit-log", tags=["SuperAdmin"])
async def sa_audit_log(
    page:        int = 1,
    actor:       str = "",
    entity_type: str = "",
    action:      str = "",
    days:        int = 30,
    current:     dict = Depends(require_superadmin),
):
    limit  = 50
    offset = (page - 1) * limit
    pool   = await get_pool()
    async with pool.acquire() as conn:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        conds:  list[str] = ["a.created_at >= $1"]
        params: list      = [cutoff]
        if actor:
            params.append(f"%{actor}%"); conds.append(f"a.actor_id ILIKE ${len(params)}")
        if entity_type:
            params.append(entity_type);  conds.append(f"a.entity_type=${len(params)}")
        if action:
            params.append(f"%{action}%"); conds.append(f"a.action ILIKE ${len(params)}")
        where = " AND ".join(conds)
        rows = await conn.fetch(
            f"""
            SELECT a.*, 
                   COALESCE(u.name, adm.name) as actor_name, 
                   COALESCE(u.email, adm.email) as actor_email,
                   t.name as tenant_name
              FROM audit_log a
              LEFT JOIN user_auth u ON a.actor_id = u.id AND a.actor_role IN ('owner', 'member')
              LEFT JOIN admin_users adm ON a.actor_id = adm.id AND a.actor_role IN ('admin', 'superadmin')
              LEFT JOIN tenants t ON a.tenant_id = t.id
             WHERE {where}
             ORDER BY a.created_at DESC
             LIMIT ${len(params)+1} OFFSET ${len(params)+2}
            """,
            *params, limit, offset,
        )
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM audit_log a WHERE {where}", *params
        )
    return {"log": [dict(r) for r in rows], "total": total, "page": page}


# ── Plan Management (Superadmin) ────────────────────────────────────────────────
@app.get("/superadmin/plans", tags=["SuperAdmin"])
async def sa_list_plans(current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM plans ORDER BY onboarding_fee_rupee")
    return {"plans": [dict(r) for r in rows]}


@app.post("/superadmin/plans", status_code=201, tags=["SuperAdmin"])
async def sa_create_plan(body: PlanCreate, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if await conn.fetchrow("SELECT id FROM plans WHERE id=$1", body.id):
            raise HTTPException(status_code=409, detail="Plan ID already exists")
        
        await conn.execute(
            "INSERT INTO plans (id, name, onboarding_fee_rupee, base_fee_rupee, currency, input_token_rate, output_token_rate, ticket_limit, domain_limit, description, razorpay_plan_id, discount_type, discount_value)"
            " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            body.id, body.name, body.onboarding_fee_rupee, body.base_fee_rupee,
            body.currency, body.input_token_rate, body.output_token_rate,
            body.ticket_limit, body.domain_limit,
            body.description, body.razorpay_plan_id, body.discount_type, body.discount_value
        )
        await _audit(conn, current, "create_plan", "plan", body.id, body.model_dump())
    return {"status": "created", "plan_id": body.id}


@app.put("/superadmin/plans/{plan_id}", tags=["SuperAdmin"])
async def sa_update_plan(plan_id: str, body: PlanUpdate, current: dict = Depends(require_superadmin)):
    updates = body.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    
    keys = list(updates.keys())
    vals = list(updates.values())
    set_clause = ", ".join([f"{k} = ${i+1}" for i, k in enumerate(keys)])
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            f"UPDATE plans SET {set_clause}, updated_at = NOW() WHERE id = ${len(keys)+1}",
            *vals, plan_id
        )
        if res == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Plan not found")
        await _audit(conn, current, "update_plan", "plan", plan_id, updates)
    return {"status": "updated", "plan_id": plan_id}


@app.delete("/superadmin/plans/{plan_id}", tags=["SuperAdmin"])
async def sa_delete_plan(plan_id: str, current: dict = Depends(require_superadmin)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Check if any tenant is using this plan
        usage = await conn.fetchval("SELECT COUNT(*) FROM tenants WHERE plan=$1", plan_id)
        if usage > 0:
            raise HTTPException(status_code=400, detail=f"Cannot delete plan: {usage} tenants are using it")
        
        res = await conn.execute("DELETE FROM plans WHERE id=$1", plan_id)
        if res == "DELETE 0":
            raise HTTPException(status_code=404, detail="Plan not found")
        await _audit(conn, current, "delete_plan", "plan", plan_id, {})
    return {"status": "deleted", "plan_id": plan_id}

@app.get("/superadmin/stats", tags=["SuperAdmin"])
async def sa_platform_stats(days: int = 30, current: dict = Depends(require_superadmin)):
    from datetime import datetime, timedelta, timezone
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_clients = await conn.fetchval("SELECT COUNT(*) FROM tenants")
        active_clients = await conn.fetchval("SELECT COUNT(*) FROM tenants WHERE status='active'")
        mrr = await conn.fetchval("""
            SELECT SUM(s.amount_rupee) 
            FROM subscriptions s 
            JOIN tenants t ON s.tenant_id = t.id 
            WHERE t.status='active' AND s.status='active'
            AND s.amount_rupee > 0
            AND EXISTS (SELECT 1 FROM payments p WHERE p.tenant_id = t.id AND p.status='captured')
        """) or 0
        
        sessions_today = await conn.fetchval("SELECT COUNT(*) FROM sessions WHERE started_at >= CURRENT_DATE")
        leads_today = await conn.fetchval("SELECT COUNT(*) FROM leads WHERE created_at >= CURRENT_DATE")
        open_tickets = await conn.fetchval("SELECT COUNT(*) FROM tickets WHERE status='open' OR status='new'")

        td = timedelta(days=days)
        
        daily_sessions_rows = await conn.fetch("""
            SELECT TO_CHAR(DATE_TRUNC('day', started_at), 'YYYY-MM-DD') as day, COUNT(*) as count
            FROM sessions
            WHERE started_at >= NOW() - $1::interval
            GROUP BY day ORDER BY day
        """, td)
        
        daily_revenue_rows = await conn.fetch("""
            SELECT TO_CHAR(DATE_TRUNC('day', created_at), 'YYYY-MM-DD') as day, SUM(amount_rupee) as amount
            FROM payments
            WHERE status='captured' AND created_at >= NOW() - $1::interval
            GROUP BY day ORDER BY day
        """, td)
        
        daily_tickets_rows = await conn.fetch("""
            SELECT TO_CHAR(DATE_TRUNC('day', created_at), 'YYYY-MM-DD') as day, COUNT(*) as count
            FROM tickets
            WHERE created_at >= NOW() - $1::interval
            GROUP BY day ORDER BY day
        """, td)

        plan_dist_rows = await conn.fetch("""
            SELECT p.name as plan, COUNT(t.id) as count
            FROM tenants t
            JOIN plans p ON t.plan = p.id
            WHERE t.status='active'
            GROUP BY p.name
        """)

    total_active_plan = sum(r["count"] for r in plan_dist_rows)
    plan_distribution = [
        {
            "plan": r["plan"],
            "count": r["count"],
            "pct": round((r["count"] / total_active_plan * 100)) if total_active_plan > 0 else 0
        }
        for r in plan_dist_rows
    ]

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - td
    date_list = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days + 1)]
    
    sessions_dict = {r["day"]: r["count"] for r in daily_sessions_rows}
    revenue_dict = {r["day"]: r["amount"] for r in daily_revenue_rows}
    tickets_dict = {r["day"]: r["count"] for r in daily_tickets_rows}
    
    filled_sessions = [{"day": d, "sessions": sessions_dict.get(d, 0)} for d in date_list]
    filled_revenue = [{"day": d, "amount": revenue_dict.get(d, 0)} for d in date_list]
    filled_tickets = [{"day": d, "tickets": tickets_dict.get(d, 0)} for d in date_list]

    return {
        "kpis": {
            "total_clients": total_clients,
            "active_clients": active_clients,
            "mrr": mrr,
            "sessions_today": sessions_today,
            "leads_today": leads_today,
            "open_tickets": open_tickets
        },
        "trends": {
            "sessions": filled_sessions,
            "revenue": filled_revenue,
            "tickets": filled_tickets
        },
        "plan_distribution": plan_distribution
    }

@app.post("/superadmin/billing/rollover", tags=["SuperAdmin"])
async def sa_billing_rollover(current: dict = Depends(require_superadmin)):
    """Freeze past-due active billing cycles, mark invoiced, and create a new cycle."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            cycles = await conn.fetch(
                "SELECT id, tenant_id, subscription_id, base_fee_rupee, end_date"
                " FROM billing_cycles"
                " WHERE status='active' AND end_date <= NOW()"
            )

            for c in cycles:
                # Mark previous cycle as invoiced
                await conn.execute(
                    "UPDATE billing_cycles SET status='invoiced', updated_at=NOW() WHERE id=$1",
                    c["id"]
                )
                
                # Start new cycle
                now = datetime.now(timezone.utc)
                y, m = now.year, now.month
                if now.day >= 5:
                    m += 1
                    if m > 12:
                        m = 1
                        y += 1
                new_end = datetime(y, m, 5, tzinfo=timezone.utc)
                
                new_id = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO billing_cycles (id, tenant_id, subscription_id, start_date, end_date, base_fee_rupee, status)"
                    " VALUES ($1, $2, $3, $4, $5, $6, 'active')",
                    new_id, c["tenant_id"], c["subscription_id"], now, new_end, c["base_fee_rupee"]
                )
            
            await _audit(conn, current, "billing_rollover", "system", "all", {"processed": len(cycles)})
            
        return {"status": "success", "processed": len(cycles)}
    except Exception as e:
        logger.error(f"Billing rollover failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))