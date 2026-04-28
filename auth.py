"""
Wins Soft - Leads AI — Authentication  v3.0
=====================================
JWT-based auth for three account types — all config from .env, no config.py.

Account types
  user_auth   → tenant dashboard users   (role: owner | member)
  admin_users → Wins Soft - Leads AI staff           (role: admin | superadmin)

Token payload
  { sub, role, account_type, tenant_id? }

Routes
  POST /auth/login
  POST /auth/logout
  GET  /auth/me
  POST /auth/register/request-otp
  POST /auth/register
  POST /auth/forgot-password
  POST /auth/reset-password
  POST /auth/change-password
"""

from __future__ import annotations

import hashlib
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr

from db import (
    get_pool,
    hash_password,
    verify_password,
    _audit,
    create_tenant_schema,
)
from payments import get_plan

load_dotenv()

try:
    import jwt as pyjwt
except ImportError:
    raise RuntimeError("PyJWT not installed — run: pip install PyJWT")

logger   = logging.getLogger(__name__)
router   = APIRouter(prefix="/auth", tags=["Auth"])
_bearer  = HTTPBearer(auto_error=False)

# ── JWT settings from .env ────────────────────────────────────────────────────
_SECRET    = os.environ.get("SECRET_KEY", "change-me")
if _SECRET == "change-me":
    import warnings
    warnings.warn(
        "⚠️  SECRET_KEY is set to the default 'change-me'. "
        "Set a strong random value in .env for production!",
        stacklevel=2,
    )
_ALGORITHM = "HS256"
_ACCESS_EXP_MIN  = int(os.getenv("JWT_ACCESS_EXPIRE_MINUTES", "60"))  # 24h default
_REFRESH_EXP_DAYS = int(os.getenv("JWT_REFRESH_EXPIRE_DAYS",  "7"))


async def _get_session_hours() -> int:
    """Read jwt_session_hours from platform_settings (falls back to env/24h)."""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT value FROM platform_settings WHERE key='jwt_session_hours'"
            )
            if val:
                return int(val)
    except Exception:
        pass
    return _ACCESS_EXP_MIN // 60 or 24


# ═════════════════════════════════════════════════════════════════════════════
# TOKEN HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _make_token(payload: dict, expires_delta: timedelta) -> str:
    data = payload | {"exp": datetime.utcnow() + expires_delta, "iat": datetime.utcnow()}
    return pyjwt.encode(data, _SECRET, algorithm=_ALGORITHM)


def _decode_token(token: str) -> dict:
    try:
        return pyjwt.decode(token, _SECRET, algorithms=[_ALGORITHM])
    except pyjwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except pyjwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


def _access_token(payload: dict, expire_hours: int | None = None) -> str:
    mins = (expire_hours * 60) if expire_hours else _ACCESS_EXP_MIN
    return _make_token(payload, timedelta(minutes=mins))


# ═════════════════════════════════════════════════════════════════════════════
# AUTH DEPENDENCIES  (imported by main.py)
# ═════════════════════════════════════════════════════════════════════════════

async def get_current_user(
    creds: Optional[HTTPAuthorizationCredentials] = Depends(_bearer),
) -> dict:
    """Decode JWT, verify user still exists and is active."""
    if not creds:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    payload = _decode_token(creds.credentials)
    uid     = payload.get("sub")
    atype   = payload.get("account_type")
    role    = payload.get("role")

    pool = await get_pool()
    async with pool.acquire() as conn:
        if atype == "user":
            row = await conn.fetchrow(
                "SELECT ua.id, ua.name, ua.email, ua.role, ua.status, ua.tenant_id,"
                " t.plan, t.ticket_limit AS tenant_ticket_limit, t.logo_url, t.company,"
                " t.status AS tenant_status, t.suspension_reason, t.domain_verified"
                " FROM user_auth ua"
                " JOIN tenants t ON ua.tenant_id = t.id"
                " WHERE ua.id = $1",
                uid,
            )
        else:
            row = await conn.fetchrow(
                "SELECT id,name,email,role,status,ticket_limit FROM admin_users WHERE id=$1",
                uid,
            )
        if not row or row["status"] not in ("active", "suspended"):
            raise HTTPException(status_code=401, detail="Account not found or inactive")

        # refresh last_active
        tbl = "user_auth" if atype == "user" else "admin_users"
        await conn.execute(
            f"UPDATE {tbl} SET last_active=NOW() WHERE id=$1", uid
        )

    user = dict(row)
    user["account_type"] = atype
    return user


def require_role(*roles: str):
    """Dependency factory — raises 403 if the caller's role is not in `roles`."""
    async def _check(current: dict = Depends(get_current_user)) -> dict:
        if current["role"] not in roles:
            raise HTTPException(
                status_code=403,
                detail=f"Requires role: {' or '.join(roles)}",
            )
        return current
    return _check


# Convenience shorthand deps
require_user       = require_role("owner", "member")
require_admin      = require_role("admin", "superadmin")
require_superadmin = require_role("superadmin")


# ═════════════════════════════════════════════════════════════════════════════
# REQUEST SCHEMAS
# ═════════════════════════════════════════════════════════════════════════════

class LoginReq(BaseModel):
    email:    EmailStr
    password: str
    otp:      Optional[str] = None  # Required after 3 failed attempts


class RegisterOTPReq(BaseModel):
    """Step 1: Send OTP — client sends full registration payload."""
    name:     str
    email:    EmailStr
    password: str
    company:  str = ""
    website:  str = ""
    logo_url: str = ""
    phone:    str = ""
    country_code: str = "+91"


class RegisterReq(BaseModel):
    """Step 2: Verify OTP — client sends only email + OTP."""
    email: EmailStr
    otp:   str


class ForgotPwdReq(BaseModel):
    email: EmailStr


class ResetPwdReq(BaseModel):
    token:       str
    new_password: str


class ChangePwdReq(BaseModel):
    current_password: str
    new_password:     str
    otp:              str


class ChangePwdOTPReq(BaseModel):
    current_password: str


class MeUpdate(BaseModel):
    name: Optional[str] = None


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═════════════════════════════════════════════════════════════════════════════

@router.post("/login")
async def login(req: LoginReq, request: Request):
    """
    Universal login — checks user_auth first, then admin_users.
    After 3 failed attempts in 30 minutes, OTP is required.
    Returns JWT access_token + user info.
    """
    _LOGIN_FAIL_THRESHOLD = 3
    _LOGIN_FAIL_WINDOW_MIN = 30

    client_ip = request.client.host if request.client else "unknown"
    pool = await get_pool()
    async with pool.acquire() as conn:

        # ── Step 0: Check failed attempt count ─────────────────────────────
        fail_count = await conn.fetchval(
            "SELECT COUNT(*) FROM login_attempts"
            " WHERE email=$1 AND purpose='login' AND success=FALSE"
            "   AND created_at > NOW() - INTERVAL '30 minutes'",
            req.email,
        ) or 0

        otp_required = fail_count >= _LOGIN_FAIL_THRESHOLD

        if otp_required:
            if not req.otp:
                # Send OTP and tell client to retry with OTP
                otp_raw = "".join(str(secrets.randbelow(10)) for _ in range(6))
                otp_hash = hashlib.sha256(otp_raw.encode()).hexdigest()

                # Invalidate any previous login OTPs
                await conn.execute(
                    "UPDATE otps SET used=TRUE WHERE email=$1 AND purpose='login_verify' AND used=FALSE",
                    req.email,
                )
                await conn.execute(
                    "INSERT INTO otps (id, email, otp_hash, purpose, expires_at)"
                    " VALUES ($1,$2,$3,'login_verify', NOW() + INTERVAL '5 minutes')",
                    secrets.token_hex(8), req.email, otp_hash,
                )
                try:
                    await request.app.state.leads.send_otp_email(req.email, otp_raw, "Login Verification")
                except Exception as e:
                    logger.error(f"Login OTP email failed for {req.email}: {e}")

                raise HTTPException(
                    status_code=403,
                    detail="Too many failed attempts. OTP sent to your email.",
                    headers={"X-Requires-OTP": "true"},
                )

            # Verify OTP
            otp_hash = hashlib.sha256(req.otp.encode()).hexdigest()
            otp_row = await conn.fetchrow(
                "SELECT id FROM otps"
                " WHERE email=$1 AND otp_hash=$2 AND purpose='login_verify'"
                "   AND used=FALSE AND expires_at > NOW()"
                " ORDER BY created_at DESC LIMIT 1",
                req.email, otp_hash,
            )
            if not otp_row:
                raise HTTPException(status_code=400, detail="Invalid or expired OTP")

            # Mark OTP as used
            await conn.execute("UPDATE otps SET used=TRUE WHERE id=$1", otp_row["id"])

        # ── Step 1: Try tenant dashboard user ──────────────────────────────
        row = await conn.fetchrow(
            "SELECT ua.*, t.plan, t.ticket_limit AS tenant_ticket_limit, t.logo_url, t.company,"
            " t.status AS tenant_status, t.suspension_reason, t.domain_verified"
            " FROM user_auth ua"
            " JOIN tenants t ON ua.tenant_id = t.id"
            " WHERE ua.email = $1 AND ua.status IN ('active', 'suspended')",
            req.email,
        )
        if row and verify_password(req.password, row["password_hash"]):
            # Success — clear failed attempts
            await conn.execute(
                "DELETE FROM login_attempts WHERE email=$1 AND purpose='login'",
                req.email,
            )
            await conn.execute(
                "INSERT INTO login_attempts (email, ip_address, success, purpose)"
                " VALUES ($1, $2, TRUE, 'login')",
                req.email, client_ip,
            )

            payload = {
                "sub":          row["id"],
                "role":         row["role"],
                "account_type": "user",
                "tenant_id":    row["tenant_id"],
                "plan":         row["plan"],
            }
            user_out = {
                "id":           row["id"],
                "name":         row["name"],
                "email":        row["email"],
                "role":         row["role"],
                "account_type": "user",
                "tenant_id":    row["tenant_id"],
                "plan":         row["plan"],
                "ticket_limit": row["tenant_ticket_limit"],
                "logo_url":     row["logo_url"],
                "company":      row["company"],
                "tenant_status":     row["tenant_status"],
                "suspension_reason": row.get("suspension_reason"),
                "domain_verified":   row.get("domain_verified", False),
            }
            await conn.execute(
                "UPDATE user_auth SET last_active=NOW() WHERE id=$1", row["id"]
            )
            session_hrs = await _get_session_hours()
            return {"access_token": _access_token(payload, session_hrs), "user": user_out}

        # ── Step 2: Try admin / superadmin ─────────────────────────────────
        row = await conn.fetchrow(
            "SELECT * FROM admin_users WHERE email=$1 AND status='active'",
            req.email,
        )
        if row and verify_password(req.password, row["password_hash"]):
            # Success — clear failed attempts
            await conn.execute(
                "DELETE FROM login_attempts WHERE email=$1 AND purpose='login'",
                req.email,
            )
            await conn.execute(
                "INSERT INTO login_attempts (email, ip_address, success, purpose)"
                " VALUES ($1, $2, TRUE, 'login')",
                req.email, client_ip,
            )

            payload = {
                "sub":          row["id"],
                "role":         row["role"],
                "account_type": "admin",
            }
            user_out = {
                "id":           row["id"],
                "name":         row["name"],
                "email":        row["email"],
                "role":         row["role"],
                "account_type": "admin",
                "ticket_limit": row["ticket_limit"],
            }
            await conn.execute(
                "UPDATE admin_users SET last_active=NOW() WHERE id=$1", row["id"]
            )
            session_hrs = await _get_session_hours()
            return {"access_token": _access_token(payload, session_hrs), "user": user_out}

        # ── Step 3: Failed login — record attempt ──────────────────────────
        await conn.execute(
            "INSERT INTO login_attempts (email, ip_address, success, purpose)"
            " VALUES ($1, $2, FALSE, 'login')",
            req.email, client_ip,
        )

    raise HTTPException(status_code=401, detail="Invalid email or password")


@router.post("/logout")
async def logout(_: dict = Depends(get_current_user)):
    # Stateless JWT — client discards token.
    # For token-blacklisting add a revoked_tokens table and check it in get_current_user.
    return {"status": "logged_out"}


@router.get("/me")
async def me(current: dict = Depends(get_current_user)):
    return current


@router.put("/me")
async def update_me(body: MeUpdate, current: dict = Depends(get_current_user)):
    """Update personal profile (currently just name)."""
    if not body.name:
        raise HTTPException(status_code=400, detail="Name is required")
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        tbl = "user_auth" if current["account_type"] == "user" else "admin_users"
        await conn.execute(
            f"UPDATE {tbl} SET name=$1, updated_at=NOW() WHERE id=$2",
            body.name, current["id"]
        )
    return {"status": "updated", "name": body.name}


import re
import httpx
from fastapi import BackgroundTasks

async def scrape_website_background(tenant_id: str, website: str, company: str):
    logger.info(f"Background scrape for new tenant {tenant_id}: {website}")
    if not website.startswith("http"):
        website = f"https://{website}"
    
    # Simple logic using AI Backend to scrape homepage, find about us, and extract
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post("http://127.0.0.1:8001/internal/scrape/single", json={"url": website})
            if resp.status_code == 200:
                html_text = resp.json().get("content", "")
                # Extract About Us link (heuristic)
                about_match = re.search(r'href=["\']([^"\']*about[^"\']*)["\']', html_text, re.IGNORECASE)
                if about_match:
                    about_url = about_match.group(1)
                    if not about_url.startswith("http"):
                        about_url = website.rstrip("/") + "/" + about_url.lstrip("/")
                    logger.info(f"Found About Us URL: {about_url}")
                    about_resp = await client.post("http://127.0.0.1:8001/internal/scrape/single", json={"url": about_url})
                    if about_resp.status_code == 200:
                        about_text = about_resp.json().get("content", "")
                        # Try to extract company profile using AI Backend's prompt capability (simulated here)
                        # We will save the raw text into kb_company_data for the tenant, under 'introduction'
                        pool = await get_pool()
                        async with pool.acquire() as conn:
                            await conn.execute(
                                "INSERT INTO kb_company_data (id, tenant_id, section, content, source_url)"
                                " VALUES ($1, $2, $3, $4, $5)",
                                secrets.token_hex(8), tenant_id, "introduction", about_text[:5000], about_url
                            )
                        logger.info("Saved About Us data to KB")
    except Exception as e:
        logger.error(f"Error scraping website for {tenant_id}: {e}")

@router.post("/register/request-otp", status_code=200)
async def register_request_otp(req: RegisterOTPReq, request: Request):
    """
    Step 1 — validate details, hash password, store payload + OTP in `otps` table.
    Send OTP to email. Client then calls POST /auth/register with email+otp.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        if await conn.fetchrow("SELECT id FROM user_auth WHERE email=$1", req.email):
            raise HTTPException(status_code=409, detail="Email already registered")

        # Invalidate any previous unused registration OTPs for this email
        await conn.execute(
            "UPDATE otps SET used=TRUE WHERE email=$1 AND purpose='registration' AND used=FALSE",
            req.email,
        )

        otp_raw = "".join(str(secrets.randbelow(10)) for _ in range(6))
        otp_hash = hashlib.sha256(otp_raw.encode()).hexdigest()

        # Store full registration payload in JSONB
        import json
        payload = json.dumps({
            "name":          req.name,
            "company":       req.company or req.name,
            "website":       req.website,
            "phone":         req.phone or "",
            "country_code":  req.country_code or "+91",
            "password_hash": hash_password(req.password),
            "logo_url":      req.logo_url or "",
        })

        await conn.execute(
            "INSERT INTO otps"
            " (id, email, otp_hash, purpose, payload, expires_at)"
            " VALUES ($1,$2,$3,'registration',$4, NOW() + INTERVAL '5 minutes')",
            secrets.token_hex(8), req.email, otp_hash, payload,
        )

    # Send OTP email
    try:
        await request.app.state.leads.send_otp_email(req.email, otp_raw, "Registration")
    except Exception as e:
        logger.error(f"OTP email send failed for {req.email}: {e}")

    return {"message": "OTP sent to your email", "expires_in": 300}


@router.post("/register", status_code=201)
async def register(req: RegisterReq, request: Request, bg_tasks: BackgroundTasks):
    """
    Step 2 — verify OTP, create tenant + owner user.
    Payload is read from otps.payload JSONB (client only sends email + OTP).
    """
    otp_hash = hashlib.sha256(req.otp.encode()).hexdigest()

    pool = await get_pool()
    async with pool.acquire() as conn:
        # 1. Look up valid registration OTP
        otp_row = await conn.fetchrow(
            "SELECT * FROM otps"
            " WHERE email=$1 AND otp_hash=$2 AND purpose='registration'"
            "   AND used=FALSE AND expires_at > NOW()"
            " ORDER BY created_at DESC LIMIT 1",
            req.email, otp_hash,
        )
        if not otp_row:
            raise HTTPException(status_code=400, detail="Invalid or expired OTP")

        # 2. Check not already registered (race condition guard)
        if await conn.fetchrow("SELECT id FROM user_auth WHERE email=$1", req.email):
            raise HTTPException(status_code=409, detail="Email already registered")

        # 3. Read payload from OTP row's JSONB
        data          = otp_row["payload"] if isinstance(otp_row["payload"], dict) else __import__("json").loads(otp_row["payload"])
        name          = data["name"]
        company       = data["company"]
        website       = data["website"]
        phone         = data["phone"]
        country_code  = data.get("country_code", "+91")
        password_hash = data["password_hash"]
        logo_url      = data["logo_url"]

        # 4. Generic email check
        email_domain = req.email.split("@")[1].lower()
        generic_domains_str = await conn.fetchval(
            "SELECT value FROM platform_settings WHERE key='generic_email_domains'"
        ) or ""
        generic_domains = [d.strip().lower() for d in generic_domains_str.split(",") if d.strip()]
        is_generic = email_domain in generic_domains

        tenant_status = "suspended" if is_generic else "active"
        suspension_reason = "generic_email" if is_generic else None

        # 5. Domain auto-verify: compare email domain with website domain
        domain_verified = False
        if website:
            website_domain = website.split("://")[-1].split("/")[0].split(":")[0].lower()
            website_domain = website_domain.replace("www.", "")
            if email_domain == website_domain:
                domain_verified = True

        # 6. Get plan config
        cfg = await get_plan("trial")
        if not cfg:
            cfg = {"ticket_limit": 2, "domain_limit": 1}

        # 7. Get trial duration from platform_settings
        trial_days_str = await conn.fetchval(
            "SELECT value FROM platform_settings WHERE key='trial_duration_days'"
        ) or "7"
        trial_days = int(trial_days_str)

        async with conn.transaction():
            tid = secrets.token_hex(8)
            tlimit = cfg.get("ticket_limit", 2)
            c_name = company or name
            w_slug = re.sub(r'[^a-z0-9]+', '-', c_name.lower()).strip('-') + "-" + secrets.token_hex(2)
            w_secret = secrets.token_hex(32)

            await conn.execute(
                "INSERT INTO tenants"
                " (id,name,email,company,domain,logo_url,phone,country_code,plan,status,"
                "  ticket_limit,widget_slug,widget_secret,domain_verified,suspension_reason)"
                " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'trial',$9,$10,$11,$12,$13,$14)",
                tid, name, req.email, c_name, website, logo_url,
                phone, country_code, tenant_status, tlimit, w_slug, w_secret,
                domain_verified, suspension_reason,
            )

            # Create subscription with trial period
            now = datetime.utcnow()
            end = now + timedelta(days=trial_days)
            await conn.execute(
                "INSERT INTO subscriptions"
                " (id,tenant_id,plan,status,amount_rupee,"
                "  current_period_start,current_period_end)"
                " VALUES ($1,$2,'trial','active',0,$3,$4)",
                secrets.token_hex(8), tid, now, end,
            )

            # Create tenant schema and insert widget_configs there
            await create_tenant_schema(conn, tid)
            await conn.execute(
                f'INSERT INTO "t_{tid}".widget_configs (tenant_id,name,notification_email,logo_url)'
                " VALUES ($1,$2,$3,$4)",
                tid, c_name, req.email, logo_url,
            )
            
            # Initialize tenant_stats
            await conn.execute(
                "INSERT INTO tenant_stats (tenant_id) VALUES ($1)"
                " ON CONFLICT (tenant_id) DO NOTHING", tid
            )

            # Seed signup domain into tenant_domains (CORS whitelist)
            if website:
                signup_domain = website.split("://")[-1].split("/")[0].split(":")[0].lower()
                signup_domain = signup_domain.replace("www.", "")
                if signup_domain:
                    await conn.execute(
                        "INSERT INTO tenant_domains (id, tenant_id, domain, added_by, verified)"
                        " VALUES ($1, $2, $3, 'system', $4)"
                        " ON CONFLICT (tenant_id, domain) DO NOTHING",
                        secrets.token_hex(8), tid, signup_domain, domain_verified,
                    )

            uid = secrets.token_hex(8)
            await conn.execute(
                "INSERT INTO user_auth"
                " (id,tenant_id,name,email,phone,country_code,password_hash,role,status)"
                " VALUES ($1,$2,$3,$4,$5,$6,$7,'owner',$8)",
                uid, tid, name, req.email, phone, country_code, password_hash, tenant_status,
            )

            # Mark OTP as used
            await conn.execute(
                "UPDATE otps SET used=TRUE WHERE id=$1", otp_row["id"]
            )

            await _audit(conn, {"id": uid, "role": "owner"}, "register", "tenant", tid,
                         {"email": req.email, "plan": "trial", "status": tenant_status},
                         tenant_id=tid)

    # Background scrape
    if website:
        bg_tasks.add_task(scrape_website_background, tid, website, c_name)

    # Return JWT so user can log in immediately
    payload = {
        "sub": uid, "role": "owner", "account_type": "user",
        "tenant_id": tid, "plan": "trial",
    }
    user_out = {
        "id": uid, "name": name, "email": req.email,
        "role": "owner", "account_type": "user",
        "tenant_id": tid, "plan": "trial",
        "ticket_limit": tlimit,
        "tenant_status": tenant_status,
        "suspension_reason": suspension_reason,
        "domain_verified": domain_verified,
    }
    session_hrs = await _get_session_hours()
    return {
        "access_token": _access_token(payload, session_hrs),
        "user": user_out,
    }


@router.post("/forgot-password")
async def forgot_password(req: ForgotPwdReq, request: Request):
    """
    Generate a password-reset OTP and send it via email.
    Locked after 2 attempts per day — admin must unlock via ticket.
    """
    _FORGOT_PWD_DAILY_LIMIT = 2
    client_ip = request.client.host if request.client else "unknown"

    pool = await get_pool()
    async with pool.acquire() as conn:
        # ── Lockout check: count today's forgot-password attempts ──────────
        today_count = await conn.fetchval(
            "SELECT COUNT(*) FROM login_attempts"
            " WHERE email=$1 AND purpose='forgot_password'"
            "   AND created_at >= CURRENT_DATE",
            req.email,
        ) or 0

        if today_count >= _FORGOT_PWD_DAILY_LIMIT:
            raise HTTPException(
                status_code=429,
                detail="Password reset locked for today. Please raise a support ticket from your company dashboard.",
            )

        # Check user_auth first, then admin_users
        user_row = await conn.fetchrow(
            "SELECT id,'user' AS utype FROM user_auth WHERE email=$1 AND status='active'",
            req.email,
        )
        if not user_row:
            user_row = await conn.fetchrow(
                "SELECT id,'admin' AS utype FROM admin_users WHERE email=$1 AND status='active'",
                req.email,
            )
        if not user_row:
            # Don't reveal whether the email exists — but still record the attempt
            await conn.execute(
                "INSERT INTO login_attempts (email, ip_address, success, purpose)"
                " VALUES ($1, $2, FALSE, 'forgot_password')",
                req.email, client_ip,
            )
            return {"message": "If that email is registered, a reset link has been sent."}

        # Record this forgot-password attempt
        await conn.execute(
            "INSERT INTO login_attempts (email, ip_address, success, purpose)"
            " VALUES ($1, $2, FALSE, 'forgot_password')",
            req.email, client_ip,
        )

        # Invalidate any existing unused tokens for this user
        await conn.execute(
            "UPDATE password_reset_tokens SET used=TRUE "
            "WHERE user_id=$1 AND user_type=$2 AND used=FALSE",
            user_row["id"], user_row["utype"]
        )

        # Generate 6-digit numeric OTP
        otp = "".join(secrets.choice("0123456789") for _ in range(6))
        token_hash = hashlib.sha256(otp.encode()).hexdigest()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

        await conn.execute(
            "INSERT INTO password_reset_tokens"
            " (id,user_id,user_type,token_hash,expires_at)"
            " VALUES ($1,$2,$3,$4,$5)"
            " ON CONFLICT (token_hash) DO UPDATE SET "
            "  user_id=EXCLUDED.user_id, user_type=EXCLUDED.user_type, "
            "  expires_at=EXCLUDED.expires_at, used=FALSE, created_at=NOW()",
            secrets.token_hex(8),
            user_row["id"],
            user_row["utype"],
            token_hash,
            expires_at,
        )

    # Send real email with OTP
    await request.app.state.leads.send_otp_email(req.email, otp, "Password Reset")
    logger.info(f"[DEV] OTP for {req.email}: {otp}")
    return {
        "message": "OTP sent (check email)",
    }


@router.post("/reset-password")
async def reset_password(req: ResetPwdReq, request: Request):
    token_hash = hashlib.sha256(req.token.encode()).hexdigest()
    pool       = await get_pool()

    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM password_reset_tokens"
            " WHERE token_hash=$1 AND used=FALSE AND expires_at > $2",
            token_hash, now,
        )
        if not row:
            raise HTTPException(status_code=400, detail="Invalid or expired OTP")

        new_hash = hash_password(req.new_password)
        tbl      = "user_auth" if row["user_type"] == "user" else "admin_users"
        await conn.execute(
            f"UPDATE {tbl} SET password_hash=$1, updated_at=NOW() WHERE id=$2",
            new_hash, row["user_id"],
        )
        await conn.execute(
            "UPDATE password_reset_tokens SET used=TRUE WHERE id=$1", row["id"]
        )

        # Send security alert
        await request.app.state.leads.send_security_alert(row.get("email") or "User", "Password Reset")

    return {"message": "Password updated successfully"}


@router.post("/request-password-change-otp")
async def request_password_change_otp(
    req:     ChangePwdOTPReq,
    request: Request,
    current: dict = Depends(get_current_user),
):
    """
    Verify current password and generate an OTP for changing password.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        tbl = "user_auth" if current["account_type"] == "user" else "admin_users"
        row = await conn.fetchrow(
            f"SELECT password_hash, email FROM {tbl} WHERE id=$1", current["id"]
        )
        if not row or not verify_password(req.current_password, row["password_hash"]):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        # Generate 6-digit OTP
        otp = "".join(secrets.choice("0123456789") for _ in range(6))
        token_hash = hashlib.sha256(otp.encode()).hexdigest()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=10)

        await conn.execute(
            "INSERT INTO password_reset_tokens"
            " (id,user_id,user_type,token_hash,expires_at)"
            " VALUES ($1,$2,$3,$4,$5)",
            secrets.token_hex(8), current["id"], current["account_type"],
            token_hash, expires_at
        )

        await conn.execute(...)

    # Send email
    await request.app.state.leads.send_otp_email(row['email'], otp, "Password Change")
    logger.info(f"[DEV] Password change OTP for {row['email']}: {otp}")
    return {"message": "OTP sent"}


@router.post("/change-password")
async def change_password(
    req:     ChangePwdReq,
    request: Request,
    current: dict = Depends(get_current_user),
):
    token_hash = hashlib.sha256(req.otp.encode()).hexdigest()
    pool       = await get_pool()

    async with pool.acquire() as conn:
        now = datetime.now(timezone.utc)
        # Verify OTP
        otp_row = await conn.fetchrow(
            "SELECT id FROM password_reset_tokens"
            " WHERE token_hash=$1 AND user_id=$2 AND used=FALSE AND expires_at > $3",
            token_hash, current["id"], now
        )
        if not otp_row:
            raise HTTPException(status_code=400, detail="Invalid or expired OTP")

        # Verify current password again (security)
        tbl = "user_auth" if current["account_type"] == "user" else "admin_users"
        row = await conn.fetchrow(
            f"SELECT password_hash FROM {tbl} WHERE id=$1", current["id"]
        )
        if not row or not verify_password(req.current_password, row["password_hash"]):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        # Update password
        await conn.execute(
            f"UPDATE {tbl} SET password_hash=$1, updated_at=NOW() WHERE id=$2",
            hash_password(req.new_password), current["id"],
        )
        
        # Mark OTP as used
        await conn.execute(
            "UPDATE password_reset_tokens SET used=TRUE WHERE id=$1", otp_row["id"]
        )
        
        # Send security alert
        await request.app.state.leads.send_security_alert(current["email"], "Password Changed")
        
        await _audit(conn, current, "change_password", "user", current["id"], {}, tenant_id=current.get("tenant_id"))

    return {"message": "Password changed successfully"}
