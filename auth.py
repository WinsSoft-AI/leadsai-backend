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
import base64
from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, EmailStr

from cryptography.hazmat.primitives.asymmetric import rsa, padding as asym_padding
from cryptography.hazmat.primitives import serialization, hashes

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

# ── RSA keypair for password encryption in transit ─────────────────────────────
_RSA_PRIVATE_KEY = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
)
_RSA_PUBLIC_KEY = _RSA_PRIVATE_KEY.public_key()
_RSA_PUBLIC_PEM = _RSA_PUBLIC_KEY.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode()

logger.info("RSA keypair generated for password encryption in transit")


def decrypt_password(encrypted_b64: str) -> str:
    """Decrypt an RSA-OAEP encrypted password sent from the frontend."""
    try:
        ciphertext = base64.b64decode(encrypted_b64)
        plaintext = _RSA_PRIVATE_KEY.decrypt(
            ciphertext,
            asym_padding.OAEP(
                mgf=asym_padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None,
            ),
        )
        return plaintext.decode("utf-8")
    except Exception as e:
        logger.error(f"Password decryption failed: {e}")
        raise HTTPException(status_code=400, detail="Invalid encrypted password")


@router.get("/public-key")
async def get_public_key():
    """Return the RSA public key for client-side password encryption."""
    return {"public_key": _RSA_PUBLIC_PEM}

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
                " t.status AS tenant_status, t.suspension_reason, t.domain_verified,"
                " t.onboarding_completed, t.terms_accepted"
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
    Universal login with graduated security (tenant users only):
      Attempt 1: normal 401
      Attempt 2: 401 + suggest forgot-password
      Attempt 3 (wrong password): LOCK account (status='locked')
      Attempt 3 (correct password): require OTP
        OTP correct → login success
        OTP wrong after 2 resends → LOCK account

    Admins/superadmins: unlimited attempts, no OTP, no locking.
    Suspended users: allowed to login (dashboard overlay handles restrictions).
    Locked users: login blocked entirely (HTTP 423).
    """
    client_ip = request.client.host if request.client else "unknown"
    password = decrypt_password(req.password)
    pool = await get_pool()
    async with pool.acquire() as conn:

        # ── Pre-check account status ───────────────────────────────────────
        ua_row = await conn.fetchrow(
            "SELECT id, status, name, tenant_id FROM user_auth WHERE email=$1", req.email
        )
        admin_row = await conn.fetchrow(
            "SELECT id, status FROM admin_users WHERE email=$1", req.email
        )

        # Locked → 423 (tenant users only, never admins)
        if ua_row and ua_row["status"] == "locked":
            raise HTTPException(
                status_code=423,
                detail="Account locked due to multiple failed login attempts. Please raise a support ticket to unlock your account.",
            )
        # Admin inactive → 403
        if admin_row and admin_row["status"] != "active":
            raise HTTPException(
                status_code=403,
                detail="Account suspended. Contact a superadmin to reactivate.",
            )
        # NOTE: suspended tenant users are ALLOWED to login — the dashboard
        # overlay will show the suspension reason and restrict access.

        # ── Count recent failures (30-min window) — tenant users only ──────
        # Admin/superadmin accounts are exempt from fail counting entirely.
        is_admin_account = admin_row is not None
        fail_count = 0
        if not is_admin_account:
            fail_count = await conn.fetchval(
                "SELECT COUNT(*) FROM login_attempts"
                " WHERE email=$1 AND purpose='login' AND success=FALSE"
                "   AND created_at > NOW() - INTERVAL '30 minutes'",
                req.email,
            ) or 0

        # ── OTP-required phase (fail_count >= 2, tenant users only) ────────
        if fail_count >= 2:
            # First verify that the password is actually correct
            # before sending OTP. Wrong password at this stage → LOCK.
            matched_row, matched_type = None, None
            user_row = await conn.fetchrow(
                "SELECT ua.*, t.plan, t.ticket_limit AS tenant_ticket_limit,"
                " t.logo_url, t.company, t.status AS tenant_status,"
                " t.suspension_reason, t.domain_verified"
                " FROM user_auth ua JOIN tenants t ON ua.tenant_id = t.id"
                " WHERE ua.email = $1 AND ua.status IN ('active','suspended')",
                req.email,
            )
            if user_row and verify_password(password, user_row["password_hash"]):
                matched_row, matched_type = user_row, "user"

            if not matched_row:
                # 3rd+ wrong password → LOCK tenant user account
                await conn.execute(
                    "INSERT INTO login_attempts (email, ip_address, success, purpose)"
                    " VALUES ($1, $2, FALSE, 'login')",
                    req.email, client_ip,
                )
                if ua_row:
                    await conn.execute(
                        "UPDATE user_auth SET status='locked', updated_at=NOW()"
                        " WHERE email=$1", req.email
                    )
                    await _audit(conn, {"id": "system", "role": "system"}, "lock_account", "user", ua_row["id"], {"reason": "too_many_failed_logins", "user_name": ua_row["name"], "user_email": req.email}, tenant_id=ua_row["tenant_id"])
                    raise HTTPException(
                        status_code=423,
                        detail="Account locked due to multiple failed login attempts. Please raise a support ticket to unlock your account.",
                    )
                raise HTTPException(
                    status_code=401,
                    detail="Invalid email or password",
                )

            # Password is correct — now require OTP
            if not req.otp:
                # Send OTP
                otp_raw = "".join(str(secrets.randbelow(10)) for _ in range(6))
                otp_hash = hashlib.sha256(otp_raw.encode()).hexdigest()

                # Invalidate any previous login OTPs
                await conn.execute(
                    "UPDATE otps SET used=TRUE"
                    " WHERE email=$1 AND purpose='login_verify' AND used=FALSE",
                    req.email,
                )
                await conn.execute(
                    "INSERT INTO otps (id, email, otp_hash, purpose, expires_at)"
                    " VALUES ($1,$2,$3,'login_verify', NOW() + INTERVAL '5 minutes')",
                    secrets.token_hex(8), req.email, otp_hash,
                )
                try:
                    await request.app.state.leads.send_otp_email(
                        req.email, otp_raw, "Login Verification"
                    )
                except Exception as e:
                    logger.error(f"Login OTP email failed for {req.email}: {e}")

                return JSONResponse(
                    status_code=428,
                    content={"detail": "OTP sent to your email for verification.", "requires_otp": True},
                    headers={"X-Requires-OTP": "true"},
                )

            # Verify OTP
            otp_hash = hashlib.sha256(req.otp.encode()).hexdigest()
            otp_row = await conn.fetchrow(
                "SELECT id, resend_count FROM otps"
                " WHERE email=$1 AND purpose='login_verify'"
                "   AND used=FALSE AND expires_at > NOW()"
                " ORDER BY created_at DESC LIMIT 1",
                req.email,
            )
            if not otp_row:
                raise HTTPException(status_code=400, detail="Invalid or expired OTP")

            if otp_row and hashlib.sha256(req.otp.encode()).hexdigest() != (
                await conn.fetchval(
                    "SELECT otp_hash FROM otps WHERE id=$1", otp_row["id"]
                )
            ):
                # Wrong OTP — increment resend count
                current_resends = otp_row["resend_count"]
                if current_resends >= 2:
                    # 2 resends exhausted → LOCK tenant user account
                    await conn.execute(
                        "UPDATE otps SET used=TRUE WHERE id=$1", otp_row["id"]
                    )
                    if ua_row:
                        await conn.execute(
                            "UPDATE user_auth SET status='locked', updated_at=NOW()"
                            " WHERE email=$1", req.email
                        )
                        await _audit(conn, {"id": "system", "role": "system"}, "lock_account", "user", ua_row["id"], {"reason": "too_many_failed_otps", "user_name": ua_row["name"], "user_email": req.email}, tenant_id=ua_row["tenant_id"])
                        raise HTTPException(
                            status_code=423,
                            detail="Account locked after multiple failed OTP attempts. Please raise a support ticket to unlock your account.",
                        )
                    raise HTTPException(
                        status_code=400,
                        detail="Too many failed OTP attempts. Please try logging in again.",
                    )

                # Allow retry — bump resend count
                await conn.execute(
                    "UPDATE otps SET resend_count = resend_count + 1 WHERE id=$1",
                    otp_row["id"],
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid OTP. {2 - current_resends} resend(s) remaining.",
                    headers={"X-Requires-OTP": "true"},
                )

            # OTP hash matches — verify
            stored_hash = await conn.fetchval(
                "SELECT otp_hash FROM otps WHERE id=$1", otp_row["id"]
            )
            if otp_hash != stored_hash:
                current_resends = otp_row["resend_count"]
                if current_resends >= 2:
                    await conn.execute(
                        "UPDATE otps SET used=TRUE WHERE id=$1", otp_row["id"]
                    )
                    if ua_row:
                        await conn.execute(
                            "UPDATE user_auth SET status='locked', updated_at=NOW()"
                            " WHERE email=$1", req.email
                        )
                        await _audit(conn, {"id": "system", "role": "system"}, "lock_account", "user", ua_row["id"], {"reason": "too_many_failed_otps", "user_name": ua_row["name"], "user_email": req.email}, tenant_id=ua_row["tenant_id"])
                        raise HTTPException(
                            status_code=423,
                            detail="Account locked after multiple failed OTP attempts. Please raise a support ticket to unlock your account.",
                        )
                    raise HTTPException(
                        status_code=400,
                        detail="Too many failed OTP attempts. Please try logging in again.",
                    )
                await conn.execute(
                    "UPDATE otps SET resend_count = resend_count + 1 WHERE id=$1",
                    otp_row["id"],
                )
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid OTP. {2 - current_resends} resend(s) remaining.",
                    headers={"X-Requires-OTP": "true"},
                )

            # OTP correct — mark used, clear attempts, login
            await conn.execute("UPDATE otps SET used=TRUE WHERE id=$1", otp_row["id"])
            await conn.execute(
                "DELETE FROM login_attempts WHERE email=$1 AND purpose='login'",
                req.email,
            )

            # Build response for matched user
            if matched_type == "user":
                payload = {
                    "sub": matched_row["id"], "role": matched_row["role"],
                    "account_type": "user", "tenant_id": matched_row["tenant_id"],
                    "plan": matched_row["plan"],
                }
                user_out = {
                    "id": matched_row["id"], "name": matched_row["name"],
                    "email": matched_row["email"], "role": matched_row["role"],
                    "account_type": "user", "tenant_id": matched_row["tenant_id"],
                    "plan": matched_row["plan"],
                    "ticket_limit": matched_row["tenant_ticket_limit"],
                    "logo_url": matched_row["logo_url"],
                    "company": matched_row["company"],
                    "tenant_status": matched_row["tenant_status"],
                    "suspension_reason": matched_row.get("suspension_reason"),
                    "domain_verified": matched_row.get("domain_verified", False),
                }
            else:
                payload = {
                    "sub": matched_row["id"], "role": matched_row["role"],
                    "account_type": "admin",
                }
                user_out = {
                    "id": matched_row["id"], "name": matched_row["name"],
                    "email": matched_row["email"], "role": matched_row["role"],
                    "account_type": "admin", "ticket_limit": matched_row["ticket_limit"],
                }

            await conn.execute(
                f"UPDATE {'user_auth' if matched_type == 'user' else 'admin_users'}"
                " SET last_active=NOW() WHERE id=$1", matched_row["id"]
            )
            session_hrs = await _get_session_hours()
            return {
                "access_token": _access_token(payload, session_hrs),
                "user": user_out,
            }

        # ── Normal login flow (fail_count < 2) ─────────────────────────────

        # Try tenant dashboard user (active OR suspended — both can login)
        row = await conn.fetchrow(
            "SELECT ua.*, t.plan, t.ticket_limit AS tenant_ticket_limit, t.logo_url, t.company,"
            " t.status AS tenant_status, t.suspension_reason, t.domain_verified"
            " FROM user_auth ua"
            " JOIN tenants t ON ua.tenant_id = t.id"
            " WHERE ua.email = $1 AND ua.status IN ('active','suspended')",
            req.email,
        )
        if row and verify_password(password, row["password_hash"]):
            # Success — clear failed attempts
            await conn.execute(
                "DELETE FROM login_attempts WHERE email=$1 AND purpose='login'",
                req.email,
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
            return {
                "access_token": _access_token(payload, session_hrs),
                "user": user_out,
            }

        # Try admin / superadmin
        row = await conn.fetchrow(
            "SELECT * FROM admin_users WHERE email=$1 AND status='active'",
            req.email,
        )
        if row and verify_password(password, row["password_hash"]):
            await conn.execute(
                "DELETE FROM login_attempts WHERE email=$1 AND purpose='login'",
                req.email,
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
            return {
                "access_token": _access_token(payload, session_hrs),
                "user": user_out,
            }

        # ── Failed login — record attempt ──────────────────────────────────
        await conn.execute(
            "INSERT INTO login_attempts (email, ip_address, success, purpose)"
            " VALUES ($1, $2, FALSE, 'login')",
            req.email, client_ip,
        )
        new_fail_count = fail_count + 1

        # Graduated response
        headers = {}
        if new_fail_count >= 2:
            headers["X-Suggest-Forgot"] = "true"

    raise HTTPException(
        status_code=401,
        detail="Invalid email or password",
        headers=headers if headers else None,
    )


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
            "password_hash": hash_password(decrypt_password(req.password)),
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
                "  ticket_limit,widget_slug,widget_secret,domain_verified,suspension_reason,terms_accepted)"
                " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'trial',$9,$10,$11,$12,$13,$14,TRUE)",
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
    Password reset request with graduated lockout:
      - 30-day cooldown since last password change
      - Attempt 1: send OTP, 2 resends, then lock for 6 hours
      - Attempt 2 (after 6hr): send OTP, 2 resends, then SUSPEND account
    """
    client_ip = request.client.host if request.client else "unknown"
    pool = await get_pool()

    async with pool.acquire() as conn:
        # ── 30-day cooldown check ──────────────────────────────────────────
        pwd_changed = await conn.fetchval(
            "SELECT password_changed_at FROM user_auth WHERE email=$1",
            req.email,
        )
        if not pwd_changed:
            pwd_changed = await conn.fetchval(
                "SELECT updated_at FROM admin_users WHERE email=$1",
                req.email,
            )
        if pwd_changed:
            days_since = (datetime.now(timezone.utc) - pwd_changed).days
            if days_since < 30:
                raise HTTPException(
                    status_code=429,
                    detail=f"Password can only be changed once every 30 days. "
                           f"Try again in {30 - days_since} day(s).",
                )

        # ── Count forgot-password attempts (last 24 hours) ────────────────
        attempts = await conn.fetch(
            "SELECT created_at FROM login_attempts"
            " WHERE email=$1 AND purpose='forgot_password'"
            "   AND created_at >= NOW() - INTERVAL '24 hours'"
            " ORDER BY created_at ASC",
            req.email,
        )
        attempt_count = len(attempts)

        if attempt_count >= 2:
            # Check if 2nd attempt was also exhausted → SUSPEND
            raise HTTPException(
                status_code=403,
                detail="Account suspended. Too many password reset attempts. Contact administrator.",
            )

        if attempt_count == 1:
            # Check 6-hour lockout from first attempt
            first_attempt_time = attempts[0]["created_at"]
            hours_elapsed = (datetime.now(timezone.utc) - first_attempt_time).total_seconds() / 3600
            if hours_elapsed < 6:
                remaining_hrs = int(6 - hours_elapsed) + 1
                raise HTTPException(
                    status_code=429,
                    detail=f"Password reset locked. Try again in ~{remaining_hrs} hour(s).",
                )

        # ── Look up user ───────────────────────────────────────────────────
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
            # Don't reveal whether the email exists — but still record
            await conn.execute(
                "INSERT INTO login_attempts (email, ip_address, success, purpose)"
                " VALUES ($1, $2, FALSE, 'forgot_password')",
                req.email, client_ip,
            )
            return {"message": "If that email is registered, a reset link has been sent."}

        # ── Record attempt ─────────────────────────────────────────────────
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
        "attempt": attempt_count + 1,
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

        new_hash = hash_password(decrypt_password(req.new_password))
        tbl      = "user_auth" if row["user_type"] == "user" else "admin_users"
        await conn.execute(
            f"UPDATE {tbl} SET password_hash=$1, password_changed_at=NOW(),"
            " updated_at=NOW() WHERE id=$2",
            new_hash, row["user_id"],
        )
        await conn.execute(
            "UPDATE password_reset_tokens SET used=TRUE WHERE id=$1", row["id"]
        )
        # Clear forgot-password lockout attempts on success
        user_email = await conn.fetchval(
            f"SELECT email FROM {tbl} WHERE id=$1", row["user_id"]
        )
        if user_email:
            await conn.execute(
                "DELETE FROM login_attempts WHERE email=$1 AND purpose='forgot_password'",
                user_email,
            )

        # Send security alert
        await request.app.state.leads.send_security_alert(user_email or "User", "Password Reset")

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
        if not row or not verify_password(decrypt_password(req.current_password), row["password_hash"]):
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

        await conn.execute(
            "UPDATE password_reset_tokens SET used=TRUE"
            " WHERE user_id=$1 AND used=FALSE",
            current["id"],
        )

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
        if not row or not verify_password(decrypt_password(req.current_password), row["password_hash"]):
            raise HTTPException(status_code=400, detail="Current password is incorrect")

        # Update password
        await conn.execute(
            f"UPDATE {tbl} SET password_hash=$1, password_changed_at=NOW(),"
            " updated_at=NOW() WHERE id=$2",
            hash_password(decrypt_password(req.new_password)), current["id"],
        )
        
        # Mark OTP as used
        await conn.execute(
            "UPDATE password_reset_tokens SET used=TRUE WHERE id=$1", otp_row["id"]
        )
        
        # Send security alert
        await request.app.state.leads.send_security_alert(current["email"], "Password Changed")
        
        await _audit(conn, current, "change_password", "user", current["id"], {}, tenant_id=current.get("tenant_id"))

    return {"message": "Password changed successfully"}


# ═════════════════════════════════════════════════════════════════════════════
# LOCKED ACCOUNT TICKET  (unauthenticated — only for locked tenant users)
# ═════════════════════════════════════════════════════════════════════════════

class LockedTicketReq(BaseModel):
    email: str
    context: str = ""


@router.post("/locked-ticket", status_code=201)
async def locked_ticket(req: LockedTicketReq):
    """
    Unauthenticated endpoint for LOCKED tenant users to raise a support ticket.
    Only works when the user_auth row for the email has status='locked'.
    Limited to 1 open locked-ticket per email to prevent abuse.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 1. Verify user exists and is locked
        row = await conn.fetchrow(
            "SELECT ua.id, ua.tenant_id, ua.name FROM user_auth ua"
            " WHERE ua.email=$1 AND ua.status='locked'",
            req.email,
        )
        if not row:
            raise HTTPException(
                status_code=404,
                detail="No locked account found for this email.",
            )

        user_id   = row["id"]
        tenant_id = row["tenant_id"]
        user_name = row["name"]

        # 2. Rate limit: max 1 open locked-ticket per email
        existing = await conn.fetchval(
            "SELECT COUNT(*) FROM tickets"
            " WHERE user_id=$1 AND type='locked_account'"
            "   AND status IN ('open','claimed')",
            user_id,
        )
        if existing and existing > 0:
            raise HTTPException(
                status_code=429,
                detail="A support ticket for this account is already open. Our team will contact you shortly.",
            )

        # 3. Create the ticket
        tid = secrets.token_hex(10)
        heading = "Account Locked — Login Issue"
        context = req.context.strip() if req.context else "User account was locked due to multiple failed login attempts."

        async with conn.transaction():
            await conn.execute(
                "INSERT INTO tickets (id,tenant_id,user_id,heading,context,type,priority)"
                " VALUES ($1,$2,$3,$4,$5,'locked_account','high')",
                tid, tenant_id, user_id, heading, context,
            )
            await conn.execute(
                "INSERT INTO ticket_status_log"
                " (ticket_id,changed_by,changer_role,from_status,to_status,note)"
                " VALUES ($1,$2,'user','none','open','Auto-created: locked account')",
                tid, user_id,
            )

    return {
        "ticket_id": tid,
        "message": f"Support ticket created successfully. Our team will review your account and contact you at {req.email}.",
    }
