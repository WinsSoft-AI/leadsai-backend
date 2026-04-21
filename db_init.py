"""
Leads AI — Database Initialization  v3.1  (PostgreSQL / asyncpg)
=====================================================================
CLI usage
---------
python db_init.py              # create tables + seed demo data
python db_init.py --reset      # drop everything, recreate, seed
python db_init.py --no-seed    # create tables only, skip seed

All settings come from .env — no config.py.

Schema
------
CORE        tenants, subscriptions, payments,
            sessions, leads, ingest_jobs, usage_events, widget_configs

KNOWLEDGE   knowledge_qa, kb_company_data, kb_products

AUTH        user_auth, admin_users, password_reset_tokens, otps

SUPPORT     tickets, ticket_attachments, ticket_messages, ticket_status_log

PLATFORM    platform_settings, audit_log
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import secrets
import sys
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

try:
    import asyncpg
except ImportError:
    logger.error("asyncpg not installed — run: pip install asyncpg")
    sys.exit(1)


# ═════════════════════════════════════════════════════════════════════════════
# DSN helper — strips SQLAlchemy prefix so asyncpg can use it directly
# ═════════════════════════════════════════════════════════════════════════════

def _dsn() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        logger.error("DATABASE_URL is not set in .env")
        sys.exit(1)
    # SQLAlchemy uses postgresql+asyncpg://, asyncpg wants postgresql://
    return raw.replace("postgresql+asyncpg://", "postgresql://") \
               .replace("postgres+asyncpg://",   "postgresql://")


# ═════════════════════════════════════════════════════════════════════════════
# CONNECTION POOL (module-level singleton — shared across the whole app)
# ═════════════════════════════════════════════════════════════════════════════

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Return (or lazily create) the shared asyncpg connection pool."""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=_dsn(),
            min_size=int(os.getenv("DB_POOL_MIN",     "5")),
            max_size=int(os.getenv("DB_POOL_MAX",     "20")),
            command_timeout=float(os.getenv("DB_POOL_TIMEOUT", "30")),
            init=_init_conn,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def _init_conn(conn: asyncpg.Connection) -> None:
    """Register JSONB codec so asyncpg auto-encodes/decodes Python dicts."""
    import json
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


# ═════════════════════════════════════════════════════════════════════════════
# SCHEMA — one CREATE TABLE per string so asyncpg can execute them individually
# ═════════════════════════════════════════════════════════════════════════════

_TABLES: list[str] = [

    # ── plans ─────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS plans (
        id                   TEXT        PRIMARY KEY,
        name                 TEXT        NOT NULL,
        onboarding_fee_rupee INTEGER     NOT NULL DEFAULT 0,
        base_fee_rupee       INTEGER     NOT NULL DEFAULT 0,
        currency             TEXT        NOT NULL DEFAULT 'INR',
        input_token_rate     NUMERIC     NOT NULL DEFAULT 0.0,
        output_token_rate    NUMERIC     NOT NULL DEFAULT 0.0,
        ticket_limit         INTEGER     NOT NULL DEFAULT 2,
        domain_limit         INTEGER     NOT NULL DEFAULT 2,
        description          TEXT,
        razorpay_plan_id     TEXT,
        discount_type        TEXT,
        discount_value       INTEGER,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── tenants ───────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS tenants (
        id               TEXT        PRIMARY KEY,
        name             TEXT        NOT NULL,
        email            TEXT        NOT NULL UNIQUE,
        company          TEXT,
        domain           TEXT,
        logo_url         TEXT,
        phone            TEXT,
        country_code     TEXT        NOT NULL DEFAULT '+91',
        plan             TEXT        NOT NULL DEFAULT 'trial',
        status           TEXT        NOT NULL DEFAULT 'active',
        razorpay_cust_id TEXT,
        ticket_limit     INTEGER     NOT NULL DEFAULT 5,
        widget_slug      TEXT        UNIQUE,
        widget_secret    TEXT,
        widget_secret_rotated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        notification_emails TEXT     NOT NULL DEFAULT '',
        domain_verified  BOOLEAN     NOT NULL DEFAULT FALSE,
        verification_token TEXT,
        suspension_reason TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── subscriptions ─────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS subscriptions (
        id                   TEXT        PRIMARY KEY,
        tenant_id            TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        plan                 TEXT        NOT NULL,
        status               TEXT        NOT NULL DEFAULT 'active',
        razorpay_sub_id      TEXT        UNIQUE,
        razorpay_plan_id     TEXT,
        amount_rupee         INTEGER     NOT NULL DEFAULT 0,
        currency             TEXT        NOT NULL DEFAULT 'INR',
        billing_cycle        TEXT        NOT NULL DEFAULT 'monthly',
        current_period_start TIMESTAMPTZ,
        current_period_end   TIMESTAMPTZ,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── billing_cycles ────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS billing_cycles (
        id                   TEXT        PRIMARY KEY,
        tenant_id            TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        subscription_id      TEXT        NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
        start_date           TIMESTAMPTZ NOT NULL,
        end_date             TIMESTAMPTZ NOT NULL,
        base_fee_rupee       INTEGER     NOT NULL DEFAULT 1000,
        input_tokens_used    BIGINT      NOT NULL DEFAULT 0,
        output_tokens_used   BIGINT      NOT NULL DEFAULT 0,
        invoice_id           TEXT,
        status               TEXT        NOT NULL DEFAULT 'active',
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── payments ──────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS payments (
        id                  TEXT        PRIMARY KEY,
        tenant_id           TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        subscription_id     TEXT        REFERENCES subscriptions(id),
        razorpay_order_id   TEXT,
        razorpay_payment_id TEXT        UNIQUE,
        razorpay_signature  TEXT,
        amount_rupee        INTEGER     NOT NULL,
        currency            TEXT        NOT NULL DEFAULT 'INR',
        status              TEXT        NOT NULL DEFAULT 'pending',
        plan                TEXT,
        payment_method      TEXT,
        error_code          TEXT,
        error_description   TEXT,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── sessions (widget chat sessions) ───────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS sessions (
        id            TEXT        PRIMARY KEY,
        tenant_id     TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        visitor_id    TEXT        NOT NULL,
        language      TEXT        NOT NULL DEFAULT 'en',
        message_count INTEGER     NOT NULL DEFAULT 0,
        pii_collected BOOLEAN     NOT NULL DEFAULT FALSE,
        lead_id       TEXT,
        intent        TEXT,
        sentiment     TEXT,
        chat_history  JSONB       DEFAULT '[]'::jsonb,
        notes         TEXT,
        started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        ended_at      TIMESTAMPTZ,
        last_active   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── session_metadata ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS session_metadata (
        session_id    TEXT        PRIMARY KEY,
        tenant_id     TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        visitor_name  TEXT        NOT NULL DEFAULT 'Guest',
        ip_address    TEXT,
        country       TEXT,
        city          TEXT,
        region        TEXT,
        timezone      TEXT,
        user_agent    TEXT,
        browser       TEXT,
        browser_ver   TEXT,
        os            TEXT,
        os_ver        TEXT,
        device_type   TEXT,
        screen_res    TEXT,
        language      TEXT,
        referrer      TEXT,
        page_url      TEXT,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── leads ─────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS leads (
        id                 TEXT        PRIMARY KEY,
        tenant_id          TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        session_id         TEXT        REFERENCES sessions(id),
        name               TEXT        NOT NULL,
        email              TEXT,
        phone              TEXT,
        canonical_lead_id  TEXT        REFERENCES leads(id),
        intent             TEXT,
        product_interest   TEXT,
        product_quantities JSONB       DEFAULT '{}'::jsonb,
        quality            TEXT        NOT NULL DEFAULT 'warm',
        sentiment          TEXT        NOT NULL DEFAULT 'neutral',
        notes              TEXT,
        email_sent         BOOLEAN     NOT NULL DEFAULT FALSE,
        is_merged          BOOLEAN     NOT NULL DEFAULT FALSE,
        merge_count        INTEGER     NOT NULL DEFAULT 0,
        merged_session_ids TEXT[]      DEFAULT '{}',
        merged_names       TEXT[]      DEFAULT '{}',
        created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── ingest_jobs ───────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ingest_jobs (
        id              TEXT        PRIMARY KEY,
        tenant_id       TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        filename        TEXT        NOT NULL,
        file_size_bytes INTEGER,
        content_type    TEXT,
        status          TEXT        NOT NULL DEFAULT 'processing',
        chunks_indexed  INTEGER     NOT NULL DEFAULT 0,
        error_message   TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        completed_at    TIMESTAMPTZ
    )
    """,

    # ── usage_events ──────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS usage_events (
        id          BIGSERIAL   PRIMARY KEY,
        tenant_id   TEXT        NOT NULL,
        domain      TEXT,
        session_id  TEXT,
        event_type  TEXT        NOT NULL,
        tokens_in   INTEGER     NOT NULL DEFAULT 0,
        tokens_out  INTEGER     NOT NULL DEFAULT 0,
        latency_ms  INTEGER,
        status      TEXT        NOT NULL DEFAULT 'ok',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── widget_configs ────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS widget_configs (
        tenant_id          TEXT        PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
        name               TEXT,
        greeting           TEXT        NOT NULL DEFAULT 'Hi! How can I help you today?',
        primary_color      TEXT        NOT NULL DEFAULT '#2952e3',
        accent_color       TEXT        NOT NULL DEFAULT '#00d4f5',
        secondary_color    TEXT        NOT NULL DEFAULT '#ffffff',
        text_color         TEXT        NOT NULL DEFAULT '#000000',
        bot_text_color     TEXT        NOT NULL DEFAULT '#ffffff',
        user_text_color    TEXT        NOT NULL DEFAULT '#ffffff',
        bg_image_url       TEXT,
        position           TEXT        NOT NULL DEFAULT 'bottom-right',
        logo_url           TEXT,
        proactive_enabled  BOOLEAN     NOT NULL DEFAULT TRUE,
        proactive_delay_s  INTEGER     NOT NULL DEFAULT 30,
        proactive_message  TEXT        NOT NULL DEFAULT 'Hi there! Need help? Chat with us!',
        pii_after_messages INTEGER     NOT NULL DEFAULT 3,
        tts_enabled        BOOLEAN     NOT NULL DEFAULT TRUE,
        stt_enabled        BOOLEAN     NOT NULL DEFAULT TRUE,
        cv_search_enabled  BOOLEAN     NOT NULL DEFAULT TRUE,
        notification_email TEXT,
        languages          TEXT        NOT NULL DEFAULT 'en',
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── user_auth (tenant dashboard logins) ───────────────────────────────────
    # role: owner (full access) | member (limited)
    """
    CREATE TABLE IF NOT EXISTS user_auth (
        id            TEXT        PRIMARY KEY,
        tenant_id     TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        name          TEXT        NOT NULL,
        email         TEXT        NOT NULL UNIQUE,
        phone         TEXT,
        country_code  TEXT        NOT NULL DEFAULT '+91',
        password_hash TEXT        NOT NULL,
        role          TEXT        NOT NULL DEFAULT 'owner',
        status        TEXT        NOT NULL DEFAULT 'active',
        last_active   TIMESTAMPTZ,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── admin_users (Leads AI internal staff) ────────────────────────────────────────────────
    # role: admin (claim/resolve tickets, view clients)
    #       superadmin (full control; CANNOT claim or chat in tickets)
    """
    CREATE TABLE IF NOT EXISTS admin_users (
        id            TEXT        PRIMARY KEY,
        name          TEXT        NOT NULL,
        email         TEXT        NOT NULL UNIQUE,
        password_hash TEXT        NOT NULL,
        role          TEXT        NOT NULL DEFAULT 'admin',
        status        TEXT        NOT NULL DEFAULT 'active',
        ticket_limit  INTEGER     NOT NULL DEFAULT 10,
        last_active   TIMESTAMPTZ,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── password_reset_tokens ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS password_reset_tokens (
        id         TEXT        PRIMARY KEY,
        user_id    TEXT        NOT NULL,
        user_type  TEXT        NOT NULL,
        token_hash TEXT        NOT NULL UNIQUE,
        expires_at TIMESTAMPTZ NOT NULL,
        used       BOOLEAN     NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── tickets ───────────────────────────────────────────────────────────────
    # Status flow:
    #   open  → claimed (admin claims it)
    #   open  → closed  (user closes, closure_note required)
    #   claimed → solved (admin resolves, resolution_note required)
    #   claimed → closed (user closes, closure_note required)
    """
    CREATE TABLE IF NOT EXISTS tickets (
        id              TEXT        PRIMARY KEY,
        tenant_id       TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        user_id         TEXT        NOT NULL REFERENCES user_auth(id),
        heading         TEXT        NOT NULL,
        context         TEXT        NOT NULL,
        type            TEXT        NOT NULL DEFAULT 'issue',
        priority        TEXT        NOT NULL DEFAULT 'medium',
        status          TEXT        NOT NULL DEFAULT 'open',
        claimed_by      TEXT        REFERENCES admin_users(id),
        assigned_to     TEXT        REFERENCES admin_users(id),
        resolution_note TEXT,
        closure_note    TEXT,
        unread_user     INTEGER     NOT NULL DEFAULT 0,
        unread_admin    INTEGER     NOT NULL DEFAULT 0,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── ticket_attachments ────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ticket_attachments (
        id         TEXT        PRIMARY KEY,
        ticket_id  TEXT        NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
        filename   TEXT        NOT NULL,
        file_path  TEXT        NOT NULL,
        file_size  INTEGER,
        mime_type  TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── ticket_messages ───────────────────────────────────────────────────────
    # Chat between tenant user and admin inside a ticket thread.
    # Users can only send once ticket is 'claimed'.
    """
    CREATE TABLE IF NOT EXISTS ticket_messages (
        id          TEXT        PRIMARY KEY,
        ticket_id   TEXT        NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
        sender_id   TEXT        NOT NULL,
        sender_name TEXT        NOT NULL,
        sender_role TEXT        NOT NULL,
        message     TEXT        NOT NULL,
        read_at     TIMESTAMPTZ,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── ticket_status_log (immutable audit trail) ─────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ticket_status_log (
        id           BIGSERIAL   PRIMARY KEY,
        ticket_id    TEXT        NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
        changed_by   TEXT        NOT NULL,
        changer_role TEXT        NOT NULL,
        from_status  TEXT        NOT NULL,
        to_status    TEXT        NOT NULL,
        note         TEXT,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── platform_settings (superadmin key/value store) ────────────────────────
    """
    CREATE TABLE IF NOT EXISTS platform_settings (
        key        TEXT        PRIMARY KEY,
        value      TEXT        NOT NULL,
        updated_by TEXT,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── audit_log (every admin/superadmin action) ─────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id          BIGSERIAL   PRIMARY KEY,
        actor_id    TEXT        NOT NULL,
        actor_role  TEXT        NOT NULL,
        action      TEXT        NOT NULL,
        entity_type TEXT        NOT NULL,
        entity_id   TEXT        NOT NULL,
        tenant_id   TEXT,
        meta        JSONB,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── knowledge_qa (table for custom Q/A) ──────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS knowledge_qa (
        id          TEXT        PRIMARY KEY,
        tenant_id   TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        question    TEXT        NOT NULL,
        answer      TEXT        NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── kb_company_data (structured company info per tenant) ───────────────────
    """
    CREATE TABLE IF NOT EXISTS kb_company_data (
        id               TEXT        PRIMARY KEY,
        tenant_id        TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        section          TEXT        NOT NULL,
        field_key        TEXT        NOT NULL,
        field_value      TEXT        NOT NULL DEFAULT '',
        display_order    INTEGER     NOT NULL DEFAULT 0,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE(tenant_id, section, field_key)
    )
    """,

    # ── kb_products (product catalog per tenant) ──────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS kb_products (
        id               TEXT        PRIMARY KEY,
        tenant_id        TEXT        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
        category         TEXT        NOT NULL DEFAULT '',
        sub_category     TEXT        NOT NULL DEFAULT '',
        name             TEXT        NOT NULL,
        description      TEXT        NOT NULL DEFAULT '',
        image_url        TEXT,
        pricing          TEXT,
        min_order_qty    TEXT,
        source_url       TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,

    # ── otps (generic OTP table for all purposes) ─────────────────────────────
    # purpose: 'registration' | 'change_password' | 'member_invite' | ...
    # payload: JSONB storing purpose-specific data (e.g. registration fields)
    """
    CREATE TABLE IF NOT EXISTS otps (
        id            TEXT        PRIMARY KEY,
        email         TEXT        NOT NULL,
        otp_hash      TEXT        NOT NULL,
        purpose       TEXT        NOT NULL DEFAULT 'registration',
        payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
        expires_at    TIMESTAMPTZ NOT NULL,
        used          BOOLEAN     NOT NULL DEFAULT FALSE,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
]

_INDEXES: list[str] = [
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_tenants_widget_slug ON tenants(widget_slug) WHERE widget_slug IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_sessions_tenant       ON sessions(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_sessions_visitor      ON sessions(visitor_id)",
    "CREATE INDEX IF NOT EXISTS idx_leads_tenant          ON leads(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_leads_email           ON leads(email)",
    "CREATE INDEX IF NOT EXISTS idx_leads_canonical       ON leads(canonical_lead_id)",
    "CREATE INDEX IF NOT EXISTS idx_leads_created         ON leads(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_usage_tenant          ON usage_events(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_usage_created         ON usage_events(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_payments_tenant       ON payments(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant  ON subscriptions(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_user_auth_email       ON user_auth(email)",
    "CREATE INDEX IF NOT EXISTS idx_user_auth_tenant      ON user_auth(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_admin_email           ON admin_users(email)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_tenant        ON tickets(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_status        ON tickets(status)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_claimed       ON tickets(claimed_by)",
    "CREATE INDEX IF NOT EXISTS idx_tickets_assigned      ON tickets(assigned_to)",
    "CREATE INDEX IF NOT EXISTS idx_ticket_msgs_ticket    ON ticket_messages(ticket_id)",
    "CREATE INDEX IF NOT EXISTS idx_ticket_msgs_created   ON ticket_messages(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_ticket_log_ticket     ON ticket_status_log(ticket_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_actor           ON audit_log(actor_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_tenant          ON audit_log(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_entity          ON audit_log(entity_type, entity_id)",
    "CREATE INDEX IF NOT EXISTS idx_audit_created         ON audit_log(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_pwd_reset_hash        ON password_reset_tokens(token_hash)",
    "CREATE INDEX IF NOT EXISTS idx_knowledge_qa_tenant   ON knowledge_qa(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_kb_company_tenant     ON kb_company_data(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_kb_products_tenant    ON kb_products(tenant_id)",
    "CREATE INDEX IF NOT EXISTS idx_kb_products_cat       ON kb_products(tenant_id, category)",
    "CREATE INDEX IF NOT EXISTS idx_otp_email             ON otps(email)",
    "CREATE INDEX IF NOT EXISTS idx_otp_purpose            ON otps(email, purpose)",
]


# ═════════════════════════════════════════════════════════════════════════════
# PASSWORD & KEY HELPERS
# ═════════════════════════════════════════════════════════════════════════════

# hash_api_key removed — domain-based auth does not need key hashing


def hash_password(password: str) -> str:
    """PBKDF2-HMAC-SHA256 — no bcrypt dependency needed."""
    salt = secrets.token_hex(16)
    dk   = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 260_000)
    return f"pbkdf2$sha256$260000${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Constant-time verification against a stored PBKDF2 hash."""
    try:
        _, algo, iters, salt, dk_hex = stored.split("$")
        dk = hashlib.pbkdf2_hmac(algo, password.encode(), salt.encode(), int(iters))
        return secrets.compare_digest(dk.hex(), dk_hex)
    except Exception:
        return False


# Plan-driven defaults — also written to platform_settings at seed time
# Plan ticket limits are now driven by the 'plans' table.
# Default limits for trial accounts:
TRIAL_TICKET_LIMIT = 2


# ═════════════════════════════════════════════════════════════════════════════
# INIT
# ═════════════════════════════════════════════════════════════════════════════

async def _audit(conn, actor: dict, action: str, entity_type: str, entity_id: str, meta: dict = {}, tenant_id: Optional[str] = None):
    """
    Generic audit logging.
    Actor: dict from get_current_user (id, role/account_type)
    """
    try:
        actor_id   = actor.get("id")
        actor_role = actor.get("role") or actor.get("account_type")
        await conn.execute(
            "INSERT INTO audit_log (id, actor_id, actor_role, action, entity_type, entity_id, meta, tenant_id)"
            " VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7)",
            actor_id, actor_role, action, entity_type, entity_id, json.dumps(meta), tenant_id
        )
        logger.info(f"Audit: {actor_role}:{actor_id} -> {action} {entity_type}:{entity_id}")
    except Exception as e:
        logger.error(f"Audit log failed: {e}")


async def init_db(reset: bool = False, seed: bool = True) -> None:
    pool = await get_pool()

    async with pool.acquire() as conn:
        async with conn.transaction():
            if reset:
                drops = [
                    "audit_log", "platform_settings",
                    "ticket_status_log", "ticket_messages",
                    "ticket_attachments", "tickets",
                    "password_reset_tokens", "otps",
                    "admin_users", "user_auth",
                    "usage_events", "widget_configs", "ingest_jobs", "knowledge_qa",
                    "kb_products", "kb_company_data", "scrape_jobs", "leads",
                    "sessions", "payments", "subscriptions", "tenants", "plans",
                ]
                for t in drops:
                    await conn.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
                logger.info("  All tables dropped")

            for stmt in _TABLES:
                await conn.execute(stmt)
            
            # Ensure tenant_id exists before indexing it
            await conn.execute("ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant_id TEXT")

            # ── Schema migrations for existing databases ──────────────────────
            await conn.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS domain_verified BOOLEAN NOT NULL DEFAULT FALSE")
            await conn.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS verification_token TEXT")
            await conn.execute("ALTER TABLE tenants ADD COLUMN IF NOT EXISTS suspension_reason TEXT")

            for stmt in _INDEXES:
                await conn.execute(stmt)

    logger.info("✅ Schema applied")

    if seed:
        await _seed(pool)

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        logger.info(f"📋 Tables ({len(rows)}): {', '.join(r['tablename'] for r in rows)}")

    logger.info("🎉 Database ready!")


# ═════════════════════════════════════════════════════════════════════════════
# SEED
# ═════════════════════════════════════════════════════════════════════════════

async def _seed(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():

            # ── plans ────────────────────────────────────────────────────────────────────────
            starter_rzp = os.getenv("RAZORPAY_PLAN_STARTER", "")
            pro_rzp = os.getenv("RAZORPAY_PLAN_PRO", "")
            ent_rzp = os.getenv("RAZORPAY_PLAN_ENTERPRISE", "")
            
            plans_data = [
                ("trial", "Free Trial", 0, 0, "INR", 0, 0, 2, 1, "7-day free trial to explore all features", None, None, None),
                ("starter", "Starter", 5000, 1000, "INR", 0.0025, 0.0210, 2, 2, "Perfect for small businesses", starter_rzp, None, None),
                ("pro", "Pro", 8000, 1000, "INR", 0.0025, 0.0210, 10, 5, "For growing teams", pro_rzp, None, None),
                ("customized", "Customized", 0, 0, "INR", 0.0025, 0.0210, 15, 10, "Tailored to your business — contact sales", ent_rzp, None, None),
            ]
            
            await conn.executemany(
                "INSERT INTO plans (id, name, onboarding_fee_rupee, base_fee_rupee, currency, input_token_rate, output_token_rate, ticket_limit, domain_limit, description, razorpay_plan_id, discount_type, discount_value)"
                " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (id) DO NOTHING",
                plans_data,
            )

            # ── platform_settings defaults ────────────────────────────────────
            # Default product import template columns (JSON)
            default_template = json.dumps([
                {"key": "name",          "label": "Name",           "required": True},
                {"key": "category",      "label": "Category",       "required": True},
                {"key": "sub_category",  "label": "Sub Category",   "required": False},
                {"key": "description",   "label": "Description",    "required": False},
                {"key": "pricing",       "label": "Pricing",        "required": False},
                {"key": "min_order_qty", "label": "Min Order Qty",  "required": False},
                {"key": "image_url",     "label": "Image URL",      "required": False},
                {"key": "source_url",    "label": "Source URL",      "required": False},
            ])

            settings_defaults = [
                ("admin_ticket_limit",      "10"),
                ("registration_enabled",    "true"),
                ("maintenance_mode",        "false"),
                ("trial_duration_days",     "7"),
                ("jwt_session_hours",       "24"),
                ("vector_refresh_interval_hours", "24"),
                ("vector_refresh_time",     "00:00"),
                ("product_import_template", default_template),
                ("generic_email_domains",   "gmail.com,yahoo.com,yahoo.co.in,outlook.com,hotmail.com,aol.com,icloud.com,mail.com,protonmail.com,zoho.com,yandex.com,gmx.com,live.com,rediffmail.com"),
                ("queries_email",           ""),
            ]
            await conn.executemany(
                "INSERT INTO platform_settings (key, value) VALUES ($1, $2)"
                " ON CONFLICT (key) DO NOTHING",
                settings_defaults,
            )

            # ── superadmin ────────────────────────────────────────────────────
            sa_email = "superadmin@winssoft.com"
            if not await conn.fetchrow("SELECT id FROM admin_users WHERE email=$1", sa_email):
                await conn.execute(
                    "INSERT INTO admin_users"
                    " (id, name, email, password_hash, role, ticket_limit)"
                    " VALUES ($1,'Super Admin',$2,$3,'superadmin',0)",
                    secrets.token_hex(8), sa_email, hash_password("SuperAdmin123!"),
                )
                logger.info("  ✅ superadmin@winssoft.com / SuperAdmin123!")

            # ── staff admin ───────────────────────────────────────────────────
            ad_email = "admin@winssoft.com"
            if not await conn.fetchrow("SELECT id FROM admin_users WHERE email=$1", ad_email):
                await conn.execute(
                    "INSERT INTO admin_users"
                    " (id, name, email, password_hash, role, ticket_limit)"
                    " VALUES ($1,'Support Admin',$2,$3,'admin',10)",
                    secrets.token_hex(8), ad_email, hash_password("Admin123!"),
                )
                logger.info("  ✅ admin@winssoft.com / Admin123!")

            # ── demo tenant (Pro) ─────────────────────────────────────────────
            demo_email = "demo@winssoft.com"
            demo_row = await conn.fetchrow(
                "SELECT id FROM tenants WHERE email=$1", demo_email
            )
            if demo_row:
                logger.info(f"  ℹ  Demo tenant exists (id={demo_row['id']})")
            else:
                tid  = secrets.token_hex(8)
                now  = datetime.now(timezone.utc)
                end  = now + timedelta(days=30)

                await conn.execute(
                    "INSERT INTO tenants"
                    " (id,name,email,company,domain,plan,status,ticket_limit)"
                    " VALUES ($1,'Demo Store',$2,'Demo Company','localhost','pro','active',10)",
                    tid, demo_email,
                )

                # No needed — widget uses JWT auth

                sub = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO subscriptions"
                    " (id,tenant_id,plan,status,amount_rupee,"
                    "  current_period_start,current_period_end)"
                    " VALUES ($1,$2,'pro','active',7900,$3,$4)",
                    sub, tid, now, end,
                )

                await conn.execute(
                    "INSERT INTO widget_configs"
                    " (tenant_id,name,greeting,notification_email)"
                    " VALUES ($1,'Demo Store',$2,$3)",
                    tid,
                    "Hi! I'm your AI assistant. How can I help?",
                    demo_email,
                )

                ua = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO user_auth"
                    " (id,tenant_id,name,email,password_hash,role)"
                    " VALUES ($1,$2,'Demo Owner',$3,$4,'owner')",
                    ua, tid, demo_email, hash_password("Demo123!"),
                )

                logger.info("  ✅ demo@winssoft.com / Demo123!  (Pro plan)")
                Path(".env.demo").write_text(
                    f"DEMO_TENANT_ID={tid}\n"
                    f"DEMO_DASHBOARD_EMAIL={demo_email}\n"
                    f"DEMO_DASHBOARD_PASS=Demo123!\n"
                )

            # ── starter test client ───────────────────────────────────────────
            st_email = "test@shop.com"
            if not await conn.fetchrow("SELECT id FROM tenants WHERE email=$1", st_email):
                st  = secrets.token_hex(8)
                now2 = datetime.now(timezone.utc)
                await conn.execute(
                    "INSERT INTO tenants"
                    " (id,name,email,company,plan,status,ticket_limit)"
                    " VALUES ($1,'Test Shop',$2,'Test Ltd','starter','active',2)",
                    st, st_email,
                )
                await conn.execute(
                    "INSERT INTO widget_configs (tenant_id,name,notification_email)"
                    " VALUES ($1,'Test Shop',$2)",
                    st, st_email,
                )
                await conn.execute(
                    "INSERT INTO subscriptions"
                    " (id,tenant_id,plan,status,amount_rupee,"
                    "  current_period_start,current_period_end)"
                    " VALUES ($1,$2,'starter','active',2900,$3,$4)",
                    secrets.token_hex(8), st, now2, now2 + timedelta(days=30),
                )
                await conn.execute(
                    "INSERT INTO user_auth"
                    " (id,tenant_id,name,email,password_hash,role)"
                    " VALUES ($1,$2,'Shop Owner',$3,$4,'owner')",
                    secrets.token_hex(8), st, st_email, hash_password("Test123!"),
                )
                logger.info("  ✅ test@shop.com / Test123!  (Starter plan)")
            
            # ── test ticket with attachment ──────────────────────────────────
            test_tid = await conn.fetchval("SELECT id FROM tenants WHERE email='test@shop.com'")
            if test_tid:
                t_id = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO tickets (id, tenant_id, user_id, heading, context, priority, status)"
                    " VALUES ($1, $2, (SELECT id FROM user_auth WHERE tenant_id=$2 LIMIT 1),"
                    " 'Testing attachment', 'This ticket has an attachment for verification.', 'high', 'open')",
                    t_id, test_tid
                )
                await conn.execute(
                    "INSERT INTO ticket_attachments (id, ticket_id, filename, file_path, mime_type)"
                    " VALUES ($1, $2, 'test_image.png', 'https://placehold.co/600x400', 'image/png')",
                    secrets.token_hex(8), t_id
                )
                logger.info("  ✅ Created test ticket with attachment")

    logger.info("✅ Seed complete")


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Leads AI — DB init (PostgreSQL)")
    p.add_argument("--reset",   action="store_true", help="Drop all tables first")
    p.add_argument("--no-seed", action="store_true", help="Skip demo seed data")
    args = p.parse_args()

    async def _run():
        await init_db(reset=args.reset, seed=not args.no_seed)
        await close_pool()

    asyncio.run(_run())
