"""
Leads AI — Database Runtime Helpers  (PostgreSQL / asyncpg)
=============================================================
Deployed to production. Imported by main.py, auth.py, payments.py.

Provides:
  - Connection pool (get_pool / close_pool)
  - Tenant schema management (create_tenant_schema / drop_tenant_schema)
  - tenant_conn() — async context manager that sets search_path
  - Password hashing / verification (PBKDF2-HMAC-SHA256)
  - _audit() — generic audit log writer

db_init.py is NOT deployed — it contains DDL only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
from contextlib import asynccontextmanager
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

try:
    import asyncpg
except ImportError:
    import sys
    logger.error("asyncpg not installed — run: pip install asyncpg")
    sys.exit(1)


# ═════════════════════════════════════════════════════════════════════════════
# DSN helper — strips SQLAlchemy prefix so asyncpg can use it directly
# ═════════════════════════════════════════════════════════════════════════════

def _dsn() -> str:
    if (os.getenv("DEV_MODE", "false").lower() == "true"):
        raw = os.getenv("DEV_DB_URL", "")
    else:
        raw = os.environ.get("DATABASE_URL", "")
        if not raw:
            logger.error("DATABASE_URL is not set in .env")
            import sys
            sys.exit(1)
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
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
        format="text",
    )


# ═════════════════════════════════════════════════════════════════════════════
# TENANT SCHEMA HELPERS
# ═════════════════════════════════════════════════════════════════════════════

# Table names in the 'tenant' template schema
TENANT_TABLE_NAMES: list[str] = [
    "sessions", "session_metadata", "leads", "ingest_jobs",
    "widget_configs", "knowledge_qa", "kb_company_data", "kb_products",
]

# Plan-driven defaults
TRIAL_TICKET_LIMIT = 2


async def create_tenant_schema(conn, tenant_id: str) -> None:
    """
    Clone the 'tenant' template schema into t_{tenant_id}.
    Uses LIKE ... INCLUDING ALL to copy columns, defaults, constraints, indexes.
    Safe to call multiple times (IF NOT EXISTS).
    """
    schema = f"t_{tenant_id}"
    await conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    for table in TENANT_TABLE_NAMES:
        await conn.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}".{table} '
            f'(LIKE tenant.{table} INCLUDING ALL)'
        )
    logger.info(f"  Created tenant schema: {schema}")


async def drop_tenant_schema(conn, tenant_id: str) -> None:
    """Drop a tenant schema and ALL its data. Irreversible."""
    schema = f"t_{tenant_id}"
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    logger.info(f"  Dropped tenant schema: {schema}")


@asynccontextmanager
async def tenant_conn(tenant_id: str):
    """
    Acquire a connection with search_path set to the tenant's schema.
    Queries like 'SELECT * FROM sessions' resolve to t_{tenant_id}.sessions.
    Queries like 'SELECT * FROM tenants' resolve to public.tenants.
    search_path is reset on exit.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        schema = f"t_{tenant_id}"
        await conn.execute(f'SET search_path TO "{schema}", leadsai')
        try:
            yield conn
        finally:
            await conn.execute("SET search_path TO leadsai")


# ═════════════════════════════════════════════════════════════════════════════
# PASSWORD & KEY HELPERS
# ═════════════════════════════════════════════════════════════════════════════

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


# ═════════════════════════════════════════════════════════════════════════════
# AUDIT LOGGING
# ═════════════════════════════════════════════════════════════════════════════

async def _audit(
    conn,
    actor: dict,
    action: str,
    entity_type: str,
    entity_id: str,
    meta: dict = {},
    tenant_id: Optional[str] = None,
):
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
