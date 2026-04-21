"""
Winssoft BMA — Razorpay Payment Service  v3.0  (PostgreSQL)
============================================================
All config from .env — no config.py.

.env keys
    RAZORPAY_KEY_ID      = rzp_live_...
    RAZORPAY_KEY_SECRET  = ...
    RAZORPAY_PLAN_STARTER    (optional — for recurring subscriptions)
    RAZORPAY_PLAN_PRO        (optional)
    RAZORPAY_PLAN_ENTERPRISE (optional)
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import asyncpg
from dotenv import load_dotenv

from db_init import get_pool

load_dotenv()

logger = logging.getLogger(__name__)

try:
    import razorpay as _rzp_sdk
    _RZP_AVAILABLE = True
except ImportError:
    _rzp_sdk       = None
    _RZP_AVAILABLE = False
    logger.warning("razorpay not installed — run: pip install razorpay")

# ── Plan access helper ────────────────────────────────────────────────────────
async def get_plan(plan_id: str) -> Optional[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, onboarding_fee_rupee, base_fee_rupee, currency,"
            " input_token_rate, output_token_rate,"
            " ticket_limit, domain_limit,"
            " description, razorpay_plan_id"
            " FROM plans WHERE id=$1",
            plan_id,
        )
        return dict(row) if row else None


class RazorpayService:
    def __init__(self):
        self._key_id     = os.getenv("RAZORPAY_KEY_ID",     "")
        self._key_secret = os.getenv("RAZORPAY_KEY_SECRET", "")
        self._client     = None

        if _RZP_AVAILABLE and self._key_id and self._key_secret:
            self._client = _rzp_sdk.Client(auth=(self._key_id, self._key_secret))
            logger.info("✅ Razorpay client ready")
        elif os.getenv("ENV", "dev") == "prod":
            raise RuntimeError(
                "RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET must be set in production"
            )
        else:
            logger.warning("⚠️  Razorpay running in mock mode (missing keys or package)")

    # ── Create Razorpay order ─────────────────────────────────────────────────
    async def create_order(self, tenant_id: str, plan: str) -> Dict:
        cfg = await get_plan(plan)
        if not cfg:
            raise ValueError(f"Unknown plan: {plan}")

        # First payment = onboarding + first month base fee
        amount = cfg["onboarding_fee_rupee"] + cfg["base_fee_rupee"]
        if cfg.get("discount_type") == "percentage" and cfg.get("discount_value"):
            amount = amount - int(amount * (cfg["discount_value"] / 100.0))
        elif cfg.get("discount_type") == "amount" and cfg.get("discount_value"):
            amount = amount - cfg["discount_value"]
        
        amount = max(0, amount)

        if not self._client:
            return await self._mock_order(tenant_id, plan, cfg, amount)

        order = self._client.order.create({
            "amount":   amount * 100,
            "currency": cfg["currency"],
            "receipt":  f"oc_{tenant_id[:8]}_{secrets.token_hex(4)}",
            "notes":    {"tenant_id": tenant_id, "plan": plan},
        })

        await self._save_payment(
            tenant_id=tenant_id, plan=plan,
            order_id=order["id"], amount=amount, status="pending",
        )

        return {
            "order_id":  order["id"],
            "amount":    amount * 100,
            "currency":  cfg["currency"],
            "plan":      plan,
            "plan_name": cfg["name"],
            "key_id":    self._key_id,
        }

    # ── Verify payment + activate subscription ────────────────────────────────
    async def verify_payment(
        self,
        razorpay_order_id:   str,
        razorpay_payment_id: str,
        razorpay_signature:  str,
        tenant_id:           str,
        plan:                str,
    ) -> Dict:
        # Signature check
        expected = hmac.new(
            self._key_secret.encode(),
            f"{razorpay_order_id}|{razorpay_payment_id}".encode(),
            hashlib.sha256,
        ).hexdigest()
        if expected != razorpay_signature:
            logger.warning(f"Bad Razorpay signature for order {razorpay_order_id}")
            return {"success": False, "error": "Invalid payment signature"}

        cfg = await get_plan(plan)
        if not cfg:
            return {"success": False, "error": f"Plan {plan} not found in database"}

        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Mark payment captured
                await conn.execute(
                    "UPDATE payments"
                    " SET razorpay_payment_id=$1, razorpay_signature=$2,"
                    "     status='captured', updated_at=NOW()"
                    " WHERE razorpay_order_id=$3 AND tenant_id=$4",
                    razorpay_payment_id, razorpay_signature,
                    razorpay_order_id, tenant_id,
                )

                # Activate tenant
                await conn.execute(
                    "UPDATE tenants SET plan=$1, status='active', updated_at=NOW()"
                    " WHERE id=$2",
                    plan, tenant_id,
                )

                # Upsert subscription
                now    = datetime.now(timezone.utc)
                
                # Calculate next 5th of the month
                y, m = now.year, now.month
                if now.day >= 5:
                    m += 1
                    if m > 12:
                        m = 1
                        y += 1
                end = datetime(y, m, 5, tzinfo=timezone.utc)
                sub_id = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO subscriptions"
                    " (id,tenant_id,plan,status,amount_rupee,currency,"
                    "  current_period_start,current_period_end)"
                    " VALUES ($1,$2,$3,'active',$4,$5,$6,$7)"
                    " ON CONFLICT (razorpay_sub_id) DO NOTHING",
                    sub_id, tenant_id, plan,
                    cfg["onboarding_fee_rupee"] + cfg["base_fee_rupee"],
                    cfg["currency"],
                    now, end,
                )

                # Create initial billing cycle
                cycle_id = secrets.token_hex(8)
                await conn.execute(
                    "INSERT INTO billing_cycles"
                    " (id, tenant_id, subscription_id, start_date, end_date, base_fee_rupee, status)"
                    " VALUES ($1, $2, $3, $4, $5, $6, 'active')",
                    cycle_id, tenant_id, sub_id, now, end, cfg["base_fee_rupee"],
                )

                # API key generation removed — domain-based auth now

        logger.info(
            f"✅ Payment captured: tenant={tenant_id} plan={plan} "
            f"payment={razorpay_payment_id}"
        )
        return {
            "success":    True,
            "plan":       plan,
            "plan_name":  cfg["name"],
            "payment_id": razorpay_payment_id,
        }

    # ── Usage / subscription status ───────────────────────────────────────────
    async def check_usage(self, tenant_id: str) -> Dict:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Get active subscription
            sub = await conn.fetchrow(
                "SELECT s.id, s.plan, s.status, s.amount_rupee, s.currency,"
                "       s.current_period_start, s.current_period_end"
                " FROM subscriptions s"
                " WHERE s.tenant_id=$1 AND s.status='active'"
                " ORDER BY s.created_at DESC LIMIT 1",
                tenant_id,
            )
            


            if not sub:
                return {"plan": "trial"}

            r = dict(sub)
            r["period_end"]   = r.pop("current_period_end")
            r["period_start"] = r.pop("current_period_start")

            # Get active billing cycle token usage
            cycle = await conn.fetchrow(
                "SELECT input_tokens_used, output_tokens_used, base_fee_rupee,"
                "       start_date, end_date"
                " FROM billing_cycles"
                " WHERE tenant_id=$1 AND status='active'"
                " ORDER BY created_at DESC LIMIT 1",
                tenant_id,
            )
            if cycle:
                plan_cfg = await get_plan(r["plan"])
                input_rate  = float(plan_cfg["input_token_rate"])  if plan_cfg else 0.0
                output_rate = float(plan_cfg["output_token_rate"]) if plan_cfg else 0.0
                input_cost  = round(cycle["input_tokens_used"]  * input_rate,  2)
                output_cost = round(cycle["output_tokens_used"] * output_rate, 2)
                r["billing_cycle"] = {
                    "input_tokens_used":  cycle["input_tokens_used"],
                    "output_tokens_used": cycle["output_tokens_used"],
                    "base_fee_rupee":     cycle["base_fee_rupee"],
                    "input_cost_rupee":   input_cost,
                    "output_cost_rupee":  output_cost,
                    "total_due_rupee":    cycle["base_fee_rupee"] + input_cost + output_cost,
                    "cycle_start":        cycle["start_date"],
                    "cycle_end":          cycle["end_date"],
                }
            else:
                r["billing_cycle"] = None

            return r

    # ── Usage analytics ───────────────────────────────────────────────────────
    async def get_analytics(self, tenant_id: str, days: int = 30) -> Dict:
        pool = await get_pool()
        async with pool.acquire() as conn:
            daily = await conn.fetch(
                "SELECT DATE(created_at) AS day,"
                "       COUNT(*) AS requests,"
                "       SUM(tokens_in + tokens_out) AS tokens,"
                "       AVG(latency_ms) AS avg_latency"
                " FROM usage_events"
                " WHERE tenant_id=$1 AND created_at >= NOW() - $2::INTERVAL"
                " GROUP BY DATE(created_at) ORDER BY day",
                tenant_id, timedelta(days=days),
            )
            totals = await conn.fetchrow(
                "SELECT COUNT(*) AS total_requests,"
                "       COALESCE(SUM(tokens_in+tokens_out),0) AS total_tokens,"
                "       COUNT(DISTINCT session_id) AS total_sessions"
                " FROM usage_events"
                " WHERE tenant_id=$1 AND created_at >= NOW() - $2::INTERVAL",
                tenant_id, timedelta(days=days),
            )

        return {
            "daily":    [dict(r) for r in daily],
            "totals":   dict(totals) if totals else {},
        }

    # ── Log usage event ───────────────────────────────────────────────────────
    async def log_usage(
        self,
        tenant_id:  str,
        domain:     str = "",
        session_id: str = "",
        event_type: str = "",
        tokens_in:  int = 0,
        tokens_out: int = 0,
        latency_ms: int = 0,
        status:     str = "ok",
    ) -> None:
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO usage_events"
                    " (tenant_id,domain,session_id,event_type,"
                    "  tokens_in,tokens_out,latency_ms,status)"
                    " VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                    tenant_id, domain, session_id, event_type,
                    tokens_in, tokens_out, latency_ms, status,
                )
        except Exception as e:
            logger.error(f"Usage log failed: {e}")

    # ── Payment history ───────────────────────────────────────────────────────
    async def get_payment_history(self, tenant_id: str) -> list[Dict]:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, created_at, plan, amount_rupee, currency,"
                "       status, razorpay_payment_id, razorpay_order_id"
                " FROM payments WHERE tenant_id=$1"
                " ORDER BY created_at DESC",
                tenant_id,
            )
        return [dict(r) for r in rows]

    # ── Mock (no Razorpay configured) ─────────────────────────────────────────
    def _mock_order(self, tenant_id: str, plan: str, cfg: Dict, amount: int) -> Dict:
        mid = "order_MOCK_" + secrets.token_hex(8)
        logger.info(f"[MOCK] Razorpay order {mid} — tenant={tenant_id} plan={plan}")
        return {
            "order_id":  mid,
            "amount":    amount * 100,
            "currency":  cfg["currency"],
            "plan":      plan,
            "plan_name": cfg["name"],
            "key_id":    "rzp_test_DEMO",
            "_mock":     True,
        }

    async def _save_payment(
        self,
        tenant_id: str,
        plan:      str,
        order_id:  str,
        amount:    int,
        status:    str,
    ) -> None:
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO payments"
                    " (id,tenant_id,razorpay_order_id,amount_rupee,status,plan)"
                    " VALUES ($1,$2,$3,$4,$5,$6)",
                    secrets.token_hex(8), tenant_id, order_id, amount, status, plan,
                )
        except Exception as e:
            logger.error(f"Payment save failed: {e}")
