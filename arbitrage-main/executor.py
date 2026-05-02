"""executor.py — Kalshi order API, paper simulation, and position lifecycle."""

from __future__ import annotations

import base64
import os
import time
import uuid
from typing import Any, Optional

import httpx

from kalshi_fees import calc_fee, calc_maker_fee, infer_market
from learn import record_trade
from state import (
    AppState, ClosedTrade, DirectionalPosition, Leg, OpenPosition,
    add_scan_log,
)

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"


def _api_key() -> str:
    key = os.environ.get("KALSHI_API_KEY")
    if not key:
        raise RuntimeError("KALSHI_API_KEY is not set in environment variables")
    return key


def _private_key_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "private_key.pem")


def _signed_headers(method: str, path: str) -> dict[str, str]:
    """
    Build RSA-PSS signed headers for the Kalshi elections API.
    Falls back to Bearer token if no private key file is present
    (useful for read-only endpoints that don't require signing).
    """
    timestamp_ms = str(int(time.time() * 1000))
    headers = {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": _api_key(),
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
    }

    pem_path = _private_key_path()
    if not os.path.exists(pem_path):
        # No private key — fall back to Bearer (will fail on authenticated endpoints)
        headers["Authorization"] = f"Bearer {_api_key()}"
        return headers

    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend

        with open(pem_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )

        msg = f"{timestamp_ms}{method.upper()}{path}"
        signature = private_key.sign(
            msg.encode("utf-8"),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        headers["KALSHI-ACCESS-SIGNATURE"] = base64.b64encode(signature).decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"RSA signing failed: {e}") from e

    return headers


# ─── Kalshi Order API ──────────────────────────────────────────────────────────

async def place_order(
    ticker: str,
    client_order_id: str,
    order_type: str,        # "limit" | "market"
    side: str,              # "yes" | "no"
    count: int,
    limit_price: Optional[int] = None,  # cents
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ticker": ticker,
        "client_order_id": client_order_id,
        "type": order_type,
        "action": "buy",
        "side": side,
        "count": count,
    }
    if limit_price is not None:
        price_field = "yes_price" if side == "yes" else "no_price"
        payload[price_field] = limit_price

    _path = "/trade-api/v2/portfolio/orders"
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{BASE_URL}/portfolio/orders",
            headers=_signed_headers("POST", _path),
            json=payload,
            timeout=10.0,
        )
    if not r.is_success:
        raise RuntimeError(f"Order placement failed {r.status_code}: {r.text}")
    data = r.json()
    order = data.get("order")
    if not order:
        raise RuntimeError(f"API returned no order object: {str(data)[:200]}")
    return order


async def cancel_order(order_id: str) -> None:
    _path = f"/trade-api/v2/portfolio/orders/{order_id}"
    async with httpx.AsyncClient() as client:
        r = await client.delete(
            f"{BASE_URL}/portfolio/orders/{order_id}",
            headers=_signed_headers("DELETE", _path),
            timeout=10.0,
        )
    if not r.is_success and r.status_code != 404:
        raise RuntimeError(f"Cancel order failed {r.status_code}: {r.text}")


async def get_order(order_id: str) -> dict[str, Any]:
    _path = f"/trade-api/v2/portfolio/orders/{order_id}"
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{BASE_URL}/portfolio/orders/{order_id}",
            headers=_signed_headers("GET", _path),
            timeout=10.0,
        )
    if not r.is_success:
        raise RuntimeError(f"Get order failed {r.status_code}: {r.text}")
    data = r.json()
    order = data.get("order")
    if not order:
        raise RuntimeError(f"API returned no order object: {str(data)[:200]}")
    return order


async def get_balance() -> int:
    """Returns account balance in cents."""
    _path = "/trade-api/v2/portfolio/balance"
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{BASE_URL}/portfolio/balance",
            headers=_signed_headers("GET", _path),
            timeout=10.0,
        )
    if not r.is_success:
        raise RuntimeError(f"Get balance failed {r.status_code}: {r.text}")
    return r.json().get("balance", 0)


# ─── Paper order simulation ────────────────────────────────────────────────────

_paper_seq = 0

def _paper_order_id() -> str:
    global _paper_seq
    _paper_seq += 1
    return f"PAPER-{int(time.time() * 1000)}-{_paper_seq}"


# ─── Execute a new two-leg position ───────────────────────────────────────────

async def execute_position(
    ticker: str,
    strategy: str,
    yes_limit_cents: int,
    no_limit_cents: int,
    contracts: int,
    expected_profit_cents_per_contract: float,
    close_time: str,
    current_yes_ask_cents: int,
    current_no_ask_cents: int,
    state: AppState,
) -> Optional[OpenPosition]:
    cfg = state.config
    fee_tier = infer_market(ticker)
    now_ms = time.time() * 1000

    yes_fee_cents = round(calc_maker_fee(contracts, yes_limit_cents / 100) * 100)
    no_fee_cents = round(calc_maker_fee(contracts, no_limit_cents / 100) * 100)

    pos_id = str(uuid.uuid4())
    yes_order_id: Optional[str] = None
    no_order_id: Optional[str] = None

    if cfg.paper_mode:
        yes_order_id = _paper_order_id()
        no_order_id = _paper_order_id()
    else:
        t0 = time.time() * 1000
        try:
            import asyncio
            yes_task = asyncio.create_task(place_order(
                ticker, str(uuid.uuid4()), "limit", "yes", contracts, yes_limit_cents
            ))
            no_task = asyncio.create_task(place_order(
                ticker, str(uuid.uuid4()), "limit", "no", contracts, no_limit_cents
            ))
            yes_order, no_order = await asyncio.gather(yes_task, no_task)

            elapsed = time.time() * 1000 - t0
            if elapsed > cfg.both_legs_window_ms:
                add_scan_log(state, ticker, "error",
                             f"Both-legs window exceeded ({elapsed:.0f}ms) — cancelling both legs")
                import asyncio as _asyncio
                await _asyncio.gather(
                    cancel_order(yes_order["order_id"]),
                    cancel_order(no_order["order_id"]),
                    return_exceptions=True,
                )
                return None

            yes_order_id = yes_order["order_id"]
            no_order_id = no_order["order_id"]

        except Exception as err:
            add_scan_log(state, ticker, "error", f"Order placement failed: {err}")
            if yes_order_id:
                try:
                    await cancel_order(yes_order_id)
                except Exception as cancel_err:
                    add_scan_log(state, ticker, "error",
                                 f"ORPHANED YES order {yes_order_id} — cancel failed: {cancel_err}. "
                                 f"Cancel manually on Kalshi.")
            return None

    yes_leg = Leg(
        side="yes",
        limit_price_cents=yes_limit_cents,
        contracts=contracts,
        order_id=yes_order_id,
        placed_at=now_ms,
        filled_at=None,    # paper fills handled by scan loop
        fill_price_cents=None,
        fee_cents=yes_fee_cents,
    )
    no_leg = Leg(
        side="no",
        limit_price_cents=no_limit_cents,
        contracts=contracts,
        order_id=no_order_id,
        placed_at=now_ms,
        filled_at=None,
        fill_price_cents=None,
        fee_cents=no_fee_cents,
    )

    pos = OpenPosition(
        id=pos_id,
        ticker=ticker,
        fee_tier=fee_tier,
        strategy=strategy,
        yes_leg=yes_leg,
        no_leg=no_leg,
        opened_at=now_ms,
        expected_profit_cents_per_contract=expected_profit_cents_per_contract,
        close_time=close_time,
        last_known_yes_ask_cents=current_yes_ask_cents,
        last_known_no_ask_cents=current_no_ask_cents,
    )

    state.open_positions[pos_id] = pos
    state.deployed_capital_cents += (yes_limit_cents + no_limit_cents) * contracts

    mode = "[PAPER] " if cfg.paper_mode else ""
    add_scan_log(state, ticker, "execute",
                 f"{mode}{strategy} {contracts}x YES@{yes_limit_cents}¢ + NO@{no_limit_cents}¢ "
                 f"| edge: {expected_profit_cents_per_contract:.1f}¢/contract")

    state.ui_needs_render = True
    return pos


# ─── Cancel a position (both legs) ────────────────────────────────────────────

async def cancel_position(pos_id: str, state: AppState, reason: str) -> None:
    pos = state.open_positions.get(pos_id)
    if not pos:
        return

    if not state.config.paper_mode:
        import asyncio
        tasks = []
        if pos.yes_leg.order_id and pos.yes_leg.filled_at is None:
            tasks.append(cancel_order(pos.yes_leg.order_id))
        if pos.no_leg.order_id and pos.no_leg.filled_at is None:
            tasks.append(cancel_order(pos.no_leg.order_id))
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    deployed = (pos.yes_leg.limit_price_cents + pos.no_leg.limit_price_cents) * pos.yes_leg.contracts
    state.deployed_capital_cents = max(0, state.deployed_capital_cents - deployed)
    del state.open_positions[pos_id]
    state.ui_needs_render = True
    add_scan_log(state, pos.ticker, "info", f"Cancelled position {pos_id[:8]} — {reason}")


# ─── Settle a fully-filled position ──────────────────────────────────────────

def settle_position(pos_id: str, state: AppState) -> Optional[ClosedTrade]:
    pos = state.open_positions.get(pos_id)
    if not pos:
        return None
    if pos.yes_leg.filled_at is None or pos.no_leg.filled_at is None:
        return None

    yes_cost = (pos.yes_leg.fill_price_cents or pos.yes_leg.limit_price_cents) * pos.yes_leg.contracts
    no_cost = (pos.no_leg.fill_price_cents or pos.no_leg.limit_price_cents) * pos.no_leg.contracts
    payout_cents = 100 * pos.yes_leg.contracts
    gross_profit_cents = payout_cents - yes_cost - no_cost
    total_fee_cents = pos.yes_leg.fee_cents + pos.no_leg.fee_cents
    net_profit_cents = gross_profit_cents - total_fee_cents

    trade = ClosedTrade(
        id=pos.id,
        ticker=pos.ticker,
        strategy=pos.strategy,
        contracts=pos.yes_leg.contracts,
        gross_profit_cents=gross_profit_cents,
        total_fee_cents=total_fee_cents,
        net_profit_cents=net_profit_cents,
        yes_leg_fill_time=pos.yes_leg.filled_at,
        no_leg_fill_time=pos.no_leg.filled_at,
        closed_at=time.time() * 1000,
        adverse_selection=pos.adverse_selection_flagged,
        stop_loss=pos.stop_loss_triggered,
        spread_fill_key=pos.spread_fill_key,
    )

    state.closed_trades.append(trade)
    state.daily_realized_pl_cents += net_profit_cents

    deployed = (pos.yes_leg.limit_price_cents + pos.no_leg.limit_price_cents) * pos.yes_leg.contracts
    state.deployed_capital_cents = max(0, state.deployed_capital_cents - deployed)

    if net_profit_cents < 0:
        state.consecutive_losses += 1
    else:
        state.consecutive_losses = 0

    if state.config.paper_mode:
        state.paper_trade_count += 1
    else:
        # Only train on real live trades — paper fills are simulated and unrealistic
        from learn import TRAINING_PHASE_TRADES
        if net_profit_cents < 0 and state.learning.total_trades < TRAINING_PHASE_TRADES:
            state.learning.training_losses_cents += abs(net_profit_cents)
        record_trade(trade, state.learning)
        state.config.min_profit_cents = state.learning.adapted_params["min_profit_cents"]
        state.config.kelly_fraction = state.learning.adapted_params["kelly_fraction"]

    del state.open_positions[pos_id]
    state.ui_needs_render = True
    return trade


# ─── Adverse selection market hedge ───────────────────────────────────────────

async def hedge_leg_at_market(pos_id: str, leg_side: str, state: AppState) -> None:
    pos = state.open_positions.get(pos_id)
    if not pos:
        return
    leg = pos.yes_leg if leg_side == "yes" else pos.no_leg
    if leg.filled_at is not None:
        return

    if not state.config.paper_mode and leg.order_id:
        try:
            await cancel_order(leg.order_id)
            order = await place_order(pos.ticker, str(uuid.uuid4()), "market", leg_side, leg.contracts)
            leg.order_id = order["order_id"]
        except Exception as err:
            add_scan_log(state, pos.ticker, "error", f"Hedge {leg_side} at market failed: {err}")
    else:
        leg.filled_at = time.time() * 1000
        leg.fill_price_cents = leg.limit_price_cents

    pos.adverse_selection_flagged = True
    state.ui_needs_render = True
    add_scan_log(state, pos.ticker, "info",
                 f"[ADVERSE SEL] Hedging {leg_side} at market — pos {pos_id[:8]}")


# ─── Poll live order fill status ──────────────────────────────────────────────

async def poll_order_status(pos_id: str, state: AppState) -> None:
    pos = state.open_positions.get(pos_id)
    if not pos or state.config.paper_mode:
        return
    for leg in (pos.yes_leg, pos.no_leg):
        if leg.order_id and leg.filled_at is None:
            try:
                order = await get_order(leg.order_id)
                if order.get("status") == "filled":
                    leg.filled_at = time.time() * 1000
                    leg.fill_price_cents = order.get("fill_price") or leg.limit_price_cents
                    state.ui_needs_render = True
            except Exception:
                pass  # non-fatal polling error


# ─── Single-leg directional execution ─────────────────────────────────────────

async def execute_directional(
    ticker: str,
    strategy: str,
    side: str,
    entry_price_cents: int,
    contracts: int,
    theoretical_prob: float,
    close_time: str,
    state: AppState,
) -> Optional[DirectionalPosition]:
    """Place a single taker order and create a DirectionalPosition."""
    cfg = state.config
    fee_tier = infer_market(ticker)
    now_ms = time.time() * 1000

    fee_cents = round(calc_fee(contracts, entry_price_cents / 100, fee_tier) * 100)
    pos_id = str(uuid.uuid4())
    order_id: Optional[str] = None

    if cfg.paper_mode:
        order_id = _paper_order_id()
    else:
        try:
            order = await place_order(ticker, str(uuid.uuid4()), "limit", side, contracts, entry_price_cents)
            order_id = order["order_id"]
        except Exception as err:
            add_scan_log(state, ticker, "error", f"Directional order failed: {err}")
            return None

    potential_profit = (100 - entry_price_cents) * contracts - fee_cents

    pos = DirectionalPosition(
        id=pos_id, ticker=ticker, fee_tier=fee_tier, strategy=strategy,
        side=side, entry_price_cents=entry_price_cents, contracts=contracts,
        order_id=order_id, placed_at=now_ms,
        filled_at=now_ms if cfg.paper_mode else None,
        fill_price_cents=entry_price_cents if cfg.paper_mode else None,
        fee_cents=fee_cents, close_time=close_time,
        theoretical_prob=theoretical_prob,
        potential_profit_cents=potential_profit,
    )

    state.directional_positions[pos_id] = pos
    state.deployed_capital_cents += entry_price_cents * contracts

    mode = "[PAPER] " if cfg.paper_mode else ""
    add_scan_log(state, ticker, "execute",
                 f"{mode}{strategy.upper()} {contracts}x {side.upper()}@{entry_price_cents}¢ "
                 f"| prob: {theoretical_prob:.0%} | pot: +${potential_profit / 100:.2f}")
    state.ui_needs_render = True
    return pos


def settle_directional(pos_id: str, state: AppState) -> None:
    """Remove a settled directional position (P&L captured via balance sync)."""
    pos = state.directional_positions.get(pos_id)
    if not pos:
        return
    state.deployed_capital_cents = max(0, state.deployed_capital_cents - pos.entry_price_cents * pos.contracts)
    del state.directional_positions[pos_id]
    add_scan_log(state, pos.ticker, "info",
                 f"[SETTLED] {pos.strategy.upper()} {pos.side.upper()}@{pos.entry_price_cents}¢ "
                 f"× {pos.contracts} — check balance for P&L")
    state.ui_needs_render = True


async def poll_directional_status(pos_id: str, state: AppState) -> None:
    """Poll fill status for a live directional order."""
    pos = state.directional_positions.get(pos_id)
    if not pos or state.config.paper_mode or pos.filled_at is not None:
        return
    if pos.order_id:
        try:
            order = await get_order(pos.order_id)
            if order.get("status") == "filled":
                pos.filled_at = time.time() * 1000
                pos.fill_price_cents = order.get("fill_price") or pos.entry_price_cents
                state.ui_needs_render = True
        except Exception:
            pass
