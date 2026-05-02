"""
kalshi_fees.py
──────────────────────────────────────────────────────────────────────────────
Kalshi fee calculator + async market data fetcher.

Fee schedule source: https://kalshi.com/fee-schedule (effective Feb 5, 2026)
API base:            https://api.elections.kalshi.com/trade-api/v2

SETUP
  Add to your .env file:
    KALSHI_API_KEY=your_key_here
"""

from __future__ import annotations

import math
import os
from typing import Any, Literal

import httpx

# ─── Config ───────────────────────────────────────────────────────────────────

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

FeeMarket = Literal["GENERAL", "INX", "NASDAQ100"]

TAKER_RATES: dict[str, float] = {
    "GENERAL": 0.07,
    "INX": 0.035,
    "NASDAQ100": 0.035,
}

MAKER_RATE = 0.0175

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _round_up_to_cent(value: float) -> float:
    """Round up to the nearest cent (Kalshi always rounds in their favour)."""
    return math.ceil(value * 100) / 100


def _api_key() -> str:
    key = os.environ.get("KALSHI_API_KEY")
    if not key:
        raise RuntimeError("KALSHI_API_KEY is not set in environment variables.")
    return key


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
    }


# ─── Fee tier inference ────────────────────────────────────────────────────────

def infer_market(ticker: str) -> str:
    """
    Infer the FeeMarket tier from a Kalshi ticker string.

    >>> infer_market("INXD-25APR28-B5550")
    'INX'
    >>> infer_market("NASDAQ100W-25APR")
    'NASDAQ100'
    >>> infer_market("KXBTCD")
    'GENERAL'
    """
    t = ticker.upper()
    if t.startswith("NASDAQ100"):
        return "NASDAQ100"
    if t.startswith("INX"):
        return "INX"
    return "GENERAL"


# ─── Fee calculators ──────────────────────────────────────────────────────────

def calc_fee(contracts: int, price: float, market: str = "GENERAL") -> float:
    """
    Taker fee for an immediately-matched order.

    Formula: roundUp(rate × C × P × (1 − P))

    :param contracts: number of contracts
    :param price:     price in dollars (0–1), e.g. 0.50 for 50¢
    :param market:    fee tier; use infer_market(ticker) first
    :returns:         fee in dollars, rounded up to nearest cent
    """
    if not 0 <= price <= 1:
        raise ValueError(f"price must be 0–1, got {price}")
    if contracts <= 0:
        raise ValueError(f"contracts must be > 0, got {contracts}")
    return _round_up_to_cent(TAKER_RATES[market] * contracts * price * (1 - price))


def calc_maker_fee(contracts: int, price: float) -> float:
    """
    Maker fee for a resting order that later gets matched.

    Formula: roundUp(0.0175 × C × P × (1 − P))

    :param contracts: number of contracts
    :param price:     price in dollars (0–1)
    :returns:         fee in dollars, rounded up to nearest cent
    """
    if not 0 <= price <= 1:
        raise ValueError(f"price must be 0–1, got {price}")
    if contracts <= 0:
        raise ValueError(f"contracts must be > 0, got {contracts}")
    return _round_up_to_cent(MAKER_RATE * contracts * price * (1 - price))


def trade_cost(
    contracts: int,
    price: float,
    market: str = "GENERAL",
    is_maker: bool = False,
) -> dict[str, float]:
    """
    Full trade cost breakdown: principal + fee + total.

    :returns: {"principal": float, "fee": float, "total": float}
    """
    principal = round(contracts * price * 100) / 100
    fee = calc_maker_fee(contracts, price) if is_maker else calc_fee(contracts, price, market)
    return {"principal": principal, "fee": fee, "total": round((principal + fee) * 100) / 100}


# ─── Async Kalshi API client ───────────────────────────────────────────────────

async def get_market(ticker: str) -> dict[str, Any]:
    """
    Fetch a single market by ticker. Adds 'fee_tier' field.
    Prices are in cents as returned by the API.
    """
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{BASE_URL}/markets/{ticker}",
            headers=_headers(),
            timeout=10.0,
        )
    if not r.is_success:
        raise RuntimeError(f"Kalshi API {r.status_code} for '{ticker}': {r.text}")
    data = r.json()
    market: dict[str, Any] = data.get("market", data)
    market["fee_tier"] = infer_market(ticker)
    return market


async def get_markets(params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    """
    Fetch a list of markets with optional filters.

    Common params: status, series_ticker, limit, cursor
    """
    async with httpx.AsyncClient() as client:
        r = await client.get(
            f"{BASE_URL}/markets",
            headers=_headers(),
            params={k: str(v) for k, v in (params or {}).items()},
            timeout=15.0,
        )
    if not r.is_success:
        raise RuntimeError(f"Kalshi API {r.status_code}: {r.text}")
    return r.json().get("markets", [])


async def price_out(ticker: str, contracts: int, is_maker: bool = False) -> dict[str, Any]:
    """
    Fetch live market data and return a complete fee breakdown.
    """
    market = await get_market(ticker)
    price_cents = (
        market.get("yes_ask")
        or market.get("last_price")
        or round((market.get("yes_bid", 0) + market.get("yes_ask", market.get("yes_bid", 0))) / 2)
    )
    price = price_cents / 100
    fee_tier = market["fee_tier"]
    return {
        "ticker": ticker,
        "fee_tier": fee_tier,
        "price_used": price,
        "contracts": contracts,
        "breakdown": trade_cost(contracts, price, fee_tier, is_maker),
    }
