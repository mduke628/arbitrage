"""scanner.py — Market polling, opportunity detection, and execution gating."""

from __future__ import annotations

import asyncio
import math
import re
import time
from typing import Any, Optional

from kalshi_fees import calc_maker_fee, get_markets, infer_market
from executor import execute_directional, execute_position, poll_directional_status, poll_order_status
from external_data import get_spx_price
from learn import score_opportunity
from risk import clear_api_error, record_api_error, run_risk_checks
from sizer import size_directional, size_position
from state import AppState, OpenPosition, add_scan_log

_PRIVATE_KEY_BYTES: Optional[bytes] = None
_pem_path = Path(__file__).parent / "private_key.pem"
if _pem_path.exists():
    _PRIVATE_KEY_BYTES = _pem_path.read_bytes()

# Simple bearer token (KALSHI_API_TOKEN) takes priority over RSA when set.
# If not set, falls back to RSA with private_key.pem + KALSHI_API_KEY as key ID.
_KALSHI_BEARER_TOKEN: str = os.getenv("KALSHI_API_TOKEN", "")

if _KALSHI_BEARER_TOKEN:
    print("[kalshi] KALSHI_API_TOKEN found — using bearer token auth")
elif _PRIVATE_KEY_BYTES:
    print("[kalshi] RSA private key loaded — using KALSHI-ACCESS-SIGNATURE auth")
else:
    print("[kalshi] No KALSHI_API_TOKEN or private_key.pem — will attempt bearer with KALSHI_API_KEY")

# Tracks whether RSA auth succeeded this session (only relevant if bearer token not set).
# None = not yet tried, True = working, False = failed → fall back to KALSHI_API_KEY as bearer.
_kalshi_rsa_ok: Optional[bool] = None


# ─── Market field helpers ──────────────────────────────────────────────────────

def _dollars_to_cents(val: Any) -> Optional[int]:
    """Convert a dollar string/float from the Kalshi API to integer cents."""
    if val is None:
        return None
    try:
        return round(float(val) * 100)
    except (ValueError, TypeError):
        return None


def _valid_price(cents: Optional[int]) -> Optional[int]:
    """Return cents only if it's a tradeable price (1–99). 0 or 100 means no active order."""
    if cents is None:
        return None
    return cents if 1 <= cents <= 99 else None


def _normalize_market(m: dict[str, Any]) -> dict[str, Any]:
    """
    Normalise the new dollar-denominated API fields into the legacy
    cents-based yes_ask / yes_bid / no_ask / no_bid keys used by
    the rest of the scanner. Prices of 0¢ or 100¢ are treated as absent
    (no active order on that side of the book).
    """
    yes_ask = _valid_price(_dollars_to_cents(m.get("yes_ask_dollars")))
    yes_bid = _valid_price(_dollars_to_cents(m.get("yes_bid_dollars")))
    no_ask  = _valid_price(_dollars_to_cents(m.get("no_ask_dollars")))
    no_bid  = _valid_price(_dollars_to_cents(m.get("no_bid_dollars")))

    # Derive missing sides from the complementary price (YES + NO = 100¢)
    if yes_ask is None and no_bid is not None:
        yes_ask = _valid_price(100 - no_bid)
    if yes_bid is None and no_ask is not None:
        yes_bid = _valid_price(100 - no_ask)
    if no_ask is None and yes_bid is not None:
        no_ask = _valid_price(100 - yes_bid)
    if no_bid is None and yes_ask is not None:
        no_bid = _valid_price(100 - yes_ask)

    result = dict(m)
    result["yes_ask"] = yes_ask
    result["yes_bid"] = yes_bid
    result["no_ask"]  = no_ask
    result["no_bid"]  = no_bid
    return result


def _no_ask_cents(m: dict[str, Any]) -> int:
    """NO ask in cents (already normalised)."""
    v = m.get("no_ask")
    return int(v) if v is not None else 100


def _ms_to_close(close_time: str) -> float:
    from datetime import datetime
    dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
    return (dt.timestamp() - time.time()) * 1000


# ─── Paper fill simulation ────────────────────────────────────────────────────

def simulate_paper_fills(state: AppState) -> None:
    """
    Simulate order fills for paper positions.
    Fills happen after 2 × adverse_selection_ms (default 60 s) so they
    don't falsely trip the 30-second adverse-selection detector.
    """
    if not state.config.paper_mode:
        return
    now_ms = time.time() * 1000
    fill_delay = state.config.adverse_selection_ms * 2

    for pos in state.open_positions.values():
        for leg in (pos.yes_leg, pos.no_leg):
            if leg.filled_at is None and leg.placed_at is not None:
                if now_ms >= leg.placed_at + fill_delay:
                    leg.filled_at = leg.placed_at + fill_delay
                    leg.fill_price_cents = leg.limit_price_cents
                    state.ui_needs_render = True


# ─── Opportunity detection ────────────────────────────────────────────────────

def _net_per_contract(
    yes_cents: int, no_cents: int, gross_per_contract: float, contracts: int
) -> float:
    """Net profit per contract after both maker fees."""
    yes_fee = calc_maker_fee(contracts, yes_cents / 100) * 100  # dollars → cents
    no_fee = calc_maker_fee(contracts, no_cents / 100) * 100
    total_fee_cents = yes_fee + no_fee
    return gross_per_contract - total_fee_cents / contracts


def _detect_mispricing(m: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Mode 1: yes_ask + no_ask < 100¢ → guaranteed profit."""
    yes_ask = m.get("yes_ask")
    no_ask = m.get("no_ask")
    if yes_ask is None or no_ask is None:
        return None
    gross = 100 - int(yes_ask) - int(no_ask)
    if gross <= 0:
        return None
    return {"yes_limit": int(yes_ask), "no_limit": int(no_ask), "gross": gross}


def _detect_spread(m: dict[str, Any]) -> Optional[dict[str, Any]]:
    """
    Mode 2: post YES bid and NO bid (= YES ask complement) simultaneously.
    Profit = yes_ask − yes_bid when both legs fill.
    """
    yes_bid = m.get("yes_bid")
    yes_ask = m.get("yes_ask")
    if yes_bid is None or yes_ask is None:
        return None
    spread = int(yes_ask) - int(yes_bid)
    if spread <= 0:
        return None
    if spread > 15:  # skip illiquid markets where bid is far from ask
        return None
    no_bid_cents = 100 - int(yes_ask)  # our NO resting bid price
    return {"yes_limit": int(yes_bid), "no_limit": no_bid_cents, "gross": spread}


# ── INX ticker parsing ────────────────────────────────────────────────────────

_INX_RE = re.compile(r'^(INX[A-Z]*)-\d{2}[A-Z]{3}\d*-([A-Z])(\d+(?:\.\d+)?)$')


def _parse_inx_strike(ticker: str) -> Optional[tuple[str, float]]:
    """Return (direction, strike) for INX-family tickers, or None."""
    m = _INX_RE.match(ticker)
    if not m:
        return None
    code, strike_str = m.group(2), m.group(3)
    if code == 'B':
        return ('above', float(strike_str))
    if code == 'S':
        return ('below', float(strike_str))
    return None


def _spx_win_prob(spx: float, strike: float, ms_to_close: float, direction: str) -> float:
    """Probability of SPX closing above (or below) strike. Log-normal intraday vol model."""
    hours = max(0.1, ms_to_close / 3_600_000)
    vol = 0.16 * math.sqrt(hours / (252 * 6.5))   # annualised 16% vol
    z = math.log(spx / strike) / vol
    prob_above = (1.0 + math.erf(z / math.sqrt(2))) / 2.0
    return prob_above if direction == 'above' else 1.0 - prob_above


def _detect_spx_lag(m: dict[str, Any], spx: float, ms_to_close: float) -> Optional[dict[str, Any]]:
    """
    Return trade signal when SPX probability model disagrees with Kalshi by ≥12pp.
    Only fires during market hours (< 8h to close for daily contracts).
    """
    parsed = _parse_inx_strike(m.get('ticker', ''))
    if not parsed:
        return None
    direction, strike = parsed
    if ms_to_close <= 0 or ms_to_close > 8 * 3_600_000:
        return None

    yes_ask = m.get('yes_ask')
    no_ask = m.get('no_ask')
    if yes_ask is None:
        return None

    from kalshi_fees import calc_fee, infer_market
    fee_tier = infer_market(m['ticker'])
    prob = _spx_win_prob(spx, strike, ms_to_close, direction)
    kalshi_prob = yes_ask / 100.0
    LAG = 0.12
    MIN_NET = 5
    MAX_ENTRY = 95

    if prob - kalshi_prob >= LAG and yes_ask <= MAX_ENTRY:
        fee = round(calc_fee(1, yes_ask / 100, fee_tier) * 100)
        net = (100 - yes_ask) - fee
        if net >= MIN_NET:
            return {'side': 'yes', 'entry': yes_ask, 'prob': prob, 'kalshi_prob': kalshi_prob, 'net': net}

    if no_ask is not None and kalshi_prob - prob >= LAG and no_ask <= MAX_ENTRY:
        fee = round(calc_fee(1, no_ask / 100, fee_tier) * 100)
        net = (100 - no_ask) - fee
        if net >= MIN_NET:
            return {'side': 'no', 'entry': no_ask, 'prob': 1.0 - prob, 'kalshi_prob': kalshi_prob, 'net': net}

    return None


def _detect_near_settlement(m: dict[str, Any], spx: Optional[float], ms_to_close: float) -> Optional[dict[str, Any]]:
    """
    Markets closing in < 2 hours where price is 88–97¢ on one side.
    For S&P markets: uses probability model to confirm direction.
    For other markets: buys near-certain-priced side directly.
    """
    if not (2 * 60_000 <= ms_to_close <= 2 * 3_600_000):
        return None

    yes_ask = m.get('yes_ask')
    no_ask = m.get('no_ask')
    if yes_ask is None:
        return None

    ticker = m.get('ticker', '')
    from kalshi_fees import calc_fee, infer_market
    fee_tier = infer_market(ticker)
    MIN_PRICE, MAX_PRICE, MIN_NET = 88, 97, 2

    parsed = _parse_inx_strike(ticker)
    if spx is not None and parsed:
        direction, strike = parsed
        prob = _spx_win_prob(spx, strike, ms_to_close, direction)
        if prob >= 0.92 and yes_ask <= MAX_PRICE:
            fee = round(calc_fee(1, yes_ask / 100, fee_tier) * 100)
            net = (100 - yes_ask) - fee
            if net >= MIN_NET:
                return {'side': 'yes', 'entry': yes_ask, 'prob': prob, 'net': net}
        if prob <= 0.08 and no_ask is not None and no_ask <= MAX_PRICE:
            fee = round(calc_fee(1, no_ask / 100, fee_tier) * 100)
            net = (100 - no_ask) - fee
            if net >= MIN_NET:
                return {'side': 'no', 'entry': no_ask, 'prob': 1.0 - prob, 'net': net}
        return None  # S&P market but not near-certain

    # Non-S&P: buy whichever side is 88–97¢
    if MIN_PRICE <= yes_ask <= MAX_PRICE:
        fee = round(calc_fee(1, yes_ask / 100, fee_tier) * 100)
        net = (100 - yes_ask) - fee
        if net >= MIN_NET:
            return {'side': 'yes', 'entry': yes_ask, 'prob': yes_ask / 100, 'net': net}

    if no_ask is not None and MIN_PRICE <= no_ask <= MAX_PRICE:
        fee = round(calc_fee(1, no_ask / 100, fee_tier) * 100)
        net = (100 - no_ask) - fee
        if net >= MIN_NET:
            return {'side': 'no', 'entry': no_ask, 'prob': no_ask / 100, 'net': net}

    return None


def _detect_tail_risk(m: dict[str, Any], ms_to_close: float) -> Optional[dict[str, Any]]:
    """
    Markets priced 1–8¢ with 7–90 days remaining.
    Thesis: Kalshi users underprice low-probability events. Small diversified positions
    have positive EV if actual hit rate exceeds implied probability.
    Excludes financial market tickers (handled by spx_lag).
    """
    ticker = m.get('ticker', '')
    if ticker.upper().startswith('INX') or ticker.upper().startswith('NASDAQ'):
        return None

    MIN_MS = 7 * 24 * 3_600_000
    MAX_MS = 90 * 24 * 3_600_000
    if not (MIN_MS <= ms_to_close <= MAX_MS):
        return None

    yes_ask = m.get('yes_ask')
    no_ask = m.get('no_ask')
    CONTRACTS = 3

    from kalshi_fees import calc_fee, infer_market
    fee_tier = infer_market(ticker)

    if yes_ask is not None and 1 <= yes_ask <= 8:
        fee = round(calc_fee(CONTRACTS, yes_ask / 100, fee_tier) * 100)
        potential = (100 - yes_ask) * CONTRACTS - fee
        if potential > 0:
            return {'side': 'yes', 'entry': yes_ask, 'prob': yes_ask / 100,
                    'potential': potential, 'contracts': CONTRACTS}

    if no_ask is not None and 1 <= no_ask <= 8:
        fee = round(calc_fee(CONTRACTS, no_ask / 100, fee_tier) * 100)
        potential = (100 - no_ask) * CONTRACTS - fee
        if potential > 0:
            return {'side': 'no', 'entry': no_ask, 'prob': no_ask / 100,
                    'potential': potential, 'contracts': CONTRACTS}

    return None


# ─── Main scan cycle ──────────────────────────────────────────────────────────

async def run_scan_cycle(state: AppState) -> None:
    if state.paused:
        return
    if _scan_lock.locked():
        return  # previous cycle still running

    async with _scan_lock:
        await _inner_scan(state)


async def _inner_scan(state: AppState) -> None:
    state.last_scan_time = time.time() * 1000

    # Simulate paper fills first so risk checks see up-to-date fill status
    simulate_paper_fills(state)

    # Poll fill status for live positions
    if not state.config.paper_mode:
        for pos_id in list(state.open_positions.keys()):
            await poll_order_status(pos_id, state)
        for pos_id in list(state.directional_positions.keys()):
            await poll_directional_status(pos_id, state)

    # Run risk checks (stop loss, adverse selection, timeouts, settle)
    await run_risk_checks(state)

    # Fetch SPX price (non-blocking, uses cache)
    spx = await get_spx_price()
    if spx is not None:
        state.spx_price = spx
        state.spx_updated_at = time.time() * 1000

    # Fetch candidate markets
    try:
        raw_markets = await get_markets({"status": "open", "limit": 1000})
        clear_api_error(state)
    except Exception as err:
        record_api_error(state, err)
        return

    cfg = state.config

    # Filter: must close between 5 min and 7 days from now, then normalise fields
    markets = [
        _normalize_market(m) for m in raw_markets
        if m.get("close_time") and cfg.min_time_to_close_ms <= _ms_to_close(m["close_time"]) <= cfg.max_time_to_close_ms
    ]

    # Sort soonest-closing first (maximises capital velocity)
    markets.sort(key=lambda m: m.get("close_time", ""))

def parse_sportsbook_events(events: list[dict]) -> list[ArbOpportunity]:
    results = []
    for ev in events:
        base_name = f"{ev.get('home_team','?')} vs {ev.get('away_team','?')}"
        sport = ev.get("sport_key", "")
        commence_time = ev.get("commence_time", "")

        # === H2H and Totals ===
        # Group by (mkt_type, point_str, outcome_label) — same-line odds only.
        # Totals are safe: "Over 6.5" and "Under 6.5" share point_str "6.5" and
        # are genuinely complementary, so grouping by raw point is correct.
        best: dict[tuple, tuple[float, str]] = {}
        for bm in ev.get("bookmakers", []):
            if bm.get("key") not in _BETTABLE_SET:
                continue
            for mkt in bm.get("markets", []):
                mkt_type = mkt["key"]
                if mkt_type not in ("h2h", "totals"):
                    continue
                for out in mkt["outcomes"]:
                    point = out.get("point")
                    point_str = str(point) if point is not None else ""
                    label = f"{out['name']} {point_str}".strip() if point is not None else out["name"]
                    price = out["price"]
                    if price <= 1.0:
                        continue  # invalid odds — would cause division by zero
                    key = (mkt_type, point_str, label)
                    if key not in best or price > best[key][0]:
                        best[key] = (price, bm["title"])

        groups: dict[tuple, list[Leg]] = {}
        for (mkt_type, point_str, label), (dec, book) in best.items():
            groups.setdefault((mkt_type, point_str), []).append(Leg(
                book=book, outcome=label, decimal_odds=dec,
                american_odds=decimal_to_american(dec), implied_prob=implied_prob(dec),
            ))

        for (mkt_type, point_str), legs in groups.items():
            if len(legs) < 2:
                continue
            total_impl = sum(l.implied_prob for l in legs)
            edge = (1 - total_impl) * 100
            tag = _MARKET_LABELS.get(mkt_type, mkt_type)
            suffix = f" [{tag} {point_str}]" if point_str else f" [{tag}]"
            results.append(ArbOpportunity(
                event_name=base_name + suffix,
                sport=sport, commence_time=commence_time,
                legs=legs, total_implied=total_impl,
                edge_pct=round(edge, 4), is_arb=total_impl < 1.0,
                source="sportsbook",
            ))

        # === Spreads ===
        # Spreads CANNOT use raw point grouping. "Team A -1.5" and "Team B -1.5"
        # are NOT complementary — both can lose if the margin is exactly 1.
        # A valid spread arb must pair (Team A -X) with (Team B +X).
        # We build a lookup keyed by (team_name, point_value), then explicitly
        # pair each negative-point leg with the opposing team's positive-point leg.
        spread_best: dict[tuple, tuple[float, str]] = {}
        for bm in ev.get("bookmakers", []):
            if bm.get("key") not in _BETTABLE_SET:
                continue
            for mkt in bm.get("markets", []):
                if mkt["key"] != "spreads":
                    continue
                for out in mkt["outcomes"]:
                    point = out.get("point")
                    if point is None:
                        continue
                    price = out["price"]
                    if price <= 1.0:
                        continue  # invalid odds
                    key = (out["name"], point)
                    if key not in spread_best or price > spread_best[key][0]:
                        spread_best[key] = (price, bm["title"])

        abs_points = {abs(p) for (_, p) in spread_best if abs(p) > 0}
        for abs_p in abs_points:
            neg_side = {t: v for (t, p), v in spread_best.items() if p == -abs_p}
            pos_side = {t: v for (t, p), v in spread_best.items() if p == abs_p}
            for team_neg, (dec_neg, book_neg) in neg_side.items():
                for team_pos, (dec_pos, book_pos) in pos_side.items():
                    if team_neg == team_pos:
                        continue  # same team on both sides — not a valid cover
                    legs = [
                        Leg(book=book_neg, outcome=f"{team_neg} -{abs_p}",
                            decimal_odds=dec_neg, american_odds=decimal_to_american(dec_neg),
                            implied_prob=implied_prob(dec_neg)),
                        Leg(book=book_pos, outcome=f"{team_pos} +{abs_p}",
                            decimal_odds=dec_pos, american_odds=decimal_to_american(dec_pos),
                            implied_prob=implied_prob(dec_pos)),
                    ]
                    total_impl = sum(l.implied_prob for l in legs)
                    edge = (1 - total_impl) * 100
                    results.append(ArbOpportunity(
                        event_name=f"{base_name} [Spread ±{abs_p}]",
                        sport=sport, commence_time=commence_time,
                        legs=legs, total_implied=total_impl,
                        edge_pct=round(edge, 4), is_arb=total_impl < 1.0,
                        source="sportsbook",
                    ))
    return results


# ---------------------------------------------------------------------------
# Kalshi API  (prediction markets)
# ---------------------------------------------------------------------------

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"
KALSHI_FEE_COEF = 0.07  # taker fee: $0.07 × C × (1−C) per $1 contract, where C = price in dollars


def _kalshi_bearer_headers(api_key: str) -> dict:
    return {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}


def _kalshi_rsa_headers(method: str, path: str, api_key: str) -> dict:
    """Build RSA-signed Kalshi headers. Kalshi requires RSA-PSS with SHA256."""
    ts_ms = str(int(time.time() * 1000))
    msg = (ts_ms + method.upper() + path).encode("utf-8")
    pk = serialization.load_pem_private_key(_PRIVATE_KEY_BYTES, password=None)
    sig = pk.sign(
        msg,
        _asym_padding.PSS(
            mgf=_asym_padding.MGF1(hashes.SHA256()),
            salt_length=_asym_padding.PSS.MAX_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type": "application/json",
    }


def _kalshi_headers(method: str, path: str, api_key: str) -> dict:
    """
    Return Kalshi auth headers.
    Priority: KALSHI_API_TOKEN bearer > RSA (private_key.pem) > KALSHI_API_KEY bearer.
    """
    if _KALSHI_BEARER_TOKEN:
        return _kalshi_bearer_headers(_KALSHI_BEARER_TOKEN)
    if _PRIVATE_KEY_BYTES and _CRYPTO_OK and _kalshi_rsa_ok is not False:
        return _kalshi_rsa_headers(method, path, api_key)
    return _kalshi_bearer_headers(api_key)


async def _kalshi_get(
    session: aiohttp.ClientSession, url: str, path: str, api_key: str
) -> dict:
    """
    GET a Kalshi endpoint with automatic RSA → bearer fallback on 401.
    Returns the parsed JSON dict.  Raises ValueError on persistent auth failure.
    """
    global _kalshi_rsa_ok

    async def _try(hdrs: dict) -> tuple[int, dict]:
        async with session.get(url, headers=hdrs, timeout=aiohttp.ClientTimeout(total=20)) as r:
            return r.status, (await r.json() if r.status == 200 else {})

    headers = _kalshi_headers("GET", path, api_key)
    status, data = await _try(headers)

    if status == 401 and _PRIVATE_KEY_BYTES and _CRYPTO_OK and _kalshi_rsa_ok is not False:
        # RSA auth failed — the KALSHI_API_KEY is a bearer token, not an RSA key ID.
        # Switch to bearer for the rest of this session.
        _kalshi_rsa_ok = False
        print("[kalshi] RSA auth returned 401 — switching to bearer token auth")
        status, data = await _try(_kalshi_bearer_headers(api_key))

    if status == 401:
        raise ValueError("Kalshi API key invalid or unauthorized")
    if status != 200:
        print(f"  [kalshi] HTTP {status}")
        return {}

    if _kalshi_rsa_ok is None and _PRIVATE_KEY_BYTES and _CRYPTO_OK:
        _kalshi_rsa_ok = True  # RSA confirmed working

    return data


async def fetch_kalshi_raw(session: aiohttp.ClientSession, api_key: str) -> list[dict]:
    """
    Fetch all open Kalshi markets, paginating via cursor.
    Returns raw market dicts filtered to sports-like markets.
    """
    markets: list[dict] = []
    cursor: Optional[str] = None
    path = "/trade-api/v2/markets"

    while True:
        url = f"{KALSHI_BASE}/markets?limit=1000&status=open"
        if cursor:
            url += f"&cursor={cursor}"
        try:
            data = await _kalshi_get(session, url, path, api_key)
        except Exception as e:
            print(f"  [kalshi] error: {e}")
            break
        if not data:
            break

        markets.extend(data.get("markets", []))
        cursor = data.get("cursor") or None
        if not cursor:
            break

    sports_markets = [m for m in markets if _looks_like_sports_market(m)]
    print(f"  [kalshi] {len(markets)} open markets ({len(sports_markets)} sports-like)")
    return sports_markets


async def fetch_kalshi_markets(session: aiohttp.ClientSession, api_key: str) -> list[ArbOpportunity]:
    """Convenience wrapper: fetch + parse into ArbOpportunity list."""
    raw = await fetch_kalshi_raw(session, api_key)
    return parse_kalshi_markets(raw)


def _looks_like_sports_market(market: dict) -> bool:
    """Best-effort filter for sports-related Kalshi contracts."""
    category = str(market.get("category") or market.get("event_category") or "").lower()
    if "sport" in category:
        return True

    text = " ".join([
        str(market.get("title", "")),
        str(market.get("subtitle", "")),
        str(market.get("ticker", "")),
        str(market.get("series_ticker", "")),
        str(market.get("event_ticker", "")),
    ]).lower()
    sports_tokens = {
        "nfl", "nba", "mlb", "nhl", "wnba", "ufc", "mma", "pga", "golf",
        "soccer", "premier league", "champions league", "tennis", "ncaa", "ncaaf", "ncaab",
        "super bowl", "world cup", "olympics", "f1", "nascar", "cricket", "boxing",
        "rangers", "knicks", "mets", "yankees", "giants", "jets", "devils", "islanders",
    }
    return any(token in text for token in sports_tokens)


def parse_kalshi_markets(markets: list[dict]) -> list[ArbOpportunity]:
    results = []
    for m in markets:
        if m.get("yes_ask") is None:
            continue
        for pos in state.open_positions.values():
            if pos.ticker == m["ticker"]:
                pos.last_known_yes_ask_cents = int(m["yes_ask"])
                pos.last_known_no_ask_cents = _no_ask_cents(m)

    n_no_edge = 0
    n_low_profit = 0
    n_executed = 0

    # Tickers already held (no double-entry across all strategy types)
    held_tickers = {p.ticker for p in state.open_positions.values()} | \
                   {p.ticker for p in state.directional_positions.values()}

    for m in markets:
        if state.paused:
            break
        if len(state.open_positions) + len(state.directional_positions) >= cfg.max_open_positions:
            break

        ticker = m["ticker"]
        yes_ask = m.get("yes_ask")
        yes_bid = m.get("yes_bid")
        ms_to_close = _ms_to_close(m["close_time"]) if m.get("close_time") else 0

        # ── Strategy 1: True mispricing (two-leg, guaranteed profit) ──────────
        if yes_ask is not None and yes_bid is not None and ticker not in held_tickers:
            misp = _detect_mispricing(m)
            if misp:
                score = score_opportunity(ticker, "mispricing", state.learning)
                if not score["allowed"]:
                    add_scan_log(state, ticker, "skip",
                                 f"[LEARN] mispricing skipped — {score['skip_reason']}")
                    n_low_profit += 1
                else:
                    result = await _evaluate_and_execute(
                        ticker, "mispricing",
                        misp["yes_limit"], misp["no_limit"],
                        misp["gross"] * score["confidence_multiplier"],
                        m["close_time"], int(yes_ask), _no_ask_cents(m),
                        score["notes"], score.get("spread_fill_key"),
                        score.get("kelly_confidence", 1.0), state,
                    )
                    if result == "executed":
                        n_executed += 1
                        held_tickers.add(ticker)
                    else:
                        n_low_profit += 1

        # ── Strategy 2: Spread capture (two-leg) ─────────────────────────────
        if yes_ask is not None and yes_bid is not None and ticker not in held_tickers:
            sprd = _detect_spread(m)
            if sprd and not _detect_mispricing(m):
                score = score_opportunity(
                    ticker, "spread", state.learning,
                    spread_cents=sprd["gross"],
                    ms_to_close=ms_to_close,
                    yes_bid_cents=sprd["yes_limit"],
                )
                if not score["allowed"]:
                    add_scan_log(state, ticker, "skip",
                                 f"[LEARN] spread skipped — {score['skip_reason']}")
                    n_low_profit += 1
                else:
                    result = await _evaluate_and_execute(
                        ticker, "spread",
                        sprd["yes_limit"], sprd["no_limit"],
                        sprd["gross"] * score["confidence_multiplier"],
                        m["close_time"], int(yes_ask), _no_ask_cents(m),
                        score["notes"], score.get("spread_fill_key"),
                        score.get("kelly_confidence", 1.0), state,
                    )
                    if result == "executed":
                        n_executed += 1
                        held_tickers.add(ticker)
                    else:
                        n_low_profit += 1

        # ── Strategy 3: S&P information lag (directional, taker) ─────────────
        if spx is not None and ticker not in held_tickers:
            lag = _detect_spx_lag(m, spx, ms_to_close)
            if lag:
                contracts = size_directional(ticker, lag['entry'], 'spx_lag', 50, state)
                if contracts > 0:
                    pos = await execute_directional(
                        ticker, 'spx_lag', lag['side'], lag['entry'],
                        contracts, lag['prob'], m["close_time"], state,
                    )
                    if pos:
                        n_executed += 1
                        held_tickers.add(ticker)
                        add_scan_log(state, ticker, "opportunity",
                                     f"SPX_LAG {lag['side'].upper()}@{lag['entry']}¢ "
                                     f"model={lag['prob']:.0%} kalshi={lag['kalshi_prob']:.0%} "
                                     f"net={lag['net']}¢")

        # ── Strategy 4: Near-settlement value (directional, taker) ───────────
        if ticker not in held_tickers:
            nsv = _detect_near_settlement(m, spx, ms_to_close)
            if nsv:
                contracts = size_directional(ticker, nsv['entry'], 'near_settlement', 20, state)
                if contracts > 0:
                    pos = await execute_directional(
                        ticker, 'near_settlement', nsv['side'], nsv['entry'],
                        contracts, nsv['prob'], m["close_time"], state,
                    )
                    if pos:
                        n_executed += 1
                        held_tickers.add(ticker)
                        add_scan_log(state, ticker, "opportunity",
                                     f"NEAR_SETTLE {nsv['side'].upper()}@{nsv['entry']}¢ "
                                     f"prob={nsv['prob']:.0%} net={nsv['net']}¢")

        # Count no-edge if none of the above strategies fired on this market
        if yes_ask is None:
            n_no_edge += 1
        elif ticker not in held_tickers:
            has_misp = _detect_mispricing(m) is not None
            has_sprd = _detect_spread(m) is not None
            has_lag = (spx is not None and _detect_spx_lag(m, spx, ms_to_close) is not None)
            has_nsv = _detect_near_settlement(m, spx, ms_to_close) is not None
            if not has_misp and not has_sprd and not has_lag and not has_nsv:
                n_no_edge += 1

    # ── Strategy 5: Tail risk scan (every 5th cycle, 7–90 day markets) ───────
    state.tail_scan_counter = (state.tail_scan_counter + 1) % 5
    if state.tail_scan_counter == 0:
        n_executed += await _tail_risk_scan(state, held_tickers)

    # Store stats for the summary panel and always log the scan summary
    state.last_scan_stats = (len(markets), n_executed, n_low_profit, n_no_edge)
    state.ui_needs_render = True
    spx_str = f" SPX=${state.spx_price:,.0f}" if state.spx_price else ""
    add_scan_log(state, "GLOBAL", "scan",
                 f"{len(markets)} mkts — {n_executed} executed | "
                 f"{n_low_profit} skipped | {n_no_edge} no edge{spx_str}")


async def _tail_risk_scan(state: AppState, held_tickers: set) -> int:
    """Fetch 7–90 day markets and hunt for underpriced tail risks."""
    TAIL_MIN_MS = 7 * 24 * 3_600_000
    TAIL_MAX_MS = 90 * 24 * 3_600_000
    n_executed = 0
    try:
        raw = await get_markets({"status": "open", "limit": 1000})
    except Exception:
        return 0

    for m in raw:
        if state.paused:
            break
        if len(state.open_positions) + len(state.directional_positions) >= state.config.max_open_positions:
            break
        close_time = m.get("close_time")
        if not close_time:
            continue
        ms_to_close = _ms_to_close(close_time)
        if not (TAIL_MIN_MS <= ms_to_close <= TAIL_MAX_MS):
            continue
        m = _normalize_market(m)
        ticker = m["ticker"]
        if ticker in held_tickers:
            continue
        tail = _detect_tail_risk(m, ms_to_close)
        if tail:
            contracts = size_directional(ticker, tail['entry'], 'tail_risk', tail['contracts'], state)
            if contracts > 0:
                pos = await execute_directional(
                    ticker, 'tail_risk', tail['side'], tail['entry'],
                    contracts, tail['prob'], close_time, state,
                )
                if pos:
                    n_executed += 1
                    held_tickers.add(ticker)
                    add_scan_log(state, ticker, "opportunity",
                                 f"TAIL {tail['side'].upper()}@{tail['entry']}¢ "
                                 f"pot=+${tail['potential'] / 100:.2f}")
    return n_executed


async def _evaluate_and_execute(
    ticker: str,
    strategy: str,
    yes_limit: int,
    no_limit: int,
    gross_per_contract: float,
    close_time: str,
    current_yes_ask: int,
    current_no_ask: int,
    learning_notes: list[str],
    spread_fill_key: Optional[str],
    kelly_confidence: float,
    state: AppState,
) -> str:
    """Returns 'executed' or 'skipped'."""
    label = "MISP" if strategy == "mispricing" else "SPRD"
    cfg = state.config

    # Estimate net at 1 contract (conservative fee estimate)
    net_at_1 = _net_per_contract(yes_limit, no_limit, gross_per_contract, 1)

    # For mispricing: enforce min_profit_cents (no EV model to gate it).
    # For spread: EV model already approved this trade — skip the redundant gate.
    if strategy == "mispricing" and net_at_1 < cfg.min_profit_cents:
        add_scan_log(state, ticker, "skip",
                     f"MISP: net {net_at_1:.1f}¢ < min {cfg.min_profit_cents}¢")
        return "skipped"

    if net_at_1 <= 0:
        add_scan_log(state, ticker, "skip",
                     f"{label}: net {net_at_1:.1f}¢ — negative after fees")
        return "skipped"

    size = size_position(ticker, net_at_1, yes_limit, no_limit, state, kelly_confidence)
    if size.contracts == 0:
        add_scan_log(state, ticker, "skip", f"{label}: {size.reason or 'size = 0'}")
        return "skipped"

    # Recompute at actual size (fees scale non-linearly)
    net_actual = _net_per_contract(yes_limit, no_limit, gross_per_contract, size.contracts)
    if net_actual <= 0:
        add_scan_log(state, ticker, "skip",
                     f"{label}: net {net_actual:.1f}¢ at {size.contracts}x — negative after fees")
        return "skipped"

    note_str = f" [learn: {'; '.join(learning_notes)}]" if learning_notes else ""
    add_scan_log(state, ticker, "opportunity",
                 f"{label}: {size.contracts}x YES@{yes_limit}¢ + NO@{no_limit}¢ "
                 f"| gross: {gross_per_contract:.1f}¢ | net: {net_actual:.1f}¢/contract{note_str}")

    pos = await execute_position(
        ticker=ticker,
        strategy=strategy,
        yes_limit_cents=yes_limit,
        no_limit_cents=no_limit,
        contracts=size.contracts,
        expected_profit_cents_per_contract=net_actual,
        close_time=close_time,
        current_yes_ask_cents=current_yes_ask,
        current_no_ask_cents=current_no_ask,
        state=state,
    )
    if pos and spread_fill_key:
        pos.spread_fill_key = spread_fill_key
    return "executed"
