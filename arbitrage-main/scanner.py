"""
Arbitrage Scanner - Core Engine
Fetches odds from The Odds API (sharp books only: Pinnacle, Bookmaker, Circa)
and Kalshi (prediction markets). Detects +EV Kalshi contracts vs. the averaged
sharp line, and submits limit orders automatically when EV >= threshold.
"""

import asyncio
import aiohttp
import base64
import json
import os
import re
import time
import uuid
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone

# RSA auth for Kalshi — loaded once at module init from private_key.pem
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as _asym_padding
    _CRYPTO_OK = True
except ImportError:
    _CRYPTO_OK = False

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


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Leg:
    book: str
    outcome: str
    decimal_odds: float
    american_odds: str
    implied_prob: float
    market_url: str = ""

@dataclass
class ArbOpportunity:
    event_name: str
    sport: str
    commence_time: str
    legs: list[Leg]
    total_implied: float
    edge_pct: float          # positive = arb exists
    is_arb: bool
    source: str              # "sportsbook" | "kalshi" | "cross"

    def stakes_for(self, total_stake: float) -> list[float]:
        """Return optimal stake per leg so payout is equal on all outcomes."""
        return [(total_stake / self.total_implied) * l.implied_prob for l in self.legs]

    def guaranteed_profit(self, total_stake: float) -> float:
        stakes = self.stakes_for(total_stake)
        payout = stakes[0] * self.legs[0].decimal_odds
        return payout - total_stake

    def to_dict(self) -> dict:
        d = asdict(self)
        d["guaranteed_profit_1000"] = round(self.guaranteed_profit(1000), 2)
        d["stakes_1000"] = [round(s, 2) for s in self.stakes_for(1000)]
        return d


# ---------------------------------------------------------------------------
# Odds utilities
# ---------------------------------------------------------------------------

def decimal_to_american(dec: float) -> str:
    if dec <= 1.0:
        return "N/A"
    if dec >= 2.0:
        return f"+{round((dec - 1) * 100)}"
    return str(round(-100 / (dec - 1)))

def american_to_decimal(american: float) -> float:
    if american > 0:
        return (american / 100) + 1
    return (100 / abs(american)) + 1

def implied_prob(decimal_odds: float) -> float:
    if decimal_odds <= 0:
        return 1.0
    return 1 / decimal_odds


# ---------------------------------------------------------------------------
# The Odds API  (sportsbooks: DraftKings, FanDuel, BetMGM, Caesars, bet365, Fanatics, etc.)
# ---------------------------------------------------------------------------

ODDS_API_BASE = "https://api.the-odds-api.com/v4"

# Sharp reference books — used ONLY for de-vig / fair-line estimation.
# Kalshi is the only book we actually bet on; these are price references only.
# Averaging across all three reduces noise vs. using a single book.
SHARP_BOOKS = ["pinnacle", "bookmaker", "circa_sports"]
SHARP_BOOK_LABELS = {
    "pinnacle":    "Pinnacle",
    "bookmaker":   "Bookmaker.eu",
    "circa_sports":"Circa Sports",
}

SPORTS = [
    # American Football
    "americanfootball_nfl",
    "americanfootball_ncaaf",
    "americanfootball_cfl",
    "americanfootball_xfl",
    "americanfootball_ufl",
    # Basketball
    "basketball_nba",
    "basketball_ncaab",
    "basketball_wnba",
    "basketball_euroleague",
    # Baseball
    "baseball_mlb",
    "baseball_llws",
    # Ice Hockey
    "icehockey_nhl",
    "icehockey_sweden_hockey_league",
    "icehockey_sweden_allsvenskan",
    "icehockey_ahl",
    # Soccer — Top leagues
    "soccer_epl",
    "soccer_germany_bundesliga",
    "soccer_spain_la_liga",
    "soccer_italy_serie_a",
    "soccer_france_ligue_one",
    "soccer_netherlands_eredivisie",
    "soccer_portugal_primeira_liga",
    "soccer_usa_mls",
    "soccer_uefa_champs_league",
    "soccer_uefa_europa_league",
    "soccer_uefa_europa_conference_league",
    "soccer_world_cup",
    "soccer_conmebol_copa_america",
    "soccer_uefa_euro",
    # Soccer — Secondary leagues
    "soccer_england_efl_champ",
    "soccer_england_league1",
    "soccer_england_league2",
    "soccer_england_fa_cup",
    "soccer_england_efl_cup",
    "soccer_spain_segunda_division",
    "soccer_germany_bundesliga2",
    "soccer_italy_serie_b",
    "soccer_france_ligue_two",
    "soccer_scotland_premier_league",
    "soccer_turkey_super_league",
    "soccer_greece_super_league",
    "soccer_denmark_superliga",
    "soccer_norway_eliteserien",
    "soccer_sweden_allsvenskan",
    "soccer_finland_veikkausliiga",
    "soccer_poland_ekstraklasa",
    "soccer_belgium_first_div",
    "soccer_switzerland_superleague",
    "soccer_austria_bundesliga",
    "soccer_czech_liga",
    "soccer_romania_liga1",
    "soccer_croatia_hnl",
    "soccer_ukraine_premier_league",
    "soccer_ireland_premier",
    "soccer_australia_aleague",
    "soccer_brazil_campeonato",
    "soccer_brazil_serie_b",
    "soccer_mexico_ligamx",
    "soccer_argentina_primera_division",
    "soccer_conmebol_copa_libertadores",
    "soccer_korea_kleague1",
    "soccer_japan_j_league",
    "soccer_china_superleague",
    "soccer_usa_nwsl",
    "soccer_africa_cup_of_nations",
    # Tennis
    "tennis_atp_french_open",
    "tennis_wta_french_open",
    "tennis_atp_wimbledon",
    "tennis_wta_wimbledon",
    "tennis_atp_us_open",
    "tennis_wta_us_open",
    "tennis_atp_australian_open",
    "tennis_wta_australian_open",
    # Combat sports
    "mma_mixed_martial_arts",
    "boxing_boxing",
    # Golf
    "golf_pga_championship",
    "golf_masters_tournament",
    "golf_the_open_championship",
    "golf_us_open",
    "golf_pga_tour",
    # Cricket
    "cricket_icc_world_cup",
    "cricket_big_bash",
    "cricket_odi",
    "cricket_test_match",
    "cricket_ipl",
    "cricket_t20_wc",
    # Rugby
    "rugbyleague_nrl",
    "rugbyunion_premiership",
    "rugbyunion_super_rugby",
    "rugbyunion_six_nations",
    "rugbyunion_world_cup",
    "rugbyunion_united_rugby_championship",
    # Australian Rules
    "aussierules_afl",
    # Darts
    "darts_betway_premier_league",
]


# ---------------------------------------------------------------------------
# +EV detection
# ---------------------------------------------------------------------------

def _devig_probs(raw_probs: list[float]) -> list[float]:
    """Power de-vig: find k > 1 s.t. Σ(p_i^k) = 1, then fair_p_i = p_i^k.

    Unlike multiplicative de-vig (divide by sum), this applies proportionally
    more margin removal to underdogs — matching how bookmakers actually distribute
    vig and preventing longshot inflation in EV calculations.
    """
    if sum(raw_probs) <= 1.0:
        return raw_probs

    # Binary search for k in (1, 20): f(k) = Σ(p_i^k) is strictly decreasing.
    # At k=1 the sum equals the raw overround (> 1); at k=20 it's near 0.
    lo, hi = 1.0, 20.0
    for _ in range(64):
        k = (lo + hi) / 2
        if sum(p ** k for p in raw_probs) > 1.0:
            lo = k
        else:
            hi = k

    result = [p ** k for p in raw_probs]
    total = sum(result)
    return [r / total for r in result]  # normalize to correct float drift


@dataclass
class PlusEVBet:
    event_name: str
    sport: str
    commence_time: str
    leg: Leg
    sharp_prob: float       # de-vigged true probability from sharp book
    ev_pct: float           # (fair_prob * decimal_odds - 1) * 100
    sharp_book: str
    sharp_raw_prob: float = 0.0   # raw implied prob before de-vig
    kalshi_ticker: str = ""       # Kalshi market ticker; set for Kalshi auto-trade bets
    kalshi_side: str = ""         # "yes" or "no"
    kalshi_ask_cents: int = 0     # current ask price in cents for limit order

    def to_dict(self) -> dict:
        return {
            "event_name": self.event_name,
            "sport": self.sport,
            "commence_time": self.commence_time,
            "leg": asdict(self.leg),
            "sharp_prob": self.sharp_prob,
            "sharp_raw_prob": self.sharp_raw_prob,
            "ev_pct": self.ev_pct,
            "sharp_book": self.sharp_book,
            "kalshi_ticker": self.kalshi_ticker,
            "kalshi_side": self.kalshi_side,
            "kalshi_ask_cents": self.kalshi_ask_cents,
            "source": "ev",
        }


def _avg_sharp_fair(bookmakers: list[dict]) -> tuple[dict[str, float], dict[str, float], str]:
    """
    Collect all sharp books present for an event and return averaged
    de-vigged fair probabilities, raw implied probs, and a label string.
    Returns (fair_dict, raw_dict, sharp_label). Empty dicts if no sharp data.
    """
    all_fair: dict[str, list[float]] = {}
    all_raw:  dict[str, list[float]] = {}
    titles_seen: list[str] = []

    for sharp_key in SHARP_BOOKS:
        for bm in bookmakers:
            if bm.get("key") != sharp_key:
                continue
            for mkt in bm.get("markets", []):
                if mkt["key"] != "h2h" or len(mkt.get("outcomes", [])) < 2:
                    continue
                raw_pairs = [
                    (out["name"], implied_prob(out["price"]))
                    for out in mkt["outcomes"]
                    if out.get("price", 0) > 1.0
                ]
                if len(raw_pairs) < 2:
                    continue
                names, probs = zip(*raw_pairs)
                fair_list = _devig_probs(list(probs))
                for n, r, f in zip(names, probs, fair_list):
                    key = n.lower()  # M6: normalize case for cross-book averaging
                    all_raw.setdefault(key, []).append(r)
                    all_fair.setdefault(key, []).append(f)
                titles_seen.append(bm["title"])
                break  # one h2h market per book is enough

    if not all_fair:
        return {}, {}, ""

    fair = {n: sum(ps) / len(ps) for n, ps in all_fair.items()}
    raw  = {n: sum(ps) / len(ps) for n, ps in all_raw.items()}
    n_books = len(set(titles_seen))
    unique_titles = list(dict.fromkeys(titles_seen))
    if n_books > 1:
        label = f"Avg({', '.join(unique_titles)})"
    else:
        label = unique_titles[0]
    return fair, raw, label


def _avg_sharp_spreads(bookmakers: list[dict]) -> dict[tuple[str, float], float]:
    """
    Returns {(team_name, spread_point): avg_de-vigged_fair_prob}.
    spread_point matches the value in the Odds API (negative for the favorite).
    Key: ("Boston Celtics", -23.5) → probability that Boston covers -23.5.
    """
    all_data: dict[tuple[str, float], list[float]] = {}
    for sharp_key in SHARP_BOOKS:
        for bm in bookmakers:
            if bm.get("key") != sharp_key:
                continue
            for mkt in bm.get("markets", []):
                if mkt["key"] != "spreads":
                    continue
                pairs = [
                    (out["name"], out["point"], implied_prob(out["price"]))
                    for out in mkt.get("outcomes", [])
                    if out.get("price", 0) > 1.0 and out.get("point") is not None
                ]
                if len(pairs) < 2:
                    continue
                names, points, probs = zip(*pairs)
                fair_list = _devig_probs(list(probs))
                for n, p, f in zip(names, points, fair_list):
                    all_data.setdefault((n.lower(), float(p)), []).append(f)  # M6: normalize case
    return {k: sum(vs) / len(vs) for k, vs in all_data.items()}


def _avg_sharp_totals(bookmakers: list[dict]) -> dict[tuple[float, str], float]:
    """
    Returns {(point_value, "over"/"under"): avg_de-vigged_fair_prob}.
    Key: (205.5, "over") → probability the total goes Over 205.5.
    """
    all_data: dict[tuple[float, str], list[float]] = {}
    for sharp_key in SHARP_BOOKS:
        for bm in bookmakers:
            if bm.get("key") != sharp_key:
                continue
            for mkt in bm.get("markets", []):
                if mkt["key"] != "totals":
                    continue
                pairs = [
                    (out["name"].lower(), out["point"], implied_prob(out["price"]))
                    for out in mkt.get("outcomes", [])
                    if out.get("price", 0) > 1.0 and out.get("point") is not None
                ]
                if len(pairs) < 2:
                    continue
                names, points, probs = zip(*pairs)
                fair_list = _devig_probs(list(probs))
                for n, p, f in zip(names, points, fair_list):
                    all_data.setdefault((float(p), n), []).append(f)
    return {k: sum(vs) / len(vs) for k, vs in all_data.items()}


def find_plus_ev_bets(events: list[dict]) -> list[PlusEVBet]:
    """
    Average de-vigged lines from all available sharp books (Pinnacle, Bookmaker,
    Circa) to build a fair-probability reference. Any soft-book h2h price that
    beats the averaged fair line is flagged as +EV.
    """
    results = []
    for ev in events:
        base_name = f"{ev.get('home_team','?')} vs {ev.get('away_team','?')}"
        sport = ev.get("sport_key", "")
        commence_time = ev.get("commence_time", "")
        bookmakers = ev.get("bookmakers", [])

        fair, raw_dict, sharp_title = _avg_sharp_fair(bookmakers)
        if not fair:
            continue

        for bm in bookmakers:
            if bm.get("key") in set(SHARP_BOOKS):
                continue
            for mkt in bm.get("markets", []):
                if mkt["key"] != "h2h":
                    continue
                for out in mkt.get("outcomes", []):
                    name = out["name"]
                    price = out.get("price", 0)
                    if price <= 1.0 or name not in fair:
                        continue
                    ev_val = fair[name] * price - 1
                    if ev_val > 0:
                        results.append(PlusEVBet(
                            event_name=base_name,
                            sport=sport,
                            commence_time=commence_time,
                            leg=Leg(
                                book=bm["title"],
                                outcome=name,
                                decimal_odds=price,
                                american_odds=decimal_to_american(price),
                                implied_prob=implied_prob(price),
                            ),
                            sharp_prob=round(fair[name], 4),
                            ev_pct=round(ev_val * 100, 4),
                            sharp_book=sharp_title,
                            sharp_raw_prob=round(raw_dict.get(name, 0.0), 4),
                        ))
    return results


async def fetch_all_active_sports(session: aiohttp.ClientSession, api_key: str) -> list[str]:
    """
    Query The Odds API for every sport currently active. Returns their keys.
    Falls back to the static SPORTS list if the endpoint fails.
    This call does not count against the odds quota.
    """
    url = f"{ODDS_API_BASE}/sports/?apiKey={api_key}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                print(f"  [odds-api] /sports returned HTTP {resp.status}, using built-in list")
                return SPORTS
            data = await resp.json()
            active = [s["key"] for s in data if s.get("active", False)]
            print(f"  [odds-api] {len(active)} active sports found via API")
            return active
    except Exception as e:
        print(f"  [odds-api] Could not fetch sport list ({e}), using built-in list")
        return SPORTS



async def fetch_sport_odds(session: aiohttp.ClientSession, api_key: str, sport: str) -> list[dict]:
    # Request all three market types — needed to match Kalshi spreads and totals
    # against the sharp reference line at the same point value.
    books = ",".join(SHARP_BOOKS)
    url = (
        f"{ODDS_API_BASE}/sports/{sport}/odds/"
        f"?apiKey={api_key}&regions=us,us2,uk,eu&markets=h2h,spreads,totals"
        f"&oddsFormat=decimal&bookmakers={books}"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 401:
                raise ValueError("Invalid Odds API key")
            if resp.status == 422:
                return []  # sport not active
            if resp.status != 200:
                print(f"  [odds-api] {sport}: HTTP {resp.status}")
                return []
            remaining = resp.headers.get("x-requests-remaining", "?")
            print(f"  [odds-api] {sport}: OK  ({remaining} requests remaining)")
            return await resp.json()
    except asyncio.TimeoutError:
        print(f"  [odds-api] {sport}: timeout")
        return []
    except Exception as e:
        print(f"  [odds-api] {sport}: {e}")
        return []


# Empty set — no bettable sportsbooks configured; all execution goes through Kalshi.
_BETTABLE_SET: set[str] = set()

_MARKET_LABELS = {"h2h": "ML", "spreads": "Spread", "totals": "Total"}

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

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
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
            salt_length=_asym_padding.PSS.DIGEST_LENGTH,
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

    normalized = [_normalize_kalshi_market(m) for m in markets]
    sports_markets = [m for m in normalized if _looks_like_sports_market(m)]
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


def _normalize_kalshi_market(m: dict) -> dict:
    """
    Kalshi's API now returns prices as dollar floats (yes_ask_dollars, etc.).
    Convert them to integer cents and populate the legacy yes_ask/no_ask keys
    that the rest of the scanner expects.  Prices of 0¢ or 100¢ mean no active
    order on that side; treat them as absent.
    """
    def _to_cents(val) -> Optional[int]:
        if val is None:
            return None
        try:
            c = round(float(val) * 100)
            return c if 1 <= c <= 99 else None
        except (ValueError, TypeError):
            return None

    yes_ask = _to_cents(m.get("yes_ask_dollars") or m.get("yes_ask"))
    yes_bid = _to_cents(m.get("yes_bid_dollars") or m.get("yes_bid"))
    no_ask  = _to_cents(m.get("no_ask_dollars")  or m.get("no_ask"))
    no_bid  = _to_cents(m.get("no_bid_dollars")  or m.get("no_bid"))

    # Derive missing sides from the complement (YES + NO = 100¢)
    if yes_ask is None and no_bid is not None:
        yes_ask = 100 - no_bid if 1 <= 100 - no_bid <= 99 else None
    if no_ask is None and yes_bid is not None:
        no_ask = 100 - yes_bid if 1 <= 100 - yes_bid <= 99 else None

    result = dict(m)
    result["yes_ask"] = yes_ask
    result["yes_bid"] = yes_bid
    result["no_ask"]  = no_ask
    result["no_bid"]  = no_bid
    return result


def parse_kalshi_markets(markets: list[dict]) -> list[ArbOpportunity]:
    results = []
    for m in markets:
        yes_ask = m.get("yes_ask")   # cents — price to BUY yes
        no_ask  = m.get("no_ask")    # cents — price to BUY no
        if yes_ask is None or no_ask is None:
            continue
        if yes_ask <= 0 or no_ask <= 0 or yes_ask >= 100 or no_ask >= 100:
            continue

        # Kalshi taker fee: $0.07 × c × (1−c) per $1 contract, where c = price in dollars.
        # Net payout per $1 contract = $1 − fee; effective decimal = net_payout / cost.
        c_yes = yes_ask / 100
        c_no  = no_ask  / 100
        yes_dec = (1 - KALSHI_FEE_COEF * c_yes * (1 - c_yes)) / c_yes
        no_dec  = (1 - KALSHI_FEE_COEF * c_no  * (1 - c_no )) / c_no

        total_impl = implied_prob(yes_dec) + implied_prob(no_dec)
        edge = (1 - total_impl) * 100

        ticker = m.get("ticker", "")
        legs = [
            Leg(book="Kalshi", outcome="YES", decimal_odds=round(yes_dec, 4),
                american_odds=decimal_to_american(yes_dec),
                implied_prob=implied_prob(yes_dec),
                market_url=f"https://kalshi.com/markets/{ticker}"),
            Leg(book="Kalshi", outcome="NO", decimal_odds=round(no_dec, 4),
                american_odds=decimal_to_american(no_dec),
                implied_prob=implied_prob(no_dec),
                market_url=f"https://kalshi.com/markets/{ticker}"),
        ]

        results.append(ArbOpportunity(
            event_name=m.get("title", ticker or "Unknown"),
            sport="prediction_market",
            commence_time=m.get("close_time", ""),
            legs=legs,
            total_implied=total_impl,
            edge_pct=round(edge, 4),
            is_arb=total_impl < 1.0,
            source="kalshi",
        ))
    return results


# ---------------------------------------------------------------------------
# Kalshi +EV detection (cross-referenced against sharp sportsbook lines)
# ---------------------------------------------------------------------------

_STOP_WORDS = {"the", "a", "an", "vs", "at", "in", "of", "to", "for",
               "will", "win", "game", "who", "which", "team", "over",
               "under", "next", "be", "on", "by", "is", "are", "and",
               "not", "no", "yes", "beat", "defeat", "take", "series"}

def _title_tokens(text: str) -> set[str]:
    return set(re.sub(r"[^a-z0-9 ]", " ", text.lower()).split()) - _STOP_WORDS


def _match_score(km_title: str, km_tokens: set[str], ref: dict) -> int:
    """
    Score how well a Kalshi market title matches a sportsbook event reference.
    Higher is better; 0 means no match.
    Uses substring matching on full team names AND token overlap on short names.
    """
    score = 0
    km_lower = km_title.lower()
    for team in (ref["home"], ref["away"]):
        if not team:
            continue
        # Full team name as substring (highest confidence)
        if team.lower() in km_lower:
            score += 6
            continue
        # Last word of team name = mascot/city identifier (e.g. "Lakers")
        parts = team.split()
        if parts:
            last = parts[-1].lower()
            if len(last) > 3 and last in km_lower:
                score += 3
                continue
        # Token overlap fallback
        score += len(km_tokens & _title_tokens(team))
    return score


def _map_yes_to_team(km_title: str, km_tokens: set[str], fair: dict) -> tuple[Optional[str], float]:
    """
    Determine which sportsbook outcome corresponds to Kalshi YES by scoring
    each outcome name against the market title. Returns (team_name, fair_prob).
    """
    km_lower = km_title.lower()
    best_team, best_fair, best_score = None, 0.0, -1
    for outcome_name, fair_p in fair.items():
        score = 0
        if outcome_name.lower() in km_lower:
            score += 6
        else:
            parts = outcome_name.split()
            if parts:
                last = parts[-1].lower()
                if len(last) > 3 and last in km_lower:
                    score += 3
            score += len(km_tokens & _title_tokens(outcome_name))
        if score > best_score:
            best_score = score
            best_team = outcome_name
            best_fair = fair_p
    return (best_team, best_fair) if best_score > 0 else (None, 0.0)


# ---------------------------------------------------------------------------
# Kalshi market type parser
# ---------------------------------------------------------------------------

_SPREAD_PATTERNS = [
    # "Boston wins by over 23.5 points"
    re.compile(r'^(.+?)\s+wins?\s+by\s+(?:over|more\s+than)\s+([\d.]+)\s+points?', re.I),
    # "Boston to win by 23.5+ points"
    re.compile(r'^(.+?)\s+(?:to\s+)?win\s+by\s+([\d.]+)\+?\s+points?', re.I),
    # "Boston by 23.5 or more"
    re.compile(r'^(.+?)\s+by\s+([\d.]+)\s+or\s+more', re.I),
]
_TOTAL_PATTERNS = [
    # "Over/Under 205.5 points scored"
    re.compile(r'^(over|under)\s+([\d.]+)\s+points?', re.I),
    # "More than 205.5 total points"
    re.compile(r'^more\s+than\s+([\d.]+)\s+(?:total\s+)?points?', re.I),
]
_TEAM_TOTAL_PATTERNS = [
    # "Boston over/under 95.5 points scored"
    re.compile(r'^(.+?)\s+(over|under)\s+([\d.]+)\s+points?', re.I),
]


def _parse_kalshi_mkt(title: str) -> Optional[dict]:
    """
    Return market-type metadata for a Kalshi market title, or None for moneyline/unknown.
    Result keys:
      spread:     {"type":"spread",  "team":str_lower,  "point":float}
      total:      {"type":"total",   "side":"over"/"under", "point":float}
      team_total: {"type":"team_total", "team":str_lower, "side":"over"/"under", "point":float}
    """
    for pat in _SPREAD_PATTERNS:
        m = pat.match(title)
        if m:
            return {"type": "spread", "team": m.group(1).strip().lower(), "point": float(m.group(2))}

    for i, pat in enumerate(_TOTAL_PATTERNS):
        m = pat.match(title)
        if m:
            if i == 3:  # "more than X total points" — group(1) is the number
                return {"type": "total", "side": "over", "point": float(m.group(1))}
            else:  # patterns 0-2: group(1)=side, group(2)=number
                return {"type": "total", "side": m.group(1).lower(), "point": float(m.group(2))}

    for pat in _TEAM_TOTAL_PATTERNS:
        m = pat.match(title)
        if m:
            return {"type": "team_total", "team": m.group(1).strip().lower(),
                    "side": m.group(2).lower(), "point": float(m.group(3))}

    return None  # moneyline or unrecognized


def find_kalshi_ev_bets(
    kalshi_raw: list[dict],
    sportsbook_events: list[dict],
) -> list[PlusEVBet]:
    """
    Cross-reference Kalshi markets (moneyline, spread, total) against sharp
    sportsbook lines to find +EV contracts. Only pre-game markets are considered.

    For spreads and totals the Kalshi point value must match the sharp book's
    line exactly, since we can't interpolate across different lines. Moneyline
    markets are matched by team-name string search.
    """
    now = datetime.now(timezone.utc)
    results: list[PlusEVBet] = []

    # Build a sharp reference per sportsbook event containing all three market types.
    sharp_refs: list[dict] = []
    for ev in sportsbook_events:
        home = ev.get("home_team", "")
        away = ev.get("away_team", "")
        commence_time = ev.get("commence_time", "")
        bookmakers = ev.get("bookmakers", [])

        fair_h2h, _, sharp_title = _avg_sharp_fair(bookmakers)
        # Spreads and totals are independent — compute even if h2h is unavailable.
        fair_spreads = _avg_sharp_spreads(bookmakers)
        fair_totals  = _avg_sharp_totals(bookmakers)

        if not fair_h2h and not fair_spreads and not fair_totals:
            continue

        sharp_refs.append({
            "home": home,
            "away": away,
            "fair_h2h":    fair_h2h,     # {team_name: prob}
            "fair_spreads": fair_spreads, # {(team_name, point): prob}
            "fair_totals":  fair_totals,  # {(point, "over"/"under"): prob}
            "sharp_title":  sharp_title or "Sharp",
            "commence_time": commence_time,
        })

    matched = unmatched = 0

    def _make_ev_bet(km, ticker, side, ask_cents, fair_p, outcome_label,
                     sharp_title, commence_time, auto_trade=True) -> Optional[PlusEVBet]:
        c = ask_cents / 100
        dec = (1 - KALSHI_FEE_COEF * c * (1 - c)) / c
        ev = fair_p * dec - 1
        if ev <= 0:
            return None
        km_title = km.get("title", ticker)
        return PlusEVBet(
            event_name=km_title,
            sport="prediction_market",
            commence_time=commence_time,
            leg=Leg(
                book="Kalshi",
                outcome=outcome_label,
                decimal_odds=round(dec, 4),
                american_odds=decimal_to_american(dec),
                implied_prob=implied_prob(dec),
                market_url=f"https://kalshi.com/markets/{ticker}",
            ),
            sharp_prob=round(fair_p, 4),
            ev_pct=round(ev * 100, 4),
            sharp_book=sharp_title,
            # Only populate auto-trade fields when the point matched exactly.
            # Approximate matches (different reference line) display in the UI
            # but are excluded from auto-trading via the empty ticker check.
            kalshi_ticker=ticker if auto_trade else "",
            kalshi_side=side if auto_trade else "",
            kalshi_ask_cents=ask_cents if auto_trade else 0,
        )

    for km in kalshi_raw:
        ticker = km.get("ticker", "")
        yes_ask = km.get("yes_ask")
        no_ask  = km.get("no_ask")
        if not ticker or yes_ask is None or no_ask is None:
            continue
        if yes_ask <= 0 or no_ask <= 0 or yes_ask >= 100 or no_ask >= 100:
            continue

        km_title  = km.get("title", ticker)
        km_tokens = _title_tokens(km_title)

        # Find the sportsbook event that best matches this Kalshi market.
        best_ref, best_score = None, 0
        for ref in sharp_refs:
            s = _match_score(km_title, km_tokens, ref)
            if s > best_score:
                best_score = s
                best_ref = ref

        if best_ref is None or best_score == 0:
            unmatched += 1
            continue

        # Skip live bets.
        try:
            ct = datetime.fromisoformat(best_ref["commence_time"].replace("Z", "+00:00"))
            if ct <= now:
                continue
        except Exception:
            continue

        matched += 1
        sharp_title   = best_ref["sharp_title"]
        commence_time = best_ref["commence_time"]

        # Determine the Kalshi market type and look up the corresponding fair prob.
        mkt_meta = _parse_kalshi_mkt(km_title)

        # ── TOTAL market ──────────────────────────────────────────────────────
        # Kalshi total lines are adjustable in ~3-point increments, so we find
        # the nearest sharp total within MAX_TOTAL_GAP. Power de-vig is applied
        # at the sharp book's actual line, not Kalshi's displayed point.
        if mkt_meta and mkt_meta["type"] == "total":
            pt   = mkt_meta["point"]
            side = mkt_meta["side"]       # "over" or "under"
            opp  = "under" if side == "over" else "over"

            MAX_TOTAL_GAP = 9.0  # allow up to 3 Kalshi increments (3 pts each)

            # Nearest sharp total in the same direction (over or under)
            yes_fair = sharp_pt_yes = None
            best_d = float("inf")
            for (spt, ss), fp in best_ref["fair_totals"].items():
                if ss != side:
                    continue
                d = abs(spt - pt)
                if d < best_d:
                    best_d = d
                    yes_fair = fp
                    sharp_pt_yes = spt
            if yes_fair is None or best_d > MAX_TOTAL_GAP:
                continue

            # Opposite direction at the same sharp point
            no_fair = best_ref["fair_totals"].get((sharp_pt_yes, opp), 1.0 - yes_fair)

            exact = (sharp_pt_yes == pt)
            ref_note = f" (ref {sharp_pt_yes} — display only)" if not exact else ""
            for s, ask, fp, lbl in [
                ("yes", yes_ask, yes_fair, f"YES — {side.title()} {pt}{ref_note}"),
                ("no",  no_ask,  no_fair,  f"NO  — {opp.title()} {pt}{ref_note}"),
            ]:
                bet = _make_ev_bet(km, ticker, s, ask, fp, lbl, sharp_title, commence_time,
                                   auto_trade=exact)
                if bet:
                    results.append(bet)

        # ── SPREAD market ─────────────────────────────────────────────────────
        # Kalshi spread markets step in ~3-point increments. We find the nearest
        # sharp line for the same team within MAX_SPREAD_GAP and use its power-
        # de-vigged fair probability as the EV reference.
        elif mkt_meta and mkt_meta["type"] == "spread":
            pt        = mkt_meta["point"]   # positive number from Kalshi title
            team_hint = mkt_meta["team"]    # lowercase partial team name

            MAX_SPREAD_GAP = 9.0  # up to 3 increments

            yes_team = no_team = None
            yes_fair = no_fair = None
            sharp_pt_spread = None
            best_d = float("inf")

            for (sbook_team, sbook_pt), fp in best_ref["fair_spreads"].items():
                team_lower = sbook_team.lower()
                last_word  = sbook_team.split()[-1].lower() if sbook_team else ""
                if not (team_hint in team_lower or last_word in team_hint or team_hint in last_word):
                    continue
                # sbook_pt is negative for the favourite; Kalshi pt is positive
                d = abs(abs(sbook_pt) - pt)
                if d < best_d:
                    best_d       = d
                    yes_fair     = fp
                    yes_team     = sbook_team
                    sharp_pt_spread = abs(sbook_pt)

            if yes_fair is None or best_d > MAX_SPREAD_GAP:
                continue

            # NO side = the other team covering at the same line
            opp_entry = next(
                ((t, p) for (t, p) in best_ref["fair_spreads"]
                 if t != yes_team and abs(abs(p) - sharp_pt_spread) < 0.26),
                None,
            )
            if opp_entry:
                no_fair = best_ref["fair_spreads"][opp_entry]
                no_team = opp_entry[0]
            else:
                no_fair = 1.0 - yes_fair
                no_team = "Opp"

            exact = (sharp_pt_spread == pt)
            ref_note = f" (ref ±{sharp_pt_spread} — display only)" if not exact else ""
            for s, ask, fp, lbl in [
                ("yes", yes_ask, yes_fair, f"YES — {yes_team} by >{pt}{ref_note}"),
                ("no",  no_ask,  no_fair,  f"NO  — {no_team} covers +{pt}{ref_note}"),
            ]:
                bet = _make_ev_bet(km, ticker, s, ask, fp, lbl, sharp_title, commence_time,
                                   auto_trade=exact)
                if bet:
                    results.append(bet)

        # ── TEAM TOTAL market ─────────────────────────────────────────────────
        # No sharp team-total reference exists in the Odds API response, so we
        # cannot compute a fair probability. Skip rather than misuse h2h data.
        elif mkt_meta and mkt_meta["type"] == "team_total":
            continue

        # ── MONEYLINE (or unrecognized type) ──────────────────────────────────
        else:
            fair_h2h = best_ref["fair_h2h"]
            if not fair_h2h:
                continue
            yes_team, yes_fair = _map_yes_to_team(km_title, km_tokens, fair_h2h)
            if yes_team is None:
                continue
            no_fair = 1.0 - yes_fair
            no_team = next((n for n in fair_h2h if n != yes_team), "Opp")
            for s, ask, fp, lbl in [
                ("yes", yes_ask, yes_fair, f"YES — {yes_team}"),
                ("no",  no_ask,  no_fair,  f"NO  — {no_team}"),
            ]:
                bet = _make_ev_bet(km, ticker, s, ask, fp, lbl, sharp_title, commence_time)
                if bet:
                    results.append(bet)

    print(f"  [kalshi-ev] {matched} matched / {unmatched} unmatched → {len(results)} +EV bets")
    return results


async def place_kalshi_order(
    session: aiohttp.ClientSession,
    api_key: str,
    ticker: str,
    side: str,          # "yes" or "no"
    count: int,         # number of contracts
    limit_cents: int,   # limit price in cents (max you'll pay per contract)
) -> dict:
    """Place a limit buy order on Kalshi. Returns {"http_status": code, ...}."""
    path = "/trade-api/v2/portfolio/orders"
    client_id = f"autobet-{ticker[:20]}-{side}-{uuid.uuid4().hex[:8]}"
    yes_price = limit_cents if side == "yes" else (100 - limit_cents)
    no_price  = limit_cents if side == "no"  else (100 - limit_cents)
    body = {
        "ticker": ticker,
        "client_order_id": client_id,
        "type": "limit",
        "action": "buy",
        "side": side,
        "count": count,
        "yes_price": yes_price,
        "no_price": no_price,
        "expiration_ts": None,
    }
    url = f"{KALSHI_BASE}/portfolio/orders"

    async def _post(hdrs: dict) -> dict:
        async with session.post(url, headers=hdrs, json=body,
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            return {"http_status": resp.status, "order": await resp.json()}

    try:
        result = await _post(_kalshi_headers("POST", path, api_key))
        if result["http_status"] == 401 and _PRIVATE_KEY_BYTES and _CRYPTO_OK and _kalshi_rsa_ok is not False:
            print("[kalshi] RSA auth 401 on order — retrying with bearer token")
            result = await _post(_kalshi_bearer_headers(api_key))
        return result
    except Exception as e:
        return {"http_status": 0, "error": str(e)}


# ---------------------------------------------------------------------------
# Cross-market arb: Kalshi YES vs sportsbook moneyline
# ---------------------------------------------------------------------------

def find_cross_market_arbs(
    sportsbook_opps: list[ArbOpportunity],
    kalshi_opps: list[ArbOpportunity],
    fuzzy_threshold: float = 0.85,
) -> list[ArbOpportunity]:
    """
    Attempt to match Kalshi markets to sportsbook events by title similarity
    and check if mixing the best Kalshi price with the best sportsbook price creates an arb.
    This is a best-effort heuristic — exact matching requires manual market mapping.
    """
    cross = []
    kalshi_titles = [(k.event_name.lower(), k) for k in kalshi_opps]

    for sb in sportsbook_opps:
        sb_name = sb.event_name.lower()
        teams = sb_name.replace(" vs ", " ").split()
        for team in teams:
            if len(team) < 4:
                continue
            for k_title, k_opp in kalshi_titles:
                if team in k_title:
                    # Try to combine: use best sportsbook leg + kalshi opposing leg
                    for sb_leg in sb.legs:
                        for k_leg in k_opp.legs:
                            total = sb_leg.implied_prob + k_leg.implied_prob
                            edge = (1 - total) * 100
                            if total < 1.0:
                                cross.append(ArbOpportunity(
                                    event_name=f"{sb.event_name} [cross-market]",
                                    sport=sb.sport,
                                    commence_time=sb.commence_time,
                                    legs=[sb_leg, k_leg],
                                    total_implied=total,
                                    edge_pct=round(edge, 4),
                                    is_arb=True,
                                    source="cross",
                                ))
    return cross


# ---------------------------------------------------------------------------
# Main scanner
# ---------------------------------------------------------------------------

@dataclass
class ScanResult:
    opportunities: list[ArbOpportunity]
    arb_count: int
    total_scanned: int
    best_edge: float
    books_seen: set
    scan_time: str
    errors: list[str] = field(default_factory=list)
    ev_bets: list[PlusEVBet] = field(default_factory=list)

    def arbs_only(self) -> list[ArbOpportunity]:
        return [o for o in self.opportunities if o.is_arb]

    def sorted_by_edge(self) -> list[ArbOpportunity]:
        return sorted(self.opportunities, key=lambda o: o.edge_pct, reverse=True)

    def to_json(self) -> str:
        import math
        def _clean(v):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return 0.0
            return v
        def _clean_dict(d):
            return {k: _clean(v) if isinstance(v, float) else v for k, v in d.items()}
        return json.dumps({
            "scan_time": self.scan_time,
            "arb_count": self.arb_count,
            "total_scanned": self.total_scanned,
            "best_edge": _clean(self.best_edge),
            "errors": self.errors,
            "opportunities": [_clean_dict(o.to_dict()) for o in self.sorted_by_edge()],
            "ev_bets": [_clean_dict(b.to_dict()) for b in sorted(self.ev_bets, key=lambda b: b.ev_pct, reverse=True)],
        }, indent=2)


async def scan(
    odds_api_key: str = "",
    kalshi_api_key: str = "",
    sports: Optional[list[str]] = None,
    arbs_only: bool = False,
    min_edge: float = 0.0,
    include_cross_market: bool = True,
) -> ScanResult:
    all_opps: list[ArbOpportunity] = []
    books_seen: set[str] = set()
    errors: list[str] = []

    async with aiohttp.ClientSession() as session:
        # Resolve sport list — fetch all active sports from API when none specified
        if not sports and odds_api_key:
            sports = await fetch_all_active_sports(session, odds_api_key)
        elif not sports:
            sports = SPORTS

        tasks = []

        if odds_api_key:
            for sport in sports:
                tasks.append(fetch_sport_odds(session, odds_api_key, sport))

        kalshi_raw_task = None
        if kalshi_api_key:
            kalshi_raw_task = fetch_kalshi_raw(session, kalshi_api_key)

        print(f"[scanner] Launching {len(tasks)} sportsbook + {'1 Kalshi' if kalshi_raw_task else '0 Kalshi'} requests...")
        t0 = time.time()

        sb_results_raw = await asyncio.gather(*tasks, return_exceptions=True)
        kalshi_raw_markets: list[dict] = []
        if kalshi_raw_task:
            try:
                kalshi_raw_markets = await kalshi_raw_task
            except Exception as e:
                errors.append(f"Kalshi: {e}")

        kalshi_opps = parse_kalshi_markets(kalshi_raw_markets)

        sb_opps: list[ArbOpportunity] = []
        ev_bets: list[PlusEVBet] = []
        all_raw_events: list[dict] = []   # kept for Kalshi EV cross-reference
        for res in sb_results_raw:
            if isinstance(res, Exception):
                errors.append(str(res))
            elif res:
                all_raw_events.extend(res)
                parsed = parse_sportsbook_events(res)
                sb_opps.extend(parsed)
                for opp in parsed:
                    for leg in opp.legs:
                        books_seen.add(leg.book)
                ev_bets.extend(find_plus_ev_bets(res))

        # Kalshi +EV bets (cross-referenced against sharp sportsbook lines)
        if kalshi_raw_markets and all_raw_events:
            kalshi_ev = find_kalshi_ev_bets(kalshi_raw_markets, all_raw_events)
            ev_bets.extend(kalshi_ev)
            if kalshi_ev:
                print(f"  [kalshi-ev] {len(kalshi_ev)} +EV Kalshi bets found")

        all_opps = sb_opps + kalshi_opps

        if include_cross_market and sb_opps and kalshi_opps:
            cross = find_cross_market_arbs(sb_opps, kalshi_opps)
            all_opps.extend(cross)
            print(f"  [cross-market] {len(cross)} potential cross-market arbs found")

        elapsed = round(time.time() - t0, 2)
        print(f"[scanner] Done in {elapsed}s — {len(all_opps)} markets analyzed")

    if arbs_only:
        all_opps = [o for o in all_opps if o.is_arb]
    if min_edge > 0:
        all_opps = [o for o in all_opps if o.edge_pct >= min_edge]

    arb_count = sum(1 for o in all_opps if o.is_arb)
    best_edge = max((o.edge_pct for o in all_opps if o.is_arb), default=0.0)

    return ScanResult(
        opportunities=all_opps,
        arb_count=arb_count,
        total_scanned=len(all_opps),
        best_edge=round(best_edge, 4),
        books_seen=books_seen,
        scan_time=datetime.now(timezone.utc).isoformat(),
        errors=errors,
        ev_bets=ev_bets,
    )


# ---------------------------------------------------------------------------
# Auto-refresh loop
# ---------------------------------------------------------------------------

async def run_loop(
    odds_api_key: str = "",
    kalshi_api_key: str = "",
    interval_seconds: int = 60,
    min_edge: float = 0.0,
    on_result=None,   # callback(ScanResult)
    sports: Optional[list[str]] = None,
):
    """Continuously scan and call on_result with each ScanResult."""
    print(f"[scanner] Starting auto-refresh loop (interval={interval_seconds}s)")
    while True:
        try:
            result = await scan(
                odds_api_key=odds_api_key,
                kalshi_api_key=kalshi_api_key,
                sports=sports,
                min_edge=min_edge,
            )
            if on_result:
                on_result(result)
        except Exception as e:
            print(f"[scanner] Loop error: {e}")
        await asyncio.sleep(interval_seconds)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import os
    import argparse

    parser = argparse.ArgumentParser(description="Arbitrage Scanner")
    parser.add_argument("--odds-key", default=os.getenv("ODDS_API_KEY", ""), help="The Odds API key")
    parser.add_argument("--kalshi-key", default=os.getenv("KALSHI_API_KEY", ""), help="Kalshi API key")
    parser.add_argument("--min-edge", type=float, default=0.0, help="Minimum edge %% to display")
    parser.add_argument("--arbs-only", action="store_true", help="Only show arbs")
    parser.add_argument("--output", choices=["table", "json"], default="table")
    parser.add_argument("--loop", action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=60, help="Refresh interval (seconds)")
    args = parser.parse_args()

    def print_result(result: ScanResult):
        print(f"\n{'='*60}")
        print(f"  Scan: {result.scan_time}")
        print(f"  Markets: {result.total_scanned}  |  Arbs: {result.arb_count}  |  Best edge: {result.best_edge}%")
        print(f"{'='*60}")
        for opp in result.sorted_by_edge():
            if not opp.is_arb and args.arbs_only:
                continue
            tag = "  [ARB]" if opp.is_arb else "       "
            print(f"{tag} {opp.edge_pct:+.2f}%  {opp.event_name[:50]:<50}  [{opp.source}]")
            for i, (leg, stake) in enumerate(zip(opp.legs, opp.stakes_for(1000))):
                print(f"         Leg {i+1}: {leg.book:<20} {leg.outcome:<20} {leg.american_odds:>6}  stake=${stake:.2f}")
            if opp.is_arb:
                print(f"         Profit on $1000: +${opp.guaranteed_profit(1000):.2f}")

    async def main():
        if args.output == "json":
            result = await scan(odds_api_key=args.odds_key, kalshi_api_key=args.kalshi_key, min_edge=args.min_edge)
            print(result.to_json())
        elif args.loop:
            await run_loop(
                odds_api_key=args.odds_key,
                kalshi_api_key=args.kalshi_key,
                interval_seconds=args.interval,
                min_edge=args.min_edge,
                on_result=print_result,
            )
        else:
            result = await scan(odds_api_key=args.odds_key, kalshi_api_key=args.kalshi_key, min_edge=args.min_edge)
            print_result(result)

    asyncio.run(main())
