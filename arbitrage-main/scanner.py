"""
Arbitrage Scanner - Core Engine
Fetches odds from The Odds API (sportsbooks) and Kalshi (prediction markets),
detects arbitrage opportunities, and ranks them by edge.
"""

import asyncio
import aiohttp
import json
import re
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime, timezone


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

# Books the user can actually bet on — only these are used as arb legs.
BETTABLE_BOOKS = [
    "draftkings",      # DraftKings
    "fanduel",         # FanDuel
    "betmgm",          # BetMGM
    "borgataonline",   # Borgata Sports (BetMGM-powered; key unconfirmed — drop if 422)
    "williamhill_us",  # Caesars Sportsbook (legacy key for Caesars US)
    "betrivers",       # BetRivers
    "fanatics",        # Fanatics
    "bet365",          # bet365
    "hardrockbet",     # Hard Rock Bet
    "espnbet",         # ESPN BET (formerly theScore Bet US)
    "betparx",         # BetParx
    "ballybet",        # Bally Bet
    "sporttrade",      # Sporttrade exchange (NJ/PA; key unconfirmed — drop if 422)
    # Note: Kalshi is fetched separately via its own API, not The Odds API.
    # Note: PrimeSports does not appear to have an Odds API key.
]

# Sharp-line reference books — used ONLY for +EV de-vig, never as arb legs.
# Averaging across multiple sharp books reduces noise in fair-line estimation.
SHARP_BOOKS = ["pinnacle", "bookmaker", "circa_sports"]

# All bookmakers sent to The Odds API (bettable + sharp for EV reference).
BOOKMAKERS = BETTABLE_BOOKS + SHARP_BOOKS

# Fast-lookup set used by parse functions to exclude sharp books from arb legs.
_BETTABLE_SET = set(BETTABLE_BOOKS)

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
                    all_raw.setdefault(n, []).append(r)
                    all_fair.setdefault(n, []).append(f)
                titles_seen.append(bm["title"])
                break  # one h2h market per book is enough

    if not all_fair:
        return {}, {}, ""

    fair = {n: sum(ps) / len(ps) for n, ps in all_fair.items()}
    raw  = {n: sum(ps) / len(ps) for n, ps in all_raw.items()}
    label = " / ".join(dict.fromkeys(titles_seen))  # deduplicated, ordered
    return fair, raw, label


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
    books = ",".join(BOOKMAKERS)
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

KALSHI_BASE = "https://trading-api.kalshi.com/trade-api/v2"
KALSHI_FEE_COEF = 0.07  # taker fee: $0.07 × C × (1−C) per $1 contract, where C = price in dollars

async def fetch_kalshi_raw(session: aiohttp.ClientSession, api_key: str) -> list[dict]:
    """
    Fetch all open Kalshi markets, paginating via cursor.
    Returns raw market dicts filtered to sports-like markets.
    """
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    markets: list[dict] = []
    cursor: Optional[str] = None

    while True:
        url = f"{KALSHI_BASE}/markets?limit=1000&status=open"
        if cursor:
            url += f"&cursor={cursor}"
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                if resp.status == 401:
                    raise ValueError("Kalshi API key invalid or unauthorized")
                if resp.status != 200:
                    print(f"  [kalshi] HTTP {resp.status}")
                    break
                data = await resp.json()
        except Exception as e:
            print(f"  [kalshi] error: {e}")
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
               "under", "next", "be", "on", "by", "is", "are"}

def _title_tokens(text: str) -> set[str]:
    return set(re.sub(r"[^a-z0-9 ]", " ", text.lower()).split()) - _STOP_WORDS


def find_kalshi_ev_bets(
    kalshi_raw: list[dict],
    sportsbook_events: list[dict],
) -> list[PlusEVBet]:
    """
    Cross-reference Kalshi markets against sharp sportsbook lines to find +EV
    contracts. Only considers pre-game markets (game not yet started).
    YES/NO is mapped to a team by overlapping token matching on the market title.
    """
    now = datetime.now(timezone.utc)
    results: list[PlusEVBet] = []

    # Pre-compute sharp fair probs for every sportsbook event.
    # Store alongside the home/away tokens for fast matching.
    sharp_refs: list[dict] = []
    for ev in sportsbook_events:
        home = ev.get("home_team", "")
        away = ev.get("away_team", "")
        commence_time = ev.get("commence_time", "")
        bookmakers = ev.get("bookmakers", [])

        fair, _, sharp_title = _avg_sharp_fair(bookmakers)
        if not fair:
            continue

        sharp_refs.append({
            "home": home,
            "away": away,
            "tokens": _title_tokens(f"{home} {away}"),
            "fair": fair,           # {outcome_name: fair_prob}
            "sharp_title": sharp_title,
            "commence_time": commence_time,
        })

    for km in kalshi_raw:
        ticker = km.get("ticker", "")
        yes_ask = km.get("yes_ask")
        no_ask  = km.get("no_ask")
        if not ticker or yes_ask is None or no_ask is None:
            continue
        if yes_ask <= 0 or no_ask <= 0 or yes_ask >= 100 or no_ask >= 100:
            continue

        km_tokens = _title_tokens(km.get("title", ""))
        if not km_tokens:
            continue

        # Find best-matching sportsbook event by token overlap.
        best_ref = None
        best_overlap = 1  # require at least 2 overlapping tokens
        for ref in sharp_refs:
            overlap = len(km_tokens & ref["tokens"])
            if overlap > best_overlap:
                best_overlap = overlap
                best_ref = ref

        if best_ref is None:
            continue

        # Skip live bets (game already started).
        try:
            ct = datetime.fromisoformat(best_ref["commence_time"].replace("Z", "+00:00"))
            if ct <= now:
                continue
        except Exception:
            continue  # can't verify pre-game — skip to be safe

        fair = best_ref["fair"]
        sharp_title = best_ref["sharp_title"]
        commence_time = best_ref["commence_time"]
        km_title = km.get("title", ticker)

        # Map YES to whichever sportsbook outcome name has the most token overlap
        # with the Kalshi market title (e.g. "Will the Lakers win?" → Lakers).
        yes_team: Optional[str] = None
        yes_fair: float = 0.0
        best_team_overlap = 0
        for outcome_name in fair:
            team_tokens = _title_tokens(outcome_name)
            ov = len(km_tokens & team_tokens)
            if ov > best_team_overlap:
                best_team_overlap = ov
                yes_team = outcome_name
                yes_fair = fair[outcome_name]

        if yes_team is None or best_team_overlap == 0:
            continue

        no_fair = 1.0 - yes_fair  # binary market

        # Effective decimal odds including Kalshi fee
        c_yes = yes_ask / 100
        c_no  = no_ask  / 100
        yes_dec = (1 - KALSHI_FEE_COEF * c_yes * (1 - c_yes)) / c_yes
        no_dec  = (1 - KALSHI_FEE_COEF * c_no  * (1 - c_no )) / c_no

        yes_ev = yes_fair * yes_dec - 1
        no_ev  = no_fair  * no_dec  - 1

        if yes_ev > 0:
            results.append(PlusEVBet(
                event_name=km_title,
                sport="prediction_market",
                commence_time=commence_time,
                leg=Leg(
                    book="Kalshi",
                    outcome=f"YES — {yes_team}",
                    decimal_odds=round(yes_dec, 4),
                    american_odds=decimal_to_american(yes_dec),
                    implied_prob=implied_prob(yes_dec),
                    market_url=f"https://kalshi.com/markets/{ticker}",
                ),
                sharp_prob=round(yes_fair, 4),
                ev_pct=round(yes_ev * 100, 4),
                sharp_book=sharp_title,
                kalshi_ticker=ticker,
                kalshi_side="yes",
                kalshi_ask_cents=yes_ask,
            ))

        if no_ev > 0:
            no_team = next((n for n in fair if n != yes_team), "NO")
            results.append(PlusEVBet(
                event_name=km_title,
                sport="prediction_market",
                commence_time=commence_time,
                leg=Leg(
                    book="Kalshi",
                    outcome=f"NO — {no_team}",
                    decimal_odds=round(no_dec, 4),
                    american_odds=decimal_to_american(no_dec),
                    implied_prob=implied_prob(no_dec),
                    market_url=f"https://kalshi.com/markets/{ticker}",
                ),
                sharp_prob=round(no_fair, 4),
                ev_pct=round(no_ev * 100, 4),
                sharp_book=sharp_title,
                kalshi_ticker=ticker,
                kalshi_side="no",
                kalshi_ask_cents=no_ask,
            ))

    return results


async def place_kalshi_order(
    session: aiohttp.ClientSession,
    api_key: str,
    ticker: str,
    side: str,          # "yes" or "no"
    count: int,         # number of contracts
    limit_cents: int,   # limit price in cents (max you'll pay per contract)
) -> dict:
    """Place a limit buy order on Kalshi. Returns {"status": http_code, ...}."""
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    client_id = f"autobet-{ticker[:20]}-{side}-{uuid.uuid4().hex[:8]}"
    # yes_price + no_price should approximately sum to 100 for a limit order.
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
    try:
        async with session.post(url, headers=headers, json=body,
                                timeout=aiohttp.ClientTimeout(total=15)) as resp:
            data = await resp.json()
            return {"http_status": resp.status, "order": data}
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
        return json.dumps({
            "scan_time": self.scan_time,
            "arb_count": self.arb_count,
            "total_scanned": self.total_scanned,
            "best_edge": self.best_edge,
            "errors": self.errors,
            "opportunities": [o.to_dict() for o in self.sorted_by_edge()],
            "ev_bets": [b.to_dict() for b in sorted(self.ev_bets, key=lambda b: b.ev_pct, reverse=True)],
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
