"""
Arbitrage Scanner - Core Engine
Fetches odds from The Odds API (sportsbooks) and Kalshi (prediction markets),
detects arbitrage opportunities, and ranks them by edge.
"""

import asyncio
import aiohttp
import json
import time
from dataclasses import dataclass, field, asdict
from typing import Optional
from datetime import datetime, timezone, timedelta


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


@dataclass(frozen=True)
class CrossMarketMapping:
    sportsbook_event_contains: str
    sportsbook_outcome_contains: str
    kalshi_ticker: str
    kalshi_leg: str


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

# All bookmakers available on free/paid tiers
BOOKMAKERS = [
    # NJ-legal sportsbooks with supported The Odds API bookmaker keys
    "draftkings", "fanduel", "betmgm", "williamhill_us", "betrivers",
    "fanatics", "hardrockbet", "betparx", "espnbet", "ballybet", "bet365",
]

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

# Player prop market keys supported by The Odds API, keyed by sport.
# Only sports listed here will have their per-event prop endpoints queried.
# Only over/under markets are included — every entry here produces "Over"/"Under"
# outcomes with a numeric point line. Yes/No props (anytime TD, first basket,
# double-double, goal scorer, method of victory, cards) are excluded.
PLAYER_PROP_MARKETS: dict[str, list[str]] = {
    "americanfootball_nfl": [
        "player_pass_tds", "player_pass_yds", "player_pass_completions",
        "player_pass_attempts", "player_pass_interceptions", "player_pass_longest_completion",
        "player_rush_yds", "player_rush_attempts", "player_rush_longest",
        "player_receptions", "player_reception_yds", "player_reception_longest",
        "player_kicking_points", "player_field_goals",
    ],
    "americanfootball_ncaaf": [
        "player_pass_tds", "player_pass_yds", "player_rush_yds", "player_reception_yds",
    ],
    "basketball_nba": [
        "player_points", "player_rebounds", "player_assists", "player_threes",
        "player_blocks", "player_steals", "player_turnovers",
        "player_points_rebounds_assists", "player_points_rebounds",
        "player_points_assists", "player_rebounds_assists",
    ],
    "basketball_ncaab": [
        "player_points", "player_rebounds", "player_assists", "player_threes",
    ],
    "basketball_wnba": [
        "player_points", "player_rebounds", "player_assists", "player_threes",
    ],
    "baseball_mlb": [
        "player_strikeouts", "player_hits_allowed", "player_earned_runs", "player_walks",
        "player_total_bases", "player_hits", "player_rbis", "player_runs_scored",
        "player_hits_runs_rbis", "player_home_runs", "player_stolen_bases",
    ],
    "icehockey_nhl": [
        "player_points", "player_goals", "player_assists",
        "player_shots_on_goal", "player_saves",
    ],
    "soccer_epl": [
        "player_shots_on_target",
    ],
    "soccer_usa_mls": [
        "player_shots_on_target",
    ],
    "soccer_uefa_champs_league": [
        "player_shots_on_target",
    ],
}

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


async def fetch_sport_events(session: aiohttp.ClientSession, api_key: str, sport: str) -> list[dict]:
    """Return upcoming event objects (with id, home_team, etc.) for a sport."""
    url = f"{ODDS_API_BASE}/sports/{sport}/events?apiKey={api_key}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status in (401, 422):
                return []
            if resp.status == 429:
                return [{"_quota_limited": True}]
            if resp.status != 200:
                print(f"  [props] {sport} events: HTTP {resp.status}")
                return []
            return await resp.json()
    except Exception as e:
        print(f"  [props] {sport} events: {e}")
        return []


async def fetch_event_player_props(
    session: aiohttp.ClientSession,
    api_key: str,
    sport: str,
    event_id: str,
    prop_markets: list[str],
) -> Optional[dict]:
    """Fetch player prop odds for one event. Returns the raw event object or None."""
    books = ",".join(BOOKMAKERS)
    markets = ",".join(prop_markets)
    # Note: when 'bookmakers' param is set, 'regions' is ignored by the API.
    # Using only us region to avoid unexpected 422s from unsupported region combos.
    url = (
        f"{ODDS_API_BASE}/sports/{sport}/events/{event_id}/odds"
        f"?apiKey={api_key}&regions=us&markets={markets}"
        f"&oddsFormat=decimal&bookmakers={books}"
    )
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status == 401:
                print(f"  [props] {sport}/{event_id}: 401 Unauthorized — API key may not support player props (paid tier required)")
                return None
            if resp.status == 422:
                return None  # sport/market combo not available, expected
            if resp.status != 200:
                print(f"  [props] {sport}/{event_id}: HTTP {resp.status}")
                return None
            return await resp.json()
    except Exception as e:
        print(f"  [props] {sport}/{event_id}: {e}")
        return None


def _is_today_upcoming(commence_time_str: str) -> bool:
    """Return True if the event hasn't started yet and begins within the next 24 hours."""
    if not commence_time_str:
        return True  # no time info — keep it
    try:
        ct = datetime.fromisoformat(commence_time_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        return now < ct < now + timedelta(hours=24)
    except Exception:
        return True  # malformed timestamp — keep it


def parse_player_props(event_data: dict) -> list[ArbOpportunity]:
    """
    Parse a per-event odds response that contains player prop markets.
    Each market has a 'description' field with the player name. We group
    by (market_key, player, point) and find the best price per outcome
    across all books, then check for arb.
    """
    results = []
    base_name = f"{event_data.get('home_team','?')} vs {event_data.get('away_team','?')}"
    sport = event_data.get("sport_key", "")
    commence_time = event_data.get("commence_time", "")

    if not _is_today_upcoming(commence_time):
        return results

    # best[(market_key, player_name, point_str, outcome_name)] = (decimal_odds, book_title)
    best: dict[tuple, tuple[float, str]] = {}

    for bm in event_data.get("bookmakers", []):
        for mkt in bm.get("markets", []):
            mkt_key = mkt["key"]
            mkt_player = mkt.get("description") or ""
            for out in mkt.get("outcomes", []):
                # Player name lives on the market OR on each outcome depending on bookmaker
                player = mkt_player or out.get("description") or "Unknown"
                point = out.get("point")
                point_str = str(point) if point is not None else ""
                price = out.get("price", 0)
                if price <= 1.0:
                    continue
                raw_outcome = str(out.get("name", "")).strip()
                outcome_lower = raw_outcome.lower()
                normalized_outcome = (
                    "Over" if outcome_lower.startswith("over") else
                    "Under" if outcome_lower.startswith("under") else
                    raw_outcome
                )
                key = (mkt_key, player, point_str, normalized_outcome)
                if key not in best or price > best[key][0]:
                    best[key] = (price, bm["title"])

    # Re-group into (market_key, player, point_str) -> legs
    groups: dict[tuple, list[Leg]] = {}
    for (mkt_key, player, point_str, outcome), (dec, book) in best.items():
        groups.setdefault((mkt_key, player, point_str), []).append(Leg(
            book=book,
            outcome=outcome,
            decimal_odds=dec,
            american_odds=decimal_to_american(dec),
            implied_prob=implied_prob(dec),
        ))

    for (mkt_key, player, point_str), legs in groups.items():
        if len(legs) < 2:
            continue
        # Hard guard: skip anything that isn't a numeric over/under line
        if {l.outcome for l in legs} != {"Over", "Under"} or not point_str:
            continue
        total_impl = sum(l.implied_prob for l in legs)
        edge = (1 - total_impl) * 100
        prop_label = mkt_key.replace("player_", "").replace("_", " ").title()
        suffix = f" {point_str}" if point_str else ""
        results.append(ArbOpportunity(
            event_name=f"{base_name} · {player} {prop_label}{suffix}",
            sport=sport,
            commence_time=commence_time,
            legs=legs,
            total_implied=total_impl,
            edge_pct=round(edge, 4),
            is_arb=total_impl < 1.0,
            source="props",
        ))
    return results


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
        commence_time = ev.get("commence_time", "")
        if not _is_today_upcoming(commence_time):
            continue
        base_name = f"{ev.get('home_team','?')} vs {ev.get('away_team','?')}"
        sport = ev.get("sport_key", "")

        # === H2H and Totals ===
        # Group by (mkt_type, point_str, outcome_label) — same-line odds only.
        # Totals are safe: "Over 6.5" and "Under 6.5" share point_str "6.5" and
        # are genuinely complementary, so grouping by raw point is correct.
        best: dict[tuple, tuple[float, str]] = {}
        for bm in ev.get("bookmakers", []):
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
KALSHI_AUTH_BASE = KALSHI_BASE  # kept for compat with fetch_kalshi_markets

async def kalshi_login(session: aiohttp.ClientSession, email: str, password: str) -> str:
    """Exchange Kalshi email/password for a session token."""
    url = f"{KALSHI_BASE}/login"
    payload = {"email": email, "password": password}
    async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        body = await resp.text()
        if resp.status != 200:
            print(f"  [kalshi] Login failed HTTP {resp.status}: {body[:600]}")
            raise ValueError(f"Kalshi login failed (HTTP {resp.status})")
        try:
            data = json.loads(body)
        except Exception:
            print(f"  [kalshi] Login response not JSON: {body[:600]}")
            raise ValueError("Kalshi login: non-JSON response")
        token = data.get("token") or data.get("access_token") or data.get("member_token")
        if not token:
            print(f"  [kalshi] Login response keys: {list(data.keys())}  body: {body[:600]}")
            raise ValueError("Kalshi login response missing token")
        print("  [kalshi] Login successful")
        return token


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


async def fetch_kalshi_markets(session: aiohttp.ClientSession, kalshi_token: str) -> list[ArbOpportunity]:
    """
    Fetch active binary markets from Kalshi and model each YES/NO as a two-leg opportunity.
    Kalshi YES + NO prices should sum to ~$1.00 (100 cents). If they sum to < $1.00, arb exists.
    """
    url = f"{KALSHI_BASE}/markets?limit=200&status=open"
    headers = {"Content-Type": "application/json"}
    if kalshi_token:
        headers["Authorization"] = f"Bearer {kalshi_token}"

    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                body = await resp.text()
                print(f"  [kalshi] markets HTTP {resp.status}: {body[:300]}")
                return []
            data = await resp.json()
            markets = data.get("markets", [])
            sports_markets = [m for m in markets if _looks_like_sports_market(m)]
            print(f"  [kalshi] {len(markets)} open markets ({len(sports_markets)} sports-related)")
            return parse_kalshi_markets(sports_markets)
    except Exception as e:
        print(f"  [kalshi] {e}")
        return []


def parse_kalshi_markets(markets: list[dict]) -> list[ArbOpportunity]:
    results = []
    for m in markets:
        yes_ask = m.get("yes_ask")   # cents — price to BUY yes
        no_ask = m.get("no_ask")    # cents — price to BUY no
        if yes_ask is None or no_ask is None:
            continue
        if yes_ask <= 0 or no_ask <= 0 or yes_ask >= 100 or no_ask >= 100:
            continue

        # Convert cents to decimal odds: pay X cents to win 100 cents
        yes_dec = 100 / yes_ask
        no_dec = 100 / no_ask
        total_impl = (yes_ask / 100) + (no_ask / 100)
        edge = (1 - total_impl) * 100

        legs = [
            Leg(book="Kalshi", outcome="YES", decimal_odds=round(yes_dec, 4),
                american_odds=decimal_to_american(yes_dec),
                implied_prob=yes_ask / 100,
                market_url=f"https://kalshi.com/markets/{m.get('ticker','')}"),
            Leg(book="Kalshi", outcome="NO", decimal_odds=round(no_dec, 4),
                american_odds=decimal_to_american(no_dec),
                implied_prob=no_ask / 100,
                market_url=f"https://kalshi.com/markets/{m.get('ticker','')}"),
        ]

        results.append(ArbOpportunity(
            event_name=m.get("title", m.get("ticker", "Unknown")),
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
# Cross-market arb: Kalshi YES vs sportsbook moneyline
# ---------------------------------------------------------------------------

def find_cross_market_arbs(
    sportsbook_opps: list[ArbOpportunity],
    kalshi_opps: list[ArbOpportunity],
) -> list[ArbOpportunity]:
    mappings = list(CROSS_MARKET_MAPPINGS)
    if CROSS_MARKET_AUTO_MAP_ENABLED:
        mappings.extend(auto_generate_cross_market_mappings(
            sportsbook_opps, kalshi_opps, max_hours=CROSS_MARKET_AUTO_MAP_MAX_HOURS
        ))
    if not mappings:
        return []

    cross = []
    kalshi_by_ticker = {}
    for k in kalshi_opps:
        for leg in k.legs:
            if "/markets/" in leg.market_url:
                t = leg.market_url.rsplit("/", 1)[-1].strip().upper()
                if t:
                    kalshi_by_ticker[t] = k

    for mapping in mappings:
        ticker = mapping.kalshi_ticker.strip().upper()
        k_opp = kalshi_by_ticker.get(ticker)
        if not k_opp:
            continue
        k_leg = next((leg for leg in k_opp.legs if leg.outcome.upper() == mapping.kalshi_leg.upper()), None)
        if not k_leg:
            continue

        sb_candidates = [s for s in sportsbook_opps if mapping.sportsbook_event_contains.lower() in s.event_name.lower()]
        for sb_opp in sb_candidates:
            for sb_leg in sb_opp.legs:
                if mapping.sportsbook_outcome_contains.lower() not in sb_leg.outcome.lower():
                    continue
                total = sb_leg.implied_prob + k_leg.implied_prob
                edge = (1 - total) * 100
                if total < 1.0:
                    cross.append(ArbOpportunity(
                        event_name=f"{sb_opp.event_name} [cross-market mapped:{ticker}]",
                        sport=sb_opp.sport,
                        commence_time=sb_opp.commence_time,
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
        }, indent=2)


async def scan(
    odds_api_key: str = "",
    kalshi_token: str = "",
    kalshi_email: str = "",
    kalshi_password: str = "",
    sports: Optional[list[str]] = None,
    arbs_only: bool = False,
    min_edge: float = 0.0,
    include_cross_market: bool = False,
) -> ScanResult:
    all_opps: list[ArbOpportunity] = []
    books_seen: set[str] = set()
    errors: list[str] = []

    async with aiohttp.ClientSession() as session:
        # Kalshi's elections API is public — no login needed to read market data.
        # Login is skipped; markets are fetched unauthenticated.

        # Resolve sport list — fetch all active sports from API when none specified
        if not sports and odds_api_key:
            sports = await fetch_all_active_sports(session, odds_api_key)
        elif not sports:
            sports = SPORTS

        tasks = []

        if odds_api_key:
            for sport in sports:
                tasks.append(fetch_sport_odds(session, odds_api_key, sport))

        kalshi_task = None
        # Always attempt Kalshi if any credentials were configured.
        # fetch_kalshi_markets falls back to the public API when token is empty,
        # so a failed login doesn't silently skip all Kalshi data.
        if kalshi_token or kalshi_email:
            kalshi_task = fetch_kalshi_markets(session, kalshi_token)

        print(f"[scanner] Launching {len(tasks)} sportsbook + {'1 Kalshi' if kalshi_task else '0 Kalshi'} requests...")
        t0 = time.time()

        sb_results_raw = await asyncio.gather(*tasks, return_exceptions=True)
        kalshi_opps: list[ArbOpportunity] = []
        if kalshi_task:
            try:
                kalshi_opps = await kalshi_task
            except Exception as e:
                errors.append(f"Kalshi: {e}")

        sb_opps: list[ArbOpportunity] = []
        for res in sb_results_raw:
            if isinstance(res, Exception):
                errors.append(str(res))
            elif res:
                parsed = parse_sportsbook_events(res)
                sb_opps.extend(parsed)
                for opp in parsed:
                    for leg in opp.legs:
                        books_seen.add(leg.book)

        # Player props — two-step: event IDs first, then per-event prop odds
        prop_opps: list[ArbOpportunity] = []
        if odds_api_key:
            prop_sports = [s for s in sports if s in PLAYER_PROP_MARKETS]
            if prop_sports:
                print(f"  [props] Fetching event lists for {len(prop_sports)} sport(s)...")
                event_lists = await asyncio.gather(
                    *[fetch_sport_events(session, odds_api_key, sp) for sp in prop_sports],
                    return_exceptions=True,
                )
                prop_tasks = []
                for sp, events in zip(prop_sports, event_lists):
                    if isinstance(events, Exception) or not events:
                        print(f"  [props] {sp}: no events found")
                        continue
work/kalshi-nj-fix
                    if events and events[0].get("_quota_limited"):
                        props_quota_limited = True
                        continue
                    print(f"  [props] {sp}: {len(events)} event(s)")
main
                    prop_markets = PLAYER_PROP_MARKETS[sp]
                    for ev in events:
                        prop_tasks.append(
                            fetch_event_player_props(session, odds_api_key, sp, ev["id"], prop_markets)
                        )
                if prop_tasks:
                    print(f"  [props] Fetching props for {len(prop_tasks)} event(s)...")
                    prop_results = await asyncio.gather(*prop_tasks, return_exceptions=True)
                    none_count = sum(1 for r in prop_results if r is None or isinstance(r, Exception))
                    for res in prop_results:
                        if res and not isinstance(res, Exception):
                            prop_opps.extend(parse_player_props(res))
                    print(f"  [props] {len(prop_opps)} prop markets parsed ({none_count}/{len(prop_tasks)} event fetches failed)")
                else:
                    print(f"  [props] No events to fetch props for")

        all_opps = sb_opps + kalshi_opps + prop_opps

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
    )


# ---------------------------------------------------------------------------
# Auto-refresh loop
# ---------------------------------------------------------------------------

async def run_loop(
    odds_api_key: str = "",
    kalshi_token: str = "",
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
                kalshi_token=kalshi_token,
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
    parser.add_argument("--kalshi-token", default=os.getenv("KALSHI_API_TOKEN", ""), help="Kalshi API token")
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
            result = await scan(odds_api_key=args.odds_key, kalshi_token=args.kalshi_token, min_edge=args.min_edge)
            print(result.to_json())
        elif args.loop:
            await run_loop(
                odds_api_key=args.odds_key,
                kalshi_token=args.kalshi_token,
                interval_seconds=args.interval,
                min_edge=args.min_edge,
                on_result=print_result,
            )
        else:
            result = await scan(odds_api_key=args.odds_key, kalshi_token=args.kalshi_token, min_edge=args.min_edge)
            print_result(result)

    asyncio.run(main())


PROP_EVENT_CAP_PER_SPORT = 5

CROSS_MARKET_MAPPINGS = []
CROSS_MARKET_AUTO_MAP_ENABLED = True
CROSS_MARKET_AUTO_MAP_MAX_HOURS = 24



def _parse_iso8601_utc(ts: str):
    if not ts:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _extract_teams_from_event_name(event_name: str):
    base = event_name.split("[", 1)[0].strip()
    if " vs " not in base:
        return None
    a, b = [x.strip() for x in base.split(" vs ", 1)]
    return (a, b) if a and b else None

def _infer_yes_team_from_title(title: str, team_a: str, team_b: str):
    t = title.lower()
    if f"will {team_a.lower()}" in t:
        return team_a
    if f"will {team_b.lower()}" in t:
        return team_b
    return None

def auto_generate_cross_market_mappings(sportsbook_opps, kalshi_opps, max_hours: int = 24):
    from datetime import datetime, timezone
    mappings = []
    now = datetime.now(timezone.utc)
    blocked_tokens = {"championship", "futures", "season", "playoffs", "title", "cup winner"}

    kalshi_items = []
    for k in kalshi_opps:
        ticker = ""
        for leg in k.legs:
            if "/markets/" in leg.market_url:
                ticker = leg.market_url.rsplit("/", 1)[-1].strip().upper()
                if ticker:
                    break
        if ticker:
            kalshi_items.append((k.event_name, ticker, _parse_iso8601_utc(k.commence_time)))

    for sb in sportsbook_opps:
        if "[ML]" not in sb.event_name:
            continue
        teams = _extract_teams_from_event_name(sb.event_name)
        if not teams:
            continue
        team_a, team_b = teams
        sb_dt = _parse_iso8601_utc(sb.commence_time)
        if not sb_dt:
            continue
        if abs((sb_dt - now).total_seconds()) > max_hours * 3600:
            continue

        for k_title, ticker, k_dt in kalshi_items:
            kt = k_title.lower()
            if any(tok in kt for tok in blocked_tokens):
                continue
            if team_a.lower() not in kt or team_b.lower() not in kt:
                continue
            if k_dt is not None and abs((k_dt - sb_dt).total_seconds()) > max_hours * 3600:
                continue
            yes_team = _infer_yes_team_from_title(k_title, team_a, team_b)
            if not yes_team:
                continue
            no_team = team_b if yes_team == team_a else team_a
            mappings.append(CrossMarketMapping(f"{team_a} vs {team_b}", no_team, ticker, "YES"))
            mappings.append(CrossMarketMapping(f"{team_a} vs {team_b}", yes_team, ticker, "NO"))

    dedup = {(m.sportsbook_event_contains, m.sportsbook_outcome_contains, m.kalshi_ticker, m.kalshi_leg): m for m in mappings}
    return list(dedup.values())
