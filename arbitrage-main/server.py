"""
Arb Scanner API Server
Run with: uvicorn server:app --reload --port 8000

Endpoints:
  GET  /scan          - one-shot scan, returns JSON
  GET  /status        - last scan result + auto-trade log
  WS   /ws            - websocket, pushes new results every interval
  POST /config        - update API keys / settings at runtime
  GET  /trade-log     - recent auto-trade actions
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Load .env from the same directory as this file, if present.
# override=True ensures .env values win over any stale OS-level env vars.
try:
    from dotenv import load_dotenv
    _env = Path(__file__).parent / ".env"
    if _env.exists():
        load_dotenv(_env, override=True, encoding="utf-8-sig")  # utf-8-sig strips Windows BOM if present
        print(f"[server] Loaded environment from {_env}")
    else:
        print(f"[server] No .env file found at {_env} — using OS environment only")
except ImportError:
    print("[server] python-dotenv not installed — reading keys from OS environment only")

from scanner import scan, run_loop, ScanResult, SPORTS, place_kalshi_order

# ---------------------------------------------------------------------------
# Config (loaded from env or set via /config)
# ---------------------------------------------------------------------------

class Config(BaseModel):
    # Use default_factory so os.getenv is called at instantiation time, not at class-definition
    # time. This guarantees load_dotenv has already run when the values are read.
    odds_api_key:      str   = Field(default_factory=lambda: os.getenv("ODDS_API_KEY",      ""))
    kalshi_api_key:    str   = Field(default_factory=lambda: os.getenv("KALSHI_API_KEY",    ""))
    kalshi_api_token:  str   = Field(default_factory=lambda: os.getenv("KALSHI_API_TOKEN",  ""))
    interval_seconds:  int   = Field(default_factory=lambda: int(os.getenv("SCAN_INTERVAL", "60")))
    min_edge:          float = Field(default_factory=lambda: float(os.getenv("MIN_EDGE", "0.0")))
    arbs_only:         bool  = False
    sports:            list[str] = Field(default_factory=list)
    bankroll:          float = Field(default_factory=lambda: float(os.getenv("BANKROLL", "1000.0")))
    auto_trade:        bool  = Field(default_factory=lambda: os.getenv("AUTO_TRADE", "false").lower() == "true")
    ev_threshold:      float = Field(default_factory=lambda: float(os.getenv("EV_THRESHOLD", "5.0")))

config = Config()

# Print key-configuration status at startup so problems are obvious in the log.
print(f"[server] ODDS_API_KEY    : {'SET (' + str(len(config.odds_api_key)) + ' chars)' if config.odds_api_key else 'NOT SET — add ODDS_API_KEY=... to your .env'}")
print(f"[server] KALSHI_API_KEY  : {'SET (' + str(len(config.kalshi_api_key)) + ' chars)' if config.kalshi_api_key else 'NOT SET — add KALSHI_API_KEY=... to your .env'}")
print(f"[server] KALSHI_API_TOKEN: {'SET (' + str(len(config.kalshi_api_token)) + ' chars)' if config.kalshi_api_token else 'NOT SET (optional — add KALSHI_API_TOKEN=... to your .env for bearer auth)'}")
last_result: ScanResult | None = None
ws_clients: list[WebSocket] = []

# Tracks (ticker, side) pairs already ordered this session to avoid duplicates.
placed_orders: set[tuple[str, str]] = set()
# Last 50 auto-trade actions for the /trade-log endpoint.
trade_log: list[dict] = []

# ---------------------------------------------------------------------------
# Kalshi auto-trading
# ---------------------------------------------------------------------------

async def auto_trade_kalshi(result: ScanResult) -> None:
    """
    For each +EV Kalshi bet in the scan result that exceeds the EV threshold,
    place a 25%-Kelly limit order if we haven't already ordered it this session.
    Live bets are excluded by find_kalshi_ev_bets (commence_time check).
    """
    if not config.auto_trade or not config.kalshi_api_key:
        return

    qualifying = [
        b for b in result.ev_bets
        if b.ev_pct >= config.ev_threshold
        and b.kalshi_ticker
        and b.kalshi_side
        and (b.kalshi_ticker, b.kalshi_side) not in placed_orders
    ]
    if not qualifying:
        return

    async with aiohttp.ClientSession() as session:
        for bet in qualifying:
            dec = bet.leg.decimal_odds
            net_odds = dec - 1
            if net_odds <= 0:
                continue

            kelly_full    = (bet.ev_pct / 100) / net_odds
            kelly_quarter = kelly_full / 4
            stake         = kelly_quarter * config.bankroll
            count         = max(1, int(stake / (bet.kalshi_ask_cents / 100)))

            print(
                f"[auto-trade] {bet.kalshi_side.upper()} {count}c "
                f"on {bet.kalshi_ticker} @ {bet.kalshi_ask_cents}¢  "
                f"EV={bet.ev_pct:.2f}%  Kelly={kelly_quarter*100:.2f}%  "
                f"stake=${stake:.2f}"
            )

            resp = await place_kalshi_order(
                session,
                config.kalshi_api_key,
                bet.kalshi_ticker,
                bet.kalshi_side,
                count,
                bet.kalshi_ask_cents,
            )

            entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "ticker": bet.kalshi_ticker,
                "side": bet.kalshi_side,
                "count": count,
                "limit_cents": bet.kalshi_ask_cents,
                "ev_pct": bet.ev_pct,
                "kelly_pct": round(kelly_quarter * 100, 2),
                "stake_usd": round(count * bet.kalshi_ask_cents / 100, 2),
                "event": bet.event_name,
                "http_status": resp.get("http_status"),
                "success": resp.get("http_status") == 201,
                "error": resp.get("error"),
            }
            trade_log.insert(0, entry)
            if len(trade_log) > 50:
                trade_log.pop()

            if resp.get("http_status") == 201:
                placed_orders.add((bet.kalshi_ticker, bet.kalshi_side))
                print(f"[auto-trade] ✓ order placed for {bet.kalshi_ticker}")
            else:
                print(f"[auto-trade] ✗ failed: {resp}")

# ---------------------------------------------------------------------------
# Background scan loop
# ---------------------------------------------------------------------------

async def broadcast(result: ScanResult):
    global last_result
    last_result = result
    try:
        payload = result.to_json()
    except Exception as e:
        print(f"[server] ERROR serializing scan result to JSON: {e}")
        import traceback; traceback.print_exc()
        return
    print(f"[server] Broadcasting to {len(ws_clients)} client(s): {result.total_scanned} markets, {result.arb_count} arbs, {len(result.ev_bets)} EV bets")
    dead = []
    for ws in ws_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.remove(ws)

async def background_loop():
    while True:
        if config.odds_api_key or config.kalshi_api_key:
            try:
                result = await scan(
                    odds_api_key=config.odds_api_key,
                    kalshi_api_key=config.kalshi_api_key,
                    sports=config.sports,
                    min_edge=config.min_edge,
                )
                await broadcast(result)
                await auto_trade_kalshi(result)
            except Exception as e:
                print(f"[server] scan error: {e}")
        await asyncio.sleep(config.interval_seconds)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(background_loop())
    yield
    task.cancel()

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Arb Scanner", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/status")
async def status():
    if last_result is None:
        return {"status": "no_scan_yet", "message": "Configure keys and trigger /scan"}
    data = json.loads(last_result.to_json())
    data["auto_trade_enabled"] = config.auto_trade
    data["recent_trades"] = trade_log[:5]
    return JSONResponse(content=data)

@app.get("/scan")
async def trigger_scan():
    if not config.odds_api_key and not config.kalshi_api_key:
        return JSONResponse(status_code=400, content={
            "error": "No API keys configured.",
            "fix": "Add ODDS_API_KEY and/or KALSHI_API_KEY to your .env file, then restart the server.",
        })
    result = await scan(
        odds_api_key=config.odds_api_key,
        kalshi_api_key=config.kalshi_api_key,
        sports=config.sports,
        min_edge=config.min_edge,
    )
    await broadcast(result)
    await auto_trade_kalshi(result)
    return JSONResponse(content=json.loads(result.to_json()))

@app.post("/config")
async def update_config(new_cfg: Config):
    global config
    new_cfg.odds_api_key   = config.odds_api_key
    new_cfg.kalshi_api_key = config.kalshi_api_key
    config = new_cfg
    return {
        "status": "updated",
        "config": config.model_dump(exclude={"odds_api_key", "kalshi_api_key"}),
    }

@app.get("/trade-log")
async def get_trade_log():
    return {"trades": trade_log, "auto_trade_enabled": config.auto_trade}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.append(websocket)
    if last_result:
        try:
            await websocket.send_text(last_result.to_json())
        except Exception as e:
            print(f"[server] ERROR sending cached result to new WebSocket client: {e}")
            import traceback; traceback.print_exc()
    try:
        while True:
            await websocket.receive_text()  # keep alive
    except WebSocketDisconnect:
        if websocket in ws_clients:
            ws_clients.remove(websocket)

@app.get("/sports")
async def list_sports():
    return {"sports": SPORTS}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
