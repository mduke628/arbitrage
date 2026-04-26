"""
Arb Scanner API Server
Run with: uvicorn server:app --reload --port 8000

Endpoints:
  GET  /scan          - one-shot scan, returns JSON
  GET  /status        - last scan result
  WS   /ws            - websocket, pushes new results every interval
  POST /config        - update API keys / settings at runtime
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from scanner import scan, run_loop, ScanResult, SPORTS

# ---------------------------------------------------------------------------
# Config (loaded from env or set via /config)
# ---------------------------------------------------------------------------

class Config(BaseModel):
    odds_api_key: str = os.getenv("ODDS_API_KEY", "e4e92b78d437e818a1af0355704c4de9")
    kalshi_token: str = ""  # populated at runtime via login; do not hardcode
    kalshi_email: str = os.getenv("KALSHI_EMAIL", "dukem628@gmail.com")
    kalshi_password: str = os.getenv("KALSHI_PASSWORD", "F@ntasyB@sk3tball")
    interval_seconds: int = int(os.getenv("SCAN_INTERVAL", "60"))
    min_edge: float = float(os.getenv("MIN_EDGE", "0.0"))
    arbs_only: bool = False
    sports: list[str] = []  # empty = scanner fetches all active sports from API at runtime

config = Config()
last_result: ScanResult | None = None
ws_clients: list[WebSocket] = []

# ---------------------------------------------------------------------------
# Background scan loop
# ---------------------------------------------------------------------------

async def broadcast(result: ScanResult):
    global last_result
    last_result = result
    payload = result.to_json()
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
        if config.odds_api_key or config.kalshi_email:
            try:
                result = await scan(
                    odds_api_key=config.odds_api_key,
                    kalshi_token=config.kalshi_token,
                    kalshi_email=config.kalshi_email,
                    kalshi_password=config.kalshi_password,
                    sports=config.sports,
                    min_edge=config.min_edge,
                )
                await broadcast(result)
            except Exception as e:
                print(f"[server] scan error: {e}")
        await asyncio.sleep(config.interval_seconds)

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield  # no background auto-scan; scan only fires on /scan

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="Arb Scanner", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/status")
async def status():
    if last_result is None:
        return {"status": "no_scan_yet", "message": "Configure keys and trigger /scan"}
    return JSONResponse(content=json.loads(last_result.to_json()))

@app.get("/scan")
async def trigger_scan():
    if not config.odds_api_key and not config.kalshi_email:
        return JSONResponse(status_code=400, content={"error": "No API keys configured."})
    result = await scan(
        odds_api_key=config.odds_api_key,
        kalshi_token=config.kalshi_token,
        kalshi_email=config.kalshi_email,
        kalshi_password=config.kalshi_password,
        sports=config.sports,
        min_edge=config.min_edge,
    )
    await broadcast(result)
    return JSONResponse(content=json.loads(result.to_json()))

@app.post("/config")
async def update_config(new_cfg: Config):
    global config
    # Preserve the hardcoded API keys — the UI no longer sends them
    new_cfg.odds_api_key = config.odds_api_key
    new_cfg.kalshi_token = config.kalshi_token
    new_cfg.kalshi_email = config.kalshi_email
    new_cfg.kalshi_password = config.kalshi_password
    config = new_cfg
    return {"status": "updated", "config": config.model_dump(exclude={"odds_api_key", "kalshi_token", "kalshi_email", "kalshi_password"})}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    ws_clients.append(websocket)
    # Send latest result immediately on connect
    if last_result:
        await websocket.send_text(last_result.to_json())
    try:
        while True:
            await websocket.receive_text()  # keep alive
    except WebSocketDisconnect:
        ws_clients.remove(websocket)

@app.get("/sports")
async def list_sports():
    return {"sports": SPORTS}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
