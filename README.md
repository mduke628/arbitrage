# arbitrage
Arbitrage Scanner
Scans DraftKings, FanDuel, BetMGM, Caesars, bet365, Fanatics, Pinnacle, Bovada, and 10+ more sportsbooks — plus Kalshi prediction markets — for guaranteed-profit arbitrage opportunities. Runs continuously and pushes live updates to the browser UI.

Quick start
1. Install dependencies
bashpip install -r requirements.txt
2. Get your API keys
The Odds API (sportsbooks)

Sign up free at https://the-odds-api.com
Free tier: 500 requests/month
Paid tiers from $79/mo for real-time polling

Kalshi (prediction markets)

Sign up at https://kalshi.com
Generate an API token at https://kalshi.com/profile/api
Kalshi markets are legal US prediction markets

3. Set environment variables (optional)
bashexport ODDS_API_KEY="2d9066e02eb94480c455e7c2f0421dd3"
export KALSHI_API_TOKEN="8b8ae9ee-a48d-4da8-ab18-f9a32d8c990e"
export SCAN_INTERVAL=60   # seconds between scans
export MIN_EDGE=0.01       # minimum edge % to report

CLI usage
One-shot scan (table output)
bashpython scanner.py --odds-key YOUR_KEY --kalshi-token YOUR_TOKEN
One-shot scan (JSON output)
bashpython scanner.py --odds-key YOUR_KEY --kalshi-token YOUR_TOKEN --output json > results.json
Continuous loop (prints on every refresh)
bashpython scanner.py --odds-key YOUR_KEY --kalshi-token YOUR_TOKEN --loop --interval 60
Show only arbs, minimum 0.5% edge
bashpython scanner.py --odds-key YOUR_KEY --arbs-only --min-edge 0.5

API server + browser UI
The server exposes a REST API and WebSocket that the browser UI connects to.
Start the server
bashpython server.py
# or
uvicorn server:app --reload --port 8000
Open the UI
Open ui.html in your browser. Enter your API keys in the settings panel, click Connect, and the dashboard will auto-refresh.
API endpoints
MethodPathDescriptionGET/scanTrigger a one-shot scanGET/statusReturn the last scan resultPOST/configUpdate API keys / settingsWS/wsWebSocket stream of scan resultsGET/sportsList available sports

Architecture
scanner.py          Core engine — fetches odds, detects arbs, computes stakes
server.py           FastAPI server — background loop, WebSocket broadcast
ui.html             Browser dashboard — connects to server via WebSocket
requirements.txt    Python dependencies
How arb detection works

For each event, find the best decimal odds for each outcome across all books
Sum the implied probabilities of those best odds
If the sum < 1.0 → guaranteed profit exists
Edge % = (1 - sum) × 100
Optimal stakes: split your total bankroll proportionally to implied probs so the payout is equal on every leg

Kalshi markets
Kalshi YES/NO contracts sum to roughly $1.00 (100 cents). When yes_ask + no_ask < 100, you can buy both sides and lock in a profit. The scanner models each market as a two-leg opportunity.
Cross-market arbs
The scanner attempts fuzzy-matching between Kalshi markets and sportsbook events by team name. This is heuristic — validate any cross-market opportunity manually before betting.

Limitations & risks

Account bans: sportsbooks actively limit or ban arbitrageurs. Use multiple accounts, keep stakes reasonable, and don't always take the max.
Line movement: odds can change between the time you see an arb and when you place both bets. Always place the harder-to-fill leg first.
Withdrawal limits: some books take days to withdraw. Factor in capital tied up.
Minimum bet sizes: some arbs require very small stakes on one leg and may fall below a book's minimum.
Kalshi: prediction markets are legal but have lower liquidity than sportsbooks; large bets move the market.
This tool does not place bets automatically (sportsbook APIs are not publicly available for bet placement).


Extending
To add a new sportsbook available on The Odds API, add its key to BOOKMAKERS in scanner.py.
To add a new prediction market (Polymarket, Manifold, etc.), implement an async fetch_X_markets() function following the same pattern as fetch_kalshi_markets() and call it from scan().Share
