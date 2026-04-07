# Algo Trading Desk — Project Memory

## Mission
Build profitable, automated trading bots. Start with Kalshi prediction markets (weather contracts), expand to IBKR equities and other exchanges.

## Current State (Updated 2026-03-24)

### Architecture
```
algo-desk/
├── main.py                          # Main loop: fetches Kalshi markets, runs strategies, logs signals
├── strategies/kalshi_weather/       # WeatherStrategy: ensemble forecasts → probability → edge detection
├── execution/kalshi_executor.py     # Kalshi REST API v2 client (auth, markets, orders, positions)
├── execution/paper_executor.py      # Paper trading: virtual positions, P&L, slippage simulation
├── risk/daily_limits.py             # Pre-trade risk checks (loss limit, concentration, frequency)
├── risk/position_manager.py         # SQLite-backed position tracking across platforms
├── risk/kill_switch.py              # Emergency shutdown
├── monitoring/                      # Dashboard, alerts, health checks
├── data/                            # Market data pipeline, SQLite storage
├── claude_integration/              # Claude API for briefings/journal/probability
├── config/settings.yaml             # System config
└── config/strategies.yaml           # Strategy params (kalshi_weather enabled, min_edge=0.05)
```

### What Works
- Kalshi API v2 authenticated via API key (Bearer token)
- Fetches real markets from 10 weather series: KXHIGHNY, KXLOWNY, KXHIGHLAX, etc.
- Ticker parsing: KXHIGHNY-26MAR25-T58 → location=NYC, date=2026-03-25, threshold=58°F
- Ticker date format: YYMmmDD with Kalshi fiscal year (25=FY25=calendar 2026)
- B-prefix contracts (KXLOWNY-26MAR25-B32) = TEMP_BELOW, inverted in probability calc
- Signal generation: model prob vs market implied prob → edge in bps → BUY/SELL/HOLD/SKIP
- Deduplication: best signal per contract_id by edge
- Expired and same-day contracts filtered out
- Paper trading mode (--paper flag) logs signals without executing

### What's Fake / Not Wired
- ~~NOAA data is MOCK~~ → **FIXED 2026-03-24**: Real NOAA API (api.weather.gov gridpoint forecasts) with synthetic fallback
- ~~Paper executor not called from main.py~~ → **FIXED 2026-03-24**: Signals now flow through risk gate → paper executor → P&L tracking
- ~~Risk checks not in execution path~~ → **FIXED 2026-03-24**: daily_limits.can_trade() gates every paper trade
- No backtesting framework
- IBKR disabled (Mac Mini arriving ~end of April 2026)
- Optional deps (scipy, sqlalchemy, ib_insync) gracefully degrade — __init__.py imports are conditional

### Key Design Decisions
- Kalshi prices: API returns yes_bid_dollars (0.0-1.0) or yes_bid (cents 0-100). Normalization in main.py: `price * 100 if price < 1.0 else price` → always 0-100 scale
- Min edge threshold: 150 bps (config min_edge=0.05 × 10000)
- Paper trading capital: $10,000, 5bps slippage (Kalshi spreads are wide), $0 commission
- Risk limits: 3% daily loss ($300), 2% max per trade ($200), 20% max concentration, 100% max gross exposure
- Position sizing: base 5 contracts, scaled by edge_multiplier × confidence × vol_discount. Volume discount: 0-vol=25%, <10=50%, <50=75%, 50+=100%
- Weather strategy: real NOAA hourly temps → extract daily high → Normal(forecast_high, uncertainty) → P(high > threshold). Uncertainty = sqrt(2.5² + top_quartile_spread²)°F. Precip uses empirical fraction.
- Dynamic thresholds: extracted from live Kalshi tickers, injected via `_dynamic_thresholds`
- NOAA API: rate-limited to ~2 req/sec (0.5s sleep), grid lookups cached per session
- Position limits: max 8 new per scan, max 15 total open, no duplicate contracts
- Liquidity filters: skip no-quote/no-volume contracts, spread > 40c, deep ITM/OTM (< 3c or > 97c). Per-filter counts logged each scan.
- Signal logging includes bid/ask/volume/spread for each signal for transparency
- CRITICAL FINDING (2026-03-24): ALL Kalshi weather contracts showed volume=0 in live testing. MM quotes are tight (1-3c spread) but zero actual trades. Volume-based position sizing discount applied (75% haircut for zero-vol). Need to investigate whether weather markets are fundamentally illiquid or if we're hitting wrong time of day / wrong series.
- Paper settlement: expired positions auto-closed using actual NOAA high temp data

### Known Bugs / Risks (as of 2026-03-24)
- ~~get_market_by_ticker() uses old field names~~ → **FIXED**: now uses same yes_bid_dollars/yes_bid priority as get_markets()
- ~~Probability calc used fraction-of-hours instead of daily-high~~ → **FIXED 2026-03-24**: Now models P(daily_high > threshold) using Normal(forecast_max, uncertainty) where uncertainty = sqrt(NWS_RMSE² + top_quartile_spread²). NWS day-1 high RMSE ≈ 2.5°F.
- ~~27 positions opened in first run, no position cap~~ → **FIXED 2026-03-24**: Max 8 new positions per scan, max 15 total open, no duplicates.
- ~~Phantom 6000-9400 bps edges on illiquid contracts~~ → **FIXED 2026-03-24**: Added three liquidity filters: (1) skip contracts with no bid/ask and no volume, (2) skip if bid-ask spread > 40 cents, (3) skip deep ITM/OTM (price < 3c or > 97c). Root cause: stale `last_price` on thinly traded deep-ITM contracts was treated as real market disagreement.
- ~~Kalshi executor `or`-chain treated 0 as falsy~~ → **FIXED 2026-03-24**: Price field parsing now uses `is not None` checks so a genuine $0 bid/ask is not skipped.
- ~~Paper positions never settle~~ → **FIXED 2026-03-24**: Settlement logic in main.py checks expired positions each scan, fetches actual high from NOAA, closes at 1.0 (YES) or 0.0 (NO).
- Price normalization boundary case: a contract at exactly $1.00 (0-1 scale) would be treated as 1 cent. Not a real-world issue since $1.00 contracts are settled, never trading.
- NOAA hourly data != true ensemble — it's a time-series from one model, not multiple model runs. We use the max as the forecasted daily high and model uncertainty around it. Consider supplementing with GFS/HRRR ensemble spread later for better uncertainty estimation.
- The daily-high model uses a single normal distribution centered on the forecast max. Real forecast error distributions have fatter tails. Could improve with historical bias data per city/season.
- Settlement depends on NOAA still having past-date data in the gridpoint forecast. NWS typically keeps ~7 days of historical data; older contracts may not auto-settle.

### API Details
- Kalshi API base: https://api.elections.kalshi.com/trade-api/v2
- Auth: Bearer token via API key (env: KALSHI_API_KEY)
- NOAA API: https://api.weather.gov (no auth needed, User-Agent required)
- NOAA gridpoint mapping: location code → lat,lon → /points/{lat},{lon} → grid office/X,Y → /gridpoints/{office}/{X},{Y}

### Environment
- Python with scipy, numpy, pandas, requests, SQLAlchemy, anthropic
- SQLite databases: trading.db, market_data.db
- Config: .env for secrets, config/*.yaml for settings
- Paper state: logs/paper_state.json (positions, trade log, cash — persists across restarts)

## Milestones
1. ✅ Kalshi market fetching + ticker parsing
2. ✅ Weather strategy signal generation (mock data)
3. ✅ Real NOAA ensemble data integration (2026-03-24)
4. ✅ Paper executor wired into signal loop with risk checks (2026-03-24)
5. ✅ get_market_by_ticker field fix + price normalization tests (2026-03-24)
6. ✅ FIRST LIVE RUN — real NOAA + real Kalshi + paper execution (2026-03-24)
7. ✅ Critical fix: daily-high probability model (was using hourly fraction) (2026-03-24)
8. ✅ Position cap: max 8 new/scan, 15 total, no duplicates (2026-03-24)
9. ✅ Paper position settlement logic — auto-close at expiry using actual NOAA temps (2026-03-24)
10. ✅ Liquidity filtering — skip illiquid/stale-priced contracts, log bid/ask/spread (2026-03-24)
11. ✅ Kalshi executor 0-value price parsing fix (2026-03-24)
12. ✅ Volume-based position sizing discount (75% haircut for zero-volume contracts) (2026-03-24)
13. ✅ Per-filter diagnostics logging (counts per filter each scan) (2026-03-24)
14. ✅ Paper executor state persistence (positions survive restart via logs/paper_state.json) (2026-03-24)
15. ⬜ FIRST MODEL CALIBRATION — run tomorrow morning after March 25 contracts settle
16. ⬜ NOAA forecast accuracy tracking (log predictions vs actuals, build calibration table)
17. ⬜ Backtesting framework with historical data
18. ⬜ Live trading on Kalshi (real money, small size) — only after 2+ weeks of paper validation
19. ⬜ IBKR integration (post Mac Mini arrival ~end April 2026)

## How to Run
```bash
cd algo-desk
python main.py --paper                    # Paper trading mode
python main.py --paper --strategy kalshi_weather  # Weather only
python main.py --dashboard                # With dashboard display
```

## How Matt Wants Us to Work
- Read source files before touching anything
- State plan in 2-3 sentences before coding
- Make changes directly in repo — no patch scripts
- After every change, say exactly what command to test
- End every response with prioritized "Up Next" (top 3)
- Think like a quant: edge, risk, signal quality, capital efficiency
- Flag any decision that could affect live money
