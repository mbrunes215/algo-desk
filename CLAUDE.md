# Algo Trading Desk — Complete Project Guide (Updated 2026-04-10)

## Mission
Build profitable, automated trading bots for crypto and prediction markets. Currently live with two primary strategies: crypto funding rate arbitrage and BTC/ETH pairs trading.

---

## Current Operational State (as of 2026-04-10)

### Strategy Hierarchy
1. **PRIMARY — Crypto Funding Rate Arbitrage** (LIVE)
   - Delta-neutral: spot long + perp short to collect 8-hourly funding payments
   - Exchange: Kraken only (Coinbase INTX perps unavailable on US accounts)
   - Symbols: BTC, ETH, SOL
   - Status: Running, no open positions (market in negative funding regime)
   - Live rates (2026-04-10 ~20:00 UTC): BTC -246% to -964% ann, ETH brief +8.5–12%, SOL ~0%
   - April 7 spike: +378% ann on BTC, first paper trade opened and closed correctly when rate went negative
   - Market context: Broad risk-off / tariff sell-off driving perp shorts to be more valuable than longs

2. **SECONDARY — BTC/ETH Pairs Trading** (LIVE)
   - Market-neutral cointegration stat arb using Z-score of log(BTC/ETH) spread
   - Window: 288 observations (24 hours at 5-min intervals) — fully warmed up since April 7
   - Status: Running, no open positions
   - Live state (2026-04-10 ~20:56 UTC): Z-score -0.75 to -1.06 (BTC underperforming, below ±2.0 entry)
   - Signal valid at 72 obs minimum (25% of window) — has been generating valid signals since April 7
   - State persistence: Restart-safe via logs/pairs_state.json — warmup no longer lost

3. **TERTIARY — Kalshi Economic Event Scanner** (Passive, cron-based)
   - Monitors Kalshi economic prediction markets (Fed decisions, CPI, GDP, PCE, NFP)
   - Runs: 6:00 AM + 12:00 PM UTC daily via cron
   - Status: Monitoring, 0 active trades
   - CPI April 10 outcome: Found 50 contracts but 0 tradeable (SIG withdrew quotes at/after release)
   - Upcoming windows: GDP/PCE April 30 (quotes ~April 23), FOMC May 7 (quotes ~April 30)
   - Issue: Scanner timing — should run pre-release (before 8 AM ET) not after. Post-mortem needed.

4. **ARCHIVED — Kalshi Weather** (Disabled 2026-03-26)
   - Structural zero-volume confirmed across 250 contracts over 9+ days
   - MM spreads tight (1–3¢) but zero actual retail/institutional volume
   - Paper trading final: 25 positions, 4 wins / 21 losses, -$2.19 (-0.02%), cash $10,013.81
   - Ceiling: ~$20/week (confirmed via research on comparable bots)
   - Status: `enabled: false` in config/strategies.yaml — code preserved, archived

---

## Architecture & File Structure

```
algo-desk/
├── main.py                                    # Main system entry point
├── CLAUDE.md                                  # This file — project reference
├── config/
│   ├── strategies.yaml                        # All strategy configs (which enabled, params)
│   └── settings.yaml                          # System-level config
├── strategies/
│   ├── base_strategy.py                       # Abstract base class
│   ├── crypto_funding_arb/
│   │   ├── funding_arb_strategy.py            # Main strategy class
│   │   ├── run_monitor.py                     # Standalone runner (--loop for 30 min scans)
│   │   └── __init__.py
│   ├── pairs_trading/
│   │   ├── pairs_strategy.py                  # Z-score stat arb
│   │   ├── run_pairs.py                       # Standalone runner (--loop for 5 min scans)
│   │   └── __init__.py
│   ├── kalshi_weather/
│   │   └── weather_strategy.py                # ARCHIVED
│   └── kalshi_econ/
│       └── econ_strategy.py                   # Passive monitor
├── execution/
│   ├── kalshi_executor.py                     # Kalshi API v2 client (REST)
│   ├── kraken_executor.py                     # Kraken spot + futures orders
│   ├── paper_executor.py                      # Paper trading (virtual positions)
│   └── order_validator.py
├── risk/
│   ├── daily_limits.py                        # Pre-trade risk gates
│   ├── position_manager.py                    # Position tracking (SQLite)
│   └── kill_switch.py                         # Emergency shutdown
├── monitoring/
│   ├── health_checker.py                      # System health (API, memory, disk)
│   └── alerts.py
├── data/
│   ├── storage.py                             # SQLite session factory
│   └── outcome_tracker.py
├── logs/
│   ├── funding_arb.log                        # Funding arb scan output (live)
│   ├── funding_arb.pid                        # Process ID (managed by start_funding_arb.sh)
│   ├── funding_arb_state.json                 # Open positions state
│   ├── pairs_trading.log                      # Pairs trading output (live)
│   ├── pairs_trading.pid
│   ├── pairs_state.json                       # Price history + Z-score state
│   ├── memory_consolidation_log.md            # This week's memory updates
│   ├── econ_opportunity_log.md                # Econ scanner findings
│   ├── opportunity_log.md                     # Daily weather scan results
│   ├── latest_scan.json                       # Diagnostic snapshot
│   └── *.db                                   # SQLite databases
├── econ_scan.py                               # Economic events scanner (cron trigger)
├── daily_scan.py                              # Daily weather opportunity scan (cron trigger)
├── daily_report.py                            # Email summary reporter (cron trigger)
├── start_funding_arb.sh                       # Background process manager for funding arb
├── install_cron.sh                            # Cron job installer
├── check_funding_arb.py                       # Diagnostic script
├── settle_today.py                            # Manual settlement trigger (deprecated — weather archived)
├── .env                                       # Secrets (git-ignored)
└── .gitignore
```

---

## How to Run (from ~/Desktop/algo-desk/ on trading Mac)

### Funding Arbitrage
```bash
# One scan cycle
python3 strategies/crypto_funding_arb/run_monitor.py

# Background loop (every 30 minutes)
nohup python3 strategies/crypto_funding_arb/run_monitor.py --loop >> logs/funding_arb.log 2>&1 &
echo $! > logs/funding_arb.pid

# Or use the shell wrapper
./start_funding_arb.sh start
./start_funding_arb.sh status
./start_funding_arb.sh logs
./start_funding_arb.sh stop

# Check live log
tail -f logs/funding_arb.log
```

### Pairs Trading
```bash
# One scan
python3 strategies/pairs_trading/run_pairs.py

# Background loop (every 5 minutes)
nohup python3 strategies/pairs_trading/run_pairs.py --loop --interval 300 >> logs/pairs_trading.log 2>&1 &
echo $! > logs/pairs_trading.pid

# Monitor live
tail -f logs/pairs_trading.log
```

### Econ Scanner (usually cron, can run manually)
```bash
python3 econ_scan.py
# Output: logs/econ_opportunity_log.md, logs/econ_scan.json
```

### Daily Report (usually cron, can run manually)
```bash
python3 daily_report.py              # Send email report
python3 daily_report.py --preview    # Print to terminal
```

### Full System (Paper Trading)
```bash
python3 main.py --paper              # All enabled strategies
python3 main.py --paper --dashboard  # With dashboard
```

---

## Configuration Parameters

### Crypto Funding Arbitrage (strategies.yaml)
```yaml
crypto_funding_arb:
  enabled: true
  params:
    min_net_yield: 0.08          # 8% annualized net of fees to open
    exit_yield: 0.04             # 4% — close when yield drops here
    max_basis_pct: 0.005         # 0.5% max spot/perp divergence at entry
    position_size_usd: 500       # Per leg ($1000 total deployed)
    max_positions: 6             # Concurrent positions
    symbols: [BTC, ETH, SOL]
```

**Fee Structure:**
| Exchange | Spot Maker | Perp Maker | Round-Trip |
|----------|-----------|-----------|-----------|
| Kraken   | 0.16%     | 0.02%     | ~0.36%    |
| Binance  | 0.10%     | 0.02%     | ~0.24%    |

Break-even annualized: ~3.8% (Kraken), ~2.4% (Binance). Minimum threshold: 8% conservative.

### BTC/ETH Pairs Trading (strategies.yaml)
```yaml
pairs_trading:
  enabled: true
  params:
    entry_z: 2.0              # Open when |Z| > this
    exit_z: 0.5               # Close when |Z| < this
    stop_z: 3.5               # Stop-loss if spread widens
    window: 288               # 288 obs × 5 min = 24 hours
    position_size_usd: 500    # Per leg ($1000 total)
    max_positions: 2
    max_hold_hours: 72        # Force close after 3 days
```

### Kalshi Economic Scanner (econ_scan.py, hardcoded)
```python
MIN_EDGE_PCT = 5.0          # Minimum edge to flag
MIN_VOLUME = 100            # Skip ghost-town markets
MAX_SPREAD_CENTS = 15       # Skip wide spreads
PRICE_RANGE_LOW = 10        # Skip < 10¢ (bias kills edge)
PRICE_RANGE_HIGH = 90       # Skip > 90¢
```

---

## Critical Technical Details

### Crypto Funding Arb
- **Perpetual Funding:** Exchanged every 8 hours (3 payments/day = 1095/year)
- **Rate Annualization:** `rate_per_8h * 1095`
- **Entry Signal:** `net_annual_yield > min_net_yield` after all fees
- **Exit Signal:** `funding_rate < 0` (shorts now pay longs) or `yield < exit_yield`
- **Regime Behavior:** Negative rates occur in risk-off periods (e.g., April 2026 tariff sell-off pushed BTC to -800%+ ann). This is the worst environment for long arb. Reverse arb opportunity: short spot, long perp to collect negative rates.

### BTC/ETH Pairs Trading
- **Window Valid After:** 72 observations (25% of 288 window) = 6 hours at 5-min scans
- **Z-Score:** `(log(BTC/ETH) - rolling_mean) / rolling_std`
- **Entry Thresholds:** |Z| > 2.0 (standard 95th percentile deviation)
- **Stop-Loss:** |Z| > 3.5 (spread widening against position)
- **Mean Reversion Duration:** 2–4 hours typical (fast), up to 72h max hold
- **State Persistence:** Restart-safe via JSON state file — price history and Z-score model survive restarts

### Kalshi API v2 — Critical Gotchas
- **Price Fields:** Use `yes_bid_dollars` (0.0–1.0) or `yes_bid` (cents 0–100), NOT `bid_price` or `ask_price`
- **Normalization:** `price * 100 if price < 1.0 else price` → always 0–100 scale
- **Fiscal Year:** `KXHIGHNY-26MAR25-T58` = March 26, 2026 (FY25 = calendar 2026). Use `close_time` for settlement date.
- **B-Prefix Contracts:** Inverted probability. `KXLOWNY-26MAR25-B32` = TEMP_BELOW, P(high < 32) = 1 - P(high > 32)
- **Status Filter:** `status=open` works, `status=initialized` returns 400 error. For weather, omit status and use `series_ticker=KXHIGH*`
- **Stale Quotes:** Multi-leg parlay contracts return null bid/ask — filter by `bid is not None`
- **Auth:** Bearer token via `KALSHI_API_KEY` env var, header: `X-API-Key`

### NOAA Weather API
- **Base:** `https://api.weather.gov` (no auth, User-Agent required)
- **Flow:** location → lat/lon → `/points/{lat},{lon}` → grid office/X/Y → `/gridpoints/{office}/{X},{Y}/forecast/hourly`
- **Model:** Single NWS model run (not true ensemble). Max hourly temp = forecasted daily high.
- **Uncertainty:** sqrt(NWS_RMSE² + top_quartile_spread²). Day-1 RMSE ≈ 2.5°F.
- **Rate Limit:** ~2 req/sec, 0.5s sleep between calls. Grid lookups cached per session.
- **Data Retention:** ~7 days history. Older settlement may fail if NOAA drops past data.

---

## Known Issues & Limitations

### Active Issues (As of 2026-04-10)
1. **CLAUDE.md Staleness** (resolved this session — document was dated 2026-03-24)
2. **Econ Scanner CPI Miss (April 10):** Scanner found 50 contracts but 0 tradeable on release day. CPI released 12:30 UTC; scanner ran 10:00 UTC (2.5h before). SIG likely withdraws quotes AT release, not before. Need pre-release scan window (6–8 AM ET). Post-mortem in progress.
3. **daily_scan.py Volume Sort Broken:** Returns KXMVE multi-leg parlay contracts and far-dated KXFED contracts (all vol=0, OI=0) instead of real high-volume markets. Confirmed broken 9+ days. Low priority (not impacting live trades).
4. **Binance 451 Geo-Block:** US-based accounts hit 451 errors on Binance. Downgraded from WARNING to INFO (expected). Kraken-only for funding arb is correct approach.
5. **Memory Usage:** Occasionally touches 80% threshold during extended runs. Monitor in Mac Mini deployment.

### Kalshi Weather — Definitively Archived
- **Zero-Volume Confirmed:** All 250 weather contracts averaged 0 volume across 9+ days (March 25 – April 3)
- **Liquidity Ceiling:** Comparable bots (TSA Ferraiolo bot) cap out at ~$20/week
- **MM Quotes Present:** SIG (Susquehanna) provides tight 1–3¢ spreads but zero counterparties
- **Root Cause:** Structural — retail/institutional volume is near-zero on weather. Not a time-of-day artifact, not model quality.
- **Code Status:** Preserved in `strategies/kalshi_weather/` and git history. Safe to reference for pattern library but do not re-enable without market structural change.

### Kalshi Economic Markets — Timing Flaw Discovered
- **Issue:** CPI April 10 had 0 tradeable contracts despite 50 found. SIG quotes are available days before release but withdrawn AT/just before release.
- **Implication:** The profitable window is the days BEFORE release (e.g., April 3–9 for April 10 CPI), not release day itself.
- **Fix Required:** Adjust cron timing. Run `econ_scan.py` at 6 AM UTC (1 AM ET) and 12 PM UTC (7 AM ET) on release days to catch pre-release quote window.

---

## Paper Trading Results (Kalshi Weather)

**Final State (Settled April 5, 2026)**
- Total Trades: 25
- Settled: 25 | Open: 0
- P&L: -$2.19 (-0.022%) on $10,000 starting capital
- Win Rate: 4 wins / 21 losses (16%)
- Cash Remaining: $10,013.81

**Root Cause:** Anomalously warm week (April 1–7). Chicago +19°F, Denver +25°F above normal. NOAA single-model forecast missed the warm regime entirely. Model uncertainty was too tight.

**Lessons:**
1. Single NWS model run is insufficient — need ensemble uncertainty (GFS, HRRR spread)
2. Historical bias per city/season critical (warm/cold bias varies)
3. Weather strategy closure was correct — market structural illiquidity is the real blocker

---

## Infrastructure & Operations

### Two-Machine Setup
- **Dev Mac:** `~/Documents/Claude/Projects/Algo Trading Desk/algo-desk/` (main editing machine)
- **Trading Mac (Old MacBook):** `~/Desktop/algo-desk/` (live execution machine)
- **Git Workflow:** Push from dev → Pull + restart on trading Mac
- **Environment:** `.env` with API keys — must be manually maintained on each machine (git-ignored)

### Cron Jobs (Running on Trading Mac)
Installed via `install_cron.sh`:
- **6:00 AM + 12:00 PM UTC daily:** `econ_scan.py` (economic events)
- **7:30 AM + 5:00 PM ET daily:** `daily_scan.py` (opportunity scan)
- **8:00 AM ET daily:** `daily_report.py` (email summary)

Cron log: Check `logs/econ_cron.log` and `logs/scan_cron.log` for any failures.

### Background Process Management
- **Funding Arb:** Managed by `start_funding_arb.sh` — handles start/stop/status/logs
  - Uses nohup to persist after terminal close
  - PID saved to `logs/funding_arb.pid`
- **Pairs Trading:** Manual nohup command
  - PID saved to `logs/pairs_trading.pid`
- **State Persistence:**
  - Funding arb: `logs/funding_arb_state.json` (open positions)
  - Pairs trading: `logs/pairs_state.json` (price history + Z-score model)
  - Both restore on restart — warmup not lost

### Email Configuration (daily_report.py)
```
.env settings needed:
  REPORT_TO=mattbrunetto215@gmail.com
  REPORT_FROM=mattbrunetto215@gmail.com
  GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx  (16-char App Password from Google)
```

---

## Upcoming Work & Roadmap

### High Priority (Next 2 weeks)
1. **Reverse Funding Arb:** Build short-spot/long-perp strategy for negative rate environments. April 10 rates -246% to -964% ann show massive opportunity cost. Implementation: mirror buy logic but invert signal to sell on -rates.
2. **Econ Scanner Post-Mortem:** Implement pre-release scan window. CPI April 10 miss was timing, not model. Adjust cron or add release-day early-morning scan.
3. **Pairs Trading State Validation:** Run trading Mac for 3+ days, verify Z-score behavior and any positions opened/closed. Check `logs/pairs_trading.log` for convergence quality.

### Medium Priority (This month)
1. **Mac Mini Setup:** Arriving ~end of April. Plan: launchd for auto-start/restart instead of nohup. Git pull auto-deployment on boot.
2. **Fix daily_scan.py Volume Sort:** Currently returns zero-volume parlay contracts. Need to investigate endpoint params or filtering logic. Low impact on live trades but documentation debt.
3. **IBKR Integration:** Post Mac Mini, wire IBKR connection for real account testing. Wheel strategy (ThetaGang) deprioritized for now — requires $25K+ capital.

### Low Priority (April–May)
1. **ORB Strategy (NQ Futures):** Opening Range Breakout on Nasdaq futures. Build after IBKR + Mac Mini.
2. **Polymarket Cross-Platform Arb:** Research 0.5–3% arb opportunities between Kalshi and Polymarket.
3. **Enhanced Uncertainty Modeling:** Add GFS/HRRR ensemble spread and historical bias tables to weather signal engine (if weather ever returns).

---

## Paper Trading Capital Allocation

**Approved Deployment:**
- Funding arb: $500/leg, max 6 positions = $6K max deployed
- Pairs trading: $500/leg, max 2 positions = $2K max deployed
- Total paper capital: $10,000 (used in Kalshi weather, now archived)

**Real Money:** Paper-only until validation complete. Estimated 2–4 weeks live testing before considering $500–$1000 real capital.

---

## How Matt Wants Us to Work (from feedback_algo_workflow.md)

1. **Read source files first** before touching any code
2. **State plan in 2–3 sentences** before making changes
3. **Edit directly in repo** — no patch scripts or workarounds
4. **After every change, state exactly what command to test**
5. **End responses with prioritized "Up Next" (top 3 items)**
6. **Think like a quant:** Is this improving edge? Managing risk? Improving signal quality? Flag any decision that could affect live money.

---

## Key Contacts & Resources

- **Primary Dev:** Matt (@mbrunetto) — mattbrunetto215@gmail.com
- **GitHub:** github.com/mbrunes215/algo-desk (private)
- **Kalshi API Docs:** https://api.elections.kalshi.com/docs
- **NOAA API:** https://api.weather.gov
- **Kraken API:** https://docs.kraken.com
- **FRED API:** https://fred.stlouisfed.org/api (free, requires registration)

---

## Quick Diagnostics

**Is funding arb running?**
```bash
./start_funding_arb.sh status
tail -10 logs/funding_arb.log
```

**Is pairs trading running?**
```bash
tail -10 logs/pairs_trading.log
ps aux | grep run_pairs
```

**Check last econ scan?**
```bash
cat logs/econ_scan.json | jq .
```

**Check last daily scan?**
```bash
cat logs/latest_scan.json | jq .
```

**Verify cron jobs running?**
```bash
crontab -l
tail -20 logs/econ_cron.log
tail -20 logs/scan_cron.log
```

**Git sync between machines?**
```bash
# On dev Mac
git status
git push

# On trading Mac
git pull && ./start_funding_arb.sh restart
```

---

## Last Updated
2026-04-10 — Full system state review completed. Funding arb + pairs trading confirmed live, Kalshi weather archived, econ scanner timing issue identified.

See `/logs/memory_consolidation_log.md` for detailed change history.
