#!/usr/bin/env python3
"""
econ_scan.py -- Kalshi Economic Event Scanner with Edge Signals
Run from your MacBook to scan macro/econ Kalshi markets and generate trading signals.

This is Phase 1 of the strategy pivot away from weather (zero-volume) toward
economic data release markets where real institutional + retail volume exists.

Core approach:
  1. Fetch live Kalshi econ/macro markets by known series ticker
  2. Pull recent actuals + prior values from FRED API (requires free key: fred.stlouisfed.org/api)
  3. Compute edge = |model_probability - market_implied_probability|
  4. Flag contracts where edge >= MIN_EDGE_PCT with recommended action
  5. Write results to logs/econ_scan.json + append to logs/econ_opportunity_log.md

Usage:
    cd ~/Documents/Claude/Projects/Algo Trading Desk/algo-desk
    python3 econ_scan.py

Cron (runs at 6:00 AM + 12:00 PM daily):
    0 6,12 * * * cd ~/Documents/Claude/Projects/Algo Trading Desk/algo-desk && python3 econ_scan.py >> logs/econ_cron.log 2>&1

Key markets tracked (Kalshi series tickers):
    KXFED           -- Fed Funds rate decisions (very high volume around FOMC)
    KXCPIYOY        -- CPI year-over-year print
    KXCORECPI       -- Core CPI
    KXPCE           -- PCE (Fed's preferred inflation gauge)
    KXCOREPCE       -- Core PCE
    KXNFP           -- Non-Farm Payroll (monthly jobs report)
    KXUNEMPLOY      -- Unemployment rate
    KXGDP           -- GDP growth rate
    KXRECSSNBER-26  -- NBER recession probability (ongoing market)
    RATECUTCOUNT    -- Fed rate cut count for calendar year
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

# ── Load .env ──────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent
try:
    from dotenv import load_dotenv
    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass  # dotenv optional if env vars already set

API_KEY = os.getenv("KALSHI_API_KEY", "")
KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
FRED_BASE   = "https://api.stlouisfed.org/fred"
FRED_KEY    = os.getenv("FRED_API_KEY", "")  # Optional: get free key at fred.stlouisfed.org/api
KALSHI_HEADERS = {
    "X-API-Key": API_KEY,
    "Accept": "application/json",
}

# ── Strategy Parameters ────────────────────────────────────────────────────────
MIN_EDGE_PCT      = 5.0    # Minimum edge in percentage points to flag as opportunity
MIN_VOLUME        = 100    # Minimum contract volume — skip ghost-town markets
MAX_SPREAD_CENTS  = 15     # Skip contracts with spread > 15¢ (liquidity too thin)
PRICE_RANGE_LOW   = 10     # Skip contracts below 10¢ (favorite-longshot bias kills edge)
PRICE_RANGE_HIGH  = 90     # Skip contracts above 90¢

# ── Known Kalshi Econ Series ───────────────────────────────────────────────────
# Each entry: (series_ticker, indicator_name, fred_series_id_or_None)
# FRED series IDs: https://fred.stlouisfed.org/
ECON_SERIES = [
    ("KXFED",          "Fed Funds Rate Decision",   None),         # FOMC decision markets
    ("KXCPIYOY",       "CPI YoY",                   "CPIAUCSL"),   # CPI all items YoY
    ("KXCORECPI",      "Core CPI",                  "CPILFESL"),   # Core CPI ex food/energy
    ("KXPCE",          "PCE Inflation",              "PCEPI"),      # PCE price index
    ("KXCOREPCE",      "Core PCE",                  "PCEPILFE"),   # Core PCE
    ("KXNFP",          "Non-Farm Payroll",           "PAYEMS"),     # Total nonfarm payroll
    ("KXUNEMPLOY",     "Unemployment Rate",          "UNRATE"),     # Unemployment rate
    ("KXGDP",          "GDP Growth Rate",            "A191RL1Q225SBEA"),  # Real GDP growth
    ("KXRECSSNBER-26", "NBER Recession 2026",        None),         # Recession probability
    ("RATECUTCOUNT",   "Fed Rate Cut Count",         None),         # Cumulative cuts in year
    ("KXJOBSNFP",      "Jobs / NFP alt",             "PAYEMS"),     # Alt NFP series ticker
    ("KXINFLATION",    "Inflation Rate",             "CPIAUCSL"),   # Alt inflation series
]

# ── Historical Surprise Distributions (from econ_strategy.py, baked in) ──────
# Format: indicator_name -> (mean_surprise, std_surprise, skew)
# mean_surprise > 0 means consensus tends to under-predict (data beats consensus)
SURPRISE_DIST = {
    "CPI YoY":           (0.15,  0.45, 0.20),   # consensus misses low by ~0.15%
    "Core CPI":          (0.10,  0.40, 0.15),
    "PCE Inflation":     (0.08,  0.38, 0.15),
    "Core PCE":          (0.06,  0.35, 0.10),
    "Non-Farm Payroll":  (45.0, 145.0, 0.30),    # beats consensus by ~45k jobs avg
    "Jobs / NFP alt":    (45.0, 145.0, 0.30),
    "Unemployment Rate": (-0.05,  0.18, 0.10),   # tends to surprise low (better)
    "GDP Growth Rate":   (0.10,  0.35, 0.15),
    "Inflation Rate":    (0.08,  0.40, 0.25),
}

LOG_PATH   = REPO_ROOT / "logs" / "econ_opportunity_log.md"
CACHE_PATH = REPO_ROOT / "logs" / "econ_scan.json"
LOG_PATH.parent.mkdir(exist_ok=True)

# ── Upcoming Release Calendar ──────────────────────────────────────────────────
# Manually maintained — update when BLS/Fed/BEA schedules publish.
# Format: (release_date_str, indicator_name, kalshi_series, note)
# Kalshi quotes typically go live ~5-7 trading days before the release date.
RELEASE_CALENDAR = [
    # April 2026
    ("2026-04-10", "CPI YoY (March)",         "KXCPIYOY", "BLS 8:30 AM ET — expect quotes ~Apr 3"),
    ("2026-04-11", "Core PPI (March)",         None,       "BLS 8:30 AM ET — no Kalshi series confirmed"),
    ("2026-04-16", "Retail Sales (March)",     None,       "Census 8:30 AM ET"),
    ("2026-04-30", "GDP Q1 Advance",           "KXGDP",   "BEA 8:30 AM ET — expect quotes ~Apr 23"),
    ("2026-04-30", "Core PCE (March)",         "KXCOREPCE","BEA 8:30 AM ET — same release as GDP"),
    # May 2026
    ("2026-05-02", "Jobs / NFP (April)",       "KXNFP",   "BLS 8:30 AM ET — expect quotes ~Apr 27"),
    ("2026-05-07", "FOMC Decision",            "KXFED",   "Fed 2:00 PM ET — expect quotes ~Apr 30"),
    ("2026-05-13", "CPI YoY (April)",          "KXCPIYOY","BLS 8:30 AM ET — expect quotes ~May 6"),
    ("2026-05-28", "GDP Q1 Revised",           "KXGDP",   "BEA 8:30 AM ET"),
    ("2026-05-30", "Core PCE (April)",         "KXCOREPCE","BEA 8:30 AM ET"),
    # June 2026
    ("2026-06-06", "Jobs / NFP (May)",         "KXNFP",   "BLS 8:30 AM ET"),
    ("2026-06-11", "CPI YoY (May)",            "KXCPIYOY","BLS 8:30 AM ET"),
    ("2026-06-18", "FOMC Decision",            "KXFED",   "Fed 2:00 PM ET"),
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def normalize_price(p):
    """Normalize Kalshi price to cents (0–100 scale)."""
    if p is None:
        return None
    try:
        p = float(p)
    except (TypeError, ValueError):
        return None
    return round(p * 100 if p < 1.0 else p, 1)


def mid_price(bid, ask):
    if bid is not None and ask is not None:
        return round((bid + ask) / 2, 1)
    return None


def kalshi_get(path, params=None, retries=3):
    """GET from Kalshi API with retry and backoff."""
    url = f"{KALSHI_BASE}{path}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=KALSHI_HEADERS, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP {e.response.status_code} on {path}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  Kalshi error (attempt {attempt+1}): {e}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def fred_get_latest(series_id, retries=2):
    """
    Fetch the two most recent observations for a FRED series.
    Returns (current_value, prior_value, release_date_str) or (None, None, None).

    FRED API requires a free key: fred.stlouisfed.org/api/request_api_key
    Add FRED_API_KEY=your_key to .env to enable. Skipped gracefully without it.
    """
    if not FRED_KEY:
        return None, None, None  # Skip silently — no key, no error spam

    params = {
        "series_id":  series_id,
        "sort_order": "desc",
        "limit":      2,
        "file_type":  "json",
        "api_key":    FRED_KEY,
    }

    url = f"{FRED_BASE}/series/observations"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code == 200:
                obs = r.json().get("observations", [])
                if len(obs) >= 2:
                    current_val  = float(obs[0]["value"]) if obs[0]["value"] != "." else None
                    prior_val    = float(obs[1]["value"]) if obs[1]["value"] != "." else None
                    release_date = obs[0]["date"]
                    return current_val, prior_val, release_date
                elif len(obs) == 1:
                    val = float(obs[0]["value"]) if obs[0]["value"] != "." else None
                    return val, None, obs[0]["date"]
            else:
                print(f"  FRED {series_id}: HTTP {r.status_code}", file=sys.stderr)
        except Exception as e:
            print(f"  FRED error for {series_id}: {e}", file=sys.stderr)
        if attempt < retries - 1:
            time.sleep(1)
    return None, None, None


# ── Core Signal Engine ─────────────────────────────────────────────────────────

def compute_edge(market_price_cents, indicator_name):
    """
    Calculate the edge between our model probability and market-implied probability.

    Model: uses historical surprise distribution to estimate P(beat consensus).
    If market prices 60¢ for "will CPI beat X?" but our model says 70%, edge = +10%.
    Positive edge = market is underpricing the YES outcome → BUY signal.
    Negative edge = market overpricing YES → SELL signal (buy NO).

    Returns:
        (model_prob_pct, edge_pct, signal, rationale)
        signal = "BUY YES" | "BUY NO" | "HOLD"
    """
    if indicator_name not in SURPRISE_DIST:
        # No surprise data — can't compute edge, return neutral
        return None, None, "HOLD", "No surprise distribution data"

    mean_s, std_s, skew = SURPRISE_DIST[indicator_name]

    if std_s == 0:
        return 50.0, 0.0, "HOLD", "Zero variance in surprise distribution"

    # P(beat) = P(historical_surprise > 0)
    # Using normal approximation: z = -mean / std
    # prob = 1 - Phi(z)
    import math
    z = -mean_s / std_s
    # Standard normal CDF approximation (no scipy needed)
    prob_beat = 0.5 * (1 + math.erf(-z / math.sqrt(2)))

    # Skew adjustment: positive skew → more likely to beat
    prob_beat = prob_beat * (1 + skew * 0.05)
    prob_beat = max(0.05, min(0.95, prob_beat))

    model_prob_pct    = round(prob_beat * 100, 1)
    market_prob_pct   = market_price_cents  # already in 0-100 scale
    edge_pct          = round(model_prob_pct - market_prob_pct, 1)

    if abs(edge_pct) < MIN_EDGE_PCT:
        signal    = "HOLD"
        rationale = f"Edge {edge_pct:+.1f}% below threshold ({MIN_EDGE_PCT}%)"
    elif edge_pct > 0:
        signal    = "BUY YES"
        rationale = (
            f"Model: {model_prob_pct}% vs market {market_prob_pct}% → "
            f"YES underpriced by {edge_pct:.1f}%. "
            f"Historical surprises: mean={mean_s:+}, σ={std_s}"
        )
    else:
        signal    = "BUY NO"
        rationale = (
            f"Model: {model_prob_pct}% vs market {market_prob_pct}% → "
            f"YES overpriced by {abs(edge_pct):.1f}%, buy NO. "
            f"Historical surprises: mean={mean_s:+}, σ={std_s}"
        )

    return model_prob_pct, edge_pct, signal, rationale


def is_tradeable(m):
    """Return True if contract passes liquidity and price filters."""
    bid    = m.get("bid")
    ask    = m.get("ask")
    spread = m.get("spread")
    mid    = m.get("mid")
    vol    = m.get("volume", 0) or 0

    if bid is None or ask is None:
        return False, "No bid/ask"
    if spread is not None and spread > MAX_SPREAD_CENTS:
        return False, f"Spread {spread}¢ > {MAX_SPREAD_CENTS}¢ max"
    if mid is not None and (mid < PRICE_RANGE_LOW or mid > PRICE_RANGE_HIGH):
        return False, f"Price {mid}¢ outside {PRICE_RANGE_LOW}-{PRICE_RANGE_HIGH}¢ range"
    if vol < MIN_VOLUME:
        return False, f"Volume {vol} < {MIN_VOLUME} min"
    return True, "OK"


# ── Fetch Econ Markets ─────────────────────────────────────────────────────────

def fetch_econ_markets():
    """
    Fetch live Kalshi markets for all known econ series.
    Returns list of enriched market dicts with FRED context and edge signals.
    """
    all_markets = []
    fred_cache  = {}  # series_id -> (current, prior, date) to avoid repeat calls

    for series_ticker, indicator_name, fred_series_id in ECON_SERIES:
        print(f"  Fetching {series_ticker} ({indicator_name})...")
        # Request status=open only — 'initialized' contracts are pre-created shells
        # with no quotes yet. They have null bid/ask and zero volume.
        data = kalshi_get("/markets", params={
            "series_ticker": series_ticker,
            "status":        "open",
            "limit":         50,
        })

        if data is None:
            print(f"    [WARN] No data for {series_ticker}")
            continue

        markets = data.get("markets", [])
        # Also filter out any initialized contracts that slip through
        markets = [m for m in markets if m.get("status", "") != "initialized"]

        if not markets:
            print(f"    [INFO] No open/active markets for {series_ticker}")
            continue

        # Fetch FRED context once per series
        fred_current = fred_prior = fred_date = None
        if fred_series_id and fred_series_id not in fred_cache:
            fred_current, fred_prior, fred_date = fred_get_latest(fred_series_id)
            fred_cache[fred_series_id] = (fred_current, fred_prior, fred_date)
            if fred_current is not None:
                print(f"    FRED {fred_series_id}: current={fred_current}, prior={fred_prior}, date={fred_date}")
        elif fred_series_id:
            fred_current, fred_prior, fred_date = fred_cache[fred_series_id]

        for m in markets:
            bid    = normalize_price(m.get("yes_bid") or m.get("yes_bid_price"))
            ask    = normalize_price(m.get("yes_ask") or m.get("yes_ask_price"))
            spread = round(ask - bid, 1) if bid is not None and ask is not None else None
            mid    = mid_price(bid, ask)
            vol    = int(m.get("volume") or 0)

            # Tradability check
            enriched = {
                "ticker":        m.get("ticker", ""),
                "series":        series_ticker,
                "indicator":     indicator_name,
                "title":         m.get("title", ""),
                "bid":           bid,
                "ask":           ask,
                "spread":        spread,
                "mid":           mid,
                "volume":        vol,
                "open_interest": int(m.get("open_interest") or 0),
                "status":        m.get("status", ""),
                "close_time":    m.get("close_time", ""),
                "fred_current":  fred_current,
                "fred_prior":    fred_prior,
                "fred_date":     fred_date,
            }

            tradeable, tradeable_reason = is_tradeable(enriched)
            enriched["tradeable"]        = tradeable
            enriched["tradeable_reason"] = tradeable_reason

            # Compute edge signal (only meaningful for beat/miss contracts)
            if mid is not None and tradeable:
                model_prob, edge_pct, signal, rationale = compute_edge(mid, indicator_name)
            else:
                model_prob = edge_pct = None
                signal = "SKIP"
                rationale = tradeable_reason if not tradeable else "No mid price"

            enriched["model_prob"]  = model_prob
            enriched["edge_pct"]    = edge_pct
            enriched["signal"]      = signal
            enriched["rationale"]   = rationale

            all_markets.append(enriched)

        print(f"    → {len(markets)} contracts")
        time.sleep(0.3)

    return all_markets


# ── Report Builder ─────────────────────────────────────────────────────────────

def build_econ_report(scan_time, markets):
    now_str  = scan_time.strftime("%Y-%m-%d %H:%M UTC")
    date_str = scan_time.strftime("%Y-%m-%d")

    actionable = [m for m in markets if m["signal"] in ("BUY YES", "BUY NO")]
    tradeable  = [m for m in markets if m["tradeable"]]
    total      = len(markets)

    actionable.sort(key=lambda x: abs(x["edge_pct"] or 0), reverse=True)

    lines = [
        f"\n## {date_str} Econ Event Scan\n",
        f"**Scan time:** {now_str}  |  **Script:** econ_scan.py\n",
        f"**Summary:** {total} contracts across {len(ECON_SERIES)} series | "
        f"{len(tradeable)} tradeable | {len(actionable)} with edge signal\n",
    ]

    # ── Actionable Signals ──
    lines.append("### Actionable Signals (Edge ≥ {}%)\n".format(MIN_EDGE_PCT))
    if actionable:
        lines.append("| Contract | Signal | Market Price | Model Prob | Edge | Volume | Closes | Rationale |")
        lines.append("|----------|--------|-------------|-----------|------|--------|--------|-----------|")
        for m in actionable[:15]:
            ct = m["close_time"][:10] if m["close_time"] else "?"
            lines.append(
                f"| {m['ticker']} | **{m['signal']}** | {m['mid']}¢ | {m['model_prob']}% "
                f"| {m['edge_pct']:+.1f}% | {m['volume']:,} | {ct} | {m['rationale'][:80]} |"
            )
    else:
        lines.append("_No contracts with edge ≥ {}% found in this scan._".format(MIN_EDGE_PCT))
    lines.append("")

    # ── Tradeable Markets (no edge but liquid) ──
    lines.append("### All Tradeable Econ Markets\n")
    hold_markets = [m for m in tradeable if m["signal"] == "HOLD"]
    if hold_markets:
        hold_markets.sort(key=lambda x: -(x["volume"] or 0))
        lines.append("| Contract | Indicator | Bid | Ask | Spread | Volume | Open Int | Close | FRED Prior |")
        lines.append("|----------|-----------|-----|-----|--------|--------|----------|-------|------------|")
        for m in hold_markets[:20]:
            ct       = m["close_time"][:10] if m["close_time"] else "?"
            fred_str = f"{m['fred_prior']}" if m["fred_prior"] is not None else "—"
            lines.append(
                f"| {m['ticker']} | {m['indicator'][:20]} | {m['bid']}¢ | {m['ask']}¢ "
                f"| {m['spread']}¢ | {m['volume']:,} | {m['open_interest']:,} | {ct} | {fred_str} |"
            )
    else:
        lines.append("_No tradeable markets without signals found._")
    lines.append("")

    # ── All Series Status ──
    lines.append("### Series Coverage\n")
    lines.append("| Series | Indicator | Markets Found | Tradeable | With Signal |")
    lines.append("|--------|-----------|--------------|-----------|-------------|")
    for series_ticker, indicator_name, _ in ECON_SERIES:
        series_markets    = [m for m in markets if m["series"] == series_ticker]
        series_tradeable  = [m for m in series_markets if m["tradeable"]]
        series_actionable = [m for m in series_markets if m["signal"] in ("BUY YES","BUY NO")]
        found_str = str(len(series_markets)) if series_markets else "—"
        lines.append(
            f"| {series_ticker} | {indicator_name[:25]} | {found_str} "
            f"| {len(series_tradeable)} | {len(series_actionable)} |"
        )
    lines.append("")

    # ── Edge Opportunities Narrative ──
    lines.append("### Edge Assessment\n")
    if actionable:
        best = actionable[0]
        lines.append(
            f"**Top signal:** {best['ticker']} ({best['indicator']}) — "
            f"{best['signal']} at {best['mid']}¢. "
            f"Model probability {best['model_prob']}% vs market {best['mid']}¢. "
            f"Edge: {best['edge_pct']:+.1f}%. Volume: {best['volume']:,}.\n"
        )
        lines.append(f"Historical context: {best['rationale']}\n")
    else:
        lines.append(
            "No live quoted markets found. Econ contracts are `status=active` but unquoted "
            "— the market maker (SIG) posts quotes ~5-7 trading days before each release. "
            "Zero contracts today = between events, not a data error.\n"
        )

    # ── Upcoming Release Calendar ──
    lines.append("### Upcoming Release Calendar\n")
    lines.append("_Kalshi quotes expected ~5-7 trading days before each release date._\n")
    lines.append("| Release Date | Event | Kalshi Series | Days Away | Quote Window Opens |")
    lines.append("|-------------|-------|--------------|-----------|-------------------|")

    today = scan_time.date()
    for release_date_str, event_name, series, note in RELEASE_CALENDAR:
        from datetime import date as _date
        rel_date   = _date.fromisoformat(release_date_str)
        days_away  = (rel_date - today).days
        if days_away < -3:
            continue  # skip past events (allow 3-day grace for recent)
        quote_days = max(0, days_away - 7)
        if quote_days == 0:
            quote_status = "🟢 **Live now or imminent**"
        elif quote_days <= 5:
            quote_status = f"🟡 ~{quote_days}d"
        else:
            quote_status = f"⚪ ~{quote_days}d"
        series_str = series if series else "—"
        lines.append(
            f"| {release_date_str} | {event_name} | {series_str} "
            f"| {days_away}d | {quote_status} |"
        )
    lines.append("")
    lines.append(f"_Note: {note}_\n" if 'note' in dir() else "")

    lines.append("---")
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    if not API_KEY:
        print("ERROR: KALSHI_API_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    scan_time = datetime.now(timezone.utc)
    print(f"[{scan_time.isoformat()}] Starting Kalshi econ event scan...")
    if not FRED_KEY:
        print("  [INFO] No FRED_API_KEY set — prior values will be skipped. "
              "Get a free key at fred.stlouisfed.org/api/request_api_key "
              "and add FRED_API_KEY=your_key to .env to enable.")

    print("\nFetching econ markets...")
    markets = fetch_econ_markets()

    actionable = [m for m in markets if m["signal"] in ("BUY YES", "BUY NO")]
    tradeable  = [m for m in markets if m["tradeable"]]

    # Build and write report
    report = build_econ_report(scan_time, markets)
    with open(LOG_PATH, "a") as f:
        f.write(report)
    print(f"\n  ✅ Appended to {LOG_PATH}")

    # Save JSON cache
    cache = {
        "scan_time":  scan_time.isoformat(),
        "markets":    markets,
        "stats": {
            "total_contracts": len(markets),
            "tradeable":       len(tradeable),
            "actionable":      len(actionable),
            "series_scanned":  len(ECON_SERIES),
        },
        "top_signals": sorted(
            [m for m in actionable],
            key=lambda x: abs(x["edge_pct"] or 0),
            reverse=True
        )[:10],
    }
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2, default=str)
    print(f"  ✅ Cache saved to {CACHE_PATH}")

    # Terminal summary
    print(f"\n=== ECON SCAN SUMMARY ===")
    print(f"Series scanned:   {len(ECON_SERIES)}")
    print(f"Contracts found:  {len(markets)}")
    print(f"Tradeable:        {len(tradeable)}")
    print(f"Actionable edge:  {len(actionable)}")
    if actionable:
        print(f"\nTop signals:")
        for m in sorted(actionable, key=lambda x: abs(x["edge_pct"] or 0), reverse=True)[:5]:
            print(f"  {m['signal']:8s} {m['ticker']:35s} edge={m['edge_pct']:+.1f}% vol={m['volume']:,}")
    else:
        print("  No live quoted markets (between events — this is expected)")

    # Show upcoming events
    from datetime import date as _date
    today = scan_time.date()
    print("\nUpcoming release windows:")
    for release_date_str, event_name, series, note in RELEASE_CALENDAR:
        rel_date  = _date.fromisoformat(release_date_str)
        days_away = (rel_date - today).days
        if days_away < -3 or days_away > 45:
            continue
        quote_days = max(0, days_away - 7)
        flag = "🟢 LIVE" if quote_days == 0 else f"  {days_away}d away (quotes in ~{quote_days}d)"
        series_str = f" [{series}]" if series else ""
        print(f"  {release_date_str}  {event_name}{series_str}  {flag}")
    print(f"\nLog:   {LOG_PATH}")
    print(f"Cache: {CACHE_PATH}")
    print(f"Done:  {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
