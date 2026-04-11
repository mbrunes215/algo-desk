#!/usr/bin/env python3
"""
daily_scan.py -- Kalshi Daily Opportunity Scanner
Run this from your MacBook each morning to populate opportunity_log.md.

Usage:
    cd ~/Documents/Claude/Projects/Algo Trading Desk/algo-desk
    python3 daily_scan.py

Cron (runs at 7:30 AM daily):
    30 7 * * * cd ~/Documents/Claude/Projects/Algo Trading Desk/algo-desk && python3 daily_scan.py >> logs/scan_cron.log 2>&1
"""

import os
import sys
import json
import time
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(REPO_ROOT / ".env")

API_KEY = os.getenv("KALSHI_API_KEY", "")
BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
HEADERS = {
    "X-API-Key": API_KEY,
    "Accept": "application/json",
}

WEATHER_SERIES = [
    "KXHIGHNY",
    "KXLOWNY",
    "KXHIGHLAX",
    "KXHIGHCHI",
    "KXHIGHMIA",
    "KXHIGHDEN",
    "KXHIGHOU",
]

LOG_PATH = REPO_ROOT / "logs" / "opportunity_log.md"
CACHE_PATH = REPO_ROOT / "logs" / "latest_scan.json"
LOG_PATH.parent.mkdir(exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize_price(p):
    """Normalize Kalshi price to cents (0–100 scale)."""
    if p is None:
        return None
    try:
        p = float(p)
    except (TypeError, ValueError):
        return None
    return round(p * 100 if p < 1.0 else p, 1)


def kalshi_get(path, params=None, retries=3):
    """GET from Kalshi API with retry."""
    url = f"{BASE_URL}{path}"
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP {e.response.status_code} on {path}: {e}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  Error on {path} (attempt {attempt+1}): {e}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    return None


def mid_price(bid, ask):
    if bid is not None and ask is not None:
        return round((bid + ask) / 2, 1)
    return None


def parse_close_time(ct):
    if not ct:
        return None
    try:
        return datetime.fromisoformat(ct.replace("Z", "+00:00"))
    except Exception:
        return None


# ── Fetch Functions ───────────────────────────────────────────────────────────

def fetch_weather_series():
    """Fetch all weather series contracts. Returns list of enriched market dicts."""
    results = []
    for series in WEATHER_SERIES:
        data = kalshi_get("/markets", params={"series_ticker": series, "limit": 50})
        if data is None:
            print(f"  [WARN] Failed to fetch {series}", file=sys.stderr)
            continue
        markets = data.get("markets", [])
        for m in markets:
            bid = normalize_price(m.get("yes_bid") or m.get("yes_bid_price"))
            ask = normalize_price(m.get("yes_ask") or m.get("yes_ask_price"))
            spread = round(ask - bid, 1) if bid is not None and ask is not None else None
            results.append({
                "ticker":     m.get("ticker", ""),
                "series":     series,
                "title":      m.get("title", ""),
                "bid":        bid,
                "ask":        ask,
                "spread":     spread,
                "mid":        mid_price(bid, ask),
                "volume":     int(m.get("volume") or 0),
                "open_interest": int(m.get("open_interest") or 0),
                "status":     m.get("status", ""),
                "close_time": m.get("close_time", ""),
            })
        time.sleep(0.3)  # Be kind to Kalshi rate limits
    return results


def fetch_top_markets(limit=200):
    """Fetch top open markets across all categories, sorted by volume.

    Strategy (2026-03-27 fix):
    - The Kalshi v2 /markets endpoint does NOT support order_by=volume at the
      market level — that param is silently ignored and the API returns its default
      insertion order (mostly KXMVE parlays with null bid/ask).
    - Workaround: fetch by known high-volume series tickers directly, plus explicit
      category pulls for economics/crypto/sports. Deduplicate, sort by volume client-side.
    - Skip any contract with status='initialized' — those are pre-created shells with
      no quotes yet. Only 'open' or 'active' contracts have live bid/ask.
    """
    all_markets = []

    # Pull from known high-volume individual series — these always have real quotes
    HIGH_VOLUME_SERIES = [
        # Economics / macro
        "KXFED", "KXCPIYOY", "KXGDP", "KXRECSSNBER-26", "RATECUTCOUNT",
        "KXUNEMPLOY", "KXNFP", "KXCORECPI", "KXPCE",
        # Crypto
        "KXBTC", "KXETH", "KXBTCUSD", "KXETHUSD",
        # Sports (March Madness + NBA)
        "KXNCAAB", "KXNBA", "KXNCAAB-CHAMPION",
    ]

    for series in HIGH_VOLUME_SERIES:
        data = kalshi_get("/markets", params={"series_ticker": series, "limit": 50, "status": "open"})
        if data is None:
            continue
        for m in data.get("markets", []):
            status = m.get("status", "")
            if status == "initialized":
                continue  # no quotes yet — skip shell contracts
            ticker = m.get("ticker", "")
            if not ticker:
                continue
            bid = normalize_price(m.get("yes_bid") or m.get("yes_bid_price"))
            ask = normalize_price(m.get("yes_ask") or m.get("yes_ask_price"))
            spread = round(ask - bid, 1) if bid is not None and ask is not None else None
            all_markets.append({
                "ticker":        ticker,
                "series":        m.get("series_ticker", ""),
                "title":         m.get("title", ""),
                "category":      m.get("category", ""),
                "bid":           bid,
                "ask":           ask,
                "spread":        spread,
                "mid":           mid_price(bid, ask),
                "volume":        int(m.get("volume") or 0),
                "open_interest": int(m.get("open_interest") or 0),
                "status":        status,
                "close_time":    m.get("close_time", ""),
            })
        time.sleep(0.2)

    # Deduplicate by ticker (keep highest volume record)
    seen = {}
    for m in all_markets:
        t = m["ticker"]
        if t not in seen or m["volume"] > seen[t]["volume"]:
            seen[t] = m

    result = list(seen.values())

    # Sort by (volume desc, open_interest desc) — priced contracts first.
    # Using open_interest as tiebreaker because volume can be 0 early in the day
    # (Kalshi resets volume at session open) while OI persists across resets.
    # Zero-volume priced markets are sorted below non-zero but above unpriced.
    priced   = [m for m in result if m["bid"] is not None and m["ask"] is not None]
    unpriced = [m for m in result if m["bid"] is None or m["ask"] is None]

    priced.sort(key=lambda x: (x["volume"], x["open_interest"]), reverse=True)
    unpriced.sort(key=lambda x: (x["volume"], x["open_interest"]), reverse=True)

    # Filter out zero-volume AND zero-OI priced markets from the top — they're
    # MM-quoted shells with no real participation. Keep them at the end.
    real_priced   = [m for m in priced if m["volume"] > 0 or m["open_interest"] > 0]
    ghost_priced  = [m for m in priced if m["volume"] == 0 and m["open_interest"] == 0]

    return real_priced + ghost_priced + unpriced


def find_closing_soon(markets, hours=48):
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=hours)
    closing = []
    for m in markets:
        ct = parse_close_time(m.get("close_time", ""))
        if ct and now < ct <= cutoff:
            closing.append(m)
    closing.sort(key=lambda x: x.get("close_time", ""))
    return closing


def find_mispricings(markets):
    """Wide spread + mid-range price = potential edge."""
    candidates = [
        m for m in markets
        if m.get("spread") is not None
        and m["spread"] > 15
        and m.get("mid") is not None
        and 10 <= m["mid"] <= 90
        and m.get("bid") is not None
    ]
    candidates.sort(key=lambda x: x["spread"], reverse=True)
    return candidates[:10]


# ── Report Builder ────────────────────────────────────────────────────────────

def build_report(scan_time, weather_markets, top_markets, closing_soon, mispricings):
    now_str = scan_time.strftime("%Y-%m-%d %H:%M UTC")
    date_str = scan_time.strftime("%Y-%m-%d")

    # Weather analysis
    tradeable = [
        m for m in weather_markets
        if m["bid"] is not None and m["ask"] is not None
        and m["spread"] is not None and m["spread"] < 20
        and m["mid"] is not None and 10 <= m["mid"] <= 90
    ]
    tradeable.sort(key=lambda x: (x["spread"] or 999, -x["volume"]))
    nonzero_vol = [m for m in tradeable if m["volume"] > 0]
    zero_vol    = [m for m in tradeable if m["volume"] == 0]

    lines = [f"\n## {date_str} Daily Opportunity Scan\n",
             f"**Scan time:** {now_str}  |  **Script:** daily_scan.py (local MacBook run)\n"]

    # ── Weather ──
    lines.append("### Top Weather Contract Setups\n")
    if tradeable:
        lines.append("| Contract | Bid | Ask | Spread | Volume | Close | Notes |")
        lines.append("|----------|-----|-----|--------|--------|-------|-------|")
        for m in tradeable[:10]:
            ct = m["close_time"][:10] if m["close_time"] else "?"
            note = "✅ volume" if m["volume"] > 0 else "⚠️ zero vol"
            lines.append(
                f"| {m['ticker']} | {m['bid']}¢ | {m['ask']}¢ | {m['spread']}¢ "
                f"| {m['volume']} | {ct} | {note} |"
            )
    else:
        lines.append("_No weather contracts met spread/price filter (bid/ask may be null for initialized contracts)._")

    lines.append(f"\n**Volume status:** {len(nonzero_vol)} contracts with volume > 0 "
                 f"| {len(zero_vol)} zero-volume (MM-quoted only)")
    if len(nonzero_vol) == 0:
        lines.append("⚠️ **Zero-volume issue persists** — all weather contracts showing vol=0. "
                     "Structural illiquidity confirmed.")
    else:
        lines.append(f"✅ **Volume detected** — {len(nonzero_vol)} contracts with real trades.")
    lines.append("")

    # ── Top Markets ──
    lines.append("### Top Kalshi Markets by Volume (All Categories)\n")
    if top_markets:
        lines.append("| Market | Category | Bid | Ask | Spread | Volume | Open Int | Closing |")
        lines.append("|--------|----------|-----|-----|--------|--------|----------|---------|")
        for m in top_markets[:15]:
            ct = m["close_time"][:10] if m["close_time"] else "?"
            b  = f"{m['bid']}¢"  if m["bid"]  is not None else "—"
            a  = f"{m['ask']}¢"  if m["ask"]  is not None else "—"
            sp = f"{m['spread']}¢" if m["spread"] is not None else "—"
            cat = (m.get("category") or m.get("series") or "")[:20]
            title_short = m["title"][:55] if m["title"] else m["ticker"]
            lines.append(
                f"| {title_short} | {cat} | {b} | {a} | {sp} "
                f"| {m['volume']:,} | {m['open_interest']:,} | {ct} |"
            )
    else:
        lines.append("_No open markets fetched — API error._")
    lines.append("")

    # ── Closing Soon ──
    lines.append("### Contracts Closing in 48h (Watch List)\n")
    # Separate weather from non-weather
    weather_closing = [m for m in closing_soon if any(s in m.get("ticker","") for s in ["KXHIGH","KXLOW"])]
    other_closing   = [m for m in closing_soon if m not in weather_closing]

    if closing_soon:
        lines.append("| Contract | Bid | Ask | Volume | Closes |")
        lines.append("|----------|-----|-----|--------|--------|")
        for m in closing_soon[:20]:
            b  = f"{m['bid']}¢"  if m["bid"]  is not None else "—"
            a  = f"{m['ask']}¢"  if m["ask"]  is not None else "—"
            ct = m["close_time"][:16].replace("T"," ") if m["close_time"] else "?"
            lines.append(f"| {m['ticker']} | {b} | {a} | {m['volume']:,} | {ct} |")
        if weather_closing:
            lines.append(f"\n_{len(weather_closing)} weather contracts settling — check NOAA actuals for settlement._")
    else:
        lines.append("_None found._")
    lines.append("")

    # ── Mispricings ──
    lines.append("### Potential Mispricings / High-Edge Setups\n")
    if mispricings:
        lines.append("| Contract | Category | Bid | Ask | Spread | Mid | Volume |")
        lines.append("|----------|----------|-----|-----|--------|-----|--------|")
        for m in mispricings:
            cat = (m.get("category") or m.get("series") or "")[:20]
            lines.append(
                f"| {m['ticker']} | {cat} | {m['bid']}¢ | {m['ask']}¢ "
                f"| {m['spread']}¢ | {m['mid']}¢ | {m['volume']:,} |"
            )
    else:
        lines.append("_No obvious mispricings (spread > 15¢ in mid-range) found._")
    lines.append("")

    # ── Stats summary ──
    total_vol = sum(m["volume"] for m in top_markets[:20])
    top_cat = {}
    for m in top_markets[:50]:
        cat = m.get("category") or "other"
        top_cat[cat] = top_cat.get(cat, 0) + m["volume"]
    top_cat_sorted = sorted(top_cat.items(), key=lambda x: -x[1])

    lines.append("### Platform Volume Snapshot\n")
    lines.append(f"Top 20 markets combined volume: **{total_vol:,}**\n")
    lines.append("| Category | Volume (top 50 markets) |")
    lines.append("|----------|------------------------|")
    for cat, vol in top_cat_sorted[:8]:
        lines.append(f"| {cat} | {vol:,} |")
    lines.append("")

    # ── Recommendation ──
    lines.append("### Recommendation\n")
    # Auto-generate a brief recommendation based on data
    rec_parts = []
    if nonzero_vol:
        best = nonzero_vol[0]
        rec_parts.append(
            f"Weather volume has appeared — {best['ticker']} (spread {best['spread']}¢, vol {best['volume']}) "
            f"is the tightest opportunity in the bot's primary market."
        )
    else:
        rec_parts.append(
            "Weather contracts remain zero-volume (MM-only). Bot has theoretical edge "
            "but no real counterparties — monitor for any volume pickup."
        )

    priced_top = [m for m in top_markets if m["bid"] is not None]
    if priced_top:
        top1 = priced_top[0]
        rec_parts.append(
            f"Highest-volume priced market: **{top1['title'][:60]}** "
            f"(vol {top1['volume']:,}, spread {top1['spread']}¢)."
        )

    if mispricings:
        mp = mispricings[0]
        rec_parts.append(
            f"Widest spread / potential edge: {mp['ticker']} (spread {mp['spread']}¢, mid {mp['mid']}¢)."
        )

    lines.append(" ".join(rec_parts) if rec_parts else "_Insufficient data for recommendation._")
    lines.append("")
    lines.append("---")

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not API_KEY:
        print("ERROR: KALSHI_API_KEY not set in .env", file=sys.stderr)
        sys.exit(1)

    scan_time = datetime.now(timezone.utc)
    print(f"[{scan_time.isoformat()}] Starting Kalshi opportunity scan...")

    print("  Fetching weather series...")
    weather_markets = fetch_weather_series()
    print(f"  → {len(weather_markets)} weather contracts")

    print("  Fetching top markets (all categories)...")
    top_markets = fetch_top_markets(limit=200)
    print(f"  → {len(top_markets)} unique markets")

    print("  Analyzing closing times...")
    all_markets = weather_markets + [m for m in top_markets
                                     if not any(s in m.get("ticker","") for s in WEATHER_SERIES)]
    closing_soon = find_closing_soon(all_markets, hours=48)
    print(f"  → {len(closing_soon)} contracts closing within 48h")

    print("  Identifying mispricings...")
    mispricings = find_mispricings(top_markets)
    print(f"  → {len(mispricings)} mispricing candidates")

    # Build report
    report = build_report(scan_time, weather_markets, top_markets, closing_soon, mispricings)

    # Write to opportunity log
    with open(LOG_PATH, "a") as f:
        f.write(report)
    print(f"  ✅ Appended to {LOG_PATH}")

    # Save full JSON cache for the Cowork scheduled task to read
    cache = {
        "scan_time": scan_time.isoformat(),
        "weather_markets": weather_markets[:50],
        "top_markets": top_markets[:30],
        "closing_soon": closing_soon[:20],
        "mispricings": mispricings[:10],
        "stats": {
            "total_weather": len(weather_markets),
            "tradeable_weather": len([
                m for m in weather_markets
                if m["spread"] is not None and m["spread"] < 20
                and m.get("mid") is not None and 10 <= m["mid"] <= 90
            ]),
            "nonzero_vol_weather": len([m for m in weather_markets if m["volume"] > 0]),
            "total_markets_fetched": len(top_markets),
        }
    }
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2, default=str)
    print(f"  ✅ Cache saved to {CACHE_PATH}")

    # Print summary to stdout (captured by cron log)
    nonzero = [m for m in weather_markets if m["volume"] > 0]
    print(f"\n=== SCAN SUMMARY ===")
    print(f"Weather contracts:   {len(weather_markets)} fetched, {len(nonzero)} with volume > 0")
    print(f"Top markets:         {len(top_markets)} fetched")
    print(f"Closing in 48h:      {len(closing_soon)}")
    print(f"Mispricing flags:    {len(mispricings)}")
    # Show the top market that actually has a price (skip null-price parlays)
    priced_top = [m for m in top_markets if m["bid"] is not None]
    if priced_top:
        t = priced_top[0]
        print(f"Top priced market:   {t['title'][:60]} (vol {t['volume']:,}, spread {t['spread']}c)")
    print(f"Log:                 {LOG_PATH}")
    print(f"Cache:               {CACHE_PATH}")
    print(f"Done: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
