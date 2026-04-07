"""
Funding Arb — MacBook Diagnostic Script
========================================
Run this ONCE from your MacBook before starting the loop.
It checks that all APIs are reachable, data parses correctly,
and signals generate as expected.

Usage (from algo-desk/ directory):
    python check_funding_arb.py

Expected output: green checkmarks on all steps, a live rate table,
and either "opportunities found" or "no opportunities" (both are fine —
it means the system is working; rates may just be below threshold).

If you see FAILED on any step, the error message tells you exactly what to fix.
"""

import sys
import os
import json
import logging
import requests
from datetime import datetime, timezone

# Suppress noisy HTTP logs
logging.basicConfig(level=logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.ERROR)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

PASS = "  ✅"
FAIL = "  ❌"
INFO = "  ℹ️ "

def check(label, fn):
    try:
        result = fn()
        print(f"{PASS} {label}")
        return result
    except Exception as e:
        print(f"{FAIL} {label}")
        print(f"      Error: {e}")
        return None


def section(title):
    print(f"\n{'─' * 55}")
    print(f"  {title}")
    print(f"{'─' * 55}")


# ─────────────────────────────────────────────────────────────
# 1. KRAKEN FUTURES
# ─────────────────────────────────────────────────────────────
section("1 of 4 — Kraken Futures API")

def test_kraken_futures():
    r = requests.get("https://futures.kraken.com/derivatives/api/v3/tickers", timeout=10)
    r.raise_for_status()
    tickers = r.json().get("tickers", [])
    assert len(tickers) > 0, "Empty tickers list"
    return tickers

kraken_tickers = check("Kraken futures endpoint reachable", test_kraken_futures)

if kraken_tickers:
    target_perps = {"PF_XBTUSD", "PF_ETHUSD", "PF_SOLUSD"}
    found = {t["symbol"]: t for t in kraken_tickers if t.get("symbol") in target_perps}

    def check_kraken_perps():
        missing = target_perps - set(found.keys())
        assert not missing, f"Missing tickers: {missing}"
        return found

    found_data = check("BTC/ETH/SOL perp tickers present", check_kraken_perps)

    if found_data:
        print(f"\n  Live Kraken funding rates:")
        for sym, t in found_data.items():
            rate = t.get("fundingRate", "N/A")
            mark = t.get("markPrice", "N/A")
            if rate != "N/A":
                annualized = float(rate) * 1095
                print(f"    {sym}: rate={float(rate):.6f} ({annualized:.2%} ann) | mark=${float(mark):,.0f}")
            else:
                print(f"    {sym}: fundingRate field missing — check raw response")
                print(f"    Raw: {json.dumps(t, indent=6)}")

# ─────────────────────────────────────────────────────────────
# 2. KRAKEN SPOT
# ─────────────────────────────────────────────────────────────
section("2 of 4 — Kraken Spot API")

def test_kraken_spot():
    r = requests.get(
        "https://api.kraken.com/0/public/Ticker",
        params={"pair": "XBTUSD,ETHUSD,SOLUSD"},
        timeout=10,
    )
    r.raise_for_status()
    result = r.json().get("result", {})
    assert len(result) > 0, "Empty result"
    return result

kraken_spot = check("Kraken spot endpoint reachable", test_kraken_spot)

if kraken_spot:
    print(f"\n  Live Kraken spot prices:")
    for pair, data in kraken_spot.items():
        price = data.get("c", ["N/A"])[0]
        print(f"    {pair}: ${float(price):,.2f}")

# ─────────────────────────────────────────────────────────────
# 3. COINBASE SPOT
# ─────────────────────────────────────────────────────────────
section("3 of 4 — Coinbase API (Spot + Perp)")

def test_coinbase_spot():
    r = requests.get(
        "https://api.coinbase.com/api/v3/brokerage/market/products",
        params={"product_type": "SPOT", "limit": 250},
        timeout=10,
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()
    products = r.json().get("products", [])
    target = {"BTC-USD", "ETH-USD", "SOL-USD"}
    found = {p["product_id"]: p for p in products if p.get("product_id") in target}
    assert len(found) == 3, f"Expected 3 spot products, got {len(found)}: {list(found.keys())}"
    return found

cb_spot = check("Coinbase spot products reachable (BTC/ETH/SOL)", test_coinbase_spot)

if cb_spot:
    print(f"\n  Live Coinbase spot prices:")
    for pid, p in cb_spot.items():
        print(f"    {pid}: ${float(p.get('price', 0)):,.2f}")

def test_coinbase_perp():
    r = requests.get(
        "https://api.coinbase.com/api/v3/brokerage/market/products",
        params={"product_type": "FUTURE", "limit": 250},
        timeout=10,
        headers={"Content-Type": "application/json"},
    )
    r.raise_for_status()
    products = r.json().get("products", [])
    target = {"BTC-PERP-INTX", "ETH-PERP-INTX", "SOL-PERP-INTX"}
    found = {p["product_id"]: p for p in products if p.get("product_id") in target}
    # Perp products may not be available in all regions — warn rather than fail
    return found, products

cb_perp_result = check("Coinbase perp endpoint reachable", test_coinbase_perp)

if cb_perp_result:
    found_perps, all_futures = cb_perp_result
    if found_perps:
        print(f"\n  Live Coinbase perp funding rates:")
        for pid, p in found_perps.items():
            pd = p.get("perpetual_details", {})
            rate = pd.get("funding_rate", "N/A")
            price = p.get("price", "N/A")
            if rate != "N/A" and rate:
                annualized = float(rate) * 1095
                print(f"    {pid}: rate={float(rate):.6f} ({annualized:.2%} ann) | price=${float(price):,.0f}")
            else:
                print(f"    {pid}: price=${float(price or 0):,.0f} | fundingRate={rate}")
    else:
        # Not necessarily broken — CB perps (INTX) may not be available in all US states
        intx_any = [p["product_id"] for p in all_futures if "INTX" in p.get("product_id","")]
        futures_any = [p["product_id"] for p in all_futures[:5]]
        print(f"\n  {INFO} BTC/ETH/SOL-PERP-INTX not found in response.")
        print(f"       Any INTX products visible: {intx_any[:5]}")
        print(f"       Sample FUTURE products: {futures_any}")
        print(f"       NOTE: Coinbase International perps require non-US or eligible accounts.")
        print(f"       If you're US-based, Coinbase perps may be unavailable. Kraken perps are fine.")

# ─────────────────────────────────────────────────────────────
# 4. FULL STRATEGY SIGNAL TEST
# ─────────────────────────────────────────────────────────────
section("4 of 4 — Strategy Signal Generation (End-to-End)")

def test_strategy():
    from strategies.crypto_funding_arb import FundingArbStrategy
    strategy = FundingArbStrategy(paper_mode=True, config={"min_net_yield": 0.08})
    snapshots = strategy.scan_rates()
    assert len(snapshots) > 0, "No snapshots returned — all API calls failed inside strategy"
    return strategy, snapshots

result = check("Import + instantiate FundingArbStrategy", lambda: __import__('strategies.crypto_funding_arb', fromlist=['FundingArbStrategy']))

strat_result = check("Full scan_rates() returns data", test_strategy)

if strat_result:
    strategy, snapshots = strat_result
    print(f"\n  Snapshots collected: {len(snapshots)}")
    strategy.print_rate_table(snapshots)

    opps = strategy.find_opportunities(snapshots)
    if opps:
        print(f"  🟢 {len(opps)} OPPORTUNITY(s) found above 8% threshold:")
        for o in sorted(opps, key=lambda x: x.net_annual_yield, reverse=True):
            print(f"     → {o.symbol} on {o.exchange}: {o.net_annual_yield:.1%} net annualized")
    else:
        print(f"  ⚪ No opportunities above 8% threshold right now — rates may be low.")
        print(f"     This is normal. The monitor will catch them when rates rise.")
        best = max(snapshots, key=lambda s: s.net_annual_yield)
        print(f"     Best current rate: {best.symbol}/{best.exchange} at {best.net_annual_yield:.1%} net")

# ─────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────
section("SUMMARY")

print("""
  If all 4 sections show ✅, you're ready to run the monitor.

  START THE MONITOR (from algo-desk/):
  ─────────────────────────────────────
  One-time scan:
    python -m strategies.crypto_funding_arb.run_monitor

  Continuous loop (every 30 min):
    python -m strategies.crypto_funding_arb.run_monitor --loop

  Paper trade via main system:
    python main.py --paper

  ─────────────────────────────────────
  NOTE: If Coinbase PERP-INTX products are missing (US accounts),
  the strategy still works with Kraken perps only. Kraken covers
  BTC, ETH, SOL and has better fee structure anyway (0.36% round-trip
  vs 0.86% on Coinbase).
""")
