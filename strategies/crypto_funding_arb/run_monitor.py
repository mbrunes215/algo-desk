"""
Funding Rate Monitor — Standalone Runner
=========================================
Run this directly to see live funding rates and opportunities.

Usage:
    python -m strategies.crypto_funding_arb.run_monitor          # one scan
    python -m strategies.crypto_funding_arb.run_monitor --loop   # repeat every 30 min
    python -m strategies.crypto_funding_arb.run_monitor --paper  # paper trading signals

From algo-desk/:
    python -m strategies.crypto_funding_arb.run_monitor
"""

import argparse
import logging
import time
import sys
import os

# Make sure we can import from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from strategies.crypto_funding_arb import FundingArbStrategy
from strategies.base_strategy import StrategyResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
# Quiet the HTTP noise
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def run_once(strategy: FundingArbStrategy) -> None:
    """Run one full scan cycle."""
    logger.info("Scanning funding rates")  # parsed by daily_report.py to count scans
    snapshots = strategy.scan_rates()
    strategy.print_rate_table(snapshots)

    opportunities = strategy.find_opportunities(snapshots)
    if opportunities:
        logger.info(f"{len(opportunities)} opportunity(s) found above {strategy.MIN_NET_YIELD:.0%} threshold")
        best = max(opportunities, key=lambda o: o.net_annual_yield)
        for opp in sorted(opportunities, key=lambda o: o.net_annual_yield, reverse=True):
            logger.info(
                f"  → {opp.symbol} on {opp.exchange}: "
                f"{opp.net_annual_yield:.1%} net annualized | "
                f"${opp.recommended_notional_usd:,.0f}/leg"
            )
        # Build signal from already-fetched data — no second API call
        signal = StrategyResult(
            signal=True,
            confidence=min(best.net_annual_yield / 0.30, 1.0),
            side="BUY",
            size=1,
            metadata={
                "symbol": best.symbol,
                "exchange": best.exchange,
                "funding_rate": best.funding_rate,
                "annualized_rate": best.annualized_rate,
                "net_annual_yield": best.net_annual_yield,
                "spot_price": best.spot_price,
                "perp_price": best.perp_price,
                "basis_pct": best.basis_pct,
                "notional_usd": best.recommended_notional_usd,
                "all_opportunities": len(opportunities),
            },
        )
        strategy.execute_trade(signal)
    else:
        logger.info(f"No opportunities above {strategy.MIN_NET_YIELD:.0%} net threshold")

    strategy.print_open_positions()


def main():
    parser = argparse.ArgumentParser(description="Crypto Funding Rate Arb Monitor")
    parser.add_argument("--loop", action="store_true", help="Run continuously every 30 minutes")
    parser.add_argument("--interval", type=int, default=1800, help="Loop interval in seconds (default: 1800 = 30 min)")
    parser.add_argument("--paper", action="store_true", default=True, help="Paper trading mode (default)")
    parser.add_argument("--min-yield", type=float, default=0.08, help="Min net annual yield to signal (default: 0.08 = 8%%)")
    args = parser.parse_args()

    strategy = FundingArbStrategy(
        paper_mode=args.paper,
        config={"min_net_yield": args.min_yield},
    )

    if args.loop:
        print(f"Running in loop mode — scanning every {args.interval // 60} minutes. Ctrl+C to stop.\n")
        while True:
            try:
                run_once(strategy)
                print(f"Next scan in {args.interval // 60} minutes...\n")
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        run_once(strategy)


if __name__ == "__main__":
    main()
