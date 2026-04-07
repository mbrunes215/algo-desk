"""
BTC/ETH Pairs Monitor — Standalone Runner
==========================================
Run this to monitor the BTC/ETH spread and generate paper trading signals.

Usage (from algo-desk/):
    python3 strategies/pairs_trading/run_pairs.py           # one scan
    python3 strategies/pairs_trading/run_pairs.py --loop    # every 5 min
    python3 strategies/pairs_trading/run_pairs.py --loop --interval 300

Note on warmup:
    The Z-score model needs at least 72 obs to be valid (288 window / 4).
    At 5-min intervals that's 6 hours. Run --loop and let it warm up.
    A status bar shows progress during warmup.
"""

import argparse
import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from strategies.pairs_trading import PairsTradingStrategy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


def run_once(strategy: PairsTradingStrategy) -> None:
    """Run one full scan cycle."""
    print("\nFetching prices and computing spread...")
    signal = strategy.generate_signals()
    state  = strategy.compute_spread_state()

    # Fetch latest snapshot for the table (already fetched inside generate_signals)
    snap = strategy._price_history[-1] if strategy._price_history else None
    strategy.print_status_table(state, snap)

    if signal.signal:
        meta = signal.metadata
        print(
            f"  → ENTRY SIGNAL: {meta['direction']} | "
            f"Z={meta['z_score']:+.3f} | confidence={signal.confidence:.0%}\n"
        )
        # Execute the paper trade
        strategy.execute_trade(signal)
    else:
        meta = signal.metadata
        if not meta.get("is_valid", True):
            pct = meta.get("window_size", 0) / strategy.WINDOW * 100
            print(f"  Warmup progress: {meta.get('window_size',0)}/{strategy.WINDOW} obs "
                  f"({pct:.0f}%) — need {max(30, strategy.WINDOW//4)} minimum\n")


def main():
    parser = argparse.ArgumentParser(description="BTC/ETH Pairs Trading Monitor")
    parser.add_argument("--loop",     action="store_true", help="Run continuously")
    parser.add_argument("--interval", type=int, default=300, help="Loop interval seconds (default: 300 = 5 min)")
    parser.add_argument("--paper",    action="store_true", default=True, help="Paper mode (default)")
    parser.add_argument("--entry-z",  type=float, default=2.0, help="Entry Z-score threshold (default: 2.0)")
    parser.add_argument("--window",   type=int, default=288, help="Rolling window size (default: 288 obs = 24h at 5min)")
    args = parser.parse_args()

    strategy = PairsTradingStrategy(
        paper_mode=args.paper,
        config={
            "entry_z": args.entry_z,
            "window":  args.window,
        },
    )

    print(f"\nBTC/ETH Pairs Trading Monitor")
    print(f"  Entry threshold: Z = ±{args.entry_z}")
    print(f"  Window: {args.window} observations ({args.window * args.interval // 60} minutes at {args.interval//60}-min intervals)")
    print(f"  Warmup needed: {max(30, args.window // 4)} observations ({max(30, args.window // 4) * args.interval // 60} min)")

    if args.loop:
        print(f"\nRunning in loop mode — scanning every {args.interval // 60} min. Ctrl+C to stop.\n")
        while True:
            try:
                run_once(strategy)
                print(f"Next scan in {args.interval // 60} min...\n")
                time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\nStopped.")
                # Print final P&L summary
                if strategy.closed_positions:
                    total = sum(p["pnl_usd"] for p in strategy.closed_positions)
                    wins  = sum(1 for p in strategy.closed_positions if p["pnl_usd"] > 0)
                    print(f"\nSession summary: {len(strategy.closed_positions)} trades | "
                          f"Win rate: {wins/len(strategy.closed_positions):.0%} | "
                          f"Total P&L: ${total:+.2f}")
                break
    else:
        run_once(strategy)


if __name__ == "__main__":
    main()
