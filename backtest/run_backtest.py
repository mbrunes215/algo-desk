"""
backtest/run_backtest.py

Main entry point for running the backtest pipeline.

Run this on either Mac (old or new). Results are stored in trading.db
and the live strategy will automatically pick up the calibration data.

Usage:
    cd algo-desk
    python3 backtest/run_backtest.py

    # Customize:
    python3 backtest/run_backtest.py --days 30 --cities NYC LAX CHI
    python3 backtest/run_backtest.py --days 60 --skip-kalshi   # no API needed
    python3 backtest/run_backtest.py --summary-only             # print last results
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

from backtest.schema import init_schema, create_run, finish_run
from backtest.data_collector import HistoricalDataCollector
from backtest.engine import BacktestEngine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest")

DEFAULT_CITIES = ["NYC", "LAX", "CHI", "MIA", "DEN", "HOU"]
DEFAULT_DB = "trading.db"


def run_backtest(
    days: int = 30,
    cities: list = None,
    db_path: str = DEFAULT_DB,
    skip_kalshi: bool = False,
    min_edge_bps: float = 150.0,
) -> int:
    """
    Run the full backtest pipeline. Returns run_id.
    """
    if cities is None:
        cities = DEFAULT_CITIES

    kalshi_api_key = os.getenv("KALSHI_API_KEY", "")
    if not kalshi_api_key and not skip_kalshi:
        logger.warning("No KALSHI_API_KEY found — Kalshi market data will be skipped")
        logger.warning("Set KALSHI_API_KEY in .env or use --skip-kalshi to suppress this")
        skip_kalshi = True

    logger.info("="*60)
    logger.info("ALGO TRADING DESK — BACKTEST PIPELINE")
    logger.info("="*60)
    logger.info(f"Lookback: {days} days")
    logger.info(f"Cities:   {cities}")
    logger.info(f"DB:       {db_path}")
    logger.info(f"Kalshi:   {'SKIPPED' if skip_kalshi else 'enabled'}")
    logger.info("="*60)

    # Initialize DB schema
    init_schema(db_path)
    run_id = create_run(db_path, days, cities)
    logger.info(f"Started backtest run_id={run_id}")

    collector = HistoricalDataCollector(kalshi_api_key=kalshi_api_key)
    engine = BacktestEngine(db_path=db_path)

    counts = {"forecast": 0, "signal": 0, "calibration": 0}
    start_time = time.time()

    try:
        # ── Step 1: Collect forecast accuracy data ─────────────────────
        logger.info(f"\n[1/3] Collecting ECMWF hindcasts + NOAA actuals for {len(cities)} cities × {days} days...")
        logger.info("      (This takes ~2-3 minutes due to API rate limits)")

        forecast_rows = collector.collect_forecast_accuracy(cities=cities, lookback_days=days)
        counts["forecast"] = engine.insert_forecast_accuracy(run_id, forecast_rows)
        logger.info(f"      ✓ {counts['forecast']} forecast accuracy rows inserted")

        # ── Step 2: Collect Kalshi settled markets + replay signals ───
        if not skip_kalshi:
            logger.info(f"\n[2/3] Fetching settled Kalshi markets (last {days} days)...")
            kalshi_markets = collector.fetch_kalshi_settled_markets(lookback_days=days)

            if kalshi_markets:
                logger.info(f"      Replaying {len(kalshi_markets)} markets as signals...")
                signal_rows = engine.replay_signals(
                    kalshi_markets=kalshi_markets,
                    forecast_rows=forecast_rows,
                    min_edge_bps=min_edge_bps,
                )
                counts["signal"] = engine.insert_signal_replay(run_id, signal_rows)
                logger.info(f"      ✓ {counts['signal']} signal replay rows inserted")
            else:
                logger.warning("      No settled Kalshi markets found — signal replay skipped")
        else:
            logger.info("\n[2/3] Kalshi step skipped (--skip-kalshi)")

        # ── Step 3: Compute calibration stats ─────────────────────────
        logger.info("\n[3/3] Computing calibration statistics...")
        calibration_rows = engine.compute_calibration(run_id)
        counts["calibration"] = len(calibration_rows)
        logger.info(f"      ✓ {counts['calibration']} calibration rows written")

        # ── Done ──────────────────────────────────────────────────────
        elapsed = time.time() - start_time
        finish_run(db_path, run_id, counts, status="complete")

        logger.info(f"\nBacktest complete in {elapsed:.1f}s (run_id={run_id})")
        engine.print_summary(run_id)

        return run_id

    except Exception as e:
        logger.error(f"Backtest failed: {e}", exc_info=True)
        finish_run(db_path, run_id, counts, status="failed", error=str(e))
        raise


def print_last_summary(db_path: str = DEFAULT_DB) -> None:
    """Print summary of the most recent completed backtest run."""
    import sqlite3

    init_schema(db_path)  # ensure tables exist

    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "SELECT id FROM bt_runs WHERE status='complete' ORDER BY id DESC LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            print("No completed backtest runs found. Run without --summary-only first.")
            return
        run_id = row[0]

    engine = BacktestEngine(db_path=db_path)
    engine.print_summary(run_id)


def main():
    parser = argparse.ArgumentParser(
        description="Backtest the Kalshi weather trading strategy",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 backtest/run_backtest.py
  python3 backtest/run_backtest.py --days 60 --cities NYC LAX CHI MIA
  python3 backtest/run_backtest.py --skip-kalshi          # forecast accuracy only
  python3 backtest/run_backtest.py --summary-only         # show last results
        """,
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--cities", nargs="+", default=DEFAULT_CITIES, help="Cities to backtest")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite DB (default: trading.db)")
    parser.add_argument("--skip-kalshi", action="store_true", help="Skip Kalshi API calls (forecast accuracy only)")
    parser.add_argument("--min-edge", type=float, default=150.0, help="Min edge in bps to count as signal (default: 150)")
    parser.add_argument("--summary-only", action="store_true", help="Print last backtest results and exit")

    args = parser.parse_args()

    if args.summary_only:
        print_last_summary(args.db)
        return

    run_backtest(
        days=args.days,
        cities=args.cities,
        db_path=args.db,
        skip_kalshi=args.skip_kalshi,
        min_edge_bps=args.min_edge,
    )


if __name__ == "__main__":
    main()
