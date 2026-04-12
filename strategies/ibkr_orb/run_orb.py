"""
ORB Runner — Standalone script for the Opening Range Breakout strategy
======================================================================

Runs the /MNQ ORB strategy in one of two modes:

  1. LIVE mode (--live): Connects to IBKR TWS/Gateway, streams real-time
     bars, and places bracket orders on breakouts. Requires TWS running
     with API enabled on port 7497 (paper) or 7496 (live).

  2. BACKTEST mode (--backtest): Replays historical 1-minute bars from
     IBKR and processes them through the strategy. Good for validation
     before going live.

  3. STATUS mode (--status): Print current strategy state and exit.

Usage:
    # From algo-desk/:
    python -m strategies.ibkr_orb.run_orb --live           # run live (paper)
    python -m strategies.ibkr_orb.run_orb --live --port 7496  # live trading
    python -m strategies.ibkr_orb.run_orb --backtest --days 20  # backtest
    python -m strategies.ibkr_orb.run_orb --status          # check state

    # Or directly:
    python strategies/ibkr_orb/run_orb.py --live
"""

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

# Make sure we can import from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from strategies.ibkr_orb import ORBStrategy
from strategies.ibkr_orb.orb_strategy import (
    is_market_hours, market_open_utc, to_et, utc_now, today_str, MNQ_MULTIPLIER,
)
from execution.ibkr_executor import IBKRExecutor, BarSnapshot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("ib_insync").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Live runner
# ---------------------------------------------------------------------------

async def run_live(args):
    """
    Run the ORB strategy live against IBKR TWS/Gateway.

    Connects to TWS, qualifies the /MNQ contract, requests historical bars
    to backfill today's range (in case we start mid-session), then streams
    real-time bars and processes each one through the strategy.
    """
    strategy = ORBStrategy(
        paper_mode=args.paper,
        config={
            "symbol": args.symbol,
            "range_minutes": args.range_minutes,
            "rr_multiple": args.rr,
            "contracts": args.contracts,
            "max_daily_loss_usd": args.max_loss,
            "min_range_points": args.min_range,
            "max_range_points": args.max_range,
            "close_time_et": args.close_time,
        },
    )

    executor = IBKRExecutor(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        paper_trading=args.paper,
    )

    strategy.executor = executor

    # Connect to TWS
    logger.info(f"Connecting to TWS at {args.host}:{args.port}...")
    connected = await executor.connect()
    if not connected:
        logger.error("Failed to connect to TWS. Is IBKR Desktop running with API enabled?")
        logger.error("  TWS → Global Config → API → Settings → Enable ActiveX and Socket Clients")
        logger.error(f"  Expected port: {args.port} (7497=paper, 7496=live)")
        return

    # Get account info
    account = await executor.get_account_value()
    if account:
        logger.info(
            f"Account: NLV=${account.get('net_liquidation', 0):,.0f} "
            f"Buying power=${account.get('buying_power', 0):,.0f}"
        )

    # Qualify MNQ futures contract (front month)
    try:
        contract = executor.make_contract(args.symbol, sec_type="FUT")
        contract = await executor.qualify_contract(contract)
        logger.info(
            f"Contract qualified: {contract.symbol} {contract.lastTradeDateOrContractMonth} "
            f"on {contract.exchange} (conId={contract.conId})"
        )
    except Exception as e:
        logger.error(f"Failed to qualify {args.symbol} contract: {e}")
        await executor.disconnect()
        return

    # Backfill today's bars if we're starting after 9:30 ET
    now = utc_now()
    if is_market_hours(now):
        logger.info("Market is open — backfilling today's bars...")
        try:
            bars = await executor.get_historical_bars(
                contract, duration="1 D", bar_size="1 min",
                what_to_show="TRADES", use_rth=True,
            )
            today = today_str()
            today_bars = [
                b for b in bars
                if to_et(b.timestamp).strftime("%Y-%m-%d") == today
            ]
            logger.info(f"Backfilled {len(today_bars)} bars for today")

            for bar in today_bars:
                action = strategy.process_bar(
                    bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume
                )
                if action and action not in ("MONITORING", "RANGE_BAR"):
                    logger.info(f"Backfill action: {action}")

        except Exception as e:
            logger.warning(f"Backfill failed (non-fatal): {e}")

    strategy.print_status()

    # If session already done (from backfill), just report and exit
    if strategy.state.session_done:
        logger.info("Session already complete for today. Exiting.")
        await executor.disconnect()
        return

    # Stream real-time bars
    logger.info("Starting real-time bar stream...")
    logger.info("Strategy will run until session close or Ctrl+C.")
    print()

    # Aggregate 5-second bars into 1-minute bars
    current_minute_bars: list = []
    last_minute: int = -1

    def on_5s_bar(bar: BarSnapshot):
        nonlocal current_minute_bars, last_minute

        bar_minute = bar.timestamp.minute
        bar_et = to_et(bar.timestamp)

        # New minute? Process the previous minute's aggregated bar
        if bar_minute != last_minute and current_minute_bars:
            agg_open = current_minute_bars[0].open
            agg_high = max(b.high for b in current_minute_bars)
            agg_low = min(b.low for b in current_minute_bars)
            agg_close = current_minute_bars[-1].close
            agg_volume = sum(b.volume for b in current_minute_bars)
            agg_time = current_minute_bars[0].timestamp

            action = strategy.process_bar(
                agg_time, agg_open, agg_high, agg_low, agg_close, agg_volume
            )
            if action and action not in ("MONITORING",):
                logger.info(
                    f"[{bar_et.strftime('%H:%M')} ET] "
                    f"O={agg_open:.2f} H={agg_high:.2f} L={agg_low:.2f} "
                    f"C={agg_close:.2f} → {action}"
                )

            current_minute_bars = []

        current_minute_bars.append(bar)
        last_minute = bar_minute

    try:
        executor.subscribe_realtime_bars(contract, on_5s_bar, what_to_show="TRADES")

        # Keep running until market close or session done
        while True:
            executor.sleep(1)

            if strategy.state.session_done:
                logger.info("Session complete. Strategy done for the day.")
                break

            if not is_market_hours():
                # Check if we're past close
                now_et = to_et(utc_now())
                if now_et.hour >= 16:
                    logger.info("Market closed. Wrapping up.")
                    break
                # Before open — wait
                if now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30):
                    pass  # keep waiting for open

    except KeyboardInterrupt:
        logger.info("\nStopped by user (Ctrl+C)")
    finally:
        executor.unsubscribe_realtime_bars(contract)
        await executor.disconnect()

    # Final status
    print()
    strategy.print_status()

    stats = strategy.get_stats()
    if stats["total_trades"] > 0:
        logger.info(
            f"All-time stats: {stats['total_trades']} trades, "
            f"win rate={stats['win_rate']:.0%}, "
            f"total P&L=${stats['total_pnl']:.2f}, "
            f"profit factor={stats['profit_factor']:.2f}"
        )


# ---------------------------------------------------------------------------
# Backtest runner
# ---------------------------------------------------------------------------

async def run_backtest(args):
    """
    Backtest the ORB strategy using historical IBKR data.

    Requests N days of 1-minute bars and replays them through the strategy,
    resetting state at each new trading day.
    """
    executor = IBKRExecutor(
        host=args.host,
        port=args.port,
        client_id=args.client_id,
        paper_trading=True,
    )

    logger.info(f"Connecting to TWS for historical data...")
    connected = await executor.connect()
    if not connected:
        logger.error("Failed to connect to TWS. Is IBKR Desktop running?")
        return

    # Qualify contract
    try:
        contract = executor.make_contract(args.symbol, sec_type="FUT")
        contract = await executor.qualify_contract(contract)
        logger.info(f"Contract: {contract.symbol} {contract.lastTradeDateOrContractMonth}")
    except Exception as e:
        logger.error(f"Failed to qualify contract: {e}")
        await executor.disconnect()
        return

    # Request historical bars
    duration = f"{args.days} D"
    logger.info(f"Requesting {duration} of 1-min bars (RTH only)...")

    try:
        bars = await executor.get_historical_bars(
            contract, duration=duration, bar_size="1 min",
            what_to_show="TRADES", use_rth=True,
        )
    except Exception as e:
        logger.error(f"Failed to get historical bars: {e}")
        await executor.disconnect()
        return

    await executor.disconnect()

    if not bars:
        logger.error("No bars returned")
        return

    logger.info(f"Got {len(bars)} bars from {bars[0].timestamp} to {bars[-1].timestamp}")

    # Group bars by date
    bars_by_date: dict = {}
    for bar in bars:
        date_str = to_et(bar.timestamp).strftime("%Y-%m-%d")
        if date_str not in bars_by_date:
            bars_by_date[date_str] = []
        bars_by_date[date_str].append(bar)

    logger.info(f"Trading days: {len(bars_by_date)}")
    print()

    # Replay each day
    all_trades = []

    for date_str in sorted(bars_by_date.keys()):
        day_bars = bars_by_date[date_str]

        strategy = ORBStrategy(
            paper_mode=True,
            config={
                "symbol": args.symbol,
                "range_minutes": args.range_minutes,
                "rr_multiple": args.rr,
                "contracts": args.contracts,
                "max_daily_loss_usd": args.max_loss,
                "min_range_points": args.min_range,
                "max_range_points": args.max_range,
                "close_time_et": args.close_time,
            },
        )
        # Force date
        strategy.state = strategy.state.__class__(date=date_str)

        for bar in day_bars:
            action = strategy.process_bar(
                bar.timestamp, bar.open, bar.high, bar.low, bar.close, bar.volume
            )

        # Collect results
        day_trades = strategy.get_trade_history()
        range_info = strategy.state.opening_range

        if day_trades:
            t = day_trades[-1]
            print(
                f"  {date_str}: {t['direction']:5s} "
                f"entry={t['entry_price']:.2f} exit={t['exit_price']:.2f} "
                f"reason={t['exit_reason']:10s} P&L=${t['pnl_usd']:+.2f} "
                f"({t['pnl_points']:+.1f}pts)"
            )
            all_trades.extend(day_trades)
        elif range_info and not range_info.is_valid:
            width = range_info.width_points
            reason = "tight" if width < args.min_range else "wide"
            print(f"  {date_str}: NO TRADE (range {reason}: {width:.1f}pts)")
        elif range_info:
            print(
                f"  {date_str}: NO TRADE (no breakout) "
                f"range={range_info.low:.2f}-{range_info.high:.2f}"
            )
        else:
            print(f"  {date_str}: NO TRADE (insufficient bars: {len(day_bars)})")

    # Summary
    print()
    print("=" * 60)
    print(f"BACKTEST SUMMARY — {args.symbol} ORB ({args.range_minutes}min range, {args.rr}R)")
    print("=" * 60)

    if not all_trades:
        print("No trades taken.")
        return

    pnls = [t["pnl_usd"] for t in all_trades]
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]
    gross_profit = sum(winners) if winners else 0
    gross_loss = abs(sum(losers)) if losers else 1

    print(f"  Days tested:    {len(bars_by_date)}")
    print(f"  Days traded:    {len(all_trades)}")
    print(f"  Trade rate:     {len(all_trades)/len(bars_by_date):.0%}")
    print(f"  Total P&L:      ${sum(pnls):+.2f}")
    print(f"  Winners:        {len(winners)} ({len(winners)/len(all_trades):.0%})")
    print(f"  Losers:         {len(losers)} ({len(losers)/len(all_trades):.0%})")
    print(f"  Avg winner:     ${sum(winners)/len(winners):+.2f}" if winners else "  Avg winner:     N/A")
    print(f"  Avg loser:      ${sum(losers)/len(losers):+.2f}" if losers else "  Avg loser:      N/A")
    print(f"  Profit factor:  {gross_profit/gross_loss:.2f}")
    print(f"  Max win:        ${max(pnls):+.2f}")
    print(f"  Max loss:       ${min(pnls):+.2f}")
    print(f"  Avg P&L/trade:  ${sum(pnls)/len(pnls):+.2f}")

    exit_reasons = {}
    for t in all_trades:
        r = t["exit_reason"]
        exit_reasons[r] = exit_reasons.get(r, 0) + 1
    print(f"  Exit breakdown: {exit_reasons}")
    print()


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def run_status(args):
    """Print current ORB strategy state."""
    strategy = ORBStrategy(paper_mode=True)
    strategy.print_status()

    stats = strategy.get_stats()
    if stats["total_trades"] > 0:
        print()
        print(f"All-time stats ({stats['total_trades']} trades):")
        print(f"  Win rate:       {stats['win_rate']:.0%}")
        print(f"  Total P&L:      ${stats['total_pnl']:+.2f}")
        print(f"  Avg P&L/trade:  ${stats['avg_pnl']:+.2f}")
        print(f"  Profit factor:  {stats['profit_factor']:.2f}")
        print(f"  Exit breakdown: {stats['exit_reasons']}")

    recent = strategy.get_trade_history(5)
    if recent:
        print()
        print("Last 5 trades:")
        for t in recent:
            print(
                f"  {t['date']}: {t['direction']:5s} "
                f"${t['pnl_usd']:+.2f} ({t['exit_reason']})"
            )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="ORB Strategy Runner — /MNQ Opening Range Breakout"
    )

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true", help="Run live against TWS")
    mode.add_argument("--backtest", action="store_true", help="Backtest with historical data")
    mode.add_argument("--status", action="store_true", help="Print current state")

    # Connection
    parser.add_argument("--host", default="127.0.0.1", help="TWS host")
    parser.add_argument("--port", type=int, default=7497, help="TWS port (7497=paper, 7496=live)")
    parser.add_argument("--client-id", type=int, default=2, help="IBKR client ID (use 2 to avoid conflicts)")

    # Strategy params
    parser.add_argument("--symbol", default="MNQ", help="Futures symbol (default: MNQ)")
    parser.add_argument("--range-minutes", type=int, default=15, help="Opening range window in minutes")
    parser.add_argument("--rr", type=float, default=2.0, help="Risk:reward multiple")
    parser.add_argument("--contracts", type=int, default=1, help="Number of contracts")
    parser.add_argument("--max-loss", type=float, default=200.0, help="Max daily loss in USD")
    parser.add_argument("--min-range", type=float, default=10.0, help="Min range width in points")
    parser.add_argument("--max-range", type=float, default=100.0, help="Max range width in points")
    parser.add_argument("--close-time", default="15:55", help="Hard close time ET (HH:MM)")

    # Backtest
    parser.add_argument("--days", type=int, default=20, help="Days to backtest")

    # Mode
    parser.add_argument("--paper", action="store_true", default=True, help="Paper trading (default)")
    parser.add_argument("--no-paper", action="store_true", help="Live trading (use with caution)")

    args = parser.parse_args()

    if args.no_paper:
        args.paper = False
        logger.warning("⚠️  LIVE TRADING MODE — real orders will be placed!")

    if args.live:
        asyncio.run(run_live(args))
    elif args.backtest:
        asyncio.run(run_backtest(args))
    elif args.status:
        run_status(args)


if __name__ == "__main__":
    main()
