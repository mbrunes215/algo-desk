"""
settle_today.py  —  One-shot script to settle all expired paper positions.

Fetches the final settlement result for each open 26MAR25 contract from
Kalshi, calculates P&L, and clears the position from paper state so the
bot can open new ones on the next scan.

Usage:
    cd algo-desk
    python3 settle_today.py
"""

import asyncio
import json
import os
import sys
import logging
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from dotenv import load_dotenv
load_dotenv()

from execution.kalshi_executor import KalshiExecutor

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger("settle_today")

PAPER_STATE_FILE = "logs/paper_state.json"


async def fetch_market(executor: KalshiExecutor, ticker: str):
    """Return the KalshiMarket object for a ticker, or None."""
    try:
        return await executor.get_market_by_ticker(ticker)
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return None


def get_settlement(market) -> tuple:
    """
    Extract (result_str, settlement_price_dollars) from a KalshiMarket.
    Returns ('unknown', None) if market hasn't settled yet.
    """
    if market is None:
        return "unknown", None

    # Try result field first (most reliable)
    result = getattr(market, "result", None) or (
        market.get("result") if isinstance(market, dict) else None
    )
    if result in ("yes", "no"):
        return result, 1.0 if result == "yes" else 0.0

    # Try status + last_price
    status = getattr(market, "status", "") or (
        market.get("status", "") if isinstance(market, dict) else ""
    )
    last_price = getattr(market, "last_price", None) or (
        market.get("last_price") if isinstance(market, dict) else None
    )

    if isinstance(status, str) and "settled" in status.lower():
        if last_price in (0.0, 0, 100.0, 100, 1.0, 1):
            price_norm = float(last_price)
            # Kalshi prices are 0-100 cents; normalize to dollars
            if price_norm > 1:
                price_norm = price_norm / 100.0
            return ("yes" if price_norm >= 0.5 else "no"), price_norm

    return "unknown", None


def main():
    api_key = os.getenv("KALSHI_API_KEY", "")
    executor = KalshiExecutor(api_key=api_key)

    with open(PAPER_STATE_FILE) as f:
        state = json.load(f)

    positions  = state.get("positions", {})
    trade_log  = state.get("trade_log", [])
    cash       = state.get("current_cash", 10000.0)

    if not positions:
        logger.info("No open positions — nothing to settle.")
        return

    logger.info(f"Found {len(positions)} open positions. Querying Kalshi for settlements...\n")

    # Build entry_price lookup from trade_log
    entry_prices = {}
    for trade in trade_log:
        sym = trade.get("symbol")
        if sym and sym not in entry_prices:
            entry_prices[sym] = trade.get("executed_price", trade.get("entry_price", 0.5))

    settled = []
    total_pnl = 0.0
    wins = 0
    losses = 0

    for ticker, pos_data in list(positions.items()):
        market = asyncio.run(fetch_market(executor, ticker))
        result, settlement_price = get_settlement(market)

        qty   = pos_data.get("quantity", 4)
        side  = pos_data.get("side", "BUY")
        entry = entry_prices.get(ticker, pos_data.get("entry_price", 0.5))

        if settlement_price is None:
            # Contract not settled yet or couldn't fetch — leave it open
            logger.warning(f"  {ticker}: Not settled yet (result={result}) — leaving open")
            continue

        # P&L calculation: Kalshi pays $1 per contract on YES
        if side == "BUY":
            pnl = (settlement_price - entry) * qty
            outcome = "WIN" if settlement_price > entry else "LOSS"
        else:  # SELL (we sold YES contracts short)
            pnl = (entry - settlement_price) * qty
            outcome = "WIN" if settlement_price < entry else "LOSS"

        # Return contract value to cash
        cash += settlement_price * qty
        total_pnl += pnl

        if outcome == "WIN":
            wins += 1
        else:
            losses += 1

        logger.info(
            f"  [{outcome}] {ticker} ({side} {qty}x) | "
            f"result={result} | "
            f"entry=${entry:.4f} → settlement=${settlement_price:.2f} | "
            f"P&L=${pnl:+.4f}"
        )

        settled.append(ticker)

    if not settled:
        logger.info("\nNo contracts settled yet. Try again after market close.")
        return

    # Remove settled positions
    for ticker in settled:
        del state["positions"][ticker]

    state["current_cash"] = cash

    with open(PAPER_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)

    logger.info(f"\n{'='*60}")
    logger.info(f"Settled: {len(settled)} contracts  |  Wins: {wins}  |  Losses: {losses}")
    logger.info(f"Win rate: {wins / len(settled):.0%}" if settled else "")
    logger.info(f"Total P&L: ${total_pnl:+.4f}")
    logger.info(f"Cash balance: ${cash:.2f}")
    logger.info(f"Open positions remaining: {len(state['positions'])}")
    logger.info(f"{'='*60}")
    logger.info("\nPaper state saved. Run 'python3 main.py --paper --strategy kalshi_weather' to resume.")


if __name__ == "__main__":
    main()
