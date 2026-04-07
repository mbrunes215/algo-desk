"""
Crypto Funding Rate Arbitrage Strategy
=======================================

HOW IT WORKS:
  Perpetual futures contracts use a "funding rate" mechanism to keep their price
  anchored to spot. Every 8 hours, the exchange transfers money between longs and
  shorts. When the rate is positive, longs pay shorts. When negative, shorts pay longs.

  This strategy holds:
    - SPOT LONG:  Buy the asset on spot market (e.g., buy ETH on Coinbase)
    - PERP SHORT: Short an equal notional on the perpetual futures market

  Since spot and perp move together, price changes cancel out. What remains is
  the funding payment, collected every 8 hours. We only open when the annualized
  rate exceeds our minimum threshold after fees.

SUPPORTED EXCHANGES:
  - Kraken (spot + futures via api.kraken.com and futures.kraken.com)
  - Coinbase Advanced Trade (spot + perp futures via api.coinbase.com)

MONITORED PAIRS:
  BTC/USD, ETH/USD, SOL/USD (configurable)

ENTRY CONDITIONS:
  - Annualized funding rate > MIN_ANNUAL_RATE (default 10%)
  - Spread between spot and perp < MAX_BASIS_PCT (default 0.3%)
  - Sufficient liquidity on both legs

EXIT CONDITIONS:
  - Funding rate drops below EXIT_RATE (default 5% annualized)
  - Funding rate flips negative (shorts now paying longs — we'd lose money)
  - Position held > MAX_HOLD_DAYS without sufficient yield

FEES (conservative estimates):
  - Kraken maker: 0.16% spot, 0.02% futures
  - Coinbase maker: 0.40% spot, 0.03% futures
  - Total round-trip (open + close): ~0.42% Kraken, ~0.86% Coinbase
  - Break-even annualized rate: ~3.8% Kraken, ~7.8% Coinbase

PAPER TRADING NOTE:
  In paper mode, positions are simulated. No real API calls to order endpoints.
  Funding rates ARE fetched live so signals reflect real market conditions.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..base_strategy import BaseStrategy, StrategyResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Funding is paid every 8 hours → 3 payments/day → 1095 payments/year
FUNDING_PERIODS_PER_YEAR = 1095

# Exchanges and their fee structures (round-trip: open + close both legs)
EXCHANGE_FEES = {
    "kraken": {
        "spot_maker": 0.0016,    # 0.16%
        "perp_maker": 0.0002,    # 0.02%
        "round_trip": 0.0036,    # (spot + perp) × 2 legs
    },
    "binance": {
        "spot_maker": 0.0010,    # 0.10% (standard tier)
        "perp_maker": 0.0002,    # 0.02%
        "round_trip": 0.0024,    # (spot + perp) × 2 legs — cheapest of the three
    },
}

# Kraken API endpoints
KRAKEN_SPOT_URL = "https://api.kraken.com/0/public/Ticker"
KRAKEN_FUTURES_URL = "https://futures.kraken.com/derivatives/api/v3/tickers"

# Binance API endpoints (public, no auth required)
# premiumIndex returns markPrice, indexPrice, lastFundingRate, nextFundingTime
BINANCE_PREMIUM_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"

# Symbols to monitor — maps our internal name to exchange-specific tickers
SYMBOLS = {
    "BTC": {
        "kraken_spot": "XBTUSD",
        "kraken_perp": "PF_XBTUSD",        # Kraken perpetual futures ticker
        "binance_perp": "BTCUSDT",          # Binance USDT-margined perpetual
    },
    "ETH": {
        "kraken_spot": "ETHUSD",
        "kraken_perp": "PF_ETHUSD",
        "binance_perp": "ETHUSDT",
    },
    "SOL": {
        "kraken_spot": "SOLUSD",
        "kraken_perp": "PF_SOLUSD",
        "binance_perp": "SOLUSDT",
    },
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FundingSnapshot:
    """A point-in-time snapshot of funding rate data for one asset/exchange."""
    symbol: str               # e.g. "BTC"
    exchange: str             # e.g. "kraken"
    timestamp: datetime
    funding_rate: float       # Per-period rate (e.g. 0.0001 = 0.01% per 8 hours)
    annualized_rate: float    # funding_rate × 1095
    spot_price: float
    perp_price: float
    basis_pct: float          # (perp - spot) / spot — how far perp trades from spot
    is_profitable: bool       # True if rate > threshold after fees
    net_annual_yield: float   # annualized_rate minus annualized fee drag
    raw_response: Dict = field(default_factory=dict)


@dataclass
class ArbOpportunity:
    """A confirmed arb opportunity that passes all filters."""
    symbol: str
    exchange: str
    funding_rate: float
    annualized_rate: float
    net_annual_yield: float
    spot_price: float
    perp_price: float
    basis_pct: float
    timestamp: datetime
    recommended_notional_usd: float  # How much to deploy (both legs each)


# ---------------------------------------------------------------------------
# Exchange clients
# ---------------------------------------------------------------------------

def fetch_kraken_funding(symbol: str, tickers: Dict) -> Optional[FundingSnapshot]:
    """
    Parse Kraken funding rate data from pre-fetched tickers response.

    Kraken futures API returns all tickers in one call. The funding rate field
    is 'fundingRate' — this is the predicted rate for the NEXT 8-hour period.
    Kraken also provides 'fundingRateRelative' and 'openInterest'.
    """
    perp_ticker = SYMBOLS[symbol]["kraken_perp"]
    spot_ticker = SYMBOLS[symbol]["kraken_spot"]

    # Find the perp ticker in the response
    perp_data = None
    for t in tickers.get("tickers", []):
        if t.get("symbol") == perp_ticker:
            perp_data = t
            break

    if not perp_data:
        logger.debug(f"Kraken: no perp ticker found for {symbol} ({perp_ticker})")
        return None

    try:
        # Kraken fundingRate is expressed as a percentage (e.g. -0.2367 means -0.2367%)
        # Divide by 100 to convert to decimal before annualizing
        funding_rate = float(perp_data.get("fundingRate", 0)) / 100
        perp_price = float(perp_data.get("markPrice", 0) or perp_data.get("last", 0))
    except (TypeError, ValueError) as e:
        logger.warning(f"Kraken: failed to parse perp data for {symbol}: {e}")
        return None

    # Spot price — fetch separately (already done by caller)
    spot_price = tickers.get("_spot_prices", {}).get(spot_ticker, 0.0)

    if spot_price <= 0 or perp_price <= 0:
        logger.debug(f"Kraken: zero price for {symbol} spot={spot_price} perp={perp_price}")
        return None

    annualized = funding_rate * FUNDING_PERIODS_PER_YEAR
    basis_pct = (perp_price - spot_price) / spot_price
    fees = EXCHANGE_FEES["kraken"]
    # Annualized fee drag: round-trip fees amortized — assume we hold ~30 days
    # Round-trip is paid once at open/close; spread over the hold period.
    # 30-day hold → fee_drag = round_trip / (30/365) ... but we want annualized:
    # net_yield = annualized_rate - (round_trip * 12)  [12 round-trips/year if 30-day holds]
    # Conservative: assume 6 round-trips/year (avg 60-day hold)
    annualized_fee_drag = fees["round_trip"] * 6
    net_yield = annualized - annualized_fee_drag

    return FundingSnapshot(
        symbol=symbol,
        exchange="kraken",
        timestamp=datetime.now(timezone.utc),
        funding_rate=funding_rate,
        annualized_rate=annualized,
        spot_price=spot_price,
        perp_price=perp_price,
        basis_pct=basis_pct,
        is_profitable=net_yield > 0.05,  # > 5% net annualized
        net_annual_yield=net_yield,
        raw_response=perp_data,
    )


def fetch_binance_funding(symbol: str, all_data: List[Dict]) -> Optional[FundingSnapshot]:
    """
    Parse Binance perpetual funding rate from pre-fetched premiumIndex response.

    Binance premiumIndex endpoint (public, no auth) returns per-symbol data:
      - symbol:          e.g. "BTCUSDT"
      - markPrice:       current mark price
      - indexPrice:      spot index price (used as spot reference)
      - lastFundingRate: most recent 8-hour funding rate (decimal, e.g. -0.001185)
      - nextFundingTime: unix ms timestamp of next settlement

    Funding rate is already in decimal form (not percentage). Paid every 8 hours.
    Binance is the deepest perp venue — these rates are the market benchmark.
    """
    perp_ticker = SYMBOLS[symbol]["binance_perp"]

    entry = None
    for item in all_data:
        if item.get("symbol") == perp_ticker:
            entry = item
            break

    if not entry:
        logger.debug(f"Binance: no entry found for {symbol} ({perp_ticker})")
        return None

    try:
        funding_rate = float(entry.get("lastFundingRate", 0) or 0)
        perp_price = float(entry.get("markPrice", 0) or 0)
        spot_price = float(entry.get("indexPrice", 0) or 0)
    except (TypeError, ValueError) as e:
        logger.warning(f"Binance: failed to parse data for {symbol}: {e}")
        return None

    if spot_price <= 0 or perp_price <= 0:
        logger.debug(f"Binance: zero price for {symbol}")
        return None

    annualized = funding_rate * FUNDING_PERIODS_PER_YEAR
    basis_pct = (perp_price - spot_price) / spot_price
    fees = EXCHANGE_FEES["binance"]
    annualized_fee_drag = fees["round_trip"] * 6
    net_yield = annualized - annualized_fee_drag

    return FundingSnapshot(
        symbol=symbol,
        exchange="binance",
        timestamp=datetime.now(timezone.utc),
        funding_rate=funding_rate,
        annualized_rate=annualized,
        spot_price=spot_price,
        perp_price=perp_price,
        basis_pct=basis_pct,
        is_profitable=net_yield > 0.05,
        net_annual_yield=net_yield,
        raw_response=entry,
    )


# ---------------------------------------------------------------------------
# Main strategy class
# ---------------------------------------------------------------------------

class FundingArbStrategy(BaseStrategy):
    """
    Crypto Funding Rate Arbitrage Strategy.

    Monitors BTC, ETH, SOL funding rates on Kraken and Coinbase.
    Generates BUY signals when annualized net yield > MIN_NET_YIELD.
    Generates SELL signals when yield drops below EXIT_YIELD.

    In paper mode: logs all signals and simulates position P&L.
    In live mode: would place real orders on both spot and perp legs.
    """

    # --- Strategy parameters (can be overridden via config) ---
    MIN_NET_YIELD = 0.08       # 8% annualized net of fees to open
    EXIT_YIELD = 0.04          # 4% annualized — close if yield drops here
    MAX_BASIS_PCT = 0.005      # 0.5% max spread between spot and perp at entry
    MAX_HOLD_DAYS = 60         # Force close if held this long regardless of yield
    POSITION_SIZE_USD = 500    # USD notional per leg (both spot and perp legs)
    MAX_POSITIONS = 6          # Max concurrent arb positions (3 symbols × 2 exchanges)
    REQUEST_TIMEOUT = 10       # API request timeout in seconds

    def __init__(
        self,
        paper_mode: bool = True,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name="crypto_funding_arb",
            enabled=True,
            paper_mode=paper_mode,
        )

        # Override defaults from config if provided
        if config:
            self.MIN_NET_YIELD = config.get("min_net_yield", self.MIN_NET_YIELD)
            self.EXIT_YIELD = config.get("exit_yield", self.EXIT_YIELD)
            self.MAX_BASIS_PCT = config.get("max_basis_pct", self.MAX_BASIS_PCT)
            self.POSITION_SIZE_USD = config.get("position_size_usd", self.POSITION_SIZE_USD)
            self.MAX_POSITIONS = config.get("max_positions", self.MAX_POSITIONS)

        # Track open arb positions: key = "SYMBOL_EXCHANGE"
        self.open_positions: Dict[str, ArbOpportunity] = {}
        # Track all snapshots for logging/analysis
        self.snapshot_history: List[FundingSnapshot] = []

        logger.info(
            f"FundingArbStrategy initialized | paper={paper_mode} | "
            f"min_yield={self.MIN_NET_YIELD:.1%} | position_size=${self.POSITION_SIZE_USD}"
        )

    # -----------------------------------------------------------------------
    # Data fetching
    # -----------------------------------------------------------------------

    def _fetch_kraken_data(self) -> Dict:
        """
        Fetch all Kraken futures tickers + spot prices in one pass.
        Returns a combined dict with perp tickers and '_spot_prices' injected.
        """
        combined = {"tickers": [], "_spot_prices": {}}

        # Futures tickers (all symbols in one call)
        try:
            resp = requests.get(KRAKEN_FUTURES_URL, timeout=self.REQUEST_TIMEOUT)
            resp.raise_for_status()
            combined["tickers"] = resp.json().get("tickers", [])
            logger.debug(f"Kraken futures: fetched {len(combined['tickers'])} tickers")
        except requests.RequestException as e:
            logger.warning(f"Kraken futures API error: {e}")
            return combined

        # Spot prices — one call with all pairs
        spot_pairs = [SYMBOLS[s]["kraken_spot"] for s in SYMBOLS]
        try:
            resp = requests.get(
                KRAKEN_SPOT_URL,
                params={"pair": ",".join(spot_pairs)},
                timeout=self.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            result = resp.json().get("result", {})
            # Kraken returns non-standard keys: XXBTZUSD, XETHZUSD, SOLUSD
            # Build an explicit alias map so we always match regardless of prefix
            kraken_aliases = {
                "XXBTZUSD": "XBTUSD",
                "XBTUSD":   "XBTUSD",
                "XETHZUSD": "ETHUSD",
                "ETHUSD":   "ETHUSD",
                "SOLUSD":   "SOLUSD",
            }
            for pair, data in result.items():
                # Kraken spot 'c' field = [last_trade_price, lot_volume]
                price = float(data["c"][0]) if "c" in data else 0.0
                canonical = kraken_aliases.get(pair)
                if canonical:
                    combined["_spot_prices"][canonical] = price
            logger.debug(f"Kraken spot prices: {combined['_spot_prices']}")
        except requests.RequestException as e:
            logger.warning(f"Kraken spot API error: {e}")

        return combined

    def _fetch_binance_data(self) -> List[Dict]:
        """
        Fetch all Binance USDT perpetual funding rates in one call (public, no auth).

        GET https://fapi.binance.com/fapi/v1/premiumIndex
        Returns a list of objects with symbol, markPrice, indexPrice,
        lastFundingRate, nextFundingTime for every USDT-margined perp.
        One call covers all symbols — no pagination needed.
        """
        try:
            resp = requests.get(
                BINANCE_PREMIUM_URL,
                timeout=self.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            # Filter to only the symbols we track
            target = {SYMBOLS[s]["binance_perp"] for s in SYMBOLS}
            filtered = [item for item in data if item.get("symbol") in target]
            logger.debug(f"Binance: fetched {len(data)} perps, {len(filtered)} relevant")
            return filtered
        except requests.RequestException as e:
            # 451 = geo-block (expected for US IPs) — not a real error
            logger.info(f"Binance API unavailable (expected for US): {e}")
            return []

    # -----------------------------------------------------------------------
    # Signal generation
    # -----------------------------------------------------------------------

    def scan_rates(self) -> List[FundingSnapshot]:
        """
        Fetch current funding rates from all exchanges and return snapshots.
        This is the core monitoring function — call it on a schedule.
        """
        snapshots = []

        # --- Kraken ---
        kraken_data = self._fetch_kraken_data()
        for symbol in SYMBOLS:
            snap = fetch_kraken_funding(symbol, kraken_data)
            if snap:
                snapshots.append(snap)
                logger.info(
                    f"Kraken {symbol}: rate={snap.funding_rate:.6f} "
                    f"({snap.annualized_rate:.1%} ann) | "
                    f"net={snap.net_annual_yield:.1%} | "
                    f"basis={snap.basis_pct:.3%} | "
                    f"spot=${snap.spot_price:,.0f}"
                )

        # Small delay to be polite to APIs
        time.sleep(0.5)

        # --- Binance (public, no auth — deepest liquidity, benchmark rates) ---
        binance_data = self._fetch_binance_data()
        for symbol in SYMBOLS:
            snap = fetch_binance_funding(symbol, binance_data)
            if snap:
                snapshots.append(snap)
                logger.info(
                    f"Binance {symbol}: rate={snap.funding_rate:.6f} "
                    f"({snap.annualized_rate:.1%} ann) | "
                    f"net={snap.net_annual_yield:.1%} | "
                    f"basis={snap.basis_pct:.3%} | "
                    f"spot=${snap.spot_price:,.0f}"
                )

        self.snapshot_history.extend(snapshots)
        return snapshots

    def find_opportunities(self, snapshots: List[FundingSnapshot]) -> List[ArbOpportunity]:
        """
        Filter snapshots down to actionable arb opportunities.

        Filters applied:
          1. Net yield > MIN_NET_YIELD (default 8%)
          2. Basis < MAX_BASIS_PCT (spot and perp trading close together)
          3. Funding rate is positive (we want to be the SHORT collecting payments)
          4. Not already in this position
        """
        opportunities = []

        for snap in snapshots:
            pos_key = f"{snap.symbol}_{snap.exchange}"

            # Already in this position — check if we should EXIT instead
            if pos_key in self.open_positions:
                if snap.net_annual_yield < self.EXIT_YIELD or snap.funding_rate < 0:
                    logger.info(
                        f"EXIT signal: {pos_key} | yield={snap.net_annual_yield:.1%} "
                        f"(threshold={self.EXIT_YIELD:.1%}) | "
                        f"rate={'negative' if snap.funding_rate < 0 else 'below exit'}"
                    )
                continue

            # Skip if funding rate is negative (we'd be paying, not collecting)
            if snap.funding_rate <= 0:
                logger.debug(f"Skip {snap.symbol}/{snap.exchange}: negative funding rate")
                continue

            # Skip if basis is too wide (spot and perp have diverged — execution risk)
            if abs(snap.basis_pct) > self.MAX_BASIS_PCT:
                logger.info(
                    f"Skip {snap.symbol}/{snap.exchange}: basis too wide "
                    f"({snap.basis_pct:.3%} > {self.MAX_BASIS_PCT:.3%})"
                )
                continue

            # Skip if net yield is below our threshold
            if snap.net_annual_yield < self.MIN_NET_YIELD:
                logger.debug(
                    f"Skip {snap.symbol}/{snap.exchange}: yield too low "
                    f"({snap.net_annual_yield:.1%} < {self.MIN_NET_YIELD:.1%})"
                )
                continue

            # Skip if we're at max positions
            if len(self.open_positions) >= self.MAX_POSITIONS:
                logger.info(f"Skip {snap.symbol}/{snap.exchange}: at max positions ({self.MAX_POSITIONS})")
                continue

            opp = ArbOpportunity(
                symbol=snap.symbol,
                exchange=snap.exchange,
                funding_rate=snap.funding_rate,
                annualized_rate=snap.annualized_rate,
                net_annual_yield=snap.net_annual_yield,
                spot_price=snap.spot_price,
                perp_price=snap.perp_price,
                basis_pct=snap.basis_pct,
                timestamp=snap.timestamp,
                recommended_notional_usd=self.POSITION_SIZE_USD,
            )
            opportunities.append(opp)

        return opportunities

    def generate_signals(self) -> StrategyResult:
        """
        Main signal generation method (required by BaseStrategy).

        Scans all rates, finds opportunities, returns the best one as a signal.
        In practice, call scan_rates() + find_opportunities() directly for
        multi-signal handling.
        """
        snapshots = self.scan_rates()
        opportunities = self.find_opportunities(snapshots)

        if not opportunities:
            return StrategyResult(
                signal=False,
                confidence=0.0,
                side="HOLD",
                size=0,
                metadata={"reason": "no profitable opportunities found", "scanned": len(snapshots)},
            )

        # Return the highest-yield opportunity as the primary signal
        best = max(opportunities, key=lambda o: o.net_annual_yield)
        confidence = min(best.net_annual_yield / 0.30, 1.0)  # Scale: 30% yield = full confidence

        return StrategyResult(
            signal=True,
            confidence=confidence,
            side="BUY",
            size=1,  # 1 arb unit = 1 spot + 1 perp position
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

    # -----------------------------------------------------------------------
    # Required BaseStrategy methods
    # -----------------------------------------------------------------------

    def execute_trade(self, signal: StrategyResult) -> bool:
        """
        Execute the arb trade. In paper mode, logs the position.
        In live mode, would place simultaneous spot buy + perp sell orders.
        """
        if not signal.signal:
            return False

        meta = signal.metadata
        pos_key = f"{meta['symbol']}_{meta['exchange']}"

        if self._paper_mode:
            logger.info(
                f"[PAPER] OPEN ARB: {meta['symbol']} on {meta['exchange']} | "
                f"notional=${meta['notional_usd']:,.0f} each leg | "
                f"net yield={meta['net_annual_yield']:.1%} ann | "
                f"spot=${meta['spot_price']:,.2f} perp=${meta['perp_price']:,.2f}"
            )
            # Record the position
            self.open_positions[pos_key] = ArbOpportunity(
                symbol=meta["symbol"],
                exchange=meta["exchange"],
                funding_rate=meta["funding_rate"],
                annualized_rate=meta["annualized_rate"],
                net_annual_yield=meta["net_annual_yield"],
                spot_price=meta["spot_price"],
                perp_price=meta["perp_price"],
                basis_pct=meta["basis_pct"],
                timestamp=meta.get("timestamp", datetime.now(timezone.utc)),
                recommended_notional_usd=meta["notional_usd"],
            )
            return True
        else:
            # Live execution placeholder — do NOT execute without further testing
            logger.warning("Live execution not yet implemented — use paper mode only")
            return False

    def calculate_position_size(self, signal: StrategyResult) -> int:
        """
        Position size for funding arb is fixed at 1 unit (= 1 spot + 1 perp pair).
        Dollar size is set by POSITION_SIZE_USD in config.
        """
        if not signal.signal:
            return 0
        return 1

    # -----------------------------------------------------------------------
    # Reporting
    # -----------------------------------------------------------------------

    def print_rate_table(self, snapshots: List[FundingSnapshot]) -> None:
        """Print a clean summary table of all current funding rates."""
        if not snapshots:
            print("No funding rate data available.")
            return

        print("\n" + "=" * 75)
        print(f"{'CRYPTO FUNDING RATE MONITOR':^75}")
        print(f"{'Updated: ' + datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'):^75}")
        print("=" * 75)
        print(f"{'Symbol':<8} {'Exchange':<12} {'Rate/8h':>9} {'Annual':>9} {'Net':>9} {'Basis':>8} {'Spot':>12} {'Status'}")
        print("-" * 75)

        for snap in sorted(snapshots, key=lambda s: s.net_annual_yield, reverse=True):
            status = "✓ OPEN" if snap.is_profitable else "  wait"
            neg = "-" if snap.funding_rate < 0 else " "
            print(
                f"{snap.symbol:<8} {snap.exchange:<12} "
                f"{neg}{abs(snap.funding_rate):.4%}  "
                f"{snap.annualized_rate:>8.1%} "
                f"{snap.net_annual_yield:>8.1%} "
                f"{snap.basis_pct:>7.3%} "
                f"${snap.spot_price:>10,.0f}  "
                f"{status}"
            )

        print("-" * 75)
        print(f"  Threshold: {self.MIN_NET_YIELD:.0%} net annualized | "
              f"Max basis: {self.MAX_BASIS_PCT:.1%} | "
              f"Open positions: {len(self.open_positions)}/{self.MAX_POSITIONS}")
        print("=" * 75 + "\n")

    def print_open_positions(self) -> None:
        """Print current open arb positions."""
        if not self.open_positions:
            print("No open arb positions.\n")
            return

        print("\n" + "=" * 60)
        print(f"{'OPEN ARB POSITIONS':^60}")
        print("=" * 60)
        for key, pos in self.open_positions.items():
            age = (datetime.now(timezone.utc) - pos.timestamp).total_seconds() / 3600
            est_earned = pos.net_annual_yield * pos.recommended_notional_usd * (age / 8760)
            print(
                f"  {pos.symbol} / {pos.exchange} | "
                f"yield={pos.net_annual_yield:.1%} | "
                f"notional=${pos.recommended_notional_usd:,.0f} | "
                f"age={age:.1f}h | "
                f"est. earned=${est_earned:.2f}"
            )
        print("=" * 60 + "\n")
