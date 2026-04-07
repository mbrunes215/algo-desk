"""
Daily Limits Enforcer

Pre-trade risk checks to ensure proposed trades don't violate risk limits.
Tracks daily statistics and enforces position concentration limits.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum


logger = logging.getLogger(__name__)


class LimitViolationType(Enum):
    """Enumeration of limit violation types."""

    DAILY_LOSS = "Daily loss limit would be exceeded"
    POSITION_CONCENTRATION = "Position concentration limit exceeded"
    TRADE_NOTIONAL = "Trade notional exceeds max"
    TRADE_FREQUENCY = "Trade frequency limit exceeded"
    TOTAL_EXPOSURE = "Total exposure limit exceeded"
    NONE = "No violation"


@dataclass
class TradeRecord:
    """Record of a single executed trade."""

    timestamp: datetime
    symbol: str
    side: str
    quantity: int
    price: float
    notional: float


@dataclass
class DailyStats:
    """Daily trading statistics."""

    realized_pnl: float
    unrealized_pnl: float
    total_pnl: float
    trades_executed: int
    trades_cancelled: int
    max_concentration: float
    gross_notional: float
    net_notional: float
    trades: List[TradeRecord] = field(default_factory=list)


class DailyLimits:
    """
    Enforces pre-trade risk limits to prevent excessive losses or exposure.

    Checks:
    - Daily P&L loss limit
    - Position concentration limits
    - Maximum notional per trade
    - Trade frequency limits
    - Total portfolio exposure limits
    """

    def __init__(
        self,
        daily_loss_limit: float = 5000.0,
        max_notional_per_trade: float = 100000.0,
        max_concentration_pct: float = 20.0,
        max_trades_per_day: int = 100,
        max_gross_exposure_pct: float = 500.0,
        portfolio_value: float = 1000000.0,
    ):
        """
        Initialize daily limits enforcer.

        Args:
            daily_loss_limit: Max daily loss in dollars
            max_notional_per_trade: Max notional per single trade
            max_concentration_pct: Max position concentration as % of portfolio
            max_trades_per_day: Max trades per day
            max_gross_exposure_pct: Max gross exposure as % of portfolio
            portfolio_value: Current portfolio value for % calculations
        """
        self.daily_loss_limit = daily_loss_limit
        self.max_notional_per_trade = max_notional_per_trade
        self.max_concentration_pct = max_concentration_pct
        self.max_trades_per_day = max_trades_per_day
        self.max_gross_exposure_pct = max_gross_exposure_pct
        self.portfolio_value = portfolio_value

        # Daily tracking
        self.current_date = datetime.utcnow().date()
        self.trades_today: List[TradeRecord] = []
        self.realized_pnl_today = 0.0
        self.unrealized_pnl_today = 0.0

        # Position tracking
        self.current_positions: Dict[str, Dict] = {}

        logger.info(
            f"DailyLimits initialized: loss_limit=${daily_loss_limit:.2f}, "
            f"max_notional=${max_notional_per_trade:.2f}, "
            f"max_concentration={max_concentration_pct}%, "
            f"max_trades={max_trades_per_day}"
        )

    async def can_trade(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        current_positions: Optional[Dict[str, float]] = None,
    ) -> Tuple[bool, LimitViolationType, str]:
        """
        Pre-trade check to determine if trade is allowed.

        Args:
            symbol: Asset symbol
            side: 'BUY' or 'SELL'
            quantity: Number of shares
            price: Proposed price
            current_positions: Current position sizes by symbol (notional)

        Returns:
            Tuple (allowed, violation_type, reason_message)
        """
        self._check_daily_reset()

        if current_positions:
            self.current_positions = current_positions

        notional = quantity * price

        # Check 1: Daily loss limit
        total_pnl_today = self.realized_pnl_today + self.unrealized_pnl_today
        if total_pnl_today < -self.daily_loss_limit:
            return (
                False,
                LimitViolationType.DAILY_LOSS,
                f"Daily loss ${abs(total_pnl_today):.2f} already exceeds limit "
                f"${self.daily_loss_limit:.2f}",
            )

        # Check 2: Max notional per trade
        if notional > self.max_notional_per_trade:
            return (
                False,
                LimitViolationType.TRADE_NOTIONAL,
                f"Trade notional ${notional:.2f} exceeds max "
                f"${self.max_notional_per_trade:.2f}",
            )

        # Check 3: Position concentration
        post_trade_position = self.current_positions.get(symbol, 0)
        if side.upper() == "BUY":
            post_trade_position += notional
        else:
            post_trade_position -= notional

        concentration_pct = (abs(post_trade_position) / self.portfolio_value) * 100
        if concentration_pct > self.max_concentration_pct:
            return (
                False,
                LimitViolationType.POSITION_CONCENTRATION,
                f"Post-trade concentration {concentration_pct:.2f}% exceeds max "
                f"{self.max_concentration_pct:.2f}%",
            )

        # Check 4: Trade frequency
        if len(self.trades_today) >= self.max_trades_per_day:
            return (
                False,
                LimitViolationType.TRADE_FREQUENCY,
                f"Already executed {len(self.trades_today)} trades, "
                f"exceeds limit {self.max_trades_per_day}",
            )

        # Check 5: Total gross exposure
        gross_exposure = self._calculate_gross_exposure(post_trade_position)
        gross_exposure_pct = (gross_exposure / self.portfolio_value) * 100
        if gross_exposure_pct > self.max_gross_exposure_pct:
            return (
                False,
                LimitViolationType.TOTAL_EXPOSURE,
                f"Gross exposure {gross_exposure_pct:.2f}% exceeds max "
                f"{self.max_gross_exposure_pct:.2f}%",
            )

        logger.debug(
            f"Trade allowed: {symbol} {side} x{quantity} @ ${price:.4f} "
            f"(concentration: {concentration_pct:.2f}%)"
        )

        return True, LimitViolationType.NONE, "Trade allowed"

    def record_trade(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        realized_pnl: float = 0.0,
    ) -> None:
        """
        Record an executed trade.

        Args:
            symbol: Asset symbol
            side: 'BUY' or 'SELL'
            quantity: Number of shares
            price: Execution price
            realized_pnl: Realized P&L from trade (if closing position)
        """
        self._check_daily_reset()

        notional = quantity * price
        trade = TradeRecord(
            timestamp=datetime.utcnow(),
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            notional=notional,
        )

        self.trades_today.append(trade)
        self.realized_pnl_today += realized_pnl

        logger.info(
            f"Trade recorded: {symbol} {side} x{quantity} @ ${price:.4f} "
            f"(realized_pnl: ${realized_pnl:.2f})"
        )

    def update_unrealized_pnl(self, unrealized_pnl: float) -> None:
        """
        Update current unrealized P&L.

        Args:
            unrealized_pnl: Current unrealized P&L from open positions
        """
        self.unrealized_pnl_today = unrealized_pnl
        logger.debug(f"Unrealized P&L updated: ${unrealized_pnl:.2f}")

    def get_daily_stats(self) -> DailyStats:
        """
        Get daily trading statistics.

        Returns:
            DailyStats object with current day's metrics
        """
        self._check_daily_reset()

        total_pnl = self.realized_pnl_today + self.unrealized_pnl_today

        # Calculate max concentration
        max_concentration = 0.0
        if self.current_positions:
            max_concentration = max(
                (abs(pos) / self.portfolio_value) * 100
                for pos in self.current_positions.values()
            )

        # Calculate exposure
        gross_notional = sum(abs(pos) for pos in self.current_positions.values())
        net_notional = sum(self.current_positions.values())

        return DailyStats(
            realized_pnl=self.realized_pnl_today,
            unrealized_pnl=self.unrealized_pnl_today,
            total_pnl=total_pnl,
            trades_executed=len(self.trades_today),
            trades_cancelled=0,
            max_concentration=max_concentration,
            gross_notional=gross_notional,
            net_notional=net_notional,
            trades=self.trades_today.copy(),
        )

    def reset(self) -> None:
        """Reset daily limits (called at end of day)."""
        self.trades_today.clear()
        self.realized_pnl_today = 0.0
        self.unrealized_pnl_today = 0.0
        self.current_date = datetime.utcnow().date()

        logger.info(f"Daily limits reset for {self.current_date}")

    def _check_daily_reset(self) -> None:
        """Reset if date has changed."""
        today = datetime.utcnow().date()

        if today != self.current_date:
            self.reset()

    def _calculate_gross_exposure(
        self, additional_position_notional: float = 0.0
    ) -> float:
        """
        Calculate total gross exposure.

        Args:
            additional_position_notional: Additional position to include

        Returns:
            Gross exposure in dollars
        """
        gross = sum(abs(pos) for pos in self.current_positions.values())
        gross += abs(additional_position_notional)
        return gross

    def update_portfolio_value(self, new_value: float) -> None:
        """
        Update portfolio value (for concentration % calculations).

        Args:
            new_value: New portfolio value
        """
        old_value = self.portfolio_value
        self.portfolio_value = new_value

        logger.debug(f"Portfolio value updated: ${old_value:.2f} -> ${new_value:.2f}")

    def get_available_notional(self) -> float:
        """
        Calculate remaining available notional for trading.

        Returns:
            Available notional in dollars
        """
        max_gross = (self.max_gross_exposure_pct / 100.0) * self.portfolio_value
        current_gross = self._calculate_gross_exposure()

        return max(0, max_gross - current_gross)

    def get_daily_loss_headroom(self) -> float:
        """
        Calculate remaining loss headroom for the day.

        Returns:
            Remaining loss allowance in dollars
        """
        total_pnl = self.realized_pnl_today + self.unrealized_pnl_today
        remaining = self.daily_loss_limit - abs(total_pnl)

        return max(0, remaining)

    def to_dict(self) -> Dict:
        """
        Export daily limits configuration to dict.

        Returns:
            Dict representation of limits
        """
        return {
            "daily_loss_limit": self.daily_loss_limit,
            "max_notional_per_trade": self.max_notional_per_trade,
            "max_concentration_pct": self.max_concentration_pct,
            "max_trades_per_day": self.max_trades_per_day,
            "max_gross_exposure_pct": self.max_gross_exposure_pct,
            "portfolio_value": self.portfolio_value,
        }
