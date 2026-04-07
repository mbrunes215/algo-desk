"""
Kill Switch

Implements emergency shutdown logic with configurable risk limits.
Monitors daily P&L, position sizes, and trade frequency.
Triggers automatic position closure when limits are breached.
"""

import logging
import asyncio
from dataclasses import dataclass
from typing import Dict, Optional, List
from datetime import datetime, timedelta
from enum import Enum


logger = logging.getLogger(__name__)


class ShutdownReason(Enum):
    """Enumeration of shutdown trigger reasons."""

    DAILY_LOSS_EXCEEDED = "Daily loss limit exceeded"
    MAX_POSITION_SIZE_EXCEEDED = "Max position size exceeded"
    MAX_TRADES_EXCEEDED = "Max trades per day exceeded"
    HEARTBEAT_TIMEOUT = "Heartbeat timeout"
    MANUAL_TRIGGER = "Manual trigger"
    UNKNOWN = "Unknown"


@dataclass
class ShutdownEvent:
    """Record of a shutdown event."""

    timestamp: datetime
    reason: ShutdownReason
    message: str
    positions_affected: int = 0


class KillSwitch:
    """
    Emergency kill switch for risk management.

    Monitors trading activity against configurable limits and triggers
    automatic position closure when thresholds are breached.
    """

    def __init__(
        self,
        max_daily_loss: float = 5000.0,
        max_position_size: float = 50000.0,
        max_trades_per_day: int = 100,
        heartbeat_timeout_seconds: int = 300,
    ):
        """
        Initialize kill switch.

        Args:
            max_daily_loss: Maximum allowed daily loss in dollars
            max_position_size: Maximum position size in dollars
            max_trades_per_day: Maximum trades allowed per day
            heartbeat_timeout_seconds: Heartbeat timeout in seconds
        """
        self.max_daily_loss = max_daily_loss
        self.max_position_size = max_position_size
        self.max_trades_per_day = max_trades_per_day
        self.heartbeat_timeout_seconds = heartbeat_timeout_seconds

        self.active = False
        self.armed = True
        self.triggered = False
        self.shutdown_reason: Optional[ShutdownReason] = None
        self.shutdown_timestamp: Optional[datetime] = None

        # Daily counters (reset daily)
        self.daily_loss = 0.0
        self.daily_trade_count = 0
        self.last_reset_date = datetime.utcnow().date()

        # Heartbeat monitoring
        self.last_heartbeat = datetime.utcnow()
        self.heartbeat_task: Optional[asyncio.Task] = None

        # Shutdown history
        self.shutdown_events: List[ShutdownEvent] = []

        # Callbacks for position closure
        self.on_shutdown_callback: Optional[callable] = None

        logger.info(
            f"KillSwitch initialized: max_daily_loss=${max_daily_loss:.2f}, "
            f"max_position_size=${max_position_size:.2f}, "
            f"max_trades={max_trades_per_day}, "
            f"heartbeat_timeout={heartbeat_timeout_seconds}s"
        )

    def arm(self) -> None:
        """Arm the kill switch."""
        self.armed = True
        logger.info("Kill switch armed")

    def disarm(self) -> None:
        """Disarm the kill switch (manual override)."""
        self.armed = False
        logger.warning("Kill switch disarmed")

    def is_armed(self) -> bool:
        """Check if kill switch is armed."""
        return self.armed

    def is_triggered(self) -> bool:
        """Check if kill switch has been triggered."""
        return self.triggered

    def is_active(self) -> bool:
        """Check if kill switch is currently active."""
        return self.active

    def heartbeat(self) -> None:
        """
        Signal a heartbeat to prevent timeout shutdown.

        Call this regularly (e.g., every iteration of main loop).
        """
        self.last_heartbeat = datetime.utcnow()

    async def check_limits(
        self,
        daily_pnl: float,
        positions: Dict[str, float],
        current_trade_count: int,
    ) -> Dict[str, bool]:
        """
        Check if any risk limits have been breached.

        Args:
            daily_pnl: Daily P&L (negative = loss)
            positions: Dict mapping position_id to notional value
            current_trade_count: Number of trades today

        Returns:
            Dict with keys: loss_limit_ok, position_size_ok, trade_count_ok,
            heartbeat_ok, all_ok
        """
        self._check_daily_reset()

        checks = {
            "loss_limit_ok": abs(daily_pnl) <= self.max_daily_loss,
            "position_size_ok": all(
                abs(val) <= self.max_position_size for val in positions.values()
            ),
            "trade_count_ok": current_trade_count <= self.max_trades_per_day,
            "heartbeat_ok": self._check_heartbeat(),
        }

        checks["all_ok"] = all(checks.values())

        if not checks["all_ok"]:
            # Determine which limit failed
            if not checks["loss_limit_ok"]:
                await self.trigger_shutdown(
                    ShutdownReason.DAILY_LOSS_EXCEEDED,
                    f"Daily loss ${abs(daily_pnl):.2f} exceeds limit ${self.max_daily_loss:.2f}",
                )
            elif not checks["position_size_ok"]:
                oversized = [
                    (pid, val)
                    for pid, val in positions.items()
                    if abs(val) > self.max_position_size
                ]
                await self.trigger_shutdown(
                    ShutdownReason.MAX_POSITION_SIZE_EXCEEDED,
                    f"Position(s) {oversized} exceed max size ${self.max_position_size:.2f}",
                )
            elif not checks["trade_count_ok"]:
                await self.trigger_shutdown(
                    ShutdownReason.MAX_TRADES_EXCEEDED,
                    f"Trade count {current_trade_count} exceeds limit {self.max_trades_per_day}",
                )
            elif not checks["heartbeat_ok"]:
                await self.trigger_shutdown(
                    ShutdownReason.HEARTBEAT_TIMEOUT,
                    f"No heartbeat for {self.heartbeat_timeout_seconds}s",
                )

        return checks

    def _check_daily_reset(self) -> None:
        """Reset daily counters if date has changed."""
        today = datetime.utcnow().date()

        if today != self.last_reset_date:
            self.reset_daily_counters()

    def _check_heartbeat(self) -> bool:
        """Check if heartbeat is still active."""
        elapsed = (datetime.utcnow() - self.last_heartbeat).total_seconds()
        return elapsed <= self.heartbeat_timeout_seconds

    def record_trade(self) -> None:
        """Record a trade (increments daily trade counter)."""
        self.daily_trade_count += 1

    def record_loss(self, loss_amount: float) -> None:
        """
        Record a loss against daily limit.

        Args:
            loss_amount: Loss amount (positive value)
        """
        self.daily_loss += loss_amount
        logger.debug(f"Daily loss recorded: ${loss_amount:.2f}, total: ${self.daily_loss:.2f}")

    def reset_daily_counters(self) -> None:
        """Reset daily counters (called automatically at midnight)."""
        self.daily_loss = 0.0
        self.daily_trade_count = 0
        self.last_reset_date = datetime.utcnow().date()

        logger.info(
            f"Daily counters reset for {self.last_reset_date}"
        )

    async def trigger_shutdown(
        self,
        reason: ShutdownReason,
        message: str,
        positions_affected: int = 0,
    ) -> bool:
        """
        Trigger emergency shutdown.

        Args:
            reason: Shutdown reason
            message: Detailed message
            positions_affected: Number of positions to close

        Returns:
            True if shutdown was triggered, False if disabled/not armed
        """
        if not self.armed:
            logger.warning(
                f"Kill switch not armed, ignoring trigger: {reason.value}"
            )
            return False

        if self.triggered:
            logger.warning("Kill switch already triggered, ignoring duplicate trigger")
            return False

        self.triggered = True
        self.active = True
        self.shutdown_reason = reason
        self.shutdown_timestamp = datetime.utcnow()

        # Log event
        event = ShutdownEvent(
            timestamp=self.shutdown_timestamp,
            reason=reason,
            message=message,
            positions_affected=positions_affected,
        )
        self.shutdown_events.append(event)

        logger.error(
            f"KILL SWITCH TRIGGERED: {reason.value} | {message}"
        )

        # Execute shutdown callback if set
        if self.on_shutdown_callback:
            try:
                if asyncio.iscoroutinefunction(self.on_shutdown_callback):
                    await self.on_shutdown_callback(reason, message)
                else:
                    self.on_shutdown_callback(reason, message)
            except Exception as e:
                logger.error(f"Error in shutdown callback: {e}")

        return True

    def reset(self) -> None:
        """Reset kill switch to armed state (allows trading to resume)."""
        if not self.armed:
            logger.warning("Cannot reset disarmed kill switch")
            return

        self.triggered = False
        self.active = False
        self.shutdown_reason = None
        self.shutdown_timestamp = None
        self.daily_loss = 0.0
        self.daily_trade_count = 0

        logger.info("Kill switch reset - trading resumed")

    def set_shutdown_callback(self, callback: callable) -> None:
        """
        Set callback function to execute on shutdown.

        The callback will be called with (reason, message) arguments.

        Args:
            callback: Async or sync callable
        """
        self.on_shutdown_callback = callback
        logger.debug("Shutdown callback set")

    def get_status(self) -> Dict[str, any]:
        """
        Get current kill switch status.

        Returns:
            Dict with status information
        """
        return {
            "armed": self.armed,
            "triggered": self.triggered,
            "active": self.active,
            "shutdown_reason": self.shutdown_reason.value if self.shutdown_reason else None,
            "shutdown_timestamp": self.shutdown_timestamp,
            "daily_loss": self.daily_loss,
            "max_daily_loss": self.max_daily_loss,
            "daily_loss_remaining": self.max_daily_loss - self.daily_loss,
            "daily_trade_count": self.daily_trade_count,
            "max_trades_per_day": self.max_trades_per_day,
            "heartbeat_healthy": self._check_heartbeat(),
            "last_heartbeat": self.last_heartbeat,
            "last_reset_date": self.last_reset_date,
        }

    def get_shutdown_events(self) -> List[ShutdownEvent]:
        """
        Get history of shutdown events.

        Returns:
            List of ShutdownEvent objects
        """
        return self.shutdown_events.copy()
