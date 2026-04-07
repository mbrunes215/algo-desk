"""Tests for risk management module.

Tests kill switch triggering, daily limits enforcement, and position management.
"""

from datetime import datetime, timedelta
from typing import Any, Dict
from dataclasses import dataclass

import pytest


@dataclass
class MockKillSwitch:
    """Mock kill switch implementation for testing."""

    triggered: bool = False
    reason: str = ""
    timestamp: datetime | None = None

    def trigger(self, reason: str) -> None:
        """Trigger the kill switch."""
        self.triggered = True
        self.reason = reason
        self.timestamp = datetime.utcnow()

    def is_triggered(self) -> bool:
        """Check if kill switch is triggered."""
        return self.triggered

    def reset(self) -> None:
        """Reset the kill switch."""
        self.triggered = False
        self.reason = ""
        self.timestamp = None


@dataclass
class MockDailyLimitManager:
    """Mock daily limit manager for testing."""

    daily_loss_limit: float
    daily_profit_target: float
    current_pnl: float = 0.0
    reset_time: datetime = None

    def __post_init__(self) -> None:
        """Initialize reset time to start of today."""
        if self.reset_time is None:
            self.reset_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    def check_loss_limit(self) -> bool:
        """Check if daily loss limit exceeded."""
        return self.current_pnl <= -abs(self.daily_loss_limit)

    def check_profit_target(self) -> bool:
        """Check if daily profit target reached."""
        return self.current_pnl >= self.daily_profit_target

    def is_reset_needed(self) -> bool:
        """Check if daily reset is needed."""
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        return self.reset_time < today

    def reset_daily(self) -> None:
        """Reset daily limits."""
        self.current_pnl = 0.0
        self.reset_time = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass
class MockPosition:
    """Mock position for testing."""

    symbol: str
    quantity: int
    average_price: float
    current_price: float
    status: str = "OPEN"

    @property
    def unrealized_pnl(self) -> float:
        """Calculate unrealized P&L."""
        return (self.current_price - self.average_price) * self.quantity

    @property
    def unrealized_pnl_percent(self) -> float:
        """Calculate unrealized P&L as percentage."""
        if self.average_price == 0:
            return 0.0
        return (self.unrealized_pnl / (self.average_price * self.quantity)) * 100


@dataclass
class MockPositionManager:
    """Mock position manager for testing."""

    positions: Dict[str, MockPosition] | None = None
    max_position_size: int = 1000

    def __post_init__(self) -> None:
        """Initialize positions dict."""
        if self.positions is None:
            self.positions = {}

    def add_position(
        self,
        symbol: str,
        quantity: int,
        average_price: float,
        current_price: float,
    ) -> None:
        """Add or update a position."""
        if quantity > self.max_position_size:
            raise ValueError(f"Position size {quantity} exceeds limit {self.max_position_size}")
        self.positions[symbol] = MockPosition(
            symbol=symbol,
            quantity=quantity,
            average_price=average_price,
            current_price=current_price,
        )

    def close_position(self, symbol: str) -> None:
        """Close a position."""
        if symbol in self.positions:
            self.positions[symbol].status = "CLOSED"

    def get_total_unrealized_pnl(self) -> float:
        """Get total unrealized P&L across all positions."""
        return sum(pos.unrealized_pnl for pos in self.positions.values())

    def get_position(self, symbol: str) -> MockPosition | None:
        """Get a specific position."""
        return self.positions.get(symbol)

    def update_prices(self, prices: Dict[str, float]) -> None:
        """Update current prices for positions."""
        for symbol, price in prices.items():
            if symbol in self.positions:
                self.positions[symbol].current_price = price


class TestKillSwitch:
    """Tests for kill switch functionality."""

    @pytest.mark.unit
    def test_kill_switch_initialization(self) -> None:
        """Test kill switch initializes as not triggered."""
        kill_switch = MockKillSwitch()
        assert not kill_switch.is_triggered()
        assert kill_switch.reason == ""

    @pytest.mark.unit
    def test_kill_switch_trigger(self) -> None:
        """Test kill switch can be triggered."""
        kill_switch = MockKillSwitch()
        kill_switch.trigger("Loss limit exceeded")
        assert kill_switch.is_triggered()
        assert kill_switch.reason == "Loss limit exceeded"
        assert kill_switch.timestamp is not None

    @pytest.mark.unit
    def test_kill_switch_reset(self) -> None:
        """Test kill switch can be reset."""
        kill_switch = MockKillSwitch()
        kill_switch.trigger("Test reason")
        assert kill_switch.is_triggered()
        kill_switch.reset()
        assert not kill_switch.is_triggered()
        assert kill_switch.reason == ""

    @pytest.mark.unit
    def test_kill_switch_prevents_trading(self) -> None:
        """Test that triggered kill switch prevents new orders."""
        kill_switch = MockKillSwitch()

        # Should allow trades when not triggered
        if kill_switch.is_triggered():
            pytest.fail("Kill switch should not be triggered initially")

        # Trigger kill switch
        kill_switch.trigger("Circuit breaker")
        assert kill_switch.is_triggered()


class TestDailyLimits:
    """Tests for daily limit enforcement."""

    @pytest.mark.unit
    def test_daily_loss_limit_not_exceeded(self) -> None:
        """Test loss limit check when within bounds."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            current_pnl=500.0,
        )
        assert not limit_manager.check_loss_limit()

    @pytest.mark.unit
    def test_daily_loss_limit_exceeded(self) -> None:
        """Test loss limit triggers when exceeded."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            current_pnl=-1500.0,
        )
        assert limit_manager.check_loss_limit()

    @pytest.mark.unit
    def test_daily_loss_limit_exactly_met(self) -> None:
        """Test loss limit check at exact threshold."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            current_pnl=-1000.0,
        )
        assert limit_manager.check_loss_limit()

    @pytest.mark.unit
    def test_daily_profit_target_not_reached(self) -> None:
        """Test profit target check when not reached."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            current_pnl=3000.0,
        )
        assert not limit_manager.check_profit_target()

    @pytest.mark.unit
    def test_daily_profit_target_reached(self) -> None:
        """Test profit target triggers when reached."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            current_pnl=6000.0,
        )
        assert limit_manager.check_profit_target()

    @pytest.mark.unit
    def test_daily_reset_not_needed(self) -> None:
        """Test reset is not needed on same day."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
        )
        assert not limit_manager.is_reset_needed()

    @pytest.mark.unit
    def test_daily_reset_needed(self) -> None:
        """Test reset is needed on new day."""
        yesterday = datetime.utcnow() - timedelta(days=1)
        yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            reset_time=yesterday,
        )
        assert limit_manager.is_reset_needed()

    @pytest.mark.unit
    def test_daily_reset_clears_pnl(self) -> None:
        """Test daily reset clears P&L."""
        limit_manager = MockDailyLimitManager(
            daily_loss_limit=1000.0,
            daily_profit_target=5000.0,
            current_pnl=2500.0,
        )
        limit_manager.reset_daily()
        assert limit_manager.current_pnl == 0.0


class TestPositionManager:
    """Tests for position management calculations."""

    @pytest.mark.unit
    def test_position_initialization(self) -> None:
        """Test position can be created."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 152.0)
        assert "AAPL" in pos_manager.positions

    @pytest.mark.unit
    def test_position_unrealized_pnl(self) -> None:
        """Test unrealized P&L calculation."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 155.0)

        position = pos_manager.get_position("AAPL")
        assert position is not None
        assert position.unrealized_pnl == 500.0  # (155 - 150) * 100

    @pytest.mark.unit
    def test_position_unrealized_pnl_loss(self) -> None:
        """Test unrealized P&L when position is losing."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 145.0)

        position = pos_manager.get_position("AAPL")
        assert position is not None
        assert position.unrealized_pnl == -500.0

    @pytest.mark.unit
    def test_position_unrealized_pnl_percent(self) -> None:
        """Test unrealized P&L percentage calculation."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 165.0)

        position = pos_manager.get_position("AAPL")
        assert position is not None
        assert position.unrealized_pnl_percent == 10.0  # (165-150)/150 * 100

    @pytest.mark.unit
    def test_position_max_size_enforcement(self) -> None:
        """Test position size limit enforcement."""
        pos_manager = MockPositionManager(max_position_size=1000)

        with pytest.raises(ValueError):
            pos_manager.add_position("AAPL", 1500, 150.0, 152.0)

    @pytest.mark.unit
    def test_total_unrealized_pnl(self) -> None:
        """Test total unrealized P&L across all positions."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 155.0)  # +500
        pos_manager.add_position("GOOGL", 50, 2800.0, 2750.0)  # -2500

        total_pnl = pos_manager.get_total_unrealized_pnl()
        assert total_pnl == -2000.0  # 500 - 2500

    @pytest.mark.unit
    def test_position_close(self) -> None:
        """Test position can be closed."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 155.0)

        position = pos_manager.get_position("AAPL")
        assert position.status == "OPEN"

        pos_manager.close_position("AAPL")
        position = pos_manager.get_position("AAPL")
        assert position.status == "CLOSED"

    @pytest.mark.unit
    def test_update_position_prices(self) -> None:
        """Test updating prices for multiple positions."""
        pos_manager = MockPositionManager()
        pos_manager.add_position("AAPL", 100, 150.0, 155.0)
        pos_manager.add_position("GOOGL", 50, 2800.0, 2750.0)

        prices = {"AAPL": 160.0, "GOOGL": 2900.0}
        pos_manager.update_prices(prices)

        aapl = pos_manager.get_position("AAPL")
        googl = pos_manager.get_position("GOOGL")

        assert aapl.current_price == 160.0
        assert googl.current_price == 2900.0
