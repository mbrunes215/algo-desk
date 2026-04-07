"""
Risk Management Module

Provides risk management tools:
- PositionManager: Position tracking across platforms
- KillSwitch: Emergency shutdown with configurable limits
- DailyLimits: Pre-trade risk checks
"""

try:
    from .position_manager import (
        PositionManager,
        Position,
        PositionStatus,
        PositionModel,
    )
except ImportError:
    PositionManager = None  # sqlalchemy not installed
    Position = None
    PositionStatus = None
    PositionModel = None
from .kill_switch import (
    KillSwitch,
    ShutdownReason,
    ShutdownEvent,
)
from .daily_limits import (
    DailyLimits,
    DailyStats,
    LimitViolationType,
    TradeRecord,
)

__all__ = [
    # Position Manager
    "PositionManager",
    "Position",
    "PositionStatus",
    "PositionModel",
    # Kill Switch
    "KillSwitch",
    "ShutdownReason",
    "ShutdownEvent",
    # Daily Limits
    "DailyLimits",
    "DailyStats",
    "LimitViolationType",
    "TradeRecord",
]
