"""
Execution Module

Provides multiple order executors for different trading platforms:
- IBKRExecutor: Interactive Brokers executor
- KalshiExecutor: Kalshi API executor
- PaperExecutor: Paper trading simulator
"""

try:
    from .ibkr_executor import (
        IBKRExecutor,
        OrderResult as IBKROrderResult,
        Position as IBKRPosition,
        MarketDataSnapshot,
    )
except ImportError:
    IBKRExecutor = None  # ib_insync not installed — IBKR disabled
    IBKROrderResult = None
    IBKRPosition = None
    MarketDataSnapshot = None
from .kalshi_executor import (
    KalshiExecutor,
    KalshiOrderResult,
    KalshiPosition,
    KalshiMarket,
)
from .paper_executor import (
    PaperExecutor,
    PaperOrderResult,
    PaperPosition,
    PaperTrade,
    OrderStatus,
)

__all__ = [
    # IBKR
    "IBKRExecutor",
    "IBKROrderResult",
    "IBKRPosition",
    "MarketDataSnapshot",
    # Kalshi
    "KalshiExecutor",
    "KalshiOrderResult",
    "KalshiPosition",
    "KalshiMarket",
    # Paper
    "PaperExecutor",
    "PaperOrderResult",
    "PaperPosition",
    "PaperTrade",
    "OrderStatus",
]
