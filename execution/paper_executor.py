"""
Paper Trading Executor

Simulates order execution for backtesting and paper trading.
Tracks virtual positions and P&L with configurable slippage.
"""

import logging
import csv
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from enum import Enum
import uuid


logger = logging.getLogger(__name__)


class OrderStatus(Enum):
    """Order status enumeration."""

    PENDING = "PENDING"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


@dataclass
class PaperTrade:
    """Represents a single trade execution."""

    trade_id: str
    symbol: str
    side: str  # 'BUY' or 'SELL'
    quantity: int
    entry_price: float
    executed_price: float
    timestamp: datetime
    slippage: float
    commission: float = 0.0
    pnl: float = 0.0


@dataclass
class PaperPosition:
    """Paper trading position."""

    symbol: str
    side: str
    quantity: int
    entry_price: float
    current_price: float
    entry_timestamp: datetime
    trades: List[PaperTrade] = field(default_factory=list)

    @property
    def market_value(self) -> float:
        """Calculate current market value."""
        return self.quantity * self.current_price

    @property
    def cost_basis(self) -> float:
        """Calculate cost basis."""
        return self.quantity * self.entry_price

    @property
    def unrealized_pnl(self) -> float:
        """Calculate unrealized P&L."""
        if self.side == "BUY":
            return self.market_value - self.cost_basis
        else:  # SELL
            return self.cost_basis - self.market_value

    @property
    def unrealized_pnl_percent(self) -> float:
        """Calculate unrealized P&L percentage."""
        if self.cost_basis == 0:
            return 0.0
        return (self.unrealized_pnl / abs(self.cost_basis)) * 100


@dataclass
class PaperOrderResult:
    """Result of a paper order execution."""

    success: bool
    order_id: str
    symbol: str
    side: str
    quantity: int
    executed_price: float
    status: OrderStatus
    message: str = ""
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class PaperExecutor:
    """
    Paper trading executor for backtesting and simulation.

    Simulates order execution with realistic slippage and position tracking.
    Maintains virtual positions and calculates P&L.
    """

    def __init__(
        self,
        initial_capital: float = 100000.0,
        slippage_bps: float = 2.0,
        commission_per_trade: float = 1.0,
    ):
        """
        Initialize paper executor.

        Args:
            initial_capital: Starting capital in dollars
            slippage_bps: Slippage in basis points (default: 2 bps)
            commission_per_trade: Commission per trade in dollars
        """
        self.initial_capital = initial_capital
        self.current_cash = initial_capital
        self.slippage_bps = slippage_bps
        self.commission_per_trade = commission_per_trade

        self.positions: Dict[str, PaperPosition] = {}
        self.trade_log: List[PaperTrade] = []
        self.order_history: List[PaperOrderResult] = []
        self.market_prices: Dict[str, float] = {}

        logger.info(
            f"PaperExecutor initialized: capital=${initial_capital:.2f}, "
            f"slippage={slippage_bps}bps, commission=${commission_per_trade} "
            f"[LIMIT orders: no slippage, $0 commission — simulating maker fills]"
        )

    def set_market_price(self, symbol: str, price: float) -> None:
        """
        Set current market price for a symbol.

        Args:
            symbol: Asset symbol
            price: Current market price
        """
        self.market_prices[symbol] = price

        # Update current price in all positions
        if symbol in self.positions:
            self.positions[symbol].current_price = price

        logger.debug(f"{symbol} price set to ${price:.4f}")

    def _calculate_slipped_price(self, symbol: str, side: str, price: float) -> float:
        """
        Calculate execution price with slippage.

        Args:
            symbol: Asset symbol
            side: 'BUY' or 'SELL'
            price: Reference price

        Returns:
            Executed price after slippage
        """
        slippage = price * (self.slippage_bps / 10000.0)

        if side.upper() == "BUY":
            # BUY slips upward
            return price + slippage
        else:
            # SELL slips downward
            return price - slippage

    async def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float,
        order_type: str = "MARKET",
    ) -> PaperOrderResult:
        """
        Place a paper trading order.

        Args:
            symbol: Asset symbol
            side: 'BUY' or 'SELL'
            quantity: Number of shares
            price: Reference price (used for limit orders)
            order_type: 'MARKET' or 'LIMIT'

        Returns:
            PaperOrderResult with execution details
        """
        order_id = str(uuid.uuid4())

        if quantity <= 0:
            return PaperOrderResult(
                success=False,
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                executed_price=0.0,
                status=OrderStatus.REJECTED,
                message="Quantity must be positive",
            )

        if side.upper() not in ["BUY", "SELL"]:
            return PaperOrderResult(
                success=False,
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                executed_price=0.0,
                status=OrderStatus.REJECTED,
                message="Side must be 'BUY' or 'SELL'",
            )

        # Normalise order type
        is_limit = order_type.upper() == "LIMIT"

        if is_limit:
            # ── LIMIT / MAKER ORDER ───────────────────────────────────────
            # Fill at the exact price the caller specified (their bid or ask).
            # No slippage: we are the passive/maker side.
            # No commission: Kalshi charges $0 for maker orders.
            # Reference price for position tracking = the limit price itself.
            if price <= 0:
                return PaperOrderResult(
                    success=False,
                    order_id=order_id,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    executed_price=0.0,
                    status=OrderStatus.REJECTED,
                    message="Limit price must be positive",
                )
            executed_price = price
            current_price = price  # use for position tracking
            effective_commission = 0.0
        else:
            # ── MARKET / TAKER ORDER ──────────────────────────────────────
            # Apply slippage (we cross the spread) and charge commission.
            current_price = self.market_prices.get(symbol)
            if current_price is None:
                return PaperOrderResult(
                    success=False,
                    order_id=order_id,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    executed_price=0.0,
                    status=OrderStatus.REJECTED,
                    message=f"No market price available for {symbol}",
                )
            executed_price = self._calculate_slipped_price(symbol, side, current_price)
            effective_commission = self.commission_per_trade

        # Check buying power for BUY orders
        if side.upper() == "BUY":
            cost = (quantity * executed_price) + effective_commission
            if cost > self.current_cash:
                return PaperOrderResult(
                    success=False,
                    order_id=order_id,
                    symbol=symbol,
                    side=side,
                    quantity=quantity,
                    executed_price=executed_price,
                    status=OrderStatus.REJECTED,
                    message=f"Insufficient buying power: need ${cost:.2f}, have ${self.current_cash:.2f}",
                )

        # Execute the order
        try:
            self._execute_order(
                order_id, symbol, side, quantity, executed_price, current_price,
                commission_override=effective_commission,
            )

            order_mode = "LIMIT/maker" if is_limit else "MARKET/taker"
            result = PaperOrderResult(
                success=True,
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                executed_price=executed_price,
                status=OrderStatus.FILLED,
                message=f"[{order_mode}] Order filled at ${executed_price:.4f}",
            )

            self.order_history.append(result)
            return result

        except Exception as e:
            logger.error(f"Failed to execute order: {e}")
            return PaperOrderResult(
                success=False,
                order_id=order_id,
                symbol=symbol,
                side=side,
                quantity=quantity,
                executed_price=executed_price,
                status=OrderStatus.REJECTED,
                message=str(e),
            )

    def _execute_order(
        self,
        order_id: str,
        symbol: str,
        side: str,
        quantity: int,
        executed_price: float,
        current_price: float,
        commission_override: Optional[float] = None,
    ) -> None:
        """
        Internal method to execute an order.

        Args:
            order_id: Unique order ID
            symbol: Asset symbol
            side: 'BUY' or 'SELL'
            quantity: Number of shares
            executed_price: Execution price
            current_price: Current market price (reference for slippage calc)
            commission_override: If provided, use this instead of self.commission_per_trade
                                 (pass 0.0 for maker/limit orders, which are free on Kalshi)
        """
        side = side.upper()
        commission = self.commission_per_trade if commission_override is None else commission_override

        # Update cash
        if side == "BUY":
            cash_impact = (quantity * executed_price) + commission
            self.current_cash -= cash_impact
        else:  # SELL
            cash_impact = (quantity * executed_price) - commission
            self.current_cash += cash_impact

        # Create trade record
        slippage = abs(executed_price - current_price)
        trade = PaperTrade(
            trade_id=order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            entry_price=current_price,
            executed_price=executed_price,
            timestamp=datetime.utcnow(),
            slippage=slippage,
            commission=commission,
        )

        self.trade_log.append(trade)

        # Update position
        if symbol in self.positions:
            pos = self.positions[symbol]

            if side == pos.side:
                # Add to existing position
                total_cost = (pos.quantity * pos.entry_price) + (
                    quantity * executed_price
                )
                pos.quantity += quantity
                pos.entry_price = total_cost / pos.quantity
            else:
                # Close or reverse position
                if quantity >= pos.quantity:
                    # Close entire position
                    pnl = self._calculate_close_pnl(pos, executed_price, quantity, commission)
                    trade.pnl = pnl
                    del self.positions[symbol]
                else:
                    # Partial close
                    pnl = self._calculate_partial_close_pnl(
                        pos, executed_price, quantity, commission
                    )
                    trade.pnl = pnl
                    pos.quantity -= quantity

            pos.current_price = current_price
        else:
            # New position
            self.positions[symbol] = PaperPosition(
                symbol=symbol,
                side=side,
                quantity=quantity,
                entry_price=executed_price,
                current_price=current_price,
                entry_timestamp=datetime.utcnow(),
                trades=[trade],
            )

        logger.info(
            f"Order executed: {order_id} | {symbol} {side} x{quantity} "
            f"@ ${executed_price:.4f}"
        )

    def _calculate_close_pnl(
        self, position: PaperPosition, close_price: float, quantity: int,
        commission: float = 0.0,
    ) -> float:
        """Calculate P&L when closing a position."""
        if position.side == "BUY":
            pnl = (close_price - position.entry_price) * quantity
        else:
            pnl = (position.entry_price - close_price) * quantity

        return pnl - commission

    def _calculate_partial_close_pnl(
        self, position: PaperPosition, close_price: float, quantity: int,
        commission: float = 0.0,
    ) -> float:
        """Calculate P&L when partially closing a position."""
        if position.side == "BUY":
            pnl = (close_price - position.entry_price) * quantity
        else:
            pnl = (position.entry_price - close_price) * quantity

        return pnl - commission

    async def cancel_order(self, order_id: str) -> bool:
        """
        Cancel a pending order (no-op in paper trading).

        Args:
            order_id: Order ID to cancel

        Returns:
            False (cannot cancel executed orders)
        """
        logger.warning(f"Cannot cancel executed order {order_id} in paper trading")
        return False

    async def get_positions(self) -> Dict[str, PaperPosition]:
        """
        Get all open positions.

        Returns:
            Dictionary mapping symbol to PaperPosition
        """
        return self.positions.copy()

    async def get_cash_balance(self) -> float:
        """
        Get current cash balance.

        Returns:
            Available cash in dollars
        """
        return self.current_cash

    async def get_total_portfolio_value(self) -> float:
        """
        Get total portfolio value (positions + cash).

        Returns:
            Total portfolio value in dollars
        """
        position_value = sum(
            pos.market_value for pos in self.positions.values()
        )
        return position_value + self.current_cash

    async def get_pnl(self) -> Tuple[float, float]:
        """
        Get total P&L (realized + unrealized).

        Returns:
            Tuple (realized_pnl, unrealized_pnl)
        """
        realized_pnl = sum(trade.pnl for trade in self.trade_log)

        unrealized_pnl = sum(
            pos.unrealized_pnl for pos in self.positions.values()
        )

        return realized_pnl, unrealized_pnl

    async def get_returns(self) -> float:
        """
        Get total return percentage.

        Returns:
            Return as percentage
        """
        portfolio_value = await self.get_total_portfolio_value()
        pnl = portfolio_value - self.initial_capital
        return (pnl / self.initial_capital) * 100

    def export_trades_to_csv(self, filepath: str) -> bool:
        """
        Export trade log to CSV file.

        Args:
            filepath: Path to write CSV file

        Returns:
            True if successful, False otherwise
        """
        if not self.trade_log:
            logger.warning("No trades to export")
            return False

        try:
            with open(filepath, "w", newline="") as csvfile:
                fieldnames = [
                    "trade_id",
                    "symbol",
                    "side",
                    "quantity",
                    "entry_price",
                    "executed_price",
                    "slippage",
                    "commission",
                    "pnl",
                    "timestamp",
                ]

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()

                for trade in self.trade_log:
                    writer.writerow(
                        {
                            "trade_id": trade.trade_id,
                            "symbol": trade.symbol,
                            "side": trade.side,
                            "quantity": trade.quantity,
                            "entry_price": trade.entry_price,
                            "executed_price": trade.executed_price,
                            "slippage": trade.slippage,
                            "commission": trade.commission,
                            "pnl": trade.pnl,
                            "timestamp": trade.timestamp.isoformat(),
                        }
                    )

            logger.info(f"Exported {len(self.trade_log)} trades to {filepath}")
            return True

        except IOError as e:
            logger.error(f"Failed to export trades: {e}")
            return False

    async def settle_position(
        self,
        symbol: str,
        settlement_price: float,
    ) -> Optional[float]:
        """
        Settle a position at a known final price (0.0 or 1.0 for binary contracts).

        Kalshi binary contracts settle at $1.00 (YES wins) or $0.00 (NO wins).
        This method closes the position at the settlement price and realizes P&L.

        Args:
            symbol: Contract ticker to settle
            settlement_price: Final settlement price (0.0 or 1.0 for Kalshi)

        Returns:
            Realized P&L from settlement, or None if no position found
        """
        if symbol not in self.positions:
            logger.debug(f"No position to settle for {symbol}")
            return None

        pos = self.positions[symbol]

        # Calculate realized P&L
        if pos.side == "BUY":
            pnl = (settlement_price - pos.entry_price) * pos.quantity
        else:  # SELL
            pnl = (pos.entry_price - settlement_price) * pos.quantity

        # Update cash — we get back the settlement value
        self.current_cash += pos.quantity * settlement_price

        # Record the settlement as a trade
        trade = PaperTrade(
            trade_id=str(uuid.uuid4()),
            symbol=symbol,
            side="SETTLE",
            quantity=pos.quantity,
            entry_price=pos.entry_price,
            executed_price=settlement_price,
            timestamp=datetime.utcnow(),
            slippage=0.0,
            commission=0.0,
            pnl=pnl,
        )
        self.trade_log.append(trade)

        logger.info(
            f"[SETTLED] {symbol}: {pos.side} {pos.quantity}x "
            f"@ entry ${pos.entry_price:.4f} → settlement ${settlement_price:.2f} "
            f"| P&L: ${pnl:.2f}"
        )

        del self.positions[symbol]
        return pnl

    def save_state(self, filepath: str) -> bool:
        """
        Persist paper executor state to a JSON file.

        Saves positions, trade log, cash balance, and order history so they
        survive process restarts. Called after every execution cycle.

        Args:
            filepath: Path to write JSON state file

        Returns:
            True if successful
        """
        import json as _json

        state = {
            "initial_capital": self.initial_capital,
            "current_cash": self.current_cash,
            "slippage_bps": self.slippage_bps,
            "commission_per_trade": self.commission_per_trade,
            "positions": {
                sym: {
                    "symbol": pos.symbol,
                    "side": pos.side,
                    "quantity": pos.quantity,
                    "entry_price": pos.entry_price,
                    "current_price": pos.current_price,
                    "entry_timestamp": pos.entry_timestamp.isoformat(),
                }
                for sym, pos in self.positions.items()
            },
            "trade_log": [
                {
                    "trade_id": t.trade_id,
                    "symbol": t.symbol,
                    "side": t.side,
                    "quantity": t.quantity,
                    "entry_price": t.entry_price,
                    "executed_price": t.executed_price,
                    "timestamp": t.timestamp.isoformat(),
                    "slippage": t.slippage,
                    "commission": t.commission,
                    "pnl": t.pnl,
                }
                for t in self.trade_log
            ],
            "market_prices": self.market_prices,
        }

        try:
            with open(filepath, "w") as f:
                _json.dump(state, f, indent=2)
            logger.debug(f"Paper state saved to {filepath}")
            return True
        except IOError as e:
            logger.error(f"Failed to save paper state: {e}")
            return False

    def load_state(self, filepath: str) -> bool:
        """
        Restore paper executor state from a JSON file.

        Call this on startup to resume from a previous session.
        If the file doesn't exist, starts fresh (not an error).

        Args:
            filepath: Path to JSON state file

        Returns:
            True if state was loaded, False if file not found or parse error
        """
        import json as _json
        import os as _os

        if not _os.path.exists(filepath):
            logger.info(f"No saved paper state at {filepath} — starting fresh")
            return False

        try:
            with open(filepath, "r") as f:
                state = _json.load(f)

            self.initial_capital = state.get("initial_capital", self.initial_capital)
            self.current_cash = state.get("current_cash", self.initial_capital)
            self.slippage_bps = state.get("slippage_bps", self.slippage_bps)
            self.commission_per_trade = state.get("commission_per_trade", self.commission_per_trade)
            self.market_prices = state.get("market_prices", {})

            # Restore positions
            self.positions = {}
            for sym, pdata in state.get("positions", {}).items():
                self.positions[sym] = PaperPosition(
                    symbol=pdata["symbol"],
                    side=pdata["side"],
                    quantity=pdata["quantity"],
                    entry_price=pdata["entry_price"],
                    current_price=pdata["current_price"],
                    entry_timestamp=datetime.fromisoformat(pdata["entry_timestamp"]),
                )

            # Restore trade log
            self.trade_log = []
            for tdata in state.get("trade_log", []):
                self.trade_log.append(PaperTrade(
                    trade_id=tdata["trade_id"],
                    symbol=tdata["symbol"],
                    side=tdata["side"],
                    quantity=tdata["quantity"],
                    entry_price=tdata["entry_price"],
                    executed_price=tdata["executed_price"],
                    timestamp=datetime.fromisoformat(tdata["timestamp"]),
                    slippage=tdata["slippage"],
                    commission=tdata["commission"],
                    pnl=tdata["pnl"],
                ))

            logger.info(
                f"Paper state loaded from {filepath}: "
                f"cash=${self.current_cash:.2f}, "
                f"{len(self.positions)} open positions, "
                f"{len(self.trade_log)} historical trades"
            )
            return True

        except (IOError, KeyError, ValueError) as e:
            logger.error(f"Failed to load paper state: {e}")
            return False

    def reset(self) -> None:
        """Reset paper executor to initial state."""
        self.current_cash = self.initial_capital
        self.positions.clear()
        self.trade_log.clear()
        self.order_history.clear()
        self.market_prices.clear()

        logger.info("Paper executor reset to initial state")
