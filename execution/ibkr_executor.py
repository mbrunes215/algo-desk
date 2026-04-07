"""
IBKR (Interactive Brokers) Order Executor

Handles order execution, position management, and market data retrieval
via the Interactive Brokers API using ib_insync library.
"""

import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import asyncio

try:
    from ib_insync import (
        IB,
        Stock,
        Forex,
        Future,
        Option,
        MarketOrder,
        LimitOrder,
        StopOrder,
        Order,
        Contract,
        BarData,
    )
except ImportError:
    raise ImportError("ib_insync is required: pip install ib_insync")


logger = logging.getLogger(__name__)


@dataclass
class OrderResult:
    """Result of an order placement attempt."""

    success: bool
    order_id: Optional[int] = None
    message: str = ""
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class Position:
    """Position data structure."""

    symbol: str
    qty: float
    avg_cost: float
    current_price: float
    market_value: float
    unrealized_pnl: float
    realized_pnl: float


@dataclass
class MarketDataSnapshot:
    """Market data snapshot."""

    symbol: str
    price: float
    bid: float
    ask: float
    volume: int
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


class IBKRExecutor:
    """
    Interactive Brokers order executor using ib_insync library.

    Provides methods for:
    - Connecting/disconnecting to TWS/Gateway
    - Placing and canceling orders
    - Retrieving positions and account information
    - Fetching market data
    - Supporting both live and paper trading
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        paper_trading: bool = False,
        timeout: int = 30,
    ):
        """
        Initialize IBKR executor.

        Args:
            host: TWS/Gateway host address
            port: TWS/Gateway port (7497 for paper, 7496 for live)
            client_id: Unique client ID for this connection
            paper_trading: If True, operates in paper trading mode
            timeout: Connection timeout in seconds
        """
        self.host = host
        self.port = port
        self.client_id = client_id
        self.paper_trading = paper_trading
        self.timeout = timeout

        self.ib = IB()
        self.connected = False
        self._order_callbacks: Dict[int, callable] = {}
        self._position_cache: Dict[str, Position] = {}
        self._market_data_cache: Dict[str, MarketDataSnapshot] = {}

        logger.info(
            f"IBKRExecutor initialized: host={host}, port={port}, "
            f"client_id={client_id}, paper_trading={paper_trading}"
        )

    async def connect(self) -> bool:
        """
        Connect to TWS or IB Gateway.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            await asyncio.wait_for(
                self.ib.connectAsync(
                    self.host, self.port, clientId=self.client_id, readonly=False
                ),
                timeout=self.timeout,
            )
            self.connected = True
            logger.info(
                f"Successfully connected to TWS/Gateway at {self.host}:{self.port}"
            )
            return True
        except asyncio.TimeoutError:
            logger.error(f"Connection timeout after {self.timeout}s")
            self.connected = False
            return False
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            self.connected = False
            return False

    async def disconnect(self) -> None:
        """Disconnect from TWS/Gateway."""
        if self.connected:
            try:
                await self.ib.disconnectAsync()
                self.connected = False
                logger.info("Disconnected from TWS/Gateway")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")

    async def place_order(
        self,
        symbol: str,
        qty: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
        account_id: Optional[str] = None,
    ) -> OrderResult:
        """
        Place an order.

        Args:
            symbol: Stock symbol (e.g., 'AAPL')
            qty: Quantity (positive for buy, negative for sell)
            order_type: 'MARKET', 'LIMIT', or 'STOP'
            price: Limit price (required for LIMIT orders)
            stop_price: Stop price (required for STOP orders)
            account_id: Account to trade with (optional)

        Returns:
            OrderResult with order_id if successful
        """
        if not self.connected:
            return OrderResult(
                success=False, message="Not connected to TWS/Gateway"
            )

        try:
            # Create contract
            contract = Stock(symbol, "SMART", "USD")

            # Create order based on type
            if order_type.upper() == "LIMIT":
                if price is None:
                    return OrderResult(
                        success=False, message="price required for LIMIT order"
                    )
                order = LimitOrder("BUY" if qty > 0 else "SELL", abs(qty), price)
            elif order_type.upper() == "STOP":
                if stop_price is None:
                    return OrderResult(
                        success=False, message="stop_price required for STOP order"
                    )
                order = StopOrder("BUY" if qty > 0 else "SELL", abs(qty), stop_price)
            else:  # MARKET
                order = MarketOrder("BUY" if qty > 0 else "SELL", abs(qty))

            # Place the order
            trade = self.ib.placeOrder(contract, order)
            order_id = trade.order.orderId

            logger.info(
                f"Order placed: {symbol} qty={qty} type={order_type} "
                f"orderId={order_id} account={account_id}"
            )

            # Wait for order to be submitted
            await asyncio.sleep(0.1)

            return OrderResult(
                success=True,
                order_id=order_id,
                message=f"Order {order_id} placed successfully",
            )

        except Exception as e:
            logger.error(f"Failed to place order for {symbol}: {e}")
            return OrderResult(success=False, message=str(e))

    async def cancel_order(self, order_id: int) -> OrderResult:
        """
        Cancel an existing order.

        Args:
            order_id: Order ID to cancel

        Returns:
            OrderResult indicating success/failure
        """
        if not self.connected:
            return OrderResult(
                success=False, message="Not connected to TWS/Gateway"
            )

        try:
            # Find the trade by order ID
            trade = None
            for t in self.ib.trades():
                if t.order.orderId == order_id:
                    trade = t
                    break

            if not trade:
                return OrderResult(
                    success=False, message=f"Order {order_id} not found"
                )

            self.ib.cancelOrder(trade.order)
            logger.info(f"Order {order_id} cancellation requested")

            await asyncio.sleep(0.1)

            return OrderResult(
                success=True, message=f"Order {order_id} canceled"
            )

        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return OrderResult(success=False, message=str(e))

    async def get_positions(self) -> Dict[str, Position]:
        """
        Get all open positions.

        Returns:
            Dictionary mapping symbol to Position
        """
        if not self.connected:
            logger.warning("Not connected, returning cached positions")
            return self._position_cache

        try:
            positions = {}
            for portfolio_item in self.ib.portfolio():
                contract = portfolio_item.contract
                symbol = contract.symbol

                positions[symbol] = Position(
                    symbol=symbol,
                    qty=portfolio_item.position,
                    avg_cost=portfolio_item.avgCost,
                    current_price=portfolio_item.marketPrice,
                    market_value=portfolio_item.marketValue,
                    unrealized_pnl=portfolio_item.unrealizedPNL,
                    realized_pnl=portfolio_item.realizedPNL,
                )

            self._position_cache = positions
            return positions

        except Exception as e:
            logger.error(f"Failed to retrieve positions: {e}")
            return self._position_cache

    async def get_account_value(self) -> Dict[str, float]:
        """
        Get account information.

        Returns:
            Dict with keys: account_value, buying_power, net_liquidation,
            cash, realized_pnl, unrealized_pnl
        """
        if not self.connected:
            logger.warning("Not connected, unable to retrieve account value")
            return {}

        try:
            account_values = {}
            for value in self.ib.accountValues():
                if value.account and value.tag in [
                    "NetLiquidation",
                    "TotalCashBalance",
                    "BuyingPower",
                    "RealizedPnL",
                    "UnrealizedPnL",
                ]:
                    account_values[value.tag] = float(value.value)

            return {
                "account_value": account_values.get("NetLiquidation", 0),
                "buying_power": account_values.get("BuyingPower", 0),
                "net_liquidation": account_values.get("NetLiquidation", 0),
                "cash": account_values.get("TotalCashBalance", 0),
                "realized_pnl": account_values.get("RealizedPnL", 0),
                "unrealized_pnl": account_values.get("UnrealizedPnL", 0),
            }

        except Exception as e:
            logger.error(f"Failed to retrieve account value: {e}")
            return {}

    async def get_market_data(self, symbol: str) -> Optional[MarketDataSnapshot]:
        """
        Get current market data for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            MarketDataSnapshot or None if failed
        """
        if not self.connected:
            logger.warning("Not connected, cannot fetch market data")
            return self._market_data_cache.get(symbol)

        try:
            contract = Stock(symbol, "SMART", "USD")

            # Request market data
            ticker = self.ib.reqMktData(contract)
            await asyncio.sleep(0.5)  # Wait for data to arrive

            if ticker.last is None or ticker.bid is None or ticker.ask is None:
                logger.warning(f"Incomplete market data for {symbol}")
                return self._market_data_cache.get(symbol)

            snapshot = MarketDataSnapshot(
                symbol=symbol,
                price=ticker.last,
                bid=ticker.bid,
                ask=ticker.ask,
                volume=ticker.volume if ticker.volume else 0,
            )

            self._market_data_cache[symbol] = snapshot
            self.ib.cancelMktData(contract)

            return snapshot

        except Exception as e:
            logger.error(f"Failed to get market data for {symbol}: {e}")
            return self._market_data_cache.get(symbol)

    async def wait_for_completion(
        self, order_id: int, timeout: int = 60
    ) -> Tuple[bool, str]:
        """
        Wait for an order to complete.

        Args:
            order_id: Order ID to wait for
            timeout: Maximum seconds to wait

        Returns:
            Tuple (success, status_message)
        """
        if not self.connected:
            return False, "Not connected"

        try:
            start_time = datetime.utcnow()
            while (datetime.utcnow() - start_time).total_seconds() < timeout:
                for trade in self.ib.trades():
                    if trade.order.orderId == order_id:
                        if trade.orderStatus.status == "Filled":
                            return True, f"Order {order_id} filled"
                        elif trade.orderStatus.status == "Cancelled":
                            return False, f"Order {order_id} cancelled"
                        elif trade.orderStatus.status == "Rejected":
                            return False, f"Order {order_id} rejected"

                await asyncio.sleep(0.5)

            return False, f"Order {order_id} timeout after {timeout}s"

        except Exception as e:
            logger.error(f"Error waiting for order: {e}")
            return False, str(e)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        asyncio.run(self.disconnect())
        return False
