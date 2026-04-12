"""
IBKR (Interactive Brokers) Order Executor
==========================================

Handles order execution, position management, and market data retrieval
via the Interactive Brokers API using ib_insync library.

Supports:
  - Stocks (SMART routing)
  - Futures (MNQ, NQ, ES, MES, etc.)
  - Bracket orders (entry + stop loss + take profit)
  - Real-time 1-minute bar streaming
  - Historical bar requests
  - Paper and live trading modes

Connection:
  - TWS Desktop: port 7497 (paper), 7496 (live)
  - IB Gateway:  port 4002 (paper), 4001 (live)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Callable
from datetime import datetime, timezone
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
        Trade,
        util,
    )
except ImportError:
    raise ImportError("ib_insync is required: pip install ib_insync")


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class OrderResult:
    """Result of an order placement attempt."""

    success: bool
    order_id: Optional[int] = None
    message: str = ""
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class BracketResult:
    """Result of a bracket order placement (entry + stop + target)."""

    success: bool
    parent_order_id: Optional[int] = None
    stop_order_id: Optional[int] = None
    target_order_id: Optional[int] = None
    message: str = ""
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


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
    contract_type: str = "STK"  # STK, FUT, OPT, etc.


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
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class BarSnapshot:
    """Single OHLCV bar."""

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


# ---------------------------------------------------------------------------
# Futures contract definitions
# ---------------------------------------------------------------------------

# CME micro/mini futures — localSymbol format: MYMZ6 = Dec 2026
# We use generic front-month via lastTradeDateOrContractMonth
FUTURES_SPECS = {
    "MNQ": {"exchange": "CME", "multiplier": 2, "currency": "USD",
             "description": "Micro E-mini Nasdaq 100"},
    "NQ":  {"exchange": "CME", "multiplier": 20, "currency": "USD",
             "description": "E-mini Nasdaq 100"},
    "MES": {"exchange": "CME", "multiplier": 5, "currency": "USD",
             "description": "Micro E-mini S&P 500"},
    "ES":  {"exchange": "CME", "multiplier": 50, "currency": "USD",
             "description": "E-mini S&P 500"},
    "MYM": {"exchange": "CBOT", "multiplier": 0.5, "currency": "USD",
             "description": "Micro E-mini Dow"},
    "YM":  {"exchange": "CBOT", "multiplier": 5, "currency": "USD",
             "description": "E-mini Dow"},
    "MCL": {"exchange": "NYMEX", "multiplier": 100, "currency": "USD",
             "description": "Micro WTI Crude Oil"},
}


class IBKRExecutor:
    """
    Interactive Brokers order executor using ib_insync library.

    Provides methods for:
    - Connecting/disconnecting to TWS/Gateway
    - Placing market, limit, stop, and bracket orders
    - Supporting stocks AND futures contracts
    - Streaming real-time 1-minute bars
    - Requesting historical bars
    - Retrieving positions and account information
    - Fetching market data snapshots
    - Paper and live trading modes
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
            port: TWS/Gateway port (7497 paper TWS, 7496 live TWS,
                  4002 paper Gateway, 4001 live Gateway)
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
        self._position_cache: Dict[str, Position] = {}
        self._market_data_cache: Dict[str, MarketDataSnapshot] = {}
        self._bar_subscribers: Dict[str, List[Callable]] = {}
        self._realtime_bars: Dict[str, List[BarSnapshot]] = {}

        logger.info(
            f"IBKRExecutor initialized: host={host}, port={port}, "
            f"client_id={client_id}, paper_trading={paper_trading}"
        )

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

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
                f"Connected to TWS/Gateway at {self.host}:{self.port}"
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
                self.ib.disconnect()
                self.connected = False
                logger.info("Disconnected from TWS/Gateway")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")

    def ensure_connected(self) -> bool:
        """Check connection is alive, reconnect if needed."""
        if not self.connected or not self.ib.isConnected():
            logger.warning("Connection lost, attempting reconnect")
            try:
                self.ib.disconnect()
                self.ib.connect(
                    self.host, self.port, clientId=self.client_id, readonly=False
                )
                self.connected = True
                logger.info("Reconnected successfully")
                return True
            except Exception as e:
                logger.error(f"Reconnect failed: {e}")
                self.connected = False
                return False
        return True

    # ------------------------------------------------------------------
    # Contract builders
    # ------------------------------------------------------------------

    def make_contract(
        self,
        symbol: str,
        sec_type: str = "STK",
        exchange: str = "SMART",
        currency: str = "USD",
        contract_month: str = "",
    ) -> Contract:
        """
        Build an ib_insync Contract object.

        Args:
            symbol: Instrument symbol (e.g. 'AAPL', 'MNQ', 'NQ')
            sec_type: Security type — 'STK' for stocks, 'FUT' for futures
            exchange: Exchange (e.g. 'SMART', 'CME', 'CBOT')
            currency: Currency (default 'USD')
            contract_month: For futures — YYYYMM (e.g. '202506' for June 2025).
                            If empty, resolves to front-month via qualifyContracts.

        Returns:
            Qualified Contract object
        """
        if sec_type.upper() == "FUT":
            spec = FUTURES_SPECS.get(symbol.upper(), {})
            exch = spec.get("exchange", exchange)
            cur = spec.get("currency", currency)
            contract = Future(
                symbol=symbol.upper(),
                exchange=exch,
                currency=cur,
            )
            if contract_month:
                contract.lastTradeDateOrContractMonth = contract_month
        else:
            contract = Stock(symbol, exchange, currency)

        return contract

    async def qualify_contract(self, contract: Contract) -> Contract:
        """
        Qualify a contract with IBKR to fill in missing fields (conId, etc.).
        Required before placing orders on futures.

        Args:
            contract: Unqualified contract

        Returns:
            Qualified contract with all fields populated
        """
        if not self.connected:
            raise ConnectionError("Not connected to TWS/Gateway")

        qualified = await self.ib.qualifyContractsAsync(contract)
        if qualified:
            return qualified[0]
        raise ValueError(f"Could not qualify contract: {contract}")

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    async def place_order(
        self,
        contract: Contract,
        qty: float,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        stop_price: Optional[float] = None,
    ) -> OrderResult:
        """
        Place a single order (market, limit, or stop).

        Args:
            contract: Qualified Contract object
            qty: Quantity (positive for buy, negative for sell)
            order_type: 'MARKET', 'LIMIT', or 'STOP'
            price: Limit price (required for LIMIT orders)
            stop_price: Stop price (required for STOP orders)

        Returns:
            OrderResult with order_id if successful
        """
        if not self.connected:
            return OrderResult(
                success=False, message="Not connected to TWS/Gateway"
            )

        try:
            action = "BUY" if qty > 0 else "SELL"
            abs_qty = abs(qty)

            if order_type.upper() == "LIMIT":
                if price is None:
                    return OrderResult(
                        success=False, message="price required for LIMIT order"
                    )
                order = LimitOrder(action, abs_qty, price)
            elif order_type.upper() == "STOP":
                if stop_price is None:
                    return OrderResult(
                        success=False, message="stop_price required for STOP order"
                    )
                order = StopOrder(action, abs_qty, stop_price)
            else:  # MARKET
                order = MarketOrder(action, abs_qty)

            trade = self.ib.placeOrder(contract, order)
            order_id = trade.order.orderId

            logger.info(
                f"Order placed: {contract.symbol} qty={qty} type={order_type} "
                f"orderId={order_id}"
            )

            # Give TWS a moment to acknowledge
            await asyncio.sleep(0.1)

            return OrderResult(
                success=True,
                order_id=order_id,
                message=f"Order {order_id} placed successfully",
            )

        except Exception as e:
            logger.error(f"Failed to place order for {contract.symbol}: {e}")
            return OrderResult(success=False, message=str(e))

    async def place_bracket_order(
        self,
        contract: Contract,
        qty: float,
        entry_price: float,
        stop_price: float,
        target_price: float,
        entry_type: str = "LIMIT",
    ) -> BracketResult:
        """
        Place a bracket order: entry + stop loss + take profit.

        The entry can be LIMIT (for limit entries) or STOP (for breakout entries).
        Stop loss and take profit are always attached as child orders.

        Args:
            contract: Qualified Contract object
            qty: Quantity (positive for long, negative for short)
            entry_price: Entry price (limit or stop trigger)
            stop_price: Stop loss price
            target_price: Take profit price
            entry_type: 'LIMIT' or 'STOP' for the entry order

        Returns:
            BracketResult with all three order IDs
        """
        if not self.connected:
            return BracketResult(
                success=False, message="Not connected to TWS/Gateway"
            )

        try:
            action = "BUY" if qty > 0 else "SELL"
            reverse_action = "SELL" if qty > 0 else "BUY"
            abs_qty = abs(qty)

            # Use ib_insync's built-in bracket order builder
            bracket = self.ib.bracketOrder(
                action=action,
                quantity=abs_qty,
                limitPrice=entry_price if entry_type == "LIMIT" else 0,
                takeProfitPrice=target_price,
                stopLossPrice=stop_price,
            )

            parent_order, tp_order, sl_order = bracket

            # If entry is a stop order (breakout), replace parent with StopOrder
            if entry_type.upper() == "STOP":
                parent_order = StopOrder(action, abs_qty, entry_price)
                parent_order.orderId = self.ib.client.getReqId()
                parent_order.transmit = False

                # Re-link children
                tp_order.parentId = parent_order.orderId
                tp_order.transmit = False
                sl_order.parentId = parent_order.orderId
                sl_order.transmit = True  # last child transmits all

            # Place all three orders
            parent_trade = self.ib.placeOrder(contract, parent_order)
            tp_trade = self.ib.placeOrder(contract, tp_order)
            sl_trade = self.ib.placeOrder(contract, sl_order)

            await asyncio.sleep(0.2)

            logger.info(
                f"Bracket order placed: {contract.symbol} {action} {abs_qty} "
                f"entry={entry_price} stop={stop_price} target={target_price} "
                f"parent_id={parent_trade.order.orderId}"
            )

            return BracketResult(
                success=True,
                parent_order_id=parent_trade.order.orderId,
                stop_order_id=sl_trade.order.orderId,
                target_order_id=tp_trade.order.orderId,
                message="Bracket order placed",
            )

        except Exception as e:
            logger.error(f"Failed to place bracket order: {e}")
            return BracketResult(success=False, message=str(e))

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

    async def cancel_all_orders(self) -> int:
        """Cancel all open orders. Returns count of orders canceled."""
        if not self.connected:
            return 0

        open_orders = self.ib.openOrders()
        count = 0
        for order in open_orders:
            try:
                self.ib.cancelOrder(order)
                count += 1
            except Exception as e:
                logger.error(f"Failed to cancel order {order.orderId}: {e}")
        logger.info(f"Canceled {count} open orders")
        return count

    # ------------------------------------------------------------------
    # Market data — snapshots
    # ------------------------------------------------------------------

    async def get_market_data(
        self, contract: Contract
    ) -> Optional[MarketDataSnapshot]:
        """
        Get current market data snapshot for a contract.

        Args:
            contract: Qualified Contract object

        Returns:
            MarketDataSnapshot or None if failed
        """
        if not self.connected:
            return self._market_data_cache.get(contract.symbol)

        try:
            ticker = self.ib.reqMktData(contract)
            await asyncio.sleep(1.0)  # wait for data

            price = ticker.last if ticker.last and ticker.last > 0 else ticker.close
            snapshot = MarketDataSnapshot(
                symbol=contract.symbol,
                price=price or 0.0,
                bid=ticker.bid or 0.0,
                ask=ticker.ask or 0.0,
                volume=int(ticker.volume) if ticker.volume else 0,
            )

            self._market_data_cache[contract.symbol] = snapshot
            self.ib.cancelMktData(contract)
            return snapshot

        except Exception as e:
            logger.error(f"Failed to get market data for {contract.symbol}: {e}")
            return self._market_data_cache.get(contract.symbol)

    # ------------------------------------------------------------------
    # Market data — historical bars
    # ------------------------------------------------------------------

    async def get_historical_bars(
        self,
        contract: Contract,
        duration: str = "1 D",
        bar_size: str = "1 min",
        what_to_show: str = "TRADES",
        use_rth: bool = True,
    ) -> List[BarSnapshot]:
        """
        Request historical OHLCV bars.

        Args:
            contract: Qualified Contract object
            duration: How far back — e.g. '1 D', '2 D', '1 W'
            bar_size: Bar period — e.g. '1 min', '5 mins', '1 hour'
            what_to_show: 'TRADES', 'MIDPOINT', 'BID', 'ASK'
            use_rth: True = regular trading hours only

        Returns:
            List of BarSnapshot ordered oldest → newest
        """
        if not self.connected:
            raise ConnectionError("Not connected to TWS/Gateway")

        try:
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=what_to_show,
                useRTH=use_rth,
                formatDate=2,  # UTC timestamps
            )

            result = []
            for bar in bars:
                result.append(BarSnapshot(
                    timestamp=bar.date if isinstance(bar.date, datetime) else datetime.fromisoformat(str(bar.date)),
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=int(bar.volume),
                ))

            logger.info(
                f"Historical bars: {contract.symbol} {duration} {bar_size} "
                f"→ {len(result)} bars"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to get historical bars for {contract.symbol}: {e}")
            return []

    # ------------------------------------------------------------------
    # Market data — real-time bar streaming
    # ------------------------------------------------------------------

    def subscribe_realtime_bars(
        self,
        contract: Contract,
        callback: Callable[[BarSnapshot], None],
        what_to_show: str = "TRADES",
        bar_size: int = 5,
    ) -> None:
        """
        Subscribe to real-time bar updates (5-second bars from IBKR).

        Note: IBKR only supports 5-second real-time bars natively.
        For 1-minute bars, the ORB strategy aggregates 5-second bars internally.

        Args:
            contract: Qualified Contract object
            callback: Function called with each new BarSnapshot
            what_to_show: 'TRADES', 'MIDPOINT', 'BID', 'ASK'
            bar_size: Always 5 (IBKR restriction)
        """
        if not self.connected:
            raise ConnectionError("Not connected to TWS/Gateway")

        key = contract.symbol

        def on_bar_update(bars, has_new_bar):
            if has_new_bar and bars:
                bar = bars[-1]
                snap = BarSnapshot(
                    timestamp=bar.date if isinstance(bar.date, datetime) else datetime.fromisoformat(str(bar.date)),
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=int(bar.volume),
                )
                # Store locally
                if key not in self._realtime_bars:
                    self._realtime_bars[key] = []
                self._realtime_bars[key].append(snap)
                # Notify subscriber
                callback(snap)

        rt_bars = self.ib.reqRealTimeBars(
            contract, barSize=bar_size, whatToShow=what_to_show, useRTH=False
        )
        rt_bars.updateEvent += on_bar_update

        if key not in self._bar_subscribers:
            self._bar_subscribers[key] = []
        self._bar_subscribers[key].append(rt_bars)

        logger.info(f"Subscribed to real-time bars: {contract.symbol}")

    def unsubscribe_realtime_bars(self, contract: Contract) -> None:
        """Cancel real-time bar subscription for a contract."""
        key = contract.symbol
        if key in self._bar_subscribers:
            for rt_bars in self._bar_subscribers[key]:
                self.ib.cancelRealTimeBars(rt_bars)
            del self._bar_subscribers[key]
            logger.info(f"Unsubscribed from real-time bars: {contract.symbol}")

    def get_buffered_bars(self, symbol: str) -> List[BarSnapshot]:
        """Get all buffered real-time bars for a symbol."""
        return self._realtime_bars.get(symbol, [])

    def clear_buffered_bars(self, symbol: str) -> None:
        """Clear buffered real-time bars for a symbol."""
        if symbol in self._realtime_bars:
            self._realtime_bars[symbol] = []

    # ------------------------------------------------------------------
    # Positions & account
    # ------------------------------------------------------------------

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
                    contract_type=contract.secType or "STK",
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
                if value.tag in [
                    "NetLiquidation",
                    "TotalCashBalance",
                    "BuyingPower",
                    "RealizedPnL",
                    "UnrealizedPnL",
                    "AvailableFunds",
                    "MaintMarginReq",
                ]:
                    try:
                        account_values[value.tag] = float(value.value)
                    except (ValueError, TypeError):
                        pass

            return {
                "account_value": account_values.get("NetLiquidation", 0),
                "buying_power": account_values.get("BuyingPower", 0),
                "net_liquidation": account_values.get("NetLiquidation", 0),
                "cash": account_values.get("TotalCashBalance", 0),
                "realized_pnl": account_values.get("RealizedPnL", 0),
                "unrealized_pnl": account_values.get("UnrealizedPnL", 0),
                "available_funds": account_values.get("AvailableFunds", 0),
                "margin_required": account_values.get("MaintMarginReq", 0),
            }

        except Exception as e:
            logger.error(f"Failed to retrieve account value: {e}")
            return {}

    # ------------------------------------------------------------------
    # Order monitoring
    # ------------------------------------------------------------------

    async def wait_for_fill(
        self, order_id: int, timeout: int = 60
    ) -> Tuple[bool, str]:
        """
        Wait for an order to fill.

        Args:
            order_id: Order ID to wait for
            timeout: Maximum seconds to wait

        Returns:
            Tuple (success, status_message)
        """
        if not self.connected:
            return False, "Not connected"

        try:
            start = datetime.now(timezone.utc)
            while (datetime.now(timezone.utc) - start).total_seconds() < timeout:
                for trade in self.ib.trades():
                    if trade.order.orderId == order_id:
                        status = trade.orderStatus.status
                        if status == "Filled":
                            fill_price = trade.orderStatus.avgFillPrice
                            return True, f"Filled at {fill_price}"
                        elif status in ("Cancelled", "ApiCancelled"):
                            return False, f"Order {order_id} cancelled"
                        elif status in ("Inactive", "Rejected"):
                            return False, f"Order {order_id} rejected"

                await asyncio.sleep(0.5)
                self.ib.sleep(0)  # pump ib_insync event loop

            return False, f"Timeout after {timeout}s"

        except Exception as e:
            logger.error(f"Error waiting for order: {e}")
            return False, str(e)

    def get_open_orders(self) -> List[Dict]:
        """Get all open/pending orders."""
        result = []
        for trade in self.ib.openTrades():
            order = trade.order
            status = trade.orderStatus
            result.append({
                "order_id": order.orderId,
                "symbol": trade.contract.symbol,
                "action": order.action,
                "qty": order.totalQuantity,
                "order_type": order.orderType,
                "limit_price": getattr(order, "lmtPrice", None),
                "stop_price": getattr(order, "auxPrice", None),
                "status": status.status,
                "filled": status.filled,
                "remaining": status.remaining,
                "parent_id": order.parentId,
            })
        return result

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def sleep(self, seconds: float = 0) -> None:
        """Pump the ib_insync event loop. Call periodically in sync code."""
        self.ib.sleep(seconds)

    async def run_loop(self) -> None:
        """Run ib_insync event loop forever (for streaming)."""
        await self.ib.runAsync()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.connected:
            self.ib.disconnect()
            self.connected = False
        return False
