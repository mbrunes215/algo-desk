"""
Kalshi API Order Executor

Handles order execution and market data retrieval via the Kalshi REST API.
Supports authentication, market browsing, and position management.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
from datetime import datetime
import json

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    raise ImportError("requests is required: pip install requests")


logger = logging.getLogger(__name__)


# Kalshi API Base URL
KALSHI_API_BASE = "https://api.elections.kalshi.com/trade-api/v2"


@dataclass
class KalshiOrderResult:
    """Result of a Kalshi order operation."""

    success: bool
    order_id: Optional[str] = None
    message: str = ""
    timestamp: datetime = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()


@dataclass
class KalshiPosition:
    """Kalshi position data structure."""

    ticker: str
    side: str  # 'BUY' or 'SELL'
    quantity: int
    entry_price: float
    current_price: float
    pnl: float
    pnl_percent: float


@dataclass
class KalshiMarket:
    """Kalshi market information."""

    ticker: str
    title: str
    status: str  # 'initialized', 'open', 'closed', 'settled'
    bid: float
    ask: float
    last_price: float
    volume: int
    close_time: Optional[str] = None  # ISO timestamp from API e.g. "2026-03-26T04:59:00Z"
    result: Optional[str] = None      # 'yes' | 'no' | None (None until settled)


class KalshiExecutor:
    """
    Kalshi API order executor.

    Provides methods for:
    - Authentication via email/password
    - Market browsing and searching
    - Order placement and cancellation
    - Position management
    - Balance inquiry
    - Order book retrieval
    """

    def __init__(
        self, api_key: str = "", email: str = "", password: str = "", timeout: int = 30, max_retries: int = 3
    ):
        """
        Initialize Kalshi executor.

        Args:
            api_key: Kalshi API key (preferred auth method)
            email: Kalshi account email (fallback)
            password: Kalshi account password (fallback)
            timeout: Request timeout in seconds
            max_retries: Max retries for rate-limited requests
        """
        self.api_key = api_key
        self.email = email
        self.password = password
        self.timeout = timeout
        self.max_retries = max_retries

        self.token: Optional[str] = None
        self.session = self._create_session()
        # If API key provided, we're immediately authenticated
        self.authenticated = bool(api_key)
        if api_key:
            self.session.headers.update({"Authorization": f"Bearer {api_key}"})

        logger.info(f"KalshiExecutor initialized: {'api_key' if api_key else 'email=' + email}")

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry strategy.

        Returns:
            Configured requests.Session
        """
        session = requests.Session()

        # Retry strategy: exponential backoff for rate limits
        retry_strategy = Retry(
            total=self.max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "DELETE", "PUT"],
            backoff_factor=1,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _get_headers(self) -> Dict[str, str]:
        """
        Get request headers with auth token if available.

        Returns:
            Dict of headers
        """
        headers = {"Content-Type": "application/json"}

        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"

        return headers

    async def login(self) -> bool:
        """
        Authenticate with Kalshi API.

        Returns:
            True if authentication successful, False otherwise
        """
        if not self.email or not self.password:
            logger.error("Email and password required for login")
            return False

        try:
            url = f"{KALSHI_API_BASE}/login"
            payload = {"email": self.email, "password": self.password}

            response = self.session.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if response.status_code not in [200, 201]:
                logger.error(
                    f"Login failed: {response.status_code} - {response.text}"
                )
                self.authenticated = False
                return False

            data = response.json()
            self.token = data.get("token")

            if not self.token:
                logger.error("No token returned from login")
                self.authenticated = False
                return False

            self.authenticated = True
            logger.info(f"Successfully authenticated with Kalshi")
            return True

        except requests.exceptions.RequestException as e:
            logger.error(f"Login request failed: {e}")
            self.authenticated = False
            return False
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse login response: {e}")
            self.authenticated = False
            return False

    async def get_markets(
        self, status: Optional[str] = None, limit: int = 100, series_ticker: Optional[str] = None
    ) -> List[KalshiMarket]:
        """
        Get list of available markets.

        Args:
            status: Filter by status ('OPEN', 'CLOSED', 'RESOLVED')
            limit: Max markets to return

        Returns:
            List of KalshiMarket objects
        """
        if not self.authenticated:
            logger.warning("Not authenticated")
            return []

        try:
            url = f"{KALSHI_API_BASE}/markets"
            params = {"limit": limit}

            if status:
                params["status"] = status

            if series_ticker:
                params["series_ticker"] = series_ticker

            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code != 200:
                logger.error(f"Failed to get markets: {response.status_code}")
                return []

            data = response.json()
            markets = []

            for market_data in data.get("markets", []):
                try:
                    # Kalshi API v2 returns prices in *_dollars fields (0.0-1.0 range)
                    # Fall back to integer cent fields for older API shapes.
                    # IMPORTANT: use `is not None` checks, not `or`, because 0 is a
                    # valid price and Python's `or` treats 0 as falsy.
                    def _pick_price(*keys, default=0):
                        for k in keys:
                            v = market_data.get(k)
                            if v is not None:
                                return float(v)
                        return float(default)

                    bid = _pick_price("yes_bid_dollars", "yes_bid", "bid_price")
                    ask = _pick_price("yes_ask_dollars", "yes_ask", "ask_price")
                    last_price = _pick_price("last_price_dollars", "last_price")
                    market = KalshiMarket(
                        ticker=market_data.get("ticker", ""),
                        title=market_data.get("title", ""),
                        status=market_data.get("status", ""),
                        bid=bid,
                        ask=ask,
                        last_price=last_price,
                        volume=int(market_data.get("volume", 0)),
                        close_time=market_data.get("close_time"),
                    )
                    markets.append(market)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Skipping malformed market data: {e}")
                    continue

            logger.info(f"Retrieved {len(markets)} markets")
            return markets

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response: {e}")
            return []

    async def get_market_by_ticker(self, ticker: str) -> Optional[KalshiMarket]:
        """
        Get market information by ticker.

        Args:
            ticker: Market ticker symbol

        Returns:
            KalshiMarket or None if not found
        """
        if not self.authenticated:
            logger.warning("Not authenticated")
            return None

        try:
            url = f"{KALSHI_API_BASE}/markets/{ticker}"

            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code == 404:
                logger.warning(f"Market not found: {ticker}")
                return None

            if response.status_code != 200:
                logger.error(f"Failed to get market {ticker}: {response.status_code}")
                return None

            data = response.json().get("market", {})

            # Use same field priority as get_markets(): dollars → cents → legacy
            # Use `is not None` checks — 0 is a valid price.
            def _pick(*keys, default=0):
                for k in keys:
                    v = data.get(k)
                    if v is not None:
                        return float(v)
                return float(default)

            bid = _pick("yes_bid_dollars", "yes_bid", "bid_price")
            ask = _pick("yes_ask_dollars", "yes_ask", "ask_price")
            last_price = _pick("last_price_dollars", "last_price")

            market = KalshiMarket(
                ticker=data.get("ticker", ticker),
                title=data.get("title", ""),
                status=data.get("status", ""),
                bid=bid,
                ask=ask,
                last_price=last_price,
                volume=int(data.get("volume", 0)),
                close_time=data.get("close_time"),
                result=data.get("result"),  # 'yes' | 'no' | None
            )

            logger.debug(
                f"Market {ticker}: status={market.status}, result={market.result}, "
                f"last_price={market.last_price}"
            )
            return market

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse response: {e}")
            return None

    async def place_order(
        self,
        ticker: str,
        side: str,
        quantity: int,
        price: float,
        order_type: str = "limit",
    ) -> KalshiOrderResult:
        """
        Place an order on a market.

        Args:
            ticker: Market ticker
            side: 'BUY' or 'SELL'
            quantity: Number of shares
            price: Price per share (for limit orders)
            order_type: 'limit' or 'market' (default: 'limit')

        Returns:
            KalshiOrderResult with order_id if successful
        """
        if not self.authenticated:
            return KalshiOrderResult(success=False, message="Not authenticated")

        if side.upper() not in ["BUY", "SELL"]:
            return KalshiOrderResult(
                success=False, message="side must be 'BUY' or 'SELL'"
            )

        try:
            url = f"{KALSHI_API_BASE}/orders"

            payload = {
                "ticker": ticker,
                "side": side.upper(),
                "quantity": quantity,
                "order_type": order_type.lower(),
            }

            if order_type.lower() == "limit":
                payload["price"] = price

            response = self.session.post(
                url,
                json=payload,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code not in [200, 201]:
                logger.error(
                    f"Failed to place order: {response.status_code} - {response.text}"
                )
                return KalshiOrderResult(
                    success=False, message=response.text
                )

            data = response.json().get("order", {})
            order_id = data.get("order_id")

            logger.info(
                f"Order placed: {ticker} {side} x{quantity} @ {price} "
                f"(orderId={order_id})"
            )

            return KalshiOrderResult(
                success=True,
                order_id=order_id,
                message=f"Order {order_id} placed successfully",
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return KalshiOrderResult(success=False, message=str(e))
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response: {e}")
            return KalshiOrderResult(success=False, message=str(e))

    async def cancel_order(self, order_id: str) -> KalshiOrderResult:
        """
        Cancel an existing order.

        Args:
            order_id: Order ID to cancel

        Returns:
            KalshiOrderResult indicating success/failure
        """
        if not self.authenticated:
            return KalshiOrderResult(success=False, message="Not authenticated")

        try:
            url = f"{KALSHI_API_BASE}/orders/{order_id}"

            response = self.session.delete(
                url,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code == 404:
                return KalshiOrderResult(
                    success=False, message=f"Order {order_id} not found"
                )

            if response.status_code not in [200, 204]:
                logger.error(f"Failed to cancel order: {response.status_code}")
                return KalshiOrderResult(
                    success=False, message=f"HTTP {response.status_code}"
                )

            logger.info(f"Order {order_id} cancelled")

            return KalshiOrderResult(
                success=True, message=f"Order {order_id} cancelled"
            )

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return KalshiOrderResult(success=False, message=str(e))

    async def get_positions(self) -> List[KalshiPosition]:
        """
        Get all open positions.

        Returns:
            List of KalshiPosition objects
        """
        if not self.authenticated:
            logger.warning("Not authenticated")
            return []

        try:
            url = f"{KALSHI_API_BASE}/portfolio/positions"

            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code != 200:
                logger.error(f"Failed to get positions: {response.status_code}")
                return []

            data = response.json()
            positions = []

            for pos_data in data.get("positions", []):
                try:
                    position = KalshiPosition(
                        ticker=pos_data.get("ticker", ""),
                        side=pos_data.get("side", ""),
                        quantity=int(pos_data.get("quantity", 0)),
                        entry_price=float(pos_data.get("entry_price", 0)),
                        current_price=float(pos_data.get("current_price", 0)),
                        pnl=float(pos_data.get("pnl", 0)),
                        pnl_percent=float(pos_data.get("pnl_percent", 0)),
                    )
                    positions.append(position)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Skipping malformed position: {e}")
                    continue

            logger.info(f"Retrieved {len(positions)} positions")
            return positions

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response: {e}")
            return []

    async def get_balance(self) -> float:
        """
        Get account balance.

        Returns:
            Account balance in dollars
        """
        if not self.authenticated:
            logger.warning("Not authenticated")
            return 0.0

        try:
            url = f"{KALSHI_API_BASE}/portfolio/balance"

            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code != 200:
                logger.error(f"Failed to get balance: {response.status_code}")
                return 0.0

            data = response.json()
            balance = float(data.get("balance", 0))

            logger.debug(f"Current balance: ${balance:.2f}")
            return balance

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return 0.0
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse response: {e}")
            return 0.0

    async def get_orderbook(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Get order book for a market.

        Args:
            ticker: Market ticker

        Returns:
            Dict with 'bids' and 'asks' lists, or None if failed
        """
        if not self.authenticated:
            logger.warning("Not authenticated")
            return None

        try:
            url = f"{KALSHI_API_BASE}/markets/{ticker}/orderbook"

            response = self.session.get(
                url,
                headers=self._get_headers(),
                timeout=self.timeout,
            )

            if response.status_code == 404:
                logger.warning(f"Market not found: {ticker}")
                return None

            if response.status_code != 200:
                logger.error(
                    f"Failed to get orderbook: {response.status_code}"
                )
                return None

            data = response.json().get("orderbook", {})

            return {
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
                "timestamp": datetime.utcnow(),
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse response: {e}")
            return None
