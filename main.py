"""Main entry point for the algorithmic trading desk.

Initializes all trading systems, manages strategy execution, and handles
graceful shutdown.
"""

import argparse
import logging
import logging.handlers
import os
import signal
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure project root is on the path regardless of how the script is invoked
sys.path.insert(0, str(Path(__file__).resolve().parent))

import yaml
from dotenv import load_dotenv

from monitoring import TradingDashboard, AlertManager, HealthChecker, AlertLevel
from data.storage import SessionFactory
from data.pipelines import MarketDataPipeline
from execution.kalshi_executor import KalshiExecutor
from execution.paper_executor import PaperExecutor
from risk.daily_limits import DailyLimits
from strategies.kalshi_weather.weather_strategy import WeatherStrategy
from strategies.crypto_funding_arb import FundingArbStrategy
from strategies.pairs_trading import PairsTradingStrategy
from data.outcome_tracker import OutcomeTracker
from reports.generate_report import generate_report


logger = logging.getLogger(__name__)


class TradingDeskApplication:
    """Main application class for the algorithmic trading desk.

    Manages initialization, configuration loading, strategy execution,
    and graceful shutdown.
    """

    def __init__(
        self,
        config_path: str = "config/settings.yaml",
        paper_trading: bool = False,
        strategy_filter: Optional[str] = None,
        show_dashboard: bool = False,
    ) -> None:
        """Initialize the trading desk application.

        Args:
            config_path: Path to YAML configuration file.
            paper_trading: Enable paper trading mode (no real orders).
            strategy_filter: Only run specific strategy by name.
            show_dashboard: Display dashboard on startup.
        """
        self.config_path = config_path
        self.paper_trading = paper_trading
        self.strategy_filter = strategy_filter
        self.show_dashboard = show_dashboard
        self.running = False

        # Core components
        self.config: Dict[str, Any] = {}
        self.alert_manager: Optional[AlertManager] = None
        self.health_checker: Optional[HealthChecker] = None
        self.dashboard: Optional[TradingDashboard] = None
        self.market_data_pipeline: Optional[MarketDataPipeline] = None

        # Strategies and executors
        self.strategies: Dict[str, Any] = {}
        self.risk_managers: Dict[str, Any] = {}
        self.kalshi_executor: Optional[KalshiExecutor] = None
        self.paper_executor: Optional[PaperExecutor] = None
        self.daily_limits: Optional[DailyLimits] = None
        self.weather_strategy: Optional[WeatherStrategy] = None
        self.funding_arb_strategy: Optional[FundingArbStrategy] = None
        self.last_funding_scan: Optional[datetime] = None
        self.funding_scan_interval_seconds: int = 1800  # Scan every 30 minutes
        self.outcome_tracker: Optional[OutcomeTracker] = None
        self.last_signal_run: Optional[datetime] = None
        self.last_settle_run: Optional[datetime] = None
        self.signal_interval_seconds: int = 300  # Run signals every 5 minutes
        self.settle_interval_seconds: int = 3600  # Check settlements every hour

    def setup_logging(self) -> None:
        """Configure logging system."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)

        # File handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / "trading_desk.log",
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
        )
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
        )
        file_handler.setFormatter(file_formatter)

        # Root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)

        logger.info("Logging configured")

    def load_config(self) -> None:
        """Load configuration from YAML and environment files."""
        # Load environment variables
        load_dotenv()

        # Load YAML config
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file not found: {self.config_path}")
            self.config = self._get_default_config()
        else:
            with open(self.config_path, "r") as f:
                self.config = yaml.safe_load(f) or {}
            logger.info(f"Configuration loaded from {self.config_path}")

        # Merge strategies.yaml if it exists
        strategies_path = str(Path(self.config_path).parent / "strategies.yaml")
        if os.path.exists(strategies_path):
            with open(strategies_path, "r") as f:
                strategies_config = yaml.safe_load(f) or {}
            self.config["strategies"] = strategies_config.get("strategies", strategies_config)
            logger.info(f"Strategies config loaded: {list(self.config['strategies'].keys())}")

        # Override with environment variables
        self._apply_env_overrides()

        logger.info(f"Paper trading: {self.paper_trading}")

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default configuration.

        Returns:
            Default configuration dictionary.
        """
        return {
            "database": {
                "url": os.getenv("DATABASE_URL", "sqlite:///trading.db"),
            },
            "smtp": {
                "host": os.getenv("SMTP_HOST", "localhost"),
                "port": int(os.getenv("SMTP_PORT", "587")),
                "user": os.getenv("SMTP_USER"),
                "password": os.getenv("SMTP_PASSWORD"),
            },
            "alerts": {
                "to_emails": os.getenv("ALERT_EMAILS", "").split(","),
                "max_per_hour": 10,
            },
            "ibkr": {
                "host": os.getenv("IBKR_HOST", "127.0.0.1"),
                "port": int(os.getenv("IBKR_PORT", "7497")),
            },
            "kalshi": {
                "api_url": os.getenv("KALSHI_API_URL", "https://api.kalshi.com"),
                "api_key": os.getenv("KALSHI_API_KEY"),
            },
            "strategies": {},
        }

    def _apply_env_overrides(self) -> None:
        """Apply environment variable overrides to config."""
        if "database" not in self.config:
            self.config["database"] = {}
        self.config["database"]["url"] = os.getenv(
            "DATABASE_URL",
            self.config.get("database", {}).get("url", "sqlite:///trading.db"),
        )

    def initialize_components(self) -> None:
        """Initialize all trading system components."""
        logger.info("Initializing trading system components...")

        try:
            # Initialize database
            db_url = self.config.get("database", {}).get("url", "sqlite:///trading.db")
            SessionFactory.initialize(db_url)
            logger.info(f"Database initialized: {db_url}")

            # Initialize alerts
            smtp_config = self.config.get("smtp", {})
            alert_config = self.config.get("alerts", {})
            self.alert_manager = AlertManager(
                smtp_host=smtp_config.get("host", "localhost"),
                smtp_port=smtp_config.get("port", 587),
                smtp_user=smtp_config.get("user"),
                smtp_password=smtp_config.get("password"),
                to_emails=alert_config.get("to_emails", []),
                max_alerts_per_hour=alert_config.get("max_per_hour", 10),
                enable_email=not self.paper_trading,
            )
            logger.info("Alert manager initialized")

            # Initialize health checker
            ibkr_config = self.config.get("ibkr", {})
            self.health_checker = HealthChecker(
                ibkr_host=ibkr_config.get("host", "127.0.0.1"),
                ibkr_port=ibkr_config.get("port", 7497),
                database_url=db_url,
                paper_mode=self.paper_trading,
            )
            logger.info("Health checker initialized")

            # Initialize dashboard
            self.dashboard = TradingDashboard()
            logger.info("Dashboard initialized")

            # Initialize market data pipeline
            self.market_data_pipeline = MarketDataPipeline()
            logger.info("Market data pipeline initialized")

            # Initialize Kalshi executor with API key from env
            kalshi_api_key = os.getenv("KALSHI_API_KEY", "")
            kalshi_config = self.config.get("kalshi", {})
            self.kalshi_executor = KalshiExecutor(
                api_key=kalshi_api_key or kalshi_config.get("api_key", ""),
                email=kalshi_config.get("email", ""),
                password=kalshi_config.get("password", ""),
            )
            logger.info(f"Kalshi executor initialized (authenticated={self.kalshi_executor.authenticated})")

            # Initialize paper executor and risk limits for paper trading mode
            if self.paper_trading:
                risk_config = self.config.get("risk", {})
                paper_capital = 10000.0  # $10K starting capital for Kalshi paper trading

                self.paper_executor = PaperExecutor(
                    initial_capital=paper_capital,
                    slippage_bps=5.0,  # 5 bps slippage — Kalshi spreads are wide
                    commission_per_trade=0.0,  # Kalshi has no commission, just spread
                )

                self.daily_limits = DailyLimits(
                    daily_loss_limit=paper_capital * risk_config.get("max_daily_loss_pct", 0.03),
                    max_notional_per_trade=paper_capital * risk_config.get("max_position_pct", 0.02),
                    max_concentration_pct=20.0,
                    max_trades_per_day=risk_config.get("max_trades_per_day", 50),
                    max_gross_exposure_pct=100.0,  # Up to 100% of capital deployed
                    portfolio_value=paper_capital,
                )
                # Restore previous session state if it exists
                self._paper_state_file = "logs/paper_state.json"
                self.paper_executor.load_state(self._paper_state_file)

                logger.info(
                    f"Paper executor initialized: ${paper_capital:.0f} capital, "
                    f"loss limit=${self.daily_limits.daily_loss_limit:.0f}/day, "
                    f"max per trade=${self.daily_limits.max_notional_per_trade:.0f}"
                )

            # Initialize outcome tracker (SQLite-backed signal log)
            self.outcome_tracker = OutcomeTracker(db_path="trading.db")
            logger.info("Outcome tracker initialized (trading.db)")

            logger.info("All components initialized successfully")

        except Exception as e:
            logger.error(f"Error initializing components: {e}", exc_info=True)
            self.alert_manager.send_alert(
                AlertLevel.CRITICAL,
                f"Failed to initialize trading system: {e}",
            )
            raise

    def initialize_strategies(self) -> None:
        """Initialize configured trading strategies."""
        strategy_configs = self.config.get("strategies", {})

        for strategy_name, strategy_config in strategy_configs.items():
            # Apply strategy filter if specified
            if self.strategy_filter and strategy_name != self.strategy_filter:
                logger.info(f"Skipping strategy (filtered): {strategy_name}")
                continue

            # Check if strategy is enabled
            if not strategy_config.get("enabled", False):
                logger.info(f"Strategy disabled (skipped): {strategy_name}")
                continue

            logger.info(f"Initializing strategy: {strategy_name}")

            if strategy_name == "kalshi_weather":
                params = strategy_config.get("params", {})
                self.weather_strategy = WeatherStrategy(
                    min_edge_bps=params.get("min_edge", 0.05) * 10000,
                    min_confidence=0.65,
                )
                # Load per-city calibration from backtest results (if available)
                db_url = self.config.get("database", {}).get("url", "trading.db")
                db_path = db_url.replace("sqlite:///", "")
                n_cal = self.weather_strategy.load_calibration(db_path=db_path)
                if n_cal:
                    logger.info(f"WeatherStrategy: loaded calibration for {n_cal} cities")
                else:
                    logger.info("WeatherStrategy: no backtest calibration found — using defaults (run backtest/run_backtest.py to generate)")
                self.strategies[strategy_name] = {
                    "name": strategy_name,
                    "config": strategy_config,
                    "status": "INITIALIZED",
                    "instance": self.weather_strategy,
                }
                logger.info("WeatherStrategy initialized")
            elif strategy_name == "crypto_funding_arb":
                params = strategy_config.get("params", {})
                self.funding_arb_strategy = FundingArbStrategy(
                    paper_mode=self.paper_trading,
                    config={
                        "min_net_yield": params.get("min_net_yield", 0.08),
                        "exit_yield": params.get("exit_yield", 0.04),
                        "max_basis_pct": params.get("max_basis_pct", 0.005),
                        "position_size_usd": params.get("position_size_usd", 500),
                        "max_positions": params.get("max_positions", 6),
                    },
                )
                self.strategies[strategy_name] = {
                    "name": strategy_name,
                    "config": strategy_config,
                    "status": "INITIALIZED",
                    "instance": self.funding_arb_strategy,
                }
                logger.info(
                    f"FundingArbStrategy initialized | "
                    f"min_yield={params.get('min_net_yield', 0.08):.0%} | "
                    f"position_size=${params.get('position_size_usd', 500):,}/leg"
                )
            elif strategy_name == "ibkr_orb":
                params = strategy_config.get("params", {})
                from strategies.ibkr_orb import ORBStrategy
                self.orb_strategy = ORBStrategy(
                    paper_mode=self.paper_trading,
                    config={
                        "symbol": params.get("symbol", "MNQ"),
                        "range_minutes": params.get("range_minutes", 15),
                        "rr_multiple": params.get("rr_multiple", 2.0),
                        "contracts": params.get("contracts", 1),
                        "max_daily_loss_usd": params.get("max_daily_loss_usd", 200),
                        "min_range_points": params.get("min_range_points", 10),
                        "max_range_points": params.get("max_range_points", 100),
                        "close_time_et": params.get("close_time_et", "15:55"),
                    },
                )
                self.strategies[strategy_name] = {
                    "name": strategy_name,
                    "config": strategy_config,
                    "status": "INITIALIZED",
                    "instance": self.orb_strategy,
                }
                logger.info(
                    f"ORBStrategy initialized | "
                    f"symbol={params.get('symbol', 'MNQ')} | "
                    f"range={params.get('range_minutes', 15)}min | "
                    f"R:R=1:{params.get('rr_multiple', 2.0)}"
                )
            else:
                self.strategies[strategy_name] = {
                    "name": strategy_name,
                    "config": strategy_config,
                    "status": "INITIALIZED",
                }

    def _settle_expired_positions(self) -> None:
        """
        Check for expired paper positions and settle them.

        For each open position, parse the contract date from the ticker.
        If the contract date is in the past (settled), fetch actual temperature
        from NOAA and determine settlement (YES=1.0, NO=0.0).

        Kalshi KXHIGH-T contracts: YES pays $1 if actual high > threshold.
        Kalshi KXLOW-B contracts:  YES pays $1 if actual high < threshold.
        """
        if not self.paper_executor:
            return

        import re as _re

        positions = asyncio.run(self.paper_executor.get_positions())
        if not positions:
            return

        now = datetime.now()
        settled_count = 0

        for symbol, pos in list(positions.items()):
            try:
                parts = symbol.split("-")
                if len(parts) < 3:
                    continue

                # Parse contract date from ticker (e.g., 26MAR25 → 2026-03-25)
                date_str = parts[1]
                contract_date = datetime.strptime(date_str, "%y%b%d")

                # Only settle if the contract date is strictly in the past
                if contract_date.date() >= now.date():
                    continue

                # Parse threshold and direction
                series = parts[0]
                threshold_str = parts[2]
                direction = threshold_str[0].upper()
                threshold_num = float(_re.sub(r'^[A-Za-z]+', '', threshold_str))

                is_below = (direction == "B")

                # Determine location from series
                series_to_location = {
                    "KXHIGHNY": "NYC", "KXLOWNY": "NYC",
                    "KXHIGHLAX": "LAX", "KXLOWLAX": "LAX",
                    "KXHIGHCHI": "CHI", "KXLOWCHI": "CHI",
                    "KXHIGHMIA": "MIA", "KXHIGHDEN": "DEN",
                    "KXHIGHHOU": "HOU", "KXHIGHPHX": "PHX",
                }
                location = series_to_location.get(series)
                if not location:
                    continue

                # Try to fetch actual temperature from NOAA for settlement
                actual_high = self._fetch_actual_high(location, contract_date)
                if actual_high is None:
                    logger.warning(
                        f"Cannot settle {symbol}: no actual temp data for "
                        f"{location} on {contract_date.date()}"
                    )
                    continue

                # Determine settlement
                if is_below:
                    # YES pays if actual high < threshold
                    yes_wins = actual_high < threshold_num
                else:
                    # YES pays if actual high > threshold (standard KXHIGH-T)
                    yes_wins = actual_high > threshold_num

                settlement_price = 1.0 if yes_wins else 0.0

                pnl = asyncio.run(
                    self.paper_executor.settle_position(symbol, settlement_price)
                )
                if pnl is not None:
                    settled_count += 1
                    logger.info(
                        f"  Settled {symbol}: actual high={actual_high:.1f}°F, "
                        f"threshold={threshold_num:.0f}°F, "
                        f"{'YES' if yes_wins else 'NO'} wins → P&L ${pnl:.2f}"
                    )

            except Exception as e:
                logger.debug(f"Could not settle {symbol}: {e}")

        if settled_count > 0:
            logger.info(f"Settled {settled_count} expired positions this scan")

    def _fetch_actual_high(self, location: str, target_date: datetime) -> Optional[float]:
        """
        Fetch the actual observed high temperature for a past date.

        Uses NOAA gridpoint forecast data. For recent past dates, the NWS
        gridpoint data often still contains the actuals in the temperature
        time-series. If not available, returns None.

        Args:
            location: Location code (e.g., "NYC")
            target_date: The date to get the actual high for

        Returns:
            Actual high temperature in °F, or None if unavailable
        """
        if not self.weather_strategy:
            return None

        try:
            grid = self.weather_strategy._resolve_gridpoint(location)
            if not grid:
                return None

            office, gx, gy = grid
            props = self.weather_strategy._fetch_raw_gridpoint(office, gx, gy)
            if not props:
                return None

            # Try maxTemperature first (daily summary, °C)
            max_t_series = props.get("maxTemperature", {}).get("values", [])
            max_vals = self.weather_strategy._extract_values_for_date(
                max_t_series, target_date
            )
            if max_vals:
                actual_high_c = max(max_vals)
                return self.weather_strategy._c_to_f(actual_high_c)

            # Fallback: use hourly temperature series
            temp_series = props.get("temperature", {}).get("values", [])
            temp_vals = self.weather_strategy._extract_values_for_date(
                temp_series, target_date
            )
            if temp_vals:
                actual_high_c = max(temp_vals)
                return self.weather_strategy._c_to_f(actual_high_c)

            return None

        except Exception as e:
            logger.debug(f"Failed to fetch actual high for {location} on {target_date.date()}: {e}")
            return None

    def run_strategies(self) -> None:
        """Fetch live Kalshi markets and run all active strategies."""
        import time as time_module

        now = datetime.now()
        if self.last_signal_run and (now - self.last_signal_run).seconds < self.signal_interval_seconds:
            return  # Not time yet

        self.last_signal_run = now
        logger.info("--- Running strategy signal scan ---")

        # Settle any expired paper positions before generating new signals
        if self.paper_trading:
            self._settle_expired_positions()

        # Run weather strategy if active
        if self.weather_strategy and self.kalshi_executor:
            try:
                # Fetch live Kalshi weather markets by known series tickers
                # Correct Kalshi weather series use KXHIGH/KXLOW prefix
                logger.info("Fetching live Kalshi weather markets by series...")
                weather_series = [
                    "KXHIGHNY", "KXLOWNY",
                    "KXHIGHLAX", "KXLOWLAX",
                    "KXHIGHCHI", "KXLOWCHI",
                    "KXHIGHMIA", "KXHIGHDEN",
                    "KXHIGHHOU", "KXHIGHPHX",
                ]
                raw_markets = []
                for series in weather_series:
                    # No status filter — Kalshi uses 'initialized' for upcoming
                    # markets that haven't opened for trading yet
                    series_markets = asyncio.run(
                        self.kalshi_executor.get_markets(limit=20, series_ticker=series)
                    )
                    if series_markets:
                        raw_markets.extend(series_markets)
                        logger.info(f"  {series}: {len(series_markets)} markets found")

                # Fall back to broad fetch if series approach returns nothing
                if not raw_markets:
                    logger.info("Series fetch empty, trying broad fetch...")
                    raw_markets = asyncio.run(self.kalshi_executor.get_markets(limit=200))

                weather_keywords = [
                    "temperature", "temp", "rain", "snow", "precip", "weather",
                    "high", "low", "wind", "heat", "freeze", "frost", "degrees",
                ]
                weather_markets = []

                # Series → location code mapping for probability key matching
                series_to_location = {
                    "KXHIGHNY": "NYC", "KXLOWNY": "NYC",
                    "KXHIGHLAX": "LAX", "KXLOWLAX": "LAX",
                    "KXHIGHCHI": "CHI", "KXLOWCHI": "CHI",
                    "KXHIGHMIA": "MIA", "KXHIGHDEN": "DEN",
                    "KXHIGHHOU": "HOU", "KXHIGHPHX": "PHX",
                }

                if isinstance(raw_markets, list):
                    # Log sample tickers for debugging
                    sample_tickers = [m.ticker if hasattr(m, "ticker") else "" for m in raw_markets[:5]]
                    logger.info(f"Sample tickers: {sample_tickers}")

                    # Filter counters for diagnostics
                    _f_no_quotes = 0
                    _f_wide_spread = 0
                    _f_deep_itm_otm = 0
                    _f_expired = 0
                    _f_today = 0

                    for m in raw_markets:
                        # Handle both KalshiMarket dataclass and raw dict
                        if hasattr(m, "title"):
                            title = str(m.title).lower()
                            ticker = m.ticker
                            # Use bid/ask midpoint; fall back to last_price, then 50
                            bid = m.bid or 0
                            ask = m.ask or 0
                            last = m.last_price or 0
                            volume = m.volume or 0
                            if bid > 0 and ask > 0:
                                raw_price = (bid + ask) / 2
                            elif last > 0:
                                raw_price = last
                            elif ask > 0:
                                raw_price = ask
                            elif bid > 0:
                                raw_price = bid
                            else:
                                raw_price = None  # No price data — skip
                        else:
                            title = str(m.get("title", "")).lower()
                            ticker = m.get("ticker", m.get("id", ""))
                            bid = float(m.get("yes_bid_dollars") or m.get("yes_bid") or 0)
                            ask = float(m.get("yes_ask_dollars") or m.get("yes_ask") or 0)
                            last = float(m.get("last_price") or 0)
                            volume = int(m.get("volume", 0))
                            raw_price = m.get("last_price") or m.get("yes_ask") or None

                        # ── Liquidity filters ─────────────────────────────
                        # 1. No quotes AND no volume → completely illiquid, skip
                        if bid == 0 and ask == 0 and volume == 0:
                            logger.debug(f"Skipped (no quotes, no volume): {ticker}")
                            _f_no_quotes += 1
                            continue

                        # 2. Bid-ask spread check: if both sides are quoted, ensure
                        #    the spread isn't so wide that any "edge" is illusory.
                        #    Spread in the 0-1 dollar range; 0.40 = 40 cents = huge.
                        spread = (ask - bid) if (bid > 0 and ask > 0) else None
                        if spread is not None and spread > 0.40:
                            logger.debug(
                                f"Skipped (spread too wide: {spread:.2f}): {ticker}"
                            )
                            _f_wide_spread += 1
                            continue

                        # For initialized markets (not yet open), prices are null.
                        # Use neutral 50 so we can still generate pre-open signals.
                        if raw_price is None:
                            price = 50.0
                            logger.debug(f"No price yet ({getattr(m,'status','?')}), using neutral 50: {ticker}")
                        else:
                            # Normalize to 0-100 scale (Kalshi prices are in cents: 0-100)
                            price = raw_price * 100 if raw_price < 1.0 else raw_price

                        # 3. Deep ITM/OTM filter: contracts priced below 3 or above 97
                        #    have no real price discovery — skip them.
                        if price < 3.0 or price > 97.0:
                            logger.debug(
                                f"Skipped (deep {'OTM' if price < 3 else 'ITM'}, "
                                f"price={price:.1f}c): {ticker}"
                            )
                            _f_deep_itm_otm += 1
                            continue

                        # Parse KXHIGH/KXLOW tickers into probability metric keys
                        # Kalshi ticker format: KXHIGHNY-26MAR25-T58
                        #   where 26MAR25 uses Kalshi fiscal year (25 = FY25 = ~2026).
                        # We use close_time from the API for the actual settlement date
                        # and only use the ticker for threshold parsing.
                        import re as _re
                        metric = ticker  # default
                        contract_date = None

                        # Determine contract date from API close_time first (most reliable)
                        close_time_str = getattr(m, "close_time", None)
                        if close_time_str:
                            try:
                                contract_date = datetime.strptime(
                                    close_time_str[:10], "%Y-%m-%d"
                                )
                            except ValueError:
                                pass

                        try:
                            parts = ticker.split("-")
                            if len(parts) >= 3:
                                series = parts[0]
                                threshold_str = parts[2]

                                location = series_to_location.get(series, "")
                                # Determine if this is an ABOVE (T) or BELOW (B) contract
                                contract_direction = threshold_str[0].upper() if threshold_str else "T"
                                is_below_contract = contract_direction == "B"

                                # Strip any leading alphabetic characters (T, B, A, etc.)
                                threshold_num = _re.sub(r'^[A-Za-z]+', '', threshold_str)
                                threshold = float(threshold_num)

                                # Always parse date from ticker (Kalshi format: YYMmmDD e.g. 26MAR24)
                                # close_time reflects series-level close (same for all contracts in
                                # a batch), so it can't be used to identify individual trading dates.
                                date_str = parts[1]
                                ticker_date = datetime.strptime(date_str, "%y%b%d")
                                contract_date = ticker_date  # override close_time-derived date
                                date_key = contract_date.strftime("%Y%m%d")

                                if location:
                                    if is_below_contract:
                                        # B contracts: YES pays if temp BELOW threshold.
                                        # Store as TEMP_BELOW so signal logic can invert probability.
                                        metric = f"{location}_{date_key}_TEMP_BELOW_{int(threshold)}"
                                    else:
                                        metric = f"{location}_{date_key}_TEMP_ABOVE_{int(threshold)}"
                        except Exception as parse_err:
                            logger.debug(f"Ticker parse skipped ({ticker}): {parse_err}")

                        # Skip contracts already past their settlement date
                        if contract_date and contract_date.date() < now.date():
                            logger.debug(f"Skipped (settled {contract_date.date()}): {ticker}")
                            _f_expired += 1
                            continue

                        # Skip same-day contracts only within 1 hour of close time.
                        # Before that window the forecast is still meaningful and MM quotes
                        # are still live. Default to skipping if close_time is unavailable.
                        if contract_date and contract_date.date() == now.date():
                            skip_today = True  # conservative default
                            if close_time_str:
                                try:
                                    close_dt = datetime.strptime(
                                        close_time_str, "%Y-%m-%dT%H:%M:%SZ"
                                    ).replace(tzinfo=timezone.utc)
                                    now_utc = datetime.now(timezone.utc)
                                    mins_until_close = (close_dt - now_utc).total_seconds() / 60
                                    if mins_until_close > 60:
                                        skip_today = False  # still >1h to close, keep it
                                    else:
                                        logger.debug(
                                            f"Skipped (expires today, {mins_until_close:.0f}m to close): {ticker}"
                                        )
                                except (ValueError, Exception):
                                    pass  # unparseable close_time → default skip
                            if skip_today:
                                if not close_time_str:
                                    logger.debug(f"Skipped (expires today, no close_time): {ticker}")
                                _f_today += 1
                                continue

                        weather_markets.append({
                            "contract_id": ticker,
                            "price": price,
                            "metric": metric,
                            "title": title,
                            "bid": bid,
                            "ask": ask,
                            "last": last,
                            "volume": volume,
                            "spread": spread,
                        })

                total_raw = len(raw_markets) if isinstance(raw_markets, list) else 0
                total_filtered = _f_no_quotes + _f_wide_spread + _f_deep_itm_otm + _f_expired + _f_today
                vol_zero = sum(1 for m in weather_markets if m.get("volume", 0) == 0)
                vol_positive = len(weather_markets) - vol_zero
                logger.info(
                    f"Found {len(weather_markets)} weather markets on Kalshi "
                    f"(filtered {total_filtered}/{total_raw}: "
                    f"no_quotes={_f_no_quotes}, wide_spread={_f_wide_spread}, "
                    f"deep_itm_otm={_f_deep_itm_otm}, expired={_f_expired}, "
                    f"same_day={_f_today})"
                )
                logger.info(
                    f"Volume breakdown: {vol_positive} with trades, "
                    f"{vol_zero} zero-volume (MM quotes only)"
                )

                if not weather_markets:
                    # Use next calendar day so mock signals target a future date that
                    # actually has NOAA forecast data (today+1 always has forecasts).
                    mock_date = (now + timedelta(days=1)).strftime("%Y%m%d")
                    logger.info(
                        f"No weather markets found — running with mock market data for testing "
                        f"(mock date: {mock_date})"
                    )
                    weather_markets = [
                        {"contract_id": "MOCK_TEMP_NYC_70", "price": 45, "metric": f"NYC_{mock_date}_TEMP_ABOVE_70", "title": "Mock: NYC temp above 70"},
                        {"contract_id": "MOCK_TEMP_NYC_75", "price": 30, "metric": f"NYC_{mock_date}_TEMP_ABOVE_75", "title": "Mock: NYC temp above 75"},
                        {"contract_id": "MOCK_PRECIP_NYC",  "price": 25, "metric": f"NYC_{mock_date}_PRECIP_ABOVE_0.1", "title": "Mock: NYC precip > 0.1in"},
                    ]

                # Extract unique locations and dates from actual Kalshi markets
                active_locations = list(set(
                    series_to_location.get(m["metric"].split("_")[0] if "_" in m["metric"] else "", "")
                    or m["metric"].split("_")[0]
                    for m in weather_markets
                ) - {""}) or ["NYC", "LAX", "CHI"]

                # Extract target dates from market metrics (e.g. NYC_20260325_...)
                active_dates = set()
                for m in weather_markets:
                    parts = m["metric"].split("_")
                    if len(parts) >= 2 and len(parts[1]) == 8:
                        try:
                            active_dates.add(datetime.strptime(parts[1], "%Y%m%d"))
                        except ValueError:
                            pass
                if not active_dates:
                    active_dates = {now + timedelta(days=i) for i in range(1, 4)}
                target_dates = sorted(active_dates)

                # Also extract thresholds and pre-calculate probabilities for them.
                # BELOW contracts need the ABOVE probability to invert, so we
                # extract thresholds from both _TEMP_ABOVE_ and _TEMP_BELOW_ metrics.
                thresholds_needed = set()
                for m in weather_markets:
                    for tag in ("_TEMP_ABOVE_", "_TEMP_BELOW_"):
                        parts = m["metric"].split(tag)
                        if len(parts) == 2:
                            try:
                                thresholds_needed.add(float(parts[1]))
                            except ValueError:
                                pass
                if not thresholds_needed:
                    thresholds_needed = {45, 50, 55, 60, 65, 70, 75, 80, 85, 90}
                logger.info(f"Temperature thresholds in play: {sorted(thresholds_needed)}")
                logger.info(f"Active locations: {active_locations} | Dates: {[d.strftime('%Y-%m-%d') for d in target_dates]}")

                # Inject dynamic thresholds into strategy before generating signals
                self.weather_strategy._dynamic_thresholds = sorted(thresholds_needed)

                signals = self.weather_strategy.generate_signals(
                    locations=active_locations,
                    target_dates=target_dates,
                    kalshi_markets=weather_markets,
                )

                # Log signals clearly
                # Build a lookup of liquidity data by contract_id for logging
                liquidity_info = {
                    m["contract_id"]: m for m in weather_markets
                }

                if signals:
                    logger.info(f"*** {len(signals)} ACTIONABLE SIGNALS FOUND ***")
                    for sig in signals:
                        liq = liquidity_info.get(sig.contract_id, {})
                        bid_str = f"{liq.get('bid', 0):.2f}" if liq.get('bid') else "–"
                        ask_str = f"{liq.get('ask', 0):.2f}" if liq.get('ask') else "–"
                        vol_str = str(liq.get('volume', 0))
                        spread_str = f"{liq.get('spread', 0):.2f}" if liq.get('spread') is not None else "n/a"
                        logger.info(
                            f"  [{sig.signal.value}] {sig.contract_id} | "
                            f"Model: {sig.model_probability:.1%} | "
                            f"Market: {sig.market_probability:.1%} | "
                            f"Edge: {sig.edge:.0f}bps | "
                            f"Confidence: {sig.confidence:.1%} | "
                            f"Bid/Ask: {bid_str}/{ask_str} Spread: {spread_str} Vol: {vol_str}"
                        )

                    # ── Log signals to outcome tracker ───────────────────
                    if self.outcome_tracker:
                        # Build a quick lookup: metric → contract_date from market list
                        metric_to_date: dict = {}
                        for _m in weather_markets:
                            _metric = _m.get("metric", "")
                            _parts = _metric.split("_")
                            if len(_parts) >= 2 and len(_parts[1]) == 8:
                                try:
                                    metric_to_date[_metric] = datetime.strptime(_parts[1], "%Y%m%d")
                                except ValueError:
                                    pass

                        for sig in signals:
                            # Parse contract_date from weather_metric (e.g. NYC_20260326_TEMP_ABOVE_58)
                            _cdate = metric_to_date.get(sig.weather_metric)
                            if _cdate is None:
                                _mp = sig.weather_metric.split("_")
                                if len(_mp) >= 2 and len(_mp[1]) == 8:
                                    try:
                                        _cdate = datetime.strptime(_mp[1], "%Y%m%d")
                                    except ValueError:
                                        pass
                            if _cdate is None:
                                _cdate = now + timedelta(days=1)

                            self.outcome_tracker.log_signal({
                                "ticker": sig.contract_id,
                                "direction": sig.signal.value,
                                "model_prob": sig.model_probability,
                                "market_prob": sig.market_probability,
                                "edge_bps": sig.edge,
                                "confidence": sig.confidence,
                                "position_size": sig.recommended_position_size,
                                "contract_date": _cdate,
                                "weather_metric": sig.weather_metric,
                                "model_source": "OPEN_METEO_ECMWF",
                                "is_paper": 1 if self.paper_trading else 0,
                            })

                    # ── Paper execution ──────────────────────────────────
                    if self.paper_trading and self.paper_executor and self.daily_limits:
                        self._execute_paper_trades(signals, weather_markets)

                else:
                    logger.info("No actionable signals this scan (edge below threshold)")

                # Register heartbeat so health checker knows strategy is alive
                if self.health_checker:
                    self.health_checker.record_heartbeat("kalshi_weather")

            except Exception as e:
                logger.error(f"WeatherStrategy error: {e}", exc_info=True)

        # ── Crypto Funding Rate Arb ───────────────────────────────────────
        # Scan every 30 minutes (funding resets every 8 hours, no need to poll faster).
        if self.funding_arb_strategy:
            run_funding = (
                self.last_funding_scan is None
                or (now - self.last_funding_scan).total_seconds() >= self.funding_scan_interval_seconds
            )
            if run_funding:
                self.last_funding_scan = now
                try:
                    logger.info("Scanning crypto funding rates...")
                    snapshots = self.funding_arb_strategy.scan_rates()
                    self.funding_arb_strategy.print_rate_table(snapshots)
                    opportunities = self.funding_arb_strategy.find_opportunities(snapshots)
                    if opportunities:
                        logger.info(
                            f"[FUNDING ARB] {len(opportunities)} opportunity(s) above "
                            f"{self.funding_arb_strategy.MIN_NET_YIELD:.0%} threshold"
                        )
                        for opp in opportunities:
                            signal = self.funding_arb_strategy.generate_signals()
                            if signal.signal:
                                self.funding_arb_strategy.execute_trade(signal)
                    else:
                        logger.info("[FUNDING ARB] No opportunities above threshold this scan")
                    self.funding_arb_strategy.print_open_positions()
                except Exception as e:
                    logger.error(f"FundingArbStrategy error: {e}", exc_info=True)

        # ── Periodic settlement check ─────────────────────────────────────
        # Run once per hour to detect settled contracts and record outcomes.
        if self.outcome_tracker and self.kalshi_executor:
            run_settle = (
                self.last_settle_run is None
                or (now - self.last_settle_run).total_seconds() >= self.settle_interval_seconds
            )
            if run_settle:
                self.last_settle_run = now
                # Wrap async get_market_by_ticker so outcome_tracker can call it synchronously
                _ke = self.kalshi_executor

                class _SyncExecutorProxy:
                    """Thin synchronous proxy around async KalshiExecutor."""
                    def get_market_by_ticker(self, ticker: str):
                        return asyncio.run(_ke.get_market_by_ticker(ticker))

                settled = self.outcome_tracker.check_and_settle(_SyncExecutorProxy())
                if settled > 0:
                    logger.info(f"[OUTCOME] Settled {settled} contract(s) this cycle")

        logger.info("--- Signal scan complete ---")

        # ── Auto-refresh Excel dashboard ─────────────────────────────────
        try:
            report_path = generate_report()
            logger.info(f"[REPORT] Dashboard refreshed → {report_path}")
        except Exception as _report_err:
            logger.warning(f"[REPORT] Could not refresh dashboard: {_report_err}")

        # ── Paper P&L summary ────────────────────────────────────────────
        if self.paper_trading and self.paper_executor:
            realized, unrealized = asyncio.run(self.paper_executor.get_pnl())
            portfolio_val = asyncio.run(self.paper_executor.get_total_portfolio_value())
            positions = asyncio.run(self.paper_executor.get_positions())
            returns = asyncio.run(self.paper_executor.get_returns())
            logger.info(
                f"[PAPER P&L] Portfolio: ${portfolio_val:.2f} | "
                f"Realized: ${realized:.2f} | Unrealized: ${unrealized:.2f} | "
                f"Return: {returns:.2f}% | Open positions: {len(positions)}"
            )
            if positions:
                for sym, pos in positions.items():
                    logger.info(
                        f"  Position: {pos.side} {pos.quantity}x {pos.symbol} "
                        f"@ ${pos.entry_price:.2f} → ${pos.current_price:.2f} "
                        f"(P&L: ${pos.unrealized_pnl:.2f})"
                    )

            # Persist state so positions survive restarts
            state_file = getattr(self, "_paper_state_file", "logs/paper_state.json")
            self.paper_executor.save_state(state_file)

    def _execute_paper_trades(
        self,
        signals: list,
        weather_markets: list,
    ) -> None:
        """
        Execute paper trades for actionable signals through the risk gate.

        For each signal:
        1. Run pre-trade risk check (daily_limits.can_trade)
        2. If approved, place paper order via PaperExecutor
        3. Record the trade in DailyLimits for ongoing tracking

        Kalshi contracts trade 0-100 (cents per contract, each contract pays $1).
        Position sizing: we buy N contracts where N is scaled by edge and confidence,
        capped by risk limits. Price is in cents (0-100).
        """
        from strategies.kalshi_weather.weather_strategy import SignalType

        # Build lookups from weather_markets
        market_prices = {m["contract_id"]: m["price"] for m in weather_markets}
        market_volumes = {m["contract_id"]: m.get("volume", 0) for m in weather_markets}
        # Bid/ask in dollars — used for limit order pricing
        # BUY limit = bid (we post below the ask, let market come to us)
        # SELL limit = ask (we post above the bid, let market come to us)
        market_bids = {m["contract_id"]: m.get("bid") for m in weather_markets}
        market_asks = {m["contract_id"]: m.get("ask") for m in weather_markets}

        # Capital discipline: cap how many positions we open per scan.
        # Signals are already sorted by edge (highest first), so we take the best.
        MAX_NEW_POSITIONS_PER_SCAN = 8
        existing_positions = asyncio.run(self.paper_executor.get_positions())
        MAX_TOTAL_OPEN = 25  # Never hold more than 25 contracts at once

        executed = 0
        skipped_risk = 0
        skipped_other = 0
        skipped_cap = 0
        skipped_no_vol = 0

        for sig in signals:
            if sig.signal not in (SignalType.BUY, SignalType.SELL):
                continue

            # Position cap checks
            if executed >= MAX_NEW_POSITIONS_PER_SCAN:
                skipped_cap += 1
                continue
            if (len(existing_positions) + executed) >= MAX_TOTAL_OPEN:
                skipped_cap += 1
                continue
            # Don't double up on contracts we already hold
            if sig.contract_id in existing_positions:
                logger.debug(f"  Skipping {sig.contract_id} — already in portfolio")
                skipped_cap += 1
                continue

            # Price in cents (0-100); each contract costs price cents, pays 100 cents
            price_cents = market_prices.get(sig.contract_id, sig.market_probability * 100)
            price_dollars = price_cents / 100.0  # Mid price in dollars (for sizing/risk)

            # Limit order price: BUY at bid, SELL at ask.
            # We are the passive/maker side — we post and wait for the market to fill us.
            # Fall back to mid price if bid/ask is unavailable.
            bid_dollars = market_bids.get(sig.contract_id)
            ask_dollars = market_asks.get(sig.contract_id)
            if sig.signal.value == "BUY":
                limit_price = bid_dollars if bid_dollars else price_dollars
            else:  # SELL
                limit_price = ask_dollars if ask_dollars else price_dollars

            # ── Volume-based liquidity discount ───────────────────────
            # Zero-volume contracts have only MM quotes — prices may not
            # be executable. Apply a steep discount to position size.
            contract_vol = market_volumes.get(sig.contract_id, 0)
            if contract_vol == 0:
                vol_discount = 0.25  # 75% haircut for zero-volume
            elif contract_vol < 10:
                vol_discount = 0.50  # 50% haircut for thin
            elif contract_vol < 50:
                vol_discount = 0.75  # 25% haircut for moderate
            else:
                vol_discount = 1.0   # Full sizing for liquid

            # Position sizing: scale contracts by edge, confidence, and liquidity
            # Base: 5 contracts, scale up to 20 for strong signals
            base_contracts = 5
            edge_multiplier = min(sig.edge / self.weather_strategy.min_edge_bps, 4.0)
            quantity = max(1, int(base_contracts * edge_multiplier * sig.confidence * vol_discount))

            # Notional = quantity × price_dollars (cost to enter)
            notional = quantity * price_dollars

            # Set market price for paper executor
            self.paper_executor.set_market_price(sig.contract_id, limit_price)

            # ── Risk check ───────────────────────────────────────────
            allowed, violation, reason = asyncio.run(
                self.daily_limits.can_trade(
                    symbol=sig.contract_id,
                    side=sig.signal.value,
                    quantity=quantity,
                    price=limit_price,
                )
            )

            if not allowed:
                logger.warning(
                    f"  [RISK BLOCKED] {sig.contract_id}: {reason}"
                )
                skipped_risk += 1
                continue

            # ── Execute paper order (LIMIT / maker) ──────────────────
            # We post at bid (BUY) or ask (SELL) — passive side, $0 Kalshi fee.
            result = asyncio.run(
                self.paper_executor.place_order(
                    symbol=sig.contract_id,
                    side=sig.signal.value,
                    quantity=quantity,
                    price=limit_price,
                    order_type="LIMIT",
                )
            )

            if result.success:
                # Record in risk tracker
                self.daily_limits.record_trade(
                    symbol=sig.contract_id,
                    side=sig.signal.value,
                    quantity=quantity,
                    price=limit_price,
                )
                executed += 1
                vol_tag = f"vol={contract_vol}" if contract_vol > 0 else "ZERO-VOL"
                logger.info(
                    f"  [PAPER LIMIT] {sig.signal.value} {quantity}x {sig.contract_id} "
                    f"@ ${result.executed_price:.4f} [maker/limit] (edge={sig.edge:.0f}bps, "
                    f"{vol_tag}, liq_disc={vol_discount:.0%}) "
                    f"| Order {result.order_id[:8]}..."
                )
            else:
                skipped_other += 1
                logger.warning(
                    f"  [PAPER REJECTED] {sig.contract_id}: {result.message}"
                )

        logger.info(
            f"Paper execution summary: {executed} filled, "
            f"{skipped_risk} risk-blocked, {skipped_cap} position-capped, "
            f"{skipped_other} rejected"
        )

    def run_health_checks(self) -> None:
        """Run system health checks."""
        if not self.health_checker:
            logger.warning("Health checker not initialized")
            return

        logger.info("Running health checks...")
        report = self.health_checker.run_all_checks()

        logger.info(f"Overall health status: {report.overall_status.value}")
        for check_name, result in report.checks.items():
            logger.info(f"  {check_name}: {result.status.value} - {result.message}")

        # Alert on critical issues
        if report.overall_status == "CRITICAL":
            self.alert_manager.send_alert(
                AlertLevel.CRITICAL,
                f"System health degraded: {report.overall_status.value}",
                context={"checks": {k: v.status.value for k, v in report.checks.items()}},
            )

    def refresh_dashboard(self) -> None:
        """Refresh and display the dashboard."""
        if not self.dashboard:
            logger.warning("Dashboard not initialized")
            return

        # Gather current data (mock data in this example)
        positions = [
            {
                "symbol": "AAPL",
                "quantity": 100,
                "avg_price": 150.25,
                "current_price": 152.50,
                "pnl": 225.00,
                "pnl_percent": 1.50,
            }
        ]

        daily_pnl = 1250.00
        daily_pnl_percent = 0.85

        strategy_status = {
            "WeatherStrategy": "RUNNING",
            "EconomicStrategy": "RUNNING",
        }

        recent_trades = [
            {
                "timestamp": datetime.now().isoformat(),
                "symbol": "AAPL",
                "side": "BUY",
                "quantity": 100,
                "price": 150.25,
                "status": "FILLED",
            }
        ]

        health_report = self.health_checker.run_all_checks() if self.health_checker else None
        system_health = {
            k: {"status": v.status.value, "message": v.message}
            for k, v in (health_report.checks.items() if health_report else {}.items())
        }

        self.dashboard.refresh(
            positions=positions,
            daily_pnl=daily_pnl,
            daily_pnl_percent=daily_pnl_percent,
            strategy_status=strategy_status,
            recent_trades=recent_trades,
            system_health=system_health,
        )

        if self.show_dashboard:
            print(self.dashboard.display())

    def main_loop(self) -> None:
        """Main trading loop."""
        self.running = True
        logger.info("Starting main trading loop...")

        try:
            while self.running:
                # Periodic health checks
                self.run_health_checks()

                # Update dashboard
                self.refresh_dashboard()

                # Run strategy signal scans
                self.run_strategies()

                # Sleep to prevent busy-waiting
                import time

                time.sleep(30)

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            if self.alert_manager:
                self.alert_manager.send_alert(
                    AlertLevel.CRITICAL,
                    f"Main loop error: {e}",
                )
            raise
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully shutdown the trading system."""
        self.running = False
        logger.info("Shutting down trading system...")

        try:
            # Cancel any pending orders (placeholder)
            logger.info("Cancelling pending orders...")

            # Close database connections
            logger.info("Closing database connections...")

            # Log final status
            logger.info("Trading system shutdown complete")

        except Exception as e:
            logger.error(f"Error during shutdown: {e}", exc_info=True)

    def handle_signal(self, signum: int, frame: Any) -> None:
        """Handle system signals (SIGINT, SIGTERM).

        Args:
            signum: Signal number.
            frame: Stack frame.
        """
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
        sys.exit(0)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Algorithmic Trading Desk",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # Run with live trading
  python main.py --paper            # Run in paper trading mode
  python main.py --strategy Weather # Run only Weather strategy
  python main.py --dashboard        # Show live dashboard
        """,
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config/settings.yaml",
        help="Path to configuration file (default: config/settings.yaml)",
    )

    parser.add_argument(
        "--paper",
        action="store_true",
        help="Enable paper trading mode (no real orders)",
    )

    parser.add_argument(
        "--strategy",
        type=str,
        help="Run only specific strategy by name",
    )

    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Display live dashboard on startup",
    )

    args = parser.parse_args()

    # Initialize application
    app = TradingDeskApplication(
        config_path=args.config,
        paper_trading=args.paper,
        strategy_filter=args.strategy,
        show_dashboard=args.dashboard,
    )

    # Setup logging
    app.setup_logging()

    logger.info("=" * 80)
    logger.info("ALGORITHMIC TRADING DESK STARTUP")
    logger.info("=" * 80)
    logger.info(f"Paper trading: {app.paper_trading}")
    logger.info(f"Strategy filter: {app.strategy_filter}")
    logger.info(f"Dashboard: {app.show_dashboard}")

    # ── Single-instance lock ──────────────────────────────────────────────────
    # Prevent two copies of the bot running simultaneously and double-trading.
    import fcntl as _fcntl
    _lock_path = Path("logs/trading_desk.lock")
    _lock_path.parent.mkdir(exist_ok=True)
    _lock_file = open(_lock_path, "w")
    try:
        _fcntl.flock(_lock_file, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
    except OSError:
        logger.critical(
            "Another instance of the trading desk is already running. "
            "Kill it first (check 'ps aux | grep main.py') before starting a new one."
        )
        sys.exit(1)

    try:
        # Setup signal handlers
        signal.signal(signal.SIGINT, app.handle_signal)
        signal.signal(signal.SIGTERM, app.handle_signal)

        # Initialize and start
        app.load_config()
        app.initialize_components()
        app.initialize_strategies()
        app.main_loop()

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        _fcntl.flock(_lock_file, _fcntl.LOCK_UN)
        _lock_file.close()


if __name__ == "__main__":
    main()
