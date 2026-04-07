"""
Base Strategy Module

This module provides the abstract base class for all trading strategies.
All concrete strategy implementations must inherit from BaseStrategy and implement
the required abstract methods.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml


# Configure logging
logger = logging.getLogger(__name__)


@dataclass
class StrategyResult:
    """
    Dataclass representing the result of a strategy signal generation.

    Attributes:
        signal (bool): Whether a trading signal was generated
        confidence (float): Confidence level of the signal (0.0 to 1.0)
        side (str): Trade side - 'BUY', 'SELL', or 'HOLD'
        size (int): Position size in units
        metadata (Dict[str, Any]): Additional metadata about the signal
    """

    signal: bool
    confidence: float
    side: str
    size: int
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Validate StrategyResult on initialization."""
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError("Confidence must be between 0.0 and 1.0")
        if self.side not in ("BUY", "SELL", "HOLD"):
            raise ValueError("Side must be 'BUY', 'SELL', or 'HOLD'")
        if self.size < 0:
            raise ValueError("Size must be non-negative")


class BaseStrategy(ABC):
    """
    Abstract base class for all trading strategies.

    This class defines the interface and provides common functionality for all
    strategy implementations. Subclasses must implement the abstract methods:
    - generate_signals()
    - execute_trade()
    - calculate_position_size()

    Attributes:
        name (str): Name of the strategy
        enabled (bool): Whether the strategy is enabled
        paper_mode (bool): Whether running in paper trading mode
        config (Dict[str, Any]): Strategy configuration dictionary
    """

    def __init__(
        self,
        name: str,
        enabled: bool = True,
        paper_mode: bool = True,
        config_path: Optional[str] = None,
    ) -> None:
        """
        Initialize the base strategy.

        Args:
            name (str): Name of the strategy
            enabled (bool): Whether the strategy is enabled. Defaults to True.
            paper_mode (bool): Whether to run in paper trading mode. Defaults to True.
            config_path (Optional[str]): Path to strategy configuration file.
        """
        self._name = name
        self._enabled = enabled
        self._paper_mode = paper_mode
        self._config: Dict[str, Any] = {}
        self._trades_log: List[Dict[str, Any]] = []

        logger.info(
            f"Initializing strategy '{name}' (enabled={enabled}, "
            f"paper_mode={paper_mode})"
        )

        if config_path:
            self._config = self.load_config(config_path)

    @property
    def name(self) -> str:
        """Get the strategy name."""
        return self._name

    @property
    def enabled(self) -> bool:
        """Get whether the strategy is enabled."""
        return self._enabled

    @property
    def paper_mode(self) -> bool:
        """Get whether the strategy is in paper trading mode."""
        return self._paper_mode

    @enabled.setter
    def enabled(self, value: bool) -> None:
        """Set whether the strategy is enabled."""
        self._enabled = value
        logger.info(f"Strategy '{self._name}' enabled set to {value}")

    @abstractmethod
    def generate_signals(self) -> StrategyResult:
        """
        Generate trading signals based on strategy logic.

        This method must be implemented by subclasses to produce trading signals
        based on market data and strategy parameters.

        Returns:
            StrategyResult: The result containing signal information
        """
        pass

    @abstractmethod
    def execute_trade(self, signal: StrategyResult) -> bool:
        """
        Execute a trade based on the generated signal.

        This method must be implemented by subclasses to execute trades via
        the appropriate broker or exchange API.

        Args:
            signal (StrategyResult): The signal to execute

        Returns:
            bool: True if trade was executed successfully, False otherwise
        """
        pass

    @abstractmethod
    def calculate_position_size(self, signal: StrategyResult) -> int:
        """
        Calculate the position size for a trade.

        This method must be implemented by subclasses to determine appropriate
        position sizing based on risk management rules and signal confidence.

        Args:
            signal (StrategyResult): The signal to size

        Returns:
            int: The position size in units
        """
        pass

    def run(self) -> None:
        """
        Main run loop for the strategy.

        This method orchestrates the strategy execution:
        1. Checks if strategy is enabled
        2. Generates trading signals
        3. Filters signals by risk limits
        4. Calculates position size
        5. Executes trades
        6. Logs results
        """
        if not self._enabled:
            logger.debug(f"Strategy '{self._name}' is disabled, skipping run")
            return

        try:
            logger.info(f"Running strategy '{self._name}'")

            # Generate signals
            signal = self.generate_signals()

            if not signal.signal:
                logger.debug(f"No signal generated by '{self._name}'")
                return

            logger.info(
                f"Signal generated: side={signal.side}, "
                f"confidence={signal.confidence:.2%}"
            )

            # Calculate position size
            position_size = self.calculate_position_size(signal)
            signal.size = position_size

            # Filter by risk limits (would be implemented with risk manager)
            # This is a placeholder for integration with risk management system
            if position_size == 0:
                logger.warning(
                    f"Position size calculated as 0 for signal in '{self._name}'"
                )
                return

            # Execute trade
            success = self.execute_trade(signal)

            if success:
                self.log_trade(signal)
                logger.info(
                    f"Trade executed successfully in '{self._name}': "
                    f"{signal.side} {signal.size} units"
                )
            else:
                logger.error(f"Trade execution failed in '{self._name}'")

        except Exception as e:
            logger.error(
                f"Error in strategy '{self._name}' run: {str(e)}", exc_info=True
            )

    def log_trade(self, signal: StrategyResult) -> None:
        """
        Log a trade to the trades log.

        Args:
            signal (StrategyResult): The signal that was executed
        """
        trade_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "strategy": self._name,
            "side": signal.side,
            "size": signal.size,
            "confidence": signal.confidence,
            "paper_mode": self._paper_mode,
            "metadata": signal.metadata,
        }
        self._trades_log.append(trade_record)
        logger.debug(f"Trade logged: {trade_record}")

    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from a YAML file.

        Args:
            config_path (str): Path to the configuration file

        Returns:
            Dict[str, Any]: Configuration dictionary

        Raises:
            FileNotFoundError: If configuration file not found
            yaml.YAMLError: If configuration file is invalid YAML
        """
        try:
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {config_path}")
            return config if config else {}
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {str(e)}")
            raise

    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Calculate and return performance statistics for the strategy.

        Returns:
            Dict[str, Any]: Dictionary containing performance metrics

        Metrics included:
            - total_trades: Total number of trades executed
            - winning_trades: Number of profitable trades
            - losing_trades: Number of losing trades
            - win_rate: Percentage of profitable trades
            - avg_size: Average position size
        """
        if not self._trades_log:
            logger.warning(f"No trades recorded for strategy '{self._name}'")
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_size": 0.0,
            }

        total_trades = len(self._trades_log)
        sizes = [trade["size"] for trade in self._trades_log]
        avg_size = sum(sizes) / len(sizes) if sizes else 0.0

        stats = {
            "total_trades": total_trades,
            "winning_trades": sum(
                1 for trade in self._trades_log if trade["side"] == "BUY"
            ),
            "losing_trades": sum(
                1 for trade in self._trades_log if trade["side"] == "SELL"
            ),
            "win_rate": (
                sum(1 for trade in self._trades_log if trade["side"] == "BUY")
                / total_trades
            ),
            "avg_size": avg_size,
        }

        logger.info(f"Performance stats for '{self._name}': {stats}")
        return stats

    def get_trades_log(self) -> List[Dict[str, Any]]:
        """
        Get the complete trades log.

        Returns:
            List[Dict[str, Any]]: List of trade records
        """
        return self._trades_log.copy()
