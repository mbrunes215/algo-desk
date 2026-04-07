"""Pytest configuration and shared fixtures for trading desk tests."""

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Generator

import pytest

from data.storage import (
    SessionFactory,
    Trade,
    TradeStatus,
    TradeType,
    Position,
    PositionStatus,
    MarketData,
    StrategySignal,
    Alert,
)
from data.pipelines import MarketDataPoint
from monitoring import AlertManager, HealthChecker


@pytest.fixture(scope="session")
def test_db_path() -> str:
    """Path for test database."""
    return "test_trading.db"


@pytest.fixture
def test_database(test_db_path: str) -> Generator[str, None, None]:
    """Create and cleanup test database.

    Yields:
        Path to test database.
    """
    # Remove if exists
    if os.path.exists(test_db_path):
        os.remove(test_db_path)

    # Initialize database
    db_url = f"sqlite:///{test_db_path}"
    SessionFactory.initialize(db_url)

    yield test_db_path

    # Cleanup
    if os.path.exists(test_db_path):
        os.remove(test_db_path)


@pytest.fixture
def db_session(test_database: str):
    """Get a database session for testing.

    Args:
        test_database: Path to test database.

    Yields:
        SQLAlchemy session.
    """
    session = SessionFactory.get_session()
    yield session
    session.close()


@pytest.fixture
def sample_trade() -> Dict[str, Any]:
    """Create sample trade data.

    Returns:
        Trade data dictionary.
    """
    return {
        "strategy_name": "TestStrategy",
        "symbol": "AAPL",
        "trade_type": TradeType.BUY,
        "quantity": 100,
        "price": 150.25,
        "status": TradeStatus.FILLED,
        "order_id": "TEST-001",
        "execution_timestamp": datetime.utcnow(),
    }


@pytest.fixture
def sample_position() -> Dict[str, Any]:
    """Create sample position data.

    Returns:
        Position data dictionary.
    """
    return {
        "strategy_name": "TestStrategy",
        "symbol": "AAPL",
        "quantity": 100,
        "average_price": 150.25,
        "current_price": 152.50,
        "status": PositionStatus.OPEN,
        "realized_pnl": None,
    }


@pytest.fixture
def sample_market_data_point() -> MarketDataPoint:
    """Create sample market data point.

    Returns:
        MarketDataPoint instance.
    """
    now = datetime.utcnow()
    return MarketDataPoint(
        symbol="AAPL",
        timestamp=now,
        open=150.0,
        high=155.0,
        low=149.0,
        close=152.50,
        volume=5000000,
    )


@pytest.fixture
def sample_market_data_points() -> list[MarketDataPoint]:
    """Create sample market data points for a period.

    Returns:
        List of MarketDataPoint instances.
    """
    points = []
    base_date = datetime.utcnow() - timedelta(days=10)
    price = 150.0

    for i in range(10):
        timestamp = base_date + timedelta(days=i)
        close_price = price + (i * 0.5)

        points.append(
            MarketDataPoint(
                symbol="AAPL",
                timestamp=timestamp,
                open=price,
                high=close_price + 2.0,
                low=close_price - 2.0,
                close=close_price,
                volume=5000000 + (i * 100000),
            )
        )
        price = close_price

    return points


@pytest.fixture
def alert_manager() -> AlertManager:
    """Create an AlertManager instance for testing.

    Returns:
        AlertManager instance with email disabled.
    """
    return AlertManager(
        enable_email=False,
        to_emails=["test@example.com"],
    )


@pytest.fixture
def health_checker() -> HealthChecker:
    """Create a HealthChecker instance for testing.

    Returns:
        HealthChecker instance.
    """
    return HealthChecker(
        ibkr_host="127.0.0.1",
        ibkr_port=7497,
        database_url="sqlite:///test_trading.db",
    )


@pytest.fixture
def mock_positions() -> list[Dict[str, Any]]:
    """Create mock positions for dashboard testing.

    Returns:
        List of position dictionaries.
    """
    return [
        {
            "symbol": "AAPL",
            "quantity": 100,
            "avg_price": 150.25,
            "current_price": 152.50,
            "pnl": 225.00,
            "pnl_percent": 1.50,
        },
        {
            "symbol": "GOOGL",
            "quantity": 50,
            "avg_price": 2800.00,
            "current_price": 2750.00,
            "pnl": -2500.00,
            "pnl_percent": -1.79,
        },
    ]


@pytest.fixture
def mock_trades() -> list[Dict[str, Any]]:
    """Create mock trades for dashboard testing.

    Returns:
        List of trade dictionaries.
    """
    now = datetime.now()
    return [
        {
            "timestamp": (now - timedelta(minutes=5)).isoformat(),
            "symbol": "AAPL",
            "side": "BUY",
            "quantity": 50,
            "price": 150.25,
            "status": "FILLED",
        },
        {
            "timestamp": (now - timedelta(minutes=3)).isoformat(),
            "symbol": "GOOGL",
            "side": "SELL",
            "quantity": 25,
            "price": 2800.00,
            "status": "FILLED",
        },
    ]


@pytest.fixture
def mock_strategy_status() -> Dict[str, str]:
    """Create mock strategy status for dashboard testing.

    Returns:
        Dictionary of strategy names to status strings.
    """
    return {
        "WeatherStrategy": "RUNNING",
        "EconomicStrategy": "RUNNING",
        "TrendStrategy": "STOPPED",
    }


@pytest.fixture
def mock_system_health() -> Dict[str, Dict[str, Any]]:
    """Create mock system health for dashboard testing.

    Returns:
        Dictionary of health check results.
    """
    return {
        "IBKR Connection": {
            "status": "HEALTHY",
            "message": "Connected",
        },
        "Kalshi API": {
            "status": "HEALTHY",
            "message": "Responding",
        },
        "Database": {
            "status": "HEALTHY",
            "message": "Accessible",
        },
        "Memory": {
            "status": "WARNING",
            "message": "75% usage",
        },
    }


# Pytest configuration
def pytest_configure(config):
    """Configure pytest markers.

    Args:
        config: Pytest config object.
    """
    config.addinivalue_line("markers", "unit: mark test as a unit test")
    config.addinivalue_line("markers", "integration: mark test as an integration test")
    config.addinivalue_line("markers", "slow: mark test as slow")
