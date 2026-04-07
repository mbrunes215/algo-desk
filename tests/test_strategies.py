"""Tests for trading strategy signal generation.

Tests weather probability calculation, economic surprise distribution,
and signal generation with mock data.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List
from enum import Enum

import pytest


class SignalType(str, Enum):
    """Trading signal types."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


@dataclass
class Signal:
    """Represents a trading signal."""

    symbol: str
    signal_type: SignalType
    confidence: float
    timestamp: datetime
    data: Dict[str, Any]


class WeatherStrategy:
    """Mock weather prediction strategy."""

    def __init__(self, base_confidence: float = 0.7) -> None:
        """Initialize weather strategy.

        Args:
            base_confidence: Base confidence threshold.
        """
        self.base_confidence = base_confidence

    def calculate_weather_probability(
        self,
        temperature: float,
        humidity: float,
        pressure: float,
    ) -> float:
        """Calculate probability of adverse weather.

        Args:
            temperature: Current temperature in Celsius.
            humidity: Humidity percentage (0-100).
            pressure: Barometric pressure in mb.

        Returns:
            Probability between 0 and 1.
        """
        # Simplified model: high humidity + low pressure = high probability
        humidity_factor = humidity / 100.0
        pressure_factor = max(0.0, (1010 - pressure) / 50.0)
        temp_factor = 1.0 if temperature < 5 or temperature > 35 else 0.5

        probability = (humidity_factor + pressure_factor + temp_factor) / 3.0
        return min(1.0, max(0.0, probability))

    def generate_signal(
        self,
        symbol: str,
        weather_probability: float,
    ) -> Signal:
        """Generate trading signal based on weather probability.

        Args:
            symbol: Market symbol (e.g., 'WEATHER_GRAIN').
            weather_probability: Calculated weather probability.

        Returns:
            Trading signal.
        """
        if weather_probability >= self.base_confidence:
            signal_type = SignalType.BUY
            confidence = weather_probability
        elif weather_probability < 0.3:
            signal_type = SignalType.SELL
            confidence = 1.0 - weather_probability
        else:
            signal_type = SignalType.HOLD
            confidence = 0.5

        return Signal(
            symbol=symbol,
            signal_type=signal_type,
            confidence=confidence,
            timestamp=datetime.utcnow(),
            data={"weather_probability": weather_probability},
        )


class EconomicStrategy:
    """Mock economic surprise strategy."""

    def __init__(self, sensitivity: float = 1.0) -> None:
        """Initialize economic strategy.

        Args:
            sensitivity: Sensitivity to economic surprises.
        """
        self.sensitivity = sensitivity

    def calculate_surprise_distribution(
        self,
        actual: float,
        forecast: float,
    ) -> float:
        """Calculate surprise magnitude from economic data.

        Args:
            actual: Actual economic value.
            forecast: Forecasted economic value.

        Returns:
            Surprise score (-1 to 1, where 1 is maximum positive surprise).
        """
        if forecast == 0:
            return 0.0

        surprise_percent = (actual - forecast) / abs(forecast)
        normalized_surprise = max(-1.0, min(1.0, surprise_percent * self.sensitivity))
        return normalized_surprise

    def generate_signal(
        self,
        symbol: str,
        surprise_score: float,
        confidence_threshold: float = 0.5,
    ) -> Signal:
        """Generate trading signal based on economic surprise.

        Args:
            symbol: Market symbol.
            surprise_score: Calculated surprise score.
            confidence_threshold: Minimum confidence threshold.

        Returns:
            Trading signal.
        """
        if surprise_score > confidence_threshold:
            signal_type = SignalType.BUY
            confidence = min(1.0, abs(surprise_score))
        elif surprise_score < -confidence_threshold:
            signal_type = SignalType.SELL
            confidence = min(1.0, abs(surprise_score))
        else:
            signal_type = SignalType.HOLD
            confidence = 0.5

        return Signal(
            symbol=symbol,
            signal_type=signal_type,
            confidence=confidence,
            timestamp=datetime.utcnow(),
            data={"surprise_score": surprise_score},
        )


class MockMarketDataPoint:
    """Mock market data point."""

    def __init__(
        self,
        symbol: str,
        timestamp: datetime,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int,
    ) -> None:
        """Initialize market data point."""
        self.symbol = symbol
        self.timestamp = timestamp
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

    @property
    def daily_return(self) -> float:
        """Calculate daily return percentage."""
        if self.open == 0:
            return 0.0
        return ((self.close - self.open) / self.open) * 100

    @property
    def intraday_range(self) -> float:
        """Calculate intraday price range."""
        return self.high - self.low


class TestWeatherStrategy:
    """Tests for weather strategy."""

    @pytest.mark.unit
    def test_weather_probability_calculation(self) -> None:
        """Test weather probability calculation."""
        strategy = WeatherStrategy()
        prob = strategy.calculate_weather_probability(
            temperature=10.0,
            humidity=85.0,
            pressure=980.0,
        )
        assert 0.0 <= prob <= 1.0

    @pytest.mark.unit
    def test_weather_probability_high_humidity_low_pressure(self) -> None:
        """Test high probability with high humidity and low pressure."""
        strategy = WeatherStrategy()
        prob = strategy.calculate_weather_probability(
            temperature=20.0,
            humidity=95.0,
            pressure=950.0,
        )
        assert prob > 0.6

    @pytest.mark.unit
    def test_weather_probability_low_humidity_high_pressure(self) -> None:
        """Test low probability with low humidity and high pressure."""
        strategy = WeatherStrategy()
        prob = strategy.calculate_weather_probability(
            temperature=20.0,
            humidity=30.0,
            pressure=1020.0,
        )
        assert prob < 0.4

    @pytest.mark.unit
    def test_weather_probability_extreme_temperature(self) -> None:
        """Test probability with extreme temperatures."""
        strategy = WeatherStrategy()
        prob_cold = strategy.calculate_weather_probability(
            temperature=-5.0,
            humidity=50.0,
            pressure=1000.0,
        )
        prob_hot = strategy.calculate_weather_probability(
            temperature=40.0,
            humidity=50.0,
            pressure=1000.0,
        )
        assert prob_cold > prob_hot

    @pytest.mark.unit
    def test_weather_signal_generation_buy(self) -> None:
        """Test BUY signal generation for high weather probability."""
        strategy = WeatherStrategy(base_confidence=0.6)
        signal = strategy.generate_signal("WEATHER_GRAIN", weather_probability=0.8)
        assert signal.signal_type == SignalType.BUY
        assert signal.confidence == 0.8

    @pytest.mark.unit
    def test_weather_signal_generation_sell(self) -> None:
        """Test SELL signal generation for low weather probability."""
        strategy = WeatherStrategy(base_confidence=0.6)
        signal = strategy.generate_signal("WEATHER_GRAIN", weather_probability=0.2)
        assert signal.signal_type == SignalType.SELL
        assert signal.confidence == 0.8

    @pytest.mark.unit
    def test_weather_signal_generation_hold(self) -> None:
        """Test HOLD signal generation for medium probability."""
        strategy = WeatherStrategy(base_confidence=0.7)
        signal = strategy.generate_signal("WEATHER_GRAIN", weather_probability=0.5)
        assert signal.signal_type == SignalType.HOLD
        assert signal.confidence == 0.5

    @pytest.mark.unit
    def test_weather_signal_metadata(self) -> None:
        """Test signal contains correct metadata."""
        strategy = WeatherStrategy()
        signal = strategy.generate_signal("WEATHER_GRAIN", weather_probability=0.75)
        assert signal.symbol == "WEATHER_GRAIN"
        assert signal.timestamp is not None
        assert "weather_probability" in signal.data


class TestEconomicStrategy:
    """Tests for economic strategy."""

    @pytest.mark.unit
    def test_surprise_calculation_positive(self) -> None:
        """Test positive economic surprise calculation."""
        strategy = EconomicStrategy()
        surprise = strategy.calculate_surprise_distribution(
            actual=100.5,
            forecast=100.0,
        )
        assert surprise > 0.0
        assert surprise < 1.0

    @pytest.mark.unit
    def test_surprise_calculation_negative(self) -> None:
        """Test negative economic surprise calculation."""
        strategy = EconomicStrategy()
        surprise = strategy.calculate_surprise_distribution(
            actual=99.5,
            forecast=100.0,
        )
        assert surprise < 0.0
        assert surprise > -1.0

    @pytest.mark.unit
    def test_surprise_calculation_no_surprise(self) -> None:
        """Test no surprise when actual equals forecast."""
        strategy = EconomicStrategy()
        surprise = strategy.calculate_surprise_distribution(
            actual=100.0,
            forecast=100.0,
        )
        assert surprise == 0.0

    @pytest.mark.unit
    def test_surprise_calculation_large_positive(self) -> None:
        """Test large positive surprise."""
        strategy = EconomicStrategy(sensitivity=1.0)
        surprise = strategy.calculate_surprise_distribution(
            actual=120.0,
            forecast=100.0,
        )
        assert surprise >= 0.9

    @pytest.mark.unit
    def test_surprise_calculation_large_negative(self) -> None:
        """Test large negative surprise."""
        strategy = EconomicStrategy(sensitivity=1.0)
        surprise = strategy.calculate_surprise_distribution(
            actual=80.0,
            forecast=100.0,
        )
        assert surprise <= -0.9

    @pytest.mark.unit
    def test_surprise_calculation_sensitivity(self) -> None:
        """Test sensitivity parameter effect."""
        strategy_low_sensitivity = EconomicStrategy(sensitivity=0.5)
        strategy_high_sensitivity = EconomicStrategy(sensitivity=2.0)

        surprise_low = strategy_low_sensitivity.calculate_surprise_distribution(
            actual=110.0,
            forecast=100.0,
        )
        surprise_high = strategy_high_sensitivity.calculate_surprise_distribution(
            actual=110.0,
            forecast=100.0,
        )

        assert surprise_low < surprise_high

    @pytest.mark.unit
    def test_economic_signal_generation_buy(self) -> None:
        """Test BUY signal for positive surprise."""
        strategy = EconomicStrategy()
        signal = strategy.generate_signal(
            "ECON_MARKET",
            surprise_score=0.8,
            confidence_threshold=0.5,
        )
        assert signal.signal_type == SignalType.BUY
        assert signal.confidence > 0.0

    @pytest.mark.unit
    def test_economic_signal_generation_sell(self) -> None:
        """Test SELL signal for negative surprise."""
        strategy = EconomicStrategy()
        signal = strategy.generate_signal(
            "ECON_MARKET",
            surprise_score=-0.8,
            confidence_threshold=0.5,
        )
        assert signal.signal_type == SignalType.SELL
        assert signal.confidence > 0.0

    @pytest.mark.unit
    def test_economic_signal_generation_hold(self) -> None:
        """Test HOLD signal for neutral surprise."""
        strategy = EconomicStrategy()
        signal = strategy.generate_signal(
            "ECON_MARKET",
            surprise_score=0.2,
            confidence_threshold=0.5,
        )
        assert signal.signal_type == SignalType.HOLD


class TestSignalGeneration:
    """Tests for signal generation with mock data."""

    @pytest.mark.unit
    def test_market_data_point_daily_return(self) -> None:
        """Test daily return calculation."""
        data = MockMarketDataPoint(
            symbol="AAPL",
            timestamp=datetime.utcnow(),
            open_price=150.0,
            high=155.0,
            low=149.0,
            close=153.0,
            volume=1000000,
        )
        assert data.daily_return == 2.0

    @pytest.mark.unit
    def test_market_data_point_intraday_range(self) -> None:
        """Test intraday range calculation."""
        data = MockMarketDataPoint(
            symbol="AAPL",
            timestamp=datetime.utcnow(),
            open_price=150.0,
            high=155.0,
            low=148.0,
            close=152.0,
            volume=1000000,
        )
        assert data.intraday_range == 7.0

    @pytest.mark.unit
    def test_signal_generation_with_market_data(self) -> None:
        """Test signal generation using market data."""
        weather_strategy = WeatherStrategy()
        market_data = MockMarketDataPoint(
            symbol="WEATHER_GRAIN",
            timestamp=datetime.utcnow(),
            open_price=100.0,
            high=105.0,
            low=99.0,
            close=104.0,
            volume=5000000,
        )

        weather_prob = weather_strategy.calculate_weather_probability(
            temperature=15.0,
            humidity=75.0,
            pressure=990.0,
        )
        signal = weather_strategy.generate_signal(
            market_data.symbol,
            weather_prob,
        )

        assert signal.symbol == market_data.symbol
        assert signal.timestamp is not None
        assert signal.confidence >= 0.0 and signal.confidence <= 1.0

    @pytest.mark.unit
    def test_multiple_signal_generation(self) -> None:
        """Test generating signals for multiple markets."""
        weather_strategy = WeatherStrategy()
        econ_strategy = EconomicStrategy()

        weather_signal = weather_strategy.generate_signal(
            "WEATHER_GRAIN",
            weather_probability=0.8,
        )
        econ_signal = econ_strategy.generate_signal(
            "ECON_JOBLESS",
            surprise_score=0.6,
        )

        assert weather_signal.symbol != econ_signal.symbol
        assert weather_signal.timestamp <= econ_signal.timestamp or \
               (weather_signal.timestamp - econ_signal.timestamp).total_seconds() < 1
