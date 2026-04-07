"""Kalshi trading strategies package."""

from .kalshi_weather import (
    WeatherStrategy,
    WeatherForecast,
    ContractSignal as WeatherSignal,
)

from .kalshi_econ import (
    EconDataStrategy,
    EconomicIndicator,
    ConsensusEstimate,
    EconSignal,
    SurpriseDistribution,
)

__all__ = [
    # Weather strategy
    "WeatherStrategy",
    "WeatherForecast",
    "WeatherSignal",
    # Economic strategy
    "EconDataStrategy",
    "EconomicIndicator",
    "ConsensusEstimate",
    "EconSignal",
    "SurpriseDistribution",
]
