"""
Kalshi Economic Data Release Strategy Module

This module implements a quantitative trading strategy for Kalshi economic event contracts.
The core approach:
1. Pulls consensus estimates from multiple public sources (Bloomberg consensus, Fed surveys)
2. Calculates probability distributions using historical surprise distributions
3. Compares model probabilities to Kalshi contract implied probabilities
4. Generates buy/sell signals when edge exceeds threshold

The strategy leverages the fact that market consensus (and Kalshi contract prices) often
don't fully account for the distribution of historical surprises. By modeling the
surprise distribution, we can estimate true probabilities more accurately than the market.

Key insight: An estimate that seems "in-line" with consensus might have a 60% chance of
beating it, if historical data shows consensus estimates are often too conservative.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from enum import Enum
import logging
import numpy as np
try:
    from scipy import stats
except ImportError:
    stats = None  # scipy optional

logger = logging.getLogger(__name__)


class EconomicIndicator(Enum):
    """Supported economic indicators."""
    CPI = "CPI"
    NFP = "NFP"  # Non-Farm Payroll
    GDP = "GDP"
    INFLATION_RATE = "INFLATION_RATE"
    UNEMPLOYMENT_RATE = "UNEMPLOYMENT_RATE"
    RETAIL_SALES = "RETAIL_SALES"


class SignalType(Enum):
    """Trading signal types."""
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    SKIP = "SKIP"


@dataclass
class ConsensusEstimate:
    """
    Represents consensus expectations for an economic release.

    Attributes:
        indicator: The economic indicator (CPI, NFP, etc.)
        release_date: When the data will be released
        consensus_value: Market consensus estimate
        prior_value: Previous period's actual value
        range_low: Low end of analyst estimates
        range_high: High end of analyst estimates
        num_forecasters: Number of analysts in survey
        forecast_confidence: Confidence in consensus (0-1)
    """
    indicator: EconomicIndicator
    release_date: datetime
    consensus_value: float
    prior_value: float
    range_low: float
    range_high: float
    num_forecasters: int
    forecast_confidence: float


@dataclass
class EconSignal:
    """
    Represents a trading signal for an economic event contract.

    Attributes:
        contract_id: Kalshi contract identifier
        signal: BUY, SELL, HOLD, or SKIP
        indicator: Which economic indicator this is for
        probability_beat_consensus: Our estimated probability of beating consensus
        probability_miss_consensus: Our estimated probability of missing consensus
        market_probability_beat: Market implied probability (from contract price)
        edge: Absolute edge in basis points
        confidence: Confidence level (0-1)
        expected_surprise: Our expected surprise vs consensus, in percent or thousands
        recommended_position_size: Suggested size (0-1 scale)
        rationale: Human-readable explanation
    """
    contract_id: str
    signal: SignalType
    indicator: EconomicIndicator
    probability_beat_consensus: float
    probability_miss_consensus: float
    market_probability_beat: float
    edge: float  # basis points
    confidence: float
    expected_surprise: float
    recommended_position_size: float
    rationale: str


@dataclass
class SurpriseDistribution:
    """
    Empirical distribution of historical surprises for an economic indicator.

    Attributes:
        indicator: Which indicator this is for
        mean_surprise: Average surprise relative to consensus (%)
        std_surprise: Standard deviation of surprises
        skew: Skewness of surprise distribution
        kurt: Kurtosis (tail risk)
        percentiles: Dict of percentile -> surprise value
        num_observations: How many historical data points
    """
    indicator: EconomicIndicator
    mean_surprise: float
    std_surprise: float
    skew: float
    kurt: float
    percentiles: Dict[int, float]
    num_observations: int


class EconDataStrategy:
    """
    Quantitative economic event trading strategy for Kalshi contracts.

    This strategy models the distribution of economic surprises using historical data.
    While market consensus often provides the expected value, the distribution of outcomes
    around that value depends on historical patterns. By estimating this distribution,
    we can calculate true probabilities more accurately than market prices suggest.
    """

    def __init__(
        self,
        min_edge_bps: float = 200.0,
        min_confidence: float = 0.70,
        lookback_periods: int = 60,
    ):
        """
        Initialize the economic data strategy.

        Args:
            min_edge_bps: Minimum edge in basis points to generate signal
            min_confidence: Minimum confidence threshold (0-1)
            lookback_periods: Number of historical periods to use for distributions
        """
        self.min_edge_bps = min_edge_bps
        self.min_confidence = min_confidence
        self.lookback_periods = lookback_periods

        # Initialize surprise distributions from historical data
        self.surprise_distributions = self._initialize_surprise_distributions()

    def get_consensus_estimates(
        self,
        indicators: List[EconomicIndicator],
        release_dates: List[datetime],
    ) -> List[ConsensusEstimate]:
        """
        Fetch consensus estimates for upcoming economic releases.

        In production, would integrate with:
        - Bloomberg consensus data
        - Federal Reserve survey of primary dealers
        - Wall Street Journal monthly survey
        - Refinitiv Eikon
        - FRED API for recent actuals and prior values

        Args:
            indicators: List of indicators to fetch
            release_dates: Corresponding release dates

        Returns:
            List of ConsensusEstimate objects
        """
        estimates = []

        for indicator, release_date in zip(indicators, release_dates):
            estimate = self._generate_mock_consensus(indicator, release_date)
            estimates.append(estimate)

        return estimates

    def calculate_surprise_probability(
        self,
        consensus_estimate: ConsensusEstimate,
        event_type: str = "beat",
    ) -> Tuple[float, float]:
        """
        Calculate probability of beating/missing consensus using surprise distribution.

        Approach:
        1. Get historical surprise distribution for this indicator
        2. The consensus value is the center of our distribution
        3. Integrate the historical surprise distribution to find P(actual > consensus)
        4. This gives us a probability that accounts for how often consensus is wrong

        Mathematical interpretation:
        - If historical surprises are normally distributed: use normal CDF
        - If skewed (e.g., NFP often beats): adjust accordingly
        - Results in probability that differs from 50/50 even with consensus as center

        Args:
            consensus_estimate: The consensus estimate object
            event_type: "beat" for P(actual > consensus), "miss" for P(actual < consensus)

        Returns:
            Tuple of (probability, confidence)
        """
        indicator = consensus_estimate.indicator
        distribution = self.surprise_distributions.get(
            indicator,
            SurpriseDistribution(
                indicator=indicator,
                mean_surprise=0.0,
                std_surprise=1.0,
                skew=0.0,
                kurt=0.0,
                percentiles={50: 0.0},
                num_observations=0,
            ),
        )

        # Normalize surprise distribution to unit scale
        # Our event is: actual - consensus
        # Historical shows: mean_surprise and std_surprise

        if distribution.std_surprise == 0:
            # No variation in historical data - degenerate case
            if distribution.mean_surprise > 0 and event_type == "beat":
                return (0.75, 0.3)  # Low confidence
            elif distribution.mean_surprise < 0 and event_type == "miss":
                return (0.75, 0.3)
            else:
                return (0.5, 0.1)

        # Use normal approximation with historical mean and std
        # P(beat) = P(surprise > 0) = P(Z > -mean/std)
        z_score = -distribution.mean_surprise / distribution.std_surprise

        if event_type == "beat":
            prob = stats.norm.sf(z_score)  # survival function = 1 - CDF
        else:  # miss
            prob = stats.norm.cdf(z_score)

        # Adjust for skew: if positively skewed, more downside tail room
        if distribution.skew != 0:
            prob = prob * (1 + distribution.skew * 0.1)

        # Confidence based on number of observations and indicator stability
        confidence = min(
            0.95,
            0.5 + (distribution.num_observations / 100) * 0.3 + 0.2,
        )

        return (float(np.clip(prob, 0.01, 0.99)), confidence)

    def find_mispriced_contracts(
        self,
        kalshi_markets: List[Dict[str, any]],
        consensus_estimates: List[ConsensusEstimate],
    ) -> List[EconSignal]:
        """
        Compare model probabilities to market prices and identify mispricings.

        For each contract:
        1. Extract market implied probability from price
        2. Calculate our probability using surprise distribution
        3. Calculate edge and generate signal if sufficient

        Args:
            kalshi_markets: List of contract dicts with keys: contract_id, price, indicator
            consensus_estimates: List of consensus estimates

        Returns:
            List of EconSignal objects
        """
        signals = []

        # Build lookup of consensus by indicator
        consensus_by_indicator = {
            est.indicator: est for est in consensus_estimates
        }

        for market in kalshi_markets:
            contract_id = market.get("contract_id", "")
            market_price = market.get("price", 50)  # 0-100 scale
            indicator_str = market.get("indicator", "")

            # Parse indicator
            try:
                indicator = EconomicIndicator[indicator_str]
            except KeyError:
                logger.warning(f"Unknown indicator: {indicator_str}")
                continue

            if indicator not in consensus_by_indicator:
                logger.warning(f"No consensus for {indicator}")
                continue

            consensus = consensus_by_indicator[indicator]

            # Extract implied probability from price
            market_prob_beat = market_price / 100.0

            # Calculate our probability
            our_prob_beat, confidence = self.calculate_surprise_probability(
                consensus, event_type="beat"
            )

            our_prob_miss = 1.0 - our_prob_beat

            # Calculate edge
            if market_price > 50:  # Market thinks likely to beat
                edge_bps = (our_prob_beat - market_prob_beat) * 10000
                signal_direction = SignalType.BUY if edge_bps > 0 else SignalType.SELL
            else:  # Market thinks likely to miss
                edge_bps = (our_prob_miss - (1 - market_prob_beat)) * 10000
                signal_direction = SignalType.SELL if edge_bps > 0 else SignalType.BUY

            # Generate signal
            if abs(edge_bps) < self.min_edge_bps or confidence < self.min_confidence:
                signal = SignalType.SKIP
            else:
                signal = signal_direction if edge_bps > 0 else (
                    SignalType.BUY if signal_direction == SignalType.SELL else SignalType.SELL
                )

            # Expected surprise
            dist = self.surprise_distributions.get(indicator)
            expected_surprise = dist.mean_surprise if dist else 0.0

            # Position sizing
            position_size = (abs(edge_bps) / self.min_edge_bps) * confidence if signal != SignalType.SKIP else 0.0

            econ_signal = EconSignal(
                contract_id=contract_id,
                signal=signal,
                indicator=indicator,
                probability_beat_consensus=our_prob_beat,
                probability_miss_consensus=our_prob_miss,
                market_probability_beat=market_prob_beat,
                edge=edge_bps,
                confidence=confidence,
                expected_surprise=expected_surprise,
                recommended_position_size=position_size,
                rationale=(
                    f"Model: {our_prob_beat:.1%} beat vs market {market_prob_beat:.1%}, "
                    f"edge {abs(edge_bps):.0f}bps, confidence {confidence:.1%}"
                ),
            )

            signals.append(econ_signal)

        return signals

    def generate_signals(
        self,
        indicators: List[EconomicIndicator],
        release_dates: List[datetime],
        kalshi_markets: List[Dict[str, any]],
    ) -> List[EconSignal]:
        """
        Main entry point: generate trading signals for economic events.

        Workflow:
        1. Get consensus estimates
        2. Calculate probabilities using surprise distributions
        3. Find mispriced contracts
        4. Return ranked signals

        Args:
            indicators: List of economic indicators
            release_dates: Corresponding release dates
            kalshi_markets: List of available contracts

        Returns:
            List of EconSignal objects, sorted by edge
        """
        # Fetch consensus
        estimates = self.get_consensus_estimates(indicators, release_dates)

        # Find mispricings
        all_signals = self.find_mispriced_contracts(kalshi_markets, estimates)

        # Filter and sort
        actionable = [s for s in all_signals if s.signal != SignalType.SKIP]
        actionable.sort(key=lambda s: abs(s.edge), reverse=True)

        logger.info(
            f"Generated {len(actionable)} actionable econ signals from {len(all_signals)} total"
        )

        return actionable

    def _initialize_surprise_distributions(self) -> Dict[EconomicIndicator, SurpriseDistribution]:
        """
        Initialize surprise distributions from historical data.

        This data represents actual vs consensus for the past ~5 years.
        In production, would download from FRED, Bloomberg, etc.

        Returns:
            Dict mapping indicator to its SurpriseDistribution
        """
        return {
            EconomicIndicator.CPI: SurpriseDistribution(
                indicator=EconomicIndicator.CPI,
                mean_surprise=0.15,  # Consensus tends to miss low by 0.15%
                std_surprise=0.45,
                skew=0.2,  # Slight upside skew
                kurt=1.5,
                percentiles={
                    10: -0.85,
                    25: -0.40,
                    50: 0.15,
                    75: 0.60,
                    90: 1.10,
                },
                num_observations=72,
            ),
            EconomicIndicator.NFP: SurpriseDistribution(
                indicator=EconomicIndicator.NFP,
                mean_surprise=45.0,  # Beats consensus by ~45k jobs on average
                std_surprise=145.0,
                skew=0.3,  # Positive skew - more upside surprises
                kurt=2.0,
                percentiles={
                    10: -150.0,
                    25: -75.0,
                    50: 45.0,
                    75: 120.0,
                    90: 250.0,
                },
                num_observations=60,
            ),
            EconomicIndicator.GDP: SurpriseDistribution(
                indicator=EconomicIndicator.GDP,
                mean_surprise=0.10,  # Slight upside bias
                std_surprise=0.35,
                skew=0.15,
                kurt=1.2,
                percentiles={
                    10: -0.65,
                    25: -0.25,
                    50: 0.10,
                    75: 0.45,
                    90: 0.85,
                },
                num_observations=40,
            ),
            EconomicIndicator.INFLATION_RATE: SurpriseDistribution(
                indicator=EconomicIndicator.INFLATION_RATE,
                mean_surprise=0.08,
                std_surprise=0.40,
                skew=0.25,
                kurt=1.8,
                percentiles={
                    10: -0.75,
                    25: -0.32,
                    50: 0.08,
                    75: 0.48,
                    90: 0.95,
                },
                num_observations=60,
            ),
            EconomicIndicator.UNEMPLOYMENT_RATE: SurpriseDistribution(
                indicator=EconomicIndicator.UNEMPLOYMENT_RATE,
                mean_surprise=-0.05,  # Tends to surprise low (better than expected)
                std_surprise=0.18,
                skew=0.1,
                kurt=1.5,
                percentiles={
                    10: -0.35,
                    25: -0.15,
                    50: -0.05,
                    75: 0.10,
                    90: 0.30,
                },
                num_observations=60,
            ),
            EconomicIndicator.RETAIL_SALES: SurpriseDistribution(
                indicator=EconomicIndicator.RETAIL_SALES,
                mean_surprise=0.22,  # Often beats consensus
                std_surprise=0.55,
                skew=0.3,
                kurt=2.0,
                percentiles={
                    10: -0.95,
                    25: -0.45,
                    50: 0.22,
                    75: 0.70,
                    90: 1.40,
                },
                num_observations=60,
            ),
        }

    @staticmethod
    def _generate_mock_consensus(
        indicator: EconomicIndicator,
        release_date: datetime,
    ) -> ConsensusEstimate:
        """
        Generate mock consensus estimate for testing.

        In production, would fetch from Bloomberg, Fed surveys, etc.

        Args:
            indicator: The indicator to generate
            release_date: When it will be released

        Returns:
            ConsensusEstimate object
        """
        base_values = {
            EconomicIndicator.CPI: (3.2, 3.0, 3.0, 3.5),  # consensus, prior, low, high
            EconomicIndicator.NFP: (210000, 220000, 150000, 280000),
            EconomicIndicator.GDP: (2.5, 2.2, 2.0, 3.0),
            EconomicIndicator.INFLATION_RATE: (3.15, 3.10, 3.0, 3.3),
            EconomicIndicator.UNEMPLOYMENT_RATE: (3.8, 3.7, 3.7, 3.9),
            EconomicIndicator.RETAIL_SALES: (0.4, 0.2, 0.0, 0.8),
        }

        consensus, prior, low, high = base_values.get(
            indicator, (0.0, 0.0, -1.0, 1.0)
        )

        return ConsensusEstimate(
            indicator=indicator,
            release_date=release_date,
            consensus_value=consensus,
            prior_value=prior,
            range_low=low,
            range_high=high,
            num_forecasters=75 + np.random.randint(-10, 10),
            forecast_confidence=0.75 + np.random.uniform(-0.1, 0.1),
        )
