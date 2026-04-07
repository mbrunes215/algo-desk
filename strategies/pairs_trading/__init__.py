"""
BTC/ETH Pairs Trading Strategy

Market-neutral stat arb: trade the BTC/ETH price ratio using Z-score mean reversion.
When the spread deviates beyond a threshold, short the outperformer and long the
underperformer. Close when the spread reverts to its mean.
"""

from .pairs_strategy import PairsTradingStrategy

__all__ = ["PairsTradingStrategy"]
