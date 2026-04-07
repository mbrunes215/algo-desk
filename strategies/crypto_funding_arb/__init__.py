"""
Crypto Funding Rate Arbitrage Strategy

Delta-neutral strategy: hold spot long + perpetual futures short to collect
funding rate payments every 8 hours. No directional risk when properly hedged.
"""

from .funding_arb_strategy import FundingArbStrategy

__all__ = ["FundingArbStrategy"]
