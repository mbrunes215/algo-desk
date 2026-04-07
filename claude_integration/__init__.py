"""Claude AI integration modules for the algo trading desk."""

from .briefing import (
    DailyBriefing,
    BriefingResult,
)

from .probability import (
    ClaudeProbabilityEstimator,
    ProbabilityEstimate,
)

from .journal import (
    TradeJournal,
    TradeEntry,
    WeeklyReport,
)

__all__ = [
    # Briefing
    "DailyBriefing",
    "BriefingResult",
    # Probability estimation
    "ClaudeProbabilityEstimator",
    "ProbabilityEstimate",
    # Trade journal
    "TradeJournal",
    "TradeEntry",
    "WeeklyReport",
]
