"""
Claude-Based Probability Estimator Module

Uses Claude to process market news and data, providing calibrated probability
estimates for economic and event-based Kalshi contracts.

Key insight: Claude can integrate disparate news sources and apply domain
knowledge to update probability estimates more intelligently than mechanical
models alone.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
import logging

try:
    from anthropic import Anthropic
except ImportError:
    raise ImportError(
        "anthropic package required. Install with: pip install anthropic"
    )

logger = logging.getLogger(__name__)


@dataclass
class ProbabilityEstimate:
    """
    Result of probability estimation.

    Attributes:
        event_description: What is being estimated (e.g., "CPI beats 3.0%")
        prior_probability: Base probability before news
        posterior_probability: Updated probability after analyzing news
        confidence: How confident in estimate (0-1)
        key_factors: List of factors driving the estimate
        supporting_evidence: News items and data supporting the estimate
        counter_evidence: Factors arguing against the estimate
        notes: Additional commentary
    """
    event_description: str
    prior_probability: float
    posterior_probability: float
    confidence: float
    key_factors: List[str]
    supporting_evidence: List[str]
    counter_evidence: List[str]
    notes: str


class ClaudeProbabilityEstimator:
    """
    Uses Claude to estimate and update event probabilities.

    Leverages Claude's ability to:
    - Integrate multiple news sources
    - Apply domain knowledge and context
    - Identify base rates and relevant precedents
    - Weight evidence appropriately
    - Explain reasoning clearly

    Results are calibrated via few-shot examples.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-3-5-sonnet-20241022",
    ):
        """
        Initialize the probability estimator.

        Args:
            api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
            model: Claude model to use
        """
        self.client = Anthropic(api_key=api_key) if api_key else Anthropic()
        self.model = model

    def estimate_event_probability(
        self,
        event_description: str,
        context_data: Dict[str, any],
        prior_probability: float = 0.5,
    ) -> ProbabilityEstimate:
        """
        Estimate probability of an event using Claude.

        Claude will:
        1. Review the event description and context
        2. Identify relevant base rates
        3. Consider analogous historical events
        4. Provide step-by-step reasoning
        5. Calibrate probability estimate

        Args:
            event_description: Clear description of the event
                (e.g., "CPI releases above 3.2% next month")
            context_data: Dict with relevant context:
                - "consensus_estimate": Current consensus
                - "prior_actual": Previous reading
                - "recent_trends": List of recent data points
                - "analyst_views": Recent analyst commentary
            prior_probability: Base rate / prior probability (default 0.5)

        Returns:
            ProbabilityEstimate with detailed reasoning
        """
        logger.info(f"Estimating probability for: {event_description}")

        prompt = self._build_estimation_prompt(
            event_description,
            context_data,
            prior_probability,
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1500,
                system=self._get_system_prompt(),
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )

            content = response.content[0].text
            estimate = self._parse_probability_response(
                content,
                event_description,
                prior_probability,
            )

            return estimate

        except Exception as e:
            logger.error(f"Failed to estimate probability: {e}")
            return ProbabilityEstimate(
                event_description=event_description,
                prior_probability=prior_probability,
                posterior_probability=prior_probability,
                confidence=0.1,
                key_factors=["Error in estimation"],
                supporting_evidence=[],
                counter_evidence=[],
                notes=f"Estimation failed: {e}",
            )

    def update_with_news(
        self,
        current_estimate: float,
        news_items: List[str],
        event_description: str,
    ) -> Tuple[float, List[str]]:
        """
        Update probability estimate with new news.

        Takes current estimate and list of news items, uses Claude to
        assess impact of news and provide updated probability.

        Args:
            current_estimate: Current estimated probability (0-1)
            news_items: List of recent news headlines/summaries
            event_description: What we're estimating

        Returns:
            Tuple of (updated_probability, impact_analysis)
        """
        logger.info(f"Updating estimate with {len(news_items)} news items")

        news_str = "\n".join([f"- {item}" for item in news_items])

        prompt = f"""
You are a probability estimation expert. A trader needs to update their probability estimate
given new information.

EVENT: {event_description}
CURRENT ESTIMATE: {current_estimate:.1%}

NEW INFORMATION:
{news_str}

Analyze how this news should update the probability. Consider:
1. Signal strength of each news item
2. Base rate implications
3. Whether this is consensus-changing news
4. Historical precedents

Provide:
1. Updated probability (0-100%)
2. Confidence in the update (0-100%)
3. Key factors in the update
4. Impact analysis (1-2 sentences on each news item)

Format:
UPDATED_PROBABILITY: [number]%
CONFIDENCE: [number]%
KEY_FACTORS:
- [factor 1]
- [factor 2]

IMPACT_ANALYSIS:
- [news 1]: [impact]
- [news 2]: [impact]
"""

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                system=self._get_system_prompt(),
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )

            content = response.content[0].text

            # Parse updated probability
            prob_line = [
                line for line in content.split("\n")
                if "UPDATED_PROBABILITY" in line
            ]
            if prob_line:
                prob_str = prob_line[0].split(":")[-1].strip().rstrip("%")
                updated_prob = float(prob_str) / 100.0
            else:
                updated_prob = current_estimate

            impact = content.split("IMPACT_ANALYSIS:")[-1].strip().split("\n")
            impact = [line.strip() for line in impact if line.strip()]

            return (updated_prob, impact)

        except Exception as e:
            logger.error(f"Failed to update estimate: {e}")
            return (current_estimate, [f"Error: {e}"])

    def _build_estimation_prompt(
        self,
        event: str,
        context: Dict[str, any],
        prior: float,
    ) -> str:
        """
        Build the prompt for probability estimation.

        Includes few-shot examples for calibration and clear instructions
        for step-by-step reasoning.

        Args:
            event: Event description
            context: Context data
            prior: Prior probability

        Returns:
            Formatted prompt string
        """
        consensus = context.get("consensus_estimate", "Not provided")
        prior_actual = context.get("prior_actual", "Not provided")
        recent_trends = context.get("recent_trends", [])
        analyst_views = context.get("analyst_views", [])

        trends_str = "\n".join(
            [f"  - {t}" for t in recent_trends]
        ) if recent_trends else "  None provided"

        analyst_str = "\n".join(
            [f"  - {v}" for v in analyst_views]
        ) if analyst_views else "  None provided"

        prompt = f"""
You are a probability estimation expert specializing in financial events.
Your task is to estimate the probability of an event occurring.

EVENT TO ESTIMATE: {event}

CONTEXT DATA:
- Consensus Estimate: {consensus}
- Prior Actual: {prior_actual}
- Base Rate (Prior Probability): {prior:.1%}

RECENT TRENDS:
{trends_str}

ANALYST VIEWS:
{analyst_str}

YOUR TASK:
1. Think step-by-step about base rates and similar events
2. Assess which trend is most predictive
3. Consider if consensus is optimistic/pessimistic
4. Weigh evidence carefully
5. Provide calibrated probability estimate

REASONING:
[Provide 3-4 sentences of reasoning]

PROBABILITY ESTIMATE: [number]%
CONFIDENCE: [number between 40-95]%

KEY_SUPPORTING_FACTORS:
- [factor 1]
- [factor 2]

KEY_COUNTER_FACTORS:
- [factor 1]
- [factor 2]

NOTES: [Any additional context]
"""
        return prompt

    @staticmethod
    def _get_system_prompt() -> str:
        """
        Get system prompt for probability estimation.

        Instructs Claude on how to think about probabilities and calibration.

        Returns:
            System prompt string
        """
        return """You are an expert probability estimator for financial events.

Your goal is to provide well-calibrated probability estimates that reflect true uncertainty.
Key principles:
1. Probabilities near 50% are honest uncertainty, not bias
2. Extreme probabilities (>90%) require very strong evidence
3. Base rates matter - use historical data when relevant
4. Distinguish signal from noise
5. Account for unknown unknowns - avoid false confidence

When estimating, think through:
- What similar events happened historically?
- What would move this probability significantly?
- Am I being overconfident?
- What is the prior probability before any data?

Be precise: give specific probabilities like 57%, not "probably" or "likely".
Be transparent: explain your reasoning clearly so the trader can update you with new info."""

    @staticmethod
    def _parse_probability_response(
        content: str,
        event: str,
        prior: float,
    ) -> ProbabilityEstimate:
        """
        Parse Claude's probability response into structured estimate.

        Args:
            content: Raw response from Claude
            event: Event description
            prior: Prior probability

        Returns:
            ProbabilityEstimate object
        """
        try:
            # Extract probability
            prob_line = [
                line for line in content.split("\n")
                if "PROBABILITY ESTIMATE" in line
            ]
            if prob_line:
                prob_str = prob_line[0].split(":")[-1].strip().rstrip("%")
                posterior = float(prob_str) / 100.0
            else:
                posterior = prior

            # Extract confidence
            conf_line = [
                line for line in content.split("\n")
                if "CONFIDENCE" in line and "PROBABILITY" not in line
            ]
            if conf_line:
                conf_str = conf_line[0].split(":")[-1].strip().rstrip("%")
                confidence = float(conf_str) / 100.0
            else:
                confidence = 0.6

            # Extract factors
            supporting = ClaudeProbabilityEstimator._extract_bullets(
                content, "KEY_SUPPORTING_FACTORS"
            )
            counter = ClaudeProbabilityEstimator._extract_bullets(
                content, "KEY_COUNTER_FACTORS"
            )

            # Extract notes
            notes_start = content.find("NOTES:")
            if notes_start != -1:
                notes = content[notes_start + 6:].strip().split("\n")[0]
            else:
                notes = ""

            return ProbabilityEstimate(
                event_description=event,
                prior_probability=prior,
                posterior_probability=posterior,
                confidence=confidence,
                key_factors=supporting + counter,
                supporting_evidence=supporting,
                counter_evidence=counter,
                notes=notes,
            )

        except Exception as e:
            logger.warning(f"Error parsing response: {e}")
            return ProbabilityEstimate(
                event_description=event,
                prior_probability=prior,
                posterior_probability=prior,
                confidence=0.3,
                key_factors=[],
                supporting_evidence=[],
                counter_evidence=[],
                notes=f"Parsing error: {e}",
            )

    @staticmethod
    def _extract_bullets(text: str, section: str) -> List[str]:
        """
        Extract bullet points from a section.

        Args:
            text: Full text
            section: Section header to look for

        Returns:
            List of bullet points
        """
        try:
            start = text.find(section)
            if start == -1:
                return []

            start = text.find("\n", start) + 1
            end = text.find("\n\n", start)
            if end == -1:
                end = len(text)

            section_text = text[start:end]
            bullets = [
                line.strip("- ").strip()
                for line in section_text.split("\n")
                if line.strip().startswith("-")
            ]
            return bullets

        except Exception:
            return []
