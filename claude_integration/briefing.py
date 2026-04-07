"""
Daily Briefing Generator Module

Uses Claude to generate structured trading briefings and reviews.
Provides market context, position summaries, and strategic analysis.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

try:
    from anthropic import Anthropic
except ImportError:
    raise ImportError(
        "anthropic package required. Install with: pip install anthropic"
    )

logger = logging.getLogger(__name__)


@dataclass
class BriefingResult:
    """
    Result of a briefing generation.

    Attributes:
        briefing_type: "premarket", "postmarket", or "analysis"
        generated_at: Timestamp of generation
        content: The generated briefing text
        key_points: List of bullet-point summaries
        action_items: List of recommended actions
        risk_alerts: Any risk factors identified
    """
    briefing_type: str
    generated_at: datetime
    content: str
    key_points: List[str]
    action_items: List[str]
    risk_alerts: List[str]


class DailyBriefing:
    """
    Generates daily trading briefings using Claude.

    Uses Claude's language model to synthesize market data, positions,
    and trading signals into coherent, actionable briefings.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-3-5-sonnet-20241022",
    ):
        """
        Initialize the daily briefing generator.

        Args:
            api_key: Anthropic API key (defaults to ANTHROPIC_API_KEY env var)
            model: Claude model to use
        """
        self.client = Anthropic(api_key=api_key) if api_key else Anthropic()
        self.model = model
        self.conversation_history: List[Dict[str, str]] = []

    def generate_premarket_briefing(
        self,
        positions: Dict[str, Any],
        calendar: List[Dict[str, Any]],
        news: List[str],
        market_levels: Optional[Dict[str, float]] = None,
    ) -> BriefingResult:
        """
        Generate a pre-market briefing summarizing the day ahead.

        This briefing synthesizes:
        - Current positions and exposure
        - Economic calendar events scheduled
        - Recent relevant news
        - Overnight price action
        - Key risks and opportunities

        Args:
            positions: Dict of current holdings, e.g.,
                {"KLS:TEMP_ABOVE_72": {"size": 10, "entry": 65}, ...}
            calendar: List of economic events scheduled today
            news: List of relevant news headlines
            market_levels: Dict of key market prices

        Returns:
            BriefingResult with structured premarket summary
        """
        logger.info("Generating premarket briefing")

        # Format positions for Claude
        positions_str = self._format_positions(positions)
        calendar_str = self._format_calendar(calendar)
        news_str = "\n".join([f"- {n}" for n in news]) if news else "No major news"

        prompt = f"""
You are a quantitative trading assistant providing a concise pre-market briefing.

CURRENT POSITIONS:
{positions_str}

TODAY'S ECONOMIC CALENDAR:
{calendar_str}

OVERNIGHT NEWS:
{news_str}

Generate a brief pre-market briefing that:
1. Summarizes current risk exposure (long/short, by strategy)
2. Identifies key events that could impact positions
3. Highlights risks to watch
4. Suggests any tactical adjustments needed
5. Recommends position monitoring priority

Format your response as:
BRIEFING:
[Main briefing text, 2-3 paragraphs]

KEY POINTS:
- [bullet 1]
- [bullet 2]
- [bullet 3]

RISK ALERTS:
- [risk 1]
- [risk 2]

ACTION ITEMS:
- [action 1]
- [action 2]
"""

        result = self._call_claude(prompt, "premarket")
        return result

    def generate_postmarket_review(
        self,
        trades_today: List[Dict[str, Any]],
        pnl: Dict[str, float],
        positions_closed: List[Dict[str, Any]],
        market_moves: Dict[str, float],
    ) -> BriefingResult:
        """
        Generate a post-market review of the day's trading.

        Analyzes:
        - Trades executed and their rationale
        - P&L attribution by strategy
        - Which signals worked well, which didn't
        - Position adjustments made
        - Lessons for tomorrow

        Args:
            trades_today: List of executed trades with details
            pnl: Dict of P&L by strategy, e.g., {"weather": 1250, "econ": -340}
            positions_closed: List of positions closed today
            market_moves: Dict of significant market moves

        Returns:
            BriefingResult with postmarket review
        """
        logger.info("Generating postmarket review")

        trades_str = self._format_trades(trades_today)
        pnl_str = self._format_pnl(pnl)
        moves_str = "\n".join(
            [f"- {k}: {v:+.1f}%" for k, v in market_moves.items()]
        ) if market_moves else "Minor moves"

        prompt = f"""
You are a quantitative trading assistant providing a post-market review.

TRADES EXECUTED TODAY:
{trades_str}

P&L SUMMARY:
{pnl_str}

NOTABLE MARKET MOVES:
{moves_str}

Generate a post-market review that:
1. Analyzes execution quality and timing
2. Evaluates whether signals performed as expected
3. Identifies wins and learning opportunities
4. Assesses strategy performance
5. Recommends adjustments for tomorrow

Format your response as:
BRIEFING:
[Review summary, 2-3 paragraphs]

KEY POINTS:
- [bullet 1]
- [bullet 2]
- [bullet 3]

ACTION ITEMS:
- [adjustment 1]
- [adjustment 2]

RISK ALERTS:
- [any emerging risks]
"""

        result = self._call_claude(prompt, "postmarket")
        return result

    def analyze_strategy_performance(
        self,
        backtest_results: Dict[str, Any],
        parameter_sensitivity: Optional[Dict[str, Any]] = None,
    ) -> BriefingResult:
        """
        Deep analysis of strategy performance from backtest results.

        Examines:
        - Win rate, profit factor, Sharpe ratio
        - Drawdown characteristics
        - Parameter sensitivity
        - Robustness across time periods
        - Recommended parameter adjustments

        Args:
            backtest_results: Dict with keys like "win_rate", "profit_factor",
                "max_drawdown", "annual_return", "trades_list"
            parameter_sensitivity: Dict showing performance across parameters

        Returns:
            BriefingResult with deep performance analysis
        """
        logger.info("Analyzing strategy performance")

        results_str = self._format_backtest_results(backtest_results)
        params_str = (
            self._format_parameter_sensitivity(parameter_sensitivity)
            if parameter_sensitivity else "Not provided"
        )

        prompt = f"""
You are a quantitative finance analyst evaluating strategy backtests.

BACKTEST RESULTS:
{results_str}

PARAMETER SENSITIVITY ANALYSIS:
{params_str}

Provide a detailed analysis that:
1. Evaluates the strategy's statistical strength
2. Identifies potential overfitting concerns
3. Assesses robustness
4. Recommends parameter adjustments
5. Suggests forward-testing approach

Format your response as:
ANALYSIS:
[Detailed analysis, 3-4 paragraphs]

KEY FINDINGS:
- [finding 1]
- [finding 2]
- [finding 3]

RECOMMENDED ADJUSTMENTS:
- [adjustment 1]
- [adjustment 2]

RISK ALERTS:
- [risk 1 - e.g., overfitting, low sample size]
"""

        result = self._call_claude(prompt, "analysis")
        return result

    def _call_claude(self, prompt: str, briefing_type: str) -> BriefingResult:
        """
        Call Claude API and parse response into BriefingResult.

        Args:
            prompt: The prompt to send to Claude
            briefing_type: Type of briefing being generated

        Returns:
            BriefingResult object
        """
        try:
            # Add user message to conversation
            self.conversation_history.append(
                {"role": "user", "content": prompt}
            )

            # Call Claude
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                system="You are an expert quantitative trading analyst. Provide clear, "
                       "actionable insights backed by data. Be concise but comprehensive.",
                messages=self.conversation_history,
            )

            # Extract response
            content = response.content[0].text

            # Add assistant message to conversation
            self.conversation_history.append(
                {"role": "assistant", "content": content}
            )

            # Parse response into structured format
            briefing_result = self._parse_briefing_response(
                content, briefing_type
            )

            return briefing_result

        except Exception as e:
            logger.error(f"Failed to call Claude: {e}")
            # Return empty briefing on error
            return BriefingResult(
                briefing_type=briefing_type,
                generated_at=datetime.utcnow(),
                content=f"Error generating briefing: {e}",
                key_points=[],
                action_items=[],
                risk_alerts=[],
            )

    @staticmethod
    def _parse_briefing_response(
        content: str, briefing_type: str
    ) -> BriefingResult:
        """
        Parse Claude's response into structured BriefingResult.

        Extracts sections like KEY POINTS, ACTION ITEMS, RISK ALERTS.

        Args:
            content: Raw response from Claude
            briefing_type: Type of briefing

        Returns:
            BriefingResult with parsed fields
        """
        key_points = DailyBriefing._extract_section(content, "KEY POINTS:")
        action_items = DailyBriefing._extract_section(content, "ACTION ITEMS:")
        risk_alerts = DailyBriefing._extract_section(content, "RISK ALERTS:")

        return BriefingResult(
            briefing_type=briefing_type,
            generated_at=datetime.utcnow(),
            content=content,
            key_points=key_points,
            action_items=action_items,
            risk_alerts=risk_alerts,
        )

    @staticmethod
    def _extract_section(text: str, section_header: str) -> List[str]:
        """
        Extract bullet points from a section of text.

        Args:
            text: Full text to search
            section_header: Header to look for (e.g., "KEY POINTS:")

        Returns:
            List of bullet points in that section
        """
        try:
            start_idx = text.find(section_header)
            if start_idx == -1:
                return []

            start_idx += len(section_header)
            # Find next section or end
            next_section = text.find(":", start_idx + 1)
            if next_section == -1:
                end_text = text[start_idx:]
            else:
                end_text = text[start_idx:next_section]

            # Extract bullet points
            lines = end_text.split("\n")
            bullets = [
                line.strip("- ").strip()
                for line in lines
                if line.strip().startswith("-")
            ]
            return bullets

        except Exception as e:
            logger.warning(f"Failed to extract section {section_header}: {e}")
            return []

    @staticmethod
    def _format_positions(positions: Dict[str, Any]) -> str:
        """Format positions dict for Claude prompt."""
        if not positions:
            return "No open positions"

        lines = []
        for contract_id, details in positions.items():
            size = details.get("size", 0)
            entry = details.get("entry", 0)
            current = details.get("current", entry)
            pnl = (current - entry) * size if entry else 0
            lines.append(
                f"- {contract_id}: {size:+d} contracts @ {entry:.1f}, "
                f"current {current:.1f} (P&L: {pnl:+.0f})"
            )

        return "\n".join(lines)

    @staticmethod
    def _format_calendar(calendar: List[Dict[str, Any]]) -> str:
        """Format economic calendar for Claude prompt."""
        if not calendar:
            return "No scheduled events"

        lines = []
        for event in calendar:
            time = event.get("time", "TBD")
            indicator = event.get("indicator", "Unknown")
            consensus = event.get("consensus", "TBD")
            lines.append(f"- {time}: {indicator} (consensus: {consensus})")

        return "\n".join(lines)

    @staticmethod
    def _format_trades(trades: List[Dict[str, Any]]) -> str:
        """Format executed trades for Claude prompt."""
        if not trades:
            return "No trades executed"

        lines = []
        for trade in trades:
            contract = trade.get("contract_id", "Unknown")
            action = trade.get("action", "Unknown")
            size = trade.get("size", 0)
            price = trade.get("price", 0)
            reason = trade.get("reason", "Tactical adjustment")
            lines.append(
                f"- {action} {size} {contract} @ {price:.1f} - {reason}"
            )

        return "\n".join(lines)

    @staticmethod
    def _format_pnl(pnl: Dict[str, float]) -> str:
        """Format P&L by strategy for Claude prompt."""
        lines = []
        total = 0
        for strategy, pnl_val in pnl.items():
            lines.append(f"- {strategy}: ${pnl_val:+,.0f}")
            total += pnl_val

        lines.append(f"\nTotal: ${total:+,.0f}")
        return "\n".join(lines)

    @staticmethod
    def _format_backtest_results(results: Dict[str, Any]) -> str:
        """Format backtest results for Claude prompt."""
        lines = [
            f"Win Rate: {results.get('win_rate', 0):.1%}",
            f"Profit Factor: {results.get('profit_factor', 0):.2f}",
            f"Sharpe Ratio: {results.get('sharpe_ratio', 0):.2f}",
            f"Annual Return: {results.get('annual_return', 0):.1%}",
            f"Max Drawdown: {results.get('max_drawdown', 0):.1%}",
            f"Total Trades: {results.get('num_trades', 0)}",
        ]
        return "\n".join(lines)

    @staticmethod
    def _format_parameter_sensitivity(params: Dict[str, Any]) -> str:
        """Format parameter sensitivity analysis for Claude prompt."""
        lines = []
        for param_name, results in params.items():
            lines.append(f"{param_name}:")
            for param_val, metrics in results.items():
                sharpe = metrics.get("sharpe", 0)
                lines.append(f"  {param_val}: Sharpe {sharpe:.2f}")

        return "\n".join(lines)
