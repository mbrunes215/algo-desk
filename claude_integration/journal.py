"""
Automated Trade Journal Module

Uses Claude to automatically analyze and journal trades, providing insights
into decision quality, edge recognition, and trading pattern identification.

Helps traders develop better intuition and decision-making by maintaining
a structured record with AI-assisted commentary.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
import json

try:
    from anthropic import Anthropic
except ImportError:
    raise ImportError(
        "anthropic package required. Install with: pip install anthropic"
    )

logger = logging.getLogger(__name__)


@dataclass
class TradeEntry:
    """
    A single trade entry in the journal.

    Attributes:
        trade_id: Unique identifier for this trade
        timestamp: When the trade was executed
        contract_id: What was traded
        action: BUY or SELL
        size: Position size
        entry_price: Entry price
        exit_price: Exit price (if closed)
        exit_timestamp: When position was closed
        rationale: Why trade was taken (from signal)
        realized_pnl: P&L if closed, None if open
        decision_quality_score: Claude's assessment (0-100)
        analysis: Claude's commentary on the trade
        key_insights: Lessons from this trade
        market_context: Market conditions at entry
    """
    trade_id: str
    timestamp: datetime
    contract_id: str
    action: str  # BUY or SELL
    size: float
    entry_price: float
    exit_price: Optional[float] = None
    exit_timestamp: Optional[datetime] = None
    rationale: str = ""
    realized_pnl: Optional[float] = None
    decision_quality_score: Optional[float] = None
    analysis: str = ""
    key_insights: List[str] = field(default_factory=list)
    market_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WeeklyReport:
    """
    Weekly trading report with aggregate analysis.

    Attributes:
        week_start: Start of week
        week_end: End of week
        num_trades: Number of trades in period
        win_rate: Percentage of winning trades
        avg_win: Average P&L on winners
        avg_loss: Average P&L on losers
        total_pnl: Total P&L for week
        best_trade: Best performing trade
        worst_trade: Worst performing trade
        patterns_identified: Trading patterns noticed
        improvement_areas: Areas to work on
        next_week_focus: Recommendations for next week
    """
    week_start: datetime
    week_end: datetime
    num_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    total_pnl: float
    best_trade: TradeEntry
    worst_trade: TradeEntry
    patterns_identified: List[str]
    improvement_areas: List[str]
    next_week_focus: List[str]


class TradeJournal:
    """
    Automated trade journaling using Claude.

    Maintains a journal of all trades with AI-assisted analysis.
    Helps traders understand decision quality and identify patterns.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-3-5-sonnet-20241022",
    ):
        """
        Initialize the trade journal.

        Args:
            api_key: Anthropic API key
            model: Claude model to use
        """
        self.client = Anthropic(api_key=api_key) if api_key else Anthropic()
        self.model = model
        self.trades: List[TradeEntry] = []

    def log_trade_with_analysis(
        self,
        trade_data: Dict[str, Any],
    ) -> TradeEntry:
        """
        Log a trade and immediately analyze it with Claude.

        Claude evaluates:
        1. Was the rationale sound?
        2. Was risk/reward appropriate?
        3. Was timing good?
        4. How does this compare to similar past trades?
        5. What can be learned?

        Args:
            trade_data: Dict with trade details:
                - "contract_id": str
                - "action": "BUY" or "SELL"
                - "size": float
                - "entry_price": float
                - "timestamp": datetime
                - "rationale": str (why trade was taken)
                - "market_context": dict (market conditions)
                - ["exit_price"]: float (optional)
                - ["exit_timestamp"]: datetime (optional)

        Returns:
            TradeEntry with analysis populated
        """
        logger.info(f"Logging trade: {trade_data.get('contract_id')}")

        # Build trade entry
        trade_id = f"{trade_data['contract_id']}_{int(trade_data['timestamp'].timestamp())}"

        trade_entry = TradeEntry(
            trade_id=trade_id,
            timestamp=trade_data["timestamp"],
            contract_id=trade_data["contract_id"],
            action=trade_data["action"],
            size=trade_data["size"],
            entry_price=trade_data["entry_price"],
            exit_price=trade_data.get("exit_price"),
            exit_timestamp=trade_data.get("exit_timestamp"),
            rationale=trade_data.get("rationale", ""),
            market_context=trade_data.get("market_context", {}),
        )

        # Calculate P&L if closed
        if trade_entry.exit_price:
            if trade_entry.action == "BUY":
                trade_entry.realized_pnl = (
                    (trade_entry.exit_price - trade_entry.entry_price)
                    * trade_entry.size
                )
            else:  # SELL
                trade_entry.realized_pnl = (
                    (trade_entry.entry_price - trade_entry.exit_price)
                    * trade_entry.size
                )

        # Get Claude analysis
        prompt = self._build_trade_analysis_prompt(trade_entry)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=800,
                system=self._get_journal_system_prompt(),
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )

            content = response.content[0].text

            # Parse analysis
            (quality_score, analysis, insights) = self._parse_trade_analysis(
                content
            )

            trade_entry.decision_quality_score = quality_score
            trade_entry.analysis = analysis
            trade_entry.key_insights = insights

        except Exception as e:
            logger.error(f"Failed to analyze trade: {e}")
            trade_entry.analysis = f"Analysis failed: {e}"

        # Store trade
        self.trades.append(trade_entry)

        return trade_entry

    def generate_weekly_report(
        self,
        trades: Optional[List[TradeEntry]] = None,
    ) -> WeeklyReport:
        """
        Generate comprehensive weekly report with Claude analysis.

        Summarizes:
        - Trade statistics
        - Decision quality trends
        - Patterns in winning vs losing trades
        - Areas for improvement
        - Focus areas for next week

        Args:
            trades: List of trades to analyze (defaults to recent week)

        Returns:
            WeeklyReport object
        """
        logger.info("Generating weekly report")

        if trades is None:
            # Use trades from past 7 days
            from datetime import timedelta
            cutoff = datetime.utcnow() - timedelta(days=7)
            trades = [t for t in self.trades if t.timestamp >= cutoff]

        if not trades:
            logger.warning("No trades for weekly report")
            return self._empty_weekly_report()

        # Calculate statistics
        num_trades = len(trades)
        winning_trades = [
            t for t in trades
            if t.realized_pnl and t.realized_pnl > 0
        ]
        losing_trades = [
            t for t in trades
            if t.realized_pnl and t.realized_pnl < 0
        ]

        win_rate = len(winning_trades) / num_trades if num_trades > 0 else 0
        avg_win = (
            sum(t.realized_pnl for t in winning_trades) / len(winning_trades)
            if winning_trades else 0
        )
        avg_loss = (
            sum(t.realized_pnl for t in losing_trades) / len(losing_trades)
            if losing_trades else 0
        )
        total_pnl = sum(
            t.realized_pnl for t in trades if t.realized_pnl
        )

        best_trade = max(
            [t for t in trades if t.realized_pnl],
            key=lambda t: t.realized_pnl,
            default=trades[0],
        )
        worst_trade = min(
            [t for t in trades if t.realized_pnl],
            key=lambda t: t.realized_pnl,
            default=trades[0],
        )

        # Get Claude analysis
        prompt = self._build_weekly_report_prompt(
            trades, win_rate, avg_win, avg_loss, total_pnl
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1200,
                system=self._get_journal_system_prompt(),
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )

            content = response.content[0].text
            (
                patterns,
                improvements,
                focus_items,
            ) = self._parse_weekly_report(content)

        except Exception as e:
            logger.error(f"Failed to generate weekly report: {e}")
            patterns, improvements, focus_items = [], [], []

        return WeeklyReport(
            week_start=min(t.timestamp for t in trades),
            week_end=max(t.timestamp for t in trades),
            num_trades=num_trades,
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            total_pnl=total_pnl,
            best_trade=best_trade,
            worst_trade=worst_trade,
            patterns_identified=patterns,
            improvement_areas=improvements,
            next_week_focus=focus_items,
        )

    def identify_patterns(
        self,
        trade_history: Optional[List[TradeEntry]] = None,
    ) -> List[str]:
        """
        Identify patterns in trade history using Claude.

        Looks for:
        - Recurring losing patterns
        - Winning trade characteristics
        - Time-based patterns (time of day, day of week)
        - Contract-specific patterns
        - Decision quality trends

        Args:
            trade_history: List of trades to analyze (defaults to all)

        Returns:
            List of identified patterns with descriptions
        """
        logger.info("Identifying trading patterns")

        if trade_history is None:
            trade_history = self.trades

        if not trade_history:
            return []

        # Organize trades by various dimensions
        by_contract = {}
        by_hour = {}
        winning = []
        losing = []

        for trade in trade_history:
            # By contract
            contract = trade.contract_id
            if contract not in by_contract:
                by_contract[contract] = []
            by_contract[contract].append(trade)

            # By hour of day
            hour = trade.timestamp.hour
            if hour not in by_hour:
                by_hour[hour] = []
            by_hour[hour].append(trade)

            # Winning vs losing
            if trade.realized_pnl:
                if trade.realized_pnl > 0:
                    winning.append(trade)
                else:
                    losing.append(trade)

        prompt = self._build_pattern_analysis_prompt(
            trade_history, by_contract, by_hour, winning, losing
        )

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=1000,
                system=self._get_journal_system_prompt(),
                messages=[
                    {"role": "user", "content": prompt}
                ],
            )

            content = response.content[0].text
            patterns = self._extract_patterns(content)

        except Exception as e:
            logger.error(f"Failed to identify patterns: {e}")
            patterns = []

        return patterns

    def _build_trade_analysis_prompt(self, trade: TradeEntry) -> str:
        """Build prompt for analyzing a single trade."""
        exit_status = "CLOSED" if trade.exit_price else "OPEN"
        pnl_str = f"P&L: ${trade.realized_pnl:+,.0f}" if trade.realized_pnl else "Position still open"

        prompt = f"""
Analyze this trade for decision quality and learning opportunities.

TRADE DETAILS:
- Contract: {trade.contract_id}
- Action: {trade.action} {trade.size:,.0f}
- Entry Price: ${trade.entry_price:.2f}
- Entry Time: {trade.timestamp.strftime('%Y-%m-%d %H:%M UTC')}
- Status: {exit_status}
{f"- Exit Price: ${trade.exit_price:.2f}" if trade.exit_price else ""}
{f"- Exit Time: {trade.exit_timestamp.strftime('%Y-%m-%d %H:%M UTC')}" if trade.exit_timestamp else ""}
- {pnl_str}

RATIONALE:
{trade.rationale}

MARKET CONTEXT:
{json.dumps(trade.market_context, indent=2, default=str)}

ANALYZE:
1. Was the trade rationale sound?
2. Did the trader understand the edge?
3. Was position sizing appropriate?
4. What went well or poorly?
5. What should be learned from this trade?

FORMAT YOUR RESPONSE AS:
DECISION_QUALITY_SCORE: [0-100]
ANALYSIS: [2-3 sentences analyzing the trade]
KEY_INSIGHTS:
- [insight 1]
- [insight 2]
"""
        return prompt

    def _build_weekly_report_prompt(
        self,
        trades: List[TradeEntry],
        win_rate: float,
        avg_win: float,
        avg_loss: float,
        total_pnl: float,
    ) -> str:
        """Build prompt for weekly report."""
        trades_summary = "\n".join([
            f"- {t.contract_id}: {t.action} {t.realized_pnl:+,.0f}"
            for t in trades if t.realized_pnl
        ])[:500]

        prompt = f"""
Generate a comprehensive weekly trading review.

WEEKLY STATISTICS:
- Total Trades: {len(trades)}
- Win Rate: {win_rate:.1%}
- Average Winner: ${avg_win:,.0f}
- Average Loser: ${avg_loss:,.0f}
- Total P&L: ${total_pnl:+,.0f}

TRADES EXECUTED:
{trades_summary}

PROVIDE:
1. PATTERNS: What patterns do you see in the trades?
2. IMPROVEMENTS: What areas need work?
3. NEXT_WEEK: What should the trader focus on?

FORMAT:
PATTERNS_IDENTIFIED:
- [pattern 1]
- [pattern 2]

IMPROVEMENT_AREAS:
- [area 1]
- [area 2]

NEXT_WEEK_FOCUS:
- [focus 1]
- [focus 2]
"""
        return prompt

    def _build_pattern_analysis_prompt(
        self,
        all_trades: List[TradeEntry],
        by_contract: Dict[str, List[TradeEntry]],
        by_hour: Dict[int, List[TradeEntry]],
        winning: List[TradeEntry],
        losing: List[TradeEntry],
    ) -> str:
        """Build prompt for pattern identification."""
        prompt = f"""
Analyze the following trading history to identify patterns.

OVERALL STATISTICS:
- Total trades: {len(all_trades)}
- Winning trades: {len(winning)}
- Losing trades: {len(losing)}
- Win rate: {len(winning) / len(all_trades) if all_trades else 0:.1%}

TOP CONTRACTS BY VOLUME:
{chr(10).join([f"- {c}: {len(ts)} trades" for c, ts in sorted(by_contract.items(), key=lambda x: len(x[1]), reverse=True)[:5]])}

WHAT PATTERNS DO YOU SEE?
1. Patterns in winning vs losing trades
2. Contract-specific patterns
3. Time-based patterns (trading better at certain times?)
4. Decision quality patterns

RESPOND WITH SPECIFIC PATTERNS AND EXAMPLES.
"""
        return prompt

    @staticmethod
    def _get_journal_system_prompt() -> str:
        """Get system prompt for trade journal analysis."""
        return """You are an experienced trading coach and journal analyst.

Your role is to help traders improve by:
1. Analyzing decision quality fairly but objectively
2. Identifying patterns in trading behavior
3. Highlighting learning opportunities
4. Providing constructive feedback
5. Helping the trader see what's working and what isn't

Be honest but encouraging. The goal is improvement, not criticism.
Provide specific, actionable feedback when possible."""

    @staticmethod
    def _parse_trade_analysis(content: str) -> tuple:
        """Parse Claude's trade analysis response."""
        try:
            quality_line = [
                line for line in content.split("\n")
                if "DECISION_QUALITY_SCORE" in line
            ]
            if quality_line:
                quality_str = quality_line[0].split(":")[-1].strip()
                quality_score = float(quality_str)
            else:
                quality_score = 50.0

            # Extract analysis text
            analysis_start = content.find("ANALYSIS:")
            if analysis_start != -1:
                analysis_end = content.find("\nKEY_INSIGHTS:", analysis_start)
                if analysis_end == -1:
                    analysis_end = len(content)
                analysis = content[analysis_start + 9:analysis_end].strip()
            else:
                analysis = content[:200]

            # Extract insights
            insights = TradeJournal._extract_bullets(
                content, "KEY_INSIGHTS"
            )

            return (quality_score, analysis, insights)

        except Exception as e:
            logger.warning(f"Error parsing trade analysis: {e}")
            return (50.0, "", [])

    @staticmethod
    def _parse_weekly_report(content: str) -> tuple:
        """Parse Claude's weekly report response."""
        try:
            patterns = TradeJournal._extract_bullets(
                content, "PATTERNS_IDENTIFIED"
            )
            improvements = TradeJournal._extract_bullets(
                content, "IMPROVEMENT_AREAS"
            )
            focus = TradeJournal._extract_bullets(
                content, "NEXT_WEEK_FOCUS"
            )
            return (patterns, improvements, focus)

        except Exception as e:
            logger.warning(f"Error parsing weekly report: {e}")
            return ([], [], [])

    @staticmethod
    def _extract_bullets(text: str, section: str) -> List[str]:
        """Extract bullet points from response."""
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

    @staticmethod
    def _extract_patterns(content: str) -> List[str]:
        """Extract patterns from analysis."""
        return TradeJournal._extract_bullets(content, "PATTERNS")

    @staticmethod
    def _empty_weekly_report() -> WeeklyReport:
        """Return empty weekly report when no trades."""
        now = datetime.utcnow()
        empty_trade = TradeEntry(
            trade_id="",
            timestamp=now,
            contract_id="N/A",
            action="BUY",
            size=0,
            entry_price=0,
        )

        return WeeklyReport(
            week_start=now,
            week_end=now,
            num_trades=0,
            win_rate=0,
            avg_win=0,
            avg_loss=0,
            total_pnl=0,
            best_trade=empty_trade,
            worst_trade=empty_trade,
            patterns_identified=[],
            improvement_areas=[],
            next_week_focus=[],
        )
