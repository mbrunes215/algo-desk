"""
Opening Range Breakout (ORB) Strategy — /MNQ (Micro E-mini Nasdaq 100)
=======================================================================

HOW IT WORKS:
  The Opening Range Breakout is one of the most studied intraday strategies.
  It defines a "range" (the high and low) during the first N minutes after
  the equity market open (9:30 AM ET), then enters a trade when price breaks
  decisively above the range high or below the range low.

  The thesis: the opening range captures the overnight order flow, institutional
  repositioning, and initial price discovery. A breakout from this range with
  conviction tends to follow through because it represents a shift in intraday
  balance. The edge decays as the day progresses, so we trade only the first
  breakout and close by end of day.

STRATEGY LOGIC:
  1. RANGE FORMATION (9:30–9:45 ET, configurable):
     - Collect 1-minute bars from market open
     - Record the HIGH and LOW of the opening range window
     - Range must have minimum width (avoid flat/pre-holiday opens)

  2. BREAKOUT DETECTION (after range closes):
     - LONG:  1-min close > range HIGH → buy at market, stop at range LOW
     - SHORT: 1-min close < range LOW  → sell at market, stop at range HIGH
     - Only take the FIRST breakout of the day (no re-entry after stops)

  3. RISK MANAGEMENT:
     - Stop loss: opposite side of the range (worst case = range width)
     - Take profit: configurable R:R multiple (default 2.0× the range width)
     - Max loss per trade: range_width × $2/point × 1 contract
     - Daily loss limit: configurable, enforced before entry
     - Hard close: all positions flat by 15:55 ET (before futures close)

  4. EXIT RULES:
     - Stop loss hit → flat, done for the day
     - Take profit hit → flat, done for the day
     - Time stop at 15:55 ET → market close all positions
     - No re-entry after any exit

INSTRUMENT:
  /MNQ — Micro E-mini Nasdaq 100
  - Tick size: 0.25 points
  - Tick value: $0.50 (multiplier = $2/point)
  - Margin: ~$2,000 per contract (varies)
  - RTH: 9:30 AM – 4:00 PM ET
  - Futures session: nearly 24h (Sun 6pm – Fri 5pm ET)

PAPER TRADING NOTE:
  In paper mode with IBKR disconnected, the strategy simulates entries and
  exits using historical/streaming data but does not place real orders.
  When connected to IBKR paper account, orders flow through TWS paper
  trading with simulated fills.

CONFIGURATION (strategies.yaml):
  ibkr_orb:
    enabled: true
    params:
      symbol: MNQ
      range_minutes: 15        # opening range window
      rr_multiple: 2.0         # take profit = range × this
      contracts: 1             # position size
      max_daily_loss_usd: 200  # stop trading for the day
      min_range_points: 10     # skip if range too tight
      max_range_points: 100    # skip if range too wide (news/event)
      close_time_et: "15:55"   # hard close time
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..base_strategy import BaseStrategy, StrategyResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Eastern Time offset from UTC (handles EST/EDT)
# We calculate dynamically but these are the standard offsets
ET_UTC_OFFSET_EST = -5  # Nov–Mar
ET_UTC_OFFSET_EDT = -4  # Mar–Nov

# MNQ contract specs
MNQ_MULTIPLIER = 2.0   # $2 per point
MNQ_TICK_SIZE = 0.25    # minimum price increment

# State file for persistence across restarts
STATE_FILE = Path("logs/orb_state.json")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class OpeningRange:
    """The opening range for a trading session."""
    date: str                   # YYYY-MM-DD
    range_start: datetime       # UTC timestamp of range start
    range_end: datetime         # UTC timestamp of range end
    high: float                 # highest price in range
    low: float                  # lowest price in range
    width_points: float         # high - low
    bar_count: int              # number of bars that formed the range
    is_valid: bool              # True if range meets min/max width requirements

    @property
    def midpoint(self) -> float:
        return (self.high + self.low) / 2.0

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "range_start": self.range_start.isoformat(),
            "range_end": self.range_end.isoformat(),
            "high": self.high,
            "low": self.low,
            "width_points": self.width_points,
            "bar_count": self.bar_count,
            "is_valid": self.is_valid,
        }


@dataclass
class ORBPosition:
    """An active ORB position."""
    direction: str              # 'LONG' or 'SHORT'
    entry_price: float
    entry_time: datetime        # UTC
    stop_price: float
    target_price: float
    contracts: int
    range_width: float          # for P&L calculation
    parent_order_id: Optional[int] = None
    stop_order_id: Optional[int] = None
    target_order_id: Optional[int] = None

    @property
    def risk_usd(self) -> float:
        """Max loss if stopped out."""
        return self.range_width * MNQ_MULTIPLIER * self.contracts

    @property
    def reward_usd(self) -> float:
        """Expected gain if target hit."""
        if self.direction == "LONG":
            return (self.target_price - self.entry_price) * MNQ_MULTIPLIER * self.contracts
        else:
            return (self.entry_price - self.target_price) * MNQ_MULTIPLIER * self.contracts

    def unrealized_pnl(self, current_price: float) -> float:
        """Calculate unrealized P&L at a given price."""
        if self.direction == "LONG":
            return (current_price - self.entry_price) * MNQ_MULTIPLIER * self.contracts
        else:
            return (self.entry_price - current_price) * MNQ_MULTIPLIER * self.contracts

    def to_dict(self) -> dict:
        return {
            "direction": self.direction,
            "entry_price": self.entry_price,
            "entry_time": self.entry_time.isoformat(),
            "stop_price": self.stop_price,
            "target_price": self.target_price,
            "contracts": self.contracts,
            "range_width": self.range_width,
            "parent_order_id": self.parent_order_id,
            "stop_order_id": self.stop_order_id,
            "target_order_id": self.target_order_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "ORBPosition":
        return cls(
            direction=d["direction"],
            entry_price=d["entry_price"],
            entry_time=datetime.fromisoformat(d["entry_time"]),
            stop_price=d["stop_price"],
            target_price=d["target_price"],
            contracts=d["contracts"],
            range_width=d["range_width"],
            parent_order_id=d.get("parent_order_id"),
            stop_order_id=d.get("stop_order_id"),
            target_order_id=d.get("target_order_id"),
        )


@dataclass
class DailyState:
    """Tracks intraday state — resets each session."""
    date: str                           # YYYY-MM-DD
    opening_range: Optional[OpeningRange] = None
    position: Optional[ORBPosition] = None
    breakout_taken: bool = False         # True after first entry (no re-entry)
    daily_pnl_usd: float = 0.0
    trades_today: int = 0
    session_done: bool = False           # True when done for the day (hit limit, closed)
    bars_collected: List[dict] = field(default_factory=list)  # 1-min bars during range

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "opening_range": self.opening_range.to_dict() if self.opening_range else None,
            "position": self.position.to_dict() if self.position else None,
            "breakout_taken": self.breakout_taken,
            "daily_pnl_usd": self.daily_pnl_usd,
            "trades_today": self.trades_today,
            "session_done": self.session_done,
        }


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

def get_et_offset() -> int:
    """
    Get current ET offset from UTC (-4 for EDT, -5 for EST).
    Uses a simple DST rule: EDT from second Sunday of March to first Sunday of November.
    """
    now = datetime.now(timezone.utc)
    year = now.year

    # Second Sunday of March
    march1 = datetime(year, 3, 1, tzinfo=timezone.utc)
    dst_start = march1 + timedelta(days=(6 - march1.weekday()) % 7 + 7)
    dst_start = dst_start.replace(hour=7)  # 2am ET = 7am UTC

    # First Sunday of November
    nov1 = datetime(year, 11, 1, tzinfo=timezone.utc)
    dst_end = nov1 + timedelta(days=(6 - nov1.weekday()) % 7)
    dst_end = dst_end.replace(hour=6)  # 2am ET = 6am UTC

    if dst_start <= now < dst_end:
        return ET_UTC_OFFSET_EDT  # -4
    return ET_UTC_OFFSET_EST  # -5


def utc_now() -> datetime:
    """Current time in UTC."""
    return datetime.now(timezone.utc)


def to_et(utc_dt: datetime) -> datetime:
    """Convert UTC datetime to Eastern Time."""
    offset = get_et_offset()
    et_tz = timezone(timedelta(hours=offset))
    return utc_dt.astimezone(et_tz)


def market_open_utc(date_str: str) -> datetime:
    """Get 9:30 AM ET as UTC for a given date string (YYYY-MM-DD)."""
    offset = get_et_offset()
    et_tz = timezone(timedelta(hours=offset))
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    open_et = dt.replace(hour=9, minute=30, second=0, microsecond=0, tzinfo=et_tz)
    return open_et.astimezone(timezone.utc)


def market_close_utc(date_str: str, close_time_str: str = "15:55") -> datetime:
    """Get close time as UTC for a given date string."""
    offset = get_et_offset()
    et_tz = timezone(timedelta(hours=offset))
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    h, m = map(int, close_time_str.split(":"))
    close_et = dt.replace(hour=h, minute=m, second=0, microsecond=0, tzinfo=et_tz)
    return close_et.astimezone(timezone.utc)


def is_market_hours(now_utc: Optional[datetime] = None) -> bool:
    """Check if current time is during RTH (9:30 AM – 4:00 PM ET)."""
    if now_utc is None:
        now_utc = utc_now()
    et = to_et(now_utc)
    market_open = et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = et.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= et < market_close


def today_str() -> str:
    """Today's date in ET timezone as YYYY-MM-DD."""
    return to_et(utc_now()).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# ORB Strategy
# ---------------------------------------------------------------------------

class ORBStrategy(BaseStrategy):
    """
    Opening Range Breakout strategy for /MNQ.

    Lifecycle per trading day:
      1. Reset state at start of session
      2. Collect bars during range window (9:30–9:45 ET)
      3. Calculate opening range (high/low)
      4. Monitor for breakout above high or below low
      5. Enter on first breakout with bracket order (stop + target)
      6. Exit on stop, target, or time stop (15:55 ET)
      7. Log results and persist state
    """

    def __init__(
        self,
        paper_mode: bool = True,
        config: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name="ORBStrategy",
            enabled=True,
            paper_mode=paper_mode,
        )

        cfg = config or {}

        # Strategy parameters
        self.symbol = cfg.get("symbol", "MNQ")
        self.range_minutes = cfg.get("range_minutes", 15)
        self.rr_multiple = cfg.get("rr_multiple", 2.0)
        self.contracts = cfg.get("contracts", 1)
        self.max_daily_loss_usd = cfg.get("max_daily_loss_usd", 200.0)
        self.min_range_points = cfg.get("min_range_points", 10.0)
        self.max_range_points = cfg.get("max_range_points", 100.0)
        self.close_time_et = cfg.get("close_time_et", "15:55")

        # IBKR executor (injected later or via connect)
        self.executor = None

        # Daily state
        self.state = DailyState(date=today_str())

        # Trade history (persisted)
        self._trade_history: List[dict] = []

        # Load persisted state
        self._load_state()

        logger.info(
            f"ORBStrategy initialized: symbol={self.symbol}, "
            f"range={self.range_minutes}min, R:R={self.rr_multiple}, "
            f"contracts={self.contracts}, paper={paper_mode}"
        )

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        """Load persisted state from JSON."""
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, "r") as f:
                    data = json.load(f)

                # Only restore if same trading day
                saved_date = data.get("date", "")
                if saved_date == today_str():
                    self.state.breakout_taken = data.get("breakout_taken", False)
                    self.state.daily_pnl_usd = data.get("daily_pnl_usd", 0.0)
                    self.state.trades_today = data.get("trades_today", 0)
                    self.state.session_done = data.get("session_done", False)

                    if data.get("position"):
                        self.state.position = ORBPosition.from_dict(data["position"])

                    logger.info(
                        f"Restored ORB state for {saved_date}: "
                        f"breakout_taken={self.state.breakout_taken}, "
                        f"pnl=${self.state.daily_pnl_usd:.2f}"
                    )
                else:
                    logger.info(f"New trading day ({today_str()}), starting fresh")

                # Always load trade history
                self._trade_history = data.get("trade_history", [])

            except Exception as e:
                logger.warning(f"Could not load ORB state: {e}")

    def _save_state(self) -> None:
        """Persist state to JSON."""
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {
                **self.state.to_dict(),
                "trade_history": self._trade_history[-100:],  # keep last 100
            }
            with open(STATE_FILE, "w") as f:
                json.dump(data, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save ORB state: {e}")

    # ------------------------------------------------------------------
    # BaseStrategy interface
    # ------------------------------------------------------------------

    def generate_signals(self) -> StrategyResult:
        """
        Check if we have a breakout signal.

        This is called in the main loop. It checks:
        1. Is the range formed?
        2. Has a breakout occurred?
        3. Should we enter?
        """
        # No signal if session is done
        if self.state.session_done:
            return StrategyResult(signal=False, confidence=0.0, side="HOLD", size=0)

        # No signal if we already took a breakout today
        if self.state.breakout_taken:
            return StrategyResult(signal=False, confidence=0.0, side="HOLD", size=0)

        # No signal if range isn't formed yet
        if not self.state.opening_range or not self.state.opening_range.is_valid:
            return StrategyResult(signal=False, confidence=0.0, side="HOLD", size=0)

        # No signal if outside market hours
        now = utc_now()
        if not is_market_hours(now):
            return StrategyResult(signal=False, confidence=0.0, side="HOLD", size=0)

        # Daily loss limit check
        if self.state.daily_pnl_usd <= -self.max_daily_loss_usd:
            logger.warning(
                f"Daily loss limit reached: ${self.state.daily_pnl_usd:.2f} "
                f"(limit: -${self.max_daily_loss_usd:.2f})"
            )
            self.state.session_done = True
            self._save_state()
            return StrategyResult(signal=False, confidence=0.0, side="HOLD", size=0)

        # If we get here, we're waiting for a breakout.
        # The actual breakout detection happens in process_bar() which sets up
        # the signal. generate_signals() is the check-in for the base class run loop.
        return StrategyResult(signal=False, confidence=0.0, side="HOLD", size=0)

    def execute_trade(self, signal: StrategyResult) -> bool:
        """
        Execute a trade based on signal.
        In practice, ORB entries are handled by process_bar() → _enter_breakout()
        because we need real-time bar data. This method exists to satisfy the
        BaseStrategy interface.
        """
        logger.info(f"execute_trade called: {signal}")
        return True

    def calculate_position_size(self, signal: StrategyResult) -> int:
        """Return configured contract count."""
        return self.contracts

    # ------------------------------------------------------------------
    # Core logic — call these from the runner
    # ------------------------------------------------------------------

    def process_bar(self, bar_time: datetime, open_: float, high: float,
                    low: float, close: float, volume: int) -> Optional[str]:
        """
        Process a new 1-minute bar. This is the heartbeat of the strategy.

        Called by the runner script every minute during RTH.

        Args:
            bar_time: Bar timestamp (UTC)
            open_, high, low, close: OHLC prices
            volume: Bar volume

        Returns:
            Action taken: 'RANGE_BAR', 'RANGE_COMPLETE', 'LONG_ENTRY',
            'SHORT_ENTRY', 'TIME_STOP', 'MONITORING', or None
        """
        now_et = to_et(bar_time)
        date_str = now_et.strftime("%Y-%m-%d")

        # New day? Reset state
        if date_str != self.state.date:
            logger.info(f"New trading day: {date_str}")
            self.state = DailyState(date=date_str)
            self._save_state()

        # Session done? Nothing to do
        if self.state.session_done:
            return None

        open_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        range_end_time = open_time + timedelta(minutes=self.range_minutes)
        close_h, close_m = map(int, self.close_time_et.split(":"))
        hard_close_time = now_et.replace(hour=close_h, minute=close_m, second=0, microsecond=0)

        # ---- PHASE 1: Collecting range bars ----
        if self.state.opening_range is None and open_time <= now_et < range_end_time:
            self.state.bars_collected.append({
                "time": bar_time.isoformat(),
                "open": open_, "high": high, "low": low, "close": close,
                "volume": volume,
            })
            logger.debug(
                f"Range bar {len(self.state.bars_collected)}: "
                f"O={open_:.2f} H={high:.2f} L={low:.2f} C={close:.2f}"
            )
            return "RANGE_BAR"

        # ---- PHASE 2: Range complete, calculate it ----
        if self.state.opening_range is None and now_et >= range_end_time and self.state.bars_collected:
            range_high = max(b["high"] for b in self.state.bars_collected)
            range_low = min(b["low"] for b in self.state.bars_collected)
            width = range_high - range_low

            is_valid = self.min_range_points <= width <= self.max_range_points

            self.state.opening_range = OpeningRange(
                date=date_str,
                range_start=market_open_utc(date_str),
                range_end=market_open_utc(date_str) + timedelta(minutes=self.range_minutes),
                high=range_high,
                low=range_low,
                width_points=width,
                bar_count=len(self.state.bars_collected),
                is_valid=is_valid,
            )

            if is_valid:
                logger.info(
                    f"Opening range formed: HIGH={range_high:.2f} LOW={range_low:.2f} "
                    f"WIDTH={width:.2f}pts "
                    f"(${width * MNQ_MULTIPLIER * self.contracts:.2f} risk per trade)"
                )
            else:
                reason = "too tight" if width < self.min_range_points else "too wide"
                logger.info(
                    f"Opening range INVALID ({reason}): "
                    f"HIGH={range_high:.2f} LOW={range_low:.2f} WIDTH={width:.2f}pts"
                )
                self.state.session_done = True

            self._save_state()
            return "RANGE_COMPLETE"

        # ---- PHASE 3: Time stop — hard close before end of day ----
        if now_et >= hard_close_time and self.state.position:
            pnl = self.state.position.unrealized_pnl(close)
            logger.info(
                f"TIME STOP: closing {self.state.position.direction} at {close:.2f} "
                f"P&L=${pnl:.2f}"
            )
            self._close_position(close, "TIME_STOP")
            return "TIME_STOP"

        # Past hard close with no position? Done
        if now_et >= hard_close_time:
            if not self.state.session_done:
                self.state.session_done = True
                self._save_state()
            return None

        # ---- PHASE 4: Breakout detection (only if range is valid) ----
        if (self.state.opening_range
                and self.state.opening_range.is_valid
                and not self.state.breakout_taken
                and not self.state.position):

            rng = self.state.opening_range

            # Long breakout: bar closes above range high
            if close > rng.high:
                logger.info(
                    f"LONG BREAKOUT: close={close:.2f} > range_high={rng.high:.2f}"
                )
                self._enter_breakout("LONG", close, bar_time)
                return "LONG_ENTRY"

            # Short breakout: bar closes below range low
            if close < rng.low:
                logger.info(
                    f"SHORT BREAKOUT: close={close:.2f} < range_low={rng.low:.2f}"
                )
                self._enter_breakout("SHORT", close, bar_time)
                return "SHORT_ENTRY"

        # ---- PHASE 5: Position management (check stop/target in paper mode) ----
        if self.state.position:
            pos = self.state.position

            # Check stop loss
            if pos.direction == "LONG" and low <= pos.stop_price:
                logger.info(
                    f"STOP HIT (LONG): low={low:.2f} <= stop={pos.stop_price:.2f}"
                )
                self._close_position(pos.stop_price, "STOP_LOSS")
                return "STOP_LOSS"
            elif pos.direction == "SHORT" and high >= pos.stop_price:
                logger.info(
                    f"STOP HIT (SHORT): high={high:.2f} >= stop={pos.stop_price:.2f}"
                )
                self._close_position(pos.stop_price, "STOP_LOSS")
                return "STOP_LOSS"

            # Check take profit
            if pos.direction == "LONG" and high >= pos.target_price:
                logger.info(
                    f"TARGET HIT (LONG): high={high:.2f} >= target={pos.target_price:.2f}"
                )
                self._close_position(pos.target_price, "TARGET")
                return "TARGET"
            elif pos.direction == "SHORT" and low <= pos.target_price:
                logger.info(
                    f"TARGET HIT (SHORT): low={low:.2f} <= target={pos.target_price:.2f}"
                )
                self._close_position(pos.target_price, "TARGET")
                return "TARGET"

        return "MONITORING"

    # ------------------------------------------------------------------
    # Entry / exit logic
    # ------------------------------------------------------------------

    def _enter_breakout(self, direction: str, entry_price: float,
                        entry_time: datetime) -> None:
        """
        Enter a breakout trade.

        Args:
            direction: 'LONG' or 'SHORT'
            entry_price: Price at entry
            entry_time: Timestamp of entry
        """
        rng = self.state.opening_range
        width = rng.width_points

        if direction == "LONG":
            stop_price = rng.low
            target_price = entry_price + (width * self.rr_multiple)
        else:  # SHORT
            stop_price = rng.high
            target_price = entry_price - (width * self.rr_multiple)

        self.state.position = ORBPosition(
            direction=direction,
            entry_price=entry_price,
            entry_time=entry_time,
            stop_price=stop_price,
            target_price=target_price,
            contracts=self.contracts,
            range_width=width,
        )
        self.state.breakout_taken = True
        self.state.trades_today += 1

        risk_usd = self.state.position.risk_usd
        reward_usd = self.state.position.reward_usd

        logger.info(
            f"ENTERED {direction}: price={entry_price:.2f} "
            f"stop={stop_price:.2f} target={target_price:.2f} "
            f"risk=${risk_usd:.2f} reward=${reward_usd:.2f} "
            f"R:R=1:{self.rr_multiple}"
        )

        self._save_state()

    def _close_position(self, exit_price: float, reason: str) -> None:
        """
        Close the current position and log the trade.

        Args:
            exit_price: Price at exit
            reason: 'STOP_LOSS', 'TARGET', or 'TIME_STOP'
        """
        pos = self.state.position
        if not pos:
            return

        pnl = pos.unrealized_pnl(exit_price)
        self.state.daily_pnl_usd += pnl

        trade_record = {
            "date": self.state.date,
            "direction": pos.direction,
            "entry_price": pos.entry_price,
            "entry_time": pos.entry_time.isoformat(),
            "exit_price": exit_price,
            "exit_time": utc_now().isoformat(),
            "exit_reason": reason,
            "contracts": pos.contracts,
            "range_width": pos.range_width,
            "pnl_usd": round(pnl, 2),
            "pnl_points": round(
                (exit_price - pos.entry_price) if pos.direction == "LONG"
                else (pos.entry_price - exit_price), 2
            ),
        }

        self._trade_history.append(trade_record)
        self._trades_log.append({
            "timestamp": utc_now().isoformat(),
            "strategy": self._name,
            "side": pos.direction,
            "size": pos.contracts,
            "confidence": 1.0,
            "paper_mode": self._paper_mode,
            "metadata": trade_record,
        })

        result_emoji = "+" if pnl >= 0 else ""
        logger.info(
            f"CLOSED {pos.direction}: exit={exit_price:.2f} reason={reason} "
            f"P&L={result_emoji}${pnl:.2f} | Daily P&L=${self.state.daily_pnl_usd:.2f}"
        )

        self.state.position = None

        # Check daily loss limit
        if self.state.daily_pnl_usd <= -self.max_daily_loss_usd:
            logger.warning(f"Daily loss limit hit: ${self.state.daily_pnl_usd:.2f}")
            self.state.session_done = True

        # No re-entry after exit
        self.state.session_done = True
        self._save_state()

    # ------------------------------------------------------------------
    # Status / display
    # ------------------------------------------------------------------

    def print_status(self) -> None:
        """Print current strategy status to logger."""
        now = to_et(utc_now())
        logger.info(f"=== ORB Status ({now.strftime('%H:%M ET')}) ===")
        logger.info(f"  Symbol: {self.symbol}")
        logger.info(f"  Date: {self.state.date}")
        logger.info(f"  Paper mode: {self._paper_mode}")

        if self.state.opening_range:
            rng = self.state.opening_range
            logger.info(
                f"  Range: HIGH={rng.high:.2f} LOW={rng.low:.2f} "
                f"WIDTH={rng.width_points:.2f}pts "
                f"({'VALID' if rng.is_valid else 'INVALID'})"
            )
        else:
            bars = len(self.state.bars_collected)
            logger.info(f"  Range: forming ({bars} bars collected)")

        if self.state.position:
            pos = self.state.position
            logger.info(
                f"  Position: {pos.direction} @ {pos.entry_price:.2f} "
                f"stop={pos.stop_price:.2f} target={pos.target_price:.2f}"
            )
        else:
            logger.info(f"  Position: FLAT")

        logger.info(f"  Breakout taken: {self.state.breakout_taken}")
        logger.info(f"  Daily P&L: ${self.state.daily_pnl_usd:.2f}")
        logger.info(f"  Session done: {self.state.session_done}")
        logger.info(f"  Trades today: {self.state.trades_today}")

    def get_summary(self) -> Dict[str, Any]:
        """Get strategy summary as a dict (for daily report integration)."""
        return {
            "strategy": "ORB",
            "symbol": self.symbol,
            "date": self.state.date,
            "paper_mode": self._paper_mode,
            "range": self.state.opening_range.to_dict() if self.state.opening_range else None,
            "position": self.state.position.to_dict() if self.state.position else None,
            "breakout_taken": self.state.breakout_taken,
            "daily_pnl_usd": self.state.daily_pnl_usd,
            "trades_today": self.state.trades_today,
            "session_done": self.state.session_done,
            "total_trades": len(self._trade_history),
        }

    def get_trade_history(self, last_n: int = 20) -> List[dict]:
        """Get recent trade history."""
        return self._trade_history[-last_n:]

    def get_stats(self) -> Dict[str, Any]:
        """Calculate performance statistics from trade history."""
        if not self._trade_history:
            return {
                "total_trades": 0, "win_rate": 0.0, "avg_pnl": 0.0,
                "total_pnl": 0.0, "avg_winner": 0.0, "avg_loser": 0.0,
                "profit_factor": 0.0, "max_win": 0.0, "max_loss": 0.0,
            }

        trades = self._trade_history
        pnls = [t["pnl_usd"] for t in trades]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p < 0]

        gross_profit = sum(winners) if winners else 0
        gross_loss = abs(sum(losers)) if losers else 0

        return {
            "total_trades": len(trades),
            "winners": len(winners),
            "losers": len(losers),
            "win_rate": len(winners) / len(trades) if trades else 0,
            "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
            "total_pnl": sum(pnls),
            "avg_winner": sum(winners) / len(winners) if winners else 0,
            "avg_loser": sum(losers) / len(losers) if losers else 0,
            "profit_factor": gross_profit / gross_loss if gross_loss > 0 else float("inf"),
            "max_win": max(pnls) if pnls else 0,
            "max_loss": min(pnls) if pnls else 0,
            "exit_reasons": {
                "STOP_LOSS": sum(1 for t in trades if t["exit_reason"] == "STOP_LOSS"),
                "TARGET": sum(1 for t in trades if t["exit_reason"] == "TARGET"),
                "TIME_STOP": sum(1 for t in trades if t["exit_reason"] == "TIME_STOP"),
            },
        }
