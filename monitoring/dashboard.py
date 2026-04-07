"""
Trading dashboard for monitoring positions, P&L, and strategy status.

Provides both terminal-based and HTML dashboard output for real-time monitoring
of trading activity and system health.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
import html


@dataclass
class DashboardState:
    """Represents the current state of trading dashboard."""

    positions: List[Dict[str, Any]]
    daily_pnl: float
    daily_pnl_percent: float
    strategy_status: Dict[str, str]
    recent_trades: List[Dict[str, Any]]
    system_health: Dict[str, Any]
    timestamp: datetime


class TradingDashboard:
    """Terminal-based dashboard for monitoring trading activity.

    Displays current positions, daily P&L, strategy status, recent trades,
    and system health in a formatted terminal output.
    """

    def __init__(self, max_recent_trades: int = 10) -> None:
        """Initialize the trading dashboard.

        Args:
            max_recent_trades: Maximum number of recent trades to display.
        """
        self.max_recent_trades = max_recent_trades
        self.last_refresh: Optional[datetime] = None
        self.current_state: Optional[DashboardState] = None

    def refresh(
        self,
        positions: List[Dict[str, Any]],
        daily_pnl: float,
        daily_pnl_percent: float,
        strategy_status: Dict[str, str],
        recent_trades: List[Dict[str, Any]],
        system_health: Dict[str, Any],
    ) -> None:
        """Refresh dashboard with latest data.

        Args:
            positions: List of current positions.
            daily_pnl: Daily profit/loss in absolute terms.
            daily_pnl_percent: Daily profit/loss as percentage.
            strategy_status: Dictionary of strategy names to status strings.
            recent_trades: List of recent trades.
            system_health: Dictionary of system health metrics.
        """
        self.current_state = DashboardState(
            positions=positions[:self.max_recent_trades],
            daily_pnl=daily_pnl,
            daily_pnl_percent=daily_pnl_percent,
            strategy_status=strategy_status,
            recent_trades=recent_trades[:self.max_recent_trades],
            system_health=system_health,
            timestamp=datetime.now(),
        )
        self.last_refresh = datetime.now()

    def display(self) -> str:
        """Generate formatted terminal display.

        Returns:
            Formatted string suitable for terminal output.
        """
        if not self.current_state:
            return "No data available. Call refresh() first."

        state = self.current_state
        output: List[str] = []

        # Header
        output.append("=" * 100)
        output.append(f"TRADING DASHBOARD - {state.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        output.append("=" * 100)
        output.append("")

        # P&L Section
        output.append("--- DAILY P&L ---")
        pnl_color = "+" if state.daily_pnl >= 0 else "-"
        output.append(
            f"  Daily P&L: {pnl_color}${abs(state.daily_pnl):,.2f} "
            f"({pnl_color}{abs(state.daily_pnl_percent):.2f}%)"
        )
        output.append("")

        # Positions Section
        output.append("--- CURRENT POSITIONS ---")
        if state.positions:
            for pos in state.positions:
                symbol = pos.get("symbol", "N/A")
                quantity = pos.get("quantity", 0)
                avg_price = pos.get("avg_price", 0.0)
                current_price = pos.get("current_price", 0.0)
                pnl = pos.get("pnl", 0.0)
                pnl_pct = pos.get("pnl_percent", 0.0)

                output.append(
                    f"  {symbol:12s} | Qty: {quantity:>10} | "
                    f"Avg: ${avg_price:>10.2f} | Current: ${current_price:>10.2f} | "
                    f"P&L: {pnl:>12,.2f} ({pnl_pct:>7.2f}%)"
                )
        else:
            output.append("  No open positions")
        output.append("")

        # Strategy Status Section
        output.append("--- STRATEGY STATUS ---")
        if state.strategy_status:
            for strategy_name, status in state.strategy_status.items():
                status_icon = "✓" if status == "RUNNING" else "✗"
                output.append(f"  {status_icon} {strategy_name:30s} {status}")
        else:
            output.append("  No strategies configured")
        output.append("")

        # Recent Trades Section
        output.append("--- RECENT TRADES ---")
        if state.recent_trades:
            output.append(
                f"  {'Time':<20} {'Symbol':<10} {'Side':<6} {'Qty':<8} "
                f"{'Price':<10} {'Status':<10}"
            )
            output.append("  " + "-" * 80)
            for trade in state.recent_trades:
                trade_time = trade.get("timestamp", "N/A")
                symbol = trade.get("symbol", "N/A")
                side = trade.get("side", "N/A")
                quantity = trade.get("quantity", 0)
                price = trade.get("price", 0.0)
                trade_status = trade.get("status", "N/A")

                output.append(
                    f"  {str(trade_time):<20} {symbol:<10} {side:<6} "
                    f"{quantity:<8} ${price:<9.2f} {trade_status:<10}"
                )
        else:
            output.append("  No recent trades")
        output.append("")

        # System Health Section
        output.append("--- SYSTEM HEALTH ---")
        for check_name, check_result in state.system_health.items():
            status = check_result.get("status", "UNKNOWN")
            status_icon = "✓" if status == "HEALTHY" else "⚠" if status == "WARNING" else "✗"
            message = check_result.get("message", "")
            output.append(f"  {status_icon} {check_name:30s} {status:12s} {message}")
        output.append("")

        output.append("=" * 100)

        return "\n".join(output)

    def get_status_summary(self) -> Dict[str, Any]:
        """Get a summary of current dashboard status.

        Returns:
            Dictionary containing key status metrics.
        """
        if not self.current_state:
            return {"status": "No data available"}

        state = self.current_state
        healthy_strategies = sum(
            1 for s in state.strategy_status.values() if s == "RUNNING"
        )
        total_strategies = len(state.strategy_status)

        all_healthy = all(
            h.get("status") == "HEALTHY"
            for h in state.system_health.values()
        )

        return {
            "timestamp": state.timestamp.isoformat(),
            "daily_pnl": state.daily_pnl,
            "daily_pnl_percent": state.daily_pnl_percent,
            "open_positions": len(state.positions),
            "strategies_running": f"{healthy_strategies}/{total_strategies}",
            "system_health": "HEALTHY" if all_healthy else "DEGRADED",
            "recent_trade_count": len(state.recent_trades),
        }


def generate_html_dashboard(
    positions: List[Dict[str, Any]],
    pnl: float,
    pnl_percent: float,
    trades: List[Dict[str, Any]],
    health: Dict[str, Any],
    strategy_status: Dict[str, str],
    output_path: str = "trading_dashboard.html",
) -> str:
    """Generate a self-contained HTML dashboard file.

    Args:
        positions: List of current positions.
        pnl: Daily profit/loss.
        pnl_percent: Daily profit/loss percentage.
        trades: List of recent trades.
        health: System health metrics.
        strategy_status: Strategy running status.
        output_path: Path to write HTML file to.

    Returns:
        Path to the generated HTML file.
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pnl_color = "#2ecc71" if pnl >= 0 else "#e74c3c"
    pnl_sign = "+" if pnl >= 0 else ""

    # Build positions table
    positions_html = ""
    if positions:
        positions_html = "<tr><th>Symbol</th><th>Qty</th><th>Avg Price</th><th>Current Price</th><th>P&L</th><th>P&L %</th></tr>"
        for pos in positions:
            pos_pnl = pos.get("pnl", 0.0)
            pos_pnl_color = "#2ecc71" if pos_pnl >= 0 else "#e74c3c"
            positions_html += f"""
            <tr>
                <td>{html.escape(str(pos.get('symbol', 'N/A')))}</td>
                <td>{pos.get('quantity', 0)}</td>
                <td>${pos.get('avg_price', 0.0):.2f}</td>
                <td>${pos.get('current_price', 0.0):.2f}</td>
                <td style="color: {pos_pnl_color}">${pos_pnl:,.2f}</td>
                <td>{pos.get('pnl_percent', 0.0):.2f}%</td>
            </tr>
            """
    else:
        positions_html = "<tr><td colspan='6'>No open positions</td></tr>"

    # Build trades table
    trades_html = ""
    if trades:
        trades_html = "<tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Status</th></tr>"
        for trade in trades:
            trades_html += f"""
            <tr>
                <td>{html.escape(str(trade.get('timestamp', 'N/A')))}</td>
                <td>{html.escape(str(trade.get('symbol', 'N/A')))}</td>
                <td>{html.escape(str(trade.get('side', 'N/A')))}</td>
                <td>{trade.get('quantity', 0)}</td>
                <td>${trade.get('price', 0.0):.2f}</td>
                <td>{html.escape(str(trade.get('status', 'N/A')))}</td>
            </tr>
            """
    else:
        trades_html = "<tr><td colspan='6'>No recent trades</td></tr>"

    # Build strategy status
    strategies_html = ""
    for strat_name, strat_status in strategy_status.items():
        status_color = "#2ecc71" if strat_status == "RUNNING" else "#e74c3c"
        strategies_html += f"""
        <div style="display: flex; justify-content: space-between; padding: 8px; border-bottom: 1px solid #ecf0f1;">
            <span>{html.escape(strat_name)}</span>
            <span style="color: {status_color}; font-weight: bold;">{html.escape(strat_status)}</span>
        </div>
        """

    # Build health checks
    health_html = ""
    for check_name, check_result in health.items():
        check_status = check_result.get("status", "UNKNOWN")
        status_color = "#2ecc71" if check_status == "HEALTHY" else "#f39c12" if check_status == "WARNING" else "#e74c3c"
        health_html += f"""
        <div style="display: flex; justify-content: space-between; padding: 8px; border-bottom: 1px solid #ecf0f1;">
            <span>{html.escape(check_name)}</span>
            <span style="color: {status_color}; font-weight: bold;">{html.escape(check_status)}</span>
        </div>
        """

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trading Dashboard</title>
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #1a1a1a;
                color: #ecf0f1;
                padding: 20px;
            }}
            .container {{
                max-width: 1400px;
                margin: 0 auto;
            }}
            header {{
                text-align: center;
                margin-bottom: 30px;
                border-bottom: 2px solid #3498db;
                padding-bottom: 15px;
            }}
            h1 {{
                font-size: 28px;
                margin-bottom: 10px;
            }}
            .timestamp {{
                font-size: 12px;
                color: #95a5a6;
            }}
            .pnl-section {{
                background-color: #262626;
                border-left: 4px solid {pnl_color};
                padding: 20px;
                border-radius: 4px;
                margin-bottom: 20px;
            }}
            .pnl-value {{
                font-size: 36px;
                font-weight: bold;
                color: {pnl_color};
                margin-bottom: 5px;
            }}
            .pnl-label {{
                font-size: 14px;
                color: #95a5a6;
            }}
            .grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
                margin-bottom: 20px;
            }}
            @media (max-width: 1024px) {{
                .grid {{
                    grid-template-columns: 1fr;
                }}
            }}
            .card {{
                background-color: #262626;
                border-radius: 4px;
                overflow: hidden;
            }}
            .card-header {{
                background-color: #1a1a1a;
                padding: 15px;
                border-bottom: 2px solid #3498db;
                font-weight: bold;
                font-size: 16px;
            }}
            .card-body {{
                padding: 15px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                font-size: 13px;
            }}
            th {{
                text-align: left;
                padding: 10px;
                background-color: #1a1a1a;
                border-bottom: 2px solid #3498db;
                font-weight: bold;
            }}
            td {{
                padding: 10px;
                border-bottom: 1px solid #404040;
            }}
            tr:hover {{
                background-color: #2f2f2f;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <header>
                <h1>📊 Trading Dashboard</h1>
                <div class="timestamp">Last updated: {now}</div>
            </header>

            <div class="pnl-section">
                <div class="pnl-value">{pnl_sign}${abs(pnl):,.2f}</div>
                <div class="pnl-label">Daily P&L ({pnl_sign}{abs(pnl_percent):.2f}%)</div>
            </div>

            <div class="grid">
                <div class="card">
                    <div class="card-header">📈 Current Positions</div>
                    <div class="card-body">
                        <table>
                            {positions_html}
                        </table>
                    </div>
                </div>

                <div class="card">
                    <div class="card-header">🔧 System Health</div>
                    <div class="card-body">
                        {health_html}
                    </div>
                </div>
            </div>

            <div class="grid">
                <div class="card">
                    <div class="card-header">💼 Strategy Status</div>
                    <div class="card-body">
                        {strategies_html}
                    </div>
                </div>

                <div class="card">
                    <div class="card-header">📋 Recent Trades</div>
                    <div class="card-body">
                        <table>
                            {trades_html}
                        </table>
                    </div>
                </div>
            </div>
        </div>
    </body>
    </html>
    """

    with open(output_path, "w") as f:
        f.write(html_content)

    return output_path
