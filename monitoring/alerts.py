"""
Alert system for trading desk monitoring.

Manages alert generation, rate limiting, and delivery via email and other
channels for critical trading events.
"""

import smtplib
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional
from collections import deque
import logging


logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert severity levels."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class Alert:
    """Represents a single alert event."""

    level: AlertLevel
    message: str
    timestamp: datetime
    context: Dict[str, Any]


class AlertManager:
    """Manages alert generation, rate limiting, and delivery.

    Supports email alerts with configurable rate limiting to prevent
    alert fatigue. Maintains alert history and allows custom alert handlers.
    """

    def __init__(
        self,
        smtp_host: str = "localhost",
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        from_email: str = "trading-alerts@example.com",
        to_emails: List[str] | None = None,
        max_alerts_per_hour: int = 10,
        enable_email: bool = True,
    ) -> None:
        """Initialize alert manager.

        Args:
            smtp_host: SMTP server hostname.
            smtp_port: SMTP server port (usually 587 for TLS).
            smtp_user: SMTP authentication username.
            smtp_password: SMTP authentication password.
            from_email: Email address to send alerts from.
            to_emails: List of email addresses to send alerts to.
            max_alerts_per_hour: Maximum alerts to send per hour (rate limiting).
            enable_email: Whether to enable email alerts.
        """
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_email = from_email
        self.to_emails = to_emails or []
        self.max_alerts_per_hour = max_alerts_per_hour
        self.enable_email = enable_email

        # Alert history and rate limiting
        self.alert_history: List[Alert] = []
        self.sent_timestamps: Deque[datetime] = deque(maxlen=max_alerts_per_hour)
        self.custom_handlers: List[Callable[[Alert], None]] = []

    def add_custom_handler(self, handler: Callable[[Alert], None]) -> None:
        """Register a custom alert handler function.

        Args:
            handler: Function that accepts an Alert object.
        """
        self.custom_handlers.append(handler)

    def _is_rate_limited(self) -> bool:
        """Check if alert sending is rate limited.

        Returns:
            True if rate limit exceeded, False otherwise.
        """
        if len(self.sent_timestamps) < self.max_alerts_per_hour:
            return False

        oldest_timestamp = self.sent_timestamps[0]
        time_since_oldest = datetime.now() - oldest_timestamp
        return time_since_oldest < timedelta(hours=1)

    def send_alert(
        self,
        level: AlertLevel,
        message: str,
        context: Dict[str, Any] | None = None,
        send_email: bool = True,
    ) -> bool:
        """Send an alert at the specified level.

        Args:
            level: Alert severity level.
            message: Alert message text.
            context: Additional context information.
            send_email: Whether to send email for this alert.

        Returns:
            True if alert was sent, False if rate limited.
        """
        context = context or {}
        alert = Alert(
            level=level,
            message=message,
            timestamp=datetime.now(),
            context=context,
        )

        # Store in history
        self.alert_history.append(alert)

        # Log the alert
        log_func = {
            AlertLevel.INFO: logger.info,
            AlertLevel.WARNING: logger.warning,
            AlertLevel.CRITICAL: logger.critical,
        }.get(level, logger.info)

        log_func(f"[{level.value}] {message}")

        # Call custom handlers
        for handler in self.custom_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"Error in custom alert handler: {e}")

        # Check rate limiting for CRITICAL alerts only; WARNING/INFO can be skipped
        if level != AlertLevel.CRITICAL and self._is_rate_limited():
            logger.debug("Alert rate limited, not sending email")
            return False

        # Send email if enabled
        if send_email and self.enable_email:
            success = self.send_email_alert(
                subject=f"[{level.value}] Trading Alert",
                body=self._format_email_body(alert),
            )
            if success:
                self.sent_timestamps.append(datetime.now())
                return True

        return True

    def send_email_alert(self, subject: str, body: str) -> bool:
        """Send email alert.

        Args:
            subject: Email subject line.
            body: Email body content.

        Returns:
            True if email was sent successfully, False otherwise.
        """
        if not self.enable_email or not self.to_emails:
            logger.debug("Email alerts disabled or no recipients configured")
            return False

        try:
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                if self.smtp_user and self.smtp_password:
                    server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            logger.info(f"Alert email sent: {subject}")
            return True

        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")
            return False

    def check_and_alert(
        self,
        condition: bool,
        level: AlertLevel,
        message: str,
        context: Dict[str, Any] | None = None,
        send_email: bool = True,
    ) -> bool:
        """Conditionally send an alert.

        Args:
            condition: If True, send alert.
            level: Alert severity level.
            message: Alert message.
            context: Additional context.
            send_email: Whether to send email.

        Returns:
            True if alert was sent, False otherwise.
        """
        if condition:
            return self.send_alert(level, message, context, send_email)
        return False

    def alert_kill_switch_triggered(self, reason: str, context: Dict[str, Any] | None = None) -> bool:
        """Alert when kill switch is triggered.

        Args:
            reason: Reason for kill switch trigger.
            context: Additional context.

        Returns:
            True if alert was sent.
        """
        msg = f"KILL SWITCH TRIGGERED: {reason}"
        return self.send_alert(
            AlertLevel.CRITICAL,
            msg,
            context or {},
            send_email=True,
        )

    def alert_connection_lost(self, service: str, context: Dict[str, Any] | None = None) -> bool:
        """Alert when connection to a service is lost.

        Args:
            service: Name of service (e.g., 'IBKR', 'Kalshi API').
            context: Additional context.

        Returns:
            True if alert was sent.
        """
        msg = f"Connection lost to {service}"
        return self.send_alert(
            AlertLevel.CRITICAL,
            msg,
            context or {},
            send_email=True,
        )

    def alert_strategy_error(self, strategy: str, error: str, context: Dict[str, Any] | None = None) -> bool:
        """Alert when strategy encounters an error.

        Args:
            strategy: Strategy name.
            error: Error description.
            context: Additional context.

        Returns:
            True if alert was sent.
        """
        msg = f"Strategy error in {strategy}: {error}"
        return self.send_alert(
            AlertLevel.WARNING,
            msg,
            context or {},
            send_email=True,
        )

    def alert_pnl_threshold(self, current_pnl: float, threshold: float, context: Dict[str, Any] | None = None) -> bool:
        """Alert when daily P&L crosses a threshold.

        Args:
            current_pnl: Current daily P&L.
            threshold: P&L threshold.
            context: Additional context.

        Returns:
            True if alert was sent.
        """
        level = AlertLevel.WARNING if current_pnl > 0 else AlertLevel.CRITICAL
        msg = f"Daily P&L threshold breached: ${current_pnl:,.2f} (threshold: ${threshold:,.2f})"
        return self.send_alert(
            level,
            msg,
            context or {"current_pnl": current_pnl, "threshold": threshold},
            send_email=True,
        )

    def _format_email_body(self, alert: Alert) -> str:
        """Format alert as email body.

        Args:
            alert: Alert object.

        Returns:
            Formatted email body text.
        """
        lines = [
            f"Alert Level: {alert.level.value}",
            f"Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Message: {alert.message}",
            "",
        ]

        if alert.context:
            lines.append("Context:")
            for key, value in alert.context.items():
                lines.append(f"  {key}: {value}")

        return "\n".join(lines)

    def get_alert_history(
        self,
        level: Optional[AlertLevel] = None,
        max_results: int = 100,
    ) -> List[Alert]:
        """Get alert history with optional filtering.

        Args:
            level: Filter by alert level, or None for all.
            max_results: Maximum number of results to return.

        Returns:
            List of alerts, most recent first.
        """
        filtered = self.alert_history
        if level:
            filtered = [a for a in filtered if a.level == level]

        return sorted(
            filtered,
            key=lambda a: a.timestamp,
            reverse=True,
        )[:max_results]

    def get_alert_summary(self) -> Dict[str, Any]:
        """Get summary statistics of alerts.

        Returns:
            Dictionary with alert counts by level and recent alerts.
        """
        by_level = {level: 0 for level in AlertLevel}
        for alert in self.alert_history:
            by_level[alert.level] += 1

        return {
            "total_alerts": len(self.alert_history),
            "by_level": {level.value: count for level, count in by_level.items()},
            "rate_limited": self._is_rate_limited(),
            "last_alert": (
                self.alert_history[-1].timestamp.isoformat()
                if self.alert_history
                else None
            ),
        }
