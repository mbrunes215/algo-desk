"""
System health monitoring for trading infrastructure.

Monitors connectivity to external services and system resources to ensure
the trading system is operating normally.
"""

import logging
import os
import shutil
import psutil
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status levels."""

    HEALTHY = "HEALTHY"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class HealthCheckResult:
    """Result of a single health check."""

    name: str
    status: HealthStatus
    message: str
    timestamp: datetime
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthReport:
    """Complete health report for all monitored systems."""

    timestamp: datetime
    checks: Dict[str, HealthCheckResult]

    @property
    def overall_status(self) -> HealthStatus:
        """Get overall health status across all checks."""
        if any(c.status == HealthStatus.CRITICAL for c in self.checks.values()):
            return HealthStatus.CRITICAL
        if any(c.status == HealthStatus.WARNING for c in self.checks.values()):
            return HealthStatus.WARNING
        return HealthStatus.HEALTHY

    @property
    def is_healthy(self) -> bool:
        """Check if all critical components are healthy."""
        return all(
            c.status != HealthStatus.CRITICAL
            for c in self.checks.values()
        )


class HealthChecker:
    """Monitors health of trading system components.

    Checks connectivity to IBKR and Kalshi APIs, database accessibility,
    strategy heartbeats, and system resources (disk space, memory).
    """

    def __init__(
        self,
        ibkr_host: str = "127.0.0.1",
        ibkr_port: int = 7497,
        kalshi_api_url: str = "https://api.elections.kalshi.com/trade-api/v2",
        database_url: str = "sqlite:///trading.db",
        min_disk_space_gb: float = 1.0,
        max_memory_percent: float = 80.0,
        paper_mode: bool = False,
    ) -> None:
        """Initialize health checker.

        Args:
            ibkr_host: IBKR connection host.
            ibkr_port: IBKR connection port.
            kalshi_api_url: Kalshi API base URL.
            database_url: Database connection URL.
            min_disk_space_gb: Minimum required free disk space in GB.
            max_memory_percent: Maximum acceptable memory usage percentage.
        """
        self.ibkr_host = ibkr_host
        self.ibkr_port = ibkr_port
        self.kalshi_api_url = kalshi_api_url
        self.database_url = database_url
        self.min_disk_space_gb = min_disk_space_gb
        self.max_memory_percent = max_memory_percent
        # In paper mode, IBKR not being connected is expected — downgrade to WARNING
        self.paper_mode = paper_mode

        # Track strategy heartbeats
        self.strategy_heartbeats: Dict[str, datetime] = {}
        # Timeout must exceed the longest signal interval (300s) plus buffer
        self.heartbeat_timeout_seconds: int = 600

    def run_all_checks(self) -> HealthReport:
        """Run all health checks.

        Returns:
            Complete health report with all check results.
        """
        results: Dict[str, HealthCheckResult] = {}

        results["ibkr_connection"] = self.check_ibkr()
        results["kalshi_api"] = self.check_kalshi()
        results["database"] = self.check_database()
        results["disk_space"] = self.check_disk_space()
        results["memory_usage"] = self.check_memory()
        results["strategy_heartbeat"] = self.check_strategy_heartbeats()

        return HealthReport(
            timestamp=datetime.now(),
            checks=results,
        )

    def check_ibkr(self) -> HealthCheckResult:
        """Check IBKR connection status.

        Returns:
            HealthCheckResult for IBKR connection.
        """
        try:
            import socket

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((self.ibkr_host, self.ibkr_port))
            sock.close()

            if result == 0:
                return HealthCheckResult(
                    name="IBKR Connection",
                    status=HealthStatus.HEALTHY,
                    message=f"Connected to {self.ibkr_host}:{self.ibkr_port}",
                    timestamp=datetime.now(),
                    details={
                        "host": self.ibkr_host,
                        "port": self.ibkr_port,
                    },
                )
            else:
                # In paper mode IBKR not running is expected — downgrade to WARNING
                ibkr_status = HealthStatus.WARNING if self.paper_mode else HealthStatus.CRITICAL
                suffix = " (paper mode — IBKR optional)" if self.paper_mode else ""
                return HealthCheckResult(
                    name="IBKR Connection",
                    status=ibkr_status,
                    message=f"Cannot connect to {self.ibkr_host}:{self.ibkr_port}{suffix}",
                    timestamp=datetime.now(),
                    details={
                        "host": self.ibkr_host,
                        "port": self.ibkr_port,
                    },
                )

        except Exception as e:
            logger.error(f"IBKR health check failed: {e}")
            ibkr_status = HealthStatus.WARNING if self.paper_mode else HealthStatus.CRITICAL
            return HealthCheckResult(
                name="IBKR Connection",
                status=ibkr_status,
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_kalshi(self) -> HealthCheckResult:
        """Check Kalshi API connectivity.

        Returns:
            HealthCheckResult for Kalshi API.
        """
        try:
            import urllib.request
            import urllib.error

            req = urllib.request.Request(
                f"{self.kalshi_api_url}/markets?limit=1",
                method="GET",
            )
            req.add_header("User-Agent", "TradingDeskHealthCheck/1.0")

            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    if response.status == 200:
                        return HealthCheckResult(
                            name="Kalshi API",
                            status=HealthStatus.HEALTHY,
                            message=f"API responding at {self.kalshi_api_url}",
                            timestamp=datetime.now(),
                            details={"api_url": self.kalshi_api_url},
                        )
            except urllib.error.HTTPError as e:
                # API might not have /health endpoint, treat as warning
                if e.code < 500:
                    return HealthCheckResult(
                        name="Kalshi API",
                        status=HealthStatus.WARNING,
                        message=f"API returned {e.code}",
                        timestamp=datetime.now(),
                        details={
                            "api_url": self.kalshi_api_url,
                            "status_code": e.code,
                        },
                    )
                else:
                    return HealthCheckResult(
                        name="Kalshi API",
                        status=HealthStatus.CRITICAL,
                        message=f"API server error: {e.code}",
                        timestamp=datetime.now(),
                        details={
                            "api_url": self.kalshi_api_url,
                            "status_code": e.code,
                        },
                    )
            except urllib.error.URLError as e:
                return HealthCheckResult(
                    name="Kalshi API",
                    status=HealthStatus.CRITICAL,
                    message=f"Cannot reach API: {str(e)}",
                    timestamp=datetime.now(),
                    details={"api_url": self.kalshi_api_url, "error": str(e)},
                )

        except Exception as e:
            logger.error(f"Kalshi health check failed: {e}")
            return HealthCheckResult(
                name="Kalshi API",
                status=HealthStatus.CRITICAL,
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_database(self) -> HealthCheckResult:
        """Check database accessibility.

        Returns:
            HealthCheckResult for database.
        """
        try:
            # For SQLite, check if the database file exists and is accessible
            if self.database_url.startswith("sqlite:///"):
                db_path = self.database_url.replace("sqlite:///", "")
                if os.path.exists(db_path):
                    if os.access(db_path, os.R_OK | os.W_OK):
                        return HealthCheckResult(
                            name="Database",
                            status=HealthStatus.HEALTHY,
                            message=f"Database accessible at {db_path}",
                            timestamp=datetime.now(),
                            details={"path": db_path},
                        )
                    else:
                        return HealthCheckResult(
                            name="Database",
                            status=HealthStatus.CRITICAL,
                            message=f"Database file not readable/writable: {db_path}",
                            timestamp=datetime.now(),
                            details={"path": db_path},
                        )
                else:
                    return HealthCheckResult(
                        name="Database",
                        status=HealthStatus.WARNING,
                        message=f"Database file does not exist: {db_path}",
                        timestamp=datetime.now(),
                        details={"path": db_path},
                    )
            else:
                # For other databases, assume healthy (would need actual connection test)
                return HealthCheckResult(
                    name="Database",
                    status=HealthStatus.HEALTHY,
                    message=f"Database URL configured: {self.database_url}",
                    timestamp=datetime.now(),
                    details={"url": self.database_url},
                )

        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return HealthCheckResult(
                name="Database",
                status=HealthStatus.CRITICAL,
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_disk_space(self) -> HealthCheckResult:
        """Check available disk space.

        Returns:
            HealthCheckResult for disk space.
        """
        try:
            usage = shutil.disk_usage("/")
            available_gb = usage.free / (1024**3)

            if available_gb >= self.min_disk_space_gb:
                status = HealthStatus.HEALTHY
                message = f"Disk space OK: {available_gb:.2f} GB available"
            else:
                status = HealthStatus.CRITICAL
                message = (
                    f"Low disk space: {available_gb:.2f} GB available "
                    f"(minimum: {self.min_disk_space_gb} GB)"
                )

            return HealthCheckResult(
                name="Disk Space",
                status=status,
                message=message,
                timestamp=datetime.now(),
                details={
                    "available_gb": available_gb,
                    "required_gb": self.min_disk_space_gb,
                    "total_gb": usage.total / (1024**3),
                    "used_gb": usage.used / (1024**3),
                },
            )

        except Exception as e:
            logger.error(f"Disk space health check failed: {e}")
            return HealthCheckResult(
                name="Disk Space",
                status=HealthStatus.WARNING,
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def check_memory(self) -> HealthCheckResult:
        """Check system memory usage.

        Returns:
            HealthCheckResult for memory.
        """
        try:
            memory = psutil.virtual_memory()
            percent_used = memory.percent

            if percent_used <= self.max_memory_percent:
                status = HealthStatus.HEALTHY
                message = f"Memory usage OK: {percent_used:.1f}%"
            else:
                status = HealthStatus.WARNING
                message = (
                    f"High memory usage: {percent_used:.1f}% "
                    f"(threshold: {self.max_memory_percent}%)"
                )

            return HealthCheckResult(
                name="Memory Usage",
                status=status,
                message=message,
                timestamp=datetime.now(),
                details={
                    "percent_used": percent_used,
                    "available_mb": memory.available / (1024**2),
                    "total_mb": memory.total / (1024**2),
                },
            )

        except Exception as e:
            logger.error(f"Memory health check failed: {e}")
            return HealthCheckResult(
                name="Memory Usage",
                status=HealthStatus.WARNING,
                message=f"Health check error: {str(e)}",
                timestamp=datetime.now(),
                details={"error": str(e)},
            )

    def record_heartbeat(self, strategy_name: str) -> None:
        """Record a strategy heartbeat.

        Args:
            strategy_name: Name of the strategy.
        """
        self.strategy_heartbeats[strategy_name] = datetime.now()

    def check_strategy_heartbeats(self) -> HealthCheckResult:
        """Check strategy heartbeats.

        Returns:
            HealthCheckResult for strategy heartbeats.
        """
        if not self.strategy_heartbeats:
            return HealthCheckResult(
                name="Strategy Heartbeats",
                status=HealthStatus.HEALTHY,
                message="No strategies registered",
                timestamp=datetime.now(),
                details={},
            )

        now = datetime.now()
        timeout = timedelta(seconds=self.heartbeat_timeout_seconds)
        inactive_strategies = []

        for strategy_name, last_heartbeat in self.strategy_heartbeats.items():
            if now - last_heartbeat > timeout:
                inactive_strategies.append(strategy_name)

        if not inactive_strategies:
            return HealthCheckResult(
                name="Strategy Heartbeats",
                status=HealthStatus.HEALTHY,
                message=f"All {len(self.strategy_heartbeats)} strategies active",
                timestamp=datetime.now(),
                details={
                    "active_strategies": len(self.strategy_heartbeats),
                    "timeout_seconds": self.heartbeat_timeout_seconds,
                },
            )
        else:
            status = (
                HealthStatus.WARNING
                if len(inactive_strategies) < len(self.strategy_heartbeats)
                else HealthStatus.CRITICAL
            )
            return HealthCheckResult(
                name="Strategy Heartbeats",
                status=status,
                message=f"Inactive strategies: {', '.join(inactive_strategies)}",
                timestamp=datetime.now(),
                details={
                    "active_strategies": len(self.strategy_heartbeats) - len(inactive_strategies),
                    "inactive_strategies": inactive_strategies,
                    "timeout_seconds": self.heartbeat_timeout_seconds,
                },
            )


from datetime import timedelta
