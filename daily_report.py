"""
Daily Funding Arb Report
========================
Sends a plain-text email summary of the funding arb monitor's activity
over the past 24 hours. Run once a day via cron (e.g. 8:00 AM).

Setup (one time):
  1. Set REPORT_TO and REPORT_FROM in .env (see bottom of this file)
  2. Uses Gmail "App Password" — regular Gmail password won't work.
     Go to: myaccount.google.com → Security → 2-Step Verification → App passwords
     Create one for "Mail" → copy the 16-char password into .env as GMAIL_APP_PASSWORD

.env entries needed:
  REPORT_TO=mattbrunetto215@gmail.com
  REPORT_FROM=mattbrunetto215@gmail.com   (or a dedicated sender address)
  GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx

Usage:
  python daily_report.py              # send report now
  python daily_report.py --preview    # print to terminal instead of sending
"""

import argparse
import os
import re
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ─── Config ─────────────────────────────────────────────────────────────────

LOG_FILE = Path(__file__).parent / "logs" / "funding_arb.log"
REPORT_TO = os.getenv("REPORT_TO", "")
REPORT_FROM = os.getenv("REPORT_FROM", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")

LOOKBACK_HOURS = 24  # How far back to scan the log


# ─── Log parsing ────────────────────────────────────────────────────────────

def parse_log(log_path: Path, since: datetime) -> dict:
    """
    Parse the funding arb log file for the past N hours.
    Returns a structured summary dict.
    """
    summary = {
        "scans": 0,
        "opportunities": [],         # List of (symbol, exchange, yield_pct)
        "paper_opens": [],           # List of (symbol, exchange, yield_pct, notional)
        "exit_signals": [],          # List of (symbol, exchange, reason)
        "errors": [],                # Warning/error lines
        "snapshots": [],             # All rate lines for the rate table
        "log_lines": 0,
        "log_start": None,
        "log_end": None,
    }

    if not log_path.exists():
        summary["errors"].append(f"Log file not found: {log_path}")
        return summary

    # Regex patterns
    re_timestamp = re.compile(r"^(\d{2}:\d{2}:\d{2})")
    re_rate_line = re.compile(
        r"(Kraken|Coinbase)\s+(\w+):\s+rate=([0-9.\-]+)\s+\(([0-9.\-]+)%\s+ann\).*net=([0-9.\-]+)%.*spot=\$([0-9,]+)"
    )
    re_paper_open = re.compile(
        r"\[PAPER\] OPEN ARB:\s+(\w+)\s+on\s+(\w+).*net yield=([0-9.]+)%\s+ann.*notional=\$([0-9,]+)"
    )
    re_exit = re.compile(r"EXIT signal:\s+(\w+)_(\w+)\s+\|.*yield=([0-9.]+)%")
    re_scan_start = re.compile(r"Scanning funding rates")
    re_warning = re.compile(r"\[(WARNING|ERROR)\]")

    today = since.date()

    try:
        with open(log_path, "r") as f:
            lines = f.readlines()
    except Exception as e:
        summary["errors"].append(f"Could not read log: {e}")
        return summary

    summary["log_lines"] = len(lines)

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Try to extract timestamp — log format: "HH:MM:SS [INFO] ..."
        # Since log doesn't store dates (rotates daily), we treat all lines as today
        ts_match = re_timestamp.match(line)

        if re_scan_start.search(line):
            summary["scans"] += 1

        rate_match = re_rate_line.search(line)
        if rate_match:
            exchange, symbol, rate, ann, net, spot = rate_match.groups()
            summary["snapshots"].append({
                "exchange": exchange,
                "symbol": symbol,
                "rate": float(rate),
                "annualized": float(ann),
                "net": float(net),
                "spot": spot.replace(",", ""),
            })

        paper_match = re_paper_open.search(line)
        if paper_match:
            symbol, exchange, net_yield, notional = paper_match.groups()
            summary["paper_opens"].append({
                "symbol": symbol,
                "exchange": exchange,
                "net_yield": float(net_yield),
                "notional": notional,
            })

        exit_match = re_exit.search(line)
        if exit_match:
            symbol, exchange, yield_val = exit_match.groups()
            summary["exit_signals"].append({
                "symbol": symbol,
                "exchange": exchange,
                "yield": float(yield_val),
            })

        if re_warning.search(line):
            # Truncate very long error lines
            summary["errors"].append(line[:200])

    return summary


# ─── Report formatting ───────────────────────────────────────────────────────

def format_report(summary: dict, since: datetime) -> str:
    """Build the plain-text email body."""
    now = datetime.now(timezone.utc)
    lines = []

    lines.append("=" * 55)
    lines.append("  ALGO DESK — FUNDING ARB DAILY REPORT")
    lines.append(f"  {now.strftime('%A, %B %d %Y')} | Past 24 hours")
    lines.append("=" * 55)
    lines.append("")

    # ── Activity summary ──
    lines.append("ACTIVITY")
    lines.append(f"  Scans completed:     {summary['scans']}")
    lines.append(f"  Paper trades opened: {len(summary['paper_opens'])}")
    lines.append(f"  Exit signals fired:  {len(summary['exit_signals'])}")
    lines.append(f"  Errors/warnings:     {len(summary['errors'])}")
    lines.append("")

    # ── Paper trades opened ──
    if summary["paper_opens"]:
        lines.append("PAPER TRADES OPENED TODAY")
        for t in summary["paper_opens"]:
            lines.append(
                f"  ✅  {t['symbol']} / {t['exchange']}  |  "
                f"{t['net_yield']:.1f}% net annualized  |  "
                f"${t['notional']}/leg"
            )
        lines.append("")
    else:
        lines.append("PAPER TRADES OPENED TODAY")
        lines.append("  None — rates below 8% threshold all day.")
        lines.append("")

    # ── Exit signals ──
    if summary["exit_signals"]:
        lines.append("EXIT SIGNALS")
        for e in summary["exit_signals"]:
            lines.append(
                f"  ⚠️   {e['symbol']} / {e['exchange']}  |  "
                f"yield dropped to {e['yield']:.1f}%"
            )
        lines.append("")

    # ── Best rates seen today ──
    if summary["snapshots"]:
        # Deduplicate — take highest net yield per symbol/exchange pair
        best = {}
        for s in summary["snapshots"]:
            key = f"{s['symbol']}_{s['exchange']}"
            if key not in best or s["net"] > best[key]["net"]:
                best[key] = s

        sorted_snaps = sorted(best.values(), key=lambda x: x["net"], reverse=True)
        lines.append("BEST RATES SEEN TODAY (peak per asset)")
        lines.append(f"  {'Asset':<8} {'Exchange':<12} {'Rate/8h':>9} {'Ann':>8} {'Net':>8} {'Spot':>12}")
        lines.append("  " + "-" * 53)
        for s in sorted_snaps:
            marker = "  ← above threshold" if s["net"] >= 8.0 else ""
            lines.append(
                f"  {s['symbol']:<8} {s['exchange']:<12} "
                f"{s['rate']:>8.4f}%  "
                f"{s['annualized']:>6.1f}%  "
                f"{s['net']:>6.1f}%  "
                f"${float(s['spot']):>9,.0f}"
                f"{marker}"
            )
        lines.append("")
    else:
        lines.append("RATES")
        lines.append("  No rate data in log — monitor may not have run yet.")
        lines.append("")

    # ── Errors ──
    if summary["errors"]:
        lines.append("WARNINGS / ERRORS")
        for e in summary["errors"][:10]:  # Cap at 10 to keep email short
            lines.append(f"  ⚠️  {e}")
        if len(summary["errors"]) > 10:
            lines.append(f"  ... and {len(summary['errors']) - 10} more. Check logs/funding_arb.log")
        lines.append("")

    # ── Footer ──
    lines.append("─" * 55)
    lines.append("  Log: algo-desk/logs/funding_arb.log")
    lines.append("  Check status: ./start_funding_arb.sh status")
    lines.append("─" * 55)

    return "\n".join(lines)


# ─── Email sending ───────────────────────────────────────────────────────────

def send_email(subject: str, body: str) -> None:
    """Send via Gmail SMTP using app password."""
    if not REPORT_TO:
        raise ValueError("REPORT_TO not set in .env")
    if not REPORT_FROM:
        raise ValueError("REPORT_FROM not set in .env")
    if not GMAIL_APP_PASSWORD:
        raise ValueError(
            "GMAIL_APP_PASSWORD not set in .env\n"
            "Get one at: myaccount.google.com → Security → App passwords"
        )

    msg = MIMEText(body, "plain")
    msg["Subject"] = subject
    msg["From"] = REPORT_FROM
    msg["To"] = REPORT_TO

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(REPORT_FROM, GMAIL_APP_PASSWORD)
        server.sendmail(REPORT_FROM, [REPORT_TO], msg.as_string())


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Send daily funding arb report")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Print report to terminal instead of sending email",
    )
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    summary = parse_log(LOG_FILE, since)

    subject = (
        f"[Algo Desk] Funding Arb Report — "
        f"{datetime.now().strftime('%b %d')} | "
        f"{summary['scans']} scans | "
        f"{len(summary['paper_opens'])} trades"
    )
    body = format_report(summary, since)

    if args.preview:
        print(f"\nSubject: {subject}\n")
        print(body)
        return

    try:
        send_email(subject, body)
        print(f"✅ Report sent to {REPORT_TO}")
    except Exception as e:
        print(f"❌ Failed to send: {e}")
        print("\nPrinting report to terminal instead:\n")
        print(body)
        sys.exit(1)


if __name__ == "__main__":
    main()


# ─── .env setup reminder ─────────────────────────────────────────────────────
#
# Add these three lines to your .env file in algo-desk/:
#
#   REPORT_TO=mattbrunetto215@gmail.com
#   REPORT_FROM=mattbrunetto215@gmail.com
#   GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
#
# Get the app password:
#   1. Go to myaccount.google.com
#   2. Security → 2-Step Verification (must be enabled)
#   3. Scroll down → App passwords
#   4. Select app: Mail, device: Mac → Generate
#   5. Copy the 16-character password (spaces are fine, just paste it)
#
