"""
Daily Algo Desk Report
======================
Sends a plain-text email summary of all active strategies over the past
24 hours. Run once a day via cron (e.g. 8:00 AM).

Covers:
  - Funding arb monitor (crypto_funding_arb)
  - Pairs trading (BTC/ETH Z-score)

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
import json
import math
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

LOG_FILE        = Path(__file__).parent / "logs" / "funding_arb.log"
PAIRS_STATE_FILE = Path(__file__).parent / "logs" / "pairs_state.json"
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
    # NOTE: These must match the EXACT log format in funding_arb_strategy.py.
    # Bot logs:  [PAPER] OPEN ARB [STANDARD]: ETH on Kraken | ... notional=$500 ... net |yield|=8.5% ann
    # Bot logs:  EXIT signal: ETH_Kraken [STANDARD] | yield=2.1% ...
    # Bot logs:  EXIT signal: ETH_Kraken_reverse [REVERSE] | |yield|=2.1% ...
    re_timestamp = re.compile(r"^(\d{2}:\d{2}:\d{2})")
    re_rate_line = re.compile(
        r"(Kraken|Coinbase|Binance)\s+(\w+):\s+rate=([0-9.\-]+)\s+\(([0-9.\-]+)%\s+ann\).*net=([0-9.\-]+)%.*basis=([0-9.\-]+)%.*spot=\$([0-9,]+)"
    )
    # Matches both: [PAPER] OPEN ARB [STANDARD]: and [PAPER] OPEN ARB [REVERSE]:
    # Captures: symbol, exchange, notional, net_yield (handles both "net yield=" and "net |yield|=")
    re_paper_open = re.compile(
        r"\[PAPER\] OPEN ARB \[(\w+)\]:\s+(\w+)\s+on\s+(\w+)\s+\|.*notional=\$([0-9,]+).*net\s+\|?yield\|?=([0-9.]+)%\s+ann"
    )
    # Matches both standard and reverse exit signals
    # Standard: EXIT signal: ETH_Kraken [STANDARD] | yield=2.1% ...
    # Reverse:  EXIT signal: ETH_Kraken_reverse [REVERSE] | |yield|=2.1% ...
    re_exit = re.compile(
        r"EXIT signal:\s+(\w+?)(?:_reverse)?\s+\[(\w+)\]\s+\|\s+\|?yield\|?=([0-9.]+)%"
    )
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
            exchange, symbol, rate, ann, net, basis, spot = rate_match.groups()
            summary["snapshots"].append({
                "exchange": exchange,
                "symbol": symbol,
                "rate": float(rate),
                "annualized": float(ann),
                "net": float(net),
                "basis": float(basis),
                "spot": spot.replace(",", ""),
            })

        paper_match = re_paper_open.search(line)
        if paper_match:
            # Groups: direction, symbol, exchange, notional, net_yield
            direction, symbol, exchange, notional, net_yield = paper_match.groups()
            # Deduplicate: use symbol_exchange_direction as unique key
            dedup_key = f"{symbol}_{exchange}_{direction}"
            trade_entry = {
                "symbol": symbol,
                "exchange": exchange,
                "direction": direction,
                "net_yield": float(net_yield),
                "notional": notional,
                "_dedup_key": dedup_key,
            }
            # Only add if we haven't seen this exact trade today
            existing_keys = {t.get("_dedup_key") for t in summary["paper_opens"]}
            if dedup_key not in existing_keys:
                summary["paper_opens"].append(trade_entry)

        exit_match = re_exit.search(line)
        if exit_match:
            # Groups: pos_key (e.g. "ETH_Kraken"), direction, yield_val
            pos_key_raw, direction, yield_val = exit_match.groups()
            # pos_key_raw is like "ETH_Kraken" — split on first underscore
            parts = pos_key_raw.split("_", 1)
            symbol = parts[0] if parts else pos_key_raw
            exchange = parts[1] if len(parts) > 1 else "unknown"
            summary["exit_signals"].append({
                "symbol": symbol,
                "exchange": exchange,
                "direction": direction,
                "yield": float(yield_val),
            })

        if re_warning.search(line):
            # Truncate very long error lines
            summary["errors"].append(line[:200])

    return summary


# ─── Pairs state parsing ────────────────────────────────────────────────────

def parse_pairs_state(state_path: Path) -> dict:
    """
    Read the pairs trading state file and compute current Z-score and warmup status.
    Returns a summary dict for inclusion in the daily report.
    """
    result = {
        "ok": False,
        "error": None,
        "window_size": 0,
        "window_target": 2016,
        "z_score": None,
        "spread_mean": None,
        "spread_std": None,
        "is_valid": False,
        "saved_at": None,
        "btc_price": None,
        "eth_price": None,
        "ratio": None,
    }

    if not state_path.exists():
        result["error"] = "State file not found — pairs monitor may not be running"
        return result

    try:
        data = json.loads(state_path.read_text())
    except Exception as e:
        result["error"] = f"Could not read pairs state: {e}"
        return result

    history = data.get("price_history", [])
    result["saved_at"] = data.get("saved_at")
    result["window_size"] = len(history)
    result["ok"] = True

    if not history:
        result["error"] = "No price history in state file yet"
        return result

    # Latest snapshot
    latest = history[-1]
    result["btc_price"] = latest.get("btc_price")
    result["eth_price"] = latest.get("eth_price")
    result["ratio"] = latest.get("ratio")

    # Compute Z-score from rolling window (same logic as pairs_strategy.py)
    # Default window: 2016 (7 days × 288 obs/day). Match strategies.yaml pairs_trading.window
    WINDOW = 2016
    result["window_target"] = WINDOW
    MIN_OBS = max(30, WINDOW // 4)  # At least 30 obs, or 25% of window
    spreads = [h["log_spread"] for h in history[-WINDOW:]]

    if len(spreads) >= MIN_OBS:
        mean = sum(spreads) / len(spreads)
        variance = sum((x - mean) ** 2 for x in spreads) / len(spreads)
        std = math.sqrt(variance)
        current_spread = spreads[-1]
        result["spread_mean"] = mean
        result["spread_std"] = std
        result["is_valid"] = std > 1e-8
        if result["is_valid"]:
            result["z_score"] = (current_spread - mean) / std

    return result


def format_pairs_section(pairs: dict) -> list:
    """Build the pairs trading section for the daily report."""
    lines = []
    lines.append("PAIRS TRADING  (BTC/ETH Z-Score)")
    lines.append("─" * 55)

    if pairs["error"] and not pairs["ok"]:
        lines.append(f"  ⚠️  {pairs['error']}")
        lines.append("")
        return lines

    # Warmup progress
    w = pairs["window_size"]
    target = pairs["window_target"]
    pct = min(100, int(w / target * 100))
    bar_len = 30
    filled = int(bar_len * pct / 100)
    bar = "█" * filled + "░" * (bar_len - filled)
    lines.append(f"  Warmup: [{bar}] {pct}%  ({w}/{target} obs)")

    if pairs["btc_price"]:
        lines.append(
            f"  Prices: BTC=${pairs['btc_price']:,.0f}  "
            f"ETH=${pairs['eth_price']:,.2f}  "
            f"Ratio={pairs['ratio']:.4f}"
        )

    if pairs["is_valid"] and pairs["z_score"] is not None:
        z = pairs["z_score"]
        direction = ""
        if z > 2.0:
            direction = "  ← SIGNAL: SHORT BTC / LONG ETH"
        elif z < -2.0:
            direction = "  ← SIGNAL: LONG BTC / SHORT ETH"
        elif abs(z) > 1.5:
            direction = "  ← approaching threshold"
        lines.append(
            f"  Z-Score: {z:+.3f}  "
            f"(mean={pairs['spread_mean']:.4f}, "
            f"std={pairs['spread_std']:.4f}){direction}"
        )
    else:
        min_obs = target // 4
        remaining = max(0, min_obs - w)
        lines.append(f"  Z-Score: not yet valid — need {remaining} more observations (~{remaining * 5} min)")

    if pairs["saved_at"]:
        try:
            saved = datetime.fromisoformat(pairs["saved_at"])
            age_min = int((datetime.now(timezone.utc) - saved).total_seconds() / 60)
            lines.append(f"  Last update: {age_min} min ago")
        except Exception:
            pass

    if pairs["error"]:
        lines.append(f"  ⚠️  {pairs['error']}")

    lines.append("")
    return lines


# ─── Report formatting ───────────────────────────────────────────────────────

def format_report(summary: dict, pairs: dict, since: datetime) -> str:
    """Build the plain-text email body."""
    now = datetime.now(timezone.utc)
    lines = []

    lines.append("=" * 55)
    lines.append("  ALGO DESK — DAILY REPORT")
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
            dir_tag = f" [{t.get('direction', 'STD')}]" if t.get("direction") else ""
            lines.append(
                f"  ✅  {t['symbol']} / {t['exchange']}{dir_tag}  |  "
                f"{t['net_yield']:.1f}% net annualized  |  "
                f"${t['notional']}/leg"
            )
        lines.append("")
    else:
        lines.append("PAPER TRADES OPENED TODAY")
        lines.append("  None — rates below threshold all day.")
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
        lines.append(f"  {'Asset':<8} {'Exchange':<12} {'Rate/8h':>9} {'Ann':>8} {'Net':>8} {'Basis':>7} {'Spot':>12}")
        lines.append("  " + "-" * 61)
        for s in sorted_snaps:
            basis = s.get("basis", 0.0)
            basis_flag = "  ← basis too wide" if abs(basis) >= 0.5 else ""
            marker = "  ← above threshold" if s["net"] >= 8.0 else ""
            annotation = basis_flag if basis_flag else marker
            lines.append(
                f"  {s['symbol']:<8} {s['exchange']:<12} "
                f"{s['rate']:>8.4f}%  "
                f"{s['annualized']:>6.1f}%  "
                f"{s['net']:>6.1f}%  "
                f"{basis:>6.3f}%  "
                f"${float(s['spot']):>9,.0f}"
                f"{annotation}"
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

    # ── Pairs trading section ──
    lines.extend(format_pairs_section(pairs))

    # ── Footer ──
    lines.append("─" * 55)
    lines.append("  Logs: logs/funding_arb.log | logs/pairs_trading.log")
    lines.append("  Status: ./start_funding_arb.sh status")
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
    pairs   = parse_pairs_state(PAIRS_STATE_FILE)

    # Build a compact subject line
    pairs_status = (
        f"Z={pairs['z_score']:+.2f}" if pairs["is_valid"] and pairs["z_score"] is not None
        else f"warming {pairs['window_size']}/{pairs['window_target']}"
    )
    subject = (
        f"[Algo Desk] {datetime.now().strftime('%b %d')} | "
        f"Arb: {summary['scans']} scans / {len(summary['paper_opens'])} trades | "
        f"Pairs: {pairs_status}"
    )
    body = format_report(summary, pairs, since)

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
