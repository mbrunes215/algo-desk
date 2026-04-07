#!/bin/bash
# install_cron.sh — Install all algo desk cron jobs
#
# Usage:
#   cd ~/Desktop/algo-desk
#   chmod +x install_cron.sh
#   ./install_cron.sh
#
# This installs five cron jobs:
#   • 7:30 AM daily  — daily_scan.py morning (weather + all-market overview)
#   • 5:00 PM daily  — daily_scan.py evening (next-day contracts)
#   • 6:00 AM daily  — econ_scan.py morning (macro/econ edge signals, pre-market)
#   • 12:00 PM daily — econ_scan.py midday (refresh before US afternoon data)
#   • 8:00 AM daily  — daily_report.py (funding arb summary email)
#
# NOTE: The funding arb monitor (start_funding_arb.sh) runs as a persistent
# background process, not a cron job. Start it manually after deploying:
#   ./start_funding_arb.sh start

set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$(which python3)"
CRON_LOG="$REPO_DIR/logs/scan_cron.log"
ECON_LOG="$REPO_DIR/logs/econ_cron.log"
REPORT_LOG="$REPO_DIR/logs/daily_report.log"

echo "Repo:      $REPO_DIR"
echo "Python:    $PYTHON"
echo "Scan log:  $CRON_LOG"
echo "Econ log:  $ECON_LOG"
echo ""

# Validate scripts exist
if [ ! -f "$REPO_DIR/daily_scan.py" ]; then
    echo "ERROR: daily_scan.py not found at $REPO_DIR"
    exit 1
fi
if [ ! -f "$REPO_DIR/econ_scan.py" ]; then
    echo "ERROR: econ_scan.py not found at $REPO_DIR"
    exit 1
fi

# Validate .env has an API key
if ! grep -q "^KALSHI_API_KEY=" "$REPO_DIR/.env" 2>/dev/null; then
    echo "ERROR: KALSHI_API_KEY not found in .env"
    exit 1
fi
echo "✅ .env has KALSHI_API_KEY"

# Build cron lines — cd into repo so relative paths work
# Wrap path in quotes so spaces in the dir name are safe
MORNING_CRON="30 7 * * * bash -c 'cd \"$REPO_DIR\" && $PYTHON daily_scan.py >> \"$CRON_LOG\" 2>&1'"
EVENING_CRON="0 17 * * * bash -c 'cd \"$REPO_DIR\" && $PYTHON daily_scan.py >> \"$CRON_LOG\" 2>&1'"
ECON_MORNING_CRON="0 6 * * * bash -c 'cd \"$REPO_DIR\" && $PYTHON econ_scan.py >> \"$ECON_LOG\" 2>&1'"
ECON_MIDDAY_CRON="0 12 * * * bash -c 'cd \"$REPO_DIR\" && $PYTHON econ_scan.py >> \"$ECON_LOG\" 2>&1'"
REPORT_CRON="0 8 * * * bash -c 'cd \"$REPO_DIR\" && $PYTHON daily_report.py >> \"$REPORT_LOG\" 2>&1'"

# Read existing crontab (ignore error if empty)
EXISTING=$(crontab -l 2>/dev/null || true)

# Check if already installed — ask to replace if so
if echo "$EXISTING" | grep -qE "daily_scan.py|econ_scan.py|daily_report.py"; then
    echo ""
    echo "⚠️  Existing scan cron entries found:"
    echo "$EXISTING" | grep -E "daily_scan.py|econ_scan.py|daily_report.py" || true
    echo ""
    read -p "Replace existing entries? (y/N) " confirm
    if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
        echo "Aborted."
        exit 0
    fi
    # Remove old entries
    EXISTING=$(echo "$EXISTING" | grep -vE "daily_scan.py|econ_scan.py|daily_report.py")
fi

# Install all five cron entries
NEW_CRONTAB=$(printf "%s\n%s\n%s\n%s\n%s\n%s\n" \
    "$EXISTING" \
    "$MORNING_CRON" \
    "$EVENING_CRON" \
    "$ECON_MORNING_CRON" \
    "$ECON_MIDDAY_CRON" \
    "$REPORT_CRON")
echo "$NEW_CRONTAB" | crontab -

echo ""
echo "✅ Cron jobs installed:"
echo "   daily_scan.py   — 7:30 AM + 5:00 PM daily  → $CRON_LOG"
echo "   econ_scan.py    — 6:00 AM + 12:00 PM daily → $ECON_LOG"
echo "   daily_report.py — 8:00 AM daily             → $REPORT_LOG (emails you)"
echo ""
echo "Verify with:  crontab -l | grep -E 'daily_scan|econ_scan|daily_report'"
echo ""

# Check if the funding arb monitor is running — it's a persistent process, not a cron job
if pgrep -f "start_funding_arb.sh" > /dev/null 2>&1 || pgrep -f "funding_arb_monitor" > /dev/null 2>&1; then
    echo "✅ Funding arb monitor is running"
else
    echo "⚠️  Funding arb monitor is NOT running (it's a persistent process, not a cron job)"
    echo "   Start it with:  cd \"$REPO_DIR\" && ./start_funding_arb.sh start"
fi

echo ""
echo "Test the report now (prints to terminal, no email):"
echo "   cd \"$REPO_DIR\" && python3 daily_report.py --preview"
echo ""
echo "Send a test email:"
echo "   cd \"$REPO_DIR\" && python3 daily_report.py"
