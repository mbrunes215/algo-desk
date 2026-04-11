#!/bin/bash
# start_funding_arb.sh — Start the funding rate arb monitor as a background process
#
# This keeps the monitor running indefinitely (every 30 min scan loop).
# Logs go to logs/funding_arb.log. Safe to run multiple times — checks
# if already running first.
#
# Usage:
#   cd ~/Documents/Claude/Projects/Algo\ Trading\ Desk/algo-desk
#   chmod +x start_funding_arb.sh
#   ./start_funding_arb.sh
#
# Other commands:
#   ./start_funding_arb.sh stop     — stop the running monitor
#   ./start_funding_arb.sh restart  — stop then immediately start fresh
#   ./start_funding_arb.sh status   — check if it's running
#   ./start_funding_arb.sh logs     — tail the live log

set -e

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$(which python3)"
LOG_FILE="$REPO_DIR/logs/funding_arb.log"
PID_FILE="$REPO_DIR/logs/funding_arb.pid"

# Ensure logs directory exists
mkdir -p "$REPO_DIR/logs"

# ─── STOP ───────────────────────────────────────────────────
_do_stop() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID"
            rm "$PID_FILE"
            echo "✅ Funding arb monitor stopped (PID $PID)"
        else
            echo "⚠️  Process $PID not found — already stopped?"
            rm -f "$PID_FILE"
        fi
    else
        echo "⚠️  No PID file found — monitor may not be running."
        echo "   Check manually: ps aux | grep run_monitor"
    fi
}

if [ "${1:-}" = "stop" ]; then
    _do_stop
    exit 0
fi

# ─── RESTART ────────────────────────────────────────────────
if [ "${1:-}" = "restart" ]; then
    _do_stop
    sleep 1
    # Fall through to START below
fi

# ─── STATUS ─────────────────────────────────────────────────
if [ "${1:-}" = "status" ]; then
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "✅ Funding arb monitor is RUNNING (PID $PID)"
            echo "   Log: $LOG_FILE"
            echo "   Last 3 log lines:"
            tail -3 "$LOG_FILE" 2>/dev/null | sed 's/^/   /'
        else
            echo "❌ PID file exists but process $PID is NOT running."
            echo "   The monitor may have crashed. Check the log:"
            echo "   tail -50 $LOG_FILE"
            rm -f "$PID_FILE"
        fi
    else
        echo "⚪ Funding arb monitor is NOT running."
    fi
    exit 0
fi

# ─── LOGS ───────────────────────────────────────────────────
if [ "${1:-}" = "logs" ]; then
    echo "Tailing $LOG_FILE (Ctrl+C to stop)..."
    tail -f "$LOG_FILE"
    exit 0
fi

# ─── START ──────────────────────────────────────────────────

# Check if already running (restart falls through here with PID already killed)
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "⚠️  Monitor already running (PID $PID)"
        echo "   Use './start_funding_arb.sh restart' to stop and relaunch."
        exit 0
    else
        echo "Stale PID file found — cleaning up."
        rm -f "$PID_FILE"
    fi
fi

echo "Starting funding arb monitor..."
echo "Repo:    $REPO_DIR"
echo "Python:  $PYTHON"
echo "Log:     $LOG_FILE"
echo ""

# Launch in background — nohup keeps it running after terminal close
nohup bash -c "cd \"$REPO_DIR\" && $PYTHON -m strategies.crypto_funding_arb.run_monitor --loop" \
    >> "$LOG_FILE" 2>&1 &

PID=$!
echo $PID > "$PID_FILE"

# Brief pause then verify it started
sleep 2
if kill -0 "$PID" 2>/dev/null; then
    echo "✅ Monitor started successfully (PID $PID)"
    echo ""
    echo "   Watch live:    ./start_funding_arb.sh logs"
    echo "   Check status:  ./start_funding_arb.sh status"
    echo "   Stop:          ./start_funding_arb.sh stop"
    echo ""
    echo "   First scan takes ~10 seconds. Subsequent scans every 30 minutes."
else
    echo "❌ Monitor failed to start. Check the log:"
    echo "   cat $LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi
