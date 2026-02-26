#!/bin/bash
set -e
# Wrapper script that ensures Python + Chrome are killed when this shell dies.
# When the parent process sends a termination signal (e.g., Ctrl+C, CI timeout),
# the trap fires and kills the Python scraper process.

cleanup() {
    if [ -n "$PYTHON_PID" ] && kill -0 "$PYTHON_PID" 2>/dev/null; then
        echo "[wrapper] Killing Python PID $PYTHON_PID..."
        kill "$PYTHON_PID" 2>/dev/null
        sleep 2
        # Force kill if still alive
        kill -0 "$PYTHON_PID" 2>/dev/null && kill -9 "$PYTHON_PID" 2>/dev/null
    fi
}

trap cleanup EXIT INT TERM

cd "$(dirname "$0")/.."
python scripts/cricinfo_scraper.py "$@" &
PYTHON_PID=$!
echo "[wrapper] Python PID: $PYTHON_PID"
wait "$PYTHON_PID"
exit $?
