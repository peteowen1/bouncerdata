#!/bin/bash
# run_scrape.sh - Wrapper script for daily bouncer scraper
#
# This script:
# 1. Adds random delay (0-30 min) to avoid predictable patterns
# 2. Sets up environment variables
# 3. Runs the R scraper with logging
# 4. Cleans up old logs
#
# Install on Oracle VM:
#   mkdir -p ~/bouncer-scraper/{data,logs}
#   cp run_scrape.sh ~/bouncer-scraper/
#   cp daily_scrape.R ~/bouncer-scraper/
#   chmod +x ~/bouncer-scraper/run_scrape.sh
#
# Cron (7 AM UTC daily, 1 hour after pannadata):
#   0 7 * * * /home/opc/bouncer-scraper/run_scrape.sh

set -e

# Configuration
SCRAPER_DIR="$HOME/bouncer-scraper"
LOG_DIR="$SCRAPER_DIR/logs"
DATA_DIR="$SCRAPER_DIR/data"

# Create directories
mkdir -p "$LOG_DIR" "$DATA_DIR"

# Random delay (0-1800 seconds = 0-30 minutes)
DELAY=$((RANDOM % 1800))
echo "$(date '+%Y-%m-%d %H:%M:%S') - Waiting $DELAY seconds before starting..."
sleep $DELAY

# Log file with timestamp
LOG_FILE="$LOG_DIR/scrape_$(date +%Y%m%d_%H%M%S).log"

echo "========================================" | tee "$LOG_FILE"
echo "Bouncer Daily Scrape" | tee -a "$LOG_FILE"
echo "Started: $(date '+%Y-%m-%d %H:%M:%S %Z')" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Set environment variables
if [ -f "$HOME/.github_pat" ]; then
    export GITHUB_PAT=$(cat "$HOME/.github_pat")
    echo "GitHub PAT loaded from ~/.github_pat" | tee -a "$LOG_FILE"
else
    echo "ERROR: ~/.github_pat not found!" | tee -a "$LOG_FILE"
    exit 1
fi

export R_ZIPCMD=/usr/bin/zip
export BOUNCER_DATA_DIR="$DATA_DIR"

# Run the R scraper
echo "" | tee -a "$LOG_FILE"
echo "Running daily_scrape.R..." | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

$HOME/bin/micromamba run -n r-env Rscript "$SCRAPER_DIR/daily_scrape.R" 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}

echo "" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"
echo "Finished: $(date '+%Y-%m-%d %H:%M:%S %Z')" | tee -a "$LOG_FILE"
echo "Exit code: $EXIT_CODE" | tee -a "$LOG_FILE"
echo "========================================" | tee -a "$LOG_FILE"

# Cleanup old logs (keep 14 days)
find "$LOG_DIR" -name "scrape_*.log" -mtime +14 -delete 2>/dev/null || true

# Keep only last 30 log files regardless of age
ls -1t "$LOG_DIR"/scrape_*.log 2>/dev/null | tail -n +31 | xargs -r rm 2>/dev/null || true

exit $EXIT_CODE
