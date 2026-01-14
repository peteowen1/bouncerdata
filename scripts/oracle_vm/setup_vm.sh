#!/bin/bash
# setup_vm.sh - Set up bouncer scraper on Oracle Cloud VM
#
# Run this script on the VM after SSH'ing in:
#   ssh -i "ssh-key.key" opc@168.138.108.69
#   curl -sSL https://raw.githubusercontent.com/peteowen1/bouncerdata/dev/scripts/oracle_vm/setup_vm.sh | bash
#
# Or copy files manually and run:
#   bash setup_vm.sh

set -e

echo "========================================"
echo "Setting up Bouncer Scraper on Oracle VM"
echo "========================================"

SCRAPER_DIR="$HOME/bouncer-scraper"

# 1. Create directories
echo ""
echo "1. Creating directories..."
mkdir -p "$SCRAPER_DIR"/{data,logs}
echo "   Created: $SCRAPER_DIR/{data,logs}"

# 2. Check if micromamba and R environment exist (from pannadata setup)
echo ""
echo "2. Checking R environment..."
if [ -f "$HOME/bin/micromamba" ]; then
    echo "   micromamba found!"

    # Check if r-env exists
    if $HOME/bin/micromamba env list | grep -q "r-env"; then
        echo "   r-env environment found!"
    else
        echo "   ERROR: r-env environment not found. Run pannadata setup first."
        exit 1
    fi
else
    echo "   ERROR: micromamba not found. Run pannadata setup first."
    echo "   See: ORACLE_CLOUD_SCRAPER_SETUP.md"
    exit 1
fi

# 3. Install bouncer package
echo ""
echo "3. Installing bouncer R package..."
$HOME/bin/micromamba run -n r-env Rscript -e "
if (!requireNamespace('remotes', quietly = TRUE)) install.packages('remotes')
remotes::install_github('peteowen1/bouncer', ref = 'dev', upgrade = 'never')
cat('bouncer package installed!\n')
"

# 4. Copy scraper scripts (if not already present)
echo ""
echo "4. Setting up scraper scripts..."

# Check if daily_scrape.R exists
if [ ! -f "$SCRAPER_DIR/daily_scrape.R" ]; then
    echo "   Downloading daily_scrape.R..."
    curl -sSL -o "$SCRAPER_DIR/daily_scrape.R" \
        "https://raw.githubusercontent.com/peteowen1/bouncerdata/main/scripts/oracle_vm/daily_scrape.R"
fi

if [ ! -f "$SCRAPER_DIR/run_scrape.sh" ]; then
    echo "   Downloading run_scrape.sh..."
    curl -sSL -o "$SCRAPER_DIR/run_scrape.sh" \
        "https://raw.githubusercontent.com/peteowen1/bouncerdata/main/scripts/oracle_vm/run_scrape.sh"
fi

chmod +x "$SCRAPER_DIR/run_scrape.sh"
echo "   Scripts ready!"

# 5. Install gh CLI if not present
echo ""
echo "5. Checking gh CLI..."
if command -v gh &> /dev/null; then
    echo "   gh CLI found!"
else
    echo "   Installing gh CLI..."
    # For Oracle Linux / RHEL
    sudo dnf install -y 'dnf-command(config-manager)'
    sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo
    sudo dnf install -y gh
    echo "   gh CLI installed!"
fi

# 6. Check GitHub PAT and authenticate gh
echo ""
echo "6. Checking GitHub PAT..."
if [ -f "$HOME/.github_pat" ]; then
    echo "   GitHub PAT found!"
    # Authenticate gh CLI
    gh auth login --with-token < "$HOME/.github_pat" 2>/dev/null || true
    echo "   gh CLI authenticated!"
else
    echo "   WARNING: ~/.github_pat not found!"
    echo "   Create it with: echo 'ghp_YOUR_TOKEN' > ~/.github_pat && chmod 600 ~/.github_pat"
fi

# 7. Set up cron job
echo ""
echo "7. Setting up cron job..."
CRON_LINE="0 7 * * * $SCRAPER_DIR/run_scrape.sh"

if crontab -l 2>/dev/null | grep -q "bouncer-scraper"; then
    echo "   Cron job already exists!"
else
    echo "   Adding cron job (7 AM UTC daily)..."
    (crontab -l 2>/dev/null || true; echo "$CRON_LINE") | crontab -
    echo "   Cron job added!"
fi

# 8. Summary
echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Directory: $SCRAPER_DIR"
echo "Cron: 7 AM UTC daily (+ 0-30 min random delay)"
echo ""
echo "To test manually:"
echo "  $SCRAPER_DIR/run_scrape.sh"
echo ""
echo "To check logs:"
echo "  tail -f $SCRAPER_DIR/logs/scrape_*.log"
echo ""
echo "To check cron:"
echo "  crontab -l"
echo ""
