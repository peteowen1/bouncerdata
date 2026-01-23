# full_export_and_upload.R
# Complete workflow: Export parquets and upload to GitHub Release
#
# This script runs both:
# 1. export_parquets.R - Creates parquet files from DuckDB
# 2. upload_to_release.R - Uploads to GitHub Release
#
# Prerequisites:
# - bouncer.duckdb exists with all tables
# - GITHUB_PAT set or ~/.github_pat file exists
#
# Usage:
#   setwd("path/to/bouncerdata")
#   source("scripts/full_export_and_upload.R")

library(cli)

cli_h1("Bouncerdata Full Export and Upload")
cli_alert_info("Started at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}")

# Step 1: Export parquets
cli_h2("Step 1: Exporting Parquets")
source("scripts/export_parquets.R")

# Step 2: Upload to GitHub
cli_h2("Step 2: Uploading to GitHub Release")
source("scripts/upload_to_release.R")

cli_h1("Complete!")
cli_alert_success("Finished at: {format(Sys.time(), '%Y-%m-%d %H:%M:%S')}")
