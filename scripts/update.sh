#!/bin/bash
# Auto-update ETF + macro data and push to GitHub
# Run via cron on macOS: every 15min during market hours

set -e

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

PYTHON="/usr/bin/python3"
LOG="$REPO_DIR/.update.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running pipelines..." >> "$LOG"

git pull --rebase origin main >> "$LOG" 2>&1

# ETF pipeline
"$PYTHON" scripts/etf_pipeline.py --json-out public/etf-data.json >> "$LOG" 2>&1

# Macro pipeline
"$PYTHON" scripts/macro_pipeline.py --json-out public/macro-data.json >> "$LOG" 2>&1

# Commit + push if anything changed
git add public/etf-data.json public/macro-data.json
if git diff --staged --quiet; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] No changes, skip commit" >> "$LOG"
else
    git commit -m "data: update $(TZ='Asia/Shanghai' date '+%Y-%m-%d %H:%M') CST" >> "$LOG" 2>&1
    for attempt in 1 2 3; do
        if git push >> "$LOG" 2>&1; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pushed ✅" >> "$LOG"
            exit 0
        fi
        echo "Push failed, rebasing (attempt ${attempt})" >> "$LOG"
        git pull --rebase origin main >> "$LOG" 2>&1
    done
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Push failed after 3 attempts ❌" >> "$LOG"
    exit 1
fi
