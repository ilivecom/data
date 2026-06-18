#!/bin/bash
# Auto-update ETF + macro data and push to GitHub
# Designed to run via cron on macOS (working dir = repo root)

set -e

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

PYTHON="/usr/bin/python3"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running pipelines..."

git pull --rebase origin main

# ETF pipeline
"$PYTHON" scripts/etf_pipeline.py --json-out public/etf-data.json

# Macro pipeline  
"$PYTHON" scripts/macro_pipeline.py --json-out public/macro-data.json

# Commit + push if anything changed
git add public/etf-data.json public/macro-data.json
if git diff --staged --quiet; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] No changes, skip commit"
else
    git commit -m "data: update $(TZ='Asia/Shanghai' date '+%Y-%m-%d %H:%M') CST"
    git push
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Pushed ✅"
fi
