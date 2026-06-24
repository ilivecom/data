#!/bin/bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

LOG="$REPO_DIR/.update.log"

is_trade_day() {
  PYTHONDONTWRITEBYTECODE=1 python3 - <<'PY'
import datetime
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('etf_pipeline', Path('scripts/etf_pipeline.py'))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
raise SystemExit(0 if mod._is_cn_trade_day() else 1)
PY
}

is_market_window() {
  local hm hour minute total
  hm="${MARKET_TIME_OVERRIDE_HM:-$(TZ=Asia/Shanghai date +%H%M)}"
  hour="${hm%??}"
  minute="${hm#??}"
  total=$((10#$hour * 60 + 10#$minute))

  # launchd missed calendar events are coalesced on wake, so tolerate delayed
  # starts inside the active sessions instead of requiring an exact hh:mm match.
  if ((
    (total >=  9 * 60 + 25 && total <= 11 * 60 + 35) ||
    (total >= 12 * 60 + 55 && total <= 15 * 60 + 10) ||
    (total >= 16 * 60 + 15 && total <= 16 * 60 + 35)
  )); then
    return 0
  fi

  return 1
}

if ! is_trade_day; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Skip local fallback: non-trading day" >> "$LOG"
  exit 0
fi

if ! is_market_window; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Skip local fallback: outside allowed market sessions" >> "$LOG"
  exit 0
fi

bash scripts/update.sh
