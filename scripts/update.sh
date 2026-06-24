#!/bin/bash
# Fetch ETF + macro data, commit & push to GitHub (used by Actions + optional local cron)
#
# Local cron example (A 股交易时段 on weekdays):
#   30 9 * * 1-5  cd /path/to/data && bash scripts/update.sh >> .update.log 2>&1
#   0,30 10-11 * * 1-5  cd /path/to/data && bash scripts/update.sh >> .update.log 2>&1
#   0,30 13-14 * * 1-5  cd /path/to/data && bash scripts/update.sh >> .update.log 2>&1
#   0 15 * * 1-5  cd /path/to/data && bash scripts/update.sh >> .update.log 2>&1

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

PYTHON="${PYTHON:-python3}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

run_pipeline() {
  set +e
  "$PYTHON" "$@"
  local rc=$?
  set -e
  return "$rc"
}

continue_rebase_with_local_data() {
  local conflicted file
  conflicted="$(git diff --name-only --diff-filter=U || true)"
  if [ -z "$conflicted" ]; then
    return 1
  fi

  while IFS= read -r file; do
    case "$file" in
      public/etf-data.json|public/macro-data.json|scripts/cn-trade-dates.json) ;;
      *)
        log "Rebase hit unexpected conflict: $file"
        return 1
        ;;
    esac
  done <<EOF
$conflicted
EOF

  # During rebase, --theirs refers to the commit being replayed, i.e. this
  # run's freshly generated JSON.
  while IFS= read -r file; do
    [ -n "$file" ] || continue
    git checkout --theirs -- "$file"
    git add "$file"
  done <<EOF
$conflicted
EOF

  GIT_EDITOR=true git rebase --continue
}

if [ "${SKIP_GIT_PULL:-0}" != "1" ]; then
  log "Syncing main..."
  git pull --rebase origin main
fi

ETF_OK=0
MACRO_OK=0

log "Running ETF pipeline..."
if run_pipeline scripts/etf_pipeline.py --json-out public/etf-data.json; then
  ETF_OK=1
else
  rc=$?
  log "ETF pipeline failed (exit $rc)"
fi

log "Running macro pipeline..."
if run_pipeline scripts/macro_pipeline.py --json-out public/macro-data.json; then
  MACRO_OK=1
else
  rc=$?
  log "Macro pipeline failed (exit $rc)"
fi

if [ "$ETF_OK" = "0" ] && [ "$MACRO_OK" = "0" ]; then
  log "Both pipelines failed — aborting"
  exit 1
fi

log "Pipeline result: ETF=$ETF_OK MACRO=$MACRO_OK"

git config user.name  "${GIT_AUTHOR_NAME:-github-actions[bot]}"
git config user.email "${GIT_AUTHOR_EMAIL:-github-actions[bot]@users.noreply.github.com}"

git add public/etf-data.json public/macro-data.json
if [ -f scripts/cn-trade-dates.json ]; then
  git add scripts/cn-trade-dates.json
fi
if git diff --staged --quiet; then
  log "No file changes — skip commit"
  exit 0
fi

git commit -m "data: update $(date -u +'%Y-%m-%d %H:%M UTC')"

for attempt in 1 2 3; do
  if git push origin HEAD:main; then
    log "Pushed successfully"
    exit 0
  fi

  log "Push failed, rebasing (attempt ${attempt})"
  if git pull --rebase origin main; then
    continue
  fi

  if continue_rebase_with_local_data; then
    continue
  fi

  log "Automatic rebase failed"
  exit 1
done

log "Push failed after 3 attempts"
exit 1
