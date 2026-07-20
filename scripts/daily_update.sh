#!/bin/bash
# FinanceData Lake — daily orchestrator
# Fonte: yfinance + FRED -> S3 (equity-plataform)
set -euo pipefail

PROJECT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${PROJECT}/logs"
mkdir -p "${LOG_DIR}"

cd "${PROJECT}"

if [ -f "${PROJECT}/.env" ]; then
  set -a
  source "${PROJECT}/.env"
  set +a
fi

if [ -f "${PROJECT}/.venv/bin/python" ]; then
  PYTHON="${PROJECT}/.venv/bin/python"
else
  PYTHON="$(command -v python3)"
fi

AWS_CLI="$(command -v aws)"

echo "========================================"
echo "Daily Update Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "========================================"

if [ -z "${FRED_API_KEY:-}" ]; then
  echo "[ERROR] FRED_API_KEY not set — check .env"
  exit 1
fi

echo "[$(date -u '+%H:%M:%S')] Running download_fred.py..."
if ! "${PYTHON}" "${PROJECT}/scripts/download_fred.py"; then
  echo "[WARN] download_fred.py FAILED — continuing"
fi

echo "[$(date -u '+%H:%M:%S')] Running download_yfinance.py..."
if ! "${PYTHON}" "${PROJECT}/scripts/download_yfinance.py"; then
  echo "[WARN] download_yfinance.py FAILED — continuing"
fi

echo "[$(date -u '+%H:%M:%S')] Backup to S3..."

"${AWS_CLI}" s3 sync \
  "${PROJECT}/data/business_day" \
  s3://equity-plataform/data_lake/business_day \
  --exclude "._*" \
  --only-show-errors

"${AWS_CLI}" s3 sync \
  "${PROJECT}/data/macro_daily" \
  s3://equity-plataform/data_lake/macro_daily \
  --exclude "._*" \
  --only-show-errors

echo "[$(date -u '+%H:%M:%S')] S3 backup COMPLETE"
echo "[$(date -u '+%H:%M:%S')] Daily Update COMPLETE"
