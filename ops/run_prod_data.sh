#!/usr/bin/env bash
# Golden-path daily production DATA build (full S&P universe, no pytest).
# From repo root: bash ops/run_prod_data.sh
# Cron: see scripts/setup_cron.sh

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ -f "$ROOT/.env" ]; then
  set -a
  # shellcheck source=/dev/null
  source "$ROOT/.env"
  set +a
fi

export PIPELINE_MODE="${PIPELINE_MODE:-prod}"
export PYTHONPATH="${PYTHONPATH:-$ROOT}"
export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"

PY="${ROOT}/.venv/bin/python"
if [ ! -x "$PY" ]; then
  echo "error: expected venv at $PY (create with: python3 -m venv .venv && pip install -r requirements.txt)" >&2
  exit 1
fi

PARALLEL="${PROD_DATA_PARALLEL:-8}"

exec "$PY" pipeline/run_pipeline.py --full --parallel "$PARALLEL" --skip-tests
