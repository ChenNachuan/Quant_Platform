#!/bin/bash
# Pull the golden sample's minimal dataset (QUANT-35).
#
# Calendar + daily bars only — the golden sample needs those two. The other
# three fetchers are optional and are listed in README.
#
# Source is AmazingData (human-authorized 2026-10-28), staged in two hops
# because the SDK only exists inside the container and pandera only exists on
# the host:
#
#   1. amazing_pull.py  (container)  -> data_platform/raw/amazingdata/*.parquet
#   2. fetch_*.py --from-amazing (host) -> data_platform/asset/...
#
# Set SOURCE=tushare to use the tushare path instead (needs a token whose tier
# includes fund_daily — the bootstrap token does not).
set -euo pipefail

cd "$(dirname "$0")/../.."

TICKERS="510300.SH,510500.SH,518880.SH,511010.SH,159915.SZ"
START="2024-12-01"
END="2025-03-31"
STAGING="data_platform/raw/amazingdata"
SOURCE="${SOURCE:-amazingdata}"
PYTHON="${PYTHON:-uv run python}"

if [ "$SOURCE" = "amazingdata" ]; then
    echo "== stage 1: AmazingData -> raw staging =="
    docker compose run --rm --no-deps amazingdata \
        python3 /opt/project/scripts/bootstrap/amazing_pull.py \
        --tickers "$TICKERS" --start "$START" --end "$END" \
        --staging "/opt/project/$STAGING"

    echo "== calendar (SSE) =="
    $PYTHON scripts/bootstrap/fetch_calendar.py \
        --exchange SSE --from-amazing "$STAGING"

    echo "== bars_daily (${TICKERS}) =="
    $PYTHON scripts/bootstrap/fetch_bars_daily.py \
        --tickers "$TICKERS" --from-amazing "$STAGING"
else
    echo "== calendar (SSE) via tushare =="
    $PYTHON scripts/bootstrap/fetch_calendar.py --exchange SSE --start "$START" --end "$END"

    echo "== bars_daily (${TICKERS}) via tushare =="
    $PYTHON scripts/bootstrap/fetch_bars_daily.py --tickers "$TICKERS" --start "$START" --end "$END"
fi

echo "== landed =="
find data_platform/asset -name '*.parquet' | sort | head -20
echo "row count:"
$PYTHON - <<'PY'
from pathlib import Path
import pandas as pd

total = 0
for path in sorted(Path("data_platform/asset").rglob("*.parquet")):
    rows = len(pd.read_parquet(path))
    total += rows
    print(f"  {path}: {rows} rows")
print(f"total rows: {total}")
PY

echo "Done. Verify: uv run pytest tests/golden/ -q"
