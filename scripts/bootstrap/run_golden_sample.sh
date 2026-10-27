#!/bin/bash
# Pull the golden sample's minimal dataset (QUANT-35).
#
# Calendar + daily bars only — the golden sample needs those two. The other
# three fetchers are optional and are listed in README.
set -euo pipefail

cd "$(dirname "$0")/../.."

TICKERS="510300.SH,510500.SH,518880.SH,511010.SH,159915.SZ"
START="2024-12-01"
END="2025-03-01"

PYTHON="${PYTHON:-uv run python}"

echo "== calendar (SSE) =="
$PYTHON scripts/bootstrap/fetch_calendar.py --exchange SSE --start "$START" --end "$END"

echo "== bars_daily (${TICKERS}) =="
$PYTHON scripts/bootstrap/fetch_bars_daily.py --tickers "$TICKERS" --start "$START" --end "$END"

echo "== landed =="
find data_platform -name '*.parquet' | head -20
echo "row count:"
$PYTHON - <<'PY'
from pathlib import Path
import pandas as pd

total = 0
for path in sorted(Path("data_platform").rglob("*.parquet")):
    rows = len(pd.read_parquet(path))
    total += rows
    print(f"  {path}: {rows} rows")
print(f"total rows: {total}")
PY

echo "Done. Verify: uv run pytest tests/golden/ -q"
