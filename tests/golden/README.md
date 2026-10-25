# Golden Sample — S2-T2 (QUANT-11)

Price fixture and per-ticker baseline for the hand-calc comparison against the
human Excel. Per-ticker arithmetic only — no portfolio layer (Option A).

## Files

| File | Content |
|---|---|
| `golden_prices.parquet` | 5 ETF × 77 trading days daily close |
| `golden_expected.json` | 15 scalars (3 metrics × 5 tickers), 6 decimal places |
| `golden_baseline.py` | ≤ 30-line per-ticker metrics implementation |
| `test_golden.py` | `test_pandas_matches_hand_calc` + fixture/anchor checks |

## Universe

`510300.SH` · `510500.SH` · `518880.SH` · `511010.SH` · `159915.SZ`

## Date range

| Phase | Dates | Trading days | Role |
|---|---|---|---|
| warmup | `2024-12-04` ~ `2024-12-31` | 20 | momentum lookback only; **not** in NAV |
| sample | `2025-01-02` ~ `2025-03-31` | 57 | measurement window |
| **total** | | **77** | |

## Fixture schema

| Column | Type | Description |
|---|---|---|
| `date` | str `YYYY-MM-DD` | trading date |
| `phase` | str `warmup` / `sample` | NAV measurement boundary |
| `510300.SH` … `159915.SZ` | float | daily close |

Daily close = `close` of the **last 5-minute bar** of the day, by `trade_time`.

## Source

5-minute ETF bars fetched with `akshare==1.18.40` (pin from `pyproject.toml`),
saved to `~/Downloads/5m_etf/{2024,2025}/*.parquet`.
Columns: `code` / `trade_time` / `open` / `high` / `low` / `close` / `vol` /
`amount` / `vwap` / `date`. Each parquet file is one trading day.

## Re-fetch command

From the repository root, with `~/Downloads/5m_etf/` populated:

```bash
python3 - << 'EOF'
from pathlib import Path
import numpy as np
import pandas as pd

SRC = Path.home() / "Downloads" / "5m_etf"
UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
WARMUP_LAST = "20241231"

warmup = sorted(p.stem for p in (SRC / "2024").glob("202412*.parquet"))
sample = sorted(p.stem for p in (SRC / "2025").glob("2025*.parquet") if "20250102" <= p.stem <= "20250331")
rows = []
for day in warmup + sample:
    df = pd.read_parquet(SRC / day[:4] / f"{day}.parquet", columns=["code", "trade_time", "close"])
    df = df[df["code"].isin(UNIVERSE)].sort_values("trade_time")
    last = df.groupby("code")["close"].last()
    iso = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    row = {"date": iso, "phase": "warmup" if day <= WARMUP_LAST else "sample"}
    for t in UNIVERSE:
        row[t] = float(last.get(t, np.nan))
    rows.append(row)
pd.DataFrame(rows).to_parquet("tests/golden/golden_prices.parquet", index=False)
print(f"wrote {len(rows)} rows")
EOF
```

Expected output: `wrote 77 rows`.

## Metric conventions

All three metrics share one input series: `r_t = close_t / close_{t-1} - 1`
(simple returns), 57 sample days, `rf = 0`, no fees, no slippage. Warmup
returns never enter.

| Metric | Formula | Trap |
|---|---|---|
| `total_return` | `Π(1 + r_t) - 1`, `t = 1..57` | **not** `close(3-31)/close(1-02) - 1` |
| `sharpe` | `mean(r) / std(r, ddof=1) × √252` | not monthly; not `ddof=0` |
| `mdd` | `min_t(NAV_t / peak_t - 1)`, `NAV_0 = 1.0` at `2024-12-31` | **running peak must include `NAV_0`** |

Full definitions: QUANT-11 description §4.1 / §4.2 / §4.3.

## Portfolio layer (S8)

S2-T2 closed the per-ticker layer under Option A: `golden_expected.json` keeps
exactly `total_return`/`sharpe`/`mdd` × 5 tickers and gains no `portfolio`
key. The portfolio layer arrived in S8 as its own step with its own files, so
the two expected files never mix.

### Fixture source

The portfolio layer reuses `golden_prices.parquet` unchanged — same 5 ETF × 77
trading days, same warmup/sample split. What is new is the expected values:

| File | Content |
|---|---|
| `golden_expected_portfolio.json` | 3 scalars (`total_return`, `sharpe`, `mdd`) at 6 dp |
| `portfolio_baseline.py` | the pandas reference the SimBroker chain is compared against |
| `portfolio_handcalc.md` | the independent hand-calc the expected file is transcribed from |

### Rebalance schedule

| Date | Role |
|---|---|
| `2024-12-31` | formation close — `NAV_0 = 1.0`, weights 1/5 each |
| `2025-01-27` | month-end rebalance (last January trading day; Spring Festival) |
| `2025-02-28` | month-end rebalance |
| `2025-03-31` | month-end rebalance; the last sample day, so it moves no sample return |

Four decision dates, never `_select_rebalance_dates("monthly")` — monthly keeps
the *first* trading day of each month and would move every fill. Weights decide
at a decision-date close and apply to returns strictly after it; between
rebalances they drift with prices.

### Hand-calc provenance

`golden_expected_portfolio.json` is a 6 dp transcription of
`portfolio_handcalc.md`, which derives every number from `golden_prices.parquet`
and first principles. Per QUANT-11 Ruling 2B the expected numbers are authored
**outside** the implementation — no portfolio implementation was consulted —
so the comparison is not circular. A human-authored Excel, if supplied,
supersedes the document as authority.

Steps 1–2 fill at the **decision-date close** (no fees, no slippage). Steps 3–4
fill at the **T+1 open** (gap #5 ruling B); because the fixture is closes-only
and bars carry `open(D) := close(D-1)`, that open strikes the same number the
hand-calc used. That is a fixture modelling choice, not a third fill basis.

## Zero-network

All golden tests run under the autouse `_no_network` fixture in
`tests/conftest.py`. No test opens a socket.
