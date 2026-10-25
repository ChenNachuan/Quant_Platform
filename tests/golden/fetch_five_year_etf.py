"""Five-year ETF fixture fetch (QUANT-32 S8-T3 step 5).

Route: akshare fund_etf_hist_em daily direct — NOT 5-minute aggregation.
Adjustment: hfq (后复权) so returns include ETF distributions.
Window: 2019-11-01 .. 2024-12-31 (warmup 20d + sample 2020-2024).

Usage:  uv run python tests/golden/fetch_five_year_etf.py
"""

import pandas as pd
import akshare as ak

UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
START, END = "20191101", "20241231"
WARMUP_DAYS = 20

frames = {}
for full in UNIVERSE:
    bare = full.split(".")[0]
    df = ak.fund_etf_hist_em(
        symbol=bare, period="daily", start_date=START, end_date=END, adjust="hfq"
    )
    df = df[["日期", "开盘", "收盘"]].rename(
        columns={"日期": "date", "开盘": f"{full}.open", "收盘": f"{full}.close"}
    )
    df["date"] = pd.to_datetime(df["date"])
    frames[full] = df.set_index("date")

out = pd.concat(frames.values(), axis=1, join="inner").sort_index().reset_index()
sample_start = pd.Timestamp("2020-01-02")
warmup_idx = out.index[out["date"] < sample_start][-WARMUP_DAYS:]
out = out.loc[warmup_idx.union(out.index[out["date"] >= sample_start])].copy()
out["phase"] = out["date"].apply(lambda d: "warmup" if d < sample_start else "sample")
out.to_parquet("tests/golden/golden_prices_2020_2024.parquet", index=False)
print(
    f"wrote {len(out)} rows ({(out['phase'] == 'warmup').sum()} warmup + {(out['phase'] == 'sample').sum()} sample)"
)
