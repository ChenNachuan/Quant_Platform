"""Five-year ETF fixture from AmazingData staging (QUANT-32 S8-T3, human ruling 2026-10-29).

Red line 6: reproducible via bootstrap scripts (was <=30-line akshare).
Authorized source: AmazingData (supersedes O1 akshare fund_etf_hist_em).

Pipeline (two hops, SDK only in container):
  1. docker compose run --rm --no-deps amazingdata \\
       python3 /opt/project/scripts/bootstrap/amazing_pull.py \\
       --tickers 510300.SH,510500.SH,518880.SH,511010.SH,159915.SZ \\
       --start 20191101 --end 20241231 \\
       --staging /opt/project/data_platform/raw/amazingdata
  2. uv run python tests/golden/fetch_five_year_etf.py

Adjustment: hfq = raw x NAV_ADJ_FACTOR / adj_first (arg_min by date).
NAV_ADJ_FACTOR is written as-is by scripts/bootstrap/_common.py; no renormalization.
"""

import pandas as pd

UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
WARMUP_DAYS = 20
STAGING = "data_platform/raw/amazingdata"

kline = pd.read_parquet(f"{STAGING}/kline.parquet").rename(
    columns={"kline_time": "trade_date"}
)
nav = pd.read_parquet(f"{STAGING}/fund_nav.parquet")
nav["trade_date"] = pd.to_datetime(nav["PRICE_DATE"], format="%Y%m%d")
nav = nav.rename(columns={"MARKET_CODE": "code"})[
    ["code", "trade_date", "NAV_ADJ_FACTOR"]
]

merged = kline.merge(nav, on=["code", "trade_date"], how="left")
if merged["NAV_ADJ_FACTOR"].isna().any():
    raise ValueError("NAV_ADJ_FACTOR missing for some bar days")
merged = merged.sort_values(["code", "trade_date"])
merged["adj_first"] = merged.groupby("code")["NAV_ADJ_FACTOR"].transform("first")

for col in ["open", "close"]:
    merged[f"{col}_hfq"] = merged[col] * merged["NAV_ADJ_FACTOR"] / merged["adj_first"]

frames = []
for code in UNIVERSE:
    d = merged[merged["code"] == code].sort_values("trade_date").set_index("trade_date")
    d = d[["open_hfq", "close_hfq"]]
    d.columns = [f"{code}.open", f"{code}.close"]
    frames.append(d)

out = pd.concat(frames, axis=1, join="inner").sort_index().reset_index()
out = out.rename(columns={"trade_date": "date"})
sample_start = pd.Timestamp("2020-01-02")
warmup_idx = out.index[out["date"] < sample_start][-WARMUP_DAYS:]
out = out.loc[warmup_idx.union(out.index[out["date"] >= sample_start])].copy()
out["phase"] = out["date"].apply(lambda d: "warmup" if d < sample_start else "sample")
out.to_parquet("tests/golden/golden_prices_2020_2024.parquet", index=False)
print(
    f"wrote {len(out)} rows "
    f"({(out['phase'] == 'warmup').sum()} warmup + "
    f"{(out['phase'] == 'sample').sum()} sample)"
)
