"""Fundamentals -> ``data_platform/asset/fundamental/{date}.parquet``.

Source: tushare ``daily_basic`` (valuation ratios + share counts).  One file
per day, 11 columns per ``FUNDAMENTAL_SCHEMA``.

Unit conversions into the storage schema (``test_storage_schema`` fixtures:
Maotai ``total_market_cap=2.0e12`` 元, ``total_shares=1_256_000_000`` 股):

* ``total_mv`` / ``circ_mv`` are 万元 -> x 1e4 -> 元
* ``total_share`` / ``float_share`` are 万股 -> x 1e4 -> 股

``pe_lyr`` (static PE) has no ``daily_basic`` column and is left null — the
schema allows it.  Optional for the golden sample.
"""

from __future__ import annotations

import argparse

import pandas as pd

from _common import DATA_ROOT, init_tushare, ticker_convert, write_daily, yyyymmdd

_RENAME = {"ts_code": "ticker"}

_CAP_SCALE = 1e4  # 万元 -> 元
_SHARE_SCALE = 1e4  # 万股 -> 股


def build_frame(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    raw = pro.daily_basic(ts_code=ts_code, start_date=start, end_date=end)
    if raw is None or raw.empty:
        raise RuntimeError(
            f"{ts_code}: daily_basic returned no rows (ETF/LOF codes are not in "
            f"daily_basic — it is stock-only, like `daily`)"
        )
    out = raw.copy()
    out["ticker"] = out["ts_code"].map(ticker_convert)
    out["trade_date"] = pd.to_datetime(
        out["trade_date"].astype(str), format="%Y%m%d"
    ).dt.date
    out = out.rename(columns=_RENAME)

    out["total_market_cap"] = pd.to_numeric(out["total_mv"]) * _CAP_SCALE
    out["float_market_cap"] = pd.to_numeric(out["circ_mv"]) * _CAP_SCALE
    out["total_shares"] = (
        (pd.to_numeric(out["total_share"]) * _SHARE_SCALE).round().astype("int64")
    )
    out["float_shares"] = (
        (pd.to_numeric(out["float_share"]) * _SHARE_SCALE).round().astype("int64")
    )
    out["pe_ttm"] = pd.to_numeric(out["pe_ttm"], errors="coerce")
    out["pb"] = pd.to_numeric(out["pb"], errors="coerce")
    out["ps_ttm"] = pd.to_numeric(out["ps_ttm"], errors="coerce")
    out["pe_lyr"] = float("nan")  # no daily_basic column; schema-nullable
    out["quality_flag"] = 0
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", required=True, help="comma-separated ts_codes")
    parser.add_argument("--start", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--data-root", default=None, help=f"default: {DATA_ROOT}")
    args = parser.parse_args()
    start, end = yyyymmdd(args.start), yyyymmdd(args.end)

    pro = init_tushare()
    frames = [
        build_frame(pro, t.strip(), start, end)
        for t in args.tickers.split(",")
        if t.strip()
    ]
    written = write_daily(
        pd.concat(frames, ignore_index=True), "fundamental", args.data_root
    )
    print(f"wrote {sum(len(f) for f in frames)} rows across {len(written)} day files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
