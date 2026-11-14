"""基本面估值与股本拉取脚本 -> ``data_platform/asset/cn_stock_fundamental/{month}.parquet``。

从 tushare ``daily_basic`` 按月写入 11 列基本面数据，并换算为标准单位：
``total_mv``/``circ_mv``（万元 → 元）、``total_share``/``float_share``（万股 → 股），
``pe_lyr`` 无对应来源字段故留空。
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
    out["trade_date"] = pd.to_datetime(
        out["trade_date"].astype(str), format="%Y%m%d"
    ).dt.date
    # 先重命名再做 ticker 校验，避免出现重复 ticker 列
    out = out.rename(columns=_RENAME)
    out["ticker"] = out["ticker"].map(ticker_convert)

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
    out["pe_lyr"] = float("nan")  # daily_basic 无静态市盈率字段，按 schema 留空
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
    print(
        f"wrote {sum(len(f) for f in frames)} rows across {len(written)} partition files"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
