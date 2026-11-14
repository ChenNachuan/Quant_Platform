"""AmazingData 原始暂存拉取脚本（QUANT-35，运行于 amazingdata 容器内）。

从 AmazingData 拉取交易日历、日线（volume 股、amount 元）、基金净值（NAV_ADJ_FACTOR）
与份额（FLOAT_SHARE 万份），以原始列名写入暂存 parquet，再由宿主机拉取脚本完成 schema 对齐。
登录凭证从容器环境变量读取，不打印、不落盘。
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

#: 暂存保留列白名单，其余 SDK 返回字段写入前丢弃
KLINE_COLS = ["code", "kline_time", "open", "high", "low", "close", "volume", "amount"]
NAV_COLS = [
    "MARKET_CODE",
    "PRICE_DATE",
    "NAV_ADJ_FACTOR",
    "UNIT_NAV",
    "ACCUM_UNIT_DIST",
]
SHARE_COLS = ["MARKET_CODE", "CHANGE_DATE", "FUND_SHARE", "FLOAT_SHARE"]


def parse_ymd(value: str) -> int:
    """将 YYYYMMDD 或 YYYY-MM-DD 转为整数日期。"""
    text = str(value).replace("-", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"not a date (YYYYMMDD or YYYY-MM-DD): {value!r}")
    return int(text)


def filter_calendar(all_days, start: int, end: int) -> pd.DataFrame:
    """将全量交易日历裁剪至 [start, end] 区间。"""
    frame = pd.DataFrame({"cal_date": list(all_days)})
    frame["cal_date"] = pd.to_numeric(frame["cal_date"], errors="coerce").astype(
        "Int64"
    )
    frame = frame.dropna(subset=["cal_date"]).astype({"cal_date": "int64"})
    window = frame[(frame["cal_date"] >= start) & (frame["cal_date"] <= end)]
    return window.sort_values("cal_date").reset_index(drop=True)


def pull_kline(market_data, tickers: list[str], start: int, end: int) -> pd.DataFrame:
    """批量调用 query_kline 拉取指定标的日线并拼接。"""
    import AmazingData as ad

    result = market_data.query_kline(
        tickers, begin_date=start, end_date=end, period=ad.constant.Period.day.value
    )
    if isinstance(result, dict):
        frames = [
            f for f in result.values() if isinstance(f, pd.DataFrame) and not f.empty
        ]
    elif isinstance(result, pd.DataFrame):
        frames = [result]
    else:
        frames = []
    if not frames:
        raise RuntimeError(
            f"query_kline({tickers}, {start}, {end}) returned no rows — "
            f"refusing to write an empty staging file"
        )
    return pd.concat(frames, ignore_index=True)


def _dict_frames(result, label: str) -> pd.DataFrame:
    if isinstance(result, dict):
        frames = [
            f for f in result.values() if isinstance(f, pd.DataFrame) and not f.empty
        ]
    elif isinstance(result, pd.DataFrame):
        frames = [result]
    else:
        frames = []
    if not frames:
        raise RuntimeError(f"{label}: returned no rows")
    return pd.concat(frames, ignore_index=True)


def keep(frame: pd.DataFrame, wanted: list[str]) -> pd.DataFrame:
    """筛选并校验必需列，缺列时报错。"""
    present = [c for c in wanted if c in frame.columns]
    missing = [c for c in wanted if c not in frame.columns]
    if missing:
        raise RuntimeError(f"staging source is missing expected columns: {missing}")
    return frame[present].copy()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", required=True, help="comma-separated codes")
    parser.add_argument("--start", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--staging", required=True, help="output directory")
    args = parser.parse_args()

    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    start, end = parse_ymd(args.start), parse_ymd(args.end)
    staging = Path(args.staging)
    staging.mkdir(parents=True, exist_ok=True)

    import AmazingData as ad
    from AmazingData import BaseData, InfoData, MarketData

    username = os.getenv("USER")
    password = os.getenv("PASSWORD")
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    if not all([username, password, host, port]):
        raise RuntimeError(
            "USER / PASSWORD / HOST / PORT are not all set — the container's "
            "env_file did not load.  Nothing is written without them."
        )

    ad.login(username=username, password=password, host=host, port=int(port))
    print("login ok", flush=True)
    try:
        base_data = BaseData()
        market_data = MarketData(base_data.get_calendar())
        info = InfoData()

        cal = filter_calendar(base_data.get_calendar(), start, end)
        if cal.empty:
            raise RuntimeError(f"no trading days in {start}..{end}")
        cal_path = staging / "calendar.parquet"
        cal.to_parquet(cal_path, engine="pyarrow", index=False)
        print(f"calendar: {len(cal)} days -> {cal_path}", flush=True)

        kline = keep(pull_kline(market_data, tickers, start, end), KLINE_COLS)
        kline_path = staging / "kline.parquet"
        kline.to_parquet(kline_path, engine="pyarrow", index=False)
        per_code = kline.groupby("code").size() if "code" in kline.columns else {}
        print(f"kline: {len(kline)} rows -> {kline_path}", flush=True)
        for code, n in per_code.items() if hasattr(per_code, "items") else []:
            print(f"  {code}: {n} rows", flush=True)

        nav = _dict_frames(
            info.get_fund_nav(code_list=tickers, begin_date=start, end_date=end),
            "get_fund_nav",
        )
        nav = keep(nav, NAV_COLS)
        nav_path = staging / "fund_nav.parquet"
        nav.to_parquet(nav_path, engine="pyarrow", index=False)
        print(f"fund_nav: {len(nav)} rows -> {nav_path}", flush=True)

        share = _dict_frames(
            info.get_fund_share(code_list=tickers, begin_date=start, end_date=end),
            "get_fund_share",
        )
        share = keep(share, SHARE_COLS)
        share_path = staging / "fund_share.parquet"
        share.to_parquet(share_path, engine="pyarrow", index=False)
        print(f"fund_share: {len(share)} rows -> {share_path}", flush=True)
    finally:
        # 释放登录连接名额（共享账号有并发上限）
        try:
            ad.logout(username=os.getenv("USER"))
            print("logout ok", flush=True)
        except Exception as exc:  # noqa: BLE001 — logout must never mask the pull
            print(f"logout failed: {exc}", flush=True)

    print("staging complete", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
