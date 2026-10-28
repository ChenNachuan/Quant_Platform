"""AmazingData -> raw staging parquet (QUANT-35 bootstrap, human-authorized).

Runs **inside** the amazingdata container — the SDK is only installed there::

    docker compose run --rm --no-deps amazingdata \\
        python3 /opt/project/scripts/bootstrap/amazing_pull.py \\
        --tickers 510300.SH,... --start 2024-12-01 --end 2025-03-31 \\
        --staging /opt/project/data_platform/raw/amazingdata

Writes the source's own frames (source column names, source units) with plain
``to_parquet``.  No schema alignment happens here: the container has no
pandera, and ``data/storage_schema.py`` is the host-side authority.  The
fetchers land the schema-aligned files from this staging.

Measured source facts (``task/probe_amazing_quant35.log``, not inferred):

* ``query_kline`` columns ``code, kline_time, open, high, low, close, volume,
  amount``; ``volume`` is 股 and ``amount`` is 元 — already the storage unit,
  so no ×100 / ×1000 scaling the way tushare needs.
* ``code`` is ``{6 digits}.{SH|SZ|BJ}``, the same string as the repo ticker.
* ``InfoData.get_fund_nav`` carries ``NAV_ADJ_FACTOR`` — a measured cumulative
  adjustment factor that moves on distributions.  ``get_dividend`` /
  ``get_right_issue`` return 0 rows for these ETFs over 1990-2025, so the NAV
  factor is the only adjustment source this vendor exposes.
* ``InfoData.get_fund_share`` carries ``FLOAT_SHARE`` in 万份.

Credentials arrive from the container's env (``docker-compose.yml`` env_file);
they are never printed and never written to disk by this script.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

#: Source frames that must survive the staging round-trip.  Anything else the
#: SDK returns is dropped on write so the staging files stay small and stable.
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
    """``YYYYMMDD`` or ``YYYY-MM-DD`` -> the int AmazingData expects."""
    text = str(value).replace("-", "").strip()
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"not a date (YYYYMMDD or YYYY-MM-DD): {value!r}")
    return int(text)


def filter_calendar(all_days, start: int, end: int) -> pd.DataFrame:
    """Narrow ``BaseData.get_calendar()``'s full history to ``[start, end]``."""
    frame = pd.DataFrame({"cal_date": list(all_days)})
    frame["cal_date"] = pd.to_numeric(frame["cal_date"], errors="coerce").astype(
        "Int64"
    )
    frame = frame.dropna(subset=["cal_date"]).astype({"cal_date": "int64"})
    window = frame[(frame["cal_date"] >= start) & (frame["cal_date"] <= end)]
    return window.sort_values("cal_date").reset_index(drop=True)


def pull_kline(market_data, tickers: list[str], start: int, end: int) -> pd.DataFrame:
    """One ``query_kline`` call for the whole ticker list; stacked source frame."""
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
    """Project *frame* onto *wanted*, keeping every column that actually exists."""
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
        # Release the connection slot — the shared account has a login cap.
        try:
            ad.logout(username=os.getenv("USER"))
            print("logout ok", flush=True)
        except Exception as exc:  # noqa: BLE001 — logout must never mask the pull
            print(f"logout failed: {exc}", flush=True)

    print("staging complete", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
