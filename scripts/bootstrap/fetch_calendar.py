"""交易日历拉取脚本 -> ``data_platform/asset/calendar/{exchange}.parquet``。

默认通过 tushare ``trade_cal`` 拉取开市交易日；传入 ``--from-amazing`` 时直接读取
``amazing_pull.py`` 暂存的日历文件落库。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _common import (
    DATA_ROOT,
    frame_from_amazing_calendar,
    init_tushare,
    write_reference,
    yyyymmdd,
)


def fetch_calendar(pro, exchange: str, start: str, end: str) -> pd.DataFrame:
    """拉取指定交易所 [start, end] 区间内的开市交易日。"""
    raw = pro.trade_cal(exchange=exchange, start_date=start, end_date=end)
    if raw is None or raw.empty:
        raise RuntimeError(f"trade_cal({exchange}, {start}, {end}) returned no rows")
    open_days = raw[raw["is_open"].astype(int) == 1].copy()
    if open_days.empty:
        raise RuntimeError(f"trade_cal({exchange}) has no open days in {start}..{end}")
    open_days["trade_date"] = pd.to_datetime(open_days["cal_date"]).dt.date
    return open_days[["trade_date"]].sort_values("trade_date").reset_index(drop=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--exchange", default="SSE", help="tushare exchange code (SSE/SZSE)"
    )
    parser.add_argument("--start", help="YYYYMMDD or YYYY-MM-DD (tushare path)")
    parser.add_argument("--end", help="YYYYMMDD or YYYY-MM-DD (tushare path)")
    parser.add_argument("--data-root", default=None, help=f"default: {DATA_ROOT}")
    parser.add_argument(
        "--from-amazing",
        metavar="STAGING_DIR",
        default=None,
        help="land from amazing_pull.py staging instead of calling tushare",
    )
    args = parser.parse_args()

    if args.from_amazing is not None:
        path = Path(args.from_amazing) / "calendar.parquet"
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} is missing — run scripts/bootstrap/amazing_pull.py first"
            )
        frame = frame_from_amazing_calendar(pd.read_parquet(path))
    else:
        if not args.start or not args.end:
            parser.error("--start and --end are required unless --from-amazing is set")
        pro = init_tushare()
        frame = fetch_calendar(
            pro, args.exchange, yyyymmdd(args.start), yyyymmdd(args.end)
        )

    # 输出文件名使用小写交易所代码（SSE -> sse，SZSE -> szse）
    target = write_reference(
        frame, "calendar", args.data_root, exchange=args.exchange.lower()
    )
    print(f"wrote {len(frame)} trading days -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
