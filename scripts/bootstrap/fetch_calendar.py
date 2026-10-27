"""Trading calendar -> ``data_platform/asset/calendar/{exchange}.parquet``.

Source is **tushare ``trade_cal``** (see README "日历源" for why not akshare).
One call covers the whole window; the file is a single per-exchange parquet,
not one file per day.

``--from-amazing`` lands the calendar ``amazing_pull.py`` staged instead —
used by the golden-sample runner, whose bars come from AmazingData and whose
tushare ``trade_cal`` quota is 1 call/hour.
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
    """Return the open trading days in ``[start, end]`` for *exchange*."""
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

    # Filename exchange token is the lowercase storage name (core.asset_rules
    # EXCHANGE_MAP): SSE -> sse, SZSE -> szse.  AmazingData's calendar is
    # market-wide; the exchange only names the output file.
    target = write_reference(
        frame, "calendar", args.data_root, exchange=args.exchange.lower()
    )
    print(f"wrote {len(frame)} trading days -> {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
