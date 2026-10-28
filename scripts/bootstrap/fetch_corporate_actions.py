"""Corporate actions -> ``data_platform/asset/reference/corporate_actions.parquet``.

Source: tushare ``dividend`` (分红送转).  A single reference file, PK
``(ticker, ex_date, event_type)``.

``event_type`` is narrowed to the schema's four values
(``dividend`` / ``split`` / ``rights`` / ``bonus``) from the dividend record's
fields: cash -> ``dividend``, share allotment -> ``rights``, share bonus /
capitalisation -> ``bonus``, pure split -> ``split``.  Optional for the golden
sample — its role is cross-checking ``adjfactor`` later.
"""

from __future__ import annotations

import argparse

import pandas as pd

from _common import DATA_ROOT, init_tushare, ticker_convert, write_reference, yyyymmdd


def _event_type(row: pd.Series) -> str:
    if _num(row.get("stk_div")) > 0:
        return "bonus"
    if _num(row.get("stk_bo_rate")) > 0 or _num(row.get("stk_co_rate")) > 0:
        return "rights"
    if _num(row.get("cash_div")) > 0:
        return "dividend"
    return "split"


def _num(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def build_frame(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    raw = pro.dividend(ts_code=ts_code)
    if raw is None or raw.empty:
        return pd.DataFrame(columns=["ticker", "ex_date", "event_type", "detail"])
    frame = raw.copy()
    frame = frame[frame["ex_date"].notna() & (frame["ex_date"].astype(str) != "")]
    frame = frame[
        (frame["ex_date"].astype(str) >= start) & (frame["ex_date"].astype(str) <= end)
    ]
    if frame.empty:
        return pd.DataFrame(columns=["ticker", "ex_date", "event_type", "detail"])

    out = pd.DataFrame(
        {
            "ticker": frame["ts_code"].map(ticker_convert),
            "ex_date": pd.to_datetime(
                frame["ex_date"].astype(str), format="%Y%m%d"
            ).dt.date,
            "event_type": frame.apply(_event_type, axis=1),
            "announce_date": _opt_date(frame.get("ann_date")),
            "record_date": _opt_date(frame.get("record_date")),
            "detail": frame.apply(_detail, axis=1),
            "cash_dividend": pd.to_numeric(frame.get("cash_div"), errors="coerce"),
            "stock_dividend": pd.to_numeric(frame.get("stk_div"), errors="coerce"),
            "rights_price": float("nan"),
            "rights_ratio": pd.to_numeric(frame.get("stk_bo_rate"), errors="coerce"),
        }
    )
    return out.reset_index(drop=True)


def _opt_date(series: object) -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")
    text = series.astype(str)
    parsed = pd.to_datetime(text, format="%Y%m%d", errors="coerce")
    return parsed.dt.date


def _detail(row: pd.Series) -> str:
    return f"cash_div={row.get('cash_div', 0)} stk_div={row.get('stk_div', 0)}"


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
    combined = pd.concat(frames, ignore_index=True)
    if combined.empty:
        print("no corporate actions in window; nothing written")
        return 0
    path = write_reference(combined, "corporate_actions", args.data_root)
    print(f"wrote {len(combined)} rows -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
