"""Daily bars -> ``data_platform/asset/bars_daily/{date}.parquet``.

One file per day, 14 columns per ``BARS_DAILY_SCHEMA``.  Sources per the
ticket's source table: tushare ``daily`` + ``adj_factor`` + ``suspend_d`` +
limit flags, merged onto the schema.  ``quality_flag`` is always 0.

Known limits of the tushare API surface (measured, not inferred — see README
"实测" and ``task/probe_tushare_quant35*.log``):

* ``daily`` is **stock-only**; an ETF/LOF ``ts_code`` returns 0 rows.  Its bars
  live in ``fund_daily``, which is a separate permission.
* ``pro.limit_list`` is not a real interface name.  The real ones are
  ``limit_list_d`` / ``stk_limit``, both of which may be permission-gated.
  When no limit data is reachable, ``is_limit_up`` / ``is_limit_down`` are
  written as ``False`` — a **source limitation, not a market fact**.

Nothing here fabricates a price or an adjustment factor: if the bar or
``adj_factor`` source is unreachable the fetch fails loudly instead of
writing a plausible-looking wrong file.
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd

from _common import (
    DATA_ROOT,
    init_tushare,
    ticker_convert,
    write_daily,
    yyyymmdd,
)

#: Column renames from tushare frame -> BARS_DAILY_SCHEMA names.
_RENAME = {
    "ts_code": "ticker",
    "vol": "volume",
    "adj_factor": "adjfactor",
}


def _to_date(series: pd.Series) -> pd.Series:
    """tushare ``trade_date`` (``YYYYMMDD``) -> ``datetime.date``."""
    return pd.to_datetime(series.astype(str), format="%Y%m%d").dt.date


def _try_api(pro, name: str, **kwargs) -> pd.DataFrame | None:
    """Call tushare *name*; return its frame, or ``None`` when unreachable."""
    fn = getattr(pro, name, None)
    if fn is None:
        return None
    try:
        result = fn(**kwargs)
    except Exception as exc:  # noqa: BLE001 — permission / rate / bad name
        print(f"  [{name}] unavailable: {exc}", file=sys.stderr)
        return None
    return result if isinstance(result, pd.DataFrame) else None


def _fetch_bars(pro, ts_code: str, start: str, end: str) -> tuple[pd.DataFrame, str]:
    """Return (raw bars, api_name) for *ts_code*.

    ``daily`` covers stocks; ``fund_daily`` covers ETF/LOF.  Whichever answers
    first wins; when both return nothing the ticket's premise is wrong for this
    token and we fail rather than write an empty day file.
    """
    for api_name in ("daily", "fund_daily"):
        frame = _try_api(pro, api_name, ts_code=ts_code, start_date=start, end_date=end)
        if frame is not None and not frame.empty:
            return frame, api_name
    raise RuntimeError(
        f"{ts_code}: no daily bars from tushare. `daily()` is stock-only (returns 0 "
        f"rows for an ETF/LOF code) and `fund_daily()` is the ETF interface — if "
        f"the log above says 没有接口(fund_daily)访问权限, the token's points tier "
        f"does not include fund_daily and the golden-sample pull cannot run on it."
    )


def _fetch_adjfactor(pro, ts_code: str, start: str, end: str) -> pd.DataFrame:
    for api_name in ("adj_factor", "fund_adj"):
        frame = _try_api(pro, api_name, ts_code=ts_code, start_date=start, end_date=end)
        if frame is not None and not frame.empty:
            return frame
    raise RuntimeError(
        f"{ts_code}: no adjustment factor from tushare (tried adj_factor, fund_adj). "
        f"adjfactor is required and non-nullable — refusing to write 1.0 as if it "
        f"were a measured factor."
    )


def _fetch_suspend(pro, ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    frame = _try_api(pro, "suspend_d", ts_code=ts_code, start_date=start, end_date=end)
    return frame


def _fetch_limit_flags(pro, ts_code: str, start: str, end: str) -> pd.DataFrame | None:
    """Try each known limit-flag interface; ``None`` when none is reachable."""
    for api_name in ("limit_list_d", "stk_limit", "limit_list"):
        frame = _try_api(
            pro,
            api_name,
            ts_code=ts_code,
            start_date=start,
            end_date=end,
        )
        if frame is not None:
            return frame
    return None


def _turnover(bars: pd.DataFrame, basic: pd.DataFrame | None) -> pd.Series:
    """AC-A2: ``amount / float_market_cap``, NaN when market cap is unreachable.

    *amount* is already in CNY here; tushare ``circ_mv`` is 流通市值 in 万元,
    so the cap is scaled by 1e4 before the division.
    """
    if basic is None or basic.empty or "circ_mv" not in basic.columns:
        return pd.Series(float("nan"), index=bars.index, dtype="float64")
    cap = basic.set_index("trade_date")["circ_mv"]
    market_cap = pd.to_numeric(bars["trade_date"].map(cap), errors="coerce")
    return bars["amount"] / (market_cap * 1e4)


def build_frame(pro, ts_code: str, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    """Pull one ticker's bars and map them onto the 14-column schema frame."""
    bars, bars_api = _fetch_bars(pro, ts_code, start, end)
    adj = _fetch_adjfactor(pro, ts_code, start, end)
    suspend = _fetch_suspend(pro, ts_code, start, end)
    limits = _fetch_limit_flags(pro, ts_code, start, end)
    basic = _try_api(
        pro, "daily_basic", ts_code=ts_code, start_date=start, end_date=end
    )

    out = bars.copy()
    out["ticker"] = out["ts_code"].map(ticker_convert)
    out["trade_date"] = _to_date(out["trade_date"])
    out = out.rename(columns=_RENAME)
    # Unit conversions into the storage schema's units (storage_schema.md §55.2
    # + test_storage_schema fixtures): volume is 股, amount is 元.
    out["volume"] = (pd.to_numeric(out["volume"]) * 100).round().astype("int64")
    out["amount"] = pd.to_numeric(out["amount"]) * 1000.0

    merged = out.merge(
        adj[["trade_date", "adj_factor"]].assign(
            trade_date=_to_date(adj["trade_date"])
        ),
        on="trade_date",
        how="left",
    )
    if merged["adj_factor"].isna().any():
        raise RuntimeError(f"{ts_code}: adj_factor missing for some days; not writing")

    merged["turnover"] = _turnover(merged, basic)
    merged["suspend"] = False
    if suspend is not None and not suspend.empty:
        halted = set(_to_date(suspend["trade_date"]))
        merged["suspend"] = merged["trade_date"].isin(halted)

    merged["is_limit_up"] = False
    merged["is_limit_down"] = False
    if limits is not None and not limits.empty:
        merged = _apply_limit_flags(merged, limits)

    merged["quality_flag"] = 0
    evidence = {
        "ticker": ts_code,
        "bars_api": bars_api,
        "bar_rows": int(len(bars)),
        "adj_rows": int(len(adj)),
        "suspend_rows": int(len(suspend)) if suspend is not None else 0,
        "limit_rows": int(len(limits)) if limits is not None else 0,
        "basic_rows": int(len(basic)) if basic is not None else 0,
    }
    return merged, evidence


def _apply_limit_flags(merged: pd.DataFrame, limits: pd.DataFrame) -> pd.DataFrame:
    flags = limits.copy()
    flags["trade_date"] = _to_date(flags["trade_date"])
    if "limit" in flags.columns:
        up = set(flags.loc[flags["limit"].astype(str).str.upper() == "U", "trade_date"])
        down = set(
            flags.loc[flags["limit"].astype(str).str.upper() == "D", "trade_date"]
        )
    else:
        up = set(flags.loc[flags.get("is_limit_up", False) == True, "trade_date"])  # noqa: E712
        down = set(flags.loc[flags.get("is_limit_down", False) == True, "trade_date"])  # noqa: E712
    merged["is_limit_up"] = merged["trade_date"].isin(up)
    merged["is_limit_down"] = merged["trade_date"].isin(down)
    return merged


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", required=True, help="comma-separated ts_codes")
    parser.add_argument("--start", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="YYYYMMDD or YYYY-MM-DD")
    parser.add_argument("--data-root", default=None, help=f"default: {DATA_ROOT}")
    args = parser.parse_args()
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
    start, end = yyyymmdd(args.start), yyyymmdd(args.end)

    pro = init_tushare()
    frames: list[pd.DataFrame] = []
    for ts_code in tickers:
        frame, evidence = build_frame(pro, ts_code, start, end)
        print(f"  {ts_code}: {evidence}")
        frames.append(frame)

    combined = pd.concat(frames, ignore_index=True)
    written = write_daily(combined, "bars_daily", args.data_root)
    print(f"wrote {len(combined)} rows across {len(written)} day files")
    for path in written[:5]:
        print(f"  {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
