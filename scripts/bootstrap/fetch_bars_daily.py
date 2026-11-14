"""日线行情拉取脚本 -> ``data_platform/asset/bars_daily/{date}.parquet``。

按日写入符合 ``BARS_DAILY_SCHEMA`` 的 14 列数据：合并 tushare ``daily``/``fund_daily``、
``adj_factor``/``fund_adj``、``suspend_d`` 及涨跌停标志（不可达时置为 False），
或通过 ``--from-amazing`` 读取 AmazingData 暂存文件落库；缺失行情或复权因子直接报错。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from _common import (
    DATA_ROOT,
    frame_from_amazing_kline,
    init_tushare,
    ticker_convert,
    write_daily,
    yyyymmdd,
)

#: 合并复权因子前先重命名基础列；adj_factor 留待合并后再重命名为 adjfactor
_RENAME = {
    "ts_code": "ticker",
    "vol": "volume",
}


def _to_date(series: pd.Series) -> pd.Series:
    """将 tushare YYYYMMDD 日期列转为 datetime.date。"""
    return pd.to_datetime(series.astype(str), format="%Y%m%d").dt.date


def _try_api(pro, name: str, **kwargs) -> pd.DataFrame | None:
    """安全调用 tushare 接口，无权限或不可用时返回 None。"""
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
    """拉取单标的行情（股票走 daily，ETF/LOF 走 fund_daily），均无数据则报错。"""
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
    """依次尝试涨跌停接口，均不可达时返回 None。"""
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
    """计算换手率 amount / (circ_mv * 1e4)，缺少流通市值数据时返回 NaN。"""
    if basic is None or basic.empty or "circ_mv" not in basic.columns:
        return pd.Series(float("nan"), index=bars.index, dtype="float64")
    # basic["trade_date"] 需先转为 datetime.date 再与 bars 对齐映射
    keyed = basic.assign(trade_date=_to_date(basic["trade_date"]))
    cap = keyed.set_index("trade_date")["circ_mv"]
    market_cap = pd.to_numeric(bars["trade_date"].map(cap), errors="coerce")
    return bars["amount"] / (market_cap * 1e4)


def build_frame(pro, ts_code: str, start: str, end: str) -> tuple[pd.DataFrame, dict]:
    """拉取单标的日线、复权因子及辅助字段并映射为 14 列标准表。"""
    bars, bars_api = _fetch_bars(pro, ts_code, start, end)
    adj = _fetch_adjfactor(pro, ts_code, start, end)
    suspend = _fetch_suspend(pro, ts_code, start, end)
    limits = _fetch_limit_flags(pro, ts_code, start, end)
    basic = _try_api(
        pro, "daily_basic", ts_code=ts_code, start_date=start, end_date=end
    )

    out = bars.copy()
    out["trade_date"] = _to_date(out["trade_date"])
    # 先重命名再做 ticker 校验，避免出现重复 ticker 列
    out = out.rename(columns=_RENAME)
    out["ticker"] = out["ticker"].map(ticker_convert)
    # 单位换算为存储标准：volume 股（手×100），amount 元（千元×1000）
    out["volume"] = (pd.to_numeric(out["volume"]) * 100).round().astype("int64")
    out["amount"] = pd.to_numeric(out["amount"]) * 1000.0

    merged = out.merge(
        adj[["trade_date", "adj_factor"]].assign(
            trade_date=_to_date(adj["trade_date"])
        ),
        on="trade_date",
        how="left",
    )
    # 合并后将 adj_factor 重命名为 adjfactor
    merged = merged.rename(columns={"adj_factor": "adjfactor"})
    if merged["adjfactor"].isna().any():
        raise RuntimeError(f"{ts_code}: adjfactor missing for some days; not writing")

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


def _frame_from_staging(staging: Path) -> pd.DataFrame:
    """读取 amazing_pull.py 暂存文件并映射为标准日线表。"""

    def load(name: str) -> pd.DataFrame:
        path = staging / name
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} is missing — run scripts/bootstrap/amazing_pull.py first"
            )
        return pd.read_parquet(path)

    return frame_from_amazing_kline(
        load("kline.parquet"), load("fund_nav.parquet"), load("fund_share.parquet")
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", required=True, help="comma-separated ts_codes")
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
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]

    if args.from_amazing is not None:
        combined = _frame_from_staging(Path(args.from_amazing))
        covered = set(combined["ticker"])
        unknown = [t for t in tickers if t not in covered]
        if unknown:
            raise RuntimeError(
                f"staging has no bars for {unknown}; it only holds "
                f"{sorted(covered)} — re-run amazing_pull.py with the full list"
            )
    else:
        if not args.start or not args.end:
            parser.error("--start and --end are required unless --from-amazing is set")
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
