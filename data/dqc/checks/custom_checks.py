"""DQC 自定义检查函数（无法用声明式列规则表达的逻辑）。

所有检查均为纯函数，返回 ``pd.Series[bool]``（``True`` 表示校验失败）：
- 行级检查：索引与输入 DataFrame 对齐。
- 聚合检查：长度为 1，索引为 ``__aggregate__``。
- 跨表检查（``cross_checks.py``）：索引为 ``(ticker, trade_date)``。

交易日历、股票池、除权日等外部参考数据均在调用时注入，不在函数内读盘；
列名优先匹配 ``storage_schema`` 标准名，并兼容旧列名回退。
"""

from __future__ import annotations

import pandas as pd


# ── 辅助函数 ─────────────────────────────────────────────────────────────


def _col(df: pd.DataFrame, *candidates: str) -> str | None:
    """返回 *candidates* 中首个存在于 *df* 的列名。"""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _value_col(df: pd.DataFrame) -> str | None:
    """因子值列名：标准名 ``value``，兼容旧名 ``factor_value``。"""
    return _col(df, "value", "factor_value")


def _dt_col(df: pd.DataFrame) -> str | None:
    """分钟时间戳列名：标准名 ``datetime``，兼容旧名 ``timestamp``。"""
    return _col(df, "datetime", "timestamp")


def _date_col(df: pd.DataFrame) -> str | None:
    """日期列名：``trade_date``，兼容旧名 ``date``。"""
    return _col(df, "trade_date", "date")


def _prev_close(df: pd.DataFrame) -> pd.Series:
    """优先取 ``prev_close``，缺失时按代码与交易日计算前收盘价。"""
    if "prev_close" in df.columns:
        return df["prev_close"]
    sorted_df = df.sort_values(["ticker", "trade_date"])
    prev = sorted_df.groupby("ticker")["close"].shift(1)
    return prev.reindex(df.index)


def _bool_series(s: pd.Series) -> pd.Series:
    """布尔列转型，NaN 视为 False。"""
    return s.fillna(False).astype(bool)


def _is_suspended(df: pd.DataFrame) -> pd.Series:
    """返回停牌行布尔掩码（无 ``suspend`` 列时全为 False）。"""
    c = _col(df, "suspend")
    if c is None:
        return pd.Series(False, index=df.index)
    return _bool_series(df[c])


# ── 完整性 C01–C10 ──────────────────────────────────────────────────────


def check_row_count_vs_calendar(
    df: pd.DataFrame, calendar_dates=None, comparison: str = "eq", **_
) -> pd.Series:
    """C01：去重交易日数须与日历区间内交易日数一致（未注入日历则失败）。"""
    if calendar_dates is None:
        return pd.Series([True], index=["__aggregate__"])
    dc = _date_col(df)
    if dc is None:
        return pd.Series([True], index=["__aggregate__"])
    dates = pd.to_datetime(df[dc])
    cal = pd.to_datetime(list(calendar_dates))
    expected = int(((cal >= dates.min()) & (cal <= dates.max())).sum())
    actual = dates.nunique()
    failed = actual != expected if comparison == "eq" else actual < expected
    return pd.Series([failed], index=["__aggregate__"])


def check_row_count_vs_universe(
    df: pd.DataFrame, universe=None, min_ratio: float = 0.95, **_
) -> pd.Series:
    """C02：单日去重标的数量须 >= ``len(universe) * min_ratio``。"""
    if universe is None or len(universe) == 0:
        return pd.Series([True], index=["__aggregate__"])
    ratios = df.groupby(_date_col(df) or "trade_date")["ticker"].nunique() / len(
        universe
    )
    failed = bool((ratios < min_ratio).any())
    return pd.Series([failed], index=["__aggregate__"])


def check_minute_bar_count(
    df: pd.DataFrame, expected_bars: int = 240, **_
) -> pd.Series:
    """C03：每只标的每日分钟线根数须等于 *expected_bars*（默认 240）。"""
    counts = df.groupby(["ticker", _date_col(df) or "trade_date"]).size()
    has_bad = bool((counts != expected_bars).any())
    return pd.Series([has_bad], index=["__aggregate__"])


def check_null_soft_fields(
    df: pd.DataFrame, max_null_rate: float = 0.01, columns=None, **_
) -> pd.Series:
    """C05：指定软字段空值率须 <= *max_null_rate*（表中不存在的列跳过）。"""
    if columns is None:
        return pd.Series([False], index=["__aggregate__"])
    for col in columns:
        if col in df.columns:
            rate = df[col].isna().mean()
            if rate > max_null_rate:
                return pd.Series([True], index=["__aggregate__"])
    return pd.Series([False], index=["__aggregate__"])


def check_adjfactor_present(
    df: pd.DataFrame, max_missing_rate: float = 0.0, **_
) -> pd.Series:
    """C06：复权因子缺失率须 <= *max_missing_rate*（默认 0）。"""
    if "adjfactor" not in df.columns:
        return pd.Series([True], index=["__aggregate__"])
    missing = df["adjfactor"].isna()
    rate = missing.mean()
    return pd.Series([rate > max_missing_rate], index=["__aggregate__"])


def check_calendar_completeness(
    df: pd.DataFrame,
    forward_days: int = 30,
    today=None,
    max_gap_days: int = 12,
    **_,
) -> pd.Series:
    """C07：交易日历须至少向后覆盖 *forward_days* 个交易日，且间隔 <= *max_gap_days*（容忍春节等长假约 11 天）。"""
    ref = (
        pd.to_datetime(today) if today is not None else pd.Timestamp.today().normalize()
    )
    dc = _date_col(df) or "trade_date"
    dates = pd.to_datetime(df[dc]).sort_values().unique()
    window_end = ref + pd.Timedelta(days=forward_days * 2)
    forward_dates = dates[(dates >= ref) & (dates <= window_end)]
    if len(forward_dates) < forward_days:
        return pd.Series([True], index=["__aggregate__"])
    diffs = pd.Series(forward_dates).diff().dropna()
    has_gap = bool((diffs > pd.Timedelta(days=max_gap_days)).any())
    return pd.Series([has_gap], index=["__aggregate__"])


def check_factor_date_coverage(
    df: pd.DataFrame,
    window: int = 252,
    max_missing: int = 2,
    calendar_dates=None,
    **_,
) -> pd.Series:
    """C08：近 *window* 个交易日内因子缺失天数须 <= *max_missing*。"""
    dc = _date_col(df) or "trade_date"
    frame_dates = pd.to_datetime(df[dc])
    dates = set(frame_dates.unique())
    if calendar_dates is not None:
        cal_all = pd.to_datetime(list(calendar_dates))
        cal_sorted = sorted(cal_all[cal_all <= frame_dates.max()])[-window:]
        missing = len(set(cal_sorted) - dates)
        return pd.Series([missing > max_missing], index=["__aggregate__"])
    if len(dates) >= window:
        return pd.Series([False], index=["__aggregate__"])
    return pd.Series([(window - len(dates)) > max_missing], index=["__aggregate__"])


def check_factor_ticker_coverage(
    df: pd.DataFrame, universe=None, min_ratio: float = 0.90, **_
) -> pd.Series:
    """C09：因子覆盖标的比例须 >= *min_ratio*。"""
    if universe is None or len(universe) == 0:
        return pd.Series([True], index=["__aggregate__"])
    coverage = df["ticker"].nunique() / len(universe)
    return pd.Series([coverage < min_ratio], index=["__aggregate__"])


def check_industry_coverage(
    df: pd.DataFrame, universe=None, min_ratio: float = 1.0, **_
) -> pd.Series:
    """C10：行业分类对目标股票池覆盖率须 >= *min_ratio*（默认 100%）。"""
    if universe is None or len(universe) == 0:
        return pd.Series([True], index=["__aggregate__"])
    covered = set(df["ticker"].unique()) if "ticker" in df.columns else set()
    ratio = len(covered & set(universe)) / len(universe)
    return pd.Series([ratio < min_ratio], index=["__aggregate__"])


# ── 合法性 L03–L05, L12 ─────────────────────────────────────────────────


def check_ohlc_relation(df: pd.DataFrame, tolerance: float = 0.0, **_) -> pd.Series:
    """L03：检查 ``low <= min(open, close)`` 且 ``max(open, close) <= high``（允许 *tolerance* 容差）。"""
    lower = df[["open", "close"]].min(axis=1)
    upper = df[["open", "close"]].max(axis=1)
    failed = (df["low"] > lower + tolerance) | (df["high"] < upper - tolerance)
    return failed


def check_price_limit_stock(
    df: pd.DataFrame,
    main_limit: float = 0.10,
    chinext_star_limit: float = 0.20,
    ipo_first_5d_limit: float = 0.30,
    tolerance: float = 0.005,
    **_,
) -> pd.Series:
    """L04：个股日涨跌幅须在板块涨跌停限制 + *tolerance* 范围内。

    无板块标记列时按代码前缀判定（``30``/``68`` 开头为创业板/科创板 20%）。
    """
    pc = _prev_close(df)
    if pc.isna().all():
        return pd.Series([False] * len(df), index=df.index)
    change = ((df["close"] - pc) / pc).abs()
    limit = pd.Series(main_limit, index=df.index)

    # 缺少显式板块列时根据代码前缀推断 20% 板块
    tickers = df["ticker"].astype(str)
    is_chinext_star = tickers.str.startswith("30") | tickers.str.startswith("68")
    if "is_chinext_star" in df.columns:
        is_chinext_star = _bool_series(df["is_chinext_star"])
    limit = limit.where(~is_chinext_star, chinext_star_limit)

    if "is_ipo_first_5d" in df.columns:
        is_ipo = _bool_series(df["is_ipo_first_5d"])
        limit = limit.where(~is_ipo, ipo_first_5d_limit)

    return change > limit + tolerance


def check_price_limit_futures(
    df: pd.DataFrame,
    tolerance: float = 0.005,
    contract_specs=None,
    lookup_table=None,
    **_,
) -> pd.Series:
    """L05：期货日涨跌幅须在合约涨跌停板 + *tolerance* 范围内。"""
    if contract_specs is None:
        contract_specs = lookup_table
    pc = _prev_close(df)
    if contract_specs is None or pc.isna().all():
        return pd.Series([False] * len(df), index=df.index)
    change = ((df["close"] - pc) / pc).abs()
    limits = df["ticker"].map(contract_specs).fillna(0.10)
    return change > limits + tolerance


def check_factor_nan_rate(
    df: pd.DataFrame, max_nan_rate: float = 0.10, **_
) -> pd.Series:
    """L12：因子值 NaN 比例须 <= *max_nan_rate*。"""
    vc = _value_col(df)
    if vc is None:
        return pd.Series([False], index=["__aggregate__"])
    rate = df[vc].isna().mean()
    return pd.Series([rate > max_nan_rate], index=["__aggregate__"])


# ── 一致性 K01–K05, K09–K11 ─────────────────────────────────────────────


def check_open_gap(
    df: pd.DataFrame, max_gap: float = 0.10, ca_dates=None, **_
) -> pd.Series:
    """K01：非除权除息日开盘跳空幅度须 <= *max_gap*。"""
    pc = _prev_close(df)
    if pc.isna().all():
        return pd.Series([False] * len(df), index=df.index)
    gap = ((df["open"] - pc) / pc).abs()
    failed = gap > max_gap
    if ca_dates is not None and len(ca_dates):
        is_ca = df[_date_col(df) or "trade_date"].isin(ca_dates)
        failed = failed & ~is_ca
    return failed


def check_volume_amount_match(df: pd.DataFrame, **_) -> pd.Series:
    """K02：``volume > 0`` 与 ``amount > 0`` 须双向一致。"""
    vol_pos = df["volume"] > 0
    amt_pos = df["amount"] > 0
    return vol_pos != amt_pos


def check_suspend_consistency(df: pd.DataFrame, **_) -> pd.Series:
    """K03：停牌行须满足 ``volume == 0`` 且 OHLC 四价相等。"""
    if "suspend" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    sus = _bool_series(df["suspend"])
    flat = (
        (df["open"] == df["high"])
        & (df["high"] == df["low"])
        & (df["low"] == df["close"])
    )
    zero_vol = df["volume"] == 0
    return sus & ~(flat & zero_vol)


def check_limit_flag_consistency(
    df: pd.DataFrame, tolerance: float = 0.01, **_
) -> pd.Series:
    """K04：涨停时 ``close`` 须贴近 ``high``，跌停时贴近 ``low``（偏差 <= *tolerance*）。"""
    failed = pd.Series(False, index=df.index)
    up_col = _col(df, "is_limit_up", "limit_up")
    down_col = _col(df, "is_limit_down", "limit_down")
    if up_col is not None:
        lu = _bool_series(df[up_col])
        failed |= lu & ((df["close"] - df["high"]).abs() > tolerance)
    if down_col is not None:
        ld = _bool_series(df[down_col])
        failed |= ld & ((df["close"] - df["low"]).abs() > tolerance)
    return failed


def check_adjfactor_stable(df: pd.DataFrame, ca_dates=None, **_) -> pd.Series:
    """K05：非公司行为日 ``adjfactor`` 不得变动。"""
    if "adjfactor" not in df.columns:
        return pd.Series([False] * len(df), index=df.index)
    dc = _date_col(df) or "trade_date"
    df_sorted = df.sort_values(["ticker", dc])
    prev = df_sorted.groupby("ticker")["adjfactor"].shift(1)
    changed = df_sorted["adjfactor"] != prev
    is_ca = (
        df_sorted[dc].isin(ca_dates)
        if ca_dates is not None and len(ca_dates)
        else pd.Series(False, index=df_sorted.index)
    )
    failed = changed & ~is_ca & prev.notna()
    return failed.reindex(df.index, fill_value=False)


def check_factor_cross_section(
    df: pd.DataFrame,
    min_std: float = 1e-8,
    max_abs_mean: float = 100,
    **_,
) -> pd.Series:
    """K09：因子横截面标准差须 > *min_std* 且均值绝对值 < *max_abs_mean*。"""
    vc = _value_col(df)
    if vc is None:
        return pd.Series([False], index=["__aggregate__"])
    dc = _date_col(df) or "trade_date"
    grouped = df.groupby(dc)[vc]
    bad_std = (grouped.std() <= min_std).any()
    bad_mean = (grouped.mean().abs() >= max_abs_mean).any()
    return pd.Series([bool(bad_std or bad_mean)], index=["__aggregate__"])


def check_industry_universe_match(df: pd.DataFrame, universe=None, **_) -> pd.Series:
    """K10：行业表标的集合须与股票池完全一致。"""
    if universe is None:
        return pd.Series([True], index=["__aggregate__"])
    industry_set = set(df["ticker"].unique()) if "ticker" in df.columns else set()
    universe_set = set(universe)
    return pd.Series([industry_set != universe_set], index=["__aggregate__"])


def check_fund_vs_daily_tickers(
    df: pd.DataFrame, daily_tickers=None, max_diff: int = 5, **_
) -> pd.Series:
    """K11：基本面表与日线表标的集合对称差须 <= *max_diff*。"""
    if daily_tickers is None:
        return pd.Series([True], index=["__aggregate__"])
    fund_set = set(df["ticker"].unique()) if "ticker" in df.columns else set()
    diff = fund_set.symmetric_difference(set(daily_tickers))
    return pd.Series([len(diff) > max_diff], index=["__aggregate__"])


# ── 时效性 T01–T09 ──────────────────────────────────────────────────────


def check_daily_freshness(
    df: pd.DataFrame,
    max_stale_days: int = 1,
    today=None,
    calendar_dates=None,
    **_,
) -> pd.Series:
    """T01：最新交易日距 *today* 须在 *max_stale_days* 个交易日内（无日历时退化为工作日计数）。"""
    dc = _date_col(df) or "trade_date"
    latest = pd.to_datetime(df[dc]).max()
    ref = (
        pd.to_datetime(today) if today is not None else pd.Timestamp.today().normalize()
    )
    if calendar_dates is not None:
        cal = pd.to_datetime(list(calendar_dates))
        between = cal[(cal > latest) & (cal <= ref)]
        stale = len(between) > max_stale_days
    else:
        stale = max(len(pd.bdate_range(latest, ref)) - 1, 0) > max_stale_days
    return pd.Series([bool(stale)], index=["__aggregate__"])


def check_minute_freshness(
    df: pd.DataFrame,
    max_stale_days: int = 1,
    today=None,
    calendar_dates=None,
    **_,
) -> pd.Series:
    """T02：分钟线新鲜度检查，规则同 T01。"""
    return check_daily_freshness(
        df, max_stale_days=max_stale_days, today=today, calendar_dates=calendar_dates
    )


def check_no_future_dates(df: pd.DataFrame, today=None, **_) -> pd.Series:
    """T03：所有交易日须 <= *today*（禁止未来日期）。"""
    ref = (
        pd.to_datetime(today) if today is not None else pd.Timestamp.today().normalize()
    )
    dc = _date_col(df) or "trade_date"
    dates = pd.to_datetime(df[dc])
    has_future = bool((dates > ref).any())
    return pd.Series([has_future], index=["__aggregate__"])


def check_date_ascending(df: pd.DataFrame, **_) -> pd.Series:
    """T04：分区内时间须严格递增无重复（分钟线按 ``(ticker, trade_date)`` 检查 ``datetime``，其余按 ``ticker`` 检查日期）。"""
    dc = _date_col(df) or "trade_date"
    ts_col = _dt_col(df)
    if ts_col is not None and "ticker" in df.columns:
        ts = pd.to_datetime(df[ts_col])
        diff = ts.groupby([df["ticker"], df[dc]]).diff()
        bad = (diff <= pd.Timedelta(0)) & diff.notna()
        return bad.fillna(False)

    dates = pd.to_datetime(df[dc])
    failed = pd.Series(False, index=df.index)
    grp_col = "ticker" if "ticker" in df.columns else None
    if grp_col:
        for _, group in df.groupby(grp_col):
            gdates = dates.loc[group.index]
            diff = gdates.diff()
            bad = (diff <= pd.Timedelta(0)) & diff.notna()
            failed.loc[group.index] = bad.fillna(False)
    else:
        diff = dates.diff()
        failed = ((diff <= pd.Timedelta(0)) & diff.notna()).reindex(
            df.index, fill_value=False
        )
    return failed


def check_minute_timestamp_order(df: pd.DataFrame, **_) -> pd.Series:
    """T05：同一 ``(ticker, trade_date)`` 内分钟时间戳须严格递增且连续（午休间隔 88–92 分钟属正常）。"""
    ts_col = _dt_col(df)
    if ts_col is None:
        return pd.Series([False] * len(df), index=df.index)
    dc = _date_col(df) or "trade_date"
    ts = pd.to_datetime(df[ts_col])
    diff = ts.groupby([df["ticker"], df[dc]]).diff()
    bad_order = (diff <= pd.Timedelta(0)) & diff.notna()
    # 午休间隔（11:30 -> 13:01）约 91 分钟，容差范围 88–92 分钟
    lunch_break = (diff > pd.Timedelta(minutes=88)) & (diff <= pd.Timedelta(minutes=92))
    gap = (diff > pd.Timedelta(minutes=2)) & ~lunch_break
    return (bad_order | gap).fillna(False)


def check_adjfactor_idempotent(
    df: pd.DataFrame, previous_adjfactor=None, **_
) -> pd.Series:
    """T06：重入库时历史复权因子不得变化。"""
    if previous_adjfactor is None:
        return pd.Series([False], index=["__aggregate__"])
    dc = _date_col(df) or "trade_date"
    merged = df[["ticker", dc, "adjfactor"]].merge(
        previous_adjfactor,
        on=["ticker", dc],
        suffixes=("_new", "_old"),
        how="inner",
    )
    changed = merged["adjfactor_new"].ne(merged["adjfactor_old"]) & merged[
        ["adjfactor_new", "adjfactor_old"]
    ].notna().all(axis=1)
    return pd.Series([bool(changed.any())], index=["__aggregate__"])


def check_factor_lag(
    df: pd.DataFrame,
    max_lag_days: int = 2,
    today=None,
    calendar_dates=None,
    **_,
) -> pd.Series:
    """T07：因子到达延迟（``arrival_date - trade_date``）须 <= *max_lag_days*（有日历时按交易日计）。"""
    if "arrival_date" not in df.columns:
        return pd.Series([False], index=["__aggregate__"])
    dc = _date_col(df) or "trade_date"
    td = pd.to_datetime(df[dc])
    ad = pd.to_datetime(df["arrival_date"])
    if today is not None:
        ref = pd.to_datetime(today)
        mask = td <= ref
        td = td[mask]
        ad = ad[mask]
    if len(td) == 0:
        return pd.Series([False], index=["__aggregate__"])
    if calendar_dates is not None:
        cal = pd.to_datetime(list(calendar_dates)).sort_values()
        lag = pd.Series(
            [
                int(((cal > d) & (cal <= a)).sum()) if pd.notna(a) else 0
                for d, a in zip(td, ad)
            ],
            index=td.index,
        )
    else:
        lag = (ad - td).dt.days
    return pd.Series([bool((lag > max_lag_days).any())], index=["__aggregate__"])


def check_calendar_forward(
    df: pd.DataFrame, min_forward_days: int = 30, today=None, **_
) -> pd.Series:
    """T08：交易日历须包含不少于 *min_forward_days* 个未来交易日。"""
    ref = (
        pd.to_datetime(today) if today is not None else pd.Timestamp.today().normalize()
    )
    dc = _date_col(df) or "trade_date"
    dates = pd.to_datetime(df[dc])
    forward = dates[dates >= ref]
    return pd.Series([forward.nunique() < min_forward_days], index=["__aggregate__"])


def check_late_correction(
    df: pd.DataFrame,
    max_change_rate: float = 0.0001,
    previous_data=None,
    **_,
) -> pd.Series:
    """T09：历史数值字段相对变动超过 *max_change_rate* 时触发告警。"""
    if previous_data is None:
        return pd.Series([False], index=["__aggregate__"])
    key = ["ticker", _date_col(df) or "trade_date"]
    val_cols = [
        c
        for c in df.select_dtypes(include="number").columns
        if c not in key and c in previous_data.columns
    ]
    merged = df[key + val_cols].merge(
        previous_data[key + val_cols], on=key, suffixes=("_new", "_old"), how="inner"
    )
    for col in val_cols:
        old = merged[f"{col}_old"]
        new = merged[f"{col}_new"]
        denom = old.abs().replace(0, 1)
        rate = (new - old).abs() / denom
        if bool((rate > max_change_rate).any()):
            return pd.Series([True], index=["__aggregate__"])
    return pd.Series([False], index=["__aggregate__"])
