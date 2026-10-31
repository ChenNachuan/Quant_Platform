"""DQC custom check functions for rules that cannot be declarative column checks.

All check functions are pure: they take a DataFrame plus keyword params (from
YAML) and return ``pd.Series[bool]`` where **True = failure**.

Return shapes (CheckResult candidate for dqc_runtime.md):
- Row-level checks: Series indexed like the input DataFrame.
- Aggregate checks: Series of length 1 (single verdict).
- Cross-table checks (cross_checks.py): Series indexed by (ticker, trade_date).

External reference data (calendar, universe, corporate actions, etc.) is
injected at call time, never read from disk inside these functions.

Column names follow ``data/storage_schema.py``.  Where a check historically
used a different name (``factor_value`` vs ``value``, ``timestamp`` vs
``datetime``, ``limit_up`` vs ``is_limit_up``), the canonical schema name is
tried first with a legacy fallback so both DQC-frame spellings bind.
"""

from __future__ import annotations

import pandas as pd


# ── Helpers ────────────────────────────────────────────────────────────


def _col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Return the first column name from *candidates* present in *df*."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _value_col(df: pd.DataFrame) -> str | None:
    """Factor value column: schema name ``value``, legacy ``factor_value``."""
    return _col(df, "value", "factor_value")


def _dt_col(df: pd.DataFrame) -> str | None:
    """Minute bar timestamp column: schema name ``datetime``, legacy ``timestamp``."""
    return _col(df, "datetime", "timestamp")


def _date_col(df: pd.DataFrame) -> str | None:
    """Date column: ``trade_date`` (DQC contract), legacy ``date`` (factor schema)."""
    return _col(df, "trade_date", "date")


def _prev_close(df: pd.DataFrame) -> pd.Series:
    """Return ``prev_close`` if present, else derive from prior-day close."""
    if "prev_close" in df.columns:
        return df["prev_close"]
    sorted_df = df.sort_values(["ticker", "trade_date"])
    prev = sorted_df.groupby("ticker")["close"].shift(1)
    return prev.reindex(df.index)


def _bool_series(s: pd.Series) -> pd.Series:
    """Coerce a flag column to bool, treating NaN as False."""
    return s.fillna(False).astype(bool)


def _is_suspended(df: pd.DataFrame) -> pd.Series:
    """Return a bool Series of suspended rows (all-False when no suspend col)."""
    c = _col(df, "suspend")
    if c is None:
        return pd.Series(False, index=df.index)
    return _bool_series(df[c])


# ── Completeness C01–C10 ──────────────────────────────────────────────


def check_row_count_vs_calendar(
    df: pd.DataFrame, calendar_dates=None, comparison: str = "eq", **_
) -> pd.Series:
    """C01: deduplicated trade_date count must match calendar trading days.

    When *calendar_dates* is not injected the check fails (missing dependency),
    and the expected count is restricted to the frame's date range.
    """
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
    """C02: single-day ticker count >= universe * min_ratio."""
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
    """C03: every ticker every day must have exactly *expected_bars* bars."""
    counts = df.groupby(["ticker", _date_col(df) or "trade_date"]).size()
    has_bad = bool((counts != expected_bars).any())
    return pd.Series([has_bad], index=["__aggregate__"])


def check_null_soft_fields(
    df: pd.DataFrame, max_null_rate: float = 0.01, columns=None, **_
) -> pd.Series:
    """C05: null rate of *columns* must be <= max_null_rate.

    Columns absent from the frame are silently skipped (the same C05 rule
    covers both daily and fundamental frames whose column sets differ).
    """
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
    """C06: ticker x date adjfactor missing rate must be 0 %."""
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
    """C07: calendar must cover [today, today+forward_days] trading days without gaps.

    *max_gap_days* accommodates A-share holidays (Spring Festival ~11 calendar
    days).  Gaps beyond this threshold indicate genuinely missing sessions.
    """
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
    """C08: rolling *window*-day missing dates <= *max_missing*.

    The window is anchored at the frame's latest date.  When *calendar_dates*
    is provided, counts truly absent sessions within the last *window*
    calendar dates up to the frame's max date.
    """
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
    """C09: factor ticker coverage >= universe * min_ratio."""
    if universe is None or len(universe) == 0:
        return pd.Series([True], index=["__aggregate__"])
    coverage = df["ticker"].nunique() / len(universe)
    return pd.Series([coverage < min_ratio], index=["__aggregate__"])


def check_industry_coverage(
    df: pd.DataFrame, universe=None, min_ratio: float = 1.0, **_
) -> pd.Series:
    """C10: active ticker industry coverage must be *min_ratio* (default 100 %)."""
    if universe is None or len(universe) == 0:
        return pd.Series([True], index=["__aggregate__"])
    covered = set(df["ticker"].unique()) if "ticker" in df.columns else set()
    ratio = len(covered & set(universe)) / len(universe)
    return pd.Series([ratio < min_ratio], index=["__aggregate__"])


# ── Legality L03–L05, L12 ─────────────────────────────────────────────


def check_ohlc_relation(df: pd.DataFrame, tolerance: float = 0.0, **_) -> pd.Series:
    """L03: low <= min(open, close) and max(open, close) <= high (inclusive)."""
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
    """L04: daily price change must stay within board limit + tolerance.

    Board type is derived from the ticker prefix when dedicated flag columns
    are absent: 30xxxx.SZ / 68xxxx.SH are ChiNext / STAR (20 % limit).
    """
    pc = _prev_close(df)
    if pc.isna().all():
        return pd.Series([False] * len(df), index=df.index)
    change = ((df["close"] - pc) / pc).abs()
    limit = pd.Series(main_limit, index=df.index)

    # Derive board from ticker prefix when flags are not in the frame.
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
    """L05: futures price change must stay within contract limit + tolerance."""
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
    """L12: factor NaN rate must be <= max_nan_rate."""
    vc = _value_col(df)
    if vc is None:
        return pd.Series([False], index=["__aggregate__"])
    rate = df[vc].isna().mean()
    return pd.Series([rate > max_nan_rate], index=["__aggregate__"])


# ── Consistency K01–K05, K09–K11 ─────────────────────────────────────


def check_open_gap(
    df: pd.DataFrame, max_gap: float = 0.10, ca_dates=None, **_
) -> pd.Series:
    """K01: non-ex-right-day adjusted open / prev_close gap <= max_gap."""
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
    """K02: volume > 0 iff amount > 0 (bidirectional)."""
    vol_pos = df["volume"] > 0
    amt_pos = df["amount"] > 0
    return vol_pos != amt_pos


def check_suspend_consistency(df: pd.DataFrame, **_) -> pd.Series:
    """K03: suspended rows must have volume == 0 and OHLC all equal."""
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
    """K04: limit_up => close ≈ high; limit_down => close ≈ low (within tolerance)."""
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
    """K05: adjfactor unchanged on non-corporate-action days."""
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
    """K09: cross-sectional std > min_std and abs(mean) < max_abs_mean."""
    vc = _value_col(df)
    if vc is None:
        return pd.Series([False], index=["__aggregate__"])
    dc = _date_col(df) or "trade_date"
    grouped = df.groupby(dc)[vc]
    bad_std = (grouped.std() <= min_std).any()
    bad_mean = (grouped.mean().abs() >= max_abs_mean).any()
    return pd.Series([bool(bad_std or bad_mean)], index=["__aggregate__"])


def check_industry_universe_match(df: pd.DataFrame, universe=None, **_) -> pd.Series:
    """K10: universe set must equal industry set."""
    if universe is None:
        return pd.Series([True], index=["__aggregate__"])
    industry_set = set(df["ticker"].unique()) if "ticker" in df.columns else set()
    universe_set = set(universe)
    return pd.Series([industry_set != universe_set], index=["__aggregate__"])


def check_fund_vs_daily_tickers(
    df: pd.DataFrame, daily_tickers=None, max_diff: int = 5, **_
) -> pd.Series:
    """K11: set difference between fundamental and daily tickers <= max_diff."""
    if daily_tickers is None:
        return pd.Series([True], index=["__aggregate__"])
    fund_set = set(df["ticker"].unique()) if "ticker" in df.columns else set()
    diff = fund_set.symmetric_difference(set(daily_tickers))
    return pd.Series([len(diff) > max_diff], index=["__aggregate__"])


# ── Timeliness T01–T09 ────────────────────────────────────────────────


def check_daily_freshness(
    df: pd.DataFrame,
    max_stale_days: int = 1,
    today=None,
    calendar_dates=None,
    **_,
) -> pd.Series:
    """T01: latest trade_date within max_stale_days trading days of *today*.

    Trading-day distance is computed against the injected *calendar_dates*
    when available; falls back to business-day count otherwise.
    """
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
    """T02: same as T01 but for minute data."""
    return check_daily_freshness(
        df, max_stale_days=max_stale_days, today=today, calendar_dates=calendar_dates
    )


def check_no_future_dates(df: pd.DataFrame, today=None, **_) -> pd.Series:
    """T03: all trade_date <= today."""
    ref = (
        pd.to_datetime(today) if today is not None else pd.Timestamp.today().normalize()
    )
    dc = _date_col(df) or "trade_date"
    dates = pd.to_datetime(df[dc])
    has_future = bool((dates > ref).any())
    return pd.Series([has_future], index=["__aggregate__"])


def check_date_ascending(df: pd.DataFrame, **_) -> pd.Series:
    """T04: trade_date strictly ascending with no duplicates within partition.

    For minute frames (240 rows per ticker x trade_date) the partition key is
    ``(ticker, trade_date)`` and ordering is validated on the ``datetime``
    column; for other frames it partitions by ``ticker`` on the date column.
    """
    dc = _date_col(df) or "trade_date"
    ts_col = _dt_col(df)
    if ts_col is not None and "ticker" in df.columns:
        # Minute-frame: check that timestamps within (ticker, trade_date) are
        # strictly ascending (no duplicates or out-of-order bars).
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
    """T05: within ticker x date, timestamps strictly ascending and continuous."""
    ts_col = _dt_col(df)
    if ts_col is None:
        return pd.Series([False] * len(df), index=df.index)
    dc = _date_col(df) or "trade_date"
    ts = pd.to_datetime(df[ts_col])
    diff = ts.groupby([df["ticker"], df[dc]]).diff()
    bad_order = (diff <= pd.Timedelta(0)) & diff.notna()
    # Lunch break (11:30 → 13:01 bar-end) is 91 minutes; accept 88-92.
    lunch_break = (diff > pd.Timedelta(minutes=88)) & (diff <= pd.Timedelta(minutes=92))
    gap = (diff > pd.Timedelta(minutes=2)) & ~lunch_break
    return (bad_order | gap).fillna(False)


def check_adjfactor_idempotent(
    df: pd.DataFrame, previous_adjfactor=None, **_
) -> pd.Series:
    """T06: re-ingestion adjfactor must be unchanged."""
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
    """T07: D-day factor data must arrive by D+2 (trading days when calendar available)."""
    if "arrival_date" not in df.columns:
        return pd.Series([False], index=["__aggregate__"])
    dc = _date_col(df) or "trade_date"
    td = pd.to_datetime(df[dc])
    ad = pd.to_datetime(df["arrival_date"])
    # Filter to rows at or before *today*.
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
    """T08: calendar must extend >= min_forward_days trading days from today."""
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
    """T09: historical change > max_change_rate triggers alert."""
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
