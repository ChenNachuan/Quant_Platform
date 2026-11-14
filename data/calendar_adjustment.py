"""Time/price coordinate layer: trading calendar, night-session attribution,
continuous contracts and price adjustment.

Design authority: ``docs/design/data/calendar_adjustment.md`` (§42–§44).

QUANT-41 lands the four time/price-coordinate pieces across two iterations:
iteration 1 — :class:`TradingCalendar` (bisect-based trade-date navigation over
``asset/calendar/{exchange}.parquet``) and :func:`assign_trade_date_futures`
(futures night-session trade-date attribution); iteration 2 —
:func:`build_continuous` (volume-dominant continuous contract with
roll-cumulative adjfactor) and :class:`PriceAccessor` (raw / forward-adjusted /
returns views).  ``get_minute_adj`` is a design-doc signature item outside both
iterations' cut and is not landed here.

Conventions pinned for the whole platform (kb snapshot K1–K6):

* Calendar files hold a single column ``trade_date: date``; non-trading days
  are absent.  A missing file raises :class:`FileNotFoundError` — no silent
  fallback, no holiday table maintained in code.
* Timestamps are exchange-local naive (Asia/Shanghai), never UTC.
* Futures night session: ``hour >= 21`` belongs to the next trade date;
  ``hour < 3`` continues the previous evening onto the current trade date.
  A source-provided ``trade_date`` always wins.
* Adjustment formulas (K4): ``hfq = raw × adj``; ``qfq = raw × adj / adj_latest``.
  The cumulative ``adjfactor`` is anchored at the series' first date (Tushare
  convention, base 1.0).  Returns use
  ``close_T×adj_T / (close_{T-n}×adj_{T-n}) − 1`` — the base cancels, so
  backtest and live compute the same number.
* Continuous contracts (K5): daily dominant by **volume** → roll-day close
  ratio → cumulative ``adjfactor`` → forward-adjusted prices; full daily
  rebuild, never incremental.  The roll ratio uses **close** prices (the
  design lists settle-vs-close as open; this is the documented default).
* Adjusted prices (K6) are derived on read and never persisted — the asset
  layer stores raw + ``adjfactor`` only, zero backfill.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from data.storage_schema import PARTITION_PATHS
from core.config import DATA_ROOT

_DEFAULT_DATA_ROOT = str(DATA_ROOT)


def _as_date(d: date) -> date:
    """Return *d* as a plain ``date`` (``datetime``/``pd.Timestamp`` narrowed).

    ``datetime`` subclasses ``date``, so a Timestamp survives an
    ``isinstance(x, date)`` guard and then compares unequal to every stored
    ``date`` — bisect would silently misplace it.  Narrow once here instead.
    """
    return d.date() if isinstance(d, datetime) else d


class TradingCalendar:
    """Trade-date navigation over one exchange's calendar file.

    ``exchange`` is the storage exchange name — the values of
    :data:`core.asset_rules.EXCHANGE_MAP` (``sse``, ``shfe``, …).  The file
    is ``{data_root}/asset/calendar/{exchange}.parquet`` per
    :data:`data.storage_schema.PARTITION_PATHS`; when it is missing the
    constructor raises :class:`FileNotFoundError` (documented default: no
    silent fallback).  Days are sorted and deduplicated on load; a duplicated
    ``trade_date`` row must not inflate ``range()``/``count()``.  ``is_trading_day``
    is an O(1) set lookup, ``prev``/``next``/``range``/``count`` are O(log n)
    via :mod:`bisect`.
    """

    def __init__(
        self,
        exchange: str,
        data_root: str | Path = _DEFAULT_DATA_ROOT,
    ) -> None:
        rel = PARTITION_PATHS["calendar"].format(exchange=exchange)
        path = Path(data_root) / rel
        if not path.exists():
            legacy = Path(data_root) / "asset" / "calendar" / f"{exchange}.parquet"
            if legacy.exists():
                path = legacy
            else:
                raise FileNotFoundError(f"calendar file not found: {path}")
        trade_date = pd.read_parquet(path)["trade_date"]
        self._days: list[date] = sorted(
            set(pd.to_datetime(trade_date).dt.date.tolist())
        )
        self._set: set[date] = set(self._days)

    def is_trading_day(self, d: date) -> bool:
        """True when *d* is a trading day (O(1) set lookup)."""
        return _as_date(d) in self._set

    def prev(self, d: date, n: int = 1) -> date:
        """The *n*-th trading day strictly before *d*.

        Raises ``IndexError`` when fewer than *n* trading days exist before
        that point (including *d* earlier than the calendar's first day).
        """
        i = bisect_left(self._days, _as_date(d))
        if i - n < 0:
            raise IndexError(f"prev({d}, {n}) out of range: before calendar start")
        return self._days[i - n]

    def next(self, d: date, n: int = 1) -> date:
        """The *n*-th trading day strictly after *d*.

        Raises ``IndexError`` past the calendar's last day — no wall-clock
        fallback; the file carries the official future days (trade_cal
        contract, refreshed monthly).
        """
        i = bisect_right(self._days, _as_date(d))
        if i + n - 1 >= len(self._days):
            raise IndexError(f"next({d}, {n}) out of range: past calendar end")
        return self._days[i + n - 1]

    def range(self, start: date, end: date) -> list[date]:
        """Trading days in ``[start, end]``, both ends inclusive.

        Empty when *start* > *end* or when the span holds no trading day.
        """
        lo = bisect_left(self._days, _as_date(start))
        hi = bisect_right(self._days, _as_date(end))
        return self._days[lo:hi]

    def count(self, start: date, end: date) -> int:
        """Number of trading days in ``[start, end]`` — ``len(range(...))``.

        Bisect only, no list materialisation; 0 when *start* > *end*.
        """
        lo = bisect_left(self._days, _as_date(start))
        hi = bisect_right(self._days, _as_date(end))
        return max(hi - lo, 0)


def assign_trade_date_futures(
    ts: pd.Timestamp,
    cal: TradingCalendar,
    source_trade_date: date | None = None,
) -> date:
    """Attribute a futures bar timestamp to its trade date (kb snapshot K3).

    * ``hour >= 21`` — the evening session belongs to the *next* trade date.
      Friday 21:00 lands on Monday: ``cal.next`` skips the weekend itself.
    * ``hour < 3`` — the previous evening's session continuing past midnight,
      attributed to the *current* trade date.  A Saturday-morning bar follows
      Friday's evening session onto Monday (same fall-through rule).
    * anything else (day session) — the bar date itself when it is a trading
      day, else the next one.
    * *source_trade_date* — a trade date already stamped by the data source is
      trusted as-is (source wins).
    """
    if source_trade_date is not None:
        return _as_date(source_trade_date)
    d = ts.date()
    if ts.hour >= 21:
        return cal.next(d, 1)
    # hour < 3 and the day session share one rule: the current trade date,
    # falling forward only when the bar date is not a trading day.
    return d if cal.is_trading_day(d) else cal.next(d, 1)


_REQUIRED_BARS = ("trade_date", "contract", "close", "volume")
_PASSTHROUGH = (
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "open_interest",
    "settle",
)


def build_continuous(symbol: str, bars: pd.DataFrame) -> pd.DataFrame:
    """Continuous dominant-contract series for one symbol (design §42.4, kb K5).

    Daily pipeline — full rebuild, no incremental state:

    1. dominant contract per trade date = max **volume** (ties broken by the
       smallest contract code, deterministic);
    2. roll dates = dates where the dominant changes; the roll step is
       ``close_old / close_new`` from **both** contracts' closes on the roll
       day itself (close-price convention — the design lists settle-vs-close
       as open; this is the documented default);
    3. ``adjfactor`` = forward cumulative product of the steps, base 1.0 at the
       series' first date (Tushare convention), so ``hfq = raw × adj`` (kb K4)
       is continuous across rolls and anchored at the first real price;
    4. ``close_adj = close × adjfactor / adjfactor_latest`` — forward-adjusted
       (qfq, kb K4).  Derived only: the storage layer drops ``close_adj`` and
       keeps raw + ``adjfactor`` (kb K6, zero backfill).

    Parameters
    ----------
    symbol:
        Variety code (``"RB"``, ``"IF"``, …) kept per the design signature;
        used in error messages — the per-symbol file is the identity.
    bars:
        Per-contract daily rows.  Required columns: ``trade_date``,
        ``contract``, ``close``, ``volume``; optional passthrough:
        ``open``/``high``/``low``/``amount``/``open_interest``/``settle``.
        ``(trade_date, contract)`` must be unique and volume non-null.  On a
        roll day the old contract's row must be present — its close is the
        ratio denominator — missing data raises :class:`ValueError`, with no
        fallback to a stale close.

    Returns one row per trade date, ascending: ``trade_date``,
    ``dominant_contract``, the input's passthrough columns
    (``open``/``high``/``low``/``close``/``volume``/``amount``/
    ``open_interest``/``settle`` — whichever are present), and the derived
    ``adjfactor`` / ``close_adj``.  The output column set mirrors the input,
    not ``CONTINUOUS_SCHEMA``: persisting to ``asset/continuous/`` requires
    the full schema columns and is validated at the storage layer.  On a roll
    day a non-positive dominant (new) close raises; a non-positive old close
    degrades the step to 1.0 (§42.4 guard).
    """
    required = [c for c in _REQUIRED_BARS if c not in bars.columns]
    if required:
        raise ValueError(f"bars[{symbol}] missing required columns: {required}")
    if bars.empty:
        raise ValueError(f"bars[{symbol}] is empty")
    df = bars.copy()
    if df.duplicated(subset=["trade_date", "contract"]).any():
        raise ValueError(f"bars[{symbol}] has duplicate (trade_date, contract) rows")
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # Dominant = max volume per date; ties -> smallest contract code.
    ranked = df.sort_values(
        ["trade_date", "volume", "contract"],
        ascending=[True, False, True],
        kind="stable",
    )
    dom = (
        ranked.groupby("trade_date", sort=True, as_index=False)
        .head(1)
        .sort_values("trade_date", kind="stable")
        .reset_index(drop=True)
    )

    step = pd.Series(1.0, index=dom.index)
    for k in range(1, len(dom)):
        old_c = dom["contract"].iloc[k - 1]
        new_c = dom["contract"].iloc[k]
        if old_c == new_c:
            continue
        t = dom["trade_date"].iloc[k]
        new_close = float(dom["close"].iloc[k])
        if not new_close > 0:
            raise ValueError(
                f"bars[{symbol}]: dominant close {new_close} on {t.date()} "
                f"({new_c}) is not positive"
            )
        old_row = df[(df["trade_date"] == t) & (df["contract"] == old_c)]
        if old_row.empty:
            raise ValueError(
                f"bars[{symbol}]: roll on {t.date()} needs {old_c}'s close that "
                f"day (roll ratio denominator); row missing"
            )
        old_close = float(old_row["close"].iloc[0])
        if old_close > 0:
            # Design §42.4 computes the roll-day ratio as new/old; the
            # cumulative adjfactor that K4 defines (hfq = raw × adj, base =
            # first date) accumulates its inverse so the adjusted series
            # continues the old contract's level across the roll.
            step.iloc[k] = old_close / new_close

    adjfactor = step.cumprod()
    out = pd.DataFrame(
        {
            "trade_date": dom["trade_date"].dt.date,
            "dominant_contract": dom["contract"],
        }
    )
    for col in _PASSTHROUGH:
        if col in dom.columns:
            out[col] = dom[col].to_numpy()
    out["adjfactor"] = adjfactor.to_numpy()
    out["close_adj"] = out["close"] * out["adjfactor"] / float(adjfactor.iloc[-1])
    return out


def _series_key(df: pd.DataFrame) -> str | None:
    """Identity column for per-series adjfactor semantics (§44.4).

    ``ticker`` is the platform identity on daily bars.  A continuous frame
    carries no ticker column — its roll-cumulative ``adjfactor`` spans the
    whole symbol — so the whole frame is one series.
    """
    return "ticker" if "ticker" in df.columns else None


class PriceAccessor:
    """Raw / forward-adjusted / returns views over a raw daily frame (§44.4).

    The frame carries price fields plus the cumulative ``adjfactor``; views
    are derived on read (kb K6 — adjusted prices are never persisted).
    ``adjfactor_latest`` is read on every call — no cache — so a rewritten
    adjfactor column changes the derived prices with zero backfill (the
    documented default for the design's caching open item).

    Rows must be in trade-date order within each series: ``adj_latest`` is the
    last row's factor per series, matching the design sketch and
    ``DataAPI``'s ``arg_max(adjfactor, trade_date)`` over ordered storage.
    """

    def __init__(self, daily_df: pd.DataFrame) -> None:
        self._df = daily_df

    def _adj_latest(self) -> pd.Series | float:
        key = _series_key(self._df)
        if key is None:
            return float(self._df["adjfactor"].iloc[-1])
        return self._df.groupby(key)["adjfactor"].transform("last")

    def raw(self, field: str = "close") -> pd.Series:
        """Unadjusted *field*, as stored."""
        return self._df[field]

    def forward_adj(self, field: str = "close") -> pd.Series:
        """Forward-adjusted (qfq): ``raw × adj / adj_latest`` (kb K4)."""
        return self._df[field] * self._df["adjfactor"] / self._adj_latest()

    def returns(self, n: int = 1) -> pd.Series:
        """n-period return ``close_T×adj_T / (close_{T-n}×adj_{T-n}) − 1``.

        Computed on the forward-adjusted close — ``adj_latest`` cancels in
        the ratio, so the value is base-independent (design §44.5).  The first
        *n* rows of each series are NaN (the design's listing-bounded NaN
        question is open; pandas ``pct_change`` semantics, no fill).
        """
        adj_close = self.forward_adj("close")
        key = _series_key(self._df)
        if key is None:
            return adj_close.pct_change(n)
        return adj_close.groupby(self._df[key]).pct_change(n)
