"""Time/price coordinate layer: trading calendar, night-session attribution,
continuous contracts and price adjustment.

Design authority: ``docs/design/data/calendar_adjustment.md`` (§42–§44).

QUANT-41 iteration 1 lands the two time-coordinate pieces —
:class:`TradingCalendar` (bisect-based trade-date navigation over
``asset/calendar/{exchange}.parquet``) and :func:`assign_trade_date_futures`
(futures night-session trade-date attribution).  ``build_continuous`` and
``PriceAccessor`` land in iteration 2.

Conventions pinned for the whole platform (kb snapshot K1–K3):

* Calendar files hold a single column ``trade_date: date``; non-trading days
  are absent.  A missing file raises :class:`FileNotFoundError` — no silent
  fallback, no holiday table maintained in code.
* Timestamps are exchange-local naive (Asia/Shanghai), never UTC.
* Futures night session: ``hour >= 21`` belongs to the next trade date;
  ``hour < 3`` continues the previous evening onto the current trade date.
  A source-provided ``trade_date`` always wins.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from data.storage_schema import PARTITION_PATHS

_DEFAULT_DATA_ROOT = "data_platform"


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
    silent fallback).  Days are sorted on load; ``is_trading_day`` is an
    O(1) set lookup, ``prev``/``next``/``range``/``count`` are O(log n) via
    :mod:`bisect`.
    """

    def __init__(
        self,
        exchange: str,
        data_root: str | Path = _DEFAULT_DATA_ROOT,
    ) -> None:
        rel = PARTITION_PATHS["calendar"].format(exchange=exchange)
        path = Path(data_root) / rel
        if not path.exists():
            raise FileNotFoundError(f"calendar file not found: {path}")
        trade_date = pd.read_parquet(path)["trade_date"]
        self._days: list[date] = sorted(pd.to_datetime(trade_date).dt.date.tolist())
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
