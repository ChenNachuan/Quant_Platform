"""Gap #10 data-fetching contract for the execution domain.

Design authority: ``docs/design/execution/order_generation.md`` (§66) and the
contract settled in QUANT-27 (pinned in issue metadata ``kb_snapshot``).
Three entry points shared by order generation now and the broker (S7-T4)
later — defined once here; consumers import, never re-derive.

Rules (kb_snapshot):

1. *bars* is a ``load_bars`` long table (``date`` / ``ticker`` / OHLCV).
   Pure functions: no IO, no module state, no caching.
2. ``asof`` is strict trade-date equality — **no last-known fallback**.  A
   suspended day returns ``None`` / is omitted; the order path then skips
   the ticker and the broker REJECTs.
3. Missing / NaN / non-positive prices return ``None`` (keys are omitted)
   — NaN-safe refusal to report a price, never a fabricated one.
4. Prices are **raw and unadjusted** (tradeable).  Adjustment belongs
   upstream; never adjust here.
5. ``field`` is ``"close"`` or ``"open"`` only.  T3 sizes and limits on the
   decision-day close; T4 fills on T+1 open via ``field="open"`` (gap #5
   ruling B) — no second fetch, no second copy of this code.
6. ``get_daily_volume`` is that day's **shares** (``bars_daily.volume``,
   int64 shares).  Liquidity / rule-7 checks compare order shares against
   ``max_volume_share * volume``.
7. A duplicate ``(ticker, date)`` row raises ``ValueError`` — a frame with
   two rows for one day has no single price.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from typing import Sequence

import pandas as pd

PRICE_FIELDS = ("close", "open")


def get_latest_price(
    bars: pd.DataFrame,
    ticker: str,
    asof: date | str,
    field: str = "close",
) -> float | None:
    """Raw *field* price of *ticker* on *asof*, or ``None``.

    Strict trade-date equality (rule 2): a day without a row is a suspended
    day and reports no price — the caller skips the ticker rather than
    pricing it off a stale close.  A NaN or non-positive price reports
    ``None`` too (rule 3).  Raises ``ValueError`` for an unknown *field*
    (rule 5), a *bars* frame missing its contract columns (rule 1), or
    duplicate ``(ticker, date)`` rows (rule 7).
    """
    _check_field(field)
    _require_columns(bars, ("date", "ticker", field))
    rows = _day_rows(bars, ticker, asof)
    if rows.empty:
        return None
    return _positive_float(rows.iloc[0][field])


def get_prices(
    bars: pd.DataFrame,
    tickers: Sequence[str],
    asof: date | str,
    field: str = "close",
) -> dict[str, float]:
    """Map of ticker -> raw *field* price on *asof*.

    Keys with no valid price are **omitted** (rules 2/3) — the dict holds
    only prices a caller may trade against.  A bare string is refused: it
    would iterate as characters instead of one ticker.
    """
    if isinstance(tickers, (str, bytes)):
        raise TypeError(
            f"tickers must be a sequence of tickers, not a bare string — "
            f"got {tickers!r}"
        )
    out: dict[str, float] = {}
    for ticker in tickers:
        price = get_latest_price(bars, ticker, asof, field=field)
        if price is not None:
            out[ticker] = price
    return out


def get_daily_volume(
    bars: pd.DataFrame,
    ticker: str,
    asof: date | str,
) -> float | None:
    """That day's traded **shares** for *ticker*, or ``None``.

    Rule 6: the unit is shares (``bars_daily.volume``), not lots or notional.
    A missing row (suspended), a NaN volume or a negative volume reports
    ``None`` — refusing to make up liquidity.  A genuine zero-volume day
    reports ``0.0``: that is data, not absence of data.  Raises ``ValueError``
    for a *bars* frame without the contract columns or duplicate rows.
    """
    _require_columns(bars, ("date", "ticker", "volume"))
    rows = _day_rows(bars, ticker, asof)
    if rows.empty:
        return None
    raw = rows.iloc[0]["volume"]
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value < 0:
        return None
    return value


# ---------------------------------------------------------------------------
# internals
# ---------------------------------------------------------------------------


def _check_field(field: str) -> None:
    if field not in PRICE_FIELDS:
        raise ValueError(f"field must be one of {PRICE_FIELDS}, got {field!r}")


def _require_columns(bars: pd.DataFrame, columns: Sequence[str]) -> None:
    if not isinstance(bars, pd.DataFrame):
        raise TypeError(f"bars must be a pandas DataFrame, got {type(bars).__name__}")
    missing = [c for c in columns if c not in bars.columns]
    if missing:
        raise ValueError(f"bars must carry columns {list(columns)}, missing {missing}")


def _as_date(asof: date | str) -> date:
    if isinstance(asof, datetime):
        return asof.date()
    if isinstance(asof, date):
        return asof
    if isinstance(asof, str):
        try:
            return pd.Timestamp(asof).date()
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"asof must be a date or date string, got {asof!r}"
            ) from exc
    raise ValueError(f"asof must be a date or date string, got {asof!r}")


def _day_rows(bars: pd.DataFrame, ticker: str, asof: date | str) -> pd.DataFrame:
    """Rows of *bars* for (*ticker*, *asof*) — exact day, no fallback (rule 2).

    Duplicate ``(ticker, date)`` rows raise (rule 7) before any value is
    read: the caller must never have to pick between two closes.
    """
    day = _as_date(asof)
    subset = bars.loc[bars["ticker"] == ticker]
    if subset.empty:
        return subset
    days = pd.to_datetime(subset["date"]).dt.date
    rows = subset.loc[days == day]
    if len(rows) > 1:
        raise ValueError(
            f"duplicate (ticker, date) rows for ({ticker!r}, {day.isoformat()}) "
            f"— a frame with two rows for one day has no single price"
        )
    return rows


def _positive_float(value: object) -> float | None:
    """Report only a strictly positive finite float (NaN-safe, rule 3)."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not (number > 0) or not math.isfinite(number):
        return None
    return number
