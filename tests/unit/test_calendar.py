"""Unit tests for data.calendar_adjustment (QUANT-41, S10-T2).

Groups per the issue deliverables — calendar navigation, night-session
attribution, adjustment formulas.  Iteration 1 covers the first two (AC1–AC3:
``TradingCalendar`` navigation + IndexError bounds, ``assign_trade_date_futures``
night-session rules); iteration 2 lands the adjustment group (AC4:
``build_continuous`` / ``PriceAccessor`` hand-calc cross-check).

Fixture calendar (one weekend gap, no holiday tables):
2024-01-02..05 (Tue..Fri), 2024-01-08..09 (Mon..Tue).

Run: ``uv run pytest tests/unit/test_calendar.py -q``
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data.calendar_adjustment import (
    PriceAccessor,
    TradingCalendar,
    assign_trade_date_futures,
    build_continuous,
)

EXCHANGE = "sse"

DAYS = [
    date(2024, 1, 2),
    date(2024, 1, 3),
    date(2024, 1, 4),
    date(2024, 1, 5),
    date(2024, 1, 8),
    date(2024, 1, 9),
]


# ---------------------------------------------------------------------------
# Calendar fixture — one exchange file under a tmp data root
# ---------------------------------------------------------------------------
def _write_calendar(root: Path, days: list[date], exchange: str = EXCHANGE) -> Path:
    path = root / "asset" / "calendar" / f"{exchange}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    # date objects -> parquet date32, the CALENDAR_SCHEMA trade_date dtype.
    pd.DataFrame({"trade_date": days}).to_parquet(path)
    return path


@pytest.fixture()
def cal(tmp_path: Path) -> TradingCalendar:
    _write_calendar(tmp_path, DAYS)
    return TradingCalendar(EXCHANGE, data_root=tmp_path)


# ---------------------------------------------------------------------------
# AC1 — is_trading_day / prev / next / range / count correctness
# ---------------------------------------------------------------------------
def test_is_trading_day(cal: TradingCalendar) -> None:
    assert cal.is_trading_day(date(2024, 1, 2)) is True
    assert cal.is_trading_day(date(2024, 1, 5)) is True
    assert cal.is_trading_day(date(2024, 1, 6)) is False  # Saturday
    assert cal.is_trading_day(date(2024, 1, 7)) is False  # Sunday
    assert cal.is_trading_day(date(2024, 1, 1)) is False  # before span


def test_prev(cal: TradingCalendar) -> None:
    assert cal.prev(date(2024, 1, 3)) == date(2024, 1, 2)  # one back
    assert cal.prev(date(2024, 1, 8), 2) == date(2024, 1, 4)  # across weekend
    assert cal.prev(date(2024, 1, 6)) == date(2024, 1, 5)  # from a weekend day


def test_next(cal: TradingCalendar) -> None:
    assert cal.next(date(2024, 1, 2)) == date(2024, 1, 3)  # one forward
    assert cal.next(date(2024, 1, 5)) == date(2024, 1, 8)  # Fri -> Mon
    assert cal.next(date(2024, 1, 5), 2) == date(2024, 1, 9)  # across weekend
    assert cal.next(date(2024, 1, 6)) == date(2024, 1, 8)  # from a weekend day


def test_range(cal: TradingCalendar) -> None:
    assert cal.range(date(2024, 1, 3), date(2024, 1, 8)) == DAYS[1:5]
    assert cal.range(date(2024, 1, 2), date(2024, 1, 2)) == [date(2024, 1, 2)]
    assert cal.range(date(2024, 1, 6), date(2024, 1, 7)) == []  # weekend only
    assert cal.range(date(2024, 1, 8), date(2024, 1, 3)) == []  # start > end


def test_count(cal: TradingCalendar) -> None:
    assert cal.count(date(2024, 1, 3), date(2024, 1, 8)) == 4
    assert cal.count(date(2024, 1, 2), date(2024, 1, 2)) == 1
    assert cal.count(date(2024, 1, 2), date(2024, 1, 9)) == len(DAYS)
    assert cal.count(date(2024, 1, 8), date(2024, 1, 3)) == 0  # start > end
    # count == len(range) across the weekend gap
    assert cal.count(date(2024, 1, 5), date(2024, 1, 8)) == len(
        cal.range(date(2024, 1, 5), date(2024, 1, 8))
    )


# ---------------------------------------------------------------------------
# AC2 — out-of-range navigation raises IndexError
# ---------------------------------------------------------------------------
def test_prev_out_of_range_raises(cal: TradingCalendar) -> None:
    with pytest.raises(IndexError):
        cal.prev(DAYS[0], 1)  # before calendar start
    with pytest.raises(IndexError):
        cal.prev(DAYS[1], 5)  # more steps than history


def test_next_out_of_range_raises(cal: TradingCalendar) -> None:
    with pytest.raises(IndexError):
        cal.next(DAYS[-1], 1)  # past calendar end
    with pytest.raises(IndexError):
        cal.next(DAYS[-2], 3)  # more steps than remaining


# ---------------------------------------------------------------------------
# AC3 — night-session trade-date attribution (two cases)
# ---------------------------------------------------------------------------
def test_night_session_hour_21_maps_to_next_trade_date(cal: TradingCalendar) -> None:
    # Thursday 21:00 -> Friday
    assert assign_trade_date_futures(pd.Timestamp("2024-01-04 21:00"), cal) == date(
        2024, 1, 5
    )
    # Friday 21:30 -> Monday (weekend skipped by cal.next)
    assert assign_trade_date_futures(pd.Timestamp("2024-01-05 21:30"), cal) == date(
        2024, 1, 8
    )


def test_early_morning_maps_to_current_trade_date(cal: TradingCalendar) -> None:
    # Friday 01:00 = Thursday evening's session past midnight -> Friday
    assert assign_trade_date_futures(pd.Timestamp("2024-01-05 01:00"), cal) == date(
        2024, 1, 5
    )
    # Saturday 01:30 = Friday evening's session past midnight -> Monday
    assert assign_trade_date_futures(pd.Timestamp("2024-01-06 01:30"), cal) == date(
        2024, 1, 8
    )


def test_source_trade_date_wins(cal: TradingCalendar) -> None:
    # Source-stamped trade_date overrides the 21:00 rule (kb snapshot K3).
    assert assign_trade_date_futures(
        pd.Timestamp("2024-01-05 21:00"), cal, source_trade_date=date(2024, 1, 5)
    ) == date(2024, 1, 5)


# ---------------------------------------------------------------------------
# Calendar-file contract — missing file raises FileNotFoundError (no fallback)
# ---------------------------------------------------------------------------
def test_missing_calendar_file_raises_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        TradingCalendar(EXCHANGE, data_root=tmp_path)


def test_calendar_duplicate_trade_dates_deduped(tmp_path: Path) -> None:
    # Review (PR 68): a duplicated trade_date row must not inflate
    # range()/count() — days are sorted(set(...)) on load.
    _write_calendar(tmp_path, [date(2024, 1, 2), date(2024, 1, 2), date(2024, 1, 3)])
    c = TradingCalendar(EXCHANGE, data_root=tmp_path)
    assert c.count(date(2024, 1, 2), date(2024, 1, 3)) == 2
    assert c.range(date(2024, 1, 2), date(2024, 1, 3)) == [
        date(2024, 1, 2),
        date(2024, 1, 3),
    ]


# ---------------------------------------------------------------------------
# AC4–AC5 — continuous roll + adjustment formulas, hand-calc cross-check
# ---------------------------------------------------------------------------
# Hand-calc fixture ("RB", two contracts, one roll):
#
#   trade_date  contract  close  volume
#   2024-01-02  RB2405      80     500   <- dominant (500 > 100)
#   2024-01-02  RB2410     100     100
#   2024-01-03  RB2405      82     480   <- dominant; real +2.5% old-contract move
#   2024-01-03  RB2410     100     120
#   2024-01-04  RB2405      80     100   <- roll day; old contract still bars
#   2024-01-04  RB2410     100     400   <- dominant switches (400 > 100)
#   2024-01-05  RB2410     105     420
#
# Roll at 2024-01-04 uses the roll-DAY closes: step = 80/100 = 0.8.  The old
# contract's previous close (82) must not enter — a wrong prev-day step (0.82)
# fails the hfq hand values below.
#   adjfactor (base 1.0, forward cumprod):  [1.0, 1.0, 0.8, 0.8]
#   hfq = raw × adj (K4):  [80, 82, 100*0.8, 105*0.8] = [80, 82, 80, 84]
#     — continuous across the roll (82 -> 80 is the old contract's real move).
#   qfq = raw × adj / adj_latest, adj_latest = 0.8 (K4):
#     [80/0.8, 82/0.8, 80/0.8, 84/0.8] = [100, 102.5, 100, 105]
#     — latest qfq equals latest raw, as K4 requires.
#   returns(n) = close_T×adj_T / (close_{T-n}×adj_{T-n}) − 1 (K4):
#     n=1: [NaN, 82/80−1, 80/82−1, 84/80−1] = [NaN, 0.025, −0.0243…, 0.05]
#     n=2: [NaN, NaN, 80/80−1, 84/82−1]   = [NaN, NaN, 0.0, 0.0243…]
CONT_DATES = [date(2024, 1, 2), date(2024, 1, 3), date(2024, 1, 4), date(2024, 1, 5)]

BARS = pd.DataFrame(
    {
        "trade_date": [
            date(2024, 1, 2),
            date(2024, 1, 2),
            date(2024, 1, 3),
            date(2024, 1, 3),
            date(2024, 1, 4),
            date(2024, 1, 4),
            date(2024, 1, 5),
        ],
        "contract": [
            "RB2405",
            "RB2410",
            "RB2405",
            "RB2410",
            "RB2405",
            "RB2410",
            "RB2410",
        ],
        "close": [80.0, 100.0, 82.0, 100.0, 80.0, 100.0, 105.0],
        "volume": [500, 100, 480, 120, 100, 400, 420],
    }
)


@pytest.fixture()
def cont() -> pd.DataFrame:
    return build_continuous("RB", BARS)


def test_build_continuous_dominant_by_volume(cont: pd.DataFrame) -> None:
    assert list(cont["trade_date"]) == CONT_DATES  # one row per date, ascending
    assert list(cont["dominant_contract"]) == ["RB2405", "RB2405", "RB2410", "RB2410"]


def test_build_continuous_adjustment_hand_calc(cont: pd.DataFrame) -> None:
    # adjfactor: forward cumprod of roll steps, base 1.0 at the first date.
    assert cont["adjfactor"].tolist() == pytest.approx([1.0, 1.0, 0.8, 0.8])
    # hfq = raw × adj (AC4 hand calc) — jump-free across the contract switch.
    hfq = cont["close"] * cont["adjfactor"]
    assert hfq.tolist() == pytest.approx([80.0, 82.0, 80.0, 84.0])


def test_price_accessor_forward_adj_hand_calc(cont: pd.DataFrame) -> None:
    pa = PriceAccessor(cont)
    # qfq = raw × adj / adj_latest, adj_latest = 0.8 (AC4 hand calc).
    assert pa.forward_adj().tolist() == pytest.approx([100.0, 102.5, 100.0, 105.0])
    # build_continuous's close_adj column is the same derived view.
    assert cont["close_adj"].tolist() == pytest.approx([100.0, 102.5, 100.0, 105.0])
    # raw() is the unadjusted dominant close series.
    assert pa.raw().tolist() == pytest.approx([80.0, 82.0, 100.0, 105.0])


def test_price_accessor_returns_hand_calc(cont: pd.DataFrame) -> None:
    pa = PriceAccessor(cont)
    r1 = pa.returns(1)
    assert pd.isna(r1.iloc[0])
    assert r1.iloc[1] == pytest.approx(0.025)  # 82/80 − 1
    assert r1.iloc[2] == pytest.approx(80 / 82 - 1)  # base cancels (K4)
    assert r1.iloc[3] == pytest.approx(0.05)  # 84/80 − 1
    r2 = pa.returns(2)
    assert pd.isna(r2.iloc[0]) and pd.isna(r2.iloc[1])
    assert r2.iloc[2] == pytest.approx(0.0)  # 80/80 − 1
    assert r2.iloc[3] == pytest.approx(84 / 82 - 1)


def test_price_accessor_multi_ticker_groups_by_ticker() -> None:
    # §44.4 equity frame: adj_latest is per ticker (2.0 and 5.0).
    df = pd.DataFrame(
        {
            "ticker": ["600519.SH", "600519.SH", "000001.SZ", "000001.SZ"],
            "close": [100.0, 110.0, 20.0, 18.0],
            "adjfactor": [1.0, 2.0, 5.0, 5.0],
        }
    )
    pa = PriceAccessor(df)
    assert pa.forward_adj().tolist() == pytest.approx([50.0, 110.0, 20.0, 18.0])
    r = pa.returns(1)
    assert pd.isna(r.iloc[0]) and pd.isna(r.iloc[2])  # first row per ticker
    assert r.iloc[1] == pytest.approx(110.0 / 50.0 - 1)  # within 600519
    assert r.iloc[3] == pytest.approx(18.0 / 20.0 - 1)  # within 000001


def test_build_continuous_roll_day_missing_old_contract_raises() -> None:
    # The roll ratio denominator is the old contract's close ON the roll day;
    # a missing row raises rather than falling back to a stale close.
    bars = BARS[
        ~((BARS["trade_date"] == date(2024, 1, 4)) & (BARS["contract"] == "RB2405"))
    ]
    with pytest.raises(ValueError, match="roll"):
        build_continuous("RB", bars)


def test_build_continuous_rejects_bad_input() -> None:
    with pytest.raises(ValueError, match="duplicate"):
        build_continuous("RB", pd.concat([BARS, BARS.iloc[[0]]], ignore_index=True))
    with pytest.raises(ValueError, match="missing required columns"):
        build_continuous("RB", BARS.drop(columns=["volume"]))
    with pytest.raises(ValueError, match="empty"):
        build_continuous("RB", BARS.iloc[0:0])
