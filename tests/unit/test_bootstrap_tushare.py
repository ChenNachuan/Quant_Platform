"""Offline tests for the tushare-path fetchers (QUANT-35).

``fetch_bars_daily.build_frame`` / ``fetch_fundamental.build_frame`` need
network; everything here drives them from a fake ``pro`` whose column shapes
and units are the measured tushare ones (``task/probe_tushare_quant35*.log``).
The load targets are the real fetchers (PAS-849 — no copy beside this test).

Three regressions these pin, each of which previously landed silently or
loudly at ``align_schema``:

* two ``ticker`` columns (``out["ticker"] = ...`` before a ``ts_code``→
  ``ticker`` rename) — ``ValueError: Cannot set a DataFrame with multiple
  columns to the single column ticker``
* ``adj_factor`` never renamed to ``adjfactor`` (the rename fired before the
  merge created the column) — required non-nullable column missing
* ``_turnover`` keyed ``circ_mv`` on tushare's ``YYYYMMDD`` int against
  ``datetime.date`` bar dates — every lookup missed and turnover was all-NaN
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

from data.storage_schema import SCHEMA

_BOOTSTRAP = Path(__file__).resolve().parents[2] / "scripts" / "bootstrap"
if str(_BOOTSTRAP) not in sys.path:
    sys.path.insert(0, str(_BOOTSTRAP))


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _BOOTSTRAP / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


fetch_bars = _load("fetch_bars_daily")
fetch_fund = _load("fetch_fundamental")
#: The same ``_common`` the fetchers bound at import — not a second copy.
import _common  # noqa: E402

TS = "510300.SH"
DAYS = ["20250115", "20250116"]
START, END = "20250115", "20250116"

#: tushare ``daily``/``fund_daily``: volume is 手, amount is 千元, trade_date YYYYMMDD.
#: tushare ``daily_basic``: circ_mv is 万元, trade_date YYYYMMDD **int**.
_VOL_HAND = 12345.0
_AMOUNT_QIAN = 4876.275
_CIRC_MV_WAN = 200000.0
#: amount 元 / (circ_mv 万元 x 1e4) = 4_876_275 / 2_000_000_000
EXPECTED_TURNOVER = (_AMOUNT_QIAN * 1000.0) / (_CIRC_MV_WAN * 1e4)


class FakePro:
    """tushare pro surface the fetchers touch; anything else returns None."""

    def fund_daily(self, ts_code, start_date, end_date):
        return pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": d,
                    "open": 3.9,
                    "high": 4.0,
                    "low": 3.8,
                    "close": 3.95,
                    "vol": _VOL_HAND,
                    "amount": _AMOUNT_QIAN,
                }
                for d in DAYS
            ]
        )

    def fund_adj(self, ts_code, start_date, end_date):
        return pd.DataFrame(
            [{"ts_code": ts_code, "trade_date": d, "adj_factor": 1.25} for d in DAYS]
        )

    def daily_basic(self, ts_code, start_date, end_date):
        return pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": int(d),
                    "circ_mv": _CIRC_MV_WAN,
                    "total_mv": 300000.0,
                    "total_share": 50000.0,
                    "float_share": 40000.0,
                    "pe_ttm": 15.0,
                    "pb": 1.5,
                    "ps_ttm": 2.0,
                }
                for d in DAYS
            ]
        )

    def __getattr__(self, name):
        def _unavailable(**kwargs):
            return None

        return _unavailable


@pytest.fixture(scope="module")
def pro() -> FakePro:
    return FakePro()


@pytest.fixture(scope="module")
def bars_frame(pro) -> pd.DataFrame:
    frame, _ = fetch_bars.build_frame(pro, TS, START, END)
    return frame


class TestBarsDailyFrame:
    def test_exactly_one_ticker_column(self, bars_frame):
        # Duplicate `ticker` is what made align_schema raise.
        assert list(bars_frame.columns).count("ticker") == 1
        assert "ts_code" not in bars_frame.columns
        assert bars_frame["ticker"].tolist() == [TS, TS]

    def test_adjfactor_is_the_merged_column_name(self, bars_frame):
        # _RENAME fired before the merge created adj_factor, so the frame kept
        # the tushare name and align_schema refused the missing adjfactor.
        assert "adjfactor" in bars_frame.columns
        assert "adj_factor" not in bars_frame.columns
        assert bars_frame["adjfactor"].tolist() == [1.25, 1.25]

    def test_turnover_uses_the_measured_market_cap(self, bars_frame):
        # Circ_mv is keyed on tushare's YYYYMMDD int while bars carry
        # datetime.date; without the conversion every lookup missed and
        # turnover was silently all-NaN.
        assert not bars_frame["turnover"].isna().any()
        assert bars_frame["turnover"].tolist() == pytest.approx(
            [EXPECTED_TURNOVER, EXPECTED_TURNOVER]
        )

    def test_schema_columns_and_validation(self, bars_frame):
        assert list(bars_frame.columns) == [
            "ticker",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "adjfactor",
            "turnover",
            "suspend",
            "is_limit_up",
            "is_limit_down",
            "quality_flag",
        ]
        aligned = _common.align_schema(bars_frame, "bars_daily")
        SCHEMA["bars_daily"].validate(aligned)

    def test_write_daily_lands_one_file_per_day(self, bars_frame, tmp_path):
        written = fetch_bars.write_daily(bars_frame, "bars_daily", tmp_path)
        assert {p.name for p in written} == {
            "2025-01-15.parquet",
            "2025-01-16.parquet",
        }
        for path in written:
            assert path.exists()

    def test_volume_and_amount_are_rescaled(self, bars_frame):
        # 手 -> 股 (x100), 千元 -> 元 (x1000).
        assert bars_frame["volume"].tolist() == [1234500, 1234500]
        assert bars_frame["amount"].tolist() == pytest.approx(
            [_AMOUNT_QIAN * 1000.0] * 2
        )


class TestFundamentalFrame:
    @pytest.fixture(scope="class")
    def frame(self, pro) -> pd.DataFrame:
        return fetch_fund.build_frame(pro, TS, START, END)

    def test_exactly_one_ticker_column(self, frame):
        assert list(frame.columns).count("ticker") == 1
        assert "ts_code" not in frame.columns
        assert frame["ticker"].tolist() == [TS, TS]

    def test_write_daily_lands_one_file_per_day(self, frame, tmp_path):
        written = fetch_fund.write_daily(frame, "fundamental", tmp_path)
        assert {p.name for p in written} == {
            "2025-01-15.parquet",
            "2025-01-16.parquet",
        }

    def test_passes_schema_validation(self, frame):
        aligned = _common.align_schema(frame, "fundamental")
        SCHEMA["fundamental"].validate(aligned)


class TestTurnoverKeyAlignment:
    """The date-key bug in isolation — the call the merged frame makes."""

    def test_maps_datetime_date_onto_yyyymmdd_market_cap(self):
        bars = pd.DataFrame(
            {
                "trade_date": pd.to_datetime(DAYS).date,
                "amount": [_AMOUNT_QIAN * 1000.0] * 2,
            }
        )
        out = fetch_bars._turnover(bars, FakePro().daily_basic(TS, START, END))
        assert not out.isna().any()
        assert out.tolist() == pytest.approx([EXPECTED_TURNOVER] * 2)

    def test_still_nan_when_market_cap_is_unreachable(self):
        bars = pd.DataFrame(
            {"trade_date": pd.to_datetime(DAYS).date, "amount": [1.0, 2.0]}
        )
        out = fetch_bars._turnover(bars, None)
        assert out.isna().all()
