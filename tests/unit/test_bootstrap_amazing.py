"""Offline tests for the AmazingData staging -> schema mapping (QUANT-35).

``amazing_pull.py`` needs the container and a live login; everything here is
the host half of the two-stage pull and runs without network.  The load target
is the real ``scripts/bootstrap/_common.py`` (PAS-849 — no copy beside this
test).
"""

from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from data.storage_schema import SCHEMA

_MODULE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "bootstrap" / "_common.py"
)
_spec = importlib.util.spec_from_file_location("bootstrap_common", _MODULE_PATH)
assert _spec is not None and _spec.loader is not None
common = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(common)


D1, D2 = date(2024, 12, 2), date(2024, 12, 3)


def _kline() -> pd.DataFrame:
    # Shapes and units copied from task/probe_amazing_quant35.log: 510300.SH
    # 2024-12-02 close 4.031, volume 981634781 股, amount 3.948067e9 元.
    return pd.DataFrame(
        {
            "code": ["510300.SH", "510300.SH"],
            "kline_time": pd.to_datetime(["2024-12-02", "2024-12-03"]),
            "open": [3.997, 4.030],
            "high": [4.043, 4.052],
            "low": [3.987, 4.005],
            "close": [4.031, 4.035],
            "volume": [981634781, 1524423707],
            "amount": [3.948067e9, 6.140455e9],
        }
    )


def _nav(**overrides) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "MARKET_CODE": ["510300.SH", "510300.SH"],
            "PRICE_DATE": [20241202, 20241203],
            "NAV_ADJ_FACTOR": [0.447898, 0.447898],
            "UNIT_NAV": [4.0333, 4.0377],
            "ACCUM_UNIT_DIST": [0.669, 0.669],
        }
    )
    for key, value in overrides.items():
        frame[key] = value
    return frame


def _share(**overrides) -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "MARKET_CODE": ["510300.SH", "510300.SH"],
            "CHANGE_DATE": [20241202, 20241203],
            "FUND_SHARE": [9006618.77, 8998338.77],
            "FLOAT_SHARE": [9006618.77, 8998338.77],
        }
    )
    for key, value in overrides.items():
        frame[key] = value
    return frame


class TestAmazingCalendar:
    def test_yyyymmdd_ints_become_dates(self):
        out = common.frame_from_amazing_calendar(
            pd.DataFrame({"cal_date": [20241202, 20241203]})
        )
        assert list(out.columns) == ["trade_date"]
        assert list(out["trade_date"]) == [D1, D2]

    def test_rejects_missing_column(self):
        with pytest.raises(ValueError, match="cal_date"):
            common.frame_from_amazing_calendar(pd.DataFrame({"day": [20241202]}))


class TestAmazingKlineMapping:
    def test_columns_match_bars_daily_schema(self):
        out = common.frame_from_amazing_kline(_kline(), _nav(), _share())
        assert list(out.columns) == list(SCHEMA["bars_daily"].columns)

    def test_ticker_is_the_source_code_unchanged(self):
        out = common.frame_from_amazing_kline(_kline(), _nav(), _share())
        assert list(out["ticker"]) == ["510300.SH", "510300.SH"]

    def test_volume_and_amount_need_no_rescale(self):
        # AmazingData volume is 股 and amount is 元 — already storage units.
        # A ×100 / ×1000 copy of the tushare conversion would be wrong here.
        out = common.frame_from_amazing_kline(_kline(), _nav(), _share())
        assert out["volume"].tolist() == [981634781, 1524423707]
        assert out["amount"].tolist() == [3.948067e9, 6.140455e9]
        assert out["volume"].dtype.kind in "iu"

    def test_adjfactor_is_the_measured_nav_factor(self):
        nav = _nav(NAV_ADJ_FACTOR=[0.447898, 0.500000])
        out = common.frame_from_amazing_kline(_kline(), nav, _share())
        assert out["adjfactor"].tolist() == [0.447898, 0.5]

    def test_adjfactor_missing_day_is_refused(self):
        nav = _nav().iloc[:1]  # drop 2024-12-03
        with pytest.raises(ValueError, match="NAV_ADJ_FACTOR missing"):
            common.frame_from_amazing_kline(_kline(), nav, _share())

    def test_turnover_is_amount_over_float_market_cap(self):
        out = common.frame_from_amazing_kline(_kline(), _nav(), _share())
        cap = 9006618.77 * 1e4 * 4.031
        assert out["turnover"].iloc[0] == pytest.approx(3.948067e9 / cap)

    def test_turnover_nan_when_float_share_missing(self):
        share = _share().iloc[:1]  # no row for 2024-12-03
        out = common.frame_from_amazing_kline(_kline(), _nav(), share)
        assert pd.isna(out["turnover"].iloc[1])
        SCHEMA["bars_daily"].validate(common.align_schema(out, "bars_daily"))

    def test_limit_flags_are_false_not_true(self):
        # Source limitation, not a market fact — README says so explicitly.
        out = common.frame_from_amazing_kline(_kline(), _nav(), _share())
        assert not out["suspend"].any()
        assert not out["is_limit_up"].any()
        assert not out["is_limit_down"].any()
        assert (out["quality_flag"] == 0).all()

    def test_result_passes_schema_validation(self):
        out = common.frame_from_amazing_kline(_kline(), _nav(), _share())
        SCHEMA["bars_daily"].validate(common.align_schema(out, "bars_daily"))

    def test_refuses_empty_staging(self):
        with pytest.raises(ValueError, match="empty"):
            common.frame_from_amazing_kline(_kline(), _nav(), _share().iloc[0:0])

    def test_refuses_duplicate_nav_rows(self):
        nav = pd.concat([_nav(), _nav().iloc[:1]], ignore_index=True)
        with pytest.raises(ValueError, match="duplicate"):
            common.frame_from_amazing_kline(_kline(), nav, _share())
