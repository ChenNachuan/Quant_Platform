"""Tests for ``scripts/bootstrap/_common.py`` (QUANT-35).

The fetchers themselves need network; everything here is offline.  The load
target is the bootstrap helper module — a copy of it is not placed beside this
test (PAS-849).
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


GOLDEN_UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
D = date(2025, 1, 15)


# ---------------------------------------------------------------------------
# ticker_convert (AC-A3) — tushare ts_code and repo ticker are the same string
# ---------------------------------------------------------------------------
class TestTickerConvert:
    def test_identity_for_golden_universe(self):
        for code in GOLDEN_UNIVERSE:
            assert common.ticker_convert(code) == code

    def test_identity_for_stock_samples(self):
        # Measured tushare pro.daily ts_code samples: '000001.SZ', '000002.SZ', …
        for code in ("000001.SZ", "000002.SZ", "600519.SH", "830799.BJ"):
            assert common.ticker_convert(code) == code

    def test_rejects_malformed(self):
        for bad in ("510300", "", "510300.SHX", None):
            with pytest.raises(ValueError):
                common.ticker_convert(bad)


def _bars_row(**overrides) -> dict:
    row = {
        "ticker": "510300.SH",
        "trade_date": D,
        "open": 3.9,
        "high": 4.0,
        "low": 3.8,
        "close": 3.95,
        "volume": 1_234_500,
        "amount": 4_876_275.0,
        "adjfactor": 1.1,
        "turnover": 0.02,
        "suspend": False,
        "is_limit_up": False,
        "is_limit_down": False,
        "quality_flag": 0,
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# align_schema
# ---------------------------------------------------------------------------
class TestAlignSchema:
    def test_pass_through_valid_row(self):
        df = pd.DataFrame([_bars_row()])
        out = common.align_schema(df, "bars_daily")
        assert list(out.columns) == list(SCHEMA["bars_daily"].columns)
        assert out["ticker"].iloc[0] == "510300.SH"
        assert out["volume"].iloc[0] == 1_234_500

    def test_drops_unknown_columns(self):
        df = pd.DataFrame([_bars_row(pre_close=1.0, pct_chg=2.5)])
        out = common.align_schema(df, "bars_daily")
        assert "pre_close" not in out.columns
        assert "pct_chg" not in out.columns

    def test_fills_optional_defaults(self):
        row = _bars_row()
        for name in (
            "quality_flag",
            "suspend",
            "is_limit_up",
            "is_limit_down",
            "turnover",
        ):
            row.pop(name)
        out = common.align_schema(pd.DataFrame([row]), "bars_daily")
        assert out["quality_flag"].iloc[0] == 0
        assert out["suspend"].iloc[0] is False or out["suspend"].iloc[0] == False  # noqa: E712
        assert out["is_limit_up"].iloc[0] == False  # noqa: E712
        assert out["is_limit_down"].iloc[0] == False  # noqa: E712
        assert pd.isna(out["turnover"].iloc[0])

    def test_turnover_nan_is_schema_valid(self):
        # AC-A2: an all-NaN turnover column must still pass the bars_daily schema.
        row = _bars_row(turnover=float("nan"))
        out = common.align_schema(pd.DataFrame([row]), "bars_daily")
        SCHEMA["bars_daily"].validate(out)

    def test_refuses_missing_required_column(self):
        row = _bars_row()
        row.pop("adjfactor")
        with pytest.raises(ValueError, match="adjfactor"):
            common.align_schema(pd.DataFrame([row]), "bars_daily")

    def test_coerces_integer_volume(self):
        df = pd.DataFrame([_bars_row(volume=1234500.0)])
        out = common.align_schema(df, "bars_daily")
        assert out["volume"].dtype.kind in "iu"

    def test_refuses_non_integral_volume(self):
        df = pd.DataFrame([_bars_row(volume=1234500.5)])
        with pytest.raises(ValueError, match="non-integral"):
            common.align_schema(df, "bars_daily")


# ---------------------------------------------------------------------------
# write paths (AC-A1 — root is data_platform/, per PARTITION_PATHS)
# ---------------------------------------------------------------------------
class TestWritePaths:
    def test_table_path_uses_data_platform_root(self):
        assert common.table_path("bars_daily", date="2025-01-15") == Path(
            "data_platform/asset/bars_daily/2025-01-15.parquet"
        )
        assert common.table_path("calendar", exchange="sse") == Path(
            "data_platform/asset/calendar/sse.parquet"
        )

    def test_write_daily_one_file_per_day(self, tmp_path):
        df = pd.DataFrame([_bars_row(), _bars_row(trade_date=date(2025, 1, 16))])
        written = common.write_daily(df, "bars_daily", tmp_path)
        assert len(written) == 2
        for path in written:
            assert path.exists()
            assert path.parent == tmp_path / "asset" / "bars_daily"
        assert {p.name for p in written} == {"2025-01-15.parquet", "2025-01-16.parquet"}

    def test_write_reference_single_file(self, tmp_path):
        df = pd.DataFrame([{"trade_date": D}])
        path = common.write_reference(df, "calendar", tmp_path, exchange="sse")
        assert path == tmp_path / "asset" / "calendar" / "sse.parquet"
        assert path.exists()
        assert len(pd.read_parquet(path)) == 1


class TestDateArg:
    def test_accepts_both_formats(self):
        assert common.yyyymmdd("2024-12-01") == "20241201"
        assert common.yyyymmdd("20241201") == "20241201"

    def test_rejects_garbage(self):
        for bad in ("2024/12/01", "2024", "Dec 1"):
            with pytest.raises(ValueError):
                common.yyyymmdd(bad)
