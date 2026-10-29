"""Integration tests for the ingest pipeline normalizers (ac-3).

Three edge classes × three sources = nine tests, plus explicit §21.3
regression coverage (ingestion.md: ``DLS_Basedata_US`` writes
``adjfactor=1`` when ``suspend==1``).  Each test feeds a source-native
raw DataFrame through its normalizer and asserts the unified-schema output
preserves the edge semantics.

Run: ``uv run pytest tests/integration/test_ingest_pipeline.py -q``
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data.normalizers import get_normalizer
from data.normalizers.akshare_normalizer import normalize as normalize_akshare
from data.normalizers.amazingdata_normalizer import normalize as normalize_amazingdata
from data.normalizers.tushare_normalizer import normalize as normalize_tushare

# ---------------------------------------------------------------------------
# Source-native raw fixtures
# ---------------------------------------------------------------------------

# AKShare stock_zh_a_hist — Chinese column names, no adjfactor / suspend
_AKSHARE_RAW = pd.DataFrame(
    {
        "日期": ["2024-01-15", "2024-01-16", "2024-01-17"],
        "股票代码": ["000001", "000001", "000001"],
        "开盘": [10.0, 10.5, 11.0],
        "最高": [10.5, 11.0, 11.5],
        "最低": [9.5, 10.0, 10.5],
        "收盘": [10.2, 10.8, 11.2],
        "成交量": [100000.0, 120000.0, 110000.0],
        "成交额": [1020000.0, 1296000.0, 1232000.0],
        "换手率": [0.5, 0.6, 0.55],
    }
)

# Tushare pro.daily — English columns, vol in lots, amount in thousands.
# adj_factor / suspend merged by the adapter (present here to test mapping).
_TUSHARE_RAW = pd.DataFrame(
    {
        "ts_code": ["000001.SZ", "000001.SZ", "000001.SZ"],
        "trade_date": ["20240115", "20240116", "20240117"],
        "open": [10.0, 10.5, 11.0],
        "high": [10.5, 11.0, 11.5],
        "low": [9.5, 10.0, 10.5],
        "close": [10.2, 10.8, 11.2],
        "vol": [1000.0, 1200.0, 1100.0],
        "amount": [1020.0, 1296.0, 1232.0],
        "adj_factor": [1.0, 1.05, 1.05],
    }
)

# AmazingData kline + fund_nav — code / kline_time columns, explicit
# adjfactor and suspend.
_AMAZING_RAW = pd.DataFrame(
    {
        "code": ["000001.SZ", "000001.SZ", "000001.SZ"],
        "kline_time": ["2024-01-15", "2024-01-16", "2024-01-17"],
        "open": [10.0, 10.5, 11.0],
        "high": [10.5, 11.0, 11.5],
        "low": [9.5, 10.0, 10.5],
        "close": [10.2, 10.8, 11.2],
        "volume": [100000, 120000, 110000],
        "amount": [1020000.0, 1296000.0, 1232000.0],
        "adjfactor": [1.0, 1.05, 1.05],
        "suspend": [False, False, False],
    }
)

_NORMALIZERS = {
    "akshare": normalize_akshare,
    "tushare": normalize_tushare,
    "amazingdata": normalize_amazingdata,
}


# ---------------------------------------------------------------------------
# Edge 1: Suspension (停牌)
# ---------------------------------------------------------------------------
class TestSuspensionEdge:
    """Suspended stocks must carry ``suspend=True``; active rows ``False``."""

    def test_akshare_suspension(self):
        """AKShare omits suspended rows entirely — present rows are active."""
        out = normalize_akshare("stock", _AKSHARE_RAW.copy())
        assert (out["suspend"] == False).all()  # noqa: E712

    def test_tushare_suspension(self):
        """Tushare suspend_d marks halted days; absent column → all active."""
        raw = _TUSHARE_RAW.copy()
        out = normalize_tushare("stock", raw)
        assert (out["suspend"] == False).all()  # noqa: E712

    def test_amazingdata_suspension(self):
        """AmazingData provides an explicit suspend flag — must be preserved."""
        raw = _AMAZING_RAW.copy()
        raw.loc[1, "suspend"] = True  # second day halted
        out = normalize_amazingdata("stock", raw)
        assert out["suspend"].tolist() == [False, True, False]


# ---------------------------------------------------------------------------
# Edge 2: Missing values (缺值)
# ---------------------------------------------------------------------------
class TestMissingValueEdge:
    """NaN in OHLC must be preserved as NaN, never fabricated to 0 or ffill."""

    def test_akshare_missing_values(self):
        raw = _AKSHARE_RAW.copy()
        raw.loc[1, "开盘"] = np.nan
        raw.loc[1, "收盘"] = np.nan
        out = normalize_akshare("stock", raw)
        assert pd.isna(out.loc[1, "open"])
        assert pd.isna(out.loc[1, "close"])
        # Adjacent rows untouched
        assert out.loc[0, "open"] == 10.0
        assert out.loc[2, "close"] == 11.2

    def test_tushare_missing_values(self):
        raw = _TUSHARE_RAW.copy()
        raw.loc[0, "high"] = np.nan
        out = normalize_tushare("stock", raw)
        assert pd.isna(out.loc[0, "high"])
        assert out.loc[1, "high"] == 11.0

    def test_amazingdata_missing_values(self):
        raw = _AMAZING_RAW.copy()
        raw.loc[2, "low"] = np.nan
        out = normalize_amazingdata("stock", raw)
        assert pd.isna(out.loc[2, "low"])
        assert out.loc[0, "low"] == 9.5


# ---------------------------------------------------------------------------
# Edge 3: Corporate actions / adjfactor (复权)
# ---------------------------------------------------------------------------
class TestCorporateActionEdge:
    """adjfactor must reflect source values; §21.3 bug must be caught."""

    def test_akshare_raw_prices(self):
        """AKShare raw contract (adjust="") → adjfactor is identity."""
        out = normalize_akshare("stock", _AKSHARE_RAW.copy())
        assert (out["adjfactor"] == 1.0).all()

    def test_tushare_adjfactor_preserved(self):
        """Source adj_factor must survive mapping; §21.3 regression.

        When suspend=1 the source may still carry a real adjfactor (e.g.
        0.85).  The normalizer must NOT overwrite it to 1.0 — that is the
        exact bug ``DLS_Basedata_US`` exhibits (ingestion.md §21.3).
        """
        raw = _TUSHARE_RAW.copy()
        raw["adj_factor"] = [1.0, 0.85, 1.05]
        raw["suspend"] = [False, True, False]  # day 2 is halted
        out = normalize_tushare("stock", raw)
        # §21.3: adjfactor on the suspended day stays 0.85, NOT 1.0
        assert out.loc[1, "adjfactor"] == 0.85
        assert out.loc[1, "suspend"] == True  # noqa: E712
        # Non-suspended days also preserved
        assert out.loc[0, "adjfactor"] == 1.0
        assert out.loc[2, "adjfactor"] == 1.05

    def test_amazingdata_adjfactor_preserved(self):
        """AmazingData NAV_ADJ_FACTOR must survive mapping; §21.3 regression.

        Same bug pattern as above: ``suspend==1`` must not force
        ``adjfactor=1``.
        """
        raw = _AMAZING_RAW.copy()
        raw["adjfactor"] = [1.0, 0.85, 1.05]
        raw["suspend"] = [False, True, False]  # day 2 is halted
        out = normalize_amazingdata("stock", raw)
        # §21.3: adjfactor on the suspended day stays 0.85, NOT 1.0
        assert out.loc[1, "adjfactor"] == 0.85
        assert out.loc[1, "suspend"] == True  # noqa: E712
        # Non-suspended days also preserved
        assert out.loc[0, "adjfactor"] == 1.0
        assert out.loc[2, "adjfactor"] == 1.05

    def test_amazingdata_nav_column_maps(self):
        """NAV_ADJ_FACTOR alone maps to adjfactor (module CA contract).

        Mapping contract, not a review-fix regression: this input takes the
        rename path and behaves the same with or without the Fix 4 coalesce.
        """
        raw = _AMAZING_RAW.copy()
        raw = raw.drop(columns=["adjfactor"])
        raw["NAV_ADJ_FACTOR"] = [1.0, 1.05, 1.05]
        out = normalize_amazingdata("stock", raw)
        assert out["adjfactor"].tolist() == [1.0, 1.05, 1.05]


# ---------------------------------------------------------------------------
# Dispatch and column contract
# ---------------------------------------------------------------------------
class TestNormalizerContract:
    """Every normalizer emits the unified column set and dispatch works."""

    @pytest.mark.parametrize("source", ["akshare", "tushare", "amazingdata"])
    def test_dispatch(self, source):
        fn = get_normalizer(source)
        assert callable(fn)

    def test_dispatch_unknown_source(self):
        with pytest.raises(ValueError, match="no normalizer"):
            get_normalizer("nonexistent")

    @pytest.mark.parametrize("source", ["akshare", "tushare", "amazingdata"])
    def test_unified_columns(self, source):
        from data.normalizers import UNIFIED_COLUMNS

        raw = {
            "akshare": _AKSHARE_RAW,
            "tushare": _TUSHARE_RAW,
            "amazingdata": _AMAZING_RAW,
        }[source]
        out = _NORMALIZERS[source]("stock", raw.copy())
        assert list(out.columns) == UNIFIED_COLUMNS

    @pytest.mark.parametrize("source", ["akshare", "tushare", "amazingdata"])
    def test_rejects_unknown_asset_type(self, source):
        raw = {
            "akshare": _AKSHARE_RAW,
            "tushare": _TUSHARE_RAW,
            "amazingdata": _AMAZING_RAW,
        }[source]
        with pytest.raises(ValueError, match="unsupported asset_type"):
            _NORMALIZERS[source]("futures", raw.copy())


# ---------------------------------------------------------------------------
# Unit conversion (Tushare lots → shares, thousands → yuan)
# ---------------------------------------------------------------------------
class TestUnitConversion:
    """Tushare vol is lots (手) and amount is thousands (千元)."""

    def test_tushare_volume_lots_to_shares(self):
        out = normalize_tushare("stock", _TUSHARE_RAW.copy())
        assert out.loc[0, "volume"] == 100000  # 1000 lots × 100
        assert out.loc[1, "volume"] == 120000

    def test_tushare_amount_thousands_to_yuan(self):
        out = normalize_tushare("stock", _TUSHARE_RAW.copy())
        assert out.loc[0, "amount"] == pytest.approx(1020000.0)  # 1020 × 1000
        assert out.loc[1, "amount"] == pytest.approx(1296000.0)


# ---------------------------------------------------------------------------
# Review regression tests (PR #51 findings 1-5)
# ---------------------------------------------------------------------------
class TestReviewRegressions:
    """Each test fails without its fix (probes P1-P4 + optional P5)."""

    # Fix 1 (probe P1): AKShare bare codes must gain exchange suffix.
    def test_akshare_ticker_qualification(self):
        out = normalize_akshare("stock", _AKSHARE_RAW.copy())
        assert out["ticker"].iloc[0] == "000001.SZ"  # prefix 00 → SZ
        assert (out["ticker"] == "000001.SZ").all()

    def test_akshare_ticker_qualification_sh(self):
        raw = _AKSHARE_RAW.copy()
        raw["股票代码"] = ["600000", "600000", "600000"]
        out = normalize_akshare("stock", raw)
        assert (out["ticker"] == "600000.SH").all()  # prefix 60 → SH

    def test_akshare_ticker_unknown_prefix(self):
        raw = _AKSHARE_RAW.copy()
        raw["股票代码"] = ["999999", "999999", "999999"]
        with pytest.raises(ValueError, match="unknown prefix"):
            normalize_akshare("stock", raw)

    # Fix 2 (probe P2): Tushare volume NaN must not raise IntCastingNaNError.
    def test_tushare_volume_nan_preserved(self):
        raw = _TUSHARE_RAW.copy()
        raw.loc[1, "vol"] = np.nan  # missing volume
        out = normalize_tushare("stock", raw)  # must not raise
        assert pd.isna(out.loc[1, "volume"])
        assert out.loc[0, "volume"] == 100000  # non-NaN preserved

    # Fix 3 (probe P4): Tushare suspend_d 'S'/' ' strings coerced to bool.
    def test_tushare_suspend_d_string_coercion(self):
        raw = _TUSHARE_RAW.copy()
        raw["suspend_d"] = ["S", " ", "S"]  # real suspend_d API output
        out = normalize_tushare("stock", raw)
        assert out["suspend"].tolist() == [True, False, True]

    def test_tushare_suspend_d_no_silently_dropped(self):
        """suspend_d column must survive mapping (was silently dropped)."""
        raw = _TUSHARE_RAW.copy()
        raw["suspend_d"] = [" ", "S", " "]
        out = normalize_tushare("stock", raw)
        assert out["suspend"].tolist() == [False, True, False]

    # Fix 4 (probe P3): AmazingData adjfactor + NAV_ADJ_FACTOR coalesced.
    def test_amazingdata_adjfactor_coalesce(self):
        raw = _AMAZING_RAW.copy()
        raw["NAV_ADJ_FACTOR"] = [1.0, 0.90, 1.05]  # second source
        # adjfactor has NaN where NAV_ADJ_FACTOR has value
        raw.loc[1, "adjfactor"] = np.nan
        out = normalize_amazingdata("stock", raw)  # must not raise TypeError
        assert out.loc[1, "adjfactor"] == pytest.approx(0.90)  # coalesced
        assert out.loc[0, "adjfactor"] == 1.0  # primary preserved
        # No duplicate columns
        assert out.columns.tolist().count("adjfactor") == 1

    def test_amazingdata_nav_only(self):
        """NAV_ADJ_FACTOR is the sole adjfactor *value* source (kline NaN).

        The kline ``adjfactor`` column exists but carries no values, so every
        value must come from ``NAV_ADJ_FACTOR`` via the Fix 4 coalesce.  Both
        source columns are present: without the coalesce the rename maps both
        to ``adjfactor``, ``to_numeric`` raises TypeError on the duplicate
        columns, and the batch dies — so this fails with Fix 4 reverted.
        """
        raw = _AMAZING_RAW.copy()
        raw["adjfactor"] = np.nan  # present but empty — NAV carries the values
        raw["NAV_ADJ_FACTOR"] = [1.0, 1.05, 1.05]
        out = normalize_amazingdata("stock", raw)  # must not raise TypeError
        assert out.columns.tolist().count("adjfactor") == 1
        assert out["adjfactor"].tolist() == pytest.approx([1.0, 1.05, 1.05])

    # Fix 5 (optional P5): AKShare volume must be integer-typed.
    def test_akshare_volume_int_dtype(self):
        out = normalize_akshare("stock", _AKSHARE_RAW.copy())
        assert (
            pd.api.types.is_integer_dtype(out["volume"])
            or str(out["volume"].dtype) == "Int64"
        )
