"""Integration tests for the ingest pipeline (ac-2/ac-3/ac-4/ac-5).

Three edge classes × three sources = nine tests, plus explicit §21.3
regression coverage (ingestion.md: ``DLS_Basedata_US`` writes
``adjfactor=1`` when ``suspend==1``).  Iteration 3 adds raw landing,
watermark idempotent re-run, quarantine three paths, and cleanup safety.

Run: ``uv run pytest tests/integration/test_ingest_pipeline.py -q``
"""

from __future__ import annotations

import json
from datetime import date

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

    def test_amazingdata_adjfactor_nav_nan_falls_back(self):
        """Kline adjfactor used when NAV_ADJ_FACTOR is NaN (coalesce invariant).

        Reclassified from the #53 regression set: with NAV all-NaN both
        coalesce directions resolve to kline's value, so this passes with the
        direction reverted and cannot claim #53.  The direction claim lives
        in ``TestOptionalFindingRegressions`` mixed-row check.
        """
        raw = pd.DataFrame(
            {
                "code": ["000001.SZ"],
                "kline_time": ["2025-03-14"],
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [10.2],
                "volume": [1000],
                "amount": [10200.0],
                "adjfactor": [0.85],  # kline has value
                "NAV_ADJ_FACTOR": [None],  # NAV missing
            }
        )
        out = normalize_amazingdata("stock", raw)
        assert out["adjfactor"].iloc[0] == pytest.approx(0.85)


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


# ---------------------------------------------------------------------------
# ac-2: raw landing + watermark idempotent re-run + cleanup safety
# ---------------------------------------------------------------------------


class TestRawLanding:
    """S3: raw zstd landing at ``raw/{source}/{asset_type}/{date}.parquet``."""

    def test_raw_path_layout(self, tmp_path):
        from data.raw import raw_path

        p = raw_path("akshare", "stock", date(2025, 3, 14), data_root=tmp_path)
        assert p == tmp_path / "raw" / "akshare" / "stock" / "2025-03-14.parquet"

    def test_save_raw_roundtrip(self, tmp_path):
        from data.raw import load_raw, save_raw

        df = pd.DataFrame({"close": [10.5, 11.0], "ticker": ["000001.SZ", "600000.SH"]})
        path = save_raw(df, "akshare", "stock", date(2025, 3, 14), data_root=tmp_path)
        assert path.exists()
        loaded = load_raw("akshare", "stock", date(2025, 3, 14), data_root=tmp_path)
        assert loaded is not None
        assert list(loaded.columns) == ["close", "ticker"]
        assert len(loaded) == 2

    def test_save_raw_zstd(self, tmp_path):
        import pyarrow.parquet as pq

        from data.raw import save_raw

        df = pd.DataFrame({"x": [1, 2]})
        path = save_raw(df, "akshare", "stock", date(2025, 1, 1), data_root=tmp_path)
        meta = pq.read_metadata(str(path))
        assert meta.row_group(0).column(0).compression == "ZSTD"

    def test_load_raw_missing_returns_none(self, tmp_path):
        from data.raw import load_raw

        assert (
            load_raw("akshare", "stock", date(2025, 1, 1), data_root=tmp_path) is None
        )


class TestWatermarkIdempotentRerun:
    """ac-2: watermark persistence + idempotent re-run.

    After a successful fetch + save_raw + write_watermark, re-running the
    same fetch must not duplicate rows — the watermark prevents re-fetching
    already-processed dates.
    """

    def test_watermark_roundtrip(self, tmp_path):
        from data.source_adapter import read_watermark, write_watermark

        write_watermark("akshare", "stock", "2025-03-14", tmp_path)
        assert read_watermark("akshare", "stock", tmp_path) == "2025-03-14"

    def test_watermark_overwrite(self, tmp_path):
        from data.source_adapter import read_watermark, write_watermark

        write_watermark("akshare", "stock", "2025-03-14", tmp_path)
        write_watermark("akshare", "stock", "2025-03-15", tmp_path)
        assert read_watermark("akshare", "stock", tmp_path) == "2025-03-15"

    def test_watermark_multi_asset_type(self, tmp_path):
        from data.source_adapter import read_watermark, write_watermark

        write_watermark("akshare", "stock", "2025-03-14", tmp_path)
        write_watermark("akshare", "option", "2025-03-14", tmp_path)
        assert read_watermark("akshare", "stock", tmp_path) == "2025-03-14"
        assert read_watermark("akshare", "option", tmp_path) == "2025-03-14"

    def test_watermark_multi_source(self, tmp_path):
        from data.source_adapter import read_watermark, write_watermark

        write_watermark("akshare", "stock", "2025-03-14", tmp_path)
        write_watermark("tushare", "stock", "2025-03-14", tmp_path)
        assert read_watermark("akshare", "stock", tmp_path) == "2025-03-14"
        assert read_watermark("tushare", "stock", tmp_path) == "2025-03-14"

    def test_watermark_preserved_by_cleanup_raw(self, tmp_path):
        """ac-2 explicit: cleanup_raw must NOT delete .watermarks.json."""
        from data.raw import cleanup_raw
        from data.source_adapter import WATERMARK_FILENAME, write_watermark

        # Watermark lives at raw/{source}/.watermarks.json
        write_watermark("akshare", "stock", "2020-01-01", tmp_path)
        wm_path = tmp_path / "raw" / "akshare" / WATERMARK_FILENAME
        assert wm_path.exists()

        # Create an expired raw file
        raw_dir = tmp_path / "raw" / "akshare" / "stock"
        raw_dir.mkdir(parents=True, exist_ok=True)
        (raw_dir / "2020-01-01.parquet").write_bytes(b"fake")

        # lifecycle.yaml needed by cleanup_expired
        (tmp_path / "lifecycle.yaml").write_text(
            "retention:\n  raw/:\n    days: 365\n  quarantine/:\n    days: 30\n",
        )

        cleanup_raw(data_root=tmp_path, dry_run=False, today=date(2025, 6, 1))
        assert wm_path.exists(), ".watermarks.json must survive cleanup_raw"

    def test_watermark_survives_actual_delete(self, tmp_path):
        """ac-2: even when cleanup actually deletes, .watermarks.json survives."""
        from data.raw import cleanup_raw
        from data.source_adapter import WATERMARK_FILENAME, write_watermark

        write_watermark("akshare", "stock", "2025-06-01", tmp_path)
        wm_path = tmp_path / "raw" / "akshare" / WATERMARK_FILENAME

        raw_dir = tmp_path / "raw" / "akshare" / "stock"
        raw_dir.mkdir(parents=True, exist_ok=True)
        old_file = raw_dir / "2020-01-01.parquet"
        old_file.write_bytes(b"fake")
        new_file = raw_dir / "2025-06-01.parquet"
        new_file.write_bytes(b"fake")

        (tmp_path / "lifecycle.yaml").write_text(
            "retention:\n  raw/:\n    days: 365\n  quarantine/:\n    days: 30\n",
        )

        cleanup_raw(data_root=tmp_path, dry_run=False, today=date(2025, 6, 1))
        assert not old_file.exists(), "expired raw file should be deleted"
        assert new_file.exists(), "recent raw file should survive"
        assert wm_path.exists(), ".watermarks.json must never be deleted"


# ---------------------------------------------------------------------------
# ac-4: quarantine three paths (save/load/recheck/cleanup)
# ---------------------------------------------------------------------------


class TestQuarantineSavePath:
    """ac-4: quarantine/{asset_type}/{date}.parquet + .rejection.json."""

    def test_quarantine_path_layout(self, tmp_path):
        from data.quarantine import quarantine_path, rejection_path

        q = quarantine_path("stock", date(2025, 3, 14), data_root=tmp_path)
        r = rejection_path("stock", date(2025, 3, 14), data_root=tmp_path)
        assert q == tmp_path / "quarantine" / "stock" / "2025-03-14.parquet"
        assert r == tmp_path / "quarantine" / "stock" / "2025-03-14.rejection.json"

    def test_save_quarantine_writes_both_files(self, tmp_path):
        from data.quarantine import save_quarantine

        df = pd.DataFrame({"ticker": ["000001.SZ"], "close": [10.5]})
        meta = {
            "data_date": "2025-03-14",
            "asset_type": "stock",
            "reject_time": "2025-03-14T16:00:00",
            "batch_status": "rejected",
            "total_rows": 100,
            "rows_would_drop": 5,
            "drop_rate": 0.05,
            "hard_rules_triggered": ["price_outlier"],
        }
        q_path, r_path = save_quarantine(df, "stock", date(2025, 3, 14), meta, tmp_path)
        assert q_path.exists()
        assert r_path.exists()
        assert q_path.suffix == ".parquet"
        assert r_path.suffix == ".json"

    def test_rejection_json_required_keys(self, tmp_path):
        """ac-4: .rejection.json must carry all 8 required fields."""
        from data.quarantine import save_quarantine

        df = pd.DataFrame({"x": [1]})
        meta = {
            "data_date": "2025-03-14",
            "asset_type": "stock",
            "reject_time": "2025-03-14T16:00:00",
            "batch_status": "rejected",
            "total_rows": 100,
            "rows_would_drop": 5,
            "drop_rate": 0.05,
            "hard_rules_triggered": ["price_outlier"],
        }
        _, r_path = save_quarantine(df, "stock", date(2025, 3, 14), meta, tmp_path)
        loaded = json.loads(r_path.read_text(encoding="utf-8"))
        required = {
            "data_date",
            "asset_type",
            "reject_time",
            "batch_status",
            "total_rows",
            "rows_would_drop",
            "drop_rate",
            "hard_rules_triggered",
        }
        assert required.issubset(loaded.keys()), (
            f"missing keys: {required - loaded.keys()}"
        )

    def test_load_quarantine_roundtrip(self, tmp_path):
        from data.quarantine import load_quarantine, save_quarantine

        df = pd.DataFrame({"ticker": ["000001.SZ"], "close": [10.5]})
        meta = {"data_date": "2025-03-14", "batch_status": "rejected"}
        save_quarantine(df, "stock", date(2025, 3, 14), meta, tmp_path)
        result = load_quarantine("stock", date(2025, 3, 14), tmp_path)
        assert result is not None
        loaded_df, loaded_meta = result
        assert len(loaded_df) == 1
        assert loaded_meta["batch_status"] == "rejected"

    def test_load_quarantine_missing_returns_none(self, tmp_path):
        from data.quarantine import load_quarantine

        assert load_quarantine("stock", date(2025, 1, 1), tmp_path) is None

    def test_recheck_calls_validator(self, tmp_path):
        from data.quarantine import recheck, save_quarantine

        df = pd.DataFrame({"x": [1, 2]})
        save_quarantine(df, "stock", date(2025, 3, 14), {}, tmp_path)

        calls = []

        def fake_validator(frame):
            calls.append(len(frame))
            return {"passed": True, "reasons": []}

        verdict = recheck(df, fake_validator, "stock", date(2025, 3, 14), tmp_path)
        assert verdict == {"passed": True, "reasons": []}
        assert calls == [2]

    def test_cleanup_quarantine_preserves_recent(self, tmp_path):
        from data.quarantine import cleanup_quarantine

        q_dir = tmp_path / "quarantine" / "stock"
        q_dir.mkdir(parents=True)
        old_file = q_dir / "2020-01-01.parquet"
        old_file.write_bytes(b"fake")
        new_file = q_dir / "2025-06-01.parquet"
        new_file.write_bytes(b"fake")

        lifecycle = tmp_path / "lifecycle.yaml"
        lifecycle.write_text(
            "retention:\n  raw/:\n    days: 365\n  quarantine/:\n    days: 30\n",
        )

        cleanup_quarantine(data_root=tmp_path, dry_run=False, today=date(2025, 6, 1))
        assert not old_file.exists(), "expired quarantine file should be deleted"
        assert new_file.exists(), "recent quarantine file should survive"


# ---------------------------------------------------------------------------
# Optional review finding regressions (PR #51)
# ---------------------------------------------------------------------------


class TestOptionalFindingRegressions:
    """Regression tests for PR #51 optional findings #52 and #53."""

    def test_tushare_dual_suspend_coalesce(self):
        """Finding #52: frame with both suspend_d and suspend must not crash."""
        raw = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20250314"],
                "vol": [100],
                "amount": [500],
                "suspend_d": ["S"],
                "suspend": [False],
            }
        )
        out = normalize_tushare("stock", raw)
        assert out["suspend"].iloc[0] is True or out["suspend"].iloc[0] == True  # noqa: E712

    def test_tushare_dual_suspend_suspend_wins_when_d_nan(self):
        """Finding #52: suspend fallback used when suspend_d is NaN."""
        raw = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20250314"],
                "vol": [100],
                "amount": [500],
                "suspend_d": [None],
                "suspend": [True],
            }
        )
        out = normalize_tushare("stock", raw)
        assert bool(out["suspend"].iloc[0]) is True

    def test_amazingdata_adjfactor_nav_wins(self):
        """Finding #53: NAV_ADJ_FACTOR wins when both carry non-NaN values."""
        raw = pd.DataFrame(
            {
                "code": ["000001.SZ"],
                "kline_time": ["2025-03-14"],
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [10.2],
                "volume": [1000],
                "amount": [10200.0],
                "adjfactor": [1.0],  # kline says 1.0
                "NAV_ADJ_FACTOR": [0.90],  # NAV says 0.90
            }
        )
        out = normalize_amazingdata("stock", raw)
        assert out["adjfactor"].iloc[0] == pytest.approx(0.90), (
            "NAV_ADJ_FACTOR must win when both carry values"
        )

    def test_amazingdata_adjfactor_nav_mixed_rows(self):
        """Finding #53: NAV wins where present, kline fills NAV-NaN rows.

        One frame carries both shapes.  NAV-present rows hold values that
        conflict with kline's, so reverting the direction (NAV wins → kline
        wins) flips those rows and fails the column assert; the NAV-NaN row
        keeps the fallback claim in the same check.  Replaces the old
        ``nav_nan_falls_back`` (NAV all-NaN): that shape resolves to kline's
        value under *both* directions and cannot discriminate #53.
        """
        raw = _AMAZING_RAW.copy()  # kline adjfactor = [1.0, 1.05, 1.05]
        raw["NAV_ADJ_FACTOR"] = [0.90, np.nan, 0.70]
        out = normalize_amazingdata("stock", raw)
        assert out["adjfactor"].tolist() == pytest.approx([0.90, 1.05, 0.70])
