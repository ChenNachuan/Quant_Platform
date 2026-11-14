"""Unit tests for data.ingestion (QUANT-40, S10-T1 + 51-table expansion).

Three groups per the issue deliverables — tolerance five tiers, adjfactor
always warns, fallback source_tag in Parquet metadata — plus the tiered
response (AC4) and the trigger hook / config loader coverage.

Run: ``uv run pytest tests/unit/test_ingestion.py -q``
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq
import pytest

from data.dqc.engine import ingest_with_dqc
from data.ingestion import (
    NUMERIC_FIELDS,
    backfill_source_tag,
    cross_source_check,
    get_field_overrides,
    get_primary_source,
    get_source_priority,
    load_source_priority,
    raw_exists,
    respond,
    response_level,
    should_cross_check,
    tolerance_for,
    write_asset,
)
from data.raw import raw_path
from data.storage_ops import DiskFullError
from data.storage_schema import PARTITION_PATHS

TRADE_DATE = date(2024, 6, 14)


# ---------------------------------------------------------------------------
# Frame builders — post-Normalizer shaped cross-sections
# ---------------------------------------------------------------------------
def _bars_frame(n: int = 200, **cols) -> pd.DataFrame:
    """A bars_daily cross-section of *n* tickers, one field overridden per kwarg."""
    df = pd.DataFrame(
        {
            "ticker": [f"{i:06d}" for i in range(1, n + 1)],
            "trade_date": [TRADE_DATE] * n,
            "open": [10.0] * n,
            "high": [10.5] * n,
            "low": [9.5] * n,
            "close": [10.0] * n,
            "volume": [1_000_000] * n,
            "amount": [10_200_000.0] * n,
            "adjfactor": [1.05] * n,
            "turnover": [0.5] * n,
        }
    )
    for name, value in cols.items():
        df[name] = value
    return df


def _fundamental_frame(n: int = 200, **cols) -> pd.DataFrame:
    """A fundamental cross-section of *n* tickers."""
    df = pd.DataFrame(
        {
            "ticker": [f"{i:06d}" for i in range(1, n + 1)],
            "trade_date": [TRADE_DATE] * n,
            "total_market_cap": [1.0e10] * n,
            "float_market_cap": [5.0e9] * n,
            "pe_ttm": [30.0] * n,
            "pe_lyr": [28.0] * n,
            "pb": [1.5] * n,
            "ps_ttm": [4.0] * n,
            "total_shares": [1_000_000_000] * n,
            "float_shares": [500_000_000] * n,
        }
    )
    for name, value in cols.items():
        df[name] = value
    return df


def _mismatch_rows(
    df: pd.DataFrame, field: str, delta: float, rows: int
) -> pd.DataFrame:
    """A copy of *df* with *field* shifted by *delta* on its first *rows* rows."""
    out = df.copy()
    out.loc[out.index[:rows], field] = out.loc[out.index[:rows], field] + delta
    return out


# ---------------------------------------------------------------------------
# AC1 — tolerance five tiers
# ---------------------------------------------------------------------------
class TestToleranceFiveTiers:
    """price 0.01 abs / volume,amount 0.001 rel / adjfactor 1e-6 /
    pe_pb 0.02 / default 0.001 — one discriminating case per tier."""

    def test_price_is_absolute_001(self):
        # diff 0.008 on a 0.5 price: 1.6% relative but inside the 0.01
        # absolute tick-size tolerance -> matched.  diff 0.02 -> mismatched.
        primary = _bars_frame(close=[0.5] * 200)
        close_enough = _bars_frame(close=[0.508] * 200)
        result = cross_source_check(primary, close_enough, "bars_daily")
        assert "close" not in result.mismatched_fields

        too_far = _bars_frame(close=[0.52] * 200)
        result = cross_source_check(primary, too_far, "bars_daily")
        detail = result.mismatched_fields["close"]
        assert detail.mismatch_count == 200
        assert detail.mismatch_rate == pytest.approx(1.0)
        assert detail.max_abs_diff == pytest.approx(0.02, abs=1e-9)
        assert detail.sample_tickers == [f"{i:06d}" for i in range(1, 6)]

    def test_volume_amount_are_relative_0001(self):
        # diffs of 500 / 5100 are huge absolutely but 0.05% relative -> matched.
        primary = _bars_frame()
        close_enough = _bars_frame(
            volume=[1_000_500] * 200, amount=[10_205_100.0] * 200
        )
        result = cross_source_check(primary, close_enough, "bars_daily")
        assert "volume" not in result.mismatched_fields
        assert "amount" not in result.mismatched_fields

        too_far = _bars_frame(volume=[1_005_000] * 200, amount=[10_251_000.0] * 200)
        result = cross_source_check(primary, too_far, "bars_daily")
        assert result.mismatched_fields["volume"].mismatch_count == 200
        assert result.mismatched_fields["amount"].mismatch_count == 200
        assert result.mismatched_fields["volume"].mismatch_rate == pytest.approx(1.0)

    def test_adjfactor_is_near_exact_1e_6(self):
        primary = _bars_frame()
        close_enough = _bars_frame(adjfactor=[1.05 + 5e-7] * 200)
        result = cross_source_check(primary, close_enough, "bars_daily")
        assert "adjfactor" not in result.mismatched_fields

        too_far = _bars_frame(adjfactor=[1.05 + 5e-6] * 200)
        result = cross_source_check(primary, too_far, "bars_daily")
        assert result.mismatched_fields["adjfactor"].mismatch_count == 200

    def test_pe_pb_is_relative_02(self):
        # diff 0.5 on a PE of 30 = 1.67% relative -> inside the 2% band.
        primary = _fundamental_frame()
        close_enough = _fundamental_frame(pe_ttm=[30.5] * 200)
        result = cross_source_check(primary, close_enough, "fundamental")
        assert "pe_ttm" not in result.mismatched_fields

        too_far = _fundamental_frame(pe_ttm=[31.0] * 200)
        result = cross_source_check(primary, too_far, "fundamental")
        assert result.mismatched_fields["pe_ttm"].mismatch_count == 200

    def test_default_is_relative_0001(self):
        # turnover is unlisted in TOLERANCE_CLASS -> default 0.001 relative.
        primary = _bars_frame()
        close_enough = _bars_frame(turnover=[0.5004] * 200)  # 0.08% relative
        result = cross_source_check(primary, close_enough, "bars_daily")
        assert "turnover" not in result.mismatched_fields

        too_far = _bars_frame(turnover=[0.501] * 200)  # 0.2% relative
        result = cross_source_check(primary, too_far, "bars_daily")
        assert result.mismatched_fields["turnover"].mismatch_count == 200

    def test_tolerance_table_five_tiers(self):
        assert tolerance_for("close") == (0.01, "absolute")
        assert tolerance_for("volume") == (0.001, "relative")
        assert tolerance_for("amount") == (0.001, "relative")
        assert tolerance_for("adjfactor") == (1e-6, "absolute")
        assert tolerance_for("pe_ttm") == (0.02, "relative")
        assert tolerance_for("turnover") == (0.001, "relative")

    def test_missing_ticker_counts_and_coverage(self):
        primary = _bars_frame(n=3)
        secondary = _bars_frame(n=3).iloc[:2]  # one ticker absent in backup
        extra = pd.DataFrame(
            {
                "ticker": ["999999"],
                "trade_date": [TRADE_DATE],
                "open": [10.0],
                "high": [10.5],
                "low": [9.5],
                "close": [10.0],
                "volume": [1_000_000],
                "amount": [10_200_000.0],
                "adjfactor": [1.05],
                "turnover": [0.5],
            }
        )
        secondary = pd.concat([secondary, extra], ignore_index=True)
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.total_rows == 4
        assert result.matched_rows == 2
        assert result.missing_in_secondary == 1
        assert result.missing_in_primary == 1
        assert result.mismatched_fields == {}
        assert result.mismatch_rate == 0.0


# ---------------------------------------------------------------------------
# AC2 — adjfactor mismatch always warns
# ---------------------------------------------------------------------------
class TestAdjfactorAlwaysWarns:
    def test_single_row_adjfactor_mismatch_warns_below_silent_band(self, caplog):
        # 1/2000 = 0.05% — a plain field would be silently recorded, but the
        # adjfactor rule forbids suppression.
        primary = _bars_frame(n=2000)
        secondary = _mismatch_rows(_bars_frame(n=2000), "adjfactor", 1e-5, rows=1)
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.mismatched_fields["adjfactor"].mismatch_rate == pytest.approx(
            0.0005
        )
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "WARNING"
        assert any(
            r.levelname == "WARNING" and "adjfactor" in r.message
            for r in caplog.records
        )

    def test_plain_field_below_silent_band_is_not_suppressed_up(self, caplog):
        # The control case: same 0.05% band, non-adjfactor field -> silent.
        primary = _bars_frame(n=2000)
        secondary = _mismatch_rows(_bars_frame(n=2000), "turnover", 0.002, rows=1)
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.mismatched_fields["turnover"].mismatch_rate == pytest.approx(
            0.0005
        )
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "silent"
        assert not any(
            r.levelname == "WARNING" and r.name == "data.ingestion"
            for r in caplog.records
        )


# ---------------------------------------------------------------------------
# One-sided-NaN comparison semantics (review finding 1): a value present on
# one side only is a mismatch; both sides null matches.
# ---------------------------------------------------------------------------
class TestOneSidedNaNSemantics:
    def test_one_sided_nan_is_mismatch_and_adjfactor_elevates(self, caplog):
        # A backup NaN adjfactor cell must not pass as matched — dropping
        # `bad | one_missing` would silently disable the suspend-adjfactor
        # catch (§21.3).  1/2000 = 0.05% is below the silent band, so the
        # WARNING can only come from the adjfactor rule.
        primary = _bars_frame(n=2000)
        secondary = _bars_frame(n=2000)
        secondary.at[0, "adjfactor"] = float("nan")
        result = cross_source_check(primary, secondary, "bars_daily")
        detail = result.mismatched_fields["adjfactor"]
        assert detail.mismatch_count == 1
        assert detail.mismatch_rate == pytest.approx(0.0005)
        assert detail.sample_tickers == ["000001"]
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "WARNING"
        assert any(
            r.levelname == "WARNING" and "adjfactor" in r.message
            for r in caplog.records
        )

    def test_both_null_matches(self):
        primary = _bars_frame(n=3)
        secondary = _bars_frame(n=3)
        primary.at[0, "adjfactor"] = float("nan")
        secondary.at[0, "adjfactor"] = float("nan")
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.mismatched_fields == {}
        assert result.matched_rows == 3
        assert result.mismatch_rate == 0.0


# ---------------------------------------------------------------------------
# AC3 — fallback backfill: source_tag in Parquet metadata, not data column
# ---------------------------------------------------------------------------
class TestFallbackSourceTag:
    def test_backfill_source_tag_format(self):
        assert backfill_source_tag("qdc") == "qdc:backfill"

    def test_backfill_tag_lands_in_metadata_not_columns(self, tmp_path):
        df = _bars_frame(n=5)
        tag = backfill_source_tag("qdc")
        path = write_asset(df, "bars_daily", TRADE_DATE, tag, data_root=tmp_path)
        assert path == tmp_path / "asset" / "cn_stock_bars_daily" / "2024-06-14.parquet"
        assert path.exists()

        written = pd.read_parquet(path)
        assert "source_tag" not in written.columns
        assert list(written.columns) == list(df.columns)
        assert len(written) == 5

        meta = pq.read_schema(path).metadata
        assert meta[b"source_tag"].decode("utf-8") == "qdc:backfill"
        datetime.fromisoformat(meta[b"ingest_time"].decode("utf-8"))

    def test_normal_ingest_tag_round_trips(self, tmp_path):
        df = _bars_frame(n=3)
        path = write_asset(df, "bars_daily", TRADE_DATE, "tushare", data_root=tmp_path)
        meta = pq.read_schema(path).metadata
        assert meta[b"source_tag"].decode("utf-8") == "tushare"
        assert "source_tag" not in pd.read_parquet(path).columns


# ---------------------------------------------------------------------------
# AC4 — tiered response: silent / WARNING / ERROR
# ---------------------------------------------------------------------------
class TestTieredResponse:
    def test_silent_below_point_one_percent(self, caplog):
        primary = _bars_frame(n=2000)
        secondary = _mismatch_rows(_bars_frame(n=2000), "close", 0.5, rows=1)
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.mismatched_fields["close"].mismatch_rate == pytest.approx(0.0005)
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "silent"

    def test_warning_between_point_one_and_one_percent(self, caplog):
        primary = _bars_frame(n=1000)
        secondary = _mismatch_rows(_bars_frame(n=1000), "close", 0.5, rows=5)
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.mismatched_fields["close"].mismatch_rate == pytest.approx(0.005)
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "WARNING"
        assert any(
            r.levelname == "WARNING" and r.name == "data.ingestion"
            for r in caplog.records
        )

    def test_error_above_one_percent(self, caplog):
        primary = _bars_frame(n=1000)
        secondary = _mismatch_rows(_bars_frame(n=1000), "close", 0.5, rows=20)
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.mismatched_fields["close"].mismatch_rate == pytest.approx(0.02)
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "ERROR"
        assert any(
            r.levelname == "ERROR" and r.name == "data.ingestion"
            for r in caplog.records
        )

    def test_response_level_band_edges(self):
        assert response_level(0.0) == "silent"
        assert response_level(0.0009) == "silent"
        assert response_level(0.001) == "WARNING"  # 0.1% is in the WARNING band
        assert response_level(0.01) == "WARNING"  # 1% is still WARNING
        assert response_level(0.0101) == "ERROR"  # strictly above 1%


# ---------------------------------------------------------------------------
# source_priority.yaml loaders + trigger hook
# ---------------------------------------------------------------------------
class TestSourcePriorityConfig:
    def test_all_asset_tables_present(self):
        cfg = load_source_priority()
        asset_tables = {k for k in PARTITION_PATHS if k != "factor"}
        assert len(asset_tables) == 51
        missing = asset_tables - (set(cfg) - {"cross_check"})
        assert not missing, f"missing in source_priority.yaml: {sorted(missing)}"

    def test_loaders_canonical_and_legacy_aliases(self):
        assert get_primary_source("cn_stock_bars_daily") == "tushare"
        assert get_primary_source("bars_daily") == "tushare"
        assert get_source_priority("cn_stock_bars_daily") == [
            "tushare",
            "qdc",
            "amazingdata",
            "straightflush",
        ]
        assert get_source_priority("fundamental") == ["tushare", "qdc", "straightflush"]
        assert get_field_overrides("bars_daily") == {"adjfactor": "tushare"}
        assert get_field_overrides("cn_etf_bars_daily") == {"adjfactor": "tushare"}
        assert get_field_overrides("bars_minute") == {}
        assert get_source_priority("reference", "calendar") == [
            "tushare",
            "qdc",
            "straightflush",
            "amazingdata",
        ]
        assert get_primary_source("cn_stock_fundamental_consensus") == "qdc"
        assert get_primary_source("cn_stock_tick") == "qdc"
        assert get_primary_source("cn_etf_tick") == "qdc"

    def test_field_override_source_must_be_in_priority(self, tmp_path):
        bad = tmp_path / "source_priority.yaml"
        bad.write_text(
            "cn_stock_bars_daily:\n"
            "  priority: [tushare, qdc]\n"
            "  field_override:\n"
            "    adjfactor: amazingdata\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="not in priority"):
            get_field_overrides("bars_daily", path=bad)


class TestShouldCrossCheckHook:
    def test_fires_only_when_primary_done_and_all_backups_raw_arrived(self, tmp_path):
        backups = ["qdc", "amazingdata", "straightflush"]
        assert not should_cross_check(
            "tushare", "bars_daily", TRADE_DATE, data_root=tmp_path
        )

        # Partial backups still do not trigger
        first = raw_path(backups[0], "bars_daily", TRADE_DATE, tmp_path)
        first.parent.mkdir(parents=True, exist_ok=True)
        first.touch()
        assert not should_cross_check(
            "tushare", "bars_daily", TRADE_DATE, data_root=tmp_path
        )

        for b in backups[1:]:
            p = raw_path(b, "bars_daily", TRADE_DATE, tmp_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()

        assert should_cross_check(
            "tushare", "bars_daily", TRADE_DATE, data_root=tmp_path
        )
        assert raw_exists("qdc", "bars_daily", TRADE_DATE, data_root=tmp_path)

    def test_backup_ingest_never_triggers(self, tmp_path):
        for b in ("qdc", "amazingdata", "straightflush"):
            p = raw_path(b, "bars_daily", TRADE_DATE, tmp_path)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.touch()
        assert not should_cross_check(
            "qdc", "bars_daily", TRADE_DATE, data_root=tmp_path
        )

    def test_single_source_asset_never_cross_checks(self, tmp_path, monkeypatch):
        single = tmp_path / "source_priority.yaml"
        single.write_text(
            "cn_stock_bars_minute:\n  priority: [qdc]\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            "data.ingestion._resolve_source_priority_path", lambda path=None: single
        )
        assert not should_cross_check(
            "qdc", "bars_minute", TRADE_DATE, data_root=tmp_path
        )


# ---------------------------------------------------------------------------
# ocr findings 1/2/3/7 (task/ocr-report.json) — regression cases
# ---------------------------------------------------------------------------
class TestOcrRegressions:
    def test_should_cross_check_unarbitrated_types_return_false(self, tmp_path):
        # factor / bare reference are not arbitrated by the YAML; futures
        # without arrived backup raw also answers False instead of raising.
        assert not should_cross_check(
            "tushare", "factor", TRADE_DATE, data_root=tmp_path
        )
        assert not should_cross_check(
            "tushare", "bars_daily_futures", TRADE_DATE, data_root=tmp_path
        )
        assert not should_cross_check(
            "tushare", "reference", TRADE_DATE, data_root=tmp_path
        )

    def test_zero_matched_rows_is_inconclusive_not_clean(self, caplog):
        # An empty backup compares nothing; "silent" would read as success
        # (finding 2).
        primary = _bars_frame(n=3)
        result = cross_source_check(primary, _bars_frame(n=3).iloc[0:0], "bars_daily")
        assert result.matched_rows == 0
        assert result.total_rows == 3
        with caplog.at_level(logging.INFO, logger="data.ingestion"):
            assert respond(result) == "WARNING"
        assert any(
            r.levelname == "WARNING" and r.name == "data.ingestion"
            for r in caplog.records
        )

    def test_zero_overlap_tickers_is_inconclusive_not_clean(self):
        primary = _bars_frame(n=3)
        secondary = _bars_frame(n=3)
        secondary["ticker"] = ["999001", "999002", "999003"]
        result = cross_source_check(primary, secondary, "bars_daily")
        assert result.matched_rows == 0
        assert result.total_rows == 6
        assert respond(result) == "WARNING"

    def test_partial_tolerances_mapping_without_default(self):
        # table[cls] must be looked up lazily: a caller-supplied map with the
        # class present but no "default" key used to raise KeyError (finding 3).
        assert tolerance_for("close", {"price": 0.02}) == (0.02, "absolute")
        assert tolerance_for("volume", {"volume": 0.005}) == (0.005, "relative")

    def test_write_asset_refuses_critical_capacity(self, tmp_path, monkeypatch):
        # the asset-layer write must honor the CRITICAL disk tier (finding 7)
        monkeypatch.setattr("data.ingestion.check_capacity", lambda _p: "CRITICAL")
        with pytest.raises(DiskFullError):
            write_asset(
                _bars_frame(n=3),
                "bars_daily",
                TRADE_DATE,
                "tushare",
                data_root=tmp_path,
            )


def test_numeric_fields_cover_the_multi_source_asset_types():
    # The participating-field lists (§41 open item) must exist for every
    # asset table and preserve legacy aliases.
    for asset_type in (
        "bars_daily",
        "bars_minute",
        "fundamental",
        "cn_stock_bars_daily",
        "cn_etf_bars_daily",
        "cn_etf_fundamental",
        "cn_fund_nav",
        "cn_stock_fundamental_income",
        "option_basic",
    ):
        assert asset_type in NUMERIC_FIELDS
    assert "adjfactor" in NUMERIC_FIELDS["bars_daily"]
    assert "adjfactor" not in NUMERIC_FIELDS["bars_minute"]
    assert "adjfactor" in NUMERIC_FIELDS["cn_etf_bars_daily"]
    assert "adjfactor" not in NUMERIC_FIELDS["cn_etf_bars_minute"]


class TestMonthlyPartitionWrite:
    def test_write_asset_merges_monthly_fundamental_and_preserves_metadata(
        self, tmp_path
    ):
        d1 = date(2024, 6, 14)
        d2 = date(2024, 6, 17)
        f1 = _fundamental_frame(n=2, trade_date=d1, pe_ttm=20.0)
        f1["ticker"] = ["000001.SZ", "600519.SH"]
        f2 = _fundamental_frame(n=2, trade_date=d2, pe_ttm=22.0)
        f2["ticker"] = ["000001.SZ", "600519.SH"]

        p1 = write_asset(f1, "fundamental", d1, "qdc", data_root=tmp_path)
        p2 = write_asset(f2, "fundamental", d2, "tushare", data_root=tmp_path)

        assert p1 == p2
        assert p1 == tmp_path / "asset" / "cn_stock_fundamental" / "2024-06.parquet"
        table = pq.read_table(p1)
        assert table.schema.metadata[b"source_tag"] == b"tushare"
        stored = table.to_pandas()
        assert len(stored) == 4

        # Re-writing d1 replaces only d1 rows, keeping d2 intact.
        f1_updated = _fundamental_frame(n=2, trade_date=d1, pe_ttm=35.0)
        f1_updated["ticker"] = ["000001.SZ", "600519.SH"]
        write_asset(f1_updated, "fundamental", d1, "qdc", data_root=tmp_path)
        stored2 = pd.read_parquet(p1)
        assert len(stored2) == 4
        d1_rows = stored2[pd.to_datetime(stored2["trade_date"]).dt.date == d1]
        d2_rows = stored2[pd.to_datetime(stored2["trade_date"]).dt.date == d2]
        assert d1_rows["pe_ttm"].tolist() == pytest.approx([35.0, 35.0])
        assert d2_rows["pe_ttm"].tolist() == pytest.approx([22.0, 22.0])

    def test_ingest_with_dqc_lands_fundamental_in_monthly_parquet(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setattr(
            "data.dqc.engine.run_all_checks", lambda *args, **kwargs: ([], [])
        )
        d1 = date(2024, 6, 14)
        d2 = d1 + timedelta(days=3)
        for day, pe in ((d1, 20.0), (d2, 21.0), (d1, 25.0)):
            df = _fundamental_frame(n=2, trade_date=day, pe_ttm=pe)
            df["ticker"] = ["000001.SZ", "600519.SH"]
            ingest_with_dqc(df, day, "fundamental", data_root=tmp_path)
        month_file = tmp_path / "asset" / "cn_stock_fundamental" / "2024-06.parquet"
        assert month_file.exists()
        stored = pd.read_parquet(month_file)
        assert len(stored) == 4
        d1_rows = stored[pd.to_datetime(stored["trade_date"]).dt.date == d1]
        d2_rows = stored[pd.to_datetime(stored["trade_date"]).dt.date == d2]
        assert d1_rows["pe_ttm"].tolist() == pytest.approx([25.0, 25.0])
        assert d2_rows["pe_ttm"].tolist() == pytest.approx([21.0, 21.0])
