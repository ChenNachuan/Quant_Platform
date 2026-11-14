"""Tests for data.data_revision — tolerances, severity, alerting, write order.

Four groups, matching the QUANT-42 acceptance criteria:

* ``CORRECTION_TOLERANCES`` — one boundary case per field (ac-1)
* severity classification — HIGH / MEDIUM / LOW (ac-2)
* alert policy — only MEDIUM and above warn (ac-3)
* apply writes the correction log **before** ``daily_ingest`` overwrites, and
  ``mark_factors_stale`` marks without recomputing (ac-4)

Run: ``uv run pytest tests/unit/test_data_revision.py -q``
"""

from __future__ import annotations

import inspect
import json
import logging
import math
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

import data.adapters
import data.calendar_adjustment
import data.dqc.engine
import data.ingestion
import data.normalizers
import data.data_revision as dr
from core.config import DATA_ROOT

D = date(2024, 6, 14)
ASSET = "bars_daily"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _frames(field: str, old_value: float, new_value: float):
    """One ticker / one trade date, differing in exactly *field*."""
    old = pd.DataFrame(
        [{"ticker": "000001.SZ", "trade_date": str(D), field: old_value}]
    )
    new = pd.DataFrame(
        [{"ticker": "000001.SZ", "trade_date": str(D), field: new_value}]
    )
    return old, new


def _change(
    field: str = "close",
    old_value: float = 100.0,
    new_value: float = 100.5,
    abs_diff: float = 0.5,
    rel_diff: float = 0.005,
) -> dr.FieldChange:
    return dr.FieldChange(
        ticker="000001.SZ",
        field=field,
        old_value=old_value,
        new_value=new_value,
        abs_diff=abs_diff,
        rel_diff=rel_diff,
        asset_type=ASSET,
        trade_date=D,
    )


def _record(*changes: dr.FieldChange) -> dr.CorrectionRecord:
    return dr.CorrectionRecord(
        asset_type=ASSET,
        trade_date=D,
        detected_at=datetime(2024, 6, 16, 2, 0, 0),
        changes=tuple(changes),
    )


# ---------------------------------------------------------------------------
# ac-1 — CORRECTION_TOLERANCES, one boundary per field
# ---------------------------------------------------------------------------


class TestCorrectionTolerances:
    """At the tolerance is source-side noise; past it is a correction."""

    def test_close_tolerance_is_one_cent(self):
        assert dr.CORRECTION_TOLERANCES["close"] == 0.01
        tol = dr.CORRECTION_TOLERANCES["close"]

        old, new = _frames("close", 10.0, 10.0 + tol)
        assert dr.compare_frames(old, new, ASSET, D) == []

        old, new = _frames("close", 10.0, 10.0 + tol + 0.005)
        changes = dr.compare_frames(old, new, ASSET, D)
        assert len(changes) == 1
        assert changes[0].field == "close"
        assert changes[0].old_value == 10.0

    def test_volume_tolerance_is_one_hundred(self):
        assert dr.CORRECTION_TOLERANCES["volume"] == 100
        tol = dr.CORRECTION_TOLERANCES["volume"]

        old, new = _frames("volume", 100000.0, 100000.0 + tol)
        assert dr.compare_frames(old, new, ASSET, D) == []

        old, new = _frames("volume", 100000.0, 100000.0 + tol + 50.0)
        changes = dr.compare_frames(old, new, ASSET, D)
        assert len(changes) == 1
        assert changes[0].field == "volume"
        assert changes[0].abs_diff == 150.0

    def test_amount_tolerance_is_one_thousand(self):
        assert dr.CORRECTION_TOLERANCES["amount"] == 1000
        tol = dr.CORRECTION_TOLERANCES["amount"]

        old, new = _frames("amount", 1_000_000.0, 1_000_000.0 + tol)
        assert dr.compare_frames(old, new, ASSET, D) == []

        old, new = _frames("amount", 1_000_000.0, 1_000_000.0 + tol + 500.0)
        changes = dr.compare_frames(old, new, ASSET, D)
        assert len(changes) == 1
        assert changes[0].field == "amount"

    def test_adjfactor_tolerance_is_one_e_minus_six(self):
        assert dr.CORRECTION_TOLERANCES["adjfactor"] == 1e-6
        tol = dr.CORRECTION_TOLERANCES["adjfactor"]

        old, new = _frames("adjfactor", 1.0, 1.0 + tol / 2)
        assert dr.compare_frames(old, new, ASSET, D) == []

        old, new = _frames("adjfactor", 1.0, 1.0 + tol * 2)
        changes = dr.compare_frames(old, new, ASSET, D)
        assert len(changes) == 1
        assert changes[0].field == "adjfactor"

    def test_change_is_recorded_with_asset_and_date(self):
        """The flat change list has to carry its own location (apply groups on it)."""
        old, new = _frames("close", 10.0, 11.0)
        (change,) = dr.compare_frames(old, new, ASSET, D)
        assert change.asset_type == ASSET
        assert change.trade_date == D
        assert change.rel_diff == 0.1


# ---------------------------------------------------------------------------
# ac-2 — severity classification
# ---------------------------------------------------------------------------


class TestSeverityClassification:
    """HIGH = adjfactor change · MEDIUM = max_rel_diff > 0.01 · LOW = rest."""

    def test_adjfactor_change_is_high(self, tmp_path):
        record = _record(_change(field="adjfactor", rel_diff=0.05))
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path) == dr.SEVERITY_HIGH
        )

    def test_rel_diff_above_one_percent_is_medium(self, tmp_path):
        record = _record(
            _change(field="close", old_value=100.0, new_value=102.0, rel_diff=0.02)
        )
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path)
            == dr.SEVERITY_MEDIUM
        )

    def test_small_non_adjfactor_change_is_low(self, tmp_path):
        record = _record(
            _change(field="close", old_value=100.0, new_value=100.5, rel_diff=0.005)
        )
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path) == dr.SEVERITY_LOW
        )

    def test_adjfactor_dominates_a_small_price_move(self, tmp_path):
        record = _record(
            _change(field="close", old_value=100.0, new_value=100.5, rel_diff=0.005),
            _change(field="adjfactor", rel_diff=1e-4),
        )
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path) == dr.SEVERITY_HIGH
        )

    def test_impact_log_records_the_payload(self, tmp_path):
        record = _record(_change())
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path) == dr.SEVERITY_LOW
        )

        lines = (
            (tmp_path / "corrections" / "impact_log.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        )
        assert len(lines) == 1
        payload = json.loads(lines[0])
        assert payload["severity"] == "LOW"
        assert payload["asset_type"] == ASSET
        assert payload["trade_date"] == str(D)
        assert payload["num_changes"] == 1
        assert payload["affected_tickers"] == ["000001.SZ"]


# ---------------------------------------------------------------------------
# ac-3 — alert policy: only MEDIUM and above
# ---------------------------------------------------------------------------


class TestAlertPolicy:
    """A LOW correction is noise and must not raise a warning."""

    def _warnings(self, caplog) -> list[str]:
        return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]

    def test_low_correction_does_not_alert(self, caplog, tmp_path):
        caplog.set_level(logging.INFO, logger="data.data_revision")
        record = _record(
            _change(field="close", old_value=100.0, new_value=100.5, rel_diff=0.005)
        )
        severity = dr.assess_correction_impact(record, data_root=tmp_path)
        assert severity == dr.SEVERITY_LOW
        assert self._warnings(caplog) == []

    def test_medium_correction_alerts(self, caplog, tmp_path):
        caplog.set_level(logging.INFO, logger="data.data_revision")
        record = _record(
            _change(field="close", old_value=100.0, new_value=102.0, rel_diff=0.02)
        )
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path)
            == dr.SEVERITY_MEDIUM
        )
        warnings = self._warnings(caplog)
        assert len(warnings) == 1
        assert "MEDIUM" in warnings[0]

    def test_high_correction_alerts(self, caplog, tmp_path):
        caplog.set_level(logging.INFO, logger="data.data_revision")
        record = _record(_change(field="adjfactor", rel_diff=0.05))
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path) == dr.SEVERITY_HIGH
        )
        warnings = self._warnings(caplog)
        assert len(warnings) == 1
        assert "HIGH" in warnings[0]


# ---------------------------------------------------------------------------
# ac-4 — write order through daily_ingest, and stale marking
# ---------------------------------------------------------------------------


class TestApplyWriteOrder:
    """The correction log lands before ``daily_ingest`` overwrites the data."""

    def test_log_is_written_before_daily_ingest(self, monkeypatch, tmp_path):
        order: list[str] = []
        real_append = dr._append_correction_log

        def append_then_record(record, data_root):
            path = real_append(record, data_root)
            order.append("log")
            return path

        def ingest_recorder(source, asset_type, trade_date):
            # Overwrite must never precede the audit record.
            path = dr.correction_log_path(asset_type, trade_date, tmp_path)
            assert path.exists(), "correction log missing at daily_ingest time"
            assert path.read_text(encoding="utf-8").strip(), "correction log is empty"
            order.append("ingest")

        monkeypatch.setattr(dr, "_append_correction_log", append_then_record)
        monkeypatch.setattr(dr, "_daily_ingest", ingest_recorder)
        monkeypatch.setattr(
            data.ingestion,
            "get_primary_source",
            lambda asset_type, *a, **k: "amazingdata",
        )

        records = dr.apply_corrections([_change()], data_root=tmp_path)

        assert order == ["log", "ingest"]
        assert len(records) == 1
        assert records[0].asset_type == ASSET
        assert records[0].trade_date == D

    def test_apply_calls_the_full_daily_ingest_pipeline(self, monkeypatch, tmp_path):
        calls: list[tuple] = []

        def ingest_recorder(source, asset_type, trade_date):
            calls.append((source, asset_type, trade_date))

        monkeypatch.setattr(dr, "_daily_ingest", ingest_recorder)
        monkeypatch.setattr(
            data.ingestion,
            "get_primary_source",
            lambda asset_type, *a, **k: "amazingdata",
        )

        dr.apply_corrections([_change()], data_root=tmp_path)

        # One record → exactly one re-ingest, on the record's asset and date.
        assert calls == [("amazingdata", ASSET, D)]

    def test_correction_log_is_appended_not_truncated(self, monkeypatch, tmp_path):
        monkeypatch.setattr(dr, "_daily_ingest", lambda *a, **k: None)
        monkeypatch.setattr(
            data.ingestion,
            "get_primary_source",
            lambda asset_type, *a, **k: "amazingdata",
        )

        dr.apply_corrections(
            [_change(old_value=10.0, new_value=11.0)], data_root=tmp_path
        )
        dr.apply_corrections(
            [_change(old_value=11.0, new_value=12.0)], data_root=tmp_path
        )

        path = dr.correction_log_path(ASSET, D, tmp_path)
        lines = path.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 2
        first, second = (json.loads(line) for line in lines)
        assert first["old"] == 10.0 and first["new"] == 11.0
        assert second["old"] == 11.0 and second["new"] == 12.0

    def test_missing_corrections_directory_is_created(self, monkeypatch, tmp_path):
        monkeypatch.setattr(dr, "_daily_ingest", lambda *a, **k: None)
        monkeypatch.setattr(
            data.ingestion,
            "get_primary_source",
            lambda asset_type, *a, **k: "amazingdata",
        )

        root = tmp_path / "nested" / "data_platform"
        assert not root.exists()
        dr.apply_corrections([_change()], data_root=root)

        path = dr.correction_log_path(ASSET, D, root)
        assert path.is_file()

    def test_changes_without_location_are_refused(self):
        orphan = dr.FieldChange(
            ticker="000001.SZ",
            field="close",
            old_value=10.0,
            new_value=11.0,
            abs_diff=1.0,
            rel_diff=0.1,
        )
        try:
            dr.apply_corrections([orphan], data_root="unused")
        except ValueError as exc:
            assert "asset_type" in str(exc)
        else:
            raise AssertionError("apply_corrections accepted a location-less change")


class TestMarkFactorsStale:
    """Stale marking only — never a recompute (issue 边界)."""

    def _write_factor(self, root: Path, name: str, depends_on: list[str]) -> Path:
        out = root / "factors" / name / "v1.0"
        out.mkdir(parents=True, exist_ok=True)
        meta = {
            "factor_name": name,
            "version": "1.0",
            "depends_on": depends_on,
        }
        path = out / "metadata.json"
        path.write_text(json.dumps(meta), encoding="utf-8")
        # A values file: must survive marking untouched.
        (out / "2024.parquet").write_bytes(b"parquet-bytes")
        return path

    def test_marks_only_factors_depending_on_the_touched_table(self, tmp_path):
        touched = self._write_factor(tmp_path, "mom_20d", ["bars_daily"])
        unrelated = self._write_factor(tmp_path, "fund_yield", ["fundamental"])
        composite = self._write_factor(
            tmp_path, "risk_adj_mom", ["bars_daily", "factor:mom_20d"]
        )

        record = _record(_change())
        affected = dr.mark_factors_stale(record, data_root=tmp_path)

        assert affected == ["mom_20d", "risk_adj_mom"]

        marked = json.loads(touched.read_text(encoding="utf-8"))
        assert marked["stale_since"] == str(D)
        assert json.loads(composite.read_text(encoding="utf-8"))["stale_since"] == str(
            D
        )
        assert "stale_since" not in json.loads(unrelated.read_text(encoding="utf-8"))

    def test_marking_never_recomputes_factor_values(self, tmp_path):
        meta_path = self._write_factor(tmp_path, "mom_20d", ["bars_daily"])
        values = meta_path.parent / "2024.parquet"
        before = values.read_bytes()

        affected = dr.mark_factors_stale(_record(_change()), data_root=tmp_path)

        assert affected == ["mom_20d"]
        # Only metadata.json changes; the factor's values are left alone.
        assert values.read_bytes() == before
        assert sorted(p.name for p in meta_path.parent.iterdir()) == [
            "2024.parquet",
            "metadata.json",
        ]

    def test_no_factors_directory_yields_empty_list(self, tmp_path):
        assert dr.mark_factors_stale(_record(_change()), data_root=tmp_path) == []


# ---------------------------------------------------------------------------
# Review finding 2 — outer merge must see one-sided rows and NaN ↔ value
# ---------------------------------------------------------------------------


class TestCompareFramesCoverage:
    """An inner merge + dropna hides two real classes of correction.

    A source backfill that adds a ticker, or a field that was missing and got
    filled in, is a correction.  Both must land in the change list.
    """

    def test_row_only_in_new_is_a_change(self):
        old = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        new = pd.DataFrame(
            [
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0},
                {"ticker": "600519.SH", "trade_date": str(D), "close": 20.0},
            ]
        )
        changes = dr.compare_frames(old, new, ASSET, D)

        assert [c.ticker for c in changes] == ["600519.SH"]
        change = changes[0]
        assert change.field == "close"
        assert math.isnan(change.old_value)
        assert change.new_value == 20.0
        assert change.rel_diff == dr.TOTAL_CHANGE_REL_DIFF

    def test_row_only_in_old_is_a_change(self):
        old = pd.DataFrame(
            [
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0},
                {"ticker": "600519.SH", "trade_date": str(D), "close": 20.0},
            ]
        )
        new = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        changes = dr.compare_frames(old, new, ASSET, D)

        assert [c.ticker for c in changes] == ["600519.SH"]
        change = changes[0]
        assert change.old_value == 20.0
        assert math.isnan(change.new_value)
        assert change.abs_diff == 20.0
        assert change.rel_diff == dr.TOTAL_CHANGE_REL_DIFF

    def test_nan_to_value_is_a_change(self):
        old = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": float("nan")}]
        )
        new = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        changes = dr.compare_frames(old, new, ASSET, D)

        assert len(changes) == 1
        assert changes[0].field == "close"
        assert math.isnan(changes[0].old_value)
        assert changes[0].new_value == 10.0
        assert changes[0].rel_diff == dr.TOTAL_CHANGE_REL_DIFF

    def test_value_to_nan_is_a_change(self):
        old = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        new = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": float("nan")}]
        )
        changes = dr.compare_frames(old, new, ASSET, D)

        assert len(changes) == 1
        assert changes[0].old_value == 10.0
        assert math.isnan(changes[0].new_value)

    def test_both_sides_missing_is_not_a_change(self):
        old = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": float("nan")}]
        )
        new = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": float("nan")}]
        )
        assert dr.compare_frames(old, new, ASSET, D) == []

    def test_empty_old_reports_every_new_row(self):
        """Local has nothing for the date; the source has a batch."""
        old = pd.DataFrame(columns=["ticker", "trade_date", "close"])
        new = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        changes = dr.compare_frames(old, new, ASSET, D)

        assert len(changes) == 1
        assert changes[0].new_value == 10.0

    def test_both_empty_reports_nothing(self):
        old = pd.DataFrame(columns=["ticker", "trade_date", "close"])
        assert dr.compare_frames(old, old.copy(), ASSET, D) == []

    def test_populated_pair_within_tolerance_still_not_a_change(self):
        """The outer-merge fix must not turn ordinary noise into corrections."""
        tol = dr.CORRECTION_TOLERANCES["close"]
        old, new = _frames("close", 10.0, 10.0 + tol)
        assert dr.compare_frames(old, new, ASSET, D) == []

    def test_presence_change_is_medium_not_low(self, tmp_path):
        """A value that appeared from nothing is not a minor correction."""
        record = _record(
            _change(
                field="close",
                old_value=float("nan"),
                new_value=10.0,
                abs_diff=10.0,
                rel_diff=dr.TOTAL_CHANGE_REL_DIFF,
            )
        )
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path)
            == dr.SEVERITY_MEDIUM
        )

    def test_presence_change_on_adjfactor_is_high(self, tmp_path):
        record = _record(
            _change(
                field="adjfactor",
                old_value=float("nan"),
                new_value=1.05,
                abs_diff=1.05,
                rel_diff=dr.TOTAL_CHANGE_REL_DIFF,
            )
        )
        assert (
            dr.assess_correction_impact(record, data_root=tmp_path) == dr.SEVERITY_HIGH
        )

    def test_correction_log_is_strict_json_for_missing_sides(
        self, monkeypatch, tmp_path
    ):
        """NaN must land as ``null``, not the bare ``NaN`` token."""
        monkeypatch.setattr(dr, "_daily_ingest", lambda *a, **k: None)
        monkeypatch.setattr(
            data.ingestion,
            "get_primary_source",
            lambda asset_type, *a, **k: "amazingdata",
        )

        old = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        new = pd.DataFrame(
            [{"ticker": "600519.SH", "trade_date": str(D), "close": 20.0}]
        )
        changes = dr.compare_frames(old, new, ASSET, D)
        assert len(changes) == 2  # 000001 removed, 600519 added
        dr.apply_corrections(changes, data_root=tmp_path)

        raw = dr.correction_log_path(ASSET, D, tmp_path).read_text(encoding="utf-8")
        assert "NaN" not in raw
        assert "Infinity" not in raw
        payloads = [json.loads(line) for line in raw.splitlines()]
        assert len(payloads) == 2

        by_ticker = {p["ticker"]: p for p in payloads}
        assert by_ticker["600519.SH"]["old"] is None
        assert by_ticker["600519.SH"]["new"] == 20.0
        assert by_ticker["000001.SZ"]["old"] == 10.0
        assert by_ticker["000001.SZ"]["new"] is None
        for payload in payloads:
            assert payload["rel_diff"] == dr.TOTAL_CHANGE_REL_DIFF


# ---------------------------------------------------------------------------
# Review finding 1 — a missing daily_ingest is a named failure, not ImportError
# ---------------------------------------------------------------------------


class TestDailyIngestGuard:
    """The overwrite must go through ``daily_ingest`` or refuse loudly."""

    def test_missing_daily_ingest_raises_named_runtime_error(self, monkeypatch):
        monkeypatch.delattr(data.ingestion, "daily_ingest", raising=False)

        with pytest.raises(RuntimeError, match="daily_ingest") as excinfo:
            dr._daily_ingest("amazingdata", ASSET, D)

        message = str(excinfo.value)
        assert "data.ingestion.daily_ingest" in message
        assert "QUANT-40" in message
        # The original ImportError stays attached for diagnosis.
        assert isinstance(excinfo.value.__cause__, ImportError)

    def test_guarded_failure_does_not_write_the_asset_layer(
        self, monkeypatch, tmp_path
    ):
        """Log-first stays true: the log lands, the overwrite refuses."""
        monkeypatch.delattr(data.ingestion, "daily_ingest", raising=False)
        monkeypatch.setattr(
            data.ingestion,
            "get_primary_source",
            lambda asset_type, *a, **k: "amazingdata",
        )

        with pytest.raises(RuntimeError, match="daily_ingest"):
            dr.apply_corrections([_change()], data_root=tmp_path)

        assert dr.correction_log_path(ASSET, D, tmp_path).is_file()


# ---------------------------------------------------------------------------
# OCR round 1 — findings 1, 2, 8
# ---------------------------------------------------------------------------


class TestSourceAssetDomain:
    """find.1 — the storage table name is not the adapter/normalizer domain."""

    def test_bars_daily_translates_to_stock(self):
        assert dr.STORAGE_TO_SOURCE_ASSET["bars_daily"] == "stock"

    def test_fetch_and_normalize_receive_the_translated_type(self, monkeypatch):
        seen: dict[str, str] = {}

        class _Result:
            df = pd.DataFrame([{"ticker": "000001.SZ"}])

        class _Adapter:
            def fetch(self, asset_type, trade_date):
                seen["fetch"] = asset_type
                return _Result()

            def close(self):
                seen["closed"] = "yes"

        def _primary(table):
            seen["primary"] = table
            return "tushare"

        def _normalize(source):
            def normalize(asset_type, raw_df):
                seen["normalize"] = asset_type
                return raw_df

            return normalize

        monkeypatch.setattr(data.ingestion, "get_primary_source", _primary)
        monkeypatch.setattr(data.adapters, "get_adapter", lambda source: _Adapter())
        monkeypatch.setattr(data.normalizers, "get_normalizer", _normalize)

        dr._fetch_normalized(ASSET, D)

        assert seen["primary"] == "bars_daily"  # storage domain
        assert seen["fetch"] == "stock"  # adapter domain
        assert seen["normalize"] == "stock"
        assert seen["closed"] == "yes"

    def test_unmapped_table_is_refused_before_a_fetch(self, monkeypatch):
        def _no_adapter(source):
            raise AssertionError("adapter must not open for an unmapped table")

        monkeypatch.setattr(
            data.ingestion, "get_primary_source", lambda table: "tushare"
        )
        monkeypatch.setattr(data.adapters, "get_adapter", _no_adapter)

        with pytest.raises(ValueError, match="fundamental"):
            dr._fetch_normalized("fundamental", D)


class TestCompareDuplicateKeys:
    """find.8 — a duplicated key fans the outer merge out; refuse it."""

    def test_duplicate_keys_in_the_new_frame_are_refused(self):
        old = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        new = pd.DataFrame(
            [
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0},
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 11.0},
            ]
        )
        with pytest.raises(ValueError, match="duplicate join keys"):
            dr.compare_frames(old, new, ASSET, D)

    def test_duplicate_keys_in_the_old_frame_are_refused(self):
        old = pd.DataFrame(
            [
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0},
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 11.0},
            ]
        )
        new = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        with pytest.raises(ValueError, match="duplicate join keys"):
            dr.compare_frames(old, new, ASSET, D)


class TestDetectDataRoot:
    """find.2 — detection reads the tree it is told to, like the other stages."""

    def test_local_read_accepts_a_custom_data_root(self, tmp_path):
        day_dir = tmp_path / "asset" / "cn_stock_bars_daily"
        day_dir.mkdir(parents=True)
        pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        ).to_parquet(day_dir / f"{D.isoformat()}.parquet")

        frame = dr._load_local(ASSET, D, data_root=tmp_path)

        assert frame is not None
        assert float(frame.iloc[0]["close"]) == 10.0

    def test_missing_batch_in_the_custom_root_reads_as_none(self, tmp_path):
        assert dr._load_local(ASSET, D, data_root=tmp_path) is None

    def test_detect_threads_data_root_to_calendar_and_local(
        self, monkeypatch, tmp_path
    ):
        seen: dict[str, object] = {}

        class _Cal:
            def __init__(self, exchange, data_root="data_platform"):
                seen["cal_root"] = data_root

            def prev(self, when, n=1):
                return when - timedelta(days=n)

            def range(self, start, end):
                return [end]

        def _local(asset_type, trade_date, data_root="data_platform"):
            seen["local_root"] = data_root
            return None

        monkeypatch.setattr(data.calendar_adjustment, "TradingCalendar", _Cal)
        monkeypatch.setattr(dr, "_fetch_normalized", lambda a, d: None)
        monkeypatch.setattr(dr, "_load_local", _local)

        dr.detect_corrections(ASSET, lookback=1, data_root=tmp_path)

        assert seen["cal_root"] == tmp_path
        assert seen["local_root"] == tmp_path

    def test_data_root_defaults_match_across_the_four_stages(self):
        for fn in (
            dr.detect_corrections,
            dr.apply_corrections,
            dr.assess_correction_impact,
            dr.mark_factors_stale,
        ):
            assert inspect.signature(fn).parameters["data_root"].default == str(
                DATA_ROOT
            ), fn.__name__


# ---------------------------------------------------------------------------
# OCR round 2 — findings 1, 5, 6
# ---------------------------------------------------------------------------


def _stub_scan(monkeypatch, fresh, local, compare=None):
    """One scan date with both I/O seams stubbed; compare recorded if given."""
    seen: list = []

    class _Cal:
        def __init__(self, exchange, data_root="data_platform"):
            pass

        def prev(self, when, n=1):
            return when - timedelta(days=n)

        def range(self, start, end):
            return [D]

    def _record_compare(old, new, asset_type, trade_date):
        seen.append((old, new))
        return []

    monkeypatch.setattr(data.calendar_adjustment, "TradingCalendar", _Cal)
    monkeypatch.setattr(dr, "_fetch_normalized", lambda a, d: fresh)
    monkeypatch.setattr(
        dr, "_load_local", lambda a, d, data_root="data_platform": local
    )
    monkeypatch.setattr(dr, "compare_frames", compare or _record_compare)
    return seen


class TestEmptySideGuard:
    """find.1 — an empty frame is "no data", never "everything was removed"."""

    def test_empty_fresh_frame_is_not_a_removal_correction(self, monkeypatch, tmp_path):
        local = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        fresh = local.iloc[0:0]  # 0 rows with columns — what reindex yields
        seen = _stub_scan(monkeypatch, fresh, local)

        assert dr.detect_corrections(ASSET, lookback=1, data_root=tmp_path) == []
        assert seen == []

    def test_empty_local_frame_is_skipped(self, monkeypatch, tmp_path):
        fresh = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )
        local = fresh.iloc[0:0]
        seen = _stub_scan(monkeypatch, fresh, local)

        assert dr.detect_corrections(ASSET, lookback=1, data_root=tmp_path) == []
        assert seen == []


class TestPostDqcBaseline:
    """find.5 — the fresh pull is reduced to what would actually land."""

    def test_hard_dropped_rows_are_excluded(self, monkeypatch):
        df = pd.DataFrame(
            [
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0},
                {"ticker": "600519.SH", "trade_date": str(D), "close": 20.0},
            ]
        )

        class _Eval:
            batch_status = "accepted"
            rows_to_drop = pd.Series([False, True], index=df.index)

        monkeypatch.setattr(data.dqc.engine, "evaluate_batch", lambda *a, **k: _Eval())

        out = dr._post_dqc_baseline(df, ASSET, D, "data_platform")

        assert list(out["ticker"]) == ["000001.SZ"]

    def test_rejected_batch_has_no_like_for_like_baseline(self, monkeypatch):
        df = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )

        class _Eval:
            batch_status = "rejected"
            rows_to_drop = pd.Series([True], index=df.index)

        monkeypatch.setattr(data.dqc.engine, "evaluate_batch", lambda *a, **k: _Eval())

        assert dr._post_dqc_baseline(df, ASSET, D, "data_platform") is None

    def test_dropped_rows_never_reach_the_comparison(self, monkeypatch, tmp_path):
        """The must-fix loop: a DQC-rejected row must not read as a backfill.

        The asset layer lacks the row (its ingest dropped it); the raw re-pull
        returns it again.  Post-DQC the two sides match — no correction.
        """
        frame = pd.DataFrame(
            [
                {"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0},
                {"ticker": "600519.SH", "trade_date": str(D), "close": 20.0},
            ]
        )

        class _Eval:
            batch_status = "accepted"
            rows_to_drop = pd.Series([False, True], index=frame.index)

        monkeypatch.setattr(data.dqc.engine, "evaluate_batch", lambda *a, **k: _Eval())
        seen = _stub_scan(monkeypatch, frame.copy(), frame.iloc[:1].copy())

        assert dr.detect_corrections(ASSET, lookback=1, data_root=tmp_path) == []
        ((old, new),) = seen
        assert list(new["ticker"]) == ["000001.SZ"]
        assert list(old["ticker"]) == ["000001.SZ"]

    def test_rejected_batch_skips_the_date(self, monkeypatch, tmp_path):
        frame = pd.DataFrame(
            [{"ticker": "000001.SZ", "trade_date": str(D), "close": 10.0}]
        )

        class _Eval:
            batch_status = "rejected"
            rows_to_drop = pd.Series([False], index=frame.index)

        monkeypatch.setattr(data.dqc.engine, "evaluate_batch", lambda *a, **k: _Eval())
        seen = _stub_scan(monkeypatch, frame.copy(), frame.copy())

        assert dr.detect_corrections(ASSET, lookback=1, data_root=tmp_path) == []
        assert seen == []


class TestForeignOverrideFields:
    """find.5 (fields) — only fields this pull owns are compared."""

    def test_override_from_another_source_is_foreign(self, monkeypatch):
        monkeypatch.setattr(
            data.ingestion, "get_primary_source", lambda a, *x, **k: "tushare"
        )
        monkeypatch.setattr(
            data.ingestion,
            "get_field_overrides",
            lambda a, *x, **k: {"adjfactor": "akshare"},
        )

        assert dr._foreign_fields(ASSET) == {"adjfactor"}

    def test_override_from_the_pull_source_stays_comparable(self, monkeypatch):
        monkeypatch.setattr(
            data.ingestion, "get_primary_source", lambda a, *x, **k: "tushare"
        )
        monkeypatch.setattr(
            data.ingestion,
            "get_field_overrides",
            lambda a, *x, **k: {"adjfactor": "tushare"},
        )

        assert dr._foreign_fields(ASSET) == set()

    def test_foreign_fields_are_dropped_before_compare(self, monkeypatch, tmp_path):
        frame = pd.DataFrame(
            [
                {
                    "ticker": "000001.SZ",
                    "trade_date": str(D),
                    "close": 10.0,
                    "adjfactor": 2.0,
                }
            ]
        )
        monkeypatch.setattr(
            data.ingestion, "get_primary_source", lambda a, *x, **k: "tushare"
        )
        monkeypatch.setattr(
            data.ingestion,
            "get_field_overrides",
            lambda a, *x, **k: {"adjfactor": "akshare"},
        )
        monkeypatch.setattr(dr, "_post_dqc_baseline", lambda df, a, d, r: df)
        seen = _stub_scan(monkeypatch, frame.copy(), frame.copy())

        dr.detect_corrections(ASSET, lookback=1, data_root=tmp_path)

        ((old, new),) = seen
        assert "adjfactor" not in old.columns
        assert "adjfactor" not in new.columns
        assert "close" in new.columns


class TestStaleSinceKeepsEarliest:
    """find.6 — a later correction must not window out an earlier staleness."""

    def _mark(self, root: Path, stale_since, record) -> str:
        out = root / "factors" / "mom_20d" / "v1.0"
        out.mkdir(parents=True)
        meta = {"factor_name": "mom_20d", "depends_on": ["bars_daily"]}
        if stale_since is not None:
            meta["stale_since"] = stale_since
        path = out / "metadata.json"
        path.write_text(json.dumps(meta), encoding="utf-8")
        dr.mark_factors_stale(record, data_root=root)
        return json.loads(path.read_text(encoding="utf-8"))["stale_since"]

    @staticmethod
    def _record_on(day: date) -> dr.CorrectionRecord:
        return dr.CorrectionRecord(
            asset_type=ASSET,
            trade_date=day,
            detected_at=datetime(2024, 10, 1, 2, 0, 0),
            changes=(_change(),),
        )

    def test_later_correction_keeps_the_earlier_date(self, tmp_path):
        kept = self._mark(tmp_path, "2024-06-18", self._record_on(date(2024, 9, 20)))
        assert kept == "2024-06-18"

    def test_earlier_correction_moves_the_marker_back(self, tmp_path):
        moved = self._mark(tmp_path, "2024-09-20", self._record_on(date(2024, 6, 14)))
        assert moved == "2024-06-14"

    def test_no_prior_marker_is_set_from_the_record(self, tmp_path):
        fresh = self._mark(tmp_path, None, self._record_on(date(2024, 6, 14)))
        assert fresh == "2024-06-14"
