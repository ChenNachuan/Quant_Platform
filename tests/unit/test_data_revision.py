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

import json
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

import data.ingestion
import data.data_revision as dr

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
