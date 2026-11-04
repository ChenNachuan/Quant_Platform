"""Tests for ops.scheduler — run_job engine, run records, run mutex (ac-5).

Covers the ac-5 rows: retry default 1 / data job 2, the 300s timeout, the
four-state status enum, the nine record fields, and the failure-propagation
mechanics (seams 1/2: HOLD mapping and downstream SKIPPED records).
"""

from __future__ import annotations

import inspect
import json
import re
import threading
import time

import pytest

import ops.scheduler as scheduler
from data.storage_ops import ConcurrentWriteError
from ops.scheduler import (
    JOB_CONFIG,
    JOB_ORDER,
    JOB_REGISTRY,
    RUN_RECORD_FIELDS,
    RUN_STATUSES,
    log_job_run,
    mark_downstream_skipped,
    run_job,
    run_with_timeout,
)


@pytest.fixture(autouse=True)
def jobs_log(tmp_path, monkeypatch):
    """Point the run record at a per-test tmp file."""
    path = tmp_path / "jobs.jsonl"
    monkeypatch.setattr(scheduler, "JOBS_JSONL", path)
    return path


@pytest.fixture(autouse=True)
def sent_alerts(monkeypatch):
    """Capture scheduler send_alert calls instead of logging them."""
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        scheduler, "send_alert", lambda level, msg, **kw: calls.append((level, msg))
    )
    return calls


def _read_rows():
    path = scheduler.JOBS_JSONL
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _ok():
    return "45 factors, 2 failed"


def _boom():
    raise RuntimeError("boom")


# ---------------------------------------------------------------------------
# run records: schema and run_id
# ---------------------------------------------------------------------------


class TestRunRecords:
    def test_success_writes_one_nine_field_row(self):
        record = run_job("compute_all", fn=_ok)
        rows = _read_rows()
        assert len(rows) == 1
        row = rows[0]
        assert set(row) == set(RUN_RECORD_FIELDS)
        assert row == record
        assert row["status"] == "SUCCESS"
        assert row["error"] is None
        assert row["retry_count"] == 0
        assert row["output_summary"] == "45 factors, 2 failed"
        assert row["duration_s"] >= 0
        assert re.fullmatch(r"\d{8}_compute_all_001", row["run_id"])

    def test_skipped_row_carries_nine_fields(self):
        mark_downstream_skipped("fetch_stock_daily")
        rows = _read_rows()
        assert [r["job_name"] for r in rows] == [
            "compute_all",
            "run_daily_signal",
            "health_check",
            "write_daily_report",
        ]
        for row in rows:
            assert set(row) == set(RUN_RECORD_FIELDS)
            assert row["status"] == "SKIPPED"
            assert row["retry_count"] == 0
            assert row["duration_s"] == 0
            assert "fetch_stock_daily" in row["error"]

    def test_log_job_run_rejects_unknown_status(self):
        with pytest.raises(ValueError, match="status"):
            log_job_run("compute_all", "BOGUS")

    def test_run_id_seq_restarts_per_day_and_job(self):
        for _ in range(3):
            run_job("compute_all", fn=_ok)
        run_job("health_check", fn=_ok)
        ids = [r["run_id"] for r in _read_rows()]
        day = ids[0][:8]
        assert ids == [
            f"{day}_compute_all_001",
            f"{day}_compute_all_002",
            f"{day}_compute_all_003",
            f"{day}_health_check_001",
        ]


# ---------------------------------------------------------------------------
# retry semantics (ac-5: default 1, data job 2)
# ---------------------------------------------------------------------------


class TestRetries:
    def test_default_retries_is_one_two_attempts(self):
        calls = []

        def flaky():
            calls.append(1)
            raise RuntimeError("boom")

        record = run_job("compute_all", fn=flaky)
        assert len(calls) == 2  # retry default 1 = 2 attempts
        rows = [r for r in _read_rows() if r["status"] != "SKIPPED"]
        assert [r["status"] for r in rows] == ["RETRY", "FAILED"]
        assert [r["retry_count"] for r in rows] == [0, 1]
        assert record["status"] == "FAILED"
        assert "boom" in record["error"]

    def test_data_job_config_is_two_retries_three_attempts(self):
        # The design job table: data 2 / factor 1 / signal 1 / health_check 1.
        assert JOB_CONFIG["fetch_stock_daily"]["retries"] == 2
        calls = []

        def flaky():
            calls.append(1)
            raise RuntimeError("boom")

        record = run_job(
            "fetch_stock_daily", fn=flaky, **JOB_CONFIG["fetch_stock_daily"]
        )
        assert len(calls) == 3  # retry 2 = 3 attempts
        rows = [r for r in _read_rows() if r["status"] != "SKIPPED"]
        assert [r["status"] for r in rows] == ["RETRY", "RETRY", "FAILED"]
        assert [r["retry_count"] for r in rows] == [0, 1, 2]
        assert record["status"] == "FAILED"

    def test_retry_then_success_records_retry_and_success(self):
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("transient")
            return _ok()

        record = run_job("compute_all", fn=flaky)
        assert len(calls) == 2
        rows = _read_rows()
        assert [r["status"] for r in rows] == ["RETRY", "SUCCESS"]
        assert [r["retry_count"] for r in rows] == [0, 1]
        assert record["status"] == "SUCCESS"
        assert rows[0]["error"] is not None

    def test_retries_guard_rejects_negative(self):
        with pytest.raises(ValueError, match="retries"):
            run_job("compute_all", fn=_ok, retries=-1)


# ---------------------------------------------------------------------------
# timeout (ac-5: 300s enforced)
# ---------------------------------------------------------------------------


class TestTimeout:
    def test_timeout_fails_attempt(self):
        def slow():
            time.sleep(0.5)

        record = run_job("compute_all", fn=slow, retries=0, timeout_s=0.05)
        assert record["status"] == "FAILED"
        assert "timed out" in record["error"]
        assert record["duration_s"] < 0.5

    def test_timeout_default_is_300s(self):
        default = inspect.signature(scheduler.run_job).parameters["timeout_s"].default
        assert default == 300
        assert all(cfg["timeout_s"] == 300 for cfg in JOB_CONFIG.values())

    def test_run_with_timeout_returns_value(self):
        assert run_with_timeout(_ok, 5) == "45 factors, 2 failed"

    def test_run_with_timeout_reraises_fn_error(self):
        with pytest.raises(RuntimeError, match="boom"):
            run_with_timeout(_boom, 5)


# ---------------------------------------------------------------------------
# four-state status enum
# ---------------------------------------------------------------------------


class TestStatusEnum:
    def test_four_statuses_all_reachable(self):
        run_job("compute_all", fn=_ok)  # SUCCESS
        calls = []

        def flaky():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("transient")
            return _ok()

        run_job("compute_all", fn=flaky)  # RETRY then SUCCESS
        run_job("compute_all", fn=_boom, retries=0)  # FAILED
        mark_downstream_skipped("fetch_stock_daily")  # SKIPPED
        assert {r["status"] for r in _read_rows()} == set(RUN_STATUSES)

    def test_on_failure_vocabulary_guarded(self):
        with pytest.raises(ValueError, match="on_failure"):
            run_job("compute_all", fn=_ok, on_failure="liquidate")


# ---------------------------------------------------------------------------
# failure propagation (seams 1/2) and alerts
# ---------------------------------------------------------------------------


class TestFailurePropagation:
    def test_data_failure_marks_every_later_job_skipped(self):
        run_job("fetch_stock_daily", fn=_boom, retries=0)
        rows = _read_rows()
        skipped = [r for r in rows if r["status"] == "SKIPPED"]
        assert [r["job_name"] for r in skipped] == list(JOB_ORDER[1:])
        assert rows[0]["status"] == "FAILED"

    def test_skip_failure_sends_exactly_one_high_alert(self, sent_alerts):
        run_job("fetch_stock_daily", fn=_boom, retries=1)
        assert len(sent_alerts) == 1
        level, msg = sent_alerts[0]
        assert level == "HIGH"
        assert "fetch_stock_daily" in msg

    def test_hold_return_maps_to_failed_without_retry(self, monkeypatch, sent_alerts):
        # Seam 1: run_daily_signal returns HOLD instead of raising.
        holds = []
        monkeypatch.setattr(
            scheduler, "hold_previous_portfolio", lambda name: holds.append(name)
        )
        monkeypatch.setattr(
            scheduler,
            "mark_downstream_skipped",
            lambda name: pytest.fail("hold path must not mark downstream skipped"),
        )
        calls = []

        def held():
            calls.append(1)
            return {"status": "HOLD", "error": "signal failed alpha-1"}

        record = run_job("run_daily_signal", fn=held, retries=1, on_failure="skip")
        assert len(calls) == 1  # no re-retry: the inner retry already happened
        assert record["status"] == "FAILED"  # no HOLD state in the enum
        assert record["error"] == "signal failed alpha-1"  # inner reason
        assert holds == ["run_daily_signal"]
        assert len(sent_alerts) == 1 and sent_alerts[0][0] == "HIGH"

    def test_hold_return_error_defaults_when_inner_reason_absent(self, monkeypatch):
        monkeypatch.setattr(scheduler, "hold_previous_portfolio", lambda name: {})
        record = run_job("run_daily_signal", fn=lambda: {"status": "HOLD"}, retries=1)
        assert record["status"] == "FAILED"
        assert record["error"] == "returned status=HOLD"

    def test_signal_failure_takes_hold_path(self, monkeypatch):
        holds, skips = [], []
        monkeypatch.setattr(
            scheduler, "hold_previous_portfolio", lambda name: holds.append(name)
        )
        monkeypatch.setattr(
            scheduler, "mark_downstream_skipped", lambda name: skips.append(name)
        )
        record = run_job("run_daily_signal", fn=_boom, retries=0, on_failure="hold")
        assert record["status"] == "FAILED"
        assert holds == ["run_daily_signal"]
        assert skips == []  # hold is never a liquidation and never a skip mark


# ---------------------------------------------------------------------------
# run mutex (seam 6)
# ---------------------------------------------------------------------------


class TestRunMutex:
    def test_refuses_concurrent_run(self):
        entered, release = threading.Event(), threading.Event()

        def holder():
            with scheduler.run_mutex():
                entered.set()
                release.wait(5)

        thread = threading.Thread(target=holder)
        thread.start()
        try:
            assert entered.wait(5)
            with pytest.raises(ConcurrentWriteError):
                run_job("compute_all", fn=_ok)
        finally:
            release.set()
            thread.join(5)
        assert _read_rows() == []  # the refused run recorded nothing

    def test_reentrant_within_one_thread(self):
        with scheduler.run_mutex():
            record = run_job("compute_all", fn=_ok)  # nests the same mutex
        assert record["status"] == "SUCCESS"
        assert len(_read_rows()) == 1


# ---------------------------------------------------------------------------
# job table (design facts ac-5 asserts against)
# ---------------------------------------------------------------------------


class TestJobTable:
    def test_registry_config_and_order_agree(self):
        assert set(JOB_REGISTRY) == set(JOB_CONFIG) == set(JOB_ORDER)
        assert all(callable(fn) for fn in JOB_REGISTRY.values())

    def test_retry_and_hold_facts(self):
        assert JOB_CONFIG["fetch_stock_daily"]["retries"] == 2
        for name in ("compute_all", "run_daily_signal", "health_check"):
            assert JOB_CONFIG[name]["retries"] == 1
        holds = [n for n, c in JOB_CONFIG.items() if c["on_failure"] == "hold"]
        assert holds == ["run_daily_signal"]  # hold only for the signal job
