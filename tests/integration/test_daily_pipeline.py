"""End-to-end tests for ``ops.scheduler.daily_pipeline`` (QUANT-48 T2).

ac-1: a manual ``daily_pipeline`` runs the 5 jobs in order.
ac-2: every ``logs/runs/jobs.jsonl`` row carries the nine schema fields.
ac-3: ``SUCCESS | FAILED | RETRY | SKIPPED`` are all reachable and recorded.
ac-4: data-job failure -> later jobs ``SKIPPED`` + exactly one HIGH alert;
      signal-job failure -> hold the previous portfolio, never a liquidation.
ac-6 (QUANT-49 T2): after the pipeline, ``data/ops/metrics.jsonl`` holds the
      day's 12-row snapshot — the real ``health_check`` body inside the chain.

Seam 1: a returned ``{"status": "HOLD"}`` maps to ``FAILED`` with no re-retry.
Seam 3: a ``SKIPPED`` signal job also holds. Review gap (T1): the hold path
also writes the downstream ``SKIPPED`` rows.
"""

from __future__ import annotations

import json
from datetime import date

import pytest

import ops.alerting as alerting
import ops.monitoring as monitoring
import ops.scheduler as scheduler
from ops.monitoring import STRATEGY_METRICS, SYSTEM_METRICS
from ops.scheduler import JOB_ORDER, RUN_RECORD_FIELDS, RUN_STATUSES, daily_pipeline

HOLD_MARKER = {
    "action": "hold_previous_portfolio",
    "job": "run_daily_signal",
    "portfolio": "previous",
}


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


def _boom():
    raise RuntimeError("boom")


def _install(monkeypatch, **bodies):
    """Swap JOB_REGISTRY entries for this test (fakes never touch Docker)."""
    for name, fn in bodies.items():
        monkeypatch.setitem(scheduler.JOB_REGISTRY, name, fn)


def _never(name):
    def _fail():
        pytest.fail(f"{name} must not run after an upstream failure")

    return _fail


def _spy_hold(monkeypatch):
    """Wrap the real ``hold_previous_portfolio`` and record its markers."""
    real = scheduler.hold_previous_portfolio
    markers: list[dict] = []

    def spy(name):
        marker = real(name)
        markers.append(marker)
        return marker

    monkeypatch.setattr(scheduler, "hold_previous_portfolio", spy)
    return markers


class TestPipelineOrder:
    def test_five_jobs_in_order_all_success(self, monkeypatch):
        order: list[str] = []

        def make(name):
            def body():
                order.append(name)
                return f"{name} ok"

            return body

        _install(monkeypatch, **{name: make(name) for name in JOB_ORDER})
        assert daily_pipeline() is None  # design-doc signature: -> None
        assert order == list(JOB_ORDER)  # ac-1
        rows = _read_rows()
        assert [r["job_name"] for r in rows] == list(JOB_ORDER)
        assert all(r["status"] == "SUCCESS" for r in rows)
        assert all(r["retry_count"] == 0 for r in rows)
        assert [r["error"] for r in rows] == [None] * 5


class TestRecords:
    def test_nine_fields_and_four_states_in_one_run(self, monkeypatch):
        # Data job flaky (RETRY -> SUCCESS); factor job fails (RETRY, FAILED)
        # and marks the tail SKIPPED — all four states in one log.
        attempts: list[int] = []

        def flaky_fetch():
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("transient")
            return "fetched"

        _install(
            monkeypatch,
            fetch_stock_daily=flaky_fetch,
            compute_all=_boom,
            run_daily_signal=_never("run_daily_signal"),
            health_check=_never("health_check"),
            write_daily_report=_never("write_daily_report"),
        )
        _spy_hold(monkeypatch)  # seam 3: the skipped signal holds
        daily_pipeline()
        rows = _read_rows()
        assert rows
        for row in rows:
            assert set(row) == set(RUN_RECORD_FIELDS)  # ac-2
            assert row["status"] in RUN_STATUSES
        assert set(r["status"] for r in rows) == set(RUN_STATUSES)  # ac-3
        assert [r["job_name"] for r in rows if r["status"] == "SKIPPED"] == [
            "run_daily_signal",
            "health_check",
            "write_daily_report",
        ]


class TestFailurePropagation:
    def test_data_failure_skips_tail_with_one_high_alert(
        self, monkeypatch, sent_alerts
    ):
        _install(
            monkeypatch,
            fetch_stock_daily=_boom,
            compute_all=_never("compute_all"),
            run_daily_signal=_never("run_daily_signal"),
            health_check=_never("health_check"),
            write_daily_report=_never("write_daily_report"),
        )
        markers = _spy_hold(monkeypatch)
        daily_pipeline()
        rows = _read_rows()
        assert rows[0]["job_name"] == "fetch_stock_daily"
        assert [r["status"] for r in rows[:3]] == ["RETRY", "RETRY", "FAILED"]
        assert [r["job_name"] for r in rows if r["status"] == "SKIPPED"] == list(
            JOB_ORDER[1:]
        )
        assert len(sent_alerts) == 1  # exactly one HIGH alert, ac-4
        level, msg = sent_alerts[0]
        assert level == "HIGH" and "fetch_stock_daily" in msg
        # Seam 3: the signal never ran -> hold the previous book. The hold
        # marker is the only portfolio action — there is no liquidation path.
        assert markers == [HOLD_MARKER]

    def test_signal_failure_holds_and_skips_tail(self, monkeypatch, sent_alerts):
        attempts: list[int] = []

        def bad_signal():
            attempts.append(1)
            raise RuntimeError("signal exploded")

        _install(
            monkeypatch,
            fetch_stock_daily=lambda: "fetched",
            compute_all=lambda: "45 factors",
            run_daily_signal=bad_signal,
            health_check=_never("health_check"),
            write_daily_report=_never("write_daily_report"),
        )
        markers = _spy_hold(monkeypatch)
        daily_pipeline()
        assert len(attempts) == 2  # JOB_CONFIG retries=1 -> 2 attempts
        rows = _read_rows()
        assert [r["job_name"] for r in rows[:2]] == ["fetch_stock_daily", "compute_all"]
        assert all(r["status"] == "SUCCESS" for r in rows[:2])
        assert [r["status"] for r in rows[2:4]] == ["RETRY", "FAILED"]
        assert "signal exploded" in rows[3]["error"]
        # Review gap closed: the hold path also records the tail skips.
        assert [r["job_name"] for r in rows if r["status"] == "SKIPPED"] == [
            "health_check",
            "write_daily_report",
        ]
        assert markers == [HOLD_MARKER]  # hold once, never a liquidation
        assert len(sent_alerts) == 1 and sent_alerts[0][0] == "HIGH"

    def test_hold_return_maps_to_failed_holds_and_skips_tail(
        self, monkeypatch, sent_alerts
    ):
        # Seam 1 end-to-end: the signal returns HOLD instead of raising.
        attempts: list[int] = []

        def held_signal():
            attempts.append(1)
            return {"status": "HOLD", "error": "signal failed alpha-1"}

        _install(
            monkeypatch,
            fetch_stock_daily=lambda: "fetched",
            compute_all=lambda: "45 factors",
            run_daily_signal=held_signal,
            health_check=_never("health_check"),
            write_daily_report=_never("write_daily_report"),
        )
        markers = _spy_hold(monkeypatch)
        daily_pipeline()
        assert len(attempts) == 1  # no re-retry: the inner retry already ran
        rows = _read_rows()
        signal_rows = [r for r in rows if r["job_name"] == "run_daily_signal"]
        assert len(signal_rows) == 1
        assert signal_rows[0]["status"] == "FAILED"  # the enum has no HOLD
        assert signal_rows[0]["error"] == "signal failed alpha-1"
        assert [r["job_name"] for r in rows if r["status"] == "SKIPPED"] == [
            "health_check",
            "write_daily_report",
        ]
        assert markers == [HOLD_MARKER]
        assert len(sent_alerts) == 1 and sent_alerts[0][0] == "HIGH"

    def test_skipped_signal_also_holds(self, monkeypatch):
        # Seam 3: an upstream failure leaves the signal SKIPPED, and a skipped
        # signal also holds — no new signal, no new book.
        _install(
            monkeypatch,
            fetch_stock_daily=lambda: "fetched",
            compute_all=_boom,
            run_daily_signal=_never("run_daily_signal"),
            health_check=_never("health_check"),
            write_daily_report=_never("write_daily_report"),
        )
        markers = _spy_hold(monkeypatch)
        daily_pipeline()
        rows = _read_rows()
        signal_rows = [r for r in rows if r["job_name"] == "run_daily_signal"]
        assert [r["status"] for r in signal_rows] == ["SKIPPED"]
        assert markers == [HOLD_MARKER]

    def test_report_failure_stops_only_the_report(self, monkeypatch, sent_alerts):
        # The chain stops at the first non-SUCCESS; a succeeded signal's book
        # stands (hold is only for a missing or failed signal).
        _install(
            monkeypatch,
            fetch_stock_daily=lambda: "fetched",
            compute_all=lambda: "45 factors",
            run_daily_signal=lambda: {"status": "OK", "portfolio": "new"},
            health_check=_boom,
            write_daily_report=_never("write_daily_report"),
        )
        markers = _spy_hold(monkeypatch)
        daily_pipeline()
        rows = _read_rows()
        assert [r["job_name"] for r in rows if r["status"] == "SKIPPED"] == [
            "write_daily_report"
        ]
        assert [r["status"] for r in rows if r["job_name"] == "run_daily_signal"] == [
            "SUCCESS"
        ]
        assert markers == []  # signal succeeded -> no hold
        assert len(sent_alerts) == 1  # health_check's own failure alert


class TestMetricsSnapshot:
    """ac-6: ``daily_pipeline`` leaves the day's metrics snapshot on disk.

    The chain runs the REAL ``health_check`` body (the only registry entry not
    faked). The monitoring sources point at tmp: the run records this run
    writes are the day's job rows (so the system metrics come from the chain
    itself) and the lake is unreachable, which is the seam-6 fallback.
    """

    def test_daily_pipeline_lands_the_day_metrics_snapshot(
        self, monkeypatch, tmp_path, jobs_log
    ):
        monkeypatch.setattr(
            monitoring, "METRICS_JSONL", tmp_path / "ops" / "metrics.jsonl"
        )
        monkeypatch.setattr(monitoring, "JOURNAL_DIR", tmp_path / "journal")
        monkeypatch.setattr(monitoring, "SIGNALS_DIR", tmp_path / "signals")
        monkeypatch.setattr(monitoring, "JOBS_JSONL", jobs_log)  # this run's rows
        monkeypatch.setattr(
            monitoring, "RISK_AUDIT_JSONL", tmp_path / "risk_audit.jsonl"
        )
        monkeypatch.setattr(monitoring, "DATA_ROOT", str(tmp_path / "asset"))
        alert_calls: list[tuple] = []
        monkeypatch.setattr(
            alerting,
            "send_alert",
            lambda level, msg, **kw: alert_calls.append((level, kw)),
        )
        _install(
            monkeypatch,
            fetch_stock_daily=lambda: "fetched",
            compute_all=lambda: "45 factors",
            run_daily_signal=lambda: {"status": "OK", "portfolio": "new"},
            write_daily_report=lambda: "daily report written",
        )
        daily_pipeline()

        metrics_path = monitoring.METRICS_JSONL
        rows = [
            json.loads(line)
            for line in metrics_path.read_text(encoding="utf-8").splitlines()
        ]
        assert len(rows) == 12
        assert {r["date"] for r in rows} == {date.today().isoformat()}
        assert [r["name"] for r in rows] == list(STRATEGY_METRICS) + list(
            SYSTEM_METRICS
        )
        by_name = {r["name"]: r for r in rows}
        # This run's three earlier jobs succeeded before health_check read the
        # records — its own row lands after the body returns.
        assert by_name["job_success_rate_7d"]["value"] == 1.0
        assert by_name["job_success_rate_7d"]["status"] == "ok"
        assert by_name["pipeline_duration_min"]["status"] == "ok"
        assert by_name["pipeline_duration_min"]["value"] is not None
        # No lake at the tmp root -> seam-6 fallback for both data metrics.
        assert by_name["data_delay_days"] == {
            "date": by_name["data_delay_days"]["date"],
            "ts": by_name["data_delay_days"]["ts"],
            "scope": "system",
            "name": "data_delay_days",
            "value": None,
            "status": "MEDIUM",
        }
        assert by_name["data_count_deviation"]["value"] is None
        assert by_name["data_count_deviation"]["status"] == "MEDIUM"
        # No journal / eval sources -> the 8 strategy metrics fall back too.
        for name in STRATEGY_METRICS:
            assert by_name[name]["value"] is None
            assert by_name[name]["status"] == "warning"
        # A quiet day sends nothing; the health_check run record carries the
        # summary the body returned.
        assert alert_calls == []
        run_rows = _read_rows()
        assert [r["job_name"] for r in run_rows] == list(JOB_ORDER)
        assert all(r["status"] == "SUCCESS" for r in run_rows)
        health_row = run_rows[JOB_ORDER.index("health_check")]
        assert "12 rows" in health_row["output_summary"]
