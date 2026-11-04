"""Tests for ops.monitoring — 12-metric tiers, source readers, snapshot, health_check.

ac-5: the 8 strategy metrics judge 3 tiers and the 4 system metrics 2 tiers per
``docs/design/ops/monitoring.md`` — band tables verbatim, gap bands per
``task/plan_qu49.md`` seam 5, missing sources per seam 6 (value null + warning /
MEDIUM, never an alert). The readers are exercised against the real on-disk
formats (execution.ledger.Journal parquet, factor_combiner's eval_report.json,
scheduler run records, risk_audit.jsonl, DataAPI bars_daily); the
``daily_pipeline`` end-to-end snapshot case lives in
``tests/integration/test_daily_pipeline.py`` (ac-6).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

import ops.monitoring as monitoring
from core.types import EventType
from execution.ledger import Journal, JournalEntry
from ops.monitoring import (
    STRATEGY_METRICS,
    SYSTEM_METRICS,
    collect_metrics,
    health_check,
    tier_cum_return,
    tier_data_count_deviation,
    tier_data_delay_days,
    tier_excess_return,
    tier_job_success_rate_7d,
    tier_mdd,
    tier_pipeline_duration_min,
    tier_position_count,
    tier_rolling_ic,
    tier_sharpe_6m,
    tier_trade_cost,
    tier_turnover,
)

DAY = "2026-11-04"


@pytest.fixture(autouse=True)
def sources(tmp_path, monkeypatch):
    """Point every source and the snapshot at per-test tmp paths."""
    monkeypatch.setattr(monitoring, "METRICS_JSONL", tmp_path / "metrics.jsonl")
    monkeypatch.setattr(monitoring, "JOURNAL_DIR", tmp_path / "journal")
    monkeypatch.setattr(monitoring, "SIGNALS_DIR", tmp_path / "signals")
    monkeypatch.setattr(monitoring, "JOBS_JSONL", tmp_path / "jobs.jsonl")
    monkeypatch.setattr(monitoring, "RISK_AUDIT_JSONL", tmp_path / "risk_audit.jsonl")
    monkeypatch.setattr(monitoring, "DATA_ROOT", str(tmp_path / "asset"))
    return tmp_path


# ---------------------------------------------------------------------------
# Source helpers — all write the REAL formats the readers bind to.
# ---------------------------------------------------------------------------


def _journal(root, days):
    """Write real Journal day files: {day: [(EventType, payload), ...]}."""
    journal = Journal(root / "journal")
    for day, entries in days.items():
        for i, (etype, payload) in enumerate(entries):
            journal.log(
                JournalEntry(
                    timestamp=datetime.fromisoformat(f"{day}T15:{i:02d}:00"),
                    event_type=etype,
                    portfolio_id="PF-001",
                    payload=payload,
                )
            )
    journal.flush()


def _day_end(nav=None, benchmark_nav=None, position_count=None, design=None):
    payload = {}
    if nav is not None:
        payload["nav"] = nav
    if benchmark_nav is not None:
        payload["benchmark_nav"] = benchmark_nav
    if position_count is not None:
        payload["position_count"] = position_count
    if design is not None:
        payload["design_position_count"] = design
    return (EventType.DAY_END, payload)


def _eval(root, name, *, max_drawdown=-0.10, ic_mean=0.04, mtime=None):
    from research.factor_combiner import EVAL_METRIC_KEYS, write_eval_report

    metrics = dict.fromkeys(EVAL_METRIC_KEYS, 0.0)
    metrics["max_drawdown"] = max_drawdown
    metrics["ic_mean"] = ic_mean
    config = root / "config.toml"
    if not config.exists():
        config.write_text("[alpha]\nname = 'x'\n", encoding="utf-8")
    path = write_eval_report(name, metrics, str(config), out_dir=str(root / "signals"))
    if mtime is not None:
        import os

        os.utime(path, (mtime, mtime))
    return path


def _jobs(root, rows):
    with (root / "jobs.jsonl").open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def _job(day, status, job_name="compute_all", duration_s=1.0, error=None, hour=9):
    started = f"{day}T{hour:02d}:00:00+00:00"
    return {
        "run_id": f"{day.replace('-', '')}_{job_name}_001",
        "job_name": job_name,
        "started_at": started,
        "finished_at": started,
        "status": status,
        "duration_s": duration_s,
        "error": error,
        "retry_count": 0,
        "output_summary": None,
    }


def _risk(root, day, action, count):
    with (root / "risk_audit.jsonl").open("a", encoding="utf-8") as fh:
        for i in range(count):
            fh.write(
                json.dumps(
                    {
                        "timestamp": f"{day}T10:{i:02d}:00+00:00",
                        "rule": "single_weight",
                        "ticker": "600519.SH",
                        "action": action,
                        "reason": "exceeds max_single_weight",
                    }
                )
                + "\n"
            )


def _bars(root, counts_by_day):
    rows = [
        {"trade_date": day, "ticker": f"{600000 + i:06d}.SH", "close": 10.0 + i}
        for day, n in counts_by_day.items()
        for i in range(n)
    ]
    out = root / "asset" / "bars_daily"
    out.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out / "data.parquet", index=False)


def _cells(name):
    """The (value, status) cell for metric *name* from the day's collection."""
    metrics = collect_metrics(DAY)
    group = "strategy" if name in STRATEGY_METRICS else "system"
    return metrics[group][name]


# ---------------------------------------------------------------------------
# ac-5: strategy tier bands (monitoring.md table, seam 5 gap readings)
# ---------------------------------------------------------------------------


class TestStrategyTiers:
    @pytest.mark.parametrize(
        "streak,expected",
        [
            (None, "warning"),
            (0, "healthy"),
            (5, "healthy"),
            (6, "warning"),  # "< 0 持续 6 月" inclusive
            (9, "warning"),
        ],
    )
    def test_cum_return(self, streak, expected):
        assert tier_cum_return(-0.05 if streak else 0.5, streak) == expected
        assert tier_cum_return(None, streak) == "warning"  # value missing

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "warning"),
            (-0.1, "exit"),
            (0.0, "warning"),
            (0.29, "warning"),
            (0.3, "healthy"),  # gap [0.3, 0.5] reads healthy
            (0.4, "healthy"),
            (0.5, "healthy"),
            (0.9, "healthy"),
        ],
    )
    def test_sharpe_6m(self, value, expected):
        assert tier_sharpe_6m(value) == expected

    @pytest.mark.parametrize(
        "mdd,base,expected",
        [
            (None, 0.10, "warning"),
            (0.12, None, "warning"),
            (0.12, 0.0, "warning"),  # degenerate baseline is no source
            (0.08, 0.10, "healthy"),
            (0.10, 0.10, "healthy"),  # gap [1.0x, 1.2x] reads healthy
            (0.12, 0.10, "healthy"),
            (0.121, 0.10, "warning"),
            (0.15, 0.10, "warning"),  # 1.5x exactly is not > 1.5x
            (0.16, 0.10, "exit"),
        ],
    )
    def test_mdd(self, mdd, base, expected):
        assert tier_mdd(mdd, base) == expected

    @pytest.mark.parametrize(
        "value,streak,expected",
        [
            (0.1, None, "warning"),
            (None, 6, "warning"),
            (0.1, 5, "healthy"),
            (-0.1, 5, "healthy"),  # short negative run is a gap -> healthy
            (-0.1, 6, "warning"),
            (-0.1, 11, "warning"),
            (-0.1, 12, "exit"),  # "< 0 持续 1 年" inclusive
        ],
    )
    def test_excess_return(self, value, streak, expected):
        assert tier_excess_return(value, streak) == expected

    @pytest.mark.parametrize(
        "value,baseline,expected",
        [
            (None, 0.04, "warning"),
            (0.02, None, "warning"),
            (0.02, 0.0, "warning"),  # no historical mean -> no band
            (0.011, 0.04, "exit"),  # < 30% of the mean
            (0.012, 0.04, "warning"),  # 30% exactly is not < 30%
            (0.019, 0.04, "warning"),
            (0.02, 0.04, "healthy"),  # 50% exactly is not < 50%
            (0.03, 0.04, "healthy"),  # gap [50%, 70%] reads healthy
        ],
    )
    def test_rolling_ic(self, value, baseline, expected):
        assert tier_rolling_ic(value, baseline) == expected

    @pytest.mark.parametrize(
        "actual,design,expected",
        [
            (None, 25, "warning"),
            (25, None, "warning"),
            (25, 0, "warning"),
            (25, 25, "healthy"),
            (20, 25, "healthy"),  # exactly 20% off is not > 20%
            (19, 25, "warning"),
            (31, 25, "warning"),
        ],
    )
    def test_position_count(self, actual, design, expected):
        assert tier_position_count(actual, design) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "warning"),
            (0.2, "healthy"),  # below the 30-100% cell is a gap -> healthy
            (0.3, "healthy"),
            (1.0, "healthy"),
            (1.5, "healthy"),  # seam 5 ceiling reading: > 1.5 fires
            (1.6, "warning"),
        ],
    )
    def test_turnover(self, value, expected):
        assert tier_turnover(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "warning"),
            (0.003, "healthy"),
            (0.004, "healthy"),  # gap [0.4%, 0.5%] reads healthy
            (0.005, "healthy"),  # > 0.5% strictly
            (0.0051, "warning"),
        ],
    )
    def test_trade_cost(self, value, expected):
        assert tier_trade_cost(value) == expected


# ---------------------------------------------------------------------------
# ac-5: system tier bands (2-tier: ok / breach level in status)
# ---------------------------------------------------------------------------


class TestSystemTiers:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "MEDIUM"),
            (1.0, "ok"),
            (0.8, "ok"),  # < 80% strictly
            (0.79, "HIGH"),
        ],
    )
    def test_job_success_rate_7d(self, value, expected):
        assert tier_job_success_rate_7d(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "MEDIUM"),
            (30.0, "ok"),  # > 30 min strictly
            (30.5, "MEDIUM"),
        ],
    )
    def test_pipeline_duration_min(self, value, expected):
        assert tier_pipeline_duration_min(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "MEDIUM"),
            (0.0, "ok"),
            (1.0, "ok"),  # > 1 day strictly
            (1.5, "HIGH"),
            (2.0, "HIGH"),
        ],
    )
    def test_data_delay_days(self, value, expected):
        assert tier_data_delay_days(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            (None, "MEDIUM"),
            (0.0, "ok"),
            (0.3, "ok"),  # beyond ±30% strictly
            (0.31, "HIGH"),
            (-0.31, "HIGH"),
        ],
    )
    def test_data_count_deviation(self, value, expected):
        assert tier_data_count_deviation(value) == expected


# ---------------------------------------------------------------------------
# Seam 6: no sources at all -> 12 nulls at the fallback status, and the
# snapshot still lands (ac-6's "even with all-null sources").
# ---------------------------------------------------------------------------


class TestMissingSources:
    def test_all_null_snapshot(self, sources):
        metrics = collect_metrics(DAY)
        assert list(metrics["strategy"]) == list(STRATEGY_METRICS)
        assert list(metrics["system"]) == list(SYSTEM_METRICS)
        for name in STRATEGY_METRICS:
            assert metrics["strategy"][name] == {"value": None, "status": "warning"}
        for name in SYSTEM_METRICS:
            assert metrics["system"][name] == {"value": None, "status": "MEDIUM"}

    def test_snapshot_still_lands(self, sources):
        monitoring.append_metrics_snapshot(DAY, collect_metrics(DAY))
        rows = [
            json.loads(line)
            for line in (sources / "metrics.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert len(rows) == 12
        assert {r["scope"] for r in rows} == {"strategy", "system"}


# ---------------------------------------------------------------------------
# Seam 4: source readers against the real on-disk formats
# ---------------------------------------------------------------------------


class TestSourceReaders:
    def test_nav_series_drives_cum_sharpe_mdd(self, sources):
        # 100 -> 90 -> 95: cum = -5%, mdd = 10%, sharpe from the two returns.
        _journal(
            sources,
            {
                "2026-11-02": [_day_end(nav=100.0)],
                "2026-11-03": [_day_end(nav=90.0)],
                DAY: [_day_end(nav=95.0)],
            },
        )
        cell = collect_metrics(DAY)["strategy"]
        assert cell["cum_return"]["value"] == pytest.approx(-0.05)
        assert cell["mdd"]["value"] == pytest.approx(0.10)
        # mean = -1/45, std = 7*sqrt(2)/90 -> -sqrt(2)/7 * sqrt(252)
        assert cell["sharpe_6m"]["value"] == pytest.approx(-(2 * 126**0.5) / 7)

    def test_cum_return_six_month_negative_streak_warns(self, sources):
        # Base month at 100, then six month-ends all below it -> streak 6.
        days = {"2026-04-28": [_day_end(nav=100.0)]}
        for month, nav in zip(
            ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"],
            [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
        ):
            days[f"{month}-28"] = [_day_end(nav=nav)]
        _journal(sources, days)
        cell = collect_metrics(DAY)["strategy"]["cum_return"]
        assert cell["value"] == pytest.approx(-0.06)
        assert cell["status"] == "warning"

    def test_excess_return_paired_series(self, sources):
        # Paired nav/benchmark: strategy beats the benchmark -> +10% healthy.
        _journal(
            sources,
            {
                "2026-11-02": [_day_end(nav=100.0, benchmark_nav=100.0)],
                "2026-11-03": [_day_end(nav=105.0, benchmark_nav=100.0)],
                DAY: [_day_end(nav=110.0, benchmark_nav=100.0)],
            },
        )
        cell = collect_metrics(DAY)["strategy"]["excess_return"]
        assert cell["value"] == pytest.approx(0.10)
        assert cell["status"] == "healthy"

    def test_excess_six_month_negative_streak_warns(self, sources):
        # Six trailing months of underperformance -> the fact streak is 6.
        days = {"2026-04-28": [_day_end(nav=100.0, benchmark_nav=100.0)]}
        for month, nav in zip(
            ["2026-05", "2026-06", "2026-07", "2026-08", "2026-09", "2026-10"],
            [99.0, 98.0, 97.0, 96.0, 95.0, 94.0],
        ):
            days[f"{month}-28"] = [_day_end(nav=nav, benchmark_nav=100.0)]
        _journal(sources, days)
        _, facts = monitoring._collect(DAY)
        assert facts["excess_negative_months"] == 6
        cell = collect_metrics(DAY)["strategy"]["excess_return"]
        assert cell["value"] == pytest.approx(-0.06)
        assert cell["status"] == "warning"

    def test_position_count_snapshot_vs_design(self, sources):
        _journal(
            sources,
            {DAY: [_day_end(position_count=20, design=25)]},
        )
        cell = collect_metrics(DAY)["strategy"]["position_count"]
        assert cell["value"] == 20
        assert cell["status"] == "healthy"  # exactly 20% deviation

    def test_turnover_from_consecutive_books(self, sources):
        # old {A .6, B .4} -> new {A .5, B .3, C .2}: sum|dw| = .4, /2 = .2
        _journal(
            sources,
            {
                "2026-11-03": [
                    (EventType.PORTFOLIO, {"target_weights": {"A": 0.6, "B": 0.4}})
                ],
                DAY: [
                    (
                        EventType.PORTFOLIO,
                        {"target_weights": {"A": 0.5, "B": 0.3, "C": 0.2}},
                    )
                ],
            },
        )
        cell = collect_metrics(DAY)["strategy"]["turnover"]
        assert cell["value"] == pytest.approx(0.2)
        assert cell["status"] == "healthy"

    def test_trade_cost_mean_over_fills(self, sources):
        _journal(
            sources,
            {
                DAY: [
                    (
                        EventType.TRADE,
                        {
                            "fill_price": 10.0,
                            "filled_qty": 1000,
                            "commission": 30.0,
                            "slippage": 10.0,
                        },
                    ),
                    (
                        EventType.TRADE,
                        {
                            "fill_price": 20.0,
                            "filled_qty": 500,
                            "commission": 60.0,
                        },
                    ),
                ]
            },
        )
        cell = collect_metrics(DAY)["strategy"]["trade_cost"]
        assert cell["value"] == pytest.approx(0.005)  # (.004 + .006) / 2
        assert cell["status"] == "healthy"  # > 0.5% strictly

    def test_rolling_ic_against_eval_baseline(self, sources):
        _journal(
            sources,
            {
                "2026-11-02": [(EventType.SIGNAL, {"ic": 0.02})],
                "2026-11-03": [(EventType.SIGNAL, {"ic": 0.04})],
                DAY: [(EventType.SIGNAL, {"ic": 0.06})],
            },
        )
        _eval(sources, "alpha-x", ic_mean=0.04)
        cell = collect_metrics(DAY)["strategy"]["rolling_ic"]
        assert cell["value"] == pytest.approx(0.04)  # mean of the window
        assert cell["status"] == "healthy"  # 100% of the historical mean

    def test_job_records_feed_rate_and_duration(self, sources):
        _jobs(
            sources,
            [
                _job("2026-11-02", "SUCCESS", job_name="fetch_stock_daily"),
                _job(DAY, "SUCCESS", job_name="fetch_stock_daily", duration_s=600.0),
                _job(DAY, "SUCCESS", job_name="compute_all", duration_s=1200.0),
                _job(
                    DAY,
                    "FAILED",
                    job_name="run_daily_signal",
                    duration_s=0.0,
                    error="signal exploded",
                ),
            ],
        )
        system = collect_metrics(DAY)["system"]
        assert system["job_success_rate_7d"]["value"] == pytest.approx(0.75)
        assert system["job_success_rate_7d"]["status"] == "HIGH"
        assert system["pipeline_duration_min"]["value"] == pytest.approx(30.0)
        assert system["pipeline_duration_min"]["status"] == "ok"  # 30 exactly

    def test_data_lake_feeds_freshness_and_deviation(self, sources):
        _bars(sources, {"2026-11-02": 100, "2026-11-03": 100, DAY: 200})
        system = collect_metrics(DAY)["system"]
        assert system["data_delay_days"]["value"] == pytest.approx(0.0)
        assert system["data_delay_days"]["status"] == "ok"
        assert system["data_count_deviation"]["value"] == pytest.approx(1.0)
        assert system["data_count_deviation"]["status"] == "HIGH"

    def test_stale_data_lake_reports_delay(self, sources):
        _bars(sources, {"2026-11-01": 100})
        system = collect_metrics(DAY)["system"]
        assert system["data_delay_days"]["value"] == pytest.approx(3.0)
        assert system["data_delay_days"]["status"] == "HIGH"
        assert system["data_count_deviation"] == {
            "value": None,
            "status": "MEDIUM",
        }  # no prior days to average

    def test_eval_report_baseline_and_alpha_name(self, sources):
        _journal(
            sources, {"2026-11-03": [_day_end(nav=100.0)], DAY: [_day_end(nav=84.0)]}
        )
        _eval(sources, "alpha-x", max_drawdown=-0.10, mtime=1.0)
        _eval(sources, "alpha-y", max_drawdown=-0.02, mtime=2.0)  # latest wins
        _, facts = monitoring._collect(DAY)
        assert facts["alpha_type_id"] == "alpha-y"
        assert facts["backtest_mdd"] == pytest.approx(0.02)  # magnitude
        # mdd 0.16 vs baseline 0.02 -> 8x, deep in the exit band
        cell = collect_metrics(DAY)["strategy"]["mdd"]
        assert cell["value"] == pytest.approx(0.16)
        assert cell["status"] == "exit"


# ---------------------------------------------------------------------------
# Facts assembly for the 8 alerting conditions (seam 3)
# ---------------------------------------------------------------------------


class TestFacts:
    def test_facts_carry_the_alerting_contract(self, sources):
        _journal(
            sources,
            {
                # Three observations -> two returns, so the sharpe window is
                # computable and the contract can show a float fact.
                "2026-11-02": [_day_end(nav=100.0, benchmark_nav=100.0)],
                "2026-11-03": [_day_end(nav=100.0, benchmark_nav=100.0)],
                DAY: [_day_end(nav=96.0, benchmark_nav=100.0)],
            },
        )
        _eval(sources, "alpha-x", max_drawdown=-0.10)
        _jobs(
            sources,
            [
                _job(DAY, "SUCCESS", job_name="fetch_stock_daily", duration_s=600.0),
                _job(
                    DAY,
                    "FAILED",
                    job_name="compute_all",
                    duration_s=1.0,
                    error="boom",
                ),
            ],
        )
        _risk(sources, DAY, "BLOCK", 4)
        _risk(sources, DAY, "WARN", 2)
        _, facts = monitoring._collect(DAY)
        assert facts["alpha_type_id"] == "alpha-x"
        assert facts["failed_jobs"] == [{"job_name": "compute_all", "error": "boom"}]
        assert facts["risk_reject_count"] == 4  # BLOCK only
        assert facts["mdd"] == pytest.approx(0.04)
        assert facts["backtest_mdd"] == pytest.approx(0.10)
        assert facts["nav_daily_return"] == pytest.approx(-0.04)
        assert facts["excess_negative_months"] == 1
        assert facts["turnover"] is None
        assert facts["sharpe_6m"] is not None
        assert facts["data_delay_days"] is None

    def test_null_facts_never_fire(self, sources):
        _, facts = monitoring._collect(DAY)
        assert all(
            facts[key] is None
            for key in (
                "data_delay_days",
                "mdd",
                "backtest_mdd",
                "sharpe_6m",
                "excess_negative_months",
                "nav_daily_return",
                "turnover",
                "risk_reject_count",
            )
        )
        assert facts["failed_jobs"] == []


# ---------------------------------------------------------------------------
# The metrics.jsonl snapshot (schema pinned by the PR)
# ---------------------------------------------------------------------------


class TestSnapshot:
    def test_twelve_rows_six_fields_table_order(self, sources):
        monitoring.append_metrics_snapshot(DAY, collect_metrics(DAY))
        rows = [
            json.loads(line)
            for line in (sources / "metrics.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert len(rows) == 12
        assert [r["name"] for r in rows] == list(STRATEGY_METRICS) + list(
            SYSTEM_METRICS
        )
        for row in rows:
            assert set(row) == {"date", "ts", "scope", "name", "value", "status"}
            assert row["date"] == DAY
            assert row["ts"].endswith("+00:00")
        assert {r["scope"] for r in rows[:8]} == {"strategy"}
        assert {r["scope"] for r in rows[8:]} == {"system"}

    def test_append_stacks_snapshots(self, sources):
        monitoring.append_metrics_snapshot(DAY, collect_metrics(DAY))
        monitoring.append_metrics_snapshot(DAY, collect_metrics(DAY))
        lines = (sources / "metrics.jsonl").read_text(encoding="utf-8").splitlines()
        assert len(lines) == 24  # append-only time series

    def test_missing_cell_still_lands_its_row(self, sources):
        monitoring.append_metrics_snapshot(DAY, {"strategy": {}, "system": {}})
        rows = [
            json.loads(line)
            for line in (sources / "metrics.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert len(rows) == 12
        # A missing cell lands value null + the seam-6 status for its scope.
        assert rows[0] == {
            "date": DAY,
            "ts": rows[0]["ts"],
            "scope": "strategy",
            "name": "cum_return",
            "value": None,
            "status": "warning",
        }
        assert rows[8] == {
            "date": DAY,
            "ts": rows[8]["ts"],
            "scope": "system",
            "name": "job_success_rate_7d",
            "value": None,
            "status": "MEDIUM",
        }


# ---------------------------------------------------------------------------
# health_check: collect -> snapshot -> facts -> evaluate -> send (seam 3)
# ---------------------------------------------------------------------------


class TestHealthCheck:
    @pytest.fixture(autouse=True)
    def sent(self, monkeypatch):
        import ops.alerting as alerting

        calls = []
        monkeypatch.setattr(
            alerting,
            "send_alert",
            lambda level, msg, **kw: calls.append((level, msg, kw)),
        )
        return calls

    def test_quiet_day_lands_snapshot_without_alerts(self, sources, sent):
        summary = health_check()
        assert "12 rows" in summary and "0 alert(s)" in summary
        assert sent == []
        rows = [
            json.loads(line)
            for line in (sources / "metrics.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert len(rows) == 12
        assert all(r["date"] == date.today().isoformat() for r in rows)

    def test_breaches_fire_each_condition(self, sources, sent):
        today = date.today().isoformat()
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        two_days_ago = (date.today() - timedelta(days=2)).isoformat()
        # Three observations -> a computable negative sharpe (cond 4) plus a
        # -10% day (cond 6); the failed job fires cond 1 and the BLOCKs cond 8.
        _journal(
            sources,
            {
                two_days_ago: [_day_end(nav=100.0)],
                yesterday: [_day_end(nav=100.0)],
                today: [_day_end(nav=90.0)],
            },
        )
        _jobs(
            sources,
            [
                _job(
                    today,
                    "FAILED",
                    job_name="fetch_stock_daily",
                    error="docker down",
                )
            ],
        )
        _risk(sources, today, "BLOCK", 4)
        health_check()
        fired = {
            (kw["alert_type"], kw["alpha_type_id"]): level for level, _, kw in sent
        }
        assert fired[("job_failure", "fetch_stock_daily")] == alerting_level("HIGH")
        assert fired[("risk_reject", "")] == alerting_level("MEDIUM")
        assert fired[("sharpe_negative", "")] == alerting_level("CRITICAL")

    def test_job_failure_fact_converges_on_the_dedup_key(self, sources, sent):
        today = date.today().isoformat()
        _jobs(sources, [_job(today, "FAILED", job_name="compute_all", error="boom")])
        health_check()
        assert len(sent) == 1
        level, _, kw = sent[0]
        assert level == alerting_level("HIGH")
        assert kw["alert_type"] == "job_failure"
        assert kw["alpha_type_id"] == "compute_all"  # same key as the direct path

    def test_corrupt_sources_do_not_kill_the_snapshot(self, sources, sent):
        (sources / "journal").mkdir()
        (sources / "journal" / "2026-11-03.parquet").write_bytes(b"not a parquet")
        (sources / "jobs.jsonl").write_text("{corrupt\n", encoding="utf-8")
        (sources / "signals").mkdir()
        (sources / "signals" / "x").mkdir()
        (sources / "signals" / "x" / "eval_report.json").write_text(
            "{oops", encoding="utf-8"
        )
        summary = health_check()
        assert "12 rows" in summary
        assert sent == []


def alerting_level(name):
    """The alerting level value a firing carries (enum for cond levels)."""
    import ops.alerting as alerting

    return alerting.AlertLevel[name]
