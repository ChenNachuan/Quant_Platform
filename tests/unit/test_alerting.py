"""Tests for ops.alerting — dedup key, severity only-up, channels, 8 conditions.

Covers the ac-1..ac-4 rows: exactly one alerts.jsonl row per
``{date}_{alert_type}_{alpha_type_id}`` per day (state survives a restart),
severity that only ever rises (equal/lower is a no-op, an upgrade rewrites the
day's row upward and re-fires), CRITICAL-only desktop notify against a
terminal channel at every level, and all 8 conditions firing at the design
table's level + type (nulls never fire).
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

import pytest

import ops.alerting as alerting
from ops.alerting import (
    ALERT_CONDITIONS,
    AlertLevel,
    desktop_notify,
    evaluate_alerts,
    log_alert,
    reset_state,
    send_alert,
)

# One row per condition: (alert_type, level, rule, breach facts, quiet facts,
# expected alpha_type_id scope). Quiet facts sit exactly at the threshold, so
# the table doubles as the boundary check.
CONDITION_CASES = [
    (
        "job_failure",
        AlertLevel.HIGH,
        "threshold",
        {"failed_jobs": [{"job_name": "fetch_stock_daily", "error": "boom"}]},
        {"failed_jobs": []},
        "fetch_stock_daily",
    ),
    (
        "data_delay",
        AlertLevel.HIGH,
        "threshold",
        {"data_delay_days": 2.0},
        {"data_delay_days": 1.0},
        "",
    ),
    (
        "mdd_breach",
        AlertLevel.CRITICAL,
        "mutation",
        {"mdd": 0.16, "backtest_mdd": 0.10, "alpha_type_id": "alpha-x"},
        {"mdd": 0.15, "backtest_mdd": 0.10, "alpha_type_id": "alpha-x"},
        "alpha-x",
    ),
    (
        "sharpe_negative",
        AlertLevel.CRITICAL,
        "trend",
        {"sharpe_6m": -0.2, "alpha_type_id": "alpha-x"},
        {"sharpe_6m": 0.0, "alpha_type_id": "alpha-x"},
        "alpha-x",
    ),
    (
        "excess_negative",
        AlertLevel.HIGH,
        "trend",
        {"excess_negative_months": 6, "alpha_type_id": "alpha-x"},
        {"excess_negative_months": 5, "alpha_type_id": "alpha-x"},
        "alpha-x",
    ),
    (
        "nav_drop",
        AlertLevel.MEDIUM,
        "mutation",
        {"nav_daily_return": -0.04, "alpha_type_id": "alpha-x"},
        {"nav_daily_return": -0.03, "alpha_type_id": "alpha-x"},
        "alpha-x",
    ),
    (
        "turnover_excess",
        AlertLevel.MEDIUM,
        "threshold",
        {"turnover": 1.6, "alpha_type_id": "alpha-x"},
        {"turnover": 1.5, "alpha_type_id": "alpha-x"},
        "alpha-x",
    ),
    (
        "risk_reject",
        AlertLevel.MEDIUM,
        "threshold",
        {"risk_reject_count": 4},
        {"risk_reject_count": 3},
        "",
    ),
]


@pytest.fixture(autouse=True)
def alerts_file(tmp_path, monkeypatch):
    """Point the alert log at a per-test tmp file with empty dedup state."""
    path = tmp_path / "alerts.jsonl"
    monkeypatch.setattr(alerting, "ALERTS_JSONL", path)
    reset_state()
    yield path
    reset_state()


@pytest.fixture(autouse=True)
def osa_calls(monkeypatch):
    """Record osascript invocations instead of popping real notifications."""
    calls: list[list[str]] = []

    def fake_run(cmd, **kwargs):
        calls.append(list(cmd))
        return None

    monkeypatch.setattr(alerting.subprocess, "run", fake_run)
    return calls


def _rows():
    path = alerting.ALERTS_JSONL
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


# ---------------------------------------------------------------------------
# ac-1: one row per {date}_{alert_type}_{alpha_type_id} per day
# ---------------------------------------------------------------------------


class TestDedupKey:
    def test_same_key_twice_lands_exactly_one_row(self, capsys):
        send_alert("HIGH", "first", alert_type="job_failure", alpha_type_id="j1")
        send_alert("HIGH", "second", alert_type="job_failure", alpha_type_id="j1")
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["msg"] == "first"
        assert capsys.readouterr().out.count("[HIGH]") == 1

    def test_key_day_part_is_yyyymmdd(self):
        send_alert("HIGH", "m", alert_type="data_delay")
        today = date.today()
        assert _rows()[0]["key"] == f"{today:%Y%m%d}_data_delay_"

    def test_key_carries_alpha_type_id(self):
        send_alert("HIGH", "x", alert_type="mdd_breach", alpha_type_id="alpha-x")
        send_alert("HIGH", "y", alert_type="mdd_breach", alpha_type_id="alpha-y")
        keys = sorted(row["key"] for row in _rows())
        day = f"{date.today():%Y%m%d}"
        assert keys == [f"{day}_mdd_breach_alpha-x", f"{day}_mdd_breach_alpha-y"]

    def test_key_carries_alert_type(self):
        send_alert("HIGH", "d", alert_type="data_delay")
        send_alert("HIGH", "n", alert_type="nav_drop")
        assert len(_rows()) == 2

    def test_state_survives_restart(self, capsys):
        send_alert("HIGH", "first", alert_type="job_failure", alpha_type_id="j1")
        reset_state()  # a restart reseeds from the day's rows
        send_alert("HIGH", "again", alert_type="job_failure", alpha_type_id="j1")
        assert len(_rows()) == 1
        assert capsys.readouterr().out.count("[HIGH]") == 1

    def test_other_dates_do_not_dedup_today(self):
        yesterday = date.today() - timedelta(days=1)
        stale = {
            "date": yesterday.isoformat(),
            "ts": datetime.now().isoformat(timespec="seconds"),
            "level": "HIGH",
            "msg": "stale",
            "alert_type": "data_delay",
            "alpha_type_id": "",
            "key": f"{yesterday:%Y%m%d}_data_delay_",
        }
        log_alert(stale)
        reset_state()
        send_alert("HIGH", "today", alert_type="data_delay")
        send_alert("HIGH", "today again", alert_type="data_delay")
        rows = _rows()
        assert len(rows) == 2
        assert rows[0] == stale

    def test_row_carries_the_pinned_schema(self):
        send_alert("MEDIUM", "msg text", alert_type="nav_drop", alpha_type_id="alpha-x")
        row = _rows()[0]
        assert set(row) == {
            "date",
            "ts",
            "level",
            "msg",
            "alert_type",
            "alpha_type_id",
            "key",
        }
        assert row["date"] == date.today().isoformat()
        assert row["level"] == "MEDIUM"
        assert row["msg"] == "msg text"
        assert row["alert_type"] == "nav_drop"
        assert row["alpha_type_id"] == "alpha-x"

    def test_log_alert_is_a_raw_append(self):
        log_alert({"level": "HIGH", "msg": "raw", "key": "k"})
        assert _rows() == [{"level": "HIGH", "msg": "raw", "key": "k"}]


# ---------------------------------------------------------------------------
# ac-2: severity only rises per key/day
# ---------------------------------------------------------------------------


class TestSeverityOnlyUp:
    def test_equal_level_refire_is_noop(self, capsys):
        send_alert("HIGH", "one", alert_type="data_delay")
        send_alert("HIGH", "two", alert_type="data_delay")
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["msg"] == "one"
        assert capsys.readouterr().out.count("[HIGH]") == 1

    def test_lower_level_never_downgrades(self, capsys, osa_calls):
        send_alert("CRITICAL", "top", alert_type="mdd_breach")
        send_alert("MEDIUM", "down", alert_type="mdd_breach")
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["level"] == "CRITICAL"
        assert rows[0]["msg"] == "top"
        assert capsys.readouterr().out.count("[CRITICAL]") == 1
        assert len(osa_calls) == 1

    def test_higher_level_rewrites_row_up(self, capsys):
        send_alert("MEDIUM", "low", alert_type="sharpe_negative")
        send_alert("CRITICAL", "escalated", alert_type="sharpe_negative")
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["level"] == "CRITICAL"
        assert rows[0]["msg"] == "escalated"
        out = capsys.readouterr().out
        assert "[MEDIUM] low" in out
        assert "[CRITICAL] escalated" in out

    def test_escalation_reaches_the_desktop(self, osa_calls):
        send_alert("MEDIUM", "low", alert_type="nav_drop")
        assert osa_calls == []
        send_alert("CRITICAL", "up", alert_type="nav_drop")
        assert len(osa_calls) == 1

    def test_medium_to_high_stays_off_the_desktop(self, osa_calls):
        send_alert("MEDIUM", "low", alert_type="turnover_excess")
        send_alert("HIGH", "up", alert_type="turnover_excess")
        assert osa_calls == []
        assert _rows()[0]["level"] == "HIGH"

    def test_upgrade_chain_stops_at_the_top(self):
        for level in ("MEDIUM", "HIGH", "CRITICAL", "HIGH"):
            send_alert(level, f"at {level}", alert_type="excess_negative")
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["level"] == "CRITICAL"
        assert rows[0]["msg"] == "at CRITICAL"


# ---------------------------------------------------------------------------
# ac-3: channels — terminal at every level, desktop (osascript) at CRITICAL
# ---------------------------------------------------------------------------


class TestChannels:
    def test_critical_fires_terminal_and_desktop(self, capsys, osa_calls):
        send_alert(AlertLevel.CRITICAL, "desk me", alert_type="nav_drop")
        assert "[CRITICAL] desk me" in capsys.readouterr().out
        assert len(osa_calls) == 1

    def test_high_is_terminal_only(self, capsys, osa_calls):
        send_alert("HIGH", "term me", alert_type="job_failure")
        assert "[HIGH] term me" in capsys.readouterr().out
        assert osa_calls == []

    def test_medium_is_terminal_only(self, capsys, osa_calls):
        send_alert("MEDIUM", "term me", alert_type="risk_reject")
        assert "[MEDIUM] term me" in capsys.readouterr().out
        assert osa_calls == []

    def test_desktop_notify_invokes_osascript(self, osa_calls):
        desktop_notify('alert "x"')
        assert len(osa_calls) == 1
        cmd = osa_calls[0]
        assert cmd[0] == "osascript"
        assert cmd[1] == "-e"
        assert cmd[2] == 'display notification "alert \\"x\\"" with title "Quant Alert"'

    def test_desktop_notify_falls_back_without_osascript(self, monkeypatch, capsys):
        def raiser(cmd, **kwargs):
            raise FileNotFoundError("osascript")

        monkeypatch.setattr(alerting.subprocess, "run", raiser)
        desktop_notify("no desktop")
        assert "[desktop] no desktop" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ac-4: the 8 conditions fire at the table's level + type
# ---------------------------------------------------------------------------


class TestConditions:
    def test_table_has_the_eight_design_rows(self):
        assert len(ALERT_CONDITIONS) == 8
        assert [c.id for c in ALERT_CONDITIONS] == list(range(1, 9))
        assert {(c.alert_type, c.level, c.rule) for c in ALERT_CONDITIONS} == {
            ("job_failure", AlertLevel.HIGH, "threshold"),
            ("data_delay", AlertLevel.HIGH, "threshold"),
            ("mdd_breach", AlertLevel.CRITICAL, "mutation"),
            ("sharpe_negative", AlertLevel.CRITICAL, "trend"),
            ("excess_negative", AlertLevel.HIGH, "trend"),
            ("nav_drop", AlertLevel.MEDIUM, "mutation"),
            ("turnover_excess", AlertLevel.MEDIUM, "threshold"),
            ("risk_reject", AlertLevel.MEDIUM, "threshold"),
        }

    @pytest.mark.parametrize(
        "alert_type,level,rule,breach,quiet,scope",
        CONDITION_CASES,
        ids=[case[0] for case in CONDITION_CASES],
    )
    def test_condition_fires_at_table_level_and_type(
        self, alert_type, level, rule, breach, quiet, scope
    ):
        firings = evaluate_alerts(breach)
        assert len(firings) == 1
        firing = firings[0]
        assert firing["alert_type"] == alert_type
        assert firing["level"] is level
        assert firing["rule"] == rule
        assert firing["alpha_type_id"] == scope
        assert firing["msg"]
        assert evaluate_alerts(quiet) == []

    @pytest.mark.parametrize(
        "alert_type,level,rule,breach,quiet,scope",
        CONDITION_CASES,
        ids=[case[0] for case in CONDITION_CASES],
    )
    def test_firings_reach_the_log_at_table_level_and_type(
        self, alert_type, level, rule, breach, quiet, scope
    ):
        for firing in evaluate_alerts(breach):
            send_alert(
                firing["level"],
                firing["msg"],
                firing["alert_type"],
                firing["alpha_type_id"],
            )
        rows = _rows()
        assert len(rows) == 1
        assert rows[0]["alert_type"] == alert_type
        assert rows[0]["level"] == level.value

    def test_job_failure_fires_per_failed_job(self):
        facts = {
            "failed_jobs": [
                {"job_name": "compute_all", "error": "boom"},
                {"job_name": "health_check", "error": None},
            ]
        }
        firings = evaluate_alerts(facts)
        assert [(f["alert_type"], f["alpha_type_id"]) for f in firings] == [
            ("job_failure", "compute_all"),
            ("job_failure", "health_check"),
        ]
        assert all(f["level"] is AlertLevel.HIGH for f in firings)
        assert "unknown error" in firings[1]["msg"]

    def test_excess_negative_is_inclusive_at_six_months(self):
        facts = {"excess_negative_months": 6, "alpha_type_id": "alpha-x"}
        assert len(evaluate_alerts(facts)) == 1
        facts["excess_negative_months"] = 6.0
        assert len(evaluate_alerts(facts)) == 1

    def test_nulls_never_fire(self):
        assert evaluate_alerts({}) == []
        assert (
            evaluate_alerts(
                {
                    "alpha_type_id": "alpha-x",
                    "failed_jobs": None,
                    "data_delay_days": None,
                    "mdd": None,
                    "backtest_mdd": None,
                    "sharpe_6m": None,
                    "excess_negative_months": None,
                    "nav_daily_return": None,
                    "turnover": None,
                    "risk_reject_count": None,
                }
            )
            == []
        )

    def test_partial_sources_do_not_fire(self):
        assert evaluate_alerts({"mdd": 0.9, "backtest_mdd": None}) == []
        assert evaluate_alerts({"mdd": None, "backtest_mdd": 0.1}) == []

    def test_non_mapping_facts_are_rejected(self):
        with pytest.raises(TypeError):
            evaluate_alerts([1, 2, 3])
