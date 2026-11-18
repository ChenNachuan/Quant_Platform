"""Unit tests for the DQC severity + engine core (S9-T3).

Covers AC2 (``quality_flag & 1`` filtering) and AC3 (HARD drop rate > 20 %
rejects the batch; SOFT flag rate > 50 % alarms but never rejects).

The rule runner is stubbed at its public seam (``engine.run_all_checks``) so
the disposition core — steps 2-7 of dqc_severity.md §37.6 — is exercised on
synthetic CheckResults; one end-to-end smoke test drives the real rule YAMLs
through ``ingest_with_dqc`` on a clean daily batch.
"""

from __future__ import annotations

import json
from datetime import date

import pandas as pd
import pytest

from core.types import QualityFlag as CoreQualityFlag
from data.dqc import engine, severity
from data.dqc.severity import (
    BatchRejectConfig,
    DQCBatchReject,
    DQCSeverityConfig,
    severity_config_for,
)
from data.quarantine import quarantine_path, rejection_path

DATA_DATE = date(2024, 1, 3)


# ── fixtures / helpers ────────────────────────────────────────────────


def _daily_frame(n_tickers: int = 10, date_str: str = "2024-01-03") -> pd.DataFrame:
    """A schema-valid bars_daily batch: *n_tickers* rows on one date."""
    rows = []
    for i in range(1, n_tickers + 1):
        px = 10.0 + i
        rows.append(
            {
                "ticker": f"000{i:03d}.SZ",
                "trade_date": date_str,
                "open": px,
                "high": px * 1.02,
                "low": px * 0.98,
                "close": px * 1.01,
                "volume": 1000 + i,
                "amount": px * 1000.0,
                "adjfactor": 1.0,
                "turnover": 0.5,
                "suspend": False,
                "is_limit_up": False,
                "is_limit_down": False,
                "prev_close": px,
            }
        )
    return pd.DataFrame(rows)


def _minute_frame() -> pd.DataFrame:
    """20 rows of bars_minute: 2 bars in (000001.SZ, 01-03), the rest single."""
    rows = []

    def row(ticker, d, ts, px):
        return {
            "ticker": ticker,
            "trade_date": d,
            "datetime": ts,
            "open": px,
            "high": px * 1.01,
            "low": px * 0.99,
            "close": px,
            "volume": 500,
            "amount": px * 500.0,
        }

    rows.append(row("000001.SZ", "2024-01-03", "2024-01-03 09:31:00", 10.0))
    rows.append(row("000001.SZ", "2024-01-03", "2024-01-03 09:32:00", 10.1))
    for i in range(2, 12):
        rows.append(
            row(f"000{i:03d}.SZ", "2024-01-03", "2024-01-03 09:31:00", 10.0 + i)
        )
    for i in range(2, 10):
        rows.append(
            row(f"000{i:03d}.SZ", "2024-01-04", "2024-01-04 09:31:00", 11.0 + i)
        )
    return pd.DataFrame(rows)


def _fake_result(rule_id: str, df: pd.DataFrame, failing, severity_name: str = "HARD"):
    """Build a CheckResult with a row-aligned failure mask (list of bools)."""
    mask = pd.Series(failing, index=df.index, dtype=bool)
    cfg = severity_config_for(rule_id)
    failing_count = int(mask.sum())
    return engine.CheckResult(
        rule_id=rule_id,
        rule_name=rule_id,
        dimension="legality",
        severity=severity_name,
        action_taken=cfg.row_action,
        total_rows=len(df),
        failing_rows=mask,
        failing_rate=failing_count / len(df),
        sample_tickers=[],
        sample_values=[],
    )


def _fake_runner(hard=(), soft=()):
    """A run_all_checks replacement returning prebuilt results."""

    def run(df, data_date, dqc_group, *, overrides=None, cross_df=None):
        return list(hard), list(soft)

    return run


# ── QualityFlag bitmask (§47.15) ──────────────────────────────────────


def test_quality_flag_reexport_and_bits():
    assert severity.QualityFlag is CoreQualityFlag
    assert int(severity.QualityFlag.CLEAN) == 0
    assert severity.RULE_FLAG_BITS == {
        "C02": 1,
        "C05": 2,
        "L04": 4,
        "L07": 8,
        "K01": 16,
    }


def test_compute_quality_flag_multi_bit():
    df = pd.DataFrame({"ticker": list("abcde")})
    c02 = _fake_result("C02", df, [True, True, False, False, False], "SOFT")
    c05 = _fake_result("C05", df, [False, True, False, False, False], "SOFT")
    flags = severity.compute_quality_flag(df, [c02, c05])
    assert flags.dtype == "int64"
    assert flags.tolist() == [1, 3, 0, 0, 0]


def test_quality_flag_bit0_filter():
    """AC2: ``quality_flag & 1`` filtering keeps rows not flagged by C02."""
    df = pd.DataFrame({"ticker": list("abcde")})
    c02 = _fake_result("C02", df, [True, True, False, False, False], "SOFT")
    c05 = _fake_result("C05", df, [False, True, False, False, False], "SOFT")
    flags = severity.compute_quality_flag(df, [c02, c05])
    kept = df[(flags & 1) == 0]
    dropped = df[(flags & 1) != 0]
    assert list(kept.index) == [2, 3, 4]
    assert list(dropped.index) == [0, 1]
    # the second row fails both rules: bit0 and bit1 set together
    assert flags[1] == (1 | 2)


def test_compute_quality_flag_unmapped_rule_no_bit():
    df = pd.DataFrame({"ticker": list("abc")})
    k04 = _fake_result("K04", df, [True, True, True], "SOFT")
    flags = severity.compute_quality_flag(df, [k04])
    assert flags.tolist() == [0, 0, 0]  # bit5+ reserved, K04 has no bit


# ── disposition table (§37.3) ─────────────────────────────────────────


def test_batch_reject_config_defaults():
    cfg = BatchRejectConfig()
    assert cfg.max_hard_drop_rate == 0.20
    assert cfg.soft_alarm_rate == 0.50


def test_disposition_table_spot_checks():
    expected = (
        {f"C{i:02d}" for i in range(1, 13)}
        | {f"L{i:02d}" for i in range(1, 15)}
        | {f"K{i:02d}" for i in range(1, 13)}
        | {f"T{i:02d}" for i in range(1, 10)}
    )
    assert set(severity.SEVERITY_CONFIGS) == expected  # all 47 rules
    l04 = severity_config_for("L04")
    assert (l04.row_action, l04.escalation_threshold) == ("flag", None)
    t03 = severity_config_for("T03")
    assert (t03.row_action, t03.escalation_threshold, t03.granularity) == (
        "drop",
        0.0,
        "row",
    )
    c04 = severity_config_for("C04")
    assert (c04.row_action, c04.escalation_threshold) == ("drop", 0.20)
    k06 = severity_config_for("K06")
    assert (k06.row_action, k06.escalation_threshold) == ("flag", 0.10)
    t04 = severity_config_for("T04")
    assert (t04.row_action, t04.escalation_threshold, t04.granularity) == (
        "drop",
        None,
        "partition",
    )


def test_severity_config_for_unknown_rule_raises():
    with pytest.raises(KeyError):
        severity_config_for("X99")


def test_severity_config_validation():
    with pytest.raises(ValueError):
        DQCSeverityConfig("nuke")
    with pytest.raises(ValueError):
        DQCSeverityConfig("drop", 0.1, "galaxy")
    with pytest.raises(ValueError):
        DQCSeverityConfig("drop", 1.5)


# ── AC3: batch safety valve ───────────────────────────────────────────


def test_hard_drop_rate_above_valve_rejects_batch(tmp_path, monkeypatch):
    """AC3.1: five rules each under threshold, union 40 % > 20 % -> reject."""
    df = _daily_frame(20)
    hard = [
        _fake_result("L10", df, [i == 0 for i in range(20)]),
        _fake_result("L11", df, [i == 1 for i in range(20)]),
        _fake_result("K02", df, [i == 2 for i in range(20)]),
        _fake_result("K03", df, [i == 3 for i in range(20)]),
        _fake_result("C04", df, [4 <= i <= 7 for i in range(20)]),
    ]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(hard=hard))
    with pytest.raises(DQCBatchReject) as excinfo:
        engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    detail = excinfo.value.detail
    assert detail["batch_status"] == "rejected"
    assert detail["data_date"] == "2024-01-03"
    assert detail["table"] == "bars_daily"
    assert detail["total_rows"] == 20
    assert detail["rows_would_drop"] == 8
    assert detail["drop_rate"] == 0.4
    assert detail["reject_reason"].startswith("global_valve")
    assert set(detail["hard_rules_triggered"]) == {"L10", "L11", "K02", "K03", "C04"}
    # the raw batch is quarantined with its rejection metadata
    q_path = quarantine_path("bars_daily", DATA_DATE, tmp_path)
    m_path = rejection_path("bars_daily", DATA_DATE, tmp_path)
    assert q_path.exists() and m_path.exists()
    assert len(pd.read_parquet(q_path)) == 20
    meta = json.loads(m_path.read_text(encoding="utf-8"))
    assert meta["batch_status"] == "rejected"
    assert meta["drop_rate"] == 0.4


def test_soft_flag_rate_above_alarm_rate_alarms_only(tmp_path, monkeypatch, caplog):
    """AC3.2: flagged rate 60 % > 50 % -> alarm, batch accepted, no reject."""
    df = _daily_frame(10)
    soft = [
        _fake_result("C05", df, [i < 6 for i in range(10)], "SOFT"),
    ]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(soft=soft))
    out = engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert "soft alarm" in caplog.text
    assert (out["quality_flag"] != 0).sum() == 6
    # SOFT never rejects: nothing quarantined, batch written
    assert not quarantine_path("bars_daily", DATA_DATE, tmp_path).exists()
    assert (tmp_path / "asset" / "cn_stock_bars_daily" / "2024-01-03.parquet").exists()


def test_rule_escalation_rejects_batch(tmp_path, monkeypatch):
    """Rule-level escalation fires before the global valve (10 % <= 20 %)."""
    df = _daily_frame(20)
    hard = [_fake_result("L10", df, [i < 2 for i in range(20)])]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(hard=hard))
    with pytest.raises(DQCBatchReject) as excinfo:
        engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    detail = excinfo.value.detail
    assert detail["reject_reason"] == "rule_escalation:L10"
    assert detail["rows_would_drop"] == 2
    assert detail["drop_rate"] == 0.1


def test_single_future_row_rejects_batch(tmp_path, monkeypatch):
    """T03: one future-dated row rejects the whole batch (threshold 0.0)."""
    df = _daily_frame(20)
    hard = [_fake_result("T03", df, [i == 7 for i in range(20)])]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(hard=hard))
    with pytest.raises(DQCBatchReject) as excinfo:
        engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert excinfo.value.detail["reject_reason"] == "rule_escalation:T03"


def test_partition_granularity_expands_drop(tmp_path, monkeypatch):
    """T04 partition granularity drops the whole (ticker, trade_date) group."""
    df = _minute_frame()  # rows 0-1 are (000001.SZ, 2024-01-03)
    hard = [_fake_result("T04", df, [i == 0 for i in range(20)])]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(hard=hard))
    out = engine.ingest_with_dqc(df, DATA_DATE, "bars_minute", data_root=tmp_path)
    assert len(out) == 18
    gone = (out["ticker"] == "000001.SZ") & (out["trade_date"] == "2024-01-03")
    assert not gone.any()  # the sibling bar was expanded away too
    assert (tmp_path / "asset" / "cn_stock_bars_minute" / "2024-01-03.parquet").exists()


def test_partition_boundary_escalation_uses_post_expansion_rate(tmp_path, monkeypatch):
    """Issue #63: partition escalation must compare post-expansion drop rate.

    10 tickers × 10 bars = 100 rows.  1 ticker fails 1 bar (C03).
    raw rate = 1/100 = 1 % — below C03's 5 % escalation_threshold.
    post-expansion = 10/100 = 10 % — above threshold, so escalation fires.
    """
    rows = []
    for t in range(10):
        for b in range(10):
            rows.append(
                {
                    "ticker": f"000{t:03d}.SZ",
                    "trade_date": "2024-01-03",
                    "datetime": f"2024-01-03 09:{b + 31:02d}:00",
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.0,
                    "volume": 500,
                    "amount": 5000.0,
                }
            )
    df = pd.DataFrame(rows)

    # C03: granularity=partition, escalation_threshold=0.05
    # Only 1 row out of 100 fails → raw rate 1 % < 5 %, but expansion lifts to 10 %
    hard = [_fake_result("C03", df, [i == 0 for i in range(100)])]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(hard=hard))

    with pytest.raises(DQCBatchReject) as excinfo:
        engine.ingest_with_dqc(df, DATA_DATE, "bars_minute", data_root=tmp_path)
    assert excinfo.value.detail["reject_reason"] == "rule_escalation:C03"


# ── real-rule smoke test ──────────────────────────────────────────────


def test_ingest_clean_daily_smoke(tmp_path):
    """A clean batch runs every bars_daily rule and lands on disk unflagged."""
    df = _daily_frame(2)
    overrides = {
        "calendar_dates": ["2024-01-03"],
        "universe": ["000001.SZ", "000002.SZ"],
        "daily_tickers": ["000001.SZ", "000002.SZ"],
        "today": "2024-01-03",
    }
    hard, soft = engine.run_all_checks(df, DATA_DATE, "bars_daily", overrides=overrides)
    assert len(hard) + len(soft) >= 20  # cross rules K06-K08 run only with cross_df
    assert all(r.failing_count == 0 for r in hard + soft)
    assert all(r.rule_version == engine.RULE_VERSION for r in hard + soft)

    out = engine.ingest_with_dqc(
        df, DATA_DATE, "bars_daily", overrides=overrides, data_root=tmp_path
    )
    assert len(out) == 2
    assert (out["quality_flag"] == 0).all()
    written = pd.read_parquet(
        tmp_path / "asset" / "cn_stock_bars_daily" / "2024-01-03.parquet"
    )
    assert len(written) == 2
    assert set(written.columns) == set(engine.build_schema("bars_daily").columns)


# ── Webhook, run_id, quality_score & thresholds decoupling ────────────


def test_compute_quality_score():
    from data.dqc.report import compute_quality_score

    df = _daily_frame(4)
    h_pass = _fake_result("L01", df, [False] * 4, "HARD")
    h_fail = _fake_result("L01", df, [True, False, False, False], "HARD")
    s_pass = _fake_result("C05", df, [False] * 4, "SOFT")
    s_fail = _fake_result("C05", df, [True, False, False, False], "SOFT")
    s_fail2 = _fake_result("L04", df, [True, False, False, False], "SOFT")

    assert compute_quality_score([h_pass], [s_pass], 4) == 100.0
    assert compute_quality_score([h_fail], [s_fail, s_fail2], 4) == 86.0
    # Clamped at 0.0 when deductions exceed 100
    assert compute_quality_score([h_fail] * 12, [s_fail] * 5, 4) == 0.0


def test_run_id_and_quality_score_written_to_reports(tmp_path, monkeypatch):
    from data.dqc import report

    df = _daily_frame(10)
    soft = [_fake_result("C05", df, [i == 0 for i in range(10)], "SOFT")]
    monkeypatch.setattr(engine, "run_all_checks", _fake_runner(soft=soft))
    engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)

    summary = report.load_summary(tmp_path)
    detail = report.load_detail(tmp_path)
    assert len(summary) == 1
    run_id = summary.iloc[0]["run_id"]
    assert isinstance(run_id, str) and len(run_id) == 8
    assert (detail["run_id"] == run_id).all()
    assert summary.iloc[0]["quality_score"] == 98.0


def test_webhook_fault_isolation_and_notifications(tmp_path, monkeypatch):
    import requests

    from data.dqc import notify, report

    df = _daily_frame(10)
    # 1) DQC_WEBHOOK_URL unset -> accepted and rejected work cleanly
    monkeypatch.delenv("DQC_WEBHOOK_URL", raising=False)
    monkeypatch.setattr(
        engine,
        "run_all_checks",
        _fake_runner(
            soft=[_fake_result("C05", df, [i < 6 for i in range(10)], "SOFT")]
        ),
    )
    out = engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert len(out) == 10

    # 2) Invalid/failing webhook URL -> ConnectionError must NOT escape ingest_with_dqc
    monkeypatch.setenv("DQC_WEBHOOK_URL", "http://127.0.0.1:9/invalid-hook")

    def _boom(*args, **kwargs):
        raise requests.ConnectionError("network unreachable")

    monkeypatch.setattr(notify.requests, "post", _boom)
    out2 = engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert len(out2) == 10

    # Rejected batch still raises DQCBatchReject (not ConnectionError)
    monkeypatch.setattr(
        engine,
        "run_all_checks",
        _fake_runner(hard=[_fake_result("T03", df, [i == 0 for i in range(10)])]),
    )
    with pytest.raises(DQCBatchReject) as excinfo:
        engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert len(excinfo.value.detail["run_id"]) == 8

    # 3) Capture actual webhook payloads on WARN and REJECT
    sent: list[dict] = []

    def _capture(url, json=None, timeout=None):
        sent.append({"url": url, "json": json, "timeout": timeout})

    monkeypatch.setattr(notify.requests, "post", _capture)
    monkeypatch.setattr(
        engine,
        "run_all_checks",
        _fake_runner(
            soft=[_fake_result("C05", df, [i < 6 for i in range(10)], "SOFT")]
        ),
    )
    engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert len(sent) == 1
    assert "[DQC WARN][run_id=" in sent[0]["json"]["content"]["text"]

    monkeypatch.setattr(
        engine,
        "run_all_checks",
        _fake_runner(hard=[_fake_result("T03", df, [i == 0 for i in range(10)])]),
    )
    with pytest.raises(DQCBatchReject):
        engine.ingest_with_dqc(df, DATA_DATE, "bars_daily", data_root=tmp_path)
    assert len(sent) == 2
    assert "[DQC REJECT][run_id=" in sent[1]["json"]["content"]["text"]
    assert report.load_summary(tmp_path).iloc[-1]["quality_score"] == 90.0


def test_dqc_thresholds_yaml_matches_inline_rules_and_supports_override(tmp_path):
    import yaml

    empty_thr = tmp_path / "empty_thresholds.yaml"
    empty_thr.write_text("thresholds: {}\n", encoding="utf-8")

    for group in engine.DQC_RULE_FILES:
        with_file = engine._load_rules(group)
        without_file = engine._load_rules(group, thresholds_path=empty_thr)
        assert with_file == without_file, (
            f"dqc_thresholds.yaml diverged from inline rules for group={group}"
        )

    # Verify escalation_threshold parity across all 47 rules
    for rid, base_cfg in severity.SEVERITY_CONFIGS.items():
        assert severity_config_for(rid) == base_cfg

    # Verify custom threshold override works
    custom_thr = tmp_path / "custom_thresholds.yaml"
    custom_thr.write_text(
        yaml.safe_dump(
            {
                "thresholds": {
                    "L01": {"value": 0.5, "escalation_threshold": 0.05},
                    "L04": {"params": {"tolerance": 0.01}},
                }
            }
        ),
        encoding="utf-8",
    )
    daily_rules = {
        r["id"]: r for r in engine._load_rules("bars_daily", thresholds_path=custom_thr)
    }
    assert daily_rules["L01"]["value"] == 0.5
    assert daily_rules["L04"]["params"]["tolerance"] == 0.01
    assert (
        daily_rules["L04"]["params"]["main_limit"] == 0.10
    )  # preserved inline default
    assert (
        severity_config_for("L01", thresholds_path=custom_thr).escalation_threshold
        == 0.05
    )


def test_legacy_report_parquet_backcompat(tmp_path):
    from data.dqc import report

    # Write legacy summary & detail without run_id / quality_score
    s_path = report.summary_path(tmp_path)
    s_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_summary_cols = [
        c for c in report.SUMMARY_COLUMNS if c not in ("run_id", "quality_score")
    ]
    row = report.build_summary_row(
        DATA_DATE,
        "bars_daily",
        total_rows=10,
        rows_dropped=0,
        rows_flagged=0,
        batch_status="accepted",
        hard_failed=[],
        soft_failed=[],
        elapsed_seconds=0.1,
    )
    pd.DataFrame([{k: row[k] for k in legacy_summary_cols}]).to_parquet(s_path)

    loaded = report.load_summary(tmp_path)
    assert list(loaded.columns) == report.SUMMARY_COLUMNS
    assert loaded.iloc[0]["run_id"] == ""
    assert pd.isna(loaded.iloc[0]["quality_score"])

    report.append_summary(row, data_root=tmp_path)
    loaded2 = report.load_summary(tmp_path)
    assert len(loaded2) == 2
