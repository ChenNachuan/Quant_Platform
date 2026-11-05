"""Tests for ops.reporting — daily report, Alpha-Beta fit, attribution read.

Three lines named by the ticket, plus the two seams the ACs pin:

* **日报结构** (ac-3) — ``daily_{date}.md`` carries the metrics table, the alert
  list and the nav curve link, asserted row by row rather than by "some table
  exists".
* **归因数值精度** (ac-1) — a constructed ``r_p = 0.01 + 0.8·r_b + noise``
  recovers ``alpha_annual ≈ 0.12`` and ``beta ≈ 0.8`` inside the 0.02 tolerance
  the ticket states, and the fit exposes the five fields of ac-2.
* **归因报告解读三线** — the design's three interpretation lines each fire on
  their own condition and stay silent when the source is missing.
* **ac-4** — the factor-contribution breakdown is
  ``research.factor_combiner.factor_contribution`` called verbatim, not a copy.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from scipy.stats import linregress

import ops.reporting as reporting
import research.factor_combiner as factor_combiner
from ops.alerting import AlertLevel
from ops.reporting import (
    ANNUALIZATION,
    alpha_beta_attribution,
    interpret_attribution,
    write_attribution_report,
    write_daily_report,
)

DAY = "2026-11-05"

# The ticket's construction: r_p = 0.01 + 0.8·r_b + noise.  120 monthly points
# is the design's 月频×12 basis; noise std 0.0025 against benchmark std 0.05
# keeps the OLS estimates well inside the stated 0.02 tolerance for any seed,
# so the assertion tests the estimator rather than the draw.
NOISE_STD = 0.0025
BENCH_STD = 0.05
ALPHA_MONTHLY = 0.01
BETA = 0.8
N_OBS = 120


@pytest.fixture()
def reports(tmp_path, monkeypatch):
    monkeypatch.setattr(reporting, "REPORTS_DIR", tmp_path)
    return tmp_path


@pytest.fixture()
def returns():
    rng = np.random.default_rng(20261105)
    r_b = pd.Series(rng.normal(0.0, BENCH_STD, N_OBS))
    noise = pd.Series(rng.normal(0.0, NOISE_STD, N_OBS))
    r_p = ALPHA_MONTHLY + BETA * r_b + noise
    return r_p, r_b


@pytest.fixture()
def metrics():
    return {
        "strategy": {
            "cum_return": {"value": 0.1234, "status": "healthy"},
            "sharpe_6m": {"value": 1.25, "status": "healthy"},
            "mdd": {"value": None, "status": "warning"},
            "position_count": {"value": 18, "status": "healthy"},
        },
        "system": {
            "job_success_rate_7d": {"value": 1.0, "status": "ok"},
            "data_delay_days": {"value": 0, "status": "ok"},
        },
    }


@pytest.fixture()
def alerts():
    return [
        {
            "alert_type": "job_failure",
            "level": AlertLevel.HIGH,
            "msg": "job fetch_stock_daily failed: boom",
            "alpha_type_id": "momentum",
            "rule": "threshold",
        },
        {
            "alert_type": "mdd",
            "level": AlertLevel.MEDIUM,
            "msg": "mdd 0.18 breaches 1.2x backtest",
            "alpha_type_id": "momentum",
            "rule": "threshold",
        },
    ]


@pytest.fixture()
def nav():
    return pd.Series(
        [1.0, 1.01, 1.021, 1.014, 1.03],
        index=pd.to_datetime(
            ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08"]
        ),
    )


def _frames():
    """A two-date, two-ticker book whose contributions are hand-checkable.

    weights 0.5 on both tickers both dates, exposures momentum (1.0, 0.5) and
    value (1.0, 1.0), factor returns +0.02 / -0.01 on both dates.  Per date
    ``Σ_j w·x`` is 0.75 for momentum and 1.00 for value, so over two dates
    momentum = 2·0.75·0.02 = 0.03 and value = 2·1.00·(-0.01) = -0.02.  Shares
    normalised on the absolute sum (0.05) are 0.6 and -0.4.
    """
    dates = ["2026-01-30", "2026-02-27"]
    weights = pd.DataFrame(
        [
            {"date": d, "ticker": t, "weight": 0.5}
            for d in dates
            for t in ("600000", "600036")
        ]
    )
    factor_returns = pd.DataFrame(
        [
            {"date": d, "factor_name": name, "factor_return": ret}
            for d in dates
            for name, ret in (("momentum", 0.02), ("value", -0.01))
        ]
    )
    factor_exposures = pd.DataFrame(
        [
            {"date": d, "ticker": t, "momentum": mom, "value": val}
            for d in dates
            for t, mom, val in (
                ("600000", 1.0, 1.0),
                ("600036", 0.5, 1.0),
            )
        ]
    )
    return weights, factor_returns, factor_exposures


# ---------------------------------------------------------------------------
# 归因数值精度 (ac-1) + 五字段 (ac-2)
# ---------------------------------------------------------------------------


class TestAlphaBetaAccuracy:
    def test_recovers_alpha_and_beta_inside_the_stated_tolerance(self, returns):
        r_p, r_b = returns
        result = alpha_beta_attribution(r_p, r_b)

        assert abs(result["alpha_annual"] - 0.12) <= 0.02
        assert abs(result["beta"] - 0.8) <= 0.02

    def test_returns_exactly_the_five_fields(self, returns):
        r_p, r_b = returns
        result = alpha_beta_attribution(r_p, r_b)

        assert set(result) == {
            "alpha_monthly",
            "alpha_annual",
            "beta",
            "r_squared",
            "t_alpha",
        }
        for name, value in result.items():
            assert value is None or isinstance(value, float), name

    def test_alpha_annual_is_the_intercept_times_annualization(self, returns):
        r_p, r_b = returns
        result = alpha_beta_attribution(r_p, r_b)

        assert result["alpha_annual"] == pytest.approx(
            result["alpha_monthly"] * ANNUALIZATION, abs=1e-12
        )

    def test_t_alpha_is_intercept_over_standard_error(self, returns):
        r_p, r_b = returns
        fit = linregress(r_b.to_numpy(), r_p.to_numpy())
        result = alpha_beta_attribution(r_p, r_b)

        assert result["t_alpha"] == pytest.approx(fit.intercept / fit.stderr, abs=1e-9)

    def test_drops_nan_pairs_instead_of_failing(self):
        r_b = pd.Series([0.01, 0.02, np.nan, 0.03, 0.04, 0.05])
        r_p = pd.Series([0.01, 0.02, 0.03, np.nan, 0.04, 0.05])
        result = alpha_beta_attribution(r_p, r_b)

        assert result["beta"] == pytest.approx(1.0, abs=1e-9)
        assert result["alpha_monthly"] == pytest.approx(0.0, abs=1e-9)

    def test_rejects_length_mismatch(self):
        with pytest.raises(ValueError, match="length mismatch"):
            alpha_beta_attribution([0.01, 0.02], [0.01, 0.02, 0.03])

    def test_rejects_under_two_usable_observations(self):
        with pytest.raises(ValueError, match=">= 2 paired observations"):
            alpha_beta_attribution([0.01, np.nan], [0.02, np.nan])

    def test_rejects_flat_benchmark(self):
        with pytest.raises(ValueError, match="no variance"):
            alpha_beta_attribution([0.01, 0.02, 0.03], [0.0, 0.0, 0.0])


# ---------------------------------------------------------------------------
# 日报结构 (ac-3)
# ---------------------------------------------------------------------------


class TestDailyReport:
    def test_writes_the_report_to_the_stated_path(self, reports, metrics, alerts):
        path = write_daily_report(DAY, metrics, alerts)

        assert path == str(reports / f"daily_{DAY}.md")
        assert Path(path).is_file()

    def test_metrics_table_carries_every_row(self, reports, metrics, alerts):
        content = Path(write_daily_report(DAY, metrics, alerts)).read_text("utf-8")

        assert "## 策略指标" in content
        assert "## 系统指标" in content
        assert "| 指标 | 值 | 状态 |" in content
        assert "| cum_return | 0.1234 | healthy |" in content
        assert "| sharpe_6m | 1.2500 | healthy |" in content
        assert "| mdd | — | warning |" in content
        assert "| position_count | 18 | healthy |" in content
        assert "| job_success_rate_7d | 1.0000 | ok |" in content
        assert "| data_delay_days | 0 | ok |" in content

    def test_alert_list_carries_level_and_message(self, reports, metrics, alerts):
        content = Path(write_daily_report(DAY, metrics, alerts)).read_text("utf-8")

        assert "## 告警" in content
        assert "- [HIGH] job fetch_stock_daily failed: boom" in content
        assert "- [MEDIUM] mdd 0.18 breaches 1.2x backtest" in content

    def test_nav_curve_link_is_present(self, reports, metrics, alerts, nav):
        content = Path(write_daily_report(DAY, metrics, alerts, nav)).read_text("utf-8")

        assert "## Nav 曲线" in content
        assert f"![nav curve](nav_{DAY}.png)" in content

    def test_nav_png_is_written_when_a_series_is_supplied(
        self, reports, metrics, alerts, nav
    ):
        write_daily_report(DAY, metrics, alerts, nav)

        png = reports / f"nav_{DAY}.png"
        assert png.is_file()
        assert png.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"

    def test_nav_link_survives_a_missing_series(self, reports, metrics, alerts):
        content = Path(write_daily_report(DAY, metrics, alerts)).read_text("utf-8")

        assert f"![nav curve](nav_{DAY}.png)" in content
        assert not (reports / f"nav_{DAY}.png").exists()

    def test_empty_alerts_are_marked_not_omitted(self, reports, metrics):
        content = Path(write_daily_report(DAY, metrics, [])).read_text("utf-8")

        assert "## 告警" in content
        assert "- 无告警" in content

    def test_rejects_a_non_mapping_metrics(self, reports, alerts):
        with pytest.raises(TypeError, match="metrics must be a mapping"):
            write_daily_report(DAY, [{"value": 1}], alerts)


# ---------------------------------------------------------------------------
# 归因报告解读三线
# ---------------------------------------------------------------------------


class TestInterpretation:
    def test_alpha_above_eight_percent_calls_for_a_backtest_audit(self):
        lines = interpret_attribution({"alpha_annual": 0.12, "r_squared": 0.8})

        assert "Alpha 年化 > 8%：查回测问题（未来函数/费用遗漏）——太高不真实" in lines
        assert not any("优秀散户" in line for line in lines)

    def test_alpha_in_the_three_to_five_band_reads_as_excellent_retail(self):
        lines = interpret_attribution({"alpha_annual": 0.04, "r_squared": 0.8})

        assert "Alpha 年化 3-5%：优秀散户水平" in lines

    def test_r_squared_above_0_7_calls_the_alpha_trustworthy(self):
        lines = interpret_attribution({"alpha_annual": 0.04, "r_squared": 0.85})

        assert "R² > 0.7：beta 解释力强，alpha 可信" in lines

    def test_r_squared_below_0_5_flags_the_sample(self):
        lines = interpret_attribution({"alpha_annual": 0.01, "r_squared": 0.31})

        assert "R² < 0.5：超额波动大，样本不足" in lines

    def test_silent_when_the_source_is_missing(self):
        assert interpret_attribution({"alpha_annual": None, "r_squared": None}) == []
        assert interpret_attribution({}) == []

    def test_rejects_a_non_mapping(self):
        with pytest.raises(TypeError, match="must be a mapping"):
            interpret_attribution([("alpha_annual", 0.12)])


# ---------------------------------------------------------------------------
# ac-4 — the factor-contribution breakdown reuses research.factor_combiner
# ---------------------------------------------------------------------------


class TestFactorContributionReuse:
    def test_reporting_uses_the_research_function_not_a_copy(self):
        assert reporting.factor_contribution is factor_combiner.factor_contribution

    def test_attribution_report_calls_it_with_the_callers_frames(
        self, reports, monkeypatch
    ):
        weights, factor_returns, factor_exposures = _frames()
        seen: dict[str, object] = {}

        def spy(w, fr, fe):
            seen["args"] = (w, fr, fe)
            return pd.Series({"momentum": 0.6, "value": 0.4})

        monkeypatch.setattr(reporting, "factor_contribution", spy)
        r_p, r_b = pd.Series([0.01, 0.02]), pd.Series([0.01, 0.02])
        write_attribution_report(
            "momentum",
            "2026Q4",
            r_p,
            r_b,
            weights=weights,
            factor_returns=factor_returns,
            factor_exposures=factor_exposures,
        )

        assert seen["args"] == (weights, factor_returns, factor_exposures)
        assert seen["args"][0] is weights

    def test_report_renders_the_research_shares_verbatim(self, reports):
        weights, factor_returns, factor_exposures = _frames()
        r_p = pd.Series([0.01, 0.02, 0.03])
        r_b = pd.Series([0.01, 0.02, 0.025])

        path = write_attribution_report(
            "momentum",
            "2026Q4",
            r_p,
            r_b,
            weights=weights,
            factor_returns=factor_returns,
            factor_exposures=factor_exposures,
        )
        content = Path(path).read_text("utf-8")

        assert path == str(reports / "attribution_momentum_2026Q4.md")
        assert "| momentum | 0.6000 |" in content
        assert "| value | -0.4000 |" in content

    def test_report_carries_the_fit_the_breakdown_and_the_read(self, reports):
        weights, factor_returns, factor_exposures = _frames()
        r_p = pd.Series([0.01, 0.02, 0.03])
        r_b = pd.Series([0.01, 0.02, 0.025])

        content = Path(
            write_attribution_report(
                "momentum",
                "2026Q4",
                r_p,
                r_b,
                weights=weights,
                factor_returns=factor_returns,
                factor_exposures=factor_exposures,
            )
        ).read_text("utf-8")

        assert "## Alpha-Beta 回归" in content
        assert "| alpha_monthly |" in content
        assert "| alpha_annual |" in content
        assert "| beta |" in content
        assert "| r_squared |" in content
        assert "| t_alpha |" in content
        assert "## 因子贡献拆解" in content
        assert "## 解读" in content
        assert "- R² > 0.7：beta 解释力强，alpha 可信" in content
