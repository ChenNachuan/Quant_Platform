"""Golden-sample step 5: five-year fixture and month-end clock (QUANT-32 iter 1).

Zero-network tests over the committed five-year fixture.  The fetch script
(``fetch_five_year_etf.py``) is its provenance; no test opens a socket.

Iteration 1 covers ac-4: fixture structure, month-end clock anchors, and the
no-splice rule.  Iteration 2 adds the baseline metrics and range assertions.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tests.golden.month_end_clock import month_end_decision_dates

pytestmark = pytest.mark.golden

FIXTURE_DIR = Path(__file__).parent
UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    path = FIXTURE_DIR / "golden_prices_2020_2024.parquet"
    if not path.exists():
        pytest.skip(f"fixture not generated: {path}")
    return pd.read_parquet(path)


class TestFixtureStructure:
    """ac-4: the five-year fixture is well-formed and reproducible."""

    def test_columns(self, prices):
        expected = ["date", "phase"] + [
            f"{t}.{f}" for t in UNIVERSE for f in ("open", "close")
        ]
        assert sorted(prices.columns) == sorted(expected)

    def test_warmup_count(self, prices):
        assert (prices["phase"] == "warmup").sum() == 20

    def test_sample_count(self, prices):
        n = (prices["phase"] == "sample").sum()
        assert 1200 <= n <= 1250  # ~5 years × 252 trading days

    def test_date_range(self, prices):
        assert str(prices["date"].min()).split()[0] <= "2019-12-31"
        assert str(prices["date"].max()).split()[0] == "2024-12-31"

    def test_no_missing_values(self, prices):
        for t in UNIVERSE:
            assert prices[f"{t}.open"].notna().all()
            assert prices[f"{t}.close"].notna().all()

    def test_true_opens(self, prices):
        """O1: five-year bars carry true opens — open(D) != close(D-1) in general."""
        sample = prices[prices["phase"] == "sample"]
        for t in UNIVERSE[:2]:
            opens = sample[f"{t}.open"].to_numpy()[1:]
            prev_closes = sample[f"{t}.close"].to_numpy()[:-1]
            assert (opens != prev_closes).any(), f"{t}: all opens equal prev close"

    def test_fixture_distinct_from_s2t2(self):
        """O1: the five-year fixture is a separate artifact from golden_prices.parquet."""
        five_year = FIXTURE_DIR / "golden_prices_2020_2024.parquet"
        s2t2 = FIXTURE_DIR / "golden_prices.parquet"
        assert five_year.exists()
        assert s2t2.exists()
        fy = pd.read_parquet(five_year)
        s2 = pd.read_parquet(s2t2)
        # Different schemas: five-year has open+close, S2-T2 is closes-only
        assert set(fy.columns) != set(s2.columns)
        assert "510300.SH.open" in fy.columns
        assert "510300.SH" in s2.columns


class TestMonthEndClock:
    """O2: month-end clock matches S8-T1 exactly."""

    def test_formation_is_last_warmup(self, prices):
        dates = month_end_decision_dates(prices)
        warmup = prices[prices["phase"] == "warmup"]
        expected = str(warmup["date"].iloc[-1]).split()[0]
        assert dates[0] == expected

    def test_anchor_2020_01_23(self, prices):
        """2020-01-23 is the last January trading day (Spring Festival)."""
        dates = month_end_decision_dates(prices)
        assert "2020-01-23" in dates

    def test_anchor_2020_02_28(self, prices):
        """2020-02-28 is the last February trading day."""
        dates = month_end_decision_dates(prices)
        assert "2020-02-28" in dates

    def test_anchor_window_end(self, prices):
        """2024-12-31 is the last month-end in the window."""
        dates = month_end_decision_dates(prices)
        assert "2024-12-31" in dates
        assert dates[-1] == "2024-12-31"

    def test_month_ends_are_last_trading_days(self, prices):
        """Every decision date (except formation) is the last trading day of its month."""
        dates = month_end_decision_dates(prices)
        sample = prices[prices["phase"] == "sample"].copy()
        sample["ts"] = pd.to_datetime(sample["date"])
        for d in dates[1:]:
            ts = pd.Timestamp(d)
            month_days = sample[sample["ts"].dt.to_period("M") == ts.to_period("M")]
            last = str(month_days["date"].iloc[-1]).split()[0]
            assert d == last, f"{d} is not the last trading day of its month"

    def test_never_first_trading_day(self, prices):
        """Never _select_rebalance_dates('monthly') — it keeps first trading days."""
        from research.cross_sectional_backtest import _select_rebalance_dates

        dates = month_end_decision_dates(prices)
        sample_dates = prices[prices["phase"] == "sample"]["date"].tolist()
        monthly = _select_rebalance_dates(
            [pd.Timestamp(d).date() for d in sample_dates], "monthly"
        )
        monthly_str = [d.isoformat() for d in monthly]
        # The month-end clock must differ from the first-trading-day clock
        assert set(dates[1:]) != set(monthly_str)

    def test_count_matches_months(self, prices):
        """One decision date per calendar month + formation."""
        dates = month_end_decision_dates(prices)
        sample = prices[prices["phase"] == "sample"]
        sample_ts = pd.to_datetime(sample["date"])
        n_months = len(sample_ts.dt.to_period("M").unique())
        assert len(dates) == n_months + 1  # + formation


class TestNoSplice:
    """O1: the five-year fixture is never mixed with the S2-T2 fixture."""

    def test_no_baseline_spec_import(self):
        """Red line 6: never import or compare baseline_spec.py."""
        import ast

        for py_file in FIXTURE_DIR.glob("*.py"):
            tree = ast.parse(py_file.read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        assert "baseline_spec" not in alias.name
                elif isinstance(node, ast.ImportFrom):
                    if node.module and "baseline_spec" in node.module:
                        pytest.fail(
                            f"{py_file.name} imports baseline_spec (red line 6)"
                        )
