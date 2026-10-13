"""Golden-layer tests: verify the golden-sample conventions are in place.

The golden sample fixture (``golden_prices.parquet`` + ``golden_expected.json``)
lands at S2-T2 (QUANT-11) and is deliberately absent here.  These tests
assert the constants and layout the golden tests will depend on.
"""

from pathlib import Path

import pytest

from core.testing import TOLERANCE, TIME_BUDGET_SECONDS

pytestmark = pytest.mark.golden


class TestTolerance:
    """TOLERANCE = 1e-4 — golden_expected.json is hand-calculated to 4 decimals."""

    def test_tolerance_value(self):
        assert TOLERANCE == 1e-4

    def test_tolerance_precision(self):
        assert abs(TOLERANCE - 0.0001) < 1e-15


class TestBudgets:
    """Golden layer budget is 300s (5 min) per the three-tier design."""

    def test_golden_budget(self):
        assert TIME_BUDGET_SECONDS["golden"] == 300

    def test_all_budgets_ordered(self):
        assert TIME_BUDGET_SECONDS["unit"] < TIME_BUDGET_SECONDS["integration"]
        assert TIME_BUDGET_SECONDS["integration"] < TIME_BUDGET_SECONDS["golden"]


class TestGoldenDirectoryLayout:
    """The golden directory exists but contains no fixture files yet (S2-T2)."""

    def test_golden_dir_exists(self):
        golden_dir = Path(__file__).parent
        assert golden_dir.is_dir()

    def test_no_fixture_files_yet(self):
        """S2-T2 deliverables are explicitly absent from this PR."""
        golden_dir = Path(__file__).parent
        assert not (golden_dir / "golden_prices.parquet").exists()
        assert not (golden_dir / "golden_expected.json").exists()
