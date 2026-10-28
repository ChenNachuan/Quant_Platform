"""Golden-sample hand-calc comparison (QUANT-11 S2-T2).

Compares the pandas baseline against the human hand-calculated Excel values
stored in ``golden_expected.json``.  Per-ticker arithmetic only — the portfolio
layer is out of scope (Option A, 2026-10-14).

Also the data-existence check the ticket's AC-A2 names: the bars the bootstrap
pull lands under ``data_platform/`` must back the fixture, not merely coexist
with it.  See :class:`TestLandedBarsCoverFixture`.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from core.testing import TOLERANCE
from tests.golden.golden_baseline import golden_baseline

pytestmark = pytest.mark.golden

FIXTURE_DIR = Path(__file__).parent
UNIVERSE = ["510300.SH", "510500.SH", "518880.SH", "511010.SH", "159915.SZ"]
EXPECTED_KEYS = ("total_return", "sharpe", "mdd")

# Peak→trough anchors from QUANT-11 §4.3 — if these differ the mdd convention is wrong.
MDD_ANCHORS = {
    "510300.SH": ("2024-12-31", "2025-01-13"),
    "510500.SH": ("2024-12-31", "2025-01-10"),
    "518880.SH": ("2025-02-20", "2025-02-28"),
    "511010.SH": ("2025-01-06", "2025-03-11"),
    "159915.SZ": ("2025-02-21", "2025-03-31"),
}


@pytest.fixture(scope="module")
def prices() -> pd.DataFrame:
    return pd.read_parquet(FIXTURE_DIR / "golden_prices.parquet")


@pytest.fixture(scope="module")
def expected() -> dict:
    return json.loads((FIXTURE_DIR / "golden_expected.json").read_text())


@pytest.fixture(scope="module")
def result(prices) -> dict:
    return golden_baseline(prices)


class TestFixtureShape:
    """DoD: 5 tickers × 77 trading days, no NaN closes."""

    def test_dimensions(self, prices):
        assert prices.shape == (77, 7), f"shape {prices.shape}"

    def test_universe_columns(self, prices):
        cols = [c for c in prices.columns if c not in ("date", "phase")]
        assert sorted(cols) == sorted(UNIVERSE)

    def test_no_nan_closes(self, prices):
        assert prices[UNIVERSE].isna().sum().sum() == 0

    def test_phase_split(self, prices):
        counts = prices["phase"].value_counts().to_dict()
        assert counts == {"warmup": 20, "sample": 57}

    def test_date_range(self, prices):
        warmup = prices[prices["phase"] == "warmup"]
        sample = prices[prices["phase"] == "sample"]
        assert warmup["date"].iloc[0] == "2024-12-04"
        assert warmup["date"].iloc[-1] == "2024-12-31"
        assert sample["date"].iloc[0] == "2025-01-02"
        assert sample["date"].iloc[-1] == "2025-03-31"


class TestPandasMatchesHandCalc:
    """test_pandas_matches_hand_calc — 15 scalars within TOLERANCE = 1e-4."""

    def test_pandas_matches_hand_calc(self, result, expected):
        assert set(result) == set(EXPECTED_KEYS)
        for field in EXPECTED_KEYS:
            assert set(result[field]) == set(UNIVERSE), f"{field} tickers"
            for ticker in UNIVERSE:
                got = result[field][ticker]
                exp = expected[field][ticker]
                assert abs(got - exp) < TOLERANCE, (
                    f"{field}/{ticker}: got {got!r}, expected {exp!r}, "
                    f"diff {abs(got - exp):.3e}"
                )

    def test_no_portfolio_keys(self, expected):
        """Option A: golden_expected.json has no portfolio / weights / nav key."""
        assert set(expected) == set(EXPECTED_KEYS)

    def test_15_scalars(self, expected):
        total = sum(len(expected[k]) for k in EXPECTED_KEYS)
        assert total == 15


class TestMddAnchors:
    """§4.3 peak→trough anchors — wrong dates mean wrong convention."""

    def test_mdd_anchors(self, prices, result):
        sample = prices[prices["phase"] == "sample"].reset_index(drop=True)
        warmup_last = prices[prices["phase"] == "warmup"].iloc[-1]
        for ticker, (exp_peak, exp_trough) in MDD_ANCHORS.items():
            closes = sample[ticker].to_numpy(dtype=float)
            base = float(warmup_last[ticker])
            prev = [base] + list(closes[:-1])
            r = [c / p - 1.0 for c, p in zip(closes, prev)]
            nav = [1.0]
            for x in r:
                nav.append(nav[-1] * (1.0 + x))
            peak, peak_idx, mdd, mdd_idx, mdd_peak_idx = 1.0, 0, 0.0, 0, 0
            for i, v in enumerate(nav):
                if v > peak:
                    peak, peak_idx = v, i
                dd = v / peak - 1.0
                if dd < mdd:
                    mdd, mdd_idx, mdd_peak_idx = dd, i, peak_idx
            dates = ["2024-12-31"] + list(sample["date"])
            assert dates[mdd_peak_idx] == exp_peak, (
                f"{ticker}: peak date {dates[mdd_peak_idx]} != {exp_peak}"
            )
            assert dates[mdd_idx] == exp_trough, (
                f"{ticker}: trough date {dates[mdd_idx]} != {exp_trough}"
            )
            assert abs(mdd - result["mdd"][ticker]) < 1e-12


#: Repo root — ``data_platform`` is the root ``PARTITION_PATHS`` are relative to
#: (``data/storage_schema.py``).  Relative to this file so the check does not
#: depend on the pytest cwd.
DATA_PLATFORM = Path(__file__).resolve().parents[2] / "data_platform"
BARS_DIR = DATA_PLATFORM / "asset" / "bars_daily"


def _landed_closes() -> pd.DataFrame:
    """Wide (trade_date x ticker) close matrix from the landed daily files."""
    if not BARS_DIR.is_dir():
        raise FileNotFoundError(
            f"{BARS_DIR} does not exist — run "
            f"scripts/bootstrap/run_golden_sample.sh first"
        )
    files = sorted(BARS_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"{BARS_DIR} has no .parquet files")
    bars = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    out = bars.pivot_table(index="trade_date", columns="ticker", values="close")
    out.index = pd.Index([str(day)[:10] for day in out.index], name="date")
    return out.sort_index()


class TestLandedBarsCoverFixture:
    """AC-A2: the pull has to cover the sample window and agree with it.

    The fixture is self-contained, so every other test in this file passes
    against a fixture alone.  That is exactly how a pull window ending
    2025-03-01 — 21 sample days short of the 2025-03-31 the chain consumes —
    went out green.  This is the one check that ties ``data_platform`` to
    ``golden_prices.parquet``: window first, then the closes.
    """

    def test_landed_window_covers_fixture_and_closes_match(self, prices):
        landed = _landed_closes()

        fixture_dates = [str(d)[:10] for d in prices["date"]]
        missing = [d for d in fixture_dates if d not in landed.index]
        assert not missing, (
            f"landed bars under {BARS_DIR} do not cover the fixture window: "
            f"{len(missing)} of {len(fixture_dates)} days missing "
            f"({missing[:3]}...{missing[-1:] if missing else ''}). "
            f"landed {landed.index.min()}..{landed.index.max()}, "
            f"fixture {fixture_dates[0]}..{fixture_dates[-1]}. "
            f"Re-pull with the window the golden chain consumes."
        )

        for ticker in UNIVERSE:
            assert ticker in landed.columns, f"{ticker} missing from landed bars"

        fixture = prices.set_index("date")[UNIVERSE].astype(float)
        fixture.index = pd.Index(fixture_dates)
        got = landed.loc[fixture_dates, UNIVERSE].astype(float)
        diff = (got - fixture).abs()
        worst = diff.max().max()
        assert worst < TOLERANCE, (
            f"landed closes disagree with golden_prices.parquet: "
            f"max |diff| {worst:.3e} >= TOLERANCE {TOLERANCE:.0e} "
            f"({diff.max().sort_values(ascending=False).head(3).to_dict()})"
        )
