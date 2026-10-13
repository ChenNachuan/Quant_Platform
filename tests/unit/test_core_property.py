"""Hypothesis property tests for core/ modules (design/core/testing.md).

Real property tests only where the subject exists (the ``core/`` five).
Everything else is ``pytest.mark.skip`` with a reason naming the stage that
lands the subject — a skip is honest, a hollow ``pass`` test is noise.
"""

import os
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from core.asset_rules import ASSET_RULES, get_asset_rules
from core.config import load_strategy_config
from core.registry import FactorRegistry, RuleRegistry
from core.types import AssetType, QualityFlag


# ---------------------------------------------------------------------------
# Isolation for registry tests
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _isolated_registries():
    FactorRegistry._store.clear()
    RuleRegistry._store.clear()
    yield
    FactorRegistry._store.clear()
    RuleRegistry._store.clear()


# ===========================================================================
# Real property tests — subjects exist in core/
# ===========================================================================


class TestLoadConfigOverride:
    """Strategy config overrides global config on same-named keys."""

    @staticmethod
    def _write_toml(path, data: dict) -> None:
        lines = [f"{k} = {v}" for k, v in data.items()]
        path.write_text("\n".join(lines) + "\n")

    @given(
        shared=st.dictionaries(
            keys=st.sampled_from(["alpha", "beta", "gamma", "delta", "epsilon"]),
            values=st.integers(min_value=-1000, max_value=1000),
            min_size=1,
            max_size=5,
        ),
        strat_extra=st.dictionaries(
            keys=st.sampled_from(["zeta", "eta", "theta"]),
            values=st.integers(min_value=-1000, max_value=1000),
            max_size=3,
        ),
        glob_extra=st.dictionaries(
            keys=st.sampled_from(["iota", "kappa", "lambda"]),
            values=st.integers(min_value=-1000, max_value=1000),
            max_size=3,
        ),
    )
    def test_strategy_overrides_global_same_keys(self, shared, strat_extra, glob_extra):
        strategy = {**shared, **strat_extra}
        global_cfg = {**shared, **glob_extra}

        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            try:
                os.chdir(tmp_path)
                strat_dir = tmp_path / "config" / "strategies"
                strat_dir.mkdir(parents=True)
                self._write_toml(strat_dir / "prop.toml", strategy)

                merged = load_strategy_config("prop", global_cfg=global_cfg)
            finally:
                os.chdir(orig_cwd)

        # Strategy values win on every same-named key.
        for k, v in shared.items():
            assert merged[k] == v
        # Strategy-only keys join.
        for k, v in strat_extra.items():
            assert merged[k] == v
        # Global-only keys survive.
        for k, v in glob_extra.items():
            assert merged[k] == v


class TestAssetRulesLookup:
    """Unknown asset_type raises KeyError — no silent fallback."""

    @given(
        asset_type=st.text(min_size=1, max_size=30).filter(
            lambda s: s not in ASSET_RULES
        )
    )
    def test_unknown_asset_type_raises_keyerror(self, asset_type):
        with pytest.raises(KeyError):
            get_asset_rules(asset_type)

    @given(asset_type=st.sampled_from(list(ASSET_RULES.keys())))
    def test_known_asset_type_returns_rules(self, asset_type):
        rules = get_asset_rules(asset_type)
        assert isinstance(rules, dict)
        assert "lot_size" in rules


class TestQualityFlagAndAssetType:
    """5 quality bits non-overlapping; AssetType has exactly 6 members."""

    def test_five_flag_bits_non_overlapping(self):
        nonzero = [
            QualityFlag.C02_COVERAGE,
            QualityFlag.C05_NULL_RATE,
            QualityFlag.L04_BEYOND_LIMIT,
            QualityFlag.L07_ADJFACTOR_RANGE,
            QualityFlag.K01_OVERNIGHT_GAP,
        ]
        values = [int(f) for f in nonzero]
        # Each bit is a distinct power of two.
        assert len(set(values)) == 5
        for v in values:
            assert v > 0 and (v & (v - 1)) == 0  # power of two
        # Pairwise AND is zero — no overlap.
        for i in range(len(values)):
            for j in range(i + 1, len(values)):
                assert values[i] & values[j] == 0

    def test_asset_type_has_exactly_six_members(self):
        assert len(AssetType) == 6
        assert {a.value for a in AssetType} == {
            "stock",
            "etf",
            "fund",
            "bond",
            "futures",
            "option",
        }


class TestRegistryDuplicateName:
    """Re-registering a taken name raises — no silent overwrite."""

    @given(
        name=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
            min_size=1,
            max_size=20,
        )
    )
    def test_factor_registry_reregister_raises(self, name):
        class First:
            pass

        First.name = name
        FactorRegistry.register(First)

        class Second:
            pass

        Second.name = name
        with pytest.raises(ValueError):
            FactorRegistry.register(Second)

    @given(
        name=st.text(
            alphabet=st.characters(whitelist_categories=("Ll", "Lu", "Nd")),
            min_size=1,
            max_size=20,
        )
    )
    def test_rule_registry_reregister_raises(self, name):
        class First:
            pass

        First.name = name
        RuleRegistry.register(First)

        class Second:
            pass

        Second.name = name
        with pytest.raises(ValueError):
            RuleRegistry.register(Second)


# ===========================================================================
# Skipped property tests — subjects land at later stages
# ===========================================================================


@pytest.mark.skip(reason="module lands at S7: FeeModel")
@given(
    price=st.floats(min_value=1, max_value=1000),
    shares=st.integers(min_value=100, max_value=10000),
)
def test_fee_non_negative_and_sell_ge_buy(price, shares):
    """fee ≥ 0; sell fee ≥ buy fee (stamp tax on sell).  Subject: S7 FeeModel."""
    from execution.fee_model import FeeModel  # noqa: F811  (lands at S7)

    fee = FeeModel().buy_fee(price, shares)
    assert fee >= 0
    sell_fee = FeeModel().sell_fee(price, shares)
    assert sell_fee >= fee


@pytest.mark.skip(reason="module lands at S5: winsorize / zscore (PostProcessor)")
@given(seed=st.integers(min_value=0, max_value=2**31 - 1))
def test_zscore_cross_section_mean_std(seed):
    """zscore 后 mean≈0, std≈1 (per cross-section).  Subject: S5 PostProcessor."""
    from research.postprocess import zscore  # noqa: F811  (lands at S5)

    import numpy as np

    rng = np.random.default_rng(seed)
    values = rng.normal(loc=50, scale=10, size=100)
    result = zscore(values)
    assert abs(float(np.mean(result))) < 1e-4
    assert abs(float(np.std(result)) - 1.0) < 1e-4


@pytest.mark.skip(reason="module lands at S7: build_portfolio")
@given(
    n=st.integers(min_value=1, max_value=20),
    seed=st.integers(min_value=0, max_value=2**31 - 1),
)
def test_portfolio_weights_legal(n, seed):
    """weights.sum() ≤ 1 + 1e-9; no negative weights (no shorting).  Subject: S7."""
    from execution.portfolio_construction import build_portfolio  # noqa: F811

    import numpy as np

    rng = np.random.default_rng(seed)
    scores = rng.normal(size=n)
    weights = build_portfolio(scores)
    assert all(w >= 0 for w in weights)
    assert sum(weights) <= 1 + 1e-9


@pytest.mark.skip(reason="module lands at S7: SimAccount")
@given(
    cash=st.floats(min_value=0, max_value=1e6),
    market_value=st.floats(min_value=0, max_value=1e6),
)
def test_account_nav_identity(cash, market_value):
    """cash + market_value == nav after every fill.  Subject: S7 SimAccount."""
    from execution.ledger import SimAccount  # noqa: F811  (lands at S7)

    acct = SimAccount(cash=cash)
    acct.mark_to_market(market_value)
    assert acct.cash + acct.market_value == pytest.approx(acct.nav, abs=1e-6)


@pytest.mark.skip(reason="module lands at S7: compute_order_diff")
@given(
    target_qty=st.integers(min_value=0, max_value=10000),
    current_qty=st.integers(min_value=0, max_value=10000),
    lot_size=st.sampled_from([100, 200, 1000]),
)
def test_order_diff_lot_size_multiple(target_qty, current_qty, lot_size):
    """Result is an integer multiple of lot_size.  Subject: S7 compute_order_diff."""
    from execution.orders import compute_order_diff  # noqa: F811  (lands at S7)

    diff = compute_order_diff(target_qty, current_qty, lot_size=lot_size)
    assert diff % lot_size == 0


@pytest.mark.skip(reason="module lands at S4: FactorDAG")
@given(n=st.integers(min_value=2, max_value=10))
def test_factor_dag_cycle_detection(n):
    """Cycle detection raises; dependencies compute first.  Subject: S4 FactorDAG."""
    from research.factor_dag import FactorDAG  # noqa: F811  (lands at S4)

    dag = FactorDAG()
    # Chain: f0 -> f1 -> ... -> f(n-1)
    for i in range(n):
        dag.add_node(f"f{i}", depends_on=[f"f{i-1}"] if i > 0 else [])
    # Introducing a cycle must raise.
    dag.add_node("f0", depends_on=[f"f{n-1}"])  # noqa: F841
    with pytest.raises(ValueError):
        dag.topological_sort()


@pytest.mark.skip(reason="module lands at S12: run_job (APScheduler retry)")
@given(max_retries=st.integers(min_value=1, max_value=5))
def test_run_job_failed_after_n_retries(max_retries):
    """Failing n times yields status=FAILED.  Subject: S12 run_job."""
    from ops.scheduler import run_job  # noqa: F811  (lands at S12)

    calls = []

    def always_fail():
        calls.append(1)
        raise RuntimeError("boom")

    result = run_job(always_fail, max_retries=max_retries)
    assert result.status == "FAILED"
    assert len(calls) == max_retries
