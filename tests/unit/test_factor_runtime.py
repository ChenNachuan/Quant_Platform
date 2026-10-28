"""Tests for research.factor_runtime — FactorDAG, caching, failure isolation.

Covers every DoD row: cycle detection (ValueError), dependency ordering
(upstream before downstream), cache key shape and override semantics
(force / version-bump), failure propagation in compute_all_safe, and the
look-ahead red line (test_no_lookahead).
"""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from research.factor_interface import Factor
from research.factor_runtime import (
    FactorDAG,
    MLFactor,
    compute_all,
    compute_all_safe,
    compute_depends_hash,
    compute_factor,
)


# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------

_DATES = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
_TICKERS = ["000001.SZ", "000002.SZ"]


def _make_bars(start: str = "2024-01-02", end: str = "2024-01-08") -> pd.DataFrame:
    """Synthetic OHLCV bars for two tickers over five trading days."""
    rows = []
    for t in _TICKERS:
        base = 10.0 if t == "000001.SZ" else 20.0
        for i, d in enumerate(_DATES):
            if d < start or d > end:
                continue
            rows.append(
                {
                    "date": d,
                    "ticker": t,
                    "open": base + i * 0.5,
                    "high": base + i * 0.5 + 0.3,
                    "low": base + i * 0.5 - 0.3,
                    "close": base + i * 0.5 + 0.1,
                    "volume": 1000 + i * 100,
                    "amount": (base + i * 0.5) * (1000 + i * 100),
                }
            )
    return pd.DataFrame(rows)


def _synthetic_loader(table_name: str, start: str, end: str) -> pd.DataFrame:
    """Test-double data loader — returns synthetic bars filtered by range."""
    if table_name == "bars_daily":
        return _make_bars(start, end)
    raise ValueError(f"unknown table {table_name!r}")


# ---------------------------------------------------------------------------
# Test factor fixtures
# ---------------------------------------------------------------------------


class SimpleReturn(Factor):
    """1-day return — looks backwards only (no look-ahead)."""

    name = "returns_1d"
    description = "1-day pct change of close"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["returns_1d"] = df.groupby("ticker")["close"].pct_change(1)
        return df[["date", "ticker", "returns_1d"]].dropna()


class Volatility5D(Factor):
    """Rolling std of returns — upstream of VolAdjMomentum."""

    name = "volatility_5d"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["volatility_5d"] = (
            df.groupby("ticker")["close"].pct_change(1).rolling(3).std()
        )
        return df[["date", "ticker", "volatility_5d"]].dropna()


class VolAdjMomentum(Factor):
    """Depends on two upstream factors via the factor: prefix."""

    name = "vol_adj_momentum"
    category = "time_series"

    def depends_on(self):
        return ["factor:returns_1d", "factor:volatility_5d"]

    def compute(self, data):
        ret = data["factor:returns_1d"]
        vol = data["factor:volatility_5d"]
        merged = ret.merge(vol, on=["date", "ticker"])
        merged["vol_adj_momentum"] = merged["returns_1d"] / merged["volatility_5d"]
        return merged[["date", "ticker", "vol_adj_momentum"]].dropna()


class SlowFactor(Factor):
    """Always raises — used for failure-propagation tests."""

    name = "slow_factor"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        raise RuntimeError("intentional failure for testing")


class DownstreamOfSlow(Factor):
    """Depends on SlowFactor — must be skipped when SlowFactor fails."""

    name = "downstream_of_slow"
    category = "time_series"

    def depends_on(self):
        return ["factor:slow_factor"]

    def compute(self, data):
        df = data["factor:slow_factor"].copy()
        df["downstream_of_slow"] = 1.0
        return df[["date", "ticker", "downstream_of_slow"]]


class IndependentFactor(Factor):
    """Same layer as DownstreamOfSlow — must survive SlowFactor's failure."""

    name = "independent_factor"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df["independent_factor"] = df["close"] * 2.0
        return df[["date", "ticker", "independent_factor"]]


class CycleA(Factor):
    name = "cycle_a"

    def depends_on(self):
        return ["factor:cycle_b"]

    def compute(self, data):
        return pd.DataFrame(columns=["date", "ticker", "cycle_a"])


class CycleB(Factor):
    name = "cycle_b"

    def depends_on(self):
        return ["factor:cycle_a"]

    def compute(self, data):
        return pd.DataFrame(columns=["date", "ticker", "cycle_b"])


class SelfCycle(Factor):
    name = "self_cycle"

    def depends_on(self):
        return ["factor:self_cycle"]

    def compute(self, data):
        return pd.DataFrame(columns=["date", "ticker", "self_cycle"])


class LookAheadFactor(Factor):
    """DELIBERATE look-ahead: uses full-sample mean (includes future dates).

    Deleting the final day changes the mean and therefore changes every
    historical value — test_no_lookahead must catch this.
    """

    name = "lookahead_bad"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        # LOOKAHEAD BUG: mean over the full sample includes future dates.
        df["lookahead_bad"] = df["close"] / df["close"].mean() - 1
        return df[["date", "ticker", "lookahead_bad"]]


class CausalFactor(Factor):
    """Correct factor: 1-day return, strictly backwards-looking."""

    name = "causal_return"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["causal_return"] = df.groupby("ticker")["close"].pct_change(1)
        return df[["date", "ticker", "causal_return"]].dropna()


class StampFactor(Factor):
    """Stamps every row with the compute() call number.

    Lets a test see whether a result's rows came from a previous call's
    cached frame (append) or from a fresh compute (full).  Deterministic
    factors cannot show that — both branches would produce equal values.
    """

    name = "stamp_factor"
    category = "time_series"
    calls = 0

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        StampFactor.calls += 1
        df = data["bars_daily"][["date", "ticker"]].copy()
        df["stamp_factor"] = float(StampFactor.calls)
        return df


class TickerMajorUpstream(Factor):
    """Upstream that emits rows in (ticker, date) order, not (date, ticker)."""

    name = "ticker_major_val"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["ticker_major_val"] = df["close"] * 1.0
        return df[["date", "ticker", "ticker_major_val"]]


class ChainStampDownstream(Factor):
    """Stamps compute() calls; consumes a factor: dependency.

    Together with :class:`TickerMajorUpstream` this is the chain where a
    cache-path-dependent upstream row order used to make the consumer's
    depends_hash mismatch and kill the append branch.
    """

    name = "chain_stamp_val"
    category = "time_series"
    calls = 0

    def depends_on(self):
        return ["factor:ticker_major_val"]

    def compute(self, data):
        ChainStampDownstream.calls += 1
        df = data["factor:ticker_major_val"][["date", "ticker"]].copy()
        df["chain_stamp_val"] = float(ChainStampDownstream.calls)
        return df


class OrderSensitiveDownstream(Factor):
    """Row-diff over its factor: input, with no groupby — order-sensitive."""

    name = "order_diff_val"
    category = "time_series"

    def depends_on(self):
        return ["factor:ticker_major_val"]

    def compute(self, data):
        df = data["factor:ticker_major_val"].copy()
        df["order_diff_val"] = df["ticker_major_val"].diff()
        return df[["date", "ticker", "order_diff_val"]].dropna()


# ---------------------------------------------------------------------------
# FactorDAG — registration and topological ordering
# ---------------------------------------------------------------------------


class TestFactorDAG:
    def test_register_and_topological_sort(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        dag.register(Volatility5D())
        dag.register(VolAdjMomentum())
        order = dag.topological_sort()
        assert order.index("returns_1d") < order.index("vol_adj_momentum")
        assert order.index("volatility_5d") < order.index("vol_adj_momentum")

    def test_topological_layers_upstream_before_downstream(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        dag.register(Volatility5D())
        dag.register(VolAdjMomentum())
        layers = dag.topological_layers()
        # Layer 0: both roots; layer 1: the dependent factor.
        assert set(layers[0]) == {"returns_1d", "volatility_5d"}
        assert layers[1] == ["vol_adj_momentum"]

    def test_cycle_raises_value_error(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(CycleA())
        dag.register(CycleB())
        with pytest.raises(ValueError, match="circular"):
            dag.topological_sort()

    def test_cycle_layers_raises_value_error(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(CycleA())
        dag.register(CycleB())
        with pytest.raises(ValueError, match="circular"):
            dag.topological_layers()

    def test_self_cycle_raises(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SelfCycle())
        with pytest.raises(ValueError, match="circular"):
            dag.topological_sort()

    def test_duplicate_register_raises(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        with pytest.raises(ValueError, match="already registered"):
            dag.register(SimpleReturn())

    def test_dangling_dependency_raises(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(VolAdjMomentum())  # depends on unregistered factors
        with pytest.raises(ValueError, match="unregistered"):
            dag.topological_sort()


# ---------------------------------------------------------------------------
# Cache key
# ---------------------------------------------------------------------------


class TestCacheKey:
    def test_cache_key_shape(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        compute_factor(dag, "returns_1d", "2024-01-02", "2024-01-08")
        keys = list(dag._cache.keys())
        assert len(keys) == 1
        name, version, date_range, dep_hash = keys[0]
        assert name == "returns_1d"
        assert version == "1.0"
        assert date_range == ("2024-01-02", "2024-01-08")
        assert isinstance(dep_hash, str) and len(dep_hash) == 12

    def test_force_overrides_cache(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        result1 = compute_factor(dag, "returns_1d", "2024-01-02", "2024-01-08")
        result2 = compute_factor(dag, "returns_1d", "2024-01-02", "2024-01-08")
        assert result1 is result2  # cache hit — same object

        result3 = compute_factor(dag, "returns_1d", "2024-01-02", "2024-01-08", force=True)
        assert result3 is not result1  # force recomputed
        pd.testing.assert_frame_equal(result3, result1)

    def test_version_change_triggers_full_recompute(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        compute_factor(dag, "returns_1d", "2024-01-02", "2024-01-08")
        # Bump version — new cache key, not an append.
        SimpleReturn.version = "2.0"
        try:
            result = compute_factor(dag, "returns_1d", "2024-01-02", "2024-01-08")
        finally:
            SimpleReturn.version = "1.0"
        assert len(dag._cache) == 2  # old key + new key
        assert result is not None

    def test_append_mode(self):
        # Phase 1 — prove the append branch actually fired: the short-range
        # rows must still carry call 1's stamp (reused from cache), while the
        # new tail rows carry call 2's stamp.  A full recompute would restamp
        # every row with 2.0, and a no-op cache hit would not extend at all.
        StampFactor.calls = 0
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(StampFactor())
        short = compute_factor(dag, "stamp_factor", "2024-01-02", "2024-01-04")
        extended = compute_factor(dag, "stamp_factor", "2024-01-02", "2024-01-08")
        assert len(extended) > len(short)
        prefix = extended.loc[extended["date"] <= "2024-01-04", "stamp_factor"]
        tail = extended.loc[extended["date"] > "2024-01-04", "stamp_factor"]
        assert len(tail) > 0
        assert set(prefix.unique()) == {1.0}, "cached prefix was recomputed"
        assert set(tail.unique()) == {2.0}, "tail was not freshly computed"

        # Phase 2 — the composition must equal a fresh full compute, values
        # *and* row order.  assert_frame_equal is the check that bites when
        # the append path sorts and the full path does not: comparing prefix
        # and tail separately never looks at the seam or the order.
        dag2 = FactorDAG(data_loader=_synthetic_loader)
        dag2.register(SimpleReturn())
        short2 = compute_factor(dag2, "returns_1d", "2024-01-02", "2024-01-04")
        extended2 = compute_factor(dag2, "returns_1d", "2024-01-02", "2024-01-08")
        dag3 = FactorDAG(data_loader=_synthetic_loader)
        dag3.register(SimpleReturn())
        fresh = compute_factor(
            dag3, "returns_1d", "2024-01-02", "2024-01-08", force=True
        )
        assert len(extended2) > len(short2)
        pd.testing.assert_frame_equal(extended2, fresh)

    def test_append_mode_factor_chain(self):
        # Same contract across a factor: dependency, whose row order used to
        # follow the upstream's cache path and kill the consumer's append.
        ChainStampDownstream.calls = 0
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(TickerMajorUpstream())
        dag.register(ChainStampDownstream())
        short = compute_factor(dag, "chain_stamp_val", "2024-01-02", "2024-01-04")
        extended = compute_factor(dag, "chain_stamp_val", "2024-01-02", "2024-01-08")
        assert len(extended) > len(short)
        prefix = extended.loc[extended["date"] <= "2024-01-04", "chain_stamp_val"]
        tail = extended.loc[extended["date"] > "2024-01-04", "chain_stamp_val"]
        assert len(tail) > 0
        assert set(prefix.unique()) == {1.0}, "factor: chain append did not fire"
        assert set(tail.unique()) == {2.0}

        # Composition equals a fresh full compute — values and row order.
        dag2 = FactorDAG(data_loader=_synthetic_loader)
        dag2.register(TickerMajorUpstream())
        dag2.register(OrderSensitiveDownstream())
        short2 = compute_factor(dag2, "order_diff_val", "2024-01-02", "2024-01-04")
        extended2 = compute_factor(dag2, "order_diff_val", "2024-01-02", "2024-01-08")
        dag3 = FactorDAG(data_loader=_synthetic_loader)
        dag3.register(TickerMajorUpstream())
        dag3.register(OrderSensitiveDownstream())
        fresh = compute_factor(
            dag3, "order_diff_val", "2024-01-02", "2024-01-08", force=True
        )
        assert len(extended2) > len(short2)
        pd.testing.assert_frame_equal(extended2, fresh)

    def test_cache_path_independent_values(self):
        # Same requested range, two cache paths (upstream full vs upstream
        # append), one answer.  The consumer is order-sensitive on purpose.
        def run(pre_extend: bool) -> pd.DataFrame:
            dag = FactorDAG(data_loader=_synthetic_loader)
            dag.register(TickerMajorUpstream())
            dag.register(OrderSensitiveDownstream())
            if pre_extend:
                compute_factor(dag, "order_diff_val", "2024-01-02", "2024-01-04")
            return compute_factor(dag, "order_diff_val", "2024-01-02", "2024-01-08")

        full_path = run(pre_extend=False)
        append_path = run(pre_extend=True)
        pd.testing.assert_frame_equal(full_path, append_path)

    def test_append_rejected_on_upstream_revision(self):
        # Compute a short range on unshifted data, then revise the upstream
        # values and extend the range.  The cached prefix is stale, so append
        # must not fire — the whole range is recomputed and every row (the
        # prefix included) carries the new call's stamp.
        StampFactor.calls = 0
        shifted = {"on": False}

        def loader(table_name: str, start: str, end: str) -> pd.DataFrame:
            df = _synthetic_loader(table_name, start, end)
            if shifted["on"]:
                df = df.copy()
                df["close"] = df["close"] + 99.0
            return df

        dag = FactorDAG(data_loader=loader)
        dag.register(StampFactor())
        compute_factor(dag, "stamp_factor", "2024-01-02", "2024-01-04")
        assert StampFactor.calls == 1
        shifted["on"] = True
        extended = compute_factor(dag, "stamp_factor", "2024-01-02", "2024-01-08")
        assert StampFactor.calls == 2  # full recompute, not append
        assert set(extended["stamp_factor"].unique()) == {2.0}

    def test_depends_hash_changes_with_data(self):
        factor = SimpleReturn()
        data1 = {"bars_daily": _make_bars("2024-01-02", "2024-01-08")}
        data2 = {"bars_daily": _make_bars("2024-01-02", "2024-01-04")}
        hash1 = compute_depends_hash(factor, data1)
        hash2 = compute_depends_hash(factor, data2)
        assert hash1 != hash2
        assert len(hash1) == 12

    def test_depends_hash_changes_with_content(self):
        # Same shape (row count, date range, columns), different values —
        # the fingerprint must see the content, not just the shape.
        factor = SimpleReturn()
        data1 = {"bars_daily": _make_bars("2024-01-02", "2024-01-08")}
        data2 = {"bars_daily": _make_bars("2024-01-02", "2024-01-08")}
        assert data1["bars_daily"].shape == data2["bars_daily"].shape
        data2["bars_daily"] = data2["bars_daily"].copy()
        data2["bars_daily"]["close"] = data2["bars_daily"]["close"] + 99.0
        hash1 = compute_depends_hash(factor, data1)
        hash2 = compute_depends_hash(factor, data2)
        assert hash1 != hash2

    def test_depends_hash_stable_for_identical_content(self):
        factor = SimpleReturn()
        data1 = {"bars_daily": _make_bars("2024-01-02", "2024-01-08")}
        data2 = {"bars_daily": _make_bars("2024-01-02", "2024-01-08")}
        assert compute_depends_hash(factor, data1) == compute_depends_hash(
            factor, data2
        )


# ---------------------------------------------------------------------------
# Dependency order — upstream computed before downstream
# ---------------------------------------------------------------------------


class TestDependencyOrder:
    def test_upstream_computed_first(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        dag.register(Volatility5D())
        dag.register(VolAdjMomentum())
        results = compute_all(dag, "2024-01-02", "2024-01-08")
        assert set(results) == {"returns_1d", "volatility_5d", "vol_adj_momentum"}
        # Downstream result exists and is derived from upstream values.
        assert not results["vol_adj_momentum"].empty

    def test_compute_all_returns_all_factors(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        dag.register(IndependentFactor())
        results = compute_all(dag, "2024-01-02", "2024-01-08")
        assert set(results) == {"returns_1d", "independent_factor"}


# ---------------------------------------------------------------------------
# Failure propagation (compute_all_safe)
# ---------------------------------------------------------------------------


class TestFailurePropagation:
    def test_upstream_failure_skips_downstream(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SlowFactor())
        dag.register(DownstreamOfSlow())
        results, errors = compute_all_safe(dag, "2024-01-02", "2024-01-08")
        assert "slow_factor" in errors
        assert isinstance(errors["slow_factor"], RuntimeError)
        assert "downstream_of_slow" in errors
        assert "slow_factor" not in results
        assert "downstream_of_slow" not in results

    def test_independent_same_layer_survives(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SlowFactor())
        dag.register(DownstreamOfSlow())
        dag.register(IndependentFactor())
        results, errors = compute_all_safe(dag, "2024-01-02", "2024-01-08")
        assert "independent_factor" in results
        assert "independent_factor" not in errors

    def test_compute_all_raises_on_failure(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SlowFactor())
        with pytest.raises(RuntimeError, match="intentional"):
            compute_all(dag, "2024-01-02", "2024-01-08")

    def test_safe_returns_partial_results(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        dag.register(SlowFactor())
        dag.register(IndependentFactor())
        results, errors = compute_all_safe(dag, "2024-01-02", "2024-01-08")
        assert "returns_1d" in results
        assert "independent_factor" in results
        assert "slow_factor" in errors


# ---------------------------------------------------------------------------
# Look-ahead red line (DoD sharpest row)
# ---------------------------------------------------------------------------


class TestNoLookAhead:
    def test_no_lookahead(self):
        """Deleting the final day must leave history unchanged.

        A causal factor's historical values must not change when the last
        day is removed.  A deliberately look-ahead factor must FAIL this
        check — proving the test has teeth.
        """
        # --- causal factor: history unchanged ---
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(CausalFactor())

        full = compute_factor(dag, "causal_return", "2024-01-02", "2024-01-08")
        # Delete the final day and recompute.
        trunc = compute_factor(dag, "causal_return", "2024-01-02", "2024-01-05")

        last_date = full["date"].max()
        full_history = full[full["date"] < last_date].reset_index(drop=True)
        assert len(full_history) > 0
        assert len(trunc) == len(full_history)
        np.testing.assert_allclose(
            full_history["causal_return"].values,
            trunc["causal_return"].values,
            equal_nan=True,
            err_msg="causal factor: history changed after deleting last day",
        )

    def test_lookahead_fixture_fails(self):
        """Prove the lookahead test has teeth: a deliberately bad factor
        must NOT satisfy the history-unchanged property."""
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(LookAheadFactor())

        full = compute_factor(dag, "lookahead_bad", "2024-01-02", "2024-01-08")
        trunc = compute_factor(dag, "lookahead_bad", "2024-01-02", "2024-01-05")

        last_date = full["date"].max()
        full_history = full[full["date"] < last_date].reset_index(drop=True)
        assert len(full_history) > 0

        # The full-sample mean shifts when the last day is removed —
        # every historical value changes.  The allclose assertion must fail.
        history_matches = np.allclose(
            full_history["lookahead_bad"].values,
            trunc["lookahead_bad"].values,
            equal_nan=True,
        )
        assert not history_matches, (
            "lookahead fixture unexpectedly passed — test has no teeth"
        )


# ---------------------------------------------------------------------------
# MLFactor skeleton
# ---------------------------------------------------------------------------


class TestMLFactor:
    def test_ml_factor_skeleton_fields(self):
        assert MLFactor.retrain_freq == "monthly"
        assert MLFactor.timeout_seconds == 300
        assert MLFactor.model_path == ""
        assert MLFactor.name == "ml_factor"

    def test_ml_factor_train_raises(self):
        ml = MLFactor()
        with pytest.raises(NotImplementedError, match="S11"):
            ml.train({}, "2024-01-01")

    def test_ml_factor_predict_raises(self):
        ml = MLFactor()
        with pytest.raises(NotImplementedError, match="S11"):
            ml.predict(None, {})

    def test_ml_factor_compute_raises(self):
        ml = MLFactor()
        with pytest.raises(NotImplementedError, match="S11"):
            ml.compute({})

    def test_ml_factor_is_isolated_in_compute_all(self):
        """ML factors compute serially (separate from normal factors)."""

        class MyML(MLFactor):
            name = "my_ml_factor"
            version = "1.0"

            def depends_on(self):
                return ["bars_daily"]

            def compute(self, data):
                df = data["bars_daily"][["date", "ticker", "close"]].copy()
                df["my_ml_factor"] = df["close"] * 0.5
                return df[["date", "ticker", "my_ml_factor"]]

        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        dag.register(MyML())
        results = compute_all(dag, "2024-01-02", "2024-01-08")
        assert "my_ml_factor" in results
        assert "returns_1d" in results


# ---------------------------------------------------------------------------
# Second-review fixes: cycle guard, register atomicity, table map, max_workers
# ---------------------------------------------------------------------------


class BadPrefixFactor(Factor):
    """Invalid dependency prefix — parse_dependency rejects at register time."""

    name = "bad_prefix_factor"

    def depends_on(self):
        return ["bogus:dep"]

    def compute(self, data):
        return pd.DataFrame(columns=["date", "ticker", "bad_prefix_factor"])


class TestComputeFactorCycleGuard:
    def test_compute_factor_on_cycle_raises_value_error(self):
        """compute_factor on a cyclic graph must raise ValueError, not
        RecursionError — the guard is on the public entry point, not just
        topological_sort()."""
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(CycleA())
        dag.register(CycleB())
        with pytest.raises(ValueError, match="[Cc]ircular|cycle"):
            compute_factor(dag, "cycle_a", "2024-01-02", "2024-01-08")

    def test_compute_factor_self_cycle_raises_value_error(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SelfCycle())
        with pytest.raises(ValueError, match="[Cc]ircular|cycle"):
            compute_factor(dag, "self_cycle", "2024-01-02", "2024-01-08")

    def test_compute_factor_not_recursion_error(self):
        """Explicitly assert the exception type is not RecursionError."""
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(CycleA())
        dag.register(CycleB())
        try:
            compute_factor(dag, "cycle_a", "2024-01-02", "2024-01-08")
        except RecursionError:
            pytest.fail("cycle in compute_factor gave RecursionError, not ValueError")
        except ValueError:
            pass  # expected


class TestRegisterAtomicity:
    def test_rejected_dep_leaves_dag_clean(self):
        """A failed register must not leave a phantom node in self.factors.

        Before the fix, register() committed self.factors[name] before
        parse_dependency raised, so topological_sort() silently succeeded
        with a node that had no _upstream entry and an unparseable dep.
        """
        dag = FactorDAG(data_loader=_synthetic_loader)
        with pytest.raises(ValueError, match="prefix|dependency"):
            dag.register(BadPrefixFactor())

        assert "bad_prefix_factor" not in dag.factors
        assert "bad_prefix_factor" not in dag._upstream

    def test_sort_after_failed_register_is_consistent(self):
        """topological_sort() must not silently include a phantom node."""
        dag = FactorDAG(data_loader=_synthetic_loader)
        with pytest.raises(ValueError):
            dag.register(BadPrefixFactor())

        dag.register(SimpleReturn())
        order = dag.topological_sort()
        assert "bad_prefix_factor" not in order
        assert "returns_1d" in order

    def test_duplicate_check_still_works(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        with pytest.raises(ValueError, match="already registered"):
            dag.register(SimpleReturn())


class TestMaxWorkersWarn:
    def test_max_workers_set_warns(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compute_all(dag, "2024-01-02", "2024-01-08", max_workers=2)
            assert any("max_workers" in str(x.message) for x in w), (
                "compute_all should warn when max_workers is set"
            )

    def test_max_workers_default_no_warn(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compute_all(dag, "2024-01-02", "2024-01-08")
            assert not any("max_workers" in str(x.message) for x in w)

    def test_max_workers_safe_warns(self):
        dag = FactorDAG(data_loader=_synthetic_loader)
        dag.register(SimpleReturn())
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            compute_all_safe(dag, "2024-01-02", "2024-01-08", max_workers=8)
            assert any("max_workers" in str(x.message) for x in w), (
                "compute_all_safe should warn when max_workers is set"
            )
