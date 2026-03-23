"""
测试 RiskAdjustedMomentum 增量更新 vs 全量计算一致性。
验证 IncrementalUpdater 的 deps 传递修复是否正确。
"""
import numpy as np
import pandas as pd
import pytest

from factor_library.technical.momentum import MomentumReturn
from factor_library.technical.volatility import Volatility
from factor_library.technical.risk_adjusted_momentum import RiskAdjustedMomentum


# ── helpers ───────────────────────────────────────────────────

def _make_ohlcv(n: int = 120, seed: int = 42) -> pd.DataFrame:
    """生成单只股票的模拟 OHLCV 数据。"""
    rng = np.random.default_rng(seed)
    close = 10.0 * np.cumprod(1 + rng.normal(0, 0.01, n))
    dates = pd.date_range("2023-01-01", periods=n, freq="B")
    return pd.DataFrame({
        "timestamp": dates,
        "open": close * rng.uniform(0.99, 1.01, n),
        "high": close * rng.uniform(1.00, 1.02, n),
        "low":  close * rng.uniform(0.98, 1.00, n),
        "close": close,
        "volume": rng.integers(1_000_000, 5_000_000, n).astype(float),
    }).set_index("timestamp")


# ── fixtures ──────────────────────────────────────────────────

WINDOW = 20


@pytest.fixture
def ohlcv():
    return _make_ohlcv()


@pytest.fixture
def momentum_factor():
    return MomentumReturn(timeframe="1d", para={"window": WINDOW})


@pytest.fixture
def volatility_factor():
    return Volatility(timeframe="1d", para={"window": WINDOW})


@pytest.fixture
def ram_factor():
    return RiskAdjustedMomentum(timeframe="1d", para={"window": WINDOW})


# ── tests ─────────────────────────────────────────────────────

class TestDepsFlowThroughCache:
    """验证 incremental_cache 正确传递 deps 给复合因子。"""

    def test_ram_update_raises_without_deps(self, ram_factor, ohlcv):
        """RiskAdjustedMomentum.update() 在 deps=None 时应抛出 ValueError。"""
        history = ohlcv.iloc[:-1]
        new_data = ohlcv.iloc[-1:]
        with pytest.raises(ValueError, match="缺少依赖因子"):
            ram_factor.update(new_data, history, deps=None)

    def test_ram_update_succeeds_with_deps(self, ram_factor, momentum_factor,
                                           volatility_factor, ohlcv):
        """提供正确 deps 时，RiskAdjustedMomentum.update() 应返回非空 Series。"""
        history = ohlcv.iloc[:-1]
        new_data = ohlcv.iloc[-1:]

        mom_result = momentum_factor.update(new_data, history)
        vol_result = volatility_factor.update(new_data, history)

        deps = {"momentum_return": mom_result, "volatility": vol_result}
        result = ram_factor.update(new_data, history, deps=deps)

        assert isinstance(result, pd.Series)
        assert not result.empty

    def test_incremental_cache_key_format(self, momentum_factor):
        """验证 get_id() 格式与 generate_factor_id 一致。"""
        from factor_library.registry import generate_factor_id
        expected = generate_factor_id("momentum_return", "1d", {"window": WINDOW})
        assert momentum_factor.get_id() == expected


class TestIncrementalVsFullCompute:
    """验证增量更新结果与全量计算结果在数值上一致。"""

    def test_momentum_incremental_matches_full(self, momentum_factor, ohlcv):
        """Momentum 增量更新的最后一个值应与全量计算一致。"""
        full_result = momentum_factor.compute(ohlcv)

        history = ohlcv.iloc[:-1]
        new_data = ohlcv.iloc[-1:]
        incremental_result = momentum_factor.update(new_data, history)

        last_full = full_result.iloc[-1]
        last_incr = incremental_result.iloc[-1]

        if pd.isna(last_full):
            assert pd.isna(last_incr)
        else:
            assert abs(last_full - last_incr) < 1e-10

    def test_volatility_incremental_matches_full(self, volatility_factor, ohlcv):
        """Volatility 增量更新的最后一个值应与全量计算一致。"""
        full_result = volatility_factor.compute(ohlcv)

        history = ohlcv.iloc[:-1]
        new_data = ohlcv.iloc[-1:]
        incremental_result = volatility_factor.update(new_data, history)

        last_full = full_result.iloc[-1]
        last_incr = incremental_result.iloc[-1]

        if pd.isna(last_full):
            assert pd.isna(last_incr)
        else:
            assert abs(last_full - last_incr) < 1e-10

    def test_ram_incremental_matches_full(self, ram_factor, momentum_factor,
                                          volatility_factor, ohlcv):
        """RiskAdjustedMomentum 增量更新结果应与全量计算一致。"""
        # 全量计算
        mom_full = momentum_factor.compute(ohlcv)
        vol_full = volatility_factor.compute(ohlcv)
        ram_full = ram_factor.compute(ohlcv, deps={
            "momentum_return": mom_full,
            "volatility": vol_full,
        })

        # 增量计算
        history = ohlcv.iloc[:-1]
        new_data = ohlcv.iloc[-1:]
        mom_incr = momentum_factor.update(new_data, history)
        vol_incr = volatility_factor.update(new_data, history)
        ram_incr = ram_factor.update(new_data, history, deps={
            "momentum_return": mom_incr,
            "volatility": vol_incr,
        })

        last_full = ram_full.iloc[-1]
        last_incr = ram_incr.iloc[-1]

        if pd.isna(last_full):
            assert pd.isna(last_incr)
        else:
            assert abs(last_full - last_incr) < 1e-10
