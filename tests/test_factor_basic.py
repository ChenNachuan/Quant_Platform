"""Factor library core functionality tests — pure-logic, no database required."""
import numpy as np
import pandas as pd
import pytest

from factor_library import Factor, FactorRegistry
from factor_library.operators import ts_mean, cs_rank, cs_zscore
from factor_library.preprocessor import PostProcessor


# ── helpers ──────────────────────────────────────────────────

class MockMomentum(Factor):
    """Minimal factor for testing (not registered)."""
    name = "test_momentum"
    input_type = "bar"
    max_lookback = 60
    applicable_market = []
    store_time = "20260217"
    para_group = {"1d": {"window": [5, 10, 20]}}

    def generate_para_space(self):
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"Missing required columns: {required_cols}")
        window = self.para.get("window")
        if window is None:
            raise ValueError("Parameter 'window' is missing")
        return ts_mean(data['close'].pct_change(), window)


@pytest.fixture
def factor():
    return MockMomentum(timeframe='1d', para={'window': 10})


@pytest.fixture
def mock_ohlcv():
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=50, freq='D')
    codes = ['TEST.SZ'] * 50
    df = pd.DataFrame({
        'timestamp': dates,
        'code': codes,
        'close': 100 + np.random.randn(50).cumsum() * 2
    })
    return df.set_index(['timestamp', 'code'])


# ── module import ────────────────────────────────────────────

class TestImports:
    def test_core_modules_importable(self):
        pass

    def test_registry_has_registered_factors(self):
        # Import technical factors to trigger registration
        import factor_library.technical.momentum  # noqa: F401
        import factor_library.technical.volatility  # noqa: F401
        registered = FactorRegistry.list_factors()
        assert len(registered) >= 2


# ── factor creation ──────────────────────────────────────────

class TestFactorCreation:
    def test_create_factor_instance(self, factor):
        assert factor.name == "test_momentum"
        assert factor.timeframe == '1d'
        assert factor.para == {'window': 10}

    def test_factor_repr(self, factor):
        r = repr(factor)
        assert "test_momentum" in r
        assert "1d" in r

    def test_factor_id_format(self, factor):
        fid = factor.get_id()
        assert "test_momentum" in fid
        assert "1d" in fid
        assert "window10" in fid


# ── parameter space ──────────────────────────────────────────

class TestParameterSpace:
    def test_generate_para_space(self, factor):
        space = factor.generate_para_space()
        assert len(space) == 3
        assert space[0] == {'window': 5}
        assert space[1] == {'window': 10}
        assert space[2] == {'window': 20}

    def test_para_space_empty_for_unknown_timeframe(self):
        f = MockMomentum(timeframe='5min', para={'window': 10})
        assert f.generate_para_space() == []


# ── operators ────────────────────────────────────────────────

class TestOperators:
    def test_ts_mean(self):
        s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        result = ts_mean(s, 3)
        expected = s.rolling(3).mean()
        pd.testing.assert_series_equal(result, expected)

    def test_cs_rank(self):
        s = pd.Series([10, 20, 30, 40, 50])
        result = cs_rank(s)
        assert result.iloc[-1] == 1.0  # highest value gets rank 1.0
        assert result.iloc[0] == 0.2   # lowest value gets rank 0.2

    def test_cs_zscore(self):
        s = pd.Series([10, 20, 30, 40, 50])
        result = cs_zscore(s)
        assert abs(result.mean()) < 1e-10
        assert abs(result.std() - 1.0) < 1e-10


# ── factor computation ───────────────────────────────────────

class TestFactorComputation:
    def test_compute_returns_series(self, factor, mock_ohlcv):
        result = factor.compute(mock_ohlcv)
        assert isinstance(result, pd.Series)
        assert len(result) == 50

    def test_nan_prefix_length_matches_window(self, factor, mock_ohlcv):
        result = factor.compute(mock_ohlcv)
        # First (window-1) values should be NaN due to rolling window
        assert result.iloc[:9].isna().all()

    def test_valid_values_after_window(self, factor, mock_ohlcv):
        result = factor.compute(mock_ohlcv)
        # After the window, most values should be valid
        assert result.iloc[9:].notna().sum() > len(result.iloc[9:]) * 0.8

    def test_compute_with_postprocess(self, factor, mock_ohlcv):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = factor.compute_with_postprocess(mock_ohlcv)
        assert isinstance(result, pd.Series)
        # After postprocessing, result should have values
        valid = result.dropna()
        assert len(valid) > 0

    def test_missing_close_column_raises(self, factor):
        bad_data = pd.DataFrame({'open': [1, 2, 3]}, index=pd.MultiIndex.from_tuples([(1, 'A'), (2, 'A'), (3, 'A')]))
        with pytest.raises(KeyError):
            factor.compute(bad_data)

    def test_missing_window_param_raises(self):
        f = MockMomentum(timeframe='1d', para={})
        data = pd.DataFrame({'close': [1, 2, 3]}, index=pd.MultiIndex.from_tuples([(1, 'A'), (2, 'A'), (3, 'A')]))
        with pytest.raises(ValueError):
            f.compute(data)


# ── postprocessor ────────────────────────────────────────────

class TestPostProcessor:
    @pytest.fixture
    def series(self):
        return pd.Series(np.random.randn(100) * 10 + 50)

    def test_winsorize_clips_outliers(self, series):
        result = PostProcessor.winsorize(series, lower=0.05, upper=0.95)
        assert result.min() >= series.quantile(0.05)
        assert result.max() <= series.quantile(0.95)

    def test_standardize_zscore(self, series):
        result = PostProcessor.standardize(series, method='zscore')
        assert abs(result.mean()) < 1e-10
        assert abs(result.std() - 1.0) < 1e-10

    def test_check_validity(self, series):
        result = PostProcessor.check_validity(series)
        assert result['valid'] == True  # noqa: E712
        assert result['nan_count'] == 0

    def test_check_validity_with_nan(self):
        s = pd.Series([1.0, np.nan, 3.0, np.nan, 5.0])
        result = PostProcessor.check_validity(s, max_nan_ratio=0.0)
        assert result['valid'] == False  # noqa: E712
        assert result['nan_count'] == 2

    def test_fillna_ffill(self):
        s = pd.Series([1.0, np.nan, 3.0])
        result = PostProcessor.fillna(s, method='ffill')
        assert result.iloc[1] == 1.0

    def test_fillna_zero(self):
        s = pd.Series([1.0, np.nan, 3.0])
        result = PostProcessor.fillna(s, method='zero')
        assert result.iloc[1] == 0.0

    def test_unknown_standardize_raises(self, series):
        with pytest.raises(ValueError):
            PostProcessor.standardize(series, method='unknown')

    def test_unknown_fillna_raises(self):
        s = pd.Series([1.0, np.nan])
        with pytest.raises(ValueError):
            PostProcessor.fillna(s, method='unknown')
