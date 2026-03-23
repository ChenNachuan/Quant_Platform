import warnings
import numpy as np
import pandas as pd
import pytest

from factor_library.preprocessor import PostProcessor


@pytest.fixture
def series():
    return pd.Series([1.0, 2.0, 3.0, 100.0, 5.0])


@pytest.fixture
def series_with_nan():
    return pd.Series([1.0, np.nan, 3.0, np.nan, 5.0])


class TestWinsorize:
    def test_clips_outliers(self, series):
        result = PostProcessor.winsorize(series, lower=0.1, upper=0.9)
        assert result.max() <= series.quantile(0.9)
        assert result.min() >= series.quantile(0.1)

    def test_preserves_length(self, series):
        assert len(PostProcessor.winsorize(series)) == len(series)


class TestStandardize:
    def test_zscore_mean_zero(self, series):
        result = PostProcessor.standardize(series, method='zscore')
        assert abs(result.mean()) < 1e-10

    def test_zscore_std_one(self, series):
        result = PostProcessor.standardize(series, method='zscore')
        assert abs(result.std() - 1.0) < 1e-10

    def test_minmax_range(self, series):
        result = PostProcessor.standardize(series, method='minmax')
        assert result.min() == pytest.approx(0.0)
        assert result.max() == pytest.approx(1.0)

    def test_zscore_constant_warns(self):
        s = pd.Series([5.0, 5.0, 5.0])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = PostProcessor.standardize(s, method='zscore')
            assert len(w) == 1
        pd.testing.assert_series_equal(result, s)

    def test_minmax_constant_warns(self):
        s = pd.Series([3.0, 3.0, 3.0])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = PostProcessor.standardize(s, method='minmax')
            assert len(w) == 1
        pd.testing.assert_series_equal(result, s)

    def test_unknown_method_raises(self, series):
        with pytest.raises(ValueError):
            PostProcessor.standardize(series, method='unknown')


class TestCheckValidity:
    def test_valid_series(self, series):
        result = PostProcessor.check_validity(series)
        assert result['valid'] == True  # noqa: E712
        assert result['nan_count'] == 0

    def test_detects_nan(self, series_with_nan):
        result = PostProcessor.check_validity(series_with_nan, max_nan_ratio=0.0)
        assert result['valid'] == False  # noqa: E712
        assert result['nan_count'] == 2

    def test_detects_inf(self):
        s = pd.Series([1.0, np.inf, 3.0])
        result = PostProcessor.check_validity(s, max_inf_ratio=0.0)
        assert result['valid'] == False  # noqa: E712


class TestFillna:
    def test_ffill(self, series_with_nan):
        result = PostProcessor.fillna(series_with_nan, method='ffill')
        assert result.iloc[1] == 1.0

    def test_bfill(self, series_with_nan):
        result = PostProcessor.fillna(series_with_nan, method='bfill')
        assert result.iloc[1] == 3.0

    def test_mean(self, series_with_nan):
        result = PostProcessor.fillna(series_with_nan, method='mean')
        expected_mean = series_with_nan.mean()
        assert result.iloc[1] == pytest.approx(expected_mean)

    def test_zero(self, series_with_nan):
        result = PostProcessor.fillna(series_with_nan, method='zero')
        assert result.iloc[1] == 0.0

    def test_unknown_method_raises(self, series_with_nan):
        with pytest.raises(ValueError):
            PostProcessor.fillna(series_with_nan, method='unknown')
