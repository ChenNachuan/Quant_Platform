# -*- coding: utf-8 -*-
"""
股票 RVI 扩展因子（从 AMQI 仓库迁移）
包括：RVI Cross, RVI Diff, RVI Trend, RVI Value, RVI Volume
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockRVICross(Factor):
    """
    股票 RVI Cross 因子

    公式：短期 RVI 与长期 RVI 的交叉

    方向：正值表示金叉，负值表示死叉
    """

    name = "stock_rvi_cross"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"short_period": [4], "long_period": [10]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"short_period": 4, "long_period": 10}]

    def compute(self, data, deps=None):
        if not set(['open', 'high', 'low', 'close']).issubset(data.columns):
            raise KeyError("缺少必要列: open, high, low, close")

        short_period = self.para.get("short_period", 4)
        long_period = self.para.get("long_period", 10)

        if isinstance(data.index, pd.MultiIndex):
            open_p = data['open'].unstack(level='code')
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            open_p, high, low, close = data['open'], data['high'], data['low'], data['close']

        numerator = close - open_p
        denominator = high - low
        denominator = denominator.replace(0, np.nan)

        rvi_raw = numerator / denominator
        short_rvi = rvi_raw.rolling(short_period).mean()
        long_rvi = rvi_raw.rolling(long_period).mean()

        result = short_rvi - long_rvi

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        lookback = self.para.get("long_period", 10)
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockRVIDiff(Factor):
    """
    股票 RVI Diff 因子

    公式：RVI 的一阶差分

    方向：数值上升表示 RVI 在改善
    """

    name = "stock_rvi_diff"
    input_type = "bar"
    max_lookback = 11
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [10]}}

    dependencies = ["stock_rvi"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if deps is None or 'stock_rvi' not in deps:
            raise ValueError("缺少依赖因子: stock_rvi")

        rvi = deps['stock_rvi']

        if isinstance(rvi.index, pd.MultiIndex):
            rvi_u = rvi.unstack(level='code')
        else:
            rvi_u = rvi

        result = rvi_u.diff()

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        recent = history.iloc[-2:] if len(history) >= 2 else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockRVITrend(Factor):
    """
    股票 RVI Trend 因子

    公式：RVI 的线性回归斜率

    方向：数值越大，RVI 上升趋势越强
    """

    name = "stock_rvi_trend"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [10], "trend_window": [10]}}

    dependencies = ["stock_rvi"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": 10, "trend_window": 10}]

    def compute(self, data, deps=None):
        if deps is None or 'stock_rvi' not in deps:
            raise ValueError("缺少依赖因子: stock_rvi")

        trend_window = self.para.get("trend_window", 10)
        rvi = deps['stock_rvi']

        if isinstance(rvi.index, pd.MultiIndex):
            rvi_u = rvi.unstack(level='code')
        else:
            rvi_u = rvi

        result = rvi_u.rolling(trend_window).apply(
            lambda x: np.polyfit(np.arange(len(x)), x, 1)[0] if not np.isnan(x).any() else np.nan,
            raw=False
        )

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        lookback = sum(self.para.get(k, 10) for k in ["window", "trend_window"])
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockRVIValue(Factor):
    """
    股票 RVI Value 因子

    公式：RVI 的标准化值

    方向：数值越大，RVI 越强
    """

    name = "stock_rvi_value"
    input_type = "bar"
    max_lookback = 30
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [10], "norm_window": [20]}}

    dependencies = ["stock_rvi"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": 10, "norm_window": 20}]

    def compute(self, data, deps=None):
        if deps is None or 'stock_rvi' not in deps:
            raise ValueError("缺少依赖因子: stock_rvi")

        norm_window = self.para.get("norm_window", 20)
        rvi = deps['stock_rvi']

        if isinstance(rvi.index, pd.MultiIndex):
            rvi_u = rvi.unstack(level='code')
        else:
            rvi_u = rvi

        rvi_mean = rvi_u.rolling(norm_window).mean()
        rvi_std = rvi_u.rolling(norm_window).std()
        rvi_std = rvi_std.replace(0, np.nan)

        result = (rvi_u - rvi_mean) / rvi_std

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        lookback = sum(self.para.get(k, v) for k, v in [("window", 10), ("norm_window", 20)])
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockRVIVolume(Factor):
    """
    股票 RVI Volume 因子

    公式：RVI 与成交量的相关性

    方向：数值越大，RVI 与成交量的正相关性越强
    """

    name = "stock_rvi_volume"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [10]}}

    dependencies = ["stock_rvi"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'volume' not in data.columns:
            raise KeyError("缺少必要列: volume")
        if deps is None or 'stock_rvi' not in deps:
            raise ValueError("缺少依赖因子: stock_rvi")

        window = self.para.get("window", 10)

        if isinstance(data.index, pd.MultiIndex):
            volume = data['volume'].unstack(level='code')
            rvi = deps['stock_rvi'].unstack(level='code')
        else:
            volume = data['volume']
            rvi = deps['stock_rvi']

        result = rvi.rolling(window).corr(volume)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 10)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockRVICross', 'StockRVIDiff', 'StockRVITrend', 'StockRVIValue', 'StockRVIVolume']
