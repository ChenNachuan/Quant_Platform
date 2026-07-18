# -*- coding: utf-8 -*-
"""
股票 Ichimoku 扩展因子（从 AMQI 仓库迁移）
包括：Ichimoku Cloud Width Momentum, Ichimoku Price Position
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockIchimokuCloudWidthMomentum(Factor):
    """
    股票 Ichimoku Cloud Width Momentum 因子

    公式：云宽的变化率

    方向：数值上升表示云在变宽
    """

    name = "stock_ichimoku_cloud_width_momentum"
    input_type = "bar"
    max_lookback = 52
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"tenkan": [9], "kijun": [26], "senkou": [52]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"tenkan": 9, "kijun": 26, "senkou": 52}]

    def compute(self, data, deps=None):
        if not set(['high', 'low']).issubset(data.columns):
            raise KeyError("缺少必要列: high, low")

        tenkan_period = self.para.get("tenkan", 9)
        kijun_period = self.para.get("kijun", 26)
        senkou_period = self.para.get("senkou", 52)

        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
        else:
            high, low = data['high'], data['low']

        tenkan = (high.rolling(tenkan_period).max() + low.rolling(tenkan_period).min()) / 2
        kijun = (high.rolling(kijun_period).max() + low.rolling(kijun_period).min()) / 2
        senkou_a = (tenkan + kijun) / 2
        senkou_b = (high.rolling(senkou_period).max() + low.rolling(senkou_period).min()) / 2

        cloud_width = (senkou_a - senkou_b).abs()
        result = cloud_width.pct_change()

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        lookback = self.para.get("senkou", 52)
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockIchimokuPricePosition(Factor):
    """
    股票 Ichimoku Price Position 因子

    公式：价格相对于云的位置

    方向：正值在云上，负值在云下
    """

    name = "stock_ichimoku_price_position"
    input_type = "bar"
    max_lookback = 52
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"tenkan": [9], "kijun": [26], "senkou": [52]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"tenkan": 9, "kijun": 26, "senkou": 52}]

    def compute(self, data, deps=None):
        if not set(['high', 'low', 'close']).issubset(data.columns):
            raise KeyError("缺少必要列: high, low, close")

        tenkan_period = self.para.get("tenkan", 9)
        kijun_period = self.para.get("kijun", 26)
        senkou_period = self.para.get("senkou", 52)

        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high, low, close = data['high'], data['low'], data['close']

        tenkan = (high.rolling(tenkan_period).max() + low.rolling(tenkan_period).min()) / 2
        kijun = (high.rolling(kijun_period).max() + low.rolling(kijun_period).min()) / 2
        senkou_a = (tenkan + kijun) / 2
        senkou_b = (high.rolling(senkou_period).max() + low.rolling(senkou_period).min()) / 2

        cloud_top = pd.concat([senkou_a, senkou_b], axis=1).max(axis=1)
        cloud_bottom = pd.concat([senkou_a, senkou_b], axis=1).min(axis=1)
        cloud_width = cloud_top - cloud_bottom
        cloud_width = cloud_width.replace(0, np.nan)

        result = (close - cloud_bottom) / cloud_width

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        lookback = self.para.get("senkou", 52)
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockIchimokuCloudWidthMomentum', 'StockIchimokuPricePosition']
