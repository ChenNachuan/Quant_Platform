# -*- coding: utf-8 -*-
"""
股票 Bollinger 扩展因子（从 AMQI 仓库迁移）
包括：Bollinger Breakout Upper, Bollinger Middle Support, Bollinger Oversold Bounce
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockBollingerBreakoutUpper(Factor):
    """
    股票 Bollinger Breakout Upper 因子

    公式：(Close - UpperBand) / UpperBand

    方向：正值表示突破上轨
    """

    name = "stock_bollinger_breakout_upper"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20], "num_std": [2]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": 20, "num_std": 2}]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")
        window = self.para.get("window", 20)
        num_std = self.para.get("num_std", 2)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        ma = close.rolling(window).mean()
        std = close.rolling(window).std()
        upper = ma + num_std * std

        result = (close - upper) / upper

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockBollingerMiddleSupport(Factor):
    """
    股票 Bollinger Middle Support 因子

    公式：(Close - MA) / MA

    方向：正值表示在均线上方
    """

    name = "stock_bollinger_middle_support"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")
        window = self.para.get("window", 20)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        ma = close.rolling(window).mean()
        result = (close - ma) / ma

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockBollingerOversoldBounce(Factor):
    """
    股票 Bollinger Oversold Bounce 因子

    公式：从下轨反弹的幅度

    方向：数值越大，反弹越强
    """

    name = "stock_bollinger_oversold_bounce"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20], "num_std": [2]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": 20, "num_std": 2}]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")
        window = self.para.get("window", 20)
        num_std = self.para.get("num_std", 2)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        ma = close.rolling(window).mean()
        std = close.rolling(window).std()
        lower = ma - num_std * std

        # 计算从下轨的反弹幅度
        result = (close - lower) / lower

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockBollingerBreakoutUpper', 'StockBollingerMiddleSupport', 'StockBollingerOversoldBounce']
