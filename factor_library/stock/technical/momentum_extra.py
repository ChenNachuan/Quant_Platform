# -*- coding: utf-8 -*-
"""
股票动量扩展因子（从 AMQI 仓库迁移）
包括：Momentum 12M, Rank Momentum
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockMomentum12M(Factor):
    """
    股票 12 个月动量因子

    公式：过去 12 个月的收益率

    方向：数值越大，动量越强
    """

    name = "stock_momentum_12m"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [252]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")

        window = self.para.get("window", 252)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        result = close.pct_change(periods=window)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 252)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockRankMomentum(Factor):
    """
    股票排名动量因子

    公式：收益率的截面排名

    方向：数值越大，排名越靠前
    """

    name = "stock_rank_momentum"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20, 60]}}

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

        returns = close.pct_change(periods=window)
        result = returns.rank(axis=1, pct=True)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockMomentum12M', 'StockRankMomentum']
