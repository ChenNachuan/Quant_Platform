# -*- coding: utf-8 -*-
"""
股票 OBV 扩展因子（从 AMQI 仓库迁移）
包括：OBV Breakthrough, OBV Change Rate, OBV Divergence, OBV Rank
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockOBVBreakthrough(Factor):
    """
    股票 OBV Breakthrough 因子

    公式：OBV 突破其移动平均线

    方向：正值表示 OBV 突破均线
    """

    name = "stock_obv_breakthrough"
    input_type = "bar"
    max_lookback = 270
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20]}}

    dependencies = ["stock_obv"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if deps is None or 'stock_obv' not in deps:
            raise ValueError("缺少依赖因子: stock_obv")

        window = self.para.get("window", 20)
        obv = deps['stock_obv']

        if isinstance(obv.index, pd.MultiIndex):
            obv_u = obv.unstack(level='code')
        else:
            obv_u = obv

        obv_ma = obv_u.rolling(window).mean()
        result = (obv_u - obv_ma) / obv_ma.abs().clip(lower=1e-6)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockOBVChangeRate(Factor):
    """
    股票 OBV Change Rate 因子

    公式：OBV 的变化率

    方向：数值越大，OBV 上升越快
    """

    name = "stock_obv_change_rate"
    input_type = "bar"
    max_lookback = 260
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20]}}

    dependencies = ["stock_obv"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if deps is None or 'stock_obv' not in deps:
            raise ValueError("缺少依赖因子: stock_obv")

        window = self.para.get("window", 20)
        obv = deps['stock_obv']

        if isinstance(obv.index, pd.MultiIndex):
            obv_u = obv.unstack(level='code')
        else:
            obv_u = obv

        result = obv_u.pct_change(periods=window)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockOBVDivergence(Factor):
    """
    股票 OBV Divergence 因子

    公式：OBV 与价格的背离

    方向：正值表示正背离
    """

    name = "stock_obv_divergence"
    input_type = "bar"
    max_lookback = 270
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20]}}

    dependencies = ["stock_obv"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")
        if deps is None or 'stock_obv' not in deps:
            raise ValueError("缺少依赖因子: stock_obv")

        window = self.para.get("window", 20)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
            obv = deps['stock_obv'].unstack(level='code')
        else:
            close = data['close']
            obv = deps['stock_obv']

        price_change = close.pct_change(window)
        obv_change = obv.pct_change(window)

        result = obv_change - price_change

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockOBVRank(Factor):
    """
    股票 OBV Rank 因子

    公式：OBV 的截面排名

    方向：数值越大，OBV 排名越高
    """

    name = "stock_obv_rank"
    input_type = "bar"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {}}

    dependencies = ["stock_obv"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{}]

    def compute(self, data, deps=None):
        if deps is None or 'stock_obv' not in deps:
            raise ValueError("缺少依赖因子: stock_obv")

        obv = deps['stock_obv']

        if isinstance(obv.index, pd.MultiIndex):
            obv_u = obv.unstack(level='code')
        else:
            obv_u = obv

        result = obv_u.rank(axis=1, pct=True)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockOBVBreakthrough', 'StockOBVChangeRate', 'StockOBVDivergence', 'StockOBVRank']
