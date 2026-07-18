# -*- coding: utf-8 -*-
"""
股票 PVT 扩展因子（从 AMQI 仓库迁移）
包括：PVT MA Deviation, PVT Momentum Reversal
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockPVTMADeviation(Factor):
    """
    股票 PVT MA Deviation 因子

    公式：PVT 与其移动平均的偏差

    方向：数值越大，PVT 偏离均线越远
    """

    name = "stock_pvt_ma_deviation"
    input_type = "bar"
    max_lookback = 270
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20]}}

    dependencies = ["stock_pvt"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if deps is None or 'stock_pvt' not in deps:
            raise ValueError("缺少依赖因子: stock_pvt")

        window = self.para.get("window", 20)
        pvt = deps['stock_pvt']

        if isinstance(pvt.index, pd.MultiIndex):
            pvt_u = pvt.unstack(level='code')
        else:
            pvt_u = pvt

        pvt_ma = pvt_u.rolling(window).mean()
        result = (pvt_u - pvt_ma) / pvt_ma.abs().clip(lower=1e-6)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockPVTMomentumReversal(Factor):
    """
    股票 PVT Momentum Reversal 因子

    公式：PVT 动量反转信号

    方向：正值表示动量，负值表示反转
    """

    name = "stock_pvt_momentum_reversal"
    input_type = "bar"
    max_lookback = 270
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"short_window": [5], "long_window": [20]}}

    dependencies = ["stock_pvt"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"short_window": 5, "long_window": 20}]

    def compute(self, data, deps=None):
        if deps is None or 'stock_pvt' not in deps:
            raise ValueError("缺少依赖因子: stock_pvt")

        short_window = self.para.get("short_window", 5)
        long_window = self.para.get("long_window", 20)
        pvt = deps['stock_pvt']

        if isinstance(pvt.index, pd.MultiIndex):
            pvt_u = pvt.unstack(level='code')
        else:
            pvt_u = pvt

        short_ma = pvt_u.rolling(short_window).mean()
        long_ma = pvt_u.rolling(long_window).mean()

        result = (short_ma - long_ma) / long_ma.abs().clip(lower=1e-6)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        long_window = self.para.get("long_window", 20)
        recent = history.iloc[-long_window:] if len(history) >= long_window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockPVTMADeviation', 'StockPVTMomentumReversal']
