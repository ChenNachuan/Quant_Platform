# -*- coding: utf-8 -*-
"""
股票估值扩展因子（从 AMQI 仓库迁移）
包括：EP Change 60D, PEG DY Ratio, Revenue Per Share
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockEPChange60D(Factor):
    """
    股票 EP 60 日变化因子

    公式：EP 的 60 日变化率

    方向：数值越大，EP 上升越快
    """

    name = "stock_ep_change_60d"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [60]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'ep' not in data.columns:
            raise KeyError("缺少必要列: ep")

        window = self.para.get("window", 60)
        ep = data['ep']

        result = ep.pct_change(periods=window)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 60)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockPEGDYRatio(Factor):
    """
    股票 PEG/DY 比率因子

    公式：PEG / DY

    方向：数值越小，估值越有吸引力
    """

    name = "stock_peg_dy_ratio"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{}]

    def compute(self, data, deps=None):
        if not set(['peg', 'dv_ttm']).issubset(data.columns):
            raise KeyError("缺少必要列: peg, dv_ttm")

        peg = data['peg']
        dy = data['dv_ttm']

        result = peg / dy.replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockRevenuePerShare(Factor):
    """
    股票每股营收因子

    公式：营业收入 / 总股本

    方向：数值越大，每股营收越高
    """

    name = "stock_revenue_per_share"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{}]

    def compute(self, data, deps=None):
        if not set(['revenue', 'total_share']).issubset(data.columns):
            raise KeyError("缺少必要列: revenue, total_share")

        result = data['revenue'] / data['total_share'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockEPChange60D', 'StockPEGDYRatio', 'StockRevenuePerShare']
