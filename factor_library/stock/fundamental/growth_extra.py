# -*- coding: utf-8 -*-
"""
股票增长扩展因子（从 AMQI 仓库迁移）
包括：CAGR Capex, Issuance Growth Rate
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockCAGRCapex(Factor):
    """
    股票资本支出 CAGR 因子

    公式：资本支出的复合年增长率

    方向：数值越大，资本支出增长越快
    """

    name = "stock_cagr_capex"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"years": [3]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"years": y} for y in self.para_group[self.timeframe]["years"]]

    def compute(self, data, deps=None):
        if 'capex' not in data.columns:
            raise KeyError("缺少必要列: capex")

        years = self.para.get("years", 3)
        periods = years * 4  # 季度数据

        capex = data['capex']
        capex_shifted = capex.shift(periods)

        # CAGR = (End/Begin)^(1/n) - 1
        result = ((capex / capex_shifted.replace(0, np.nan)) ** (1 / years)) - 1
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        years = self.para.get("years", 3)
        lookback = years * 4
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockIssuanceGrowthRate(Factor):
    """
    股票发行增长因子

    公式：股本变化率

    方向：数值越大，股本扩张越快
    """

    name = "stock_issuance_growth_rate"
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
        if 'total_share' not in data.columns:
            raise KeyError("缺少必要列: total_share")

        result = data['total_share'].pct_change()
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockCAGRCapex', 'StockIssuanceGrowthRate']
