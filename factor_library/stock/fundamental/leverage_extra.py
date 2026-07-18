# -*- coding: utf-8 -*-
"""
股票资本结构扩展因子（从 AMQI 仓库迁移）
包括：Accruals to Assets, Debt YoY Growth
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockAccrualsToAssets(Factor):
    """
    股票应计项目/总资产因子

    公式：(净利润 - 经营现金流) / 总资产

    方向：数值越大，盈余质量越低
    """

    name = "stock_accruals_to_assets"
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
        if not set(['net_income', 'n_cashflow_act', 'total_assets']).issubset(data.columns):
            raise KeyError("缺少必要列: net_income, n_cashflow_act, total_assets")

        accruals = data['net_income'] - data['n_cashflow_act']
        result = accruals / data['total_assets'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockDebtYoYGrowth(Factor):
    """
    股票债务同比增长因子

    公式：(本期债务 - 去年同期债务) / 去年同期债务

    方向：数值越大，债务增长越快
    """

    name = "stock_debt_yoy_growth"
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
        if 'total_liab' not in data.columns:
            raise KeyError("缺少必要列: total_liab")

        # 同比变化（4个季度）
        result = (data['total_liab'] - data['total_liab'].shift(4)) / data['total_liab'].shift(4).abs().clip(lower=1e-6)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockAccrualsToAssets', 'StockDebtYoYGrowth']
