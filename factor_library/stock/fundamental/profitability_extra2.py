# -*- coding: utf-8 -*-
"""
股票盈利能力扩展因子2（从 AMQI 仓库迁移）
包括：EPSurplus, Int Coverage, Interest Coverage Ratio
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockEPSurplus(Factor):
    """
    股票 EPS 盈余因子

    公式：每股收益的稳定性

    方向：数值越大，EPS 越稳定
    """

    name = "stock_epsurplus"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [4]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'eps' not in data.columns:
            raise KeyError("缺少必要列: eps")

        window = self.para.get("window", 4)
        eps = data['eps']

        # EPS 的变异系数
        eps_mean = eps.rolling(window).mean()
        eps_std = eps.rolling(window).std()

        result = eps_std / eps_mean.abs().clip(lower=1e-6)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 4)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockIntCoverage(Factor):
    """
    股票利息覆盖倍数因子

    公式：EBIT / 利息费用

    方向：数值越大，偿债能力越强
    """

    name = "stock_int_coverage"
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
        if not set(['ebit', 'interest_expense']).issubset(data.columns):
            raise KeyError("缺少必要列: ebit, interest_expense")

        result = data['ebit'] / data['interest_expense'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockInterestCoverageRatio(Factor):
    """
    股票利息覆盖率因子

    公式：营业利润 / 利息费用

    方向：数值越大，偿债能力越强
    """

    name = "stock_interest_coverage_ratio"
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
        if not set(['oper_profit', 'interest_expense']).issubset(data.columns):
            raise KeyError("缺少必要列: oper_profit, interest_expense")

        result = data['oper_profit'] / data['interest_expense'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockEPSurplus', 'StockIntCoverage', 'StockInterestCoverageRatio']
