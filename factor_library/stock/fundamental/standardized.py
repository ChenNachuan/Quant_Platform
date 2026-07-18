# -*- coding: utf-8 -*-
"""
股票标准化因子（从 AMQI 仓库迁移）
包括：Standardized Financial Debt Change Ratio, Standardized Operating Profit
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockStandardizedFinancialDebtChangeRatio(Factor):
    """
    股票标准化金融债务变化率因子

    公式：金融债务变化率的截面标准化

    方向：数值越大，金融债务增长越快
    """

    name = "stock_standardized_financial_debt_change_ratio"
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
        if 'financial_debt' not in data.columns:
            raise KeyError("缺少必要列: financial_debt")

        debt_change = data['financial_debt'].pct_change()

        # 截面标准化
        if isinstance(debt_change.index, pd.MultiIndex):
            debt_change_u = debt_change.unstack(level='code')
            mean = debt_change_u.mean(axis=1)
            std = debt_change_u.std(axis=1)
            result = debt_change_u.sub(mean, axis=0).div(std.replace(0, np.nan), axis=0)
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        else:
            mean = debt_change.mean()
            std = debt_change.std()
            result = (debt_change - mean) / std if std != 0 else debt_change * 0

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockStandardizedOperatingProfit(Factor):
    """
    股票标准化营业利润因子

    公式：营业利润的截面标准化

    方向：数值越大，营业利润越高
    """

    name = "stock_standardized_operating_profit"
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
        if 'oper_profit' not in data.columns:
            raise KeyError("缺少必要列: oper_profit")

        op = data['oper_profit']

        # 截面标准化
        if isinstance(op.index, pd.MultiIndex):
            op_u = op.unstack(level='code')
            mean = op_u.mean(axis=1)
            std = op_u.std(axis=1)
            result = op_u.sub(mean, axis=0).div(std.replace(0, np.nan), axis=0)
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        else:
            mean = op.mean()
            std = op.std()
            result = (op - mean) / std if std != 0 else op * 0

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockStandardizedFinancialDebtChangeRatio', 'StockStandardizedOperatingProfit']
