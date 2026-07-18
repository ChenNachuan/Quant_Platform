# -*- coding: utf-8 -*-
"""
股票现金流扩展因子（从 AMQI 仓库迁移）
包括：Op Asset Chg, Op Cost Margin, Sales Expense Ratio, Tax Rate
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockOpAssetChg(Factor):
    """
    股票经营资产变化因子

    公式：经营资产的环比变化

    方向：数值越大，经营资产增长越快
    """

    name = "stock_op_asset_chg"
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
        if 'operating_assets' not in data.columns:
            raise KeyError("缺少必要列: operating_assets")

        result = data['operating_assets'].pct_change()
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockOpCostMargin(Factor):
    """
    股票经营成本利润率因子

    公式：营业利润 / 营业成本

    方向：数值越大，成本控制越好
    """

    name = "stock_op_cost_margin"
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
        if not set(['oper_profit', 'oper_cost']).issubset(data.columns):
            raise KeyError("缺少必要列: oper_profit, oper_cost")

        result = data['oper_profit'] / data['oper_cost'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockSalesExpenseRatio(Factor):
    """
    股票销售费用率因子

    公式：销售费用 / 营业收入

    方向：数值越大，销售费用占比越高
    """

    name = "stock_sales_expense_ratio"
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
        if not set(['sell_expense', 'revenue']).issubset(data.columns):
            raise KeyError("缺少必要列: sell_expense, revenue")

        result = data['sell_expense'] / data['revenue'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockTaxRate(Factor):
    """
    股票有效税率因子

    公式：所得税 / 利润总额

    方向：数值越大，税负越重
    """

    name = "stock_tax_rate"
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
        if not set(['income_tax', 'total_profit']).issubset(data.columns):
            raise KeyError("缺少必要列: income_tax, total_profit")

        result = data['income_tax'] / data['total_profit'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockOpAssetChg', 'StockOpCostMargin', 'StockSalesExpenseRatio', 'StockTaxRate']
