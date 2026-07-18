# -*- coding: utf-8 -*-
"""
股票运营效率扩展因子（从 AMQI 仓库迁移）
包括：AP Days, FA Ratio, NOAT, Equity Turnover
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockAPDays(Factor):
    """
    股票应付账款周转天数因子

    公式：365 / AP Turnover

    方向：数值越大，付款周期越长
    """

    name = "stock_ap_days"
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
        if not set(['oper_cost', 'accounts_payable']).issubset(data.columns):
            raise KeyError("缺少必要列: oper_cost, accounts_payable")

        ap_turnover = data['oper_cost'] / data['accounts_payable'].replace(0, np.nan)
        result = 365 / ap_turnover.replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockFARatio(Factor):
    """
    股票固定资产比率因子

    公式：固定资产 / 总资产

    方向：数值越大，固定资产占比越高
    """

    name = "stock_fa_ratio"
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
        if not set(['fix_assets', 'total_assets']).issubset(data.columns):
            raise KeyError("缺少必要列: fix_assets, total_assets")

        result = data['fix_assets'] / data['total_assets'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockNOAT(Factor):
    """
    股票净营业资产周转率因子

    公式：营业收入 / 净营业资产

    方向：数值越大，净营业资产利用效率越高
    """

    name = "stock_noat"
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
        if not set(['revenue', 'net_operating_assets']).issubset(data.columns):
            raise KeyError("缺少必要列: revenue, net_operating_assets")

        result = data['revenue'] / data['net_operating_assets'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockEquityTurnover(Factor):
    """
    股票权益周转率因子

    公式：营业收入 / 股东权益

    方向：数值越大，权益利用效率越高
    """

    name = "stock_equity_turnover"
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
        if not set(['revenue', 'total_hldr_eqy_exc_min_int']).issubset(data.columns):
            raise KeyError("缺少必要列: revenue, total_hldr_eqy_exc_min_int")

        result = data['revenue'] / data['total_hldr_eqy_exc_min_int'].replace(0, np.nan)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockAPDays', 'StockFARatio', 'StockNOAT', 'StockEquityTurnover']
