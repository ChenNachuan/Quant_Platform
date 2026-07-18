# -*- coding: utf-8 -*-
"""
股票盈利能力扩展因子（从 AMQI 仓库迁移）
包括：ROIC QoQ Change, Quarterly ROIC, Quarterly Abnormal GM
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockROICQoQChange(Factor):
    """
    股票 ROIC 环比变化因子

    公式：(本期ROIC - 上期ROIC) / 上期ROIC

    方向：数值越大，ROIC 改善越快
    """

    name = "stock_roic_qoq_change"
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
        if 'roic' not in data.columns:
            raise KeyError("缺少必要列: roic")

        roic = data['roic']
        result = (roic - roic.shift(1)) / roic.shift(1).abs().clip(lower=1e-6)
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockQuarterlyROIC(Factor):
    """
    股票季度 ROIC 因子

    公式：NOPIC / Invested Capital

    方向：数值越大，资本回报率越高
    """

    name = "stock_quarterly_roic"
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
        # ROIC 通常已经在数据中计算好
        if 'roic' in data.columns:
            return data['roic']
        elif 'nopat' in data.columns and 'invested_capital' in data.columns:
            result = data['nopat'] / data['invested_capital']
            return result.replace([np.inf, -np.inf], np.nan)
        else:
            raise KeyError("缺少必要列: roic 或 (nopat, invested_capital)")

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockQuarterlyAbnormalGM(Factor):
    """
    股票季度异常毛利率因子

    公式：毛利率 - 行业平均毛利率

    方向：数值越大，毛利率越高于行业平均
    """

    name = "stock_quarterly_abnormal_gm"
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
        if 'gross_margin' not in data.columns:
            raise KeyError("缺少必要列: gross_margin")

        gm = data['gross_margin']

        # 如果有行业信息，计算行业平均
        if 'industry' in data.columns:
            industry_avg = gm.groupby(data['industry']).transform('mean')
            result = gm - industry_avg
        else:
            # 如果没有行业信息，使用截面平均
            if isinstance(gm.index, pd.MultiIndex):
                gm_u = gm.unstack(level='code')
                result = gm_u - gm_u.mean(axis=1)
                result = result.stack()
                result.index.names = ['timestamp', 'code']
            else:
                result = gm - gm.mean()

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


__all__ = ['StockROICQoQChange', 'StockQuarterlyROIC', 'StockQuarterlyAbnormalGM']
