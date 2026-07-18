# -*- coding: utf-8 -*-
"""
股票流动性/规模扩展因子（从 AMQI 仓库迁移）
包括：Logffmv, FFMC, CVILLIQ
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockLogffmv(Factor):
    """
    股票对数自由流通市值因子

    公式：log(自由流通市值)

    方向：数值越大，自由流通市值越大
    """

    name = "stock_logffmv"
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
        if 'float_mv' not in data.columns:
            raise KeyError("缺少必要列: float_mv")

        result = np.log(data['float_mv'].replace(0, np.nan))
        result = result.replace([np.inf, -np.inf], np.nan)

        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockFFMC(Factor):
    """
    股票自由流通市值因子

    公式：自由流通市值

    方向：数值越大，自由流通市值越大
    """

    name = "stock_ffmc"
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
        if 'float_mv' not in data.columns:
            raise KeyError("缺少必要列: float_mv")

        return data['float_mv']

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockCVILLIQ(Factor):
    """
    股票 CVILLIQ 因子

    公式：Amihud 非流动性的变异系数

    方向：数值越大，流动性波动越剧烈
    """

    name = "stock_cvilliq"
    input_type = "bar"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [20]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if not set(['close', 'volume']).issubset(data.columns):
            raise KeyError("缺少必要列: close, volume")

        window = self.para.get("window", 20)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
            volume = data['volume'].unstack(level='code')
        else:
            close = data['close']
            volume = data['volume']

        returns = close.pct_change()
        illiq = (returns.abs() / volume.replace(0, np.nan)).rolling(window).mean()
        illiq_std = illiq.rolling(window).std()
        illiq_mean = illiq.rolling(window).mean()

        result = illiq_std / illiq_mean.abs().clip(lower=1e-6)
        result = result.replace([np.inf, -np.inf], np.nan)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockLogffmv', 'StockFFMC', 'StockCVILLIQ']
