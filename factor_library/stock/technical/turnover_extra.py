# -*- coding: utf-8 -*-
"""
股票换手率扩展因子（从 AMQI 仓库迁移）
包括：Daily Turnover Rate, Monthly Turnover, Turnover Residual
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockDailyTurnoverRate(Factor):
    """
    股票日换手率因子

    公式：成交量 / 流通股本

    方向：数值越大，交易越活跃
    """

    name = "stock_daily_turnover_rate"
    input_type = "bar"
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
        if 'volume' not in data.columns:
            raise KeyError("缺少必要列: volume")

        if isinstance(data.index, pd.MultiIndex):
            volume = data['volume'].unstack(level='code')
        else:
            volume = data['volume']

        # 如果有流通股本，使用真实换手率
        if 'float_share' in data.columns:
            if isinstance(data.index, pd.MultiIndex):
                float_share = data['float_share'].unstack(level='code')
            else:
                float_share = data['float_share']
            result = volume / float_share
        else:
            # 否则使用成交量作为代理
            result = volume

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        return self.compute(new_data, deps)


@register_factor
class StockMonthlyTurnover(Factor):
    """
    股票月换手率因子

    公式：过去 20 天的平均换手率

    方向：数值越大，月度交易越活跃
    """

    name = "stock_monthly_turnover"
    input_type = "bar"
    max_lookback = 270
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
        if 'volume' not in data.columns:
            raise KeyError("缺少必要列: volume")

        window = self.para.get("window", 20)

        if isinstance(data.index, pd.MultiIndex):
            volume = data['volume'].unstack(level='code')
        else:
            volume = data['volume']

        result = volume.rolling(window).mean()

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


@register_factor
class StockTurnoverResidual(Factor):
    """
    股票换手率残差因子

    公式：换手率对市值回归的残差

    方向：数值越大，异常换手率越高
    """

    name = "stock_turnover_residual"
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
        if 'volume' not in data.columns:
            raise KeyError("缺少必要列: volume")

        window = self.para.get("window", 20)

        if isinstance(data.index, pd.MultiIndex):
            volume = data['volume'].unstack(level='code')
        else:
            volume = data['volume']

        turnover = volume.rolling(window).mean()

        # 如果有市值，计算残差
        if 'total_mv' in data.columns:
            if isinstance(data.index, pd.MultiIndex):
                mv = data['total_mv'].unstack(level='code')
            else:
                mv = data['total_mv']
            log_mv = np.log(mv)

            # 简单线性回归残差
            turnover_mean = turnover.rolling(window).mean()
            mv_mean = log_mv.rolling(window).mean()
            cov = (turnover * log_mv).rolling(window).mean() - turnover_mean * mv_mean
            var = (log_mv ** 2).rolling(window).mean() - mv_mean ** 2
            beta = cov / var.replace(0, np.nan)
            result = turnover - beta * log_mv
        else:
            # 如果没有市值，使用换手率的标准化值
            result = turnover

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockDailyTurnoverRate', 'StockMonthlyTurnover', 'StockTurnoverResidual']
