# -*- coding: utf-8 -*-
"""
股票 Frazzini Pedersen Beta 因子（从 AMQI 仓库迁移）
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockFrazziniPedersenBeta(Factor):
    """
    股票 Frazzini Pedersen Beta 因子

    公式：使用 EWMA 方法计算的 Beta

    方向：数值大于1表示波动大于市场
    """

    name = "stock_frazzini_pedersen_beta"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [60], "lambda_param": [0.97]}}

    dependencies = []
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": 60, "lambda_param": 0.97}]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")

        window = self.para.get("window", 60)
        lambda_param = self.para.get("lambda_param", 0.97)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        returns = close.pct_change()

        # 如果没有市场收益率，使用等权平均
        if 'mkt_ret' in data.columns:
            if isinstance(data.index, pd.MultiIndex):
                mkt_ret = data['mkt_ret'].unstack(level='code').iloc[:, 0]
            else:
                mkt_ret = data['mkt_ret']
        else:
            mkt_ret = returns.mean(axis=1)

        # 使用 EWMA 计算 Beta
        def ewma_beta(stock_ret, mkt_ret, window, lambda_param):
            """计算 EWMA Beta"""
            cov = stock_ret.ewm(span=window, adjust=False).cov(mkt_ret)
            var = mkt_ret.ewm(span=window, adjust=False).var()
            return cov / var.replace(0, np.nan)

        if isinstance(data.index, pd.MultiIndex):
            result = pd.DataFrame(index=returns.index, columns=returns.columns)
            for col in returns.columns:
                result[col] = ewma_beta(returns[col], mkt_ret, window, lambda_param)
        else:
            result = ewma_beta(returns, mkt_ret, window, lambda_param)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 60)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockFrazziniPedersenBeta']
