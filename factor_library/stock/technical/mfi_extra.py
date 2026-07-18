# -*- coding: utf-8 -*-
"""
股票 MFI 扩展因子（从 AMQI 仓库迁移）
包括：MFI Divergence
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockMFIDivergence(Factor):
    """
    股票 MFI Divergence 因子

    公式：MFI 与价格的背离

    方向：正值表示正背离，负值表示负背离
    """

    name = "stock_mfi_divergence"
    input_type = "bar"
    max_lookback = 34
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {"1d": {"window": [14]}}

    dependencies = ["stock_mfi"]
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self):
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data, deps=None):
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")
        if deps is None or 'stock_mfi' not in deps:
            raise ValueError("缺少依赖因子: stock_mfi")

        window = self.para.get("window", 14)

        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
            mfi = deps['stock_mfi'].unstack(level='code')
        else:
            close = data['close']
            mfi = deps['stock_mfi']

        price_change = close.pct_change(window)
        mfi_change = mfi.pct_change(window)

        result = mfi_change - price_change

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 14)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockMFIDivergence']
