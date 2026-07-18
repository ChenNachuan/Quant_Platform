# -*- coding: utf-8 -*-
"""
股票量价因子（从 AMQI 仓库迁移）
包括：Volume Price Divergence
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockVolumePriceDivergence(Factor):
    """
    股票量价背离因子

    公式：成交量变化与价格变化的背离

    方向：正值表示量价正背离（量升价跌），负值表示负背离
    """

    name = "stock_volume_price_divergence"
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

        price_change = close.pct_change(window)
        volume_change = volume.pct_change(window)

        # 量价背离 = 成交量变化 - 价格变化
        result = volume_change - price_change

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        window = self.para.get("window", 20)
        recent = history.iloc[-window:] if len(history) >= window else history
        return self.compute(pd.concat([recent, new_data]), deps).iloc[-len(new_data):]


__all__ = ['StockVolumePriceDivergence']
