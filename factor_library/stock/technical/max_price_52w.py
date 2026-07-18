# -*- coding: utf-8 -*-
"""
股票 Max Price 52W Ratio 因子（从 AMQI 仓库迁移）
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockMaxPrice52WRatio(Factor):
    """
    股票 52 周最高价比率因子

    公式：Close / Max(High, 52 weeks)

    方向：数值越接近1，价格越接近52周高点
    """

    name = "stock_max_price_52w_ratio"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [252]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 52 周最高价比率因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            52 周最高价比率值
        """
        required_cols = ['high', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 252)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high = data['high']
            close = data['close']

        # 计算 52 周最高价
        max_high = high.rolling(window=window).max()

        # 计算比率
        ratio = close / max_high

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            ratio = ratio.stack()
            ratio.index.names = ['timestamp', 'code']

        return ratio

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 window 条历史数据
        """
        window = self.para.get("window", 252)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockMaxPrice52WRatio']
