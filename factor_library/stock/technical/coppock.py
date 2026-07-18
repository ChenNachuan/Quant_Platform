# -*- coding: utf-8 -*-
"""
股票 Coppock Curve 因子（从 AMQI 仓库迁移）
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockCoppockCurve(Factor):
    """
    股票 Coppock Curve 因子

    公式：Coppock = WMA of (ROC(long) + ROC(short))
         ROC = Rate of Change
         WMA = Weighted Moving Average

    方向：数值上升表示底部信号
    """

    name = "stock_coppock_curve"
    input_type = "bar"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"long_period": [14], "short_period": [11], "wma_period": [10]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"long_period": 14, "short_period": 11, "wma_period": 10}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Coppock Curve 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            Coppock Curve 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        long_period = self.para.get("long_period", 14)
        short_period = self.para.get("short_period", 11)
        wma_period = self.para.get("wma_period", 10)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算 ROC
        roc_long = close.pct_change(periods=long_period)
        roc_short = close.pct_change(periods=short_period)

        # 计算 ROC 之和
        roc_sum = roc_long + roc_short

        # 计算 WMA
        weights = np.arange(1, wma_period + 1)
        weights = weights / weights.sum()

        def wma_func(x):
            if len(x) < wma_period:
                return np.nan
            return np.dot(x, weights)

        coppock = roc_sum.rolling(window=wma_period).apply(wma_func, raw=True)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            coppock = coppock.stack()
            coppock.index.names = ['timestamp', 'code']

        return coppock

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 max(long_period, short_period) + wma_period 条历史数据
        """
        long_period = self.para.get("long_period", 14)
        wma_period = self.para.get("wma_period", 10)
        lookback = long_period + wma_period

        # 取最近 lookback 条历史数据
        recent_history = history.iloc[-lookback:] if len(history) >= lookback else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockCoppockCurve']
