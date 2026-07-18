# -*- coding: utf-8 -*-
"""
股票 SWMA 和 TEMA 因子（从 AMQI 仓库迁移）
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockSWMA(Factor):
    """
    股票 SWMA (Sine Weighted Moving Average) 因子

    公式：使用正弦函数作为权重的移动平均

    方向：数值上升表示趋势向上
    """

    name = "stock_swma"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20]},
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
        计算 SWMA 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            SWMA 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 创建正弦权重
        weights = np.sin(np.arange(window) * np.pi / window)
        weights = weights / weights.sum()

        # 计算 SWMA
        def swma_func(x):
            if len(x) < window:
                return np.nan
            return np.dot(x, weights)

        swma = close.rolling(window=window).apply(swma_func, raw=True)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            swma = swma.stack()
            swma.index.names = ['timestamp', 'code']

        return swma

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
        window = self.para.get("window", 20)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockTEMA(Factor):
    """
    股票 TEMA (Triple Exponential Moving Average) 因子

    公式：TEMA = 3*EMA - 3*EMA(EMA) + EMA(EMA(EMA))

    方向：数值上升表示趋势向上
    """

    name = "stock_tema"
    input_type = "bar"
    max_lookback = 30
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [10, 20]},
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
        计算 TEMA 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            TEMA 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算 TEMA
        ema1 = close.ewm(span=window, adjust=False).mean()
        ema2 = ema1.ewm(span=window, adjust=False).mean()
        ema3 = ema2.ewm(span=window, adjust=False).mean()

        tema = 3 * ema1 - 3 * ema2 + ema3

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            tema = tema.stack()
            tema.index.names = ['timestamp', 'code']

        return tema

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
        window = self.para.get("window", 20)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockSWMA', 'StockTEMA']
