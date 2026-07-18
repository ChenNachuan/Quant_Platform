# -*- coding: utf-8 -*-
"""
股票反转因子（从 AMQI 仓库迁移）
包括：Short Term Reversal, Monthly Excess Reversal
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockShortTermReversal(Factor):
    """
    股票短期反转因子

    公式：过去 1 个月的收益率

    方向：数值越大，短期反转效应越强
    """

    name = "stock_short_term_reversal"
    input_type = "bar"
    max_lookback = 21
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [21]},
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
        计算短期反转因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            短期反转值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 21)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算过去 window 天的收益率
        reversal = close.pct_change(periods=window)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            reversal = reversal.stack()
            reversal.index.names = ['timestamp', 'code']

        return reversal

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
        window = self.para.get("window", 21)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockMonthlyExcessReversal(Factor):
    """
    股票月度超额反转因子

    公式：月度收益率 - 市场平均月度收益率

    方向：数值越大，超额反转效应越强
    """

    name = "stock_monthly_excess_reversal"
    input_type = "bar"
    max_lookback = 21
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [21]},
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
        计算月度超额反转因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            月度超额反转值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 21)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算月度收益率
        monthly_return = close.pct_change(periods=window)

        # 计算市场平均月度收益率
        market_return = monthly_return.mean(axis=1)

        # 计算超额收益率
        excess_return = monthly_return.sub(market_return, axis=0)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            excess_return = excess_return.stack()
            excess_return.index.names = ['timestamp', 'code']

        return excess_return

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
        window = self.para.get("window", 21)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockShortTermReversal', 'StockMonthlyExcessReversal']
