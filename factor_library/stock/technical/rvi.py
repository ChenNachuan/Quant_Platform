# -*- coding: utf-8 -*-
"""
股票 RVI (Relative Vigor Index) 因子（从 AMQI 仓库迁移）
包括：RVI, RVI Strength
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockRVI(Factor):
    """
    股票 RVI (Relative Vigor Index) 因子

    公式：RVI = (Close - Open) / (High - Low) 的移动平均

    方向：数值越大，多头力量越强
    """

    name = "stock_rvi"
    input_type = "bar"
    max_lookback = 10
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [10]},
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
        计算 RVI 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            RVI 值
        """
        required_cols = ['open', 'high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 10)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            open_price = data['open'].unstack(level='code')
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            open_price = data['open']
            high = data['high']
            low = data['low']
            close = data['close']

        # 计算 RVI
        numerator = close - open_price
        denominator = high - low

        # 避免除零
        denominator = denominator.replace(0, np.nan)

        rvi_raw = numerator / denominator

        # 计算移动平均
        rvi = rvi_raw.rolling(window=window).mean()

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            rvi = rvi.stack()
            rvi.index.names = ['timestamp', 'code']

        return rvi

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
        window = self.para.get("window", 10)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockRVIStrength(Factor):
    """
    股票 RVI Strength 因子

    公式：RVI 的标准化值

    方向：数值越大，RVI 越强
    """

    name = "stock_rvi_strength"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [10], "strength_window": [20]},
    }

    dependencies = ["stock_rvi"]

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": 10, "strength_window": 20}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 RVI Strength 因子

        Args:
            data: OHLCV数据（此因子不直接使用原始数据）
            deps: 依赖因子的计算结果
                - deps['stock_rvi']: RVI 因子值

        Returns:
            RVI Strength 值
        """
        # 防御性检查
        if deps is None or 'stock_rvi' not in deps:
            raise ValueError("缺少依赖因子: stock_rvi")

        strength_window = self.para.get("strength_window", 20)
        rvi = deps['stock_rvi']

        # 按股票分组计算
        if isinstance(rvi.index, pd.MultiIndex):
            rvi_unstacked = rvi.unstack(level='code')
        else:
            rvi_unstacked = rvi

        # 计算 RVI 的滚动标准化
        rvi_mean = rvi_unstacked.rolling(window=strength_window).mean()
        rvi_std = rvi_unstacked.rolling(window=strength_window).std()

        # 避免除零
        rvi_std = rvi_std.replace(0, np.nan)

        rvi_strength = (rvi_unstacked - rvi_mean) / rvi_std

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            rvi_strength = rvi_strength.stack()
            rvi_strength.index.names = ['timestamp', 'code']

        return rvi_strength

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 strength_window 条历史数据
        """
        strength_window = self.para.get("strength_window", 20)

        # 取最近 strength_window 条历史数据
        recent_history = history.iloc[-strength_window:] if len(history) >= strength_window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockRVI', 'StockRVIStrength']
