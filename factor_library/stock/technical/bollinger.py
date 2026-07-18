# -*- coding: utf-8 -*-
"""
股票 Bollinger Bands 因子
从 AMQI 仓库迁移
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockBollinger(Factor):
    """
    股票 Bollinger Bands 因子

    公式：Percent B = (Close - LowerBand) / (UpperBand - LowerBand)
         其中 UpperBand = MA + 2*STD, LowerBand = MA - 2*STD

    方向：数值接近1表示价格接近上轨，接近0表示接近下轨
    """

    name = "stock_bollinger"
    input_type = "bar"
    max_lookback = 20
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20], "num_std": [2]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": 20, "num_std": 2}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Bollinger Bands Percent B

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            Percent B 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)
        num_std = self.para.get("num_std", 2)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 滚动均值和标准差
        ma = close.rolling(window=window).mean()
        std = close.rolling(window=window).std()

        upper = ma + num_std * std
        lower = ma - num_std * std

        # Percent B
        denominator = upper - lower
        percent_b = (close - lower) / denominator

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            percent_b = percent_b.stack()
            percent_b.index.names = ['timestamp', 'code']

        return percent_b

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
class StockBollingerSqueezeExpansion(Factor):
    """
    股票 Bollinger Squeeze Expansion 因子

    公式：Bandwidth = (Upper - Lower) / MA
         Squeeze = Bandwidth / MA(Bandwidth, window)

    方向：数值越小表示波动收缩（squeeze），越大表示波动扩张
    """

    name = "stock_bollinger_squeeze"
    input_type = "bar"
    max_lookback = 40
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20], "squeeze_window": [120]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": 20, "squeeze_window": 120}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Bollinger Squeeze Expansion

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            Squeeze 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)
        squeeze_window = self.para.get("squeeze_window", 120)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 滚动均值和标准差
        ma = close.rolling(window=window).mean()
        std = close.rolling(window=window).std()

        upper = ma + 2 * std
        lower = ma - 2 * std

        # Bandwidth
        bandwidth = (upper - lower) / ma

        # Squeeze 比率
        bandwidth_ma = bandwidth.rolling(window=squeeze_window).mean()
        squeeze = bandwidth / bandwidth_ma

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            squeeze = squeeze.stack()
            squeeze.index.names = ['timestamp', 'code']

        return squeeze

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 squeeze_window 条历史数据
        """
        squeeze_window = self.para.get("squeeze_window", 120)

        # 取最近 squeeze_window 条历史数据
        recent_history = history.iloc[-squeeze_window:] if len(history) >= squeeze_window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockBollinger', 'StockBollingerSqueezeExpansion']
