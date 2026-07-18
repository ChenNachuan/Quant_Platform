# -*- coding: utf-8 -*-
"""
股票 Ichimoku 因子（从 AMQI 仓库迁移）
包括：Ichimoku, Ichimoku Cloud Trend, Ichimoku TK Cross
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockIchimoku(Factor):
    """
    股票 Ichimoku 因子

    公式：Tenkan-sen = (最高价_9 + 最低价_9) / 2
         Kijun-sen = (最高价_26 + 最低价_26) / 2
         Senkou Span A = (Tenkan-sen + Kijun-sen) / 2
         Senkou Span B = (最高价_52 + 最低价_52) / 2

    方向：价格在云上为多头，在云下为空头
    """

    name = "stock_ichimoku"
    input_type = "bar"
    max_lookback = 52
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"tenkan": [9], "kijun": [26], "senkou": [52]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"tenkan": 9, "kijun": 26, "senkou": 52}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Ichimoku 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            价格与云的位置关系（正值在云上，负值在云下）
        """
        required_cols = ['high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        tenkan_period = self.para.get("tenkan", 9)
        kijun_period = self.para.get("kijun", 26)
        senkou_period = self.para.get("senkou", 52)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high = data['high']
            low = data['low']
            close = data['close']

        # Tenkan-sen (转换线)
        tenkan = (high.rolling(window=tenkan_period).max() + low.rolling(window=tenkan_period).min()) / 2

        # Kijun-sen (基准线)
        kijun = (high.rolling(window=kijun_period).max() + low.rolling(window=kijun_period).min()) / 2

        # Senkou Span A (先行带A)
        senkou_a = (tenkan + kijun) / 2

        # Senkou Span B (先行带B)
        senkou_b = (high.rolling(window=senkou_period).max() + low.rolling(window=senkou_period).min()) / 2

        # 计算云的上下边界
        cloud_top = pd.concat([senkou_a, senkou_b], axis=1).max(axis=1)
        cloud_bottom = pd.concat([senkou_a, senkou_b], axis=1).min(axis=1)

        # 计算价格相对于云的位置
        # 正值表示在云上，负值表示在云下
        position = (close - cloud_bottom) / (cloud_top - cloud_bottom)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            position = position.stack()
            position.index.names = ['timestamp', 'code']

        return position

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 senkou_period 条历史数据
        """
        senkou_period = self.para.get("senkou", 52)

        # 取最近 senkou_period 条历史数据
        recent_history = history.iloc[-senkou_period:] if len(history) >= senkou_period else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockIchimokuCloudTrend(Factor):
    """
    股票 Ichimoku Cloud Trend 因子

    公式：Senkou Span A - Senkou Span B

    方向：正值表示多头云，负值表示空头云
    """

    name = "stock_ichimoku_cloud_trend"
    input_type = "bar"
    max_lookback = 52
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"tenkan": [9], "kijun": [26], "senkou": [52]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"tenkan": 9, "kijun": 26, "senkou": 52}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Ichimoku Cloud Trend 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            云趋势值
        """
        required_cols = ['high', 'low']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        tenkan_period = self.para.get("tenkan", 9)
        kijun_period = self.para.get("kijun", 26)
        senkou_period = self.para.get("senkou", 52)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
        else:
            high = data['high']
            low = data['low']

        # Tenkan-sen (转换线)
        tenkan = (high.rolling(window=tenkan_period).max() + low.rolling(window=tenkan_period).min()) / 2

        # Kijun-sen (基准线)
        kijun = (high.rolling(window=kijun_period).max() + low.rolling(window=kijun_period).min()) / 2

        # Senkou Span A (先行带A)
        senkou_a = (tenkan + kijun) / 2

        # Senkou Span B (先行带B)
        senkou_b = (high.rolling(window=senkou_period).max() + low.rolling(window=senkou_period).min()) / 2

        # 云趋势 = Senkou Span A - Senkou Span B
        cloud_trend = senkou_a - senkou_b

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            cloud_trend = cloud_trend.stack()
            cloud_trend.index.names = ['timestamp', 'code']

        return cloud_trend

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 senkou_period 条历史数据
        """
        senkou_period = self.para.get("senkou", 52)

        # 取最近 senkou_period 条历史数据
        recent_history = history.iloc[-senkou_period:] if len(history) >= senkou_period else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockIchimokuTKCross(Factor):
    """
    股票 Ichimoku TK Cross 因子

    公式：Tenkan-sen - Kijun-sen

    方向：正值表示金叉，负值表示死叉
    """

    name = "stock_ichimoku_tk_cross"
    input_type = "bar"
    max_lookback = 26
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"tenkan": [9], "kijun": [26]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"tenkan": 9, "kijun": 26}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Ichimoku TK Cross 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            TK 交叉值
        """
        required_cols = ['high', 'low']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        tenkan_period = self.para.get("tenkan", 9)
        kijun_period = self.para.get("kijun", 26)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
        else:
            high = data['high']
            low = data['low']

        # Tenkan-sen (转换线)
        tenkan = (high.rolling(window=tenkan_period).max() + low.rolling(window=tenkan_period).min()) / 2

        # Kijun-sen (基准线)
        kijun = (high.rolling(window=kijun_period).max() + low.rolling(window=kijun_period).min()) / 2

        # TK 交叉 = Tenkan - Kijun
        tk_cross = tenkan - kijun

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            tk_cross = tk_cross.stack()
            tk_cross.index.names = ['timestamp', 'code']

        return tk_cross

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 kijun_period 条历史数据
        """
        kijun_period = self.para.get("kijun", 26)

        # 取最近 kijun_period 条历史数据
        recent_history = history.iloc[-kijun_period:] if len(history) >= kijun_period else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockIchimoku', 'StockIchimokuCloudTrend', 'StockIchimokuTKCross']
