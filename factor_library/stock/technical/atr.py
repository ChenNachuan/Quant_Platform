# -*- coding: utf-8 -*-
"""
股票 ATR (Average True Range) 因子
从 AMQI 仓库迁移
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockATR(Factor):
    """
    股票 ATR (Average True Range) 因子

    公式：TR = max(High-Low, |High-Close_prev|, |Low-Close_prev|)
         ATR = Rolling mean of TR

    方向：数值越大，波动越剧烈
    """

    name = "stock_atr"
    input_type = "bar"
    max_lookback = 14
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [14, 20]},
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
        计算 ATR 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            ATR 值
        """
        required_cols = ['high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 14)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high = data['high']
            low = data['low']
            close = data['close']

        # 计算 True Range
        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 计算 ATR (滚动均值)
        atr = tr.rolling(window=window).mean()

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            atr = atr.stack()
            atr.index.names = ['timestamp', 'code']

        return atr

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
        window = self.para.get("window", 14)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockATRExpansion(Factor):
    """
    股票 ATR Expansion 因子

    公式：ATR_norm = ATR / MA(close, period)
         ATR_ma = MA(ATR_norm, period)
         ATR_expansion = (ATR_norm / ATR_ma - 1) × 100

    方向：数值越大，波动扩张越明显
    """

    name = "stock_atr_expansion"
    input_type = "bar"
    max_lookback = 28
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"period": [14]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"period": p} for p in self.para_group[self.timeframe]["period"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 ATR Expansion 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            ATR Expansion 值
        """
        required_cols = ['high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        period = self.para.get("period", 14)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high = data['high']
            low = data['low']
            close = data['close']

        # 计算 True Range
        prev_close = close.shift(1)

        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()

        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # 计算 ATR
        atr = tr.rolling(window=period).mean()

        # 计算价格均值用于归一化
        price_mean = close.rolling(window=period).mean()

        # 归一化 ATR
        atr_norm = atr / price_mean

        # ATR 移动平均
        atr_ma = atr_norm.rolling(window=period).mean()

        # ATR Expansion 比率（百分比）
        atr_expansion = (atr_norm / atr_ma - 1) * 100

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            atr_expansion = atr_expansion.stack()
            atr_expansion.index.names = ['timestamp', 'code']

        return atr_expansion

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 2 * period 条历史数据
        """
        period = self.para.get("period", 14)

        # 取最近 2*period 条历史数据
        lookback = 2 * period
        recent_history = history.iloc[-lookback:] if len(history) >= lookback else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockATR', 'StockATRExpansion']
