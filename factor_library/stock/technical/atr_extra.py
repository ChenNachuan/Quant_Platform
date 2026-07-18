# -*- coding: utf-8 -*-
"""
股票 ATR 扩展因子（从 AMQI 仓库迁移）
包括：ATR Price Breakout, ATR Price Position, ATR Trend, ATR Volume Confirmation
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockATRPriceBreakout(Factor):
    """
    股票 ATR Price Breakout 因子

    公式：(Close - MA) / ATR

    方向：数值越大，价格突破越明显
    """

    name = "stock_atr_price_breakout"
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
        """计算 ATR Price Breakout 因子"""
        required_cols = ['high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        period = self.para.get("period", 14)

        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high, low, close = data['high'], data['low'], data['close']

        prev_close = close.shift(1)
        tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()
        ma = close.rolling(window=period).mean()

        result = (close - ma) / atr

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        period = self.para.get("period", 14)
        recent = history.iloc[-period*2:] if len(history) >= period*2 else history
        combined = pd.concat([recent, new_data])
        return self.compute(combined, deps).iloc[-len(new_data):]


@register_factor
class StockATRPricePosition(Factor):
    """
    股票 ATR Price Position 因子

    公式：(Close - Low) / (High - Low) 归一化后除以 ATR

    方向：数值越大，价格在区间中的位置越高
    """

    name = "stock_atr_price_position"
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
        """计算 ATR Price Position 因子"""
        required_cols = ['high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        period = self.para.get("period", 14)

        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high, low, close = data['high'], data['low'], data['close']

        hl_range = high - low
        hl_range = hl_range.replace(0, np.nan)
        position = (close - low) / hl_range

        result = position.rolling(window=period).mean()

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        period = self.para.get("period", 14)
        recent = history.iloc[-period:] if len(history) >= period else history
        combined = pd.concat([recent, new_data])
        return self.compute(combined, deps).iloc[-len(new_data):]


@register_factor
class StockATRTrend(Factor):
    """
    股票 ATR Trend 因子

    公式：ATR 的变化趋势

    方向：数值上升表示波动趋势向上
    """

    name = "stock_atr_trend"
    input_type = "bar"
    max_lookback = 28
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"period": [14], "trend_period": [10]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"period": 14, "trend_period": 10}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """计算 ATR Trend 因子"""
        required_cols = ['high', 'low', 'close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        period = self.para.get("period", 14)
        trend_period = self.para.get("trend_period", 10)

        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
        else:
            high, low, close = data['high'], data['low'], data['close']

        prev_close = close.shift(1)
        tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        result = atr.pct_change(periods=trend_period)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        lookback = self.para.get("period", 14) + self.para.get("trend_period", 10)
        recent = history.iloc[-lookback:] if len(history) >= lookback else history
        combined = pd.concat([recent, new_data])
        return self.compute(combined, deps).iloc[-len(new_data):]


@register_factor
class StockATRVolumeConfirmation(Factor):
    """
    股票 ATR Volume Confirmation 因子

    公式：ATR 与成交量的相关性

    方向：数值越大，波动与成交量的正相关性越强
    """

    name = "stock_atr_volume_confirmation"
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
        """计算 ATR Volume Confirmation 因子"""
        required_cols = ['high', 'low', 'close', 'volume']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        period = self.para.get("period", 14)

        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
            volume = data['volume'].unstack(level='code')
        else:
            high, low, close, volume = data['high'], data['low'], data['close'], data['volume']

        prev_close = close.shift(1)
        tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(window=period).mean()

        # 计算 ATR 与成交量的滚动相关性
        result = atr.rolling(window=period).corr(volume)

        if isinstance(data.index, pd.MultiIndex):
            result = result.stack()
            result.index.names = ['timestamp', 'code']
        return result

    def update(self, new_data, history, deps=None):
        period = self.para.get("period", 14)
        recent = history.iloc[-period:] if len(history) >= period else history
        combined = pd.concat([recent, new_data])
        return self.compute(combined, deps).iloc[-len(new_data):]


__all__ = ['StockATRPriceBreakout', 'StockATRPricePosition', 'StockATRTrend', 'StockATRVolumeConfirmation']
