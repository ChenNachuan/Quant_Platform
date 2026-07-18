# -*- coding: utf-8 -*-
"""
股票 MFI (Money Flow Index) 因子（从 AMQI 仓库迁移）
包括：MFI, MFI Change Rate
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockMFI(Factor):
    """
    股票 MFI (Money Flow Index) 因子

    公式：MFI = 100 - (100 / (1 + Money Flow Ratio))
         Money Flow Ratio = Positive Money Flow / Negative Money Flow
         Money Flow = Typical Price * Volume
         Typical Price = (High + Low + Close) / 3

    方向：数值越大，买盘压力越强
    """

    name = "stock_mfi"
    input_type = "bar"
    max_lookback = 14
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [14]},
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
        计算 MFI 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            MFI 值 (0-100)
        """
        required_cols = ['high', 'low', 'close', 'volume']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 14)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            high = data['high'].unstack(level='code')
            low = data['low'].unstack(level='code')
            close = data['close'].unstack(level='code')
            volume = data['volume'].unstack(level='code')
        else:
            high = data['high']
            low = data['low']
            close = data['close']
            volume = data['volume']

        # 计算典型价格
        typical_price = (high + low + close) / 3

        # 计算资金流
        money_flow = typical_price * volume

        # 计算正向和负向资金流
        price_change = typical_price.diff()
        positive_flow = pd.Series(0, index=money_flow.index, dtype=float)
        negative_flow = pd.Series(0, index=money_flow.index, dtype=float)

        positive_flow[price_change > 0] = money_flow[price_change > 0]
        negative_flow[price_change < 0] = money_flow[price_change < 0]

        # 计算滚动资金流比率
        positive_sum = positive_flow.rolling(window=window).sum()
        negative_sum = negative_flow.rolling(window=window).sum()

        # 计算 MFI
        money_flow_ratio = positive_sum / negative_sum
        mfi = 100 - (100 / (1 + money_flow_ratio))

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            mfi = mfi.stack()
            mfi.index.names = ['timestamp', 'code']

        return mfi

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
class StockMFIChangeRate(Factor):
    """
    股票 MFI Change Rate 因子

    公式：MFI 的变化率

    方向：数值越大，MFI 上升越快
    """

    name = "stock_mfi_change_rate"
    input_type = "bar"
    max_lookback = 15
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [14]},
    }

    dependencies = ["stock_mfi"]

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 MFI Change Rate 因子

        Args:
            data: OHLCV数据（此因子不直接使用原始数据）
            deps: 依赖因子的计算结果
                - deps['stock_mfi']: MFI 因子值

        Returns:
            MFI Change Rate 值
        """
        # 防御性检查
        if deps is None or 'stock_mfi' not in deps:
            raise ValueError("缺少依赖因子: stock_mfi")

        mfi = deps['stock_mfi']

        # 计算 MFI 变化率
        mfi_change = mfi.diff()

        return mfi_change

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 2 条历史数据
        """
        # 取最近 2 条历史数据
        recent_history = history.iloc[-2:] if len(history) >= 2 else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockMFI', 'StockMFIChangeRate']
