# -*- coding: utf-8 -*-
"""
股票流动性/规模因子（从 AMQI 仓库迁移）
包括：Log Market Cap, Amihud Illiquidity
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockLogMarketCap(Factor):
    """
    对数市值因子

    公式：log(总市值)

    方向：数值越大，市值越大
    """

    name = "stock_log_market_cap"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算对数市值因子

        Args:
            data: 基本面数据，需要包含 'total_mv' 列
            deps: 无依赖

        Returns:
            对数市值值
        """
        required_cols = ['total_mv']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算对数市值
        factor_value = np.log(data['total_mv'])

        return factor_value

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        基本面因子通常不需要增量更新，直接使用新数据
        """
        return self.compute(new_data, deps)


@register_factor
class StockAmihudIlliquidity(Factor):
    """
    Amihud 非流动性因子

    公式：|return| / volume

    方向：数值越大，流动性越差
    """

    name = "stock_amihud_illiquidity"
    input_type = "bar"
    max_lookback = 250
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
        计算 Amihud 非流动性因子

        Args:
            data: OHLCV数据，需要包含 'close' 和 'volume' 列
            deps: 无依赖

        Returns:
            Amihud 非流动性值
        """
        required_cols = ['close', 'volume']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
            volume = data['volume'].unstack(level='code')
        else:
            close = data['close']
            volume = data['volume']

        # 计算日收益率
        returns = close.pct_change()

        # 计算 Amihud 非流动性
        illiq = (returns.abs() / volume).rolling(window=window).mean()

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            illiq = illiq.stack()
            illiq.index.names = ['timestamp', 'code']

        return illiq

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


__all__ = ['StockLogMarketCap', 'StockAmihudIlliquidity']
