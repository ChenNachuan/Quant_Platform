# -*- coding: utf-8 -*-
"""
股票换手率因子（从 AMQI 仓库迁移）
包括：Turnover, Turnover Volatility
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockTurnover(Factor):
    """
    股票换手率因子

    公式：volume / 流通股本

    方向：数值越大，交易越活跃
    """

    name = "stock_turnover"
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
        计算换手率因子

        Args:
            data: OHLCV数据，需要包含 'volume' 和 'float_share' 列
            deps: 无依赖

        Returns:
            换手率值
        """
        required_cols = ['volume']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            volume = data['volume'].unstack(level='code')
        else:
            volume = data['volume']

        # 如果有流通股本，计算真实换手率
        if 'float_share' in data.columns:
            if isinstance(data.index, pd.MultiIndex):
                float_share = data['float_share'].unstack(level='code')
            else:
                float_share = data['float_share']
            turnover = volume / float_share
        else:
            # 如果没有流通股本，使用成交量的滚动均值作为代理
            turnover = volume.rolling(window=window).mean()

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            turnover = turnover.stack()
            turnover.index.names = ['timestamp', 'code']

        return turnover

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
class StockTurnoverVolatility(Factor):
    """
    股票换手率波动率因子

    公式：std(turnover, window)

    方向：数值越大，换手率波动越剧烈
    """

    name = "stock_turnover_volatility"
    input_type = "bar"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20, 60]},
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
        计算换手率波动率因子

        Args:
            data: OHLCV数据，需要包含 'volume' 列
            deps: 无依赖

        Returns:
            换手率波动率值
        """
        required_cols = ['volume']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            volume = data['volume'].unstack(level='code')
        else:
            volume = data['volume']

        # 计算换手率（使用成交量的滚动均值作为代理）
        turnover = volume.rolling(window=window).mean()

        # 计算换手率波动率
        turnover_vol = turnover.rolling(window=window).std()

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            turnover_vol = turnover_vol.stack()
            turnover_vol.index.names = ['timestamp', 'code']

        return turnover_vol

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


__all__ = ['StockTurnover', 'StockTurnoverVolatility']
