# -*- coding: utf-8 -*-
"""
股票增长因子（从 AMQI 仓库迁移）
包括：Capex Growth Rate, Earnings Volatility
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockCapexGrowthRate(Factor):
    """
    股本支出增长率因子

    公式：(本期资本支出 - 上期资本支出) / 上期资本支出

    方向：数值越大，资本支出增长越快
    """

    name = "stock_capex_growth_rate"
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
        计算资本支出增长率因子

        Args:
            data: 基本面数据，需要包含 'capex' 列
            deps: 无依赖

        Returns:
            资本支出增长率值
        """
        required_cols = ['capex']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算资本支出增长率
        factor_value = data['capex'].pct_change()

        # 处理除零和无穷大
        factor_value = factor_value.replace([np.inf, -np.inf], np.nan)

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
class StockEarningsVolatility(Factor):
    """
    股票盈利波动率因子

    公式：std(net_income, window)

    方向：数值越大，盈利波动越剧烈
    """

    name = "stock_earnings_volatility"
    input_type = "fundamental"
    max_lookback = 250
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [4]},
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
        计算盈利波动率因子

        Args:
            data: 基本面数据，需要包含 'net_income' 列
            deps: 无依赖

        Returns:
            盈利波动率值
        """
        required_cols = ['net_income']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 4)

        # 计算盈利波动率
        factor_value = data['net_income'].rolling(window=window).std()

        return factor_value

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
        window = self.para.get("window", 4)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockCapexGrowthRate', 'StockEarningsVolatility']
