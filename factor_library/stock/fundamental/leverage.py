# -*- coding: utf-8 -*-
"""
股票资本结构因子（从 AMQI 仓库迁移）
包括：Equity Ratio, Debt Growth Rate
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockEquityRatio(Factor):
    """
    股票权益比率因子

    公式：股东权益 / 总资产

    方向：数值越大，财务杠杆越低
    """

    name = "stock_equity_ratio"
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
        计算权益比率因子

        Args:
            data: 基本面数据，需要包含 'total_hldr_eqy_exc_min_int' 和 'total_assets' 列
            deps: 无依赖

        Returns:
            权益比率值
        """
        required_cols = ['total_hldr_eqy_exc_min_int', 'total_assets']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算权益比率
        factor_value = data['total_hldr_eqy_exc_min_int'] / data['total_assets']

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
class StockDebtGrowthRate(Factor):
    """
    股票债务增长率因子

    公式：(本期债务 - 上期债务) / 上期债务

    方向：数值越大，债务增长越快
    """

    name = "stock_debt_growth_rate"
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
        计算债务增长率因子

        Args:
            data: 基本面数据，需要包含 'total_liab' 列
            deps: 无依赖

        Returns:
            债务增长率值
        """
        required_cols = ['total_liab']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算债务增长率
        factor_value = data['total_liab'].pct_change()

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


__all__ = ['StockEquityRatio', 'StockDebtGrowthRate']
