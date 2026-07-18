# -*- coding: utf-8 -*-
"""
股票现金流因子（从 AMQI 仓库迁移）
包括：Op Cash Ratio
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockOpCashRatio(Factor):
    """
    股票经营现金流比率因子

    公式：经营活动现金流 / 营业收入

    方向：数值越大，现金流质量越高
    """

    name = "stock_op_cash_ratio"
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
        计算经营现金流比率因子

        Args:
            data: 基本面数据，需要包含 'n_cashflow_act' 和 'revenue' 列
            deps: 无依赖

        Returns:
            经营现金流比率值
        """
        required_cols = ['n_cashflow_act', 'revenue']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算经营现金流比率
        factor_value = data['n_cashflow_act'] / data['revenue']

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


__all__ = ['StockOpCashRatio']
