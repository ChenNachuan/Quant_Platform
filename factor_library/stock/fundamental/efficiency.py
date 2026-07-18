# -*- coding: utf-8 -*-
"""
股票运营效率因子（从 AMQI 仓库迁移）
包括：AP Turnover, FA Turnover, Total Asset Turnover
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockAPTurnover(Factor):
    """
    股票应付账款周转率因子

    公式：营业成本 / 平均应付账款

    方向：数值越大，应付账款周转越快
    """

    name = "stock_ap_turnover"
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
        计算应付账款周转率因子

        Args:
            data: 基本面数据，需要包含 'oper_cost' 和 'accounts_payable' 列
            deps: 无依赖

        Returns:
            应付账款周转率值
        """
        required_cols = ['oper_cost', 'accounts_payable']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算应付账款周转率
        factor_value = data['oper_cost'] / data['accounts_payable']

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
class StockFATurnover(Factor):
    """
    股票固定资产周转率因子

    公式：营业收入 / 平均固定资产

    方向：数值越大，固定资产利用效率越高
    """

    name = "stock_fa_turnover"
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
        计算固定资产周转率因子

        Args:
            data: 基本面数据，需要包含 'revenue' 和 'fix_assets' 列
            deps: 无依赖

        Returns:
            固定资产周转率值
        """
        required_cols = ['revenue', 'fix_assets']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算固定资产周转率
        factor_value = data['revenue'] / data['fix_assets']

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
class StockTotalAssetTurnover(Factor):
    """
    股票总资产周转率因子

    公式：营业收入 / 平均总资产

    方向：数值越大，总资产利用效率越高
    """

    name = "stock_total_asset_turnover"
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
        计算总资产周转率因子

        Args:
            data: 基本面数据，需要包含 'revenue' 和 'total_assets' 列
            deps: 无依赖

        Returns:
            总资产周转率值
        """
        required_cols = ['revenue', 'total_assets']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算总资产周转率
        factor_value = data['revenue'] / data['total_assets']

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


__all__ = ['StockAPTurnover', 'StockFATurnover', 'StockTotalAssetTurnover']
