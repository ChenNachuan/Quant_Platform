# -*- coding: utf-8 -*-
"""
股票盈利能力因子（从 AMQI 仓库迁移）
包括：OCF/NI, ROE MoM NA Growth
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockOCFtoNI(Factor):
    """
    股票经营现金流/净利润因子

    公式：n_cashflow_act / n_income (TTM)

    方向：数值越大，盈利质量越高
    """

    name = "stock_ocf_to_ni"
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
        计算 OCF/NI 因子

        Args:
            data: 基本面数据，需要包含 'n_cashflow_act' 和 'n_income' 列
            deps: 无依赖

        Returns:
            OCF/NI 值
        """
        required_cols = ['n_cashflow_act', 'n_income']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 计算 OCF/NI
        factor_value = data['n_cashflow_act'] / data['n_income']

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
class StockROEMomNAGrowth(Factor):
    """
    股票 ROE MoM 减去净资产 YoY 增长因子

    公式：ROE MoM = (ROE(TTM) - ROE(TTM)_t-1) / abs(ROE(TTM)_t-1)
         Net Asset YoY Growth = (BV_t - BV_t-4) / BV_t-4
         Factor = ROE MoM - Net Asset YoY Growth

    方向：数值越大，ROE 增长越快于净资产增长
    """

    name = "stock_roe_mom_na_growth"
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
        计算 ROE MoM NA Growth 因子

        Args:
            data: 基本面数据，需要包含 'roe_ttm' 和 'total_hldr_eqy_exc_min_int' 列
            deps: 无依赖

        Returns:
            ROE MoM NA Growth 值
        """
        required_cols = ['roe_ttm', 'total_hldr_eqy_exc_min_int']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # ROE MoM: ROE (TTM) 的环比变化
        roe_ttm = data['roe_ttm']
        roe_mom = (roe_ttm - roe_ttm.shift(1)) / abs(roe_ttm.shift(1))

        # Net Asset YoY Growth: 净资产的同比增长
        bv = data['total_hldr_eqy_exc_min_int']
        net_asset_yoy_growth = (bv - bv.shift(4)) / bv.shift(4)

        # ROE MoM 减去 Net Asset YoY Growth
        factor_value = roe_mom - net_asset_yoy_growth
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


__all__ = ['StockOCFtoNI', 'StockROEMomNAGrowth']
