# -*- coding: utf-8 -*-
"""
股票 PVT (Price Volume Trend) 因子（从 AMQI 仓库迁移）
包括：PVT, PVT Divergence
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockPVT(Factor):
    """
    股票 PVT (Price Volume Trend) 因子

    公式：PVT = 前一日PVT + 成交量 * (今日收盘价 - 前一日收盘价) / 前一日收盘价

    方向：数值上升表示买盘压力，下降表示卖盘压力
    """

    name = "stock_pvt"
    input_type = "bar"
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
        计算 PVT 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            PVT 值
        """
        required_cols = ['close', 'volume']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
            volume = data['volume'].unstack(level='code')
        else:
            close = data['close']
            volume = data['volume']

        # 计算价格变化率
        price_change_pct = close.pct_change()

        # 计算 PVT
        pvt = pd.Series(0, index=close.index, dtype=float)
        for i in range(1, len(close)):
            pvt.iloc[i] = pvt.iloc[i-1] + volume.iloc[i] * price_change_pct.iloc[i]

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            pvt = pvt.stack()
            pvt.index.names = ['timestamp', 'code']

        return pvt

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要最近的历史数据来继续计算 PVT
        """
        # 取最近 1 条历史数据
        recent_history = history.iloc[-1:] if len(history) > 0 else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockPVTDivergence(Factor):
    """
    股票 PVT Divergence 因子

    公式：PVT 与价格的背离

    方向：正值表示正背离（PVT 上升，价格下降），负值表示负背离
    """

    name = "stock_pvt_divergence"
    input_type = "bar"
    max_lookback = 260
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20]},
    }

    dependencies = ["stock_pvt"]

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 PVT Divergence 因子

        Args:
            data: OHLCV数据，需要包含 'close' 列
            deps: 依赖因子的计算结果
                - deps['stock_pvt']: PVT 因子值

        Returns:
            PVT Divergence 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        # 防御性检查
        if deps is None or 'stock_pvt' not in deps:
            raise ValueError("缺少依赖因子: stock_pvt")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
            pvt = deps['stock_pvt'].unstack(level='code')
        else:
            close = data['close']
            pvt = deps['stock_pvt']

        # 计算价格和 PVT 的滚动相关性
        # 背离 = 价格变化方向与 PVT 变化方向的不一致
        price_change = close.pct_change(window)
        pvt_change = pvt.pct_change(window)

        # 背离 = PVT 变化 - 价格变化
        divergence = pvt_change - price_change

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            divergence = divergence.stack()
            divergence.index.names = ['timestamp', 'code']

        return divergence

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


__all__ = ['StockPVT', 'StockPVTDivergence']
