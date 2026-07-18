# -*- coding: utf-8 -*-
"""
股票 OBV (On Balance Volume) 因子
从 AMQI 仓库迁移
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockOBV(Factor):
    """
    股票 OBV (On Balance Volume) 因子

    公式：OBV = 前一日OBV + 当日成交量（如果当日收盘价>前一日收盘价）
                                - 当日成交量（如果当日收盘价<前一日收盘价）

    方向：数值上升表示买盘压力，下降表示卖盘压力
    """

    name = "stock_obv"
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
        计算 OBV 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            OBV 值
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

        # 计算价格变化方向
        price_change = close.diff()

        # 计算 OBV
        obv = pd.Series(0, index=close.index, dtype=float)
        for i in range(1, len(close)):
            if price_change.iloc[i] > 0:
                obv.iloc[i] = obv.iloc[i-1] + volume.iloc[i]
            elif price_change.iloc[i] < 0:
                obv.iloc[i] = obv.iloc[i-1] - volume.iloc[i]
            else:
                obv.iloc[i] = obv.iloc[i-1]

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            obv = obv.stack()
            obv.index.names = ['timestamp', 'code']

        return obv

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要最近的历史数据来继续计算 OBV
        """
        # 取最近 1 条历史数据
        recent_history = history.iloc[-1:] if len(history) > 0 else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockOBVSlope(Factor):
    """
    股票 OBV Slope 因子

    公式：OBV 的线性回归斜率

    方向：数值越大，OBV 上升趋势越强
    """

    name = "stock_obv_slope"
    input_type = "bar"
    max_lookback = 260
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20]},
    }

    dependencies = ["stock_obv"]

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 OBV Slope 因子

        Args:
            data: OHLCV数据（此因子不直接使用原始数据）
            deps: 依赖因子的计算结果
                - deps['stock_obv']: OBV 因子值

        Returns:
            OBV Slope 值
        """
        # 防御性检查
        if deps is None or 'stock_obv' not in deps:
            raise ValueError("缺少依赖因子: stock_obv")

        window = self.para.get("window", 20)
        obv = deps['stock_obv']

        # 按股票分组计算线性回归斜率
        if isinstance(obv.index, pd.MultiIndex):
            obv_unstacked = obv.unstack(level='code')
        else:
            obv_unstacked = obv

        # 计算线性回归斜率
        def rolling_slope(series, window):
            """计算滚动线性回归斜率"""
            slopes = pd.Series(index=series.index, dtype=float)
            for i in range(window-1, len(series)):
                y = series.iloc[i-window+1:i+1].values
                x = np.arange(window)
                if np.isnan(y).any():
                    slopes.iloc[i] = np.nan
                else:
                    slope = np.polyfit(x, y, 1)[0]
                    slopes.iloc[i] = slope
            return slopes

        slope = obv_unstacked.rolling(window=window).apply(
            lambda x: np.polyfit(np.arange(len(x)), x, 1)[0] if not np.isnan(x).any() else np.nan,
            raw=False
        )

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            slope = slope.stack()
            slope.index.names = ['timestamp', 'code']

        return slope

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


__all__ = ['StockOBV', 'StockOBVSlope']
