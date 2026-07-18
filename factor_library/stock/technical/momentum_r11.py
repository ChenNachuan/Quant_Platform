# -*- coding: utf-8 -*-
"""
股票动量因子 R11 (12-1 个月动量)
从 AMQI 仓库迁移
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
from typing import List, Dict, Optional


@register_factor
class StockMomentumR11(Factor):
    """
    股票动量因子 R11 (12-1 个月动量)

    公式：P_{t-21} / P_{t-252} - 1
    即过去12个月收益，跳过最近1个月（约21个交易日）

    方向：数值越大，动量越强
    """

    name = "stock_momentum_r11"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"lag_1m": [21], "lag_12m": [252]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"lag_1m": 21, "lag_12m": 252}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 R11 动量因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            R11 动量值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        lag_1m = self.para.get("lag_1m", 21)
        lag_12m = self.para.get("lag_12m", 252)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            # MultiIndex [timestamp, code]
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # P_{t-lag_1m} / P_{t-lag_12m} - 1
        p_t_minus_1m = close.shift(lag_1m)
        p_t_minus_12m = close.shift(lag_12m)

        r11 = (p_t_minus_1m / p_t_minus_12m) - 1

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            r11 = r11.stack()
            r11.index.names = ['timestamp', 'code']

        return r11

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 lag_12m 天的历史数据
        """
        lag_12m = self.para.get("lag_12m", 252)

        # 取最近 lag_12m 条历史数据
        recent_history = history.iloc[-lag_12m:] if len(history) >= lag_12m else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockMomentumR11']
