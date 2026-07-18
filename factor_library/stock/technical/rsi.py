# -*- coding: utf-8 -*-
"""
股票 RSI (Relative Strength Index) 因子（从 AMQI 仓库迁移）
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockRSI(Factor):
    """
    股票 RSI (Relative Strength Index) 因子

    公式：RSI = 100 - (100 / (1 + RS))
         RS = 平均上涨幅度 / 平均下跌幅度

    方向：数值越大，超买；数值越小，超卖
    """

    name = "stock_rsi"
    input_type = "bar"
    max_lookback = 14
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [14]},
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
        计算 RSI 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            RSI 值 (0-100)
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 14)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算价格变化
        delta = close.diff()

        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = (-delta).where(delta < 0, 0)

        # 计算平均上涨和下跌
        avg_gain = gain.rolling(window=window).mean()
        avg_loss = loss.rolling(window=window).mean()

        # 计算 RS 和 RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            rsi = rsi.stack()
            rsi.index.names = ['timestamp', 'code']

        return rsi

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
        window = self.para.get("window", 14)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockRSI']
