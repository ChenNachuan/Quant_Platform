# -*- coding: utf-8 -*-
"""
期货 L2 Tick 因子基类

提供 DataLake 列名适配和逐合约分组计算能力。
"""

from abc import abstractmethod
from typing import Dict, List, Optional

import pandas as pd

from factor_library.base import Factor
from factor_library.futures.functions import add_yields_from_price, has_l2_columns


class L2TickFactor(Factor):
    """Level 2 Tick 因子基类，子类需实现 generate_para_space() 与 _compute_single()。"""

    input_type: str = "tick"
    applicable_market: List[str] = ["future"]
    max_lookback: int = 1
    store_time: str = "20250701"

    # 可选输出列名，未设置时默认取首列
    output_columns: List[str] = []

    def _check_l2_data(self, data: pd.DataFrame) -> bool:
        """检查输入是否包含 L2 五档列。"""
        return has_l2_columns(data)

    def _prepare_single(self, df: pd.DataFrame) -> pd.DataFrame:
        """补充单合约缺失的 yield 列。"""
        return add_yields_from_price(df)

    @abstractmethod
    def _compute_single(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """计算单合约因子值，返回与 df 同索引的因子 DataFrame。"""
        ...

    def compute(
        self,
        data: pd.DataFrame,
        deps: Optional[Dict[str, pd.Series]] = None,
    ) -> pd.Series:
        """逐合约调用 _compute_single()，返回含 MultiIndex[timestamp, code] 的 Series。"""
        if not self._check_l2_data(data):
            raise ValueError(
                f"L2TickFactor '{self.name}': 输入数据缺少 L2 五档列，"
                f"需要 bid_px_1..5, ask_px_1..5 等列"
            )

        has_multiindex = isinstance(data.index, pd.MultiIndex)

        if has_multiindex and "code" in data.index.names:
            results = []
            for code, group in data.groupby("code"):
                prepared = self._prepare_single(group)
                single_result = self._compute_single(prepared, **self.para)
                if single_result is not None and not single_result.empty:
                    results.append(single_result)
            if not results:
                return pd.Series(dtype=float, index=data.index)
            combined = pd.concat(results, axis=0).sort_index()
        else:
            prepared = self._prepare_single(data)
            combined = self._compute_single(prepared, **self.para)
            if combined is None or combined.empty:
                return pd.Series(dtype=float, index=data.index)

        if self.output_columns:
            cols = [c for c in self.output_columns if c in combined.columns]
            if cols:
                return combined[cols[0]]
            return pd.Series(dtype=float, index=data.index)

        if len(combined.columns) == 1:
            return combined.iloc[:, 0]
        return combined.iloc[:, 0]
