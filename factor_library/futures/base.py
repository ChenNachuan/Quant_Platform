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
    """
    Level 2 Tick 数据因子基类

    子类只需实现:
        generate_para_space()  → 参数组合列表
        _compute_single(df, **params) → 返回 pd.DataFrame（单合约）

    input_type 自动设为 "tick"
    逐合约 compute() 由基类 handle_grouped_compute() 实现
    """

    input_type: str = "tick"
    applicable_market: List[str] = ["future"]
    max_lookback: int = 1
    store_time: str = "20250701"

    # 可选：这些因子输出列名（用于从结果中提取）
    output_columns: List[str] = []

    def _check_l2_data(self, data: pd.DataFrame) -> bool:
        """快速检查数据是否包含 L2 五档列。"""
        return has_l2_columns(data)

    def _prepare_single(self, df: pd.DataFrame) -> pd.DataFrame:
        """准备单合约数据：添加 yield 列（如缺失）。"""
        return add_yields_from_price(df)

    @abstractmethod
    def _compute_single(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        计算单合约因子值。

        Args:
            df: 单合约 DataFrame（索引为时间戳，列包含 bid/ask px/vol）
            **kwargs: 来自 self.para 的参数

        Returns:
            因子 DataFrame（索引与 df 一致，列为因子值）
        """
        ...

    def compute(
        self,
        data: pd.DataFrame,
        deps: Optional[Dict[str, pd.Series]] = None,
    ) -> pd.Series:
        """
        全量计算：逐合约调用 _compute_single()。

        Args:
            data: MultiIndex[timestamp, code] 或 单合约 DataFrame
            deps: 依赖因子（本类因子通常不依赖上游）

        Returns:
            pd.Series with MultiIndex[timestamp, code]
        """
        if not self._check_l2_data(data):
            raise ValueError(
                f"L2TickFactor '{self.name}': 输入数据缺少 L2 五档列，"
                f"需要 bid_px_1..5, ask_px_1..5 等列"
            )

        has_multiindex = isinstance(data.index, pd.MultiIndex)

        if has_multiindex and "code" in data.index.names:
            # 逐合约计算
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
            # 单合约模式
            prepared = self._prepare_single(data)
            combined = self._compute_single(prepared, **self.para)
            if combined is None or combined.empty:
                return pd.Series(dtype=float, index=data.index)

        # 如果设置了 output_columns，只返回输出列；否则返回所有列
        if self.output_columns:
            cols = [c for c in self.output_columns if c in combined.columns]
            if cols:
                return combined[cols[0]]  # 默认返回第一列
            return pd.Series(dtype=float, index=data.index)

        # 返回第一列作为默认因子值（单列 Series）
        if len(combined.columns) == 1:
            return combined.iloc[:, 0]
        # 多列：返回 DataFrame（调用方可通过 output_columns 指定目标列）
        return combined.iloc[:, 0]
