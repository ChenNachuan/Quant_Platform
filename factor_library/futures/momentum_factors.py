# -*- coding: utf-8 -*-
"""
动量/趋势因子
- bid_mmt, ask_mmt, mid_mmt, wmid_mmt: 滞后价差
- 向量/标量路径效率比
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor


@register_factor
class Momentum(L2TickFactor):
    """收益率动量（滞后价差）"""

    name = "momentum"
    para_group = {
        "default": {"lag": [5, 10, 20, 50, 100]},
    }
    output_columns = ["bid_mmt", "ask_mmt", "mid_mmt", "wmid_mmt"]

    def generate_para_space(self):
        return [{"lag": lag} for lag in self.para_group["default"]["lag"]]

    def _compute_single(self, df, lag=10, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import quote_col, mid_yld, wmid_yld

        bid = quote_col(df, "bid", "yld", 1)
        ask = quote_col(df, "ask", "yld", 1)
        mid = mid_yld(df)
        wmid = wmid_yld(df)
        return pd.DataFrame(
            {
                "bid_mmt": bid - bid.shift(lag),
                "ask_mmt": ask - ask.shift(lag),
                "mid_mmt": mid - mid.shift(lag),
                "wmid_mmt": wmid - wmid.shift(lag),
            },
            index=df.index,
        )


@register_factor
class VectorScalar(L2TickFactor):
    """向量/标量路径效率比"""

    name = "vector_scalar"
    para_group = {
        "default": {"period": [10, 20, 50, 100]},
    }
    output_columns = ["bid_vs", "ask_vs", "mid_vs", "wmid_vs"]

    def generate_para_space(self):
        return [{"period": p} for p in self.para_group["default"]["period"]]

    def _compute_single(self, df, period=20, **kwargs):
        import numpy as np
        import pandas as pd
        from factor_library.futures.functions import quote_col, mid_yld, wmid_yld

        bid = quote_col(df, "bid", "yld", 1)
        ask = quote_col(df, "ask", "yld", 1)
        mid = mid_yld(df)
        wmid = wmid_yld(df)

        def _vs(series, n):
            diff = series.diff()
            abs_path = diff.abs().rolling(n, min_periods=n).sum()
            net_path = (series - series.shift(n)).abs()
            denom = abs_path.replace(0, np.nan)
            return (net_path / denom).fillna(0)

        return pd.DataFrame(
            {
                "bid_vs": _vs(bid, period),
                "ask_vs": _vs(ask, period),
                "mid_vs": _vs(mid, period),
                "wmid_vs": _vs(wmid, period),
                "bid_yld_scalar": bid.diff()
                .abs()
                .rolling(period, min_periods=period)
                .sum(),
                "ask_yld_scalar": ask.diff()
                .abs()
                .rolling(period, min_periods=period)
                .sum(),
                "mid_yld_scalar": mid.diff()
                .abs()
                .rolling(period, min_periods=period)
                .sum(),
                "wmid_yld_scalar": wmid.diff()
                .abs()
                .rolling(period, min_periods=period)
                .sum(),
            },
            index=df.index,
        )
