# -*- coding: utf-8 -*-
"""
报价价差因子
- abspread: 买卖一档收益率价差
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor
from factor_library.futures.functions import quote_col


@register_factor
class BidAskSpread(L2TickFactor):
    name = "bid_ask_spread"
    para_group = {"default": {}}
    output_columns = ["abspread"]

    def generate_para_space(self):
        return [{}]

    def _compute_single(self, df, **kwargs):
        import pandas as pd

        spread = (
            quote_col(df, "bid", "yld", 1) - quote_col(df, "ask", "yld", 1)
        ) * 100.0
        return pd.DataFrame({"abspread": spread.round(4)}, index=df.index)


@register_factor
class MidPrice(L2TickFactor):
    """中间价和 EMA 平滑"""

    name = "mid_price"
    para_group = {
        "default": {"ema_period": [5, 10, 20, 50]},
    }
    output_columns = ["mid_px", "mid_ema"]

    def generate_para_space(self):
        return [{"ema_period": p} for p in self.para_group["default"]["ema_period"]]

    def _compute_single(self, df, ema_period=10, **kwargs):
        from factor_library.futures.functions import ema, mid_px
        import pandas as pd

        mid = mid_px(df)
        return pd.DataFrame(
            {"mid_px": mid, "mid_ema": ema(mid, ema_period)}, index=df.index
        )


@register_factor
class AvgQuote(L2TickFactor):
    """加权均价（指定 size 内的成交量加权价格）"""

    name = "avg_quote"
    para_group = {
        "default": {"max_quote_sz": [5000, 10000, 20000, 50000]},
    }
    output_columns = ["bid_wpx", "ask_wpx", "mid_wpx"]

    def generate_para_space(self):
        return [{"max_quote_sz": s} for s in self.para_group["default"]["max_quote_sz"]]

    def _compute_single(self, df, max_quote_sz=10000.0, **kwargs):
        import numpy as np
        import pandas as pd
        from factor_library.futures.functions import side_frame

        def _weighted_px_at_size(px_df, vol_df, max_sz):
            capped = vol_df.where(vol_df > 0, 0.0).copy()
            cum = capped.cumsum(axis=1)
            prev = cum.shift(axis=1, fill_value=0.0)
            take = (max_sz - prev).clip(lower=0.0)
            take = pd.DataFrame(
                np.minimum(take, capped), index=vol_df.index, columns=vol_df.columns
            )
            denom = take.sum(axis=1).replace(0, np.nan)
            return (px_df * take).sum(axis=1) / denom

        bid_wpx = _weighted_px_at_size(
            side_frame(df, "bid", "px"), side_frame(df, "bid", "vol"), max_quote_sz
        )
        ask_wpx = _weighted_px_at_size(
            side_frame(df, "ask", "px"), side_frame(df, "ask", "vol"), max_quote_sz
        )
        return pd.DataFrame(
            {
                "bid_wpx": bid_wpx,
                "ask_wpx": ask_wpx,
                "mid_wpx": (bid_wpx + ask_wpx) / 2.0,
            },
            index=df.index,
        )
