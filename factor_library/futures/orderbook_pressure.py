# -*- coding: utf-8 -*-
"""
订单簿压力因子（Order-book pressure）
- 买卖挂单量衰减加权、对数变化率
- 买卖收益率压力（等规模插值）
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor


@register_factor
class QuoteSizePressure(L2TickFactor):
    """衰减加权的买卖挂单量不平衡"""

    name = "quote_size_pressure"
    para_group = {
        "default": {"decay_w": [0.3, 0.5, 0.7, 0.9]},
    }
    output_columns = ["quote_sz_pressure", "ask_sz_w", "bid_sz_w"]

    def generate_para_space(self):
        return [{"decay_w": w} for w in self.para_group["default"]["decay_w"]]

    def _compute_single(self, df, decay_w=0.5, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import LEVELS, side_frame, safe_div

        weights = pd.Series([decay_w**i for i in range(len(LEVELS))], index=LEVELS)
        bid_w = (
            side_frame(df, "bid", "vol")
            .where(lambda x: x > 0, 0.0)
            .mul(weights, axis=1)
            .sum(axis=1)
        )
        ask_w = (
            side_frame(df, "ask", "vol")
            .where(lambda x: x > 0, 0.0)
            .mul(weights, axis=1)
            .sum(axis=1)
        )
        total = bid_w + ask_w
        return pd.DataFrame(
            {
                "quote_sz_pressure": safe_div(bid_w - ask_w, total),
                "ask_sz_w": ask_w,
                "bid_sz_w": bid_w,
            },
            index=df.index,
        )


@register_factor
class QuoteSizeR(L2TickFactor):
    """EMA 平滑的衰减深度对数变化"""

    name = "quote_size_r"
    para_group = {
        "default": {"decay_w": [0.3, 0.5, 0.7], "ema_period": [10, 20, 50]},
    }
    output_columns = ["quote_ask_sz_r", "quote_bid_sz_r"]

    def generate_para_space(self):
        return [
            {"decay_w": w, "ema_period": e}
            for w in self.para_group["default"]["decay_w"]
            for e in self.para_group["default"]["ema_period"]
        ]

    def _compute_single(self, df, decay_w=0.5, ema_period=20, **kwargs):
        import numpy as np
        import pandas as pd
        from factor_library.futures.functions import LEVELS, side_frame, ema

        weights = pd.Series([decay_w**i for i in range(len(LEVELS))], index=LEVELS)
        bid_w = (
            side_frame(df, "bid", "vol")
            .where(lambda x: x > 0, 0.0)
            .mul(weights, axis=1)
            .sum(axis=1)
        )
        ask_w = (
            side_frame(df, "ask", "vol")
            .where(lambda x: x > 0, 0.0)
            .mul(weights, axis=1)
            .sum(axis=1)
        )
        bid_r = ema(np.log(bid_w.replace(0, np.nan)).diff().fillna(0), ema_period)
        ask_r = ema(np.log(ask_w.replace(0, np.nan)).diff().fillna(0), ema_period)
        return pd.DataFrame(
            {"quote_ask_sz_r": ask_r, "quote_bid_sz_r": bid_r}, index=df.index
        )


@register_factor
class QuotePricePressure(L2TickFactor):
    """等规模插值报价收益率压力"""

    name = "quote_price_pressure"
    para_group = {
        "default": {"n_bp": [1, 2, 5, 10]},
    }
    output_columns = ["bid_yld_pressure", "ask_yld_pressure"]

    def generate_para_space(self):
        return [{"n_bp": n} for n in self.para_group["default"]["n_bp"]]

    def _compute_single(self, df, n_bp=5, **kwargs):
        import numpy as np
        import pandas as pd
        from factor_library.futures.functions import side_frame

        def _pressure(side):
            vols = side_frame(df, side, "vol").where(lambda x: x > 0, 0.0)
            ylds = side_frame(df, side, "yld")
            # 简化为取前 N 档的加权收益率
            weights = vols.div(vols.sum(axis=1).replace(0, np.nan), axis=0).fillna(0)
            return (ylds * weights).sum(axis=1)

        bid_p = _pressure("bid")
        ask_p = _pressure("ask")
        return pd.DataFrame(
            {"bid_yld_pressure": bid_p, "ask_yld_pressure": ask_p}, index=df.index
        )
