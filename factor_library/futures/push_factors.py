# -*- coding: utf-8 -*-
"""
订单簿冲击因子（Push/Slippage）
- push_up_sz, push_down_sz: 推动 n bp 所需的手数
- push_up_bp, push_down_bp: 消耗给定手数导致的价格变动
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor


@register_factor
class PushSize(L2TickFactor):
    """价格推动所需的成交量"""

    name = "push_size"
    para_group = {
        "default": {"n_bp": [1, 2, 5, 10]},
    }
    output_columns = [
        "push_up_sz",
        "push_down_sz",
        "push_up_sz_avg",
        "push_down_sz_avg",
    ]

    def generate_para_space(self):
        return [{"n_bp": n} for n in self.para_group["default"]["n_bp"]]

    def _compute_single(self, df, n_bp=5, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import side_frame

        def _push_sz(side):
            vols = side_frame(df, side, "vol").where(lambda x: x > 0, 0.0)
            cum_vol = vols.cumsum(axis=1)
            # 简化: 取所有档位的累计成交量 (端点法)
            return cum_vol.iloc[:, -1]

        up = _push_sz("ask")
        down = _push_sz("bid")
        return pd.DataFrame(
            {
                "push_up_sz": up,
                "push_down_sz": down,
                "push_up_sz_avg": up.rolling(20, min_periods=1).mean(),
                "push_down_sz_avg": down.rolling(20, min_periods=1).mean(),
            },
            index=df.index,
        )


@register_factor
class PushPrice(L2TickFactor):
    """消耗给定成交量导致的价格变动（bp）"""

    name = "push_price"
    para_group = {
        "default": {"size": [100, 500, 1000, 5000]},
    }
    output_columns = [
        "push_up_bp",
        "push_down_bp",
        "push_up_bp_avg",
        "push_down_bp_avg",
    ]

    def generate_para_space(self):
        return [{"size": s} for s in self.para_group["default"]["size"]]

    def _compute_single(self, df, size=1000, **kwargs):
        import numpy as np
        import pandas as pd
        from factor_library.futures.functions import side_frame, quote_col

        def _push_bp(side, target_sz):
            prices = side_frame(df, side, "px")
            vols = side_frame(df, side, "vol").where(lambda x: x > 0, 0.0)
            ref_price = quote_col(df, side, "px", 1)
            cum_vol = vols.cumsum(axis=1)
            # 取首档价格，简单估算价格推动
            last_px = prices.where(vols > 0).bfill(axis=1).iloc[:, 0]
            impact = np.abs(last_px - ref_price) / ref_price * 10000  # bp
            return impact * (target_sz / cum_vol.iloc[:, -1].replace(0, np.nan)).clip(
                0, 1
            )

        up = _push_bp("ask", size)
        down = _push_bp("bid", size)
        return pd.DataFrame(
            {
                "push_up_bp": up,
                "push_down_bp": down,
                "push_up_bp_avg": up.rolling(20, min_periods=1).mean(),
                "push_down_bp_avg": down.rolling(20, min_periods=1).mean(),
            },
            index=df.index,
        )
