# -*- coding: utf-8 -*-
"""
交易驱动因子（Trade-driven factors）
- VWAP 距离、日内高低价距离
- 滚动成交量加权均价
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor


@register_factor
class VwapDistance(L2TickFactor):
    """快速/慢速 VWAP 距离"""

    name = "vwap_distance"
    para_group = {
        "default": {"fast_period": [50, 100, 200], "slow_period": [200, 500, 1000]},
    }
    output_columns = ["vwap_dist"]

    def generate_para_space(self):
        return [
            {"fast_period": f, "slow_period": s}
            for f in self.para_group["default"]["fast_period"]
            for s in self.para_group["default"]["slow_period"]
            if s > f
        ]

    def _compute_single(self, df, fast_period=100, slow_period=500, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import trade_col, rolling_vwap

        px = trade_col(df, "px")
        vol = trade_col(df, "vol", default=0.0)
        tick_sz = vol.fillna(0).rolling(fast_period, min_periods=1).sum()
        tick_n = vol.notna().astype(int).rolling(fast_period, min_periods=1).sum()
        fast_vwap = rolling_vwap(px, vol, fast_period)
        slow_vwap = rolling_vwap(px, vol, slow_period)
        return pd.DataFrame(
            {
                "vwap_dist": fast_vwap - slow_vwap,
                "slow_vwap": slow_vwap,
                "fast_vwap": fast_vwap,
                "tick_trade_sz": tick_sz,
                "tick_trade_n": tick_n,
            },
            index=df.index,
        )


@register_factor
class IntradayDistance(L2TickFactor):
    """当前 VWAP 到日内滚动高低价的距离"""

    name = "intraday_distance"
    para_group = {
        "default": {"period": [100, 200, 500]},
    }
    output_columns = ["dist_to_high", "dist_to_low"]

    def generate_para_space(self):
        return [{"period": p} for p in self.para_group["default"]["period"]]

    def _compute_single(self, df, period=200, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import trade_col, rolling_vwap

        px = trade_col(df, "px")
        vol = trade_col(df, "vol", default=0.0)
        vwap = rolling_vwap(px, vol, period)
        roll_high = px.rolling(period, min_periods=1).max()
        roll_low = px.rolling(period, min_periods=1).min()
        return pd.DataFrame(
            {
                "dist_to_high": vwap - roll_high,
                "dist_to_low": vwap - roll_low,
            },
            index=df.index,
        )


@register_factor
class AvgNTrade(L2TickFactor):
    """滚动成交量加权均价（N 笔交易）"""

    name = "avg_ntrade"
    para_group = {
        "default": {"max_trade_n": [50, 100, 200, 500]},
    }
    output_columns = ["trade_npx"]

    def generate_para_space(self):
        return [{"max_trade_n": n} for n in self.para_group["default"]["max_trade_n"]]

    def _compute_single(self, df, max_trade_n=100, **kwargs):
        import pandas as pd
        import numpy as np
        from factor_library.futures.functions import trade_col

        px = trade_col(df, "px")
        vol = trade_col(df, "vol", default=0.0)
        has_trade = vol > 0
        cum_vol = vol.where(has_trade, 0.0).rolling(max_trade_n, min_periods=1).sum()
        cum_pv = (
            (px * vol).where(has_trade, 0.0).rolling(max_trade_n, min_periods=1).sum()
        )
        denom = cum_vol.replace(0, np.nan)
        return pd.DataFrame({"trade_npx": cum_pv / denom}, index=df.index)
