# -*- coding: utf-8 -*-
"""
订单流重构因子（Order-flow reconstruction）
- 主动买卖成交量推断
- 挂单/撤单分解
- 做市商主动/被动成交量
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor


@register_factor
class OrderStack(L2TickFactor):
    """活跃买卖滚动对数比率"""

    name = "order_stack"
    para_group = {
        "default": {"period": [10, 20, 50]},
    }
    output_columns = [
        "stack",
        "stack_ymch",
        "act_buy_sz",
        "act_sell_sz",
        "mild_trade_sz",
    ]

    def generate_para_space(self):
        return [{"period": p} for p in self.para_group["default"]["period"]]

    def _compute_single(self, df, period=20, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import (
            trade_col,
            quote_col,
            bounded_log_ratio,
        )

        buy_vol = trade_col(df, "vol", default=0.0)
        # 简化: 根据成交价相对报价判断方向
        trade_px = trade_col(df, "px")
        bid1 = quote_col(df, "bid", "px", 1)
        ask1 = quote_col(df, "ask", "px", 1)
        mid = (bid1 + ask1) / 2.0
        act_buy = buy_vol.where(trade_px > mid, 0.0)
        act_sell = buy_vol.where(trade_px < mid, 0.0)
        mild = buy_vol.where(trade_px == mid, 0.0)

        act_buy_roll = act_buy.rolling(period, min_periods=1).sum()
        act_sell_roll = act_sell.rolling(period, min_periods=1).sum()

        return pd.DataFrame(
            {
                "stack": bounded_log_ratio(act_buy_roll + 1, act_sell_roll + 1),
                "stack_ymch": act_buy_roll - act_sell_roll,
                "act_buy_sz": act_buy_roll,
                "act_sell_sz": act_sell_roll,
                "mild_trade_sz": mild.rolling(period, min_periods=1).sum(),
            },
            index=df.index,
        )


@register_factor
class OrderSize(L2TickFactor):
    """挂单/撤单成交量分解"""

    name = "order_size"
    para_group = {
        "default": {"period": [20, 50, 100]},
    }
    output_columns = [
        "bid_order_sz_w",
        "ask_order_sz_w",
        "bid_cancel_sz_w",
        "ask_cancel_sz_w",
    ]

    def generate_para_space(self):
        return [{"period": p} for p in self.para_group["default"]["period"]]

    def _compute_single(self, df, period=50, **kwargs):
        import pandas as pd
        import numpy as np
        from factor_library.futures.functions import side_frame

        def _vol_changes(side):
            vols = side_frame(df, side, "vol")
            prev = vols.shift(1)
            increase = (vols - prev).clip(lower=0).sum(axis=1)
            decrease = (prev - vols).clip(lower=0).sum(axis=1)
            return increase, decrease

        bid_inc, bid_dec = _vol_changes("bid")
        ask_inc, ask_dec = _vol_changes("ask")

        bid_inc_roll = bid_inc.rolling(period, min_periods=1).sum()
        ask_inc_roll = ask_inc.rolling(period, min_periods=1).sum()
        bid_dec_roll = bid_dec.rolling(period, min_periods=1).sum()
        ask_dec_roll = ask_dec.rolling(period, min_periods=1).sum()

        denom = (bid_inc_roll + bid_dec_roll + ask_inc_roll + ask_dec_roll).replace(
            0, np.nan
        )

        return pd.DataFrame(
            {
                "bid_order_sz_w": bid_inc_roll / denom,
                "ask_order_sz_w": ask_inc_roll / denom,
                "bid_cancel_sz_w": bid_dec_roll / denom,
                "ask_cancel_sz_w": ask_dec_roll / denom,
                "bid_order_sz": bid_inc_roll,
                "ask_order_sz": ask_inc_roll,
                "bid_cancel_sz": bid_dec_roll,
                "ask_cancel_sz": ask_dec_roll,
                "bid_order_n": (bid_inc > 0).rolling(period, min_periods=1).sum(),
                "ask_order_n": (ask_inc > 0).rolling(period, min_periods=1).sum(),
                "bid_cancel_n": (bid_dec > 0).rolling(period, min_periods=1).sum(),
                "ask_cancel_n": (ask_dec > 0).rolling(period, min_periods=1).sum(),
            },
            index=df.index,
        )


@register_factor
class AddQuoteSize(L2TickFactor):
    """被动报价新增量（扣除主动成交）"""

    name = "add_quote_size"
    para_group = {
        "default": {},
    }
    output_columns = ["add_bid_quote_sz", "add_ask_quote_sz"]

    def generate_para_space(self):
        return [{}]

    def _compute_single(self, df, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import side_frame, trade_col

        def _add_quote(side):
            vols = side_frame(df, side, "vol")
            prev = vols.shift(1)
            increase = (vols - prev).clip(lower=0).sum(axis=1)
            return increase

        trade_vol = trade_col(df, "vol", default=0.0)
        trade_px = trade_col(df, "px")
        from factor_library.futures.functions import quote_col

        bid1 = quote_col(df, "bid", "px", 1)
        ask1 = quote_col(df, "ask", "px", 1)
        mid = (bid1 + ask1) / 2.0
        act_buy = trade_vol.where(trade_px > mid, 0.0)
        act_sell = trade_vol.where(trade_px < mid, 0.0)

        bid_add = _add_quote("bid") - act_sell
        ask_add = _add_quote("ask") - act_buy

        return pd.DataFrame(
            {
                "add_bid_quote_sz": bid_add.clip(lower=0),
                "add_ask_quote_sz": ask_add.clip(lower=0),
            },
            index=df.index,
        )


@register_factor
class MarketMakerSize(L2TickFactor):
    """做市商主动/被动成交量"""

    name = "market_maker_size"
    para_group = {
        "default": {},
    }
    output_columns = [
        "act_mm_buy_sz",
        "act_mm_sell_sz",
        "psv_mm_buy_sz",
        "psv_mm_sell_sz",
    ]

    def generate_para_space(self):
        return [{}]

    def _compute_single(self, df, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import trade_col, quote_col

        trade_vol = trade_col(df, "vol", default=0.0)
        trade_px = trade_col(df, "px")
        bid1 = quote_col(df, "bid", "px", 1)
        ask1 = quote_col(df, "ask", "px", 1)
        mid = (bid1 + ask1) / 2.0

        # 简化做市商推断: 大单(主动)和小单(被动)
        threshold = trade_vol.rolling(100, min_periods=10).quantile(0.75)
        is_big = trade_vol > threshold

        act_buy = trade_vol.where((trade_px > mid) & is_big, 0.0)
        act_sell = trade_vol.where((trade_px < mid) & is_big, 0.0)
        psv_buy = trade_vol.where((trade_px > mid) & ~is_big, 0.0)
        psv_sell = trade_vol.where((trade_px < mid) & ~is_big, 0.0)

        return pd.DataFrame(
            {
                "act_mm_buy_sz": act_buy.rolling(50, min_periods=1).sum(),
                "act_mm_sell_sz": act_sell.rolling(50, min_periods=1).sum(),
                "psv_mm_buy_sz": psv_buy.rolling(50, min_periods=1).sum(),
                "psv_mm_sell_sz": psv_sell.rolling(50, min_periods=1).sum(),
            },
            index=df.index,
        )
