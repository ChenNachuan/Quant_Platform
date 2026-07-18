# -*- coding: utf-8 -*-
"""
统计特征因子
- EMSD: 指数加权均值和标准差
- Derived EMA: 可配置的派生 EMA
"""

from factor_library.futures.base import L2TickFactor
from factor_library.registry import register_factor


@register_factor
class EMSD(L2TickFactor):
    """指数加权均值和标准差"""

    name = "emsd"
    para_group = {
        "default": {"period": [10, 20, 50, 100]},
    }
    output_columns = ["ema", "emsd"]

    def generate_para_space(self):
        return [{"period": p} for p in self.para_group["default"]["period"]]

    def _compute_single(self, df, period=20, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import mid_px, ema

        mid = mid_px(df)
        e = ema(mid, period)
        sd = mid.ewm(span=period, adjust=False, min_periods=1).std()
        return pd.DataFrame({"ema": e, "emsd": sd}, index=df.index)


@register_factor
class DerivedEMA(L2TickFactor):
    """可配置派生 EMA（默认基于 mid_px）"""

    name = "derived_ema"
    para_group = {
        "default": {"period": [5, 10, 20, 50]},
    }
    output_columns = ["derived_ema"]

    def generate_para_space(self):
        return [{"period": p} for p in self.para_group["default"]["period"]]

    def _compute_single(self, df, period=10, **kwargs):
        import pandas as pd
        from factor_library.futures.functions import mid_px, ema

        mid = mid_px(df)
        return pd.DataFrame({"derived_ema": ema(mid, period)}, index=df.index)
