# -*- coding: utf-8 -*-
"""
期货 L2 Tick 因子库

从 indicator_py 迁移的高频因子，适配 Factor 基类接口，支持 DataLake 格式的 L2 五档数据。

因子输出列注册表: FACTOR_REGISTRY
批量计算: compute_all_l2_factors(df, params)
"""

import pandas as pd

from factor_library.futures.base import L2TickFactor

# 导入所有因子模块（触发 @register_factor 装饰器自动注册）
from factor_library.futures.quote_spread import (  # noqa: F401
    AvgQuote,
    BidAskSpread,
    MidPrice,
)
from factor_library.futures.orderbook_pressure import (  # noqa: F401
    QuotePricePressure,
    QuoteSizePressure,
    QuoteSizeR,
)
from factor_library.futures.momentum_factors import (  # noqa: F401
    Momentum,
    VectorScalar,
)
from factor_library.futures.trade_factors import (  # noqa: F401
    AvgNTrade,
    IntradayDistance,
    VwapDistance,
)
from factor_library.futures.push_factors import (  # noqa: F401
    PushPrice,
    PushSize,
)
from factor_library.futures.order_flow import (  # noqa: F401
    AddQuoteSize,
    MarketMakerSize,
    OrderSize,
    OrderStack,
)
from factor_library.futures.statistical import (  # noqa: F401
    DerivedEMA,
    EMSD,
)
from factor_library.futures.minute_factors import (  # noqa: F401
    MinuteFactorLibrary,
    MinuteFactors,
    list_minute_factors,
    get_minute_factor_expressions,
    get_minute_factor_dict,
)

# 因子输出列注册表 (与 indicator_py/all_factors.py 的 FACTOR_REGISTRY 对应)
FACTOR_REGISTRY: dict[str, list[str]] = {
    "bid_ask_spread": ["abspread"],
    "mid_price": ["mid_px", "mid_ema"],
    "avg_quote": ["bid_wpx", "ask_wpx", "mid_wpx"],
    "quote_size_pressure": ["quote_sz_pressure", "ask_sz_w", "bid_sz_w"],
    "quote_size_r": ["quote_ask_sz_r", "quote_bid_sz_r"],
    "quote_price_pressure": ["bid_yld_pressure", "ask_yld_pressure"],
    "push_size": ["push_up_sz", "push_down_sz", "push_up_sz_avg", "push_down_sz_avg"],
    "push_price": ["push_up_bp", "push_down_bp", "push_up_bp_avg", "push_down_bp_avg"],
    "momentum": ["bid_mmt", "ask_mmt", "mid_mmt", "wmid_mmt"],
    "vector_scalar": [
        "bid_vs",
        "ask_vs",
        "mid_vs",
        "wmid_vs",
        "bid_yld_scalar",
        "ask_yld_scalar",
        "mid_yld_scalar",
        "wmid_yld_scalar",
    ],
    "vwap_distance": [
        "vwap_dist",
        "slow_vwap",
        "fast_vwap",
        "tick_trade_sz",
        "tick_trade_n",
    ],
    "intraday_distance": ["dist_to_high", "dist_to_low"],
    "avg_ntrade": ["trade_npx"],
    "order_stack": [
        "stack",
        "stack_ymch",
        "act_buy_sz",
        "act_sell_sz",
        "mild_trade_sz",
    ],
    "order_size": [
        "bid_order_sz_w",
        "ask_order_sz_w",
        "bid_cancel_sz_w",
        "ask_cancel_sz_w",
        "bid_order_sz",
        "ask_order_sz",
        "bid_cancel_sz",
        "ask_cancel_sz",
        "bid_order_n",
        "ask_order_n",
        "bid_cancel_n",
        "ask_cancel_n",
        "bid_order_per_sz",
        "ask_order_per_sz",
    ],
    "add_quote_size": ["add_bid_quote_sz", "add_ask_quote_sz"],
    "market_maker_size": [
        "act_mm_buy_sz",
        "act_mm_sell_sz",
        "psv_mm_buy_sz",
        "psv_mm_sell_sz",
    ],
    "emsd": ["ema", "emsd"],
    "derived_ema": ["derived_ema"],
}


def list_l2_factors() -> list[str]:
    """列出所有已注册的 L2 因子名称。"""
    from factor_library.registry import FactorRegistry

    return [name for name in FactorRegistry.list_factors() if name in FACTOR_REGISTRY]


def compute_all_l2_factors(
    data: pd.DataFrame,
    timeframe: str = "tick",
    params: dict | None = None,
) -> pd.DataFrame:
    """
    批量计算所有 L2 因子。

    Args:
        data: L2 tick 数据 DataFrame（MultiIndex[timestamp, code]）
        timeframe: 时间框架
        params: 可选参数字典，按因子名称 key，如 {"momentum": {"lag": 20}}

    Returns:
        所有因子列的 DataFrame
    """
    from factor_library.registry import FactorRegistry

    params = params or {}
    pieces = []

    for factor_name in FACTOR_REGISTRY:
        factor_cls = FactorRegistry.get(factor_name)
        if factor_cls is None:
            continue

        factor_params = params.get(factor_name, {})
        if not factor_params:
            # 使用第一个参数组合作为默认值
            inst = factor_cls(timeframe=timeframe, para=factor_params)
            para_space = inst.generate_para_space()
            factor_params = para_space[0] if para_space else {}

        try:
            inst = factor_cls(timeframe=timeframe, para=factor_params)
            result = inst.compute(data)
            if isinstance(result, pd.Series) and not result.empty:
                result.name = factor_name
                pieces.append(result)
        except Exception:
            continue

    if not pieces:
        return pd.DataFrame(index=data.index)

    out = pd.concat(pieces, axis=1)
    return out.loc[:, ~out.columns.duplicated()]


__all__ = [
    "L2TickFactor",
    "FACTOR_REGISTRY",
    "list_l2_factors",
    "compute_all_l2_factors",
    "MinuteFactorLibrary",
    "MinuteFactors",
    "list_minute_factors",
    "get_minute_factor_expressions",
    "get_minute_factor_dict",
]
