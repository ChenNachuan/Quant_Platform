# -*- coding: utf-8 -*-
"""数据录制器（AKShare）"""

from engine.zvt_bridge.recorders.akshare.stock_1d_kdata_recorder import (
    register_akshare_recorders,
    AKShareStock1dKdataRecorder
)
from engine.zvt_bridge.recorders.akshare.stock_adj_factor_recorder import (
    register_akshare_factor_recorder,
    AKShareStockAdjFactorRecorder
)

__all__ = [
    'register_akshare_recorders',
    'AKShareStock1dKdataRecorder',
    'register_akshare_factor_recorder',
    'AKShareStockAdjFactorRecorder'
]
