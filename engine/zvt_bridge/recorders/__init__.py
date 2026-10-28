# -*- coding: utf-8 -*-
"""数据录制器"""

from engine.zvt_bridge.recorders.akshare import (
    register_akshare_recorders,
    register_akshare_factor_recorder,
)

__all__ = [
    'register_akshare_recorders',
    'register_akshare_factor_recorder'
]
