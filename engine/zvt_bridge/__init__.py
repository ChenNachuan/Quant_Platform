# -*- coding: utf-8 -*-
"""
ZVT 桥接模块

统一管理 ZVT 数据录制与回测适配
"""

from engine.zvt_bridge.data_syncer import ZvtDataSyncer
from engine.zvt_bridge.backtest import FactorAdapter, CustomStrategyAdapter

__all__ = ['ZvtDataSyncer', 'FactorAdapter', 'CustomStrategyAdapter']
