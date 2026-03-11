"""
可插拔交易成本模型
Version: 1.0.0
Date: 2026-03-11

借鉴 QuantConnect Lean 的 FeeModel / SlippageModel / FillModel 接口设计，
适配 A 股真实交易规则，替代回测中的固定系数成本估算。

使用示例:
    fee_model      = AShareFeeModel()
    slippage_model = VolumeShareSlippageModel(volume_limit=0.1, impact_factor=0.5)
    fill_model     = AShareFillModel()

    fee = fee_model.calc_fee(trade_value=100_000, direction='SELL')
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 数据类：订单与成本结果
# ─────────────────────────────────────────────

@dataclass
class OrderCost:
    """单笔订单的完整成本明细"""
    commission:  float = 0.0   # 佣金（含最低 5 元限制）
    stamp_duty:  float = 0.0   # 印花税（卖出单边）
    slippage:    float = 0.0   # 滑点损耗
    total:       float = field(init=False)

    def __post_init__(self):
        self.total = self.commission + self.stamp_duty + self.slippage

    def cost_rate(self, trade_value: float) -> float:
        """综合成本率 = total / trade_value"""
        return self.total / trade_value if trade_value > 0 else 0.0


# ─────────────────────────────────────────────
# 一、费率模型 (Fee Model)
# ─────────────────────────────────────────────

class BaseFeeModel(ABC):
    """费率模型抽象基类"""

    @abstractmethod
    def calc_fee(self, trade_value: float, direction: str) -> float:
        """
        计算单笔佣金。

        Args:
            trade_value: 成交金额（元）
            direction: 'BUY' 或 'SELL'

        Returns:
            佣金金额（元）
        """


class AShareFeeModel(BaseFeeModel):
    """
    A 股真实费率模型。

    规则：
    - 双边佣金：max(成交额 × commission_rate, min_commission)
    - 印花税：仅卖出单边，成交额 × stamp_duty（2023 年 8 月降至 0.05%）
    - 过户费：沪市收取，深市免收（此处统一估算取近似平均 0.00002）
    """

    def __init__(
        self,
        commission_rate: float = 0.0003,   # 万三（双边）
        stamp_duty:      float = 0.0005,   # 印花税 0.05%（卖出单边）
        transfer_fee:    float = 0.00002,  # 过户费（沪市）
        min_commission:  float = 5.0       # 最低佣金：5 元/笔
    ):
        self.commission_rate = commission_rate
        self.stamp_duty = stamp_duty
        self.transfer_fee = transfer_fee
        self.min_commission = min_commission

    def calc_fee(self, trade_value: float, direction: str) -> float:
        """
        佣金 = max(成交额 × 佣金率, 最低佣金)
        印花税（仅卖出）= 成交额 × 印花税率
        """
        commission = max(trade_value * self.commission_rate, self.min_commission)
        stamp = trade_value * self.stamp_duty if direction.upper() == 'SELL' else 0.0
        transfer = trade_value * self.transfer_fee
        return commission + stamp + transfer

    def calc_order_cost(self, trade_value: float, direction: str) -> OrderCost:
        """返回完整 OrderCost 明细"""
        commission = max(trade_value * self.commission_rate, self.min_commission)
        stamp = trade_value * self.stamp_duty if direction.upper() == 'SELL' else 0.0
        return OrderCost(
            commission=commission + trade_value * self.transfer_fee,
            stamp_duty=stamp,
            slippage=0.0
        )

    @classmethod
    def from_config(cls, config: dict) -> 'AShareFeeModel':
        """从 settings.toml 的 [backtest.cost] 节创建实例"""
        return cls(
            commission_rate=config.get('commission_rate', 0.0003),
            stamp_duty=config.get('stamp_duty', 0.0005),
            min_commission=config.get('min_commission', 5.0),
        )


class FixedRateFeeModel(BaseFeeModel):
    """
    固定费率模型（向后兼容旧逻辑，不推荐用于 A 股）。
    单边费率，买卖均收，无最低限制。
    """

    def __init__(self, rate: float = 0.001):
        self.rate = rate

    def calc_fee(self, trade_value: float, direction: str) -> float:
        return trade_value * self.rate


# ─────────────────────────────────────────────
# 二、滑点模型 (Slippage Model)
# ─────────────────────────────────────────────

class BaseSlippageModel(ABC):
    """滑点模型抽象基类"""

    @abstractmethod
    def calc_slippage(
        self,
        price: float,
        order_volume: float,
        bar_volume: float,
        direction: str
    ) -> float:
        """
        返回滑点导致的额外成本金额（元）。

        Args:
            price: 委托价格
            order_volume: 委托数量（股）
            bar_volume: 当前 Bar 的总成交量（股）
            direction: 'BUY' 或 'SELL'
        """


class ConstantSlippageModel(BaseSlippageModel):
    """
    固定滑点率模型（简单估算）。
    slippage = price * slippage_rate * volume
    """

    def __init__(self, slippage_rate: float = 0.0002):
        self.slippage_rate = slippage_rate

    def calc_slippage(self, price: float, order_volume: float,
                      bar_volume: float, direction: str) -> float:
        return price * self.slippage_rate * order_volume


class VolumeShareSlippageModel(BaseSlippageModel):
    """
    成交量占比滑点模型（A 股 3min/日线 Bar 适用）。

    核心公式：
    - 基础滑点率 = slippage_rate
    - 若 order_vol / bar_vol > volume_limit，超出部分按 impact_factor 惩罚加倍
    - slippage = price * effective_slippage_rate * order_vol

    参数说明：
    - volume_limit: 单笔订单占 Bar 成交量的上限（默认 10%）
    - impact_factor: 超过上限后的市场冲击放大系数（默认 0.5，即额外加 50%）
    """

    def __init__(
        self,
        slippage_rate:  float = 0.0002,  # 基础滑点率
        volume_limit:   float = 0.1,     # 成交量占比阈值
        impact_factor:  float = 0.5      # 超限惩罚系数
    ):
        self.slippage_rate = slippage_rate
        self.volume_limit = volume_limit
        self.impact_factor = impact_factor

    def calc_slippage(self, price: float, order_volume: float,
                      bar_volume: float, direction: str) -> float:
        if bar_volume <= 0:
            return price * self.slippage_rate * order_volume

        vol_share = order_volume / bar_volume
        if vol_share > self.volume_limit:
            # 超出限制的比例乘以惩罚系数
            excess = vol_share - self.volume_limit
            effective_rate = self.slippage_rate * (1 + excess * self.impact_factor)
        else:
            effective_rate = self.slippage_rate

        return price * effective_rate * order_volume


# ─────────────────────────────────────────────
# 三、成交模型 (Fill Model)
# ─────────────────────────────────────────────

class AShareFillModel:
    """
    A 股成交模型。

    处理 A 股特有规则：
    1. 涨停一字板：当日 open == high == limit_up → 禁止买入（无卖盘）
    2. 跌停一字板：当日 open == low == limit_down → 禁止卖出（无买盘）
    3. 停牌：volume == 0 → 禁止一切成交
    4. T+0 禁止：A 股当日买入不得当日卖出（股票，期货除外）
    """

    # A 股默认涨跌停幅度
    LIMIT_RATE_NORMAL = 0.10    # 普通股票 ±10%
    LIMIT_RATE_ST     = 0.05    # ST 股票 ±5%
    LIMIT_RATE_NEW    = 0.20    # 新股上市首5日 ±20%（近似）

    def can_fill(
        self,
        direction: str,
        open_price: float,
        high:       float,
        low:        float,
        volume:     float,
        prev_close: Optional[float] = None,
        is_st:      bool = False
    ) -> tuple[bool, str]:
        """
        判断订单是否可以成交。

        Returns:
            (can_fill: bool, reason: str)
        """
        direction = direction.upper()

        # 停牌检查
        if volume == 0:
            return False, "停牌：成交量为 0"

        if prev_close is not None and prev_close > 0:
            limit_rate = self.LIMIT_RATE_ST if is_st else self.LIMIT_RATE_NORMAL
            limit_up   = round(prev_close * (1 + limit_rate), 2)
            limit_down = round(prev_close * (1 - limit_rate), 2)

            # 涨停一字板：开盘即涨停，全天无卖盘
            if (direction == 'BUY'
                    and abs(open_price - limit_up) < 0.01
                    and abs(high - limit_up) < 0.01):
                return False, f"涨停一字板（{limit_up:.2f}），无法买入"

            # 跌停一字板：开盘即跌停，全天无买盘
            if (direction == 'SELL'
                    and abs(open_price - limit_down) < 0.01
                    and abs(low - limit_down) < 0.01):
                return False, f"跌停一字板（{limit_down:.2f}），无法卖出"

        return True, "可以成交"

    def simulate_fill_price(
        self,
        bar_open:   float,
        bar_close:  float,
        direction:  str,
        slippage:   float = 0.0
    ) -> float:
        """
        简单成交价格模拟：以开盘价成交，加上滑点。
        slippage 为每股滑点金额（正值）。
        """
        return bar_open + (slippage if direction.upper() == 'BUY' else -slippage)


# ─────────────────────────────────────────────
# 四、组合使用示例（向量化批量计算）
# ─────────────────────────────────────────────

def estimate_portfolio_cost(
    turnover_df: pd.DataFrame,   # 宽表: index=date, columns=entity_id, value=换手率(0~1)
    price_df:    pd.DataFrame,   # 宽表: index=date, columns=entity_id, value=成交价
    total_value: float,          # 账户总市值
    fee_model:   BaseFeeModel,
) -> pd.Series:
    """
    向量化批量估算每日交易成本总额。

    公式：
    cost_i = fee_model.calc_fee(turnover_i × total_value, direction=BOTH)
    daily_cost = sum(cost over all positions)

    Returns:
        pd.Series: index=date, value=当日总成本（元）
    """
    # 每标的成交金额矩阵
    trade_value_df = turnover_df.abs() * total_value

    # 向量化计算：买入部分（正换手）
    buy_mask  = turnover_df > 0
    sell_mask = turnover_df < 0

    # 最低佣金检查只能逐元素做，用 applymap
    def fee_buy(v: float)  -> float: return fee_model.calc_fee(v, 'BUY')  if v > 0 else 0
    def fee_sell(v: float) -> float: return fee_model.calc_fee(v, 'SELL') if v > 0 else 0

    buy_cost  = trade_value_df.where(buy_mask,  0).map(fee_buy)
    sell_cost = trade_value_df.where(sell_mask, 0).map(fee_sell)

    return (buy_cost + sell_cost).sum(axis=1)


__all__ = [
    'OrderCost',
    'BaseFeeModel', 'AShareFeeModel', 'FixedRateFeeModel',
    'BaseSlippageModel', 'ConstantSlippageModel', 'VolumeShareSlippageModel',
    'AShareFillModel',
    'estimate_portfolio_cost',
]
