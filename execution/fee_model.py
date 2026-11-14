"""A 股交易费用模型：佣金、印花税、过户费与滑点。

仅负责计算单笔成交成本；成交价决定归属 broker，账务记账归属 ledger。
印花税豁免前缀单一数据源来自 :mod:`core.asset_rules`（按上市基金前缀集合判定）。
"""

from __future__ import annotations

from core.asset_rules import is_stamp_tax_exempt_prefix


class FeeModel:
    """A 股四项费用模型，每笔成交调用一次 :meth:`compute`。

    契约：滑点（0.1%）作为双边纯成本计入总费用。传入的 ``price`` 必须是原始参考价，
    严禁传入已被滑点模型偏移过的成交价，否则会导致滑点被重复计收且方向可能失真。
    """

    COMMISSION_RATE = 0.00025  # 万 2.5，双边
    COMMISSION_MIN = 5.0  # 最低 5 元门槛
    STAMP_TAX_RATE = 0.0005  # 万 5，仅卖出
    TRANSFER_FEE_RATE = 0.00001  # 万 0.1，双边
    SLIPPAGE_RATE = 0.001  # 0.1%，单边纯成本

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        """计算单笔成交总费用 = 佣金 + 印花税 + 过户费 + 滑点。

        ``side`` 取值为 ``"BUY"`` 或 ``"SELL"``；``price`` 必须为原始参考价。
        卖出时必须提供 ``ticker``，以防免税标的因缺省代码被误收印花税。
        使用 NaN 安全守卫校验 ``price > 0`` 与 ``shares > 0``，拒绝非法输入以保证
        返回值满足费用非负及卖出费用不低于买入费用的不变量。
        """
        if side not in ("BUY", "SELL"):
            raise ValueError(f"side must be 'BUY' or 'SELL', got {side!r}")
        if side == "SELL" and not ticker:
            raise ValueError(
                "ticker is required on SELL — an empty ticker would silently "
                "pay stamp tax on a stamp-tax-exempt instrument"
            )
        if not (price > 0 and shares > 0):
            raise ValueError(
                f"price and shares must be strictly positive (NaN refused), "
                f"got price={price!r}, shares={shares!r}"
            )
        value = price * shares
        commission = max(value * self.COMMISSION_RATE, self.COMMISSION_MIN)
        stamp = (
            value * self.STAMP_TAX_RATE
            if side == "SELL" and not is_stamp_tax_exempt_prefix(ticker)
            else 0.0
        )
        transfer = value * self.TRANSFER_FEE_RATE
        slippage = value * self.SLIPPAGE_RATE
        return commission + stamp + transfer + slippage

    def buy_fee(self, price: float, shares: int, ticker: str = "") -> float:
        """买入侧总费用（买入不收印花税，``ticker`` 可省略）。"""
        return self.compute("BUY", price, shares, ticker)

    def sell_fee(self, price: float, shares: int, ticker: str) -> float:
        """卖出侧总费用（必须提供 ``ticker`` 以按前缀判定印花税豁免）。"""
        return self.compute("SELL", price, shares, ticker)
