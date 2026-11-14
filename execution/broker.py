"""券商模块：串联盘前风控、成交撮合与账本结算。

回测与实盘共用 ``submit_orders`` 路径，仅通过 ``create_broker`` 选择具体实现。
``submit_orders`` 内部固定三步顺序：(a) 盘前风控校验与审计日志，(b) 通过
``execution.market_data`` 按 ``fill_basis`` 查询原始成交价，(c) 账本结算。
"""

from __future__ import annotations

import itertools
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime

from core.types import Direction, Order, OrderStatus, PriceType, Trade
from execution.fee_model import FeeModel
from execution.ledger import SimAccount as LedgerSimAccount
from execution.ledger import transition
from execution.market_data import get_latest_price
from execution.risk_control import log_risk_audit, pre_trade_risk_check

logger = logging.getLogger(__name__)

# 成交价基准开关："t1_open" 为正式基准；"decision_close" 仅作对照实验
FILL_BASIS_T1_OPEN = "t1_open"
FILL_BASIS_DECISION_CLOSE = "decision_close"
VALID_FILL_BASES = (FILL_BASIS_T1_OPEN, FILL_BASIS_DECISION_CLOSE)

_order_seq = itertools.count(1)


class Broker(ABC):
    """券商抽象基类（统一 4 个核心接口）。"""

    @abstractmethod
    def submit_orders(self, orders: list[dict]) -> list[dict]: ...

    @abstractmethod
    def get_holdings(self) -> dict[str, int]: ...

    @abstractmethod
    def get_cash(self) -> float: ...

    @abstractmethod
    def get_nav(self) -> float: ...


class SimAccountAdapter:
    """适配器：将 ``ledger.SimAccount`` 暴露为风控所需的账户接口。

    ``holdings`` 是基于账本持仓的只读派生视图，不维护平行账本；
    结算统一经由 ``ledger.SimAccount.settle_fill`` 完成。
    """

    def __init__(self, initial_cash: float, account_id: str = "sim"):
        self._ledger = LedgerSimAccount(cash=initial_cash, account_id=account_id)

    @property
    def cash(self) -> float:
        return self._ledger.cash

    @property
    def holdings(self) -> dict[str, int]:
        return {t: p.quantity for t, p in self._ledger.positions.items()}

    @property
    def nav(self) -> float:
        return self._ledger.nav

    @property
    def ledger(self) -> LedgerSimAccount:
        return self._ledger


class SimBroker(Broker):
    """模拟券商：在 ``submit_orders`` 中依次执行风控、撮合与结算。

    config 参数：
    - ``initial_cash``：初始资金。
    - ``bars``：长表行情数据（date / ticker / OHLCV）。
    - ``fill_date``：成交执行日期（``t1_open`` 模式使用）。
    - ``fill_basis``：``"t1_open"``（默认）或 ``"decision_close"``。
    - ``risk``：传给 ``pre_trade_risk_check`` 的风控配置字典。
    - ``account_id``、``portfolio_id``：可选账户与组合标识。
    """

    def __init__(self, config: dict):
        self._config = config
        self._account = SimAccountAdapter(
            initial_cash=config["initial_cash"],
            account_id=config.get("account_id", "sim"),
        )
        self._fee_model = FeeModel()
        self._bars = config["bars"]
        self._fill_basis = config.get("fill_basis", FILL_BASIS_T1_OPEN)
        if self._fill_basis not in VALID_FILL_BASES:
            raise ValueError(
                f"fill_basis must be one of {VALID_FILL_BASES}, "
                f"got {self._fill_basis!r}"
            )
        self._fill_date: date | str | None = config.get("fill_date")
        self._decision_date: date | str | None = config.get("decision_date")
        self._risk_config: dict = config.get("risk", {})

    @property
    def fill_date(self) -> date | str | None:
        return self._fill_date

    @fill_date.setter
    def fill_date(self, value: date | str | None) -> None:
        self._fill_date = value

    @property
    def decision_date(self) -> date | str | None:
        return self._decision_date

    @decision_date.setter
    def decision_date(self, value: date | str | None) -> None:
        self._decision_date = value

    @property
    def account(self) -> SimAccountAdapter:
        return self._account

    def submit_orders(self, orders: list[dict]) -> list[dict]:
        """批量提交订单：风控 -> 询价 -> 账本结算。

        每笔输入返回一个含 ``status``（``FILLED`` 或 ``REJECTED``）的结果字典，
        成交附带 ``fill_price``。停牌无价格或风控拦截均明确返回 ``REJECTED``，
        不静默跳过，且不模拟部分成交（全成或全拒）。
        """
        # (a) 盘前风控
        check = pre_trade_risk_check(
            orders, self._account, self.get_nav(), self._risk_config
        )
        log_risk_audit(check)

        results: list[dict] = []
        for o in orders:
            if check.is_blocked(o["ticker"]):
                results.append({
                    **o, "status": "REJECTED",
                    "reject_reason": "risk_blocked",
                })
                continue
            if o.get("skip"):
                results.append({
                    **o, "status": "REJECTED",
                    "reject_reason": "skip_flag",
                })
                continue

            # (b) 查询成交价
            price = self._lookup_fill_price(o)
            if price is None:
                results.append({
                    **o, "status": "REJECTED",
                    "reject_reason": "no_price",
                })
                continue

            # (c) 账本结算
            try:
                self._settle(o, price)
            except (ValueError, TypeError) as exc:
                logger.warning("settlement failed for %s: %s", o.get("ticker"), exc)
                results.append({
                    **o, "status": "REJECTED",
                    "reject_reason": f"settlement: {exc}",
                })
                continue

            results.append({**o, "status": "FILLED", "fill_price": price})

        return results

    def get_holdings(self) -> dict[str, int]:
        return self._account.holdings

    def get_cash(self) -> float:
        return self._account.cash

    def get_nav(self) -> float:
        return self._account.nav

    # --- 内部方法 ---

    def _lookup_fill_price(self, order: dict) -> float | None:
        """按 ``fill_basis`` 获取原始未调整成交价；停牌缺失时返回 ``None`` 触发拒单。"""
        if self._fill_basis == FILL_BASIS_DECISION_CLOSE:
            if self._decision_date is None:
                raise ValueError(
                    "decision_date is required for fill_basis='decision_close'"
                )
            return get_latest_price(
                self._bars, order["ticker"], self._decision_date, field="close"
            )
        # T+1 开盘价取原始价格，滑点由 FeeModel 统一计费
        if self._fill_date is None:
            raise ValueError(
                "fill_date is required for fill_basis='t1_open' — set it via "
                "config or the fill_date property"
            )
        return get_latest_price(
            self._bars, order["ticker"], self._fill_date, field="open"
        )

    def _settle(self, order_dict: dict, fill_price: float) -> None:
        """执行账本结算（资金流转与持仓记账）。"""
        fees = self._fee_model.compute(
            order_dict["side"],
            fill_price,
            order_dict["shares"],
            order_dict["ticker"],
        )
        direction = Direction.BUY if order_dict["side"] == "BUY" else Direction.SELL
        now = datetime.utcnow()
        portfolio_id = self._config.get("portfolio_id", "sim")
        seq = next(_order_seq)

        order = Order(
            order_id=f"sim-{seq}",
            portfolio_id=portfolio_id,
            datetime=now,
            ticker=order_dict["ticker"],
            direction=direction,
            quantity=order_dict["shares"],
            price_type=PriceType.LIMIT,
            limit_price=order_dict["limit_price"],
        )
        transition(order, OrderStatus.SUBMITTED)

        trade = Trade(
            trade_id=f"fill-{seq}",
            order_id=order.order_id,
            portfolio_id=portfolio_id,
            datetime=now,
            ticker=order_dict["ticker"],
            direction=direction,
            filled_qty=order_dict["shares"],
            fill_price=fill_price,
            commission=fees,
        )

        self._account.ledger.settle_fill(order, trade)
        transition(order, OrderStatus.FILLED)


def create_broker(mode: str, config: dict) -> Broker:
    """券商工厂：按模式选择具体实现（``"sim"`` 返回 ``SimBroker``，``"qmt"`` 预留）。"""
    if mode == "sim":
        return SimBroker(config)
    if mode == "qmt":
        raise NotImplementedError(
            "QmtBroker is the V2 slot — not implemented in V1"
        )
    raise ValueError(f"unknown broker mode {mode!r}, expected 'sim' or 'qmt'")
