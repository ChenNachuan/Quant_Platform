"""账本模块：订单状态机、资金流结算、持仓记账与追加写事件日志。

回测与实盘共用同一套状态流转、资金结算与日级 Parquet 日志逻辑。
"""

from __future__ import annotations

import logging
from datetime import datetime, date
from pathlib import Path

from core.types import (
    Account,
    Direction,
    EventType,
    JournalEntry,
    Order,
    OrderStatus,
    Position,
    Trade,
)

logger = logging.getLogger(__name__)

# --- 订单状态机（6 态） ---

LEGAL_TRANSITIONS: dict[OrderStatus, frozenset[OrderStatus]] = {
    OrderStatus.CREATED: frozenset({OrderStatus.SUBMITTED}),
    OrderStatus.SUBMITTED: frozenset({
        OrderStatus.PARTIAL,
        OrderStatus.FILLED,
        OrderStatus.CANCELLED,
        OrderStatus.REJECTED,
    }),
    OrderStatus.PARTIAL: frozenset({OrderStatus.FILLED, OrderStatus.CANCELLED}),
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELLED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
}

TERMINAL_STATES: frozenset[OrderStatus] = frozenset({
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
})


class IllegalTransition(ValueError):
    """订单尝试非法状态迁移时抛出。"""


def transition(order: Order, new_status: OrderStatus, *, reason: str = "") -> None:
    """原地推进订单状态，非法迁移或终态再迁移抛出 :class:`IllegalTransition`。"""
    allowed = LEGAL_TRANSITIONS.get(order.status, frozenset())
    if new_status not in allowed:
        raise IllegalTransition(
            f"{order.order_id}: {order.status.value} -> {new_status.value} "
            f"is not a legal transition (allowed: "
            f"{sorted(s.value for s in allowed) or 'none'})"
        )
    order.status = new_status
    order.updated_at = datetime.utcnow()
    if reason:
        order.reject_reason = reason


# --- 资金流结算 ---


def freeze_cash(order: Order, account: Account, price: float | None = None) -> None:
    """下单前为买单冻结资金（未指定价格时取限价或参考价）。"""
    if order.direction != Direction.BUY:
        return
    ref = price if price is not None else (order.limit_price or order.ref_price)
    amount = ref * order.quantity
    if amount > account.cash:
        raise ValueError(
            f"{order.order_id}: insufficient cash to freeze {amount:.2f} "
            f"(available {account.cash:.2f})"
        )
    account.cash -= amount
    account.frozen_cash += amount
    order.frozen_cash += amount


def settle_fill(order: Order, account: Account, trade: Trade) -> None:
    """结算单笔成交：买单优先扣减冻结资金，不足部分从可用现金补足。

    不变量：所有校验先于状态变更，异常时冻结余额保持不变；终态订单禁止再成交。
    佣金超出初始冻结金额的部分从可用现金扣除，保证冻结余额永不为负；
    剩余冻结资金在订单进入终态时由 :func:`release_frozen` 退还。
    """
    if order.status in TERMINAL_STATES:
        raise ValueError(
            f"{order.order_id}: cannot settle fill on terminal order "
            f"(status={order.status.value})"
        )

    total_cost = trade.fill_price * trade.filled_qty + trade.commission

    if order.direction == Direction.BUY:
        from_frozen = min(total_cost, order.frozen_cash)
        from_cash = total_cost - from_frozen

        # 先校验后变更，防止异常时破坏冻结资金
        if from_cash > 1e-9 and account.cash < from_cash - 1e-6:
            raise ValueError(
                f"{order.order_id}: insufficient cash for settlement "
                f"(need {from_cash:.2f}, have {account.cash:.2f})"
            )

        # 校验通过后执行原子更新
        order.frozen_cash -= from_frozen
        account.frozen_cash -= from_frozen
        if from_cash > 1e-9:
            account.cash -= from_cash
    else:  # 卖出
        account.cash += trade.fill_price * trade.filled_qty - trade.commission

    account.total_commission += trade.commission
    order.filled_qty += trade.filled_qty


def release_frozen(order: Order, account: Account) -> None:
    """订单进入终态（FILLED / CANCELLED / REJECTED）后退还剩余冻结资金。"""
    if order.status not in TERMINAL_STATES:
        raise ValueError(
            f"{order.order_id}: release_frozen requires terminal state, "
            f"got {order.status.value}"
        )
    refund = order.frozen_cash
    if refund < 0:
        raise ValueError(
            f"{order.order_id}: negative frozen balance ({refund:.2f})"
        )
    account.cash += refund
    account.frozen_cash -= refund
    order.frozen_cash = 0.0


# --- 事件日志（每日追加写 Parquet） ---

#: 需持久化至 Parquet 的事件类型（排除 BAR 行情与仅日终快照的 ACCOUNT_UPDATE）
PERSIST_TYPES: frozenset[EventType] = frozenset({
    EventType.SESSION_START,
    EventType.SESSION_END,
    EventType.DAY_START,
    EventType.DAY_END,
    EventType.SIGNAL,
    EventType.PORTFOLIO,
    EventType.ORDER_CREATED,
    EventType.ORDER_REJECTED,
    EventType.ORDER_FILLED,
    EventType.ORDER_CANCELLED,
    EventType.TRADE,
    EventType.POSITION_UPDATE,
})


class Journal:
    """基于每日 ``{YYYY-MM-DD}.parquet`` 的追加写事件日志。"""

    FLUSH_SIZE = 256

    def __init__(self, root_dir: str | Path, flush_size: int | None = None):
        self._root = Path(root_dir)
        self._root.mkdir(parents=True, exist_ok=True)
        self._buffer: list[dict] = []
        self._flush_size = flush_size or self.FLUSH_SIZE

    def log(self, entry: JournalEntry) -> None:
        """缓存需持久化的事件，达到批量阈值时自动刷盘。"""
        if entry.event_type not in PERSIST_TYPES:
            return
        self._buffer.append(self._entry_to_row(entry))
        if len(self._buffer) >= self._flush_size:
            self.flush()

    def flush(self) -> None:
        """将缓冲事件按日期分组追加写入对应 Parquet 文件。"""
        if not self._buffer:
            return
        import pandas as pd

        # 按日期分组，保证每日日志文件自包含
        by_date: dict[str, list[dict]] = {}
        for row in self._buffer:
            d = row["_date"]
            by_date.setdefault(d, []).append(row)

        for day_str, rows in by_date.items():
            for r in rows:
                r.pop("_date", None)
            df_new = pd.DataFrame(rows)
            path = self._root / f"{day_str}.parquet"
            if path.exists():
                df_old = pd.read_parquet(path)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_all = df_new
            df_all.to_parquet(path, index=False)

        self._buffer.clear()

    def read_day(self, day: str | date):
        """读取指定日期的日志 DataFrame，文件不存在时返回空表。"""
        import pandas as pd

        day_str = str(day)
        path = self._root / f"{day_str}.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    @property
    def buffered_count(self) -> int:
        return len(self._buffer)

    @staticmethod
    def _entry_to_row(entry: JournalEntry) -> dict:
        ts = entry.timestamp
        return {
            "timestamp": ts.isoformat(),
            "event_type": entry.event_type.value,
            "portfolio_id": entry.portfolio_id,
            "payload": _safe_json(entry.payload),
            "_date": ts.strftime("%Y-%m-%d"),
        }


def _safe_json(obj) -> str:
    """尽力将 payload 序列化为 JSON 字符串。"""
    import json

    def _default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        if hasattr(o, "value"):  # 枚举类型
            return o.value
        if hasattr(o, "__dict__"):
            return {k: v for k, v in o.__dict__.items() if not k.startswith("_")}
        return str(o)

    return json.dumps(obj, default=_default, ensure_ascii=False)


# --- 内存账本：账户与持仓记账 ---


class SimAccount:
    """模拟账户账本：独立追踪净值（NAV）与持仓。

    ``nav`` 随费用与盯市盈亏增量独立更新，而非由组件求和派生，
    因此恒等式 ``cash + frozen + Σ market_value == nav`` 可用于交叉校验结算一致性。
    """

    def __init__(self, cash: float, account_id: str = "sim"):
        self._account = Account(
            account_id=account_id,
            total_asset=cash,
            cash=cash,
        )
        self._positions: dict[str, Position] = {}  # ticker -> Position
        self._nav = cash  # 独立追踪的运行净值

    @property
    def cash(self) -> float:
        return self._account.cash

    @property
    def frozen_cash(self) -> float:
        return self._account.frozen_cash

    @property
    def market_value(self) -> float:
        return sum(p.market_value for p in self._positions.values())

    @property
    def nav(self) -> float:
        """独立追踪的账本净值（非 cash + market_value 派生）。"""
        return self._nav

    @property
    def account(self) -> Account:
        return self._account

    @property
    def positions(self) -> dict[str, Position]:
        return dict(self._positions)

    def mark_to_market(self, market_value: float) -> None:
        """更新合成持仓市值并同步调整追踪净值。"""
        if "__synthetic__" not in self._positions:
            self._positions["__synthetic__"] = Position(
                portfolio_id="sim",
                ticker="__synthetic__",
                quantity=0,
                avg_cost=0.0,
            )
        old_mv = self._positions["__synthetic__"].market_value
        self._positions["__synthetic__"].market_value = market_value
        self._nav += market_value - old_mv

    def update_position(
        self, ticker: str, quantity: int, avg_cost: float,
        market_value: float = 0.0,
    ) -> None:
        """创建或更新持仓，并按市值变化量同步调整追踪净值。"""
        old_mv = self._positions[ticker].market_value if ticker in self._positions else 0.0
        self._positions[ticker] = Position(
            portfolio_id=self._account.account_id,
            ticker=ticker,
            quantity=quantity,
            avg_cost=avg_cost,
            market_value=market_value,
        )
        self._nav += market_value - old_mv

    def settle_fill(self, order: Order, trade: Trade) -> None:
        """结算成交并更新持仓：按现有持仓隐含盯市价增减市值，成交价差计入已实现盈亏。"""
        settle_fill(order, self._account, trade)
        self._nav -= trade.commission

        ticker = trade.ticker
        fill_value = trade.fill_price * trade.filled_qty

        if order.direction == Direction.BUY:
            if ticker in self._positions:
                pos = self._positions[ticker]
                mark_price = (
                    pos.market_value / pos.quantity if pos.quantity > 0
                    else trade.fill_price
                )
                total_qty = pos.quantity + trade.filled_qty
                total_cost = pos.avg_cost * pos.quantity + fill_value
                pos.quantity = total_qty
                pos.avg_cost = total_cost / total_qty if total_qty else 0.0
                # 新增股数按原盯市价入账，成交价与盯市价差额计入即时盈亏
                pos.market_value += mark_price * trade.filled_qty
                self._nav += (mark_price - trade.fill_price) * trade.filled_qty
            else:
                # 新建仓位：初始盯市价等于成交价，无即时盈亏
                self._positions[ticker] = Position(
                    portfolio_id=trade.portfolio_id,
                    ticker=ticker,
                    quantity=trade.filled_qty,
                    avg_cost=trade.fill_price,
                    market_value=fill_value,
                )
        else:  # 卖出
            if ticker in self._positions:
                pos = self._positions[ticker]
                mark_price = (
                    pos.market_value / pos.quantity if pos.quantity > 0
                    else trade.fill_price
                )
                # 按盯市价等比例扣减市值，成交价差计入已实现盈亏
                pos.market_value -= mark_price * trade.filled_qty
                pos.quantity -= trade.filled_qty
                self._nav += (trade.fill_price - mark_price) * trade.filled_qty
                if pos.quantity <= 0:
                    # 清仓前吸收浮点残差
                    self._nav -= pos.market_value
                    del self._positions[ticker]

    def release_frozen(self, order: Order) -> None:
        """释放订单剩余冻结资金（内部划转，不改变净值）。"""
        release_frozen(order, self._account)

    def check_identity(self, atol: float = 1e-6) -> bool:
        """校验账本恒等式 ``cash + frozen + Σ market_value == nav`` 是否成立。"""
        computed = self._account.cash + self._account.frozen_cash + self.market_value
        return abs(computed - self._nav) < atol
