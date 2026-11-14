"""跨业务域共享类型定义（纯定义无 I/O，字段权威定义见各类型注释引用的设计文档）。"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum, IntEnum, IntFlag


class AssetType(str, Enum):
    """封闭资产类型枚举（对应 ``ASSET_RULES`` 键）；不含通用 ``equity`` 及存储表名。"""

    STOCK = "stock"
    ETF = "etf"
    FUND = "fund"  # LOF 等场内基金，免印花税
    BOND = "bond"  # V2 预留
    FUTURES = "futures"  # V3 预留
    OPTION = "option"  # V4 预留


# 权益类资产集合，用于 data_api 等层统一归并映射
EQUITY_TYPES: set[AssetType] = {AssetType.STOCK, AssetType.ETF, AssetType.FUND}


class QualityFlag(IntFlag):
    """行级数据质量位掩码（存储层为 int64，``CLEAN = 0``）；不含严重度分级（见 :class:`QualitySeverity`）。"""

    CLEAN = 0
    C02_COVERAGE = 1 << 0  # C02：覆盖率不足
    C05_NULL_RATE = 1 << 1  # C05：空值率过高
    L04_BEYOND_LIMIT = 1 << 2  # L04：超出涨跌幅限制
    L07_ADJFACTOR_RANGE = 1 << 3  # L07：复权因子区间异常
    K01_OVERNIGHT_GAP = 1 << 4  # K01：隔夜跳空幅度过大


class QualitySeverity(IntEnum):
    """质量异常严重度枚举（存储层为 int8）：LOW=0 / MEDIUM=1 / HIGH=2。"""

    LOW = 0
    MEDIUM = 1
    HIGH = 2


class Direction(Enum):
    """交易方向枚举（权威定义：``execution/ledger.md``）。"""

    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    """订单生命周期状态（权威定义：``execution/ledger.md``）。

    流转路径：CREATED -> SUBMITTED -> PARTIAL -> FILLED / CANCELLED / REJECTED。
    """

    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class PriceType(Enum):
    """订单价格类型枚举（权威定义：``execution/ledger.md``）。"""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class EventType(Enum):
    """流水账事件类型（14 项封闭枚举）。"""

    # 时间事件（引擎主循环）
    SESSION_START = "SESSION_START"
    SESSION_END = "SESSION_END"
    DAY_START = "DAY_START"
    DAY_END = "DAY_END"
    BAR = "BAR"
    # 执行事件
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    TRADE = "TRADE"
    # 状态事件（账本）
    POSITION_UPDATE = "POSITION_UPDATE"
    ACCOUNT_UPDATE = "ACCOUNT_UPDATE"
    # 扩展信号与组合事件
    SIGNAL = "SIGNAL"
    PORTFOLIO = "PORTFOLIO"


@dataclass
class Order:
    """订单意图与状态（权威定义：``execution/ledger.md``）。"""

    order_id: str
    portfolio_id: str
    datetime: datetime
    ticker: str
    direction: Direction
    quantity: int
    price_type: PriceType
    limit_price: float | None = None
    ref_price: float = 0.0
    status: OrderStatus = OrderStatus.CREATED
    filled_qty: int = 0
    frozen_cash: float = 0.0
    created_at: datetime | None = None
    updated_at: datetime | None = None
    reject_reason: str = ""


@dataclass
class Trade:
    """单笔成交记录，与 Order 为 1:N 关系（权威定义：``execution/ledger.md``）。"""

    trade_id: str
    order_id: str
    portfolio_id: str
    datetime: datetime
    ticker: str
    direction: Direction
    filled_qty: int
    fill_price: float
    commission: float = 0.0
    slippage: float = 0.0


@dataclass
class Position:
    """单标的持仓状态（权威定义：``execution/ledger.md``）。"""

    portfolio_id: str
    ticker: str
    quantity: int
    avg_cost: float
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class Account:
    """账户资金状态（权威定义：``execution/ledger.md``）。"""

    account_id: str
    total_asset: float
    cash: float
    frozen_cash: float = 0.0
    total_commission: float = 0.0
    total_pnl: float = 0.0


@dataclass(frozen=True)
class TargetPosition:
    """经约束裁剪与整手取整后的单标的目标持仓（权威定义：``execution/portfolio_construction.md``）。

    同时记录权重（优化空间）、整数股数（执行单位）与名义金额（风控与报表）；
    ``target_weight`` 为按整手取整后重算的权重（取整前权重见 ``TargetPortfolio``）。
    """

    ticker: str
    target_weight: float
    target_quantity: int
    target_notional: float
    ref_price: float  # T 日收盘价，仅用于权重与股数换算
    side: str  # "LONG" / "FLAT"


@dataclass(frozen=True)
class TargetPortfolio:
    """``PortfolioOptimizer.solve`` 生成的目标组合（权威定义：``execution/portfolio_construction.md``）。

    ``positions`` 为取整后目标持仓，``pre_opt_weights`` 保留取整前权重用于偏差分析。
    """

    strategy_id: str
    trade_date: date  # 信号日 T
    exec_date: date  # 执行日 T+1
    create_time: datetime
    positions: dict[str, TargetPosition]
    total_weight: float
    num_holdings: int
    cash_weight: float
    estimated_turnover: float
    solver_status: str  # "optimal" / "suboptimal" / "infeasible"
    constraints_applied: list[str]
    pre_opt_weights: dict[str, float] | None


@dataclass
class JournalEntry:
    """只追加事件流记录（权威定义：``execution/ledger.md``）。"""

    timestamp: datetime
    event_type: EventType
    portfolio_id: str
    payload: dict = field(default_factory=dict)
