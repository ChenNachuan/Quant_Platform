"""Cross-domain shared type definitions.

Pure definitions, no IO (red line 3).  Field-level authority lives in the
detail modules referenced from each type's docstring; this module carries the
actual definitions for cross-domain reuse.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum, IntEnum, IntFlag


class AssetType(str, Enum):
    """Closed asset-type enumeration.  Keys ``ASSET_RULES``.

    ``equity`` is Qlib residue in ``data_api`` and is intentionally absent
    (ROADMAP gap #12).  Table names (``bars_daily`` etc.) are table names,
    not ``asset_type`` values.
    """

    STOCK = "stock"
    ETF = "etf"
    FUND = "fund"  # LOF and other listed funds, stamp_tax False
    BOND = "bond"  # V2 reserved
    FUTURES = "futures"  # V3 reserved
    OPTION = "option"  # V4 reserved


# Single merge point for the future data_api equity mapping (gap #12).
EQUITY_TYPES: set[AssetType] = {AssetType.STOCK, AssetType.ETF, AssetType.FUND}


class QualityFlag(IntFlag):
    """Row-level quality bitmask (int64 at storage layer).  ``CLEAN = 0``.

    Bits per design/data/storage_schema.md 47.15.  Carries no severity —
    see :class:`QualitySeverity`.
    """

    CLEAN = 0
    C02_COVERAGE = 1 << 0  # C02: coverage insufficient
    C05_NULL_RATE = 1 << 1  # C05: null-rate high
    L04_BEYOND_LIMIT = 1 << 2  # L04: beyond price limit
    L07_ADJFACTOR_RANGE = 1 << 3  # L07: adjfactor range abnormal
    K01_OVERNIGHT_GAP = 1 << 4  # K01: overnight gap too large


class QualitySeverity(IntEnum):
    """Severity scale (int8 at storage layer): LOW=0 / MEDIUM=1 / HIGH=2.

    The "INT8 (0/1/2)" row in 55.2 is this scale, not a competing width for
    :class:`QualityFlag`.  Declared beside it so the two are never confused.
    """

    LOW = 0
    MEDIUM = 1
    HIGH = 2


class Direction(Enum):
    """Trade direction.  Field authority: ``execution/ledger.md``."""

    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    """Order lifecycle state.

    CREATED -> SUBMITTED -> PARTIAL -> FILLED / CANCELLED / REJECTED.
    Field authority: ``execution/ledger.md``.
    """

    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class PriceType(Enum):
    """Order price type.  Field authority: ``execution/ledger.md``."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class EventType(Enum):
    """Journal event type — closed 14-member enum (29.3 + 32 + 34)."""

    # Time events (engine main loop)
    SESSION_START = "SESSION_START"
    SESSION_END = "SESSION_END"
    DAY_START = "DAY_START"
    DAY_END = "DAY_END"
    BAR = "BAR"
    # Execution events
    ORDER_CREATED = "ORDER_CREATED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_FILLED = "ORDER_FILLED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    TRADE = "TRADE"
    # State events (ledger)
    POSITION_UPDATE = "POSITION_UPDATE"
    ACCOUNT_UPDATE = "ACCOUNT_UPDATE"
    # 32 / 34 additions
    SIGNAL = "SIGNAL"
    PORTFOLIO = "PORTFOLIO"


@dataclass
class Order:
    """Order intent + status.  Field authority: ``execution/ledger.md``."""

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
    """Single fill.  Order:Trade = 1:N.  Field authority: ``execution/ledger.md``."""

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
    """Single-instrument position.  Field authority: ``execution/ledger.md``."""

    portfolio_id: str
    ticker: str
    quantity: int
    avg_cost: float
    market_value: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class Account:
    """Account cash state.  Field authority: ``execution/ledger.md``."""

    account_id: str
    total_asset: float
    cash: float
    frozen_cash: float = 0.0
    total_commission: float = 0.0
    total_pnl: float = 0.0


@dataclass(frozen=True)
class TargetPosition:
    """One target holding after constraint clipping and lot rounding.

    Field authority: ``execution/portfolio_construction.md`` (§17/§34); the
    body lives here per gap #1 ruling A.  Three units coexist: weight is the
    optimization space, quantity the execution unit (integer lots), notional
    for risk and reporting.  ``target_weight`` is recomputed from the rounded
    quantity — the pre-rounding weights are in ``TargetPortfolio``.
    """

    ticker: str
    target_weight: float
    target_quantity: int
    target_notional: float
    ref_price: float  # T-day close, weight<->shares conversion only
    side: str  # "LONG" / "FLAT"


@dataclass(frozen=True)
class TargetPortfolio:
    """Target book produced by ``PortfolioOptimizer.solve``.

    Field authority: ``execution/portfolio_construction.md`` (§17/§34); body
    here per gap #1 ruling A.  Positions hold the rounded book;
    ``pre_opt_weights`` keeps the pre-rounding weights for deviation analysis.
    """

    strategy_id: str
    trade_date: date  # signal day T
    exec_date: date  # execution day T+1
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
    """Append-only event-stream record.  Field authority: ``execution/ledger.md``."""

    timestamp: datetime
    event_type: EventType
    portfolio_id: str
    payload: dict = field(default_factory=dict)
