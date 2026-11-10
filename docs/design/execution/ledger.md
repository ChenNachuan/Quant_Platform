# ledger

> 域：execution · 对应代码 [execution/ledger.py](execution/ledger.py)
## 职责与边界

统一账本数据模型：五个规范 dataclass + 三枚举 + 订单状态机 + Journal 事件流。**不依赖任何引擎类型**，供 SimBroker/QmtBroker 共用。事件流是本体、快照是派生——回测里 Journal 就是回测日志，实盘里是审计日志 + 对拍数据源。
## 接口签名

```python
@dataclass
class Order:
    order_id: str            # 全局唯一（薄壳生成）
    portfolio_id: str
    datetime: datetime       # 策略产出意图的时间
    ticker: str
    direction: Direction     # BUY / SELL
    quantity: int            # 目标手数（已过手数取整）
    price_type: PriceType    # MARKET / LIMIT
    limit_price: float | None
    ref_price: float         # 策略看到的参考价（事后算滑点）
    status: OrderStatus      # CREATED→SUBMITTED→PARTIAL→FILLED/CANCELLED/REJECTED
    filled_qty: int; frozen_cash: float
    created_at: datetime; updated_at: datetime
    reject_reason: str

@dataclass
class Trade:
    trade_id: str; order_id: str   # Order:Trade = 1:N
    portfolio_id: str; datetime: datetime; ticker: str
    direction: Direction
    filled_qty: int; fill_price: float
    commission: float        # 含最低 5 元逻辑
    slippage: float          # fill_price − order.ref_price（事后计算）

@dataclass
class Position:
    portfolio_id: str; ticker: str
    quantity: int; avg_cost: float       # 均价含手续费摊入
    market_value: float; unrealized_pnl: float; realized_pnl: float

@dataclass
class Account:
    account_id: str
    total_asset: float       # cash + Σ position.market_value
    cash: float; frozen_cash: float
    total_commission: float; total_pnl: float

class Direction(Enum): BUY; SELL
class OrderStatus(Enum): CREATED; SUBMITTED; PARTIAL; FILLED; CANCELLED; REJECTED
class PriceType(Enum): MARKET; LIMIT

class EventType(Enum):   # 封闭枚举 14 个
    # 时间事件（引擎主循环产生）
    SESSION_START; SESSION_END; DAY_START; DAY_END; BAR
    # 交易事件（执行链产生）
    ORDER_CREATED; ORDER_REJECTED; ORDER_FILLED; ORDER_CANCELLED; TRADE
    # 状态事件（账本产生）
    POSITION_UPDATE; ACCOUNT_UPDATE
    # §32/§34 后补（信号含决策原因进 Journal 做归因；目标组合留痕）
    SIGNAL; PORTFOLIO

@dataclass
class JournalEntry:
    timestamp: datetime
    event_type: EventType    # 14 封闭枚举，不支持自定义
    portfolio_id: str
    payload: dict            # Order/Trade/Position/Account 序列化快照
```

**订单状态机（6 态）**：

```text
Created → Submitted → PartialFilled → Filled
                      ↳ Rejected（柜台拒单）
                      ↳ Cancelled（未成交撤单）
PartialFilled → Cancelled（部分成交后撤单，已成交部分保留）
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 自定义dataclass，不依赖引擎类型 | vnpy/backtrader 类型 | 类型耦合 Gateway 状态机不完整 |
| 6 态状态机，CREATED 不能省 | 4/5 态简化 | CREATED 是「策略想下但被拦」和「策略没下」的分界线 |
| Order:Trade = 1:N | 合并一张表 | Order 跟意图+状态，Trade 跟每笔实际成交；回测 N=1 也要保留结构，否则上实盘改结构 |
| 手续费/滑点记 Trade，冻结资金记 Order | 全记一处 | 滑点=fill_price−ref_price 是事后算的，不是事先参数 |
| 冻结逐步转支出：每笔 fill 把成交额+费用从 frozen 转出，终态退剩余 | 按笔比例释放 frozen | 多笔 fill 比例释放会算错、终态回补会扣穿现金 |
| Account → Portfolio → Position 两层 | 单账户平铺 | V1 一账户够，但所有单据带 portfolio_id 预留多策略 |
| 事件流 Journal（append-only Parquet 按日）是本体 | 只存快照 | 快照可从事件流重建（replay 还原任意时刻） |
| Journal 选择性持久化 | 全量存 | BAR 不存（行情表已有）；ACCOUNT 只记日终快照 |

## 实现要点

详见 [execution/ledger.py](execution/ledger.py)

核心约定:
- 资金流转:下单冻结→每笔fill从frozen转支出(BUY扣成本,SELL入cash)→终态退剩余frozen全额回 cash
- Journal 持久化:按日 Parquet,buffer攒满 flush。SIGNAL/PORTFOLIO/ORDER/TRADE/POSITION_UPDATE 持久化,BAR不存,ACCOUNT_UPDATE只记日终快照
- Journal 链:SIGNAL→PORTFOLIO → ORDER → TRADE → POSITION_UPDATE
- 费用字段归属:commission/slippage  Trade,frozen_cash E Order,margin E Position(V3期货)
- EventType 14枚举是账本内部 Journal的形态,不是对外事件总线
- Position/Account快照内存维护,crash后从Journal replay恢复
