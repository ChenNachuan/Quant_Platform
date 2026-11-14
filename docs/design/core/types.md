# types

> 域：core · 对应代码 [core/types.py](../../../core/types.py)

## 职责与边界

公共 dataclass 与枚举的**归属索引**：跨域共用的类型在这里登记一句职责与去处，字段级定义留在各细节模块，不在此复制。`types.py` 只承载真正跨域复用的定义与命名约定。

## 接口签名

| 类型 | 职责 | 字段级权威 |
|---|---|---|
| `Order` | 下单意图 + 状态 | [../execution/ledger.md](../execution/ledger.md) |
| `Trade` | 单笔实际成交（Order:Trade = 1:N） | [../execution/ledger.md](../execution/ledger.md) |
| `Position` | 单票持仓（数量/均价/市值/盈亏） | [../execution/ledger.md](../execution/ledger.md) |
| `Account` | 账户资金（cash/冻结/总资产/累计费用） | [../execution/ledger.md](../execution/ledger.md) |
| `JournalEntry` | 事件流记录（append-only，账本本体） | [../execution/ledger.md](../execution/ledger.md) |
| `AlphaSignal` | 截面分数 DataFrame | [../research/alpha_signal.md](../research/alpha_signal.md) |
| `TargetPortfolio` | ticker → 目标权重/金额 | [../execution/portfolio_construction.md](../execution/portfolio_construction.md) |
| `Direction` / `OrderStatus` / `PriceType` | 账本三枚举 | [../execution/ledger.md](../execution/ledger.md) |
| `ASSET_RULES` 相关类型 | 多资产规则值 | [asset_rules.md](asset_rules.md) |
| 配置 dataclass（BacktestConfig 等） | 加载后的类型载体 | [config.md](config.md) |
| `QualityFlag` | DQC行级位掩码枚举 | [../data/dqc_severity.md](../data/dqc_severity.md) |

## 关键决策

| 决策 | 否决项 | 理由 |
|---|---|---|
| 自定义dataclass，不依赖引擎类型 | vnpy OrderData , backtrader Order | 字段需引擎中立；对拍和分析不耦合框架 |
| Order / Trade 分表（1:N） | 合并成一张表 | Order 跟踪意图+状态，Trade 跟踪每笔成交；回测里 N 通常=1 也要保留结构，否则上实盘要改模型 |
| 事件流（Journal）是本体，快照是派生 | 只存快照 | 快照可从事件流 replay 重建 |
| 所有 Order/Trade/Position 带 `portfolio_id` | 第一版不预留 | Account→Portfolio→Position 两层结构预留，加策略不改结构 |

## 实现要点

详见[core/types.py](../../../core/types.py)
归属约定：`core/types.py` 放跨域引用的类型定义本体；各域模块放以自己为权威的 schema 文档与构造逻辑。状态机（6 态）与费用归属见 ../execution/ledger.md，不在这里重复。
