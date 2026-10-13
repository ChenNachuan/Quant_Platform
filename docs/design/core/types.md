# types

> 域：core · 对应代码 core/types.py · 来源 §23 / §68.4（archive/OPEN_QUESTIONS.md）

## 职责与边界

公共 dataclass 与枚举的**归属索引**：跨域共用的类型在这里登记一句职责与去处，字段级定义留在各细节模块，不在此复制。`types.py` 只承载真正跨域复用的定义与命名约定。

边界：本文件是索引，不是 schema 文档——字段清单以各细节模块为权威。

## 接口签名

| 类型 | 一句话职责 | 字段级权威 |
|---|---|---|
| `Order` | 下单意图 + 状态（CREATED→SUBMITTED→PARTIAL→FILLED/CANCELLED/REJECTED） | [../execution/ledger.md](../execution/ledger.md) |
| `Trade` | 单笔实际成交（Order:Trade = 1:N） | [../execution/ledger.md](../execution/ledger.md) |
| `Position` | 单票持仓（数量/均价/市值/盈亏） | [../execution/ledger.md](../execution/ledger.md) |
| `Account` | 账户资金（cash/冻结/总资产/累计费用） | [../execution/ledger.md](../execution/ledger.md) |
| `JournalEntry` | 事件流记录（append-only，账本本体） | [../execution/ledger.md](../execution/ledger.md) |
| `AlphaSignal` | 截面分数 DataFrame（信号唯一形态） | [../research/alpha_signal.md](../research/alpha_signal.md) |
| `TargetPortfolio` | ticker → 目标权重/金额 | [../execution/portfolio_construction.md](../execution/portfolio_construction.md) |
| `Direction` / `OrderStatus` / `PriceType` | 账本三枚举 | [../execution/ledger.md](../execution/ledger.md) |
| `ASSET_RULES` 相关类型 | 多资产规则值 | [asset_rules.md](asset_rules.md) |
| 配置 dataclass（BacktestConfig 等） | 加载后的类型载体 | [config.md](config.md) |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 自定义规范模型（5 dataclass + 枚举，不依赖引擎类型） | 以 vnpy 为准（OrderData 耦合 Gateway、缺 CREATED/ref_price/slippage、Position 是柜台多空视角）；以 backtrader 为准（无 REJECTED/PARTIAL 细分、无资金冻结）；先定事件流再定 schema（假性解耦） | 字段不定后面每步都空转；对拍和分析需要引擎中立字段 |
| Order / Trade 分表（1:N） | 合并成一张表 | Order 跟踪意图+状态，Trade 跟踪每笔成交；回测里 N 通常=1 也要保留结构，否则上实盘要改模型 |
| 事件流（Journal）是本体，快照是派生 | 只存快照 | 快照可从事件流 replay 重建（公司 Replay 思路）；回测里 journal 即回测日志，实盘里是审计日志 |
| 所有 Order/Trade/Position 带 `portfolio_id` | 第一版不预留 | Account→Portfolio→Position 两层结构预留，加策略不改结构 |
| 费用字段分属不同模型 | 混在一个费用字段 | 手续费/滑点在 Trade、冻结资金在 Order、保证金在 Position |

## 实现要点

归属约定：`core/types.py` 放跨域引用的类型定义本体；各域模块放以自己为权威的 schema 文档与构造逻辑。状态机（6 态）与费用归属见 ../execution/ledger.md，不在这里重复。

枚举三件（`Direction` BUY/SELL、`OrderStatus` 6 态、`PriceType` MARKET/LIMIT）随账本四件套同放。

## 实施时定稿项

- 无——§23 未留实施级开放条目；字段级维护归 ../execution/ledger.md
- `BarData`（行情帧）在 §23.2 一并定义（datetime/ticker/OHLCV/pre_close/adjfactor + suspend/limit_up/limit_down 可交易性标记），长表口径归 [../data/data_api.md](../data/data_api.md)
