# Quant Platform 架构总纲

> 状态：设计定稿，待实施
> 依据：§16–§69 讨论结论（[archive/OPEN_QUESTIONS.md](../archive/OPEN_QUESTIONS.md)）+ 旧架构重审（§68）+ 横切收口（§69）
> 最后更新：2026-10-10

这份文档是平台架构的**唯一权威总纲**：范式、边界、跨域契约、选型定稿。各域细节见 `design/<域>/` 下的模块文档（39 份，文件名与未来代码模块同名）；现行代码放置规则见 [PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md)。

旧版总纲（含四层时间模型 / EBGraph / ClickHouse / 三层 Signal）已按 §68 重审修正，原文存档于 [archive/ARCHITECTURE.md](../archive/ARCHITECTURE.md)，不再维护。

---

## 1. 北极星

**薄壳 + pandas 优先，对齐头部机构的分层范式，不照搬其重引擎实现。**

重构完成后最应该被显著改善的一件事：**架构范式对齐**。研究迭代速度、工程可演进性、实盘落地能力是范式带来的结果，不是独立目标。

单人单机场景裁决原则（§20/§68/§69）：公司体系是主干参照，但凡为 tick 级性能、多策略并发、多人协作而生的设施（事件驱动内核、ClickHouse、分布式追踪、配置中心、C++ Traits）一律不引入——个人平台的规模使这些问题天然消失。

## 2. 五域骨架

系统按**运营域**切分，不按研究流水线阶段切分。

```text
core/         共享工具（config / registry / logging / types / asset_rules）
data/         数据接入与存储（SourceAdapter / DQC / 存储 schema / DataAPI）
research/     投研（Factor / DAG / 后处理 / 合成 / 评价 / alphaType）
execution/    执行（组合构建 / 订单 / 风控 / 费用 / 账本 / 柜台）
ops/          运维（调度 / 监控 / 归因 / 告警）
```

### 2.1 依赖方向

```text
research/ ──┐
            ├──→ core/ ←──┐
execution/ ─┘             │
data/     ──→（提供数据访问，不反向依赖）
ops/      ──→（观测所有域，不被依赖）
```

**硬规则**：`core/` 不 import 任何其他域；`research/` 与 `execution/` 互不依赖；`ops/` 只读观测，不被依赖。

### 2.2 core/ 是共享工具，不是引擎（§68.4）

旧设计的 `kernel/`（事件循环 / 时间模型 / 调度）**废弃**——内核的三件事各有去向：

| 旧 kernel 职责 | 去向 |
|---|---|
| 时间推进 | 回测脚本 for-loop（见 §3.1） |
| 事件分发 | 不需要（月频无实时事件流） |
| Signal 类 | AlphaSignal（见 §3.2） |
| 数据接口 | DataAPI / load_* 函数（[data/data_api.md](data/data_api.md)） |

`core/` 只剩真正的共享工具：`config.py`、`registry.py`、`logging.py`、`types.py`、`asset_rules.py`。**无引擎、无事件循环、无时间模型。**

## 3. 主链契约

### 3.1 时间推进：for-loop（§68.2）

四层时间模型（starfleet → EBReplay → EBClock → EBTimer）**废弃**。月频/日频下：

```python
for date in rebalance_dates():
    ...
```

tick 对齐、事件触发推进、时钟单步都不需要——"计算量与数据量解耦"在月频（全 A 截面 <100ms）天然消失。backtrader / vnpy 的事件驱动框架同样不采用。

**Service 三答**（§35.3–35.5）：公司 6 个 Service 在薄壳里就是 `run()` 里的 **6 行顺序调用**（时钟 = for 循环、行情 = `get_bars`、信号 = `on_rebalance` 返回值、组合 = `optimizer.solve`、风控 = `pre_check`）；**注册与发现 = 构造函数注入**（Python 对象引用，"发现"就是构造时传进去）；**启动顺序 = 代码书写顺序**（初始化全同步，无异步依赖）。详见 [core/registry.md](core/registry.md)。

### 3.2 三步主链（§68.5）

旧五层叙事（Signal 三层 → Portfolio → Utrade → Themis → TB）坍缩为三步：

```text
AlphaSignal → Portfolio → Execution
                            └── broker.submit_orders() 内部封装：风控 + 撮合 + 记账
```

| 旧契约 | 裁决 | 替代 |
|---|---|---|
| Signal 三层（Blueshift/USignal/Tick） | 坍缩为一层 | [research/alpha_signal.md](research/alpha_signal.md) |
| Portfolio 层 | 自研 | [execution/portfolio_construction.md](execution/portfolio_construction.md) |
| Utrade / Themis / TB | 坍缩 | `submit_orders()` 的内部三步，不是三个模块（[execution/broker.md](execution/broker.md)） |

**接口边界从"产品"移到"函数"**。执行域只认目标持仓（TargetPortfolio），订单由执行域自己算。薄壳的本质（§25.1）：**是"翻译官"，不是"总管"**——把各框架方言翻译成薄壳 dataclass，不替代框架做它擅长的事。

### 3.3 因子组织：Factor ABC + FactorDAG（§68.3）

EBGraph / EventBrick 事件订阅图**废弃**，静态拓扑 DAG 保留（事件驱动思想保留"依赖图"，丢掉"事件订阅"）：

```text
Factor(compute) → FactorDAG(拓扑) → PostProcessor → per-factor-one-table
  → FactorCombiner → AlphaSignal   =  "DataFrame 流水线"
```

月频三理由：无实时事件流、依赖静态、DataFrame 天然批量。EB 四元素的薄壳等价物（§30）：`emit()` / 硬编码主循环 + 可选 `add_listener()` / `engine.run()` / session 生命周期 + 策略 checkpoint。详见 [research/factor_interface.md](research/factor_interface.md)、[research/factor_runtime.md](research/factor_runtime.md)。

**评价与回测的层级**（§19.8）：alphalens = 回测前**筛选层门控**（快速 IC/分组，不建模成本/滑点/涨跌停），不是验证层；与正式回测结论矛盾时**以回测为准**。

## 4. 选型定稿表

| 维度 | 选型 | 否决项（一句话理由） | 详情 |
|---|---|---|---|
| 回测引擎 | pandas 自研 <300 行 + SimBroker/QmtBroker | backtrader/vnpy（依赖重、事件驱动不需要）；VectorBT 留 V2 备选 | [execution/broker.md](execution/broker.md) |
| 存储 | Parquet + DuckDB | ClickHouse（单机 <10GB，独立部署违反零外部依赖） | [data/storage_schema.md](data/storage_schema.md) |
| 数据接入 | SourceAdapter 四方法 + watermark | 公司 DataBus/Udata 容器层（多服务需求） | [data/source_adapter.md](data/source_adapter.md) |
| 因子评价 | alphalens-reloaded 0.4.6 | alphalens 原版（§28.2 选 reloaded 0.4.6） | [research/factor_evaluation.md](research/factor_evaluation.md) |
| 配置 | TOML 三层 + .env + git | YAML（on→True 陷阱）；XML（C++ 生态惯例）；配置中心（单机不需要） | [core/config.md](core/config.md) |
| 日志 | Python logging + JSONL | ELK/Loki、分布式追踪（单进程 grep/jq 足够） | [core/logging.md](core/logging.md) |
| 测试 | pytest 三层 + hypothesis + 黄金样本 | V1 上 CI（本地三档耗时预算先行） | [core/testing.md](core/testing.md) |
| 多资产 | 配置区分 + ASSET_RULES + execution 层 | C++ Traits 编译期多态（tick 级性能需求，月频不需要） | [core/asset_rules.md](core/asset_rules.md) |
| 调度 | APScheduler + job_runs.jsonl | 分布式调度；crontab（无失败重试/无状态记录）——进程内/零部署/CronTrigger 直观（§67.1） | [ops/scheduler.md](ops/scheduler.md) |
| 时间推进 | for-loop | 事件驱动内核（月频无事件流） | §3.1 |
| 因子组织 | Factor ABC + FactorDAG | EBGraph/EventBrick（动态订阅 vs 静态拓扑） | §3.3 |

**框架存储偏好**（§68.6）：alphalens（DataFrame/Parquet）**直接兼容**；VectorBT（DataFrame）兼容，V2 备选引擎；backtrader/vnpy（自有数据格式）不用；Qlib（二进制）不用。三套（VectorBT/ZVT/Qlib）**直接归档、不做对照组**（§28.5，修正 §20.7/§26.8 的"降为对照组"表述）。

## 5. 跨域契约一览

各契约的字段级定义在对应模块文档：

| 契约 | 形态 | 文档 |
|---|---|---|
| 行情长表 `load_bars` | date/ticker/OHLCV/amount/[adjfactor] | [data/data_api.md](data/data_api.md) |
| AlphaSignal | 截面分数 DataFrame | [research/alpha_signal.md](research/alpha_signal.md) |
| TargetPortfolio | ticker → 目标权重/金额 | [execution/portfolio_construction.md](execution/portfolio_construction.md) |
| 账本 dataclass | Order / Trade / Position / Account / JournalEntry | [execution/ledger.md](execution/ledger.md) |
| config_snapshot | `path@hash` 钉进运行记录 | [core/config.md](core/config.md)、[core/logging.md](core/logging.md) |
| 运行记录 | logs/runs/ 三 schema（jobs / backtests / live） | [core/logging.md](core/logging.md) |
| 资产规则 | ASSET_RULES 查表 | [core/asset_rules.md](core/asset_rules.md) |

## 6. 黄金样本与验证（§68.7）

**平台级黄金样本 = 等权 top-5 ETF 动量**（教程 §2 双重动量变体，≤30 行 akshare 可重现）。五步验证法：手算 Excel → pandas → SimBroker 全链路 → 扣费对拍 → 五年基线落盘。预期区间：年化 10–15% / 夏普 0.8–1.2 / MDD 15–25%；超范围排查表（年化 >20% 查未来函数、夏普 >2 查前视、MDD <10% 查漏极端行情段、费用 <1% 查 FeeModel 漏计）。详见 [core/testing.md](core/testing.md)。

旧 `baseline_spec.py`（sharpe 16 不可信）**保留作期货 ALSTM 策略自身回归基准，不再是平台级黄金样本**——两者不混用。

## 7. 迁移阶段与红线

| 阶段 | 资产 | 备注 |
|---|---|---|
| V1 | 股票 + ETF | 主战场（A 股特殊机制：T+1 / 涨跌停 / 印花税 ETF 免） |
| V2 | 可转债 | T+0、无印花税——规则表加一行 |
| V3 | 期货 | 保证金 + 逐日盯市——新账本，不复用 SimAccount |
| V4+ | 期权 | 不推荐散户，最后做 |

实施红线：

1. **不要写事件驱动内核**。for-loop 就是时间模型。
2. **不要给因子发明事件订阅机制**。Factor ABC + FactorDAG 即可。
3. **不要让 `core/` import 任何域**。它谁都不依赖。
4. **不要让回测和实盘走不同代码路径**。差异只注入在 Broker 实现（Sim/Qmt）。
5. **不要把一次性实验放进注册表**。归档区永不进。
6. **不要混用两套黄金样本**。平台级 = 等权 top-5 ETF 动量。
7. **不要在迁移期同时改新旧两套的同一实现**。一次只动一边。
8. **`.env` 不入 git、不打印**（敏感信息红线，§69）。

被本纲吸收、不再单独立档的讨论节（原文见归档）：**§19** 回测-实盘一致性与薄壳契约、**§20** 架构与选型复审、**§28** 六套引擎收敛与三件套版本、**§31** DataBus 不需要（直连替代）→ 并入本纲第 3–4 节；**§30** EventBrick 四元素等价物（emit / 硬编码主循环 / engine.run() / session+checkpoint）→ 本纲 §3.3、[research/factor_interface.md](research/factor_interface.md)、[data/l2_data.md](data/l2_data.md)；**§54–§56** udata 不引入 → [data/storage_schema.md](data/storage_schema.md) 概念映射；**§29** 事件系统中的 14 EventType（§29.3 12 + §32 SIGNAL + §34 PORTFOLIO）与 Journal → [execution/ledger.md](execution/ledger.md)；**§25** 薄壳边界与运行形态（"翻译官"）→ [execution/broker.md](execution/broker.md)、[execution/ledger.md](execution/ledger.md)、[data/data_api.md](data/data_api.md)；**§26** 截面回测路径（cs_portfolio_sim）→ [research/cross_sectional_backtest.md](research/cross_sectional_backtest.md)。

## 8. 文档地图

```text
design/
├── ARCHITECTURE.md     ← 本文件（总纲）
├── core/    config · logging · testing · asset_rules · registry · types
├── data/    source_adapter · ingestion · dqc_rules · dqc_severity · dqc_runtime
│            quarantine_raw · calendar_adjustment · data_revision · storage_schema
│            storage_ops · data_api · l2_data
├── research/ factor_interface · factor_runtime · post_processor · factor_storage
│            factor_governance · factor_evaluation · factor_combiner · alpha_signal
│            ml_signal · alpha_type · cross_sectional_backtest
├── execution/ portfolio_construction · order_generation · risk_control · fee_model
│            ledger · broker
└── ops/     scheduler · monitoring · reporting · alerting
```

- 讨论史（83 问 + 54 节结论，含被否决的选项与理由）：[archive/OPEN_QUESTIONS.md](../archive/OPEN_QUESTIONS.md)
- 现行代码放置规则：[PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md)
