# l2_data

> 域：data · 对应代码 data/l2_data.py · 来源 §33（archive/OPEN_QUESTIONS.md）

## 职责与边界

L2 / Tick 数据的**使用路径裁决**：只做「离线因子原料」，不做 tick 实时策略引擎。L2 入库（`data_platform/` 的 L2 表）→ 离线算因子 → 因子以日频/分钟频输出 → 既有 FactorPipeline（§27）→ alphalens → 策略。

```text
L2 Parquet → 离线因子计算脚本 → 因子值（日频/分钟频） → FactorPipeline
                                        ↓
                                  不经过事件系统
```

边界：L2 是因子的原材料，不是事件流的一部分——如同「每日成交量」是 BarData 的字段而不是事件。**80% 的 L2 使用场景走这条路**，零新增架构。

## 接口签名

路径 A（现在做）：

```text
L2 表（data_platform/；schema 已定 → §47：tick_futures_l2 Phase 2 预留，27 列 = bid/ask 5 档 20 列 + 7 基础）
  → 离线因子计算脚本（买卖价差因子 / 盘口不平衡因子 / 成交量分布因子…）
  → 因子值表（日频或分钟频，走 factor 存储规范）
  → FactorPipeline / alphalens
```

路径 B（**不做**）示意——若未来做做市/微观结构策略，需要的不是加两个事件，而是整个引擎的范式转换：

```python
# 仅示意，当前事件模型不加这两个枚举
class EventType(Enum):
    TICK         = auto()   # 逐笔/快照到达
    DEPTH_UPDATE = auto()   # 盘口(bid/ask 5档)变动
```

日频/分钟频 vs tick 级的量级差：

| 问题 | 日频/分钟频 | Tick 级 |
|---|---|---|
| 驱动频率 | 1–240 次/天 | 数万次/秒 |
| 单线程 Python | 完全够 | 不够，需要 C++/Rust |
| 策略接口 | `on_bar()` 够用 | 需要 `on_tick()` + 状态机 |
| 回测方式 | 批量加载 + 逐 bar 推进 | 需要事件回放引擎 |
| 延迟要求 | 不关心 | 微秒级 |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 现在只做路径 A：L2 = 离线因子原料 | 同时做 tick 实时路径 | 80% 场景已覆盖，且完全落在既有流水线内 |
| **不加 TICK / DEPTH_UPDATE 事件** | 在当前事件模型里扩枚举 | tick 驱动是另一个系统（独立 tick 级引擎），不是加两个事件 |
| 因子以日频/分钟频输出喂 FactorPipeline | L2 数据直接进策略 | 与「成交量是 BarData 字段」同构 |
| Tick 级引擎 = 独立系统（公司 Pysim 问题域） | 薄壳上硬长出来 | 微秒级延迟/C++/事件回放不属于单人单机薄壳 |
| Tick/L2 表 schema：§47 已定 27 列（Phase 2 预留），使用路径已定 | 先做 tick 实时路径再定 schema | 路径决定 schema 只需服务离线计算，压力小 |

呼应 §29.2「不选 D：照搬公司枚举」、§30.9「何时重新引入 EB」——事件驱动思想保留依赖图（FactorDAG），不引入事件订阅。

## 实现要点

```text
# 路径 A 的全部动作（零事件系统改动）
1. L2 数据入库 → data_platform/ 的 L2 表（raw→normalize→DQC 同既有流水线）
2. 离线因子计算脚本读 L2 表 → 算微观结构因子
3. 因子值按日频/分钟频落 factor 存储（per-factor-one-table）
4. FactorPipeline → alphalens 门控 → 策略消费（作为 BarData 附加列或独立因子表）
```

不需要：新事件类型、新时间模型、tick 回放引擎、`on_tick()` 策略接口。L2 数据量级上来后的 DQC/存储优化归 phase-2（见 [dqc_runtime.md](dqc_runtime.md)）。

## 实施时定稿项

- ~~Tick / L2 表的字段 schema~~ 已定 → §47（`tick_futures_l2` Phase 2 预留，27 列 = bid/ask 5 档 20 列 + 7 基础；pandera 展开见 [storage_schema.md](storage_schema.md)）（§33）
- L2 因子的计算脚本放哪：`research/` 还是 `scripts/data_processing/`（§33）
- `DataAPI` 是否预留 `get_l2_snapshot()` 签名（供离线因子计算调用，不进事件流）（§33）

## 使用路径登记

> 本节由 QUANT-53（M7 / S13-T3）新增：只登记 L2 五档/逐笔链路的入口落点与起用接入点，不实现任何代码。既有章节的定稿结论与红线表述不改写，「实施时定稿项」不代裁。

L2 五档/逐笔按既定使用路径（离线因子原料，不进事件流）分三个入口，分别落在 `data/` 的以下模块：

| 入口 | 落点模块（现存） | 登记说明 |
|---|---|---|
| 采集 | `data/source_adapter.py`（源适配）、`data/ingestion.py`（入库编排：raw→normalize→DQC 同既有流水线） | 归一化在 `data/normalizers/`，DQC 在 `data/dqc/`，原始落盘与异常隔离走 `data/raw.py`、`data/quarantine.py`。五档批量入库的专项转换脚本已在数据完成后删除（[future_level2_data_processing.md](../../data/future_level2_data_processing.md) §8），后续采集挂既有流水线入口 |
| 存储 | `data/storage_ops.py`（原子写）、`data/storage_schema.py`（`tick_futures_l2` 表 schema 已预留，列数口径见表下差异登记） | L2 表按 `data_platform/asset/tick_futures_l2/` 落位（Phase 2 预留）；已入库五档正式数据集口径为 `data_lake/future/market_data/tick_l2/`（不入 Git，随本地数据根挂载），DuckDB 视图名 `future_market_data_tick_l2`，口径见 [future_level2_data_processing.md](../../data/future_level2_data_processing.md) |
| 消费 | `data/data_api.py`（DataAPI 查询口） | 离线因子计算经查询口读 L2 表 → 因子值按日频/分钟频落 factor 存储（per-factor-one-table）→ FactorPipeline / alphalens。现存下游参考：`scripts/data_processing/build_tl_min1_from_ticks.py`（tick→min1 聚合）。`get_l2_snapshot()` 是否预留签名、L2 因子脚本落 `research/` 还是 `scripts/data_processing/` 仍为「实施时定稿项」，本节不裁决 |

> **差异登记（未裁决）**：`tick_futures_l2` 列数两处口径不一致——spec §47 记 27 列（7 基础 + bid/ask 5 档 20 列，未计 `quality_flag`），`data/storage_schema.py` 定义 28 列（7 基础 + 20 档位 + `quality_flag`，见该文件 tick_futures_l2 节）。按「只登记、不裁决」原则在此登记差异，不改写 §47 与 `docs/ROADMAP.md` 原文，以哪处口径为准不在本节裁决。

**起用接入点（V2 起用时从哪里接）**：V2+ 起用时，L2 链路自采集入口（`data/ingestion.py` / `data/source_adapter.py`）挂入，经存储入口（`data/storage_ops.py` → `tick_futures_l2`）落库，从消费入口（`data/data_api.py`）出数供离线因子计算——全程不进事件流（见「关键决策」）。起用阶段：**Phase 2 预留**（`docs/ROADMAP.md` §1：L2 / tick 表 `tick_futures_l2` 归 Phase 2，schema 已预留；列数口径差异见上，未裁决）。
