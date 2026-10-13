# Quant Platform 新架构设计

> 状态：设计定稿，待实施
> 依据：对齐头部量化机构体系（以公司自研体系为主干，业界通行做法补足单机场景）
> 最后更新：2026-09-26

这份文档定义重构后的目标架构。当前仓库正在按此方案渐进迁移，新旧共存、逐模块替换。
目录放置规则见 [项目结构](PROJECT_STRUCTURE.md)；本文档定义的是**范式与边界**，不重复目录清单。

---

## 1. 北极星

**对齐头部机构体系** —— 不是"最小改动把现状理顺"，而是按机构级系统形态全链路重构。

重构完成后最应该被显著改善的一件事：**架构范式对齐**。研究迭代速度、工程可演进性、实盘落地能力都是这个范式带来的结果，不是独立目标。

### 1.1 参照系

| 来源 | 作用 |
|---|---|
| 公司自研体系（pyhubble / pysim / pyutrade / pyUnity） | **主干**。分层方式、内核范式、产品契约、组织模型都以此为准 |
| 业界通行机构实践 | **补足**。单人单机场景下公司体系里不适用的部分（分布式运维、多人协作流程）用通行做法替代 |

---

## 2. 五域骨架

系统按**运营域**切分，不按研究流水线阶段切分。这是"三个垂直域 + 一个横向可观测层 + 一个横切内核"的形态。

```text
Quant/
├── kernel/          横切：统一仿真内核（pysim 等价物）
├── data_platform/   数据平台（资产范式：数据资产 / 因子表 / Job）
├── research/        投研（策略产品全流程：策略 / signal / Portfolio）
├── execution/       交易执行（订单生成 / 风控 / 账本）
└── ops/             监控运维（监控 / 归因 / 告警 / 任务调度）
```

### 2.1 依赖方向

```text
research/ ──┐
            ├──→ kernel/ ←──┐
execution/ ─┘               │
                            │
data_platform/ ──→ (数据源 Service 注入 kernel)
ops/        ──→ (观测所有域，不被依赖)
```

**硬规则**：
- `kernel/` 不 import 任何其他域
- `research/` 和 `execution/` 都依赖 `kernel/`，二者互不依赖
- `data_platform/` 通过**可替换数据源 Service** 注入内核，不反向依赖
- `ops/` 是观测层，依赖所有域的埋点接口，不被任何域依赖

### 2.2 为什么内核是独立第五域

参照 pyhubble 的真实形态：pysim 是 pyhubble 下的一个 service，和 pyutrade、pyUnity 并列，不是独立产品也不是某域的子模块。内核被投研和执行**共同**依赖，放在任一域内都会造成反向依赖。

### 2.3 投研与资产是两个板块

容易混淆的两个概念：

| 板块 | 对象 | 职责 |
|---|---|---|
| **投研** | 策略产品 | 策略 / signal / Portfolio 全流程 |
| **资产** | 数据/因子的工程化 | 数据资产 / 因子表 / Job |

"数据平台"和"资产"是**同一个东西的两种叫法** —— 资产是数据平台的组织范式，数据平台是承载它的系统。所以顶层是 4 个域 + 1 个横切内核。

---

## 3. 统一仿真内核（kernel/）

**核心范式：一套内核、一份代码，同时支撑回测 / 信号生产 / 实盘 / 压测 / 路由回测 / 数据生产。**

这是整个重构的范式核心。回测和实盘走同一条代码路径，从根上消除回测-实盘分歧。

### 3.1 时间模型：四层嵌套

```text
starfleet 轮询循环 (μs 级, usPerLoop 可配)
└── EBReplay 日期调度器 (日粒度)
    └── 发布 DI 事件，block 到当天 clock 跑完才切下一天
        └── EBClock 墙钟推进 (μs 级, us_since_today → exit_ts)
            └── EBTimer TI 网格判定
                └── us_since_day 跨越新 TI → 触发 EndTI 事件
                    └── EndTI 驱动信号计算
```

**关键设计**：信号由 **EndTI 事件**驱动，不是每个 tick 驱动。这实现了计算量与数据量解耦 —— TI 可以粗、数据可以细。

| 层 | 名称 | 粒度 | 职责 |
|---|---|---|---|
| 外层 | `starfleet` 轮询 | μs，`usPerLoop` 可配 | 周期性轮询 |
| 二层 | `EBReplay` | 日 | 日期调度器，维护日期事件队列，逐天发布 DI 事件，block 到当天 clock 跑完 |
| 三层 | `EBClock` | μs | 墙钟推进，每次 cell 增加 `us_since_today`，到 `exit_ts` 结束当天 |
| 四层 | `EBTimer` | TI | 对比 `us_since_day` 是否跨越新 TI，跨越时触发 EndTI 事件 |

### 3.2 图结构：EBGraph（运行时事件订阅图）

**这不是静态数据依赖 DAG，是运行时动态的事件订阅图。**

```text
EBGraph
├── 节点 = EventBrick (EB)      可插拔计算模块
│     契约四元素：name / publisher / Handle / invoker
└── 边 = 事件订阅关系           有向，发布者 → 订阅者，运行时动态建立
```

- **publisher**：EB 发布事件的出口
- **Handle**：EB 订阅事件的入口（订阅即建边）
- **invoker**：驱动计算的调用器
- **边是运行时动态的**：订阅关系不是编译期定死的 DAG，是跑起来才建立的

EndTI 事件沿 EBGraph 传播，触发订阅者的 Handle。

### 3.3 多模式复用：差异注入在事件源 + 数据源

**内核调度器和 EB 图跨模式完全不变**，差异全部注入在两个可替换 Service 和数据源。

| 可替换点 | 职责 | 回测 | Replay | 实盘 |
|---|---|---|---|---|
| `SystemTimeUpdateService` | 劫持 `now()` 调用，广播给 `SystemTimeManager` | mock 时间 | mock 时间 | 真实系统时间 |
| `TimeControlService` | 以启动瞬间为锚，真实流逝 × time-rate 叠加到模拟起点 | EBClock 自累加 `us_since_today`，自驱墙钟 | 注入 mock 事件 + 按 timerate 推进 | 墙钟实时 |
| 数据源 | 喂进 DataBus，EB 链订阅 | load 已生产的 udata / 从压缩行情切 k | uma 把历史行情回放进 DataBus（与生产同构） | 真实行情通道订阅实时行情 |

配置开关是几个 XML 参数。

**EB 链本身完全不知道自己在跑什么模式** —— 这就是"回测 = 实盘"的实现方式。

### 3.4 因子插件 = EventBrick

**因子插件没有独立接口，它就是 EB。** kernel 不知道什么是"因子"，只知道 EB。

```text
因子插件接口 = EventBrick 接口 (name / publisher / Handle / invoker)
├── PyIndicator / PyAlgo    Python 因子实现
├── C++ 因子                 高性能实现
└── UFormula                 公式表达式因子
```

因子的三段式归属：

| 阶段 | 归属 | 载体 |
|---|---|---|
| 因子开发 + 生产计算 | 投研层 | pysim 框架下的 C++ 因子开发框架，偏向研究层的生产计算 |
| 因子落库 + 资产化 | 数据平台 | 数据资产平台（版本、质量、血缘） |
| 因子消费 | 投研产出 → 执行入口 | 研究产出喂到执行 |

**现有 `factor_library/` 的 `Factor ABC` + `FactorDAG` 是另一套抽象，将被 EB 机制取代。**

### 3.5 单机实现，接口按分布式设计

- 实际跑单机（进程内消息总线 / 本地 DataBus）
- Service / EB / 数据源的接口边界按分布式切（未来可拆进程 / 机器）
- 保留迁移路径，不为单机牺牲接口形状

### 3.6 技术栈：分阶段下沉

| 阶段 | 做法 |
|---|---|
| 第一阶段 | 纯 Python 把内核和范式跑通，**不管性能** |
| 第二阶段 | 用 profiling 数据决定哪些 EB 下沉 C++/Rust（pybind11 / maturin） |

不要一开始就在没有性能数据的情况下写 C++。

---

## 4. 产品契约

PM 每个交易日交两样东西：**Signal** 和 **Portfolio**。

### 4.1 Signal：三层流式结构

Signal 不是单一矩阵，是三层底层结构：

| 层 | 名称 | 结构 | 更新方式 |
|---|---|---|---|
| 截面层 | **Blueshift** | `N 组 <string(合约/meta), struct(alpha)>` | 整租 Meta 或整租 signal 截面刷新 |
| 向量层 | **USignal** | `vector<factor>`，index = 合约下标 i | 增量更新一堆 |
| 流式层 | **Tick 信号数组** | 自定义元素结构，**不实际维护数组** | 增量直接传最新更新 |

### 4.2 Portfolio：目标持仓矩阵

**Portfolio 是合约 → 目标仓位的映射，不是订单。**

约束求解夹在 Signal 和 Portfolio 之间：

```text
Signal (三层)
   ↓  约束求解 / 组合构建
Portfolio (目标持仓矩阵)
   ↓  差分 + 查单
Utrade (订单生成)
   ↓
Themis (风控)
   ↓
TB (交易账本 / 柜台)
```

执行域只认目标持仓，订单是执行域自己算的。

### 4.3 新叙事取代 5 层流水线

**现有 `engine/pipeline.py` 的 5 层叙事（Universe→Alpha→Portfolio→Risk→Execution）废弃。**

新叙事：

```text
Signal (三层) → Portfolio (目标持仓) → Utrade (订单) → Themis (风控) → TB (账本)
```

README / AGENTS.md / CLAUDE.md 里的 5 层描述需要在实施时同步改写。

---

## 5. 投研域（research/）

### 5.1 组织模型：alphaType 矩阵

研究板块内部是**以策略 alphaType 为核心单元 + 多维标签打标 + 生命周期串联的矩阵组织**，不是文件夹层级。

```text
alphaType (策略标识类别，元数据维度粒度)
├── dim-alphaType-meta-info   主策略 / 策略本体定义
├── dim-alphaType-publish     发布 / 采纳记录
└── 生命周期：发布 → OA 审批 → 采纳
       多维标签打标（矩阵组织，可按标签筛选/聚合）
```

**alphaType 的粒度**：介于"完整策略配方"和"alpha 信号源"之间的**策略标识类别**。它不是完整策略配方，不是 alpha 信号源，是元数据维度的分类粒度。

- `dim-alphaType-meta-info` = 每个**策略类别**一行（主策略 / 策略本体定义 = 该类别的原型）
- `dim-alphaType-publish` = 该类别下**哪些变体被发布 / 采纳**
- `alstm` 是**一个 alphaType**，其下 35 个脚本是变体与实验，不是 35 个 alphaType

层级关系：**alphaType（类别）→ 变体 / 配方（实例）→ 实验（一次性探索）**

### 5.2 实例层：混合组织

现状：`strategies/futures/alstm/` 下 35 个脚本构成大致是

| 类型 | 例子 | 占比 |
|---|---|---|
| 叠加 / gate / 风控规则 | `atr_risk_overlay` `entry_overlay` `mass_shape_overlay` `strict_direction_gate` | ~70% |
| 算法本体变体 | `hybrid` `low_volatility_model_a_mixer` `volatility_normalized_model_a` | ~15% |
| 一次性探索 | `*_experiment.py` `signal_frequency_experiment` `notebook_reproduction` | ~15% |

**组织规则**：

| 类型 | 归宿 | 说明 |
|---|---|---|
| 叠加 / gate / 风控 | **参数化规则库** | 配方里声明式组合（最大收益点） |
| 算法本体变体 | **独立模块** | 够独立就升格新 alphaType |
| 一次性探索 | **归档区** | 永不进注册表 |

**实施顺序**：
1. 先把 35 个脚本全部归档进 `research/experiments/`
2. 再从里面往上提参数化规则库（叠加 / gate / 风控）和算法模块
3. 配方配置进注册表（`dim-alphaType-publish`），实验永不进

不预先设 DSL。纯配置表达力不够（组合规则会长成一门语言），纯模块就是现在的病根。

### 5.3 注册表与产品的关系

```text
策略本体定义 (dim-alphaType-meta-info)
  = 算法模块引用 + 规则配方配置
       ↓
发布记录 (dim-alphaType-publish)
  = 某一份配方配置 + 标签 + 生命周期状态
```

---

## 6. 数据平台域（data_platform/）

### 6.1 组织模型：多维度叠加

数据平台是**多维矩阵组织**，不是目录树。"数据资产 / 因子表 / Job"是同一套系统的三个视角，不是三个子目录。

**技术架构 5 层**：

| 层 | 职责 |
|---|---|
| 接入层 | 数据源接入 |
| 业务服务层 | 对外服务 |
| 公共能力组件 | 通用组件 |
| 数据访问层 | 存取抽象 |
| 基础设施层 | 存储引擎 / 计算资源 |

**业务域模型 4 元**：`资产` + `Job` + `依赖/血缘` + `实例`

**内容维度**：资产类型 = 标签（行情 / 基本面 / 因子 / 另类 …）

### 6.2 存储底座：分层选型

| 层 | 选型 | 理由 |
|---|---|---|
| 研究层 | **DuckDB**（嵌入式） | 交互式查询、零运维、单机友好 |
| 资产层 | **ClickHouse**（服务化） | 并发查询 / 压缩 / SQL 能力，多客户端访问 |

原始数据落盘仍走 Parquet（Hive 分区）。

---

## 7. 交易执行域（execution/）

### 7.1 范围

**重建订单生成 + 风控 + 模拟账本，柜台接口只定义抽象不实现。**

| 组件 | 公司对应 | 本次范围 |
|---|---|---|
| 订单生成 | Utrade | ✅ 重建（差分 + 查单 → 订单计划） |
| 风控 | Themis | ✅ 重建（风控裁剪） |
| 账本 | TB | ✅ 重建（模拟账本） |
| 柜台 / 券商接口 | — | ⭕ 只定义抽象，未来接 CTP 或其他通道 |

### 7.2 职责边界

执行域只拿 Portfolio（目标持仓矩阵），订单是执行域自己算的：

```text
Portfolio → 差分 + 查单 → 订单计划 → 风控裁剪 → 模拟账本
```

---

## 8. 监控运维域（ops/）

**范围：监控 + 归因 + 告警 + 任务调度统一入口。**

| 能力 | 说明 |
|---|---|
| 策略监控 | 持仓 / 盈亏 / 风险指标 / 信号衰减 |
| 归因 | 策略级归因报告 |
| 告警 | 阈值告警 |
| 任务调度 | 数据 Job / 训练 / 回测的统一编排入口（失败重试、运行记录） |

不做分布式集群运维（单机场景用 Prefect / cron 即可）。

---

## 9. 验证策略

**两层都要：黄金样本 + 性质测试。**

| 层 | 方法 | 说明 |
|---|---|---|
| 内核层 | 性质测试 + 解析解对拍 | 事件顺序不变、时间单调、订阅图拓扑正确；已知合成数据的解析解对拍 |
| 策略层 | 黄金样本回归 | 拿现有回测结果（`baseline_spec.py` 那组基准：sharpe 16.00、total_return 0.860638、trade_count 12686）作为黄金样本，新内核必须能复现同样的信号 / 净值 / 指标（容差内） |

渐进迁移期间新旧并行对拍。

---

## 10. 迁移策略

### 10.1 现有代码处置

**渐进迁移，新旧共存逐模块替换。**

| 现有资产 | 处置 |
|---|---|
| `engine/`（VectorBT / ZVT / Qlib 三套回测桥） | **降为对照组**，内核建成后废弃 |
| `libs/zvt/` | 同上，随 ZVT 引擎下线 |
| `factor_library/`（Factor ABC + DAG） | 被 EB 机制取代，迁移期保留作参考 |
| `strategies/futures/alstm/` 35 个脚本 | 归档进 `research/experiments/`，再提规则库和算法模块 |
| `engine/hft/` | 按职责归入对应域 |
| 黄金样本基准数据 | **必须保留**，是验证基线 |

### 10.2 阶段划分：kernel 先行，四域围它长

```text
P1  内核 + 数据源 stub
    ├── 时间模型（四层嵌套）
    ├── EBGraph + EventBrick 契约
    ├── SystemTimeUpdateService / TimeControlService
    └── 验收：能跑回测和信号（哪怕数据源是 stub）

P2  投研域
    ├── alphaType 注册表（dim-alphaType-meta-info / dim-alphaType-publish）
    ├── 三层 Signal 产出
    ├── Portfolio 目标持仓矩阵
    └── 验收：PM 能拿到当日 Signal + Portfolio

P3  执行域
    ├── 差分 + 查单 → 订单计划
    ├── 风控裁剪
    ├── 模拟账本
    └── 验收：Portfolio 能走到模拟账本，订单可回放

P4  运维域
    ├── 策略监控 / 归因 / 告警
    ├── 任务调度统一入口
    └── 验收：全流程有观测、有告警、任务可编排
```

**注意**：kernel 不能完全独立于数据平台建成，P1 的"数据源 stub"是必需品。

### 10.3 时间预算

**不限时间，做对为止。** 阶段划分是顺序，不是交付承诺。

---

## 11. 与现状的差异对照

| 维度 | 现状 | 新架构 |
|---|---|---|
| 顶层结构 | `infra/ engine/ factor_library/ strategies/ execution/` | `kernel/ data_platform/ research/ execution/ ops/` |
| 回测引擎 | VectorBT / ZVT / Qlib 三套并存 | 单一事件驱动内核多模式复用，三套降为对照组后废弃 |
| 因子抽象 | `Factor ABC` + `FactorDAG` | 因子即 EventBrick，三种实现路径（PyIndicator/PyAlgo、C++、UFormula） |
| 策略组织 | `strategies/<asset>/<name>/` 目录 + 平铺脚本 | alphaType 矩阵注册表 + 多维标签 + 生命周期 |
| 产品契约 | 无明确产品形态 | Signal（三层流式）+ Portfolio（目标持仓矩阵）每日交付 |
| 执行链路 | 无 | Portfolio → Utrade → Themis → TB |
| 流水线叙事 | 5 层（Universe→Alpha→Portfolio→Risk→Execution） | Signal → Portfolio → Utrade → Themis → TB |
| 存储 | Parquet + DuckDB | Parquet + DuckDB（研究层）+ ClickHouse（资产层） |
| 验证 | 散落的测试 | 黄金样本回归 + 内核性质测试 |
| 实验管理 | 35 个脚本平铺 | 归档区 + 规则库 + 算法模块，注册表管生命周期 |

---

## 12. 实施时要注意的红线

1. **不要在研究层写 C++**。先纯 Python 跑通，profile 后再下沉。
2. **不要给因子单独发明接口**。因子就是 EB。
3. **不要让 `kernel/` import 任何域**。它谁都不依赖。
4. **不要让回测和实盘走不同代码路径**。差异只能注入在 Service 和数据源。
5. **不要把一次性实验放进注册表**。`dim-alphaType-publish` 只收发布/采纳记录。
6. **不要丢黄金样本**。`baseline_spec.py` 的基准数据是迁移期的对拍依据。
7. **不要在迁移期同时改新旧两套的同一实现**。一次只动一边。
