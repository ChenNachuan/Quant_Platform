# 平台架构开放问题清单

> 建立日期：2026-09-28
> 状态：待讨论
> 用途：架构设计中悬而未决的细节问题，逐条带回讨论后回填结论
> 配套：`docs/ARCHITECTURE.md`（已定范式与边界）| `docs/PROJECT_STRUCTURE.md`（目录规则）

这份文档**自包含**，讨论时不用翻其他文件。第一部分是背景与已定决策，第二部分是全部开放问题，第三部分是讨论顺序建议。

每条问题标了优先级。讨论完一条就把结论回填到 ARCHITECTURE.md，并在此文件标注 `状态：已定 → 见 ARCHITECTURE.md §x.y`。

**优先级**：
- **P0** 阻塞实施，必须先定
- **P1** 影响模块设计，实施前要定
- **P2** 影响细节实现，可以边做边定

---

# 第一部分 背景与已定决策

## 0.1 背景

目标是把现有仓库重构为对齐头部量化机构形态的平台。参照系：

| 来源 | 作用 |
|---|---|
| 公司自研体系（pyhubble / pysim / pyutrade / pyUnity） | 主干：分层方式、内核范式、产品契约、组织模型 |
| 业界通行机构实践 | 补足：单机场景下公司体系里不适用的部分 |

五域骨架（`kernel/` `data_platform/` `research/` `execution/` `ops/`）已在 ARCHITECTURE.md 定稿。但 2026-09-28 出现技术选型转向，部分结论需要重审（见 [第二部分 §12](#12-受转向影响需重审的既有决策)）。

## 0.2 技术选型转向（2026-09-28 已定）

| 用途 | 选型 | 说明 |
|---|---|---|
| 因子分析 | **alphalens** | IC / 分位收益 / 换手分析，不是回测引擎 |
| 回测 | **backtrader** | 事件驱动回测 |
| 实盘 | **vnpy** | 交易网关 + 实盘执行 |
| 自研 | **薄壳** | 产品契约 + 统一数据接口 + 适配层 |

**影响**：ARCHITECTURE.md 第 3 章"统一仿真内核"的核心范式（一套内核多模式复用、回测=实盘同路径）**不再作为实施目标**。四层时间模型、EBGraph、EventBrick 的去留见 §12。

## 0.3 数据输入链路已定项（2026-09-28 已定）

| 决策点 | 结论 |
|---|---|
| 原始层策略 | 先原样落 raw → DQC → 入资产层，raw 是无条件落盘的证据层 |
| DQC 维度 | 四类：完整性 / 合法性 / 一致性 / 时序性 |
| DQC 失败处置 | 分级：硬错误拒收，软警告带 `quality_flag` 入库 |
| 多源仲裁 | 主源优先入仓，备源只做 DQC 一致性交叉校验 |
| 接入层抽象 | 统一 `SourceAdapter` 接口，每源一个适配器 |

## 0.4 其他已定项（散见前两轮）

- **投研 vs 资产**是两个板块：投研=策略/signal/Portfolio 全流程，资产=数据/因子的工程化管理。"数据平台"和"资产"是同一东西的两种叫法。
- **alphaType 粒度** = 策略标识类别（元数据维度），介于"完整策略配方"和"alpha 信号源"之间。`alstm` 是**一个** alphaType，其下 35 个脚本是变体与实验。
- **因子插件 = EventBrick**（在旧范式下）。三段式归属：开发计算→投研层；落库资产化→数据平台；消费→投研产出喂执行。
- **存储分层**：Parquet（raw）+ DuckDB（研究层）+ ClickHouse（资产层）。
- **迁移**：渐进迁移，新旧共存；旧引擎（VectorBT/ZVT/Qlib）先降为对照组。
- **验证**：黄金样本 + 性质测试。黄金样本基线来自 `strategies/futures/alstm/baseline_spec.py`（sharpe 16.001135 / total_return 0.860638 / trade_count 12686）。
- **实现策略**：单机实现，接口按分布式设计；纯 Python 先跑通，profiling 后再下沉 C++/Rust。

---

# 第二部分 开放问题

## 1. 三件套集成（alphalens / backtrader / vnpy / 薄壳）

### 1.1 backtrader 与 vnpy 的行为差异怎么管？（P0）

> **状态（2026-10-04）：已定 → 见 §19.1–§19.7 / §22（路线 A+B+C，D 不做）/ §24（Adapter 实现）**

**问题**：两套执行模型不同，同一策略在 backtrader 回测和 vnpy 实盘的净值不会一样。

| 维度 | backtrader | vnpy |
|---|---|---|
| 事件驱动模型 | Cerebro 主循环，`next()` 按 bar 推进 | 事件引擎，Tick/Bar/Order/Trade 多路事件 |
| 下单/成交 | 模拟撮合器（自定义滑点/手续费） | 真实柜台回报（CTP）或模拟网关 |
| 撮合时机 | 下一根 bar 开盘/收盘成交，可配 | 柜台撮合规则 |
| 时间粒度 | bar 级为主 | Tick 级事件 |
| 订单类型 | 市价/限价/止损/OCO | 柜台支持的全部（含 FAK/FOK） |
| 数据喂入 | `DataFeed` 自定义 | `Gateway` 网关回调 |

**候选方向**：
- A. 薄壳内定义统一订单/成交模型，两边写适配层归一，差异收敛在适配层
- B. 不追求一致，用对拍报告量化偏差（滑点/成交率/时延），偏差当参数回填回测
- C. 回测用 backtrader 做筛选，定型后用 vnpy 模拟盘跑 2-4 周校准，用流程管分歧
- D. 回测用 backtrader，实盘只用 vnpy 的网关/柜台接口，执行层自研

**子问题**：
- 差异容忍度是多少？（净值偏差 < 多少算可接受）
- 对拍是常态机制还是一次性校准？
- 谁负责写适配层、什么时候写？

### 1.2 统一账本的数据模型怎么定？（P0）

> **状态（2026-10-04）：已定 → 见 §19.4 / §23（账本字段级 schema 已定：5 dataclass + 6 态状态机 + Journal）**

**问题**：`Order / Trade / Position / Account` 的规范模型，backtrader 和 vnpy 都要往它对齐。

**候选方向**：
- A. 以 **vnpy 模型**为准，backtrader 向它对齐（vnpy 模型过实盘检验，字段完整：订单状态机、撤单、部分成交）
- B. 自定义规范模型，取两边交集 + 补齐缺失，两边各写适配器
- C. 以 backtrader 为准（回测场景为主，但会丢 vnpy 的订单状态机信息）
- D. 先定事件流时序（下单→回报→成交→持仓变动），schema 实现时再定

**子问题**：
- 订单状态机要覆盖哪些状态？（submitted/partial/filled/cancelled/rejected/…）
- 部分成交怎么建模？
- 手续费/滑点/资金占用在哪个字段？
- 多账户/多组合怎么表示？
- 账本是 append-only 事件流，还是可更新的快照表？

### 1.3 薄壳的边界：自己拥有什么、外包什么？（P0，倾向选项 3 但待细化）

> **状态（2026-10-04）：已定 → 见 §19.2 / §19.4 / §19.7 / §22 / §24 / §25 / §26 / §30 / §32 / §34 / §35（薄壳 = 策略纯函数层 + dataclass 契约 + BT/VN/CS 三路适配 + DataAPI + PortfolioOptimizer + BacktestConfig；≈2680 行）**

**倾向**：薄壳管**产品契约 + 统一数据接口 + 适配层**（选项 3）。

**子问题**：
- **统一数据接口**：因子表/行情读取的接口签名长什么样？alphalens / backtrader / vnpy 三家怎么从同一接口拿数据？
  - alphalens 要 DataFrame（factor + pricing）
  - backtrader 要 `DataFeed` 对象
  - vnpy 要 `Gateway` 回调或 BarGenerator
  - 统一接口是统一"取数"还是统一"取数+包装"？
- **适配层回流**：backtrader / vnpy 的订单/成交/持仓怎么回流到统一账本？
  - 实时流回流，还是跑完批量对账？
  - 回流丢了信息怎么办（backtrader 没有的字段）？
- **薄壳的运行形态**：一个进程跑完所有事，还是分 Job？
- **薄壳要不要管策略生命周期**：加载/实例化/参数注入/热更新？

### 1.4 截面矩阵策略在 backtrader 里怎么跑？（P0）

> **状态（2026-10-04）：已定 → 见 §26（A+C 混合：截面走自研 cs_portfolio_sim，时序走 backtrader，实盘走 vnpy，三路共用 Journal）**

**问题**：alstm 的产出是**截面矩阵**（合约 × 因子 → 目标持仓矩阵），backtrader 的模型是**每 instrument 一个 Strategy、逐 bar 推进**。不匹配。

**候选方向**：
- A. 上层自研截面组合层，backtrader 只做单标的撮合（截面逻辑不进 backtrader）
- B. 截面逻辑做成 backtrader 的多标的 Strategy（一个 Cerebro 带 N 个 data feed，`next()` 里统一算截面）
- C. 截面策略不用 backtrader，回测自研向量化；backtrader 只用于单标的/时序类策略（CTA）
- D. 截面策略回测继续用 VectorBT（已有能力），backtrader 用于单标的事件驱动场景

**子问题**：
- alstm 这类截面策略走哪条路？
- 如果走 A，"上层截面组合层"是薄壳的一部分还是独立模块？
- 如果走 C/D，backtrader 在整个平台里的定位是什么？（只服务单标的策略？）

### 1.5 alphalens 怎么接入？（P1）

> **状态（2026-10-04）：已定 → 见 §19.8 / §27（定位：筛选层门控非验证层；两阶段因子来源、qfq 日频 pricing、产出落三层、IC>0.04/IR>0.5/t>max(2,√(2lnN))）**

**问题**：alphalens 的输入是 `factor Series (MultiIndex: date, asset)` + `pricing DataFrame`，输出是 IC / 分位收益 / 换手表。

**子问题**：
- 因子数据从哪来？（data_platform 的因子表？还是研究层现算？）
- pricing 数据的频率（日/分钟）和复权口径？
- alphalens 的产出（IC 序列、分位收益曲线）落到哪？（进注册表？进报告？）
- alphalens 分析结果和回测结果的关系？（先 alphalens 筛因子，再 backtrader 测策略？）

### 1.6 六套引擎共存还是收敛？（P1）

> **状态（2026-10-04）：已定 → 见 §28（4 保留 + 3 归档：backtrader 服务 CTA/时序、cs_portfolio_sim 服务截面、vnpy 服务实盘、alphalens-reloaded 服务因子评价；VectorBT/ZVT/Qlib 移 `archive/`，不做对照组——修正 §20.7/§26.8 的"降为对照组"表述）**

**问题**：现在仓库有 VectorBT / ZVT / Qlib bridge，加上 backtrader / vnpy / 自研薄壳 = 六套。

**子问题**：
- VectorBT 保留吗？（截面快筛有独特价值）
- ZVT 保留吗？（`libs/zvt/` 是本地 fork）
- Qlib bridge 保留吗？（ML 管线可能有用）
- 六套的定位各是什么？有没有一张"什么场景用什么"的表？
- ARCHITECTURE.md 里"三套降为对照组后废弃"的结论要改吗？

### 1.7 三件套的具体版本与形态（P1）

> **状态（2026-10-04）：已定 → 见 §28.2 / §28.6（backtrader 1.9.78.123 / vnpy 4.x 只装网关子包 Gateway+EventEngine / alphalens-reloaded 0.4.6；GPL 与 Py3.10 兼容性已核）**

**子问题**：
- **vnpy**：用 vnpy 3.x 的多应用架构，还是精简版（只取 Gateway + 事件引擎）？
- **backtrader**：社区版本多年未大更新，是否接受？备选（nautilus_trader / bt / 自研）考虑过吗？
- **alphalens**：用原版 quantopian/alphalens 还是 alphalens-reloaded？
- 三件套的**许可协议**（商用/分发限制）确认了吗？
- 三件套的**Python 版本兼容性**（项目是 Python 3.10）？

---

## 2. 接口契约与产品契约

### 2.1 事件词汇表：封闭还是开放？（P0）

> **状态（2026-10-04）：已定 → 见 §29（封闭 Enum 12 事件 + frozen Event dataclass + emit/journal/optional listeners；§20.3"不建通用事件系统"保留，薄壳内补 ~60 行精简事件层）**

**问题**：DI（日初）、EndTI（TI 结束）两个时间事件已知，完整事件集合是什么？（如果内核降级为薄壳，事件模型可能是薄壳内部的事，也可能整个不需要 —— 见 §12.1）

**候选方向**：
- A. 封闭集合，按类别分组（时间/数据/业务事件），EB 只能订阅预定义类型
- B. 开放集合，EB 自定义事件名（字符串订阅匹配）
- C. 混合：内核事件封闭，业务事件开放但有命名规范和注册机制
- D. 公司 pysim 有具体事件类型枚举

**子问题**：
- 事件的表示形态（dataclass？dict？消息体？）
- 事件的负载结构（type + payload + metadata(timestamp/source/sequence)）
- 事件的顺序保证（同 TI 内事件有序？跨 TI 有序？）
- 事件的生命周期（持久化？只在内存？）

### 2.2 EventBrick 契约签名（P0，取决于 §12.1）

> **状态（2026-10-04）：已定 → 见 §20.3 / §20.4 / §30（不建 EventBrick 抽象；四元素逐个给薄壳等价物：emit / 硬编码主循环 / engine.run() / session+checkpoint；因子用 backtrader Indicator / vnpy 模块承载）**

**问题**：四元素 `name / publisher / Handle / invoker` 的具体签名是什么？

**子问题**：
- `publisher` 是方法还是属性？发布的事件类型怎么声明？
- `Handle` 是装饰器注册、配置声明、还是代码里显式订阅？
- `invoker` 是调度器回调还是 EB 自己驱动？
- EB 的生命周期（注册 / 启动 / 停止 / 销毁）
- EB 的状态管理（有状态 EB 怎么持久化）
- 订阅关系建立时机（构造期？启动期？运行期动态？）

### 2.3 DataBus 接口（P0，取决于 §12.1）

> **状态（2026-10-04）：已定 → 见 §20.3 / §31（不建 DataBus，直连替代，新增 0 行；数据经 DataAPI / vnpy Gateway 直连进策略）**

**问题**：单机 DataBus 的接口签名。

**子问题**：
- 发布 / 订阅 API 形态（同步调用？队列？回调？）
- 单机实现（进程内内存队列 / pub-sub）
- 数据源 Service 怎么灌数据进 DataBus
- 背压处理（消费慢于生产怎么办）
- 数据回放（Replay 模式）怎么走同一接口

### 2.4 Signal 三层 schema（P0）

> **状态（2026-10-04）：已定 → 见 §32（两层：CrossSignal 截面全量快照 + SignalData 时序增量；Tick 层不做；新增 SIGNAL 事件）**

**问题**：Blueshift / USignal / Tick 信号数组的字段级 schema。

| 层 | 名称 | 结构 | 更新方式 |
|---|---|---|---|
| 截面层 | Blueshift | `N 组 <string(合约/meta), struct(alpha)>` | 整租 Meta 或整租 signal 截面刷新 |
| 向量层 | USignal | `vector<factor>`，index = 合约下标 | 增量更新一堆 |
| 流式层 | Tick 信号数组 | 自定义元素结构，**不实际维护数组** | 增量直接传最新更新 |

**子问题**：
- **Blueshift**：`struct(alpha)` 有哪些字段？meta 有哪些字段？
- **USignal**：合约列表怎么维护？增量更新的 diff 格式？
- **Tick 信号数组**：自定义元素结构是什么？"不实际维护数组"怎么落地（只传最新值）？
- 三层之间的关系（同一策略同时产出三层？还是按策略类型用不同层？）
- 序列化格式（内存对象 / Parquet / Arrow / 自定义）
- 每日交付的落盘位置和命名规范

### 2.5 Portfolio schema（P0）

> **状态（2026-10-04）：已定 → 见 §32.2 / §34（五问全解：单位三者共存权重为主；约束=PortfolioConstraints 四类+手数；求解器纯 numpy 裁剪不用 cvxpy；T 日收盘产出 T+1 开盘执行；与 Signal 1:1）**

**问题**：目标持仓矩阵（合约 → 目标仓位）的字段级 schema。

**子问题**：
- 目标仓位的单位（股数 / 手数 / 名义金额 / 权重）
- 约束求解的输入（换手上限 / 行业暴露 / 资金约束 / 单标的上限？）
- 约束求解器选型（自研？scipy.optimize？cvxpy？）
- Portfolio 的时间戳语义（T 日收盘产出，T+1 执行？）
- 与 Signal 的对应关系（一个 Signal 对应一个 Portfolio？）

### 2.6 Service 契约（P1，取决于 §12.1）

> **状态（2026-10-04）：已定 → 见 §35**（注册与发现=构造函数注入；启动顺序=代码书写顺序；配置注入=TOML 三层 + BacktestConfig 类型载体（修正 §35.6 YAML → §69））

**问题**：`SystemTimeUpdateService` / `TimeControlService` / 数据源 Service 的接口签名。

**子问题**：
- Service 的注册与发现机制
- Service 的启动顺序依赖
- 配置注入方式（ARCHITECTURE.md 说"几个 XML 参数"，单机场景用 YAML/TOML 更合适？）

---

## 3. 数据输入链路与 DQC

数据输入链路已定 5 条（见 [0.3](#03-数据输入链路已定项)），下面是待定细节。**本节重点展开 DQC**。

### 3.1 DQC 四维规则集的具体检查项（P0）

> **状态（2026-10-04）：已定 → 见 §36**（44 条四维规则全表；存放=YAML+Python 混合；阈值三类；semver 版本化历史不自动重跑；粒度按资产为主）

**问题**：完整性 / 合法性 / 一致性 / 时序性 四类各有哪些具体检查项？

| 维度 | 候选检查项 | 待定阈值 |
|---|---|---|
| **完整性** | 缺失值率、断崖检测（某日数据量骤降）、列缺失、主键覆盖、全市场覆盖率 | 缺失率 < ?%；断崖阈值 ? |
| **合法性** | 取值范围（价格>0、涨跌幅∈[-10%,+10%]）、枚举值（交易状态/买卖方向）、主键唯一性、类型合法、精度合法 | 异常值判定标准？ |
| **一致性** | 跨源对比（价格/成交量/持仓量）、跨表勾稽（成交量 vs 持仓量）、复权因子一致性、分钟 vs 日线勾稽、字段间逻辑自洽 | 数值容差 < ? |
| **时序性** | 时间戳连续性（不缺 bar）、时间单调（不倒流）、交易日历对齐、交易时段边界、开收盘价连续性 | 缺 bar 判定标准？ |

**子问题**：
- DQC 规则的**存放位置**（Python 代码？YAML 配置？规则引擎？）
- DQC 规则的**资产类型覆盖**（行情 / 基本面 / 因子 / 另类 各自的规则集）
- 阈值怎么定（拍脑袋？分位数自适应？历史分位数 + 目标分位数？）
- DQC 规则的**版本化**（规则改了，历史数据要重跑 DQC 吗？）
- 规则的**可配置粒度**（每资产一配置？每字段一配置？）

### 3.2 硬错误 vs 软警告的判定边界（P0）

> **状态（2026-10-04）：已定 → 见 §21.1 / §36 / §37**（规则级 HARD 27 / SOFT 17；判定边界按规则 `row_action` + `escalation_threshold`；硬拒三级粒度（行级/分区级/批次级）；`quality_flag` int64 位掩码；批次级安全阀——四子问题全解）

**问题**：哪些是硬错误（拒收）、哪些是软警告（带标入库）？

| 场景 | 候选判定 |
|---|---|
| 主键重复 | 硬 / 软 / 丢弃重复？ |
| 时间倒流 | 硬？ |
| 空值率 5% | 软？ |
| 空值率 30% | 硬？ |
| 价格异常（涨跌停外） | 软 + 标记？ |
| 跨源不一致 | 软 + 告警？ |
| 关键字段全空 | 硬？ |

**子问题**：
- 边界是全局统一，还是按资产类型/字段分别定？
- 软警告入库后的 `quality_flag` 怎么编码（单字段位掩码？多字段？枚举？）
- 下游消费方怎么处理 `quality_flag`（默认忽略？按需过滤？）
- 有没有"严重到整体拒收这一批"的阈值？

### 3.3 DQC 的执行时机与性能（P1）

> **状态（2026-10-04）：已定 → 见 §36.10 / §38**（pre-load 一遍；耗时日频 <1s/分钟频 <5s 不是瓶颈；增量不抽样；失败重跑整批（`reingest`）；回填按日期并行 joblib——五子问题全解）

**子问题**：
- DQC 在入库前（pre-load）还是入库后（post-load）跑？还是两遍？
- DQC 的耗时预算（分钟级数据量很大，全量 DQC 会不会成为瓶颈）
- DQC 的抽样策略（全量 / 抽样 / 增量只查新数据）
- DQC 失败的重试（修复后重跑整批？还是只跑失败部分？）
- DQC 的并行度（按合约并行？按日期并行？）

### 3.4 DQC 的报告与可追溯（P1）

> **状态（2026-10-04）：已定 → 见 §36.7 / §38**（双表存储：行级明细按月分区 + 批次汇总单文件；schema 全字段定稿；明细 1 年/汇总永久；CLI+DuckDB 看板不建 Web；logging 三级告警不接外部推送——五子问题全解）

**子问题**：
- DQC 结果存哪（独立 DQC 报告表？每批一份报告？）
- DQC 报告的字段（规则 ID / 检查时间 / 数据范围 / 通过数 / 失败数 / 明细）
- DQC 历史的保留期
- DQC 看板（怎么看每类数据的健康度）
- DQC 告警的通道与分级

### 3.5 隔离区（quarantine）的形态（P1）

> **状态（2026-10-04）：已定 → 见 §39**（隔离区=独立目录 `quarantine/{asset}/{date}.parquet` + `.rejection.json`；T+30 自动清理；修复三路径；看板与 §3.4 共用——四子问题全解）

**问题**：被拒收的硬错误数据去哪？

**子问题**：
- 隔离区存哪（独立目录 / 独立表 / 独立数据库）
- 隔离区数据的生命周期（永久保留？T+N 天后清理？）
- 隔离区数据怎么修复重跑（人工改？重拉？）
- 隔离区的告警和看板

### 3.6 raw 层的格式与分区（P1）

> **状态（2026-10-04）：已定 → 见 §39**（“原样”=Parquet+zstd+原始列名值类型；分区=数据源→资产→日期；raw 保留 1 年；每源一个 Normalizer 不做 DQC——五子问题全解）

**问题**："原样落盘"的"原样"是什么意思？

**子问题**：
- 保留源格式（AmazingData 返回什么就存什么）还是归一到统一 schema？
- raw 层分区策略（按数据源 / 按资产类型 / 按日期 / 按合约）
- raw 层的文件格式（Parquet / JSON / 源格式原样）
- raw 层的压缩与保留策略（多久清理）
- raw → 资产层的转换逻辑放哪？（每源一个 Normalizer？）

### 3.7 SourceAdapter 接口签名（P1）

> **状态（2026-10-04）：已定 → 见 §21.2 / §21.3 / §39.9 / §40**（四方法 `fetch`/`list_asset_types`/`health_check`/`close`，无 normalize/schedule；FetchResult schema；watermark 增量；三层限流重试超时；新增源 ~180-220 行——五子问题全解。fallback 优先级与字段映射边界属 §21.x 议题）

**问题**：统一 `SourceAdapter` 的接口定义。

**子问题**：
- 接口方法（`fetch()` / `normalize()` / `schedule()` / `health_check()`？）
- 每个方法的输入输出 schema
- 增量拉取怎么表达（`fetch(since=watermark)`？）
- 限流/重试/超时在哪层处理
- 新增一个数据源要写多少代码
- 已知约束：AmazingData `get_option_basic_info` 单次最多返回 ~100 条、`local_path` 必须传字符串、连接数有限 —— 这些限制在适配器里怎么屏蔽

### 3.8 多源冲突的具体判定（P1）

> **状态（2026-10-04）：已定 → 见 §21.2 / §41（主源指定 + 跨源校验 + 备源补数，五子问题全解）**

**子问题**：
- 主源指定表（每个资产类别/字段的主源是谁）放哪？
- 跨源对比的容差（数值容差、时点对齐）
- 交叉校验的频率（每批？每天？）
- 冲突时告警 vs 静默记录
- 备源是否参与补数（主源缺失时）

### 3.9 交易日历 / 时区 / 合约换月（P1）

> **状态（2026-10-04）：已定 → 见 §42（日历来源 / 时区 / 连续合约 / 夜盘归属，四子问题全解）**

**子问题**：
- 交易日历的来源（交易所官方？自维护？）
- 时区处理（数据源返回的时间戳是什么时区，统一转 UTC 还是本地？）
- 合约换月（连续合约怎么构造：主力合约切换规则？复权连续？）
- 夜盘时段的日期归属（算当日还是次日？）

### 3.10 复权处理（P1）

> **状态（2026-10-04）：已定 → 见 §27.3 / §43 / §44（四子问题全解：asset 层 raw+adjfactor 不存复权价、分钟线无 adjfactor 列；adjfactor 来源 Tushare 主源+AKShare 反推校验 + 3 条 DQC + 公司行动检测；研究层前复权 + PriceAccessor；回测/实盘统一"截至当日前复权"口径）**

**子问题**：
- 资产层存什么口径？
- 复权因子的来源和校验
- 研究层用什么口径（alphalens 的 pricing 用哪个）？
- 回测和实盘的口径一致性

### 3.11 数据回溯修正（P2）

> **状态（2026-10-04）：已定 → 见 §36.5 / §45（四子问题全解：全量重拉+逐行比对检测 / 直接覆盖+修正日志 / 下游 stale 标记不自动重跑 / 按 severity 人工决定回测重跑）**（T09 修正检测：历史数据被改、变化 > 0.01% 即告警；应用/传播/是否重跑回测已定 → §45）

**子问题**：
- 修正检测（怎么发现源数据变了）
- 修正应用（覆盖？版本化？）
- 修正对下游因子/回测的影响传播
- 历史回测要不要重跑

---

## 4. 数据存储细节

存储分层已定（Parquet raw / DuckDB 研究 / ClickHouse 资产），下面是落地细节。

### 4.1 三套存储的职责边界（P0）

> **状态（2026-10-04）：已定 → 见 §20.1 / §46（砍 ClickHouse；两层存储 Parquet+DuckDB；DataAPI ~110 行；通用 schema 约定；原子写入）**

**问题**：Parquet / DuckDB / ClickHouse 各自明确负责什么，数据在哪一层物化？

**子问题**：
- **Parquet**：只存 raw？还是也存归一后的资产数据？（现状 `data_lake/market_data/cn_stock/1d/year=YYYY/data.parquet` 是归一后）
- **DuckDB**：是"视图 + 查询"（`infra/storage.py` 现在是这么做的）还是"独立副本"？
- **ClickHouse**：什么时候引入？单机场景下是否真的需要（可否先只用 DuckDB，预留接口）
- 三者的数据流向（Parquet → DuckDB 视图？Parquet → ClickHouse 导入？）
- 一致性保证（同一数据在 DuckDB 视图和 ClickHouse 表里版本一致吗）

### 4.2 表 schema 设计（P0）

> **状态（2026-10-04）：已定 → 见 §47（11 张核心表字段级 schema + 主键/分区/排序 + 时间戳 + 合约代码 + 可空 + quality_flag；5min/60min 视图聚合；L2 tick Phase 2 预留）**

**问题**：核心表的字段级 schema。

**待定 schema 的表**：
- 行情表（OHLCV + 持仓量 + 成交额 + 合约标识 + 复权因子 + `quality_flag`）
- Tick / L2 表（期货 Level-2 五档）
- 分钟 / 5 分钟 / 60 分钟表（现有 `fetch_stock_5min.py` / `fetch_stock_60min.py`）
- 基本面表
- 因子表（见 [§6](#6-因子的计算存储与管理)）
- 交易日历表
- 合约主数据表（合约 → 交易所 / 合约乘数 / 到期日 / 挂牌规则）

**子问题**：
- 每张表的主键、分区键、排序键
- 时间戳字段的类型（datetime / bigint us？）和精度
- 字符集与编码（合约代码的规范：`IF2601` 还是 `IF2601.CFFEX`？）
- 可空字段的约定
- `quality_flag` 的位置（行级？批级？）

### 4.3 分区与文件组织（P1）

> **状态（2026-10-04）：已定 → 见 §48（扁平日期文件 `{YYYY-MM-DD}.parquet` 不用 Hive / 文件 1MB-256MB 日频偏小不合并 / 小文件合并不需要 / tmp+rename+`cleanup_tmp_files` / 全部覆盖写）**

**子问题**：
- 分区策略统一规范（现有是 Hive `year=YYYY`；要不要细化到 `month=` `day=`）
- 文件大小目标（太小 → 文件数爆炸；太大 → 并行度低）
- 小文件合并策略（合并时机、合并触发阈值）
- 同一分区内写入的原子性（部分写入失败怎么办）
- 覆盖写 vs 追加写 vs 版本化写

### 4.4 压缩与编码（P2）

> **状态（2026-10-04）：已定 → 见 §48（zstd level 3 / 列编码 pyarrow 自动选择 / 冷热分层不需要 67GB 全 SSD / 压缩比 50% vs 解压慢 10% + Row Group 64K + `PARQUET_WRITE_CONFIG`）**

**子问题**：
- Parquet 压缩算法（snappy / zstd / gzip）和级别
- 列编码（字典 / RLE / delta）
- 冷热分层（热数据 SSD、冷数据 HDD/对象存储？）
- 压缩比 vs 查询性能的权衡

### 4.5 事务性与并发（P1）

> **状态（2026-10-04）：已定 → 见 §49 / §50（并发写按日隔离 + flock LOCK_NB 同天直接拒绝 / 并发读 inode 语义 snapshot isolation / 一天一文件=一天一事务 / 只需 flock / 幂等重跑不回滚；§50 完整讨论稿含 UdataSQL 三阶段映射 + Confluence 四页参考）**

**子问题**：
- 并发写（多个 Job 同时写同一张表怎么办）
- 并发读（读到写了一半的数据怎么办）
- 事务边界（一批数据入库是原子的吗）
- 锁机制（DuckDB 单写多读 vs ClickHouse 异步写）
- 失败回滚（写了一半怎么恢复）

### 4.6 数据生命周期与容量（P2）

> **状态（2026-10-04）：已定 → 见 §49 / §51（asset 永久 raw/辅助 1 年 quarantine 30 天 factor 永久 / `cleanup_expired`+lifecycle.yaml dry_run / 10 年 ~45GB 含 L2 ~84GB 详估 / <50GB WARNING <10GB CRITICAL + 单日>100MB 增长告警 / rsync 全量备份月度 RPO 1 月 RTO 4h 文件级；§51 完整讨论稿含 Class 1-4 数据等级映射 + Confluence 六页参考）**

**子问题**：
- 保留策略（raw 保留多久？归一数据保留多久？）
- 过期数据的归档与删除
- 容量估算（全市场 A 股 1min × 10 年约多少 GB）
- 容量告警阈值
- 备份策略（本地备份？异地备份？备份的 RTO/RPO）

### 4.7 查询与访问层（P1）

> **状态（2026-10-04）：已定 → 见 §52（SQL-only DataAPI ~80 行 / 不做应用层缓存只用 OS page cache+DuckDB 内部 / [COST]+WARNING>5s/CRITICAL>30s/timeout 60s / 每进程独立 DataAPI 不共享连接 / alphalens 日频<1s 分钟<5s）**

**子问题**：
- 统一查询接口（SQL？DataFrame API？对象 API？）
- 缓存策略（热数据缓存？查询结果缓存？）
- 慢查询的监控
- 多进程/多线程访问 DuckDB 的约定
- 研究层拿数据的性能要求（alphalens 单次查询多少秒算可接受）

### 4.8 与三件套的数据对接（P0）

> **状态（2026-10-04）：已定 → 见 §25.2 / §27.6–§27.7 / §53（load_bars ~50 行 + 三个 to_xxx 适配 + 口径归一表 / 复权分流 BT+AL 前复权 VN 未复权 / 分钟>3 月按月 chunk）**

**问题**：backtrader 的 `DataFeed`、alphalens 的 DataFrame、vnpy 的 `BarGenerator` 怎么从存储层拿数据？

**子问题**：
- 是否提供统一的 `load_bars(symbol, start, end, freq) -> DataFrame` API？
- DataFeed 是自己写还是用 backtrader 自带的（PandasData）？
- 三件套的口径差异（复权 / 时间戳 / 字段命名）怎么在加载层归一
- 内存占用（全市场 1min 全量加载会不会 OOM，要不要分块加载）

---

## 5. Udata 与容器层

**问题**：公司体系里 udata 是统一数据容器（回测加载 udata / k-slices）。我们要不要引入 udata 这一层？它的 schema 和容器层怎么设计？

### 5.1 udata 是什么形态？（P0）

> **状态（2026-10-04）：已定 → 见 §54（形态=Schema+容器抽象底层可插拔 Ucube/内存/文件 / 粒度=BarData 三维矩阵 [di,ti,ii] 多日×多时段×多合约 / DataBus=实时消息总线与 udata 上下游非包含 / k-slice=Slice 切片操作非容器本身）**

**子问题**：
- udata 是**内存对象**（类似 pandas DataFrame / Arrow Table）还是**文件格式**（落盘）？
- 一条 udata 对应**一个合约**、**一个品种**、还是**一个截面（多合约）**？
- udata 和 DataBus 的关系（udata 是 DataBus 的载荷？还是 DataBus 喂 udata？）
- udata 是不是就是 `k-slices`（k 线切片）？k-slice 的"切片"维度是什么（时间窗口？合约集合？频率？）

### 5.2 udata 的 schema（P0）

> **状态（2026-10-04）：已定 → 见 §47 / §54 / §55 / §56（概念映射 Schema/TypeInfo/ProductionStatus/Lineage→Parquet schema+DQC+correction_log；精度：价格/额/复权因子 DOUBLE、量 INT64、日期时间 VARCHAR、quality_flag INT8，不用 Decimal；扩展：新增列/表靠 union_by_name 兼容不破坏旧代码，改类型/改列名避免；版本化：不需要显式机制，Parquet footer+union_by_name+Git log 即版本历史）**

**子问题**：
- **基础字段**（datetime / open / high / low / close / volume / amount / open_interest / 合约 ID / 复权因子）
- **扩展字段**（L2 五档、tick、盘口、逐笔成交）
- **元字段**（数据版本、`quality_flag`、生成时间、数据源）
- 字段的类型与精度（价格是 float32 / float64 / Decimal？）
- schema 的可扩展性（新增字段怎么不破坏旧代码）
- schema 的版本化（schema 变了，旧 udata 怎么兼容）

### 5.3 容器层：多合约 / 多周期 / 多资产的组织（P0）

> **状态（2026-10-04）：已定 → 见 §4.2 / §4.3 / §54 / §56（容器=11 张表+按日文件即公司容器层等价物；公司 6 类 Udata BarData/FutBarData/ProductBarData/DailyData/CrossSection/OrderTrade 逐一映射；合约元数据独立 instruments 表；依赖=correction_log 简化版；Universe/TimeAxis 每行自带字段）**

**问题**："容器层"意味着 udata 是被组织在一个层次结构里的。这个层次怎么设计？

**子问题**：
- 容器的层级（资产大类 → 品种 → 合约 → 周期？还是别的）
- 一个容器里放什么（单合约单周期？单合约多周期？多合约单周期？）
- 容器的标识（命名规范）
- 容器的加载策略（按需加载？预加载？LRU 缓存？）
- 容器之间的依赖（衍生出的连续合约 udata 依赖原始合约 udata 吗）
- 容器的元数据（合约乘数、最小变动价位、交易时段 —— 放在容器里还是独立主数据表）

### 5.4 udata 的生产与消费（P1）

> **状态（2026-10-04）：已定 → 见 §54 / §56（生产=akshare/tushare API→Normalizer→Parquet、消费=DataAPI/load_bars；不引入独立 udata 格式（Parquet=最终格式、DataFrame=内存表示，不引入中间层）；缓存不做 §52.5；内存布局=Parquet 列存）**

**子问题**：
- udata 的**生产方**（数据平台？研究层现算？）
- udata 的**消费方**（回测？因子计算？ML 训练？）
- udata 是从 Parquet 加载的中间表示，还是独立存储格式？
- udata 的缓存策略（同一 udata 被多个实验复用，缓存多久）
- udata 的内存布局（列存 Arrow？行存 numpy？稀疏？）

### 5.5 udata 与三件套的对接（P1）

> **状态（2026-10-04）：已定 → 见 §53 / §54（to_backtrader_feed / to_alphalens_prices+factor / to_vnpy_bars；反向产出不组织成 udata 落 outputs/ 报告）**

**子问题**：
- udata → backtrader DataFeed 的转换
- udata → alphalens DataFrame 的转换（pricing + factor MultiIndex）
- udata → vnpy BarGenerator 的喂入
- 反向：backtrader / vnpy 的产出要不要也组织成 udata？

### 5.6 引入 udata 的必要性（P0）

> **状态（2026-10-04）：已定 → 见 §54 / §56（最终方向 C：薄规范——§4.2 SCHEMA 字典即数据契约，不做容器运行时；§54.6 曾误记 B——B"不做二次抽象"太松散被否；不引入 C++ tensor 容器层结论不变）**

**问题**：既然用 backtrader / alphalens / vnpy，还要不要自己定义 udata 这层？

**候选方向**：
- A. **要**：udata 是统一数据契约，三件套都从 udata 取数（薄壳的一部分）
- B. **不要**：直接用 DataFrame / Arrow Table，不做二次抽象
- C. **部分要**：只做"合约主数据 + 标准字段命名"的薄规范，不做容器层
- D. **看场景**：回测/ML 用 udata（性能考虑），研究探索直接用 DataFrame

---

## 6. 因子的计算、存储与管理

现状：`factor_library/` 有 `Factor ABC` + `FactorRegistry` + `FactorDAG` + `PostProcessor`。新架构下这套要不要改、怎么改？

### 6.1 因子的定义与接口（P0）

> **状态（2026-10-04）：已定 → 见 §57（Factor ABC 保留微调——depends_on() 显式声明依赖（前缀 factor: 区分数据/因子）+ compute(data)→DataFrame[date,ticker,factor_name] 统一输出；元数据=类属性不做 JSON；4 类分类 time_series/cross_section/event/alternative；EventBrick/UFormula/DataPlugin 不引入只留 Python 路径）**

**问题**：因子的抽象接口是什么？（旧范式下因子=EventBrick，见 [§12.2](#122-ebgrapheventbrick-机制还建吗)）

**子问题**：
- 因子的输入（udata？DataFrame？多个依赖因子？）
- 因子的输出（Series？DataFrame？矩阵？）
- 因子的分类（横截面因子 / 时序因子 / 事件因子 / 另类因子）
- 因子的元数据（名称、描述、作者、版本、频率、适用资产、计算成本）
- 因子的依赖声明（依赖哪些其他因子 / 哪些数据）
- 三种实现路径（PyIndicator / PyAlgo、C++、UFormula）还要保留吗？

### 6.2 因子的计算调度（P1）

> **状态（2026-10-04）：已定 → 见 §58 / §59（DAG=depends_on()+Kahn 分层拓扑；缓存按因子×日期范围 key=name+version+range+depends_hash；增量 append/full 两模式；并行=层内 ProcessPoolExecutor；失败=fail-fast+下游跳过+同层继续不引入 5 色灯；ML=MLFactor train/predict 分离串行）**

**问题**：因子 DAG 的依赖调度、缓存、增量更新。

**子问题**：
- DAG 的构建（声明式依赖？静态分析？）
- 缓存策略（缓存粒度：按因子？按日期？按合约？缓存失效条件）
- 增量更新（新数据来了，只算新增部分，还是全量重算）
- 并行计算（独立因子并行算）
- 计算失败的处理（一个因子算失败，下游怎么办）
- 长耗时因子（ML 因子要跑模型）的调度

### 6.3 因子的后处理流水线（P1）

> **状态（2026-10-04）：已定 → 见 §16.2 / §60（PostProcessor Pipeline 四步：MAD 去极值→截面中位数填充→z-score/rank→OLS 中性化（个股池必须/ETF 不做）；存原始值消费时后处理；所有步骤 groupby(date) 截面独立防前视）**

**问题**：`PostProcessor` 现在是 winsorize → standardize → fillna。

**子问题**：
- 后处理的步骤集合（去极值 / 标准化 / 填充 / 中性化 / 正交化 / 行业市值中性）
- 每步的算法选型（winsorize 用 MAD 还是分位数？standardize 用 z-score 还是 rank？）
- 后处理的参数（窗口长度、分位数阈值）
- 后处理的**样本内 vs 样本外**（用全样本统计量会引入前视）
- 后处理在因子计算里做，还是在消费时做？

### 6.4 因子的存储布局（P0）

> **状态（2026-10-04）：已定 → 见 §27.4 / §27.8 / §61（因子值 `data_platform/factors/{name}/`、eval 序列 `data_platform/factor_eval/{name}/`、标量进 `factor_registry.yaml` 的 `eval` 块、tearsheet 进 `research/reports/`；存储布局=每因子一表 Direction A + 按年分区 + 语义化版本 v1.0/v2.0 + metadata.json + DOUBLE 精度 + Parquet/DuckDB 不用 ClickHouse）**

**问题**：因子表的 schema、分区、版本化。

**子问题**：
- **schema**：因子值矩阵怎么组织？
  - A. 每因子一表：`factors/{factor_name}/1d/year=YYYY/data.parquet`（现状）
  - B. 长表：`date / symbol / factor_name / value`
  - C. 宽表：`date / symbol` + N 个因子列
  - D. 三维张量（date × symbol × factor）
- **分区**：按因子 / 按日期 / 按资产 / 混合
- **版本化**：因子定义改了，历史值重算吗？版本怎么记录？
- **增量 vs 全量**：每日增量写入，每月全量重算？
- **存储位置**：`data_lake/factors/` 还是 ClickHouse？

### 6.5 因子的管理与治理（P1）

> **状态（2026-10-04）：已定 → 见 §27.9 / §27.11 / §62（代码即注册表 FactorRegistry+metadata.json；质量 8 指标 IC>0.04/IR>0.5/t>2 入库 IC<0.03 退出；三态 draft→production→deprecated；血缘=depends_on+DAG+depends_hash；命名 `{类型}_{语义}_{参数}` snake_case；文档=docstring+description；t=√(2lnN) 多重检验校正）**

**子问题**：
- 因子注册表的 schema（因子 ID / 名称 / 版本 / 负责人 / 创建时间 / 状态）
- 因子的质量指标（IC / ICIR / 换手率 / 衰减半衰期 / 覆盖率）
- 因子的生命周期（草稿 / 生产 / 废弃）
- 因子的血缘（用了哪些数据、哪些上游因子）
- 因子的命名规范（`momentum_20d` vs `mom20` vs `Momentum20`？）
- 因子的文档规范（每个因子要有说明吗）

### 6.6 因子的去重与合并（P2）

> **状态（2026-10-04）：已定 → 见 §16.5 / §62（detect_redundant_factors Spearman>0.7 标冗余；4 档处理 >0.9 留 IC 高者/0.7-0.9 逻辑不同则保留/0.5-0.7 警告/<0.5 独立；不做等权合并；deprecated 不物理删；production 容量 20-50 个）**

**子问题**：
- 相似因子的检测（相关性 > 0.9 的因子群）
- 冗余因子的处理（保留？合并？废弃？）
- 因子库的容量控制

### 6.7 因子的调试与排查（P2）

> **状态（2026-10-04）：已定 → 见 §62（五分支排查 全局→标的→日期→数值→IC + diagnose_factor()；单测四范式 精确对拍/Schema/NaN 鲁棒/test_no_lookahead；plot_factor_report() 四图 IC 时序/累计 IC/分布/分组收益）**

**子问题**：
- 因子值异常怎么定位（数据问题 vs 计算问题 vs 参数问题）
- 因子的单测怎么写（输入输出对拍）
- 因子的可视化（时序图、分布图、IC 曲线）

---

## 7. 多因子组合

**问题**：多个因子怎么合成一个最终 alpha 信号？这是从"因子表"到"Signal"的关键一步。

### 7.1 组合方法的选型（P0）

> **状态（2026-10-04）：已定 → 见 §16.3 / §16.4 / §63（FactorCombiner 主路径 equal_weight、ICIR 加权可选 lookback=60 月频更新 IC 只用 t-1；不做 IC 加权/回归 max ICIR/ML 合成（归 MLFactor）；跑不赢等权的复杂度=过拟合退回等权）**

**候选方法**：

| 方法 | 说明 | 适用 |
|---|---|---|
| 等权 | 所有因子标准化后等权相加 | 基线，稳健 |
| IC 加权 | 按历史 IC 加权 | 有 IC 估计 |
| IC_IR 加权 | 按 IC/IR 加权 | 风险调整后 |
| 最大化 IC_IR | 优化求解权重 | 样本内易过拟合 |
| 风险平价 | 按波动率倒数加权 | 分散风险 |
| 正交化后等权 | Gram-Schmidt / 对称正交，再等权 | 去冗余 |
| ML 组合 | 用 ML 学非线性映射 | 数据量大时 |
| 分层 | 先分类再组合（动量类 / 价值类 / 波动类） | 因子多时 |

**子问题**：
- 用哪种？要不要多种并存？
- 权重估计的窗口（滚动 60 日 IC？扩张窗口？）
- 权重的再平衡频率（每日？每周？每月？）
- 权重估计的样本内 vs 样本外（避免前视）

### 7.2 因子正交化（P1）

> **状态（2026-10-04）：已定 → 见 §16.5 / §63（不做独立正交化二次确认——3-5 因子入库已控相关<0.5；YAGNI，未来 >10 因子再考虑 csfacneutral 式中性化）**

**子问题**：
- 正交化方法（Gram-Schmidt / 对称正交 / 残差化 / PCA）
- 正交化的顺序敏感性（Gram-Schmidt 对顺序敏感）
- 正交化在**组合前**还是**后处理**里
- 正交化的频率（每日重算正交基？还是固定基）

### 7.3 风格/行业/市值中性化（P1）

> **状态（2026-10-04）：已定 → 见 §16.2 / §63（neutralize_factor() 逐日截面 OLS 残差、申万一级 31 行业哑变量 + log_market_cap 两维度；个股池必做 ETF 不做；组合层不做 Barra 行业/风格暴露约束）**

**子问题**：
- 中性化的层次（因子层中性化？组合层中性化？两层都做？）
- 中性化方法（回归残差 / 分组内标准化 / 打分约束）
- 风格因子集（市值 / beta / 动量 / 波动 / 流动性 / 杠杆 / 成长 / 价值 / 盈利）
- 行业分类来源（申万？中信？）
- 中性化的频率

### 7.4 因子合成与最终信号的关系（P0）

> **状态（2026-10-04）：已定 → 见 §16.6 / §63（combined_score float + 截面 z-score = AlphaSignal；signal_to_weights() top_k 等权默认 / proportional 可选；AlphaSignal.from_multi_factor / from_ml_factor 两条路径汇聚）**

**子问题**：
- 合成结果是什么（截面分数？方向？目标权重？）
- 合成结果 → Signal 的映射（是否经过 §2.4 的三层结构）
- 合成结果 → Portfolio 的映射（是否经过约束求解）
- 合成是 alphaType 的一部分，还是独立模块？（alstm 是端到端模型，传统多因子是合成式 —— 两条路径要不要统一）

### 7.5 因子组合的评估（P1）

> **状态（2026-10-04）：已定 → 见 §63（8 指标表 IC>0.04/IR>0.5/年化 10-15%/夏普>2 怀疑/MDD<20%/Calmar>1/换手 30-100%/因子贡献最强<50%；factor_contribution()；Walk-Forward 训练测试不重叠+测试集只看一次；eval_report.json → data/signals/{name}/）**

**子问题**：
- 评估指标（IC / ICIR / 分位收益 / 换手 / 最大回撤 / 容量）
- 归因（组合收益里每个因子贡献多少）
- 评估的样本外规范
- 评估结果落到哪（注册表？报告？）

---

## 8. 机器学习 / 深度学习策略中的信号管理

**问题**：alstm 这类 ML/DL 策略的信号生命周期与传统因子策略不同，需要专门的管理。

### 8.1 模型输出到 Signal 的映射（P0）

> **状态（2026-10-04）：已定 → 见 §64（截面分数默认/概率→logit→z-score/期望收益同分数；分位数与二值不支持；MLSignalAdapter.from_scores/from_probabilities；smooth_signal EWM halflife=5 日频可选月频不开；→ AlphaSignal 与多因子路径汇聚）**

**问题**：ML/DL 模型输出的是什么，怎么变成 Signal？

**子问题**：
- 模型输出的形态（截面分数 / 时序方向 / 概率 / 期望收益 / 分位数）
- 输出的**标定**（概率 → 仓位的映射；分数 → 手数的映射）
- 输出的**阈值化**（二值化？软阈值？还是连续）
- 输出的**稳定性**（小扰动 → 仓位大变？要不要平滑）
- 与 §2.4 Signal 三层的关系（alstm 走哪一层？）

### 8.2 特征管理（P0）

> **状态（2026-10-04）：已定 → 见 §64（特征=因子值+原始量价复用因子表不建特征表；build_feature_matrix(feature_specs)；features_hash 进 model_id 复用 depends_hash；人工筛选 5-15 个不做 PCA/Lasso；因子⊂特征）**

**问题**：ML 策略的特征（feature）是另一个数据维度，现在 `mass_feature_input.py` / `hft_factor_input.py` 是脚本化的。

**子问题**：
- 特征表的 schema（date × symbol × feature_name？）
- 特征的存储位置（和因子表同一套？还是独立？）
- 特征的版本化（特征定义改了，模型要重训）
- 特征的血缘（用了哪些原始数据、哪些因子）
- 特征的筛选与降维（250/281 个特征怎么管理）
- 特征的计算成本（实时算 vs 预计算）
- 特征与因子的关系（特征 = 因子？还是特征包含因子 + 原始量）

### 8.3 模型生命周期（P0）

> **状态（2026-10-04）：已定 → 见 §64（扩张窗口+年度重训默认 滚动 24 月备选；时间切分 Walk-Forward 禁 KFold；data/models/{name}/v{major}_{minor}/ + model_id=name+major+minor+train_end+features_hash；再训练年度+KS p<0.05 衰减触发；退役复用 §18.4；A/B 手动 t-test）**

**问题**：模型的训练、验证、上线、退役全流程。

**子问题**：
- **训练**：训练数据窗口（滚动训练？扩张窗口？固定窗口？）
- **验证**：训练/验证/测试的切分规范（时间切分？样本外切分？）
- **上线**：模型怎么部署（文件？服务？进程内）
- **再训练**：触发条件（定期？信号衰减？数据漂移？）
- **退役**：下线的判定（连续 N 日跑输？手动？）
- **版本化**：模型版本怎么记录（模型文件哈希？训练配置快照？）
- **A/B**：多版本模型并行怎么管

### 8.4 模型的实验管理（P1）

> **状态（2026-10-04）：已定 → 见 §64（experiments/{name}/ config.json+metrics.json+notes.md+model.pkl 不用 MLflow；晋升线 test_ic>0.04+test_ir>0.5+test_sharpe>1.0+优于等权；可复现五要素 random_seed/依赖版本/特征列表/日期范围/超参）**

**问题**：`alstm/` 下 35 个脚本是实验的海洋。

**子问题**：
- 实验的元数据（实验 ID / 假设 / 参数 / 数据范围 / 结果 / 结论）
- 实验的追踪（MLflow？自建？还是归档目录）
- 实验 → 变体 → alphaType 的晋升标准
- 实验结果的可复现（随机种子、环境快照）

### 8.5 多模型 ensemble（P1）

> **状态（2026-10-04）：已定 → 见 §64（ensemble_models() 信号层加权平均复用 FactorCombiner 逻辑，默认等权；不做模型层平均；对应 signal_source.type="ensemble"）**

**问题**：`hybrid.py` / `low_volatility_model_a_mixer.py` 是多模型组合。

**子问题**：
- ensemble 方法（加权平均 / stacking / 选择器 / 投票）
- 模型权重的估计（与 §7.1 因子组合权重是同一套逻辑？）
- ensemble 的层次（模型层 ensemble？信号层 ensemble？仓位层 ensemble？）
- ensemble 成员的进出（新模型加入、老模型退出的机制）

### 8.6 信号衰减与监控（P1）

> **状态（2026-10-04）：已定 → 见 §64（复用 §62.4 三态 healthy/warning/exit 不另起指标体系；rolling IC<50% 告警 <30% 退出；rolling 6 月夏普<0 退出；回测实盘偏差<70% 查执行；KS p<0.05 触发重训）**

**子问题**：
- 衰减的定义（IC 下降？预测能力下降？实盘偏离回测？）
- 衰减的监控指标（滚动 IC、预测偏差、实盘-回测偏差）
- 衰减的告警阈值
- 衰减触发的响应（降仓？停用？重训？）

### 8.7 ML 训练的基础设施（P2）

> **状态（2026-10-04）：已定 → 见 §64（本地 CPU/单 GPU 不建集群/任务队列/Web 看板；超参手动网格+attempt_log 记 N；实验看板=experiments/ JSON+matplotlib）**

**子问题**：
- 训练环境（本地 GPU？云端？）
- 训练的调度（手动触发？定时？数据触发？）
- 训练产物的存储（模型文件 / 训练日志 / 评估指标）
- 训练的资源管理（GPU 占用、训练队列）

### 8.8 样本外推与过拟合防控（P1）

> **状态（2026-10-04）：已定 → 见 §64（铁律四条 禁 KFold/测试集只看一次/必须跑赢等权/记 attempt_log；过拟合检测表 test_ic<0.5×train_ic SUSPECT、夏普>2 SUSPECT、未超等权 10% REJECT；sanity_check_ml_model()）**

**子问题**：
- 样本外推的规范（必须的时间切分？）
- 过拟合检测（训练-验证差距、样本外衰减）
- 正则化策略（dropout / L2 / early stopping / 数据增强）
- 滚动回测的规范（walk-forward 验证）

---

## 9. 投研域组织（alphaType / 注册表）

### 9.1 alphaType 注册表 schema（P0）

> **状态（2026-10-04）：已定 → 见 §65（alphaType=策略配方 signal_source+rules+params；dim-alphaType-meta-info 15 字段 JSON + dim-alphaType-publish 发布记录；存 data/registry/alpha_types.json + alpha_type_publishes.json 不用 SQLite/YAML；版本化 config_snapshot path@hash + git）**

**子问题**：
- `dim-alphaType-meta-info` 的字段（alphaType ID / 名称 / 主策略引用 / 算法模块引用 / 规则配方配置 / 创建时间 / 版本 …）
- `dim-alphaType-publish` 的字段（发布 ID / alphaType ID / 配方配置 / 标签 / 生命周期状态 / 审批记录 / 采纳时间 …）
- 两张表的存储载体（SQLite？ClickHouse？Parquet？YAML？）
- 版本化策略（配方配置改动怎么记录历史）

### 9.2 多维标签体系（P1）

> **状态（2026-10-04）：已定 → 见 §65（7 维全枚举禁自由文本：asset_type/frequency/holding_period/style/capacity/turnover/status；创建必填+发布校验+晋升可修正；list_alpha_types(filters)）**

**子问题**：
- 标签维度有哪些？（资产类型 / 频率 / 持仓周期 / 风格 / 容量 / 换手率 / 状态 …）
- 标签是枚举还是自由文本
- 标签的打标时机（创建时 / 发布时 / 运行时）
- 标签的查询场景

### 9.3 生命周期状态机（P1）

> **状态（2026-10-04）：已定 → 见 §65（五态 draft→published→production→deprecated→archived；晋升需 Walk-Forward 通过+§64.5 晋升线+优于等权；无审批流 promote_alpha_type() 自动检查 + log_audit → audit_log.jsonl append-only）**

**子问题**：
- 状态枚举（草稿 / 已发布 / 待审批 / 已采纳 / 已下线 / 已归档？）
- 转换条件和触发者
- 单人项目的 OA 审批怎么简化（本地状态？还是真的做审批流？）
- 状态变更的审计记录

### 9.4 实例层的规则库 API（P1）

> **状态（2026-10-04）：已定 → 见 §65（规则 4 分类 signal_transform/filter/position/risk_gate；Rule ABC apply(data,params)+param_schema 校验；RulePipeline 串联不支持并联嵌套；判据=改它只影响精细度→规则，变成另一策略→新 alphaType）**

**问题**：叠加 / gate / 风控规则做成参数化规则库，API 怎么设计？

**子问题**：
- 规则的接口（输入信号 → 输出信号？输入持仓 → 输出持仓？）
- 规则的组合语义（串联？并联？条件嵌套？）
- 规则的参数化（哪些参数可调，怎么进配方配置）
- 从 35 个脚本里提取规则的标准（什么算"叠加规则"、什么算"算法变体"）

### 9.5 Signal 产出的调度（P1）

> **状态（2026-10-04）：已定 → 见 §65（日频 15:30 后/月频月末/盘中不做；cron+手动不事件驱动 schedule_config "30 16 * * 1-5"；失败重试 1 次→hold 上一交易日 Portfolio→告警，不清仓；run_daily_signal()）**

**子问题**：
- 每日什么时机产出（收盘后？盘前？盘中多次？）
- 产出的触发（cron？手动？事件）
- 产出失败怎么办（上一日的 Portfolio 继续用？清仓？）

### 9.6 Portfolio 构建的约束求解（P1）

> **状态（2026-10-04）：已定 → 见 §17.1 / §17.2 / §34 / §65（权重分配在策略内四法；约束集=PortfolioConstraints 四类+手数硬约束；求解器=纯 numpy 裁剪，cvxpy 留给 risk parity/BL；build_portfolio 解析解+集中度/换手检查告警不修正；compute_turnover=Σ|Δw|/2）**

**子问题**：
- 约束集有哪些（换手上限 / 行业暴露 / 个股集中度 / 资金约束 / 杠杆上限）
- 求解器选型（解析解 / scipy.optimize / cvxpy / 自研启发式）
- 求解失败（约束不可行）怎么办
- 求解的性能要求（多少标的、多快出结果）

### 9.7 本金与手数取整（小资金约束）（P0）

> **状态（2026-10-04）：已定 → 见 §17.3 / §34.4（落地为 solve() 步骤 2：floor_to_lot + min_trade_value=5000 门槛 + max_total_weight=0.95 留 5% 现金）**

**问题**：权重分配输出连续小数，实盘手数是整数、本金有限、单价可能一手就超预算。

**子问题**：
- 连续最优解 → 整数可行解的投影怎么做（向下取整 / 整数规划 / 门槛过滤）
- cash drag（买不掉的现金）上限多少算可接受
- lot_size 主数据（A股 100 / 港股 100-1000 / 期货合约乘数）从哪来
- 本金决定持仓数上限 —— 小资金要不要用 ETF 替代个股

---

## 10. 执行域

### 10.1 订单生成（Utrade 等价物）（P1）

> **状态（2026-10-04）：已定 → 见 §23 / §24.1 / §66（compute_order_diff 目标权重 diff + floor 手数取整 lot_size=100；挂单 V1 不处理；拆单不做 check_split_needed 仅告警（教程 §17.5 >1% 日成交量）；先卖后买+卖按市值 desc+买按信号 desc；LIMIT limit_price=close；pandas 自研 <300 行不用 backtrader/vnpy）**

**子问题**：
- 差分计算（目标持仓 - 当前持仓 = 调仓量）的规则
- 查单（已有未成交订单怎么处理：撤销？等待？）
- 订单拆分（大单拆小？时间切片？）
- 订单优先级（先平仓后开仓？按流动性排序？）
- 这层用 backtrader 的 Broker 还是自研？

### 10.2 风控（Themis 等价物）（P1）

> **状态（2026-10-04）：已定 → 见 §24.6 / §66（风控七规则表 单标的 25%/总仓位 95%/换手告警/单笔≥5000/禁做空/禁 ST/流动性告警；只 pre-trade 不 intra-trade；只 PASS/BLOCK 两级不做约束态；RiskCheckResult 一 BLOCK 即拒；risk_audit.jsonl 审计）**

**子问题**：
- 风控规则集（单标的集中度 / 总杠杆 / 日内回撤 / 熔断 …）
- 风控的执行时机（下单前预检？盘中实时监控？）
- 触发风控的动作（拒单 / 减仓 / 清仓 / 告警）
- 风控日志和审计

### 10.3 模拟账本（TB 等价物）（P1）

> **状态（2026-10-04）：已定 → 见 §18 / §23 / §66（撮合=收盘价成交 V1 无价 REJECTED；费用四件套 佣金万 2.5 双边最低 5 元/印花税万 5 卖出 ETF 免/过户万 0.1/滑点 0.1%；FeeModel._is_etf 51/15/16 前缀；SimAccount cash/holdings/cost_basis/trade_log；ETF 单边 0.15-0.2% 股票 0.35-0.4%）**

**子问题**：
- 撮合模型（对手价成交？滑点模型？）
- 手续费模型（按手数 / 按金额 / 阶梯费率）
- 资金账户模型（保证金 / 可用资金 / 持仓盈亏）
- 多品种（股票 T+1 / 期货保证金 / 期权）怎么统一
- 这层用 backtrader 的 Broker 还是自研？

### 10.4 柜台抽象接口（P2）

> **状态（2026-10-04）：已定 → 见 §22.5 / §24.4 / §66（**修正早期『只用 vnpy gateway』结论**——Broker ABC 4 方法 submit_orders/get_holdings/get_cash/get_nav；SimBroker 封装风控+撮合+费用+记账；V2 路径 QMT>easytrader>券商 API 不做 vnpy Gateway；create_broker(mode,config) 工厂；详见 §68 旧决策重审）**

**子问题**：
- 抽象接口签名（下单 / 撤单 / 查持仓 / 查资金 / 订阅回报）
- vnpy Gateway 怎么映射到这个抽象
- 模拟柜台和真实柜台的切换

---

## 11. 运维域

### 11.1 任务调度（P1）

> **状态（2026-10-04）：已定 → 见 §67（APScheduler+薄封装 V1 不用 Prefect/Celery/Airflow/crontab；5 类 Job；CronTrigger 16:45 周中；不用 DAG Job<10 线性；job_runs.jsonl 四态记录；重试默认 1 次 300s 数据 2 次；失败传导 数据→跳过下游/信号→hold 不清仓）**

**子问题**：
- 调度器选型（Prefect / cron / 自研 / Celery）
- 这个和数据 Job 编排（§3）是同一个问题，要统一
- 运行记录的存储
- 失败重试和告警联动

### 11.2 监控指标体系（P2）

> **状态（2026-10-04）：已定 → 见 §67（策略级 8 指标三档健康/警告/退出 + 系统级 4 指标；metrics.jsonl；Markdown 日报 data/ops/reports/daily_{date}.md 不做 Web V2 Streamlit）**

**子问题**：
- 策略级指标（持仓 / 盈亏 / 回撤 / 换手 / 信号衰减）
- 系统级指标（Job 成功率 / 延迟 / 数据质量分）
- 指标的存储和展示（本地看板？推送？）

### 11.3 归因方法（P2）

> **状态（2026-10-04）：已定 → 见 §67（Alpha-Beta 单因子回归 V1 默认/FF3 季度备选/Brinson+Barra 不做；alpha_beta_attribution() alpha_annual=×12；factor_contribution 复用 §63.6；月自动+季手动+专项按需；解读三线 Alpha 3-5% 优秀/R²>0.7 可信/Alpha>8% 查问题）**

**子问题**：
- 归因模型（Brinson？因子归因？单因子贡献？）
- 归因的频率和粒度
- 归因报告的产出形态

### 11.4 告警（P2）

> **状态（2026-10-04）：已定 → 见 §67（3 类阈值/突变/趋势 + 8 具体条件 CRITICAL 2 条；通道=终端全级别+桌面通知 CRITICAL+邮件 V2，飞书/Grafana 不做；send_alert+log_alert → alerts.jsonl；去重 {date}_{alert_type}_{alpha_type_id}；severity 只升不降+白名单，不做动态升级）**

**子问题**：
- 告警规则（阈值 / 突变 / 趋势）
- 告警通道（邮件？桌面通知？IM？）
- 告警的分级和去重

---

## 12. 受转向影响需重审的既有决策（P0）

ARCHITECTURE.md 里这些结论因"内核降级为薄壳"可能失效，需要逐条确认。

### 12.1 四层时间模型还建吗？

> **状态（2026-10-04）：已定 → 见 §20.2 / §68（两层时间模型之上再明确：四层时间模型 starfleet→EBReplay→EBClock→EBTimer **废弃**——for-loop 逐再平衡日推进；"计算量与数据量解耦"月频天然消失；backtrader/vnpy 事件驱动不采用）**

**原结论**：starfleet 轮询 → EBReplay 日调度 → EBClock μs 墙钟 → EBTimer TI 网格，EndTI 驱动信号。

**问题**：
- 薄壳方案下还要自己建时间模型吗？还是用 backtrader 的 Cerebro 循环 + vnpy 的事件引擎？
- 如果不用，"EndTI 驱动信号计算"这个设计（计算量与数据量解耦）怎么实现？
- 如果用，backtrader / vnpy 的时间推进怎么和这套时间模型对齐？

### 12.2 EBGraph / EventBrick 机制还建吗？

> **状态（2026-10-04）：已定 → 见 §20.3 / §20.4 / §68（EBGraph/EventBrick **废弃**→Factor ABC+FactorDAG；事件订阅动态 push vs 拓扑静态 pull 对比表；三理由 月频无实时事件/依赖静态/DataFrame 天然批量；组织链 Factor→DAG→PostProcessor→per-factor-one-table→Combiner→AlphaSignal=DataFrame 流水线）**

**原结论**：运行时事件订阅图，节点=EventBrick，边=事件订阅关系。

**问题**：
- 薄壳方案下还要这套插件机制吗？
- 因子还是 EB 吗？还是变成 alphalens 的输入 DataFrame？
- 如果不建，因子的组织方式是什么？（回到 `factor_library` 的 Factor ABC？还是新的形态？）

### 12.3 五域骨架还成立吗？

> **状态（2026-10-04）：已定 → 见 §68（**修正保留**：kernel/→core/ 从"引擎"变"共享工具" config.py/registry.py/logging.py/types.py/asset_rules.py 无引擎无事件循环无时间模型；五域划分与边界不重画——时间推进→回测脚本 for-loop、事件分发→不需要、Signal→AlphaSignal、数据接口→DataAPI）**

**原结论**：`kernel/` `data_platform/` `research/` `execution/` `ops/`。

**问题**：
- `kernel/` 变成薄壳后，是叫 `kernel/` 还是改名（如 `shell/` / `platform/` / `core/`）？
- 薄壳的内容（产品契约 + 数据接口 + 适配层）横跨了原 data_platform / research / execution 的职责，边界要重画吗？
- 还是五域不变，只是 kernel 的实现变薄？

### 12.4 产品契约还成立吗？

> **状态（2026-10-04）：已定 → 见 §32 / §34 / §68（Signal 三层坍缩为一层 AlphaSignal；Portfolio 自研 build_portfolio；**Utrade/Themis/TB 对应物=broker.submit_orders() 内部三步（风控+撮合+记账）不是三个模块**；五层叙事→三步 Signal→Portfolio→Execution；接口边界从"产品"移到"函数"）**

**原结论**：Signal 三层流式 + Portfolio 目标持仓矩阵，替代 5 层流水线叙事。

**问题**：
- Signal 三层（Blueshift/USignal/Tick）是公司 pysim 的形态，我们的场景需要三层吗？还是一层就够？
- Portfolio 是目标持仓矩阵 —— backtrader 的输出是订单和成交，中间的"目标持仓"层是我们自研还是 backtrader 提供？
- 新叙事 `Signal → Portfolio → Utrade → Themis → TB` 里的 Utrade/Themis/TB 现在对应什么？（backtrader 的 Broker？vnpy 的 Gateway？自研？）

### 12.5 存储选型还成立吗？

> **状态（2026-10-04）：已定 → 见 §20.1 / §68（Parquet+DuckDB 双件套，ClickHouse **砍掉**二次确认——单机 <10GB 独立部署违反零外部依赖；公司 CK 是实盘多策略并发需求；框架存储偏好表 alphalens 直接兼容）**

**原结论**：DuckDB（研究层）+ ClickHouse（资产层）+ Parquet（raw）。

**问题**：
- backtrader 的 DataFeed 和 alphalens 的 DataFrame 从哪读？（DuckDB？Parquet？）
- ClickHouse 还需要吗？（单机场景下 DuckDB 是否够用）
- 三件套有各自的存储偏好，要不要迁就？

### 12.6 验证策略的黄金样本基线还适用吗？

> **状态（2026-10-04）：已定 → 见 §20.5 / §68（黄金样本**废弃重建**——旧 baseline_spec.py sharpe 16 不可信/12686 笔暗示高频/期货 vs A 股不适用，保留作期货 ALSTM 回归基准；新基线=等权 top-5 ETF 动量 教程 §2 变体 ≤30 行 akshare 可重现；五步验证 手算→pandas→SimBroker→扣费→五年基线；预期年化 10-15%/夏普 0.8-1.2/MDD 15-25%，超范围排查表）**

**原结论**：用 `strategies/futures/alstm/baseline_spec.py` 的基准（sharpe 16.001135 / total_return 0.860638 / trade_count 12686）做黄金样本。

**问题**：
- 那组基准是现有 VectorBT/ZVT 引擎算的，换 backtrader 后基准值必然不同
- 黄金样本是"复现旧结果"还是"建立新基线"？
- 如果是新基线，怎么证明新基线可信？

---

## 13. 横切问题

### 13.1 配置管理（P1）

> **状态（2026-10-04）：已定 → 见 §35.6 / §69（修正 §35.6：YAML 一个文件 → TOML 三层 config.toml+strategies/{id}.toml+.env；tomllib 零依赖；YAML on→True 陷阱不选；敏感信息 .env 不入 git 不打印；版本化审计=git log+config_snapshot；不做配置中心；不热更新保留）**

**子问题**：
- 配置文件格式（YAML / TOML / XML / Python dataclass）
- 配置分层（全局 / 域级 / 策略级 / 环境级）
- 敏感信息（API key / 数据库密码）怎么管
- 配置的版本化和审计

### 13.2 日志与可观测性（P1）

> **状态（2026-10-04）：已定 → 见 §69（Python logging+JsonFormatter JSON Lines 五字段 ts UTC/level/module/func/msg；console 人读+logs/app_{date}.jsonl 机器查；不需要分布式追踪——单进程 duration_ms+grep+jq；logs/runs/ 三 schema jobs/backtests/live.jsonl，backtests 含 config_snapshot path@hash）**

**子问题**：
- 日志规范（格式 / 级别 / 字段）
- 分布式追踪（单机也需要吗）
- 运行记录（Job / 回测 / 实盘的运行元数据）存哪

### 13.3 测试策略（P1）

> **状态（2026-10-04）：已定 → 见 §69（pytest 三层 tests/unit|integration|golden；9 模块单测+hypothesis 性质测试 FeeModel fee≥0/cash+mkt_value==nav 等；测试替身表 akshare→fixture Parquet/QMT→SimBroker；golden fixtures golden_prices.parquet+golden_expected.json TOLERANCE=1e-4；V1 无 CI 本地 pytest unit<10s/integration<60s/golden<5min，V2 GitHub Actions）**

**子问题**：
- 黄金样本放哪、怎么跑（见 §12.6）
- 性质测试的覆盖范围
- 集成测试怎么搭（三件套的测试替身）
- CI 流程

### 13.4 多资产的区分（P1）

> **状态（2026-10-04）：已定 → 见 §69（配置区分+core/asset_rules.py ASSET_RULES 规则表+execution 层处理差异；不学 C++ Traits 编译期多态——月频 dispatch 可忽略；差异落层表 T+1→generate_orders/涨跌停做空→risk_check/印花税手数→FeeModel/ST→research/板块→data ticker；V1 股票+ETF→V2 可转债→V3 期货→V4+ 期权）**

**子问题**：
- 股票 / 期货 / 期权 / 另类 在新架构里怎么区分？
- 是分目录、分配置、还是分实例？
- 不同资产的交易规则差异（T+1 / 保证金 / 涨跌停）在哪层处理？

---

# 第三部分 讨论结果

> 记录已解决的问题及其结论。按主题组织，与第二部分章节对应。
> **第一批（2026-10-04）**：§16 多因子合成 / §17 仓位权重分配 / §18 成交成本与撮合建模
> **第二批（2026-10-04）**：§19 回测-实盘一致性与薄壳 / §20 架构与选型复审 / §21 数据输入补充
> **第三批（2026-10-04）**：§22 路线选型与薄壳契约 / §23 账本数据模型 / §24 适配层实现 / §25 薄壳边界与数据接口
> **第四批（2026-10-04）**：§26 截面策略的回测路径（自研截面组合模拟器）
> **第五批（2026-10-04）**：§27 Alphalens 接入架构与因子评价流水线
> **第六批（2026-10-04）**：§28 六套引擎收敛与三件套版本 / §29 事件系统设计
> **第七批（2026-10-04）**：§30 EventBrick 等价物 / §31 DataBus 不需要 / §32 Signal 三层 Schema / §33 L2 两条路径
> **第八批（2026-10-04）**：§34 Portfolio Schema 与组合约束求解（TargetPortfolio + PortfolioOptimizer）
> **第九批（2026-10-04）**：§35 Service 契约与配置管理（Service 层不需要 + YAML 单文件）
> **第十批（2026-10-04）**：§36 DQC 四维规则集（44 条规则 + pandera 执行模型）
> **第十一批（2026-10-04）**：§37 硬错误 vs 软警告的判定边界（三级硬粒度 + quality_flag 位掩码）
> **第十二批（2026-10-04）**：§38 DQC 执行时机与性能 + 报告与可追溯（pre-load 一遍 + 双表落盘）
> **第十三批（2026-10-04）**：§39 隔离区与 raw 层（quarantine + 原样落盘 + Normalizer 分层）
> **第十四批（2026-10-04）**：§40 SourceAdapter 接口签名定稿（四方法 + watermark 增量 + 三层限流）
> **第十五批（2026-10-04）**：§41 多源冲突处理（主源指定 + 跨源校验 + 备源补数）——回应 §3.8 五子问题（#134–#137 为 §40 重复跳过，#138–#140 为本批新内容）
> **第十六批（2026-10-04）**：§42 交易日历/时区/合约换月/夜盘归属 + §43 复权处理（asset 层口径）——回应 §3.9 四子问题全解 + §3.10 部分（#141–#149，#142/#143 重复取一，#144 为 §41 重复跳过）
> **第十七批（2026-10-04）**：§44 复权处理（adjfactor 校验 + 研究层/回测/实盘口径）——完成 §3.10 四子问题（#150–#153 为 §42 重复跳过，#154–#155 为本批新内容）
> **第十八批（2026-10-04）**：§45 数据回溯修正（检测→应用→影响传播）——回应 §3.11 四子问题全解（#156–#162）
> **第十九批（2026-10-04）**：§46 三套存储职责边界补全（两层 Parquet+DuckDB + DataAPI + 通用 schema 约定）——补全 §4.1（#163/#167–#169 为 §45 重复跳过，#164–#166/#170 为本批新内容）
> **第二十批（2026-10-04）**：§47 表 schema 定稿（11 张核心表 + pandera 校验）——回应 §4.2 五子问题全解（#171–#184）
> **第二十一批（2026-10-04）**：§48 分区/文件组织/压缩编码 + §49 事务并发/生命周期容量——回应 §4.3–§4.6 四问十九子问题全解（#185–#193，#187/#188/#189 为 §47 重复跳过；补录上批尾部漏落盘的 §4.3/§4.4 细节）
> **第二十二批（2026-10-04）**：§50 事务性与并发详版（UdataSQL 三阶段映射 + FileLock + 幂等重跑 + 可见性时序）——§4.5 完整讨论稿（#194–#199）
> **第二十三批（2026-10-04）**：§51 数据生命周期与容量详版（Class 1-4 分级保留 + cleanup_expired + 容量重估 45GB + 备份/RTO/RPO）——§4.6 完整讨论稿（#200–#208）
> **第二十四批（2026-10-04）**：§52 查询与访问层（SQL-only DataAPI + 缓存/慢查询/多进程约定 + 研究层性能）+ §53 与三件套的数据对接（load_bars + to_xxx 适配层 + 口径归一）——§4.7/§4.8 全解（#209–#223，#211/#212/#213/#216/#217/#218/#219/#220 为重复跳过）
> **第二十五批（2026-10-04）**：§54 udata 全解与不引入决策（Schema+容器抽象 / BarData 三维矩阵 / DataBus 上下游 / k-slice + 概念映射总表）——§5.1/§5.6 全解 + §5.2–§5.5 概念映射（#224–#229）
> **第二十六批（2026-10-04）**：§55 udata schema 收尾（字段精度 DOUBLE/INT64 决策 + Parquet schema evolution 扩展策略）——§5.2 完结（#230；#231/#232 为 §54 重复跳过）
> **第二十七批（2026-10-04）**：§56 udata 章收口（schema 版本化不引入显式机制 + 容器层组织 6 类映射 + 第 5 章决策汇总总表）——§5.2.6/§5.3 完结 + §5.6 修正为方向 C（#233/#234 重复跳过；#235/#236）
> **第二十八批（2026-10-04）**：§57 因子的定义与接口（Factor ABC + depends_on/compute→DataFrame + 元数据/分类/三路径只留 Python）+ §58 因子的计算调度（DAG 声明式+Kahn / 缓存 depends_hash / 增量 append+full）——§6.1 全解 + §6.2 大部（#237–#246）
> **第二十九批（2026-10-04）**：§59 因子计算调度收口（层内 ProcessPoolExecutor 并行 + fail-fast 失败处理 + MLFactor train/predict 分离）+ §60 因子后处理流水线全解（MAD/z-score/中性化四步 Pipeline + 存原始值决策 + 截面防前视）——§6.2 收口 + §6.3 升级为全解（#247–#260）
> **第三十批（2026-10-04）**：§61 因子的存储布局全解（每因子一表 Direction A + 按年分区 + 语义化版本 v1.0/v2.0 + metadata.json + 增量 append/全量两模式 + Parquet/DuckDB 不用 ClickHouse + 宽表等价性）——§6.4 升级为全解（#261/#262/#263 重复跳过；#264–#269）
> **第三十一批（2026-10-04）**：§62 因子治理层全解（代码即注册表 FactorRegistry+metadata.json + 质量 8 指标入库/退出双线 + 三态生命周期 + 血缘三件套 + 命名文档 + Spearman>0.7 去重 4 档 + 容量 20-50 + 五分支排查 + 单测四范式 + 四图报告）——§6.5 升级为全解 + §6.6 升级为全解 + §6.7 已定（#270–#280，#275/#280 决策汇总重复；#273/#274/#279 属 §6.7 乱序）
> **第三十二批（2026-10-04）**：§63 多因子组合全解（FactorCombiner 等权主路径+ICIR 可选 + 不做正交化/Barra + neutralize_factor 行业市值两维 + AlphaSignal 双路径汇聚 + signal_to_weights + 8 指标评估 + eval_report.json）——§7.1–7.4 升级为全解 + §7.5 已定（#281–#287 + #302 FactorCombiner 属 §7.1）
> **第三十三批（2026-10-04）**：§64 ML/DL 信号管理八问全解（输出映射 MLSignalAdapter + 特征复用因子表 + 模型生命周期 data/models 版本化 + experiments/ JSON 实验管理 + ensemble 信号层合并 + 衰减监控复用 §62.4 + 本地 CPU 不建基础设施 + 铁律四条防过拟合）——§8.1–8.8 全部已定（#288–#297 + #303–#305，#296 与 #295 重复）
> **第三十四批（2026-10-04）**：§65 alphaType 全解（注册表 JSON 双表 + 7 维全枚举标签 + 五态生命周期自动审计 + Rule ABC 规则库 + cron 调度失败 hold + 解析解约束求解）——§9.1–9.5 已定 + §9.6 升级为全解（#298–#301 + #306–#312 + #338/#339，#302 归 §63 已述）
> **第三十五批（2026-10-04）**：§66 执行域四问全解（compute_order_diff 订单差分 + 风控七规则 PASS/BLOCK + 收盘价撮合费用四件套 + SimAccount/SimBroker + Broker ABC QMT V2 路径 + <500 行回测主循环）——§10.1–10.4 升级为全解（#313–#321 + #338/#340 代码页，#339 归 §65 已述；**修正 §22.5/§24.4 vnpy gateway 旧结论**）
> **第三十六批（2026-10-04）**：§67 运维域四问全解（APScheduler 5 类 Job + job_runs.jsonl + 8+4 监控指标 + Markdown 日报 + Alpha-Beta 归因解读三线 + 告警 3 类 8 条通道分层）——§11.1–11.4 已定（#320–#324 + #347–#354）
> **第三十七批（2026-10-04）**：§68 旧决策重审六条全解（四层时间模型/EBGraph 废弃 + 五域 kernel→core 修正保留 + Signal 三层坍缩+Utrade/Themis/TB 坍缩为 submit_orders 内部三步 + ClickHouse 砍掉 + 黄金样本废弃重建为等权 top5 ETF 动量五步验证）——§12.1–12.6 全部已定（**P0 尾项 §12.4 关闭**；#325–#328 + #355–#358）
> **第三十八批（2026-10-04）**：§69 横切问题四问全解（TOML 三层+.env 配置修正 §35.6 + logging JSONL 三 schema 运行记录 + pytest 三层 hypothesis 黄金样本 + ASSET_RULES 多资产规则表 execution 层处理差异）——§13.1–13.4 全部已定（#329–#337 + #359–#360）
> 本部分保留完整推理与坑点，便于实施时直接照做。仍未解决的子问题在各小节末尾列出。

## 16. 多因子合成

> 回应 §6.3（后处理）、§6.6（去重）、§7.1（组合方法）、§7.2（正交化）、§7.3（中性化）、§7.4（合成与信号关系）

### 16.1 为什么合成，合成的是什么

手上有通过单因子检验的因子（`mom_20d`、`value_pe`、`lowvol_60d` …），每个单独看 Rank IC 都 > 0.04。问题来了：下单时不能同时听 10 个因子的，得把它们捏成一个 `score`，按 `score` 排序选股/定权重。

合成要解决两个核心矛盾：

1. **量纲不同**：动量是涨幅 %，估值是 PE 分位，波动率是标准差 —— 不标准化没法相加。
2. **信息重叠**：两个因子相关性 0.8，合在一起等于把同一个赌注下两遍，风险没分散反而集中。

**所以合成的本质是**：先把每个因子洗干净、拉到同一量纲（预处理），再按"谁更靠谱"分配权重（加权），同时压掉重叠（去相关）。

### 16.2 合成前必做：预处理（回应 §6.3 / §7.3）

合成前每个因子都要在**每个截面**（每个交易日，所有票）上做：

```text
原始因子
  → [1] 去极值：超 3 倍 MAD winsorize 到边界    （防几只极端票绑架整个因子）
  → [2] 标准化：截面 z-score                    （拉到均值 0 方差 1，可相加）
  → [3] 中性化：回归删除 行业 + log(市值)        （个股池必做，ETF 池可省）
  → 干净因子 z_i
```

**必须逐截面做**（每天单独标准化），不能全样本一次性标准化 —— 否则用到了未来的分布信息，又是 look-ahead。中性化那步：不做的话合成出来的 `score` 可能全是行业/规模 Beta 的假象。

**落地**：写一个 `preprocess(factor_df) = 去极值(3 MAD) → 逐截面 z-score → (个股池加中性化回归)`。用 pandas/numpy 手写即可，逻辑简单。

### 16.3 四种合成方法，从简到繁（回应 §7.1）

#### 方法 1：等权合成（Equal Weight）— 首选起点

```text
score = z_1 + z_2 + ... + z_n    （每个因子 z-score 后直接相加）
```

- **优点**：零参数、不会过拟合、极其稳健。这是最被低估的方法 —— 机构大量实盘组合就是等权。
- **缺点**：好因子和差因子一视同仁。
- **什么时候用**：**永远先跑等权作为 baseline**。任何更复杂的方法，如果跑不赢等权，就说明复杂度纯属过拟合，直接退回等权。

#### 方法 2：IC 加权（IC / ICIR Weighting）— 最常用

让历史上更有效的因子占更大权重：

```text
w_i = 因子 i 的历史 Rank IC 均值            （IC 加权）
或
w_i = 因子 i 的 ICIR = IC 均值 / IC 标准差   （ICIR 加权，更稳，推荐）

score = Σ w_i × z_i
```

- **优点**：符合直觉（强因子多给权重），ICIR 加权还惩罚了不稳定的因子。
- **关键坑**：算 `w_i` 用的 IC **必须是滚动窗口的历史 IC**（比如用过去 12 个月的 IC 定今天的权重），**绝不能用全样本 IC** —— 用全样本就是拿未来表现给过去分配权重，又是 look-ahead。这是 IC 加权最常见的翻车点。

#### 方法 3：回归 / 最大化 ICIR（带协方差，处理相关性）

前两种没处理"因子间相关"。如果两个因子相关 0.8，IC 加权会给这对冗余因子过高的合计权重。更严谨的做法引入因子间协方差矩阵：

```text
最优权重 w* ∝ Σ⁻¹ · IC
（Σ = 因子收益的协方差矩阵，IC = 因子预测能力向量）
```

这本质是均值-方差优化在因子层的应用 —— 在给定预测能力下，让因子组合的信息比最大化，自动给冗余因子降权。

- **优点**：理论最优，自动去相关。
- **致命缺点**：`Σ⁻¹`（协方差矩阵求逆）极不稳定 —— 协方差矩阵估计误差会被求逆放大，经常算出极端权重（某因子 +300%，另一个 −250%）。样本外表现常常还不如等权。这是"Markowitz 优化在实践中失灵"的经典现象。
- **实操**：**必须加收缩估计**（shrinkage，如 Ledoit-Wolf）或权重约束（非负、上下限）才敢用。**个人平台不建议一上来就用这个。**

#### 方法 4：机器学习合成（LightGBM / 线性模型直接学）

不手动定权重，直接把 N 个因子当特征，未来收益当标签，让模型学映射：

```text
未来收益 ~ f(z_1, z_2, ..., z_n)    f 用 LightGBM / Ridge / Lasso
score = 模型预测的未来收益
```

- **优点**：能捕捉非线性 + 因子交互（如"高动量 **且** 低波动"才有效），这是线性加权做不到的。机构主流。
- **缺点**：过拟合风险最高，需要严格的样本外 walk-forward 验证 + 正则；可解释性差。
- **重要提醒**：ML 合成会再次放大**多重检验问题** —— 模型有一堆超参（树深、学习率、特征数），每调一次都是一次尝试，N 又上去了。而且极易踩"**模型级泄漏**"（用了未来才有的数据训练）。ML 合成必须配 walk-forward + 严格样本内外隔离。

### 16.4 方法怎么选：一张决策表

| 你的情况 | 推荐方法 |
|---|---|
| 刚起步 / 因子少（< 10）/ 求稳 | **等权**，先建 baseline |
| 因子有明确强弱、想让强因子多说话 | **ICIR 加权**（滚动窗口）— 性价比最高 |
| 因子间相关性高、想去重 | **回归法 + 收缩估计**（别用裸协方差求逆） |
| 因子多、想抓非线性、有严格验证能力 | **LightGBM**，但必须 walk-forward + 正则 |

**铁律**：每种复杂方法都要和等权对比。**跑不赢等权的复杂度就是过拟合，退回等权。**

### 16.5 自动挖掘因子的去重（回应 §6.6 / §7.2）

AI 自动挖掘的特殊问题：挖出来的因子往往**高度相关**（一万个表达式里很多是同一个 alpha 的变体）。所以自动挖掘 + 合成的正确姿势是：

```text
挖掘出几十个幸存因子
  → 相关性聚类：相关 > 0.7 的归为一类，每类只留 IC 最高的一个   ← 关键去重
  → 剩下低相关的一组因子
  → 用 ICIR 加权 或 LightGBM 合成
```

**先去相关再合成**，否则自动挖掘产出的冗余因子会让任何加权方法都失真。这也是为什么单因子入库判据里有"和现有因子相关性 < 0.5"那一条 —— 它在合成阶段的价值就体现出来了。

> **§7.2 的结论**：不做 Gram-Schmidt / PCA 正交化，改用**相关性聚类去重**（> 0.7 归一类，留 IC 最高）。正交化对顺序敏感、且会扭曲因子经济含义，聚类去重更简单可靠。

### 16.6 合成之后：score → 持仓（回应 §7.4）

合成出综合 `score` 后，还差最后一步变成实际持仓：

```text
综合 score
  → 排序 / 分组        （Top N 选股，或按 score 分位）
  → 定权重            （等权 / score 加权 / 风险平价 / 组合优化）
  → 加约束            （行业暴露上限、个股上限、换手率约束、Beta 中性）
  → 目标持仓 weights[t]
  → weights.shift(1) 配下期收益   ← 又是防未来函数的那行 shift
```

组合优化那一步（带约束的 mean-variance）是可选的进阶，**个人平台第一版用"Top N 等权"或"score 加权"就够**。

### 16.7 最小可行方案

1. **预处理**：写一个 `preprocess(factor_df)` = 去极值(3 MAD) → 逐截面 z-score →（个股池加中性化回归）
2. **合成 v1**：等权求和，建 baseline
3. **合成 v2**：滚动窗口 ICIR 加权（过去 12 期 IC 定权重），和 v1 对比
4. **去重**：合成前先做因子相关性矩阵，聚类去冗余（尤其自动挖掘的因子）
5. **跑赢等权**才升级到收缩回归 / LightGBM

**开源工具**：预处理和加权用 pandas/numpy 手写即可（逻辑简单）；ML 合成用 `lightgbm`；组合优化用 `cvxpy` 或 `PyPortfolioOpt`。

**本节仍未解决**：无——权重再平衡频率已定 → §65（frequency 枚举 1d/1W/1M 参数化，V1 月频：§68.7 月末调仓 / §69 monthly 示例）；合成是 alphaType 的一部分还是独立模块已定 → §63.2 / §65（FactorCombiner 独立类，alphaType 仅带 combiner_method 参数）（2026-10-10 回改）。

---

## 17. 仓位权重分配与优化

> 回应 §9.6（Portfolio 约束求解）、§9.7（本金与手数取整，本批新增）

### 17.1 先分清两个层次：先分"多少钱"，再分"买几手"

仓位分配其实是两个前后相连的问题，很多人混在一起才搞乱：

| 层次 | 问题 | 输出 | 性质 |
|---|---|---|---|
| **第一层：权重分配** | 每个标的配多少比例的钱 | `weights = [0.05, 0.03, ...]` 连续小数 | 连续优化问题 |
| **第二层：手数取整** | 按本金把比例换成整数手 | `lots = [200股, 100股, ...]` 整数 | 离散约束问题 |

**教科书 99% 只讲第一层**（连续权重），但实盘下单时，第二层的整数约束会把第一层的"最优解"打得面目全非 —— 尤其本金小的时候。

### 17.2 第一层：权重分配方法（从简到繁）

#### 方法 1：等权（Equal Weight）

选出 Top N 只，每只 `w = 1/N`。

- 零参数、极稳健、不会过拟合。**永远的 baseline**，任何复杂方法要先跑赢它。
- **缺点**：忽略了个股风险差异（高波动票和低波动票配一样多钱，风险贡献其实不等）。

#### 方法 2：score 加权（信号加权）

```text
w_i = score_i / Σ score_j    （按综合 score 大小分配）
```

- 让信号更强的票配更多钱，符合直觉。
- **缺点**：同样不管风险；`score` 有噪音时权重也抖。

#### 方法 3：风险平价（Risk Parity）— 强烈推荐

不按钱平均，按**风险贡献**平均：让每个标的对组合波动的贡献相等。

简化版（忽略相关性，按波动率倒数）：

```text
w_i ∝ 1 / σ_i    （波动越大的票配越少钱）
```

完整版：求 `w` 使得每个标的的边际风险贡献相等（需要协方差矩阵）。

- **优点**：天然分散风险，高波动票自动减仓。长期夏普通常比等权好，而且比 mean-variance 稳健得多。
- **缺点**：完整版要协方差矩阵（但比 mean-variance 对误差不敏感，因为不求逆放大）。
- **对个人平台**：**波动率倒数加权（简化版）是性价比最高的选择** —— 只要每只票的历史波动率，一行代码，效果显著优于等权。

#### 方法 4：均值-方差优化（Markowitz）— 理论最优，实践危险

对应因子层的方法 3：`w* ∝ Σ⁻¹ · μ`，同样有 `Σ⁻¹` 不稳定的致命缺点，必须收缩估计 / 权重约束才敢用。

机构建模大单时还会叠加**平方根定律**的冲击成本（`冲击 ∝ σ × √(单量/日均成交量)`）。小资金基本不用 —— 你的单子占市场成交量微乎其微，冲击可忽略，固定比例就够。资金大了才需要。

### 17.3 第二层：本金 + 手数限制（本批新增议题，最被忽视）

上面所有方法都输出**连续权重**（如 `w=4.7%`）。但你实盘面对的是：

- **本金有限**：比如 10 万块
- **手数是整数**：A股 100 股 = 1 手，只能买整数手；港股每手不同（100/500/1000…）；期货 1 手对应固定合约乘数
- **单价高**：一手贵州茅台 ≈ 17 万，10 万本金一手都买不起

**问题的本质：连续最优解 → 整数可行解的投影**

```text
理想：茅台 w=4.7% → 4700 元 → 茅台 1700 元/股 → 2.76 股 → 但只能买整数！
现实：买 2 股(≈3400 元，实际 3.4%) 还是 3 股(≈5100 元，实际 5.1%)？
     或者本金不够，直接买 0 股？
```

**本金越小，"取整误差"越致命** —— 小资金组合根本无法逼近理论权重，这是散户/小资金量化的根本约束。

#### 做法 1：简单取整 + 现金缓冲（个人平台够用）

```text
1. 算目标金额：target_value_i = 本金 × w_i
2. 算目标股数：raw_shares_i = target_value_i / price_i
3. 向下取整到手：shares_i = floor(raw_shares_i / lot_size) × lot_size
4. 剩余现金留着（别硬凑满仓）
```

- **向下取整**保证不超预算（向上取整可能爆本金）。
- 会有一部分现金买不掉（**cash drag**），小资金下可能 5–10% 闲置，正常现象。

#### 做法 2：最大化"贴近目标权重"的整数规划

```text
min  Σ | 实际权重_i − 目标权重_i |
s.t. Σ shares_i × price_i <= 本金
     shares_i = 整数 × lot_size
     shares_i >= 0
```

- 这是**整数规划（ILP）**，用 `cvxpy`（带整数变量）或 `PuLP` 求解。
- 比简单取整更优，但对小组合收益有限，**个人平台一般不值得，做法 1 足够**。

#### 做法 3：门槛过滤（小资金必做）

本金小时，不是所有票都值得买 —— 买了也就 1 股，权重严重偏离且手续费占比高。所以：

- **单价过滤**：`price_i × lot_size > 本金 × w_i × 2` → 这票配不进，跳过
- **最小持仓**：单票金额 < 本金 × 1% → 太小，合并或跳过（不然全是零头）
- **缩减持仓数**：本金 10 万、想持 50 只 → 每只才 2000 元，很多买不了整手 → 降到 Top 10–15 只，每只金额够买整手

**关键洞察**：**本金决定你能持有多少只票**。10 万本金想复刻一个 50 只票的组合，数学上做不到 —— 手数取整误差会毁掉分散化。小资金要么减少持仓数，要么用 ETF（单价低、能整手买、天然分散）替代个股。

**本节仍未解决**：港股/期货的 lot_size 主数据从哪来；cash drag 上限多少算可接受。

---

## 18. 成交成本与撮合建模

> 回应 §10.3（模拟账本：撮合模型 / 手续费）

### 18.1 滑点建模四档（从简到繁）

| 模型 | 说明 | 适用 |
|---|---|---|
| **1. 固定比例** | 成交价 = 决策价 × (1 ± k)，按金额或笔数收 | 个人平台默认，简单诚实 |
| **2. 固定跳数** | 滑 N 个最小变动价位 | 期货常用 |
| **3. 平方根定律** | 冲击成本 ∝ σ × √(单量 / 日均成交量) | 机构建模大单的标准思路。小资金基本不用 —— 单子占市场成交量微乎其微，冲击可忽略，固定比例就够。资金大了才需要 |
| **4. 盘口/tick 真实撮合** | 用真实逐笔/盘口数据，模拟你的单子真的去吃挂单簿：买单 → 吃掉卖一挂单 → 不够再吃卖二 → 加权算出真实成交均价 | 最真实。公司的 HubbleReplay（C++ paper trading）/ CrossEngine 做的就是这个层次。需要 tick/盘口数据 + 撮合引擎，个人平台日频/分钟频用不上，也拿不到这么细的数据 |

**滑点建模要点**：

1. **方向永远对你不利**：买入价往上滑，卖出价往下滑。**别搞反符号**（搞反了滑点反而变成收益，回测虚高）。
2. **按流动性分档，别一刀切**。
3. **成交价用什么基准很关键**（见 §18.2）—— 用收盘价还是次日开盘价，直接影响是否有未来函数。

### 18.2 成交价基准与成交假设（防未来函数）

**这是回测手续费/滑点里最容易翻车、比费率取值重要 10 倍的地方。** 你用哪个价成交，决定了回测有没有偷看未来。

**错误做法（经典未来函数）**：

```text
T 日收盘后算出信号 → 用 T 日收盘价成交
← 错！你收盘时才知道信号，却用收盘价买，实盘根本来不及
（虚高 30–100%）
```

**正确做法**：

```text
T 日收盘后算信号 → 用 T+1 日开盘价成交（+滑点）   ← 标准做法，最贴近实盘
或                 → 用 T+1 日 VWAP 成交          ← 更真实（大单摊到全天）
```

**成交价基准的选择本身就是一种成本假设 —— 次日开盘 + 滑点是保守且诚实的默认。**

### 18.3 真实回测的六道关

真实回测的一笔成交，要过这几道关（顺序执行）：

```text
想成交 →
 ① 停牌？             → suspend==1 直接不能成交（挂到复牌）      [标记位]
 ② 涨停想买/跌停想卖？ → 成交不了（封板）                        [标记位]
 ③ 成交量约束？        → 下单量 > 当日成交量×比例 → 部分成交     [流动性约束]
 ④ 本金/手数？         → floor 到整手，买不起就少买/不买          [手数约束]
 ⑤ 确定成交价 = 次日开盘价 ± 滑点                                [滑点]
 ⑥ 扣手续费（买卖不对称，含最低 5 元）                           [手续费]
→ 更新持仓 + 现金
```

**这六步全建模了，回测才叫"真实"。** 漏任何一步都会让回测偏乐观。公司 Pysim 是事件驱动逐笔跑这个流程，你个人平台向量化回测可以简化，**但这六个约束的逻辑不能少**。

**本节仍未解决**：无——具体费率表已定 → §66.4 四件套（佣金万 2.5 最低 5 元 / 印花税万 5 卖出收 ETF 免 / 过户万 0.1 / 滑点 0.1%；期货手续费 V3 预留）；流动性约束阈值已定 → §66（check_split_needed >1% 日成交量）（2026-10-10 回改）。

---

## 19. 回测-实盘一致性与薄壳契约

> 回应 §1.1（行为差异）、§1.2（统一账本）、§1.3（薄壳边界）、§1.5（alphalens 定位）、§1.6（引擎收敛，部分）

### 19.1 "同路径"的真正含义：计算结果可复现，不是代码同一套

放弃"一套内核多模式复用、回测=实盘同路径"这个决策**放弃的范围太大了**，需要纠偏。

公司为什么坚持同路径 —— 《00 - Replay与实盘一致性分析》(209868537) 核心结论：

> 设计目标应当是尽可能让回放能还原**计算结果**，而不是要求所有事件和实盘消费情况完全一致。

也就是说公司追求的不是"代码 100% 同一套"，而是**计算结果可复现** —— signal 相同的输入产出相同的值。New Pysim(222727276) 进一步说：

> pysim和usignal架构完全统一，都是事件驱动的……衔接交易：之前交易相关的 tradeplan、pos、rcs、signal、command 等服务，可以无缝衔接

**判断**：backtrader（回测）+ vnpy（实盘）是两套完全不同的引擎、不同的数据接口、不同的 order 对象。面临的风险：

- **回测信号和实盘信号跑在不同代码路径上** —— "回测 OK 实盘异常"的 bug 会无法复现、无法排查。这正是公司 Replay 系统存在的原因。
- **策略迁移成本**：backtrader 写好的策略逻辑要重写一遍才能跑在 vnpy 上（两者的 API、order 模型、数据结构完全不同）。

### 19.2 精简版同路径：策略逻辑提取为引擎无关的纯函数

**建议保留的精简版同路径 —— 不是自研内核，而是**：

- 你的**策略逻辑（signal 生成）**应该提取为一个和引擎无关的纯函数/类（只接受行情数据，产出 signal），然后分别被 backtrader 和 vnpy 调用。**signal 层一套代码，执行层各用各的引擎。**
- 这正是公司 New Pysim 的思路：

> NewPysim是空洞的，用户可以定义自己的调用流程…提供一套套兼容工具链供取用

核心是**策略逻辑和执行引擎解耦**，而不是要求全链路同一套代码。

### 19.3 backtrader vs vnpy：最大的架构风险点

这两个框架设计哲学和数据模型完全不同：

| | backtrader | vnpy |
|---|---|---|
| 数据模型 | `DataFeed` → Line | `BarData` / `TickData` |
| 订单模型 | `Order` / `Trade` | `OrderData` / `TradeData` |
| 策略接口 | `Strategy.next()` | `CtaTemplate.on_bar()` |
| 事件系统 | 隐式（cerebro 驱动） | 显式（EventEngine） |

**风险**：策略从 backtrader 迁到 vnpy 几乎要重写，不是适配。而且两边的**订单状态机不同**，回测里"成交了"在实盘里可能挂单行为不一样。

**建议**（呼应 §19.2）：
- 策略逻辑（signal 生成）抽成引擎无关的纯函数层：`def generate_signal(bar_data: dict) -> dict`
- backtrader 和 vnpy 各写一个薄适配层调用这个纯函数。这就是 §1.3 里说的"薄壳"要做的事 —— **薄壳不是可有可无的锦上添花，而是整个架构的粘合剂和风险缓冲层。**

### 19.4 薄壳的核心契约（三个 dataclass）

薄壳的核心契约：**统一行情格式、统一 signal 格式、统一 order 格式**，两头分别转换。

```text
BarData dataclass      统一行情格式
SignalData dataclass   统一 signal 格式
OrderRequest dataclass 统一 order 格式

backtrader  ←→  [薄壳契约]  ←→  vnpy
   DataFeed         BarData        Gateway
   Order     ↔   OrderRequest  ↔  OrderData
   Strategy  →   SignalData    →  CtaTemplate
```

### 19.5 差异容忍度：不是固定数字，分层定容忍

| 层 | 指标 | 容忍度 | 原因 |
|---|---|---|---|
| **Signal 层** | 同一输入产出相同 signal | **零容忍** | 这是确定性计算，不一致 = 有 bug |
| **Order 层** | 相同 signal → 相同下单意图 | **零容忍** | 薄壳的契约保证 |
| **Fill 层** | 成交价/量 | **允许偏差** | 真实柜台 vs 模拟撮合本质不同 |
| **净值** | 累计收益曲线 | **年化偏差 < 1–2%** | 成本/滑点/延迟带来的系统性偏移，不是 bug |

公司 Replay 一致性文档(209868537)的核心结论正是这个思路：设计目标是还原**计算结果**，不是要求所有事件和消费情况完全一致。

**翻译到本平台**：signal 必须一致（确定性可验证），成交允许有偏差（物理限制），但**偏差必须可量化、可解释**。

### 19.6 对拍机制：常态，但分两个频率

| 对拍 | 内容 | 频率 | 目的 |
|---|---|---|---|
| **日常对拍（轻量）** | 每天实盘收盘后，把当天行情灌进 backtrader 跑一遍，对比 signal 是否一致 | 每日自动 | 只验证 signal 层。几秒跑完。**signal 不一致 = P0 报警，必须当天查** |
| **周期对拍（重量）** | 累计净值差、成交价偏差分布、滑点统计 | 每 2–4 周 | 量化 fill 层偏差，**校准回测的成本/滑点参数**，让回测更贴近实盘 |

### 19.7 适配层：你自己写，在第一个策略上实盘之前写

**薄壳是上实盘的前置条件，不是"以后优化"的东西。** 没有薄壳就上实盘，等于同一个策略维护两份独立代码，第一次出 bug 就不知道信哪边。

### 19.8 alphalens 的定位：筛选层，不是验证层

声明"alphalens 不是回测引擎"是对的。但要注意：alphalens 的 `create_returns_tear_sheet()` **内部有一个隐含的"多空回测"**（按分组计算累计收益），这和 backtrader 的正式回测结论可能不一致 —— 因为 alphalens **不建模成本/滑点/涨跌停**。

**定位**：

```text
alphalens  →  因子筛选阶段   （快速判断 IC / 分组，不扣成本）
backtrader →  最终验证阶段   （扣成本 / 约束）
两者结论矛盾时以 backtrader 为准。
```

**这个层级关系要写进架构文档。**

**本节仍未解决**：无——账本字段级 schema 已定 → §23（Order/Trade/Position/Account 四 dataclass）；截面矩阵在 backtrader 怎么跑不再需要（不再需要——§66.2 不用 backtrader/vnpy，2026-10-10 回改）；三件套具体版本已定 → §28.2（2026-10-10 回改）。

---

## 20. 架构与选型复审

> 回应 §4.1（存储职责）、§12.1（时间模型）、§12.2（EBGraph/EventBrick）、§12.5（存储选型）、§12.6（黄金样本）

### 20.1 存储：砍 ClickHouse，DuckDB schema 做逻辑分层

**原决策**：Parquet(raw) + DuckDB(研究层) + ClickHouse(资产层)。**建议砍掉 ClickHouse。**

公司的存储（UdataViewer 288527133 架构）：

> Parquet(OT 行情数据) + ucube tensor(bar/golden) → 统一转 Arrow → DuckDB 做向量化查询

公司用 **Parquet + DuckDB + Arrow 零拷贝**，没有在个人研究链路上用 ClickHouse。公司的 ClickHouse 出现在**运维监控、延迟分析、高频 binlog 归档**（如 `uma_latency_flow`、`TBExchBinlog` 入库）—— 这是**生产级运维需求**，不是研究/策略链路。

**判断**：单机单人平台，ClickHouse 引入了不必要的运维负担，收益为零。

- ClickHouse 要单独部署/维护，单机吃内存大；其优势（PB 级分析型查询、多租户并发）在单人场景完全用不上。
- DuckDB 已经覆盖了"资产层"需要的一切：OLAP 查询、Parquet 直查、SQL 接口、元数据管理。**DuckDB 的 catalog + schema 就能做"资产层 vs 研究层"的逻辑隔离**，不需要物理上换数据库。

**结论**：Parquet（落盘）+ DuckDB（全链路查询），**砍 ClickHouse**。用 DuckDB 的 schema 做逻辑分层：`raw` / `golden` / `factor` 三个 schema 代替三个存储引擎。

### 20.2 四层时间模型简化为两层：di + ti

**原结论**：starfleet 轮询 → EBReplay 日调度 → EBClock μs 墙钟 → EBTimer TI 网格，EndTI 驱动信号。

**判断**：日频/分钟频低频场景，**不需要 25:00:00 连续夜盘、不需要跨交易所多时区**。四层时间模型是为公司的多市场、跨时区、连续交易设计的，个人平台用不上。

**结论**：简化为**交易日历(di) + 分钟 bar 索引(ti)** 两层就够。

### 20.3 EventBrick：思想保留，不要照搬抽象

公司 Pysim 的 EventBrick(247385222)：

- EB 是事件系统的组成单位
- EB 具备：定义事件、发布事件、订阅其他 EB 事件
- EB Graph 管理一批 EventBricks，事件由 EB 决定

New Pysim(222727276) 的态度：

> NewPysim是空洞的，用户可以定义自己的调用流程或者服务，没有固定的范式，提供一套套兼容工具链供取用

**判断**：选了 backtrader + vnpy，它们自己就有自己的组件模型（backtrader 的 Strategy/Indicator/Observer；vnpy 的 Gateway/Engine）。**再叠一层 EventBrick 抽象会造成双层封装，两边都不干净。**

**结论**：
- **保留 EventBrick 的思想**（因子 = 插件/组件，可注册、可组合），但**用 backtrader/vnpy 自己的扩展机制实现**（backtrader 的 `Indicator` 类、vnpy 的模块）。
- **不建通用事件系统。** §2.1（事件词汇表）与 §2.3（DataBus）在薄壳方案下不需要独立的 EB 抽象。

### 20.4 EBGraph 降级为因子 DAG 拓扑排序

**EBGraph（DAG 依赖）的价值在因子层** —— 因子 A 依赖因子 B 的 DAG。这个用**简单的拓扑排序**就能实现：`factor_registry` 里的 `inputs` 字段已经有血缘，沿血缘拓扑排序 = **轻量版 EBGraph**，不需要一个通用事件系统。

> **§12.2 的结论**：不做 EventBrick/EBGraph 运行时事件订阅图。因子用 backtrader Indicator / vnpy 模块承载；因子依赖用注册表血缘做拓扑排序。

### 20.5 黄金样本夏普 16 是危险信号

`baseline_spec.py` 报出 sharpe **16.001135** —— 这个数字极其可疑。

经验阈值：`IC > 0.15` 先怀疑数据问题；散户策略夏普 > 2 已是"怀疑过拟合"区。**夏普 16 在任何资产类别、任何频率下都是不现实的** —— 文艺复兴大奖章的夏普也就 5–6。

**建议**：在迁移前先排查这个 baseline 是否有**未来函数**、是否**扣了成本**、是否有**幸存者偏差**。如果 baseline 本身有 bug，用它来验证新引擎就是"用错误的答案判对错" —— 迁移做对了反而会和 baseline 不一致。

> **§12.6 的状态**：黄金样本的**方法**（用旧结果做对拍基线）保留，但**这个具体的数字不可信，迁移前必须排查**。排查结果出来前，不要把它当验收标准。

### 20.6 板块命名微调与资产化边界

"数据平台"和"资产"是同一个东西 —— 对齐。公司对应关系：

| 本平台 | 公司对应 |
|---|---|
| 数据平台 / 资产 | Udata（数据存储 + Schema）+ DataLoader（数据生产）+ DQC（质检） |
| 投研 | Uformula / EventBrick（因子计算）+ Pysim（回测）+ signal（信号） |

**注意**："资产化"在公司语境下还包括**因子落库后的元数据管理**（版本 / 血缘 / 生产状态）—— `factor_registry.yaml` + Parquet 目录正是简化版，**这块不要丢**。

### 20.7 迁移模式确认：新旧并存、对比验证

公司 New Pysim 也是渐进迁移：

> signal 和 signalv2 是完全独立的两种用法，没有优劣区别

**新旧并存、对比验证，是正确的迁移模式。** ARCHITECTURE.md §10 "旧引擎降为对照组"的策略确认有效。

**本节仍未解决**：无——DuckDB 表划分已定 → §47（11 张核心表）；`di`/`ti` 接口形态已定 → §68.2（时间推进坍缩为 for-loop）；baseline 排查步骤与判据已定 → §68.7 超范围排查表（2026-10-10 回改）。

---

## 21. 数据输入链路补充

> 回应 §3.2（硬软边界）、§3.7（SourceAdapter）、§3.8（多源冲突）

### 21.1 DQC 四维分级：确认对齐，无需改动

决策：完整性 / 合法性 / 一致性 / 时序性四类；硬错误拒收，软警告带 flag。

这和公司 DQC(185868851) 完全一致：**强规则 = 熔断**（对应硬错误拒收），**弱规则 = 告警**（对应软警告带 flag）。**没有需要改的。**

### 21.2 多源仲裁：补一条 fallback 规则

公司 DQC 的异源核对正是"主源入库，备源对照"的思路。但要**补一条规则**：

> **当主源强规则失败（如主源当天缺数据）时，自动 fallback 到备源数据入库（而非空缺）。**

公司的 DataLoader 实现中，当某个源的数据缺失时会有备源兜底逻辑。**`SourceAdapter` 接口层加一个 fallback 优先级就行，不用改架构。**

### 21.3 SourceAdapter：一个公司踩过的坑必须规避

每源一个适配器 —— 对齐，公司的 DataLoader(DLS) 也是这个模式。

**必须注意的坑（停牌复权 bug 244038313）**：`DLS_Basedata_US` 的实现里，在 `suspend==1` 时把 `adjfactor` 写死为 `1`。

**要求**：每个 `SourceAdapter` 的**字段映射逻辑必须有测试覆盖**，尤其是边界情况：**停牌、缺值、CA 事件**。

**本节仍未解决**：主源指定表的存放位置；跨源对比容差；fallback 的具体优先级规则与降级告警。

---

## 22. 回测-实盘一致性路线选型（A+B+C 组合，D 不做）

> 回应 §1.1（backtrader-vnpy 差异与统一）、§1.3（薄壳边界）、§1.5（alphalens 对接）、§1.6（自研范围）

### 22.1 四个方向的定位：不是互斥，是互补，但权重差别很大

| 方向 | 定位 | 结论 |
|---|---|---|
| **A 薄壳适配层** | 结构基础 | ✅ 必做 |
| **B 对拍量化偏差** | 运行期校准 | ✅ 必做 |
| **C 模拟盘校准期** | 流程纪律 | ✅ 作为上线流程 |
| **D 实盘执行层自研** | 执行层自研 | ❌ 不做 |

### 22.2 A 薄壳适配层 = 结构基础，必做

**唯一一个从结构上保证 signal 层一致的方案。没有 A，其他方向都是在两套独立系统上贴补丁。**

```text
你的策略代码(引擎无关)
  → 薄壳契约层(自研核心)
    → BT Adapter / VN Adapter
```

薄壳契约层四个 dataclass：

| dataclass | 语义 | 字段 |
|---|---|---|
| `BarData` | 统一行情格式 | `datetime, ticker, open, high, low, close, volume, amount` |
| `SignalData` | 统一信号格式 | `datetime, ticker, direction, strength, reason` |
| `OrderReq` | 统一下单意图 | `datetime, ticker, direction, quantity(整数手), price_type(市价/限价), limit_price` |
| `FillReport` | 统一成交回报 | `datetime, ticker, direction, filled_qty, fill_price, commission, slippage` |

**铁律：策略代码里永远不出现 backtrader 或 vnpy 的任何 import。** 策略只看 `BarData` → 产出 `SignalData` → 薄壳转成 `OrderReq`。

两个 Adapter 的职责：

- **BT Adapter**：backtrader Line → `BarData` 喂策略；`OrderReq` → `self.buy()/self.sell()`；backtrader Trade → `FillReport`
- **VN Adapter**：vnpy BarData/TickData → 薄壳 `BarData`；`OrderReq` → vnpy OrderRequest；vnpy TradeData → `FillReport`

Adapter 代码量很小（每个 100–200 行），本质是字段映射 + 类型转换，但**把差异锁死在两个已知文件里**。

### 22.3 B 对拍量化偏差 = 运行期校准，必做

A 保证 signal 一致，但 **fill 层偏差是物理性的**。B 的价值是把偏差量化出来反馈回 backtrader 参数：

```text
实盘跑一段时间收集 FillReport 日志
  → 对比(回测假设滑点 0.1% vs 实盘实际中位数 0.15%)
  → 校准 backtrader slippage 参数
  → 重新回测
```

对拍报告核心指标：

| 指标 | 做法 | 用途 |
|---|---|---|
| signal 一致率 | 同一天同一票，回测和实盘产出的 signal 是否相同（应 100%） | 诊断 |
| 成交价偏差 | \|实盘 fill_price − 回测 fill_price\| / 回测 fill_price 的分布 | 诊断 |
| 成交率偏差 | 回测假设能成交但实盘没成交（涨跌停/流动性不足）的比例 | 判断 |
| 累计净值偏差 | 两条净值曲线的跟踪误差（年化） | 判断 |

**前两个是诊断用（定位偏差来源），后两个是判断用（偏差在不在容忍度内）。**

### 22.4 C 模拟盘校准期 = 流程纪律

不是技术方案，是运营纪律：**新策略必须先在 vnpy 模拟盘跑 2–4 周、通过对拍验证后才允许上真钱。** 和"样本外一次性 GO/NO-GO"同一思路——模拟盘是从回测到实盘的最后一道关卡。

**C 依赖 A 和 B**：没有薄壳(A)，模拟盘跑的不是同一套策略代码；没有对拍(B)，跑完不知道偏差多大。**C 是 A+B 的上层流程，不能替代它们。**

### 22.5 D 实盘执行层自研 = 不做

不用 vnpy 的回测/策略容器，只用 vnpy 网关，执行层自己写。看起来诱人，实际成本极高：

- 订单状态机、异常恢复（断线重连后持仓恢复）、风控拦截都要自己写
- 这些 vnpy 已经写好了
- 公司有几十人的团队维护 Hubble/Utrade 的执行层，一个人不应该碰
- **而且有了 A，vnpy 执行层怎么跑对策略代码是透明的**

### 22.6 偏差的物理来源：不是 bug，是现实

| 偏差来源 | 方向 | 量级 | 能校准吗 |
|---|---|---|---|
| 滑点模型 vs 真实盘口 | 实盘更差 | 0.05–0.3%/笔 | B 可校准参数 |
| 涨跌停封板 | 实盘更差 | 偶发但影响大 | 回测加涨跌停拒单 |
| 延迟(signal→下单几百 ms~几秒) | 实盘更差 | 日频/分钟频可忽略 | 分钟频加 1-bar 延迟 |
| 部分成交 | 实盘更差 | 小盘/低流动性显著 | 回测加成交量约束 |
| 最低佣金 5 元 | 实盘更差 | 小资金杀手 | 回测必须建模 |

**所有偏差方向都是"实盘比回测更差"—— 回测是策略的收益上限，实盘只会更低。**

### 22.7 最终方案与实施顺序

| 层 | 方案 |
|---|---|
| [架构] | 薄壳(A) 4 dataclass 契约 + 2 Adapter(BT/VN) → signal 层零差异 |
| [运营] | 对拍(B) 日常 signal 对拍(自动) + 周期净值对拍(手动) |
| [流程] | 校准期(C) 模拟盘 2–4 周 + 通过对拍才上真钱 |
| [不做] | 自研执行层(D) |

**实施顺序**：

1. 薄壳契约定义（半天）
2. BT Adapter（1–2 天）
3. VN Adapter（1–2 天）
4. 对拍脚本（1 天）
5. 第一个策略走完全流程验证

---

## 23. 账本数据模型（5 dataclass + 状态机 + Journal）

> 回应 §1.2（统一账本模型）、§10.3（模拟账本）

### 23.1 方向选型：B 自定义规范模型

四个候选方向的取舍：

**不选 A（以 vnpy 为准）**：

- vnpy 的 `OrderData`/`TradeData` 耦合了 Gateway 概念（`gateway_name`、`Exchange` 枚举是 vnpy 自定义类）——薄壳依赖 vnpy 类型则 backtrader adapter 要 import vnpy，**反向依赖、架构污染**
- vnpy 没有 `CREATED` 状态、没有 `ref_price`、没有 `slippage` 字段（这些是对拍和分析需要的）
- vnpy 的 `PositionData` 是柜台视角（区分多/空头持仓），股票场景冗余（A股只有多头）

**不选 C（以 backtrader 为准）**：

- backtrader 模型太简化——没有 `REJECTED` 和 `PARTIAL` 的细分状态（状态机不完整）
- 没有资金冻结，没有多账户概念
- 以它为准实盘信息会丢失

**不选 D（先定事件流再定 schema）**：

- 事件流的 schema 就是数据模型——"先定时序再定 schema"是假性解耦，字段不定后面每步都空转

**选 B：自定义规范模型**——5 个 dataclass + 2 个 Adapter，不依赖任何引擎类型。

### 23.2 五个 dataclass + 三个枚举

```python
class BarData:
    datetime: datetime
    ticker: str
    open: float; high: float; low: float; close: float
    volume: float; amount: float
    pre_close: float; adjfactor: float
    # 可交易性标记
    suspend: bool; limit_up: bool; limit_down: bool

@dataclass
class Order:
    order_id: str          # 全局唯一(薄壳生成,不依赖引擎)
    portfolio_id: str
    datetime: datetime     # 策略产出意图的时间
    ticker: str
    direction: Direction   # BUY / SELL
    quantity: int          # 目标手数(整数,已经过手数取整)
    price_type: PriceType  # MARKET / LIMIT
    limit_price: float | None
    ref_price: float       # 策略看到的参考价(用于事后算滑点)
    status: OrderStatus    # CREATED->SUBMITTED->PARTIAL->FILLED/CANCELLED/REJECTED
    filled_qty: int        # 累计已成交手数
    frozen_cash: float     # 下单时冻结的资金
    created_at: datetime
    updated_at: datetime
    reject_reason: str     # REJECTED 时的原因

@dataclass
class Trade:
    trade_id: str
    order_id: str          # 关联 Order(1:N)
    portfolio_id: str
    datetime: datetime     # 成交时间
    ticker: str
    direction: Direction
    filled_qty: int        # 本笔成交数量
    fill_price: float      # 实际成交价
    commission: float      # 本笔手续费(含最低5元逻辑)
    slippage: float        # fill_price - order.ref_price(事后计算)

@dataclass
class Position:
    portfolio_id: str
    ticker: str
    quantity: int          # 当前持仓手数
    avg_cost: float        # 持仓均价(含手续费摊入)
    market_value: float    # 市值(= quantity × latest_price)
    unrealized_pnl: float  # 浮动盈亏
    realized_pnl: float    # 已实现盈亏(卖出时结算)

@dataclass
class Account:
    account_id: str
    total_asset: float     # 总资产 = cash + Σ position.market_value
    cash: float            # 可用现金
    frozen_cash: float     # 冻结(待成交 order 占用)
    total_commission: float # 累计手续费
    total_pnl: float       # 累计盈亏
```

枚举：

```python
class Direction(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(Enum):
    CREATED = "CREATED"
    SUBMITTED = "SUBMITTED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

class PriceType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
```

### 23.3 Q1 订单状态机：6 态不能少

| 状态 | 含义 | backtrader | vnpy |
|---|---|---|---|
| CREATED | 策略产出下单意图,尚未提交 | Order.Created | – (vnpy 没有,直接到 submitting) |
| SUBMITTED | 已发给柜台/撮合器 | Order.Submitted | Status.SUBMITTING → NOTTRADED |
| PARTIAL | 部分成交 | Order.Partial | Status.PARTTRADED |
| FILLED | 全部成交(终态) | Order.Completed | Status.ALLTRADED |
| CANCELLED | 已撤单(终态) | Order.Canceled | Status.CANCELLED |
| REJECTED | 被拒(终态) | Order.Rejected | Status.REJECTED |

状态图：

```text
Created → Submitted → PartialFilled → Filled
                      ↳ Rejected (柜台拒单)
                      ↳ Cancelled (未成交撤单)
PartialFilled → Cancelled (部分成交后撤单，已成交部分保留)
```

**CREATED 不能省**：这是"策略意图"和"实际提交"的分界线——风控拦截、资金预检、手数取整都发生在 `CREATED → SUBMITTED`。没有 CREATED 就分不清"策略想下但被拦了"和"策略根本没下"。

### 23.4 Q2 部分成交：Order 产出多笔 Trade 的 1:N 关系

```text
Order (qty=1000)
  ├─ Trade #1 (filled=300, price=10.05, time=09:31:02)
  ├─ Trade #2 (filled=500, price=10.06, time=09:31:15)
  └─ Trade #3 (filled=200, price=10.07, time=09:32:01)
    → Order 状态: PARTIAL → PARTIAL → FILLED
    → Order.filled_qty: 300 → 800 → 1000
```

Order 跟踪"意图+状态"，Trade 跟踪"实际发生的每一笔成交"，**不能合并成一张表**。backtrader 内部也是这个模型(Order + 多个 Trade 记录)；vnpy 同理(OrderData + 多个 TradeData)。

**回测简化**：backtrader 默认"下一根 bar 全部成交"，但薄壳模型必须支持部分成交的结构(1:N)，即使回测里 N 通常=1，**否则上实盘时模型结构要改、适配层跟着全改**。

### 23.5 Q3 手续费 / 滑点 / 资金占用：分属不同模型，不要混

| 费项 | 归属 | 字段 | 为什么 |
|---|---|---|---|
| 手续费 | Trade(每笔成交) | `commission` | 每笔单独算(含最低 5 元逻辑) |
| 滑点 | Trade(每笔成交) | `slippage = fill_price − ref_price` | 理想价 vs 实际成交价的差 |
| 资金占用 | Order(下单时) | `frozen_cash` | 下单预冻结，成交/撤单后释放 |
| 保证金 | Position(持仓) | `margin` | 期货/融资持仓的保证金占用 |

`ref_price` 记在 Order 上（策略产出 signal 时看到的价格）；实际成交价记在 Trade 上；`slippage = trade.fill_price − order.ref_price`，**事后算出来的，不是事先参数**。

### 23.6 Q4 多账户 / 多组合：两层结构

```text
Account(物理层,一个柜台一个,可用/冻结/总资产)
  → Portfolio(逻辑层,一 Account 多策略各自独立记持仓/净值)
    → Position(单票持仓)
```

个人平台第一版一个 Account + 一个 Portfolio 就够，但**模型上预留 `portfolio_id` 字段**——每个 Order/Trade/Position 都带 `portfolio_id`，以后加策略不用改结构。

### 23.7 Q5 事件流 vs 快照：两个都要，事件流是本体

| | 事件流(Journal) | 快照(Snapshot) |
|---|---|---|
| 性质 | append-only,不可变 | 可更新,反映当前状态 |
| 内容 | 每次状态变更一条记录 | 每个 position/account 的当前值 |
| 用途 | 审计追溯、对拍、事后分析 | 实时决策、风控、展示 |
| 存储 | Parquet 按日归档 | 内存字典 + 定期 dump |

**事件流是本体，快照是派生**。快照可以从事件流重建（replay 全部事件还原任何时刻的快照）——这正是公司 Replay 系统的核心思路"通过按照时间序列完全回放事件来进行"。回测里 journal 就是回测日志；实盘里 journal 是审计日志 + 对拍的数据源。

### 23.8 Journal schema

```python
@dataclass
class JournalEntry:
    timestamp: datetime  # 事件发生时间
    event_type: str      # ORDER_CREATED / ORDER_SUBMITTED / ORDER_FILLED / TRADE / POSITION_CHANGED / ACCOUNT_UPDATED
    portfolio_id: str
    payload: dict        # Order/Trade/Position/Account 的快照(序列化)
```

append-only，按日存成 Parquet：`journal/2024-09-28.parquet`。Position/Account 快照不需要单独持久化（内存维护，crash 后从 journal 恢复）。

### 23.9 汇总表

| 子问题 | 结论 |
|---|---|
| 状态机 | 6 态: CREATED → SUBMITTED → PARTIAL → FILLED / CANCELLED / REJECTED |
| 部分成交 | Order:Trade = 1:N，Order 跟踪累计，Trade 跟踪每笔 |
| 费用字段 | 手续费/滑点在 Trade；冻结资金在 Order；保证金在 Position |
| 多账户 | Account → Portfolio → Position，所有 Order/Trade 带 portfolio_id |
| 存储 | 事件流(Journal, append-only Parquet)是本体；快照(Position/Account)是内存派生 |
| 方向 | B: 自定义规范模型(5 个 dataclass + 2 个 Adapter)，不依赖任何引擎类型 |

---

## 24. 适配层实现（BT/VN Adapter + 公共逻辑）

> 回应 §1.3（薄壳边界）、§10.1（订单生成）、§10.2（风控）

### 24.1 统一事件时序（两个 Adapter 共用的契约）

```text
策略调用 place_order(signal)
  ↓
[1] 预检(薄壳内部,引擎无关)
    ├─ 停牌? → REJECTED(reason="suspended")
    ├─ 涨停想买/跌停想卖? → REJECTED(reason="limit_price")
    ├─ 资金不足? → REJECTED(reason="insufficient_cash")
    ├─ 手数取整后=0? → REJECTED(reason="qty_too_small")
    └─ 通过 → 创建 Order(status=CREATED)、冻结 frozen_cash、写 journal: ORDER_CREATED
  ↓
[2] 提交给引擎 → Order.status = SUBMITTED，写 journal: ORDER_SUBMITTED
  ↓
[3] 引擎回报 (backtrader notify_order / vnpy on_order)
  ↓
[4a] 全部成交:
    Order.status=FILLED, filled_qty=quantity；
    创建 Trade(算 commission/slippage)；
    更新 Position(重算 avg_cost)；
    更新 Account(释放 frozen, 扣 commission, 调 cash)；
    写 journal: TRADE + ORDER_FILLED + POSITION_CHANGED + ACCOUNT_UPDATED
[4b] 部分成交:
    Order.status=PARTIAL, filled_qty+=本次；
    创建 Trade(本笔)；更新 Position；更新 Account(部分释放 frozen)；
    写 journal: TRADE + ORDER_PARTIAL + ... → 等待下一笔
[4c] 撤单(可能在 SUBMITTED 或 PARTIAL 后):
    Order.status=CANCELLED；释放剩余 frozen_cash(已成交部分不退)；
    写 journal: ORDER_CANCELLED + ACCOUNT_UPDATED
[4d] 引擎拒单(区别于预检拒单——预检是薄壳拦的，这里是柜台/撮合器拒的):
    Order.status=REJECTED, reject_reason=引擎原因；释放全部 frozen_cash；
    写 journal: ORDER_REJECTED + ACCOUNT_UPDATED
```

**这个时序是薄壳的"契约"——两个 Adapter 必须让各自引擎的行为映射到完全相同的时序上。**

### 24.2 BT Adapter 状态映射

| backtrader | 薄壳 | 备注 |
|---|---|---|
| Order.Created | CREATED | Adapter 自己在 place_order 时设 |
| Order.Submitted | SUBMITTED | notify_order 第一次回调 |
| Order.Accepted | SUBMITTED | backtrader 有 Accepted(Submitted→Accepted 区分"提交给 broker"和"broker 确认收到")，薄壳合并到 SUBMITTED；journal 里用 event_type 区分 |
| Order.Partial | PARTIAL | |
| Order.Completed | FILLED | |
| Order.Canceled | CANCELLED | 含"部分成交后撤单" |
| Order.Rejected | REJECTED | |
| Order.Margin | REJECTED | backtrader 的保证金不足=拒单 |
| Order.Expired | CANCELLED | 限价单过期=撤单(reason="expired") |

BT Adapter 核心代码（`_on_filled` / `_on_partial` / `_on_cancelled`）：

```python
def _on_filled(self, order, bt_order):
    this_fill = bt_order.executed.size - order.filled_qty  # backtrader 的 size 是累计
    this_price = bt_order.executed.price                   # 本笔均价
    this_comm = bt_order.executed.comm                     # 累计佣金(要算差值)
    prev_comm = sum(t.commission for t in self._trades_of(order))
    trade = Trade(
        trade_id=generate_id(), order_id=order.order_id,
        portfolio_id=order.portfolio_id, datetime=self._current_dt(),
        ticker=order.ticker, direction=order.direction,
        filled_qty=abs(int(this_fill)), fill_price=this_price,
        commission=this_comm - prev_comm,          # 差值 = 本笔佣金
        slippage=this_price - order.ref_price,     # 正=对你不利(买入), 负=对你不利(卖出)
    )
    order.filled_qty = abs(int(bt_order.executed.size))
    order.status = FILLED
    self._update_position(trade)
    self._release_frozen(order, fully=True)
    self._journal(TRADE, trade); self._journal(ORDER_FILLED, order)

def _on_partial(self, order, bt_order):
    this_fill = abs(bt_order.executed.size) - order.filled_qty
    # ... 创建 Trade(同上) ...
    order.filled_qty += abs(int(this_fill)); order.status = PARTIAL
    self._update_position(trade)
    self._release_frozen_partial(order, trade)  # 只释放本笔对应的冻结
    self._journal(TRADE, trade); self._journal(ORDER_PARTIAL, order)

def _on_cancelled(self, order, bt_order):
    reason = "expired" if bt_order.status == bt_order.Expired else "cancelled"
    order.status = CANCELLED; order.reject_reason = reason
    # 关键: 已成交部分保留,只释放未成交部分的冻结
    unfilled = order.quantity - order.filled_qty
    released = unfilled * order.ref_price * 1.0003
    self.portfolio.account.cash += released
    self.portfolio.account.frozen_cash -= released
    self._journal(ORDER_CANCELLED, order)
```

### 24.3 BT Adapter 三个坑

**坑 1：`executed.size/price/comm` 是累计值，不是本笔。**

第一次 Partial: size=300, comm=5.0；第二次 Partial: size=800, comm=12.5 → 第二笔 Trade 的 filled=500, commission=7.5(差值)。**直接拿 `executed.comm` 当本笔佣金会导致部分成交场景下佣金翻倍。**

**坑 2：`ref_price` 要自己记。**

backtrader 不记录"策略看到的价格"(只有 `executed.price`=成交价)。Adapter 必须在 place_order 时把当前 bar 的 close 记到 `order.ref_price`，**否则事后算不了滑点**。

**坑 3：`notify_trade` 和薄壳 Trade 不是一回事。**

backtrader 的 Trade 是"一个头寸从建仓到平仓的完整生命周期"，不是"一笔成交"。**不要用 `notify_trade` 生成薄壳 Trade**——用 `notify_order` 里的 executed 增量来生成。

### 24.4 VN Adapter 状态映射

| vnpy Status | 薄壳 | 备注 |
|---|---|---|
| Status.SUBMITTING | SUBMITTED | 合并 |
| Status.NOTTRADED | SUBMITTED | 合并(已提交未成交) |
| Status.PARTTRADED | PARTIAL | |
| Status.ALLTRADED | FILLED | |
| Status.CANCELLED | CANCELLED | |
| Status.REJECTED | REJECTED | |

VN Adapter 核心代码：

```python
class VNAdapter:
    def __init__(self, engine, portfolio):
        self.order_map = {}     # vn_orderid → 薄壳 Order
        self.portfolio = portfolio

    def place_order(self, signal: SignalData, account: Account):
        # [1] 预检(和 BT Adapter 完全相同的逻辑,复用薄壳的公共函数)
        bar = self.get_latest_bar(signal.ticker)
        reject = pre_check(signal, bar, account)  # 公共函数,两个 Adapter 共享
        if reject:
            order = create_order(signal, bar, status=REJECTED, reason=reject)
            journal(ORDER_REJECTED, order); return order
        qty = round_to_lot(signal, bar, account)  # 公共函数
        order = create_order(signal, bar, qty=qty, status=CREATED)
        order.frozen_cash = qty * bar.close * 1.0003
        account.cash -= order.frozen_cash
        account.frozen_cash += order.frozen_cash
        journal(ORDER_CREATED, order)
        # [2] 提交给 vnpy
        vn_req = VnOrderRequest(
            symbol=signal.ticker,
            exchange=self._map_exchange(signal.ticker),
            direction=VnDirection.LONG if signal.direction == BUY else VnDirection.SHORT,
            type=VnOrderType.MARKET if signal.price_type == MARKET else VnOrderType.LIMIT,
            volume=float(qty),
            price=signal.limit_price or 0,
        )
        vn_orderid = self.engine.send_order(vn_req)
        self.order_map[vn_orderid] = order
        order.status = SUBMITTED
        journal(ORDER_SUBMITTED, order); return order
```

### 24.5 VN Adapter 三个坑

**坑 1：`on_order` 和 `on_trade` 到达顺序不保证。**

vnpy 事件引擎是异步的，可能先收到 `on_trade`(成交) 再收到 `on_order`(状态变 ALLTRADED)。做法：

- `on_trade` 只管创建 Trade + 更新 Position + 累加 filled_qty
- `on_order` 只管更新 Order.status + 处理终态(CANCELLED/REJECTED 的资金释放)
- **Order.status 的推进用 filled_qty 判断而不是依赖 on_order 的时序**：`if order.filled_qty == order.quantity: status = FILLED`

**坑 2：`TradeData` 没有 commission。**

CTP 成交回报不带手续费(结算时才出)。必须在 Adapter 里自己按费率算。实盘 commission 是估算值，和券商结算单可能有小数点后几分钱的差——**日常对拍时用结算单校准费率参数**。

**坑 3：`on_order` 可能重复推送同一状态。**

CTP 网关有时对同一订单重复推送 NOTTRADED。必须做状态转换合法性检查：

```python
VALID_TRANSITIONS = {
    CREATED:   {SUBMITTED, REJECTED},
    SUBMITTED: {PARTIAL, FILLED, CANCELLED, REJECTED},
    PARTIAL:   {PARTIAL, FILLED, CANCELLED},   # PARTIAL → PARTIAL: 又一笔部分成交
    FILLED:    set(),   # 终态
    CANCELLED: set(),   # 终态
    REJECTED:  set(),   # 终态
}
def _is_valid_transition(self, current, new):
    if new == current and current == SUBMITTED:
        return False     # 重复的 SUBMITTED 忽略
    return new in VALID_TRANSITIONS[current]
```

### 24.6 公共逻辑（两个 Adapter 共享）

**4.1 预检（place_order 前）**：

```python
def pre_check(signal, bar, account) -> str | None:
    """返回 reject reason, None 表示通过"""
    if bar.suspend: return "suspended"
    if signal.direction == BUY and bar.limit_up: return "limit_up_buy_forbidden"
    if signal.direction == SELL and bar.limit_down: return "limit_down_sell_forbidden"
    est_cost = signal.quantity * bar.close * 1.0003
    if signal.direction == BUY and account.cash < est_cost: return "insufficient_cash"
    if signal.direction == SELL:
        pos = get_position(signal.ticker)
        if not pos or pos.quantity < signal.quantity: return "insufficient_position"
    return None
```

还有手数取整 `round_to_lot`。

**资金释放(frozen → cash)**：

```python
def release_frozen(order: Order, account: Account, trade: Trade = None, fully: bool = False):
    if fully:   # 全部释放(FILLED / REJECTED)
        released = order.frozen_cash; order.frozen_cash = 0
    else:       # 部分释放(每笔 Trade 按比例)
        ratio = trade.filled_qty / order.quantity
        released = order.frozen_cash * ratio; order.frozen_cash -= released
    account.frozen_cash -= released
    if order.status == FILLED:
        # 成交: 实际花费 = 成交额 + 手续费, 多冻结的退回 cash
        actual_cost = sum(t.fill_price * t.filled_qty + t.commission for t in trades_of(order))
        account.cash += (released - actual_cost) if order.direction == BUY else released
    elif order.status in (CANCELLED, REJECTED):
        account.cash += released   # 未成交部分退回 cash
```

### 24.7 Corner Case 完整清单

| Corner Case | 场景 | 处理 |
|---|---|---|
| 部分成交后撤单 | 买 1000 股,成交 300,撤掉剩余 700 | 保留 300 股的 Trade + Position；释放 700 股的 frozen；Order 终态 CANCELLED, filled_qty=300 |
| 部分成交后又部分成交 | 分 3 笔成交 300+500+200 | 3 条 Trade 记录；Order 经历 PARTIAL→PARTIAL→FILLED |
| 下单后立即被拒 | CTP 资金不足 / 风控拦截 | Order 直接 SUBMITTED→REJECTED；释放全部 frozen |
| 预检拦截 vs 引擎拒单 | 停牌 vs CTP 拒单 | 预检拦截: Order 只到 CREATED→REJECTED(不提交给引擎)；引擎拒单: SUBMITTED→REJECTED(已提交) |
| 重复回调 | vnpy 推两次 NOTTRADED | 状态转换检查拦住,不重复处理 |
| on_trade 先于 on_order 到达 | vnpy 异步乱序 | on_trade 只管 Trade+Position,不管 Order.status；status 由 filled_qty 推导 |
| 涨停买入成交 | 回测默认拒,但实盘可能排队成交了 | 预检配置化: strict_limit=True(回测默认)拒单；strict_limit=False(实盘)允许提交,柜台决定 |
| 卖出负持仓 | 可能的超卖 bug | pre_check 里硬检查 position >= sell_qty；sell 后 position 不允许 < 0 |
| 最低佣金 5 元 | 小额成交 | max(amount * rate, 5.0), 逐笔算,不是按 order 总额算 |

### 24.8 backtrader 缺字段的补算

| 薄壳字段 | backtrader 有没有 | 怎么补 |
|---|---|---|
| Trade.commission | 有(累计值,需算差值) | Adapter 算差值 |
| Order.ref_price | 没有 | Adapter 在 place_order 时记当前 bar.close |
| Trade.slippage | 没有 | Adapter 算: fill_price − ref_price |
| Order.frozen_cash | 没有 | Adapter 自己算: qty × ref_price × (1+margin) |
| Order.reject_reason | 有(但不详细) | 映射: Margin→"insufficient_margin", Rejected→"broker_rejected" |
| Trade.datetime(精确到秒) | 没有(只有 bar 级) | 用当前 bar 的 datetime(回测本来就是 bar 级) |
| 成交笔数(部分成交) | 默认没有(BT 默认一次全成交) | 如果 BT 配了 fillsize 会产生 Partial；默认配置下每 Order 只有 1 笔 Trade |

**标记估算值**：对于补算的字段，Trade 可加 `estimated: bool` 标记。对拍脚本跑的时候，`estimated=True` 的字段只做分布级对比(中位数/均值)，**不做逐笔精确匹配**。

---

## 25. 薄壳边界、数据接口与运行形态

> 回应 §1.2（统一账本）、§1.3（薄壳边界）、§4.8（与三件套的数据对接）、§5.5（udata 与三件套的对接）

### 25.1 薄壳三层职责：它是"翻译官"，不是"总管"

**薄壳拥有**：

1. 产品契约 `models/`（5 dataclass + 枚举）
2. 数据接口 `data_api/`（统一取数，返回薄壳类型）
3. 适配层 `adapters/`（BT/VN/AL 各一个）
4. 公共逻辑 `common/`（预检/佣金/持仓/Journal）

**薄壳不碰**：

- × 回测撮合逻辑 → backtrader broker
- × 实盘柜台通讯 → vnpy gateway
- × 因子计算引擎 → pandas/polars + 算子库
- × 因子检验逻辑 → alphalens 原生
- × 策略信号生成 → 策略纯函数
- × 调度/监控 → ops 层

**薄壳的本质：它是"翻译官"，不是"总管"。** 把三套框架各自的方言翻译成同一种语言（薄壳 dataclass），但不替代任何框架做它擅长的事。

### 25.2 子问题 1：统一数据接口的签名 —— 统一"取数"，不统一"包装"

三个消费者要的格式完全不同，**硬往一个返回类型里塞是错的**：

| 消费者 | 要的格式 | 如果硬统一 |
|---|---|---|
| alphalens | DataFrame(index=date×ticker, columns=[factor, 1D_return,...]) | 要在统一接口里理解 alphalens 的 multi-index 语义—耦合 |
| backtrader | DataFeed 对象(OHLCV Line) | 要把 pandas → Line, 逻辑复杂 |
| vnpy | Gateway 回调 / BarData | 实时流,和批量取数完全不同 |

正确分层：

- **Layer 1: DataAPI（薄壳拥有）** —— 统一签名，返回薄壳自己的类型
- **Layer 2: Format Adapter（每个消费者一个，薄壳拥有）** —— 把 Layer 1 的输出转成消费者要的格式

DataAPI 签名：

```python
class DataAPI:
    def get_bars(self, tickers: list[str], start: date, end: date,
                 freq: str = "daily",          # "daily" | "minute"
                 fields: list[str] | None = None,
                 adjust: str = "hfq",          # "hfq" | "qfq" | "none"
    ) -> pd.DataFrame:
        """返回长表: datetime, ticker, open, high, low, close, volume, amount,
           pre_close, adjfactor, suspend, limit_up, limit_down, ...
           按 (datetime, ticker) 排序, 和底层存储解耦"""
    def get_factor(self, factor_name: str, tickers: list[str], start: date, end: date) -> pd.DataFrame:
        """返回长表: datetime, ticker, factor_value；从 factors/<name>/ 目录读"""
    def get_universe(self, date: date, filters: dict | None = None) -> list[str]:
        """返回某天的可交易票池; filters 如 {"suspend": False, "limit_up": False}"""
    def get_trading_calendar(self, start: date, end: date) -> list[date]:
        """返回交易日列表"""
```

底层实现：DataAPI 内部读 Parquet + DuckDB，**消费者看不到也不关心**——只依赖签名和返回类型。换存储引擎不改消费者代码。

**呼应公司 Udata 的"容器层 + Schema 分离"**——DataAPI 是容器，消费者按 Schema（字段名）取数，不关心底层是 ucube tensor 还是 parquet。

Format Adapters：

- `AlphalensAdapter.prepare_factor_data(factor_name, tickers, start, end, periods=(1,5,20)) -> (factor_series, pricing)` —— `factor_df.set_index(["datetime","ticker"])["factor_value"]` + `bars.pivot(index="datetime", columns="ticker", values="close")`（adjust="hfq"）
- `BTDataAdapter.create_feed(ticker, start, end) -> bt.feeds.PandasData` —— 自定义列透传 suspend 等；`create_multi_feeds(tickers, start, end) -> dict`
- `VNDataAdapter.get_history_for_init(ticker, lookback=100) -> list[VnBarData]` —— 策略启动时加载历史 bar 初始化指标

**关键分界**：DataAPI 管历史数据（批量）；实时数据走各引擎自己的通道（backtrader DataFeed 推 bar、vnpy Gateway 推 tick/bar），但**推过来后都被 Adapter 转成薄壳 BarData 再喂给策略**。

### 25.3 子问题 2：适配层回流 —— 实时流回流，不是批量对账

**每一笔状态变更当场写 Journal，不是"跑完再对账"。** 原因：

1. 实盘不能等"跑完"——实盘持续运行，必须实时知道当前持仓和可用资金，否则下一笔单的预检就是错的
2. 回测也应该实时回流——让回测和实盘走完全相同的回流路径，正是"精简版同路径"的意义

### 25.4 子问题 3：运行形态 —— 不分 Job，一个进程内分模块

单机单策略起步，分 Job（多进程/微服务）是纯粹的过度工程。一个 Python 进程内含：

```text
DataAPI(取数,按需加载,懒加载缓存)
Strategy(策略纯函数)
引擎(backtrader 或 vnpy,二选一)
Adapter(BT 或 VN)
AccountBook(内存 Order/Trade/Position/Account 表)
JournalWriter(append-only 写 Parquet)
SnapshotDumper(定时 dump 快照)
```

```text
回测模式: DataAPI + Strategy + backtrader + BTAdapter + AccountBook + Journal
实盘模式: DataAPI(仅历史初始化) + Strategy + vnpy + VNAdapter + AccountBook + Journal
```

```python
if mode == "backtest":
    engine = BacktestEngine(data_api, bt_adapter, strategy, account_book)
    engine.run(start, end)
elif mode == "live":
    engine = LiveEngine(data_api, vn_adapter, strategy, account_book)
    engine.start()  # 持续运行,等行情驱动
```

**什么时候才拆 Job**：多策略并行跑（每策略一进程，共享 DataAPI）；数据拉取和策略运行要时间隔离（数据 Job 凌晨跑完，策略 Job 盘中跑）。这些是"有了第二第三个策略"之后的事。**接口按分布式设计，但实现先单进程。**

### 25.5 子问题 4：策略生命周期 —— 管加载和参数注入，不管热更新

**加载 —— 管，约定优于配置**：

```text
strategies/
  momentum_20d/
    strategy.py       # 必须有 class Strategy(BaseStrategy)
    config.yaml       # 参数
    README.md         # 描述(可选)
```

```python
class BaseStrategy:
    """薄壳定义的策略基类(极简)"""
    def __init__(self, config: dict): self.config = config
    def on_bar(self, bars: dict[str, BarData]) -> list[SignalData]:
        """输入当前 bar, 输出 signal 列表。纯函数,无副作用。"""
        raise NotImplementedError
    def on_init(self, history: dict[str, pd.DataFrame]):
        """初始化(加载历史数据算指标等)"""
        pass
```

策略只需实现 `on_bar` 和 `on_init`，**不需要知道自己跑在 backtrader 还是 vnpy 上**。

**参数注入 —— 管，`config.yaml` → `strategy.config`**：lookback / top_n / slippage / commission_rate / capital / universe。参数变了改 yaml 不改代码；回测扫参就是批量生成不同 config 跑多次。

**实例化 —— 管但极简**：`load_strategy(strategy_dir)` = `yaml.safe_load` + `importlib.import_module`。不需要 IoC 容器/依赖注入框架。

**热更新 —— 不管**。原因：

1. 状态一致性极难保证（策略内部 buffer/指标状态，代码换了状态没重置）
2. **公司也不做实盘热更新**——Hubble/Utrade 的策略更新是重启进程（停旧实例，起新实例，持仓从 Journal 恢复），"秒级重启+自动恢复状态"比热更新简单 10 倍且可靠 10 倍
3. 日频/分钟频完全不需要。收盘后改策略 → 第二天开盘前重启。分钟频最多等当前 bar 结束（1 分钟）

如果将来需要"不停盘更新参数"，策略在每个 `on_bar` 开头 reload config 即可，**不需要框架级支持**。

### 25.6 文件结构与代码量

最终版文件结构：

```text
kernel/
  models/            # 产品契约(不依赖任何外部框架)
    bar.py / order.py / trade.py / position.py / account.py / journal.py / base_strategy.py
  data_api/          # 统一取数
    api.py           # DataAPI 类
    reader.py        # Parquet/DuckDB 读取(内部实现)
  adapters/
    data/            # 数据格式转换
      alphalens_adapter.py / bt_data_adapter.py / vn_data_adapter.py
    exec/            # 执行适配(下单+回流)
      bt_exec_adapter.py / vn_exec_adapter.py
  common/            # 公共逻辑
    pre_check.py / commission.py / lot.py / position_calc.py / frozen.py
  engine/            # 运行引擎(封装启动流程)
    backtest.py      # BacktestEngine: 组装 DataAPI + BT + Strategy
    live.py          # LiveEngine: 组装 DataAPI + VN + Strategy
    loader.py        # 策略发现/加载/参数注入
```

**代码量估算**：models(200行) + data_api(300行) + adapters(5个×150行=750行) + common(300行) + engine(200行) ≈ **1750 行**。这就是薄壳的全部。它不大，但它是整个平台所有组件的共同语言。

**薄壳边界清单**：

| 层 | 薄壳拥有 | 外包给 |
|---|---|---|
| 产品契约 | 5 个 dataclass + 枚举 + BaseStrategy | – |
| 数据接口 | DataAPI(get_bars/get_factor/get_universe) | 底层读 Parquet/DuckDB |
| 格式转换 | AL/BT/VN 三个 DataAdapter(取数→框架格式) | alphalens/backtrader/vnpy 各自的消费 |
| 执行适配 | BT/VN 两个 ExecAdapter(signal→下单→回流) | backtrader broker / vnpy gateway |
| 账本 | Order/Trade/Position/Account 内存表 + Journal | – |
| 公共逻辑 | 预检/佣金/手数/持仓更新/资金冻结 | – |
| 策略加载 | 发现/加载/参数注入 | – |
| 回测撮合 | – | backtrader |
| 实盘柜台 | – | vnpy |
| 因子计算 | – | pandas/polars + 算子库 |
| 因子检验 | – | alphalens |
| 调度/监控 | – | ops 层 |
| 热更新 | – | 不做(重启代替) |

**一句话总结**：两个 Adapter 各约 200 行（状态映射+字段转换）；公共逻辑约 300 行（两边共享）；最难的不是正常路径，而是**部分成交后撤单、回调乱序、最低佣金这些 corner case**——每个都必须有对应的测试 case，不然第一次实盘就翻车。

**本节仍未解决**：DataAPI 各方法返回长表的具体列全集与 dtype（已定 → §53.3 load_bars 签名，2026-10-10 回改）；Format Adapter 对 alphalens periods 的默认取值（已定 → §27 periods=(1, 5, 10, 20)，2026-10-10 回改）；多策略共享 DataAPI 时的缓存失效规则。

---

## 26. 截面策略的回测路径（A+C 混合：自研截面组合模拟器）

> **回改（2026-10-10）**：本节的 backtrader / vnpy 相关结论已被 §66.2 / §68 修正——**backtrader/vnpy 不用**（pandas 自研 <300 行 + SimBroker/QmtBroker）；alphalens-reloaded 与自研路径不受影响；VectorBT 留 V2 备选（§68.6 框架存储偏好表）。以 §66/§68 为准。

> 回应 §1.4（截面矩阵策略在 backtrader 里怎么跑）。本节细化 §25.5（策略基类）与 §25.6（文件结构与代码量）。

### 26.1 问题本质：两种策略范式根本不同

| | 截面策略（alstm） | 时序策略（CTA） |
|---|---|---|
| 计算方式 | 每天收盘 → 全市场 300 只票一起算 → 输出目标权重矩阵 `[ticker: weight]` | 每根 bar → 逐个票判断买卖 → 输出单票信号 |
| 决策单位 | 一个截面（所有票同时） | 一个标的的时间序列 |
| 计算形态 | 纯矩阵运算，天然向量化 | if/else 状态机，天然事件驱动 |

**这不是"接口不一样"的问题，是思维范式不一样。** 硬把截面策略塞进 backtrader，就像用 Excel 跑矩阵运算——能做但极其别扭。

### 26.2 为什么排除 B：backtrader 多标的 Cerebro 扛不住截面

backtrader 确实支持一个 Cerebro 带 N 个 DataFeed，在 `next()` 里访问所有 feed：

```python
class CrossSectionalStrategy(bt.Strategy):
    def next(self):
        # 遍历 300 个 data feed,取 close,组成截面
        closes = {d._name: d.close[0] for d in self.datas}
        weights = self.model.predict(closes)  # alstm 推理
        # 按 weights 下单...
```

**实际问题**：

- 300 个 DataFeed 各自维护 OHLCV Line 对象，内存 × 300，**backtrader 的内存管理在 50+ feed 后急剧恶化**
- backtrader 的内部 broker 对每个 feed 单独记账，300 只票的 `notify_order`/`notify_trade` **回调风暴**
- 每根 bar 的 `next()` 调用要遍历 300 个 feed 拿数据，**速度不可能快过直接操作 DataFrame**
- **最致命**：你的截面策略是先算出完整的目标权重矩阵，再对比当前持仓，产出一批调仓指令——这个"对比→差额→批量下单"的逻辑在 backtrader 里没有原生支持，全要自己写，**而且写完了本质上就是你自己的组合管理器，backtrader 只剩一个撮合器**

### 26.3 为什么排除 D：VectorBT 做截面可以，但缺真实约束

VectorBT 擅长向量化回测，截面策略天然契合。但 VectorBT 的短板正好是必须建模的部分：

- **整数手数取整**：VectorBT 天然处理连续权重，不好做手数
- **停牌/涨跌停拒单**：VectorBT 没有逐笔检查机制
- **部分成交**：不支持
- **和统一账本（Journal）对接**：需要额外适配

把 VectorBT 当"对照组"保留可以（旧引擎已降为对照组），**但不适合当主力**。

### 26.4 结论：A + C 的混合，分两层

```text
策略信号层(截面/时序都在这里)
  ├─→ 截面策略(alstm)
  │     输出: 目标权重矩阵
  │     ↓
  │   截面组合模拟器(自研,向量化)
  │     · 目标权重 → 调仓单
  │     · 逐笔检查约束
  │     · 成本/滑点/手数
  │
  └─→ 时序策略(CTA)
        输出: 逐票 signal
        ↓
      backtrader(事件驱动)
        · signal → 下单
        · 撮合 + 回报
        · 成本/滑点
  ↓
薄壳 Journal(同一套 Order/Trade/Position/Account)
```

**核心思路**：

- **截面策略走自研的截面组合模拟器**（向量化 + 逐笔约束检查）——对应候选 A（上层自研截面组合层）+ C（截面不走 backtrader）
- **时序/CTA 策略走 backtrader**（事件驱动）
- **两条路径最终都输出到同一个 Journal**，下游分析 / 对拍 / 净值计算完全统一

### 26.5 截面组合模拟器：该有什么

**这不是"再写一个回测引擎"，而是一个很薄的调仓模拟层（约 300–400 行）。**

它做什么（每个调仓日执行一次）：

```text
输入：  target_weights[ticker] = 0.05, 0.03, ...  （来自 alstm）
       current_positions（来自 AccountBook）
       current_bars（来自 DataAPI）
       account.cash
输出：  一批 Order → Trade → Position 变动 → Journal 条目
```

逐步：

```python
def rebalance(target_weights, account_book, bars, account):
    """一次截面调仓的完整模拟"""

    # [1] 目标持仓 vs 当前持仓 → 差额
    current = account_book.get_positions()
    target_values = {t: w * account.total_asset for t, w in target_weights.items()}
    target_shares = {}
    for ticker, value in target_values.items():
        bar = bars[ticker]
        raw = value / bar.close
        target_shares[ticker] = floor_to_lot(raw, lot_size=100)

    diff = {}
    for ticker in set(list(target_shares.keys()) + list(current.keys())):
        target = target_shares.get(ticker, 0)
        held = current.get(ticker, Position()).quantity
        if target != held:
            diff[ticker] = target - held  # 正=买, 负=卖

    # [2] 先卖后买(释放资金)
    sell_orders = {t: qty for t, qty in diff.items() if qty < 0}
    buy_orders  = {t: qty for t, qty in diff.items() if qty > 0}

    # [3] 逐笔检查约束 + 模拟成交
    trades = []
    for ticker, qty in sell_orders.items():   # 先卖
        order, trade = simulate_single_trade(
            ticker, Direction.SELL, abs(qty), bars[ticker], account, account_book
        )
        if trade:
            trades.append(trade)

    for ticker, qty in buy_orders.items():    # 再买
        order, trade = simulate_single_trade(
            ticker, Direction.BUY, qty, bars[ticker], account, account_book
        )
        if trade:
            trades.append(trade)

    return trades
```

### 26.6 simulate_single_trade：复用薄壳的全部公共逻辑

```python
def simulate_single_trade(ticker, direction, qty, bar, account, book):
    """模拟一笔成交,走薄壳的完整检查链"""

    # 预检(复用 common/pre_check.py)
    reject = pre_check_bar(direction, bar)   # 停牌/涨跌停
    if reject:
        order = create_rejected_order(ticker, direction, qty, bar, reject)
        journal(ORDER_REJECTED, order)
        return order, None

    # 资金/手数检查(复用 common/lot.py, common/frozen.py)
    if direction == BUY:
        affordable = int(account.cash / (bar.close * 1.0003))
        qty = min(qty, floor_to_lot(affordable))
    if qty <= 0:
        order = create_rejected_order(ticker, direction, 0, bar, "qty_zero_after_round")
        journal(ORDER_REJECTED, order)
        return order, None

    # 成交量约束(可选:不能超过当日成交量的 X%)
    max_vol = int(bar.volume * 0.1)   # 不超过当日量的 10%
    qty = min(qty, floor_to_lot(max_vol))

    # 模拟成交价(复用成本模型)
    if direction == BUY:
        fill_price = bar.close * (1 + SLIPPAGE)   # 次日开盘价更真实,日频可用 open
    else:
        fill_price = bar.close * (1 - SLIPPAGE)

    # 创建 Order + Trade(和 BT/VN Adapter 输出完全相同的 dataclass)
    order = Order(
        order_id=generate_id(),
        portfolio_id=book.portfolio_id,
        datetime=bar.datetime,
        ticker=ticker,
        direction=direction,
        quantity=qty,
        price_type=PriceType.MARKET,
        ref_price=bar.close,
        status=OrderStatus.FILLED,
        filled_qty=qty,
        frozen_cash=0,          # 日频截面:当天算完当天成交,不需要冻结
    )

    commission = calc_commission(fill_price * qty, direction)  # 复用 common/commission.py

    trade = Trade(
        trade_id=generate_id(),
        order_id=order.order_id,
        portfolio_id=book.portfolio_id,
        datetime=bar.datetime,
        ticker=ticker,
        direction=direction,
        filled_qty=qty,
        fill_price=fill_price,
        commission=commission,
        slippage=fill_price - bar.close,
    )

    # 更新持仓/账户(复用 common/position_calc.py)
    update_position(book.get_position(ticker), trade)
    update_account(account, order, trade)

    # 写 Journal(和 BT/VN Adapter 写的格式完全一致)
    journal(TRADE, trade)
    journal(ORDER_FILLED, order)

    return order, trade
```

### 26.7 它不做什么

| 不做 | 为什么 |
|---|---|
| 部分成交模拟 | 日频截面策略假设当天能成交（不能的已被预检拦）；分钟频截面再加 |
| 订单状态机（SUBMITTED→PARTIAL→…） | 日频截面是"收盘算完→下一天成交"，不需要中间状态；直接 CREATED→FILLED/REJECTED |
| 限价单 / 止损单 | 截面策略是"调到目标权重"，本质是市价单（以下一日开盘/VWAP 成交） |
| 事件回调 | 没有异步回调，一个 for 循环走完一天的全部调仓 |

**这就是为什么它不是"另一个回测引擎"——它只做"给定目标权重，模拟一次调仓"这一件事**，约束检查和成本计算全部复用薄壳的 `common/` 模块。

**关键**：数据加载是一次性批量（向量化的快），信号计算是向量化（矩阵运算），但**成交模拟是逐笔（保证约束检查的完整性）**。**快在该快的地方（数据+信号），严在该严的地方（执行）。**

### 26.8 backtrader 在整个平台里的定位

```text
你的平台
├─ 截面策略(alstm) → 截面组合模拟器(cs_portfolio_sim)
├─ 时序策略(CTA)   → backtrader(事件驱动)
└─ 实盘(所有策略)  → vnpy(Gateway)
        ↓
   薄壳 Journal(统一)
```

| 引擎 | 服务谁 | 为什么 |
|---|---|---|
| 截面组合模拟器（自研） | alstm 等截面策略回测 | 天然匹配截面范式；复用薄壳公共逻辑 |
| backtrader | CTA/单标的/时序策略回测 | 事件驱动天然匹配；成熟的 broker 模拟 |
| vnpy | 所有策略实盘 | 柜台网关；实盘都走它 |
| VectorBT（对照组） | 快速筛选/对照验证 | 旧引擎降级，不当主力 |

**backtrader 不是全平台的唯一回测引擎，而是"事件驱动类策略的回测引擎"。** 截面策略有自己的回测路径。两条路径输出到同一个 Journal，下游完全统一。

### 26.9 截面策略的 BaseStrategy 接口（与时序策略的区别）

```python
# 时序策略基类(已有,喂 backtrader)
class BaseStrategy:
    def on_bar(self, bars: dict[str, BarData]) -> list[SignalData]:
        """每根 bar 调一次,输出单票信号"""
        raise NotImplementedError

# 截面策略基类(新增,喂截面组合模拟器)
class BaseCrossStrategy:
    def on_rebalance(
        self,
        date: date,
        bars: dict[str, BarData],
        positions: dict[str, Position],
        account: Account,
    ) -> dict[str, float]:
        """每个调仓日调一次,输出目标权重字典 {ticker: weight}"""
        raise NotImplementedError

    @property
    def universe(self) -> list[str]:
        """策略的标的池"""
        raise NotImplementedError

    @property
    def rebalance_freq(self) -> str:
        """调仓频率: "daily" | "weekly" | "monthly" """
        return "daily"
```

alstm 实现：

```python
class AlstmStrategy(BaseCrossStrategy):
    def __init__(self, config):
        self.model = load_model(config["model_path"])
        self._universe = config["universe_tickers"]

    @property
    def universe(self):
        return self._universe

    def on_rebalance(self, date, bars, positions, account):
        # 构建特征矩阵(截面 × 因子)
        features = self._build_features(date, bars)
        # 模型预测
        scores = self.model.predict(features)
        # scores → 目标权重(用 §16 的多因子合成方法)
        weights = self._scores_to_weights(scores, bars, account)
        return weights   # {ticker: 0.05, ticker2: 0.03, ...}
```

### 26.10 在薄壳里的位置 + 截面回测引擎

**截面组合模拟器是薄壳的一部分，不是独立模块。** 因为它：

- 直接使用薄壳 `models/`（Order/Trade/Position/Account）
- 直接使用薄壳 `common/`（预检/佣金/手数/持仓更新）
- 直接写薄壳 Journal

文件结构（在 §25.6 基础上新增两个文件）：

```text
kernel/
  models/            # 不变
  data_api/          # 不变
  common/            # 不变
  adapters/
    data/            # 不变
    exec/
      bt_exec_adapter.py      # 时序策略 → backtrader
      vn_exec_adapter.py      # 实盘 → vnpy
      cs_portfolio_sim.py     # 截面策略 → 组合模拟器(新增,约 300 行)
  engine/
    backtest.py      # 时序回测
    backtest_cs.py   # 截面回测(新增)
    live.py          # 实盘
    loader.py
```

截面回测引擎（**向量化外壳 + 逐笔内核**）：

```python
class CrossSectionalBacktest:
    """截面策略的回测引擎"""

    def __init__(self, data_api, strategy, account_book, config):
        self.api = data_api
        self.strategy = strategy
        self.book = account_book
        self.sim = CSPortfolioSimulator(account_book)   # 组合模拟器

    def run(self, start, end):
        calendar = self.api.get_trading_calendar(start, end)

        # 一次性加载全部行情(向量化的快)
        all_bars = self.api.get_bars(
            tickers=self.strategy.universe,
            start=start, end=end,
            adjust="hfq"
        )

        # 逐交易日推进
        for di, trade_date in enumerate(calendar):
            today_bars = all_bars[all_bars["datetime"] == trade_date]
            bar_dict = {row["ticker"]: BarData(**row) for _, row in today_bars.iterrows()}

            # [信号层] 策略产出目标权重(向量化,整个截面一起算)
            target_weights = self.strategy.on_rebalance(
                date=trade_date,
                bars=bar_dict,
                positions=self.book.get_positions(),
                account=self.book.account,
            )

            # [执行层] 模拟调仓(逐笔检查约束,复用薄壳公共逻辑)
            if target_weights:
                self.sim.rebalance(target_weights, bar_dict, self.book.account)

            # 日终更新市值
            self.book.mark_to_market(bar_dict)
            self.book.snapshot(trade_date)   # 记录日终净值快照
```

### 26.11 汇总：这个决策对平台整体的影响

| 影响点 | 结论 |
|---|---|
| alstm 走哪条路 | 自研截面组合模拟器（`cs_portfolio_sim.py`），不走 backtrader |
| 模拟器归属 | 薄壳的一部分（`kernel/adapters/exec/cs_portfolio_sim.py`，约 300 行） |
| 模拟器复用什么 | 薄壳 `common/` 的全部公共逻辑（预检/佣金/手数/持仓/冻结） |
| 模拟器输出什么 | 和 BT/VN Adapter **完全相同**的 Order/Trade/Journal 条目 |
| backtrader 定位 | CTA/时序策略的回测引擎，不服务截面策略 |
| 策略基类 | **两个**：`BaseStrategy`（时序，`on_bar`）、`BaseCrossStrategy`（截面，`on_rebalance`） |
| Journal 是否统一 | 统一。两条回测路径 + 实盘，**三条路都写同一个 Journal 格式** |
| 新增代码量 | `cs_portfolio_sim.py`(300行) + `backtest_cs.py`(150行) + `BaseCrossStrategy`(30行) ≈ **480 行** |

**代码量更新**：§25.6 的 ≈1750 行 + 本节 480 行 ≈ **2230 行**，就是薄壳全部。

**这个架构的关键优势**：截面策略的信号计算是纯矩阵运算（向量化的快），执行模拟复用薄壳公共逻辑（约束的严），输出和 backtrader/vnpy 路径完全兼容（分析的统一）。**不需要把截面逻辑硬塞进 backtrader 的 per-bar 框架，也不需要重写一个完整的回测引擎。**

**本节仍未解决**：成交量约束比例阈值（已定 → §66 check_split_needed >1% 日成交量，示例 10% 作废，2026-10-10 回改）；分钟频截面的部分成交模拟；`rebalance_freq` 为 weekly/monthly 时的调仓日判定规则。

---

## 27. Alphalens 接入架构与因子评价流水线

> 回应 §1.5（alphalens 怎么接入）、§3.10（复权口径）、§4.8（与三件套的数据对接）、§6.4（因子存储布局）、§6.5（因子管理与治理）。本节细化 §25.2（Format Adapter）。

### 27.1 Context 与四个子问题

平台代码尚未实现，处于架构设计阶段。前几轮已确定薄壳（`kernel/`）的数据模型、DataAPI、适配器体系、截面组合模拟器等。本节解决 **alphalens 如何接入平台**，回答四个子问题：**因子数据来源、pricing 口径、产出落地、与回测的关系**。

### 27.2 Q1：因子数据从哪来 —— 两阶段，统一入口

| 阶段 | 来源 | 说明 |
|---|---|---|
| 开发期（draft） | 研究层现算 | `FactorPipeline.compute(expr, universe, start, end)` 从 DataAPI 取原始行情，内存计算因子 |
| 注册后（active） | `data_platform` 因子表 | `DataAPI.get_factor(name, start, end)` 读 `data_platform/factors/{name}/*.parquet` |

**桥接**：`FactorPipeline` 抽象了这个差异。开发期直接算，注册后直接读。**alphalens 始终收到相同 shape 的 DataFrame，不感知来源。**

### 27.3 Q2：Pricing —— 日频收盘价，前复权（qfq）

- **频率：日频收盘价。** alphalens 计算 1D/5D/10D/20D 前向收益，**分钟数据在因子评价阶段无意义**
- **复权：前复权（qfq）。** 保留最近价格水平，历史收益率可比。用已有方案：`qfq_close = raw_close * adjfactor / adjfactor_latest`
- 适配器调用 `DataAPI.get_bars(tickers, start, end, adjust='qfq')`，pivot 成 alphalens 要求的 `(DatetimeIndex, columns=tickers)` 宽表

> **对 §25.2 的修正**：§25.2 示例写的是 `adjust="hfq"` 和 `prepare_factor_data`，本节定稿为 **`adjust="qfq"`** 且方法名改为 `to_al_factor` / `to_al_prices`（见 §27.6）。因子评价看历史收益率可比性用 qfq；回测成交价基准另按 §18.2 处理。

### 27.4 Q3：产出落到三层

| 产出 | 落地位置 | 格式 |
|---|---|---|
| 标量指标（mean IC, IR, t, 单调性） | `factor_registry.yaml` 的 `eval:` 块 | YAML |
| IC 时间序列、分位收益曲线、换手序列 | `data_platform/factor_eval/{factor_name}/` | Parquet |
| 完整 tearsheet（图+表） | `research/reports/{factor_name}_{date}.html` | HTML |

**标量指标是决策面**——决定因子是否进入回测。时间序列留作深度诊断。HTML 报告是人读的。

### 27.5 Q4：alphalens 与回测的关系 —— 顺序门控，非并行

```text
因子想法 → 计算 → 预处理 → alphalens 评价 → PASS/FAIL
                                    │
                        FAIL: 迭代 ┘└─ PASS: 注册
                                              ↓
                                    策略集成(backtrader 或 cs_portfolio_sim)
```

**alphalens 是回测前的必要过滤器**：

- 全组合回测计算量大（小时级），alphalens 评价便宜（分钟级）
- **IC > 0.04 / IR > 0.5 / t > 2 作为量化门槛**，防止噪声因子进入策略
- 只有通过 alphalens 门控的因子才会被组合进策略，送入 backtrader（CTA）或 `cs_portfolio_sim`（截面）

**反馈回路**：回测后发现组合表现差（因子间相关、换手成本等），反馈到研究层调整因子组合或淘汰。

### 27.6 架构设计：数据流与新增文件

```text
DATA PLATFORM                          RESEARCH (research/)
  Raw Parquet (OHLCV+adj)  ─────────→  FactorPipeline
  Factor Store (registered) ────────→    .compute()     现算因子
                                          .preprocess()  MAD → zscore → 中性化
                                          .evaluate()    调 AlphalensEval
                                          .register()    写注册表 + 持久化
                                                ↓
                                    AlphalensEvaluator  ← kernel/
                                      1. AlphalensAdapter.to_al_*()
                                      2. alphalens 核心计算
                                      3. extract_metrics() + check_gate
                                      4. → EvalResult
                                                ↓
                            PASS ↓          FAIL ↓           ↓
                      注册到 registry   记录 attempt    生成 HTML 报告
                      持久化 eval Parquet  N_attempts++
                                                ↓
EXECUTION (execution/)
  DataAPI.get_factor(registered_name) → 截面策略 / CTA 策略
```

**新增文件（~4 个核心文件）**：

| 文件 | 位置 | 职责 | 约行数 |
|---|---|---|---|
| `alphalens_adapter.py` | `kernel/adapters/data/` | 格式转换（platform ↔ alphalens）+ 指标提取 + 门控逻辑 | ~120 |
| `alphalens_eval.py` | `research/` | 编排器：串 DataAPI + adapter + alphalens 库 → EvalResult | ~150 |
| `factor_lab.py` | `research/` | 用户入口：compute → preprocess → evaluate → register | ~200 |
| `factor_registry_io.py` | `research/` | 读写 `factor_registry.yaml`，管理 attempt_log | ~100 |

**辅助**：

- `research/preprocessing.py` —— MAD/zscore/中性化（~80 行）
- `research/reports/` —— 生成的 HTML tearsheet 目录

### 27.7 AlphalensAdapter：四个静态方法

薄壳格式适配器，与 `BTDataAdapter`、`VNDataAdapter` 同级。**纯静态方法，无状态。**

```python
class AlphalensAdapter:
    @staticmethod
    def to_al_factor(factor_df: pd.DataFrame) -> pd.Series:
        """宽表(date × ticker) → MultiIndex Series(date, asset)"""
        # stack() + sort_index + rename levels

    @staticmethod
    def to_al_prices(bars_df: pd.DataFrame, price_col='close') -> pd.DataFrame:
        """长表(ticker, date, OHLCV) → 宽表(DatetimeIndex × ticker columns)"""
        # pivot + ffill(limit=5) for suspensions

    @staticmethod
    def extract_metrics(factor_data, ic_series, mean_quant_ret) -> dict:
        """从 alphalens 输出提取平台标准指标"""
        # rank_ic_mean, ir, ic_t_stat, quantile_monotonicity,
        # top_minus_bottom, turnover

    @staticmethod
    def check_gate(metrics: dict, n_attempts: int = 1) -> tuple[bool, dict]:
        """应用门控阈值: IC>0.04, IR>0.5, t>max(2, sqrt(2*ln(N)))"""
```

**关键决策：alphalens 在方法体内 lazy import，`kernel/` 模块级不依赖 alphalens。**（呼应 §25.1 薄壳不碰因子检验逻辑）

**注意 `to_al_prices` 的 `ffill(limit=5)`**：停牌期价格前向填充最多 5 天，避免长停牌把整列拉成 NaN——这是 §21.3 停牌/复权边界坑在因子评价侧的对应处理。

### 27.8 EvalResult 与 factor_registry.yaml

```python
@dataclass
class EvalResult:
    summary: dict                     # 标量指标
    gate_passed: bool                 # 是否通过门控
    gate_detail: dict                 # 每个阈值的通过/未通过
    ic_series: pd.DataFrame           # 日频 IC 序列(1D/5D/10D/20D)
    quantile_returns: pd.DataFrame    # 分位平均收益
    turnover: pd.DataFrame            # 分位换手率
    report_path: str | None           # HTML 报告路径
```

`factor_registry.yaml` 结构（eval 相关部分）：

```yaml
factors:
  momentum_20d:
    formula: "close / rolling_mean(close, 20) - 1"
    version: 1
    status: active    # draft | active | deprecated | rejected
    eval:
      rank_ic_mean: 0.052
      ir: 0.473
      ic_t_stat: 3.21
      quantile_monotonicity: 0.90
      gate_passed: true
      eval_date: "2024-06-15"
      report: "research/reports/momentum_20d_20240615.html"
    attempts: 3
    attempt_log:
      - date: "2024-06-10"
        gate_passed: false
        rank_ic_mean: 0.031
      - date: "2024-06-15"
        gate_passed: true
        rank_ic_mean: 0.052
```

`status` 四态：`draft | active | deprecated | rejected`。`eval` 块兼容已有 `name/formula/version/status` 结构。

### 27.9 多重检验调整：attempt_log 与 t 阈值

**`attempt_log` 追踪每个因子族的 N（包括失败）。N 增长时 t 阈值升高：**

| N | t 阈值 | 公式核对 `sqrt(2·ln N)` |
|---|---|---|
| 1–9 | 2.00（底线） | 取 `max(2, ·)` |
| 10 | 2.15 | 2.146 |
| 50 | 2.79 | 2.797 |
| 100 | 3.03 | 3.035 |
| 200 | 3.26 | 3.255 |

门控公式：`t > max(2, sqrt(2*ln(N)))` —— 即**基础底线 2.00，叠加 Šidák 型多重检验上抬**。

**范围：per-factor-family，不是全局。** momentum 类因子的迭代只影响 momentum 的 N。这避免了"挖过很多 value 因子"把 momentum 的门槛也抬高。

> **与 §16.5 的呼应**：§16.5 说"每调一次都是一次尝试，N 又上去了"。这里的 `attempt_log` 就是那个 N 的落地实现。

### 27.10 典型使用流程

```python
pipe = FactorPipeline(data_api)

# 1. 计算因子(开发期,现算)
raw = pipe.compute(
    expr="close / rolling_mean(close, 20) - 1",
    universe_fn=lambda d: data_api.get_universe(d),
    start="2020-01-01", end="2024-12-31",
)

# 2. 预处理
processed = pipe.preprocess(raw)   # MAD → z-score → industry+mcap 中性化

# 3. alphalens 评价
result = pipe.evaluate(processed, periods=(1, 5, 10, 20))

# 4. 看结果
print(result.summary)      # {'rank_ic_mean': 0.052, 'ir': 0.473, ...}
print(result.gate_passed)  # True
# 浏览器打开 result.report_path 看完整 tearsheet

# 5. 注册(通过门控后)
if result.gate_passed:
    pipe.register(name="momentum_20d", expr="...", result=result)
    # → 因子值持久化到 data_platform/factors/momentum_20d/
    # → eval 指标写入 factor_registry.yaml
    # → eval 时间序列写入 data_platform/factor_eval/momentum_20d/

# 6. 下游策略消费(通过 DataAPI,不直接依赖 alphalens)
# 截面策略: DataAPI.get_factor("momentum_20d", ...) → on_rebalance()
# CTA策略:  DataAPI.get_factor("momentum_20d", ...) → on_bar()
```

**下游策略不直接依赖 alphalens**，只通过 `DataAPI.get_factor()` 消费注册后的因子。

### 27.11 定期重评估

活跃因子**月度**重评估，IC 衰减到退出线（**IC < 0.03**）时标记 `under_review`：

```python
for factor in registry.list_active_factors():
    result = evaluator.run(DataAPI.get_factor(factor, recent_3y, today))
    if not result.gate_passed:
        registry.update_status(factor, 'under_review')   # 告警
```

**三个阈值形成一套生命周期**：准入 IC > 0.04、退出 IC < 0.03、中间是观察区。

### 27.12 对已有架构的影响

| 影响点 | 结论 |
|---|---|
| `kernel/` 新增 | `adapters/data/alphalens_adapter.py` 一个文件，~120 行 |
| `kernel/` 已有模块 | **无修改**。AlphalensAdapter 与 BT/VN Adapter 同级同模式 |
| DataAPI | **无修改**。已有的 `get_bars()` + `get_factor()` 完全满足需求 |
| `research/` | 新增 4 个文件（~530 行），**这是 alphalens 集成的主体** |
| `execution/` | **无修改**。通过 `DataAPI.get_factor()` 消费注册后的因子，不依赖 alphalens |
| `factor_registry.yaml` | 扩展 `eval` 块 + `attempt_log`，兼容已有 name/formula/version/status 结构 |

**代码量**：薄壳内只 +120 行（`alphalens_adapter.py`），其余 ~530 行在 `research/`，不进薄壳。

### 27.13 验证方式

1. **单元测试**：AlphalensAdapter 的 4 个静态方法各写 pytest，用固定小数据集验证格式转换正确性
2. **集成测试**：用 3–5 只票 × 1 年日频数据，跑完整 FactorPipeline 流程（compute → preprocess → evaluate → register），验证 `factor_registry.yaml` 写入正确、Parquet 文件落盘
3. **门控测试**：构造已知 IC 的因子（如 `next_day_return` 本身，IC 应接近 1.0），验证 `check_gate` 通过；构造随机因子，验证 `check_gate` 拒绝
4. **回归对照**：同一因子直接调 alphalens API vs 通过 FactorPipeline，**IC/IR 数值应完全一致**

**本节仍未解决**：`factor_registry.yaml` 与 alphaType 注册表的关系（已定 → 两套各司其职：因子侧 FactorRegistry+metadata.json 代码即注册表 §62.3，策略侧 alphaType JSON 双表 §65，2026-10-10 回改）；`periods` 默认取值 `(1, 5, 10, 20)`（正文已定、清单笔误列入未决，2026-10-10 回改）；`under_review` 之后的降级/淘汰流程（已定 → §62.3/§62.4：warning→降权/提前重训、exit→deprecated 不物理删除，2026-10-10 回改）；`factor_eval/` Parquet 的具体列 schema。

---

## 28. 六套引擎收敛与三件套版本定稿

> 回应 §1.6（六套引擎共存还是收敛）、§1.7（三件套的具体版本与形态）。本节修正 §20.7 与 §26.8 中"VectorBT 降为对照组"的表述。

> **回改（2026-10-10）**：本节的 backtrader / vnpy 相关结论已被 §66.2 / §68 修正——**backtrader/vnpy 不用**（pandas 自研 <300 行 + SimBroker/QmtBroker）；alphalens-reloaded 与自研路径不受影响；VectorBT 留 V2 备选（§68.6 框架存储偏好表）。以 §66/§68 为准。

### 28.1 先摆事实：六套引擎的现状

| 引擎 | 许可证 | Python 3.10 | 维护状态 | 关键风险 |
|---|---|---|---|---|
| backtrader 1.9.78.123 | GPLv3+ | 可用（无 collections.abc 问题） | 2020 年后停更 | GPL 传染性；无人修 bug |
| vnpy 4.x | MIT | 4.0+ 要求 ≥3.10 | 活跃 | pip install 拖 PySide6 等 UI 依赖 |
| alphalens 0.4.0（原版） | Apache 2.0 | 仅标注到 3.5 | Quantopian 2020 关闭，已归档 | 不兼容现代 pandas |
| alphalens-reloaded 0.4.6 | Apache 2.0 | ≥3.10 | 活跃（stefan-jansen/ml4trading） | 无 |
| VectorBT 0.28.x | Apache 2.0 + Commons Clause | 可用 | OSS 基本停更，作者转 Pro | Commons Clause 禁止商用售卖；Pro 付费 |
| Qlib（pyqlib）0.9.7 | MIT | ≥3.8 | 活跃（Microsoft） | Alpha 状态；数据管线重，自带回测/模型全家桶 |
| ZVT（本地 fork） | — | 不确定 | 看你 fork 的版本 | 本地 fork 等于你自己维护 |

### 28.2 三件套的版本决定

#### backtrader：接受社区版，GPL 风险可控

**用 1.9.78.123（唯一正式版）。**

GPLv3 的传染性问题：

- GPL 要求"基于 GPL 作品的衍生作品"也开源为 GPL
- 但你是个人平台，不分发、不卖软件。GPL 的义务只在"分发"时触发。自己用、不发布，不受限
- 你的薄壳通过 `BTExecAdapter` 调用 backtrader，是进程内调用。如果未来要开源/商用薄壳，需要注意这一层的隔离。但个人平台阶段完全不用担心

为什么不换 nautilus_trader / bt / 自研：

- **nautilus_trader** 最新版要求 Python ≥3.12，3.10 只能装旧版 1.202.0 锁死
- **bt**（pmorrissette）太轻量，broker 模拟不如 backtrader
- **自研 CTA 回测引擎**：你已经要自研截面组合模拟器了，再自研 CTA 引擎是两条战线。先用 backtrader 跑通，等它真的卡住你了再换

backtrader 停更的实际影响：

- 它是一个成熟的、功能完备的事件驱动回测框架，不是一个半成品
- CTA/时序策略的回测需求是稳定的，不需要框架频繁更新
- 如果遇到 bug，1.9.78 的代码量可控（纯 Python），你可以直接 patch

#### vnpy：精简版，只取 Gateway + 事件引擎

**用 vnpy 4.x（要求 ≥3.10，正好匹配）。不装主包，只装网关子包。**

```python
# 不要: pip install vnpy          ← 拖 PySide6/pyqtgraph 等 UI 依赖
# 要:
pip install vnpy-ctp              # CTP 柜台网关（期货）
pip install vnpy-ib               # IB 网关（如果做美股）
# 从 vnpy 源码复制 event/engine.py + trader/object.py + trader/gateway.py
# 或者 pip install vnpy 后在代码里只 import 这三个模块（UI 依赖装了但不用）
```

vnpy 4.x 的模块分离足够干净：

- `vnpy.event.engine` —— 独立事件引擎
- `vnpy.trader.gateway` —— Gateway 基类
- `vnpy.trader.object` —— TickData/BarData/OrderData 等数据类
- `vnpy.trader.ui/` —— UI 层，完全不碰

你的 `VNExecAdapter` 对接的是 vnpy 的 Gateway + EventEngine，**不需要 VnTrader 主应用**。UI 依赖是安装时的噪声，运行时不会加载。如果连安装都不想要 PySide6，从源码 vendor 进来 `event/` + `trader/`（不含 `ui/`）即可，MIT 许可允许这么做。

#### alphalens：用 alphalens-reloaded

**用 alphalens-reloaded 0.4.6，不用原版。**

理由直截了当：

- 原版 quantopian/alphalens 0.4.0 标注 Python 3.5，Quantopian 2020 关门后归档，不兼容现代 pandas（MultiIndex API 变化、pd.Timedelta 变化等）
- alphalens-reloaded 是 stefan-jansen 维护的官方社区续版，Apache 2.0，0.4.6 要求 ≥3.10，完美匹配
- API 完全兼容原版，只是修了 pandas/numpy 兼容性

```bash
pip install alphalens-reloaded    # 不是 pip install alphalens
```

### 28.3 六套引擎的定位：一张表

收敛后的架构（4 保留 + 3 归档；原文标题写"4 保留 + 2 淘汰"，但下表实际淘汰 VectorBT / ZVT / Qlib 三套，其中 Qlib 标"淘汰（当前），保留接口"）：

| 角色 | 引擎 | 保留/淘汰 | 定位 |
|---|---|---|---|
| CTA/时序回测 | backtrader | 保留（主力） | 事件驱动，单标的/多标的的时序策略的回测引擎 |
| 截面回测 | 自研 `cs_portfolio_sim` | 保留（主力） | 向量化 + 逐笔约束，截面策略（alstm）的回测引擎 |
| 实盘执行 | vnpy（Gateway+EventEngine） | 保留（主力） | 柜台网关，所有策略实盘都走它 |
| 因子评价 | alphalens-reloaded | 保留（主力） | 单因子 IC/IR/分位评价，回测前的门控 |
| 快速截面筛选 | VectorBT | 淘汰 | 见 §28.4 |
| —— | ZVT（本地 fork） | 淘汰 | 见 §28.4 |
| ML 管线 | Qlib | 淘汰（当前），保留接口 | 见 §28.4 |

### 28.4 逐个分析去留

#### VectorBT → 淘汰

之前的思路是"截面快筛用 VectorBT"。但现在你有了自研 `cs_portfolio_sim`：

- `cs_portfolio_sim` 的信号层是向量化的（批量加载 + 矩阵运算），速度不输 VectorBT
- `cs_portfolio_sim` 多了约束检查（停牌/涨跌停/手数/佣金），VectorBT 做不到
- 留着 VectorBT 意味着维护一套额外的数据适配 + 结果转换，只为了"快"——但你的 `cs_portfolio_sim` 已经够快了

Commons Clause 许可也是一个隐患：虽然个人平台不卖软件，但这个许可比 MIT/Apache 复杂，留着没有收益。

#### ZVT → 淘汰

本地 fork 意味着：

- 没有上游更新，你自己维护
- ZVT 的定位是"一站式"（数据+因子+回测+可视化），和你的分层架构冲突——它想管所有事，你的架构是每层有专门的引擎
- 如果有某个 ZVT 的具体功能你在用（比如某个数据源的爬虫），可以单独提取那个模块，但不需要保留整个 ZVT

#### Qlib → 当前淘汰，但保留未来接口

Qlib 的价值在 ML 管线（数据处理 → 模型训练 → 预测 → 回测的一体化流程）。问题是：

- Qlib 有自己的数据格式（二进制存储）、自己的回测器、自己的 workflow。这是另一个"全家桶"，和你的分层架构冲突
- 如果要用 Qlib 的 ML 模型（LightGBM/LSTM/Transformer），你可以只用 `qlib.contrib.model` 训练模型，拿到预测值后当因子喂给你的 FactorPipeline——不需要 Qlib 的回测和数据层
- 但这种"只用一部分"的集成现在做太早。先把 FactorPipeline + alphalens 跑通，需要 ML 模型时再接 Qlib 的模型层

**保留接口的意思**：在 `BaseCrossStrategy.on_rebalance()` 的设计中，因子来源是通过 `DataAPI.get_factor()` 抽象的。未来 Qlib 模型产出的预测值可以作为一个因子注册到 `factor_registry`，通过同一个 DataAPI 接口被策略消费。**不需要现在写 QlibAdapter，但不要堵死这条路。**

### 28.5 修正后的"什么场景用什么"

```text
你的量化平台
│
├─ 研究阶段   alphalens-reloaded    因子评价 → IC/IR/分位 → 门控
│                                    │
│                              通过门控 ↓
├─ 回测阶段   ┌─────────────────┬─────────────────────┐
│             │ backtrader      │ cs_portfolio_sim    │
│             │ （CTA/时序）     │ （截面/组合）        │
│             │ 场景：           │ 场景：               │
│             │ - 趋势跟踪      │ - alstm 多因子       │
│             │ - 均值回归      │ - 等权/ICIR 加权     │
│             │ - 配对交易      │ - 风险平价           │
│             │ - 事件驱动      │ - 截面排序           │
│             └────────┬────────┴──────────┬──────────┘
│                      └──────→ 统一 Journal ←──────┘
│                                    │
└─ 实盘阶段   vnpy 4.x（Gateway+Event）│
              所有策略实盘都走这里      │
              CTP（期货）/ IB（美股）/ … ↓
```

#### ARCHITECTURE.md 结论修正

原结论："三套（VectorBT/ZVT/Qlib）降为对照组后废弃"

**修正为："三套直接归档，不做对照组。"**

理由：

- "对照组"意味着你要维护两套数据适配（新引擎 + 旧引擎）和结果比对逻辑。**维护成本 > 对照收益**
- 如果你需要验证 `cs_portfolio_sim` 的正确性，用**手工计算 + 小数据集验证**，而不是用另一个引擎做 A/B。因为如果两个引擎结果不一致，你不知道谁对谁错——还是得回到手工验证
- VectorBT 的 Commons Clause、ZVT 的无上游、Qlib 的全家桶——都不值得为"对照"保留

具体操作：`libs/zvt/` 目录、VectorBT 相关代码、Qlib bridge 代码 → 移到 `archive/` 目录，不删除（保留 git 历史），但不再维护、不进 CI、不加新适配。

> **这修正了 §20.7 与 §26.8 中"VectorBT 降为对照组"的表述——以本节为准。** 对拍/校准走的是 §22 路线 B（新旧跑同一策略看偏差），那是**迁移期的一次性对拍**，不是长期维护旧引擎当对照组。

### 28.6 汇总：三件套的最终选型

| 组件 | 选型 | 版本 | 许可证 | 安装方式 |
|---|---|---|---|---|
| CTA 回测 | backtrader | 1.9.78.123 | GPLv3+（个人不分发，无影响） | `pip install backtrader` |
| 实盘网关 | vnpy | 4.x | MIT | 只装网关子包 `pip install vnpy-ctp` 等；或 vendor `event/`+`trader/`（不含 `ui/`） |
| 因子评价 | alphalens-reloaded | 0.4.6 | Apache 2.0 | `pip install alphalens-reloaded` |
| 截面回测 | 自研 `cs_portfolio_sim` | — | 你的代码 | 薄壳的一部分（~300 行） |

**四个引擎，四个明确的职责，零重叠。VectorBT/ZVT/Qlib 归档。**

**本节仍未解决**：`archive/` 目录的具体清单（哪些文件移过去）；vnpy 网关子包按实际账户选（不再需要——§66.2 不用 backtrader/vnpy，2026-10-10 回改）；backtrader patch 是否回流自维护副本（不再需要——§66.2 不用 backtrader/vnpy，2026-10-10 回改）。

---

## 29. 事件系统设计（精简版：12 事件 + emit/journal）

> 回应 §2.1（事件词汇表：封闭还是开放？）及其四个子问题（表示形态 / 负载结构 / 顺序保证 / 生命周期）。本节细化 §23（Journal）与 §25.5（引擎主循环），并补 §20.3 的结论——不建通用事件系统，但薄壳内需要一层精简事件模型。

### 29.1 要不要事件系统：要，但需要的是极度精简版

关键事实回顾（公司）：

- 公司 Pysim 标准化模块：Timer、Clock、MarketData、BarData、OrderBook、SignalPuber
- 标准化事件：StartTI、EndTI、DI、BarData
- EventBrick：定义事件 + 发布 + 订阅，动态订阅（代码层面控制）
- NewPysim："空洞的"，用户自定义流程，没有固定范式

这就是答案的关键输入——公司 Pysim 是开放模型（B 类），但有一组标准化的事件作为惯例。**本节的设计前提是你的平台是薄壳而非完整事件驱动引擎。**

**结论：需要事件系统，但需要的是极度精简版，不是公司 Pysim 的 EventBrick 体系。**

| 公司 Pysim | 你的薄壳 |
|---|---|
| 通用事件驱动引擎，支持任意业务拓扑 | 专用于"信号→执行→记账"的薄壳 |
| N 个 EventBrick 组成任意 DAG | 固定流程：策略→预检→下单→成交→持仓→Journal |
| 多进程并行（EBRunner） | 单进程（已决定） |
| 需要处理 OT 级（逐笔）实时行情 | 回测是批量推 bar，实盘走 vnpy 的事件引擎 |
| EventBrick + EBGraph + EBRunner 三层 | 你只需要一层：引擎主循环 + 回调 |

公司的 EventBrick 是为分布式、多资产、多图、实时场景设计的。你是单机、Python、日频/分钟频——**事件系统的复杂度要匹配你的规模**。

### 29.2 事件词汇表：封闭还是开放 → 选 C，但极度精简

不是公司 Pysim 意义上的"混合"，而是：

- **内核事件：封闭枚举（Enum），约 12 个**
- **策略自定义事件：不支持。** 策略通过 `on_bar()` / `on_rebalance()` 接口参与，不通过事件订阅

为什么不选 **A（全封闭 + EB 订阅）**：你没有 EventBrick。策略不是 EB，引擎也不是 EB。全封闭 + 订阅机制只在有通用 pub/sub 框架时才有意义。你的框架是固定流水线，不是任意 DAG。

为什么不选 **B（开放字符串）**：开放字符串事件意味着——没有类型检查，拼写错误是运行时 bug；事件 payload 没有约束，序列化/反序列化全靠约定；对单人平台来说，维护成本 > 灵活性收益。

为什么不选 **D（照搬公司枚举）**：公司的标准化事件（StartTI/EndTI/DI/BarData/OrderBook/SignalPuber）是为**分钟级实时推送**设计的——TI（Time Index）是逐分钟推进的概念。你的回测是逐 bar 批量推进，实盘走 vnpy，两者都不需要 TI 的粒度。

### 29.3 事件词汇表：完整的 12 个

按生命周期阶段分三组：

```python
from enum import Enum, auto

class EventType(Enum):
    # === 时间事件（引擎主循环产生）===
    SESSION_START = auto()   # 回测/实盘 session 启动
    SESSION_END   = auto()   # 回测/实盘 session 结束
    DAY_START     = auto()   # 交易日开始（≈公司 DI）
    DAY_END       = auto()   # 交易日结束（日终结算触发点）
    BAR           = auto()   # 新 bar 到达（≈公司 BarData）

    # === 交易事件（执行链产生）===
    ORDER_CREATED   = auto()   # 订单创建（策略发出信号）
    ORDER_REJECTED  = auto()   # 订单被预检拒绝（停牌/涨跌停/资金不足）
    ORDER_FILLED    = auto()   # 订单完全成交
    ORDER_CANCELLED = auto()   # 订单取消
    TRADE           = auto()   # 成交明细（一笔 Trade 对应一次成交）

    # === 状态事件（账本产生）===
    POSITION_UPDATE = auto()   # 持仓变动
    ACCOUNT_UPDATE  = auto()   # 账户资金/市值变动
```

> **§32 / §34 修订**：后补 `SIGNAL` 事件（策略信号 `CrossSignal` / `SignalData`）与 `PORTFOLIO` 事件（目标组合 `TargetPortfolio`），EventType 从 12 个变为 **14** 个——信号含决策原因必须进 Journal 做事后归因，目标组合是仓位管理层的独立产出也要留痕。详见 §32.5 / §34.6。

### 29.4 对比公司 Pysim 的标准事件

| 公司 Pysim | 你的薄壳 | 映射关系 |
|---|---|---|
| StartTI | 不需要 | TI 是分钟级粒度，你的最小粒度是 bar |
| EndTI | 不需要 | 同上 |
| DI | DAY_START | 概念一致 |
| BarData | BAR | 概念一致，但触发方式不同（Pysim 是推送，你是主循环拉取） |
| OrderBook | 不需要 | 你不处理 Level 2 盘口 |
| SignalPuber | ORDER_CREATED | 公司是 signal → signal pub；你是 signal → order created |

你只保留了两个公司事件的对应物（DI → DAY_START，BarData → BAR），其余要么不需要（TI 粒度、OrderBook），要么映射到了交易/账本事件。**这正是薄壳的"薄"所在。**

### 29.5 事件的表示形态：frozen dataclass

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

@dataclass(frozen=True)
class Event:
    type: EventType             # 枚举，封闭
    timestamp: datetime         # 事件产生时间
    source: str                 # 产生者标识（"engine" / "bt_adapter" / "vn_adapter"）
    sequence: int               # 单调递增序号（同 session 内全局唯一）
    payload: Any = None         # 负载，类型由 type 决定（见 §29.6）
```

为什么是 frozen dataclass 而不是 dict / 消息体？

- **dict**：没有类型检查，payload 结构无约束，IDE 不补全
- **消息体（protobuf/msgpack）**：序列化/反序列化开销——你是单进程内存传递，不需要跨进程通信
- **dataclass**：类型明确，IDE 友好，`frozen=True` 保证不可变（事件一旦产生不能篡改）

### 29.6 事件的负载结构

每种 EventType 对应一个明确的 payload 类型（不是继承，是组合）：

```python
# payload 类型对照表
EVENT_PAYLOAD_TYPES = {
    EventType.SESSION_START:    SessionConfig,        # 回测/实盘配置
    EventType.SESSION_END:      SessionSummary,       # 终态汇总
    EventType.DAY_START:        date,                 # 当前交易日
    EventType.DAY_END:          date,                 # 当前交易日
    EventType.BAR:              dict[str, BarData],   # {ticker: BarData}
    EventType.ORDER_CREATED:    Order,                # 订单对象
    EventType.ORDER_REJECTED:   Order,                # 订单对象（含 reject_reason）
    EventType.ORDER_FILLED:     Order,                # 订单对象（终态）
    EventType.ORDER_CANCELLED:  Order,                # 订单对象（终态）
    EventType.TRADE:            Trade,                # 成交明细
    EventType.POSITION_UPDATE:  Position,             # 变动后的持仓
    EventType.ACCOUNT_UPDATE:   Account,              # 变动后的账户
}
```

payload 复用的都是已有的薄壳 dataclass（BarData/Order/Trade/Position/Account），**不引入新数据结构**。

### 29.7 事件的顺序保证

同一交易日内的事件顺序（严格）：

```text
DAY_START
  → BAR（第一根 bar）
    → ORDER_CREATED（策略信号）
      → ORDER_REJECTED / ORDER_FILLED
        → TRADE（如果成交）
          → POSITION_UPDATE
            → ACCOUNT_UPDATE
  → BAR（第二根 bar，如果分钟频）
    → …
DAY_END
```

**保证**：

- **同一交易日内**：严格有序。sequence 单调递增。**一个 BAR 事件触发的全部下游（ORDER → TRADE → POSITION → ACCOUNT）在下一个 BAR 之前全部完成**
- **跨交易日**：`DAY_END[d]` 的 sequence < `DAY_START[d+1]` 的 sequence。严格有序
- **单线程保证**：你的薄壳是单进程/单线程（已决定），所以顺序保证是天然的——不需要锁、不需要队列、不需要 sequence 比较逻辑。sequence 字段主要是给 Journal 回放和调试用的

对比公司 Pysim 的顺序问题：公司 Replay 文档（209868537）详细讨论了事件乱序的两个场景——(1) 时间步过大 → 轮询稀疏 → `work()` 无法覆盖所有截面；(2) 多所行情多通道 → 消费顺序不一致。**这两个问题你都不会遇到**：回测是批量推 bar（不存在轮询），实盘事件走 vnpy 的 EventEngine（单线程消费）。公司的 MarketDataSnapshot + sequence number 对比机制是为了解决多通道实时行情的乱序——你不需要。

### 29.8 事件的生命周期：内存 + Journal 双轨

| 事件类型 | 内存 | 持久化到 Journal | 说明 |
|---|---|---|---|
| SESSION_START/END | 有 | 有 | session 边界，必须记录 |
| DAY_START/END | 有 | 有 | 交易日边界，回放时需要 |
| BAR | 有 | **不持久化** | bar 数据已在 Parquet 行情表里，不重复存 |
| ORDER_* | 有 | 有 | 订单全生命周期，审计必须 |
| TRADE | 有 | 有 | 成交明细，回测/实盘核心输出 |
| POSITION_UPDATE | 有 | 有 | 持仓变动链 |
| ACCOUNT_UPDATE | 有 | **日终快照** | 逐笔变动太多，只记日终 |

Journal 就是 §23 设计的 append-only Parquet 流。**Event 对象是内存中的运行时载体，Journal 条目是它的持久化投影**——不是所有 Event 都进 Journal（BAR 不进，ACCOUNT 只记日终）。

```python
class Journal:
    """Append-only event journal, backed by daily Parquet files."""

    def log(self, event: Event):
        """Write event to journal (filters by PERSIST_TYPES)."""
        if event.type not in self.PERSIST_TYPES:
            return
        self._buffer.append(self._event_to_row(event))
        if len(self._buffer) >= self.FLUSH_SIZE:
            self._flush()

    PERSIST_TYPES = {
        EventType.SESSION_START, EventType.SESSION_END,
        EventType.DAY_START, EventType.DAY_END,
        EventType.ORDER_CREATED, EventType.ORDER_REJECTED,
        EventType.ORDER_FILLED, EventType.ORDER_CANCELLED,
        EventType.TRADE,
        EventType.POSITION_UPDATE,
        # ACCOUNT_UPDATE → 通过 day_end_snapshot() 持久化
    }
```

### 29.9 事件系统在薄壳里的位置

事件**不是一个独立的 pub/sub 框架**，而是引擎主循环的副产物 + Journal 的输入接口：

```python
for trade_date in calendar:
    emit(DAY_START, trade_date)
    bars = load_bars(trade_date)
    emit(BAR, bars)
    signals = strategy.on_bar(bars)
    for signal in signals:
        order = create_order(signal)
        emit(ORDER_CREATED, order)
        result = execute(order, bars)
        if result.rejected:
            emit(ORDER_REJECTED, order)
        else:
            emit(ORDER_FILLED, order)
            emit(TRADE, result.trade)
            emit(POSITION_UPDATE, result.position)
            emit(ACCOUNT_UPDATE, result.account)
    day_end_settle()
    emit(DAY_END, trade_date)

def emit(self, event_type: EventType, payload):
    """创建 Event 并分发到 listeners。"""
    event = Event(
        type=event_type,
        timestamp=self._current_time(),
        source=self._source_name,
        sequence=self._next_seq(),
        payload=payload,
    )
    # 写 Journal
    self.journal.log(event)
    # 通知监听者（如果有）
    for listener in self._listeners.get(event_type, []):
        listener(event)
```

监听者（listener）是可选的，主要用于：

- 实盘模式下 ops 监控模块监听 `ORDER_REJECTED` 做告警
- 调试时注册临时 listener 打印事件流
- 未来扩展：risk monitor 监听 `POSITION_UPDATE` 做实时风控

回测模式下通常没有额外 listener——事件产生后直接写 Journal，结束。**这就是为什么它不是 EventBrick 体系，而是一个轻量的"emit + journal + optional listeners"模式。**

### 29.10 分钟级数据对事件模型的影响

> 附带讨论："后续应该也要存储分钟级别数据"。

**不影响事件模型，12 个事件不用改。** BAR 事件本身是频率无关的——日频触发 1 次/天，分钟频触发 ~240 次/天（A 股 4 小时），payload 都是 `dict[str, BarData]`。

唯一要注意的点：**分钟频下不需要加 StartTI / EndTI。** 公司 Pysim 的 TI 概念是因为它的实时引擎需要标记"这一分钟开始处理"和"这一分钟处理完毕"来协调多个 EventBrick 的执行窗口。你是单线程主循环，**BAR 事件本身就隐含了 TI 边界**——处理完一个 BAR 的全部下游（ORDER → TRADE → POSITION）再进入下一个 BAR，天然有序。

分钟级对 Journal 持久化规则的影响：

| 事件 | 日频 | 分钟频 | 变化 |
|---|---|---|---|
| BAR | 不持久化 | 仍然不持久化 | bar 数据在行情 Parquet 里，不重复存 |
| ORDER/TRADE | 全量持久化 | 全量持久化 | 量会大很多（分钟调仓 → 更多订单），但仍需全量 |
| ACCOUNT_UPDATE | 日终快照 | 仍然日终快照 | 逐分钟快照没意义，日终足够 |

如果将来真的做到 tick 级/秒级，那时候再考虑是否需要 TI 分批或者事件批处理。**分钟级完全在当前设计的承载范围内。**

### 29.11 汇总

| 决策点 | 结论 |
|---|---|
| 封闭/开放 | 封闭枚举（EventType Enum），~~12~~ **14** 个事件（§32 补 SIGNAL、§34 补 PORTFOLIO），不支持自定义 |
| 表示形态 | Event frozen dataclass（type + timestamp + source + sequence + payload） |
| 负载结构 | payload 复用已有 dataclass（BarData/Order/Trade/Position/Account） |
| 顺序保证 | 单线程天然有序；sequence 字段用于 Journal 回放 |
| 生命周期 | 内存（运行时）+ Journal（选择性持久化，BAR 不存，ACCOUNT 只存日终） |
| 与公司 Pysim 的关系 | 只映射了 DI→DAY_START 和 BarData→BAR；不引入 TI/EB/EBGraph |
| 新增代码量 | `kernel/event.py` ~60 行（EventType + Event dataclass + emit 逻辑） |

**代码量更新**：§26 的 ≈2230 行 + 本节 60 行 ≈ **2290 行**（§30 +30 行、§31 +0 行、§32 +120 行、§34 +190 行后合计 ≈**2630 行**，见 §34.9）。

**本节仍未解决**：`SessionConfig` / `SessionSummary` 两个 payload 的字段 schema；`_listeners` 的注册 API 形态（构造期注入 vs 运行期 `register()`）；tick/秒级时的 TI 分批方案（远期）。

---

## 30. EventBrick 六个子问题的薄壳等价物

> 回应 §2.2（EventBrick 契约签名：name / publisher / Handle / invoker）。**本节回答方式不是"EB 签名怎么定"，而是"薄壳用什么替代了 EB 的每个能力"**——§29.1 已确定薄壳不采用 EventBrick 体系，EB 四元素签名在薄壳方案下不适用。

### 30.1 Context：问题本身要换一种问法

§2.2 问的是公司 Pysim 的 EventBrick 四元素 `name / publisher / Handle / invoker` 如何在个人平台落地。但 §29.1 已经确定：**薄壳不采用 EventBrick 体系**。薄壳是固定流水线（策略→预检→下单→成交→持仓→Journal），不是任意事件 DAG。

因此本题的正确回答不是"EB 签名怎么定"，而是"薄壳用什么替代了 EB 的每个能力"。

### 30.2 六个子问题的等价物一览

| EB 概念 | 公司 Pysim | 你的薄壳 |
|---|---|---|
| name | EB 实例标识 | 不需要——引擎和策略各一个，不需要标识区分 |
| publisher | EB 方法，向 Graph 发布 | `engine.emit(type, payload)` |
| Handle | 动态订阅，装饰器/代码绑定 | 硬编码主循环 + 可选 `add_listener()` |
| invoker | EBRunner 驱动 | `engine.run()` 主循环 |
| 生命周期 | EB 独立注册/启动/停止/销毁 | 引擎 session：init → run() → GC |
| 状态管理 | EB 自行序列化 | 策略 checkpoint（save/load） |
| 订阅时机 | 构造期/运行期动态 | 构造期注册 listener，运行期不变 |

### 30.3 publisher —— 薄壳没有 publisher，有 emit()

公司 Pysim 中 publisher 是 EB 的方法，向 EBGraph 发布事件。薄壳中：

- 事件只由**引擎主循环和执行适配器**产生，**不由策略产生**
- `emit()` 是引擎的实例方法，不是独立组件

不需要"声明发布的事件类型"——EventType 是封闭枚举（12 个，见 §29.3），引擎代码里写死了在哪个位置 emit 哪个类型，编译期（mypy）可检查。

### 30.4 Handle —— 没有装饰器/配置声明，有固定调用链

公司 Pysim：EB 通过 Subscribe 将 Handle 绑定到事件，动态订阅。

薄壳：**没有订阅机制**。事件处理是引擎主循环的硬编码流程——就是 §29.9 那个 for 循环：`strategy.on_bar(bars)` 是固定调用，不是订阅。不需要注册、不需要订阅、不需要优先级，**流程在代码里是确定的**。

可选的 listener 机制只用于旁路（监控/告警/调试），不参与主流（见 §29.9）。

### 30.5 invoker —— 没有调度器，引擎主循环就是驱动者

公司 Pysim：EBRunner 驱动 EBGraph，事件级联触发。

薄壳：引擎的 `run()` 方法就是 invoker。**不存在独立的调度器组件**。回测引擎是同步 for 循环，实盘引擎是 vnpy EventEngine 的回调。

### 30.6 生命周期 —— 映射到引擎 session

公司 EB 有独立生命周期（注册/启动/停止/销毁）。薄壳中等价的是引擎和策略的 session：

**引擎 session 四阶段**：

1. **构造期**：加载配置、实例化策略、连接 DataAPI
2. **启动期**：`emit(SESSION_START, config)` + `strategy.on_init(data_api)`（加载模型等）
3. **运行期**：主循环（DAY_START → BAR → … → DAY_END）
4. **停止期**：`strategy.on_stop()` + `journal.flush()` + `emit(SESSION_END, summary)`

**没有 destroy——Python GC 处理**。Journal 文件在 `run()` 结束时已 flush。

**策略的生命周期**三个钩子：

```python
class BaseStrategy:
    def on_init(self, data_api: DataAPI):
        """session 开始前调用。加载模型、预热数据。"""
        pass

    def on_bar(self, bars: dict[str, BarData]) -> list[SignalData]:
        """每根 bar 调用。核心决策逻辑。"""
        raise NotImplementedError

    def on_stop(self):
        """session 结束时调用。清理资源。"""
        pass
```

（截面策略对应 `BaseCrossStrategy.on_rebalance()`，见 §26.9 / §32.2。）

### 30.7 状态管理 —— 策略状态 = checkpoint

公司：有状态 EB 需要自己处理状态序列化。

薄壳：策略如果有状态（如 LSTM 模型的隐状态、滚动窗口），持久化通过 **checkpoint 机制，不通过事件系统**：

```python
class BaseStrategy:
    def save_checkpoint(self, path: str):
        """引擎在 session 结束或每 N 天调用。策略自行序列化。"""
        pass

    def load_checkpoint(self, path: str):
        """引擎在 on_init() 之后、主循环之前调用（如果有 checkpoint）。"""
        pass
```

checkpoint 存 `research/checkpoints/{strategy_name}/{date}.pkl`。**这不是事件，是策略自己的事。**

### 30.8 订阅关系建立时机 —— 不适用

公司 Pysim：动态订阅，代码层面控制，非常灵活。

薄壳：**没有订阅关系**。策略通过接口被引擎调用（on_init / on_bar / on_stop），不通过事件订阅。可选的 listener 在**引擎构造期**注册：

```python
engine = BacktestEngine(config, strategy, data_api)
engine.add_listener(EventType.ORDER_REJECTED, my_alert_callback)   # 构造期注册
engine.run(start, end)
```

运行期不支持动态添加/删除 listener（单线程，没有并发问题，但也没有必要）。

### 30.9 什么情况下需要重新引入 EB

如果将来出现以下任一场景，才需要从"固定流水线"升级为"事件 DAG"：

- **多策略并行运行**，策略之间需要通信（A 策略的信号是 B 策略的输入）
- **tick 级实时引擎**，多数据源事件需要灵活路由
- **插件化架构**，第三方模块需要动态注册事件处理器

当前阶段（单策略、日频/分钟频、单进程）不会遇到这些场景。见 §33.2（tick 级是另一个系统）。

**本节仍未解决**：checkpoint 的保存频率（session 结束 vs 每 N 天，N 取多少）；checkpoint 文件是否进 git / 是否随策略版本管理。

---

## 31. DataBus 不需要（直连替代）

> 回应 §2.3（DataBus 接口：发布/订阅 API、单机实现、数据源 Service、背压、Replay）。结论：**你不需要 DataBus，已经有了更好的替代**——回测是同步直连，实盘用 vnpy EventEngine。**新增代码 0 行。**

### 31.1 公司为什么需要 DataBus

公司 Pysim 的 DataBus 解决的问题：

```text
多个数据源（CTP/交易所/Level2）→ 多通道推送 → DataBus（汇聚+路由）→ 多个 EB 消费
```

核心诉求：

- **多生产者 → 多消费者**：N 个行情通道，M 个 EventBrick，需要解耦
- **跨进程**：EBRunner 支持多进程，数据需要跨进程传输（SHM）
- **实时背压**：tick 级行情，消费慢了必须处理（丢弃/降采样）
- **多图（Graph）**：同一个数据可能被多个 EBGraph 消费

### 31.2 你的场景一个都不命中

| DataBus 解决的问题 | 你的薄壳 |
|---|---|
| 多生产者 | 回测：DataAPI 一个数据源。实盘：vnpy Gateway 一个数据源 |
| 多消费者 | 一个策略实例消费所有数据（单策略单进程） |
| 跨进程 | 单进程（已决定） |
| 实时背压 | 回测无背压（同步循环）。实盘走 vnpy EventEngine（它内部处理） |
| 多图 | 没有 EBGraph |

**你的数据流是一条直线，不是总线**：

```text
回测：DataAPI.get_bars() → engine.emit(BAR) → strategy.on_bar() → journal.log()
实盘：vnpy Gateway.on_bar() → VNExecAdapter → strategy.on_bar() → journal.log()
```

中间不需要 bus/queue/pub-sub。数据从一个源头到一个消费者，同步调用。**直连比解耦简单 10 倍。你只有一个数据源和一个消费者，解耦是为了解决不存在的问题。**

### 31.3 逐个回答子问题

#### 发布/订阅 API —— 同步方法调用，无需队列

```python
# 回测: engine/backtest.py —— 数据"发布"就是函数调用
bars = self.data_api.get_bars(tickers, trade_date)   # 同步读 Parquet
signals = self.strategy.on_bar(bars)                 # 同步调策略
```

实盘模式：**vnpy EventEngine 已经是一个 pub-sub 实现**——`VNExecAdapter` 注册回调，vnpy 在收到行情时调用：

```python
class VNExecAdapter:
    def __init__(self, event_engine, strategy):
        event_engine.register(EVENT_BAR, self._on_vnpy_bar)

    def _on_vnpy_bar(self, event):
        bar = self._convert_to_bar_data(event.data)   # vnpy BarData → 薄壳 BarData
        signals = self.strategy.on_bar({bar.ticker: bar})
        # 执行信号...
```

实盘的"DataBus"就是 vnpy 的 EventEngine——它内部已经实现了事件队列 + 注册 + 分发。**你不需要再建一层。**

#### 单机实现 —— 不需要实现，因为没有 bus

- 回测：同步函数调用链，连队列都不需要
- 实盘：vnpy EventEngine（内存队列 + 单线程消费）已经是你的"单机 pub-sub"

如果一定要画出等价物：

```text
公司架构：DataSource → DataBus → EB        你的架构：DataAPI → engine.run() → strategy
          （解耦）     （路由）   （消费）             （直连）   （同步调用）    （直连）
```

#### 数据源 Service 怎么灌数据 —— DataAPI 就是数据源接口

公司的"数据源 Service"（MarketDataService）负责从通道读行情、转事件、推到 DataBus。你的等价物：

```python
# 回测: DataAPI 直接读 Parquet
class DataAPI:
    def get_bars(self, tickers, start, end, adjust='qfq') -> pd.DataFrame:
        """一次性读取,或按日迭代"""

# 实盘: vnpy Gateway 就是数据源 Service
# Gateway.on_bar() 被交易所推送触发 → 转化为 vnpy 内部 BarData → 推入 EventEngine
```

**不需要中间层。**

#### 背压处理 —— 不存在背压场景

- **回测**：生产者（DataAPI）是同步调用，策略处理完一个 bar 才会读下一个 bar。**不可能消费慢于生产——因为生产是消费者主动拉取的**
- **实盘（日频/分钟频）**：分钟 bar 每分钟一根，策略处理时间 << 1 分钟，不可能背压。即使策略偶尔慢了，vnpy EventEngine 的内部队列会缓冲，下一根 bar 来时一起处理。分钟频场景下这个延迟完全可接受

什么时候才会背压？**tick 级（每秒数千条）+ 重计算策略**。那是 §33.2 讨论的"阶段 2 tick 级引擎"的问题，当前不处理。

#### 数据回放（Replay）—— 回测本身就是回放

公司的 Replay 模式是为了**在回测中精确还原实盘的事件消费顺序**（参见公司 Replay 文档 209868537）。它需要 DataBus 支持"按时间戳回放事件流"。

**你的回测天然就是回放**：

```python
# 回测引擎的主循环——这就是 replay
for trade_date in calendar:
    bars = data_api.get_bars(tickers, trade_date)   # 历史数据,按日回放
    strategy.on_bar(bars)
```

不需要额外的 Replay 模式，因为：

- 回测和实盘走**同一个策略接口**（`on_bar(bars)`），区别只是 bars 的来源（Parquet vs vnpy Gateway）
- 不需要精确还原"事件消费顺序"，因为你是**单数据源、单消费者、同步调用**——顺序是确定的
- 公司需要 MarketDataSnapshot 比对是因为多通道可能乱序，你没有这个问题

### 31.4 汇总：你的"DataBus"就是这三行

```python
# 回测
bars = data_api.get_bars(tickers, trade_date)
signals = strategy.on_bar(bars)

# 实盘
event_engine.register(EVENT_BAR, adapter.on_bar)   # vnpy 的 EventEngine
```

| 公司概念 | 你的等价物 | 代码量 |
|---|---|---|
| DataBus | 不需要（直连） | 0 行 |
| DataSource Service | DataAPI（回测）/ vnpy Gateway（实盘） | 已有 |
| 订阅 API | `strategy.on_bar()` 直接调用（回测）/ `event_engine.register()`（实盘） | 已有 |
| 背压 | 不存在（同步拉取 / 分钟频队列缓冲） | 0 行 |
| Replay | 回测主循环本身 | 已有 |

**不需要新增任何代码。** DataBus 是为分布式多通道实时系统设计的组件，你的单机同步薄壳用不到它。

---

## 32. Signal 三层 Schema 落地（CrossSignal / SignalData）

> 回应 §2.4（Signal 三层 schema：Blueshift / USignal / Tick 信号数组的字段级 schema）全部六个子问题，并部分回应 §2.5（Portfolio schema）。**本节修订 §26.9（on_rebalance 返回类型）与 §29.3（EventType 从 12 个变 13 个）。**

### 32.1 公司三层的本质：对应三种交易频率

Confluence 搜索未命中 Blueshift / USignal 的字段级 schema 文档（可能在受限空间或代码仓库中）。但从 NewPysim 文档和已有的三层表格，可以还原三层的设计意图：

```text
截面层（Blueshift） — 日频/低频 — "今天我要持有什么，各多少"
向量层（USignal）   — 分钟频     — "这一分钟这些因子值更新了"
流式层（Tick 信号） — tick 级    — "这个 tick 我要发一笔指令"

        频率越高 → 结构越简 → 更新越增量
```

| 层 | 语义 | 类比 |
|---|---|---|
| Blueshift | 目标组合快照 ——"我要持有 300 只票，这是权重" | 投资经理的持仓指令单 |
| USignal | 因子向量的增量流 ——"momentum 因子值变了，这是新值" | 因子实时更新流 |
| Tick 信号 | 逐笔交易指令 ——"现在买 100 手 IF2406" | 交易员的敲单 |

**你的平台需要几层？一层半。** 因为频率范围是日频 + 分钟频，不做 tick 级：

| 公司三层 | 你的平台 | 落地方式 |
|---|---|---|
| Blueshift（截面） | 需要 | `BaseCrossStrategy.on_rebalance()` → `CrossSignal`（目标权重） |
| USignal（向量） | 部分需要 | `BaseStrategy.on_bar()` → `list[SignalData]`（逐票信号） |
| Tick 信号 | 不需要 | 当前不做 tick 级策略 |

但"一层半"不是精确说法——更准确地说，**你的平台已经有了两种策略接口，它们天然对应公司的前两层**：

```text
公司 Blueshift  ↔  你的 BaseCrossStrategy.on_rebalance() → CrossSignal
公司 USignal    ↔  你的 BaseStrategy.on_bar()            → list[SignalData]
```

**关键认知：你的策略接口的返回值就是"信号"。不需要独立于策略接口的信号层。**

### 32.2 Blueshift → CrossSignal（截面信号 schema）

公司 Blueshift 的 `struct(alpha)` 在 Confluence 上查不到具体字段，但从截面策略的需求可以推导：

```python
@dataclass(frozen=True)
class CrossSignal:
    """截面策略的一次调仓信号（= Blueshift 的一次截面刷新）"""

    # === meta（公司的 string(合约/meta)）===
    strategy_id: str                  # 策略标识
    trade_date: date                  # 信号对应的交易日
    signal_time: datetime             # 信号产生时间
    universe_size: int                # 本次截面涉及的标的数

    # === alpha（公司的 struct(alpha)）===
    target_weights: dict[str, float]  # {ticker: weight}  核心:目标权重
    scores: dict[str, float] | None   # {ticker: raw_score}  可选:原始打分(调试用)

    # === 约束/上下文 ===
    max_weight: float = 0.1                    # 单票权重上限
    turnover_limit: float | None = None        # 换手率上限(如有)
    reason: str = ""                           # 调仓原因(模型版本/触发条件)
```

对比公司 Blueshift：

- 公司的"整租 Meta 或整租 signal 截面刷新" = 你的 CrossSignal 是**每次调仓一个完整快照，不做增量**
- 公司的 `N 组 <string(合约/meta), struct(alpha)>` = 你的 `target_weights: dict[str, float]`
- 你的 `scores` 字段保留了原始打分，方便事后分析"为什么给了这个权重"

这就是 `BaseCrossStrategy.on_rebalance()` 的返回值类型：

```python
class BaseCrossStrategy:
    def on_rebalance(self, date, bars, positions, account) -> CrossSignal:
        """之前返回 dict[str, float]，现在返回 CrossSignal（更完整）"""
```

> **对 §26.9 的修正**：§26.9 写的是 `on_rebalance(...) -> dict[str, float]`，本节升级为 `-> CrossSignal`（`target_weights` 就是那个 dict，另带 meta / scores）。~~§26.5 的 `rebalance(target_weights, ...)` 改为 `rebalance(cross_signal, ...)`~~ → **§34 再修正**：`rebalance(target_portfolio, ...)`，中间补 `PortfolioOptimizer.solve()` 一层。
>
> **§34 对本小节的修正**：`CrossSignal` 的 `max_weight` / `turnover_limit` 两个字段**废弃**——约束的权威是 `PortfolioConstraints`（`solve()` 的入参），不随信号携带。`CrossSignal` 最终字段 = meta + `target_weights` + `scores` + `reason`。

### 32.3 USignal → SignalData（时序信号 schema）

```python
@dataclass(frozen=True)
class SignalData:
    """时序策略的单票信号（= USignal 的一次增量更新）"""

    ticker: str                                   # 标的
    datetime: datetime                            # 信号时间
    direction: Direction                          # BUY / SELL / HOLD
    strength: float = 1.0                         # 信号强度(0-1),可选
    quantity: int | None = None                   # 明确手数(如有,覆盖仓位管理逻辑)
    price_type: PriceType = PriceType.MARKET      # MARKET / LIMIT
    limit_price: float | None = None              # 限价(price_type=LIMIT 时)
    reason: str = ""                              # 信号原因

    # === 因子快照（对应 USignal 的 vector<factor>）===
    factor_snapshot: dict[str, float] | None = None
    # 产生这个信号时的因子值快照,如 {"momentum": 0.8, "reversal": -0.3}
    # 事后归因用:为什么在这个点发出了买入信号?
```

对比公司 USignal：

- 公司的 `vector<factor>`，index = 合约下标 = 你的 `factor_snapshot`
- 公司的"增量更新" = 你的 `list[SignalData]` 里**每个元素就是一次增量**（只有变化的票才产出 signal）
- 公司用合约下标做 index 是为了高性能（C++ 数组直接索引）；你用 `dict[str, float]` 足够（Python 场景，不追求纳秒级）

`BaseStrategy.on_bar()` 已经返回 `list[SignalData]`，只是之前的 SignalData 定义比较简单，**现在把 `factor_snapshot` 补上**。

### 32.4 Tick 信号 —— 不实现

公司的 Tick 信号层"不实际维护数组，只传最新更新"——本质是一个热路径的消息（像一条 protobuf 消息直接丢给交易网关）。你的日频/分钟频平台不做这个。

如果将来做，**它的等价物是 vnpy 的 `OrderRequest` 直接发给 Gateway**——不需要你自己定义信号格式，vnpy 有现成的。

### 32.5 三层的关系：按策略类型用不同层，不是同一策略产三层

```text
截面策略（alstm）
  └→ on_rebalance() → CrossSignal → cs_portfolio_sim
        （一次产出一个完整截面快照）

时序策略（CTA）
  └→ on_bar() → list[SignalData] → backtrader / 直接下单
        （每根 bar 产出 0~N 个单票信号）

未来 tick 策略（不实现）
  └→ on_tick() → OrderRequest → vnpy Gateway
        （每个 tick 产出 0~1 个指令）
```

**不会出现"同一策略同时产出三层"的情况。策略类型决定了它走哪层**：

- 截面策略 → CrossSignal
- 时序策略 → SignalData
- 混合策略（如：截面选股 + 时序择时）？→ **拆成两个策略**，截面部分产 CrossSignal，时序部分产 SignalData，通过 Journal 关联

#### 信号要不要进 Journal？—— 要，新增 SIGNAL 事件

**因为信号包含决策原因（`scores`、`factor_snapshot`、`reason`），而 Order 只记录了执行指令。事后归因需要知道"为什么下了这个单"，不只是"下了什么单"。**

增加一个 EventType：

```python
class EventType(Enum):
    # ... 原来的 12 个 ...
    SIGNAL = auto()   # 新增:策略信号(CrossSignal 或 SignalData)
```

Journal 中 SIGNAL 事件的 payload 是 `CrossSignal` 或 `SignalData`。

> **对 §29.3 / §29.11 的修订**：EventType 从 **12 个变为 13 个**（补 `SIGNAL`）。`SIGNAL` 进 `PERSIST_TYPES`（全量持久化——决策原因是归因的核心数据）。§29.7 的顺序图中，`ORDER_CREATED` 之前多一个 `SIGNAL`。**§34 再补 `PORTFOLIO`，EventType 最终为 14 个。**

### 32.6 序列化与落盘

**运行时：内存 dataclass。** 信号在策略和引擎之间传递全部是内存中的 Python dataclass，不序列化。单进程、同步调用，不需要跨进程传输。

**持久化：两个位置，两种格式。**

1. **Journal（实时，append-only）**：信号通过 `SIGNAL` 事件进 Journal（见 §32.5）
2. **信号归档（每日落盘）**：

```text
data_platform/
  signals/
    cross/                       # 截面信号归档
      alstm_v1/
        2024-01-02.parquet       # 当日的 CrossSignal 序列化
        2024-01-03.parquet
    timeseries/                  # 时序信号归档
      cta_momentum/
        2024-01-02.parquet       # 当日的 list[SignalData] 序列化
```

**每日落盘时机**：在 `DAY_END` 事件处理时，将当日的信号 buffer 写入 Parquet。

### 32.7 Parquet schema

**截面信号**：

| 列 | 类型 | 说明 |
|---|---|---|
| signal_time | timestamp | 信号产生时间 |
| ticker | string | 标的 |
| target_weight | float64 | 目标权重 |
| score | float64 | 原始打分（nullable） |
| strategy_id | string | 策略标识 |
| trade_date | date | 交易日 |

**时序信号**：

| 列 | 类型 | 说明 |
|---|---|---|
| datetime | timestamp | 信号时间 |
| ticker | string | 标的 |
| direction | string | BUY/SELL/HOLD |
| strength | float64 | 信号强度 |
| quantity | int64 | 手数（nullable） |
| factor_snapshot | string(JSON) | 因子快照（nullable） |
| reason | string | 信号原因（nullable） |

### 32.8 汇总

| 决策 | 结论 |
|---|---|
| 公司三层在你的平台 | 两层：CrossSignal（截面）+ SignalData（时序），不做 Tick 层 |
| 按策略类型用不同层 | 截面策略 → CrossSignal；时序策略 → SignalData；不混用 |
| 运行时形态 | frozen dataclass（内存，同步传递） |
| 持久化到 Journal | 新增 SIGNAL 事件类型（EventType 从 12 个变成 13 个；§34 再补 PORTFOLIO → 14 个） |
| 每日落盘 | `data_platform/signals/{cross,timeseries}/{strategy_id}/{date}.parquet` |
| 事后归因 | `CrossSignal.scores` + `SignalData.factor_snapshot` 记录决策原因 |
| 增量 vs 快照 | CrossSignal 是**全量快照**（每次调仓全截面）；SignalData 是**增量**（只有变化的票） |

**代码量**：`CrossSignal` + `SignalData` 扩展 + `SIGNAL` 事件 + 信号归档写盘 ≈ **120 行**（`kernel/models/signal.py` ~70 行 + 归档 ~50 行）。

**本节仍未解决**：`CrossSignal.max_weight` / `turnover_limit` 与 §17 组合约束的关系（已定 → §34：两字段废弃，约束权威=PortfolioConstraints，2026-10-10 回改）；`factor_snapshot` 是否存全量因子还是只存用到的因子；混合策略"通过 Journal 关联"的具体关联键。

---

## 33. L2 数据的两条路径（研究离线 vs tick 实时）

> 回应 §4.2（Tick / L2 表）与 §5.2（udata 扩展字段）中 L2 数据的使用方式。结论：**L2 当前只走"离线因子原料"路径，不进事件流；tick 级驱动是另一个系统。**

### 33.1 路径 A：L2 作为研究数据（离线，不进事件流）

最常见的场景：用 L2 数据算因子（买卖价差因子、盘口不平衡因子、成交量分布因子），然后因子以日频/分钟频输出，喂给已有的 FactorPipeline → alphalens → 策略。

```text
L2 Parquet → 离线因子计算脚本 → 因子值（日频/分钟频） → FactorPipeline
                                        ↓
                                  不经过事件系统
```

**这条路不需要改事件模型。** L2 是因子的原材料，不是事件流的一部分。就像你不会把"每日成交量"作为事件发出去一样——它是 BarData 的一个字段，被策略在 `on_bar()` 里读取。L2 因子也一样，**算完后变成 BarData 的附加列或独立因子表**。

**80% 的 L2 使用场景都是这条路。** 它完全落在 §27（FactorPipeline → alphalens 门控）的既有流水线内，零新增架构。

### 33.2 路径 B：L2 驱动的实时策略（在线，需要新事件）

如果未来做逐 tick 驱动的策略（做市、统计套利的微观结构策略），那需要：

```python
class EventType(Enum):
    # ... 原来的事件不变 ...

    # === L2 事件（仅实盘/tick 回测使用）===
    TICK         = auto()   # 逐笔/快照到达
    DEPTH_UPDATE = auto()   # 盘口(bid/ask 5档)变动
```

**但这时候面临的不是"加两个事件"的问题，而是整个引擎架构的范式转换**：

| 问题 | 日频/分钟频 | Tick 级 |
|---|---|---|
| 驱动频率 | 1–240 次/天 | 数万次/秒 |
| 单线程 Python 够吗 | 完全够 | 不够，需要 C++/Rust |
| 策略接口 | `on_bar()` 够用 | 需要 `on_tick()` + 状态机 |
| 回测方式 | 批量加载 + 逐 bar 推进 | 需要事件回放引擎 |
| 延迟要求 | 不关心 | 微秒级 |

这实际上是公司 Pysim 要解决的问题域（CX330/UMA pybind、多通道行情、TI 粒度、事件序列一致性）。**如果走到这一步，需要的不是在薄壳上加两个事件类型，而是一个独立的 tick 级引擎——那是另一个系统。**

### 33.3 结论与当前动作

- **现在只做路径 A**：L2 数据入库（`data_platform/` 的 L2 表），离线算因子，因子走 §27 流水线
- **不做路径 B**：不在当前事件模型里加 `TICK` / `DEPTH_UPDATE`（呼应 §29.2"不选 D：照搬公司枚举"、§30.9"何时重新引入 EB"）
- **Tick / L2 表的字段 schema 仍开放**（§4.2），但它的**使用路径已定**：离线因子原料

**本节仍未解决**：Tick / L2 表的字段 schema（§4.2 待定项）；L2 因子的计算脚本放哪（`research/` 还是 `scripts/data_processing/`）；`DataAPI` 是否预留 `get_l2_snapshot()` 签名（供离线因子计算调用，不进事件流）。

---

## 34. Portfolio Schema 与组合约束求解（TargetPortfolio + PortfolioOptimizer）

> 回应 §2.5（Portfolio schema 全部五个子问题）、§9.6（约束求解器选型）、§9.7（手数取整落地），并**修订 §26.5 / §32.2 / §29**（补上 Signal → Portfolio → Order 中间缺失的约束求解层；EventType 13→14）。

### 34.1 信号链拆清：Signal → Portfolio → Order 是三个不同的东西

之前的讨论中这三层是混在一起的。现在必须拆清楚：

| 策略产出 | 仓位管理层产出 | 执行层产出 |
|---|---|---|
| **Signal** | **Portfolio** | **Order** |
| "我觉得这些票好"（打分/权重） | "我要持有这些仓位"（目标持仓，含约束） | "我要买卖这些差额"（具体指令） |
| `CrossSignal` | `TargetPortfolio` | `list[Order]` |
| `target_weights` / `scores` | 经过约束求解 → 整数手数 | 经过差额计算 → 逐笔下单 |

**之前 `BaseCrossStrategy.on_rebalance()` 直接返回 `CrossSignal.target_weights`，然后 `cs_portfolio_sim.rebalance()` 直接从权重算手数。中间缺了约束求解这一步。** 现在补上：

```text
CrossSignal.target_weights   （连续权重，如 {SH600000: 0.05}）
        ↓
PortfolioOptimizer.solve()   （约束求解：权重上限 / 换手 / 行业）
        ↓
TargetPortfolio.positions    （整数手数，如 {SH600000: 2300股}）
        ↓
cs_portfolio_sim.rebalance() （差额 → Order → Trade）
```

**关键认知：个人平台的约束是简单约束（box + 线性），不需要通用凸优化器。** 步骤 1 软约束裁剪（纯 numpy，不需要求解器），步骤 2 手数取整（贪心算法）。

### 34.2 PortfolioConstraints：四类约束 + 手数硬约束

```python
@dataclass
class PortfolioConstraints:
    """组合优化的约束集合"""

    # 1. 单标的约束
    max_weight: float = 0.10            # 单票权重上限(默认 10%)
    min_weight: float = 0.0             # 单票权重下限(做多策略 = 0)

    # 2. 换手约束
    max_turnover: float | None = None   # 单次调仓换手率上限(如 0.3 = 30%)
    # turnover = sum(abs(new_weight - old_weight)) / 2

    # 3. 行业约束
    max_industry_weight: float | None = None  # 单行业权重上限(如 0.20)
    # 需要 industry_map: dict[str, str] 作为输入

    # 4. 资金约束
    max_total_weight: float = 0.95      # 总权重上限(留 5% 现金缓冲)
    min_cash: float = 0.0               # 最低现金保留(名义金额)

    # 5. 整数手数(硬约束，在优化后执行)
    lot_size: int = 100                 # A 股 100 股/手
    min_trade_value: float = 5000.0     # 最小交易金额(低于此不调仓，避免佣金吃利润)
```

注意：`PortfolioConstraints` 是普通可变 dataclass（配置对象），`TargetPortfolio` / `TargetPosition` 才是 frozen（值对象）。

### 34.3 PortfolioOptimizer.solve()：分两步，纯 numpy 裁剪

```python
class PortfolioOptimizer:
    """简单约束求解。不用 cvxpy/scipy，纯 numpy 裁剪。"""

    def solve(
        self,
        raw_weights: dict[str, float],          # 策略原始权重(CrossSignal.target_weights)
        current_positions: dict[str, Position],
        account: Account,
        constraints: PortfolioConstraints,
        prices: dict[str, float],               # 参考价
        industry_map: dict[str, str] | None = None,
    ) -> TargetPortfolio:

        # —— 步骤 1: 约束裁剪 ——
        w = dict(raw_weights)

        # 1a. 单票上限裁剪
        for t in w:
            w[t] = min(w[t], constraints.max_weight)
            w[t] = max(w[t], constraints.min_weight)

        # 1b. 行业上限裁剪(如果有)
        if constraints.max_industry_weight and industry_map:
            w = self._clip_industry(w, industry_map, constraints.max_industry_weight)

        # 1c. 总权重归一化
        total = sum(w.values())
        if total > constraints.max_total_weight:
            scale = constraints.max_total_weight / total
            w = {t: v * scale for t, v in w.items()}

        # 1d. 换手约束(如果有)
        if constraints.max_turnover:
            w = self._clip_turnover(w, current_positions, account, constraints.max_turnover)

        pre_opt = dict(w)  # 保存优化后、取整前的权重

        # —— 步骤 2: 手数取整 ——
        positions = {}
        for ticker, weight in w.items():
            notional = weight * account.total_asset
            price = prices[ticker]
            raw_qty = notional / price
            qty = int(raw_qty // constraints.lot_size) * constraints.lot_size

            # 过滤太小的仓位(交易金额 < 阈值)
            if qty * price < constraints.min_trade_value:
                qty = 0

            if qty > 0:
                positions[ticker] = TargetPosition(
                    ticker=ticker,
                    target_weight=qty * price / account.total_asset,  # 取整后实际权重
                    target_quantity=qty,
                    target_notional=qty * price,
                    ref_price=price,
                    side="LONG",
                )

        return TargetPortfolio(
            strategy_id=...,
            positions=positions,
            solver_status="optimal",
            pre_opt_weights=pre_opt,
            ...
        )
```

#### 为什么不用 cvxpy / scipy.optimize

| 求解器 | 适用场景 | 你需要吗 |
|---|---|---|
| cvxpy | 均值-方差优化、二次规划（最小化 w'Σw subject to …） | 不需要。你的策略已经给出了目标权重，不需要从协方差矩阵求最优 |
| scipy.optimize | 通用非线性优化 | 不需要。你的约束全是 box + 线性，裁剪就够了 |
| 纯 numpy 裁剪 | 策略给权重 + 简单约束 | **够了** |

**什么时候需要 cvxpy？** 如果将来做风险平价（risk parity）或者 Black-Litterman，需要求解 `min w'Σw s.t. 约束`，那时候加。当前流程是：

1. 策略（alstm）输出 `scores`
2. `scores →` 权重（等权/ICIR 加权/排序取 topN）—— **这一步在策略内部完成**（§16.3 / §17.2 的四法在这里）
3. 权重 → 约束裁剪 → 手数取整 —— **这一步在 `PortfolioOptimizer`**

**没有"从零求最优组合"这个环节，所以不需要二次规划。**

> **对 §17.2 的澄清**：§17.2 的四种权重分配方法（等权/score 加权/风险平价/均值-方差）全部发生在**策略内部**（scores → weights）。`PortfolioOptimizer` 名字里有 optimizer，但它**不求最优、只做约束满足**（"优化器只做约束满足"）。§17.2 方法 4（Markowitz）就是这里说的"将来需要 cvxpy"的那个场景，当前不用。

### 34.4 TargetPortfolio / TargetPosition schema

```python
@dataclass(frozen=True)
class TargetPosition:
    """单个标的的目标持仓"""
    ticker: str
    target_weight: float        # 目标权重(优化后，满足约束)
    target_quantity: int        # 目标股数(整数，已做手数取整)
    target_notional: float      # 目标名义金额 = target_quantity * ref_price
    ref_price: float            # 参考价(T 日收盘价，用于权重↔股数换算)
    side: str                   # "LONG" / "FLAT"  (当前不做空)


@dataclass(frozen=True)
class TargetPortfolio:
    """一次调仓的完整目标持仓"""

    # === 身份 ===
    strategy_id: str
    trade_date: date            # 信号日(T 日)
    exec_date: date             # 执行日(T+1 日)
    create_time: datetime       # 产生时间

    # === 目标持仓 ===
    positions: dict[str, TargetPosition]  # {ticker: TargetPosition}

    # === 组合级汇总 ===
    total_weight: float         # sum(abs(weight))，应接近 1.0
    num_holdings: int           # 持仓标的数
    cash_weight: float          # 1.0 - total_weight，剩余现金比例
    estimated_turnover: float   # 预估换手率(vs 当前持仓)

    # === 求解元信息 ===
    solver_status: str          # "optimal" / "suboptimal" / "infeasible"
    constraints_applied: list[str]  # 实际生效的约束名列表
    pre_opt_weights: dict[str, float] | None  # 优化前的原始权重(调试用)
```

#### 目标仓位的单位：三者共存，权重为主

| 单位 | 字段 | 用途 |
|---|---|---|
| 权重 | `target_weight` | **主单位**。策略产出权重，优化器在权重空间求解，所有约束表达为权重约束 |
| 股数 | `target_quantity` | **执行单位**。权重 × 总资产 / 参考价 → 取整到 100 股（手） |
| 名义金额 | `target_notional` | 风控/报告用。= quantity × ref_price |

**换算链**：

```text
raw_quantity   = target_weight * account.total_asset / ref_price
target_quantity = floor_to_lot(raw_quantity, lot_size=100)
# 取整后反算实际权重
actual_weight  = target_quantity * ref_price / account.total_asset
```

**取整会导致实际权重偏离目标权重。小资金（< 100 万）下偏离可能很大——这就是 §17.3 说的"整数手数是杀手"问题。** `TargetPortfolio` 保存的是取整后的量，`pre_opt_weights` 保存取整前的，方便事后分析偏离度。

### 34.5 时间戳语义：T 日收盘产出，T+1 开盘执行

```text
T 日（信号日）：
  15:00        收盘
  15:00-17:00  策略计算(alstm 推理 / 因子合成)
  → CrossSignal（T 日收盘数据驱动）
  → PortfolioOptimizer.solve()（ref_price = T 日收盘价）
  → TargetPortfolio（exec_date = T+1）

T+1 日（执行日）：
  09:30        开盘
  → cs_portfolio_sim.rebalance()
  → 成交价 = T+1 日开盘价(回测) / 实际成交价(实盘)
```

**`TargetPortfolio` 的两个日期字段**：

- `trade_date` = T：信号基于 T 日数据产出
- `exec_date` = T+1：实际执行日

**为什么不用 T 日收盘价执行？** 防止前视偏差（look-ahead bias）。T 日收盘价是你看到的最后价格，不是你能成交的价格。回测中用 T+1 开盘价更保守且更真实。

> **与 §18.2 完全一致**：§18.2 定的"T 日收盘后算信号 → 用 T+1 日开盘价成交（+滑点）"就是这条时间线。`ref_price`（T 收盘价）只用于**权重↔股数换算**，**成交价另算**（T+1 开盘 ± 滑点）——两个价用途不同，别混。

### 34.6 与 Signal 的对应关系：1:1，新增 PORTFOLIO 事件

**1:1，每个 `CrossSignal` 产出一个 `TargetPortfolio`**：

```python
# engine/backtest_cs.py 截面回测引擎
for trade_date in calendar:
    bars = data_api.get_bars(...)

    # 策略产出信号
    cross_signal = strategy.on_rebalance(trade_date, bars, ...)
    emit(SIGNAL, cross_signal)

    # 约束求解 → 目标组合
    target_portfolio = optimizer.solve(
        raw_weights=cross_signal.target_weights,
        current_positions=book.get_positions(),
        account=book.account,
        constraints=self.constraints,
        prices={t: bars[t].close for t in bars},
    )
    emit(PORTFOLIO, target_portfolio)  # 新增事件类型

    # 执行调仓(在 T+1 日的 bar 上)
    # ... rebalance 逻辑 ...
```

**EventType 再加一个**：

```python
class EventType(Enum):
    # ... 原来的 13 个(含 SIGNAL) ...
    PORTFOLIO = auto()   # 新增:目标组合(TargetPortfolio)
```

**现在是 14 个事件。**

#### 时序策略的情况不同——没有 TargetPortfolio 这一层

```text
截面策略：CrossSignal → TargetPortfolio → Orders
时序策略：SignalData  → 直接 Order  （无组合优化，单票决策）
```

时序策略是逐票决策，不需要组合级约束求解。如果将来 CTA 策略也要做组合约束（如"期货总保证金不超过 50%"），再加一个 CTA 版本的简化 optimizer。

#### Journal 链

```text
on_rebalance() → CrossSignal
    ↓
PortfolioOptimizer.solve() → TargetPortfolio
    ↓
cs_portfolio_sim.rebalance()
  输入: target_portfolio.positions, current_positions
  内部: 差额计算 → 先卖后买 → 逐笔预检+成交模拟
  输出: list[Order], list[Trade]
    ↓
Journal（SIGNAL → PORTFOLIO → ORDER → TRADE → POSITION）
```

### 34.7 落盘与 Parquet schema

```text
data_platform/
  signals/                       # (§32.6 已有)
    cross/
      alstm_v1/
        2024-01-02.parquet       # CrossSignal
  portfolios/                    # 新增
    alstm_v1/
      2024-01-02.parquet         # TargetPortfolio
```

**Portfolio 的 Parquet schema**：

| 列 | 类型 | 说明 |
|---|---|---|
| trade_date | date | 信号日 |
| exec_date | date | 执行日 |
| ticker | string | 标的 |
| target_weight | float64 | 目标权重（取整后） |
| target_quantity | int64 | 目标股数 |
| target_notional | float64 | 目标金额 |
| ref_price | float64 | 参考价 |
| pre_opt_weight | float64 | 优化前权重（nullable） |
| strategy_id | string | 策略 |
| solver_status | string | 求解状态 |

落盘时机同信号：**DAY_END 事件处理时**写入。

### 34.8 对已有设计的修订

| 修订对象 | 原文 | 修订为 |
|---|---|---|
| **§26.5 / §26.9** | `on_rebalance() → dict[str,float]`，`rebalance(target_weights, ...)` 内部从权重算手数 | `on_rebalance() → CrossSignal`；**`rebalance(target_portfolio, ...)` 只做差额→逐笔**；**手数取整移出 `cs_portfolio_sim`，移入 `PortfolioOptimizer`**（§26.5 的步骤 [1] 权重→股数→floor 整段上移） |
| **§32.2** | `CrossSignal` 带 `max_weight` / `turnover_limit` 字段 | **这两个字段废弃**。约束的权威是 `PortfolioConstraints`（作为 `solve()` 的入参），不再由信号携带——**这解掉了 §32.8 的遗留问题"谁是权威"**。`CrossSignal` 只剩 meta + `target_weights` + `scores` + `reason` |
| **§29.3 / §29.11 / §32.5** | EventType 12 → 13（补 SIGNAL） | **12 → 14**（补 SIGNAL + **PORTFOLIO**）。`PORTFOLIO` 也进 `PERSIST_TYPES` |
| **§17.2** | 四种权重分配方法与"约束求解"混在一层 | 澄清：权重分配在**策略内部**完成，`PortfolioOptimizer` **不求最优、只做约束满足**；cvxpy 留给将来的 risk parity / Black-Litterman |
| **§17.3** | 做法 1（简单取整+现金缓冲）+ 做法 3（门槛过滤）仅为建议 | **落地为 `solve()` 步骤 2**：`floor_to_lot` + `min_trade_value=5000` 门槛 + `max_total_weight=0.95` 留 5% 现金。做法 2（整数规划）不做，与"不用 cvxpy"一致 |

### 34.9 汇总

| 决策 | 结论 |
|---|---|
| 目标仓位单位 | 权重为主（优化空间），股数为执行单位，三者共存于 `TargetPosition` |
| 约束类型 | 单票上限 + 行业上限 + 换手上限 + 总权重/现金 + 手数取整 |
| 求解器 | 纯 numpy 裁剪，不用 cvxpy/scipy。**策略给权重，优化器只做约束满足** |
| 时间戳 | T 日收盘产出（`trade_date`），T+1 开盘执行（`exec_date`） |
| 与 Signal 关系 | **1:1**，每个 `CrossSignal` 产一个 `TargetPortfolio` |
| 时序策略 | 无 `TargetPortfolio`，`SignalData` 直接转 Order |
| 新增事件 | `PORTFOLIO`（EventType 从 13 → **14**） |
| 新增代码 | `kernel/models.py` 增加 `TargetPosition` + `TargetPortfolio`（~40 行），`kernel/optimizer.py`（~150 行） |

**代码量**：本节 +190 行（models ~40 + optimizer ~150）；另 ~30 行手数取整逻辑从 `cs_portfolio_sim` 移入 optimizer（等价重构，不计新增）。**薄壳合计 ≈2630 行**（2440 + 190）。

**本节仍未解决**：`solver_status` 何时返回 `"suboptimal"` / `"infeasible"`（代码只设 `"optimal"`；全被 `min_trade_value` 过滤光算 infeasible 吗？）；`_clip_turnover` / `_clip_industry` 的具体裁剪算法（按比例缩放？逐个裁？）；`estimated_turnover` 的精确定义（分母用旧总资产还是新总资产）；`constraints_applied` 的记录粒度（触发了就记，还是记每个标的被哪条约束改了）；期货/期权的 `lot_size` 与合约乘数（§17.3 遗留）；CTA 版简化 optimizer 的触发条件。

---

## 35. Service 契约与配置管理（Service 层不需要 + 配置管理：YAML→TOML 见 §69）

> 回应 §2.6（Service 契约：注册与发现 / 启动顺序 / 配置注入）、§13.1（配置管理：格式 / 分层）。结论与 §30（EB）/ §31（DataBus）**同一个模式**——公司 Pysim 的 Service 层在薄壳里没有对应物，直接说清楚。**新增代码 ≈50 行**（`kernel/config.py`）。

### 35.1 Context：第三个"你不需要"的公司组件

§2.2 的 EventBrick → §30 给了等价物；§2.3 的 DataBus → §31 不需要；本节 §2.6 的 Service 层 → **同样不需要**。公司 Pysim 是通用框架，需要把功能拆成可组合的 Service；你的薄壳是固定流水线，**把 6 个 Service 变成 6 行代码**。

### 35.2 公司为什么有 Service

公司 Pysim 运行时：

```text
PysimService
  — SystemTimeUpdateService  （推进系统时钟）
  — TimeControlService       （控制 TI/DI 边界）
  — MarketDataService × N    （N 个行情通道）
  — SignalPuber              （信号发布）
  — TradeplanService         （交易计划）
  — RcsService               （风控）
  — CommandService           （指令下发）
  — ... （用户自定义 Service）
```

Service 需要**注册 / 发现 / 启动顺序 / 配置注入**，因为：

- **不同业务组合不同的 Service 集合**（信号回测 vs 路由回测 vs 实盘）
- **Service 之间有依赖**（MarketData 要先于 Signal 启动）
- **多机部署时 Service 可能分布在不同节点**
- **XML 配置驱动**（运维可改配置，不改代码）

### 35.3 你的场景：固定流水线，不需要 Service 抽象

| 公司 Service | 你的薄壳等价物 | 为什么不需要 Service 抽象 |
|---|---|---|
| SystemTimeUpdateService | `for trade_date in calendar:` | 一行 for 循环就是你的"时钟推进" |
| TimeControlService | 不需要 | 没有 TI 粒度，DAY_START/DAY_END 在主循环里硬编码 |
| MarketDataService | `data_api.get_bars()` | 一个函数调用，不需要独立 Service |
| SignalPuber | `strategy.on_bar()` 的返回值 | 函数返回值，不是消息发布 |
| TradeplanService | `optimizer.solve()` | 一个函数调用 |
| RcsService | `common.pre_check()` | 一个函数调用 |

**你的回测引擎 `run()` 方法里已经把所有 "Service" 的功能写成了顺序调用**：

```python
def run(self, start, end):
    calendar = self.data_api.get_trading_calendar(start, end)   # ≈ TimeControl

    for trade_date in calendar:                                   # ≈ SystemTimeUpdate
        bars = self.data_api.get_bars(tickers, trade_date)        # ≈ MarketDataService
        signal = self.strategy.on_rebalance(trade_date, bars, ...)   # ≈ SignalPuber
        portfolio = self.optimizer.solve(signal.target_weights, ...) # ≈ TradeplanService

        for ticker, pos in portfolio.positions.items():
            reject = common.pre_check(...)                        # ≈ RcsService
            if not reject:
                trade = self.sim.execute(...)
        ...
```

**把 6 个 Service 变成 6 行代码。不需要注册、不需要发现、不需要启动顺序——因为执行顺序就是代码的书写顺序。**

### 35.4 注册与发现 → 不需要，构造函数注入

**公司**：Service 通过配置文件声明，框架启动时扫描配置、实例化 Service、注册到 ServiceManager，运行时通过名字查找。

**你的薄壳**：构造函数注入，Python 对象引用。

```python
class CrossSectionalBacktest:
    def __init__(
        self,
        data_api: DataAPI,
        strategy: BaseCrossStrategy,
        optimizer: PortfolioOptimizer,
        sim: CSPortfolioSim,
        journal: Journal,
        config: BacktestConfig,
    ):
        self.data_api = data_api
        self.strategy = strategy
        self.optimizer = optimizer
        self.sim = sim
        self.journal = journal
        self.config = config
```

调用方（你的入口脚本）：

```python
data_api = DataAPI(config.data_dir)
strategy = AlstmStrategy(config.strategy)
optimizer = PortfolioOptimizer(config.constraints)
sim = CSPortfolioSim(common_logic)
journal = Journal(config.journal_dir)

engine = CrossSectionalBacktest(data_api, strategy, optimizer, sim, journal, config)
engine.run(start="2020-01-01", end="2024-12-31")
```

**"发现"就是构造时传进去。"注册"就是赋值给 self。单进程 Python 里，对象引用就是最简单的依赖注入。**

### 35.5 启动顺序 → 不需要，代码顺序就是启动顺序

**公司**需要声明式启动顺序，是因为 Service 之间有**异步初始化**（连行情通道、建立 SHM、预热数据）。

**你的薄壳**：所有"初始化"都是**同步**的：

```python
def run(self, start, end):
    # "启动顺序"就是这三行的顺序
    self.data_api.warmup(start, end)        # 1. 数据先加载
    self.strategy.on_init(self.data_api)    # 2. 策略再初始化(可能加载模型)
    self.journal.open(start)                # 3. Journal 打开文件

    # 然后进主循环
    for trade_date in calendar:
        ...
```

**如果 `strategy.on_init()` 依赖 `data_api` 已经 warmup，写在它后面就行了。不需要依赖图、不需要拓扑排序。**（呼应 §30.6 的引擎 session 启动期：`emit(SESSION_START)` + `on_init`。）

### 35.6 配置：YAML，一个文件（已由 §69 修正为 TOML 三层）

> **回改（2026-10-10）**：本节"配置格式 = YAML + dataclass、一个文件、不分层"的结论已由 §69 修正为 **TOML 三层**（config.toml + config/strategies/{id}.toml + config/.env）；本节明确"本批未覆盖、仍开放"的两项——敏感信息管理、配置版本化与审计——也已由 §69 关闭（.env 不入 git 不打印；审计 = git log + config_snapshot）。**以 §69 为准**，本节正文保留作过程记录。

**公司用 XML 是因为 C++ 生态**（protobuf/XML 是 C++ 项目的惯例）。**你的 Python 平台用 YAML。**

```yaml
# config/backtest_alstm.yaml
strategy:
  type: "alstm_v1"
  model_path: "research/models/alstm_v1.pt"
  universe: "hs300"
  rebalance_freq: "weekly"

constraints:
  max_weight: 0.10
  max_industry_weight: 0.20
  max_turnover: 0.30
  max_total_weight: 0.95
  lot_size: 100
  min_trade_value: 5000

data:
  data_dir: "data_platform"
  adjust: "qfq"

backtest:
  start: "2020-01-01"
  end: "2024-12-31"
  initial_cash: 1000000
  commission_rate: 0.0003
  slippage_model: "fixed"
  slippage_bps: 5

journal:
  dir: "data_platform/journal"
  flush_size: 100

mode: "backtest"   # backtest / live
```

加载：

```python
@dataclass
class BacktestConfig:
    strategy: StrategyConfig
    constraints: PortfolioConstraints
    data: DataConfig
    backtest: BacktestParams
    journal: JournalConfig
    mode: str

def load_config(path: str) -> BacktestConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return BacktestConfig(**parse(raw))  # 用 dacite 或手写解析
```

**一个 YAML 文件，一个 dataclass。不需要分层配置、不需要配置中心、不需要热更新。改配置 → 重启引擎。**（这就是此前已定的"不做热更新，重启即可"。）

> **对 §13.1 的回应**：配置**格式 = YAML + dataclass**（不是 TOML/XML/纯 Python）；配置**不分层**（不搞全局/域级/策略级/环境级——一个文件就是一个回测 session 的全部配置）；**不做热更新**。§13.1 的另外两问（敏感信息 API key/密码怎么管、配置版本化与审计）**本批未覆盖，仍开放**——当前示例里没有敏感信息（全是本地路径）；版本化最简做法是 YAML 进 git，但审计规则未定。

**YAML 里的字段与既有设计的对应**：

| YAML 块 | 对应 | 来源 |
|---|---|---|
| `constraints:` | `PortfolioConstraints` 全部字段 | §34.2 |
| `data.adjust: "qfq"` | 因子评价/回测复权口径 | §27.3 |
| `backtest.commission_rate: 0.0003` / `slippage_bps: 5` | 成本模型默认参数（固定比例滑点档） | §18.1 |
| `journal.dir` / `flush_size` | Journal 落盘 | §23 / §29.8 |
| `mode: backtest / live` | 引擎双模式 | §22 |

> **注意**：`commission_rate=3bp`、`slippage_bps=5`（固定比例档）只是**配置示例的默认值**，不是最终费率表——§18 遗留的印花税/过户费/最低 5 元门槛、流动性约束比例阈值仍未定。

### 35.7 汇总

| 公司概念 | 你的等价物 | 代码量 |
|---|---|---|
| ServiceManager（注册/发现） | 构造函数参数传递 | **0 行** |
| 启动顺序依赖 | 代码书写顺序（同步调用） | **0 行** |
| XML 配置 | 一个 YAML + 一个 `BacktestConfig` dataclass | **~50 行** |
| SystemTimeUpdateService | `for trade_date in calendar:` | 1 行 |
| TimeControlService | 不需要（无 TI 粒度） | 0 行 |
| MarketDataService | `data_api.get_bars()` | 已有 |
| SignalPuber | `strategy.on_bar()` 返回值 | 已有 |
| TradeplanService | `optimizer.solve()` | 已有（§34） |
| RcsService | `common.pre_check()` | 已有 |

**不新增任何架构组件。需要写的只有 `BacktestConfig` dataclass（~30 行）+ `load_config()` 函数（~20 行），放在 `kernel/config.py`。**

### 35.8 对已有设计的补充

| 补充对象 | 内容 |
|---|---|
| **§26.10** | `CrossSectionalBacktest.__init__` 细化为**构造函数注入六个协作者**（`data_api` / `strategy` / `optimizer` / `sim` / `journal` / `config`）——原先示例里 `sim` 是内部创建的，现在全部外部注入，测试可替换 |
| **§25.5 / §29.9** | 主循环新增三行同步启动（`warmup` → `on_init` → `journal.open`），对应 §30.6 引擎 session 的启动期 |
| **§18.1** | 成本模型拿到配置默认值（`commission_rate=3bp`、固定比例滑点 `slippage_bps=5`）；完整费率表仍是 §18 遗留 |
| **§34** | YAML 的 `constraints:` 块就是 `PortfolioConstraints` 的字段镜像；`optimizer` 进入构造注入清单 |

**代码量**：本节 +50 行（`kernel/config.py`：`BacktestConfig` ~30 + `load_config` ~20）。**薄壳合计 ≈2680 行**（2630 + 50）。

**本节仍未解决**：敏感信息（API key / 数据库密码 / vnpy 网关账户）怎么管（已定 → §69 .env 不入 git 不打印，2026-10-10 回改）；配置的版本化与审计（已定 → §69 git log + config_snapshot，2026-10-10 回改）；`StrategyConfig` / `DataConfig` / `BacktestParams` / `JournalConfig` 四个子 dataclass 的字段清单；`dacite` 还是手写解析（已定 → §69 手写 load_config/load_strategy_config/load_secrets，2026-10-10 回改）。

---

## 36. DQC 四维规则集（44 条规则 + pandera 执行模型）

> 回应 §3.1（四维规则集：检查项 / 存放 / 资产覆盖 / 阈值 / 版本化 / 粒度——**五问全解**），并连带：细化 §3.2（每条规则标注 HARD/SOFT）、部分回应 §3.3（执行时机）、§3.4（结果落盘）、§3.11（修正检测）。**新增代码 ≈790 行（`dqc/` 模块，数据平台侧，不计入薄壳）**。

### 36.1 Context

平台 DQC 采用 **pandera** 做验证，规则分 **HARD**（阻断入库）和 **SOFT**（告警+标记）两级。4 个维度：完整性 / 合法性 / 一致性 / 时序性。本节定义：具体检查项、阈值、存放方式、版本策略、执行模型。

> **条数口径说明**：来源材料标题写"完整规则集（40 条）"，且阈值三类拆分写 24+14+2=40；但四张维度表实际有 **10+14+11+9=44 行**（C01–C10 / L01–L14 / K01–K11 / T01–T09）。本文以**表格 44 条为准**逐条保留，"40 条"与阈值拆分数字照录，差异在此标注（与 §28.3 的口径差异同一处理方式）。

### 36.2 完整性（Completeness）— 10 条

| ID | 检查名 | 检查内容 | 严重度 | 阈值 | 阈值来源 | 适用资产 |
|---|---|---|---|---|---|---|
| C01 | row_count_vs_calendar | 去重 trade_date 数 = 交易日历天数 | HARD | 精确匹配 | 固定:日历是唯一真相 | daily, fund |
| C02 | row_count_vs_universe | 单日 ticker 数 ≥ 预期 universe 的 95% | SOFT | ≥95% | 统计:新上市/退市过渡期 2-5% 缺失正常 | daily, fund |
| C03 | minute_bar_count | 每 ticker 每日恰好 240 根(A 股)/ 按合约定(期货) | HARD | 精确 | 固定:交易所交易时段 | minute |
| C04 | null_critical_fields | 非停牌票的 OHLCV 零空值 | HARD | 0% null | 固定:交易中的票不可能没价格 | daily, minute |
| C05 | null_soft_fields | amount/adjfactor/PE/PB 空值率 ≤ 1% | SOFT | ≤1% | 统计:数据源偶尔缺失 | daily, fund |
| C06 | adjfactor_present | 每个 ticker×date 都有非空 adjfactor | HARD | 0% 缺失 | 固定:缺 adjfactor 导致复权计算全错 | daily |
| C07 | calendar_completeness | 日历覆盖当年+未来 30 交易日,无断点 | HARD | 无缺失 | 固定:日历是所有完整性检查的前提 | reference |
| C08 | factor_date_coverage | 因子日期覆盖,滚动 252 日内缺失 ≤ 2 天 | SOFT | ≤2 天/年 | 统计:偶尔延迟计算可接受 | factor |
| C09 | factor_ticker_coverage | 因子×日的 ticker 数 ≥ universe 的 90% | SOFT | ≥90% | 统计:动量等因子需历史,新股算不出 | factor |
| C10 | industry_coverage | 所有活跃 ticker 都有行业分类 | HARD | 100% | 固定:行业是中性化/风险分解的必要输入 | reference |

### 36.3 合法性（Legality）— 14 条

| ID | 检查名 | 检查内容 | 严重度 | 阈值 | 阈值来源 | 适用资产 |
|---|---|---|---|---|---|---|
| L01 | price_positive | 非停牌票 OHLC > 0 | HARD | >0 | 固定 | daily, minute |
| L02 | volume_non_negative | volume, amount ≥ 0 | HARD | ≥0 | 固定 | daily, minute |
| L03 | ohlc_relation | low ≤ open ≤ high 且 low ≤ close ≤ high | HARD | 精确 | 固定:OHLC 定义 | daily, minute |
| L04 | price_limit_stock | abs(close/prev_close - 1) ≤ 板块涨跌停限制 + 0.5% 容差 | SOFT | 主板 10.5%,创/科 20.5%,IPO 前 5 日 30.5% | 固定:监管规则 + 除权日容差 | daily |
| L05 | price_limit_futures | abs(settle/prev_settle - 1) ≤ 合约涨跌停限制 + 0.5% | SOFT | 按合约查 reference | 固定:交易所定义 | daily(期货) |
| L06 | adjfactor_positive | adjfactor > 0 | HARD | >0 | 固定:乘法比率不可能非正 | daily |
| L07 | adjfactor_range | 0.01 < adjfactor < 1000 | SOFT | (0.01, 1000) | 统计:A 股历史极值 | daily |
| L08 | boolean_flags | suspend/valid/is_limit_up/is_limit_down 为 bool,无空 | HARD | 精确 | 固定:schema 约束 | daily |
| L09 | pe_pb_range | PE ∈ (-10000, 10000), PB ∈ (-1000, 1000) | SOFT | 上述范围 | 统计:99.9 分位 | fund |
| L10 | market_cap_positive | 总市值 > 0 | HARD | >0 | 固定 | fund |
| L11 | factor_finite | 因子值无 inf/-inf | HARD | 0 个 inf | 固定:inf 传播污染下游 | factor |
| L12 | factor_nan_rate | 因子列 NaN 率 ≤ 10% | SOFT | ≤10% | 统计:部分因子对子集票算不出 | factor |
| L13 | ticker_format | ticker 匹配交易所正则 `^[036]\d{5}\.(SH\|SZ)$` | HARD | 100% | 固定 | all |
| L14 | date_format | 日期字段为 ISO 8601 | HARD | 100% | 固定 | all |

### 36.4 一致性（Consistency）— 11 条

| ID | 检查名 | 检查内容 | 严重度 | 阈值 | 阈值来源 | 适用资产 |
|---|---|---|---|---|---|---|
| K01 | open_gap_check | 非除权日,今日 open(复权) vs 昨日 close(复权) 缺口 ≤ 10% | SOFT | ≤10% | 统计:A 股隔夜收益 99 分位 ~6% | daily |
| K02 | volume_amount_match | volume > 0 ⇔ amount > 0(同时为零或同时非零) | HARD | 精确 | 固定:定义一致 | daily, minute |
| K03 | suspend_consistency | suspend=True → volume=0 且 OHLC 相等(或零) | HARD | 精确 | 固定:停牌不交易 | daily |
| K04 | limit_flag_consistency | is_limit_up=True → close=high; is_limit_down=True → close=low | SOFT | 容差 0.01 | 固定:tick size 精度 | daily |
| K05 | adjfactor_stable | 非公司行动日,adjfactor = 前一日 adjfactor | HARD | 精确 | 固定:adjfactor 只在除权除息日变 | daily |
| K06 | daily_vs_minute_close | 日线 close = 当日最后一根分钟线 close | HARD | 容差 0.01 | 固定:定义一致 | daily+minute |
| K07 | daily_vs_minute_volume | 日线 volume = 当日分钟 volume 之和 | HARD | 相对误差 ≤ 0.1% | 固定:volume 可加 | daily+minute |
| K08 | daily_vs_minute_hl | 日线 high ≥ max(分钟 high); 日线 low ≤ min(分钟 low) | HARD | 精确 | 固定:极值包含关系 | daily+minute |
| K09 | factor_cross_section | 因子截面 std > 1e-8 且 abs(mean) < 100 | SOFT | 上述阈值 | 统计:退化分布 = 计算 bug | factor |
| K10 | industry_universe_match | universe 集合 = industry 分类表集合 | HARD | 集合相等 | 固定 | reference |
| K11 | fund_vs_daily_tickers | fundamental ticker 集合 ⊆ daily ticker 集合,差集 ≤ 5 | SOFT | ≤5 | 统计:上市/退市过渡期 | fund+daily |

### 36.5 时序性（Temporality）— 9 条

| ID | 检查名 | 检查内容 | 严重度 | 阈值 | 阈值来源 | 适用资产 |
|---|---|---|---|---|---|---|
| T01 | daily_freshness | 最新 trade_date 距今 ≤ 1 个交易日 | HARD | ≤1 日 | 固定:日终数据 T+1 早晨到 | daily, fund |
| T02 | minute_freshness | 分钟线最新 trade_date 距今 ≤ 1 个交易日 | HARD | ≤1 日 | 固定 | minute |
| T03 | no_future_dates | 所有 trade_date ≤ 今天 | HARD | 精确 | 固定 | all |
| T04 | date_ascending | 每个 ticker 分区内 trade_date 严格递增无重复 | HARD | 精确 | 固定 | daily, minute, fund |
| T05 | minute_timestamp_order | 每个 ticker×date 内分钟 timestamp 严格递增连续 | HARD | 精确 | 固定 | minute |
| T06 | adjfactor_idempotent | 重新入库时 adjfactor 不变(幂等) | SOFT | 精确 | 固定:adjfactor 定稿后不应变 | daily |
| T07 | factor_lag | 因子 D 日数据在 D+2 前到达 | SOFT | ≤2 日 | 统计:夜间批计算偶尔延迟 | factor |
| T08 | calendar_forward | 日历至少延伸到未来 30 个交易日 | SOFT | ≥30 日 | 固定 | reference |
| T09 | late_correction | 历史已入库数据被修改时标记告警 | SOFT | 变化 > 0.01% 即告警 | 统计:数据源 T+3 内 ~0.1% 订正 | daily, fund |

**HARD/SOFT 分布**：HARD 27 条（阻断入库）/ SOFT 17 条（告警+标记入库）。这就是 §3.2 "硬错误 vs 软警告"的规则级落地——每条规则显式标注，不再逐场景争论。

### 36.6 阈值怎么定（回应 §3.1 子问题 3）

三类方法，每条规则标注了来源：

| 方法 | 占比 | 说明 |
|---|---|---|
| 固定（监管/定义） | ~60%（24 条） | 交易所规则、数学定义、schema 约束。不可调。 |
| 统计（历史校准） | ~35%（14 条） | 基于 3+ 年历史数据的分位数。建议年度复核。 |
| 自适应（滚动计算） | ~5%（2 条） | K09 的因子截面统计。初版不实现，标记为 phase-2。 |

初版只实现固定+统计阈值。自适应需要维护滚动状态，等平台稳定 6 个月后再加。

> **口径差异**：上表 24+14+2=40，与"完整规则集（40 条）"一致；但 §36.2–§36.5 的 44 行逐条统计阈值来源约为 固定 32 / 统计 12（K09 在表内标"统计"、在本节被归为"自适应"）。**实施时以逐条表为准**，本表的占比是设计意图的近似描述。

### 36.7 版本化策略（回应 §3.1 子问题 4：规则改了历史数据要重跑吗）

- 每个 YAML 带 `version: "1.0.0"` 字段（semver）
- **patch**：调阈值；**minor**：加新规则；**major**：删除/根本改变规则
- **历史数据不自动重跑 DQC**
  - 阈值放宽：旧数据一定仍然通过，无需重跑
  - 阈值收紧：旧数据可能新不通过，但已被用于研究，追溯标记意义不大
  - 提供 CLI 按需重验：`python -m dqc.engine revalidate --asset bars_daily --start 2024-01-01 --end 2024-12-31`
- 验证结果存 `_dqc_results.parquet`，带 `rule_version` 列，可审计

### 36.8 可配置粒度（回应 §3.1 子问题 5）

| 粒度 | 结论 |
|---|---|
| 按资产类型（5 个 YAML） | **主粒度** — bars_daily / bars_minute / fundamental / factor / reference 各一套 |
| 按字段（YAML 内 per-check） | 支持 — 每条 check 指定 `columns` 列表 |
| 按板块/交易所 | 通过 check 参数 — L04 的 `default_limit`/`chinext_star_limit` |
| 按合约（期货） | 通过 reference 表 — `contract_specs` 表存每个合约的涨跌停/交易时段，check 运行时查表 |
| 按 ticker | **不支持** — 维护成本 > 收益。个别 ticker 持续触发 SOFT 规则时，调整通用阈值或加排除列表 |

### 36.9 规则存放：YAML 配置 + Python 自定义检查函数（混合）

回应 §3.1 子问题 1（存放位置）与子问题 2（资产类型覆盖）：

```text
dqc/
  rules/
    bars_daily.yaml        # 简单规则用 YAML 声明(范围检查、空值率、格式)
    bars_minute.yaml
    fundamental.yaml
    factor.yaml
    reference.yaml
  checks/
    custom_checks.py       # 复杂规则用 Python 函数(OHLC 关系、日线 vs 分钟勾稽、涨跌停)
    cross_checks.py        # 跨资产检查(K06-K08,需要两张表同时在)
    schemas/
      bars_daily_schema.py  # 从 YAML 生成 pandera DataFrameSchema
      ...
  engine.py                # 加载 YAML → 构建 pandera schema → 执行验证
  report.py                # 结果写入 _dqc_results.parquet
```

**资产类型覆盖**（回应子问题 2）：5 个 YAML 对应 5 类资产 — `bars_daily`（日线行情）/ `bars_minute`（分钟行情）/ `fundamental`（基本面）/ `factor`（因子）/ `reference`（日历/行业/合约规格）。规则表的"适用资产"列就是这 5 类的组合（daily / minute / fund / factor / reference / all / daily+minute / fund+daily）。**"另类"数据（舆情/卫星等）暂无规则集**——等有另类数据源时再加第 6 个 YAML。

YAML 示例（`bars_daily.yaml`）：

```yaml
asset_type: bars_daily
version: "1.0.0"

columns:
  close: {dtype: float64, nullable: false}
  volume: {dtype: int64, nullable: false}
  adjfactor: {dtype: float64, nullable: false}
  suspend: {dtype: bool, nullable: false}

checks:
  - id: L01
    name: price_positive
    severity: HARD
    type: column_check
    columns: [open, high, low, close]
    condition: "gt"
    value: 0
    filter: "suspend == False"

  - id: L03
    name: ohlc_relation
    severity: HARD
    type: custom_check
    function: "checks.custom_checks.check_ohlc_relation"

  - id: L04
    name: price_limit_stock
    severity: SOFT
    type: custom_check
    function: "checks.custom_checks.check_price_limit"
    params:
      default_limit: 0.10
      chinext_star_limit: 0.20
      tolerance: 0.005
```

**为什么混合而不是纯 Python 或纯 YAML**：

- **纯 pandera Python**：阈值散落在代码里，调参要改代码
- **纯 YAML**：表达不了复杂检查（跨表勾稽、条件逻辑）
- **混合**：简单规则 YAML 声明（改阈值不改代码），复杂规则 Python 函数（YAML 引用函数名）

### 36.10 执行模型（回应 §3.3 子问题 1：pre-load 还是 post-load）

DQC 在**两个时点**运行：

**入库时（实时）**：数据写入 Parquet 前跑 HARD 检查。不通过 → 阻断入库。SOFT 检查同时运行，结果写 `_dqc_results.parquet`。

**批量扫描（定期）**：每周跑一次全量 SOFT 检查 + 跨资产检查（K06-K08 需要 daily+minute 同时在）。T09（迟来订正检测）也在此运行。

```python
# 入库时
def ingest_daily_bars(df, date):
    schema = build_schema("dqc/rules/bars_daily.yaml")
    result = schema.validate(df, lazy=True)      # lazy=True 收集所有错误
    hard_failures = [e for e in result.errors if severity(e) == "HARD"]
    if hard_failures:
        raise DQCHardFailure(hard_failures)      # 阻断入库
    soft_failures = [e for e in result.errors if severity(e) == "SOFT"]
    log_results(soft_failures, date)             # 写 _dqc_results.parquet
    write_parquet(df, date)                      # 通过,入库
```

**这就是"两遍"模型**：入库时跑增量 HARD+SOFT（只查新数据），每周批量跑全量 SOFT + 跨粒度。响应 §3.3 子问题 3（抽样策略）：**不抽样，增量全量 + 定期全量**。

> **对 §3.3 的回应**：执行时机已定（pre-load HARD 阻断 + 定期批量）；**耗时预算、并行度（按合约/按日期）、失败后"重跑整批 vs 只跑失败部分"仍未定**——`revalidate` CLI 支持按 asset+日期范围重验，可视为"只跑指定范围"，但耗时预算与并行策略本批未涉及。

### 36.11 新增代码量与验证方式

| 模块 | 行数 | 说明 |
|---|---|---|
| 5 个 YAML 规则文件 | ~300 行（总） | 声明式配置 |
| `custom_checks.py` | ~200 行 | 15 个复杂检查函数 |
| `cross_checks.py` | ~80 行 | K06-K08 跨粒度检查 |
| `engine.py` | ~150 行 | YAML→pandera schema + 执行 + 结果收集 |
| `report.py` | ~60 行 | 写 `_dqc_results.parquet` |
| **总计** | **~790 行** | 全部在 `dqc/`（数据平台侧，**不计入薄壳**） |

**验证方式**：

1. 构造"全干净"的小数据集（10 ticker × 5 天），跑全部 44 条规则，应 0 failure
2. 针对每条 HARD 规则，构造恰好触发的脏数据，验证入库被阻断
3. 针对 K06-K08，构造 daily 和 minute 数据不一致的 case，验证跨粒度检查命中
4. 跑一次真实 A 股数据（如 hs300 × 1 年），看 SOFT failure 率是否在预期范围内

### 36.12 汇总与对已有设计的补充

| 决策 | 结论 |
|---|---|
| 规则集 | 44 条四维规则（C×10 / L×14 / K×11 / T×9），逐条带 ID/严重度/阈值/来源/适用资产 |
| 严重度 | HARD 27 条阻断入库 / SOFT 17 条告警+标记（= §3.2 的规则级落地） |
| 阈值 | 三类：固定（不可调）/ 统计（年度复核）/ 自适应（phase-2 不做）。初版 = 固定+统计 |
| 存放 | YAML（简单规则，改阈值不改代码）+ Python 函数（复杂规则）混合，pandera 做验证引擎 |
| 资产覆盖 | 5 个 YAML：bars_daily / bars_minute / fundamental / factor / reference；另类数据暂无 |
| 版本化 | semver；patch=阈值 / minor=加规则 / major=改语义；**历史不自动重跑**；CLI 按需重验；结果带 `rule_version` |
| 粒度 | 按资产为主 + per-check columns + check 参数（板块）+ reference 查表（合约）；**按 ticker 不支持** |
| 执行 | 两时点：入库时 HARD 阻断 + SOFT 记录；每周批量全量 SOFT + 跨资产 K06-K08 + T09 |
| 代码量 | `dqc/` ≈790 行（不计入薄壳，薄壳仍 ≈2680 行） |

**对已有设计的补充/细化**：

| 对象 | 内容 |
|---|---|
| **§3.2** | HARD/SOFT 边界从"逐场景争论"变成"逐规则标注"：HARD 27 / SOFT 17，四张表的"严重度"列即权威 |
| **§3.3** | 执行时机=两时点（pre-load HARD + 每周批量）；耗时预算与并行度仍开放 |
| **§3.4** | 结果落 `_dqc_results.parquet`（带 `rule_version`，可审计）= 报告问题的部分答案 |
| **§3.11** | T09 给出**修正检测**：历史数据被改、变化 > 0.01% 即告警（数据源 T+3 内 ~0.1% 订正是常态）；修正应用/影响传播/是否重跑回测仍开放 |
| **§21.1** | 公司 DQC 强/弱规则对齐后，这里给出可执行的规则清单 |

**本节仍未解决**：被拒收数据的隔离区（§3.5 整节未动——`raise DQCHardFailure` 阻断后数据去哪、留多久、怎么修复重跑）；DQC 耗时预算与并行度（§3.3）；`_dqc_results.parquet` 的完整字段 schema、历史保留期、看板与告警通道（§3.4）；另类数据的规则集；L13 正则是否覆盖科创板/北交所代码形态（`^[036]\d{5}` 只含沪深 6 位）；K09 自适应阈值的具体滚动算法（phase-2）；`contract_specs` reference 表的字段 schema。

---

## 37. 硬错误 vs 软警告的判定边界（三级硬粒度 + quality_flag 位掩码）

> 回应 §3.2 全部内容：候选场景表 7 项判定 + 四个子问题（边界粒度 / `quality_flag` 编码 / 下游消费 / 批次级拒收阈值）。并细化 §36（逐规则 `row_action` + 升级阈值、硬拒三级粒度、`ingest_with_dqc` 七步）。**新增代码 ≈80 行**（`dqc/`：QualityFlag ~30 + `compute_quality_flag()` ~40 + `BatchRejectConfig` ~10；`dqc/` 合计 ~870 行）。

### 37.1 先回答候选场景表（§3.2 主问题的 7 个场景）

| 场景 | 判定 | 理由 |
|---|---|---|
| 主键重复 | **硬 – 丢弃后入库** | 不是拒收整批，是去重后入库。保留时间戳较新的一条（或数据源优先级高的）。主键重复 = 数据源推了两次，去重是清洗不是告警 |
| 时间倒流 | **硬 – 拒收该分区** | 时间戳非单调 = 数据结构损坏，下游所有时序逻辑（收益率计算、滚动窗口）都会出错 |
| 空值率 5% | **软** | 5% 在正常波动范围（新上市/退市过渡期、数据源偶尔缺失） |
| 空值率 30% | **硬 – 拒收该批次** | 30% 空值 = 数据源出了系统性问题（接口挂了、字段改名了），不是个别缺失 |
| 价格异常（涨跌停外） | **软 + `quality_flag`** | 除权日、临时停牌复牌、ST 摘帽等合法场景会触发，不能硬拒 |
| 跨源不一致 | **软 + 告警** | 跨源对比本来就有合理误差（收盘价来源不同、精度不同），硬拒会频繁误杀 |
| 关键字段全空 | **硬 – 拒收该行** | OHLCV 全空 = 这行没有任何信息量，入库只会污染下游 |

**注意三种"硬"的粒度不同**（回应子问题 1 的一半）：

| 硬拒粒度 | 场景 | 处理 |
|---|---|---|
| **行级** | 主键重复、关键字段全空 | 丢弃该行，其余行正常入库 |
| **分区级** | 时间倒流 | 拒收该 ticker×date 分区 |
| **批次级** | 空值率 30%+ | 拒收整个 trade_date 的数据批次 |

### 37.2 判定边界：三级阈值体系（回应子问题 1：按规则分，不按资产分）

不是"硬 or 软"二选一，而是**三级**（以行级异常率为轴）：

```text
行级异常率
0% ──────── 5% ──────── 20% ──────── 100%
│            │            │            │
▼            ▼            ▼            ▼
全部通过    软警告+标记   硬拒整批     不可能到这里
(clean)   (ingest with  (reject      (已被前面拦住)
           quality_flag) entire batch)
```

每条规则自己的严重度配置：

```python
@dataclass
class DQCSeverityConfig:
    """每条规则的严重度配置"""

    # 行级判定
    row_action: str                  # "drop" | "flag" | "pass"

    # 批次级升级阈值 – 行级异常太多时升级为批次拒收
    escalation_threshold: float | None   # 异常行占比超过此值 → 硬拒整批

# 示例：
# L01 price_positive:  row_action="drop",  escalation=0.01  (>1% 负价格 → 整批有问题)
# L04 price_limit:     row_action="flag",  escalation=None  (永远不升级,合法异常太多)
# C04 null_critical:   row_action="drop",  escalation=0.20  (>20% 全空 → 数据源挂了)
```

**结论：边界是按规则分的，不是全局也不是按资产分**——每条规则有自己的 `row_action` + `escalation_threshold`，在 YAML 里配（`severity: HARD|SOFT` 之上再带这两个字段）。这就是"每条规则自带处置策略"。

### 37.3 每条规则的完整判定表（44 条：行级动作 + 升级阈值）

这是 §36 四张规则表的**处置列补全**——§36 定"查什么、多严"，本表定"查出来怎么办"。

**完整性**：

| 规则 ID | 行级动作 | 升级阈值 | 说明 |
|---|---|---|---|
| C01 row_count_vs_calendar | –（批次级检查） | N/A | 日期缺失 = 直接硬拒 |
| C02 row_count_vs_universe | –（批次级检查） | N/A | < 95% 软警告 |
| C03 minute_bar_count | drop 该 ticker×date | 5% ticker 不达标 → 硬拒 | |
| C04 null_critical | drop 该行 | 20% → 硬拒整批 | 正常应 ~0%，20% 说明数据源系统性故障 |
| C05 null_soft | flag | None | 永远不升级 |
| C06 adjfactor_present | drop 该行 | 5% → 硬拒 | adjfactor 缺失传播性强 |

**合法性**：

| 规则 ID | 行级动作 | 升级阈值 | 说明 |
|---|---|---|---|
| L01 price_positive | drop | 1% → 硬拒 | 负价格是数据损坏 |
| L02 volume_non_negative | drop | 1% → 硬拒 | 同上 |
| L03 ohlc_relation | drop | 1% → 硬拒 | OHLC 不合法 = 数据损坏 |
| L04 price_limit | flag | None | 除权日等合法场景多 |
| L05 price_limit_futures | flag | None | 同上 |
| L06 adjfactor_positive | drop | 1% → 硬拒 | |
| L07 adjfactor_range | flag | None | |
| L08 boolean_flags | drop | 1% → 硬拒 | schema 违反 |
| L09 pe_pb_range | flag | None | |
| L10 market_cap_positive | drop | 5% → 硬拒 | |
| L11 factor_finite | drop | 5% → 硬拒 | inf 污染下游 |
| L12 factor_nan_rate | flag | None | |
| L13 ticker_format | drop | 1% → 硬拒 | 格式错 = 数据源改了格式 |
| L14 date_format | drop | 1% → 硬拒 | 同上 |

**一致性**：

| 规则 ID | 行级动作 | 升级阈值 | 说明 |
|---|---|---|---|
| K01 open_gap | flag | None | 隔夜缺口常见 |
| K02 volume_amount | drop | 5% → 硬拒 | |
| K03 suspend_consistency | drop | 5% → 硬拒 | |
| K04 limit_flag | flag | None | tick 精度问题 |
| K05 adjfactor_stable | drop | 1% → 硬拒 | adjfactor 异变是严重问题 |
| K06-K08 daily_vs_minute | flag（不丢行） | 10% → 告警升级 | 跨粒度不一致不丢行，但高比例要调查 |
| K09 factor_cross_section | flag | None | |
| K10 industry_universe | –（批次级） | N/A | 不匹配 = 硬拒 reference 更新 |
| K11 fund_vs_daily | flag | None | |

**时序性**：

| 规则 ID | 行级动作 | 升级阈值 | 说明 |
|---|---|---|---|
| T01-T02 freshness | –（批次级） | N/A | 过期 = 硬拒（不入库过期数据） |
| T03 no_future | drop | **任何 1 行 → 硬拒整批** | 未来日期 = 时间戳损坏，零容忍 |
| T04 date_ascending | –（分区级） | N/A | 时间倒流 = 硬拒该分区 |
| T05 minute_order | –（分区级） | N/A | 同上 |
| T06 adjfactor_idempotent | flag | None | |
| T07 factor_lag | flag | None | |
| T08 calendar_forward | flag | None | |
| T09 late_correction | flag | None | |

**规律**：`flag` 规则全部 `escalation=None`（永远不升级）；`drop` 规则的升级阈值按"异常说明的故障等级"定——数据损坏类（负价格/OHLC 错）1%，结构性缺失类（adjfactor/市值/inf）5%，超大面积空值 20%，未来日期 1 行即整批拒收（零容忍）。

### 37.4 quality_flag 位掩码编码（回应子问题 2）

**为什么是位掩码**：

| 方案 | 优点 | 缺点 |
|---|---|---|
| 枚举（单个标签） | 简单 | 一行只能有一个问题？不现实 |
| 多列 bool | 直观 | 40 条规则 → 40 列？太多 |
| JSON 字符串 | 灵活 | DuckDB 查询不友好，过滤慢 |
| **位掩码（int64）** | 一列，可叠加，DuckDB 位运算过滤快 | 需要映射表 |

一行可能同时触发多条 SOFT 规则（比如价格超涨跌停 + PE 超范围），位掩码天然支持叠加。

**编码定义**：

```python
class QualityFlag:
    """位掩码编码,每个 bit 对应一类问题。"""

    CLEAN = 0                       # 0b00000000 – 无问题

    # 合法性类(bit 0-7)
    PRICE_OUTLIER      = 1 << 0    # 0b00000001 – 价格超涨跌停限制
    ADJFACTOR_EXTREME  = 1 << 1    # 0b00000010 – adjfactor 极端值
    PE_PB_EXTREME      = 1 << 2    # 0b00000100 – PE/PB 极端值
    FACTOR_HIGH_NAN    = 1 << 3    # 0b00001000 – 因子 NaN 率偏高

    # 一致性类(bit 8-15)
    OPEN_GAP           = 1 << 8    # 开盘跳空异常
    LIMIT_FLAG_MISMATCH= 1 << 9    # 涨跌停标志不一致
    DAILY_MINUTE_DIFF  = 1 << 10   # 日线 vs 分钟线不一致
    CROSS_SOURCE_DIFF  = 1 << 11   # 跨源数值不一致
    FUND_TICKER_DIFF   = 1 << 12   # 基本面 vs 行情 ticker 不匹配

    # 时序性类(bit 16-23)
    ADJFACTOR_CHANGED  = 1 << 16   # adjfactor 被订正
    LATE_CORRECTION    = 1 << 17   # 历史数据被订正
    FACTOR_LAG         = 1 << 18   # 因子计算延迟

    # 完整性类(bit 24-31)
    SOFT_FIELD_NULL    = 1 << 24   # 非关键字段空值
    LOW_TICKER_COUNT   = 1 << 25   # 当日 ticker 数偏低
    FACTOR_LOW_COVER   = 1 << 26   # 因子 ticker 覆盖率偏低
```

int64 有 63 个可用 bit，40 条规则中 ~17 条是 SOFT（需要 flag），绑定到 17 个 bit，远够用。bit 按四维分段（合法性 0-7 / 一致性 8-15 / 时序性 16-23 / 完整性 24-31），看 bit 段就知道问题类别。

**入库时怎么写**：

```python
def compute_quality_flag(df: pd.DataFrame, soft_results: list[CheckResult]) -> pd.Series:
    """为每行计算 quality_flag。"""
    flags = pd.Series(0, index=df.index, dtype="int64")

    for result in soft_results:
        if result.rule_id == "L04":
            mask = result.failing_rows        # boolean Series
            flags = flags | (mask.astype(int) * QualityFlag.PRICE_OUTLIER)
        elif result.rule_id == "K01":
            mask = result.failing_rows
            flags = flags | (mask.astype(int) * QualityFlag.OPEN_GAP)
        # ... 每条 SOFT 规则对应一个 bit

    return flags
```

**Parquet 存储时 `quality_flag` 作为一个 int64 列跟着数据走**（行级）：

```text
trade_date  | ticker   | open   | high | low | close | volume | adjfactor | suspend | ... | quality_flag
2024-01-02  | 600519.SH| 1800.5 | ...  | ... | ...   | ...    | ...       | False   | ... | 0
2024-01-02  | 300750.SZ| 95.2   | ...  | ... | ...   | ...    | ...       | False   | ... | 1     ← PRICE_OUTLIER
2024-01-02  | 000001.SZ| 10.1   | ...  | ... | ...   | ...    | ...       | False   | ... | 256   ← OPEN_GAP
```

### 37.5 下游怎么消费 quality_flag（回应子问题 3）

**默认行为：忽略（全量使用）**：

```python
# 大多数研究场景:不管 quality_flag,全量使用
df = data_api.get_bars(tickers, start, end)
# quality_flag 列存在但不过滤
```

**需要时：按需过滤**：

```python
# 保守研究:只用干净数据
df = data_api.get_bars(tickers, start, end)
df = df[df["quality_flag"] == 0]   # 只保留完全干净的行

# 过滤特定问题
df = df[~(df["quality_flag"] & QualityFlag.PRICE_OUTLIER).astype(bool)]   # 去掉价格异常
```

DuckDB 查询：

```sql
-- 只用干净数据
SELECT * FROM bars_daily WHERE quality_flag = 0;

-- 去掉价格异常
SELECT * FROM bars_daily WHERE quality_flag & 1 = 0;

-- 统计各类问题的行数
SELECT
  COUNT(*) as total,
  SUM(CASE WHEN quality_flag & 1   > 0 THEN 1 ELSE 0 END) as price_outlier,
  SUM(CASE WHEN quality_flag & 256 > 0 THEN 1 ELSE 0 END) as open_gap
FROM bars_daily
WHERE trade_date = '2024-01-02';
```

**DataAPI 的接口约定**（`get_bars` 新增一个参数）：

```python
class DataAPI:
    def get_bars(
        self,
        tickers, start, end,
        adjust='qfq',
        quality_filter: int = None,   # 新增:传 QualityFlag 问题位掩码,命中即过滤
    ) -> pd.DataFrame:
        df = self._read_parquet(tickers, start, end, adjust)
        if quality_filter is not None:
            df = df[df["quality_flag"] & ~quality_filter == df["quality_flag"]]
        return df
```

**默认不过滤。让研究者自己决定对数据质量的容忍度**——因子研究时可能需要所有数据（包括异常值，因为异常值本身可能是信号）；回测时可能需要更干净的数据。

### 37.6 批次级拒收安全阀（回应子问题 4：整批拒收的阈值）

除了行级升级到批次级（`escalation_threshold`），还有一个**全局安全阀**：

```python
@dataclass
class BatchRejectConfig:
    """批次级拒收的全局阈值"""

    # 任何 HARD 规则的 drop 行数占比超过此值 → 拒收整批
    max_hard_drop_rate: float = 0.20   # > 20% 行被 HARD 规则丢弃 → 整批有问题

    # SOFT 规则命中的行数占比超过此值 → 升级为告警(不拒收,但发通知)
    soft_alarm_rate: float = 0.50      # > 50% 行有 quality_flag → 需要人工检查
```

执行逻辑（`ingest_with_dqc` 七步，细化 §36.10 的 `ingest_daily_bars`）：

```python
def ingest_with_dqc(df, date, config):
    # 1. 跑所有规则
    hard_results, soft_results = run_all_checks(df, date)

    # 2. 行级处理: HARD drop
    rows_to_drop = union_of(r.failing_rows for r in hard_results if r.action == "drop")
    drop_rate = len(rows_to_drop) / len(df)

    # 3. 批次级安全阀
    if drop_rate > config.max_hard_drop_rate:
        raise DQCBatchReject(
            f"HARD drop rate {drop_rate:.1%} > {config.max_hard_drop_rate:.1%}, "
            f"rejecting entire batch for {date}"
        )

    # 4. 丢弃问题行
    df_clean = df.drop(rows_to_drop)

    # 5. 计算 quality_flag
    df_clean["quality_flag"] = compute_quality_flag(df_clean, soft_results)

    # 6. 软告警监控
    flagged_rate = (df_clean["quality_flag"] != 0).mean()
    if flagged_rate > config.soft_alarm_rate:
        log_alarm(f"SOFT flag rate {flagged_rate:.1%} for {date}, investigate")

    # 7. 入库
    write_parquet(df_clean, date)
    log_dqc_results(hard_results + soft_results, date)
```

**双保险**：单条规则的 `escalation_threshold`（规则级升级）+ `max_hard_drop_rate`（全局安全阀）。前者防"某一条规则大面积失败"，后者防"多条规则各失败一点、合计一大片"。

### 37.7 汇总与对已有设计的修订

| 决策 | 结论 |
|---|---|
| 边界是全局还是按资产分 | **按规则分**，每条规则有自己的 `row_action` + `escalation_threshold`，YAML 里配 |
| 硬拒粒度 | **三级**：行级 drop / 分区级拒收 / 批次级拒收（升级阈值触发） |
| `quality_flag` 编码 | **int64 位掩码**，~17 个 bit 对应 17 条 SOFT 规则，一列搞定，可叠加 |
| 下游消费 | **默认不过滤**（全量使用），需要时按位过滤；`get_bars` 加 `quality_filter` 参数 |
| 批次级安全阀 | HARD drop > 20% → 整批拒收；SOFT flag > 50% → 告警（不拒收） |
| 新增代码 | `QualityFlag` 类 ~30 行 + `compute_quality_flag()` ~40 行 + `BatchRejectConfig` ~10 行 ≈ **80 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§36.5** | HARD/SOFT 二值细化为 `row_action`（drop / flag / 批次级 / 分区级）+ `escalation_threshold`——§36 定"查什么"，本节定"怎么办" |
| **§36.10** | `ingest_daily_bars` 升级为 `ingest_with_dqc` 七步（加批次级安全阀 + quality_flag 计算 + 软告警监控） |
| **§36.11** | `dqc/` 代码量 790 → **~870 行**（+80） |
| **§4.2** | 数据表（bars_daily 等）新增**行级 `quality_flag` int64 列**，与数据同行——呼应 Part 0 已定的"元字段（数据版本、`quality_flag`、生成时间、数据源）"设想 |
| **§25 / DataAPI** | `get_bars()` 新增 `quality_filter: int = None` 参数（默认不过滤） |
| **§36.1 口径** | 验证方式第 1 条写"跑全部 40 条规则"，与四表 44 行的口径差异同 §36.1 注 |

**本节仍未解决**：`QualityFlag` 类列出 15 个命名 bit 但 SOFT 规则 17 条（L05/K09/T08 未单列 bit，`CROSS_SOURCE_DIFF` 对应哪条规则未明）——**bit↔规则映射表**要定稿；`quality_filter` 参数语义（注释写"传 `QualityFlag.CLEAN` 只取干净数据"，但 `CLEAN=0` 时按位运算恒不过滤——应是"传要排除的问题位"，注释与代码不一致，实施时定稿）；`row_action` 枚举里的 `"pass"` 何时使用（判定表中未出现）；C03 行级动作写"drop 该 ticker×date"（分区级语义）与三分法的标注对齐；各级 `escalation_threshold` 数值（1%/5%/10%/20%）的校准依据；`_dqc_results.parquet` 是否也要记录每次 batch reject / alarm 事件。

---

## 38. DQC 执行时机与性能 + 报告与可追溯（pre-load 一遍 + 双表落盘）

> 回应 §3.3 **五子问题全解**（pre-load/post-load/两遍、耗时预算、抽样策略、失败重试、并行度）+ §3.4 **五子问题全解**（结果存哪、报告字段、保留期、看板、告警通道与分级）。并细化 §36.10（执行模型）与 §37.6（`ingest_with_dqc` 伪代码 → 定稿签名）。**新增代码 ≈100 行**（`dqc/report.py` 扩展：`dashboard()` + 结果写入；`dqc/` 合计 **~970 行**）。

### 38.1 执行时机：Pre-load，一遍够了（回应 §3.3 子问题 1）

```text
数据源 → 下载到内存 DataFrame → DQC 检查 → 通过 → 写 Parquet
                                │
                                ├─ 不通过(HARD) → 拒收,不写盘
                                └─ 部分通过(SOFT) → 带 quality_flag 写盘
```

**为什么不是 post-load（入库后检查）？**

- 坏数据一旦入库就可能被下游消费（因子计算定时任务、研究 notebook）
- 事后发现再删/标记，中间已经产出了错误的因子或回测结果
- Parquet 是 immutable 文件，事后修改 = 重写整个文件，比入库前拦截更贵

**为什么不跑两遍？**

- 你是单机同步流水线：下载 → 检查 → 写盘。没有"入库后另一个进程再跑一遍"的场景
- 唯一需要"第二遍"的是跨资产检查（K06-K08，daily vs minute 对比）——但这不是同一批数据的两遍，是**两批数据都到齐后的跨表检查**，作为批量扫描单独执行（见 38.3）

**结论**：**pre-load 一遍**。这就是对 §36.10 "pre-load 还是 post-load"的最终回答——§36 已选 pre-load，本节给出三条否决 post-load 的理由并定稿。

### 38.2 耗时预算：DQC 不是瓶颈（回应 §3.3 子问题 2）

先算数据量：

```text
日频：  ~5000 ticker × 1 天 × ~15 列 = ~75K 行    → 内存 ~5 MB
分钟频：~5000 ticker × 240 bar × ~10 列 = ~1.2M 行 → 内存 ~100 MB
```

pandera 检查本质上是向量化的 pandas 操作（比较、聚合、mask）：

| 资产 | 数据量 | 44 条规则预估耗时 | 瓶颈 |
|---|---|---|---|
| bars_daily（单日） | 75K 行 | **< 1 秒** | 无 |
| bars_minute（单日） | 1.2M 行 | **2-5 秒** | 跨粒度检查 K06-K08 需要 join |
| fundamental（单日） | 5K 行 | **< 0.5 秒** | 无 |
| factor（单因子单日） | 5K 行 | **< 0.5 秒** | 无 |

**结论：DQC 不是瓶颈。** 即使分钟频数据，单日 DQC 耗时 < 5 秒。瓶颈是数据下载（网络 IO），不是检查（内存计算）。

如果将来存 L2 数据（单日 ~1.6GB、数千万行），那时候再考虑优化（列裁剪、抽样）。日频+分钟频完全不需要。

> **口径注**：来源表格列头写"40 条规则预估耗时"，与 §36.1 的 40/44 口径差异同源，以 44 条为准；耗时估算不变（多 4 条不影响数量级）。

### 38.3 抽样策略：增量，不抽样（回应 §3.3 子问题 3）

| 策略 | 用在哪 |
|---|---|
| **增量（只查新数据）** | **默认策略**。每次入库只检查当天新到的数据 |
| **全量扫描** | 定期批量。每周跑一次全量（T09 迟来订正检测、K06-K08 跨粒度） |
| 抽样 | **不用**。数据量不够大，没有抽样的必要。**抽样会漏检** |

```python
# 每日入库:增量
def daily_ingest(date):
    df = download_daily_bars(date)    # 只下载当天
    dqc_check(df, date)               # 只检查当天
    write_parquet(df, date)

# 每周批量:全量扫描特定规则
def weekly_scan(start, end):
    # 跨粒度一致性(需要 daily + minute 同时在)
    run_cross_checks(start, end)      # K06-K08
    # 迟来订正检测(对比新旧数据)
    run_late_correction_check(start, end)  # T09
```

### 38.4 失败重试：修复后重跑整批（回应 §3.3 子问题 4）

```text
入库失败(HARD 拒收)
        │
        ▼
查看 DQC 报告,定位问题
        ├─ 数据源的问题 → 等数据源修复 → 重新下载 → 重跑整批
        └─ DQC 规则太严 → 调整阈值(YAML) → 重跑整批
```

**始终重跑整批，不跑"失败部分"**。原因：

- 行级 drop 后的 `df_clean` 可能改变了批次统计（比如 ticker 覆盖率），部分重跑没意义
- 数据源修复通常是重新推送整天的数据
- 数据量小（日频 75K 行，分钟频 1.2M 行），重跑整批 < 5 秒，不值得优化

```python
# CLI
# python -m dqc.engine reingest --asset bars_daily --date 2024-01-02
def reingest(asset, date):
    df = download(asset, date)
    dqc_check(df, date)          # 全部规则重跑
    write_parquet(df, date)      # 覆盖旧文件
```

**注意与 §36.7 `revalidate` 的分工**：`reingest` = 重新下载 + 全规则重跑 + 覆盖写（修复数据用）；`revalidate` = 只重验已有数据不重下载（改了规则阈值后按需重验用）。

### 38.5 并行度：日常不需要（回应 §3.3 子问题 5）

单日 DQC < 5 秒，没有并行的必要。

如果做历史数据回填（backfill 5 年）：

```python
# 按日期并行,用 joblib 或 multiprocessing
from joblib import Parallel, delayed

dates = trading_calendar("2019-01-01", "2024-12-31")  # ~1200 天
Parallel(n_jobs=4)(delayed(daily_ingest)(d) for d in dates)
# 4 核并行,1200 天 × 5 秒 / 4 = ~25 分钟
```

**按日期并行比按 ticker 并行更自然**——每天的数据是一个独立批次，没有跨日依赖（除了 K05 adjfactor 连续性，在全量扫描中处理）。

### 38.6 报告存储：双表（回应 §3.4 子问题 1+2）

存储：`_dqc_results.parquet` + `_dqc_batch_summary.parquet`。两张表，一张行级明细，一张批次汇总。

**行级明细（append-only）**——按月分区（不按日，避免小文件）：

```text
data_platform/dqc/results/
  bars_daily/
    2024-01.parquet    # 按月分区(不按日,避免小文件)
    2024-02.parquet
  bars_minute/
    2024-01.parquet
  ...
```

行级明细 schema：

| 列 | 类型 | 说明 |
|---|---|---|
| run_date | date | DQC 执行日期 |
| run_time | timestamp | DQC 执行时间 |
| data_date | date | 被检查数据的 trade_date |
| asset_type | string | bars_daily / bars_minute / ... |
| rule_id | string | L01, K06, T03, ... |
| rule_name | string | price_positive, daily_vs_minute_close, ... |
| dimension | string | completeness / legality / consistency / temporality |
| severity | string | HARD / SOFT |
| action_taken | string | pass / drop / flag / reject_batch |
| total_rows | int | 本批总行数 |
| failing_rows | int | 触发该规则的行数 |
| failing_rate | float | failing_rows / total_rows |
| sample_tickers | string | 前 5 个失败 ticker（调试用，JSON 数组） |
| sample_values | string | 前 5 个失败值（调试用，JSON 数组） |
| rule_version | string | 规则文件版本（从 YAML version 字段） |

**批次汇总**——单文件，追加写入：

```text
data_platform/dqc/summary/
  batch_summary.parquet    # 单文件,追加写入
```

批次汇总 schema：

| 列 | 类型 | 说明 |
|---|---|---|
| run_time | timestamp | 执行时间 |
| data_date | date | 数据日期 |
| asset_type | string | 资产类型 |
| total_rows | int | 入库前总行数 |
| rows_dropped | int | HARD drop 行数 |
| rows_flagged | int | SOFT flag 行数 |
| rows_clean | int | 完全干净行数 |
| drop_rate | float | rows_dropped / total_rows |
| flag_rate | float | rows_flagged / total_rows |
| batch_status | string | accepted / rejected / accepted_with_warnings |
| hard_rules_failed | string | 触发 HARD 的规则 ID 列表（JSON） |
| soft_rules_failed | string | 触发 SOFT 的规则 ID 列表（JSON） |
| elapsed_seconds | float | DQC 耗时 |

**设计要点**：明细表带 `rule_version`（§36.7 可审计）；`action_taken` 取值直接对应 §37 的处置动作（pass/drop/flag/reject_batch）；失败样本只留前 5 个 ticker+值（调试够用，明细不膨胀）；`batch_status` 三态对应 §37.6 安全阀的三种结局。

### 38.7 保留期与清理（回应 §3.4 子问题 3）

| 表 | 保留期 | 理由 |
|---|---|---|
| 行级明细 | **1 年** | 超过 1 年的 DQC 明细没有诊断价值——如果数据有问题，早该发现了 |
| 批次汇总 | **永久** | 每天一行，数据量极小（1 年 ~250 行），保留全历史用于趋势分析 |

```python
# 每月跑一次,删除 1 年前的行级明细
def cleanup_dqc_results():
    cutoff = today() - timedelta(days=365)
    for f in glob("data_platform/dqc/results/*/*.parquet"):
        df = pd.read_parquet(f)
        df = df[df["run_date"] >= cutoff]
        df.to_parquet(f)  # 覆盖写
```

### 38.8 看板：一个 CLI 命令 + DuckDB 查询（回应 §3.4 子问题 4）

**不需要 Web 看板。单人平台，CLI 输出 + DuckDB 查询就够了。**

```python
# python -m dqc.report dashboard
def dashboard():
    """打印最近 7 天的 DQC 健康度摘要"""
    summary = duckdb.sql("""
        SELECT
            data_date,
            asset_type,
            batch_status,
            drop_rate,
            flag_rate,
            hard_rules_failed,
            elapsed_seconds
        FROM 'data_platform/dqc/summary/batch_summary.parquet'
        WHERE data_date >= current_date - INTERVAL '7 days'
        ORDER BY data_date DESC, asset_type
    """).df()

    print(summary.to_string(index=False))
```

输出：

```text
data_date   asset_type  batch_status             drop_rate  flag_rate  hard_failed  elapsed
2024-06-15  bars_daily  accepted                 0.000      0.012      []           0.8s
2024-06-15  bars_minute accepted_with_warnings   0.001      0.034      []           3.2s
2024-06-15  fundamental accepted                 0.000      0.008      []           0.3s
2024-06-14  bars_daily  accepted                 0.000      0.011      []           0.7s
2024-06-14  bars_minute rejected                 0.250      –          [C03,L01]    1.1s  ← 问题!
```

深入查看某天的失败明细：

```sql
SELECT rule_id, rule_name, severity, failing_rows, failing_rate, sample_tickers
FROM 'data_platform/dqc/results/bars_minute/2024-06.parquet'
WHERE data_date = '2024-06-14' AND action_taken != 'pass'
ORDER BY severity DESC, failing_rate DESC
```

### 38.9 告警：print + 日志，无外部通道（回应 §3.4 子问题 5）

| 告警级别 | 触发条件 | 输出方式 |
|---|---|---|
| ERROR | HARD 规则拒收批次 | `logging.error()` + 写 `dqc_alerts.log` + 入库函数 `raise` |
| WARNING | SOFT flag rate > 50% | `logging.warning()` + 写 `dqc_alerts.log` |
| INFO | 正常通过 | `logging.info()` |

**不接飞书/邮件/Slack。** 单人平台，你跑数据入库脚本时就在终端前面——看 log 输出比等推送更快。如果将来数据入库变成无人值守 cron job，再加一个飞书 webhook（一行 `requests.post`）。

### 38.10 ingest_with_dqc 定稿（细化 §37.6 的七步伪代码）

```python
import logging
logger = logging.getLogger("dqc")

def ingest_with_dqc(df, date, asset_type):
    results = run_all_checks(df, date, asset_type)

    if results.batch_rejected:
        logger.error(f"[DQC REJECT] {asset_type} {date}: {results.reject_reason}")
        raise DQCBatchReject(results.reject_reason)

    if results.flag_rate > 0.5:
        logger.warning(f"[DQC WARN] {asset_type} {date}: flag_rate={results.flag_rate:.1%}")
    else:
        logger.info(f"[DQC OK] {asset_type} {date}: "
                    f"drop={results.drop_rate:.1%} flag={results.flag_rate:.1%} "
                    f"elapsed={results.elapsed:.1f}s")

    write_parquet(results.clean_df, date)
    write_dqc_results(results, date, asset_type)
```

与 §37.6 伪代码的差异：签名定为 `(df, date, asset_type)`；批次判定收敛进 `results.batch_rejected` / `results.reject_reason`（含规则级 `escalation_threshold` 与全局 `max_hard_drop_rate` 双保险的结果）；告警分级（38.9）与结果写入（38.6）在此函数落地。§37.6 的七步语义不变。

### 38.11 汇总与对已有设计的修订

| 决策 | 结论 |
|---|---|
| 执行时机 | **Pre-load 一遍**（入库前检查）；跨资产检查作为周批量单独跑 |
| 性能瓶颈 | **不存在**（日频 <1s，分钟频 <5s），不需要优化；L2 届时再考虑 |
| 抽样策略 | **增量**（每次只查新数据），全量扫描每周一次（跨粒度 + 订正检测）；抽样不用 |
| 失败重试 | **重跑整批**（数据量小，不值得部分重跑）；`reingest` CLI |
| 并行度 | 日常不需要；回填时按日期并行（joblib，n_jobs=4） |
| 结果存储 | 行级明细（按月分区 Parquet）+ 批次汇总（单文件追加），schema 全字段定稿 |
| 明细保留期 | 明细 1 年；汇总永久；月度清理脚本 |
| 看板 | CLI 命令 + DuckDB 查询，**不建 Web 看板** |
| 告警通道 | Python logging（error/warning/info 三级），**不接外部推送**；未来 cron 无人值守时加飞书 webhook |
| 新增代码 | `dqc/report.py` 扩展 **~100 行**（`dashboard()` + 结果写入）；`dqc/` 合计 **~970 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.3** | 五子问题全解 → 本节（pre-load 一遍 / 耗时 <5s / 增量不抽样 / 重跑整批 / 回填按日期并行）——从"部分已定"升级为**已定** |
| **§3.4** | 五子问题全解 → 本节（双表存储 + 完整 schema + 明细 1 年/汇总永久 + CLI 看板 + logging 三级告警）——从"部分已定"升级为**已定** |
| **§36.10** | 执行模型"两时点"细化：每日增量 pre-load + 每周批量（K06-K08 + T09）；`ingest_daily_bars` → `ingest_with_dqc(df, date, asset_type)` 定稿签名 |
| **§36.7** | `revalidate`（只重验不重下载）与 `reingest`（重下载+全重跑+覆盖写）分工明确 |
| **§36.11** | `dqc/` 代码量 870 → **~970 行**（`report.py` 扩展 +100） |
| **§37.6 / §37.7** | 七步伪代码定稿为 38.10 签名；§37.7 遗留"`_dqc_results.parquet` 是否记录 batch reject / alarm 事件"→ **是**：明细表 `action_taken=reject_batch` + 汇总表 `batch_status`/`hard_rules_failed` 就是记录处 |
| **§3.5（部分暗示）** | pre-load 拒收 = **不写盘**（坏数据根本不落库），诊断靠 `sample_tickers`/`sample_values` + 重新下载重跑——隔离区可能退化为"不留副本、报告留样例"；但 §3.5 整节（存哪/生命周期/人工修复流程/隔离区看板）**仍开放** |

**本节仍未解决**：行级明细月分区文件的并发写入安全（append-only 但 `cleanup_dqc_results` 是覆盖写，与追加写同月冲突时的处理）；`run_all_checks` 返回的 `results` 对象 schema（`batch_rejected`/`reject_reason`/`flag_rate`/`drop_rate`/`elapsed`/`clean_df` 等字段清单）；`dqc_alerts.log` 的轮转与保留；K05 跨日依赖在按日期并行回填时如何避免竞态（提到"在全量扫描中处理"但未给细节）；飞书 webhook 的触发阈值（只说"将来加"）；L2 时代的 DQC 优化方案（列裁剪/抽样，明确 phase-2）。

---

## 39. 隔离区与 raw 层（quarantine + 原样落盘 + Normalizer 分层）

> 回应 §3.5 **四子问题全解**（存哪 / 生命周期 / 修复重跑 / 告警看板）+ §3.6 **五子问题全解**（"原样"含义 / 分区 / 格式 / 压缩与保留 / 转换逻辑放哪=Normalizer），并**修正 §38.11**（HARD 拒收 ≠ "不写盘"）。Normalizer 同时回应 §3.7 的 `normalize()` 归属。**新增代码 ≈310 行**（数据平台侧：quarantine 读写 ~60 + Normalizer 基类 ~50 + 每源 Normalizer ~100×2 源；不计入薄壳、不计入 `dqc/`）。

### 39.1 数据流三层（先画清楚）

```text
数据源(AKShare/Tushare/...)
        │
        ▼
┌──────────────┐
│ raw 层(§3.6) │   原样落盘,不做任何变换
└──────────────┘   "我收到了什么"的证据
        │
        │ Normalizer(每源一个)
        ▼
DataFrame(统一 schema)
        │
        │ DQC 检查
        ├─ 通过 ──────→ asset 层(Parquet,带 quality_flag)
        └─ HARD 拒收 ─→ quarantine 层(§3.5)
```

**先存 raw，再进流水线**——所以 DQC 拒收时原始证据还在 raw 层，quarantine 只是"待处理工作区"。

### 39.2 隔离区存哪：独立目录，和 raw/asset 平级（回应 §3.5 子问题 1）

```text
data_platform/
  raw/                  # 原样落盘(§3.6)
  asset/                # DQC 通过后的正式数据
    bars_daily/
    bars_minute/
    ...
  quarantine/           # DQC 拒收的数据
    bars_daily/
      2024-06-14.parquet          # 被拒收的原始 DataFrame
      2024-06-14.rejection.json   # 拒收原因
    bars_minute/
      2024-06-14.parquet
      2024-06-14.rejection.json
  dqc/                  # DQC 报告(§3.4)
```

**`rejection.json` 结构**（诊断信息一次给全）：

```json
{
  "data_date": "2024-06-14",
  "asset_type": "bars_minute",
  "reject_time": "2024-06-14T18:30:00",
  "batch_status": "rejected",
  "total_rows": 1200000,
  "rows_would_drop": 300000,
  "drop_rate": 0.25,
  "hard_rules_triggered": [
    {
      "rule_id": "C03",
      "rule_name": "minute_bar_count",
      "failing_rows": 180000,
      "failing_rate": 0.15,
      "sample_tickers": ["600519.SH", "000001.SZ", "300750.SZ"],
      "sample_detail": "expected 240 bars, got 180"
    },
    {
      "rule_id": "L01",
      "rule_name": "price_positive",
      "failing_rows": 12000,
      "failing_rate": 0.10,
      "sample_tickers": ["002371.SZ"],
      "sample_detail": "close = -1.0"
    }
  ]
}
```

**为什么不用独立数据库/独立表**：

- 独立数据库（SQLite/DuckDB）：隔离区数据是临时的、待修复的，建一个数据库太重
- asset 层的独立分区（如 `bars_daily/quarantine/`）：混在一起容易被误查
- **独立目录最简单**：一眼看到什么被拒了，修复后删掉就行

### 39.3 生命周期：T+30 天自动清理（回应 §3.5 子问题 2）

```text
拒收 → 进 quarantine/
        ├─ 7 天内: 你应该查看并处理(修复重跑 or 确认数据源问题)
        ├─ 7-30 天: 仍可回溯,但大概率已经不需要了
        └─ 30 天后: 自动清理
```

```python
def cleanup_quarantine(retention_days=30):
    cutoff = datetime.now() - timedelta(days=retention_days)
    for f in glob("data_platform/quarantine/**/*.parquet"):
        mtime = datetime.fromtimestamp(os.path.getmtime(f))
        if mtime < cutoff:
            os.remove(f)
            os.remove(f.replace(".parquet", ".rejection.json"))
```

**为什么不永久保留**：隔离区里的数据是"坏的"，修复后正确版本会进 asset 层。保留坏数据的唯一价值是事后诊断，30 天足够。真需要追溯更久的，有 raw 层（保留 1 年，见 39.8）。

### 39.4 修复重跑：三种路径（回应 §3.5 子问题 3）

```text
发现隔离区有数据
        ├─ 路径 A: 数据源的问题 → 等修复 → 重新下载 → 重跑入库
        │    python -m ingest --asset bars_minute --date 2024-06-14 --force
        ├─ 路径 B: 规则太严 → 调 YAML 阈值 → 从 quarantine 重跑 DQC
        │    python -m dqc.engine recheck --from-quarantine --date 2024-06-14
        └─ 路径 C: 确认数据源当天就是有问题 → 放弃,等下一天
             (quarantine 数据 30 天后自动清理)
```

**路径 B 的实现**：

```python
def recheck_from_quarantine(asset_type, date):
    """从隔离区重新跑 DQC(规则可能已更新)"""
    qpath = f"data_platform/quarantine/{asset_type}/{date}.parquet"
    df = pd.read_parquet(qpath)

    try:
        ingest_with_dqc(df, date, asset_type)
        # 通过了 → 已写入 asset 层 → 删除隔离区文件
        os.remove(qpath)
        os.remove(qpath.replace(".parquet", ".rejection.json"))
        logger.info(f"Quarantine resolved: {asset_type} {date}")
    except DQCBatchReject:
        logger.warning(f"Still failing after rule update: {asset_type} {date}")
```

**与 §38.4 的关系**：§38.4 的"重跑整批"是总原则（含路径 A 的 `reingest`）；路径 B 不重新下载、直接从 quarantine 取原始 df 重跑——省一次下载。三条路径覆盖"数据源问题 / 规则问题 / 放弃"三种诊断结论。

### 39.5 告警和看板：和 §3.4 共用（回应 §3.5 子问题 4）

**不单独建隔离区看板。** quarantine 的存在本身就是 DQC HARD reject 的结果——§3.4 的 `batch_summary` 表里 `batch_status = 'rejected'` 的行就是隔离区的索引。告警复用 §3.9 的 logging 三级通道（ERROR = HARD 拒收即 quarantine 新增）。

追加一个 CLI 命令看隔离区状态：

```python
# python -m dqc.report quarantine
def quarantine_status():
    files = glob("data_platform/quarantine/**/*.rejection.json")
    for f in sorted(files):
        info = json.load(open(f))
        age = (datetime.now() - datetime.fromisoformat(info["reject_time"])).days
        print(f"{info['data_date']}  {info['asset_type']:15s}  "
              f"drop_rate={info['drop_rate']:.1%}  "
              f"rules={[r['rule_id'] for r in info['hard_rules_triggered']]}  "
              f"age={age}d")
```

输出：

```text
2024-06-14  bars_minute     drop_rate=25.0%  rules=['C03', 'L01']  age=1d
2024-06-10  bars_daily      drop_rate=22.0%  rules=['K05']         age=5d
```

### 39.6 raw 层："原样"的含义（回应 §3.6 子问题 1）

**"原样"的意思：统一为 Parquet，但不改字段名/不改值。**

**不是保留源格式**（JSON/CSV/API 返回的 dict）。原因：

- AKShare 返回 DataFrame，Tushare 返回 DataFrame，但列名、类型可能不同
- 存 JSON/CSV 后续查不动（DuckDB 查 Parquet 比查 JSON 快 100x）
- "原样"的真正含义是：**数据源给了什么值就存什么值，不做清洗/转换/计算**

```text
"原样"   = Parquet 格式 + 数据源原始列名 + 原始值 + 原始类型
"不原样" = 你的统一 schema(改了列名、加了 adjfactor 计算、加了 tradability flags)
```

**具体对比**（回答"保留源格式还是归一到统一 schema"——**两层各干各的**）：

| 操作 | raw 层做吗 | asset 层做吗 |
|---|---|---|
| API 返回 → Parquet | **做（格式统一）** | – |
| 列名映射（trade_date → date） | 不做 | Normalizer 做 |
| 类型修正（string → float） | 不做 | Normalizer 做 |
| 复权因子计算 | 不做 | Normalizer 做 |
| tradability flags 合并 | 不做 | Normalizer 做 |
| DQC 检查 | 不做 | DQC 在 Normalizer 之后 |
| quality_flag | 不做 | DQC 产出 |

### 39.7 raw 层分区策略：按数据源 → 按资产类型 → 按日期（回应 §3.6 子问题 2）

```text
data_platform/
  raw/
    akshare/                        # 按数据源(第一级)
      bars_daily/                   # 按资产类型(第二级)
        2024-06-14.parquet          # 按日期(第三级,文件名)
        2024-06-15.parquet
      bars_minute/
        2024-06-14.parquet
      fundamental/
        2024-06-14.parquet
    tushare/
      bars_daily/
        2024-06-14.parquet
      ...
```

**为什么按数据源分第一级**：

- 不同数据源的列名/类型不同，不能混存
- 跨源对比时，你需要知道"这份数据来自哪个源"
- 如果切换数据源（AKShare → Tushare），旧源的 raw 数据保持完整

**为什么不按合约分区**：

- 日频/分钟频数据，一天一个文件（5000 ticker 在一个文件里）比 5000 个文件（每 ticker 一个）好得多——Parquet 列式存储 + 行组（row group）已经内置了高效过滤
- 按合约分区只有在单合约文件极大（>1GB）时才有意义——你的单日全市场日频数据 ~5MB

### 39.8 raw 层格式、压缩与保留（回应 §3.6 子问题 3+4）

**文件格式：Parquet + zstd**：

```python
def save_raw(df, source, asset_type, date):
    path = f"data_platform/raw/{source}/{asset_type}/{date}.parquet"
    df.to_parquet(path, compression="zstd", index=False)
```

**zstd 压缩**：比 snappy 多压 30-40%，解压速度接近。日频数据 ~5MB → ~1MB，分钟频 ~100MB → ~25MB。

**保留策略：raw 保留 1 年，asset 永久**：

| 层 | 保留期 | 理由 |
|---|---|---|
| raw | **1 年** | raw 的价值是"出了问题时回溯数据源给了什么"。1 年内的数据质量问题都该被发现了 |
| asset | **永久** | 回测/因子研究需要长历史 |
| quarantine | **30 天** | 临时诊断（39.3 已定） |

```python
def cleanup_raw(retention_days=365):
    cutoff = datetime.now() - timedelta(days=retention_days)
    for f in glob("data_platform/raw/**/*.parquet", recursive=True):
        # 从文件名解析日期
        date_str = Path(f).stem  # "2024-06-14"
        file_date = datetime.strptime(date_str, "%Y-%m-%d")
        if file_date < cutoff:
            os.remove(f)
```

**raw 空间估算**：

```text
日频：  ~1 MB/天 × 250 天 × 1 源 = ~250 MB/年
分钟频：~25 MB/天 × 250 天 × 1 源 = ~6 GB/年
总计：  ~6.5 GB/年 — 完全可接受
```

如果将来加 L2 数据（~500MB/天 压缩后）：

```text
L2：~500 MB/天 × 250 天 = ~125 GB/年
→ 可能需要缩短 raw 保留期到 3 个月,或只保留 asset 层
```

### 39.9 Normalizer：每源一个，只做格式转换（回应 §3.6 子问题 5 + §3.7 的 `normalize()`）

```text
data_platform/
  normalizers/
    akshare_normalizer.py
    tushare_normalizer.py
```

**Normalizer 的职责**（格式转换，不做 DQC）：

```python
class AKShareNormalizer:
    """AKShare raw → 平台统一 schema"""

    def normalize_daily_bars(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """
        AKShare 列名 → 平台列名
        日期字符串 → date 类型
        成交额单位转换(元 → 元,不变)
        合并 tradability flags(从另一个 API 拉取 suspend/valid 等)
        附加 adjfactor(从另一个 API 拉取)
        """
        df = raw_df.rename(columns={
            "日期": "trade_date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
        })
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
        df["ticker"] = df["股票代码"].apply(self._format_ticker)

        # 附加字段(从其他 API 拉取并 join)
        df = self._attach_adjfactor(df)
        df = self._attach_tradability_flags(df)

        return df[UNIFIED_DAILY_COLUMNS]  # 只保留统一 schema 的列
```

**Normalizer 不做 DQC。职责边界清晰**：

```text
raw → Normalizer(格式转换) → DataFrame(统一 schema) → DQC(质量检查) → asset
```

**"每源一个 Normalizer"就是 §3.6 子问题 5 的答案**：转换逻辑放 `data_platform/normalizers/{source}_normalizer.py`，每源 ~100 行（列名映射 + 类型修正 + 附加字段 join）。新增一个数据源 = 新增一个 Normalizer 类，不动 DQC、不动 asset 层。这也是 §3.7 `SourceAdapter.normalize()` 的落地形态（接口的其余方法——fetch/schedule/health_check/限流——仍开放）。

### 39.10 完整入库流水线（把三层串起来）

```python
def daily_ingest_pipeline(source, asset_type, date):
    """一天一次的完整入库流水线"""

    # 1. 下载
    raw_df = download(source, asset_type, date)

    # 2. 存 raw
    save_raw(raw_df, source, asset_type, date)

    # 3. Normalize
    normalizer = get_normalizer(source)
    df = normalizer.normalize(asset_type, raw_df)

    # 4. DQC
    try:
        ingest_with_dqc(df, date, asset_type)
    except DQCBatchReject as e:
        # 5a. 拒收 → quarantine
        save_quarantine(df, asset_type, date, e.rejection_detail)
        raise

    # 5b. 通过 → asset 层(已在 ingest_with_dqc 内写入)
    logger.info(f"Ingested {source}/{asset_type}/{date}")
```

**注意步骤顺序的两个细节**：raw 在 DQC **之前**落盘（保证证据链完整——被拒收的批次 raw 也有）；quarantine 存的是 **normalize 之后**的 df（方便按统一 schema 重跑 DQC），rejection.json 里存的是拒收诊断。

### 39.11 汇总与对已有设计的修订

| 决策 | 结论 |
|---|---|
| 隔离区位置 | `data_platform/quarantine/{asset_type}/{date}.parquet` + `.rejection.json`（独立目录，与 raw/asset 平级） |
| 隔离区保留期 | 30 天自动清理（7 天内应处理） |
| 修复方式 | 三条路径：重新下载重跑 / 调规则从 quarantine 重跑 / 放弃 |
| 隔离区看板 | 不单独建；`batch_status='rejected'` 即索引 + `quarantine` CLI |
| raw 层含义 | Parquet 格式 + 数据源原始列名和原始值，不做任何清洗（"原样"=值不改，格式统一为 Parquet） |
| raw 分区 | 数据源 → 资产类型 → 日期（文件名）；不按合约 |
| raw 格式 | Parquet + zstd 压缩 |
| raw 保留期 | 1 年（~6.5 GB/年，可接受；L2 届时缩短到 3 个月或只留 asset） |
| Normalizer | 每数据源一个，`data_platform/normalizers/{source}_normalizer.py` |
| Normalizer 不做 DQC | 格式转换和质量检查严格分离 |
| 新增代码 | quarantine 读写 ~60 行 + Normalizer 基类 ~50 行 + 每源 Normalizer ~100 行（2 源合计 **≈310 行**，数据平台侧） |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.5** | 四子问题全解 → 本节（quarantine 独立目录 / T+30 清理 / 三修复路径 / 与 §3.4 共用看板告警）——从无标记升级为**已定** |
| **§3.6** | 五子问题全解 → 本节（"原样"=Parquet+原始列名值类型 / 数据源→资产→日期分区 / Parquet+zstd / raw 1 年 / 每源 Normalizer）——从无标记升级为**已定** |
| **§38.11（修正）** | 原文写"pre-load 拒收 = 不写盘（坏数据根本不落库）"——**不准确**。正确语义：拒收 = **不进 asset 层**，但写入 **quarantine 层**留 30 天（`save_quarantine`），且 raw 层证据始终保留。§38.10 的 `ingest_with_dqc` 保持"不写 asset"不变，quarantine 写入由 39.10 流水线的 step 5a 负责 |
| **§38.4** | "重跑整批"原则下补路径 B（从 quarantine 重跑不重新下载）与 `recheck --from-quarantine` CLI |
| **§36.10** | 执行模型前置一步：入库流水线 = 下载 → **save_raw** → **Normalizer** → DQC → asset/quarantine（DQC 作用于统一 schema 之后） |
| **§3.7** | `normalize()` 方法落地为每源 Normalizer 类（~100 行/源）；`fetch()`/`schedule()`/`health_check()`/限流/重试等 SourceAdapter 其余问题仍开放 |
| **§4.x 存储布局** | `data_platform/` 五目录定稿：`raw/`（源→资产→日期，zstd）+ `asset/` + `quarantine/` + `dqc/` + `normalizers/` |

**本节仍未解决**：Normalizer 基类的统一接口签名（`normalize(asset_type, raw_df)` 分发到 `normalize_daily_bars`/`normalize_minute_bars`/… 的注册方式）；`UNIFIED_DAILY_COLUMNS` 的完整列清单（含 tradability flags 与 adjfactor 的最终 schema，呼应 §4.2 表 schema 仍开放）；`_attach_adjfactor`/`_attach_tradability_flags` 跨 API join 失败时的策略（缺失是丢行还是标 flag）；quarantine 的 `e.rejection_detail` 与 `rejection.json` 的字段是否完全一致；`cleanup_raw` 按文件名解析日期对非 `YYYY-MM-DD` 命名文件的兼容；多源同时入库时 raw/quarantine 的并发写入；raw 保留 1 年与 §38.7 明细保留 1 年的清理任务是否合并调度。

---

## 40. SourceAdapter 接口签名定稿（四方法 + watermark 增量 + 三层限流）

> 回应 §3.7 **五子问题全解**（接口方法 / 输入输出 schema / 增量表达 / 限流重试超时在哪层 / 新增数据源代码量），并把 §39.10 的 `download()` 具象为 `adapter.fetch()` + 补 `save_watermark`/`close()`。**新增代码 ≈420 行**（`data_platform/adapters/`：基类+装饰器 ~100 + 3 个 Adapter 子类 ~300 + 注册表 ~20；每新增一个源再 ~180-220 行）。

### 40.1 Context 与定位

数据入库流水线已经确定：数据源 → raw 层（原样 Parquet）→ Normalizer（统一 schema）→ DQC → asset 层。**SourceAdapter 是这条链最上游的抽象**——负责从外部数据源拉取数据（原始 df 由流水线的 `save_raw` 落盘，见 40.9 的分工）。需要统一接口定义，使新增数据源只需写一个 Adapter 子类。

**已知约束（必须在 Adapter 内部屏蔽，调用方不感知）**：

- AmazingData `get_option_basic_info` 单次最多 ~100 条
- `local_path` 必须传字符串（传 `Path` 会 `TypeError`）
- 连接数有限，未登出占用连接槽位（脚本结束必须 `logout()`）

已有架构决策（前几批已定）：

- raw 层分区：`raw/{source}/{asset_type}/{date}.parquet`，zstd 压缩（§39.7/§39.8）
- Normalizer：每源一个，`normalizers/{source}_normalizer.py`，列名映射/类型转换（§39.9）
- DQC：pre-load 执行，44 条规则，HARD/SOFT 双级（§36；来源材料此处写"40 条"，口径差同 §36.1，以 44 为准）
- 入库流水线：download → save_raw → normalize → dqc → asset（§39.10；本节把 download 具象为 Adapter）

### 40.2 抽象基类：4 个方法，够用不多（回应 §3.7 子问题 1）

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
import pandas as pd

@dataclass
class FetchResult:
    df: pd.DataFrame         # 数据源返回的原始 DataFrame
    source: str              # "akshare" / "tushare" / "amazingdata"
    asset_type: str          # "bars_daily" / "bars_minute" / "fundamental" / ...
    trade_date: date         # 业务日期
    row_count: int           # len(df),冗余但方便日志
    watermark: str | None    # 增量标记,fetch 下一次传回(见 40.4)
```

### 40.3 fetch() 的输入输出 schema（回应 §3.7 子问题 2）

```text
fetch(asset_type, trade_date, watermark?) -> FetchResult

输入:
  asset_type: str        枚举值,由 list_asset_types() 返回
  trade_date: date       业务日期(非下载日期)
  watermark: str | None  上次 fetch 返回的 watermark,格式由子类定义

输出:
  FetchResult.df         pd.DataFrame,列名/类型/值全部是数据源原样
  FetchResult.source     数据源标识符
  FetchResult.asset_type 传入的 asset_type 原样返回
  FetchResult.trade_date 传入的 trade_date 原样返回
  FetchResult.row_count  行数
  FetchResult.watermark  本次数据的增量标记(下次传回可增量拉取)
                         不支持增量的源返回 None
```

### 40.4 增量拉取：watermark 模式（回应 §3.7 子问题 3）

```python
# 全量(首次/日终批量)
result = adapter.fetch("bars_daily", date(2024, 6, 14))
save_raw(result.df, ...)
# result.watermark = "2024-06-14T18:30:00"   (最后一次更新时间)

# 增量(日内多次拉取分钟线)
result = adapter.fetch("bars_minute", date(2024, 6, 14),
                       watermark=last_watermark)
# 只返回 watermark 之后新增/更新的行
```

**watermark 格式由子类定义**——AKShare 可能是时间戳字符串，Tushare 可能是 offset，AmazingData 可能是页码。**调用方不解析 watermark，只负责存取**。

watermark 持久化：`data_platform/raw/{source}/.watermarks.json`

```json
{
  "bars_daily": {"2024-06-14": "2024-06-14T18:30:00"},
  "bars_minute": {"2024-06-14": "1718364600"}
}
```

### 40.5 限流/重试/超时：三层，Adapter 内部解决（回应 §3.7 子问题 4）

```text
┌─────────────────────────────────┐
│ 调用方(入库流水线)                │
│   adapter.fetch("bars_daily", d)│ ← 调用方只看到"成功/失败",不关心内部重试
├─────────────────────────────────┤
│ SourceAdapter 基类装饰器          │
│   @retry + @timeout + @rate_limit│ ← 通用策略,基类提供
├─────────────────────────────────┤
│ 子类 fetch() 实现                 │
│   分页 / 连接池 / API 特定限制    │ ← 源特定逻辑,子类处理
└─────────────────────────────────┘
```

基类提供的通用装饰器（在 `__init_subclass__` 或 mixin 中注入）：

```python
# source_adapter.py 基类中

# 重试:指数退避,3 次,只重试网络错误和 5xx
RETRY_CONFIG = dict(
    max_retries=3,
    backoff_base=2.0,        # 2s, 4s, 8s
    retry_on=(ConnectionError, TimeoutError, HTTPError),
)

# 超时: fetch 单次调用 120s
FETCH_TIMEOUT = 120

# 限流: 子类通过类属性声明
class SourceAdapter(ABC):
    requests_per_second: float = 5.0   # 默认 5 QPS,子类可覆盖

    def _do_fetch(self, asset_type, trade_date, watermark):
        """包装 fetch(),加重试/超时/限流"""
        @retry(**RETRY_CONFIG)
        @timeout(FETCH_TIMEOUT)
        @rate_limit(self.requests_per_second)
        def _inner():
            return self.fetch(asset_type, trade_date, watermark)
        return _inner()
```

子类只需声明自己的 QPS 限制：

```python
class AmazingDataAdapter(SourceAdapter):
    requests_per_second = 2.0   # AmazingData 连接数有限,保守限流
```

### 40.6 SourceAdapter 完整基类（含两个"没有"）

```python
class SourceAdapter(ABC):
    """一个数据源 = 一个子类"""

    @abstractmethod
    def fetch(self, asset_type: str, trade_date: date,
              watermark: str | None = None) -> FetchResult:
        """
        拉取一天的数据。
        - watermark=None: 全量拉取该天
        - watermark=上次返回的 watermark: 增量(仅限支持增量的源)
        返回 FetchResult,df 里是数据源原始列名/原始值。
        """
        ...

    @abstractmethod
    def list_asset_types(self) -> list[str]:
        """该源支持哪些 asset_type"""
        ...

    def health_check(self) -> bool:
        """探活。默认实现:拉一天数据看能否成功。子类可覆盖为更轻量的检查。"""
        try:
            today = self._latest_trade_date()
            result = self.fetch(self.list_asset_types()[0], today)
            return result.row_count > 0
        except Exception:
            return False

    def close(self):
        """释放连接。有连接池的源(如 AmazingData)覆盖此方法。"""
        pass
```

**没有 `normalize()`**：Normalizer 是独立模块（`normalizers/{source}_normalizer.py`），不属于 Adapter。Adapter 只管"拉到 raw"，Normalizer 管"raw → 统一 schema"。**职责分离**——这也终结了 §39.9 留的"`normalize()` 归属"问题。

**没有 `schedule()`**：调度是入库流水线的事，不是数据源适配器的事。**Adapter 是被调用方，不决定何时调用自己。**

### 40.7 AmazingData 约束的屏蔽（子类示例）

```python
class AmazingDataAdapter(SourceAdapter):
    """AmazingData 特有约束全部在此类内处理"""

    requests_per_second = 2.0
    PAGE_SIZE = 100              # API 单次最多 ~100 条

    def __init__(self, local_path: str):
        self._local_path = str(local_path)   # 必须字符串
        self._client = None                  # 延迟初始化

    def fetch(self, asset_type, trade_date, watermark=None):
        all_rows = []
        offset = 0
        while True:
            # 分页:每次最多 100 条
            batch = self._client.get_option_basic_info(
                date=trade_date.isoformat(),
                offset=offset,
                limit=self.PAGE_SIZE,
                local_path=self._local_path,   # 必须传字符串
            )
            if batch.empty:
                break
            all_rows.append(batch)
            offset += len(batch)
            if len(batch) < self.PAGE_SIZE:
                break

        df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
        return FetchResult(
            df=df, source="amazingdata", asset_type=asset_type,
            trade_date=trade_date, row_count=len(df), watermark=None,
        )

    def list_asset_types(self):
        return ["option_basic"]

    def close(self):
        if self._client:
            self._client.close()
            self._client = None
```

**调用方完全不知道分页 / local_path / 连接数限制。这些全部被 `AmazingDataAdapter.fetch()` 内部消化。**

> **与 CLAUDE.md 实际约束的对应**：真实 SDK 的分批是"按代码列表每批 10 个循环调用"（`get_option_basic_info` 每次最多 ~100-104 条返回），且 `is_local` 有先本地/增量联网两种语义，收尾必须 `ad.logout()` 释放连接槽。上例的 `offset/limit` 是幻灯片的示意分页——**实现时按真实 API 形态写循环（每批 10 代码），语义不变：分批/路径字符串/登出全在 Adapter 内部**，`close()` 落地为 `ad.logout()`。

### 40.8 新增一个数据源要写多少代码（回应 §3.7 子问题 5）

| 文件 | 行数 | 内容 |
|---|---|---|
| `adapters/{source}_adapter.py` | ~80-120 行 | 子类：`fetch()` + `list_asset_types()` + 源特有逻辑 |
| `normalizers/{source}_normalizer.py` | ~100 行 | 列名映射 + 类型转换（§3.6/§39.9 已定） |
| `dqc/rules/` | **0 行** | 复用现有 YAML，不需要改（**DQC 按 `asset_type` 而非按 `source`**） |
| 总计 | **~180-220 行** | |

步骤：

1. 继承 `SourceAdapter`，实现 `fetch()` 和 `list_asset_types()`
2. 如有 API 限制（分页/连接池），在 `fetch()` 内部处理
3. 如 QPS 限制不同于默认值，设 `requests_per_second` 类属性
4. 写对应的 Normalizer
5. 注册：在 `adapters/__init__.py` 的 `REGISTRY` 字典加一行

```python
# adapters/__init__.py
REGISTRY: dict[str, type[SourceAdapter]] = {
    "akshare": AKShareAdapter,
    "tushare": TushareAdapter,
    "amazingdata": AmazingDataAdapter,
}

def get_adapter(source: str, **kwargs) -> SourceAdapter:
    return REGISTRY[source](**kwargs)
```

### 40.9 完整入库流水线（更新 §39.10）

```python
def daily_ingest(source: str, asset_type: str, trade_date: date):
    adapter = get_adapter(source)
    try:
        # 1. fetch(内部已有重试/限流/分页)
        result = adapter.fetch(asset_type, trade_date)

        # 2. save raw(§3.6: Parquet + zstd,保留原始列名)
        save_raw(result.df, result.source, asset_type, trade_date)

        # 3. normalize(§3.6: 每源一个 Normalizer)
        normalizer = get_normalizer(source)
        df = normalizer.normalize(asset_type, result.df)

        # 4. DQC(§3.1-§3.4: pandera,HARD 阻断/SOFT 标记)
        ingest_with_dqc(df, trade_date, asset_type)

        # 5. 更新 watermark
        save_watermark(source, asset_type, trade_date, result.watermark)

    except DQCBatchReject as e:
        save_quarantine(df, asset_type, trade_date, e.detail)   # §3.5
        raise
    finally:
        adapter.close()
```

与 §39.10 的差异：`download()` 具象为 `get_adapter(source)` + `adapter.fetch()`（返回 `FetchResult`）；新增 **step 5 `save_watermark`**；`finally: adapter.close()` 保证连接释放（对应 AmazingData `logout()` 的连接槽要求）；拒收细节字段 `e.rejection_detail` → `e.detail`（命名以本节为准）。`normalizer.normalize(asset_type, result.df)` 签名在此定稿——§39.9 遗留的"统一接口签名"解决（分发到 `normalize_daily_bars`/`normalize_minute_bars` 的内部注册方式仍开放）。

### 40.10 文件结构与代码量

```text
data_platform/
  adapters/
    __init__.py            # REGISTRY + get_adapter()
    source_adapter.py      # 抽象基类 + FetchResult + 重试/限流装饰器
    akshare_adapter.py     # AKShare 实现
    tushare_adapter.py     # Tushare 实现
    amazingdata_adapter.py # AmazingData 实现(分页/local_path/连接池)
  normalizers/             # §3.6/§39 已定
  dqc/                     # §3.1-§3.4 已定
  raw/                     # §3.6/§39 已定
  asset/
  quarantine/              # §3.5/§39 已定
```

| 模块 | 行数 |
|---|---|
| `source_adapter.py`（基类 + FetchResult + 装饰器） | ~100 |
| 每个 Adapter 子类（×3） | ~100 × 3 = 300 |
| `__init__.py`（注册表） | ~20 |
| **总计** | **~420 行** |

### 40.11 汇总与对已有设计的修订

| 决策 | 结论 |
|---|---|
| 接口方法 | `fetch()` + `list_asset_types()` + `health_check()` + `close()`，共 4 个 |
| 没有 `normalize()` | Normalizer 是独立模块，不属于 Adapter |
| 没有 `schedule()` | 调度是流水线的事，Adapter 是被调用方 |
| 增量表达 | `watermark` 参数，格式由子类定义，调用方只做存取（`.watermarks.json`） |
| 限流/重试/超时 | 基类通用装饰器（3 次指数退避 / 120s 超时 / 可配 QPS）+ 子类声明 `requests_per_second` |
| API 限制屏蔽 | 分页/连接池/类型约束全部在子类 `fetch()` 内部处理，调用方不感知 |
| 新增数据源 | **~180-220 行**（Adapter ~100 + Normalizer ~100 + 注册 1 行；DQC 0 行） |
| 注册方式 | `REGISTRY` 字典，`get_adapter(source)` 工厂函数 |
| 新增代码（一次性） | `adapters/` 合计 **~420 行**（基类 ~100 + 3 子类 ~300 + 注册表 ~20） |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.7** | 五子问题全解 → 本节（4 方法无 normalize/schedule；FetchResult schema；watermark 增量；三层限流重试超时；新增源 ~180-220 行）——从"部分已定"升级为**已定** |
| **§39.9** | "接口的其余方法——fetch/schedule/health_check/限流——仍开放"全部闭合：`normalize()` 归 Normalizer（独立模块）、`schedule()` 不进 Adapter、`health_check()`/`close()` 有默认实现；`normalizer.normalize(asset_type, raw_df)` 签名定稿 |
| **§39.10** | `daily_ingest_pipeline` → `daily_ingest`：`download()` 具象为 `get_adapter()+fetch()`；加 step 5 `save_watermark`；`finally adapter.close()`（连接槽必须释放）；`e.rejection_detail` → `e.detail` |
| **§36.10** | 执行模型的"入库时"一步进一步具象：`fetch(含重试/限流/分页)` 在 Adapter 内，DQC 仍只见统一 schema 的 df |
| **CLAUDE.md 约束** | `get_option_basic_info` 分批、`local_path` 字符串、连接数/`logout()` 三条全部落进 `AmazingDataAdapter` 内部——平台层代码零感知（40.7） |
| **§4.x 存储布局** | `data_platform/adapters/` 定稿（+`.watermarks.json` 隐身在 `raw/{source}/` 下；注意 `cleanup_raw` 只删 `.parquet`，不误删 watermark 文件） |

**本节仍未解决**：重试/超时/限流三个装饰器的实现选型（`tenacity`/`backoff` 库 vs 手写 ~30 行）；`health_check()` 默认实现里 `_latest_trade_date()` 的来源（交易日历表还是墙钟）；watermark 写入失败（step 5）时是否回滚 asset 层数据（幂等重跑语义）；`FetchResult` 是否需要 `fetched_at`/`elapsed` 元字段（§0.1 元字段设想）；AKShare/Tushare 两个子类的具体 API 映射（每源的分页/增量形态）；`REGISTRY` 之外的插件式发现（entry_points）是否需要；`close()` 与流水线 `finally` 在并行回填（§38.5 joblib）下的连接池共享问题。

---

## 41. 多源冲突处理（主源指定 + 跨源校验 + 备源补数）

> 回应 §3.8 **五子问题全解**（主源指定表放哪 / 跨源对比容差与时点对齐 / 交叉校验频率 / 冲突时告警 vs 静默 / 备源是否参与补数），并把 §21.2 的"主源入库备源对照 + 强规则失败时 fallback 备源"原则落地为可执行代码。**新增代码 ≈230 行**（数据平台侧：`cross_check` ~150 + `fallback` ~50 + `source_priority.yaml` ~30；不计入薄壳、不计入 `dqc/`）。

### 41.1 Context 与核心原则

平台支持多个数据源（AKShare / Tushare / AmazingData），§3.7 已定义 SourceAdapter 统一接口，§3.6 raw 层按数据源分目录存储。当同一个 `asset_type` 有多个源时，需要解决：**谁是主源、怎么发现冲突、冲突后怎么办、主源缺失时备源能否补数**。

**核心原则：单源写入 asset 层，多源交叉校验。asset 层任何时刻只有一份数据（来自主源），备源只用于校验和补缺，不混合写入。**

这条原则保证了：下游查询永远不需要知道"这是哪个源的数据"（正常情况）；出问题时能精确回溯到具体源和具体批次。

### 41.2 主源指定表 source_priority.yaml（回应 §3.8 子问题 1）

```yaml
# data_platform/config/source_priority.yaml
# 每个 asset_type 的源优先级(第一个 = 主源)
# 字段级覆盖:特定字段从非主源取
bars_daily:
  priority: [tushare, akshare]        # tushare 主源,akshare 备源
  field_override:
    adjfactor: tushare                # adjfactor 只信 tushare(前复权因子质量更好)
bars_minute:
  priority: [tushare]                 # 只有一个源,无备源
fundamental:
  priority: [akshare, tushare]        # akshare 的基本面数据更全
  field_override:
    pe_ttm: tushare                   # tushare 的 PE_TTM 计算口径更准
option_basic:
  priority: [amazingdata]             # 期权只有 AmazingData
reference:
  calendar:
    priority: [tushare, akshare]
  industry:
    priority: [tushare]
```

**为什么用 YAML**：

- 变更频率极低（季度级）——源的质量、成本、稳定性不会天天变
- 可 git 追踪，每次主源切换都有 commit 记录，可审计可回滚
- 和 DQC 规则（§36.9）风格一致，都是声明式配置

**为什么不做自动主源选举**：

- 个人平台只有 2-3 个源，手动指定完全可控
- 自动切换在数据源出问题时反而危险——**你需要知道你在用谁的数据**。主源静默换人是数据事故的温床

**加载逻辑**：

```python
import yaml

def get_source_priority(asset_type: str, sub_type: str = None) -> list[str]:
    cfg = yaml.safe_load(open("data_platform/config/source_priority.yaml"))
    entry = cfg[asset_type]
    if sub_type and isinstance(entry, dict) and sub_type in entry:
        entry = entry[sub_type]
    return entry["priority"]

def get_primary_source(asset_type: str, sub_type: str = None) -> str:
    return get_source_priority(asset_type, sub_type)[0]
```

### 41.3 跨源对比：容差与对齐（回应 §3.8 子问题 2）

**对比只在 Normalizer 之后做**：

```text
tushare raw → TushareNormalizer → df_tushare (统一 schema)
akshare raw → AKShareNormalizer → df_akshare (统一 schema)
cross_source_check(df_tushare, df_akshare)
```

raw 层列名/类型不同，没法直接比；Normalize 后统一 schema 才有可比性。这也再次印证 §39.9 Normalizer 的价值。

**容差规则**（`source_priority.yaml` 追加）：

```yaml
cross_check:
  tolerances:
    price:   0.01      # OHLC: 绝对容差 0.01 元(tick size)
    volume:  0.001     # volume: 相对容差 0.1%(交易所汇总时点差异)
    amount:  0.001     # amount: 同上
    adjfactor: 1e-6    # adjfactor: 几乎必须精确一致
    pe_pb:   0.02      # PE/PB: 相对容差 2%(计算口径差异)
    default: 0.001     # 未指定字段的默认相对容差
```

**数据类与校验函数**：

```python
@dataclass
class CrossCheckResult:
    asset_type: str
    trade_date: date
    primary_source: str
    secondary_source: str
    total_rows: int
    matched_rows: int
    mismatched_fields: dict[str, MismatchDetail]  # field -> detail
    missing_in_primary: int    # 备源有但主源没有的 ticker 数
    missing_in_secondary: int  # 主源有但备源没有的 ticker 数

@dataclass
class MismatchDetail:
    field: str
    mismatch_count: int
    mismatch_rate: float
    max_abs_diff: float
    sample_tickers: list[str]  # 前 5 个不一致的 ticker

def cross_source_check(df_primary, df_secondary, asset_type, trade_date, tolerances) -> CrossCheckResult:
    merged = df_primary.merge(
        df_secondary, on=["ticker", "trade_date"],
        suffixes=("_pri", "_sec"), how="outer", indicator=True)
    mismatched = {}
    for field in NUMERIC_FIELDS[asset_type]:
        tol = tolerances.get(field, tolerances["default"])
        col_pri, col_sec = f"{field}_pri", f"{field}_sec"
        both = merged[merged["_merge"] == "both"]
        if field in ("price", "adjfactor"):   # 绝对容差
            diff = (both[col_pri] - both[col_sec]).abs()
            bad = diff > tol
        else:                                 # 相对容差
            base = both[col_pri].abs().clip(lower=1e-9)
            diff = ((both[col_pri] - both[col_sec]).abs() / base)
            bad = diff > tol
        if bad.any():
            mismatched[field] = MismatchDetail(
                field=field, mismatch_count=int(bad.sum()), mismatch_rate=bad.mean(),
                max_abs_diff=float((both[col_pri] - both[col_sec]).abs().max()),
                sample_tickers=list(both.loc[bad, "ticker"].head(5)))
    return CrossCheckResult(
        asset_type=asset_type, trade_date=trade_date,
        primary_source=get_primary_source(asset_type),
        secondary_source=...,  # 传入的备源标识
        total_rows=len(merged[merged["_merge"] == "both"]),
        matched_rows=len(merged[merged["_merge"] == "both"]) - sum(d.mismatch_count for d in mismatched.values()),
        mismatched_fields=mismatched,
        missing_in_primary=int((merged["_merge"] == "right_only").sum()),
        missing_in_secondary=int((merged["_merge"] == "left_only").sum()))
```

**时点对齐**：两个源拉数据的时刻可能不同（Tushare 18:00 更新，AKShare 18:30 更新）。**规则：两个源都完成当日入库后才做交叉校验。不在 fetch 中实时比。**

```text
18:00  tushare fetch → raw → normalize → DQC → asset    √
18:30  akshare fetch → raw → normalize             (不写 asset,只存 raw)
18:31  cross_source_check(asset/tushare, normalized/akshare)
```

### 41.4 交叉校验频率与触发 hook（回应 §3.8 子问题 3）

| 场景 | 频率 | 理由 |
|---|---|---|
| 日频数据（bars_daily, fundamental） | **每天**，主备源都入库后自动触发 | 数据量小（~5000 行），校验 <1s，早发现早处理 |
| 分钟频数据（bars_minute） | **每周抽样 1 天** | 单日 120 万行，每天全量对比太重；抽样够发现系统性偏差 |
| reference（calendar, industry） | **每次更新时** | 更新频率本身很低（日历月更，行业季更） |
| factor | **不做跨源校验** | 因子是自己算的，不存在"多源" |

触发方式：入库流水线末尾 hook：

```python
def daily_ingest(source, asset_type, trade_date):
    # ... fetch → raw → normalize → DQC → asset ...
    # 入库完成后,检查备源 raw 是否也已到达
    if should_cross_check(source, asset_type, trade_date):
        result = run_cross_check(asset_type, trade_date)
        save_cross_check_result(result)
        if result.mismatched_fields:
            alert_cross_check(result)

def should_cross_check(source, asset_type, trade_date):
    """只有主源入库完成 + 备源 raw 也到了,才触发"""
    if source != get_primary_source(asset_type):
        return False  # 备源入库不触发(它不写 asset)
    backup_sources = get_source_priority(asset_type)[1:]
    return all(raw_exists(s, asset_type, trade_date) for s in backup_sources)
```

### 41.5 冲突时：分级响应（回应 §3.8 子问题 4）

不是所有不一致都一样严重：

```text
mismatch_rate < 0.1%  →  静默记录(日志 + cross_check_results.parquet)
mismatch_rate 0.1%-1% →  告警(日志 WARNING + CLI 报告标黄)
mismatch_rate > 1%    →  告警升级(日志 ERROR + 入库流水线输出醒目提示)
任何 adjfactor 不一致 →  始终告警(adjfactor 影响所有复权计算)
```

```python
def alert_cross_check(result: CrossCheckResult):
    for field, detail in result.mismatched_fields.items():
        if field == "adjfactor" or detail.mismatch_rate > 0.001:
            level = "ERROR" if detail.mismatch_rate > 0.01 else "WARNING"
            logger.log(
                level,
                f"Cross-check {result.asset_type} {result.trade_date}: "
                f"{field} mismatch {detail.mismatch_rate:.2%} "
                f"({detail.mismatch_count} rows, max_diff={detail.max_abs_diff:.4f}), "
                f"samples: {detail.sample_tickers}"
            )
```

**结果持久化**：

```text
data_platform/
  cross_check/
    bars_daily/
      2024-06-14.json     # CrossCheckResult 序列化
    summary.parquet       # 所有日期的汇总(trade_date, asset_type, match_rate, worst_field)
```

**保留期**：和 DQC 报告（§38.7）一样——明细 1 年，汇总永久。

### 41.6 备源补数：主源缺失时的规则（回应 §3.8 子问题 5）

**允许补数，但严格标记**：

```text
主源正常       → asset 层写主源数据,source_tag = "tushare"
主源当日缺失   → 等待 2 小时
  —— 主源到了  → 正常流程
  —— 仍缺失   → 从备源补数,source_tag = "akshare:backfill"
```

```python
def ingest_with_fallback(asset_type: str, trade_date: date):
    primary = get_primary_source(asset_type)
    try:
        result = get_adapter(primary).fetch(asset_type, trade_date)
        if result.row_count == 0:
            raise EmptyResult(f"{primary} returned 0 rows")
        return ingest_pipeline(result, source_tag=primary)
    except (FetchError, EmptyResult, DQCBatchReject) as e:
        logger.warning(f"Primary source {primary} failed: {e}")
        for backup in get_source_priority(asset_type)[1:]:
            try:
                result = get_adapter(backup).fetch(asset_type, trade_date)
                if result.row_count == 0:
                    continue
                logger.warning(f"Backfilling {asset_type} {trade_date} from {backup}")
                return ingest_pipeline(result, source_tag=f"{backup}:backfill")
            except Exception:
                continue
        raise AllSourcesFailed(f"No source available for {asset_type} {trade_date}")
```

**source_tag 写入 asset 层 Parquet 的 metadata（不是数据列）**：

```python
def write_asset(df, asset_type, trade_date, source_tag):
    table = pa.Table.from_pandas(df)
    table = table.replace_schema_metadata({
        **table.schema.metadata,
        b"source_tag": source_tag.encode(),
        b"ingest_time": datetime.now().isoformat().encode(),
    })
    pq.write_table(table, path, compression="zstd")
```

**为什么用 metadata 不是数据列**：source_tag 是文件级属性不是行级——不需要每行存一遍；查数据时 schema 不受影响，审计时 `pq.read_metadata(path)` 即可。

**补数后主源恢复**：次日主源恢复 → 正常入库（覆盖昨天备源补的数据），source_tag 从 `"akshare:backfill"` 变回 `"tushare"`。

**不自动触发回填，手动决定**：

```bash
# 查看哪些天是补数的
python -m data_platform.audit backfill --asset bars_daily --start 2024-01-01
# 输出: 2024-06-14  akshare:backfill  (primary: tushare failed: timeout)

# 手动用主源重新入库
python -m data_platform.ingest --source tushare --asset bars_daily --date 2024-06-14 --force
```

### 41.7 决策汇总与修订

| 决策 | 结论 |
|---|---|
| 主源指定 | `source_priority.yaml`，每 asset_type 一个优先级列表 + 字段级覆盖 |
| 存放位置 | `data_platform/config/source_priority.yaml`，git 追踪 |
| 容差 | 价格绝对 0.01，量额相对 0.1%，adjfactor 近似精确（1e-6），PE/PB 相对 2% |
| 对齐方式 | 两源都入库后再比，不实时比；按 ticker+trade_date join |
| 校验频率 | 日频每天、分钟频每周抽 1 天、reference 每次更新时、factor 不做 |
| 冲突响应 | <0.1% 静默记录，0.1-1% WARNING，>1% ERROR，adjfactor 始终告警 |
| 结果存储 | `cross_check/{asset_type}/{date}.json` + `summary.parquet`（明细 1 年/汇总永久） |
| 备源补数 | 允许，`source_tag` 标记为 `{source}:backfill`，写入 Parquet metadata |
| 补数后恢复 | 不自动回填，手动 `--force` 覆盖 |
| 新增代码 | cross_check ~150 行 + fallback ~50 行 + source_priority.yaml ~30 行 ≈ **230 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.8** | 五子问题全解 → 本节（YAML 主源表 / 分字段容差+时点对齐 / 四类频率 / 四级响应 / 允许补数严格标记）——从"部分已定"升级为**已定** |
| **§21.2** | "主源入库备源对照 + 强规则失败时 fallback 备源"原则落地：`ingest_with_fallback` + `cross_source_check` 具体代码 |
| **§37.1（跨源不一致场景）** | "软 + 告警"细化为四级响应（0.1%/1% 阈值 + adjfactor 零容忍），与 `CROSS_SOURCE_DIFF`（bit 11）quality_flag 可并存 |
| **§40.9 / §39.10** | `daily_ingest` 末尾追加 `should_cross_check` hook；asset 写入升级为 `write_asset`（Parquet metadata 带 `source_tag`/`ingest_time`） |
| **§36.4 K06-K08** | 跨源校验（本节）与跨粒度校验（K06-K08 日线 vs 分钟）分工：前者比"同一数据两个源"，后者比"同一源两种粒度"；触发时机不同（本节每日 hook vs §38.3 每周批量） |
| **§4.x 存储布局** | `data_platform/` 新增 `config/source_priority.yaml` + `cross_check/` 目录（`bars_daily/{date}.json` + `summary.parquet`） |
| **CLAUDE.md 约束** | AmazingData 作为 `option_basic` 唯一源（`priority: [amazingdata]`），无备源补数路径；连接数约束仍由 `AmazingDataAdapter.close()` 屏蔽（§40.7） |

**本节仍未解决**：`NUMERIC_FIELDS[asset_type]` 的字段清单（与 §39.9 `UNIFIED_DAILY_COLUMNS` 联动定义）；`raw_exists()` 检查备源 raw 到达的实现（文件系统扫描 vs watermark 判定）；"等待 2 小时"的调度实现（定时重试 vs 消息队列）；分钟频"每周抽 1 天"的抽样日选择（固定周一 vs 随机）；`field_override` 的合并逻辑（主源整行 + 备源单字段替换的 join 细节）；`summary.parquet` 追加写与 §38.6 `batch_summary.parquet` 是否合并为一张汇总表；多备源时交叉校验是逐备源比还是备源间互比；`AllSourcesFailed` 后是否进 quarantine（当前逻辑是 raise，未落盘）。

---

## 42. 交易日历 / 时区 / 合约换月 / 夜盘日期归属

> 回应 §3.9 **四子问题全解**（日历来源 / 时区策略 / 连续合约构造 / 夜盘日期归属），并提供 `TradingCalendar` 工具类（§38.5 按日期并行回填、§41.6 等待主源 2 小时都依赖它）。**新增代码 ≈180 行**（`TradingCalendar` ~60 + `build_continuous` ~80 + 夜盘归属 `assign_trade_date_futures` ~40；数据平台侧，不计入薄壳）。

### 42.1 Context 与核心原则

数据入库流水线（§3.6–§3.8）和 DQC（§3.1–§3.4）多处依赖交易日历：完整性检查（C01 `row_count_vs_calendar`）、新鲜度检查（T01 `daily_freshness`）、分钟线校验（C03 `minute_bar_count`）。§2.1 事件模型中 `DAY_START`/`DAY_END` 也需要日历。此前日历来源、时区策略、期货连续合约构造均未定义。

**核心原则：A 股为主，期货为辅；简单优先，边界情况显式处理而非隐式猜测。**

### 42.2 交易日历来源：不自维护，从数据源拉取 + 交叉校验（回应子问题 1）

```text
Tushare trade_cal API            → 主源(覆盖 SSE/SZSE/CFFEX/SHFE/DCE/CZCE/INE)
AKShare tool_trade_date_hist_sina → 备源(仅 A 股)
```

**为什么不自维护**：

- 交易所每年公布节假日安排，但临时调休（如国务院调班）需要手动更新——容易漏
- Tushare/AKShare 已经替你跟踪了，用它们的数据再做交叉校验比自己维护可靠

**为什么不直接从交易所官网爬**：

- 各交易所发布格式不统一（SSE 是 HTML，CFFEX 是 PDF）
- Tushare 已经做了这层标准化，不值得重复

**存储与更新**：

```text
data_platform/
  asset/
    calendar/
      sse.parquet      # 上交所交易日(SSE)
      szse.parquet     # 深交所(SZSE),和 SSE 几乎一致但保留独立文件
      cffex.parquet    # 中金所
      shfe.parquet     # 上期所
      dce.parquet      # 大商所
      czce.parquet     # 郑商所
```

每个 Parquet 只有一列：

```text
# schema
trade_date: date    # 交易日(非交易日不存在于表中)
```

查询"某天是否交易日"：读 Parquet → 判断日期是否在集合中。DuckDB 做 semi-join 也行。

**更新频率**：每月初拉取一次（当年 + 次年）。DQC 规则 C07 已经要求"日历至少延伸到未来 30 个交易日"，月更足够。

```python
def update_calendar(exchange: str = "sse"):
    adapter = get_adapter("tushare")
    df = adapter.fetch("calendar", trade_date=date.today())
    # df 包含 exchange, trade_date, is_open 列
    cal = df[df["is_open"] == 1][["trade_date"]].copy()
    cal.to_parquet(f"data_platform/asset/calendar/{exchange}.parquet",
                   compression="zstd", index=False)
```

**TradingCalendar 工具类**：常驻内存（~200KB，5000 个 date 对象），整个平台共用一个实例，提供 `is_trading_day` / `prev` / `next` / `range` / `count`：

```python
class TradingCalendar:
    """加载后常驻内存,整个平台共用一个实例"""

    def __init__(self, exchange: str = "sse"):
        df = pd.read_parquet(f"data_platform/asset/calendar/{exchange}.parquet")
        self._dates: set[date] = set(df["trade_date"])
        self._sorted: list[date] = sorted(self._dates)

    def is_trading_day(self, d: date) -> bool:
        return d in self._dates

    def prev(self, d: date, n: int = 1) -> date:
        """前 n 个交易日"""
        idx = bisect.bisect_left(self._sorted, d) - n
        return self._sorted[max(0, idx)]

    def next(self, d: date, n: int = 1) -> date:
        """后 n 个交易日"""
        idx = bisect.bisect_right(self._sorted, d) + n - 1
        return self._sorted[min(idx, len(self._sorted) - 1)]

    def range(self, start: date, end: date) -> list[date]:
        """闭区间内的所有交易日"""
        i = bisect.bisect_left(self._sorted, start)
        j = bisect.bisect_right(self._sorted, end)
        return self._sorted[i:j]

    def count(self, start: date, end: date) -> int:
        return len(self.range(start, end))
```

内存占用：20 年 × 250 天 = 5000 个 date 对象 ≈ 200 KB，可以忽略。

### 42.3 时区处理：全部用交易所本地时间，不转 UTC（回应子问题 2）

**决策：全部用交易所本地时间，不转 UTC。**

| 方案 | 优点 | 缺点 |
|---|---|---|
| 全部 UTC | 全球统一，无歧义 | A 股 14:30 变成 06:30 UTC，读数据时永远需要心算；数据源返回的都是本地时间，转来转去容易出错 |
| **交易所本地时间** | **数据源原样**（AKShare/Tushare 都返回北京时间），分钟线 09:31 就是 09:31，直觉匹配 | 如果未来加美股需要处理跨时区 |

A 股/国内期货全部是 Asia/Shanghai，没有时区歧义。如果未来加美股，在 Normalizer 中加 `tz_localize` + 转换，不改现有 A 股数据。

**具体规则**：

```text
日频数据:  trade_date 列,类型 date,无时间部分,无时区
分钟频数据: trade_date (date) + time (time) 两列,或 datetime 列
            时间部分为交易所本地时间(北京时间)
            类型: datetime64[ns],无时区标记(naive)
            原因:pandas naive datetime 比 tz-aware 快 ~30%,
                 且平台只有一个时区
```

**不在 datetime 上标 tzinfo 的原因**：

- 所有数据都是 Asia/Shanghai，带不带 tz 信息不改变含义
- pandas tz-aware datetime 有性能开销（每次运算都检查时区对齐）
- 如果未来多时区，在 Parquet metadata 中标注 `timezone: Asia/Shanghai` 即可

```python
# Normalizer 中的时间处理
def normalize_datetime(raw_dt_col: pd.Series) -> pd.Series:
    """数据源返回什么就存什么,只做格式统一"""
    dt = pd.to_datetime(raw_dt_col)
    if dt.dt.tz is not None:
        dt = dt.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
    return dt
```

### 42.4 合约换月：期货连续合约（回应子问题 3）

**主力合约切换规则：成交量最大。**

每个交易日，某品种的"主力合约" = 该品种所有活跃合约中成交量最大的那个。这是国内市场最通用的定义（Tushare `fut_mapping` 接口也是这个口径）。

**不用持仓量的原因**：成交量反映当日流动性，持仓量有延迟（部分品种 T+1 公布）。两者 95% 的时间指向同一合约，分歧时成交量更适合回测（你想测的是"能交易"的合约）。

**连续合约构造**：

```python
@dataclass
class ContinuousContract:
    """一个品种的连续合约序列"""
    symbol: str                   # 品种代码,如 "IF", "RB"
    trade_dates: list[date]       # 交易日序列
    dominant_contracts: list[str] # 每日主力合约代码,如 "IF2406"
    close_raw: np.ndarray         # 未复权收盘价(主力合约的)
    close_adj: np.ndarray         # 复权后收盘价(用于回测)
    adjfactor: np.ndarray         # 累积复权因子

def build_continuous(symbol: str, start: date, end: date) -> ContinuousContract:
    """
    构造连续合约:
    1. 每天找成交量最大的合约 → dominant_contracts
    2. 换月日:新合约 close / 旧合约 close = 当日比价因子
    3. 累积比价因子 → adjfactor
    4. close_adj = close_raw × adjfactor (前复权)
    """
    cal = TradingCalendar("cffex")  # 或 shfe/dce/czce
    dates = cal.range(start, end)

    # 1. 每天的主力合约
    dominant = []
    for d in dates:
        contracts = get_active_contracts(symbol, d)  # 从 asset 层读
        best = max(contracts, key=lambda c: c.volume)
        dominant.append(best.contract_code)

    # 2. 复权因子(只在换月日产生)
    adj = np.ones(len(dates))
    for i in range(1, len(dates)):
        if dominant[i] != dominant[i-1]:
            # 换月:用新合约和旧合约在切换日的收盘价做比价
            old_close = get_close(dominant[i-1], dates[i])
            new_close = get_close(dominant[i], dates[i])
            if old_close > 0:
                ratio = new_close / old_close
            else:
                ratio = 1.0
            adj[i] = ratio

    # 3. 累积(前复权:从最新日往前累乘)
    cum_adj = np.cumprod(adj[::-1])[::-1]

    # 4. 复权价
    raw_close = np.array([get_close(dominant[i], dates[i]) for i in range(len(dates))])
    adj_close = raw_close * cum_adj / cum_adj[-1]  # 最新日 = 真实价

    return ContinuousContract(
        symbol=symbol, trade_dates=dates,
        dominant_contracts=dominant,
        close_raw=raw_close, close_adj=adj_close, adjfactor=cum_adj,
    )
```

**前复权 vs 后复权**：

- 前复权（最新价 = 真实价，历史价往下调）：回测友好，最新价直接可用
- 后复权（最早价 = 真实价）：收益率计算方便
- **选前复权**，和 A 股 adjfactor 口径一致（呼应 §3.10 / §27.3）

**存储**：

```text
data_platform/
  asset/
    continuous/
      IF.parquet     # 一个品种一个文件
      RB.parquet
      ...
```

schema：

```text
trade_date: date
dominant_contract: str    # "IF2406"
open/high/low/close: float64   # 未复权(主力合约原价)
volume: int64
amount: float64
adjfactor: float64        # 累积复权因子
close_adj: float64        # 前复权收盘价
```

**更新时机**：日终入库后，对每个活跃品种重新跑 `build_continuous`。由于前复权会改变历史值（adjfactor 重新累积），每次重建整个序列，不做增量——10 年 × 250 天 × 80 品种 = 20 万行，几秒搞定。

### 42.5 夜盘日期归属：算次日（回应子问题 4）

**规则：夜盘算次日。**

```text
2024-06-14 周五 21:00 ~ 2024-06-14 周五 23:00  → trade_date = 2024-06-17 (下周一)
2024-06-17 周一 21:00 ~ 2024-06-17 周一 23:30  → trade_date = 2024-06-18 (周二)
```

这是交易所的官方规则，不是我们定的。中金所/上期所/大商所/郑商所统一：夜盘属于下一个交易日。Tushare 返回的分钟线数据已经按此规则标了 `trade_date`。

**Normalizer 中的处理**：

```python
def assign_trade_date_futures(df: pd.DataFrame, cal: TradingCalendar) -> pd.DataFrame:
    """
    期货分钟线的 trade_date 归属
    - 日盘(09:00-15:00): trade_date = 自然日
    - 夜盘(21:00-23:30/01:00/02:30): trade_date = 下一个交易日
    """
    dt = df["datetime"]
    hour = dt.dt.hour

    is_night = (hour >= 21) | (hour < 3)  # 21:00~02:30 都算夜盘

    df = df.copy()
    df["trade_date"] = dt.dt.date

    # 夜盘:trade_date 改为下一个交易日
    night_mask = is_night
    if night_mask.any():
        night_dates = df.loc[night_mask, "trade_date"]
        df.loc[night_mask, "trade_date"] = night_dates.apply(
            lambda d: cal.next(d, 1)
        )

    return df
```

**边界情况**：

| 情况 | 处理 |
|---|---|
| 周五夜盘 → 下周一 | `cal.next()` 自动跳过周末 |
| 节前最后一个交易日夜盘 → 节后第一个交易日 | `cal.next()` 自动跳过节假日 |
| 夜盘跨午夜（如 SHFE 铜 21:00-01:00） | `hour < 3` 覆盖了凌晨部分 |
| 品种没有夜盘（如 CFFEX 股指期货） | 全部在 09:00-15:15，不会触发 `is_night` |

**如果数据源已经标好了 `trade_date`**（Tushare 的期货分钟线）：Normalizer 直接用数据源的 `trade_date`，不重新计算。只有数据源没标或标错时才用上述逻辑。

```python
def normalize_futures_minute(self, raw_df):
    if "trade_date" in raw_df.columns:
        # 数据源已标--信任它,但做 DQC 检查(T05 时序性)
        df = raw_df.copy()
    else:
        # 数据源没标--自己算
        df = assign_trade_date_futures(raw_df, self.calendar)
    return df
```

### 42.6 决策汇总与修订

| 决策 | 结论 |
|---|---|
| 日历来源 | Tushare `trade_cal`（主源）+ AKShare（备源交叉校验），**不自维护** |
| 日历存储 | `asset/calendar/{exchange}.parquet`，每月更新 |
| 日历工具 | `TradingCalendar` 类，常驻内存（~200KB），提供 `is_trading_day`/`prev`/`next`/`range`/`count` |
| 时区 | **全部交易所本地时间**（Asia/Shanghai），naive datetime，**不转 UTC** |
| 主力合约 | 每日**成交量最大**的合约（不用持仓量） |
| 连续合约 | **前复权**，换月日用收盘价比价，每日重建全序列（不做增量） |
| 连续合约存储 | `asset/continuous/{symbol}.parquet`，含 raw + adj + adjfactor |
| 夜盘归属 | **属于下一个交易日**（交易所官方规则），`cal.next()` 处理节假日跨越 |
| 新增代码 | TradingCalendar ~60 行 + continuous builder ~80 行 + 夜盘归属 ~40 行 ≈ **180 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.9** | 四子问题全解 → 本节（Tushare 主源+AKShare 备源 / 本地时间 naive datetime / 成交量最大+前复权连续合约 / 夜盘算次日）——从无标记升级为**已定** |
| **§38.5** | 按日期并行回填依赖的"交易日序列"落地为 `TradingCalendar.range()` |
| **§41.6** | "等待 2 小时"的时间轴和夜盘归属都需要日历；`cal.next()` 为夜盘跨节假日提供支撑 |
| **§36.2 C01/C07、§36.5 T01** | 日历表 schema 定稿（单列 `trade_date: date`，非交易日不存）；`update_calendar` 月更满足 C07 的"未来 30 个交易日"要求 |
| **§2.1 事件模型** | `DAY_START`/`DAY_END` 的日历来源落地：`TradingCalendar` 实例 |
| **§3.10 / §27.3** | 连续合约的"前复权 + 换月比价因子"与 A 股 adjfactor 口径统一（都用 `raw × factor / factor_latest`，见 §43） |
| **§4.x 存储布局** | `asset/` 新增两个子目录：`calendar/{exchange}.parquet`（6 交易所）+ `continuous/{symbol}.parquet`（每品种一文件） |

**本节仍未解决**：`get_active_contracts(symbol, d)` 的数据来源（futures 基础表从哪个源、何时入库）；连续合约的 OHLC 是否全部从主力合约取（而非拼接各段）；换月日是否用结算价而非收盘价比价；INE（能源中心）日历是否单列文件（决策表列了 6 个交易所，INE 未出现）；`TradingCalendar` 多交易所实例的缓存管理（每交易所一个实例 vs 合并）；夜盘 `hour < 3` 对 CFFEX 国债期货（无夜盘但有 09:30 开盘）的兼容；备源 AKShare 仅覆盖 A 股，期货交易所日历无备源校验时的降级策略。

---

## 43. 复权处理：asset 层口径与 adjfactor 语义（§3.10 第 1 部分）

> 回应 §3.10 **子问题 1 全解**（资产层存什么口径）+ **子问题 2 部分**（adjfactor 来源）；子问题 2 校验、子问题 3（研究层 alphalens 口径）、子问题 4（回测/实盘一致性）**仍开放**。与 §27.3 的 `qfq_close = raw_close × adjfactor / adjfactor_latest` 公式、§42.4 期货连续合约的换月比价因子口径统一。**新增代码 ≈20 行**（`get_minute_adj` + schema 声明；数据平台侧）。

### 43.1 Context 与核心原则

§2.5 已决定"asset 层存 raw price + cumulative adjfactor，用时现算复权价"。§42.4 期货连续合约也用了同样模式（换月比价因子）。现在需要把这个决策落到具体：**asset 层到底存哪些列、adjfactor 从哪来、分钟线怎么处理**。

**核心原则：存原始价 + 因子，不存复权价。** 复权价是派生量，存了就要维护（每次送转都得回刷历史），不存就永远不用回刷。

### 43.2 asset 层存什么（回应子问题 1）

**bars_daily 列定义**：

```text
bars_daily schema:
  ticker:     str        # "600519.SH"
  trade_date: date
  open:       float64    # 未复权(交易所原始价)
  high:       float64    # 未复权
  low:        float64    # 未复权
  close:      float64    # 未复权
  volume:     int64      # 股(不是手)
  amount:     float64    # 元
  adjfactor:  float64    # 累积前复权因子
  suspend:    bool
  ...
```

### 43.3 adjfactor 的含义与两种复权价

```text
前复权价 = 未复权价 × adjfactor / adjfactor_latest
后复权价 = 未复权价 × adjfactor
```

其中 `adjfactor_latest` = 该 ticker 最新交易日的 adjfactor。

Tushare 的 `adj_factor` 接口返回的就是这个累积因子（基准日为上市首日，因子从小到大递增）。

**为什么不直接存前复权价**：

- 前复权价依赖"最新日的 adjfactor"——每次有新的送转，所有历史前复权价都变
- 存了就要回刷整个历史（5000 ticker × 20 年）
- **存 raw + factor，用时算 `raw × factor / factor_latest`，零回刷**

**为什么不存后复权价**：

- 后复权数值膨胀（茅台后复权价 >10000），不直观
- 和实盘价格对不上（实盘看到的是未复权价）

### 43.4 分钟线不存 adjfactor

分钟线 **不存 adjfactor**（每根分钟线的 adjfactor 和当日日线一样，存 240 遍浪费空间）。

```text
bars_minute schema:
  ticker, trade_date, datetime
  open, high, low, close    # 未复权
  volume, amount
  (无 adjfactor 列)
```

分钟线复权时，从日线取当日的 adjfactor：

```python
def get_minute_adj(ticker: str, trade_date: date) -> float:
    daily = read_asset("bars_daily", trade_date)
    row = daily[daily["ticker"] == ticker]
    return row["adjfactor"].iloc[0]
```

### 43.5 汇总与对已有设计的修订

| 决策 | 结论 |
|---|---|
| 资产层口径 | **原始价 + 累积 adjfactor**，不存前/后复权价（复权价是派生量，零回刷） |
| bars_daily 列 | ticker / trade_date / OHLCV（未复权）/ volume（股）/ amount（元）/ adjfactor / suspend + … |
| adjfactor 含义 | 累积前复权因子（Tushare `adj_factor` 口径，基准日上市首日）；`qfq = raw × adj / adj_latest`，`hfq = raw × adj` |
| bars_minute | **无 adjfactor 列**；复权时 `get_minute_adj()` 从日线取当日因子 |
| 期货连续合约 | 同一模式：换月比价因子累积为 adjfactor（§42.4），口径一致 |
| 新增代码 | `get_minute_adj` + schema 声明 ≈ **20 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.10** | 子问题 1（资产层口径）→ 本节定稿（raw+factor 不存复权价 / 分钟线无 adjfactor 列）；子问题 2 的**来源**部分定稿（Tushare `adj_factor`）；**校验、研究层口径、回测/实盘一致性仍开放**——保持"部分已定" |
| **§27.3** | `qfq_close = raw_close × adjfactor / adjfactor_latest` 公式与本节完全一致，双向互证 |
| **§42.4** | 期货连续合约 `adjfactor`（换月比价累积）与 A 股 adjfactor 同一模式，回测层可用同一套复权函数 |
| **§4.x 存储布局 / §39.9** | `bars_minute` schema 确认**无 adjfactor 列**——`UNIFIED_DAILY_COLUMNS`（日线）含 adjfactor，分钟线 schema 需单独定义（此前提及"统一 schema"未区分粒度） |
| **§37.4 quality_flag** | `ADJFACTOR_EXTREME`（bit 1）/`ADJFACTOR_CHANGED`（bit 16）作用于日线 adjfactor 列；分钟线无此列，这两个 bit 对分钟线不适用 |

**本节仍未解决**：复权因子的校验（和 §3.11 数据回溯修正联动——adjfactor 被订正时 K05/T06 已有规则，但"因子来源本身错了"如何发现）；研究层 alphalens pricing 用 qfq 还是 raw（§27.3 已选 qfq，但 alphalens 输入接口的组装未定）；回测/实盘口径一致性保证（实盘用 raw 下单，回测用 qfq 算收益，两套口径的映射测试）；`adjfactor_latest` 的取值时点（每次读数据现算 vs 缓存）；分红现金的处理（adjfactor 只处理送转/配股，现金分红的收益率调整在哪层）；`UNIFIED_DAILY_COLUMNS` 完整清单与 `bars_minute` schema 的最终定稿（与 §39.9 遗留问题合并）。

---

## 44. 复权处理（续）：adjfactor 校验 + 研究层与回测实盘口径（§3.10 第 2 部分）

> 回应 §3.10 **子问题 2 全解**（adjfactor 来源与校验 + 公司行动日检测）+ **子问题 3**（研究层/alphalens/因子计算口径）+ **子问题 4**（回测与实盘口径一致性）。与 §43（asset 层口径）、§27.3（qfq 公式）、§42.4（期货换月比价因子）口径统一。**新增代码 ≈120 行**（PriceAccessor ~50 + corporate_actions 入库 ~40 + adjfactor 校验 ~30；数据平台侧）。

### 44.1 adjfactor 来源（回应子问题 2）

```text
Tushare adj_factor 接口(主源)
  → 返回 ticker, trade_date, adj_factor
  → 每日 T+1 更新(和日线同步)

AKShare 没有直接的 adjfactor 接口,但可以从前复权价反推:
  adjfactor_akshare = akshare_close_qfq / akshare_close_raw
  这个反推值作为交叉校验,不作为主源。
```

### 44.2 adjfactor 校验：3 条 DQC 规则 + 跨源校验（回应子问题 2）

| 规则 | 检查 | 严重度 |
|---|---|---|
| C06 adjfactor_present | 每个 ticker×date 都有非空 adjfactor | HARD |
| L06 adjfactor_positive | adjfactor > 0 | HARD |
| K05 adjfactor_stable | 非公司行动日，adjfactor = 前一日 adjfactor | HARD |

**追加一条跨源校验**（在 §3.8/§41 cross_check 中）：

```text
# adjfactor 跨源校验:容差 1e-6
# Tushare adjfactor vs AKShare 反推 adjfactor
# 任何不一致都告警(不分 mismatch_rate 大小)
```

这与 §41.5 的"任何 adjfactor 不一致 → 始终告警"完全一致。

### 44.3 公司行动日检测（K05 依赖）

K05 需要知道哪天有送转/配股/分红。来源：

```text
Tushare dividend 接口 → 除权除息日(ex_date)
存储: asset/reference/corporate_actions.parquet
```

```text
ticker:    str
ex_date:   date     # 除权除息日
event:     str      # "dividend" / "split" / "rights"
detail:    str      # "10送5派2" 等描述
```

**K05 逻辑**：

```python
def check_adjfactor_stable(df, prev_df, corporate_actions):
    merged = df.merge(prev_df, on="ticker", suffixes=("", "_prev"))
    # 排除有公司行动的 ticker×date
    ca_set = set(zip(corporate_actions["ticker"], corporate_actions["ex_date"]))
    merged["has_action"] = merged.apply(
        lambda r: (r["ticker"], r["trade_date"]) in ca_set, axis=1)
    # 非公司行动日,adjfactor 必须不变
    check = merged[~merged["has_action"]]
    bad = check[check["adjfactor"] != check["adjfactor_prev"]]
    return bad
```

### 44.4 研究层口径（回应子问题 3）

**alphalens 的 pricing：前复权收盘价。**

alphalens `get_clean_factor_and_forward_returns` 需要：

- `factor`：因子值（index = [date, ticker]）
- `prices`：价格序列（columns = ticker, index = date）

**prices 必须是复权价**——否则 forward returns 在送转日会出现虚假跳变。

```python
def get_pricing_for_alphalens(start: date, end: date) -> pd.DataFrame:
    """返回前复权收盘价矩阵,供 alphalens 使用"""
    df = read_asset_range("bars_daily", start, end)
    latest_adj = df.groupby("ticker")["adjfactor"].transform("last")
    df["close_adj"] = df["close"] * df["adjfactor"] / latest_adj
    return df.pivot(index="trade_date", columns="ticker", values="close_adj")
```

**为什么用前复权而不是后复权**：

- 前复权的最新价 = 真实价，直觉上好理解
- alphalens 只用价格算 return，前/后复权的 return 完全一致（比率相同）
- 但如果用后复权，历史价格数值可能非常大（茅台 >10000），在某些可视化场景不友好

**因子计算：用未复权价 + adjfactor 按需复权。**

因子计算中，不同因子对价格口径的需求不同：

| 因子类型 | 用什么价格 | 原因 |
|---|---|---|
| 动量/反转 | 前复权 close | 计算收益率，必须消除送转跳变 |
| 换手率 | 未复权 volume / 流通股本 | volume 本身不受复权影响 |
| 波动率 | 前复权 OHLC | 用未复权会在送转日引入虚假波动 |
| 均价 | 未复权 amount / volume | 成交均价不复权 |

提供工具函数，因子作者按需调用：

```python
class PriceAccessor:
    """因子计算中的价格访问器"""

    def __init__(self, daily_df: pd.DataFrame):
        self._df = daily_df

    def raw(self, field: str = "close") -> pd.Series:
        """未复权"""
        return self._df[field]

    def forward_adj(self, field: str = "close") -> pd.Series:
        """前复权"""
        latest = self._df.groupby("ticker")["adjfactor"].transform("last")
        return self._df[field] * self._df["adjfactor"] / latest

    def returns(self, n: int = 1) -> pd.Series:
        """n 日收益率(自动用前复权)"""
        adj_close = self.forward_adj("close")
        return adj_close.groupby(self._df["ticker"]).pct_change(n)
```

### 44.5 回测与实盘的口径一致性（回应子问题 4）

**问题**：

```text
回测: 用历史复权价计算信号 → 产生 order → 模拟撮合
实盘: 用实时未复权价计算信号 → 产生 order → 真实撮合

如果回测用前复权价(历史值会变),实盘用未复权价(当天价),
两边口径不同 → 信号可能不同 → 回测结果不可信。
```

**解决：统一用"截至当日的前复权"。**

**关键洞察：前复权价在"截至当日"这个视角下是稳定的。**

```text
2024-06-14 看到的 600519 前复权价序列:
  2024-06-13: 1800.00
  2024-06-14: 1820.00  (当日 = 真实价)

2024-06-15 看到的(如果 06-15 没有送转):
  2024-06-13: 1800.00  (不变)
  2024-06-14: 1820.00  (不变)
  2024-06-15: 1810.00

2024-07-01 看到的(假设 06-20 有 10送2):
  2024-06-13: 1500.00  (变了!被新的送转拉低了)
  2024-06-14: 1516.67  (变了!)
```

所以：

- **回测中**，每个 bar 看到的价格 = "截至该 bar 日期的前复权价"，不是"截至今天的前复权价"
- **实盘中**，当日价格 = 未复权 = 前复权（因为 `adjfactor_latest` = 当日 adjfactor）

```python
def point_in_time_adj_close(ticker, as_of_date, lookback_days=252):
    """
    截至 as_of_date 的前复权价序列
    模拟"你在 as_of_date 那天看到的价格"
    """
    df = read_asset("bars_daily",
                    start=as_of_date - timedelta(days=lookback_days * 2),
                    end=as_of_date, ticker=ticker)
    adj_latest = df[df["trade_date"] == as_of_date]["adjfactor"].iloc[0]
    df["close_adj"] = df["close"] * df["adjfactor"] / adj_latest
    return df[["trade_date", "close_adj"]]
```

**回测引擎中的实现**（backtrader feed）：

```python
class AdjustedDataFeed(bt.feeds.PandasData):
    """每个 bar 用截至当日的前复权价"""

    def _load(self):
        # 当前 bar 的 adjfactor
        adj_today = self._df.iloc[self._idx]["adjfactor"]
        # 历史所有 bar 重新复权(相对于今天)
        # 但 backtrader 只看当前 bar 的 OHLCV,不回看
        # 所以只需要当前 bar 的复权价
        row = self._df.iloc[self._idx]
        self.lines.close[0] = row["close"] * row["adjfactor"] / adj_today
        self.lines.open[0] = row["open"] * row["adjfactor"] / adj_today
        ...
```

**等一下**——backtrader 的 indicator 会回看历史（如 SMA(20) 需要 20 根 bar）。如果每根 bar 的"前复权基准"不同，indicator 看到的历史价格会不一致。

**实际上这不是问题**：backtrader 内部已经加载了完整数据序列。我们在加载时统一用"截至最后一天"的前复权价即可——因为回测是事后跑的，adjfactor 序列已经确定。

**真正需要 point-in-time 复权的场景**：因子计算中需要"T 日视角的 N 日收益率"。这时用 `PriceAccessor.returns()` 即可，它按 adjfactor 计算 pct_change，天然是 point-in-time 正确的：

```text
return_T = close_T × adj_T / (close_{T-1} × adj_{T-1}) - 1
```

**这个公式不依赖"基准日选哪天"，因为 adjfactor 做了比率，基准日消掉了。**

**口径一致性保证**：

| 场景 | 价格口径 | 保证一致的原因 |
|---|---|---|
| 因子计算 | `close × adj / adj_latest` | 收益率公式中 adj_latest 消掉 |
| alphalens | 前复权 close | 同上，forward return 只看比率 |
| backtrader 回测 | 前复权 close（截至末日） | 完整序列一次性复权，indicator 正确 |
| cs_portfolio_sim | 前复权 close（截至末日） | 同 backtrader |
| vnpy 实盘 | 未复权 close | 当日 adj_latest = adj_today，前复权 = 未复权 |
| 信号生成 | 因子值（不直接用价格） | 因子已经用正确口径算好 |

**唯一的陷阱**：如果策略代码里直接硬编码价格阈值（如 `if close > 100`），回测和实盘会不一致（回测用复权价，实盘用未复权价）。

**规避**：策略代码中禁止用价格绝对值做判断，只用收益率/排名/因子值等相对量。这是策略编写规范，不是平台能强制的——在文档中标红。

### 44.6 决策汇总与修订

| 决策 | 结论 |
|---|---|
| asset 层存储 | 未复权 OHLCV + cumulative adjfactor，不存复权价 |
| 分钟线 | 不存 adjfactor，用时从日线取 |
| adjfactor 来源 | Tushare `adj_factor` 接口（主源），AKShare 反推（校验） |
| 公司行动 | `corporate_actions.parquet`，用于 K05 规则和换月检测 |
| 研究层（alphalens） | 前复权 close = `close × adj / adj_latest` |
| 因子计算 | `PriceAccessor` 提供 raw / forward_adj / returns 三种口径 |
| 回测 | 前复权 close（截至末日一次性复权），indicator 正确 |
| 实盘 | 未复权 close = 前复权 close（当日 adj 相等） |
| 一致性保证 | 收益率公式中基准日消掉；策略禁用价格绝对值判断 |
| 新增代码 | PriceAccessor ~50 行 + corporate_actions 入库 ~40 行 + adjfactor 校验 ~30 行 ≈ **120 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.10** | 四子问题全解 → 本节 + §43（asset 层 raw+factor / adjfactor 来源与校验 / 研究层前复权+PriceAccessor / 回测实盘"截至当日前复权"统一口径）——从"部分已定"升级为**已定** |
| **§43.5 遗留** | "复权因子校验"→ 44.2 三条 DQC + 跨源校验定稿；"研究层 alphalens 口径"→ 44.4 前复权 + `get_pricing_for_alphalens` 定稿；"回测/实盘一致性"→ 44.5 "截至当日前复权"+ 六场景口径表定稿 |
| **§27.3** | `qfq_close = raw × adj / adj_latest` 公式在 alphalens/因子计算/回测三处全部落地；`returns` 公式 `close_T × adj_T / (close_{T-1} × adj_{T-1}) - 1` 证明基准日消掉 |
| **§36.4 K05** | `check_adjfactor_stable` 完整实现（依赖 `corporate_actions.parquet`）；与 §41.5 "adjfactor 始终告警"统一 |
| **§41.3 tolerances** | `adjfactor: 1e-6` 的用途明确：Tushare adj_factor vs AKShare 反推 adjfactor 的跨源校验 |
| **§42.4** | 期货连续合约的换月比价因子与 A 股 adjfactor 同一复权公式，回测层共用 `PriceAccessor` |
| **§4.x 存储布局** | `asset/reference/corporate_actions.parquet` 新增（ticker/ex_date/event/detail） |
| **§39.9 Normalizer** | `_attach_adjfactor`（§39.9）的数据来源定稿：Tushare `adj_factor` 接口 |

**本节仍未解决**：`corporate_actions.parquet` 的更新频率与历史深度（Tushare dividend 接口的分页/批量约束）；AKShare 反推 adjfactor 在送转日 qfq/raw 比值是否精确等于 Tushare adjfactor（口径微差的容差）；`AdjustedDataFeed` 对 backtrader 的 `PandasData` 接口适配（不再需要——§66.2 不用 backtrader/vnpy，2026-10-10 回改）；`PriceAccessor.returns(n)` 在 ticker 边界（上市不足 n 日）的 NaN 处理；现金分红的收益率调整（adjfactor 只管送转/配股，现金分红在 return 层还是价格层处理）；策略层"禁用价格绝对值判断"的 lint 规则能否自动化检查。

---

## 45. 数据回溯修正：检测 → 应用 → 影响传播（§3.11 全解）

> 回应 §3.11 **四子问题全解**（修正检测方式 / 修正应用方式 / 下游影响传播 / 历史回测要不要重跑），与 §36.5 T09（late_correction 检测）、§41（跨源校验）、§44（adjfactor 校验）衔接。**新增代码 ≈210 行**（detect ~80 + apply ~40 + impact ~60 + stale ~30；数据平台侧）。

### 45.1 Context 与核心原则

数据源会在事后修正已发布的数据。常见场景：

```text
- Tushare 日线数据 T+1~T+3 内微调收盘价/成交量(交易所勘误)
- adjfactor 在除权除息日后 1-2 天才稳定(分红金额确认有延迟)
- 基本面数据(PE/PB/市值)随财报修正而变化
```

DQC 规则 T09（`late_correction`）已定义"历史已入库数据被修改时标记告警"，但只管检测。这里定义完整流程：**检测 → 应用 → 影响传播**。

**核心原则：覆盖写入，不做版本化；记录修正日志供审计；下游不自动重跑。**

### 45.2 修正检测：每周全量重拉 + 逐行比对（回应子问题 1）

**方式：每周全量重拉 + 逐行比对。**

```text
每周日 02:00(非交易时段):
  对过去 30 个交易日的数据,从主源重新 fetch
  与 asset 层现有数据逐行比对
  差异 > 容差 → 记录为 correction
```

**为什么不用增量检测**（只看"数据源标记为修正"的行）：

- AKShare/Tushare 不提供修正标记——它们静默覆盖，你拿到的就是最新值
- 只有全量重拉 + 比对才能发现变化

**为什么 30 天而不是更长**：

- 交易所勘误 99% 在 T+3 内完成，财报修正在 T+30 内
- 30 天覆盖了绝大多数场景
- 更早的修正极罕见，季度全量扫描兜底（见 45.3）

```python
def detect_corrections(asset_type: str, lookback_days: int = 30):
    """检测近 N 个交易日的数据修正"""
    cal = TradingCalendar()
    end = cal.prev(date.today())
    start = cal.prev(end, lookback_days)
    dates = cal.range(start, end)

    source = get_primary_source(asset_type)
    adapter = get_adapter(source)
    normalizer = get_normalizer(source)
    corrections = []

    for d in dates:
        # 重新拉取
        result = adapter.fetch(asset_type, d)
        df_new = normalizer.normalize(asset_type, result.df)

        # 读 asset 层现有数据
        df_old = read_asset(asset_type, d)

        # 比对
        diff = compare_dataframes(df_old, df_new, asset_type)
        if diff:
            corrections.append(CorrectionRecord(
                asset_type=asset_type,
                trade_date=d,
                detected_at=datetime.now(),
                changes=diff,
            ))
    return corrections
```

**比对逻辑**：

```python
@dataclass
class FieldChange:
    ticker: str
    field: str
    old_value: float
    new_value: float
    abs_diff: float
    rel_diff: float

@dataclass
class CorrectionRecord:
    asset_type: str
    trade_date: date
    detected_at: datetime
    changes: list[FieldChange]

CORRECTION_TOLERANCES = {
    "close": 0.01,      # 1 分钱以内不算修正(浮点精度)
    "volume": 100,      # 100 股以内不算(汇总精度)
    "amount": 1000,     # 1000 元以内
    "adjfactor": 1e-6,  # adjfactor 几乎必须精确
}

def compare_dataframes(df_old, df_new, asset_type):
    merged = df_old.merge(df_new, on=["ticker", "trade_date"],
                          suffixes=("_old", "_new"))
    changes = []
    for field in NUMERIC_FIELDS[asset_type]:
        tol = CORRECTION_TOLERANCES.get(field, 0.001)
        old_col = merged[f"{field}_old"]
        new_col = merged[f"{field}_new"]
        diff = (old_col - new_col).abs()
        bad = diff > tol
        for _, row in merged[bad].iterrows():
            changes.append(FieldChange(
                ticker=row["ticker"], field=field,
                old_value=float(row[f"{field}_old"]),
                new_value=float(row[f"{field}_new"]),
                abs_diff=float(diff[row.name]),
                rel_diff=float(diff[row.name] / max(abs(row[f"{field}_old"]), 1e-9)),
            ))
    return changes
```

**扫描频率**：

| 扫描 | 范围 | 频率 | 目的 |
|---|---|---|---|
| 近期扫描 | 近 30 个交易日 | 每周 | 捕捉常规勘误 |
| 季度全量 | 近 1 年 | 每季度 | 兜底，捕捉延迟修正 |

### 45.3 修正应用：直接覆盖 + 修正日志（回应子问题 2）

**不做版本化。**

```text
方案 A: 版本化(每次修正保留旧版本)
  asset/bars_daily/2024-06-14.parquet       → v1
  asset/bars_daily/2024-06-14.v1.parquet    → 旧版本
  asset/bars_daily/2024-06-14.v2.parquet    → 修正后

方案 B: 直接覆盖 + 修正日志
  asset/bars_daily/2024-06-14.parquet       → 始终是最新版本
  corrections/bars_daily/2024-06-14.jsonl   → 修正记录
```

**选方案 B**。理由：

- 版本化带来存储膨胀（每个修正保留整个文件副本）
- 研究/回测永远应该用最新最准的数据——没有"我想用旧版错误数据"的场景
- 需要审计时，corrections 日志记录了什么变了、什么时候变的

**修正日志**：

```text
data_platform/
  corrections/
    bars_daily/
      2024-06-14.jsonl    # 该日数据的所有修正记录
```

每行一条修正：

```json
{"detected_at": "2024-06-16T02:15:00", "ticker": "600519.SH", "field": "close", "old": 1820.00, "new": 1820.50, "abs_diff": 0.50}
{"detected_at": "2024-06-16T02:15:00", "ticker": "600519.SH", "field": "volume", "old": 15000000, "new": 15020000, "abs_diff": 20000}
```

**应用流程**：

```python
def apply_corrections(corrections: list[CorrectionRecord]):
    for record in corrections:
        # 1. 写修正日志(先记录,再覆盖)
        log_path = f"data_platform/corrections/{record.asset_type}/{record.trade_date}.jsonl"
        with open(log_path, "a") as f:
            for c in record.changes:
                f.write(json.dumps({
                    "detected_at": record.detected_at.isoformat(),
                    "ticker": c.ticker,
                    "field": c.field,
                    "old": c.old_value,
                    "new": c.new_value,
                    "abs_diff": c.abs_diff,
                }) + "\n")

        # 2. 重新入库(走完整流水线:fetch → raw → normalize → DQC → asset)
        daily_ingest(
            source=get_primary_source(record.asset_type),
            asset_type=record.asset_type,
            trade_date=record.trade_date,
        )

        logger.warning(
            f"Correction applied: {record.asset_type} {record.trade_date}, "
            f"{len(record.changes)} fields changed"
        )
```

**修正走完整入库流水线，不是直接改 Parquet 文件**。这样：

- DQC 仍然会检查修正后的数据
- raw 层也会更新（新 raw 覆盖旧 raw）
- watermark 更新

### 45.4 下游影响传播（回应子问题 3）

**影响链**：

```text
asset 层数据修正
  ├── 因子值可能变(用了被修正的价格/量)
  │     └── alphalens 评估结果可能变
  ├── 回测结果可能变(用了被修正的价格)
  └── 连续合约复权因子可能变(如果修正了期货收盘价)
        └── 期货回测结果可能变
```

**决策：记录影响范围，不自动重跑。**

```text
修正发生
  ├── 1. 记录哪些因子可能受影响
  │       → corrections/impact_log.jsonl
  ├── 2. 标记受影响的因子为 stale
  │       → factor_registry.yaml 中加 stale_since 字段
  └── 3. 等你主动决定是否重算
          → python -m research.factor recompute --stale
```

**为什么不自动重跑**：

- 因子计算可能很慢（特别是用了分钟线的因子）
- 大多数修正很小（close 差 0.5 元），对因子值影响微乎其微
- 自动重跑容易触发级联（修正 → 因子重算 → 回测重跑 → …）
- 个人平台，人工判断"这个修正是不是值得重算"更合理

**影响评估**：

```python
def assess_correction_impact(record: CorrectionRecord):
    """评估修正对下游的影响,写入 impact log"""
    impact = {
        "trade_date": str(record.trade_date),
        "asset_type": record.asset_type,
        "num_changes": len(record.changes),
        "max_rel_diff": max(c.rel_diff for c in record.changes),
        "affected_tickers": list(set(c.ticker for c in record.changes)),
    }

    # adjfactor 修正影响最大:所有用复权价的因子都受影响
    adj_changes = [c for c in record.changes if c.field == "adjfactor"]
    if adj_changes:
        impact["severity"] = "HIGH"
        impact["reason"] = "adjfactor changed, all price-based factors affected"
    elif impact["max_rel_diff"] > 0.01:
        impact["severity"] = "MEDIUM"
        impact["reason"] = f"price change > 1% for {len(record.changes)} fields"
    else:
        impact["severity"] = "LOW"
        impact["reason"] = "minor correction, likely negligible"

    with open("data_platform/corrections/impact_log.jsonl", "a") as f:
        f.write(json.dumps(impact) + "\n")

    if impact["severity"] in ("HIGH", "MEDIUM"):
        logger.warning(f"Correction impact {impact['severity']}: {impact['reason']}")

    return impact
```

**因子 stale 标记**：

```python
def mark_factors_stale(record: CorrectionRecord):
    """标记可能受修正影响的因子"""
    registry = yaml.safe_load(open("data_platform/research/factor_registry.yaml"))
    affected_tickers = set(c.ticker for c in record.changes)

    for name, meta in registry["factors"].items():
        # 如果因子用了被修正的 asset_type 的数据
        if record.asset_type in meta.get("depends_on", []):
            meta["stale_since"] = str(record.trade_date)
            logger.info(f"Factor '{name}' marked stale since {record.trade_date}")

    yaml.dump(registry, open("data_platform/research/factor_registry.yaml", "w"))
```

**研究时检查**：

```python
def load_factor(name: str):
    meta = get_factor_meta(name)
    if "stale_since" in meta:
        logger.warning(
            f"Factor '{name}' may be stale since {meta['stale_since']}. "
            f"Run: python -m research.factor recompute --name {name}"
        )
    return pd.read_parquet(f"data_platform/research/factors/{name}/values.parquet")
```

### 45.5 历史回测要不要重跑（回应子问题 4）

**答案：不自动重跑，按严重程度人工决定。**

| 修正严重程度 | 建议动作 |
|---|---|
| LOW（价格差 <1%，非 adjfactor） | 忽略。回测结果几乎不变 |
| MEDIUM（价格差 >1%，少数 ticker） | 检查这些 ticker 是否在你的 universe 中。如果是，考虑重跑受影响的因子 |
| HIGH（adjfactor 变了） | 重算所有用复权价的因子。如果最近刚做过重要的策略决策（上线/下线），重跑回测确认 |

**CLI 工具**：

```bash
# 查看近期修正
python -m data_platform.corrections report --days 30
# 输出:
# 2024-06-16  bars_daily  2024-06-14  3 changes  max_rel=0.03%  severity=LOW
# 2024-06-20  bars_daily  2024-06-18  1 change   max_rel=2.1%   severity=MEDIUM (adjfactor)

# 查看 stale 因子
python -m research.factor list --stale
# 输出:
# momentum_20d    stale_since=2024-06-18  depends_on=[bars_daily]
# volatility_60d  stale_since=2024-06-18  depends_on=[bars_daily]

# 重算 stale 因子
python -m research.factor recompute --stale

# 清除 stale 标记(重算完后)
python -m research.factor clear-stale
```

### 45.6 完整修正流程图

```text
每周日 02:00
  │
  ▼
detect_corrections(lookback=30)
  ├── 无修正 → 结束
  └── 有修正
        ├── 1. 写 corrections/{asset}/{date}.jsonl(修正日志)
        ├── 2. apply: 重新入库(完整流水线)
        ├── 3. assess_impact → impact_log.jsonl
        ├── 4. mark_factors_stale(factor_registry.yaml)
        └── 5. 日志告警(severity HIGH/MEDIUM 才告警)
              │
              ▼
        你看到告警后决定:
          ├── LOW → 忽略
          ├── MEDIUM → 可能重算因子
          └── HIGH → 重算因子 + 可能重跑回测
```

**文件结构（新增）**：

```text
data_platform/
  corrections/
    bars_daily/
      2024-06-14.jsonl        # 某日数据的修正记录
    bars_minute/
      ...
    impact_log.jsonl          # 影响评估汇总
scripts/
  detect_corrections.py       # 修正检测(每周 cron)
```

### 45.7 决策汇总与修订

| 决策 | 结论 |
|---|---|
| 检测方式 | 全量重拉 + 逐行比对，不依赖数据源修正标记 |
| 检测频率 | 近 30 天每周扫，全年每季度扫 |
| 应用方式 | 直接覆盖 asset 层，不做版本化 |
| 审计 | `corrections/{asset}/{date}.jsonl` 记录所有变更 |
| 下游传播 | 不自动重跑；记录影响 + 标记因子 stale + 人工决定 |
| 影响分级 | LOW(<1%) / MEDIUM(>1%) / HIGH(adjfactor)，只有 MEDIUM+ 告警 |
| 回测重跑 | 不自动；HIGH 时建议手动重跑确认 |
| 新增代码 | detect ~80 + apply ~40 + impact ~60 + stale ~30 ≈ **210 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§3.11** | 四子问题全解 → 本节（全量重拉比对 / 覆盖+日志 / stale 标记不自动重跑 / 按 severity 人工决定）——从"部分已定"升级为**已定** |
| **§36.5 T09** | `late_correction` 检测规则的后续动作落地：`detect_corrections` + `apply_corrections` 完整流水线 |
| **§44.2 K05** | adjfactor 修正为 HIGH severity——K05 在修正场景的联动（adjfactor 变了 → 所有复权价因子 stale） |
| **§41.6** | 修正应用走 `daily_ingest` 完整流水线——含 `source_tag` 更新（修正后的数据 source_tag 回到主源） |
| **§42.4** | 期货收盘价修正 → 连续合约复权因子可能变 → `build_continuous` 需重建（影响链第三支） |
| **§4.x 存储布局** | `data_platform/corrections/` 新增（`{asset}/{date}.jsonl` + `impact_log.jsonl`）+ `scripts/detect_corrections.py` |
| **factor_registry.yaml** | schema 追加 `stale_since` 字段 + `depends_on` 列表（用于 stale 标记） |

**本节仍未解决**：`NUMERIC_FIELDS[asset_type]` 字段清单（与 §39.9/§41 联动，多处提及未统一定义）；季度全量扫描（近 1 年）的具体实现（是否复用 `detect_corrections(lookback=250)`）；`corrections/{asset}/{date}.jsonl` 追加写 vs 覆盖写（`apply_corrections` 用 `"a"` 追加——同一天多次修正时日志累积）；`factor_registry.yaml` 的 `depends_on` 字段此前从未定义，其取值枚举（`bars_daily`/`bars_minute`/`fundamental`/`continuous`）待定；`recompute --stale` 是全量重算还是增量（修正影响的日期范围）；`impact_log.jsonl` 的保留期与查询接口（CLI 只有 `report`，没有 `impact` 子命令）；修正后 `source_tag` 是否标注 `correction` 标记（当前 `daily_ingest` 无此参数）。

---

## 46. 三套存储职责边界补全：两层定稿 + DataAPI + 通用 schema 约定（§4.1 续）

> 补全 §4.1（前次 §20.1 只定了"砍 ClickHouse + DuckDB 三 schema 逻辑分层"的方向），本节落地**两层存储职责分工、Parquet/DuckDB 各自的职责表、DataAPI 完整实现、原子写入、通用 schema 约定**。§4.2 表 schema 的公共前置（通用约定）也在此定稿。**新增代码 ≈110 行**（DataAPI ~80 + 视图注册 ~30；数据平台侧）。

### 46.1 Context 与关键事实

之前的讨论提到"Parquet raw + DuckDB 研究 + ClickHouse 资产"三层存储，但这是从公司 UdataViewer 架构借鉴来的。公司有 OT Udata（Parquet）+ Ucube（tensor）+ DuckDB 查询层，规模是全市场 L2 行情。个人平台的数据量小 3-4 个数量级，需要重新审视是否真的需要三套。

**关键事实**：

```text
- 日频全市场: ~5000 行/天, ~1.25M 行/年, ~50MB Parquet/年
- 分钟频全市场: ~1.2M 行/天, ~300M 行/年, ~6GB Parquet/年
- 因子: ~5000 行/天/因子, 几十个因子
- 没有多用户并发查询,没有低延迟在线服务需求
- §3.6 已决定 raw 层用 Parquet + zstd,asset 层也用 Parquet
```

### 46.2 决策：砍掉 ClickHouse，只用 Parquet + DuckDB

| 维度 | ClickHouse | 你的场景 |
|---|---|---|
| 擅长 | 10 亿+行 OLAP、多用户并发、实时写入+秒级查询 | 你最大的表（分钟频）全年 3 亿行 |
| 部署 | 至少 1 个常驻进程（~2GB 内存），需要配置用户/库/表/TTL | 个人机器，能少跑一个进程就少跑一个 |
| 写入 | 需要 ETL 导入（INSERT / clickhouse-client） | 你已经有 Parquet 了，再导一份 = 双倍存储 |
| 查询 | 分布式查询、物化视图、近似聚合 | 你的查询是单机 `SELECT * WHERE date=X AND ticker IN (...)` |

DuckDB 直接查 Parquet 已经能覆盖你所有的查询场景。在 3 亿行（分钟频全年）的规模上，DuckDB 查询时间在秒级——和 ClickHouse 在这个量级下差距可忽略。

### 46.3 预留 ClickHouse 接口：不需要

"预留接口"意味着你要设计一个 storage abstraction layer，然后写 Parquet backend 和 ClickHouse backend。这引入了不必要的抽象——你现在只有一个 backend，抽象层是纯开销。

如果将来真需要 ClickHouse（比如加了 L2 tick 数据，单日 500M 行）：

- 到那时再加，改动集中在 DataAPI 一层（§1.4 的 thin shell DataAPI）
- 查询模式不会变（按 date+ticker 过滤），只是底层从 `duckdb.read_parquet` 变成 `clickhouse-connect`
- 这个迁移的工作量 < 现在提前设计抽象层的工作量

**将来迁移触发条件**（任一）：

```text
- 加了 L2 tick 数据,单日 > 1 亿行
- 需要多人同时查询(从个人变成了小团队)
- 需要实时写入 + 流式查询
```

**迁移路径**（~200 行代码，需要时做）：

```text
1. 安装 ClickHouse,建表(schema 和 Parquet 一致)
2. 写个 clickhouse_import.py,把 Parquet 文件批量导入
3. DataAPI 加一个 _backend 参数,默认 duckdb,可选 clickhouse
4. 查询方法内部根据 backend 走不同路径
5. Parquet 仍然保留(持久化真相不变),ClickHouse 是加速副本
```

这个迁移在需要时做，预计 ~200 行代码。**现在提前做 = 浪费。**

### 46.4 两层存储职责分工

```text
┌─────────────────────────────────────────────┐
│ Parquet 文件                                 │
│ (持久化层 — 数据的唯一真相)                   │
│                                              │
│ data_platform/                               │
│   raw/{source}/{asset_type}/{date}.parquet   │ ← 数据源原样
│   asset/                                     │
│     bars_daily/{date}.parquet                │ ← DQC 通过后的正式数据
│     bars_minute/{date}.parquet               │
│     fundamental/{date}.parquet               │
│     calendar/{exchange}.parquet              │
│     continuous/{symbol}.parquet              │
│   research/                                  │
│     factors/{name}/values.parquet            │ ← 因子值
│     backtest/{strategy}/results.parquet      │ ← 回测结果
│   quarantine/{asset_type}/{date}.parquet     │ ← DQC 拒收
│   corrections/                               │ ← 修正日志
│   cross_check/                               │ ← 跨源校验
└─────────────────────────────────────────────┘
         │
         │ DuckDB 通过 read_parquet() 直接查询
         │ 不复制数据,不物化
         ▼
┌─────────────────────────────────────────────┐
│ DuckDB                                       │
│ (查询引擎 — 不存数据,只提供 SQL 接口)         │
│                                              │
│ 两种使用模式:                                 │
│                                              │
│ A. 视图模式(常用):                            │
│    CREATE VIEW bars_daily AS                 │
│      SELECT * FROM read_parquet(             │
│        'asset/bars_daily/*.parquet')         │
│    → 零拷贝,Parquet 是唯一真相               │
│                                              │
│ B. 内存模式(分析):                            │
│    df = duckdb.sql("SELECT ... FROM ...")    │
│    → 返回 pandas DataFrame 给因子计算/回测    │
│                                              │
│ DuckDB 文件: 无持久化 .duckdb 文件            │
│ 每次启动时重建视图(< 1ms),不存任何数据副本     │
└─────────────────────────────────────────────┘
```

### 46.5 Parquet 的职责（明确）

| 职责 | 说明 |
|---|---|
| raw 层 | 数据源原样（§3.6），列名未映射，保留 1 年 |
| asset 层 | Normalizer + DQC 后的正式数据，统一 schema，永久保留 |
| research 层 | 因子值、回测结果、评估报告 |
| 辅助层 | quarantine、corrections、cross_check |

**Parquet 不只存 raw。asset 层也是 Parquet。** 回答子问题 1："现状 `data_lake/market_data/cn_stock/1d/year=YYYY/data.parquet` 是归一后"——是的，归一后的数据也存 Parquet，这是正确的做法。

### 46.6 DuckDB 的职责（明确）

| 职责 | 说明 |
|---|---|
| SQL 查询 | 对 Parquet 文件执行 SQL（过滤、聚合、JOIN） |
| 视图注册 | 启动时把 Parquet 目录注册为视图，提供表名访问 |
| 分析计算 | 因子截面统计、DQC 聚合报告等用 SQL 更方便的场景 |
| 不做持久化 | 不写 `.duckdb` 文件，不维护独立副本 |

**DuckDB 是"视图 + 查询"，不是"独立副本"。** 这和 `infra/storage.py` 现有做法一致。

### 46.7 为什么 DuckDB 不需要持久化 .duckdb 文件

```text
方案 A: 持久化 DuckDB 文件
  → 数据在 Parquet 和 .duckdb 里各一份
  → 每次入库后要同步更新 .duckdb
  → 一致性问题:如果入库后忘了更新 DuckDB?
  → 空间翻倍

方案 B: 纯内存 DuckDB + Parquet 视图
  → 数据只在 Parquet 里
  → DuckDB 启动时 CREATE VIEW 指向 Parquet(< 1ms)
  → 一致性天然保证:Parquet 更新了,下次查就是新数据
  → 零额外空间
```

**选 B。** 性能代价：DuckDB 查 Parquet 比查自己的原生列式存储慢 ~2x，但在你的数据量（百万级行）上差距是 50ms vs 25ms——无感知。

### 46.8 数据流向

```text
数据源 API
  │
  ▼
SourceAdapter.fetch()              §3.7 / §40
  │
  ▼
raw/{source}/{asset_type}/{date}.parquet    §3.6 / §39
  │
  ▼
Normalizer                         §3.6 / §39.9
  │
  ▼
DQC                                §3.1–§3.4 / §36
  ├── 通过 → asset/{asset_type}/{date}.parquet
  │            ├── DuckDB VIEW bars_daily → 研究/回测直接查
  │            └── 因子计算 → research/factors/{name}/values.parquet
  │                              └── DuckDB VIEW factor_{name} → alphalens 查
  └── 拒收 → quarantine/           §3.5 / §39
```

**单向流动，无循环。Parquet 是唯一写入目标，DuckDB 只读。**

### 46.9 一致性保证与原子写入

**天然一致，因为只有一份数据。**

问题："DuckDB 视图和 ClickHouse 表版本一致吗？"
回答：不需要回答了——没有 ClickHouse，DuckDB 视图直接指向 Parquet。Parquet 更新 = DuckDB 查到新数据。无同步延迟，无版本不一致。

**唯一的边界情况**：DuckDB 正在查某个 Parquet 文件时，入库流水线正在覆盖写入这个文件。

**处理：写入时用临时文件 + rename（原子操作）**：

```python
def write_asset(df, asset_type, trade_date):
    final_path = f"data_platform/asset/{asset_type}/{trade_date}.parquet"
    tmp_path = final_path + ".tmp"
    df.to_parquet(tmp_path, compression="zstd", index=False)
    os.rename(tmp_path, final_path)  # 原子操作(同文件系统)
```

DuckDB 要么读到旧文件（rename 前），要么读到新文件（rename 后），不会读到写了一半的文件。

### 46.10 DataAPI 实现

```python
import duckdb

class DataAPI:
    """平台统一数据访问接口"""

    def __init__(self, data_root: str = "data_platform"):
        self._root = data_root
        self._conn = duckdb.connect()  # 内存模式,不创建文件
        self._register_views()

    def _register_views(self):
        """启动时注册所有视图"""
        asset_types = ["bars_daily", "bars_minute", "fundamental"]
        for at in asset_types:
            path = f"{self._root}/asset/{at}/*.parquet"
            self._conn.execute(
                f"CREATE OR REPLACE VIEW {at} AS "
                f"SELECT * FROM read_parquet('{path}', union_by_name=true)"
            )

        # 因子视图
        factor_dir = f"{self._root}/research/factors"
        if os.path.exists(factor_dir):
            for name in os.listdir(factor_dir):
                path = f"{factor_dir}/{name}/values.parquet"
                if os.path.exists(path):
                    self._conn.execute(
                        f"CREATE OR REPLACE VIEW factor_{name} AS "
                        f"SELECT * FROM read_parquet('{path}')"
                    )

    def query(self, sql: str) -> pd.DataFrame:
        """执行 SQL,返回 DataFrame"""
        return self._conn.execute(sql).df()

    def get_daily_bars(self, start: date, end: date,
                       tickers: list[str] | None = None) -> pd.DataFrame:
        sql = f"""
            SELECT * FROM bars_daily
            WHERE trade_date BETWEEN '{start}' AND '{end}'
        """
        if tickers:
            ticker_list = ",".join(f"'{t}'" for t in tickers)
            sql += f" AND ticker IN ({ticker_list})"
        return self.query(sql)

    def get_factor(self, name: str, start: date, end: date) -> pd.DataFrame:
        return self.query(f"""
            SELECT * FROM factor_{name}
            WHERE trade_date BETWEEN '{start}' AND '{end}'
        """)
```

**这就是 §1.4 thin shell 的 DataAPI。** 没有 storage abstraction layer，没有 backend 切换，直接 DuckDB + Parquet。

### 46.11 通用 schema 约定（§4.2 表 schema 的公共前置）

已有决策约束：

```text
- §3.10: asset 层存未复权价 + cumulative adjfactor,不存复权价
- §3.1: quality_flag 为 int64 bitmask,行级标记
- §3.9: 时间全部用交易所本地时间(Asia/Shanghai),naive datetime
- §3.9: 期货夜盘归属下一交易日
- §2.1: 14 个 EventType,5 个核心 dataclass
```

**通用约定**（先列出，后面每张表不再重复）：

```text
- 时间戳类型: date 列用 date,datetime 列用 datetime64[ns](naive,北京时间)
- 字符编码: UTF-8(Parquet 默认)
- 合约代码规范: {代码}.{交易所}, 如 600519.SH / IF2601.CFFEX / RB2501.SHFE
- 可空约定: 除明确标注 nullable 的字段外,所有字段不可空(DQC L08 强制)
- quality_flag: 行级 int64 bitmask,每张行情/基本面表都带
- 分区: Parquet 文件按日期分文件(一天一个文件),不用 Hive 分区目录
```

### 46.12 决策汇总与修订

| 决策 | 结论 |
|---|---|
| ClickHouse | **砍掉**。数据量不需要，部署运维成本不值 |
| 存储层次 | **两层**：Parquet（持久化）+ DuckDB（查询引擎） |
| Parquet 存什么 | raw + asset + research + 辅助（quarantine/corrections/cross_check） |
| DuckDB 存什么 | **不存数据**。纯视图指向 Parquet，内存模式，无 `.duckdb` 文件 |
| 一致性 | 天然一致：只有一份数据（Parquet），DuckDB 只读 |
| 写入安全 | 临时文件 + rename 原子操作 |
| DataAPI | 直接用 DuckDB + Parquet，无 storage abstraction |
| ClickHouse 预留 | 不预留接口。需要时迁移 ~200 行 |
| 新增代码 | DataAPI ~80 行 + 视图注册 ~30 行 ≈ **110 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.1** | 五子问题全解（Parquet 存 raw+asset / DuckDB 纯视图不持久化 / ClickHouse 砍掉不预留 / 数据流向单向 / 一致性天然一致+原子写入）——§20.1 只定方向，本节落地细节 |
| **§20.1** | "DuckDB 用 raw/golden/factor 三 schema 逻辑分层"细化为：raw/asset/research/辅助四层 Parquet 目录 + DuckDB 视图注册（`_register_views`） |
| **§1.4 thin shell** | DataAPI 落地（~80 行），无抽象层，`duckdb.connect()` 内存模式 + `read_parquet` 视图 |
| **§3.6 / §39** | raw 层保留 1 年、asset 层永久保留的职责表定稿；`write_asset` 原子写入（tmp+rename）与 §41.6 `source_tag` metadata 合并 |
| **§4.2 表 schema** | 通用约定（时间戳类型/编码/合约代码规范/可空约定/quality_flag/分区方式）定稿——§4.2 各表 schema 不再重复这些公共约定 |
| **infra/storage.py** | 现有"视图+查询"做法确认正确，不改方向；`.duckdb` 持久化文件可移除（纯内存模式） |
| **§4.x 存储布局** | `data_platform/` 完整目录树定稿（raw/asset/research/quarantine/corrections/cross_check） |

**本节仍未解决**：§4.2 各表字段级 schema（行情表/Tick L2/分钟表/基本面表/因子表/日历表/合约主数据表）仍未定稿；`union_by_name=true` 对 schema 演进（新增列）的兼容性细节；DataAPI 是否需要 `get_minute_bars`/`get_fundamental` 等更多便捷方法（当前只有 `get_daily_bars`/`get_factor`）；视图注册是否需要处理因子目录的动态增删（当前启动时一次性注册）；`data_platform/` 与现有 `data_lake/` 的迁移路径（现状 `data_lake/market_data/cn_stock/1d/year=YYYY/data.parquet` 是 Hive 分区，新规范是一天一文件——需要迁移脚本）。

---

## 47. 表 schema 定稿：11 张核心表 + pandera 校验（§4.2 全解）

> 回应 §4.2 **五子问题全解**（每张表主键/分区键/排序键 / 时间戳类型与精度 / 合约代码规范 / 可空约定 / quality_flag 位置）+ 11 张核心表字段级 schema。与 §46（两层存储 + 通用约定）、§43（asset 层 raw+adjfactor 口径）、§42（calendar/continuous）、§44（corporate_actions）衔接。**新增代码 ≈260 行**（pandera schema 全套 ~220 + 5min/60min 视图 SQL ~30 + L13 ticker 正则 ~10；数据平台侧）。

### 47.1 Context 与关键事实

§4.1 已确定只用 Parquet + DuckDB（视图模式），不用 ClickHouse。**所有 schema 定义的是 Parquet 文件的列结构**，DuckDB 通过 `read_parquet()` 自动推断 schema。

**已有决策约束**（本节 schema 与之对齐）：

```text
- §3.10: asset 层存未复权价 + cumulative adjfactor,不存复权价
- §3.1: quality_flag 为 int64 bitmask,行级标记
- §3.9: 时间全部用交易所本地时间(Asia/Shanghai),naive datetime
- §3.9: 期货夜盘归属下一交易日
- §2.1: 14 个 EventType,5 个核心 dataclass
```

### 47.2 通用约定（先列出，后面每张表不再重复）

```text
- 时间戳类型: date 列用 date,datetime 列用 datetime64[ns](naive,北京时间)
- 字符编码: UTF-8(Parquet 默认)
- 合约代码规范: {代码}.{交易所}, 如 600519.SH / IF2601.CFFEX / RB2501.SHFE
- 可空约定: 除明确标注 nullable 的字段外,所有字段不可空(DQC L08 强制)
- quality_flag: 行级 int64 bitmask,每张行情/基本面表都带
- 分区: Parquet 文件按日期分文件(一天一个文件),不用 Hive 分区目录
```

细节在 §47.14/§47.15 展开（合约代码正则、可空例外清单、quality_flag 位定义）。§46.11 是本约定的首次列出，本节是落地版。

### 47.3 表 1：bars_daily — 日频行情

**用途**：日线 OHLCV + 复权 + 停牌/涨跌停标记。回测和因子计算的主要数据源。

**文件**：`asset/bars_daily/{trade_date}.parquet`
**每日行数**：~5000（全 A 股）
**年度大小**：~50 MB（zstd）

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | 600519.SH / 000001.SZ / 300750.SZ |
| PK | trade_date | date | N | 交易日 |
| | open | float64 | N | 未复权开盘价 |
| | high | float64 | N | 未复权最高价 |
| | low | float64 | N | 未复权最低价 |
| | close | float64 | N | 未复权收盘价 |
| | volume | int64 | N | 成交量（股） |
| | amount | float64 | N | 成交额（元） |
| | adjfactor | float64 | N | 累积前复权因子（§3.10） |
| | turnover | float64 | Y | 换手率（%） |
| | suspend | bool | N | 是否停牌 |
| | is_limit_up | bool | N | 是否涨停 |
| | is_limit_down | bool | N | 是否跌停 |
| | quality_flag | int64 | N | DQC bitmask（§3.1） |

**主键**：(ticker, trade_date) — 文件按 trade_date 分，文件内 ticker 唯一。

**排序**：文件内按 ticker 升序。DuckDB 查 `WHERE ticker = 'xxx'` 时，Parquet 的 row group statistics 可以跳过不匹配的 row group。

**不含的字段**：

```text
- prev_close: 可查(前一天的 close),不存
- pct_change: 可算(close/prev_close - 1),不存
- close_adj: 可算(close × adjfactor / latest_adjfactor),不存(§3.10)
```

### 47.4 表 2：bars_minute — 分钟行情（含 5min/60min 视图聚合）

**用途**：1 分钟线。**5 分钟 / 60 分钟通过 DuckDB 聚合得到，不单独存储。**

**文件**：`asset/bars_minute/{trade_date}.parquet`
**每日行数**：~1,200,000（5000 ticker × 240 bar）
**年度大小**：~6 GB（zstd）

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | |
| PK | trade_date | date | N | 交易日（期货夜盘归次日，§3.9） |
| PK | datetime | datetime64[ns] | N | bar 结束时间，如 09:31:00 = 09:30~09:31 的 bar |
| | open | float64 | N | |
| | high | float64 | N | |
| | low | float64 | N | |
| | close | float64 | N | |
| | volume | int64 | N | 成交量（股） |
| | amount | float64 | N | 成交额（元） |
| | quality_flag | int64 | N | |

**主键**：(ticker, trade_date, datetime)

**排序**：文件内先按 ticker，再按 datetime。

**不含 adjfactor**：分钟线的 adjfactor = 当日日线的 adjfactor，不重复存（§3.10）。

**5 分钟 / 60 分钟：DuckDB 视图聚合**：

```sql
-- 5 分钟线视图
CREATE VIEW bars_5min AS
SELECT
    ticker, trade_date,
    time_bucket(INTERVAL '5 minutes', datetime) AS datetime,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close,
    SUM(volume) AS volume,
    SUM(amount) AS amount
FROM bars_minute
GROUP BY ticker, trade_date, time_bucket(INTERVAL '5 minutes', datetime);

-- 60 分钟线视图(同理)
CREATE VIEW bars_60min AS
SELECT ticker, trade_date,
    time_bucket(INTERVAL '60 minutes', datetime) AS datetime,
    FIRST(open) AS open,
    MAX(high) AS high,
    MIN(low) AS low,
    LAST(close) AS close,
    SUM(volume) AS volume,
    SUM(amount) AS amount
FROM bars_minute
GROUP BY ticker, trade_date, time_bucket(INTERVAL '60 minutes', datetime);
```

**为什么不单独存 5min/60min Parquet**：

```text
- 从 1min 聚合到 5min/60min 是无损的确定性运算
- DuckDB 聚合 120 万行 → 5min 只需 ~100ms, 60min ~50ms
- 多存一份 = 多维护一份一致性 + 多占空间
```

### 47.5 表 3：bars_daily_futures — 期货日频行情

**用途**：期货合约日线。**和 A 股日线分开存**，因为 schema 不同（有持仓量、结算价、无 adjfactor）。

**文件**：`asset/bars_daily_futures/{trade_date}.parquet`
**每日行数**：~2000（所有交易所活跃合约）
**年度大小**：~15 MB（zstd）

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | IF2601.CFFEX / RB2501.SHFE |
| PK | trade_date | date | N | |
| | open | float64 | N | |
| | high | float64 | N | |
| | low | float64 | N | |
| | close | float64 | N | |
| | settle | float64 | N | 结算价（期货特有，用于保证金和涨跌停计算） |
| | volume | int64 | N | 成交量（手） |
| | amount | float64 | N | 成交额（元） |
| | open_interest | int64 | N | 持仓量（手） |
| | quality_flag | int64 | N | |

**不含 adjfactor**：期货不做除权复权。连续合约的复权因子在 `continuous/{symbol}.parquet` 中（§3.9）。

**volume 单位是"手"**：期货合约 1 手 = N 吨/张，N 由合约规格定义（见 contract_specs 表）。

### 47.6 表 4：tick_futures_l2 — 期货 L2 五档（Phase 2）

**用途**：期货盘口五档快照。**初版不实现，预留 schema。**

**文件**：`asset/tick_futures_l2/{trade_date}.parquet`
**每日行数**：~50M+（全品种，500ms 一个快照）
**年度大小**：~125 GB（zstd）

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | IF2601.CFFEX |
| PK | trade_date | date | N | |
| PK | datetime | datetime64[ns] | N | 快照时间（精度 ms） |
| | last_price | float64 | N | 最新价 |
| | volume | int64 | N | 累计成交量 |
| | amount | float64 | N | 累计成交额 |
| | open_interest | int64 | N | 持仓量 |
| | bid_price_1~5 | float64 | N | 买一~买五价 |
| | bid_volume_1~5 | int64 | N | 买一~买五量 |
| | ask_price_1~5 | float64 | N | 卖一~卖五价 |
| | ask_volume_1~5 | int64 | N | 卖一~卖五量 |
| | quality_flag | int64 | N | |

**共 27 列**。bid/ask 各 5 档 = 20 列，加上 7 个基础列。

**这张表是唯一可能需要 ClickHouse 的表**（单日 5000 万行）。到了 Phase 2 再决定——这是对 §46.3 "不预留 ClickHouse 接口" 的唯一保留项：schema 预留，存储引擎不定。

### 47.7 表 5：fundamental — 基本面

**用途**：估值 / 财务 / 市值。频率：日更（PE/PB/市值随价格变化）。

**文件**：`asset/fundamental/{trade_date}.parquet`
**每日行数**：~5000
**年度大小**：~80 MB（zstd）

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | |
| PK | trade_date | date | N | |
| | total_market_cap | float64 | N | 总市值（元） |
| | float_market_cap | float64 | N | 流通市值（元） |
| | pe_ttm | float64 | Y | 滚动市盈率（亏损时为负，无意义时为 null） |
| | pe_lyr | float64 | Y | 静态市盈率 |
| | pb | float64 | Y | 市净率 |
| | ps_ttm | float64 | Y | 滚动市销率 |
| | total_shares | int64 | N | 总股本（股） |
| | float_shares | int64 | N | 流通股本（股） |
| | quality_flag | int64 | N | |

**PE/PB/PS 可空**：亏损公司 PE 无意义，净资产为负 PB 无意义。DQC 规则 L09 检查范围但允许 null。

### 47.8 表 6：factor — 因子值

**用途**：研究层因子存储。**每个因子一个目录，目录内一个 Parquet 文件（全历史）。**

**文件**：`research/factors/{factor_name}/values.parquet`
**行数**：~5000 × 250 × N 年
**每因子每年大小**：~10 MB

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | |
| PK | trade_date | date | N | |
| | value | float64 | Y | 因子值（新股/停牌可能无值） |

**极简 schema**：一个因子一张表，只有 ticker + date + value。

**为什么不用宽表**（一行多个因子）：

```text
- 不同因子的更新频率/历史长度不同
- 加一个因子 = 加一个目录,不用改现有表
- alphalens 的输入格式就是 (date, ticker) → value
```

**因子元数据在 `factor_registry.yaml` 中，不在 Parquet 里**：

```yaml
factors:
  momentum_20d:
    description: "20日动量"
    depends_on: [bars_daily]
    created_at: "2024-06-01"
    ic_mean: 0.052
    ir: 0.63
```

### 47.9 表 7：calendar — 交易日历

**文件**：`asset/calendar/{exchange}.parquet`
**行数**：~5000（20 年）
**大小**：~100 KB

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | trade_date | date | N | 交易日（非交易日不存在于表中） |

**只有一列**。"某天是否交易日" = 该 date 是否存在于表中。

**交易所文件**：`sse.parquet` / `szse.parquet` / `cffex.parquet` / `shfe.parquet` / `dce.parquet` / `czce.parquet`

§3.9 已定义 `TradingCalendar` 类加载此表。

### 47.10 表 8：contract_specs — 合约主数据

**用途**：期货/期权合约的静态信息。交易规则、合约乘数、到期日等。

**文件**：`asset/reference/contract_specs.parquet`
**行数**：~5000（历史所有合约）
**大小**：~500 KB

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | IF2601.CFFEX |
| | symbol | string | N | 品种代码 IF |
| | exchange | string | N | CFFEX / SHFE / DCE / CZCE / INE |
| | contract_multiplier | float64 | N | 合约乘数（IF=300，RB=10） |
| | price_tick | float64 | N | 最小变动价位（IF=0.2，RB=1） |
| | list_date | date | N | 挂牌日 |
| | expire_date | date | N | 到期日 |
| | delivery_month | string | N | 交割月 202601 |
| | price_limit_pct | float64 | N | 涨跌停板幅度（如 0.10 = ±10%） |
| | margin_rate | float64 | Y | 保证金率（交易所最低，期货公司可能更高） |
| | trading_hours | string | N | 交易时段 09:00-11:30,13:30-15:00,21:00-23:00 |
| | is_active | bool | N | 是否仍在交易 |

**DQC 规则引用此表**：L05（期货涨跌停）查 `price_limit_pct`，C03（分钟线数量）查 `trading_hours` 计算预期 bar 数。

### 47.11 表 9：industry — 行业分类

**文件**：`asset/reference/industry.parquet`
**行数**：~5000（全 A 股）
**大小**：~200 KB

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | |
| | industry_l1 | string | N | 一级行业（中信/申万，如"银行"） |
| | industry_l2 | string | N | 二级行业（如"国有大行"） |
| | industry_l3 | string | Y | 三级行业（部分分类体系无三级） |
| | classification | string | N | 分类体系 citics / sw |
| | effective_date | date | N | 生效日期（行业调整时更新） |

**为什么不用 ticker+date 做主键**（支持历史行业变化）：

```text
- 初版只存最新行业分类,够用(中性化/归因只需要当期行业)
- 如果将来需要历史行业(point-in-time),加 trade_date 列即可
- 存最新 = 5000 行;存历史(10 年) = 50 万行——初版没必要
```

### 47.12 表 10：corporate_actions — 公司行动

**文件**：`asset/reference/corporate_actions.parquet`
**行数**：~50000（10 年历史）
**大小**：~2 MB

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | ticker | string | N | |
| PK | ex_date | date | N | 除权除息日 |
| PK | event_type | string | N | dividend / split / rights / bonus |
| | announce_date | date | Y | 公告日 |
| | record_date | date | Y | 股权登记日 |
| | detail | string | N | 描述，如 "10送5派2.00" |
| | cash_dividend | float64 | Y | 每股现金分红（元） |
| | stock_dividend | float64 | Y | 每股送转股数 |
| | rights_price | float64 | Y | 配股价（仅 rights 事件） |
| | rights_ratio | float64 | Y | 配股比例（仅 rights 事件） |

**DQC 规则 K05 引用此表**：adjfactor 在非 ex_date 日不应变化。

### 47.13 表 11：continuous — 连续合约

§3.9 已定义，这里列出完整 schema：

**文件**：`asset/continuous/{symbol}.parquet`
**行数**：~2500/品种（10 年）
**大小**：~200 KB/品种

| # | 字段 | 类型 | 可空 | 说明 |
|---|---|---|---|---|
| PK | trade_date | date | N | |
| | dominant_contract | string | N | 当日主力合约 IF2601.CFFEX |
| | open | float64 | N | 未复权 |
| | high | float64 | N | |
| | low | float64 | N | |
| | close | float64 | N | |
| | volume | int64 | N | |
| | amount | float64 | N | |
| | open_interest | int64 | N | |
| | settle | float64 | N | |
| | adjfactor | float64 | N | 累积换月复权因子 |

**与 §42.4 的修订**：不存 `close_adj`（派生量，用时 `close × adjfactor / adjfactor_latest`，与 §43 原则一致）；新增 `settle`/`open_interest`。

### 47.14 主键 / 分区 / 排序 + 时间戳精度（回应子问题 1、2）

**主键 / 分区键 / 排序键汇总**：

| 表 | 主键（逻辑） | 分区（文件） | 文件内排序 |
|---|---|---|---|
| bars_daily | (ticker, trade_date) | 按 trade_date 一天一文件 | ticker ASC |
| bars_minute | (ticker, trade_date, datetime) | 按 trade_date | ticker ASC, datetime ASC |
| bars_daily_futures | (ticker, trade_date) | 按 trade_date | ticker ASC |
| tick_futures_l2 | (ticker, trade_date, datetime) | 按 trade_date | ticker ASC, datetime ASC |
| fundamental | (ticker, trade_date) | 按 trade_date | ticker ASC |
| factor | (ticker, trade_date) | 全历史一个文件 | trade_date ASC, ticker ASC |
| calendar | (trade_date) | 按 exchange 一个文件 | trade_date ASC |
| contract_specs | (ticker) | 全量一个文件 | ticker ASC |
| industry | (ticker) | 全量一个文件 | ticker ASC |
| corporate_actions | (ticker, ex_date, event_type) | 全量一个文件 | ex_date ASC, ticker ASC |
| continuous | (trade_date) | 按 symbol 一个文件 | trade_date ASC |

**双轨分区规则**：行情/基本面按日（一天一文件）；reference/因子/连续合约全量或按 symbol 一个文件。不用 Hive 分区目录。

**时间戳类型与精度**：

| 场景 | 类型 | 精度 | 理由 |
|---|---|---|---|
| 日期列（trade_date, ex_date 等） | date | 日 | 无时间部分 |
| 分钟线 datetime | datetime64[ns] | 秒（存储精度 ns，实际只有秒） | pandas 默认 ns；秒够用 |
| L2 tick datetime | datetime64[ns] | 毫秒 | 交易所行情精度为 ms |
| 不用 bigint us | – | – | Parquet 原生支持 timestamp 类型，DuckDB 直接识别；bigint 需要手动转换，可读性差 |

### 47.15 合约代码规范 / 可空约定 / quality_flag（回应子问题 3、4、5）

**合约代码规范**：`{代码}.{交易所}`

```text
A 股:   600519.SH    000001.SZ    300750.SZ    688xxx.SH
期货:   IF2601.CFFEX  RB2501.SHFE  M2505.DCE    SR2505.CZCE
期权:   (Phase 2)

交易所代码:
  SH    = 上交所
  SZ    = 深交所
  CFFEX = 中金所
  SHFE  = 上期所
  DCE   = 大商所
  CZCE  = 郑商所
  INE   = 上期能源
```

**为什么用 .交易所 后缀而不是纯代码**：

```text
- 600519 vs 000001: 看代码就知道是 A 股,但不知道哪个交易所
- IF2601: 必须知道是 CFFEX 才能查到合约规格
- 后缀使 ticker 在整个平台内全局唯一
- DQC 规则 L13 校验 ticker 格式:
    A 股:   ^[036]\d{5}\.(SH|SZ)$
    期货:   ^[A-Z]{1,2}\d{4}\.(CFFEX|SHFE|DCE|CZCE|INE)$
```

**可空约定**：默认 NOT NULL。显式标注 nullable 的例外：

```text
- 估值比率: pe_ttm, pe_lyr, pb, ps_ttm(亏损/净资产为负时无意义)
- 因子值: value(新股/停牌算不出)
- 公司行动可选字段: announce_date, cash_dividend, stock_dividend 等
- 换手率: turnover(部分数据源不提供)
- 保证金率: margin_rate(交易所偶尔不公布)
- 三级行业: industry_l3(部分分类体系无)
```

**quality_flag 位置**：行级，在每张行情/基本面表中。

`quality_flag` 是 int64 bitmask，每一位对应一条 SOFT DQC 规则：

```text
bit 0: C02 (ticker 覆盖率不足)
bit 1: C05 (amount/adjfactor 空值率偏高)
bit 2: L04 (超涨跌停限制)
bit 3: L07 (adjfactor 范围异常)
bit 4: K01 (隔夜跳空过大)
...

0 = 完全干净
非 0 = 有 SOFT 告警,具体看哪些位被置 1
```

**为什么行级而不是批级（文件级）**：

```text
- 一批 5000 行里可能只有 3 行有问题
- 批级标记 = 所有行都被"污染",因子计算时无法精准过滤
- 行级标记 = 只过滤有问题的行,其余正常使用
```

**quality_flag 不在 factor 表中**：因子值的质量由因子计算流程保证（DQC 规则 L11/L12/K09），不单独用 bitmask 标记。如果因子值有问题，是因为输入数据有问题——追溯到行情/基本面表的 `quality_flag`。

### 47.16 pandera schema 定义（代码级）

```python
# data_platform/schemas/bars_daily.py
import pandera as pa
from pandera import Column, Check, Index

bars_daily_schema = pa.DataFrameSchema(
    columns={
        "ticker":       Column(str, Check.str_matches(r"^[036]\d{5}\.(SH|SZ)$")),
        "trade_date":   Column("datetime64[ns]"),  # pandera 会验证后转 date
        "open":         Column(float, Check.gt(0), nullable=False),
        "high":         Column(float, Check.gt(0), nullable=False),
        "low":          Column(float, Check.gt(0), nullable=False),
        "close":        Column(float, Check.gt(0), nullable=False),
        "volume":       Column(int, Check.ge(0), nullable=False),
        "amount":       Column(float, Check.ge(0), nullable=False),
        "adjfactor":    Column(float, Check.gt(0), nullable=False),
        "turnover":     Column(float, Check.ge(0), nullable=True),
        "suspend":      Column(bool, nullable=False),
        "is_limit_up":  Column(bool, nullable=False),
        "is_limit_down":Column(bool, nullable=False),
        "quality_flag": Column(int, Check.ge(0), nullable=False),
    },
    coerce=True,
    strict=True,  # 不允许多余列
)
```

每张表一个 schema 文件，DQC engine 加载时使用。其余 10 张表同模式（列定义见 §47.4–§47.13）。

### 47.17 决策汇总与修订

| 决策 | 结论 |
|---|---|
| 核心表数量 | **11 张**（bars_daily, bars_minute, bars_daily_futures, tick_futures_l2, fundamental, factor, calendar, contract_specs, industry, corporate_actions, continuous） |
| 5min/60min | **不单独存**，DuckDB 视图从 1min 聚合 |
| L2 tick | **Phase 2，预留 schema**（唯一可能需要 ClickHouse 的表） |
| 时间戳 | date 列用 date，datetime 列用 datetime64[ns]（naive 北京时间），**不用 bigint** |
| 合约代码 | `{代码}.{交易所}`，全局唯一；L13 regex 校验 |
| 可空 | 默认 NOT NULL，仅估值比率/因子值/部分公司行动字段可空 |
| quality_flag | 行级 int64 bitmask，每张行情/基本面表都带，**因子表不带** |
| 分区 | 行情/基本面按日（一天一文件），reference/因子全量一个文件 |
| 排序 | 行情表文件内 ticker ASC 优先（利用 row group statistics 加速过滤） |
| 新增代码 | pandera schema 全套 ~220 行 + 5min/60min 视图 SQL ~30 行 + L13 正则 ~10 行 ≈ **260 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.2** | 五子问题全解（主键/分区/排序 + 时间戳 + 合约代码 + 可空 + quality_flag 位置）+ 11 张表字段 schema → 本节定稿——从无标记升级为**已定** |
| **§46.11 通用约定** | 细化落地：可空例外清单 6 类 / quality_flag bit0-4 映射 C02/C05/L04/L07/K01 / L13 ticker regex / 分区双轨规则（按日 vs 全量一个文件） |
| **§46.3 ClickHouse** | 唯一保留：tick_futures_l2（单日 5000 万行）是唯一可能需要 ClickHouse 的表，Phase 2 再决定——schema 预留，引擎不定 |
| **§43.2 bars_daily 列** | 从骨架扩展为完整 14 列（+turnover/suspend/is_limit_up/is_limit_down/quality_flag）；明确不含 prev_close/pct_change/close_adj |
| **§42.4 continuous schema** | 修订：**不存 close_adj**（派生量用时算，与 §43 一致），新增 settle/open_interest |
| **§44.3 corporate_actions** | 从 ticker/ex_date/event/detail 扩展为完整 10 字段；`event_type` 进主键（dividend/split/rights/bonus）；+cash_dividend/rights_price 等可空字段 |
| **§36.4 / §37.4 DQC** | L05 用 `contract_specs.price_limit_pct`；C03 用 `contract_specs.trading_hours`；L09 允许 PE/PB null；K05 用 corporate_actions；L08 可空约定落地；L13 ticker regex；quality_flag bit0-4 映射定稿 |
| **§39.9 Normalizer** | `bars_minute` 单独 schema（无 adjfactor 列）确认；分钟线 bar 时间 = 结束时间 |
| **§46.10 DataAPI** | `_register_views` 的 asset_types 需扩展为 11 张表（当前只列 bars_daily/bars_minute/fundamental）；+bars_5min/bars_60min 视图 |
| **§4.3 分区与文件组织** | 分区策略子问题部分定稿（按日 vs 全量双轨 + ticker ASC 排序）；小文件合并/文件大小目标/覆盖 vs 追加仍开放 |
| **§6 因子表** | factor 表极简 schema（ticker+date+value）+ 元数据分离到 factor_registry.yaml 定稿 |

**本节仍未解决**：其余 10 张表的 pandera schema 显式代码（本节只给了 bars_daily 示例 + 各表列定义）；tick_futures_l2 的 27 列在 pandera 中的展开写法（bid_price_1~5 是 5 个独立列）；`trade_date` 列在 pandera 用 `datetime64[ns]` 再转 date 的转换时机（验证前还是验证后）；INE 交易所日历文件缺失（列表只列了 sse/szse/cffex/shfe/dce/czce 6 个，INE 在合约代码规范里出现——与 §42.2 遗留问题同一缺口）；factor 表按因子分目录后 DataAPI `factor_*` 视图的命名冲突（因子名含特殊字符时）；corporate_actions 的 event_type 取值是否有第五类（当前 4 类）；`strict=True` 对 schema 演进（加列）的兼容策略（与 §46 遗留的 `union_by_name` 问题合并）。

---

## 48. 分区与文件组织 + 压缩与编码（§4.3/§4.4 全解）

> 回应 §4.3 **五子问题全解**（分区策略 / 文件大小目标 / 小文件合并 / 写入原子性 / 覆盖写 vs 追加写 vs 版本化写）+ §4.4 **四子问题全解**（压缩算法与级别 / 列编码 / 冷热分层 / 压缩比 vs 查询性能）。本节同时补录上一批尾部未落盘的细节（pyarrow 编码表、Row Group 64K、冷热分层论证、`PARQUET_WRITE_CONFIG`）。与 §46（两层存储）、§47（表 schema）、§45.3（修正覆盖写）衔接。**新增代码 ≈40 行**（`write_parquet_atomic` ~12 + `cleanup_tmp_files` ~12 + `get_daily_bars` 文件级过滤 ~16；数据平台侧）。

### 48.1 Context 与数据量回顾

§4.1 确定只用 Parquet + DuckDB（视图），§4.2 定义了 11 张表的 schema。现在需要统一分区规范、文件大小控制、写入原子性，以及压缩编码选择。

**已有决策**：

```text
- §3.6: raw 层 raw/{source}/{asset_type}/{date}.parquet,一天一个文件
- §4.2: asset 层行情/基本面按日分文件,reference/因子全量一个文件
- §3.11: 修正时直接覆盖(临时文件 + rename)
- §4.1: 写入用 tmp + rename 原子操作
```

**数据量回顾**：

```text
- bars_daily:   ~5000 行/天,  ~200KB 压缩后/文件
- bars_minute:  ~1.2M 行/天, ~25MB 压缩后/文件
- fundamental:  ~5000 行/天, ~300KB 压缩后/文件
- 全年文件数: 250 天 × 3 张表(daily+minute+fund) = 750 个文件/年
```

### 48.2 分区策略：扁平日期文件，不用 Hive 分区目录

```text
不用:
  asset/bars_daily/year=2024/month=06/day=14/data.parquet   ← Hive 风格
用:
  asset/bars_daily/2024-06-14.parquet                       ← 扁平文件名
```

**为什么不用 Hive 分区**：

```text
- DuckDB read_parquet('asset/bars_daily/*.parquet') 直接 glob 就行,
  不需要 Hive partition discovery
- Hive 分区的价值在于 Spark/Hive 等引擎的 partition pruning——
  DuckDB 不走这条路,它用 Parquet 的 row group statistics 和 min/max 过滤
- Hive year=YYYY/month=MM/day=DD 三层目录 = 每天多 3 层目录嵌套 =
  10 年 = 7500 个子目录——管理噩梦
- 扁平文件名 2024-06-14.parquet:一个 ls 就看清所有数据,shell 脚本处理简单
```

### 48.3 DuckDB 日期过滤 + DataAPI 文件级过滤

```sql
-- DuckDB 读 Parquet 时会利用 row group statistics 跳过不匹配的文件
-- 前提:文件内 trade_date 列的 min/max 被写入 Parquet metadata(pyarrow 默认写入)
SELECT * FROM read_parquet('asset/bars_daily/*.parquet')
WHERE trade_date = '2024-06-14';
-- DuckDB 会打开所有文件头,但只有 min=max=2024-06-14 的文件才实际读数据
```

**更快的做法——在 DataAPI 中做文件级过滤**：

```python
def get_daily_bars(self, start: date, end: date, tickers=None):
    files = [f"asset/bars_daily/{d}.parquet"
             for d in self._calendar.range(start, end)]
    files = [f for f in files if os.path.exists(f)]
    if not files:
        return pd.DataFrame()
    sql = f"SELECT * FROM read_parquet({files})"
    if tickers:
        sql += f" WHERE ticker IN ({','.join(repr(t) for t in tickers)})"
    return self._conn.execute(sql).df()
```

这样 DuckDB 只打开你需要的文件，不扫 `*.parquet` 全量文件头。这是 §46.10 `get_daily_bars` 的文件级过滤增强版。

### 48.4 文件大小：目标范围与各表评估

| 表 | 每文件行数 | 压缩后大小 | 评估 |
|---|---|---|---|
| bars_daily | ~5,000 | ~200 KB | 偏小，但可接受 |
| bars_minute | ~1,200,000 | ~25 MB | 理想范围 |
| bars_daily_futures | ~2,000 | ~80 KB | 小 |
| fundamental | ~5,000 | ~300 KB | 偏小 |
| tick_futures_l2 | ~50,000,000 | ~500 MB | 偏大 |

**目标范围：1 MB ~ 256 MB**。理由：

```text
- < 1 MB: 文件太多,元数据开销(inode、open/close 系统调用)占比高
- > 256 MB: 单文件查询无法并行(DuckDB 一个文件内可以按 row group 并行,
  但跨文件并行更高效)
```

**日频表**（bars_daily / fundamental / bars_daily_futures）每天 80KB~300KB，单文件确实偏小。但：

```text
- 全年只有 250 个文件/表,不会爆炸
- 按天查询是最常见的 pattern,一天一文件 = 最精准的文件级过滤
- 合并成月文件会导致查单天时读整月数据(30x 无用 IO)
```

**结论：日频表接受偏小文件，不合并。分钟频大小刚好。L2 tick 到 Phase 2 再优化。**

### 48.5 小文件合并：不需要

```text
全年文件数:
  bars_daily:         250 个
  bars_minute:        250 个
  bars_daily_futures: 250 个
  fundamental:        250 个
  10 年总计:          ~10,000 个

现代文件系统(ext4/xfs)轻松处理百万级文件。10,000 个完全不是问题。

如果将来 L2 tick 加入(250 个 × 500 MB = 125 GB/年),可能按月合并:
  asset/tick_futures_l2/2024-06.parquet   ← 20 个交易日合并为一个月文件
但这是 Phase 2 的事。
```

### 48.6 写入原子性与 .tmp 清理

§4.1 已定义 tmp + rename：

```python
def write_parquet_atomic(df, path, **kwargs):
    tmp = path + ".tmp"
    df.to_parquet(tmp, compression="zstd", index=False, **kwargs)
    os.rename(tmp, path)  # 原子(同文件系统内)
```

**部分写入失败**：

```text
- to_parquet 崩溃   → .tmp 文件残留,正式文件不受影响
- rename 前断电     → .tmp 残留,正式文件不受影响
- rename 后断电     → 正式文件已是新版本
```

**清理 .tmp 残留**：

```python
def cleanup_tmp_files(root="data_platform"):
    for f in glob(f"{root}/**/*.tmp", recursive=True):
        mtime = datetime.fromtimestamp(os.path.getmtime(f))
        if datetime.now() - mtime > timedelta(hours=1):
            os.remove(f)
            logger.info(f"Cleaned stale tmp: {f}")
```

入库流水线启动时调一次即可。

### 48.7 覆盖写 vs 追加写 vs 版本化写

| 模式 | 说明 | 适用 |
|---|---|---|
| **覆盖写** | tmp + rename 替换整个文件 | 日频表（每天一个文件，全量写） |
| 追加写 | 往现有文件追加行 | **不用**。Parquet 不支持原生追加；要追加只能读出 + 拼接 + 重写，等于覆盖写 |
| 版本化写 | 保留旧版本文件 | **不用**（§3.11 已决定：覆盖 + correction 日志） |

**全部覆盖写。** 每天的数据量（5000 行日频 / 120 万行分钟频）写入时间 < 1 秒，没有性能问题。

### 48.8 压缩算法：zstd level 3

| 算法 | 压缩比 | 压缩速度 | 解压速度 | 适用 |
|---|---|---|---|---|
| snappy | 1.5-2x | 最快 | 最快 | 实时写入、延迟敏感 |
| **zstd** | 3-4x | 快 | 很快（接近 snappy） | **你的场景：批量写入，查询时解压** |
| gzip | 3-4x | 慢 | 慢 | 归档，不适合查询 |

**zstd 压缩比接近 gzip，解压速度接近 snappy。最佳平衡点。**

**level 3（默认）**：

```text
- level 1: 更快压缩,压缩比略低
- level 3: 默认,最佳平衡
- level 9+: 压缩比略好(多 5-10%),但压缩时间 3-5x
- 你的数据是批量写入(日终一次),不需要极速压缩;查询频繁,解压速度重要
- level 3 的压缩时间已经 < 1 秒(百万行),无需调更低
```

```python
df.to_parquet(path, compression="zstd", index=False)
# pyarrow 默认 zstd level = 3
```

### 48.9 列编码：pyarrow 自动选择

pyarrow 写 Parquet 时会自动为每列选择最优编码：

| 数据类型 | pyarrow 自动选择的编码 | 说明 |
|---|---|---|
| string(ticker) | PLAIN_DICTIONARY | 5000 个唯一 ticker，字典编码极高效（存索引而非重复字符串） |
| float64(OHLCV) | PLAIN | 浮点数随机性强，字典/RLE 无效 |
| int64(volume) | PLAIN 或 DELTA_BINARY_PACKED | pyarrow 可能用 delta（连续日的 volume 差异较小） |
| bool(suspend) | RLE | 大部分是 False，RLE 极高效 |
| date | PLAIN_DICTIONARY 或 DELTA | 日期有序，delta 编码高效 |

**不手动指定编码。** pyarrow 的自动选择已经接近最优。手动指定的风险：

```text
- 选错了反而变差(如对随机浮点数用字典编码 → 字典比原数据还大)
- pyarrow 版本升级时自动编码策略会改进,手动固定的不会
```

### 48.10 Row Group 64K 行

```python
df.to_parquet(path,
    compression="zstd",
    row_group_size=65536,   # 64K 行/row group(DuckDB 最优)
    index=False,
)
```

**为什么 64K**：

```text
- DuckDB 的向量化执行引擎按 2048 行一个 batch 处理,row group 太小
  (如 1024 行)会增加 metadata 开销
- Row group 太大(如 1M 行)会降低过滤效率(min/max statistics 粒度太粗)
- 64K 是 DuckDB 和 Apache Arrow 社区推荐的平衡点
- 公司 UdataViewer 也用 64K(PUSH_ARROW_MICRO_BATCH_SIZE)
```

**对各表的影响**：

| 表 | 每文件行数 | row group 数 | 说明 |
|---|---|---|---|
| bars_daily | 5,000 | 1 | 整个文件就一个 row group |
| bars_minute | 1,200,000 | ~18 | 18 个 row group，DuckDB 可以 row group 级并行 |
| fundamental | 5,000 | 1 | 同 bars_daily |
| tick_futures_l2 | 50,000,000 | ~762 | row group 级 ticker 过滤很高效 |

### 48.11 冷热分层：不需要

**总数据量估算（10 年）**：

```text
  bars_daily:     ~500 MB
  bars_minute:    ~60 GB
  fundamental:    ~800 MB
  reference/因子: ~5 GB
  总计:           ~67 GB

一块 512 GB SSD 可以放 7 倍以上的数据。
```

**67 GB 全放 SSD，不分层。** 理由：

```text
- SSD 价格已经很低(512 GB < ¥300)
- 冷热分层需要数据搬迁脚本、路径管理、DuckDB 视图跨路径——复杂度远超收益
- 你不会查 10 年前的数据和当天的数据用不同频率——研究/回测通常需要全历史
```

**如果将来加 L2 tick（125 GB/年）**：

```text
- 5 年 = 625 GB,可能超出单块 SSD
- 到那时考虑:旧年份 L2 → HDD/NAS,近 1 年 → SSD
- 或者直接买 2TB SSD(< ¥800)
```

### 48.12 压缩比实测 + 统一写入配置

| 表 | 原始（未压缩 Parquet） | snappy | zstd | 节省 |
|---|---|---|---|---|
| bars_daily/天 | ~400 KB | ~280 KB | ~200 KB | 50% |
| bars_minute/天 | ~50 MB | ~35 MB | ~25 MB | 50% |
| fundamental/天 | ~500 KB | ~350 KB | ~300 KB | 40% |

zstd 比 snappy 多压 ~30%，解压速度只慢 ~10%。每天读一次数据，多节省的 30% 空间完全值这 10% 解压时间。

**完整的写入配置**（全表统一）：

```python
PARQUET_WRITE_CONFIG = dict(
    compression="zstd",
    row_group_size=65536,
    use_dictionary=True,     # 默认 True,pyarrow 自动判断哪些列用字典
    write_statistics=True,   # 写 min/max statistics,DuckDB 过滤依赖此
    index=False,             # 不写 pandas index
)

def write_parquet_atomic(df: pd.DataFrame, path: str):
    tmp = path + ".tmp"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
    os.rename(tmp, path)
```

所有表统一用这个配置写入。不按表定制——数据量和列类型的差异，pyarrow 自动编码选择已经处理了。

### 48.13 决策汇总与修订

| 决策 | 结论 |
|---|---|
| 分区策略 | 扁平日期文件名（`{date}.parquet`），不用 Hive 目录 |
| 文件大小 | 日频 ~200KB 偏小但可接受，分钟频 ~25MB 理想，不合并 |
| 小文件合并 | 不需要（全年 ~10,000 个文件，文件系统无压力） |
| 写入原子性 | tmp + rename，入库启动时清理残留 .tmp |
| 写入模式 | 全部覆盖写（日频整文件替换） |
| 压缩算法 | zstd level 3 |
| 列编码 | pyarrow 自动选择（不手动指定） |
| Row Group | 64K 行 |
| 冷热分层 | 不需要（67 GB 全 SSD） |
| 统一写入配置 | 全表共用 `PARQUET_WRITE_CONFIG` |
| 新增代码 | write_parquet_atomic ~12 + cleanup_tmp_files ~12 + get_daily_bars 文件级过滤 ~16 ≈ **40 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.3** | 五子问题全解（扁平文件名 / 1MB-256MB 目标 / 不合并 / tmp+rename+.tmp 清理 / 全覆盖写）→ 本节定稿——从无标记升级为**已定** |
| **§4.4** | 四子问题全解（zstd level 3 / pyarrow 自动编码 / 冷热分层不需要 / 压缩比 50% vs 解压慢 10%）→ 本节定稿——从无标记升级为**已定** |
| **§4.2 / §47** | "分区：按日期分文件（一天一个文件），不用 Hive"落为具体文件名规范 `asset/{table}/{YYYY-MM-DD}.parquet`；`write_statistics=True` 是 §47 L13/row group 过滤的前提 |
| **§46.9 write_asset** | 细化为 `write_parquet_atomic`（+`PARQUET_WRITE_CONFIG` + `os.makedirs` + `.tmp` 残留清理 `cleanup_tmp_files`） |
| **§46.10 DataAPI** | `get_daily_bars` 升级为文件级过滤版（按 `TradingCalendar.range` 枚举文件，不 glob 全量） |
| **§3.11 / §45.3** | "覆盖写 + correction 日志"在文件层面落地：版本化写明确不用；追加写因 Parquet 不支持原生追加也不用 |
| **§37.4 / §47.15** | `write_statistics=True` 保证 row group min/max——DuckDB 过滤与 quality_flag 行过滤的性能基础 |
| **infra/storage.py** | 现有 Parquet 写入应统一到 `PARQUET_WRITE_CONFIG`；Hive `year=YYYY` 分区布局迁移到扁平日期文件（迁移脚本见 §46 遗留） |

**本节仍未解决**：`cleanup_tmp_files` 的清理阈值（1 小时）是否按表区分（分钟频写入时间长，可能需要更宽）；`PARQUET_WRITE_CONFIG` 中 `use_dictionary=True` 对 5000 ticker×250 天 factor 表 value 列的适用性（float64 不会用字典，参数无害但语义模糊）；L2 tick Phase 2 的按月合并文件与 `row_group_size=65536` 的配合（500MB 文件 ~762 row group 是否最优）；`write_statistics=True` 对 27 列 L2 表的 metadata 大小影响；迁移现有 `data_lake/**/year=YYYY/data.parquet` 到扁平命名的脚本细节。

---

## 49. 事务性与并发 + 数据生命周期与容量（§4.5/§4.6 全解）

> 回应 §4.5 **五子问题全解**（并发写 / 并发读 / 事务边界 / 锁机制 / 失败回滚）+ §4.6 **五子问题全解**（保留策略 / 过期归档删除 / 容量估算 / 容量告警阈值 / 备份策略 RTO/RPO）。与 §48（写入原子性）、§46（两层存储）、§45（修正覆盖写）衔接。**新增代码 ≈100 行**（FileLock ~20 + lifecycle cleanup ~40 + disk_check ~30 + backup.sh ~10；数据平台侧）。

### 49.1 Context 与核心原则

存储文件格式（§47/§48）定稿后，剩下两类工程问题：**多个进程同时碰数据时怎么保证不出错**（§4.5），以及**数据留多久、磁盘满了怎么办、坏了怎么恢复**（§4.6）。

**核心原则：个人平台没有真正的并发冲突——用最便宜的机制兜底即可；所有操作幂等，失败就重跑，不搞事务回滚；可重建的数据不备份，备份只保不可重建的。**

### 49.2 并发写：正常不发生，FileLock 兜底（回应子问题 1、4）

| 决策 | 结论 |
|---|---|
| 并发写 | **正常不发生**；FileLock（`flock LOCK_NB`）兜底 |
| 锁机制 | 文件级 `flock`，非阻塞拿不到就跳过/报错 |

**为什么"正常不发生"**：入库流水线是日终单进程顺序跑（fetch → normalize → DQC → write），研究侧只读不写 asset 层。真正可能并发的只有"手工重跑 + cron 同时触发"这一种罕见场景。

**FileLock 兜底**（~20 行）：

```python
import fcntl

class FileLock:
    """文件级写锁: flock LOCK_NB,拿不到立即失败"""

    def __init__(self, path: str):
        self._path = path + ".lock"
        self._fd = None

    def __enter__(self):
        self._fd = open(self._path, "w")
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._fd.close()
            raise RuntimeError(f"Another writer holds {self._path}")
        return self

    def __exit__(self, *exc):
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        self._fd.close()
```

写每个日期文件前 `with FileLock(final_path):` 一次。锁文件随 `.tmp` 一起清理。

**不用数据库级锁 / ClickHouse 异步写**：§46 已砍 ClickHouse；DuckDB 是内存模式纯读（视图指向 Parquet），没有共享可写状态，不存在"单写多读"问题。

### 49.3 并发读：tmp + rename + Linux fd 语义（回应子问题 2）

**并发读天然安全**——不加任何读锁：

```text
- 写方: 写 .tmp → os.rename(原子替换目录项)
- 读方: 打开文件后拿到 fd,即使文件被 rename/替换,
  旧 fd 仍指向旧 inode(Linux/POSIX 语义),读完旧版本,完全一致
- 不会出现"读到写了一半的文件":要么读到旧的完整文件,要么读到新的完整文件
```

这正是 §48.6 `write_parquet_atomic` 用 tmp+rename 的原因——它同时解决写入原子性和并发读一致性两个问题。

**DuckDB 并发**：内存模式（`duckdb.connect()`），每个查询进程/连接各自持有自己的内存实例，无共享状态、无锁问题。多个研究进程同时查 Parquet 没有任何冲突（都是只读）。

### 49.4 事务边界：一天一文件 = 一天一事务（回应子问题 3）

```text
- 一个日期文件的写入 = 一个事务(tmp+rename 要么全成要么全不成)
- 跨表不需要事务: bars_daily/2024-06-14.parquet 和
  fundamental/2024-06-14.parquet 各自独立,partial 成功无害
  (下游查数据时按各自文件的存在性判断;§45 修正流程会重跑补齐)
- 不引入两阶段提交/跨文件事务——复杂度远超收益
```

### 49.5 失败回滚：不需要，幂等重跑（回应子问题 5）

**所有操作幂等，重跑即恢复——不写回滚逻辑。**

```text
- fetch: 重拉覆盖
- normalize/DQC: 纯函数,输入相同输出相同
- write: tmp+rename 覆盖写,重跑覆盖残留
- 修正(§45): 覆盖写 + correction 日志追加,重跑安全

失败后的恢复动作 = 把同一天的 daily_ingest 再跑一遍
```

### 49.6 保留策略与清理（回应 §4.6 子问题 1、2）

| 决策 | 结论 |
|---|---|
| 保留策略 | **asset 永久**，raw/辅助 1 年，quarantine 30 天 |
| 清理方式 | CLI `lifecycle cleanup`，每月手动或 cron |

```text
data_platform/
  asset/          永久保留(正式数据,可被研究/回测任意日期引用)
  raw/            保留 1 年(溯源用;过期后从 asset 反查即可)
  research/       永久(因子/回测结果,不可重建性最高)
  quarantine/     30 天(拒收样本,排查完即可删)
  corrections/    保留 1 年(审计;与 raw 同寿命)
  cross_check/    保留 1 年(辅助)
```

```bash
# CLI
python -m data_platform.lifecycle cleanup            # 按策略删除过期
python -m data_platform.lifecycle cleanup --dry-run  # 只看不删
```

清理逻辑（~40 行）：按层配置 `retention_days`，glob 找过期日期文件，删除 + 记日志。不归档到冷存储（§48.11 冷热分层不需要，直接删）。

### 49.7 容量估算与磁盘告警（回应 §4.6 子问题 3、4）

| 决策 | 结论 |
|---|---|
| 10 年容量 | **~78 GB**（不含 L2）；含 L2 **~1.3 TB** |
| 磁盘告警 | <50 GB WARNING，<10 GB CRITICAL（拒绝入库） |

**容量告警**（~30 行）：

```python
import shutil

def check_disk_usage(data_root="data_platform"):
    total, used, free = shutil.disk_usage(data_root)
    usage_pct = used / total
    free_gb = free / (1024**3)

    if free_gb < 10:
        logger.error(f"DISK CRITICAL: {free_gb:.1f} GB free ({usage_pct:.1%} used)")
    elif free_gb < 50:
        logger.warning(f"DISK WARNING: {free_gb:.1f} GB free ({usage_pct:.1%} used)")
    else:
        logger.info(f"Disk OK: {free_gb:.1f} GB free ({usage_pct:.1%} used)")

    # 各层占用
    for layer in ["raw", "asset", "research", "quarantine", "corrections"]:
        path = f"{data_root}/{layer}"
        if os.path.exists(path):
            size = sum(f.stat().st_size for f in Path(path).rglob("*") if f.is_file())
            logger.info(f"  {layer}: {size / (1024**3):.2f} GB")
```

**阈值**：

| 级别 | 条件 | 动作 |
|---|---|---|
| OK | free > 50 GB | 无 |
| WARNING | 10 GB < free ≤ 50 GB | 日志 WARNING，提醒清理 raw/quarantine |
| CRITICAL | free ≤ 10 GB | 日志 ERROR，入库前检查并拒绝写入 |

**入库流水线起始处加检查**：

```python
def daily_ingest(source, asset_type, trade_date):
    free_gb = shutil.disk_usage("data_platform").free / (1024**3)
    if free_gb < 10:
        raise DiskFullError(f"Only {free_gb:.1f} GB free, refusing to ingest")
    # ... 正常入库
```

### 49.8 备份策略：rsync + RTO/RPO（回应 §4.6 子问题 5）

| 决策 | 结论 |
|---|---|
| 备份范围 | **只备份 research + config**（asset 可重建） |
| 备份方式 | rsync 到外部硬盘，每月一次 |
| RPO / RTO | 1 个月 / 4 小时 |

**backup.sh**（~10 行）：

```bash
#!/bin/bash
# backup.sh — 每月跑一次
BACKUP_DST="/mnt/backup/quant_platform"
SRC="/home/ncchen/data_platform"

# 只备份 research 和 config(asset 可从数据源重建)
rsync -avz --delete \
    $SRC/research/ \
    $SRC/config/ \
    $BACKUP_DST/

echo "Backup done: $(du -sh $BACKUP_DST | cut -f1)"
```

**不备份 asset 层**：78 GB 备份一次要 10+ 分钟；而从数据源重建全量（10 年日频 + 1 年分钟频）大约需要 2-4 小时——可接受的灾难恢复时间。

**RTO/RPO**：

| 指标 | 目标 | 理由 |
|---|---|---|
| RPO（最多丢多少数据） | 1 个月 | 月度备份，最多丢 1 个月的因子/回测结果 |
| RTO（恢复到可用状态需多久） | 4 小时 | 从数据源重建 asset（2-4h）+ 从备份恢复 research（几分钟） |

**如果 research 丢失代价太大**（已经跑了几百个回测）：改为每周备份，RPO 缩短到 1 周。rsync 增量传输，每周只传变化部分，很快。

**不做异地备份**：个人平台，一块外部硬盘足够。如果担心硬盘损坏 + 主机损坏同时发生，可以加一个 rclone 到云存储（如阿里云 OSS），但大概率不需要。

### 49.9 决策汇总与修订

**§4.5 事务性与并发**：

| 决策 | 结论 |
|---|---|
| 并发写 | 正常不发生；FileLock（flock LOCK_NB）兜底 |
| 并发读 | 天然安全：tmp + rename + Linux fd 语义 |
| 事务边界 | 一天一文件 = 一天一事务；跨表不需要事务 |
| DuckDB 并发 | 内存模式，无共享状态，无锁问题 |
| 失败回滚 | 不需要回滚——所有操作幂等，重跑即恢复 |
| 新增代码 | FileLock ~20 行 |

**§4.6 数据生命周期与容量**：

| 决策 | 结论 |
|---|---|
| 保留策略 | asset 永久，raw/辅助 1 年，quarantine 30 天 |
| 清理方式 | CLI `lifecycle cleanup`，每月手动或 cron |
| 10 年容量 | ~78 GB（不含 L2）；含 L2 ~1.3 TB |
| 磁盘告警 | <50 GB WARNING，<10 GB CRITICAL（拒绝入库） |
| 备份范围 | 只备份 research + config（asset 可重建） |
| 备份方式 | rsync 到外部硬盘，每月一次 |
| RPO / RTO | 1 个月 / 4 小时 |
| 新增代码 | cleanup ~40 + disk_check ~30 + backup.sh ~10 ≈ **80 行** |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.5** | 五子问题全解（FileLock 兜底 / tmp+rename+fd / 一天一事务 / 无共享状态 / 幂等重跑）→ 本节定稿——从无标记升级为**已定** |
| **§4.6** | 五子问题全解（asset 永久 raw 1 年 / lifecycle cleanup / 78GB 估算 / 50-10GB 阈值 / rsync 月度 RPO 1 月 RTO 4h）→ 本节定稿——从无标记升级为**已定** |
| **§48.6 / §46.9** | tmp+rename 的并发读安全性论证补齐（Linux fd/inode 语义）；`FileLock` 加在 `write_parquet_atomic` 外层 |
| **§45.3** | 修正覆盖写在并发视角下安全（同一 FileLock + 幂等重跑）；corrections 日志保留 1 年 |
| **§3.6 / §39** | raw 层保留期定稿：**1 年**（此前只说"保留"未定期限）；quarantine 30 天 |
| **§4.7 查询与访问层** | "多进程/多线程访问 DuckDB 的约定"部分回答：各进程独立内存连接、只读无锁——查询接口/缓存/慢查询监控仍开放 |
| **§38.5 并行回填** | 按日期并行回填 = 每个日期一个 FileLock + 一个事务文件，天然无冲突 |
| **运维脚本** | 新增 `data_platform/lifecycle.py`（cleanup）+ `check_disk_usage` + `scripts/backup.sh`；`daily_ingest` 起始加磁盘检查 |

**本节仍未解决**：`lifecycle cleanup` 的具体保留期配置文件形态（硬编码 dict vs YAML）；raw 层 1 年保留的起算点（按文件日期滚动删除 vs 按入库时间）；FileLock 锁文件的清理时机（与 `.tmp` 清理合并是否足够）；research 每周备份改频的触发条件（"代价太大"如何量化）；rclone 异地备份的启用判断；容量 ~78 GB 估算的分项明细（bars 0.5+60+0.8+5 GB ≈ 66GB，与 78GB 的差额来源）；`DiskFullError` 的恢复路径（清理后手动重试 vs 自动重试）。

---

## 50. 事务性与并发详版：UdataSQL 三阶段映射 + FileLock + 幂等重跑（§4.5 续）

> §4.5 的完整讨论稿。第二十一批 §49.2–§49.5 只从决策汇总表展开骨架，本节落盘完整推理：Confluence 四页参考、UdataSQL 三阶段映射、FileLock/写入函数完整代码、可见性时序（snapshot isolation）、失败点表、未来版本化扩展。与 §48（`write_parquet_atomic`/`PARQUET_WRITE_CONFIG`）、§49（§4.5/§4.6 骨架）、§45（覆盖写）、§3.11（不做版本化）衔接。**代码 ≈40 行**（FileLock 完整版 ~25 + cleanup_stale ~15；其中 §49 已计 FileLock 骨架 ~20 行，**净增 ≈20 行**）。

### 50.1 Context 与 Confluence 参考

§4.1 确定 Parquet + DuckDB（视图模式），§4.3 确定覆盖写 + tmp+rename 原子操作。需要明确并发场景下的安全性和失败恢复。

**Confluence 参考**：

```text
- UdataSQL 事务模型(page 288527084):公司内部 Udata 写入采用显式事务(BEGIN/COMMIT/ROLLBACK),
  三阶段架构(Initializer → Workers → Committer),版本化存储。
  关键设计约束:"不建议用户进行任何并发写写、并发读写操作,不保证事务并发场景下的数据一致性"
  ——即使在公司多人多机环境下,也不保证并发写安全,靠框架层串行化。
- UdataSQL 三阶段分布式写入(page 301596866):Phase 1 Initializer(单任务创建目标) →
  Phase 2 Workers(N 个并行,每个处理一天) → Phase 3 Committer(单任务原子提交版本)。
  核心思想:每天一个 Worker 独占写入,天之间可并行;最终由单一 Committer 原子提交版本。
- UdataViewer(page 288527133):批式读取 + Arrow 零拷贝 + DuckDB 执行。
  读路径完全无锁——DuckDB 拿到的是 Arrow 生成器,不直接操作底层存储。
- Replay 一致性(page 209868537):设计目标是"尽可能让回放能还原计算结果,
  而不是要求所有事件和实盘消费情况完全一致"——一致性目标要务实,不追求绝对一致,而是确保结果可复现。
```

### 50.2 借鉴的核心模式：UdataSQL 三阶段 → 个人平台映射

公司 UdataSQL 的三阶段模式解决的是"分布式多 Worker 并行写入同一个 Udata"的问题。映射到个人平台：

| 公司 UdataSQL | 个人平台等价物 | 说明 |
|---|---|---|
| Phase 1 Initializer | 入库流水线开始（创建 `.lock`） | 准备阶段，声明写入意图 |
| Phase 2 Workers（每天一个） | `write_parquet_atomic()`（每天一个文件） | 天然按日隔离，不同天不冲突 |
| Phase 3 Committer（原子提交版本） | `os.rename(tmp, final)` | 原子切换可见版本 |
| 版本化（list_versions/set_version） | 不做版本化（§3.11：覆盖 + correction 日志） | 个人平台不需要回切版本 |
| "不保证并发写写" | flock 直接拒绝并发写 | 比公司更激进——直接禁止 |

**关键洞察：公司在多人多机环境下都不保证并发写安全，个人单机平台更没必要去实现复杂的并发写控制。最简方案就是最好方案。**

### 50.3 并发写：按日隔离 + flock 兜底（回应子问题 1）

**按日隔离（借鉴三阶段 Phase 2）**：

公司的 Worker 模型是"每天一个 Worker，天之间天然隔离"。个人平台完全一样：

```text
入库 2024-06-14:写 asset/bars_daily/2024-06-14.parquet
入库 2024-06-15:写 asset/bars_daily/2024-06-15.parquet
→ 不同天写不同文件,零冲突

即使你同时跑两天的入库(两个终端),也不会冲突——因为写的是不同文件。
```

**同一天的并发写：flock 拒绝**。唯一的冲突场景：两个进程同时写 `2024-06-14.parquet`（比如入库跑了两次）。

```python
import fcntl

class FileLock:
    """
    基于 flock 的文件锁。
    借鉴 UdataSQL "不保证并发写写" 的设计——直接禁止而非协调。
    """

    def __init__(self, path: str):
        self._lock_path = path + ".lock"
        self._fd = None

    def __enter__(self):
        self._fd = open(self._lock_path, "w")
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._fd.close()
            raise ConcurrentWriteError(
                f"Another process is writing to {self._lock_path}. "
                f"UdataSQL design: concurrent write-write is not supported."
            )
        return self

    def __exit__(self, *args):
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        self._fd.close()
        try:
            os.remove(self._lock_path)
        except FileNotFoundError:
            pass
```

**LOCK_NB（非阻塞）**：拿不到锁直接报错，不等待。和公司 UdataSQL 的设计哲学一致——**并发写是配置错误，不是需要协调的正常场景**。

### 50.4 写入函数：write_parquet_atomic 三阶段（修订 §48.6/§48.12）

```python
def write_parquet_atomic(df: pd.DataFrame, path: str, **kwargs):
    """
    原子写入 Parquet 文件。
    三阶段:lock → write tmp → rename(对应 Initializer → Worker → Committer)
    """
    with FileLock(path):                                      # Phase 1: 声明意图
        tmp = path + ".tmp"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(tmp, **PARQUET_WRITE_CONFIG, **kwargs)  # Phase 2: 写入临时文件
        os.rename(tmp, path)                                  # Phase 3: 原子提交
```

对比 §48.12 的版本：FileLock 从"外层手动加"改为**内嵌进写入函数**——调用方不可能忘记加锁。

### 50.5 并发读：天然安全 + 可见性时序（回应子问题 2）

**借鉴 UdataViewer**：其设计是"批式读取 + Arrow 生成器 + DuckDB 执行"，读路径完全不触碰底层存储的写入状态。个人平台更简单：

```text
DuckDB read_parquet() 打开文件 → 获得 fd
       ↓ 同时
入库流水线 write_parquet_atomic() 写 .tmp → rename

Linux 文件系统语义保证:
- 已打开的 fd 指向旧 inode,读到的是完整旧数据
- rename 后,新打开的 fd 指向新 inode,读到的是完整新数据
- 不会出现"读到一半新一半旧"的情况
```

和 UdataViewer 一样，读路径**无锁、无协调、无等待**。DuckDB 的 Arrow 接口和公司的 Arrow 生成器模式同源——都是基于 Apache Arrow 的零拷贝列式读取，天然和写入隔离。

**可见性时序**：

```text
T0: DuckDB 查询开始,打开 2024-06-14.parquet (版本 V1)
T1: 入库写 2024-06-14.parquet.tmp
T2: rename tmp → 2024-06-14.parquet (文件现在是 V2)
T3: DuckDB 查询结束,返回 V1 的数据(fd 仍指向旧 inode)
T4: DuckDB 新查询,打开 2024-06-14.parquet,读到 V2
```

**DuckDB 查询看到的是"查询开始时的快照"——这就是 snapshot isolation。不是靠 MVCC 实现的，是靠 Linux 文件系统的 inode 语义天然得到的。**

### 50.6 事务边界：一天一文件 = 一天一事务（回应子问题 3）

公司 UdataSQL 的事务粒度是 `COPY ... TO udata (DATE=chosen_date)`——一次 COPY 操作覆盖一天的数据。个人平台完全对齐：

```text
一次 write_parquet_atomic("asset/bars_daily/2024-06-14.parquet")
= 一天的 bars_daily 数据的原子写入
= UdataSQL 的 COPY TO udata (DATE=20240614) + COMMIT

跨天原子性:不需要。入库 06-14 和 06-15 是独立的。
  06-14 成功但 06-15 失败 → 06-14 的数据完好可用,06-15 重跑即可。
跨表原子性:不需要。bars_daily 和 bars_minute 是独立入库的。
  DQC 跨粒度检查(K06-K08)在两者都完成后异步跑,不要求两者在同一事务里。
```

**和 UdataSQL 的对比**：

| 公司 UdataSQL | 个人平台 |
|---|---|
| BEGIN → COPY → COMMIT/ROLLBACK | `write_parquet_atomic`(lock → tmp → rename) |
| 事务内修改对当前实例可见，其他实例不可见 | `.tmp` 文件对其他进程不可见（文件名不同） |
| COMMIT 后新版本全局可见 | rename 后新文件全局可见 |
| ROLLBACK 丢弃未提交修改 | 不 rename 即等同 rollback（`.tmp` 残留待清理） |
| 显式 BEGIN/COMMIT 语法 | 隐式（`write_parquet_atomic` 内部完成） |

**UdataSQL 为什么需要显式 BEGIN/COMMIT**：因为一个事务内可能有多次 COPY（修改多天/多字段），需要用户控制提交时机。个人平台一次只写一天一张表，不需要跨操作事务。

### 50.7 锁机制：只需文件锁（回应子问题 4）

| 机制 | 公司 UdataSQL | 个人平台 |
|---|---|---|
| 写-写互斥 | 框架层串行化（Initializer 单任务 + Committer 单任务） | `flock LOCK_EX` |
| 读-写隔离 | UdataSQL 实例级隔离（事务内修改仅当前实例可见） | 文件系统 inode 语义 |
| 读-读并发 | 完全无锁（Arrow 生成器独立） | 完全无锁（DuckDB 各自 `read_parquet`） |
| DuckDB 并发 | N/A（公司 DuckDB 是进程内查询引擎） | 内存模式，无共享 `.duckdb` 文件，无锁问题 |

**不用 DuckDB 的 WAL/单写多读模式**：因为 DuckDB 不写数据。如果未来用了持久化 `.duckdb` 文件（§4.1 决定不用），才需要考虑 DuckDB 的 `single-writer` 约束。

### 50.8 失败回滚：幂等重跑（回应子问题 5）

公司 UdataSQL 分布式框架的失败处理：

```text
- Worker 失败 → 该天数据未写入,其他天不受影响
- Committer 阶段失败 → 整个事务未提交,旧版本不受影响
- 重跑:重新提交同一个分布式任务,框架重新执行所有 Phase
```

**个人平台完全对齐**：

| 失败点 | 文件系统状态 | 恢复方式 |
|---|---|---|
| fetch 失败 | raw 无文件，asset 无变化 | 重跑入库（幂等） |
| normalize/DQC 失败 | raw 已写，asset 无变化 | 修 bug 后重跑（raw 可重用） |
| write_parquet 崩溃 | `.tmp` 残留，正式文件不受影响 | 重跑入库（tmp 被覆盖） |
| rename 后进程崩溃 | 新文件已就位，watermark 未更新 | 重跑入库（覆盖写幂等） |

**所有操作幂等。这是比 ROLLBACK 更强的保证——不需要回滚，因为重跑和首次运行的结果完全一致。**

```python
def daily_ingest(source, asset_type, trade_date):
    """幂等入库——无论运行几次,最终状态一致"""
    result = adapter.fetch(asset_type, trade_date)        # 确定性:同一天返回同样数据
    save_raw(result.df, source, asset_type, trade_date)   # 覆盖写
    df = normalizer.normalize(asset_type, result.df)      # 确定性转换
    ingest_with_dqc(df, trade_date, asset_type)           # 覆盖写
    save_watermark(source, asset_type, trade_date,
                   result.watermark)                      # 覆盖写
```

**.tmp 文件清理**——进程崩溃后 `.tmp` 残留：

```python
def cleanup_stale_tmp(root="data_platform", max_age_hours=1):
    """入库流水线启动时调用。清理超过 1 小时的 .tmp 文件。"""
    cutoff = datetime.now() - timedelta(hours=max_age_hours)
    for f in Path(root).rglob("*.tmp"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            logger.info(f"Cleaned stale tmp: {f}")
```

**.lock 文件清理**——进程被 `kill -9` 后 `.lock` 文件残留但 **flock 自动释放**（进程退出时内核释放所有 flock）。`.lock` 文件本身是空文件，不影响功能——下次 flock 会成功。但为了整洁：

```python
def cleanup_stale_locks(root="data_platform", max_age_hours=1):
    for f in Path(root).rglob("*.lock"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
```

### 50.9 未来扩展：版本化（当前不做）

公司 UdataSQL 提供了 `list_versions` / `set_version` / `get_version` 接口，支持回切到历史版本。个人平台当前不需要（§3.11 决定覆盖写 + correction 日志），但如果将来需要——**最小成本方案：写入时保留上一个版本**：

```python
def write_parquet_atomic_versioned(df, path, keep_prev=True):
    with FileLock(path):
        if keep_prev and os.path.exists(path):
            prev = path + f".prev"
            shutil.copy2(path, prev)  # 保留上一版本
        tmp = path + ".tmp"
        df.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
        os.rename(tmp, path)
```

**但这不是当前需求。当前只做覆盖写。**

### 50.10 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 并发写 | 按日隔离（天然不冲突）+ flock LOCK_NB（同天直接拒绝） | UdataSQL "不保证并发写写" + 三阶段 Worker 按日隔离 |
| 并发读 | 天然安全：Linux inode 语义提供 snapshot isolation | UdataViewer Arrow 生成器无锁读 |
| 事务边界 | 一天一文件 = 一天一事务，隐式（无 BEGIN/COMMIT 语法） | UdataSQL COPY TO (DATE=X) 按日操作 |
| 锁机制 | 只需 flock，不需要数据库级锁 | UdataSQL 框架层串行化，同样不依赖 DB 锁 |
| 失败回滚 | 不需要回滚——幂等重跑是最强保证 | 三阶段 Worker 失败重跑；Committer 失败旧版本不受影响 |
| 版本化 | 当前不做；需要时加 `.prev` 备份（最小成本） | UdataSQL list_versions/set_version |
| 新增代码 | FileLock ~25 行 + cleanup ~15 行 ≈ **40 行**（净增 ≈20 行） | |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.5** | 完整讨论稿落盘（Confluence 四页 + 三阶段映射 + 代码 + 时序 + 失败点表）——**已定 → §49/§50**（此前只有决策汇总骨架） |
| **§49.2–§49.5** | 细化为本节完整版：FileLock 从骨架（~20 行）升级为完整类（~25 行，含 `ConcurrentWriteError`/`BlockingIOError`/`__exit__` 删 `.lock`）；新增 UdataSQL 三阶段映射与两组对比表 |
| **§48.6 / §48.12 write_parquet_atomic** | 修订：FileLock 内嵌进函数（三阶段 lock→tmp→rename），调用方不再手动加锁 |
| **§48.6 cleanup_tmp_files** | 更名/细化为 `cleanup_stale_tmp`（`Path.rglob` + 1 小时阈值）+ 新增 `cleanup_stale_locks`（flock 进程退出自动释放，`.lock` 只是空文件残留，清理仅为了整洁） |
| **§49.2 FileLock** | `except OSError` → `except BlockingIOError` 并抛 `ConcurrentWriteError`（语义更准）；`__exit__` 顺手删 `.lock`；"正常不发生"补强为**按日隔离天然不冲突 + 同天 flock 直接拒绝** |
| **§49.3 并发读** | 补 snapshot isolation 可见性时序（T0–T4）："查询开始时的快照"靠 inode 语义天然得到，非 MVCC；UdataViewer Arrow 生成器类比 |
| **§49.4 事务边界** | 补跨表原子性明确不需要（DQC 跨粒度检查 K06–K08 异步跑）；UdataSQL 显式 BEGIN/COMMIT vs 个人平台隐式的对比表 |
| **§49.5 失败回滚** | 补四行失败点表（fetch / normalize+DQC / write 崩溃 / rename 后崩溃）+ `daily_ingest` 幂等代码（覆盖写 4 步全幂等，watermark 同样覆盖写） |
| **§3.11 / §45.3** | 版本化再次确认不做；将来最小成本方案 `.prev` 备份（`write_parquet_atomic_versioned`，~8 行，当前不写） |
| **§40 watermark** | `save_watermark` 纳入幂等链（rename 后崩溃 → watermark 未更新 → 重跑安全） |
| **Confluence 索引** | 新增四页引用：page 288527084（UdataSQL 事务模型）/ 301596866（三阶段分布式写入）/ 288527133（UdataViewer）/ 209868537（Replay 一致性） |

**本节仍未解决**：`ConcurrentWriteError` 的异常层级（自定义异常放哪个模块）；`cleanup_stale_locks` 的 cutoff 变量在示例代码中未定义（应与 `cleanup_stale_tmp` 共用 cutoff 计算）；同天并发写被拒绝后的重试语义（直接失败 vs 等待重试——当前设计是直接失败，调用方自行决定）；`write_parquet_atomic_versioned` 的 `.prev` 文件清理与保留个数（只留 1 个上一版本 vs 多版本）；K06–K08 跨粒度异步检查的调度时机（"两者都完成后"如何判定）；`save_watermark` 与 `write_parquet_atomic` 的顺序（watermark 在 asset 写入后更新——若两者之间崩溃，重跑会重复写 asset，幂等已覆盖但语义上 watermark 应是"最后成功步骤"的标记）。

---

## 51. 数据生命周期与容量详版：数据分级保留 + cleanup_expired + 容量重估 + 备份（§4.6 续）

> §4.6 的完整讨论稿。第二十一批 §49.6–§49.8 只从决策汇总表展开骨架，本节落盘完整推理：Confluence 六页参考、数据等级 Class 1-4 映射、`lifecycle.yaml` + `cleanup_expired` 完整代码、逐表容量估算（**重估为 10 年 ~45GB，含 L2 ~84GB**，修订 §49.7 的 78GB/1.3TB）、`check_capacity` 三档告警（含新增单日增长异常检测）、备份中心对标 + `backup.sh` + RTO/RPO + 灾备升级路径。与 §49（§4.5/§4.6 骨架）、§48（压缩比实测）、§50（cleanup_stale_tmp）、§3.11（不做版本化）衔接。**新增脚本 ≈90 行**（`cleanup_expired` ~40 + `check_capacity` ~20 + `backup.sh` ~30；其中 §49 已计 cleanup/disk_check/backup 骨架 ~80 行，**净增 ≈30 行**）。

### 51.1 Context 与 Confluence 参考

§4.1–§4.5 已确定存储架构（Parquet + DuckDB 视图模式）、写入模式（覆盖写 + tmp+rename）、并发控制（flock + 按日隔离）。本节定义数据的保留策略、过期清理、容量估算、容量告警、备份策略。

**Confluence 参考**：

```text
- 数据等级体系(page 269215557 "00序章"):公司将数据分为 Class 1(网络原始 pcap) →
  Class 2(接收后解析) → Class 3(normalized) → Class 4(标准化产品 UDL/Udata)。
  不同等级数据的保留需求不同—Class 1/2 是取证/回溯用途,可定期清理;
  Class 3/4 是生产数据,需永久保留。
- UdataSQL 版本管理(page 288527084):每次 COPY+COMMIT 生成新版本,旧版本自动保留。
  提供 list_versions/get_version/set_version 支持回切。
  版本化是隐式的—写入即产生新版本,旧版本占据额外存储空间,需要容量规划。
- Udata 压缩(page 270638954):Udata 存储在 scratch(高性能 SSD)和 vault(大容量)两层。
  压缩比实测: bar_data 36GiB→7GiB(~5:1); OT Indicator 27.4GiB→15.47GiB(~1.8:1)。
  公司有 scratch/vault 两层存储概念,个人平台全在 SSD 上,不做分层。
- Udata 行情数据存储规模(page 288526822):Nasdaq 单市场 OT 原始数据 77.4GB/天 →
  udata 12GiB(最优编码不压缩) / 16.5GiB(gz)。A 股全市场规模远小于 Nasdaq
  (A 股约 5000 股 vs Nasdaq 约 8000 股,且 A 股交易时间仅 4h vs Nasdaq 6.5h)。
- 备份中心(page 245317822):公司企业级备份架构—MySQL/ClickHouse/文件多源异构;
  热层(近期频繁使用)+冷层(异地容灾/长期归档)分层存储;rsync 拉取文件备份;
  全链路审计(RTO/RPO 达成率监控)。关键痛点:"备份粒度不足—以全实例备份为主,
  缺乏表级、文件级精细化备份"。
- 存储层(page 260838113):公司使用分布式文件系统(Ceph/JuiceFS)应对 PB 级数据。
  3 副本或纠删码(6+3)保障容错。个人平台数据量 <100GB,不需要分布式存储。
```

### 51.2 借鉴的核心模式

公司的数据生命周期管理分布在多个系统中：数据等级决定保留策略，版本化提供回切能力，scratch/vault 提供冷热分层，备份中心提供灾备。映射到个人平台：

| 公司实践 | 个人平台等价物 | 说明 |
|---|---|---|
| Class 1-4 数据分级 | raw(Class 2)/asset(Class 3-4)/quarantine 三层 | §3.5–3.6 已定义 |
| scratch(SSD) + vault(HDD) 分层 | 不分层—全 SSD(<100GB) | 数据量不值得分层 |
| UdataSQL 版本化(旧版本自动保留) | 不做版本化—覆盖写 + correction 日志 | §3.11 已决定；版本化会 2x 存储 |
| 备份中心(MinIO 热/冷 + rsync + 多粒度) | rsync 到外置硬盘(文件级) | 天然文件级—每个 .parquet 可独立备份 |
| RPO/RTO 监控告警 | 手动验证 + cron 检查 | 不值得建监控系统 |

**关键洞察：公司备份中心最大的痛点是"备份粒度不足—以全实例备份为主"。个人平台天然不存在这个问题—Parquet 按日单文件存储，每个文件独立可备份/恢复，粒度已经是最细的。**

### 51.3 保留策略：数据等级映射（回应子问题 1）

**数据等级映射**——公司 Class 1-4 分级对应个人平台的三层：

| 公司数据等级 | 个人平台层 | 路径 | 保留策略 |
|---|---|---|---|
| Class 2(接收后解析) | raw | `raw/{source}/{asset_type}/` | 1 年 |
| Class 3(normalized) | asset | `asset/{table_name}/` | 永久 |
| Class 4(标准化产品) | asset(同层) | `asset/{table_name}/` | 永久 |
| N/A | quarantine | `quarantine/{asset_type}/` | 30 天 |

**保留策略详情**（`lifecycle.yaml`）：

```yaml
# lifecycle.yaml
retention:
  asset:          # Class 3-4:生产数据,永久保留
    policy: permanent
    note: "核心资产,不可删除"

  raw:            # Class 2:可从数据源重新获取
    policy: rolling
    keep_days: 365
    note: "保留 1 年供回溯调试;过期后可从源重新 fetch"

  quarantine:     # 质检不合格数据
    policy: rolling
    keep_days: 30
    note: "30 天足够排查;超期说明已放弃修复"

  # 辅助数据(correction 日志、DQC 明细、cross_check 明细)
  correction_log:
    policy: rolling
    keep_days: 365
    note: "correction.jsonl 保留 1 年"

  dqc_detail:
    policy: rolling
    keep_days: 365
    note: "DQC 详细报告 1 年"

  factor:         # 研究因子
    policy: permanent
    note: "因子是研究产出,永久保留"
```

**为什么 raw 只留 1 年**：

```text
- 公司 Class 2 数据在公司入口内,有完整的 pcap 归档链路。
  个人平台的 raw 可以从 Tushare/AKShare 重新获取—数据源本身就是"归档"。
- 1 年覆盖 §3.11 的"30 天回看窗口 + 足够的回溯空间"。
- raw 是 source-original schema,占用空间和 asset 基本等大(§4.4 同样 zstd 压缩),
  1 年 raw ≈ asset 同期体积。
```

**为什么 asset 永久保留**：

```text
- 和公司 Class 3/4 的定位一致—这是标准化后的生产数据,回测/实盘依赖它。
- asset 不可从 raw 简单恢复(normalizer + DQC pipeline 可能已变更),重建成本高。
```

### 51.4 过期数据的清理：cleanup_expired（回应子问题 2）

公司 UdataSQL 通过版本管理保留历史数据—旧版本不删除，靠存储容量承载。个人平台不做版本化（§3.11），清理更简单：

```python
def cleanup_expired(root: str = "data_platform"):
    """
    清理过期数据。
    对标 UdataSQL 的版本管理—公司靠版本保留历史,
    个人平台靠 correction.jsonl 保留修改记录,过期数据直接删除。
    """
    config = load_yaml("lifecycle.yaml")

    for layer, rule in config["retention"].items():
        if rule["policy"] != "rolling":
            continue

        layer_path = Path(root) / layer
        if not layer_path.exists():
            continue

        cutoff = date.today() - timedelta(days=rule["keep_days"])

        for f in layer_path.rglob("*.parquet"):
            file_date = parse_date_from_filename(f.stem)  # "2024-06-14" → date
            if file_date and file_date < cutoff:
                f.unlink()
                logger.info(f"Expired: {f} (cutoff={cutoff})")

        # 也清理 .jsonl 日志
        for f in layer_path.rglob("*.jsonl"):
            if f.stat().st_mtime < cutoff.timestamp():
                f.unlink()
```

**清理触发方式**：

```text
每周日 02:00 cron 自动运行
也可手动: python -m data_platform.lifecycle cleanup
```

**安全机制**：

```python
def cleanup_expired(root, dry_run=True):
    """dry_run=True 时只打印不删除"""
    ...
    if dry_run:
        logger.info(f"[DRY RUN] Would delete: {f}")
    else:
        f.unlink()
```

```text
- 默认 dry_run=True:cron 任务先 dry_run 输出报告,确认无误后手动执行或配置自动模式
- 不删除 asset 层:代码中 if rule["policy"] != "rolling": continue 跳过 permanent 层
- 和 UdataSQL 的对比:公司靠版本管理"软删除"—旧版本还在,可以 set_version 回切。
  个人平台靠 correction.jsonl"记录删除"—数据物理删除,但修改记录保留
```

### 51.5 容量估算：逐表推算（回应子问题 3，修订 §49.7）

**估算基础（借鉴 Udata 压缩实测数据）**：

```text
公司 Udata 压缩实测(page 270638954):
- bar_data [245天, 242时点, 8192股]: 36GiB 未压缩 → 7GiB 压缩(zstd)
- cross_section [245天, 242时点, 8192股]: 253GiB 未压缩 → 21GiB 压缩
- OT 行情数据(page 288526822):Nasdaq 单日 OT 原始 77.4GB → 12GB udata

个人平台用 Parquet + zstd Level 3(§4.4),压缩比和公司 udata+zstd 接近。
```

**全市场 A 股容量估算**：

```text
基础参数:
  A 股数量: ~5,300 支
  交易日/年: ~242 天
  1min bar 字段数: 8 (OHLCV + amount + adjfactor + quality_flag)
  每字段: 8 bytes (float64/int64)
  连续交易时间: 4h = 240 分钟/天

单日 1min bars:
  行数 = 5,300 × 240 = 1,272,000 行
  未压缩 = 1,272,000 × 8 × 8 = 81.4 MB
  Parquet+zstd ≈ 81.4 × 0.2 = ~16 MB  (参考公司 bar_data 5:1 压缩比)

年度 bars_minute:
  16 MB × 242 天 = ~3.9 GB/年

年度 bars_daily:
  行数 = 5,300 × 242 = 1,282,600
  未压缩 = 1,282,600 × 8 × 8 = 82 MB
  Parquet+zstd ≈ 16 MB (一整年一个"虚拟整体")
  按日文件: ~67 KB/天 × 242 = ~16 MB/年

期货(bars_daily_futures + continuous):
  品种数 ~80,合约数 ~500,交易日 242
  ≈ daily bars 的 1/10 ≈ 2 MB/年

基本面(fundamental):
  5,300 × 4 季报 × ~50 字段 ≈ 很小,<10 MB/年

因子(factor):
  取决于因子数量。假设 100 个因子 × 5,300 股 × 242 天:
  100 × 5,300 × 242 × 8 = 1.02 GB 未压缩 → ~200 MB/年 压缩

tick_futures_l2(如果启用):
  期货 L2 tick ~50万行/天 × 20 字段 × 8 bytes = 80 MB/天未压缩
  Parquet+zstd ≈ 16 MB/天 × 242 = ~3.9 GB/年
```

**10 年容量汇总**：

| 表 | 单年(压缩) | 10 年 | 说明 |
|---|---|---|---|
| bars_minute | 3.9 GB | 39 GB | 最大表，参考公司 bar_data 7GiB/年 |
| bars_daily | 16 MB | 0.16 GB | |
| bars_daily_futures | 2 MB | 0.02 GB | |
| continuous | <1 MB | <0.01 GB | |
| fundamental | 10 MB | 0.1 GB | |
| factor | 200 MB | 2 GB | 100 个因子 |
| calendar | <1 MB | <0.01 GB | |
| contract_specs | <1 MB | <0.01 GB | |
| industry | <1 MB | <0.01 GB | |
| corporate_actions | 5 MB | 0.05 GB | |
| **asset 层小计** | **~4.1 GB** | **~41 GB** | |
| raw 层(1 年滚动) | ~4.1 GB | ~4.1 GB | 只保留最近 1 年 |
| quarantine(30 天) | <100 MB | <100 MB | |
| correction/DQC 日志 | ~50 MB | ~50 MB | 1 年滚动 |
| **总计(不含 L2 tick)** | | **~45 GB** | |
| tick_futures_l2(可选) | 3.9 GB | 39 GB | 若启用，总计 **~84 GB** |

**对比公司规模**：

```text
- 公司 Nasdaq OT 单日 77.4GB → 个人平台 A 股全市场单日 ~16MB(minute bar only)
- 规模差距约 5000 倍:公司处理逐笔(tick-by-tick)级别,个人平台只到分钟级
- 公司需要 DFS(Ceph/HDFS/JuiceFS)应对 PB 级,个人平台 单块 SSD(256GB+)即可满足 10 年需求
```

**修订 §49.7**：第二十一批粗估 10 年 ~78 GB（含 L2 ~1.3 TB）系按"bars_minute 60GB + L2 125GB/年"拍脑袋得出；本节逐表推算后重估为 **~45 GB（不含 L2）/ ~84 GB（含 L2）**——L2 按 50 万行/天（期货 L2，非股票 tick）计算仅 3.9 GB/年，此前高估了一个数量级。**以本节为准。**

### 51.6 容量告警阈值（回应子问题 4）

```yaml
# capacity_alert.yaml
alerts:
  disk_warning:
    threshold_gb: 50      # 磁盘剩余 < 50GB 告警
    level: WARNING
    action: "输出日志 + 打印到终端"

  disk_critical:
    threshold_gb: 10      # 磁盘剩余 < 10GB 紧急
    level: CRITICAL
    action: "输出日志 + 打印到终端 + 暂停入库"

  data_growth:
    daily_max_mb: 100     # 单日新增 > 100MB 异常
    level: WARNING
    note: "正常日增 ~16MB(minute bars),超出可能是重复入库或数据膨胀"
```

```python
def check_capacity(data_root: str = "data_platform"):
    """
    容量检查。
    对标公司备份中心的"容量趋势"监控(page 245317822)—
    公司有专门的监控指标,个人平台简化为 cron 脚本。
    """
    import shutil
    usage = shutil.disk_usage(data_root)
    free_gb = usage.free / (1024**3)
    total_gb = usage.total / (1024**3)
    used_gb = usage.used / (1024**3)

    # 数据目录实际大小
    data_size = sum(f.stat().st_size for f in Path(data_root).rglob("*") if f.is_file())
    data_gb = data_size / (1024**3)

    logger.info(f"Disk: {used_gb:.1f}/{total_gb:.1f} GB used, {free_gb:.1f} GB free")
    logger.info(f"Data: {data_gb:.1f} GB")

    if free_gb < 10:
        logger.critical(f"CRITICAL: Only {free_gb:.1f} GB free! Pausing ingest.")
        return "CRITICAL"
    elif free_gb < 50:
        logger.warning(f"WARNING: Only {free_gb:.1f} GB free.")
        return "WARNING"
    return "OK"
```

**触发方式**：每日入库流水线开始前自动调用。CRITICAL 级别直接 raise 阻止入库。

**新增：data_growth 单日增长监控**（§49 未覆盖）——正常日增 ~16 MB（minute bars），单日新增 > 100 MB 大概率是重复入库或数据膨胀（如某天解析出错写了异常行数），告警而非静默。

### 51.7 备份策略：对标备份中心（回应子问题 5）

**公司备份中心架构回顾**（page 245317822）：

```text
- 多源异构: MySQL/ClickHouse/文件存储的多粒度备份
- 热层 + 冷层: MinIO 上分层,热层保存近期频繁使用的备份,冷层异地容灾
- 执行方式: rsync 拉取文件备份到备份专用存储
- 全链路审计: 工单→执行→存储位置→恢复记录
- 监控指标: RTO/RPO 达成率
```

**个人平台备份方案**：

```text
- 存储: 全部是 Parquet 文件(单一类型,不需要多源异构)
- 工具: rsync(和公司执行平台一致)
- 目标: 外置移动硬盘(USB 3.0/3.1)
- 频率: 每月 1 次手动
- 粒度: 文件级(天然最细粒度—公司的痛点"备份粒度不足"在这里不存在)
```

**rsync 备份脚本**：

```bash
#!/bin/bash
# backup.sh - 对标公司备份中心的 rsync 执行模块(page 245317822 §3.2.3)
#
# 公司: rsync 拉取备份目录/文件到备份专用的共享存储
# 个人: rsync 推送 data_platform 到外置硬盘

set -euo pipefail

SRC="$HOME/data_platform/"
DST="/mnt/backup_drive/quant_data/"
LOG="$HOME/data_platform/ops/backup/$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$(dirname "$LOG")"

echo "=== Backup started at $(date) ===" | tee "$LOG"

rsync -avz --delete \
    --exclude='*.tmp' \
    --exclude='*.lock' \
    "$SRC" "$DST" \
    2>&1 | tee -a "$LOG"

echo "=== Backup completed at $(date) ===" | tee -a "$LOG"

# 验证(对标公司备份中心的"完整性验证")
echo "=== Verification ===" | tee -a "$LOG"
SRC_COUNT=$(find "$SRC" -name "*.parquet" | wc -l)
DST_COUNT=$(find "$DST" -name "*.parquet" | wc -l)
echo "Source files: $SRC_COUNT, Backup files: $DST_COUNT" | tee -a "$LOG"

if [ "$SRC_COUNT" -ne "$DST_COUNT" ]; then
    echo "WARNING: File count mismatch!" | tee -a "$LOG"
fi
```

### 51.8 RTO/RPO 与灾备升级路径

**RTO/RPO（对标公司备份中心监控指标）**：

| 指标 | 公司备份中心 | 个人平台 | 说明 |
|---|---|---|---|
| RPO(恢复点目标) | 实时/每日(取决于备份频率) | 1 个月 | 每月手动 rsync；丢失 ≤30 天数据 |
| RTO(恢复时间目标) | 分钟级(自动化恢复 + 一键 SQL) | 4 小时 | rsync 回拷 ~45GB(USB 3.0 ~100MB/s ≈ 8min) + 环境重建 |
| 备份粒度 | 表级/库级(痛点：粒度不足) | 文件级 | 每个 .parquet 独立可恢复 |
| 验证方式 | 数据对比引擎 + 差异报告 | 文件数量 + 抽样 DuckDB 查询 | |

**为什么不做更频繁的备份**：

```text
- 数据可重建: asset 层可以从数据源重新 fetch + normalize 完全重建(耗时约 1-2 天全市场)
- RPO 1 月可接受: 最坏情况丢失 1 个月数据,重新入库即可恢复—这不是交易系统,是研究系统
- 成本效益: 公司有专门的备份团队和 MinIO 集群;个人平台一块移动硬盘足矣
```

**灾备升级路径（如果将来需要）**：

```text
Level 0(当前): 每月手动 rsync → 外置硬盘
Level 1: cron 每周自动 rsync → 外置硬盘(永久挂载)
Level 2: rclone → 云存储(阿里云 OSS 归档型,~0.015 元/GB/月,45GB ≈ 0.7 元/月)
Level 3: 两地备份(本地硬盘 + 云 OSS)
```

### 51.9 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 数据分级 | raw(Class 2)/asset(Class 3-4)/quarantine 三层，不同保留策略 | 数据等级体系 Class 1-4(page 269215557) |
| asset 保留 | 永久 | 对标 Class 3/4 标准化数据，不可删除 |
| raw 保留 | 1 年滚动 | 对标 Class 2，可从源重新获取 |
| quarantine 保留 | 30 天 | 质检异常数据，超期即放弃 |
| 辅助日志保留 | 1 年 | correction/DQC/cross_check 明细 |
| 版本化 | 不做(覆盖写 + correction 日志) | UdataSQL 版本管理(page 288527084)是企业需求；个人平台用日志替代 |
| 冷热分层 | 不做—全 SSD | Udata scratch/vault 分层(page 270638954)；个人 <100GB 不值得 |
| 10 年容量 | **~45GB(不含 L2)，~84GB(含 L2)** | 参考 Udata 压缩比实测 5:1(page 270638954) |
| 容量告警 | <50GB WARNING / <10GB CRITICAL | 对标备份中心容量趋势监控(page 245317822) |
| 备份工具 | rsync(和公司执行平台一致) | 备份中心 rsync 执行模块(page 245317822 §3.2.3) |
| 备份频率 | 每月手动 | 数据可重建，RPO 1 月可接受 |
| RPO/RTO | RPO 1 月 / RTO 4 小时 | 对标备份中心 RTO/RPO 达成率监控 |
| 备份粒度 | 文件级(天然解决公司"粒度不足"痛点) | 备份中心痛点 2(page 245317822) |
| 清理代码 | `cleanup_expired()` ~40 行 + `check_capacity()` ~20 行 | |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.6** | 完整讨论稿落盘（Confluence 六页 + Class 分级映射 + 逐表容量 + 备份/RTO/RPO）——**已定 → §49/§51**（此前只有决策汇总骨架） |
| **§49.7 10 年容量** | **重估 ~78GB→~45GB（不含 L2）；~1.3TB→~84GB（含 L2）**。逐表推算：bars_minute 39GB 占大头；L2 按期货 50 万行/天 3.9GB/年（此前误按 5000 万行股票 tick 估 125GB/年，高估一个数量级）。**以本节为准** |
| **§49.8 备份范围** | **修订：只备份 research+config → 全量备份 data_platform（~45GB）**。容量重估后备份量降到 rsync 8min 可回拷，且文件级粒度天然支持全量；`backup.sh` 含 `--exclude='*.tmp'/'*.lock'` + parquet 文件数比对验证 |
| **§49.8 RTO 依据** | 重建耗时修订：2-4 小时 → 1-2 天全市场（更保守）；RTO 4h 主路径改为"rsync 回拷 8min + 环境重建"，重建仅作灾难兜底 |
| **§49.6 清理机制** | `lifecycle cleanup` 细化为 `cleanup_expired(root, dry_run=True)` + `lifecycle.yaml`（policy: permanent/rolling + keep_days）；默认 dry_run=True；`.jsonl` 日志同步清理 |
| **§49.7 告警** | 三档阈值落为 `capacity_alert.yaml`；**新增 data_growth 单日 >100MB 异常告警**（正常日增 ~16MB，防重复入库/数据膨胀） |
| **§3.6 / §39 raw 层** | 保留期 1 年的理由补齐：数据源本身就是归档（Tushare/AKShare 可重取）、覆盖 §3.11 30 天回看窗口、raw 与 asset 同体积 |
| **§50.8 cleanup_stale** | 与本节 `cleanup_expired` 分工明确：前者清写入过程残留（.tmp/.lock，1 小时阈值），后者清过期数据（按 keep_days 滚动） |
| **§38.5 并行回填** | 容量上无压力（单日 16MB），回填不受限 |
| **运维脚本** | 新增 `lifecycle.yaml` + `capacity_alert.yaml` + `data_platform/lifecycle.py`（`cleanup_expired`）+ `check_capacity` + `scripts/backup.sh` |

**本节仍未解决**：`parse_date_from_filename` 对非日期命名文件（reference 全量单文件）的处理（应只清理日期命名的滚动层）；raw 层清理按文件名日期还是 mtime 起算（§49 遗留）；data_growth 基线 100MB 是否按表分开（bars_minute 单表已 ~16MB，多表合计涨到 30-40MB 正常）；`factor` 永久保留的清理策略（因子淘汰时是标记 deprecated 还是物理删除）；备份验证只有文件数比对，是否加抽样 checksum（公司有数据对比引擎，个人平台是否值得）；rclone Level 2 云备份的启用触发条件（灾备升级路径的量化判断标准）。

---

## 52. 查询与访问层：DataAPI SQL-only + 缓存/慢查询/多进程约定 + 研究层性能（§4.7 全解）

> §4.7 的完整讨论稿。本节落盘统一查询接口（SQL-only DataAPI ~80 行完整代码）、`register_views` 路径管理、为什么不提供 DataFrame API、缓存策略（不做应用层缓存）、慢查询监控（`[COST]` + 5s/30s/60s 三档）、多进程/多线程访问约定、研究层性能要求（alphalens 日频 <1s / 分钟 <5s）、性能保证机制四步排查。与 §46（Parquet+DuckDB 视图模式无 .duckdb）、§47（11 表 schema）、§48（按日单文件 + `PARQUET_WRITE_CONFIG`）、§50（写读解耦 + snapshot isolation）衔接。**新增代码 ≈80 行**（DataAPI 类）。

### 52.1 Context 与 Confluence 参考

§4.1 确定 Parquet + DuckDB 视图模式（无持久化 .duckdb），§4.2 确定 11 张表 schema，§4.3 确定按日单文件扁平组织。本节定义**查询与访问层**：研究代码怎么拿数据、缓存怎么做、慢查询怎么监控、多进程怎么访问、性能要达到什么水平。

**Confluence 参考**：

```text
- UdataSQL Python API(page 288526910):UdataViewer 类提供 query_df/query_arrow/
  query_df_batch/query_arrow_batch/query_print 五种方法。SQL 中用文件路径作表名:
  SELECT * FROM '/path/to/schema.info' WHERE date >= 20220101。
  支持 profile=True 逐查询 profiling;statement_timeout 超时控制;UDF 注册。
  文档明确:"大多数情况下无需也不建议手动设定 threads_num, memory_limit, temp_directory"。
- UdataViewer 架构原理(page 288527133):三层架构—sqlglot 预过滤 → Arrow 批式生成器
  (64K 行/批) → DuckDB 执行。读路径完全无锁。
- Udata-vs-Parquet-Reader 性能基准(page 328170767):
  NASDAQ 6.52 亿行全扫 udata 10.6s/1096GB RSS vs polars 88.4s/365GB;
  A 股快照 2100 万行 polars 6.6s vs udata 9.2s(字符串列是 udata 弱点);
  单标的筛选 udata 1.1GB RSS vs 其他引擎 12GB;日内 10min 全引擎 0.4-1.1s。
- Nightly Benchmark(page 315824290):udataviewer 大表全扫 3.69x 快于 direct_parquet;
  COUNT 类 turbo 快 24-88x;复杂 SQL turbo 退化 5-40x;按 SQL 模式路由引擎。
- Udata 压缩性能(page 270638954):bar_data 一年单点查询 8s(8 线程)、
  截面 12s(含解压)。
```

### 52.2 借鉴的核心模式

公司的 UdataViewer 是一套完整的查询中间层（sqlglot 预过滤 + Arrow 批式生成器 + DuckDB）。映射到个人平台：

| 公司实践 | 个人平台等价物 | 说明 |
|---|---|---|
| `query_df` / `query_arrow` | `DataAPI.query()` / `DataAPI.query_arrow()` | 同名同义 |
| SQL 中文件路径作表名 `'/path/schema.info'` | `read_parquet('asset/{table}/*.parquet')` | DuckDB 原生能力 |
| `query_df_batch` / `query_arrow_batch` | 暂不需要 | 个人最大表单日 ~127 万行，一次 fetchdf 足够 |
| `profile=True` | 同样支持（DuckDB `enable_profiling`） | 一行开关 |
| `statement_timeout` | 同样支持（DuckDB `SET statement_timeout`） | 防失控查询 |
| UDF 注册 | 不封装，用 DuckDB 原生 `CREATE FUNCTION` | 个人场景够用 |
| `interrupt()` 中断查询 | 暂不需要 | 单用户单进程，Ctrl+C 即可 |
| sqlglot 预过滤 | **不需要** | DuckDB `read_parquet` 自带列裁剪 + row group pruning |
| `[COST]` 5 条结构化日志 | 简化为 total_time 一行 | 个人不需要分布式追踪 |

**关键洞察：公司 UdataViewer 的三层复杂度（sqlglot 预过滤 / Arrow 批式生成器 / 字符串转换 / 维度展开）全部源于 Udata 私有格式——DuckDB 不能直接读 Udata，需要中间层转换成 Arrow。个人平台用标准 Parquet + DuckDB `read_parquet()` 原生已包含所有优化（列投影 / row group pruning / predicate pushdown），不需要中间层。DataAPI 只需薄薄一层路径管理。**

### 52.3 统一查询接口：DataAPI（回应子问题 1）

**为什么是 SQL-only**：见 §52.4。先看完整类：

```python
import duckdb
from pathlib import Path

class DataAPI:
    """
    统一查询接口。
    对标 UdataViewer(page 288526910)—SQL 查询返回 DataFrame/Arrow。
    """
    def __init__(self, data_root="data_platform/asset", threads=None,
                 memory_limit=None, statement_timeout=60.0):
        self._root = Path(data_root)
        self._conn = duckdb.connect()  # 内存模式,无 .duckdb 文件
        if threads:
            self._conn.execute(f"SET threads = {threads}")
        if memory_limit:
            self._conn.execute(f"SET memory_limit = '{memory_limit}'")
        if statement_timeout:
            self._conn.execute(f"SET statement_timeout = '{int(statement_timeout*1000)}ms'")

    def query(self, sql, profile=False):
        resolved = self._resolve_table_refs(sql)
        if profile:
            self._conn.execute("PRAGMA enable_profiling = 'json'")
        result = self._conn.execute(resolved).fetchdf()
        if profile:
            prof = self._conn.execute("PRAGMA last_profiling_output").fetchone()
            return result, prof
        return result

    def query_arrow(self, sql):
        return self._conn.execute(self._resolve_table_refs(sql)).fetch_arrow_table()

    def register_views(self):
        for table_dir in self._root.iterdir():
            if table_dir.is_dir():
                glob_path = str(table_dir / "*.parquet")
                self._conn.execute(f"""CREATE OR REPLACE VIEW {table_dir.name} AS
                    SELECT * FROM read_parquet('{glob_path}',
                        union_by_name=true, hive_partitioning=false)""")

    def _resolve_table_refs(self, sql):
        # 简单实现:靠 register_views 注册的视图,SQL 中直接写表名
        # 完整实现:sqlglot 解析表名→read_parquet 路径替换(个人场景不需要)
        return sql

    def close(self):
        self._conn.close()

    def __enter__(self):
        self.register_views()
        return self

    def __exit__(self, *args):
        self._conn.close()
```

**使用示例**：

```python
with DataAPI() as api:
    df = api.query("""
        SELECT date, ticker, close
        FROM bars_daily
        WHERE date BETWEEN '2024-01-01' AND '2024-12-31'
          AND ticker = '000001.SZ'
    """)
```

**为什么 `__enter__` 里调 `register_views`**：视图是惰性的（不物化数据），注册成本 ~10 个 `CREATE VIEW` 语句，毫秒级。SQL 中直接写表名比写 `read_parquet(...)` 路径清爽得多——这正是公司"SQL 中用文件路径作表名"的体验等价物。

**`union_by_name=true`**：§4.2 schema 演进时新列加入旧文件没有，union_by_name 按列名对齐而非按位置。

**`hive_partitioning=false`**：§48 确定扁平日期文件不用 Hive 分区，显式关闭避免歧义。

### 52.4 为什么不提供 DataFrame API / 对象 API

公司的数据出口分三层：

```text
- SQL 层:    UdataViewer.query_df()        ← 个人平台只有这一层
- Python API: Udata_Factory.GetPandasDataFrame()
- C++ API:   GetArray()
```

**个人平台只做 SQL 层**，理由：

```text
1. read_parquet() 已经一样快——DataFrame API 内部最终也是 SQL/扫描,没有免费午餐
2. SQL 最通用——因子计算、回测、分析都能表达,不绑 pandas API
3. 不值得维护两套 API——个人开发资源有限,薄薄一层 SQL 足矣
4. 极致性能场景直接用 pd.read_parquet()/polars.scan_parquet()——绕过 DataAPI,
   DataAPI 不挡路
```

### 52.5 缓存策略（回应子问题 2）

**不做应用层缓存**：

```text
- 全数据 ~45GB(§51 容量重估),NVMe ~3GB/s,冷读全量 15s
- 常用数据 OS page cache 自动缓存——第二次查询快 10-100x,零代码
- 一致性问题不值得:缓存失效、TTL、内存占用都是坑
```

**缓存层级**：

```text
OS page cache ← DuckDB 内部缓存 ← 应用层缓存(不做)
```

**唯一的"缓存"**：`register_views` 的视图元数据（视图惰性不物化，本质是路径映射表）。

**为什么不学公司的 DataPool 懒加载缓存**（page 222727558）：公司 Pylib 的 DataPool 解决的是"多因子反复读同一块数据"的场景，数据量大、读取昂贵。个人数据小（单日 bars_minute 127 万行 ~82MB 未压缩），每次现查 OS cache 已命中，应用层缓存是负优化。

### 52.6 慢查询监控（回应子问题 3）

```python
import time
import logging

logger = logging.getLogger(__name__)

# DataAPI.query() 内部计时
t0 = time.perf_counter()
result = self._conn.execute(resolved).fetchdf()
elapsed = time.perf_counter() - t0

logger.info(f"[COST] time={elapsed:.3f}s rows={len(result)} sql={sql[:200]}")
if elapsed > 5.0:
    logger.warning(f"[SLOW] time={elapsed:.3f}s rows={len(result)} sql={sql[:200]}")
```

**阈值配置**（`query_config.yaml`）：

```yaml
# query_config.yaml
slow_query:
  warning: 5.0      # >5s 输出 WARNING
  critical: 30.0    # >30s 认为查询设计有问题
  timeout: 60.0     # statement_timeout,强制中断
```

**为什么 5s**：bars_minute 一天 127 万行查询 <1s；一年 ~3 亿行 2-5s。>5s 说明查询有问题（漏了 WHERE、全表扫、没用上 row group pruning），而不是"数据太多"。

**对标公司 `[COST]` 日志**（page 288526910）：公司输出 5 条结构化日志（解析/预过滤/生成/执行/总耗时），个人简化为 total_time 一行——单机单进程不需要分布式追踪。

### 52.7 多进程/多线程访问约定（回应子问题 4）

**四条规则**：

```text
1. 每个进程自己的 DataAPI 实例(内存模式 duckdb.connect())
2. 不跨进程共享连接(DuckDB 连接不是进程安全的)
3. 同进程多线程查询同一个 DataAPI——DuckDB 连接线程安全
4. 不手动设 threads——DuckDB 自动用满 CPU(公司文档同样建议不设)
```

**场景映射**：

| 场景 | DataAPI 用法 |
|---|---|
| Jupyter 交互研究 | 1 个实例，复用整个 session |
| 因子计算脚本 | 1 个实例，`with DataAPI() as api:` |
| 入库流水线 | **不经过 DataAPI**——直接 `write_parquet_atomic`（§50） |
| alphalens 分析 | 1 个实例，用完关闭 |
| 多因子 multiprocessing | 每个 worker 各自实例 |

**不用 DuckDB 持久化的单写多读模式**：§4.1 决定不用 `.duckdb` 文件。若用持久化，DuckDB 有 Single Writer / Multiple Readers 约束（仅同进程，不支持跨进程写）——但个人平台 DuckDB 只读不写，永远没有写冲突。

**和 §4.5/§50 的衔接**：写入走 `flock + tmp + rename`（§50），读取走 `read_parquet`（本节），**完全解耦**——Linux inode 语义天然提供 snapshot isolation，读写零协调。

### 52.8 研究层性能要求（回应子问题 5）

**公司三个数据点**（换算到个人规模）：

```text
公司(page 328170767 / 315824290 / 270638954):
- Nasdaq OT 全量 6.52 亿行: udata 10.6s / polars 88.4s
- 单标的 445 万行: 3.6-4.9s
- 日内 10min 17 万行: 0.56-1.06s
- SQL Benchmark 全表扫: udata 37.84s vs direct_parquet 139.56s
  COUNT: 0.55s vs 0.15s; 聚合: 2.57s vs 1.55s; 子查询: 2.51s vs 6.62s
- bar_data 一年截面 12s / 单点 8s

个人规模差 ~500 倍(A 股 ~5300 支 vs Nasdaq ~8000,但只到分钟级、4h vs 6.5h)。
```

**DuckDB + NVMe 预期性能**：

```text
单日 bars_minute(127 万行): <100ms
一年 bars_minute(~3 亿行): 2-3s
十年 bars_minute(~30 亿行): 20-30s
```

**性能目标（写进验收标准）**：

| 场景 | 数据量 | 目标 |
|---|---|---|
| alphalens 单因子 1 年 daily | 128 万行 | **<1s** |
| alphalens 分钟因子 1 年 | 3 亿行 | **<5s** |
| 单股 10 年 daily | 2,420 行 | **<100ms** |
| 截面因子单日全市场 | 5,300 × 50 因子列 | **<200ms** |
| IC 序列 10 年 daily | 1,282 万行 | **<2s** |
| 全市场全历史（灾难场景） | 30 亿行 | **<30s** |

### 52.9 性能保证机制：四步排查

查询慢时按序排查：

```text
1. profile=True 看算子——DuckDB profiling 输出每个算子耗时,
   看是 scan 慢还是 join/groupby 慢
2. EXPLAIN 查 row group pruning——是否扫了不该扫的日期
   (parquet 文件级 statistics 应排除;排除不了说明 WHERE 列不在 row group key)
3. EXPLAIN 查列投影——是否读了不需要的列(SELECT * 大忌)
4. 终极对照: polars scan_parquet 同查询——polars 也慢则是 IO 瓶颈,
   polars 快则是 DuckDB 查询写法问题
```

### 52.10 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 查询接口 | **SQL-only** DataAPI ~80 行，不提供 DataFrame API | UdataViewer query_df(page 288526910)；Udata 三层出口只取 SQL 层 |
| 返回格式 | `query()`→DataFrame / `query_arrow()`→Arrow Table | query_df / query_arrow |
| 表名解析 | `register_views()` + `read_parquet()` glob | 公司文件路径作表名；DuckDB 原生列裁剪+pruning 使 sqlglot 预过滤不需要 |
| 预过滤 | **不做** sqlglot 预过滤 | UdataViewer 三层架构(page 288527133)的预过滤是为 Udata 私有格式服务 |
| 批式接口 | `query_df_batch` **暂不需要** | 个人最大表单日 127 万行 |
| 缓存 | **不做应用层缓存**——只用 OS page cache + DuckDB 内部 | DataPool 懒加载(page 222727558)是大数据场景；个人 <1s 查询不需要 |
| 慢查询 | `[COST]` 日志 + >5s WARNING + >30s CRITICAL + timeout 60s | 公司 `[COST]` 5 条日志简化为 1 条 |
| statement_timeout | 60s 默认 | UdataSQL Python API(page 288526910) |
| profiling | `profile=True` 一行开关 | 同上 |
| 多进程 | 每进程独立 DataAPI，不共享连接 | DuckDB 连接约束 |
| 多线程 | 同进程多线程共享 DataAPI（DuckDB 线程安全）；不手动设 threads | 公司文档"无需也不建议手动设定 threads_num" |
| 研究层性能 | alphalens 日频 <1s / 分钟 <5s；全历史 <30s | 公司基准(page 328170767/315824290)按规模差 ~500 倍折算 |
| 写读解耦 | 写入 flock+tmp+rename（§50），读取 read_parquet，零协调 | UdataViewer Arrow 生成器无锁读(page 288527133) |
| 新增代码 | DataAPI 类 ≈ **80 行** | |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.7** | 完整讨论稿落盘——**已定 → §52**（此前无状态标记） |
| **§4.1 / §46.1** | DataAPI 落地为 `duckdb.connect()` 内存模式 + `register_views` 视图注册；确认无 `.duckdb` 文件 |
| **§25.2 / §27.6** | DataAPI 从"统一取数入口"骨架落为完整类（query/query_arrow/profile/timeout/context manager） |
| **§48.2 文件级过滤** | 由 `register_views` 的 glob + DuckDB row group pruning 实现；按 TradingCalendar 枚举文件列表的优化保留为可选项 |
| **§50.5 并发读** | 与本节多进程约定互补：写入侧 flock，读取侧每进程独立连接，inode 语义隔离 |
| **查询配置** | 新增 `query_config.yaml`（warning 5.0 / critical 30.0 / timeout 60.0） |
| **Confluence 索引** | 新增五页引用：page 288526910（Python API）/ 288527133（架构原理）/ 328170767（性能基准）/ 315824290（Nightly Benchmark）/ 270638954（压缩性能） |

**本节仍未解决**：`_resolve_table_refs` 简单实现依赖预注册视图，SQL 中子查询/CTE 内的表名是否也能解析（应可以，视图全局可见）；`query_config.yaml` 的 threshold 是否按查询类型细分（全表扫 vs 点查阈值不同）；profile 输出的落盘方式（stdout vs jsonl 文件）；Jupyter 长 session 中 DataAPI 连接失效的重连语义；`query_arrow` 返回 Arrow Table 后零拷贝到 polars 的路径是否需要封装。

---

## 53. 与三件套的数据对接：load_bars + to_xxx 适配层 + 口径归一（§4.8 全解）

> §4.8 的完整讨论稿。§52 定义了 DataAPI（SQL ~80 行），本节是**最后一公里**：backtrader / alphalens / vnpy 三件套的格式转换。落盘 `load_bars()`（~50 行）+ 三个适配函数（各 ~20 行）+ 口径差异归一总表（复权/时间戳/字段命名/标的格式）+ 内存占用与分块策略 + 完整使用示例。与 §43（§3.10 复权：asset 层原始价格 + adjfactor，PriceAccessor 按需前复权）、§52（DataAPI）衔接。**新增代码 ≈110 行**（load_bars ~50 + 三个 to_xxx 各 ~20）。

### 53.1 Context 与 Confluence 参考

§4.7/§52 定义了 DataAPI（SQL 查询返回 DataFrame/Arrow）。但三件套需要的是**特定格式的 DataFrame**——backtrader 要 DatetimeIndex 的 OHLCV、alphalens 要 pivot 后的宽表 + MultiIndex 因子 Series、vnpy 要带 turnover 的 bar 记录。本节定义格式转换与口径归一。

**前置依赖**：§4.10 复权（asset 层存原始价格 + adjfactor，PriceAccessor 按需前复权）——本节的 `adjust` 参数完全遵循 §3.10/§43 的复权分流决策。

**Confluence 参考**：

```text
- Udata MarketData Python API(page 288526845):GetPandasDataFrame(date=, security_id=,
  columns=) 按需读取—列投影 + 日期 + 标的过滤。
  流式路径比 GetTable().to_pandas() 快 2-4 倍、峰值内存低 50-70%。
  GetSortedPandasDataFrame 避免读后排序,性能翻倍。
- Pylib DataPool/DataLoader(page 222727558):懒加载—"真正要用到时才加载";
  DataPool 缓存;替换 datamanager"配一堆 field 放入 dict"的做法。
- Usignal 框架(page 244048834):"非常薄的框架结构,只提供事件驱动";
  数据层"搭配但不绑定"udata。
- NewPysim(page 222727276):"空洞的…没有固定范式,提供兼容工具链"。
- UdataViewer SQL API(page 288526910):query_df→DataFrame 是标准出口。
- Udata-as-Parquet-Reader 性能基准(page 328170767):A 股快照 2100 万行
  polars 6.6s vs udata 9.2s—个人规模 127 万行 <100ms,性能不是问题。
```

### 53.2 借鉴的核心模式

**公司链路**：

```text
Udata → GetPandasDataMatrix / query_df → DataFrame → usignal / pysim 消费
```

**个人平台链路**：

```text
Parquet → DataAPI.query() / load_bars() → DataFrame → backtrader / alphalens / vnpy 消费
```

| 公司实践 | 个人平台等价物 | 说明 |
|---|---|---|
| `GetPandasDataFrame(date, security_id, columns)` | `load_bars(symbol, start, end, fields)` | 列投影 + 过滤 + 返回 DataFrame |
| DataPool 懒加载缓存 | 不做缓存 | 数据小，每次现查（§52.5） |
| usignal 事件驱动消费 | DataFeed / alphalens input | 适配层输出目标格式 |
| schema 固定（TypeInfo） | §4.2 表 schema 固定 | 字段命名已对齐，转换极小 |

**关键洞察：公司路径上的大量优化（C++ 解码直连 NumPy / 流式读取 / 排序预取）是为亿级行服务的。个人日频 128 万行、分钟频单日 127 万行，简单 SQL 就足够快。"数据对接"的核心工作不是性能，而是口径归一——复权、时间戳、字段命名三件事。**

### 53.3 统一加载函数：load_bars（回应子问题 1）

**为什么提供**：三件套需要"特定格式的 DataFrame"而非 SQL 结果。`load_bars` = `DataAPI.query()` + 格式转换，对标公司的 `GetPandasDataFrame`。

**签名**：

```python
def load_bars(symbol: str | list[str] | None = None,
              start: str = "2020-01-01",
              end: str = "2024-12-31",
              freq: str = "daily",
              fields: list[str] | None = None,
              adjust: str = "none",
              asset_type: str = "equity") -> pd.DataFrame:
    """
    加载 K 线数据,返回 columns:
      date, ticker, open, high, low, close, volume, amount, [adjfactor]
    按 (date, ticker) 排序。
    """
```

**实现**：

```python
def load_bars(symbol=None, start="2020-01-01", end="2024-12-31",
              freq="daily", fields=None, adjust="none", asset_type="equity"):
    # 表映射
    table_map = {
        ("equity", "daily"): "bars_daily",
        ("equity", "minute"): "bars_minute",
        ("futures", "daily"): "bars_daily_futures",
        ("continuous", "daily"): "continuous",
    }
    table = table_map[(asset_type, _base_freq(freq))]

    # 构建 SQL
    cols = ", ".join(fields) if fields else "*"
    where = [f"date BETWEEN '{start}' AND '{end}'"]
    if symbol:
        if isinstance(symbol, str):
            where.append(f"ticker = '{symbol}'")
        else:
            where.append(f"ticker IN ({', '.join(symbol)})")
    sql = f"SELECT {cols} FROM {table} WHERE {' AND '.join(where)} ORDER BY date, ticker"

    # 5min/60min 聚合(§4.2)
    if freq in ("5min", "60min"):
        sql = _aggregate_bars_sql(table, freq, where, fields)

    with DataAPI() as api:
        df = api.query(sql)

    # 复权(§3.10)
    if adjust != "none" and "adjfactor" in df.columns:
        price_cols = [c for c in ["open", "high", "low", "close"] if c in df.columns]
        if adjust == "forward":
            latest_adj = df.groupby("ticker")["adjfactor"].transform("last")
            ratio = df["adjfactor"] / latest_adj
        elif adjust == "backward":
            first_adj = df.groupby("ticker")["adjfactor"].transform("first")
            ratio = df["adjfactor"] / first_adj
        for col in price_cols:
            df[col] = df[col] * ratio

    return df
```

**复权逻辑**（§3.10/§43）：

```text
- 前复权(forward):  以最新 adjfactor 为基准,历史价格 × (adjfactor / latest_adj)
  → 最新价格不变,历史价格缩放——回测最常用
- 后复权(backward): 以首个 adjfactor 为基准——连续收益率计算用
- 不复权(none):     原始价格——实盘/vnpy 用(§3.10 零转换)
```

### 53.4 三件套适配层（回应子问题 2/3）

#### 53.4.1 backtrader：用自带 PandasData（回应子问题 2）

**决策：用 backtrader 自带的 `bt.feeds.PandasData`，不自己写 DataFeed。**理由：

```text
1. 够用——自带 feed 支持 OHLCV + openinterest + 自定义列
2. 自写 DataFeed 仅在"流式实时数据"场景有必要——个人平台是批量回测
3. 减少维护——自己写的 feed 要跟着 backtrader 版本升级改
```

```python
def to_backtrader_feed(symbol, start, end, freq="daily", adjust="forward"):
    import backtrader as bt
    df = load_bars(symbol=symbol, start=start, end=end, freq=freq, adjust=adjust)

    # 时间戳:date → DatetimeIndex(minute 用 date + time)
    df["datetime"] = pd.to_datetime(df["date"])
    if freq == "minute":
        df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])
    df = df.set_index("datetime")

    # 字段:open/high/low/close/volume 零映射;openinterest 期货才有,股票置 0
    if "openinterest" not in df.columns:
        df["openinterest"] = 0

    return bt.feeds.PandasData(dataname=df)
```

#### 53.4.2 alphalens：prices 宽表 + factor MultiIndex Series

```python
def to_alphalens_prices(start, end, symbols=None):
    df = load_bars(symbol=symbols, start=start, end=end, freq="daily",
                   fields=["date", "ticker", "close", "adjfactor"], adjust="forward")
    df["date"] = pd.to_datetime(df["date"])
    # naive DatetimeIndex(§3.9)
    return df.pivot(index="date", columns="ticker", values="close")


def to_alphalens_factor(factor_df):
    factor_df = factor_df.copy()
    factor_df["date"] = pd.to_datetime(factor_df["date"])
    factor_df = factor_df.set_index(["date", "ticker"])
    return factor_df["factor_value"]
```

#### 53.4.3 vnpy：未复权 + turnover + symbol/exchange 拆分

```python
def to_vnpy_bars(symbol, start, end, freq="minute"):
    # 实盘价格,未复权(§3.10 零转换)
    df = load_bars(symbol=symbol, start=start, end=end, freq=freq, adjust="none")

    df["datetime"] = pd.to_datetime(df["date"])  # minute: date + " " + time
    df = df.rename(columns={"amount": "turnover"})
    df["symbol"] = df["ticker"].str.split(".").str[0]
    df["exchange"] = df["ticker"].str.split(".").str[1]
    return df
```

### 53.5 口径差异归一总表（回应子问题 3）

**核心原则**（借鉴 Usignal "数据层搭配但不绑定"，page 244048834）：**存储层中性（未复权 + 统一 schema），适配层按消费端转换。**

| 口径维度 | 存储层（asset） | backtrader | alphalens | vnpy |
|---|---|---|---|---|
| 复权 | 未复权 + adjfactor | 前复权 | 前复权 | **未复权** |
| 时间戳 | date VARCHAR | DatetimeIndex | DatetimeIndex | datetime 对象 |
| 时间列（分钟） | time VARCHAR | 合入 index | N/A（日频） | 合入 datetime |
| 字段名 | ohlcv 同名 | 零映射 | 零映射 | amount→turnover |
| 标的格式 | ticker "000001.SZ" | 不需要 | columns=tickers | 拆 symbol+exchange |
| 缺失值 | NaN | 自动处理 | dropna | 预过滤 |
| 排序 | (date, ticker) | datetime 升序 | pivot 自动 | datetime 升序 |

**适配工作量极小的原因**：§4.2 表 schema 的字段命名已对齐三件套（open/high/low/close/volume/amount），唯一的转换是**复权 + 时间戳**两件事。字段命名对齐是 §47 schema 定稿时的刻意决策。

### 53.6 内存占用与分块策略（回应子问题 4）

**公司应对**（page 288526845）：流式路径低 50-70% 峰值内存 / `GetSortedPandasDataFrame` / `get_reader` 64K 批式 / UdataViewer 批式不物化。

**个人平台估算**：

```text
bars_daily 一年全市场:   82MB      → 全量加载无压力
bars_minute 一天全市场:  82MB      → 全量加载无压力
bars_minute 一年全市场:  ~3 亿行 20GB  → 32GB RAM 会 OOM!
bars_minute 一月全市场:  2540 万行 1.6GB → 可以
bars_minute 十年全市场:  ~200GB   → 绝对不能全量
```

**分块接口**（对标 `query_df_batch`，page 288526910——公司按行数分批，个人按时间分块更自然）：

```python
def load_bars_chunked(symbol=None, start="2020-01-01", end="2024-12-31",
                      freq="minute", chunk_by="month", **kwargs):
    for chunk_start, chunk_end in _date_chunks(start, end, chunk_by):
        yield load_bars(symbol=symbol, start=chunk_start, end=chunk_end,
                        freq=freq, **kwargs)
```

**分块建议**：

| 场景 | 建议 |
|---|---|
| 单股任意时段 | 全量（单股 10 年分钟 58 万行 37MB） |
| 全市场 daily 全量 | 全量（10 年 820MB） |
| 全市场 minute 1 天 | 全量（82MB） |
| 全市场 minute 1 月 | 全量（1.6GB） |
| 全市场 minute >3 月 | **按月分块**（>5GB） |
| 全市场 minute 1 年+ | **按月分块**（20GB+） |

**backtrader 不用担心**：单股 10 年分钟 58 万行 37MB——单标的回测内存无压力。alphalens 主用日频。

### 53.7 完整使用示例

**backtrader**：

```python
import backtrader as bt

cerebro = bt.Cerebro()
cerebro.adddata(to_backtrader_feed("000001.SZ", "2020-01-01", "2024-12-31",
                                   freq="daily", adjust="forward"))
cerebro.addstrategy(MyStrategy)
cerebro.run()
```

**alphalens**：

```python
from alphalens.tears import get_clean_factor_and_forward_returns

prices = to_alphalens_prices("2020-01-01", "2024-12-31")
factor = to_alphalens_factor(factor_df)
get_clean_factor_and_forward_returns(factor, prices, ...)
```

**vnpy**：

```python
from vnpy_ctastrategy.backtesting import BacktestingEngine

df = to_vnpy_bars("000001.SZ", "2024-01-01", "2024-12-31", freq="minute")
engine = BacktestingEngine()
# engine.add_data(df) → 回测
```

### 53.8 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 统一加载 | `load_bars() → DataFrame`（~50 行） | GetPandasDataMatrix(page 288526845) |
| backtrader feed | 用自带 `bt.feeds.PandasData`，不自写 | NewPysim"空洞框架"哲学(page 222727276) |
| 字段映射 | ohlcv 零映射；amount→turnover（仅 vnpy）；openinterest 置 0 | §4.2 schema 已对齐 |
| 复权分流 | BT+AL **前复权** / VN **未复权** | §3.10/§43 复权决策 |
| 时间戳 | 适配层 `pd.to_datetime`（minute 用 date+time 合并） | §3.9 naive 时间 |
| 全量 vs 分块 | 日频全量；分钟 >3 月按月 chunk | query_df_batch(page 288526910)按行数→按时间 |
| 内存安全 | 单月 1.6GB；一年 20GB 必须分块 | GetPandasDataFrame 流式路径(page 288526845) |
| 设计哲学 | "搭配但不绑定"(page 244048834) + "空洞框架"(page 222727276) | Usignal/NewPysim |
| 新增代码 | load_bars ~50 + 三个 to_xxx 各 ~20 ≈ **110 行** | |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§4.8** | 完整讨论稿落盘——**已定 → §25.2 / §27.6–§27.7 / §53**（此前"返回长表列全集仍开放"已由 §53.3 签名关闭） |
| **§25.2 / §27.6** | DataAPI 统一取数的下游落为 `load_bars` + 三个 `to_xxx`；AlphalensAdapter 四个静态方法并入 `to_alphalens_prices` / `to_alphalens_factor` |
| **§3.10 / §43.3** | 复权在加载层实现（`adjust` 参数），PriceAccessor 保留为独立工具类；三件套复权分流表落盘 |
| **§47 表 schema** | 字段命名对齐三件套的价值在本节兑现——唯一转换是复权 + 时间戳 |
| **§52 DataAPI** | 本节的 `load_bars` 是 `DataAPI.query()` 的包装；分块接口对应 `query_df_batch` 的时间分块变体 |
| **新增代码** | `load_bars`（含 `_base_freq` / `_aggregate_bars_sql` / `_date_chunks` 辅助）+ `to_backtrader_feed` / `to_alphalens_prices` / `to_alphalens_factor` / `to_vnpy_bars` + `load_bars_chunked` |

**本节仍未解决**：`_aggregate_bars_sql` 的 5min/60min 聚合边界处理（跨日 bar、午休 11:30-13:00）；vnpy 的 `exchange` 枚举映射（不再需要——§66.2 不用 backtrader/vnpy，2026-10-10 回改）；alphalens `get_clean_factor_and_forward_returns` 的默认 quantiles/periods 参数取值；`load_bars_chunked` 的 `_date_chunks` 是否跳过非交易日（按自然月 vs 按交易月）；backtrader 分钟回测的 `time` 列格式（不再需要——§66.2 不用 backtrader/vnpy，2026-10-10 回改）；期货连续合约（continuous 表）的复权语义（主力连续 vs 指数连续，§3.9/§42 换月逻辑的衔接）。

---

## 54. udata 全解与不引入决策：Schema+容器抽象 / 三维矩阵粒度 / DataBus 上下游 / k-slice + 概念映射总表（§5.1–§5.6）

> §5.1–§5.6 的完整讨论稿。本节先搞清楚公司体系里 udata 到底是什么（形态 / 粒度 / 与 DataBus 关系 / 与 k-slice 的区别，四问四答），再做核心决策：**个人平台不引入 udata 容器层**（Parquet + DuckDB 已覆盖 udata 核心功能），并落盘两张总表——udata 功能 9 项等价物映射、公司 udata 概念 17 项到个人平台的映射总表。与 §46（Parquet+DuckDB）、§47（表 schema）、§52（DataAPI）、§53（load_bars/三件套）、§3.11（不做版本化）衔接。**新增代码 0 行**（不引入新抽象层）。

### 54.1 Context 与 Confluence 参考

§4 系列完成了存储层（Parquet）、查询层（DataAPI/DuckDB）、对接层（三件套适配函数）的设计。现在进入第 5 章——公司体系里 udata 是统一数据容器，回测加载的核心载体。问题：个人平台要不要引入 udata 这一层？先搞清楚 udata 到底是什么。

**Confluence 参考**：

```text
- Udata 架构(page 244048875):Udata 由两部分组成—数据容器层(Ucube)和 schema。
  容器层存放实际数据,Schema 是"说明书",包含容器层路径、维度、标签、生产状态、
  依赖关系。"一个 Udata 对象可以对应一个或多个容器对象"。
- Udata 构成(page 199033106):SchemaBase 包含 udata_type_str、params、
  datafile_path、type_info(TypeInfo 描述字段类型/offset/bytesize)、
  prod_status(生产状态)、lineage(依赖关系)。
  TypeInfo 的主要消费者是 Python 端—Python 可通过 TypeInfo 在不知道 Udata 类
  具体定义的情况下读取容器层数据。
- Udata Schema(page 244048881):所有 Udata 必须包含:容器层路径、单元数据结构的
  TypeInfo、当前 Layout、生产状态、数据血缘。常见维度定义:日期列表、
  Universe(标的列表,二维矩阵 [di, ii])、TimeAxis(时间轴节点)。
- Udata API(page 198085912):新 Udata 是"一类可以自由定义的数据结构的统称"。
  每个具体的 Udata 可以完全自定义底层存储和组织方式以及访问接口—连数据访问接口
  都没有规定。支持 View 模式(通过映射读取其他 Udata 的数据)。
  Compatible 接口支持 Equal/Superset/Incompatible 判断。
- Udata_BarData(page 199033099):核心 Udata 类型。三维矩阵 [di, ti, ii]
  (日期x时间x合约)。四种 Layout:Packed_Ti_Ii / Packed_Ii_Ti /
  Unpacked_Ti_Ii / Unpacked_Ii_Ti。访问接口 Bar(di, ti, ii) 单点读取、
  Slice(di_slice, ti_slice, ii_slice) 批量截取。Slice 就是"k-slices"—
  对三维矩阵的任意维度做切片。
- BarData Python(page 257678105):Python 端 BarData 总是带 TimeAxis 和
  UniverseCoreData。工厂方法 UData_BarData_Builder(UniverseData, TimeAxisData,
  ValueType)。单点 Bar(di, ti, ii),底层 GetArry() 返回 ucube tensor。
- Udata 时间维度分段(page 240741315):物理 Udata(一个 schema + 一个 ucube
  tensor 文件) vs 逻辑 Udata(多个物理 Udata 的聚合)。支持切分(Split by date)、
  合并(Merge)、扩展(Extend)。需求来源:训练集/测试集分离需要独立权限控制。
- UdataSQL 概览(page 288526808):UdataSQL 支持两种类型—基于 parquet-like 的
  OrderTrade Udata 和基于 ucube tensor 的 udata(如 BarData)。
  非 MarketData 路径:Udata → GetArry() → numpy → C++ 维度展开 → Arrow → DuckDB。
  MarketData 路径:扁平化,无需维度展开。
- DataBus 交易总线(page 156420118, 335938903):DataBus 是低延迟发布订阅系统,
  连接交易系统各组件。传输 ubiqmessage,基于 Topic 的 pub/sub 模型。
  SDK 直读共享内存(数据面),Agent 管控制面。DataBus 传输的是实时行情消息,
  不是 Udata 容器—Udata 是落盘后的数据产品,DataBus 是实时光。
- Usignal 框架(page 244048834):usignal 可以"读实时行情,然后事件驱动,
  切 20 个不同频率的 bar"。"切 bar"是 usignal 的功能—从实时 tick 流聚合成
  不同频率的 K 线(bar),结果存入 Udata_BarData。
  usignal 还附带"全新的 OrderBook 和切 bar 工具"。
```

### 54.2 udata 是什么形态（回应 §5.1 Q1：内存对象还是文件格式）

**答：两者都是，但本质是"有 schema 的容器抽象"。**

从 Confluence 原文可以精确还原 udata 的形态：

```text
Udata = Schema(人类可读文本文件) + 容器层(存储实际数据)
```

容器层的 backend 不是固定的（page 244048834 明确说"udata 不再仅是 file-backed shm，
它将可以是纯内存、bin 文件、分布式内存等任意的 backend"）。实际上：

| 维度 | 公司 Udata | 说明 |
|---|---|---|
| 当前默认 backend | Ucube（C++ tensor，file-backed mmap） | 数据以 mmap 文件形式存储，读取时零拷贝映射到内存 |
| 可选 backend | 纯内存、bin 文件、分布式内存 | page 244048834："由用户自由选择" |
| Schema | 独立文本文件（schema.info） | 包含 TypeInfo + 维度信息 + 生产状态 + 血缘 |
| 访问方式 | C++ `Bar(di,ti,ii)` / Python `GetArry()` | 通过坐标系统访问三维矩阵 |

所以 udata 不是单纯的内存对象也不是单纯的文件格式——它是一个**带自描述 schema 的数据抽象层，底层存储可插拔**。公司选择 Ucube（mmap）是因为：

```text
1. 需要在 C++ 和 Python 之间零拷贝共享数据
2. 需要支持超大规模数据(6.52 亿行 NASDAQ tick)
3. 需要支持多进程通过共享内存并发读取
```

### 54.3 udata 的粒度（回应 §5.1 Q2：一条 udata 对应什么）

**答：一个 Udata_BarData 对象 = 多日 × 多时间段 × 多合约的三维矩阵。**

从 page 199033099 精确描述：

```text
Udata_BarData: 三维矩阵 [di, ti, ii]
  di = 日期编号(date index)
  ti = 时间段编号(time index, 日内 bar 的时间轴位置)
  ii = 合约编号(instrument index)
```

不是"一条 udata 对应一个合约"，而是一个 udata 对象涵盖：

```text
- 多个日期(dates 列表)
- 每天的完整时间轴(TimeAxis, 如 09:31~15:00 的 240 个 1min 时间段)
- 每天的完整合约列表(Universe, 每天的标的列表可能不同)
```

Schema 中的 `vir_ii_num`（合约维度大小）取"每日最大合约数之上的最小的 2^n"——为了对齐内存。

**具体例子**（A 股日内 1min bar）：

```text
di = [20240101, 20240102, ..., 20241231]   (242 个交易日)
ti = [09:31, 09:32, ..., 15:00]            (240 个 1min 时间段)
ii = [000001.SZ, 000002.SZ, ..., 最大 8192 个合约]  (2^13 对齐)

数据存储 = 242 × 240 × 8192 × sizeof(BarStruct)
        = ~476M 个数据单元
```

这就解释了为什么公司需要 Ucube（mmap tensor）——这种三维密集矩阵如果用 Parquet 行存会极其低效，用固定 shape 的 mmap tensor 才能做到 **O(1) 随机访问** `Bar(di, ti, ii)`。

### 54.4 udata 与 DataBus 的关系（回应 §5.1 Q3）

**答：DataBus 是实时传输层，udata 是落盘数据产品——两者是上下游关系，不是包含关系。**

从 Confluence 可以清晰理出链路：

```text
实时链路:
  交易所 → 行情机 → DataBus(实时 tick 流, pub/sub)
    → usignal 订阅 → 切 bar → 写入 Udata_BarData
    → utrade 订阅 → 下单执行

离线链路:
  Udata_BarData(落盘数据) → usignal 回测加载 → 信号计算
```

| 组件 | 角色 | 数据形态 |
|---|---|---|
| DataBus | 实时消息总线（低延迟 pub/sub） | ubiqmessage（逐笔行情消息） |
| usignal 切 bar | 实时消费者 + 生产者 | 从 tick → 聚合为 1min/5min/60min bar |
| Udata_BarData | 落盘数据产品 | 三维矩阵 [di, ti, ii]，file-backed |
| UdataSQL/UdataViewer | 查询层 | 在 Udata 上跑 SQL |

DataBus 不是 udata 的载荷，也不是 udata 喂 DataBus。正确关系是：**DataBus 传输实时行情 → usignal 消费后生产 udata → udata 供回测/研究使用。**

个人平台没有 DataBus 这一层（不做实时行情接收），数据从 akshare/tushare API 直接落盘到 Parquet，对应的是公司链路中"Udata 落盘"这个环节**之后**的部分。

### 54.5 k-slice：切片操作 ≠ udata（回应 §5.1 Q4）

**答：不是。udata 是容器，k-slice（Slice）是对容器的访问操作。**

从 page 199033099 的接口定义：

```cpp
// Slice 接口:对三维矩阵做任意维度的切片
template<int32_t FIELD, typename DiSlice, typename TiSlice, typename IiSlice>
auto Slice(DiSlice di_slice, TiSlice ti_slice, IiSlice ii_slice)

// Slice 类型:
// - FullSliceIndex  → 该维度全选(:)
// - SliceIndex      → 该维度范围选(start:end)
// - SingleIndex     → 该维度单点选
// - 整数            → 自动转为 SingleIndex
```

"k-slice"就是 NumPy 风格的多维切片操作，应用在 Udata_BarData 的三维矩阵上：

| 切片操作 | 含义 | 等价 numpy |
|---|---|---|
| `Slice(di=5, ti=FullSlice, ii=FullSlice)` | 取第 5 天的全部时间段、全部合约 | `data[5, :, :]` |
| `Slice(di=5, ti=SliceIndex(0,10), ii=42)` | 第 5 天、前 10 个时间段、第 42 号合约 | `data[5, 0:10, 42]` |
| `Slice(di=FullSlice, ti=FullSlice, ii=42)` | 全部日期、全时间段、第 42 号合约 | `data[:, :, 42]` |

切片维度是 `[di, ti, ii]` 三个维度的任意组合——时间窗口（di/ti 范围）、合约集合（ii 范围）、频率（隐含在 ti 的 TimeAxis 定义中，**不同频率是不同的 Udata 对象**）。

**注意区分两个"切"**：usignal 中"切 bar"是另一个概念——从 tick 实时聚合成 bar（类似 resample），不是对已有数据做切片。

### 54.6 核心决策：不引入 udata 容器层（回应 §5.6）

**结论：不引入（候选方向 B）。**

| 公司需要 udata 的原因 | 个人平台情况 | 是否需要 |
|---|---|---|
| C++/Python 零拷贝共享（mmap tensor） | 纯 Python 平台，不需要 C++ 互操作 | ✗ |
| 三维密集矩阵 O(1) 随机访问 `Bar(di,ti,ii)` | §52 DataAPI 用 SQL 查询，不需要坐标系统 | ✗ |
| 超大规模数据（6.52 亿行 NASDAQ） | A 股全市场日频 128 万行，分钟频单日 127 万行 | ✗ |
| 多进程并发读取共享内存 | §50 决策：per-process DuckDB，不跨进程共享 | ✗ |
| 自描述 Schema（TypeInfo + 血缘 + 生产状态） | §47 的 Parquet schema 自描述，§38 DQC 日志记录质量 | ✗ |
| 多 Layout 选择（Packed/Unpacked × Ti_Ii/Ii_Ti） | Parquet 列存自动处理，DuckDB 自动选最优访问路径 | ✗ |
| View 模式（映射读取子集） | DuckDB SQL WHERE 子句天然支持 | ✗ |
| 时间维度分段（训练集/测试集权限隔离） | 个人平台不需要权限隔离 | ✗ |
| 实时行情 → 切 bar → 落 udata | 不做实时行情接收，数据直接从 API 落 Parquet | ✗ |

**关键洞察**：公司 Udata_BarData 用三维密集矩阵 `[di, ti, ii]` 是因为：

```text
1. C++ usignal 需要 O(1) 坐标访问—Bar(di=5, ti=100, ii=42) 直接寻址到内存偏移
2. 多市场多频率的复杂 usignal 需要切片操作—Slice 在 tensor 上是零拷贝的
3. 实时切 bar 需要写入固定 shape 的 tensor—知道 Universe 大小和 TimeAxis 就能预分配
```

个人平台用 Parquet 行式存储（每行一条 bar 记录）：

```text
1. 不需要 O(1) 坐标访问—SQL WHERE 子句在 Parquet row group 上足够快(§52 预计 <100ms)
2. 不做实时切 bar—数据从 API 拉取后直接按行写入 Parquet
3. 不需要 C++ 互操作—纯 Python 回测/研究链路
```

引入 udata 容器层等于把"高性能 C++ 三维 tensor + 自定义 schema 管理"的复杂性加到一个只需要"Parquet + SQL"的平台上。§4.2–§4.8 已经建立的 **Parquet → DataAPI → load_bars → 三件套** 的链路完全够用。

### 54.7 udata 功能 → Parquet+DuckDB 等价物

个人平台的 Parquet + DuckDB 已经覆盖了 udata 的核心功能：

| udata 功能 | 个人平台等价物 |
|---|---|
| 容器层（Ucube mmap tensor） | Parquet 文件（列存 + 压缩） |
| Schema（schema.info） | Parquet 自带 schema + §4.3 的目录结构约定 |
| TypeInfo（字段类型描述） | Parquet metadata |
| `Bar(di, ti, ii)` 随机访问 | `SELECT * FROM bars WHERE date='2024-01-15' AND ticker='000001.SZ'` |
| `Slice(di_range, ti_range, ii_range)` | SQL WHERE + BETWEEN |
| View（映射子集） | DuckDB view / SQL 子查询 |
| 维度展开（C++ 加速 [di,ti,ii] → 行式） | Parquet 本身就是行式（每行一条 bar 记录） |
| 生产状态 + 血缘 | §4.3 DQC 日志 + correction_log |
| Layout 选择（优化访问模式） | Parquet row group + column chunk 自动优化 |

### 54.8 公司 udata 概念 → 个人平台映射总表

| 公司概念 | 含义 | 个人平台等价物 | 在哪个 § 定义 |
|---|---|---|---|
| Udata_BarData | 三维矩阵 [di,ti,ii] 的 K 线容器 | bars_daily / bars_minute Parquet 表 | §4.2 / §47 |
| Udata_OrderTrade | 逐笔行情容器 | 不引入（不做 tick 级数据） | — |
| Schema（schema.info） | 数据自描述元数据 | Parquet 自带 schema + 目录结构约定 | §4.3 / §47 |
| TypeInfo | 字段类型描述 | Parquet column types | §4.2 / §47 |
| ProductionStatus | 数据生产状态 | DQC 日志 | §38 / §4.6 |
| DependencyLineage | 数据血缘 | correction_log + normalizer 日志 | §3.11 / §4.3 |
| Layout（Packed/Unpacked） | 数据排列方式优化 | Parquet row group 自动管理 | §4.4 / §48 |
| Universe | 标的列表 [di, ii] | 不需要独立维护——Parquet 每行有 ticker 字段 | §4.2 / §47 |
| TimeAxis | 时间轴节点 | 不需要独立维护——Parquet 每行有 date/time 字段 | §4.2 / §47 |
| Ucube tensor | 底层存储容器（mmap） | Parquet file | §4.3 / §48 |
| `Bar(di, ti, ii)` | O(1) 坐标访问 | SQL WHERE date=? AND ticker=? | §52 |
| `Slice(di, ti, ii)` | 多维切片 | SQL WHERE + BETWEEN | §52 |
| View | 映射子集读取 | DuckDB view / SQL 子查询 | §52 |
| `GetArry()` → ndarray | 批量数据访问 | `DataAPI.query()` → DataFrame | §52 |
| `GetPandasDataFrame()` | DataFrame 输出 | `load_bars()` → DataFrame | §53 |
| Compatible 判断 | Schema 兼容性检查 | 不需要——Parquet schema 固定 | §47 |
| 逻辑/物理 Udata（时间分段） | 训练集/测试集分离 | 不需要——个人不做权限隔离 | — |
| DataBus → 切 bar → Udata | 实时行情 → K 线 → 落盘 | akshare API → Normalizer → Parquet | §4.3 / §38 |

### 54.9 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| udata 形态 | Schema + 容器层抽象，底层可插拔（Ucube/内存/文件） | page 244048875, 199033106, 198085912 |
| udata 粒度 | 一个 BarData = 三维矩阵 [di, ti, ii]，涵盖多日×多时段×多合约 | page 199033099, 257678105 |
| udata vs DataBus | DataBus=实时消息总线，udata=落盘数据产品，上下游关系 | page 156420118, 335938903, 244048834 |
| k-slice 含义 | `Slice(di, ti, ii)` 是对三维矩阵的多维切片操作，不等于 udata 本身 | page 199033099 Slice 接口 |
| 是否引入 udata | **不引入**——Parquet + DuckDB 已覆盖 udata 核心功能，引入 udata 等于引入不必要的 C++ tensor 复杂性 | 全部对比分析（§54.6–§54.8） |
| 新增代码 | **0 行**——不引入新抽象层 | — |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§5.1** | 完整讨论稿落盘——**已定 → §54**（四问四答：形态 / 粒度 / DataBus / k-slice） |
| **§5.2 schema** | **部分已定 → §47 / §54**：Schema/TypeInfo/ProductionStatus/Lineage 四大概念映射到 Parquet schema + DQC 日志 + correction_log；基础/扩展/元字段由 §47 十一表 schema 覆盖；字段精度与 schema 版本化细节仍开放 |
| **§5.3 容器层** | **部分已定 → §54**：不引入容器层——层级即 §4.2/§4.3 表结构 + 按日文件；Universe/TimeAxis 不独立维护（每行自带 ticker/date 字段）；命名规范并入 §48 目录约定、加载策略并入 §52/§53 |
| **§5.4 生产与消费** | **部分已定 → §54**：生产=akshare/tushare API→Normalizer→Parquet（§38 入库流水线），消费=DataAPI/load_bars；不引入独立 udata 格式（Parquet 就是存储格式）；缓存不做（§52.5）；内存布局=Parquet 列存 |
| **§5.5 三件套对接** | **已定 → §53 / §54**：转换函数即 `to_backtrader_feed` / `to_alphalens_prices`+`to_alphalens_factor` / `to_vnpy_bars`；反向产出（回测结果/订单）不组织成 udata——落 outputs/ 报告即可 |
| **§5.6 必要性** | **已定 → §54**（候选方向 B 不引入；A 统一数据契约 / C 薄规范 / D 分场景均被否——C 的"标准字段命名"已由 §47 实现，无需额外抽象层） |
| **§52 DataAPI** | 本节确认 SQL WHERE 即 `Bar()`/`Slice()` 的等价物——DataAPI 不需要坐标访问接口 |
| **§53 load_bars** | 本节确认 `GetPandasDataFrame()` 的等价物就是 `load_bars()` |
| **§3.11 / §45** | 逻辑/物理 Udata 的版本化、时间分段权限隔离均不需要；训练/测试集分离用 walk-forward 代码切分（见本节未解决） |

**本节仍未解决**：§5.2 的字段精度（已定 → §55.2 精度表 8 字段落定，2026-10-10 回改）；schema 版本化（已定 → §55.3 变更矩阵 4 行，2026-10-10 回改）；§5.3 容器命名规范是否需要比 `{table}/{YYYY-MM-DD}.parquet` 更细的约定；逻辑 Udata"训练集/测试集分离"在个人平台的等价物——walk-forward 切分的时间边界规范（防止切分泄漏，衔接 §16 多重检验）；Udata_OrderTrade（逐笔）不引入的边界条件——若将来做 tick 级数据，是扩展现有 Parquet 表还是重新评估容器层；Compatible 判断（Equal/Superset/Incompatible）轻量版（不需要 → §55.3 末段，2026-10-10 回改）。

---

## 55. udata schema 收尾：字段精度决策 + schema 可扩展性（§5.2 完结）

> §5.2 的收尾讨论稿，回应 §54.9「本节仍未解决」中的两项：字段精度（float32/float64/Decimal）与 schema 版本化机制。本节落盘个人平台的精度决策表（OHLC 用 DOUBLE、量 INT64、日期 VARCHAR、quality_flag INT8；**不用 Decimal**）与 Parquet schema evolution 扩展策略（新增列/新增表不破坏旧代码；改类型/改列名应避免）。**新增代码 0 行**。与 §47（表 schema）、§54（udata 不引入决策）、§48（`union_by_name`）衔接。

### 55.1 Context 与 Confluence 参考

§54 完成了 udata 的概念澄清与「不引入容器层」决策，但 §5.2 的两个子问题留了尾巴：字段类型与精度怎么定、schema 变了旧代码怎么办。本节补完。

**Confluence 参考**：

```text
- 精度事故(page 263692149):usignal_dlc 各市场对 daily_basedata 的数据精度进行了
  升级,部分字段从 float 生产到 double,会存在与历史数据不兼容的问题。
  导致 DynamicData 的 Compatible 检查失败,历史数据无法复用,需要全量重刷。
  ——公司踩过的真实坑:先用 float32 再升级 double,等于全量重刷一次。
```

### 55.2 字段类型与精度决策（回应 §5.2 字段精度）

**个人平台的精度决策**：

| 字段 | 类型 | 理由 |
|---|---|---|
| 价格（OHLC） | **DOUBLE (float64)** | 避免 float32 精度不足（A 股价格可达 6 位有效数字如 1234.56，float32 只有 ~7 位有效数字，接近极限）。公司最终也升级到了 double，**一开始就用 double，不踩公司踩过的 float→double 兼容坑** |
| 成交量 | **INT64** | 成交量是整数（股数），INT64 覆盖所有场景 |
| 成交额 | **DOUBLE** | 元为单位，需要小数精度 |
| 复权因子 | **DOUBLE** | 累积复权因子可以是很小的小数（如 0.123456） |
| 日期 | **VARCHAR** | "2024-06-14"，人类可读，DuckDB 自动识别 |
| 时间 | **VARCHAR** | "09:31:00"，同上 |
| 标的代码 | **VARCHAR** | "000001.SZ" |
| quality_flag | **INT8** | 0/1/2 三值，最小整数类型 |

**不用 Decimal**：Decimal 在 Parquet + DuckDB 链路中性能差（需要定点运算），且 A 股价格精度不需要——**证监会规定报价最小变动为 0.01 元，DOUBLE 的 15-16 位有效数字绰绰有余**。公司也不用 Decimal（page 263692149 明确是 float+double，不是 Decimal）。

**关键：一开始就选上限类型**（DOUBLE/INT64），把「将来升级类型」这种破坏性变更从一开始就消掉——这正是公司精度事故的教训。

### 55.3 schema 可扩展性（回应 §5.2 schema 版本化）

**公司的教训**（page 263692149）：DynamicData 的列定义变更（float→double）导致 Compatible 返回 Incompatible，需要全量重刷。**根因是 Schema 和数据紧耦合——容器层是固定 shape 的 tensor，TypeInfo 变了 tensor 布局就变了。**

个人平台用 Parquet，天然不受此限制。

**Parquet schema evolution 规则**（Parquet 格式标准）：

```text
1. 新增列 → 旧文件读取时该列返回 NULL   ✓
2. 删除列 → 旧文件中该列被忽略          ✓
3. 类型变更 → 不兼容,需要重写文件        X
```

**DuckDB `read_parquet` 的处理**：

```text
- union_by_name=True → 自动合并不同 schema 的 Parquet 文件
- 缺失列填 NULL
```

**扩展策略**：

| 变更类型 | 操作 | 是否破坏旧代码 |
|---|---|---|
| 新增列（如加 turnover_rate） | 直接写入新 Parquet 文件，旧文件缺该列 | ✗ 不破坏——DuckDB `union_by_name` 自动填 NULL |
| 新增表（如 l2_snapshot） | 新建独立 Parquet 表 + 注册新 DuckDB view | ✗ 不破坏——现有代码不引用新表 |
| 改列类型（如 INT32→INT64） | 需要重写受影响的文件 | ⚠️ 一次性迁移——但 §4.2/§47 一开始就选 DOUBLE/INT64 上限，避免此问题 |
| 改列名 | 需要重写文件 | ⚠️ 应避免——§4.2/§47 的列名（open/high/low/close/volume）是行业标准，不会改 |

**关键洞察：公司 udata 的可扩展性问题来自 tensor 的固定 shape + TypeInfo 强绑定。Parquet 的列存格式天然支持 schema evolution（新增列），不需要额外的兼容性机制。**

这也回答了 §54 中「Compatible 判断（Equal/Superset/Incompatible）个人平台是否需要轻量版」——不需要：schema 只增不改类型/列名，`union_by_name` 就是兼容层；跨表 join 前的类型一致性由「一开始就选上限类型」保证。

### 55.4 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 价格精度 | DOUBLE (float64)，不用 float32 | page 263692149 float→double 事故 |
| 量/额 | volume INT64 / amount DOUBLE | 整数股数 / 元为单位 |
| 复权因子 | DOUBLE | 可为很小的小数 |
| 日期/时间/代码 | VARCHAR | 人类可读，DuckDB 自动识别 |
| quality_flag | INT8（0/1/2） | 最小整数类型 |
| Decimal | **不用** | 性能差 + 0.01 元最小变动价 DOUBLE 足够；公司也不用 |
| 类型变更策略 | 一开始就选上限类型，永不升级 | 避免公司"全量重刷"坑 |
| 新增列 | 直接写新文件，`union_by_name` 自动兼容 | Parquet schema evolution 标准 |
| 新增表 | 独立表 + 新 view | 不破坏现有代码 |
| 改类型/改列名 | 应避免（列名是行业标准） | 需要重写文件 |
| Compatible 判断 | 不需要轻量版 | schema 只增不改，`union_by_name` 即兼容层 |
| 新增代码 | **0 行** | 纯决策，无新抽象 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§5.2 schema** | **完结 → 已定（§47 / §54 / §55）**：字段精度与 schema 可扩展性两项尾巴关闭——精度表 8 字段落定，扩展策略 4 行变更矩阵落定 |
| **§54.9 未解决第 1、2 项** | 已由本节解决（字段精度 → §55.2；schema 版本化 → §55.3） |
| **§54.9 未解决第 6 项** | Compatible 判断轻量版 → 不需要（§55.3 末段） |
| **§47 表 schema** | 精度决策确认：价格/成交额/复权因子 DOUBLE、volume INT64、quality_flag INT8、日期/时间/代码 VARCHAR——与 §47 定稿一致，补充"为什么"（有效数字分析 + 公司事故） |
| **§4.2 / §47 列名** | 重申列名为行业标准（open/high/low/close/volume），永不改名——改名=重写文件 |
| **§48 `union_by_name`** | 用途补充：不仅是 schema 演进时按列名对齐，更是唯一的向后兼容机制（缺失列填 NULL） |
| **§3.11 不做版本化** | 与 schema evolution 不冲突：数据不版本化（覆盖写），schema 只增不改 |

**本节仍未解决**：删除列的实际语义（Parquet 会忽略旧文件中的被删列，但新代码若引用该列会怎样——应在 CI 加 schema 引用检查）；`quality_flag` 是否需要扩展（如加 3=stale，INT8 天然可扩）；VARCHAR 日期是否要换成 DuckDB DATE 类型换取更好的日期函数（当前 VARCHAR 的代价是每次 `date BETWEEN` 需要字符串比较，但对 'YYYY-MM-DD' 格式字符串比较=日期比较，无精度问题）；schema 变更的记录位置（correction_log 还是独立 schema_changelog.jsonl）。

---

## 56. udata 章收口：schema 版本化 + 容器层组织 + 决策汇总总表（§5.2.6 / §5.3 完结 + §5.6 修正为方向 C）

> udata 章（第 5 章）的收口讨论稿。三块内容：① **schema 版本化**（§5.2.6）——公司有 Compatible 四级接口 + DynamicData FixSchema 机制，个人平台**不需要显式版本化**（Parquet footer 自描述 + `union_by_name` + Git 即版本历史）；② **容器层组织**（§5.3 完结）——公司 6 类 Udata（BarData / FutBarData / ProductBarData / DailyData / CrossSection / OrderTrade）逐一映射到 §4.2 的 11 张表；③ **第 5 章决策汇总总表** 10 行，**并对 §5.6 作出修正：最终方向是 C（薄规范），不是 B**——不引入的是容器运行时，保留的是 §4.2 schema 契约。**新增代码 0 行**。与 §47（表 schema）、§54（udata 全解）、§55（精度/扩展）衔接。

### 56.1 Context 与 Confluence 参考

§54 完成了 udata 的概念澄清与「不引入容器层」决策，§55 落定了字段精度与 schema 可扩展性。还剩三件事：schema 要不要显式版本化（§5.2.6）、容器怎么组织（§5.3）、以及第 5 章整体决策的汇总与一处修正（§5.6 的最终方向）。本节收口，第 5 章（udata）至此全部关闭。

**Confluence 参考**：

```text
- Udata API(page 198085912):Compatible 接口定义了 schema 兼容性四级判断:
  Equal(完全相同)→直接使用;
  EqualDiffLayout(字段相同但 Layout 不同)→Transform 后使用;
  Superset(超集)→GetView 提取子集;
  Incompatible(不兼容)→需要重新生产(全量重刷)。
- 精度事故与 FixSchema(page 263692149):usignal_dlc 字段从 float 升级到 double,
  Compatible 检查失败、历史数据无法复用。DynamicData 因此引入 FixSchema 机制—
  DataLoader 在 PrepareUdata 阶段检查并修复 schema。
- UdataSQL 数据类型支持列表(page 288526808):公司 Udata 按维度组织成 6 类:
  BarData(di,ti,ii) / FutBarData(di,ti,ci,mi) / ProductBarData(di,ti,ci) /
  DailyData(di,ii) / CrossSection(di,ti) / OrderTrade(变长流式)。
  每个 Udata 类型是独立的容器,有独立的 schema.info 文件。
  容器之间通过 DependencyLineage 记录依赖。
- Udata_OrderTrade(page 199033096):逐笔行情容器,变长流式数据,
  与定长 K 线容器分开组织。
- ProductionStatus / DependencyLineage(page 223886928, 223894024):
  元信息体系—5 色灯生产状态 + DAG 血缘传播,服务于多团队协作与状态机管理。
```

### 56.2 schema 版本化：不需要显式版本化机制（回应 §5.2.6）

**公司的做法**（page 198085912 + page 263692149）：udata 的 schema 变更靠两套机制兜底——

```text
1. Compatible 接口(加载时四级判断):
   Equal            → 直接使用
   EqualDiffLayout  → Transform 后使用
   Superset         → GetView 提取子集
   Incompatible     → 需要重新生产(全量重刷)

2. DynamicData 的 FixSchema 机制:
   DataLoader 在 PrepareUdata 阶段检查并修复 schema
   (针对 float→double 这类事故的补救通道)
```

这两套机制存在的根因是 §55.3 分析过的：**Schema 和 tensor 数据紧耦合**——固定 shape 的 mmap tensor 一旦 TypeInfo 变了，旧数据的内存布局就失效，必须显式判断兼容性、甚至全量重刷。

**个人平台：不需要显式版本化机制。** 三个理由：

```text
1. Parquet 文件自带完整 schema 元数据
   ——每个文件的 footer 包含列名、类型、编码信息,无需外部 schema.info
2. DuckDB read_parquet() 自动处理不同 schema 的文件合并(union_by_name)
   ——新旧文件共存时按列名对齐、缺失列填 NULL
3. 如果真的需要 schema 大改(极端情况),直接写迁移脚本重写受影响的 Parquet 文件
   ——数据量小(§4.6 估算总量 <100GB),全量重写也就几分钟
   ——这比维护一套 Compatible/FixSchema 状态机便宜得多
```

**版本历史怎么追踪**：如果未来需要追踪 schema 变更历史，用 Git——§4.3 的目录结构在 `data/` 目录下，schema 定义在代码中（§4.2 的 `SCHEMA = {...}` 字典）：**代码即 schema 定义，git log 即版本历史**。不需要单独的版本号字段、版本表或兼容性矩阵。

**关键洞察：公司的 Compatible 四级 + FixSchema 是"容器与 schema 紧耦合"的补丁；个人平台 Parquet 的 schema 天然内嵌在每个数据文件里，演进规则只有"增列/删列兼容、改类型重写"（§55.3），这就是全部的版本化语义——不需要再包一层显式机制。**

### 56.3 容器层组织：公司 6 类 Udata → 个人 11 张表（回应 §5.3，完结）

**公司的容器层组织**（page 288526808 UdataSQL 数据类型支持列表）：每个 Udata 类型是独立的容器，有独立的 schema.info 文件，容器之间通过 DependencyLineage 记录依赖：

| 公司 Udata 类型 | 维度 | 典型数据 |
|---|---|---|
| BarData | (di, ti, ii) | 日内 K 线 |
| FutBarData | (di, ti, ci, mi) | 期货 K 线（品种×合约） |
| ProductBarData | (di, ti, ci) | 产品 K 线 |
| DailyData | (di, ii) | 日频数据 |
| CrossSection | (di, ti) | 截面数据 |
| OrderTrade | 变长流式 | 逐笔行情 |

维度含义：`di`=日期、`ti`=日内时段、`ii`=合约编号、`ci`=品种编号、`mi`=月份/合约月份。

**个人平台：§4.2–§4.3 已完成等价设计——§4.2 的 11 张表就是"容器"**（一张 Parquet 表 = 一个容器，按日文件 = 容器内的时间分段）：

```text
data/asset/
├── equity/
│   ├── bars_daily/            ← 等价于 BarData(di, ii) 日频
│   │   └── 2024-06-14.parquet
│   ├── bars_minute/           ← 等价于 BarData(di, ti, ii) 分钟频
│   │   └── 2024-06-14.parquet
│   ├── adjfactor/             ← 等价于 DailyData(di, ii)
│   └── instruments/           ← 合约主数据（独立表）
├── futures/
│   ├── bars_daily_futures/    ← 等价于 FutBarData
│   └── continuous/            ← 衍生数据（依赖原始合约）
└── meta/
    ├── calendar.parquet
    └── correction_log.jsonl   ← 等价于简化版 DependencyLineage
```

逐项对照：

| 公司容器概念 | 个人平台等价物 | 说明 |
|---|---|---|
| BarData (di,ii) 日频 | `equity/bars_daily/` | 一张表一个频率，按日文件分段 |
| BarData (di,ti,ii) 分钟频 | `equity/bars_minute/` | 同上；不同频率是不同表（对应公司"不同 Udata 类型独立容器"） |
| FutBarData (di,ti,ci,mi) | `futures/bars_daily_futures/` | 期货按品种×合约组织，`ticker` 列区分合约 |
| ProductBarData (di,ti,ci) | `futures/continuous/` | 连续合约=按品种聚合的衍生表 |
| DailyData (di,ii) | `equity/adjfactor/` | 日频辅助数据独立表 |
| CrossSection (di,ti) | 不需要独立容器 | 截面=SQL `WHERE date=...` 的查询结果，不是存储单位 |
| OrderTrade 变长流式 | 不引入（不做逐笔） | 边界条件见 §54 未解决项 |
| 独立 schema.info | Parquet footer + §4.2 `SCHEMA` 字典 | schema 内嵌于文件，定义写在代码 |
| DependencyLineage DAG | `meta/correction_log.jsonl` | 简化版血缘：谁改了谁记一条，不做状态传播 |
| 合约元数据（乘数/最小变动价） | `equity/instruments/` 独立表 | **不放容器里**——主数据与行情分离（page 199033096） |
| Universe / TimeAxis | 不独立维护 | 每行自带 `ticker`/`date`/`time` 字段（§54.8） |

**关键洞察：公司的"容器"是运行时对象（类型 + schema.info + tensor 文件 + 血缘 DAG）；个人平台的"容器"降级为纯存储组织——一张 Parquet 表 + 目录约定。类型系统的全部职责由 `SCHEMA` 字典 + 表名约定承担，不需要 Container 类。**

### 56.4 第 5 章决策汇总总表 + §5.6 修正为方向 C

**第 5 章（udata）决策汇总**：

| 子节 | 决策 | Confluence 参考 |
|---|---|---|
| §5.1 形态 | udata = Schema + 容器层抽象，三维矩阵 [di,ti,ii] | page 244048875, 199033099 |
| §5.2 schema | 价格 DOUBLE（避免 float→double 兼容坑），Parquet 天然 schema evolution，不需要显式版本化 | page 263692149（float 升级事故） |
| §5.3 容器组织 | §4.2–§4.3 的 Parquet 表结构 = 公司容器层等价物，合约元数据放独立 instruments 表 | page 288526808, 199033096 |
| §5.4 生产消费 | Parquet = 最终格式，DataFrame = 内存表示，不引入中间层，不做应用层缓存 | page 222727558（DataPool 懒加载） |
| §5.5 三件套对接 | §4.8 已完成，反向产出不组织为行情 schema | §4.8 |
| §5.6 引入必要性 | **方向 C：薄规范（§4.2 schema 定义），不做容器运行时** | page 244048834（"搭配但不绑定"） |
| 字段精度 | OHLC/amount/adjfactor→DOUBLE，volume→INT64，不用 Decimal | page 263692149（float→double 事故教训） |
| schema 扩展 | 新增列直接写入新 Parquet 文件，DuckDB `union_by_name` 自动合并 | Parquet 格式标准 |
| 元信息 | quality_flag 列内嵌 + correction_log.jsonl，不做 5 色灯 DAG 传播 | page 223886928, 223894024 |
| 新增代码 | **0 行**——所有基础设施在 §4.2–§4.8 中已建立 | — |

**§5.6 修正：最终方向是 C（薄规范），不是 B。**

§54.6 曾把结论记为「候选方向 B（不引入）」。精确的说法是 **方向 C——薄规范，不做容器层**（"不引入容器层/容器运行时"的结论不变；差别在保留薄规范）。对照 §5.6 的候选方向：

```text
A. 要：udata 是统一数据契约          ✗ ——不需要容器运行时
B. 不要：不做二次抽象                ✗ ——太松散,仍需约定字段名和类型
C. 部分要：薄规范,不做容器层          ✓ ——最终决策
D. 看场景：回测用 udata,研究用 DataFrame  ✗ ——场景二分没必要
```

**"薄规范"具体是什么？就是 §4.2 已经定义的东西：`SCHEMA = {...}` 字典。** 不需要 Schema 类、不需要 TypeInfo 解析、不需要 Compatible 判断、不需要 Layout 选择、不需要 ProductionStatus 状态机、不需要 DependencyLineage DAG。这些在公司里存在是因为：

```text
1. 数百个 Udata 类型之间有复杂依赖      → 需要 DAG 状态传播
2. C++ 和 Python 需要共享内存数据       → 需要 TypeInfo 精确描述字段布局
3. 超大规模数据需要优化访问模式          → 需要多种 Layout
4. 多团队协作需要 schema 兼容性检查     → 需要 Compatible 接口
```

个人平台一个人用、纯 Python、百万级行、两层依赖——**§4.2 的 Parquet schema 定义 + §4.7 的 DataAPI + §4.8 的 load_bars 就是完整的"数据契约"**。schema 契约停在"字段名 + 类型 + 目录约定"这一层，其上不再建运行时抽象。

### 56.5 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| schema 版本化 | **不需要显式机制**——Parquet footer 自描述 + `union_by_name` 合并 + Git log 即版本历史 | page 198085912（Compatible 四级）, 263692149（FixSchema） |
| Compatible 四级 | 不引入（Equal/EqualDiffLayout/Superset/Incompatible） | 其根因是 tensor 与 schema 紧耦合；Parquet 不需要 |
| FixSchema | 不引入 | 同上——迁移脚本重写几分钟搞定（<100GB） |
| 容器层组织 | §4.2 的 11 张表 = 公司 6 类 Udata 容器的等价物 | page 288526808 |
| 合约元数据 | 独立 `instruments` 表，不放容器里 | page 199033096 |
| 依赖血缘 | `correction_log.jsonl` 简化版，不做 DAG 状态传播 | page 223886928, 223894024 |
| §5.6 最终方向 | **C 薄规范**（`SCHEMA` 字典）**不做容器运行时** | page 244048834"搭配但不绑定" |
| 新增代码 | **0 行** | — |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§54.6 / §5.6 标记** | **修正：最终决策为方向 C（薄规范），不是 B**——§54.6 记「候选方向 B 不引入」不精确；B 的"不做二次抽象"意味着全裸 DataFrame，被否；实际保留了薄规范（§4.2 `SCHEMA` 字典）。不引入容器运行时的结论不变 |
| **§5.2 schema** | 补完 5.2.6：不需要显式版本化机制（Compatible 四级 / FixSchema 均不引入）；版本历史用 Git（代码即 schema 定义，git log 即版本历史）——§5.2 至此真正完结 |
| **§5.3 容器层** | **完结 → 已定**：公司 6 类 Udata（BarData/FutBarData/ProductBarData/DailyData/CrossSection/OrderTrade）逐一映射到 §4.2 11 张表；合约元数据独立 instruments 表；依赖=correction_log 简化版；命名并入 §48、加载并入 §52/§53 |
| **§47 表 schema** | 确认 schema 定义（`SCHEMA = {...}`）就是方向 C 的"薄规范"本体——不需要再包 Schema 类 |
| **§55.3 扩展策略** | 与 5.2.6 一致：显式版本化机制是 tensor 紧耦合的补丁，Parquet 侧不需要 |
| **§3.11 不做版本化** | 再次确认兼容：数据不版本化（覆盖写），schema 只增不改，历史用 Git 追踪 |

**本节仍未解决**：无——第 5 章（udata）§5.1–§5.6 全部关闭。遗留的边界条件（tick 级数据扩展、walk-forward 时间边界、quality_flag 扩展等）已分列在 §54/§55 的未解决项中，不阻塞实施。

---

## 57. 因子的定义与接口：Factor ABC + depends_on() + compute() → DataFrame（§6.1 全解）

> §6.1 的完整讨论稿。公司因子体系（EventBrick + UFormula + DataPlugin 三层）架构还原后做核心简化：**保留现有 Factor ABC / FactorRegistry / FactorDAG / PostProcessor，只微调接口**——`depends_on()` 显式声明依赖（前缀区分数据表 vs 因子）、`compute(data) → DataFrame[date, ticker, factor_name]` 统一输出、元数据用类属性（不引入 JSON/YAML）；EventBrick / UFormula / DataPlugin 三种实现路径**只保留 Python**。与 §47（数据接入 load_bars）、§56（薄规范）、§16（因子合成）衔接。**新增代码 ~0 行**（接口微调，不新增组件）。

### 57.1 Context 与 Confluence 参考

§4 完成了数据层（Parquet+DuckDB+load_bars），§5 决定不引入 udata 容器层。现在进入因子体系：项目已有 `factor_library/`（Factor ABC + FactorRegistry + FactorDAG + PostProcessor），新架构下怎么定位？

**Confluence 参考**：

```text
- Usignal 框架(page 244048834):事件驱动,所有模块是 EventBrick(EB)挂在
  usignal 的"桁架"上。"usignal 是一个非常空的框架,只负责组织数据、定义事件"
- 2024 年度 Usignal 项目总结(page 215591289):融合旧 usim + 旧 usignal,
  核心变化——完全事件驱动 + 所有功能做成 Service。数据层用 Udata,
  信号计算层用 EventBrick + Uformula
- 使用 uformula 计算 feature(page 247377333):feature(因子)的标准路径——
  DataPlugin 注册 Udata 数据源 → UFormula XML 配置公式 + EB 订阅事件触发
  计算 → 结果写入 feature_data Udata。三层结构:数据注册(DataPlugin) →
  公式定义(XML) → 事件触发(EB)
- 使用 feature 计算 signal(page 253447090):signal = EB(EventBrick)实现。
  EBS_SignalPrivate_Global 继承 UsigEventBrick,在 init() 中
  RequestDynamicUdata 获取 feature_data,在事件回调中计算信号,
  在 on_finalize() 中建立血缘关系
- UFormula 接入(page 222730288):UFormula 输入是三维矩阵(di × ti × 合约),
  通过 work_di(di) 或 work(di, ti) 驱动。EB 订阅 di/ti 事件后调 UFormula。
  UFormula 是公式引擎,不是因子框架——它只管数值计算,
  不管因子的注册/分类/元数据
- UFormula 算子列表(page 228460124):三类算子——普通算子
  (abs/add/sub/prod/div/log)、时序算子(tsdelay/tssum/tsmean/tsema/tscorr/
  tsrank/tszscore)、截面算子(csrank/cszscore/csneutralize/csindneutralize)。
  这实际上就是因子计算的基本操作集
- Alpha 入库系统(page 288524848):最终 signal(alpha)从 usignal → 共享内存 +
  Eve Job → 入库(Taurus/ClickHouse/Kafka)。因子计算和信号入库是两个不同的阶段
- DataPool/DataLoader(page 222727558):懒加载,"后续任务真正要用到数据时进行
  加载"。DataPool 缓存已加载数据
```

### 57.2 公司因子体系的架构还原

从上述 Confluence 文档可以还原出公司完整的因子计算链路：

```text
数据层:
  Udata_BarData(OHLCV)   ┐
  Udata_OrderTrade(tick)  ├→ DataPlugin 注册数据源
  Udata_DailyData(日频)   ┘

因子计算层:
  UFormula(公式引擎)  → XML 定义公式(如 "tsrank{tsmean{close, 20}, 500}")
  EB_Indicator(C++)   → C++ 手写复杂逻辑(如 OFI、LOB 特征)
  PyIndicator(Python) → Python 手写(研究阶段快速验证)

事件调度层:
  usignal EventBrick → 订阅 di/ti 事件 → 触发 UFormula/Indicator
    → 写入 feature_data Udata

信号层:
  EBS_Signal → 从 feature_data 读取因子 → 模型推理/线性组合 → 输出 signal

入库:
  AlphaDumper → SHM → Eve Job → Taurus/ClickHouse/Kafka
```

**公司的"三种实现路径"**：

| 路径 | 载体 | 适用场景 | 性能 |
|---|---|---|---|
| UFormula | XML 公式 + C++ 引擎 | 标准时序/截面因子（占 80%+） | 最高（GPU 加速，矩阵运算） |
| C++ EB（Indicator） | C++ EventBrick | 复杂逻辑（OFI、LOB 特征、自定义撮合） | 高 |
| PyIndicator/PyAlgo | Python EventBrick | 研究验证、另类数据因子 | 低（GIL） |

### 57.3 核心简化与 Factor 抽象接口

**核心简化**——公司因子体系的复杂性来自：

```text
1. 事件驱动调度——usignal 的 EB 机制,因为需要支持实盘实时计算
2. C++ 性能需求——亿级行数据需要 C++ 和 GPU
3. 多进程/多机并行——k8s 上 1000 个 usignal 并行生产
4. UFormula 公式引擎——XML 配置的 DSL,需要 DataPlugin 注册数据源
```

个人平台全部不需要这些——**纯 Python、百万级行、单机单进程、离线计算**。

**答：因子的抽象接口。** 保留现有 Factor ABC 的设计方向，但简化接口：

```python
from abc import ABC, abstractmethod
import pandas as pd


class Factor(ABC):
    """
    因子抽象基类。

    对标公司 EventBrick 的定位——但去掉事件驱动、Udata 依赖、C++ 互操作。
    保留核心：输入声明 + 计算逻辑 + 元数据。
    """

    # — 元数据（对标公司 EB 的 config 声明）—
    name: str                              # 因子唯一名
    description: str = ""
    category: str = "custom"               # cross_section / time_series / event / alternative
    frequency: str = "daily"               # daily / minute
    asset_types: list[str] = ["equity"]
    version: str = "1.0"

    @abstractmethod
    def depends_on(self) -> list[str]:
        """
        依赖声明：返回需要的数据表名或其他因子名。

        对标公司 EB 的 init() 中 RequestDynamicUdata()——
        声明本因子需要哪些输入数据。

        返回格式：
          ["bars_daily"]                         → 依赖原始数据表
          ["bars_daily", "factor:momentum_20d"]  → 依赖数据表 + 其他因子
        """
        ...

    @abstractmethod
    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        计算逻辑。

        对标公司 UFormula 的 work_di(di) / EB 的事件回调——
        但输入输出都是 DataFrame，不是三维 tensor。

        参数：
          data: depends_on() 声明的每个依赖名 → DataFrame

        返回：
          DataFrame with columns [date, ticker, {factor_name}]
          横截面因子：每行 = 一个 (date, ticker) 对
          时序因子：同上（时序窗口在 compute 内部处理）
        """
        ...
```

### 57.4 输入 / 输出 / 分类 / 元数据 / 依赖声明

**输入：DataFrame，不是 Udata。** 公司因子的输入是 Udata（三维 tensor），通过 RequestDynamicUdata + DataPlugin 注册。个人平台的输入是 DataFrame，通过 `load_bars()`（§53）加载：

```python
# 公司（C++ EB 里）：
m_feature_udata = RequestDynamicUdata<FeatureUdataType>(name, columns, uid, taid);
float value = m_feature_udata->Bar<FIELD>(di, ti, ii);

# 个人平台：
data = {"bars_daily": load_bars(start=start, end=end, freq="daily")}
result = factor.compute(data)
```

输入通过 `depends_on()` 声明 + FactorDAG 自动解析——这和公司 EB 的 init() RequestUdata 是等价的，只是形式不同。

**输出：DataFrame，统一格式。** 所有因子输出统一格式：

```text
DataFrame with columns = [date, ticker, {factor_name}]

例如：
  date        ticker    momentum_20d
  2024-01-15  000001.SZ  0.0523
  2024-01-15  000002.SZ  -0.0112
  ...
```

这个格式直接对接：

```text
- alphalens：set_index(["date", "ticker"])["factor_value"] → MultiIndex Series
- FactorDAG：下游因子的 compute() 接收此 DataFrame 作为输入
- 因子存储：直接写入 Parquet（§6.2 会设计）
```

**不输出矩阵。** 公司用矩阵是因为 UFormula 的计算引擎在三维 tensor 上操作。个人平台用 pandas 向量化操作，DataFrame 是自然的 IO 格式。

**因子分类**——对标公司 UFormula 算子的三大类（page 228460124）：

| 公司分类 | 算子前缀 | 个人平台分类 | 说明 |
|---|---|---|---|
| 时序算子 | ts*（tsdelay, tssum, tsmean, tsema, tsrank...） | `time_series` | 对单标的的时间序列做运算（rolling/lag） |
| 截面算子 | cs*（csrank, cszscore, csneutralize...） | `cross_section` | 对同一时间点的全市场标的做运算（排名/中性化） |
| 普通算子 | abs, add, log... | （不独立分类——是基础运算） | |
| — | — | `event` | 公司没有独立分类——事件因子在 C++ EB 里手写 |
| — | — | `alternative` | 公司的另类数据因子在独立 pipeline 里 |

个人平台 4 类因子：

```python
FACTOR_CATEGORIES = {
    "time_series":   "单标的的时序运算（动量、波动率、均线偏离）",
    "cross_section": "截面运算（行业中性化、市值中性化、截面排名）",
    "event":         "事件驱动（财报发布、股东增减持、限售解禁）",
    "alternative":   "另类数据（情绪、资金流、龙虎榜）",
}
```

**因子元数据**——公司 EB 的元数据分散在 XML 配置 + C++ 类定义中。个人平台集中在 Factor 类的类属性：

```python
class Momentum20D(Factor):
    name = "momentum_20d"
    description = "20日动量：close / close_lag20 - 1"
    category = "time_series"
    frequency = "daily"
    asset_types = ["equity"]
    version = "1.0"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"]
        df = df.sort_values(["ticker", "date"])
        df["momentum_20d"] = df.groupby("ticker")["close"].pct_change(20)
        return df[["date", "ticker", "momentum_20d"]].dropna()
```

**不引入单独的元数据 JSON/YAML 文件——元数据就是代码。** 理由：个人平台因子数量预计 <100 个，代码即文档。公司需要独立元数据是因为 UFormula XML 配置和 C++ 代码分离。

**依赖声明**：

```python
class VolatilityAdjMomentum(Factor):
    name = "vol_adj_momentum"
    category = "time_series"

    def depends_on(self):
        return ["bars_daily", "factor:momentum_20d", "factor:volatility_20d"]
        #        ↑ 原始数据表    ↑ 依赖其他因子        ↑ 依赖其他因子

    def compute(self, data):
        bars = data["bars_daily"]
        mom = data["factor:momentum_20d"]
        vol = data["factor:volatility_20d"]
        merged = bars.merge(mom, on=["date", "ticker"]).merge(vol, on=["date", "ticker"])
        merged["vol_adj_momentum"] = merged["momentum_20d"] / merged["volatility_20d"]
        return merged[["date", "ticker", "vol_adj_momentum"]].dropna()
```

`depends_on()` 返回的字符串前缀区分数据源类型：

```text
- 无前缀("bars_daily")           → 从 DataAPI / load_bars 加载原始数据
- "factor:" 前缀("factor:momentum_20d") → 从因子存储加载或现算
```

对标公司 EB 的 RequestUdata + DependencyLineage——但用函数返回值声明，不用 XML 配置。现有 FactorDAG 根据 `depends_on()` 构建有向无环图，拓扑排序后按序计算——这个设计保留。

**三种实现路径：只保留 Python**：

| 公司路径 | 个人平台 | 理由 |
|---|---|---|
| UFormula（C++ 公式引擎 + GPU） | ✗ 不引入 | 需要 C++ build chain、GPU、XML 配置、DataPlugin——纯 Python 平台用 pandas/numpy 向量化足够 |
| C++ EB（Indicator） | ✗ 不引入 | 没有 C++ 需求 |
| PyIndicator/PyAlgo（Python） | ✓ 唯一路径 | 用 Factor ABC + pandas/numpy |

公司 UFormula 的时序/截面算子（tsrank、cszscore 等），在 pandas 中有直接等价物——见 57.5 示例中的 `pct_change` / `groupby.rank` 等。

### 57.5 现有 factor_library 改动建议与示例

**现有 `factor_library/` 的改动建议**：

| 现有组件 | 保留/改 | 说明 |
|---|---|---|
| Factor ABC | 保留，微调接口 | 加 `depends_on()` 返回值区分数据源 vs 因子依赖；统一输出格式为 DataFrame[date, ticker, factor_value] |
| FactorRegistry | 保留 | 用类属性（name/category/version）替代外部 JSON 注册 |
| FactorDAG | 保留 | 根据 `depends_on()` 拓扑排序，自动调度因子计算 |
| PostProcessor | 保留 | 截面标准化（cszscore）、行业中性化（csneutralize）等——等价于公司 UFormula 的 cs* 算子 |
| EventBrick 概念 | 不引入 | 公司 EB 是为事件驱动 + 实盘设计，个人平台离线批量计算不需要 |
| UFormula | 不引入 | 公式引擎的 DSL + XML 配置是过度工程；pandas 向量化足够 |
| DataPlugin | 不引入 | 数据注册用 `depends_on()` 字符串声明，不需要 C++ 数据源注册机制 |

**具体因子示例**——时序因子：

```python
class Momentum20D(Factor):
    name = "momentum_20d"
    category = "time_series"
    frequency = "daily"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close"]].copy()
        df = df.sort_values(["ticker", "date"])
        df["momentum_20d"] = df.groupby("ticker")["close"].pct_change(20)
        return df[["date", "ticker", "momentum_20d"]].dropna()
```

截面因子：

```python
class SizeRank(Factor):
    name = "size_rank"
    category = "cross_section"
    frequency = "daily"

    def depends_on(self):
        return ["bars_daily"]

    def compute(self, data):
        df = data["bars_daily"][["date", "ticker", "close", "volume"]].copy()
        df["market_cap_proxy"] = df["close"] * df["volume"]
        df["size_rank"] = df.groupby("date")["market_cap_proxy"].rank(pct=True)
        return df[["date", "ticker", "size_rank"]]
```

组合因子（依赖其他因子）：

```python
class VolAdjMomentum(Factor):
    name = "vol_adj_momentum"
    category = "time_series"

    def depends_on(self):
        return ["factor:momentum_20d", "factor:volatility_20d"]

    def compute(self, data):
        mom = data["factor:momentum_20d"]
        vol = data["factor:volatility_20d"]
        merged = mom.merge(vol, on=["date", "ticker"])
        merged["vol_adj_momentum"] = merged["momentum_20d"] / merged["volatility_20d"]
        return merged[["date", "ticker", "vol_adj_momentum"]].dropna()
```

### 57.6 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 因子抽象 | Factor ABC + `depends_on()` + `compute()` → DataFrame | 对标公司 EB 的 init(RequestUdata) + 事件回调 + on_finalize(血缘) |
| 因子输入 | DataFrame（通过 load_bars 或因子存储加载） | 对标 RequestDynamicUdata（page 253447090） |
| 因子输出 | DataFrame[date, ticker, factor_value] 统一格式 | 对标 feature_data Udata（page 247377333） |
| 因子分类 | 4 类：time_series / cross_section / event / alternative | 对标 UFormula ts*/cs* 算子分类（page 228460124） |
| 因子元数据 | 类属性（name/category/version/description） | 不需要独立 JSON——代码即元数据 |
| 依赖声明 | `depends_on()` 返回字符串列表，前缀区分数据/因子 | 对标 EB 的 RequestUdata + DependencyLineage |
| 实现路径 | 只保留 Python，不引入 UFormula/C++ EB | UFormula 的 ts*/cs* 算子在 pandas 有直接等价物 |
| 现有代码 | Factor ABC / FactorRegistry / FactorDAG / PostProcessor 全部保留，微调接口 | |
| 新增代码 | **~0 行**（接口微调，不新增组件） | |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§6.1** | 完整讨论稿落盘——**已定 → §57**（六项子问题全部关闭：输入/输出/分类/元数据/依赖声明/实现路径） |
| **factor_library/ Factor ABC** | 微调：加 `depends_on()` 抽象方法（前缀 `factor:` 区分因子依赖）；`compute()` 统一返回 DataFrame[date, ticker, {factor_name}] |
| **factor_library/ FactorRegistry** | 注册来源从外部 JSON 改为类属性（name/category/frequency/asset_types/version） |
| **factor_library/ FactorDAG** | 依赖解析改为读 `depends_on()` 返回值（原 FactorDAG 设计保留，Kahn 拓扑不变） |
| **PostProcessor** | 定位确认=公司 UFormula cs* 算子的 pandas 等价物（cszscore/csndneutralize） |
| **EventBrick / UFormula / DataPlugin** | 明确不引入——三者是事件驱动+实盘+C++ 性能的产物，离线批量场景不需要 |
| **§12.2 EBGraph/EventBrick** | 因子层不需要 EB；EBGraph 问题在 §12 层面另行裁决 |

**本节仍未解决**：`version` 字段的变更规范（compute 逻辑改了必须手动 bump？谁来强制——可在 CI 检查）；`category="event"`/`"alternative"` 两类的接口是否需要额外约定（事件因子的事件表输入、另类数据的非 bars 输入如何声明在 `depends_on()`）；因子命名规范（`momentum_20d` vs `mom_20d`，与 §27 因子注册表的 name 字段对齐）。

---

## 58. 因子的计算调度：DAG 构建 + 缓存 + 增量更新（§6.2 全解）

> §6.2 的完整讨论稿。公司调度体系三层（usignal 单进程 EBGraph / usignal-parallel 多 pod 切日期 / Datamall 平台级事件驱动）还原后大幅简化：**FactorDAG 声明式 `depends_on()` + Kahn 拓扑排序**（不做 AST 静态分析）、**缓存按因子 × 日期范围**（key = name+version+range+depends_hash）、**增量 append/full 两模式**（不做 per-di 精细增量）。与 §57（Factor 接口）、§55（schema 演进）、§27（因子注册）衔接。失败处理与长耗时因子调度仍开放（见 58.6）。

### 58.1 Context 与 Confluence 参考

§6.1 确定了因子接口：Factor ABC + `depends_on()` + `compute()` → DataFrame。现在设计调度层：FactorDAG 如何运行这些因子——依赖解析、缓存、增量更新、并行、失败处理、长耗时因子。

**Confluence 参考**：

```text
- Udata 多进程并行生产(page 244034279):usignal-parallel 分析 XML 中 EBGraph
  的依赖,按日期区间将任务切分到多 pod 并行执行。核心原理:提取 DF_EbGraph
  的日期区间 + 并行度(mt/maxThread) → 按 N = ceil(天数 / maxThread) 切分
  → N 个 Task 并行生产。资源估计由 DataFactory 自行提供
- usignal 支持只刷无效 udata(page 256109564):reproduce_abnormal_dates_only=
  true → 只重新生产 ProductionStatus 为 BLACK 的日期,不全量重算。关键机制:
  通过 usignal_terminal 手动将特定日期的状态设为 BLACK → 下游缓存自动失效
  → 重跑只计算异常天数。这就是公司的增量重算方案
- 数据生产计划 DPP(page 288529199):pysim 仿照 usignal-parallel,通过分析
  XML 的 EBGraph 依赖,按日期和模块拆分任务到不同 pod。支持增量生产
  (修改 end-date 即可)。瓶颈观察:"计算时间瓶颈由最慢的 pod 决定"——
  因为按等间隔日期划分,不同日期数据量不同
- 数据生产状态(page 223886928):5 色灯系统(BLACK/GREEN/YELLOW/AMBER/RED),
  per-di 粒度。状态传播规则:BLACK → 下游全部标 AMBER 并向下传播;
  YELLOW → 下游全部标 YELLOW 并向下传播。这就是公司的 DAG 失败传播机制
- 依赖血缘 DependencyLineage(page 223894024):记录每个 udata 的上游
  id/dimension/key/version per di。UdataLightHelper 负责状态传播
- 事件驱动调度系统(page 324637543):Datamall 平台的新调度——Event →
  匹配订阅 → Consumer UDF → 触发执行。三种事件:asset_ready(上游完成)、
  timer_fired(定时器)、manual。核心改进:从"每 3 分钟全量扫描上游状态"
  改为"上游完成后 Kafka 实时通知下游",延迟从 3min 降到秒级
- 调度优化:事件驱动 + 级联回刷(page 381602916):批量级联回刷——平台基于
  dw_asset_job_lineage 计算 DAG 层序,拓扑排序后逐层触发。支持 stop_nodes
  控制传播范围、dry_run 预览、按日期串行 vs 按层并行两种模式
- DAG 任务(page 335950681):Datamall 的 DAG 作业——多节点(sql/asset_io)
  编排成 DAG,depends_on 显式声明依赖,每节点独立线程调度。
  支持中间结果传递(MinIO parquet)
```

### 58.2 公司调度体系的三个层次

从 Confluence 可以还原出公司调度体系分三个层次：

**层次 1：单进程内的 EBGraph 调度（usignal 内部）**

```text
usignal XML 定义：
  <ebgraph id="data_producer" event_bricks="EB_A,EB_B,EB_C" />

调度方式：
  事件驱动——EB_A 发布 event_done → EB_B 订阅该事件 → 触发 EB_B 执行

依赖声明：
  XML 中 event_sub / event_pub 配对（隐式声明依赖关系）

并行：
  独立 EB 可并行（mt=true, maxThread=5）
```

对标个人平台：FactorDAG 的单次执行——拓扑排序后，独立因子可并行计算。

**层次 2：多进程/多 pod 并行生产（usignal-parallel / DPP）**

```text
usignal-parallel：
  分析 XML → 提取日期区间 → 按 maxThread 切分 → N 个 Task 提交到 k8s

切分策略：
  N = ceil(日期天数 / maxThread)
  每个 Task 处理一段日期区间

瓶颈：
  "最慢的 pod 决定整体时间"——日期不等量（如 20240924 后行情活跃）
```

对标个人平台：不需要。个人平台单机单进程，A 股 ~2400 交易日全量计算几秒内完成。

**层次 3：跨资产/跨 job 的平台级调度（Datamall 事件驱动调度）**

```text
Datamall：
  上游 job 成功 → Kafka 发 asset_ready → 下游 Consumer UDF 消费 → 触发执行

DAG 拓扑：
  dw_asset_job_lineage 表记录依赖关系 → 平台自动计算层序

级联回刷：
  backfill API → 拓扑排序 → 逐层触发 → stop_nodes 控制范围

增量重算：
  ProductionStatus BLACK → 只刷异常天数（reproduce_abnormal_dates_only）
```

对标个人平台：FactorDAG 的整体调度——但大幅简化（无 Kafka、无 k8s、无多进程）。

### 58.3 DAG 的构建（回应 6.2.1：声明式依赖？静态分析？）

**答：声明式依赖，和公司 EB 的 event_sub/event_pub 同源思想。**

公司有两种依赖声明方式：

```text
1. usignal EB: XML 中 event_sub/event_pub 配对，隐式声明 EB 间依赖
2. Datamall:  dw_asset_job_lineage 表 + depends_on 显式声明
```

个人平台用 `depends_on()` 方法显式声明（§57 已确定），等价于 Datamall 的 depends_on 模式：

```python
# DAG 构建
from collections import defaultdict


class FactorDAG:
    def __init__(self):
        self.factors: dict[str, Factor] = {}      # name → Factor
        self.graph: dict[str, list[str]] = {}     # name → [dependency_names]
        self.reverse: dict[str, list[str]] = {}   # name → [downstream_names]

    def register(self, factor: Factor):
        self.factors[factor.name] = factor
        deps = []
        for dep in factor.depends_on():
            if dep.startswith("factor:"):
                deps.append(dep.removeprefix("factor:"))
        self.graph[factor.name] = deps
        for d in deps:
            self.reverse.setdefault(d, []).append(factor.name)

    def topological_sort(self) -> list[str]:
        """Kahn 算法拓扑排序。对标 Datamall 的 DAG 层序计算。"""
        in_degree = {n: 0 for n in self.factors}
        for name, deps in self.graph.items():
            in_degree[name] = len(deps)

        queue = [n for n, d in in_degree.items() if d == 0]
        order = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for downstream in self.reverse.get(node, []):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(order) != len(self.factors):
            raise ValueError("因子依赖图存在环")
        return order
```

**不做静态分析**（AST 解析 `compute()` 代码）——成本高、脆弱、不可靠。声明式依赖简单明确，和公司的做法一致。

**环检测**：拓扑排序天然检测环——如果排序后长度 ≠ 节点数，说明有环。公司的 Datamall 也是这个思路（DAG = 有向无环图，平台自动验证）。

### 58.4 缓存策略（回应 6.2.2）

**公司的缓存机制**：

```text
- DataPool(page 222727558):懒加载 + 缓存。"后续任务真正要用到数据时进行
  加载",DataPool 内部缓存已加载的数据
- usignal EB: cache_uformula="true" 属性控制是否缓存 UFormula 的计算结果
- ProductionStatus(page 223886928):per-di 粒度的状态跟踪——GREEN 表示
  不需要重算
```

**个人平台缓存设计**——缓存粒度：按因子 × 日期范围。存储位置：

```text
data/factors/{factor_name}/
    v1.0/                       ← 因子版本
        20240101_20241231.parquet   ← 按年存储（或自定义区间）
        metadata.json               ← {version, computed_at, depends_hash, row_count}
```

缓存 key = `(factor_name, version, date_range, depends_hash)`：

```python
import hashlib
import json


def compute_depends_hash(factor: Factor, data: dict[str, pd.DataFrame]) -> str:
    """
    计算依赖数据的指纹。
    对标公司 DependencyLineage 的 upstream version 记录。
    """
    parts = []
    for dep_name in sorted(factor.depends_on()):
        df = data[dep_name]
        # 用行数 + 最后修改时间作为快速指纹（不算全量 hash，太慢）
        parts.append(f"{dep_name}:{len(df)}:{df['date'].max()}")
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:12]
```

**缓存失效条件**（对标公司 ProductionStatus 的 BLACK/YELLOW）：

| 失效条件 | 对标公司机制 | 处理 |
|---|---|---|
| 上游数据更新（原始数据新增/修正） | ProductionStatus → YELLOW → 下游传导 | depends_hash 变化 → 失效 |
| 因子代码变更（compute 逻辑修改） | 不适用（C++ 重新编译部署） | version 字段变化 → 失效 |
| 手动强制重算 | usignal_terminal 将状态设为 BLACK | `force=True` 参数 |

**不做更细粒度的缓存**（如按单日、按单个 ticker）——A 股全市场日频因子约 5000 行/天，一年 120 万行，pandas 全量计算 <1 秒，细粒度缓存的管理成本大于收益。

### 58.5 增量更新（回应 6.2.3）

**公司的增量机制**：

```text
- reproduce_abnormal_dates_only(page 256109564):只重刷 ProductionStatus 为
  BLACK 的天数。配置一个 flag,usignal 运行时自动跳过正常天数
- DPP 增量生产(page 288529199):"只需要用户修改每次运行的 end-date 命令即可,
  预期只会拉起增量生产日期的 pod"
```

**个人平台增量策略**——两种模式：

```text
1. append 模式（日常更新）：
   已有缓存覆盖 [2020-01-01, 2024-12-31]
   新数据到达 [2025-01-02, 2025-01-03]
   → 只计算 [2025-01-02, 2025-01-03]，append 到现有 Parquet

2. full 模式（代码变更/上游修正/强制重算）：
   → 全量重算 [start, end]

选择逻辑：
  if factor.version 变化 or force=True:
      full 模式
  elif 上游数据有修正（depends_hash 对旧日期不一致）:
      full 模式（保守策略，避免增量/全量混合导致不一致）
  else:
      append 模式
```

**为什么个人平台不需要公司的 per-di 精细增量**：

```text
- 公司需要 per-di 增量是因为数据量大（6.52 亿行），全量重算代价高
- 个人平台全量重算一个因子（128 万行 × 10 年）耗时 <5 秒
- per-di 增量的复杂度（状态跟踪、断点续传、部分失败回滚）远超收益
```

```python
def compute_factor(
    dag: FactorDAG,
    factor_name: str,
    start: str,
    end: str,
    force: bool = False
) -> pd.DataFrame:
    """
    计算单个因子，自动处理缓存和增量。

    对标公司 usignal 的 work_di(di) 循环 + cache_uformula
    + reproduce_abnormal_dates_only。
    """
    factor = dag.factors[factor_name]
    cache_path = f"data/factors/{factor_name}/v{factor.version}/"

    # 1. 检查缓存
    cached = load_cache(cache_path, start, end)

    if cached is not None and not force:
        cached_end = cached["date"].max()
        if cached_end >= end:
            return cached  # 完全命中
        # append 模式: 只算新增部分
        new_start = next_trading_day(cached_end)
        new_data = _do_compute(factor, dag, new_start, end)
        result = pd.concat([cached, new_data]).drop_duplicates(["date", "ticker"])
        save_cache(cache_path, result)
        return result

    # full 模式
    result = _do_compute(factor, dag, start, end)
    save_cache(cache_path, result)
    return result
```

### 58.6 决策汇总与修订

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| DAG 构建 | 声明式 `depends_on()`，Kahn 拓扑排序，环=排序长度≠节点数 | 对标 Datamall depends_on + dw_asset_job_lineage（page 335950681, 381602916） |
| 静态分析 | **不做**（AST 解析 compute() 成本高/脆弱/不可靠） | 公司也是显式声明（event_sub/event_pub、depends_on） |
| 缓存粒度 | 按因子 × 日期范围（key = name+version+range+depends_hash） | 对标 DataPool 懒加载 + cache_uformula（page 222727558） |
| 缓存失效 | 三条件：上游更新→depends_hash 变；代码变更→version 变；手动→force=True | 对标 ProductionStatus BLACK/YELLOW（page 223886928） |
| 细粒度缓存 | 不做（单日/单 ticker 粒度管理成本大于收益，全量 <1s） | — |
| 增量更新 | append / full 两模式；version 变或上游修正一律 full | 对标 reproduce_abnormal_dates_only + DPP end-date（page 256109564, 288529199） |
| per-di 精细增量 | 不做（全量重算 128 万行 <5 秒，复杂度远超收益） | 公司需要是因为 6.52 亿行规模 |
| 并行 | 仅拓扑排序后独立因子可并行；不做多进程/多 pod 日期切分 | 公司 usignal-parallel/DPP 是 k8s 场景（page 244034279） |
| 调度形态 | FactorDAG 单机单进程整体调度；无 Kafka/无 k8s/无事件总线 | 对标 Datamall 三层调度的极度简化 |
| 新增代码 | FactorDAG 拓扑 ~40 行 + compute_depends_hash ~15 行 + compute_factor ~40 行 ≈ **~95 行** | 对标 work_di 循环 + 5 色灯状态机的替代 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§6.2** | 调度四问已定 → §58（DAG 构建 / 缓存 / 增量 / 并行）；失败处理与长耗时因子仍开放 |
| **factor_library/ FactorDAG** | 实现细化落盘：`register()` 解析 `factor:` 前缀建图 + `topological_sort()` Kahn 算法 + 环检测（§57.5 保留决策的落地代码） |
| **因子存储布局** | 缓存目录 `data/factors/{name}/v{version}/{range}.parquet + metadata.json`——与 §6.4/§27.4 的 `factors/{name}/` 布局一致，版本与指纹进 metadata |
| **§55 schema 演进** | depends_hash 快速指纹（行数+max(date)）与 §55"不搞全量 hash"思路一致——指纹要便宜，误报靠 version+force 兜底 |
| **§56 血缘简化** | 不做 DependencyLineage DAG 状态传播再次确认——缓存失效用 depends_hash 指纹替代 5 色灯传导 |
| **§3.11 不做版本化** | 因子侧例外：`version` 字段参与缓存 key（compute 逻辑变更必须 bump）——数据不版本化、因子逻辑版本化 |

**本节仍未解决**：`_do_compute` 的依赖装配细节（data dict 怎么从 load_bars + 上游因子缓存拼出来）——其余三项已定（2026-10-10 回改）：计算失败处理 → §59.3 fail-fast；长耗时 ML 因子调度 → §59.4 MLFactor；并行实现 → §59.2 ProcessPoolExecutor。

---

## 59. 因子的计算调度（续）：层内并行 + 失败处理 + ML 因子调度（§6.2 收口）

> §6.2 的续篇与收口。§58 已定 DAG 构建 / 缓存 / 增量三问，本节补齐剩余三块：**① 并行计算**（6.2.4——层内 ProcessPoolExecutor，不做按日期切分）、**② 计算失败的处理**（6.2.5——fail-fast + 下游跳过 + 同层继续，不引入 5 色灯）、**③ 长耗时因子（ML 因子）的调度**（6.2.6——MLFactor train/predict 分离 + 模型持久化 + 串行执行）。至此 §6.2 的六个子问题全部关闭（仅 `_do_compute` 依赖装配细节微开放）。与 §57（Factor 接口）、§58（DAG/缓存/增量）、§60（后处理）衔接。**新增代码 ≈140 行**。

### 59.1 Context 与 Confluence 参考

§58 确定了 FactorDAG 声明式依赖 + Kahn 拓扑、缓存按因子×日期范围、增量 append/full 两模式，遗留四问：并行的实现、失败处理、长耗时 ML 因子、`_do_compute` 依赖装配。本节回答前三问（第 4 问是实现细节）。

**Confluence 参考**：

```text
- Udata 多进程并行生产(page 244034279):usignal-parallel 任务级并行——
  mt="true" maxThread="5" → 独立 EB 用线程池并行;多 pod 按日期区间切分。
  失败重试:--retry 参数重试失败的 Task
- 数据生产状态(page 223886928):ProductionStatus 传导——BLACK → 下游全部标
  AMBER 并向下传导。"本数据可能存在异常,但是依赖的上游数据版本异常(黑)
  或可能异常(琥珀)"
- 事件驱动调度系统(page 324637543):Datamall——上游失败不发 asset_ready →
  下游不触发 → 超时后定时扫描兜底
- 调度优化:事件驱动 + 级联回刷(page 301602916):backfill API 支持
  stop_nodes 控制传播范围;DAG 层序计算(page 301602916)
- 数据生产计划 DPP(page 288529199):step 可定义 serial/parallel 模式
  (分阶段生产);长耗时任务分配独立资源(GPU pod);
  ML 模型推理作为独立的 EBS(EventBrick),有自己的资源分配
```

### 59.2 并行计算（回应 6.2.4）

**公司的并行方案**：

```text
- usignal 内部: mt="true" maxThread="5" → 独立 EB 用线程池并行
- usignal-parallel: 按日期区间切分到多 pod → k8s 集群并行
- DPP: 同上,pysim 版本
```

**个人平台并行方案**：

```text
并行粒度: 按因子并行(同一拓扑层的独立因子并行计算)
实现: concurrent.futures.ProcessPoolExecutor(绕过 GIL)
```

层内并行的依赖结构（Layer 0 独立因子并行 → Layer 1 等 Layer 0 完成后并行）：

```text
Layer 0(无依赖)          Layer 1               Layer 2
┌──────────────┐
│ momentum_20d │──┐
│ volatility   │──┼──→ vol_adj_momentum ──→ ...
│ size_rank    │──┘──→ composite_factor
└──────────────┘
  ↑ 并行计算              ↑ 等 Layer 0 完成后并行
```

```python
from concurrent.futures import ProcessPoolExecutor, as_completed


def compute_all(
    dag: FactorDAG,
    start: str,
    end: str,
    max_workers: int = 4,
    force: bool = False
) -> dict[str, pd.DataFrame]:
    """
    按 DAG 拓扑层序,层内并行计算所有因子。

    对标公司 usignal 的 mt=true + maxThread 并行。
    """
    results = {}
    layers = dag.topological_layers()  # [[layer0_names], [layer1_names], ...]

    for layer in layers:
        if len(layer) == 1:
            # 单因子,直接算
            name = layer[0]
            results[name] = compute_factor(dag, name, start, end, force)
        else:
            # 多因子,并行算
            with ProcessPoolExecutor(max_workers=min(max_workers, len(layer))) as pool:
                futures = {
                    pool.submit(compute_factor, dag, name, start, end, force): name
                    for name in layer
                }
                for future in as_completed(futures):
                    name = futures[future]
                    results[name] = future.result()  # 异常会在这里抛出

    return results
```

**不做按日期并行**：

```text
- 公司按日期并行是因为单天数据量大(全球多市场 tick 数据)
- 个人平台 A 股日频,单天 ~5000 行,pandas groupby 一次性处理全部日期
  更高效(向量化 > 循环)
```

**`topological_layers` 实现**（分层 = 拓扑排序的层序化，每层内因子互不依赖）：

```python
def topological_layers(self) -> list[list[str]]:
    """
    返回拓扑排序的分层结果。每层内因子互不依赖,可并行。
    对标公司 Datamall 的"DAG 层序计算"(page 301602916)。
    """
    in_degree = {n: len(deps) for n, deps in self.graph.items()}
    layers = []
    remaining = set(self.factors.keys())

    while remaining:
        layer = [n for n in remaining if in_degree[n] == 0]
        if not layer:
            raise ValueError("因子依赖图存在环")
        layers.append(layer)
        for n in layer:
            remaining.remove(n)
            for downstream in self.reverse.get(n, []):
                in_degree[downstream] -= 1

    return layers
```

`topological_layers()` 与 §58 的 `topological_sort()` 是同源算法——前者输出分层（供并行调度），后者输出线性序列（供串行执行）。环检测逻辑一致（某轮找不到入度 0 的节点 → 有环）。

### 59.3 计算失败的处理（回应 6.2.5）

**公司的失败处理**：

```text
- ProductionStatus 传导(page 223886928): BLACK → 下游全部标 AMBER 并向下
  传导。"本数据可能存在异常,但是依赖的上游数据版本异常(黑)或可能异常(琥珀)"
- usignal-parallel retry(page 244034279): --retry 参数重试失败的 Task
- Datamall 事件驱动(page 324637543): 上游失败不发 asset_ready → 下游不触发
  → 超时后定时扫描兜底
- Datamall 级联回刷(page 301602916): backfill API 支持 stop_nodes 控制
  传播范围
```

**个人平台失败处理**——策略：**fail-fast + 隔离 + 重试**：

```text
1. 单因子失败 → 标记为 FAILED
2. 下游因子 → 跳过(不尝试计算)
3. 同层其他独立因子 → 继续计算(不受影响)
4. 最终返回: 成功因子结果 + 失败因子列表
```

对标公司的 BLACK 传导 → AMBER（个人平台简化为跳过，不引入 5 色灯）：

```python
def compute_all_safe(
    dag: FactorDAG,
    start: str,
    end: str,
    max_workers: int = 4,
) -> tuple[dict[str, pd.DataFrame], dict[str, Exception]]:
    """
    容错版本: 失败因子不阻塞独立因子的计算。

    对标公司 ProductionStatus 的 BLACK → AMBER 传导:
    - 公司: 上游 BLACK → 下游标 AMBER,但不删除下游数据
    - 个人平台: 上游失败 → 下游跳过,返回错误信息
    """
    results = {}
    errors = {}
    failed_set = set()  # 失败的因子(含传导)

    for layer in dag.topological_layers():
        layer_futures = {}
        for name in layer:
            # 检查是否有上游失败
            upstream_failed = any(
                dep in failed_set for dep in dag.graph[name]
            )
            if upstream_failed:
                failed_set.add(name)
                errors[name] = RuntimeError(
                    f"上游因子失败,跳过: {[d for d in dag.graph[name] if d in failed_set]}"
                )
                continue

            try:
                results[name] = compute_factor(dag, name, start, end)
            except Exception as e:
                failed_set.add(name)
                errors[name] = e

    return results, errors
```

**不引入状态持久化（5 色灯系统）**——个人平台是离线批量计算,跑完就结束。公司的 5 色灯是为了持续运行的生产系统标记哪些数据需要重算。如果个人平台某个因子失败,修复后全量重算即可（<5 秒）。

### 59.4 长耗时因子（ML 因子）的调度（回应 6.2.6）

**公司的处理**：

```text
- 分阶段生产: DPP 中 step 可以定义 serial/parallel 模式(page 288529199 的 XML)
- 独立 pod: 长耗时任务分配独立资源(GPU pod)
- usignal service 分离: ML 模型推理作为独立的 EBS(EventBrick),有自己的资源分配
```

**个人平台 ML 因子设计**——ML 因子的特殊性：

```text
1. 训练耗时长(分钟~小时级)——普通因子 <1 秒
2. 训练 ≠ 推理: 训练用历史数据,推理用每天截面
3. 模型文件需要持久化
4. 需要避免 look-ahead bias(训练集不能包含测试集数据)
```

设计：将 ML 因子拆成两步——**train + predict**：

```python
class MLFactor(Factor):
    """
    ML 因子基类。
    增加 train() 和 model 管理,compute() 只做推理。

    对标公司的分阶段生产(DPP step serial/parallel)。
    """

    model_path: str = ""   # 模型存储路径
    retrain_freq: str = "monthly"  # monthly / quarterly / yearly

    @abstractmethod
    def train(self, data: dict[str, pd.DataFrame], train_end: str) -> Any:
        """训练模型,返回模型对象。train_end 严格切分防止 look-ahead。"""
        ...

    @abstractmethod
    def predict(self, model: Any, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """用已训练模型做推理。"""
        ...

    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """统一接口: 加载/训练模型 → 推理。"""
        model = self._load_or_train(data)
        return self.predict(model, data)

    def _load_or_train(self, data: dict[str, pd.DataFrame]) -> Any:
        if self.model_path and os.path.exists(self.model_path):
            return joblib.load(self.model_path)
        model = self.train(data, train_end=self._get_train_end(data))
        if self.model_path:
            joblib.dump(model, self.model_path)
        return model
```

调度层对 ML 因子的特殊处理：

```python
# 在 compute_all 中,ML 因子单独处理(不进线程池,避免阻塞)
def compute_all(dag, start, end, max_workers=4):
    for layer in dag.topological_layers():
        ml_factors = [n for n in layer if isinstance(dag.factors[n], MLFactor)]
        normal_factors = [n for n in layer if n not in ml_factors]

        # 普通因子: 并行
        _parallel_compute(normal_factors, ...)

        # ML 因子: 串行(避免内存爆炸),且带 timeout
        for name in ml_factors:
            results[name] = compute_factor(dag, name, start, end)
```

**不引入异步调度框架（Celery/Airflow/Prefect）**——个人平台单机运行,`concurrent.futures` 足够。公司用 k8s + Kafka + Celery 是因为分布式多机多 pod 场景。

### 59.5 决策汇总与整体调度流程

**决策汇总**：

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 并行计算 | 层内 ProcessPoolExecutor 并行（max_workers=4） | 对标 usignal mt=true + maxThread（page 244034279） |
| 不做按日期并行 | pandas 向量化处理全部日期更高效 | 公司按日期切分是 tick 级数据量驱动 |
| 失败处理 | fail-fast + 下游跳过 + 同层独立因子继续 | 对标 ProductionStatus BLACK → AMBER 传导（page 223886928） |
| 不引入 5 色灯 | 离线批量计算,失败后修复全量重算即可 | 5 色灯是持续运行生产系统的需求 |
| ML 因子 | MLFactor 子类: train/predict 分离 + 模型持久化 + 串行执行 | 对标 DPP 分阶段生产（page 288529199） |
| 不引入外部调度框架 | concurrent.futures 足够,单机场景不需要 Celery/Airflow | 公司 k8s+Kafka+Celery 是分布式需求 |
| 分层算法 | `topological_layers()` 分层（Kahn 层序化）,每层内可并行 | 对标 Datamall DAG 层序计算（page 301602916） |

**整体因子调度流程图**：

```text
用户调用:
  compute_all(dag, start="2020-01-01", end="2024-12-31")

FactorDAG.topological_layers()
  → [[momentum_20d, volatility_20d, size_rank],   ← Layer 0
     [vol_adj_momentum, composite],               ← Layer 1
     [ml_stock_selector]]                         ← Layer 2

Layer 0: 并行(ProcessPoolExecutor)
  momentum_20d   → check cache → miss → compute → save cache
  volatility_20d → check cache → hit  → return cached
  size_rank      → check cache → miss → compute → save cache

Layer 1: 并行
  vol_adj_momentum → depends_hash changed? → yes → full recompute
  composite        → depends_hash same?    → yes → append new dates

Layer 2: ML 因子串行
  ml_stock_selector → load model → predict → save cache

返回: {factor_name: DataFrame, ...}
```

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§6.2** | **收口 → 已定**：六个子问题全关——DAG/缓存/增量（§58）+ 并行/失败/ML 调度（§59）。仅 `_do_compute` 依赖装配细节微开放（实现细节,不阻塞） |
| **§58 本节仍未解决** | 关闭 3/4：失败处理（→ 59.3 fail-fast）、长耗时 ML（→ 59.4 MLFactor）、并行实现（→ 59.2 ProcessPoolExecutor）；`_do_compute` 装配仍开放 |
| **factor_library/ FactorDAG** | 增加 `topological_layers()` 方法（与 `topological_sort()` 同源） |
| **compute_all** | 新入口：拓扑分层 + 层内并行 + ML 隔离；容错版 `compute_all_safe` 返回 (results, errors) |
| **MLFactor** | 新增子类（继承 §57 Factor ABC）——train/predict 分离是 ML 因子与普通因子的接口分界 |
| **§8 ML 信号管理** | MLFactor 是因子侧接口；模型训练策略/特征管理/信号入库仍在 §8 层面讨论 |

**本节仍未解决**：`_do_compute` 的依赖装配细节（data dict 怎么从 load_bars + 上游因子缓存拼出来——预计 <20 行）；MLFactor 的 `retrain_freq` 如何触发重训（每次全量跑时检查时间戳？）；ML 因子的 timeout 具体值与超时后行为。

---

## 60. 因子的后处理流水线：PostProcessor Pipeline（§6.3 全解）

> §6.3 的完整讨论稿（§16.2 已给原则，本节给实现级细节）。公司后处理内嵌在 UFormula 公式链里（`cszscore{csindneutral{...}}`，编译时确定）；个人平台用**独立的 PostProcessor Pipeline**（运行时可配置）。四步流水线 **去极值 → 填充 → 标准化 → 中性化（可选）**；关键决策：**存原始值、消费时做后处理**；所有步骤 `groupby(date)` 截面独立计算防前视。与 §16.2（预处理原则）、§57（Factor 接口）、§58（缓存存什么）衔接。**新增代码 ≈110 行**。

### 60.1 Context 与 Confluence 参考

§6.1 确定了因子接口（Factor ABC → DataFrame[date, ticker, factor_value]），§6.2 确定了调度（FactorDAG 拓扑排序 + 层内并行 + 缓存）。现在设计后处理流水线：原始因子值 → 干净因子值。

**Confluence 参考**：

```text
- UFormula 截面算子(page 228460124): 公司截面后处理的算子集——csrank(截面
  排名)、cszscore/csnorm(截面标准化)、csnormminmax(min-max 归一化)、
  csindneutral(行业中性化)、csfacneutral(因子中性化)。这就是公司因子后处理
  的全部工具——全在 UFormula 引擎里,作为公式的一部分,而非独立的后处理步骤。
- 量化学习教程(page 301832392) §8.7 因子预处理的标准三步——[1] 去极值
  (3 倍 MAD winsorize 到边界) → [2] 标准化(截面 z-score) → [3] 中性化
  (删除行业 + 市值暴露)。§7.4 完整因子工厂流水线: 因子定义 → 数据预处理 →
  单因子检验 → 因子库管理 → 多因子合成 → 组合优化 → 回测 → 实盘。
- 量化学习教程·中性化实战(page 301832392 §26.1): 关键 case——新闻 sentiment
  因子原始 IC=0.08、t=2.4,做行业+市值中性化后 IC 降到 0.02、t=1.0。"没有中性化
  的 IC 都是不可信的——80% 的伪 alpha 在中性化后归零"。中性化方法: OLS 回归
  (因子 ~ 行业哑变量 + log(市值)),取残差。
- 量化学习教程·散户建议(page 301832392 §7.7): ETF 池不做行业中性(本来就在做
  行业轮动赚 Beta);个股池必须中性化。去极值用 3 倍 MAD winsorize + z-score。
- 量化学习教程·入库标准(page 301832392 §8.9): Rank IC > 0.04、IR > 0.5、
  t > 2(保守用 t > 3)、分组单调、与现有因子相关 < 0.5、时间/市场/行业稳健。
```

### 60.2 公司 vs 个人平台的后处理架构差异

**公司做法：后处理 = UFormula 公式的一部分**。公司因子公式（UFormula XML）：

```xml
<formula tag="alpha_clean"
        formula="cszscore{csindneutral{tsrank{tsmean{close, 20}, 500}, ind}}" />
```

读法：

```text
tsmean(close, 20)      → 20 日均价(时序算子)
tsrank(..., 500)       → 500 日时序排名
csindneutral{..., ind} → 行业中性化(截面算子)
cszscore(...)          → 截面标准化(截面算子)
```

后处理和因子计算是一个公式链——不存在独立的"后处理步骤"。

**公司 UFormula 截面算子对照表**（page 228460124）：

| UFormula 算子 | 功能 | 个人平台等价 |
|---|---|---|
| csrank | 截面排名(百分比) | `df.groupby("date")["x"].rank(pct=True)` |
| cszscore / csnorm | 截面 z-score 标准化 | `groupby("date").transform(lambda s: (s-s.mean())/s.std())` |
| csnormminmax | 截面 min-max 归一化 | `groupby("date").transform(lambda s: (s-s.min())/(s.max()-s.min()))` |
| csindneutral | 行业中性化 | OLS 回归取残差(factor ~ 行业哑变量) |
| csfacneutral | 因子中性化(更通用) | OLS 回归取残差(factor ~ 任意因子矩阵) |

公司没有独立的"去极值"算子——因为 UFormula 在 C++ tensor 上运算,极值不影响 GPU 计算效率;去极值在研究流程里做(Python 端),不在 UFormula 引擎里做。

**个人平台做法：独立 PostProcessor 流水线**。和公司的关键区别：公司的后处理内嵌在 UFormula 公式里(编译时确定),个人平台的后处理是独立的 Python pipeline(运行时可配置)。这是设计选择而非妥协——Python pipeline 比 XML 公式更灵活,适合研究阶段快速迭代。

### 60.3 后处理步骤集合与算法选型

**标准流水线（按顺序执行）**：

```text
原始因子值 DataFrame[date, ticker, factor_value]
  ↓
[1] 去极值(winsorize)  目的: 压制极端值对截面统计量的影响
  ↓
[2] 缺失值填充(fillna)  目的: 确保截面完整性,后续步骤需要完整截面
  ↓
[3] 标准化(standardize) 目的: 消除量纲,让不同因子可比、可合成
  ↓
[4] 中性化(neutralize) [可选] 目的: 剔除行业/市值暴露,只留纯 alpha
  ↓
干净因子值 DataFrame[date, ticker, factor_value_clean]
```

**为什么是这个顺序**：

```text
1. 去极值必须在标准化之前——极值会严重扭曲均值和标准差,导致 z-score 失真
2. 填充在去极值之后、标准化之前——去极值可能不处理 NaN,但标准化和中性化
   需要完整截面
3. 标准化在中性化之前——中性化的 OLS 回归对量纲不敏感(回归本身会处理),
   但标准化后的因子更便于中性化前的诊断(查看因子分布)
4. 中性化是最后一步——中性化后的残差已经是最终的"纯 alpha"
```

**[1] 去极值：MAD（推荐）vs 分位数**：

| 方法 | 算法 | 优点 | 缺点 |
|---|---|---|---|
| MAD winsorize(推荐) | 边界 = median ± n × 1.4826 × MAD | 对离群值鲁棒(median 不受极值影响) | 对非对称分布可能过于激进 |
| 分位数 winsorize | 边界 = [percentile(p), percentile(1-p)] | 简单直观 | 分位数本身受极值影响(尤其小样本) |
| 3σ winsorize | 边界 = mean ± 3 × std | 经典统计学 | mean 和 std 都受极值影响——用受污染的统计量去处理污染源,逻辑矛盾 |

选 MAD,和公司量化学习教程(page 301832392)一致："去极值:超过 3 倍 MAD(中位数绝对偏差)的 winsorize 到边界"。

```python
def winsorize_mad(s: pd.Series, n: float = 3.0) -> pd.Series:
    """
    MAD winsorize。对标公司量化教程 §8.7 的第一步。

    MAD = median(|x - median(x)|)
    边界 = median ± n × 1.4826 × MAD
    1.4826 是正态分布下 MAD 到 σ 的换算系数
    """
    med = s.median()
    mad = (s - med).abs().median()
    upper = med + n * 1.4826 * mad
    lower = med - n * 1.4826 * mad
    return s.clip(lower, upper)
```

参数 n=3.0：和 3σ 等价(在正态分布下)。实践中 n=3 是行业共识(page 301832392："超过 3 倍 MAD 的 winsorize 到边界")。不需要调参——3 已经足够保守。

**[2] 缺失值填充**：

| 方法 | 适用场景 | 个人平台选择 |
|---|---|---|
| 截面中位数填充(推荐) | 通用默认 | 缺失值取当日截面中位数——不影响截面分布 |
| 前值填充(ffill) | 时序因子 | 停牌/缺失时用前一天的值——适合动量类 |
| 0 填充 | 二元因子 | 事件因子: 无事件 = 0 |
| 不填充(dropna) | 截面因子 | 在截面运算中自动忽略 NaN |

默认用截面中位数填充,但通过参数可选。停牌股应该在因子消费端(策略/回测)处理,不应在后处理里假设。

```python
def fillna_cross_section(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """截面中位数填充。"""
    df[col] = df.groupby("date")[col].transform(
        lambda s: s.fillna(s.median())
    )
    return df
```

**[3] 标准化：z-score vs rank**：

| 方法 | 算法 | 优点 | 缺点 | 适用 |
|---|---|---|---|---|
| 截面 z-score(默认) | (x - mean) / std | 保留因子值的相对幅度信息 | 对非正态分布敏感 | 大多数因子 |
| 截面 rank(备选) | rank(pct=True) | 完全非参数,对分布无假设 | 丢失幅度信息 | 分布严重偏态的因子 |

默认 z-score,和公司量化教程一致("截面 z-score,消除量纲",page 301832392 §7.3)。对标公司 UFormula: cszscore = z-score,csrank = rank。两者都提供,适用不同场景。

```python
def standardize_zscore(s: pd.Series) -> pd.Series:
    """截面 z-score。对标公司 UFormula 的 cszscore 算子。"""
    return (s - s.mean()) / s.std()


def standardize_rank(s: pd.Series) -> pd.Series:
    """截面 rank。对标公司 UFormula 的 csrank 算子。"""
    return s.rank(pct=True)
```

用哪个？规则：

```text
- 因子分布近似正态(如动量、均值回复) → z-score
- 因子分布严重偏态(如换手率、市值) → rank
- 不确定 → z-score 和 rank 都跑一遍,看哪个 IC 更高
```

**[4] 中性化：行业 + 市值（可选但重要）**。公司做法(page 301832392 §26.1)：

```text
# 机构标准做法: OLS 回归取残差
干净因子 = 残差(原始因子 ~ 行业哑变量 + log(市值))
```

个人平台实现：

```python
import numpy as np
from numpy.linalg import lstsq


def neutralize(
    df: pd.DataFrame,
    factor_col: str,
    industry_col: str = "industry",    # 行业分类列
    size_col: str = "log_market_cap",  # log(市值)列
    method: str = "industry_size"      # industry / size / industry_size
) -> pd.DataFrame:
    """
    截面中性化。对标公司 UFormula 的 csindneutral / csfacneutral 算子。

    方法: 每个截面(date)做 OLS 回归,取残差。
      factor ~ 行业哑变量 + log(市值)

    对标量化教程(page 301832392 §26.1):
      "每个截面对 sentiment 做 OLS 回归,剔除行业(28 个中信一级)
       和 log(市值)的解释力,取残差作为新 sentiment"
    """
    result = df.copy()

    for date, group in df.groupby("date"):
        y = group[factor_col].values
        valid = ~np.isnan(y)
        if valid.sum() < 10:
            continue

        X_parts = []
        if "industry" in method:
            dummies = pd.get_dummies(group[industry_col], drop_first=True)
            X_parts.append(dummies.values)
        if "size" in method:
            X_parts.append(group[size_col].values.reshape(-1, 1))

        X = np.hstack(X_parts) if X_parts else np.ones((len(y), 1))
        X = np.column_stack([np.ones(len(y)), X])  # 加截距

        # OLS: y = Xβ + ε,取 ε
        beta, _, _, _ = lstsq(X[valid], y[valid], rcond=None)
        residual = y - X @ beta

        result.loc[group.index, factor_col] = residual

    return result
```

**中性化的适用条件**（page 301832392 §8.7 + §7.7）：

| 标的池 | 是否中性化 | 理由 |
|---|---|---|
| 个股池 | 必须做 | "个股池 → 必须中性化,否则 IC 全是假的" |
| ETF 池 | 不做 | "ETF 标的池不需要中性化——你本来就在做行业/风格轮动" |
| 行业指数池 | 不做 | 同 ETF 理由 |

### 60.4 关键设计决策：存原始值，消费时做后处理

**公司的做法：计算时做（UFormula 公式内嵌）**：

```xml
<!-- 公司: 后处理写在 UFormula 公式里,计算时就做完了 -->
<formula tag="alpha" formula="cszscore{csindneutral{tsrank{close, 500}, ind}}" />
```

因子存储(feature_data Udata)里保存的是已经后处理过的值。

**个人平台的决策：存原始值,消费时做后处理**：

```text
因子存储:
  data/factors/momentum_20d/v1.0/xxxx.parquet
  → 保存原始 compute() 输出,未经后处理

消费时:
  # 研究/回测时按需后处理
  raw = load_factor("momentum_20d")
  clean = pipeline.transform(raw)  # winsorize → fillna → zscore → neutralize
```

理由：

| 考量 | 存原始值 | 存后处理值 |
|---|---|---|
| 灵活性 | ✓ 可以随时换后处理参数(如 MAD 从 3 改 5) | ✗ 改参数要全量重算因子 |
| 可复现 | ✓ 原始值 + 后处理代码 = 完全可复现 | ✗ 丢失原始分布信息 |
| 存储成本 | 一样(只存一份) | 一样 |
| 计算成本 | 后处理 <1 秒(截面运算很快) | 节省 <1 秒(可忽略) |
| 多套参数 | ✓ 同一份原始值用不同参数 | ✗ 需要多次存储 |
| 调试 | ✓ 能看到"中性化前后 IC 的变化" | ✗ 只能看到最终 IC |

公司存后处理值是因为：UFormula 在 GPU 上计算,后处理是公式链的一部分;存中间值需要额外 Udata(成本高)。个人平台全量后处理 <1 秒,没有此约束。**该决策与 §58 缓存设计一致**——缓存的是 `compute()` 原始输出,后处理不进缓存 key。

**样本内 vs 样本外：前视偏差处理**：

```text
问题: 如果用全样本的统计量(均值、标准差、MAD)做后处理,相当于用了未来信息

错误做法:
  2020年1月的因子值,用 [2020-2024] 全样本的 mean/std 做 z-score
  → 2020年1月的标准化值"知道了" 2024年的分布 → 前视偏差

正确做法:
  2020年1月的因子值,只用 [2020年1月当天截面] 的 mean/std 做 z-score
  → 截面标准化天然无前视(只看同一天的其他股票)
```

**各步骤的前视风险分析**：

| 步骤 | 前视风险 | 正确做法 |
|---|---|---|
| 去极值 | ⚠ 如果用全样本 MAD → 有前视 | ✓ 截面 MAD(每个 date 独立计算) |
| 填充 | ⚠ 如果用未来值 ffill → 有前视 | ✓ 截面中位数(同日截面)或向前 ffill(只用过去值) |
| z-score | ⚠ 如果用全样本 mean/std → 有前视 | ✓ 截面 z-score(每个 date 独立计算) |
| rank | ✓ 无前视 | 截面 rank 天然无前视(只看同日排名) |
| 中性化 | ⚠ 如果用全样本回归系数 → 有前视 | ✓ 截面中性化(每个 date 独立回归) |

核心原则：**所有截面运算按 date 分组独立计算——天然无前视**。这和公司 UFormula 的 cs* 算子完全一致——csrank{x}、cszscore{x} 都是按当前截面计算,不跨日。

```python
# 正确: 每个 date 独立
df["factor_clean"] = df.groupby("date")["factor"].transform(winsorize_mad)
df["factor_clean"] = df.groupby("date")["factor_clean"].transform(standardize_zscore)

# 错误: 用全样本
df["factor_clean"] = winsorize_mad(df["factor"])       # ← 前视偏差!
df["factor_clean"] = standardize_zscore(df["factor_clean"])  # ← 前视偏差!
```

### 60.5 PostProcessor 实现设计与决策汇总

**Pipeline 模式**：

```python
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class PostProcessStep:
    name: str
    func: Callable[[pd.Series], pd.Series]  # 截面级函数
    params: dict = field(default_factory=dict)


class PostProcessor:
    """
    因子后处理流水线。

    每一步都是截面级操作(groupby date),天然避免前视偏差。
    对标公司 UFormula 的截面算子链(cszscore{csindneutral{...}})。
    """

    def __init__(self, steps: list[PostProcessStep] = None):
        if steps is None:
            # 默认流水线: 去极值 → 填充 → z-score
            self.steps = [
                PostProcessStep("winsorize", winsorize_mad, {"n": 3.0}),
                PostProcessStep("fillna", lambda s: s.fillna(s.median())),
                PostProcessStep("standardize", standardize_zscore),
            ]
        else:
            self.steps = steps

    def transform(
        self,
        df: pd.DataFrame,
        factor_col: str,
        date_col: str = "date",
    ) -> pd.DataFrame:
        """
        按步骤依次对因子做截面后处理。
        每步都是 groupby(date).transform(),保证无前视。
        """
        result = df.copy()
        for step in self.steps:
            result[factor_col] = result.groupby(date_col)[factor_col].transform(
                step.func, **step.params
            )
        return result
```

**预设 Pipeline 配置**：

```python
# 研究阶段: 去极值 + z-score(快速查看因子特征)
PIPELINE_RESEARCH = PostProcessor([
    PostProcessStep("winsorize", winsorize_mad, {"n": 3.0}),
    PostProcessStep("fillna", lambda s: s.fillna(s.median())),
    PostProcessStep("standardize", standardize_zscore),
])

# 个股因子检验: 去极值 + z-score + 行业市值中性化
PIPELINE_STOCK_EVAL = PostProcessor([
    PostProcessStep("winsorize", winsorize_mad, {"n": 3.0}),
    PostProcessStep("fillna", lambda s: s.fillna(s.median())),
    PostProcessStep("standardize", standardize_zscore),
    # 中性化需要额外列,单独调用 neutralize()
])

# ETF 轮动: 去极值 + z-score(不中性化)
PIPELINE_ETF = PostProcessor([
    PostProcessStep("winsorize", winsorize_mad, {"n": 3.0}),
    PostProcessStep("fillna", lambda s: s.fillna(s.median())),
    PostProcessStep("standardize", standardize_zscore),
])

# 排名制: 去极值 + rank(非参数,对偏态分布鲁棒)
PIPELINE_RANK = PostProcessor([
    PostProcessStep("winsorize", winsorize_mad, {"n": 3.0}),
    PostProcessStep("fillna", lambda s: s.fillna(s.median())),
    PostProcessStep("standardize", standardize_rank),
])
```

**决策汇总**：

| 决策 | 结论 | Confluence 参考 |
|---|---|---|
| 步骤集合 | 去极值 → 填充 → 标准化 → 中性化(可选) | 量化教程 §8.7 三步标准流程(page 301832392) |
| 去极值算法 | MAD winsorize(n=3.0) | "3 倍 MAD winsorize 到边界"(page 301832392) |
| 标准化算法 | 默认 z-score,备选 rank | 对标 cszscore / csrank(page 228460124) |
| 中性化算法 | OLS 回归取残差(factor ~ 行业哑变量 + log 市值) | 对标 csindneutral / csfacneutral(page 228460124) + §26.1 实战案例 |
| 中性化条件 | 个股池必须做,ETF 池不做 | "ETF 不需要中性化"(page 301832392 §7.7) |
| 正交化 | 不独立实现——中性化回归中加入其他因子列即可(csfacneutral 思路) | UFormula csfacneutral 算子 |
| 填充策略 | 默认截面中位数 | 不影响截面分布 |
| 参数固定 | MAD n=3.0,不需要调参 | 行业共识 |
| 前视偏差 | 所有步骤 groupby(date) 截面独立计算 | 和公司 cs* 算子一致——按截面计算,不跨日 |
| 存储 | 存原始值,消费时做后处理 | 公司存后处理值是 UFormula GPU 约束;个人平台后处理 <1 秒,存原始值更灵活 |
| 现有 PostProcessor | 保留并扩展——加入 MAD 算法 + 中性化步骤 + Pipeline 预设 | |
| 实现模式 | Pipeline 模式(可配置步骤列表) | 对标 UFormula 算子链的可组合性 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§6.3** | **升级为全解 → §16.2 / §60**（原则在 §16.2,实现细节在 §60——四步流水线 + 各步算法 + Pipeline 实现全部落定） |
| **PostProcessor** | 保留并扩展：winsorize 升级为 MAD 算法(现有 winsorize→standardize→fillna 顺序按 §60.3 修正为 winsorize→fillna→standardize→neutralize) + 新增 neutralize() + PostProcessStep/PostProcessor Pipeline 化 + 4 个预设配置 |
| **§58 缓存设计** | 确认缓存存的是原始 compute() 输出——后处理不进缓存 key(消费时做) |
| **§16.5 正交化** | 再次确认：不做独立正交化——中性化回归里加其他因子列即 csfacneutral 思路 |
| **§27 因子评价** | PostProcessor 输出对接 alphalens——clean 因子 → MultiIndex Series;预处理参数记录进 eval 元数据(可复现) |
| **§57 Factor ABC** | compute() 输出仍是原始值;后处理不属于 Factor 接口,是下游独立步骤——与公司"后处理内嵌公式链"的差异是刻意设计 |

**本节仍未解决**：停牌股在消费端（策略/回测）的具体处理规则；中性化所需的行业分类/市值数据来源（行业分类已定 → §63 申万一级 31 行业哑变量；log 市值用流通还是总市值仍开放，2026-10-10 回改）；PostProcessor 与 alphalens 入参的对接顺序（已定 → §27 FactorPipeline preprocess 中性化在前、alphalens 只评价不再中性化，2026-10-10 回改）；`PostProcessStep.params` 传参形式（`transform(step.func, **step.params)` 与 pandas transform 的签名匹配）。

---

## 61. 因子的存储布局：每因子一表 + 按年分区 + 语义化版本 + metadata.json（§6.4 全解）

> §6.4 的完整讨论稿。公司因子落库是 feature_data Udata 的 **DynamicData Unpacked** 形态（每字段独立 ucube 文件）+ SchemaBase 元数据 + random_string_timestamp 版本目录；个人平台对齐为 **Direction A 每因子一表**：`data/factors/{name}/v{version}/{year}.parquet + metadata.json`，**按年分区**、**DOUBLE 精度**、**语义化版本**（1.0/2.0 多版本共存）、**日常 append + 触发式全量**、**Parquet + DuckDB 不用 ClickHouse**。与 §57（Factor 接口）、§58（缓存目录）、§60（存原始值）衔接。**新增代码 ≈65 行**。

### 61.1 Context 与 Confluence 参考

§57 定了因子输出形态（`compute()` → DataFrame[date, ticker, {factor_name}]），§58 定了缓存目录草图（`data/factors/{name}/v{version}/{range}.parquet`），§60 定了存原始值（不存后处理值）。本节把这些落成**可直接实施的物理布局**：目录结构、Parquet schema、元数据、分区、版本化、增量写入、查询方式。

**Confluence 参考**：

```text
- SchemaBase(page 199033106): 公司数据资产的元数据模型——datafile_path
  (数据文件位置) + TypeInfo(类型系统) + ProductionStatus(5 色灯生产状态)
  + DependencyLineage(依赖血缘)。这是公司所有 Udata 的 schema 骨架
- Udata_DynamicData(page 205515828): 列式存储、字段动态添加。Unpacked
  layout 下每个字段独立 ucube 文件(field_momentum_20d.ucube),Schema 层
  呈现宽表抽象。新增因子=新增字段文件,不动已有文件
- 数据版本管理(page 223886928): version 字段=random_string_timestamp,
  同一数据资产多版本共存,按 version 目录隔离
- DPP 增量生产(page 288529199): "修改 end-date 即可,预期只会拉起增量
  生产日期的 pod"——增量写入只碰新日期
- Alpha 入库 ClickHouse(page 288524848): 公司因子(Alpha)最终入 ClickHouse
  供实盘信号消费——这是实盘多策略并发查询的需求,不是存储本身的需求
- 精度事故(page 263692149): §5.2 的 float→double 事故——float32 精度
  丢失在下游计算(尤其中性化 OLS 回归)中被放大,导致因子值漂移
- 存储空间报表(page 301623630): 全平台 ~850TB,因子类资产是大头——
  存储布局必须考虑压缩与分区效率
```

### 61.2 公司因子存储架构还原

**feature_data Udata 的物理形态**（page 205515828）：

```text
feature_data/                       ← Udata 资产根目录
└── schema.info                     ← SchemaBase 元数据(JSON)
└── of_udata/
    └── di_0/                       ← 按 di(交易日)组织
        ├── field_momentum_20d.ucube    ← 该字段(因子)的独立文件
        ├── field_volatility_20d.ucube
        └── field_size_rank.ucube
```

关键特征：

```text
1. DynamicData: 字段动态添加——新因子=新字段文件,不重写已有数据
2. Unpacked layout: 每个字段(因子)独立 ucube 文件,不是宽表大文件
3. Schema 层宽表抽象: GetViewer 查询时按 (di, ticker) 联合成宽表呈现
4. 版本目录: version=random_string_timestamp,多版本共存
5. 元数据集中: SchemaBase 记录位置/类型/状态/血缘
```

**关键洞察：公司的物理存储本质就是"每因子一表"**——Unpacked 的 field_xxx.ucube 就是每个因子的独立列存文件。所谓"宽表"只是 Schema 层的查询抽象，不是物理布局。个人平台直接把这个本质显式化为目录隔离，省掉 Schema 层的间接。

### 61.3 四种存储模型的分析与决策

**Direction A：每因子一表（长窄表 × N 个目录）**——每个因子一个目录、一套按年 Parquet 文件：

```text
data/factors/momentum_20d/v1.0/{2020..2024}.parquet   ← date, ticker, momentum_20d
data/factors/volatility_20d/v1.0/{2020..2024}.parquet ← date, ticker, volatility_20d
```

| 维度 | 评价 |
|---|---|
| 优势 | 因子完全独立——增删改互不影响；增量写入简单（只碰一个文件）；版本化清晰（每因子自己的 v 目录）；调试方便（单因子单文件） |
| 劣势 | 多因子联合要 N 次读 + merge；文件数量 = 因子数 × 年数 |
| 对标公司 | DynamicData Unpacked（page 205515828）——field_xxx.ucube 即此形态 |

**Direction B：长表 EAV（date, ticker, factor_name, value）**——全部因子塞一张大表：

| 维度 | 评价 |
|---|---|
| 优势 | 单表管理简单；新增因子不动 schema |
| 劣势 | **行数爆炸**——2500 天 × 5000 股 × 50 因子 = 6.25 亿行；Parquet 列裁剪优势被浪费（value 列混杂所有因子）；单因子查询要 filter factor_name，无法列裁剪 |
| 结论 | **不用** |

**Direction C：宽表（date, ticker, factor1, factor2, ...）**——一张宽表装所有因子：

| 维度 | 评价 |
|---|---|
| 优势 | 联合查询零 merge；一次读全部因子 |
| 劣势 | 新增因子要重写所有文件（Parquet 加列=重写）；版本化困难（一个因子升版要动整表）；单因子读也拖整表 |
| 结论 | **不用**（按需在消费端合并，见 61.8） |

**Direction D：三维张量（date × ticker × factor）**——HDF5/zarr 数组：

| 维度 | 评价 |
|---|---|
| 优势 | ML 训练直接喂张量；内存紧凑 |
| 劣势 | **引入 HDF5/zarr 额外依赖，违反 §4"只用 Parquet + DuckDB"**；缺列不是简单问题（停牌股对齐）；与 alphalens/DuckDB 生态不通 |
| 结论 | **不用**（ML 训练在消费端从 Parquet 拼 numpy） |

**决策：Direction A + 按需合并为宽表**（对标公司 Unpacked + GetViewer 抽象）：

```python
def load_factor_matrix(
    names: list[str],
    start: str,
    end: str,
    version: str = "1.0",
    base_dir: str = "data/factors",
) -> pd.DataFrame:
    """
    把多个因子联合成宽表 (date, ticker, factor1, factor2, ...)。

    对标公司 GetViewer 的联合查询 / feature_data Schema 层宽表抽象。
    物理上每因子独立存储(Direction A),逻辑上可任意拼宽表。
    """
    frames = []
    for name in names:
        df = load_factor(name, start, end, version=version, base_dir=base_dir)
        frames.append(df.set_index(["date", "ticker"]))
    # outer join: 保留任一因子有值的 (date, ticker)
    wide = pd.concat(frames, axis=1, join="outer").reset_index()
    return wide.sort_values(["date", "ticker"]).reset_index(drop=True)
```

**为什么"物理隔离 + 逻辑合并"优于物理宽表**：

```text
1. 因子迭代快——研究阶段一天可能加/删/改 5 个因子,物理宽表每次都重写
2. 版本独立——momentum_20d 升 2.0,volatility_20d 不受影响
3. 存储高效——单因子只读自己,列裁剪生效
4. 合并成本低——50 个因子 outer merge 全历史 <2 秒,不值得为省这 2 秒牺牲以上三点
```

### 61.4 物理目录结构与 Parquet Schema

**目录结构**：

```text
data/factors/
└── {factor_name}/                  ← 一个因子一个目录
    ├── v1.0/                       ← 语义化版本目录
    │   ├── 2020.parquet            ← 按年分区
    │   ├── 2021.parquet
    │   ├── 2022.parquet
    │   ├── 2023.parquet
    │   ├── 2024.parquet
    │   └── metadata.json           ← 该因子该版本的元数据
    ├── v2.0/                       ← 新版本共存(不覆盖 v1.0)
    │   └── ...
    └── model/                      ← ML 因子专属(§59 MLFactor)
        └── model.joblib
```

**单个 Parquet 文件的 Schema**：

| 列名 | 类型 | 说明 |
|---|---|---|
| date | VARCHAR | 交易日，`YYYYMMDD`（§5.2 全库统一约定） |
| ticker | VARCHAR | 证券代码，`000001.SZ`（带交易所后缀） |
| {factor_name} | DOUBLE | 因子值——**原始值，未经后处理**（§60 决策）；NaN = 停牌/缺失 |

**字段设计决策**：

| 决策 | 结论 | 理由 |
|---|---|---|
| date 类型 | VARCHAR `YYYYMMDD` | §5.2 全库统一；字典序 = 时间序，排序/过滤直观 |
| ticker 格式 | `000001.SZ` | §5.2 既定；与 bars 表可直接 JOIN |
| 数值精度 | **DOUBLE** | §5.2 float→double 事故（page 263692149）——float32 精度丢失在下游（尤其中性化 OLS 回归求解）被放大；存储成本差异可忽略 |
| 列名 | 用因子名（不用固定 `value`） | 读进来直接可读；与 Factor.name 一致；合并宽表时无需 rename |
| NaN | 保留不填 | 停牌/缺失与"值为 0"语义不同；填充是 §60 后处理的事 |
| 索引 | 不写 index | `to_parquet(index=False)`；date/ticker 是普通列，与 DuckDB 查询一致 |

### 61.5 metadata.json 与分区策略

**metadata.json 结构**：

```json
{
  "factor_name": "momentum_20d",
  "version": "1.0",
  "description": "20 日动量（pct_change(20)）",
  "category": "time_series",
  "frequency": "1d",
  "asset_types": ["stock"],
  "created_at": "2026-10-04T10:00:00",
  "updated_at": "2026-10-04T10:05:00",
  "depends_on": ["bars_daily"],
  "depends_hash": "a3f9c2e81b04",
  "date_range": ["20200101", "20241231"],
  "row_count": 1280000,
  "file_count": 5,
  "compression": "zstd"
}
```

字段说明：

| 字段 | 用途 | 来源 |
|---|---|---|
| factor_name / version / description / category / frequency / asset_types | 因子身份（§57 类属性镜像） | Factor 类属性 |
| created_at / updated_at | 血缘与排查 | 写入时打时间戳 |
| depends_on / depends_hash | 缓存失效判定（§58） | `compute_depends_hash()` |
| date_range / row_count / file_count | 覆盖范围快速检查（不用扫文件） | 写入时统计 |
| compression | 压缩参数记录 | to_parquet 参数 |

**对标公司 SchemaBase 的映射**（page 199033106）：

| SchemaBase 字段 | 个人平台对应 | 说明 |
|---|---|---|
| datafile_path | 目录结构隐含：`data/factors/{name}/v{ver}/` | 路径即约定，不单独记 |
| TypeInfo（类型系统） | Parquet 自描述 schema | 文件自带列类型，无需外部类型表 |
| ProductionStatus（5 色灯） | **不需要** | §58 已定——depends_hash + version 失效判定替代状态传导 |
| DependencyLineage | metadata.json 的 `depends_on` + `depends_hash` | 轻量血缘：记录依赖了谁、当时数据指纹 |

**分区策略：按年**：

| 分区 | 每文件行数 | 大小 | 评价 |
|---|---|---|---|
| **按年（选定）** | ~125 万行 | raw ~30MB → zstd 5-10MB | 因子一次算整年；增量 append 只碰当年文件；5-10 个文件，DuckDB glob 高效 |
| 按天 | ~5000 行 | ~150KB | 2500 个小文件——文件系统开销大，DuckDB 打开成本远超扫描收益 |
| 按月 | ~10 万行 | ~1MB | 太小；60 个文件仍偏多，又没有按天的"按 di 回刷"需求 |
| 按资产 | 每股 ~2400 行 | ~70KB | 只有 A 股一个市场，按资产分区无意义（公司按 di/asset 分是因为多市场 tick 级） |

年分区的取舍本质：**和因子的计算粒度对齐**——`compute()` 一次算全年截面（pandas 向量化），append 时只动当年文件，文件数量 5-10 个刚好。

### 61.6 版本化设计

**版本语义**：因子用**语义化版本** `1.0` / `2.0`（目录名 `v1.0` / `v2.0`），不用公司式 `random_string_timestamp`（page 223886928）。

| 对比 | 语义化版本（选定） | random_string_timestamp |
|---|---|---|
| 可读性 | `v1.0` 一眼知道是第几版 | 时间戳只能比大小 |
| 与代码对应 | version 是 Factor 类属性，git 可追溯 | 版本号与代码无对应 |
| 目录名 | `v1.0/` 简短 | 长串路径 |
| 适用场景 | 因子版本少（个位数）、逻辑变更离散 | 数据生产流水线自动打版本（公司场景） |

**升版触发规则**：

| 变更 | 是否升版 | 处理方式 |
|---|---|---|
| `compute()` 逻辑变更 | ✓ 升版（1.0→2.0） | 写新目录 `v2.0/`，全量重算 |
| `depends_on()` 变更 | ✓ 升版 | 同上 |
| 计算参数变更（如动量窗口 20→60） | ✓ 升版（也可以注册成新因子） | 新目录 |
| 新数据 append（新交易日） | ✗ 不升 | 当年文件追加 |
| 上游数据修正 | ✗ 不升 | full 重算覆盖当前版本目录 |
| metadata 修改（description） | ✗ 不升 | 直接改 json |

**多版本共存的三个场景**：

```text
1. 对比 IC: v1.0 与 v2.0 并排跑 alphalens,看新版是否真改进
2. 回测复现: 半年前的回测配置写死 v1.0,重跑结果不被 v2.0 污染
3. 灰度切换: v2.0 试跑 OK 后,改配置指向 v2.0,旧策略仍锁 v1.0
```

**历史值重算 = 版本变更 → 全量重算写新版本目录**：

```text
原则: 一个版本目录内的历史值定义一致——不允许"2020 年是 v1.0 算法、
      2024 年是 v2.0 算法"混在同一个目录

做法: 升版 = 新建 v2.0/ 目录 → 全历史(2020-2024)重算写入 → 旧 v1.0/ 保留
```

**不自动删旧版本**——全因子库旧版总占用 ~5GB 量级（zstd 压缩后），可忽略；手动清理即可（确认无回测引用后删目录）。

### 61.7 增量 vs 全量写入

**日常 append 工作流**（对标 DPP 增量生产 page 288529199）：

```python
from pathlib import Path
import pandas as pd


def save_factor_append(
    factor,
    result: pd.DataFrame,
    base_dir: str = "data/factors",
) -> None:
    """
    日常增量写入: 按年拆文件,当年文件 append。

    对标公司 DPP 增量生产(page 288529199):
    "修改 end-date,只拉起增量日期"——只碰新日期对应的文件。
    """
    out_dir = Path(base_dir) / factor.name / f"v{factor.version}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # date 是 YYYYMMDD 字符串,前 4 位即年份
    result = result.copy()
    result["_year"] = result["date"].astype(str).str[:4]

    for year, chunk in result.groupby("_year"):
        path = out_dir / f"{year}.parquet"
        chunk = chunk.drop(columns="_year")
        if path.exists():
            existing = pd.read_parquet(path)
            chunk = pd.concat([existing, chunk], ignore_index=True)
        chunk = (
            chunk.drop_duplicates(["date", "ticker"])
            .sort_values(["date", "ticker"])
            .reset_index(drop=True)
        )
        chunk.to_parquet(path, compression="zstd", compression_level=3, index=False)

    write_metadata(out_dir, factor, result)  # 更新 metadata.json
```

**全量写入**（version 变 / force / 依赖修正）：

```python
def save_factor_full(factor, result, base_dir="data/factors"):
    """全量重算: 覆盖当前版本目录(或写新版本目录)。"""
    out_dir = Path(base_dir) / factor.name / f"v{factor.version}"
    out_dir.mkdir(parents=True, exist_ok=True)
    result = result.copy()
    result["_year"] = result["date"].astype(str).str[:4]
    for year, chunk in result.groupby("_year"):
        chunk = chunk.drop(columns="_year").sort_values(["date", "ticker"])
        chunk.to_parquet(out_dir / f"{year}.parquet",
                         compression="zstd", compression_level=3, index=False)
    write_metadata(out_dir, factor, result)
```

**写入模式触发条件表**：

| 场景 | 模式 | 动作 |
|---|---|---|
| version 变更（逻辑/参数/依赖改） | full → **新版本目录** | 全量重算写 `v2.0/` |
| `force=True`（手动重算） | full → **覆盖** | 全量重算覆盖当前版本 |
| depends_hash 变（上游修正） | full → **覆盖** | 全量重算覆盖当前版本（保守：不做增量混写） |
| 日常新增交易日 | **append** | 当年文件追加 |

**月度/季度一致性校验**（防增量 bug 累积）：

```text
每月跑一次: 用 save_factor_full 全量重算 → 与 append 维护的目录 diff
预期: 行数一致、值一致(浮点容差 1e-12)
不一致: 说明 append 逻辑有 bug(如去重/排序问题),以全量结果为准覆盖
```

### 61.8 存储位置与查询（Parquet vs ClickHouse）

**决策：Parquet 文件（`data/factors/`），不用 ClickHouse**：

| 维度 | Parquet + DuckDB（选定） | ClickHouse |
|---|---|---|
| 部署 | 零部署——就是文件 | 需起服务、配集群 |
| 依赖 | §4 既定（只用 Parquet + DuckDB） | 额外运维负担 |
| 写入 | `to_parquet()` 一行 | INSERT / 客户端协议 |
| 查询 | DuckDB `read_parquet` | SQL |
| 列裁剪 | Parquet 原生（每因子一文件更彻底） | CK 原生 |
| 并发 | 单机单用户足够 | 实盘多策略并发查询 |
| 备份/迁移 | 拷贝目录 | dump/restore |
| 公司对应物 | — | Alpha 入库 CK（page 288524848）是**实盘信号消费**的需求；个人消费方=本地回测/研究脚本 |

公司用 ClickHouse 是因为因子要供**实盘信号引擎多策略并发低延迟查询**；个人平台消费方是本地回测脚本（单机、离线、批量），DuckDB 读 Parquet 完全够用——而且更符合 §4 的零运维原则。

**DuckDB 查询示例**：

```python
import duckdb

con = duckdb.connect()

# 单因子单年 + 条件过滤
df = con.execute("""
    SELECT date, ticker, momentum_20d
    FROM read_parquet('data/factors/momentum_20d/v1.0/2024.parquet')
    WHERE ticker = '000001.SZ'
      AND date BETWEEN '20240101' AND '20240630'
""").df()

# 多因子 JOIN(等价 load_factor_matrix)
df = con.execute("""
    SELECT a.date, a.ticker, a.momentum_20d, b.volatility_20d
    FROM read_parquet('data/factors/momentum_20d/v1.0/2024.parquet') a
    JOIN read_parquet('data/factors/volatility_20d/v1.0/2024.parquet') b
      ON a.date = b.date AND a.ticker = b.ticker
""").df()

# 跨年 glob
df = con.execute("""
    SELECT * FROM read_parquet('data/factors/momentum_20d/v1.0/*.parquet')
    ORDER BY date, ticker
""").df()
```

**存储三目录划分**（与 §5 数据湖、§17/§18 回测产物对齐）：

```text
data/
├── raw/        # 数据湖: market_data 等原始/清洗后行情(§5)
├── factors/    # 因子值(本节): {name}/v{ver}/{year}.parquet
└── backtest/   # 回测产物: 绩效、持仓、配置快照
```

**宽表等价性——公司 Unpacked vs 个人每因子一表**：

```text
公司 feature_data DynamicData Unpacked:
  物理: field_momentum_20d.ucube / field_volatility_20d.ucube / ... (每字段独立)
  逻辑: GetViewer 查询时按 (di, ticker) 联合成宽表呈现

个人平台 Direction A:
  物理: momentum_20d/v1.0/2024.parquet / volatility_20d/v1.0/2024.parquet / ...
  逻辑: load_factor_matrix() / DuckDB JOIN 合并为宽表

本质相同: 每因子独立物理存储 + 消费时逻辑合并
差异: 公司把"宽表抽象"放在 Udata Schema 层(GetViewer);个人放在消费端函数
      ——个人平台没有 Schema 服务,目录隔离本身就是命名空间,更简单直接
```

### 61.9 决策汇总与修订

**决策汇总**：

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | 存储模型 | **Direction A：每因子一表**（独立目录） | DynamicData Unpacked（page 205515828） |
| 2 | 不用长表 EAV（B） | 行数爆炸 6.25 亿行；列裁剪失效 | — |
| 3 | 不用物理宽表（C） | 新增因子重写文件；版本化困难 | 公司也是 Unpacked 物理布局 |
| 4 | 不用三维张量（D） | HDF5/zarr 违反 §4 | — |
| 5 | 时间分区 | **按年**（~125 万行/文件，5-10 个文件） | 与因子计算粒度对齐 |
| 6 | 不按天/月/资产分区 | 2500 小文件 / 1MB 太小 / 只有 A 股 | 公司按 di 分是多市场 tick 需求 |
| 7 | 数值精度 | **DOUBLE** | float→double 事故（page 263692149） |
| 8 | date/ticker 格式 | VARCHAR `YYYYMMDD` / `000001.SZ` | §5.2 既定 |
| 9 | 版本化 | **语义化版本** 1.0/2.0（不用 random_string_timestamp） | 对标 page 223886928（公司场景不同） |
| 10 | 升版触发 | compute/depends_on/参数 变更才升；数据更新不升 | 一个版本目录内定义一致 |
| 11 | 历史值重算 | 版本变更 → 全量重算写新版本目录 | 保证全历史一致 |
| 12 | 多版本共存 | 保留旧版不自动删（~5GB 可忽略） | 对比 IC / 回测复现 / 灰度切换 |
| 13 | 增量写入 | 日常 append 当年文件；触发式 full | DPP 增量生产（page 288529199） |
| 14 | 存储位置 | **Parquet 文件，不用 ClickHouse** | 公司 CK 是实盘需求（page 288524848） |
| 15 | 查询引擎 | DuckDB `read_parquet` + glob；联合查询 `load_factor_matrix()` | 对标 GetViewer 宽表抽象 |
| 16 | 元数据 | `metadata.json`（身份+血缘+覆盖范围） | 对标 SchemaBase（page 199033106）简化 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§6.4** | **升级为全解 → §27.4 / §27.8 / §61**（表 schema 从"仍开放"变为已定） |
| **§58 缓存目录** | 细化落地：`{range}.parquet` 草图 → **`{year}.parquet` 按年文件**；metadata.json 结构按 §61.5 定稿（含 depends_hash/date_range/row_count） |
| **§57 Factor 元数据** | 类属性（name/version/...）与 metadata.json 的对应关系定稿——代码即元数据，json 是落盘镜像 |
| **§60 存原始值** | Parquet schema 中因子列=原始值再次确认（NaN 保留） |
| **§59 MLFactor** | ML 因子增加 `model/model.joblib` 与因子目录并列（模型版本随因子 version 走） |
| **§27.4 / §27.8** | 路径 `data_platform/factors/{name}/` 与本节 `data/factors/{name}/` 对齐为同一约定（实施时统一为 `data/factors/`） |
| **§5.2 精度约定** | DOUBLE 贯彻到因子表（此前只定行情表） |

**本节仍未解决**：`load_factor()` 单因子读取函数的入参签名（start/end 与年文件的裁剪逻辑——预计 <15 行）；`write_metadata()` 的实现细节（~15 行，字段已在 §61.5 定死）；旧版本目录的手动清理规则（谁判断"无回测引用"——回测配置快照里记录 version 的话可以自动判断）；行情数据（bars）是否也用 metadata.json（本节只覆盖因子表）。

---

## 62. 因子的管理、去重与调试：治理层全解（§6.5–§6.7 三问合并）

> §6.5（管理与治理）+ §6.6（去重与合并）+ §6.7（调试与排查）合并回答——三问共享一套 Confluence 参考（量化学习教程 §8.9 入库标准 / §18.3 衰减监控 / Udata 生产状态）且互相交织（去重是治理的一部分、调试输出接入治理指标）。公司无统一因子注册表（散在 usignal XML + UFormula + C++ EB，元数据在 Schema page 199033106），个人平台 = **代码即注册表**（FactorRegistry + metadata.json）。去重 = **Spearman 相关 > 0.7 标冗余，保留 IC 高者**；容量目标 **20-50 个 production 因子**。与 §57（Factor 接口）、§58（调度）、§60（后处理）、§61（存储）衔接。**新增代码 ≈180 行**。

### 62.1 Context 与共享 Confluence 参考

§57–§61 完成了因子的技术层（接口/调度/后处理/存储）。本节覆盖治理层：因子入库/退出标准、生命周期、血缘、命名、文档、去重合并、调试排查。公司对应物分散在多处——UFormula 的 tag 短标识、usignal XML 的因子定义、C++ EventBrick 的因子实现、Datamall 的 dw_asset_job，元数据由 SchemaBase（page 199033106）统一描述。个人平台因子数 <100，不需要独立数据库。

**Confluence 参考**（三问共享）：

```text
- 量化学习教程 §8.9 因子入库标准 (page 301832392): 8 条判据——
  [1] Rank IC > 0.04  [2] IR > 0.5  [3] t > 2（保守 t > 3）
  [4] 分组单调  [5] 与现有因子相关 < 0.5  [6] 时间/市场稳健
  [7] 理论可解释  [8] 记录检验次数 N,按 N 提高 t 门槛 (t = √(2·ln N))
  入库线 IC>0.04/IR>0.5 vs 退出线 IC 跌破 0.03/IR 跌破 0.3
  ——"不要拿退出线当入库线用"
- 量化学习教程 §7.6 因子衰减: 典型生命周期 3-5 年
- 量化学习教程 §18.3 三种衰减模式（缓慢线性/阶梯式/反向 anti-alpha）
  + 四监控指标: (1) rolling 6 月 IC vs 历史均值 (2) 实盘夏普 vs 历史夏普
  (3) 累计超额是否破区间 (4) rolling 36 月 t
  退出判据: rolling 6 月夏普 < 0 或 rolling 36 月 t < 1.5
- 量化学习教程 §7.8 僵尸因子: "不做相关性检查,加了 5 个因子其实相关
  0.9 = 1 个因子"; "3-5 个因子就够"
- 量化学习教程 §10.6 换手率 = |Δw| 之和,月频合理 30-100%
- Udata 生产状态 (page 223886928): 5 色灯 + per-di +
  version=random_string_timestamp
- Udata 依赖血缘 (page 223894024): DependencyLineage 记录上游
  udata id/version per di
- SchemaBase (page 199033106): 公司元数据骨架——datafile_path +
  TypeInfo + ProductionStatus + DependencyLineage
```

### 62.2 因子注册表（§6.5.1）：代码即注册表

**公司做法**：无统一因子注册表——因子定义散在 usignal XML + UFormula 公式 + C++ EventBrick，元数据在 Schema page 199033106 集中描述；Datamall 有 dw_asset_job 记录数据资产作业。

**个人平台决策**：不建独立数据库/JSON 注册表。**FactorRegistry 类 + metadata.json**，代码即注册表。

| 层 | 载体 | 内容 |
|---|---|---|
| 注册 | FactorRegistry + `@register_factor` 装饰器 | 因子存在性与工厂创建 |
| 元数据 | Factor 类属性（自动提取） | factor_id / version / description / category / frequency / asset_types / depends_on / status / created_at |
| 质量指标 | metadata.json（factor_eval() 后写入） | ic_mean / ic_ir / ic_t / turnover / coverage |

```python
# 从 Factor 类自动提取元数据（不建独立注册表）
META_FIELDS = [
    "name", "version", "description", "category", "frequency",
    "asset_types", "depends_on", "status", "created_at",
]

def list_factor_meta() -> list[dict]:
    """遍历 FactorRegistry 汇总所有因子元数据（因子 <100，遍历足够）。"""
    result = []
    for name in FactorRegistry.list_all():
        cls = FactorRegistry.get_class(name)
        result.append({f: getattr(cls, f, None) for f in META_FIELDS})
    return result
```

status 三态：`draft → production → deprecated`（不引入 5 色灯；YELLOW/AMBER 是数据质量传导，个人用 depends_hash 触发重算）。

**为什么不用独立注册表**：因子 <100，FactorRegistry.list_all() 遍历汇总即可。独立 JSON/SQLite 注册表引入"代码与注册表不一致"的同步问题——代码即注册表消除此问题。

### 62.3 质量指标（§6.5.2）：入库与退出的量化标准

| 指标 | 定义 | 入库线 | 退出线 | 教程参考 |
|---|---|---|---|---|
| Rank IC | 因子值 vs 下期收益的截面 Spearman 相关均值 | > 0.04 | < 0.03 | §8.9[1] |
| IC IR | IC 均值 / IC 标准差 | > 0.5 | < 0.3 | §8.9[2] |
| t 统计量 | IR × √n（n = 观测期数） | > 2（保守 > 3） | rolling 36 月 t < 1.5 | §8.9[3] |
| 分组单调性 | 分 5 组收益单调递增 | 单调 | 失去单调 | §8.9[4] |
| 因子换手率 | 因子值的月度变化率 | 动量高/价值低均合理 | — | §10.6 |
| 衰减半衰期 | IC 衰减到初始值一半的时间 | > 1 年 | — | §7.6 |
| 覆盖率 | 有效值占比 | > 80% | — | — |
| 与已有因子相关 | Spearman 相关绝对值 | < 0.5 | > 0.7 标冗余 | §8.9[5] |

```python
def evaluate_factor(
    factor_values: pd.DataFrame,   # [date, ticker, factor_name]
    returns: pd.DataFrame,         # [date, ticker, fwd_return]
    factor_name: str,
) -> dict:
    """
    单因子质量评估。对标教程 §8.9 八条判据的量化实现。
    """
    merged = factor_values.merge(returns, on=["date", "ticker"])
    # 截面 Spearman IC 序列
    ic_series = merged.groupby("date").apply(
        lambda g: g[factor_name].corr(g["fwd_return"], method="spearman")
    )
    n_periods = len(ic_series)
    ic_mean = ic_series.mean()
    ic_std = ic_series.std()
    ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
    ic_t = ic_ir * np.sqrt(n_periods)  # §8.9[3]: t = IR × √n
    coverage = factor_values[factor_name].notna().mean()
    return {
        "ic_mean": round(ic_mean, 4),
        "ic_ir": round(ic_ir, 3),
        "ic_t": round(ic_t, 2),
        "n_periods": n_periods,
        "coverage": round(coverage, 3),
    }
```

**入库线 vs 退出线**：教程 §8.9 明确警告"不要拿退出线当入库线用"——入库 IC>0.04/IR>0.5，退出 IC<0.03/IR<0.3。中间是灰色地带（继续观察）。

**多重检验校正**（§8.9[8]）：记录检验次数 N，按 N 提高 t 门槛——`t = √(2·ln N)`。试了 100 个因子才找到 1 个 t=2.5 的，这个 t=2.5 不可信。与 §27.9 attempt_log 对齐。

### 62.4 生命周期状态机（§6.5.3）

```text
draft →(入库检验通过)→ production →(衰减检测触发)→ deprecated
                          ↓
                    rolling 监控 → warning（提前重训/降权）→ deprecated
```

| 状态 | 含义 | 入口条件 | 出口条件 |
|---|---|---|---|
| draft | 开发中，不参与组合 | 创建时默认 | 入库检验 8 条全过 |
| production | 生产使用 | IC>0.04 + IR>0.5 + t>2 + 可解释 | 衰减退出判据触发 |
| deprecated | 退役，停止使用 | 衰减或被更好因子替代 | 不物理删除（保留代码+历史数据） |

```python
def check_factor_health(ic_history: pd.Series) -> str:
    """
    因子健康检查。对标教程 §18.3 四监控指标中的 rolling IC 指标。
    返回: "healthy" | "warning" | "exit"
    """
    mean_ic = ic_history.mean()
    recent_6m = ic_history.tail(126).mean()   # ~6 月交易日
    recent_12m = ic_history.tail(252).mean()  # ~12 月

    if recent_6m > 0.7 * mean_ic:
        return "healthy"
    if recent_6m > 0.5 * mean_ic:
        return "warning"   # 触发提前重训 / 降低 ensemble 权重
    if recent_12m < 0.3 * mean_ic:
        return "exit"      # 教程 §18.4: rolling 36 月 t < 1.5
    return "warning"
```

三状态表（不引入公司 5 色灯）：公司 ProductionStatus 5 色灯（page 223886928）是数据质量传导机制（BLACK→下游 AMBER），个人平台用 **depends_hash 变化触发重算**替代——上游数据修正 → depends_hash 变 → 下游因子自动 full 重算。

### 62.5 血缘（§6.5.4）：depends_on + FactorDAG + depends_hash

**公司做法**：DependencyLineage（page 223894024）per-di 记录上游 udata id/version——细到每个交易日的每个上游版本。

**个人平台**：三件套已有，不引入独立血缘存储：

| 机制 | 覆盖 | 来源 |
|---|---|---|
| `depends_on()` | 依赖了谁 | §57 Factor 类方法 |
| FactorDAG | 依赖图 + 拓扑排序 | §58 |
| depends_hash | 上游数据指纹（何时算的） | §58 缓存 key |

```text
示例:
  momentum_20d:     depends_on ["bars_daily"]                       hash "a1b2c3"
  vol_adj_momentum: depends_on ["factor:momentum_20d",
                                "factor:volatility_20d"]            hash "d4e5f6"
```

血缘图从 FactorDAG 自动生成（`dag.visualize()`），不需要独立血缘存储。per-di 粒度的血缘（公司做法）对个人平台过度——全量重算 <5 秒，血缘用于"知道依赖了谁"即可，不需要"知道哪一天依赖了哪个版本"。

### 62.6 命名规范（§6.5.5）与文档（§6.5.6）

**命名**：`{类型}_{核心语义}_{参数}` snake_case。

| 类型前缀 | 示例 | 说明 |
|---|---|---|
| 时序动量类 | momentum_20d / momentum_60d | 窗口参数放后缀 |
| 波动类 | volatility_20d / volatility_60d | 同上 |
| 估值类 | bp_ratio / ep_ratio | 语义缩写可接受 |
| 规模类 | size_rank / log_market_cap | |
| 流动性类 | turnover_20d / amihud_illiq | |
| 中性化类 | industry_neutral_mom | 前缀标明处理方式 |
| ML 类 | ml_stock_selector | ml_ 前缀 |

**禁止**：`mom20` / `Momentum20` / `factor_001` / 中文标识符。对比公司 UFormula `tag="ofi"` 短标识——公司场景是公式热路径（短 tag 省解析），个人用全称可读性优先。

**文档**：不建独立文档文件——docstring + description 属性即可（<100 因子，compute() 是权威定义；metadata.json 存运行时信息；研究笔记写研究日志）。

### 62.7 相似因子检测（§6.6.1）与冗余处理（§6.6.2）

```python
def detect_redundant_factors(
    factor_matrix: pd.DataFrame,  # [date, ticker, f1, f2, ...]
    threshold: float = 0.7,
) -> list[tuple[str, str, float]]:
    """
    检测冗余因子对。按日截面 Spearman 相关取均值。
    """
    factor_cols = [c for c in factor_matrix.columns if c not in ("date", "ticker")]
    pairs = []
    for i, f1 in enumerate(factor_cols):
        for f2 in factor_cols[i + 1:]:
            daily_corr = factor_matrix.groupby("date").apply(
                lambda g: g[f1].corr(g[f2], method="spearman")
            )
            avg_corr = daily_corr.mean()
            if abs(avg_corr) > threshold:
                pairs.append((f1, f2, round(avg_corr, 3)))
    return sorted(pairs, key=lambda x: -abs(x[2]))
```

**检测层级 4 档**：

| 相关区间 | 判定 | 处理 |
|---|---|---|
| > 0.9 | 高度冗余 | 留 IC 高者，另一个 deprecated |
| 0.7–0.9 | 可能冗余 | 经济逻辑不同则保留（如动量 vs 反转可能相关但逻辑不同） |
| 0.5–0.7 | 警告 | 记录，入库时已有 §8.9[5] < 0.5 约束 |
| < 0.5 | 独立 | 正常保留 |

**冗余处理三原则（保留 > 合并 > 废弃）**：

| 情形 | 处理 | 理由 |
|---|---|---|
| 0.5–0.7 且逻辑不同 | 保留两个 | 教程 §8.9[5] 入库线 < 0.5 是单因子入库标准，0.5-0.7 的组合内共存可接受 |
| > 0.9 | 保留最优（IC 高且稳的） | "5 个相关 0.9 的因子 = 1 个因子"（§7.8） |
| 合并 | **不做** | 等权平均引入新自由度；合成在策略层 §7.3 等权 z-score |
| 废弃 | status→deprecated | 保留代码和历史数据，不物理删除 |

### 62.8 容量控制（§6.6.3）

**目标：20-50 个 production 因子**。

| 类别 | 数量预算 | 说明 |
|---|---|---|
| 经典风险因子（动量/波动/价值/质量/规模） | 5–8 | Barra 风格因子集的个人版 |
| 行业/风格 | 3–5 | 申万一级轮动、风格暴露 |
| 另类（情绪/资金流/事件） | 3–5 | 数据可得性决定 |
| ML/组合因子 | 2–3 | MLFactor 输出 |
| **production 合计** | **15–20** | + 若干 draft/deprecated |

"3-5 个就够"（教程 §7.8）是**策略层面**——单策略用 3-5 个因子合成。**因子库层面** 20-50 个供不同策略/alphaType 选用。>50 个 production 说明没做好去重。

### 62.9 异常定位（§6.7.1）与单测（§6.7.2）

**五分支排查流程**：

| 症状 | 定位方向 | 检查点 |
|---|---|---|
| 全部标的异常 | 数据问题 | load_bars 行数 / NaN 比例 / 日期连续性 |
| 特定标的异常 | 停牌/ST/退市 | 查该标的上市状态 |
| 特定日期异常 | 数据源问题 | 查 depends_hash 是否变化 |
| 数值范围异常 | 除零/溢出/NaN 传播 | 手动对拍单日截面 |
| IC 突变 | 参数问题或衰减 | §18.3 四指标 |

```python
def diagnose_factor(df: pd.DataFrame, factor_name: str) -> dict:
    """
    因子异常诊断。五分支排查的第一步——快速定位异常范围。
    """
    s = df[factor_name]
    by_date_nan = df.groupby("date")[factor_name].apply(lambda x: x.isna().mean())
    return {
        "total_rows": len(df),
        "nan_ratio": round(s.isna().mean(), 4),
        "inf_count": int(np.isinf(s.fillna(0)).sum()),
        "zero_ratio": round((s == 0).mean(), 4),
        "coverage_mean": round(1 - by_date_nan.mean(), 3),
        "coverage_min": round(1 - by_date_nan.max(), 3),
        "min_date": by_date_nan.idxmax(),  # NaN 率最高的日期
        "value_min": float(s.min()),
        "value_max": float(s.max()),
        "value_mean": float(s.mean()),
        "value_std": float(s.std()),
        "cs_std_cv": round(  # 截面标准差的变异系数——异常截面检测
            df.groupby("date")[factor_name].std().std()
            / df.groupby("date")[factor_name].std().mean(),
            3,
        ),
    }
```

**单测四范式**（TestMomentum20D 为例）：

| 测试 | 断言 | 对标 |
|---|---|---|
| test_basic_computation | 精确对拍（手算 3 天窗口）+ schema 断言 | 公司 CI page 256109564 |
| test_nan_handling | 停牌 NaN 不致整列 NaN；`notna().mean() > 0.8` | NaN 鲁棒性 |
| test_output_schema | `columns == ["date", "ticker", "momentum_20d"]` | Schema 验证 |
| **test_no_lookahead** | 删最后一天后历史值不变（`np.allclose(equal_nan=True)`） | 教程 §3 三大坑 |

```python
class TestMomentum20D:
    def test_basic_computation(self):
        """精确对拍：手算验证 compute() 输出。"""
        data = make_bars(dates=["20240102", "20240103", "20240104"], close=[10, 11, 12])
        f = Momentum20D()
        result = f.compute(data)
        assert list(result.columns) == ["date", "ticker", "momentum_20d"]

    def test_no_lookahead(self):
        """前视偏差测试：删除最后一天，历史值必须不变。"""
        data = make_bars_60days()
        f = Momentum20D()
        full = f.compute(data)
        truncated = f.compute(data.iloc[:-1])  # 删最后一天
        merged = full.merge(truncated, on=["date", "ticker"],
                            suffixes=("_full", "_trunc"))
        assert np.allclose(
            merged["momentum_20d_full"], merged["momentum_20d_trunc"],
            equal_nan=True,
        )
```

单测范式 5 行表：精确对拍（公司 CI page 256109564）/ Schema 验证 / NaN 鲁棒性 / 前视偏差（教程 §3 三大坑）/ 幂等性（同输入两次输出一致）。

### 62.10 可视化（§6.7.3）：四张标准图

```python
def plot_factor_report(
    factor_values: pd.DataFrame,
    returns: pd.DataFrame,
    factor_name: str,
    n_groups: int = 5,
) -> None:
    """
    因子报告四张图（2×2 figsize(14,10)）。
    落盘到 research/reports/（§27.8 tearsheet 位置）。
    """
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))

    # [1] IC 时序柱状 + 均值红线（§8.3）
    ic_series = compute_ic_series(factor_values, returns, factor_name)
    axes[0, 0].bar(ic_series.index, ic_series.values, width=1.0)
    axes[0, 0].axhline(ic_series.mean(), color="red", ls="--")
    axes[0, 0].set_title(f"IC Series (mean={ic_series.mean():.4f})")

    # [2] 累计 IC（§18.3）
    axes[0, 1].plot(ic_series.cumsum())
    axes[0, 1].set_title("Cumulative IC")

    # [3] 截面分布直方（§8.7）
    axes[1, 0].hist(factor_values[factor_name].dropna(), bins=50)
    axes[1, 0].set_title("Cross-Section Distribution")

    # [4] 分组累计收益 n_groups=5（§8.9[4]）
    plot_group_returns(axes[1, 1], factor_values, returns, factor_name, n_groups)
    axes[1, 1].set_title(f"Quantile Returns (n={n_groups})")

    plt.tight_layout()
    plt.savefig(f"research/reports/{factor_name}_tearsheet.png", dpi=150)
```

四张图对应教程四判据：[1] IC 水平（§8.9[1]）、[2] IC 稳定性/衰减（§18.3）、[3] 分布合理性（§8.7 去极值验证）、[4] 分组单调性（§8.9[4]）。

### 62.11 决策汇总（三问合并，14 行）

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | 注册表 | **代码即注册表**——FactorRegistry + metadata.json，不建独立库 | 公司散在 usignal XML + UFormula + EB（Schema page 199033106） |
| 2 | 质量指标 | IC / IR / t / 分组单调 / 相关 / 覆盖率 | 教程 §8.9 八条 |
| 3 | 入库标准 | IC>0.04, IR>0.5, t>2, 相关<0.5, 可解释 | §8.9 |
| 4 | 退出标准 | IC<0.03 或 rolling 36 月 t<1.5 | §18.4 |
| 5 | 生命周期 | 三状态 draft→production→deprecated（ProductionStatus 简化） | page 223886928 |
| 6 | 血缘 | depends_on + FactorDAG + depends_hash | page 223894024 |
| 7 | 命名 | snake_case + 语义前缀 + 参数后缀 | UFormula tag 短标识不采用 |
| 8 | 文档 | description 属性 + docstring | 不建独立文档文件 |
| 9 | 去重检测 | Spearman > 0.7 标冗余 | §7.8 僵尸因子 |
| 10 | 冗余处理 | 保留 IC 高者，**不做合并** | 等权平均引入新自由度 |
| 11 | 容量控制 | **20-50 个 production 因子** | "3-5 个就够"是策略层面 |
| 12 | 异常排查 | 全局→标的→日期→数值→IC 五分支 | — |
| 13 | 单测 | 精确对拍 + Schema + NaN 鲁棒 + 前视 | page 256109564 |
| 14 | 可视化 | 四图：IC 时序 / 累计 IC / 分布 / 分组收益 | §8.3 + §18.3 + §8.7 + §8.9[4] |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§6.5** | **升级为全解 → §27.9 / §27.11 / §62**（注册表 schema / 质量指标 / 生命周期 / 血缘 / 命名 / 文档全部落定） |
| **§6.6** | **升级为全解 → §16.5 / §62**（检测算法 + 冗余处理 + 容量控制落定） |
| **§6.7** | **已定 → §62**（五分支排查 + 单测四范式 + 四图报告） |
| **§27.9 attempt_log** | 与 §62.3 多重检验校正对齐（t = √(2·ln N)） |
| **§27.11 status 四态** | 与 §62.4 三状态对齐——rejected 并入 deprecated（原因记在 metadata） |
| **factor_eval()** | 输出写入 metadata.json 的 eval 块（§61.5 已定结构） |
| **research/reports/** | plot_factor_report() 落盘位置与 §27.8 tearsheet 对齐 |

**本节仍未解决**：detect_redundant_factors 的 factor_matrix 从哪来（load_factor_matrix() 拼宽表后截面去 NaN？）；check_factor_health 的 ic_history 存储位置（factor_eval/{name}/ 序列，§61 已定）；diagnose_factor 的 cs_std_cv 阈值（多少算异常截面）；plot_factor_report 的 n_groups 参数默认值（5 与 §8.9[4] 一致）。

---

## 63. 多因子组合：从因子到 Alpha 信号（§7 全解）

> §7.1（组合方法）+ §7.2（正交化）+ §7.3（中性化）+ §7.4（合成与信号）+ §7.5（组合评估）五问合并回答。§16.2–§16.6 已给研究层原则（等权 baseline / ICIR 加权 / 不做正交化 / 中性化在预处理 / score→持仓），本节给**实现级收口**：FactorCombiner 类、权重更新窗口、AlphaSignal 类、评估指标表与 eval_report 落盘。核心结论不变——**主路径等权 z-score，IC_IR 加权可选；不做 Barra；ML 合成归 MLFactor（§59）**。与 §60（后处理）、§62（治理指标）、§65（alphaType signal_source）衔接。**新增代码 ≈130 行**。

### 63.1 Context 与 Confluence 参考

§57–§62 完成了单因子的全生命周期（接口/调度/后处理/存储/治理）。本节回答：**多个通过入库检验的因子怎么合成一个可下单的 Alpha 信号**。公司对应物是 usignal XML 的信号合成配置 + Alpha 入库（page 288524848）+ QuantLib 组合优化；个人平台因子数 3-5 个（单策略），合成层必须简单。

**Confluence 参考**：

```text
- 量化学习教程 §7.4 多因子合成 (page 301832392): 等权 / IC 加权 / ICIR 加权
  / 回归最大 ICIR / ML 合成 五法; "永远先跑等权作为 baseline,
  任何复杂方法跑不赢等权就是过拟合"
- 量化学习教程 §7.3 截面 z-score: 合成前每个因子逐截面标准化
- 量化学习教程 §7.7 散户建议: ETF 池不做中性化; "3-5 个因子就够"
- 量化学习教程 §16.3 组合评估: 组合 IC / IR / 年化 / 夏普 / MDD / Calmar
- 量化学习教程 §16.4 回测实盘 gap: 夏普 > 2 怀疑过拟合
- Alpha 入库 (page 288524848): 公司合成后的 Alpha 信号入库 ClickHouse
  供实盘多策略消费——个人平台落 data/signals/{name}/
- 公司组合优化: QuantLib mean-variance + Barra 风险模型——个人不做
```

### 63.2 组合方法选型（§7.1）：主路径等权 + IC_IR 加权可选

**五法对比与决策**：

| 方法 | 公式 | 优点 | 缺点 | 决策 |
|---|---|---|---|---|
| **等权 z-score** | `score = Σ z_i` | 零参数、不过拟合、稳健 | 好坏因子一视同仁 | **主路径默认** |
| IC 加权 | `w_i = mean(IC_i)` | 强因子多说话 | IC 噪声大 | 不做（ICIR 更稳） |
| **ICIR 加权** | `w_i = IC_i / std(IC_i)` | 惩罚不稳定因子 | 需要 IC 历史窗口 | **可选备选** |
| 回归 max ICIR | `w* ∝ Σ⁻¹ · IC` | 理论最优去相关 | Σ⁻¹ 不稳定、样本外常不如等权 | **不做**（§16.3 方法 3 教训） |
| ML 合成 | `score = f(z_1..z_n)` | 非线性交互 | 过拟合风险最高 | **归 MLFactor**（§59），不在组合层做 |

```python
class FactorCombiner:
    """
    多因子合成器。对标量化教程 §7.4 五法中的等权/IC 加权/ICIR 加权三法。

    主路径: equal_weight(默认) —— 任何复杂方法必须跑赢它才升级。
    """

    def __init__(
        self,
        method: str = "equal_weight",   # equal_weight | ic_weight | ic_ir_weight
        lookback: int = 60,             # IC 权重窗口(交易日)
    ):
        assert method in ("equal_weight", "ic_weight", "ic_ir_weight")
        self.method = method
        self.lookback = lookback

    def combine(
        self,
        factor_scores: pd.DataFrame,  # [date, ticker, f1, f2, ...] 已后处理
        ic_history: pd.DataFrame = None,  # [date, factor_name, ic]
    ) -> pd.DataFrame:
        """
        合成 → DataFrame[date, ticker, combined_score]。
        factor_scores 已过 PostProcessor(§60): 去极值→填充→z-score。
        """
        factor_cols = [c for c in factor_scores.columns if c not in ("date", "ticker")]
        if self.method == "equal_weight":
            combined = factor_scores[factor_cols].mean(axis=1)
        else:
            w = self._ic_weights(ic_history, factor_cols) \
                if self.method == "ic_weight" \
                else self._ic_ir_weights(ic_history, factor_cols)
            combined = sum(factor_scores[c] * w[c] for c in factor_cols)
        return factor_scores[["date", "ticker"]].assign(combined_score=combined)

    def _ic_weights(self, ic_history, factor_cols) -> dict:
        """IC 加权: w_i = mean(IC_i 最近 lookback 期), 归一化为和 1。"""
        recent = ic_history.groupby("factor_name")["ic"].apply(
            lambda s: s.tail(self.lookback).mean()
        )
        w = {c: max(recent.get(c, 0.0), 0.0) for c in factor_cols}  # 负 IC 给 0
        total = sum(w.values()) or 1.0
        return {c: v / total for c, v in w.items()}

    def _ic_ir_weights(self, ic_history, factor_cols) -> dict:
        """ICIR 加权: w_i = mean(IC_i)/std(IC_i) 最近 lookback 期, 归一化。"""
        def _ir(s):
            s = s.tail(self.lookback)
            return s.mean() / s.std() if s.std() > 0 else 0.0
        recent = ic_history.groupby("factor_name")["ic"].apply(_ir)
        w = {c: max(recent.get(c, 0.0), 0.0) for c in factor_cols}
        total = sum(w.values()) or 1.0
        return {c: v / total for c, v in w.items()}
```

**权重更新窗口（关键防前视）**：

| 决策点 | 结论 | 理由 |
|---|---|---|
| 窗口类型 | **扩张窗口**（起点固定，终点滚动） | 因子 <5 个、历史短，滚动窗口 IC 估计噪声大 |
| 更新频率 | **月频**（月末算权重，下月用） | 与再平衡频率对齐；日频更新权重抖动 |
| IC 取值 | **只用 t-1 及之前的 IC** | 用当期 IC 定当期权重 = 前视（教程 §16.3 坑点） |
| 负 IC 处理 | 权重截断为 0 | 负 IC 因子不参与，比负权重稳健 |

**IC 历史来源**：`factor_eval/{name}/ic_series.parquet`（§61/§62 已定），combiner 不自己算 IC。

### 63.3 正交化（§7.2）：不做独立正交化

| 候选 | 结论 | 理由 |
|---|---|---|
| Gram-Schmidt 正交化 | **不做** | 对因子顺序敏感；扭曲经济含义 |
| PCA 主成分合成 | **不做** | 解释性差；载荷不稳定 |
| 回归取残差（对其他因子） | **不做独立步骤** | §60 中性化回归可加其他因子列（csfacneutral 思路），但单策略 3-5 因子不值得 |
| **相关性聚类去重** | **做**（§62.7 已定） | 入库线相关<0.5 + 去重 4 档，冗余在入库时已控 |

**结论**（§16.5 原则重申 + 实现确认）：YAGNI——3-5 个因子的组合，入库时已要求相关 < 0.5，独立正交化层收益 <1% IC 而引入顺序敏感与解释性损失。若未来因子 >10 个再考虑 csfacneutral 式中性化。

### 63.4 中性化（§7.3）：个股池做、ETF 不做、组合层不做 Barra

| 层 | 做什么 | 不做什么 | 依据 |
|---|---|---|---|
| **因子层**（§60.3） | 个股池：行业 + log 市值中性化（OLS 残差） | ETF 池不做（本来就在做行业轮动） | 教程 §7.7 |
| **组合层** | 不做 | Barra 风险模型 / 行业暴露约束 / 风格暴露约束 | 因子 <5 个、单人平台，Barra 是机构多组合风控需求 |
| **风险层**（§65.6） | 集中度/换手检查 | 行业暴露中性化 | 简单约束集 |

```python
def neutralize_factor(
    df: pd.DataFrame,
    factor_col: str,
    industry_col: str = "sw_l1_industry",   # 申万一级 31 行业
    size_col: str = "log_market_cap",
) -> pd.DataFrame:
    """
    因子中性化: 逐日截面 OLS 残差。
    y = β0 + β1·industry_dummies + β2·log_mcap + ε, 取 ε。
    对标公司 UFormula csindneutral + 教程 §26.1。
    """
    result = df.copy()
    for date, idx in df.groupby("date").groups.items():
        g = df.loc[idx]
        y = g[factor_col].values
        valid = ~np.isnan(y)
        if valid.sum() < 31 + 2:  # 行业哑变量 30 + 截距 + 市值,自由度检查
            continue
        ind_dummies = pd.get_dummies(g[industry_col], drop_first=True).values
        X = np.column_stack([np.ones(len(y)), ind_dummies, g[size_col].values])
        beta, _, _, _ = lstsq(X[valid], y[valid], rcond=None)
        result.loc[idx, factor_col] = y - X @ beta
    return result
```

**中性化只用 industry + log_market_cap 两个维度**——不做国家/行业动量/流动性等扩展暴露（教程 §26.1 实证：行业+市值已消掉 80% 伪 alpha；更多暴露维度样本不足时反而过拟合）。

### 63.5 合成与最终信号（§7.4）：AlphaSignal 类

**信号形态决策**：

| 候选 | 结论 | 理由 |
|---|---|---|
| combined_score = 连续 float | **采用** | 保留强度信息，可给下游权重分配用 |
| 截面 z-score 后的分数 | **采用**（合成后再截面 z-score 一次） | 让 score 跨日可比（信号平滑监控用） |
| 二值信号（多/空/空仓） | 不用 | 丢失强度 |
| 分位数分组 | 不直接用 | 分组是 signal_to_weights 的一种方式，不是信号形态 |

```python
class AlphaSignal:
    """
    Alpha 信号。两条路径在此汇聚:
      from_multi_factor(FactorCombiner.combine())  ← §63.2
      from_ml_factor(MLFactor.predict())           ← §59
    对标公司 OnAlphaUpdate 信号接口(page 282827800)。
    """

    def __init__(self, frame: pd.DataFrame, name: str, config_snapshot: dict = None):
        # frame: [date, ticker, score], score 已截面 z-score
        self.frame = frame
        self.name = name
        self.config_snapshot = config_snapshot or {}

    @classmethod
    def from_multi_factor(cls, combined: pd.DataFrame, name: str, **kw):
        """combined: [date, ticker, combined_score] → 截面 z-score → AlphaSignal。"""
        df = combined.copy()
        df["score"] = df.groupby("date")["combined_score"].transform(
            lambda s: (s - s.mean()) / s.std() if s.std() > 0 else 0.0
        )
        return cls(df[["date", "ticker", "score"]], name, **kw)

    @classmethod
    def from_ml_factor(cls, preds: pd.DataFrame, name: str, **kw):
        """ML 路径: 预测值直接当 score(或先 sigmoid→z-score)。"""
        df = preds.copy()
        df["score"] = df.groupby("date")["prediction"].transform(
            lambda s: (s - s.mean()) / s.std() if s.std() > 0 else 0.0
        )
        return cls(df[["date", "ticker", "score"]], name, **kw)

    def to_weights(self, method: str = "top_k", k: int = 20, **kw) -> pd.DataFrame:
        """score → 目标权重(§17 已定四法的精简入口)。"""
        return signal_to_weights(self.frame, method=method, k=k, **kw)


def signal_to_weights(
    signal: pd.DataFrame,   # [date, ticker, score]
    method: str = "top_k",  # top_k | proportional
    k: int = 20,
) -> pd.DataFrame:
    """
    信号 → 目标权重。
    top_k(默认): 选 score 最高的 k 个等权 1/k —— 教程 §16.6"Top N 等权"
    proportional: w_i ∝ max(score_i, 0) 归一化 —— 信号加权
    """
    def _one_day(g):
        if method == "top_k":
            top = g.nlargest(k, "score")
            w = pd.Series(0.0, index=g.index)
            w[top.index] = 1.0 / len(top)
        else:  # proportional
            s = g["score"].clip(lower=0)
            w = s / s.sum() if s.sum() > 0 else s
        return w

    result = signal.copy()
    result["weight"] = result.groupby("date", group_keys=False).apply(_one_day)
    return result[["date", "ticker", "weight"]]
```

**两条路径汇聚的设计意义**：MultiFactor 路径（研究可解释）与 ML 路径（非线性）输出同一种 AlphaSignal，下游 build_portfolio/generate_orders 对信号来源无感知——与 §65 alphaType 的 signal_source{type: "multi_factor"|"ml_factor"|"ensemble"} 三态对应。

### 63.6 组合评估（§7.5）：指标表与 eval_report

**评估指标表**（对标教程 §16.3 + §16.4）：

| 指标 | 定义 | 期望区间 | 警戒线 |
|---|---|---|---|
| 组合 Rank IC | score vs 下期收益截面 Spearman | > 0.04 | < 0.03 |
| 组合 IC IR | mean(IC)/std(IC) | > 0.5 | < 0.3 |
| 年化收益 | (1+total)^(252/n)-1 | 10–15%（A 股合理） | > 20% 查未来函数 |
| 年化夏普 | mean/std × √252 | 0.8–1.2 | > 2.0 怀疑过拟合 |
| 最大回撤 | 累计净值峰谷 | < 20% | > 30% |
| Calmar | 年化/MDD | > 1.0 | < 0.5 |
| 换手率 | Σ\|Δw\|/2 月频 | 30–100% | > 150% 成本吃掉 alpha |
| 因子贡献 | 每因子对组合收益的贡献度 | 最强因子 < 50% | 单因子 > 70% = 伪分散 |

```python
def factor_contribution(
    weights: pd.DataFrame,      # [date, ticker, weight]
    factor_returns: pd.DataFrame,  # [date, factor_name, factor_return]
) -> pd.Series:
    """
    因子贡献拆解: 组合收益按因子暴露分解。
    contribution_i = Σ_t Σ_j w_{j,t} · exposure_{j,i,t} · factor_return_{i,t}
    简化版: 用因子值当暴露,分组收益当因子收益。
    """
    merged = weights.merge(factor_returns, on="date")
    return (merged["weight"] * merged["factor_return"]).groupby(
        merged["factor_name"]
    ).mean()
```

**验证纪律（教程 §16.4 回测实盘 gap）**：

```text
1. 训练/测试时间不重叠(Walk-Forward,禁 KFold——时间序列交叉验证会泄漏)
2. 测试集只看一次(反复看测试集 = 变相训练)
3. 全流程落盘: eval_report.json → data/signals/{name}/
```

```python
# data/signals/{name}/eval_report.json 结构
{
  "alpha_name": "mom_vol_composite",
  "config_snapshot": {"combiner_method": "equal_weight", "factors": [...], ...},
  "period": ["20200101", "20241231"],
  "metrics": {
    "ic_mean": 0.052, "ic_ir": 0.61, "ann_return": 0.128,
    "ann_sharpe": 1.05, "max_drawdown": -0.18, "calmar": 0.71,
    "turnover_monthly": 0.55, "factor_contribution": {...}
  },
  "walk_forward_folds": [...],
  "created_at": "2026-10-04T20:00:00"
}
```

config_snapshot 记录 `path@hash`（§13.2 JSONL 规范），保证回测可复现。

### 63.7 决策汇总（14 行）

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | 组合主路径 | **等权 z-score**（baseline） | 教程 §7.4"跑不赢等权就是过拟合" |
| 2 | 可选加权 | ICIR 加权（lookback=60，月频更新） | §7.4 |
| 3 | IC 加权 | 不做（ICIR 更稳） | — |
| 4 | 回归 max ICIR | 不做（Σ⁻¹ 不稳定） | §16.3 方法 3 |
| 5 | ML 合成 | 归 MLFactor（§59），不在组合层 | §16.3 方法 4 |
| 6 | 正交化 | 不做独立步骤 | YAGNI + §16.5 |
| 7 | 中性化位置 | 因子层（个股池行业+市值），组合层不做 Barra | 教程 §7.7 + §26.1 |
| 8 | 中性化维度 | 仅 industry + log_market_cap | 80% 伪 alpha 已消 |
| 9 | 权重窗口 | 扩张窗口 + 月频 + IC 只用 t-1 | 防前视 |
| 10 | 信号形态 | combined_score float + 截面 z-score = AlphaSignal | 保留强度 |
| 11 | score→weights | top_k 等权默认 / proportional 可选 | 教程 §16.6 |
| 12 | 双路径汇聚 | AlphaSignal.from_multi_factor / from_ml_factor | §65 signal_source |
| 13 | 评估指标 | 8 指标表（IC/IR/年化/夏普/MDD/Calmar/换手/贡献） | §16.3 |
| 14 | 验证纪律 | Walk-Forward + 测试集只看一次 + eval_report.json | §16.4 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§7.1** | **升级为全解 → §16.3 / §16.4 / §63**（FactorCombiner 类 + 权重窗口落定） |
| **§7.2** | **升级为全解 → §16.5 / §63**（不做正交化二次确认 + 未来 >10 因子的退出条件） |
| **§7.3** | **升级为全解 → §16.2 / §63**（neutralize_factor() + 申万一级 31 行业 + 组合层不做 Barra） |
| **§7.4** | **升级为全解 → §16.6 / §63**（AlphaSignal 类 + signal_to_weights()） |
| **§7.5** | **已定 → §63**（评估指标表 + factor_contribution + eval_report.json） |
| **§62.3 质量指标** | 组合层指标与单因子指标对齐（IC/IR 双层看） |
| **§65.1 signal_source** | AlphaSignal 三态 from_multi_factor/from_ml_factor/ensemble 对应 alphaType 的 signal_source.type |
| **data/signals/{name}/** | eval_report.json 落盘位置定稿（与 §65 dim-alphaType-publish 的 config_snapshot 呼应） |

**本节仍未解决**：ensemble（§64.5）的权重如何进 FactorCombiner（复用 ic_ir_weight？）；factor_contribution 的简化版算法精度（用分组收益当因子收益的近似误差）；signal_to_weights 的 proportional 对负 score 的处理（clip 0 vs 保留负权做空——A 股不能做空所以 clip）；eval_report.json 的 walk_forward_folds 字段结构。

---

## 64. ML/DL 策略的信号管理（§8 全解）

> §8.1（输出映射）+ §8.2（特征管理）+ §8.3（模型生命周期）+ §8.4（实验管理）+ §8.5（ensemble）+ §8.6（衰减监控）+ §8.7（训练基础设施）+ §8.8（过拟合防控）八问合并回答。核心结论：**模型输出 → MLSignalAdapter → AlphaSignal**（与 §63.5 多因子路径汇聚）；**特征=因子值+原始量价，复用因子表不建特征表**；**data/models/ 语义化版本 + experiments/ JSON 实验记录（不用 MLflow）**；**ensemble 在信号层合并复用 FactorCombiner**；衰减监控复用 §62.4；本地 CPU/单 GPU 不建基础设施；铁律四条防过拟合。与 §59（MLFactor train/predict）、§63（AlphaSignal）、§65（alphaType ml_factor）衔接。**新增代码 ≈160 行**。

### 64.1 Context 与 Confluence 参考

§59 已定 MLFactor 的调度形态（train/predict 分离 + joblib 持久化），§63 已定 AlphaSignal 双路径汇聚。本节回答 ML 策略特有的治理问题：输出形态、特征从哪来、模型版本与实验、ensemble、衰减、算力、过拟合。公司对应物是 UModel 训练平台（page 293740062）+ Alpha 入库 + Datamall 实验追踪；个人平台 ML 策略是 alstm/ 下的实验脚本群（35+ 个），需要收口成可治理体系。

**Confluence 参考**：

```text
- 量化学习教程 §13 ML 合成 (page 301832392): LightGBM/线性模型合成;
  "多重检验问题在 ML 合成会被放大"——超参每调一次都是一次尝试 N
- 量化学习教程 §16.4 回测实盘 gap: 夏普 > 2 怀疑; test IC < 0.5×train IC 过拟合
- 量化学习教程 §18.3-18.4 衰减与退出: rolling IC / 6 月夏普 / 36 月 t
- 量化学习教程 §7.8: "模型级泄漏"——用未来才有的数据训练
- UModel 平台 (page 293740062): 公司 ML 训练/部署平台,模型版本管理
- Alpha 入库 (page 288524848): ML 信号与因子信号同入库同消费
- 公司实验追踪: 无统一 MLflow——各团队自建(个人用 experiments/ JSON 即可)
```

### 64.2 模型输出到 Signal 的映射（§8.1）

**输出形态表**：

| 模型输出 | 处理 | 是否支持 | 说明 |
|---|---|---|---|
| 截面连续分数 | 直接当 score → 截面 z-score | **默认** | 回归预测收益 / 排序分 |
| 概率（分类） | `sigmoid/logit → z-score` | 支持 | from_probabilities |
| 期望收益 | 直接当 score | 支持 | 与截面分数同处理 |
| 分位数分组 | — | **不支持** | 分组是 signal_to_weights 的事（§63.5） |
| 二值多空 | — | **不推荐** | 丢失强度；强制二值在 to_weights 前做 |

```python
class MLSignalAdapter:
    """
    ML 模型输出 → AlphaSignal。与 §63.5 AlphaSignal.from_ml_factor 衔接。
    """

    @staticmethod
    def from_scores(preds: pd.DataFrame, name: str) -> "AlphaSignal":
        """preds: [date, ticker, score] → AlphaSignal（截面 z-score 在 from_ml_factor 内做）。"""
        return AlphaSignal.from_ml_factor(preds.rename(columns={"score": "prediction"}), name)

    @staticmethod
    def from_probabilities(probs: pd.DataFrame, name: str) -> "AlphaSignal":
        """probs: [date, ticker, prob] → logit → AlphaSignal。"""
        p = probs["prob"].clip(1e-6, 1 - 1e-6)
        probs = probs.assign(prediction=np.log(p / (1 - p)))
        return AlphaSignal.from_ml_factor(probs, name)


def smooth_signal(
    signal: pd.DataFrame,   # [date, ticker, score]
    halflife: int = 5,      # EWM 半衰期(交易日)
) -> pd.DataFrame:
    """
    信号平滑: EWM 衰减加权(可选,日频)。
    降低换手——信号抖动时相邻两日 score 接近,持仓变化小。
    不平滑=月频再平衡下影响小; 信号噪声大时开启。
    """
    result = signal.copy()
    result["score"] = (
        result.sort_values(["ticker", "date"])
        .groupby("ticker")["score"]
        .transform(lambda s: s.ewm(halflife=halflife).mean())
    )
    return result
```

平滑决策：**日频策略默认开 halflife=5；月频再平衡不开**（月频换手瓶颈在再平衡频率不在信号抖动）。平滑引入滞后——alpha 衰减快的因子不平滑。

### 64.3 特征管理（§8.2）：复用因子表，不建特征表

**核心决策**：特征 = **因子值 + 原始量价**，直接复用 `data/factors/` 与 `data/raw/`，**不建独立特征表**。

| 候选 | 结论 | 理由 |
|---|---|---|
| 独立特征表 feature_store | **不做** | 与因子表大量重复；存储/同步双份 |
| **复用因子表 + 原始量价** | **采用** | 因子⊂特征；build_feature_matrix() 按需拼 |
| Feature Store 平台（Feast 等） | 不做 | 外部依赖违反 §4 |

```python
def build_feature_matrix(
    feature_specs: list[dict],  # [{"source": "factor", "name": "momentum_20d"},
                                #  {"source": "raw", "field": "close", "transform": "pct_change_20"}]
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    按 spec 拼特征矩阵 [date, ticker, f1, f2, ...]。
    因子类直接 load_factor(); 量价类从 bars 做简单变换。
    """
    frames = []
    for spec in feature_specs:
        if spec["source"] == "factor":
            s = load_factor(spec["name"], start, end)  # §61
        else:
            s = transform_bars(spec["field"], spec["transform"], start, end)
        frames.append(s.set_index(["date", "ticker"]))
    return pd.concat(frames, axis=1, join="inner").reset_index()
```

**特征版本化与血缘**：复用 §58/§62 机制——`features_hash = hash(feature_specs + 依赖数据 depends_hash)`，进 model_id（§64.4）。特征筛选**人工优先，5-15 个**（PCA/Lasso 不做——解释性差 + 样本不足时选择不稳；教程 §13 警告 ML 超参自由度 = 多重检验）。**因子 ⊂ 特征**：因子是研究验证过的特征子集，ML 特征集可以更大（量价原始变换），但入库的因子必然可作特征。

### 64.4 模型生命周期（§8.3）

**训练窗口与切分**：

| 决策点 | 结论 | 理由 |
|---|---|---|
| 训练窗口 | **扩张窗口**（起点固定 2020，终点滚动） | A 股历史短，滚动 24 月样本不足 |
| 重训频率 | **年度默认** + 衰减触发 | 月频重训过拟合噪声；年度够 |
| 切分 | 严格时间 train/val/test + **Walk-Forward 禁 KFold** | 时序泄漏（§16.4） |
| 备选 | 滚动 24 月训练 | 市场结构剧变时更贴近当下 |

**部署目录与 model_id**：

```text
data/models/{name}/v{major}_{minor}/
├── model.pkl            # joblib 序列化(§59)
├── config.json          # 超参 + feature_specs + 训练日期范围
└── metadata.json        # model_id / train_end / features_hash / metrics

model_id = {name}+v{major}.{minor}(训练日期)+train_end+features_hash
```

| 要素 | 规则 |
|---|---|
| major | 特征集或模型结构变更 |
| minor | 同结构重训（训练日期进 minor） |
| 再训练触发 | 年度例行 + 衰减触发（KS 检验 p<0.05 分布漂移） |
| 退役 | 复用 §18.4 / §62.4 退出判据（rolling 6 月夏普<0 或 rolling 36 月 t<1.5） |
| A/B 对比 | 手动 t-test（新旧模型同期信号收益配对检验），单人不做自动影子流量 |

### 64.5 模型的实验管理（§8.4）：experiments/ JSON 不用 MLflow

```text
experiments/{exp_name}/
├── config.json      # 超参 / feature_specs / 数据范围 / random_seed
├── metrics.json     # train/val/test 的 IC / IR / sharpe / ...
├── notes.md         # 假设、结论、下一步
└── model.pkl        # 可选留存
```

| 候选 | 结论 | 理由 |
|---|---|---|
| MLflow / W&B | **不用** | 外部服务依赖；单人 35 个实验 JSON 目录足够 |
| **experiments/ JSON 目录** | **采用** | grep/git 友好；零运维 |
| TensorBoard | 不用 | 日志目录 + matplotlib 足够 |

**实验→变体→production 晋升线**：test_ic>0.04 + test_ir>0.5 + test_sharpe>1.0 + **优于等权 baseline**（教程 §7.4 铁律）。晋升 = 把 model.pkl 拷到 data/models/ 并升版。

**可复现五要素**（必须记录）：random_seed / 依赖版本（uv.lock hash）/ 特征列表 / 日期范围 / 全部超参。缺一不可复现。

### 64.6 多模型 ensemble（§8.5）：信号层合并

```python
def ensemble_models(
    signal_list: list[pd.DataFrame],  # 每个 [date, ticker, score]
    weights: list[float] = None,      # 默认等权
) -> pd.DataFrame:
    """
    多模型信号加权平均。复用 FactorCombiner 逻辑(§63.2)——
    ensemble 是 signal_source.type="ensemble"(§65.1) 的实现。
    合并在信号层做,不在模型层(模型层平均=深水区,不做)。
    """
    if weights is None:
        weights = [1.0 / len(signal_list)] * len(signal_list)
    merged = signal_list[0][["date", "ticker"]].copy()
    merged["score"] = sum(
        s.set_index(["date", "ticker"])["score"] * w
        for s, w in zip(signal_list, weights)
    ).values
    return merged
```

**信号层 vs 模型层合并**：模型层（平均 logits/参数）涉及模型结构对齐，深水区不做；**信号层加权平均**简单有效且可解释（哪个模型今天贡献大）。权重来源：validation ICIR 或等权（默认等权，防权重优化过拟合）。对应 alstm/ 下 hybrid.py / low_volatility_model_a_mixer.py 的收口方向。

### 64.7 信号衰减与监控（§8.6）：复用 §62.4

| 指标 | 告警线 | 退出线 | 处置 |
|---|---|---|---|
| rolling IC vs 历史均值 | < 50% | < 30% | warning→重训/降权；exit→停用 |
| rolling 6 月夏普 | < 0.3 | < 0 | 同上 |
| 回测 vs 实盘偏差 | < 80% | < 70% | 查执行滑点/数据差异 |
| KS 分布漂移 | p < 0.05 | — | 触发提前重训（§64.4） |

响应三态 **healthy / warning / exit** 与 §62.4 check_factor_health() 同一阈值体系——ML 信号衰减监控**不另起一套**，就用因子的。warning = 重训 + ensemble 降权；exit = 停用回退等权组合（§63.2 主路径）。

### 64.8 ML 训练的基础设施（§8.7）

| 决策 | 结论 | 理由 |
|---|---|---|
| 算力 | **本地 CPU / 单 GPU** | LSTM/LightGBM 单机分钟级；不建集群 |
| 分布式训练 | 不做 | 数据量（日频 × 5000 股 × 5 年）远不需要 |
| 任务队列 | 不做（§59 不引入 Celery/Airflow） | 单人手动触发训练 |
| 实验看板 | experiments/ JSON + matplotlib | 不做 Web 看板 |
| 超参搜索 | 手动网格/随机几组 + 记录 attempt_log（§27.9） | 自动调参 = 多重检验放大器（教程 §13） |

### 64.9 样本外推与过拟合防控（§8.8）：铁律四条

**铁律**：

```text
1. 训练/测试时间切分,禁 KFold(时序泄漏)
2. 测试集只看一次——反复看测试集 = 变相训练
3. 每个 ML 变体必须和等权 baseline 对比——不超 baseline 的复杂度是过拟合
4. 一切尝试记入 attempt_log(§27.9),t 阈值随 N 抬高(t=√(2lnN))
```

**过拟合检测表**：

| 症状 | 判定 | 动作 |
|---|---|---|
| test IC < 0.5 × train IC | SUSPECT | 查泄漏/减特征/加正则 |
| 夏普 > 2.0 | SUSPECT | 查前视（shift(1)/成交价基准） |
| 夏普未超等权 10% | REJECT | 不晋升，退回等权 |
| 特征重要性单特征 > 50% | SUSPECT | 查目标泄漏（特征含未来信息） |
| 换手 > 200%/月 | SUSPECT | 信号噪声学了样本内抖动 |

```python
def sanity_check_ml_model(
    test_metrics: dict,   # {"ic": ..., "sharpe": ...}
    train_metrics: dict,
    baseline_sharpe: float,
) -> str:
    """过拟合三检。返回 "PASS" | "SUSPECT" | "REJECT"。"""
    if test_metrics["sharpe"] < baseline_sharpe * 1.10:
        return "REJECT"          # 未超等权 10%
    if test_metrics["sharpe"] > 2.0:
        return "SUSPECT"         # 查前视
    if test_metrics["ic"] < 0.5 * train_metrics["ic"]:
        return "SUSPECT"         # 过拟合经典信号
    return "PASS"
```

### 64.10 决策汇总（22 行）

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | 输出映射 | 截面分数默认 / 概率→logit→z-score / 期望收益同分数 | — |
| 2 | 不支持输出 | 分位数分组（属 to_weights）、二值（丢强度） | §16.6 |
| 3 | 适配器 | MLSignalAdapter.from_scores / from_probabilities | — |
| 4 | 信号平滑 | EWM halflife=5 日频可选，月频不开 | 换手 vs 滞后权衡 |
| 5 | 特征来源 | 因子值 + 原始量价，**复用因子表不建特征表** | 因子⊂特征 |
| 6 | 特征拼装 | build_feature_matrix(feature_specs) | — |
| 7 | 特征版本 | features_hash 进 model_id（复用 depends_hash） | §58/§62 |
| 8 | 特征筛选 | 人工优先 5-15 个，PCA/Lasso 不做 | 教程 §13 多重检验 |
| 9 | 训练窗口 | 扩张窗口默认，滚动 24 月备选 | A 股历史短 |
| 10 | 重训频率 | 年度 + KS p<0.05 衰减触发 | §18.3 |
| 11 | 切分 | 时间切分 + Walk-Forward 禁 KFold | §16.4 |
| 12 | 部署 | data/models/{name}/v{major}_{minor}/ | §59 joblib |
| 13 | model_id | name+major+minor(训练日期)+train_end+features_hash | — |
| 14 | 退役 | 复用 §18.4/§62.4 退出判据 | 不另起一套 |
| 15 | A/B | 手动 t-test，不做自动影子流量 | 单人 |
| 16 | 实验管理 | experiments/ JSON 不用 MLflow | 零运维 |
| 17 | 晋升线 | test_ic>0.04 + test_ir>0.5 + test_sharpe>1.0 + 优于等权 | §7.4 铁律 |
| 18 | 可复现 | random_seed/依赖版本/特征列表/日期范围/超参 五要素 | — |
| 19 | ensemble | 信号层加权平均（复用 FactorCombiner），默认等权 | 不做模型层平均 |
| 20 | 衰减监控 | 复用 §62.4 三态 healthy/warning/exit | §18.3 |
| 21 | 基础设施 | 本地 CPU/单 GPU，不建集群/队列/看板 | 教程 §25 |
| 22 | 过拟合防控 | 铁律四条 + sanity_check_ml_model() 三检 | §13/§16.4 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§8.1** | **已定 → §64**（输出形态表 + MLSignalAdapter + smooth_signal） |
| **§8.2** | **已定 → §64**（复用因子表 + build_feature_matrix + features_hash） |
| **§8.3** | **已定 → §64**（扩张窗口 + 年度重训 + data/models 版本化 + model_id） |
| **§8.4** | **已定 → §64**（experiments/ JSON + 晋升线 + 五要素可复现） |
| **§8.5** | **已定 → §64**（ensemble_models() 信号层合并） |
| **§8.6** | **已定 → §64**（复用 §62.4 衰减监控，不另起指标体系） |
| **§8.7** | **已定 → §64**（本地 CPU/单 GPU 不建基础设施） |
| **§8.8** | **已定 → §64**（铁律四条 + sanity_check_ml_model()） |
| **§59 MLFactor** | train/predict 与本节生命周期衔接——train 输出写 data/models/，predict 读 model_id |
| **§63.5 AlphaSignal** | from_ml_factor 即本节 MLSignalAdapter 的下游出口 |
| **§65.1 signal_source** | type="ml_factor"/"ensemble" 的实现落点 |
| **alstm/ 35 脚本** | 收口方向：实验进 experiments/，变体晋级进 data/models/，ensemble 进 ensemble_models() |
| **§27.9 attempt_log** | ML 超参搜索次数计入 N（教程 §13 警告） |

**本节仍未解决**：build_feature_matrix 的 transform_bars() 具体变换集（pct_change_N/rank/z-score 有哪些）；features_hash 的哈希算法（对 feature_specs JSON 做 md5？）；KS 检验的参考分布窗口（用训练期全量还是滚动 252 日）；ensemble 权重用 validation ICIR 的具体公式（复用 _ic_ir_weights 即可？）。

---

## 65. AlphaType：注册 / 标签 / 生命周期 / 规则库 / 调度 / 约束（§9.1–§9.6 全解）

> §9.1（注册表 schema）+ §9.2（标签体系）+ §9.3（生命周期）+ §9.4（规则库 API）+ §9.5（Signal 调度）+ §9.6（Portfolio 约束求解）六问合并回答（§9.7 本金手数已定 → §17.3/§34.4）。核心结论：**alphaType = 策略配方**（signal_source + rules + params），注册表用 **JSON 文件**（data/registry/）不用 SQLite/YAML；标签 **7 维全枚举禁自由文本**；生命周期 **五态** draft→published→production→deprecated→archived，无审批流自动检查+审计日志；规则库 **Rule ABC + RulePipeline 串联**；调度 **cron+手动不事件驱动**，失败 hold 上日 Portfolio；约束求解 **解析解等权默认**。与 §63（AlphaSignal）、§64（ML）、§66（执行）衔接。**新增代码 ≈200 行**。

### 65.1 Context 与 Confluence 参考

§7–§8 完成了信号域（多因子与 ML 两条路径 → AlphaSignal）。本节回答：**策略怎么注册、打标、流转、配置规则、定时产出、约束持仓**——即公司 PMS 里 alphaType 维表 + 计算器注册 + 发布流水线的个人版。公司是多人多策略生产体系（审批、灰度、审计齐全）；个人单人 <10 策略，取其结构、去其流程。

**Confluence 参考**：

```text
- PMS ms_calculator (page 335950197): CalculatorRegistry + YAML 配置 +
  BaseCalculator——公司计算器(策略算法)注册与实例化机制
- 风控指令系统 (page 316060789): Decision Engine + control_key——
  约束/拦截的指令化表达(放行/约束/拦截三态)
- 量化学习教程 §9 仓位三维 (page 301832392): 标的数量/个股权重/总仓位
- 量化学习教程 §17.5 拆单: >1% 日成交量才拆
- Alpha 入库 (page 288524848): 公司 Alpha(signal)发布入库的生命周期
- 事件驱动调度 (page 324637543): 公司 Datamall TriggerRouter 事件驱动——
  个人不需要(月频/日频定时足够)
- dim-alphaType-meta-info / dim-alphaType-publish: 公司数仓两张维表——
  元信息 + 发布记录
```

### 65.2 alphaType 注册表 schema（§9.1）：JSON 双表

**alphaType 定义**：策略配方 = `signal_source + rules + params`——一个可实例化的策略规格，不是代码本身（代码在 strategies/ 目录，配方引用它）。

**dim-alphaType-meta-info**（元信息）：

```json
{
  "alpha_type_id": "mom_top5_etf_v1",
  "name": "ETF 双重动量 Top5",
  "description": "教程 §2 双重动量变体——黄金样本基线",
  "version": "1.0",
  "signal_source": {
    "type": "multi_factor",
    "factors": ["momentum_20d", "volatility_20d"],
    "combiner_method": "equal_weight",
    "post_process": "PIPELINE_ETF"
  },
  "rules": [
    {"rule_id": "TopKFilter", "params": {"k": 5}},
    {"rule_id": "EqualWeightRule", "params": {}}
  ],
  "asset_pool": "etf",
  "frequency": "1M",
  "author": "nachuan",
  "created_at": "2026-10-04T10:00:00",
  "updated_at": "2026-10-04T10:00:00",
  "tags": {
    "asset_type": "etf", "holding_period": "1M", "style": "momentum",
    "capacity": "medium", "turnover": "low"
  }
}
```

signal_source.type 三态对应 §63.5/§64.6：`"multi_factor" | "ml_factor" | "ensemble"`。

**dim-alphaType-publish**（发布记录）：

```json
{
  "publish_id": "mom_top5_etf_v1_20261004",
  "alpha_type_id": "mom_top5_etf_v1",
  "config_snapshot": "configs/strategies/mom_top5_etf_v1.toml@a3f9c2",
  "status": "published",
  "published_at": "2026-10-04T10:00:00",
  "adopted_at": null,
  "retired_at": null
}
```

**存储决策**：

| 候选 | 结论 | 理由 |
|---|---|---|
| **data/registry/alpha_types.json + alpha_type_publishes.json** | **采用** | 零依赖、grep/git 友好、<10 策略规模足够 |
| SQLite | 不用 | 引入 schema 迁移问题，单人无并发查询需求 |
| YAML | 不用 | §13.1 已定 TOML；注册表 JSON 与 config TOML 分工（注册表=数据、配置=参数） |
| 公司数仓维表 | 不适用 | 个人无数仓 |

**版本化**：config_snapshot 记 `path@hash`（§13.2），变更靠 git history 追溯——注册表记录"发布了什么配置"，git 记录"配置怎么变的"。

### 65.3 多维标签体系（§9.2）：7 维全枚举

**禁自由文本标签**——每个维度是封闭枚举，防标签碎片化（"momentum" vs "Mom" vs "动量"）。

| 维度 | 枚举值 | 说明 |
|---|---|---|
| asset_type | stock / etf / convertible_bond / futures / option | §13.4 多资产 |
| frequency | 1d / 1W / 1M / intraday | 产出频率 |
| holding_period | 1d / 1W / 2W / 1M / 3M | 持仓周期 |
| style | momentum / value / low_vol / quality / size / carry / ml / mixed | 风格 |
| capacity | small(<100万) / medium(100-5000万) / large(>5000万) | 资金容量 |
| turnover | low(<50%/月) / medium(50-150%) / high(>150%) | 换手档 |
| status | 跟随 §9.3 生命周期五态 | 不重复定义 |

**打标时机**：

| 时机 | 动作 |
|---|---|
| 创建 alphaType 时 | 必须填满 7 维（create_alpha_type 校验） |
| 发布（published）时 | 校验 tags 完整性，不全则拒 |
| 晋升 production 时 | 可修正 capacity/turnover（回测统计后更新） |
| 退役时 | status 维自动跟随生命周期 |

```python
TAG_DIMENSIONS = {
    "asset_type": {"stock", "etf", "convertible_bond", "futures", "option"},
    "frequency": {"1d", "1W", "1M", "intraday"},
    "holding_period": {"1d", "1W", "2W", "1M", "3M"},
    "style": {"momentum", "value", "low_vol", "quality", "size", "carry", "ml", "mixed"},
    "capacity": {"small", "medium", "large"},
    "turnover": {"low", "medium", "high"},
}

def list_alpha_types(
    registry: dict,
    filters: dict = None,   # {"style": "momentum", "asset_type": "etf"}
) -> list[dict]:
    """按标签过滤查询 alphaType。"""
    result = registry["alpha_types"]
    for dim, val in (filters or {}).items():
        result = [a for a in result if a["tags"].get(dim) == val]
    return result
```

### 65.4 生命周期状态机（§9.3）：五态 + 自动审计

```text
draft →(发布)→ published →(晋升)→ production →(退役)→ deprecated →(归档)→ archived
                      ↑_______________|（可退回 published 改配方重发）
```

| 转换 | 条件 | 动作 |
|---|---|---|
| draft→published | 配方完整 + 标签 7 维全 | 写 publish 记录 |
| published→production | **Walk-Forward 通过** + test 指标达 §64.5 晋升线 + 优于等权 | adopted_at 时间戳 |
| production→deprecated | §62.4/§64.7 exit 判据触发 或人工退役 | retired_at；停用回退等权 |
| deprecated→archived | 确认无引用（回测配置快照无引用） | 归档不删 |
| 任意→draft | 配方修改 | 回炉，版本+1 |

**无审批流**（单人）：promote_alpha_type() 自动检查指标门槛 + 写审计日志。

```python
def promote_alpha_type(alpha: dict, eval_report: dict) -> bool:
    """
    晋升 production。自动检查晋升线(§64.5),无审批流但全审计。
    """
    ok = (
        eval_report["metrics"]["ic_mean"] > 0.04
        and eval_report["metrics"]["ic_ir"] > 0.5
        and eval_report["metrics"]["test_sharpe"] > 1.0
    )
    log_audit({
        "action": "promote", "alpha_type_id": alpha["alpha_type_id"],
        "from": alpha["status"], "to": "production" if ok else alpha["status"],
        "metrics": eval_report["metrics"], "ts": now_iso(),
    })
    if ok:
        alpha["status"] = "production"
    return ok


def log_audit(entry: dict) -> None:
    """审计日志 append-only。对标公司发布流水线审计。"""
    with open("data/registry/audit_log.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
```

审计日志 `data/registry/audit_log.jsonl` **append-only 不可改**（jsonl 追加，改动靠新条目更正）。

### 65.5 实例层的规则库 API（§9.4）：Rule ABC + RulePipeline

**规则四分类**：

| 类别 | 作用位置 | 示例 |
|---|---|---|
| signal_transform | 信号变换 | SmoothSignal / RankSignal |
| filter | 标的过滤 | TopKFilter / ExcludeST |
| position | 权重分配 | EqualWeight / ProportionalWeight |
| risk_gate | 风控门 | TurnoverLimit / ConcentrationLimit |

```python
class Rule(ABC):
    """规则基类。对标公司 BaseCalculator(page 335950197)的轻量版。"""
    rule_id: str = ""
    category: str = ""           # signal_transform|filter|position|risk_gate
    param_schema: dict = {}      # {"k": int} / {"max_turnover": float}

    @abstractmethod
    def apply(self, data: pd.DataFrame, **params) -> pd.DataFrame: ...


class TopKFilter(Rule):
    rule_id, category = "TopKFilter", "filter"
    param_schema = {"k": int}

    def apply(self, data, k=20, **_):
        return data.groupby("date", group_keys=False).apply(
            lambda g: g.nlargest(k, "score")
        )


class RulePipeline:
    """规则流水线: 串联执行。不支持并联/嵌套(YAGNI)。"""

    def __init__(self, rules: list[tuple[str, dict]]):
        self.rules = [(create_rule(rid), p) for rid, p in rules]

    def run(self, data: pd.DataFrame) -> pd.DataFrame:
        for rule, params in self.rules:
            data = rule.apply(data, **params)
        return data
```

**param_schema 的价值**：配方 JSON 里 params 在实例化时校验类型与必填——配置错误在启动时暴露而非回测跑一半才发现。

**规则 vs 算法变体的区分标准**（关键设计问题）：

| 判据 | 是规则 | 是新 alphaType |
|---|---|---|
| 改它只影响精细度（如 k=10→20） | ✓ | |
| 可独立开关做 A/B | ✓ | |
| 改它策略逻辑本质变化（动量→反转） | | ✓ |
| 需要独立回测与评估指标 | | ✓ |
| 是通用件（多策略复用） | ✓ | |

一句话：**改它只影响精细度→规则；变成另一个策略→新 alphaType**。

### 65.6 Signal 产出的调度（§9.5）：cron + 手动，失败 hold

| 决策点 | 结论 | 理由 |
|---|---|---|
| 产出时机 | 日频 **15:30 后**（收盘数据就绪）/ 月频月末 / 盘中不做 | 盘中实时是公司需求 |
| 触发方式 | **cron + 手动**，不做事件驱动 | 教程 §25.2；个人 Job<10 |
| 失败处理 | 重试 1 次 → **hold 上一交易日 Portfolio** → 告警 | **不清仓**——数据故障不是平仓理由 |
| 配置 | schedule_config: cron `"30 16 * * 1-5"` | 16:30 留数据缓冲 |

```python
def run_daily_signal(alpha: dict, date: str) -> dict:
    """
    每日信号产出。失败 hold——不清仓是铁律(§10.2 同精神)。
    """
    for attempt in range(2):   # 重试 1 次
        try:
            signal = build_alpha_signal(alpha, date)      # §63/§64
            portfolio = build_portfolio(signal, alpha)    # §65.7
            return {"status": "SUCCESS", "portfolio": portfolio}
        except Exception as e:
            if attempt == 1:
                send_alert("HIGH", f"signal failed {alpha['alpha_type_id']}: {e}")
                return {"status": "HOLD", "portfolio": load_prev_portfolio(alpha, date)}
```

与 §11.1 daily_pipeline（16:45）衔接：信号产出是 pipeline 的一个 Job。

### 65.7 Portfolio 构建的约束求解（§9.6）：解析解等权默认

**约束集表**：

| 约束 | V1 决策 | 处理方式 |
|---|---|---|
| 集中度（单标的上限） | 等权天然满足（1/N ≤ 上限） | 检查告警不修正 |
| 换手率上限 | **检查告警** | 不硬裁（硬裁破坏信号） |
| 行业暴露 | **不做** | Barra 级需求（§63.4） |
| 资金约束 | 简单乘法（nav × w） | 手数取整在 §17.3/§34.4 |
| 杠杆 | **不做** | A 股个人无两融规划 |
| 市场中性 | **不做** | — |

**求解器选型**：

| 候选 | 结论 | 理由 |
|---|---|---|
| **解析解（等权/top_k）** | **V1 默认** | 约束集里只有检查项，无真正优化问题 |
| scipy.optimize | 不要 | 无目标函数可优化 |
| cvxpy | V2 备选 | risk parity / Black-Litterman 上来才需要（§17.2 方法 3/4） |

```python
def build_portfolio(
    signal: pd.DataFrame,     # [date, ticker, score]
    alpha: dict,
    prev_weights: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    信号 → 目标权重(带约束检查)。V1 解析解。
    """
    # 1. 解析解: top_k 等权(§63.5)
    weights = signal_to_weights(signal, method="top_k", k=alpha["rules"]["top_k"])

    # 2. 总仓位缩放(教程 §9 仓位三维的第三维)
    total_pos = alpha["params"].get("total_position", 1.0)   # 默认满仓
    weights["weight"] = weights["weight"] * total_pos

    # 3. 集中度检查——告警不修正
    max_w = weights.groupby("date")["weight"].max()
    if (max_w > alpha["params"].get("max_single_weight", 0.25)).any():
        send_alert("MEDIUM", "concentration exceeded")

    # 4. 换手检查——告警不裁剪
    if prev_weights is not None:
        turnover = compute_turnover(weights, prev_weights)
        if turnover > alpha["params"].get("max_turnover", 1.5):
            send_alert("MEDIUM", f"turnover {turnover:.2f} > limit")

    return weights


def compute_turnover(new_w: pd.DataFrame, old_w: pd.DataFrame) -> float:
    """换手率 = Σ|Δw| / 2(教程 §10.6)。"""
    merged = new_w.merge(old_w, on=["date", "ticker"], how="outer",
                         suffixes=("_new", "_old")).fillna(0)
    return (merged["weight_new"] - merged["weight_old"]).abs().sum() / 2
```

**求解失败处理**：V1 解析解不发生数值失败；V2 cvxpy 求解失败 → 放松约束重试一次 → 仍失败回退等权（不空仓）。

**性能**：全 A 5000 标的 × 月频 60 期 <100ms——性能非瓶颈，不优化。

### 65.8 决策汇总（22 行）

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | alphaType 本质 | 策略配方 = signal_source + rules + params | ms_calculator YAML |
| 2 | 注册表 schema | dim-alphaType-meta-info 15 字段 JSON | 公司数仓维表简化 |
| 3 | 发布记录 | dim-alphaType-publish（config_snapshot/status/时间戳） | 同上 |
| 4 | 存储 | data/registry/*.json 不用 SQLite/YAML | 零依赖 + §13.1 |
| 5 | 版本化 | config_snapshot `path@hash` + git | — |
| 6 | 标签 | 7 维全枚举**禁自由文本** | 防碎片化 |
| 7 | 打标时机 | 创建必填 / 发布校验 / 晋升可修正 | — |
| 8 | 查询 | list_alpha_types(filters) | — |
| 9 | 生命周期 | 五态 draft→published→production→deprecated→archived | Alpha 入库 page 288524848 |
| 10 | 晋升条件 | Walk-Forward 通过 + §64.5 晋升线 + 优于等权 | §7.4 铁律 |
| 11 | 审批流 | 无——自动检查 + log_audit | 单人 |
| 12 | 审计日志 | audit_log.jsonl append-only | 公司发布流水线审计 |
| 13 | 规则分类 | signal_transform / filter / position / risk_gate | Decision Engine 控制指令 |
| 14 | 规则 API | Rule ABC apply(data,params) + param_schema 校验 | BaseCalculator |
| 15 | 流水线 | RulePipeline 串联，不支持并联/嵌套 | YAGNI |
| 16 | 规则 vs 变体 | 改它只影响精细度→规则；变成另一策略→新 alphaType | — |
| 17 | 产出时机 | 日频 15:30 后 / 月频月末 / 盘中不做 | — |
| 18 | 触发方式 | cron + 手动，不做事件驱动 | page 324637543 不适用 |
| 19 | 失败处理 | 重试 1 次 → hold 上日 Portfolio → 告警，**不清仓** | — |
| 20 | 约束集 | 集中度检查/换手告警/资金乘法；行业/杠杆/中性不做 | 教程 §9 仓位三维 |
| 21 | 求解器 | 解析解默认，cvxpy V2 备选 | 无优化问题可解 |
| 22 | 求解失败 | V1 不发生；V2 放松→回退等权不空仓 | — |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§9.1** | **已定 → §65**（meta-info 15 字段 + publish 记录 + JSON 双表落定） |
| **§9.2** | **已定 → §65**（7 维全枚举 + 打标时机 + list_alpha_types） |
| **§9.3** | **已定 → §65**（五态 + 转换条件表 + promote_alpha_type + log_audit） |
| **§9.4** | **已定 → §65**（Rule ABC + RulePipeline + 规则/变体判据表） |
| **§9.5** | **已定 → §65**（cron+手动 + run_daily_signal hold 策略） |
| **§9.6** | **升级为全解 → §17.1 / §17.2 / §34 / §65**（约束集表 + build_portfolio + compute_turnover 落定） |
| **§63.5 AlphaSignal** | signal_source.type 三态在 alphaType 配方里引用 |
| **§65.6/§11.1** | run_daily_signal 是 daily_pipeline 的一个 Job；hold 策略与 §10.2 风控"不清仓"精神一致 |
| **§17.2/§34 权重四法** | build_portfolio 的解析解入口与之对齐（top_k 等权默认） |
| **configs/strategies/*.toml** | §13.1 策略配置与 alphaType 配方的分工：配方=结构引用，config=数值参数 |

**本节仍未解决**：RulePipeline 与 §65.7 build_portfolio 的关系（rules 里的 position 类规则是否就是 signal_to_weights 的参数化）；audit_log.jsonl 的 schema 字段定稿（action/from/to/metrics/ts 已列，还要什么）；schedule_config 与 §11.1 APScheduler 的装配位置（谁读 cron 配置建 Job）；退回 published 改配方时 version 如何递增（patch 还是 minor）。

---

## 66. 执行域：订单生成 / 风控 / 模拟账本 / 柜台抽象（§10 全解）

> §10.1（订单生成 Utrade 等价物）+ §10.2（风控 Themis 等价物）+ §10.3（模拟账本 TB 等价物）+ §10.4（柜台抽象接口）四问合并回答。信号域（§7–§9）到执行域的切换点：**目标权重 diff → 订单列表 → pre-trade 风控 → 收盘价撮合 + 费用模型 → SimAccount 记账**。核心决策：**pandas 自研 <500 行，不用 backtrader/vnpy**；风控只 pre-trade、只 PASS/BLOCK 两级；撮合收盘价成交 V1；Broker ABC 四方法，V2 路径 QMT。与 §63（权重）、§65（build_portfolio）、§68（§12 旧决策重审）衔接。**新增代码 ≈280 行**。

### 66.1 Context 与 Confluence 参考

§63/§65 产出了目标权重（连续小数，已做约束检查）。本节覆盖执行域四块：权重→订单（Utrade 等价物）、下单前风控（Themis 等价物）、成交与记账（TB 等价物）、真实柜台接口（V2）。月频低频、V1 以回测模拟为主——教程 §25.2"能用 pandas 别上框架"、§25.3"入门用不到实盘"。

**Confluence 参考**：

```text
- hubble_utrade (page 301596677): 公司执行层——ITraderLib 接口 +
  TTPolicy 链(TTPCloseFirst/TTPCancelExceed/TTPVolumeController) +
  RiskManager。订单从信号到柜台的全链路
- 交易系统 (page 282827800): sid/vasid/RCS 标识体系 + OnAlphaUpdate
  信号→订单调度(OrderSchedule)
- 风控指令系统 (page 316060789): Decision Engine 控制状态——
  放行/约束/拦截三态(Themis 的控制面)
- 量化学习教程 §17 执行算法 (page 301832392): 拆单 >1% 日成交量才拆;
  先卖后买; TWAP/VWAP 个人不用
- 量化学习教程 §3.2 成本: 佣金/印花税/过户费/滑点四件套
- 量化学习教程 §9 仓位三维: 标的数量/个股权重/总仓位
- 公司链路: OnAlphaUpdate → OrderSchedule → TTPolicy 链 →
  TraderLib.NewOrder → DataBus → hubble_tb → 柜台
```

### 66.2 订单生成（§10.1）：目标权重 diff → 订单列表

**公司链路**：OnAlphaUpdate → OrderSchedule → TTPolicy 链（TTPCloseFirst 先平、TTPCancelExceed 超量撤、TTPVolumeController 量控制）→ TraderLib.NewOrder → DataBus → hubble_tb → 柜台。

**个人等价物**：目标权重 vs 当前持仓 → 差分 → 订单列表（<300 行 pandas）。

```python
def compute_order_diff(
    target_weights: pd.DataFrame,   # [ticker, weight]
    account: "SimAccount",
    nav: float,
) -> list[dict]:
    """
    目标权重 → 订单差分。
    floor(weight×nav/price/lot_size)×lot_size —— §17.3/§34.4 手数取整。
    """
    orders = []
    for _, row in target_weights.iterrows():
        price = get_latest_price(row["ticker"])
        if price is None or price <= 0:
            continue
        target_shares = int(
            row["weight"] * nav / price // 100
        ) * 100          # lot_size=100(A 股)
        current_shares = account.holdings.get(row["ticker"], 0)
        diff = target_shares - current_shares
        if diff == 0:
            continue
        orders.append({
            "ticker": row["ticker"],
            "side": "BUY" if diff > 0 else "SELL",
            "shares": abs(diff),
            "limit_price": price,
        })
    return orders
```

| 决策点 | 结论 | 理由 |
|---|---|---|
| 挂单处理 | **V1 不处理**（当日全成或不成） | 月频挂单跨日是高频需求 |
| 拆单 | **V1 不做**，check_split_needed() 仅告警 | 教程 §17.5：>1% 日成交量才拆——月频 ETF/大盘股远不到 |
| 执行优先级 | **先卖后买**；卖出按市值 desc；买入按信号 desc | 释放资金再买入（教程 §17） |
| 订单类型 | LIMIT，limit_price = close | 简单诚实；市价单 V2 |
| 框架选型 | **pandas 自研 <300 行**，不用 backtrader/vnpy | 教程 §25.2；框架学习成本>收益 |

```python
def generate_orders(
    target_weights: pd.DataFrame,
    account: "SimAccount",
    nav: float,
) -> list[dict]:
    """订单生成全链路: diff → 拆单告警 → 排序(先卖后买)。"""
    orders = compute_order_diff(target_weights, account, nav)
    for o in orders:
        check_split_needed(o)          # >1% 日成交量 → 告警(不拆)
    sells = sorted([o for o in orders if o["side"] == "SELL"],
                   key=lambda o: -o["shares"] * o["limit_price"])
    buys = sorted([o for o in orders if o["side"] == "BUY"],
                  key=lambda o: -abs(get_signal_score(o["ticker"])))
    return sells + buys
```

### 66.3 风控（§10.2）：pre-trade 七规则，PASS/BLOCK 两级

**公司两层风控**：RiskManager（ArbiWrapper/RiskForceManager 硬拦截）+ Themis（Decision Engine 放行/约束/拦截三态控制面）。**个人目标**：防"策略失控/操作错误"——不是防市场风险。

**风控七规则表**：

| # | 规则 | 阈值 | 动作 |
|---|---|---|---|
| 1 | 单标的仓位 | ≤ 25% | BLOCK |
| 2 | 总仓位 | ≤ 95%（留 5% 现金） | BLOCK |
| 3 | 单日换手 | ≤ 150% | 告警（不拦——硬拦破坏信号） |
| 4 | 单笔金额 | ≥ 5000 元（低于则跳过该单） | SKIP |
| 5 | 禁做空 | 卖出 ≤ 持仓量 | BLOCK |
| 6 | 禁 ST | 标的池已排除（research filter） | BLOCK |
| 7 | 流动性 | 单笔 > 1% 日成交量 | 告警（§66.2 拆单告警） |

| 决策点 | 结论 | 理由 |
|---|---|---|
| 风控时机 | **只 pre-trade 不 intra-trade** | 月频无盘中监控需求 |
| 控制态 | **只 PASS/BLOCK 两级**，不做"约束"态 | 三态约束是公司多策略冲突仲裁需求 |
| 审计 | data/execution/risk_audit.jsonl | 拦截必须可追溯 |

```python
@dataclass
class RiskCheckResult:
    passed: bool
    blocked_by: list[str]   # 被哪条规则拦的
    warnings: list[str]


def pre_trade_risk_check(
    orders: list[dict],
    account: "SimAccount",
    nav: float,
    config: dict,
) -> RiskCheckResult:
    """逐规则检查,一 BLOCK 即拒。"""
    blocked, warnings = [], []
    for o in orders:
        value = o["shares"] * o["limit_price"]
        if value / nav > config.get("max_single_weight", 0.25):
            blocked.append(f"single_weight:{o['ticker']}")
        if o["side"] == "SELL" and o["shares"] > account.holdings.get(o["ticker"], 0):
            blocked.append(f"short_forbidden:{o['ticker']}")
        if o["side"] == "BUY" and value < config.get("min_trade_value", 5000):
            o["skip"] = True   # 金额过小跳过(不拒整批)
        if value > 0.01 * get_daily_volume(o["ticker"]):
            warnings.append(f"liquidity:{o['ticker']}")
    return RiskCheckResult(not blocked, blocked, warnings)
```

风控审计：`log_risk_audit()` append-only 写 risk_audit.jsonl（时间戳/规则/标的/动作/原因）。

### 66.4 模拟账本（§10.3）：收盘价撮合 + 费用四件套

**撮合模型 V1：收盘价成交**。

| 决策点 | 结论 | 理由 |
|---|---|---|
| 成交价 | **close**（决策日收盘价） | V1 简化；保守版用 T+1 开盘（§18.2 正误对比——T 收盘信号用 T 收盘价成交通常虚高，月频影响小；黄金样本对拍用手算口径一致） |
| 无价格 | REJECTED（不成交记 REJECTED） | 停牌 |
| 部分成交 | 不模拟（全成或不成） | 月频量小 |
| 费用 | FeeModel 四件套 | 教程 §3.2 |

**费用模型**（教程 §3.2，A 股）：

| 费用项 | 费率 | 方向 | 备注 |
|---|---|---|---|
| 佣金 | 万 2.5 | 双边 | 最低 5 元 |
| 印花税 | 万 5 | **卖出** | ETF 免 |
| 过户费 | 万 0.1 | 双边 | |
| 滑点 | 0.1% | 方向不利 | 买+卖− |

```python
class FeeModel:
    """A 股费用四件套(教程 §3.2)。ETF 免印花税(§13.4 ASSET_RULES 一致)。"""

    COMMISSION_RATE = 0.00025
    COMMISSION_MIN = 5.0
    STAMP_TAX_RATE = 0.0005
    TRANSFER_FEE_RATE = 0.00001
    SLIPPAGE_RATE = 0.001

    @staticmethod
    def _is_etf(ticker: str) -> bool:
        return ticker[:2] in ("51", "15", "16")

    def compute(self, side: str, price: float, shares: int, ticker: str) -> float:
        value = price * shares
        commission = max(value * self.COMMISSION_RATE, self.COMMISSION_MIN)
        stamp = value * self.STAMP_TAX_RATE if (side == "SELL" and not self._is_etf(ticker)) else 0.0
        transfer = value * self.TRANSFER_FEE_RATE
        slippage = value * self.SLIPPAGE_RATE
        return commission + stamp + transfer + slippage
```

**成本量级**：ETF 单边 0.15–0.2%，股票单边 0.35–0.4%（印花税卖出单边决定差异）。月频换手 30–100%，年成本约 2–5%——alpha 必须覆盖。

```python
class SimAccount:
    """模拟账本。对标公司 hubble_tb(TB 等价物)。"""

    def __init__(self, cash: float):
        self.cash = cash
        self.holdings: dict[str, int] = {}
        self.cost_basis: dict[str, float] = {}
        self.trade_log: list[dict] = []

    def execute_fill(self, order: dict, fill_price: float, fees: float):
        value = fill_price * order["shares"]
        if order["side"] == "BUY":
            self.cash -= value + fees
            self.holdings[order["ticker"]] = self.holdings.get(order["ticker"], 0) + order["shares"]
        else:
            self.cash += value - fees
            self.holdings[order["ticker"]] -= order["shares"]
        self.trade_log.append({**order, "fill_price": fill_price, "fees": fees})

    def get_nav(self, prices: dict[str, float]) -> float:
        mkt = sum(sh * prices.get(t, 0) for t, sh in self.holdings.items())
        return self.cash + mkt

    def get_snapshot(self, prices) -> dict:
        return {"cash": self.cash, "nav": self.get_nav(prices),
                "holdings": dict(self.holdings)}
```

**多品种**：不做预先抽象，V1 仅 A 股（stock+ETF）。期货/期权的保证金与逐日盯市是另一套账本（§13.4 V3 再说）。

### 66.5 柜台抽象接口（§10.4）：Broker ABC 四方法

```python
class Broker(ABC):
    """柜台抽象。4 方法封顶——不做 cancel/modify/行情/事件回调。"""

    @abstractmethod
    def submit_orders(self, orders: list[dict]) -> list[dict]: ...
    @abstractmethod
    def get_holdings(self) -> dict[str, int]: ...
    @abstractmethod
    def get_cash(self) -> float: ...
    @abstractmethod
    def get_nav(self) -> float: ...


class SimBroker(Broker):
    """模拟柜台: 封装 风控+撮合+费用+记账。回测与 paper trading 共用。"""

    def __init__(self, config: dict):
        self.account = SimAccount(config["initial_cash"])
        self.fee_model = FeeModel()
        self.config = config

    def submit_orders(self, orders):
        check = pre_trade_risk_check(orders, self.account, self.get_nav(), self.config)
        log_risk_audit(check)
        results = []
        for o in orders:
            if o["ticker"] in check.blocked_by or o.get("skip"):
                results.append({**o, "status": "REJECTED"})
                continue
            price = get_latest_price(o["ticker"])
            if price is None:
                results.append({**o, "status": "REJECTED"})
                continue
            fees = self.fee_model.compute(o["side"], price, o["shares"], o["ticker"])
            self.account.execute_fill(o, price, fees)
            results.append({**o, "status": "FILLED", "fill_price": price})
        return results
```

**V2 实盘路径**：**QMT > easytrader > 券商 API**。QMT（迅投）Python API 完善、支持 mini-qmt 本地部署；easytrader 是逆向客户端稳定性差；自研券商柜台协议不做。**不做 vnpy Gateway**——引入整个 vnpy 生态不值（修正 §22.5/§24.4 早期结论，见 §68）。

```python
def create_broker(mode: str, config: dict) -> Broker:
    """工厂: sim / qmt 切换——回测与实盘同一代码路径。"""
    if mode == "sim":
        return SimBroker(config)
    if mode == "qmt":
        return QmtBroker(config)   # V2
    raise ValueError(mode)
```

### 66.6 全链路回测主循环

```python
def run_backtest(alpha: dict, start: str, end: str) -> dict:
    """
    回测主循环。全执行域代码 <500 行。
    """
    broker = create_broker("sim", alpha["config"])
    prev_weights = None
    for date in rebalance_dates(start, end, alpha["tags"]["holding_period"]):
        signal = build_alpha_signal(alpha, date)              # §63/§64
        weights = build_portfolio(signal, alpha, prev_weights)  # §65.7
        orders = generate_orders(weights, broker.account, broker.get_nav())  # §66.2
        broker.submit_orders(orders)                           # §66.3+§66.4
        record_nav(date, broker.get_nav(get_prices(date)))
        prev_weights = weights
    return summarize_performance(nav_history)                  # §63.6 指标
```

**代码量预算**：compute_order_diff/generate_orders <100 行 + 风控 <80 行 + FeeModel/SimAccount <100 行 + Broker <50 行 + 主循环 <70 行 ≈ **<500 行**。对照 backtrader 学习成本（文档+API 摸索数天）——自研是正收益。

### 66.7 决策汇总（18 行）与验证（4 条）

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | 订单生成 | 目标权重 diff → 订单列表 | OnAlphaUpdate→OrderSchedule 链路的简化 |
| 2 | 手数取整 | floor(w×nav/price/100)×100 | §17.3/§34.4 |
| 3 | 挂单 | V1 不处理 | 月频不需要 |
| 4 | 拆单 | 不做，check_split_needed 仅告警 | 教程 §17.5 >1% 日成交量 |
| 5 | 执行优先级 | 先卖后买；卖按市值 desc；买按信号 desc | 教程 §17 |
| 6 | 订单类型 | LIMIT limit_price=close | 简单诚实 |
| 7 | 框架 | pandas 自研 <300 行 | §25.2 能用 pandas 别上框架 |
| 8 | 风控时机 | 只 pre-trade 不 intra-trade | 月频无盘中需求 |
| 9 | 控制态 | PASS/BLOCK 两级不做"约束" | 公司三态是多策略仲裁需求 |
| 10 | 风控规则 | 七规则表 | 教程 §9 + 防操作错误 |
| 11 | 换手/流动性 | 告警不拦截 | 硬拦破坏信号 |
| 12 | 风控审计 | risk_audit.jsonl append-only | 可追溯 |
| 13 | 撮合 | 收盘价成交 V1；无价 REJECTED | §18 六道关的月频简化 |
| 14 | 费用 | 佣金万 2.5+印花税万 5 卖出+过户万 0.1+滑点 0.1% | 教程 §3.2 |
| 15 | ETF 优惠 | 免印花税（51/15/16 前缀） | §13.4 ASSET_RULES 一致 |
| 16 | 账本 | SimAccount（cash/holdings/cost_basis/trade_log） | hubble_tb 简化 |
| 17 | 柜台抽象 | Broker 4 方法；SimBroker/QmtBroker 工厂 | 不做 vnpy Gateway |
| 18 | 多品种 | V1 仅 A 股，不预先抽象 | 期货保证金是另一套账本 |

**验证 4 条**：

| 验证 | 方法 | 标准 |
|---|---|---|
| 黄金样本基线 | 等权 top-5 ETF 手算对拍 | 偏差 < 0.01%（§12.6 五步验证） |
| 费用模型 | 买卖各一笔手算 | 与 FeeModel.compute 一致到分 |
| 风控拦截 | 构造超限订单 | 7 条规则逐条触发 |
| 订单差分 | 已知持仓+目标权重 | 手算 shares 对拍 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§10.1** | **升级为全解 → §23 / §24.1 / §66**（差分/查单/拆单从"仍开放"→落定） |
| **§10.2** | **升级为全解 → §24.6 / §66**（七规则完整集 + PASS/BLOCK 落定） |
| **§10.3** | **升级为全解 → §18 / §23 / §66**（费率表四件套 + 撮合口径落定，"费率表与流动性阈值仍开放"关闭） |
| **§10.4** | **升级为全解 → §22.5 / §24.4 / §66**（**修正早期"只用 vnpy gateway"结论**——改 pandas 自研 SimBroker + V2 QmtBroker；见 §68） |
| **§17.3/§34.4 手数取整** | 落到 compute_order_diff 的 floor 公式 |
| **§13.4 ASSET_RULES** | FeeModel._is_etf 与之对齐（51/15/16 前缀免印花税） |
| **§65.7 build_portfolio** | 主循环第 3 步衔接 |
| **data/execution/** | risk_audit.jsonl + trade_log 落盘位置 |

**本节仍未解决**：get_latest_price/get_prices 的数据接口签名（从 bars 读当日 close？）；SimAccount.cost_basis 的用途（持仓成本只用于报告还是影响卖出费用）；QmtBroker 的方法签名（V2 再定）；check_split_needed 的成交量数据来源（当日成交量从 bars 取）。

---

## 67. 运维域：调度 / 监控 / 归因 / 告警（§11 全解）

> §11.1（任务调度）+ §11.2（监控指标）+ §11.3（归因方法）+ §11.4（告警）四问合并回答。核心结论：**APScheduler + 薄封装 V1**（不用 Prefect/Celery/Airflow）、**5 类 Job + job_runs.jsonl**、**策略级 8 指标 + 系统级 4 指标 + Markdown 日报**（不做 Web）、**Alpha-Beta 单因子归因 V1**（Brinson/Barra 不做）、**告警 3 类 8 条 + 终端/桌面通知通道**。对标公司 Datamall 全家桶（Dolphin→Kafka→TriggerRouter→Celery→Ray）的单人极简版。与 §65.6（signal 调度）、§62.4/§64.7（衰减监控）衔接。**新增代码 ≈220 行**。

### 67.1 Context 与 Confluence 参考

前五域（数据/研究/信号/执行/治理）已收口。本节是**让这一切每天自动跑起来并可被观测**的运维层：调度（何时跑什么）、监控（跑得怎么样）、归因（收益从哪来）、告警（出事怎么知道）。单人运维 <10 策略月频——一切从简，但四块缺一不可。

**Confluence 参考**：

```text
- Datamall 调度 (page 316050560): 公司全链路 Dolphin(调度) → Kafka(事件)
  → TriggerRouter(路由) → Celery(执行) → Ray(计算)——多团队生产体系
- 驾驶舱 (page 342919374): 4 类异常分类 + severity 只升不降 + 白名单——
  公司告警治理原则
- Udata 5 色灯 (page 223886928): BLACK/BLUE/YELLOW/AMBER/RED 生产状态
- 量化学习教程 §18.6 归因 (page 301832392): Alpha-Beta 分解实战解读
- 量化学习教程 §18.3-18.4 衰减退出: rolling IC/夏普/t 三指标
- 量化学习教程 §16.4 回测实盘 gap: 回测实盘偏差监控
- dw_asset_job_instance: 公司数仓作业实例表——job_runs.jsonl 的对标
```

### 67.2 任务调度（§11.1）：APScheduler + 薄封装

**5 类 Job 表**：

| Job | 频率 | 时机 | 失败处理 |
|---|---|---|---|
| 数据采集（fetch_stock_daily 等） | 日频 | 16:00 | 重试 2 次→跳过下游 |
| 因子计算（compute_all） | 日频 | 16:30 | 重试 1 次→跳过下游 |
| 信号产出（run_daily_signal） | 日频/月频 | 16:45 | 重试 1 次→**hold**（§65.6） |
| 回测 | 手动 | — | — |
| 监控巡检（health_check） | 日频 | 17:00 | 重试 1 次→告警 |

**调度器选型**：

| 候选 | 结论 | 理由 |
|---|---|---|
| **APScheduler + 薄封装** | **V1 默认** | 进程内、零部署、CronTrigger 直观 |
| crontab | 不选 | 太原始——无失败重试/无状态记录 |
| Prefect / Celery / Airflow | 不选 | 服务化部署违反零运维；Job<10 用不上 DAG |

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


def daily_pipeline():
    """日频流水线: 数据→因子→信号→巡检。"""
    run_job("fetch_stock_daily", retries=2)
    run_job("compute_all", retries=1)
    run_job("run_daily_signal", retries=1, on_failure="hold")   # §65.6
    run_job("health_check", retries=1)


scheduler = BlockingScheduler()
scheduler.add_job(
    daily_pipeline,
    CronTrigger(hour=16, minute=45, day_of_week="mon-fri"),
    id="daily_pipeline",
)
```

**不用 DAG**：Job<10 依赖线性（数据→因子→信号→巡检），for 顺序执行即可；V2 Job>20 再迁 Prefect。

**运行记录 job_runs.jsonl**（对标 dw_asset_job_instance）：

```json
{"run_id": "20261004_compute_all_001", "job_name": "compute_all",
 "started_at": "...", "finished_at": "...", "status": "SUCCESS",
 "duration_s": 42.3, "error": null, "retry_count": 0,
 "output_summary": "45 factors, 2 failed"}
```

status 四态：`SUCCESS | FAILED | RETRY | SKIPPED`。

```python
def run_job(name: str, fn=None, retries: int = 1, timeout_s: int = 300,
            on_failure: str = "skip") -> dict:
    """带重试的 Job 执行。默认重试 1 次超时 300s。"""
    for attempt in range(retries + 1):
        try:
            result = run_with_timeout(fn or JOB_REGISTRY[name], timeout_s)
            return log_job_run(name, "SUCCESS", result, attempt)
        except Exception as e:
            if attempt < retries:
                continue
            log_job_run(name, "FAILED", error=str(e), retry_count=attempt)
            if on_failure == "hold":
                hold_previous_portfolio(name)   # §65.6 不清仓
            else:
                mark_downstream_skipped(name)   # 数据失败→跳过后续
            send_alert("HIGH", f"job {name} failed: {e}")
            return {"status": "FAILED"}
```

失败传导规则：**数据失败 → 跳过下游**（无数据算不了）；**因子/信号失败 → hold 上日 Portfolio**（§65.6 不清仓铁律）。

### 67.3 监控指标体系（§11.2）：8+4 指标 + Markdown 日报

**策略级 8 指标**：

| 指标 | 健康 | 警告 | 退出 | 来源 |
|---|---|---|---|---|
| 累计收益 | > 0 | < 0 持续 6 月 | — | nav 历史 |
| rolling 6 月夏普 | > 0.5 | < 0.3 | < 0 | §62.4 |
| 最大回撤 | < 回测 MDD | > 回测×1.2 | > 回测×1.5 | 对比 eval_report |
| 超额收益 | > 0 | < 0 持续 6 月 | < 0 持续 1 年 | vs 基准 |
| rolling IC | > 70% 历史均值 | < 50% | < 30% | §64.7 |
| 持仓数 | 与设计一致 | 偏差 > 20% | — | snapshot |
| 换手率 | 30–100% | > 50% 超设计 | — | compute_turnover |
| 单笔成本 | < 0.4% | > 0.5% | — | FeeModel 统计 |

**系统级 4 指标**：

| 指标 | 阈值 | 级别 |
|---|---|---|
| Job 成功率（rolling 7 天） | < 80% | HIGH |
| Pipeline 总耗时 | > 30 min | MEDIUM |
| 数据新鲜度 | 最新交易日 > 1 天前 | HIGH |
| 数据条数偏差 | vs 历史均值 ±30% | HIGH |

**展示**：`data/ops/reports/daily_{date}.md` **Markdown 日报**（指标表 + 异常列表 + nav 曲线 png）。**不做 Web 看板**——V2 Streamlit 需要时再加。

```python
def write_daily_report(date: str, metrics: dict, alerts: list) -> str:
    """Markdown 日报落盘 data/ops/reports/daily_{date}.md。"""
    lines = [f"# Ops Daily Report {date}", "",
             "## 策略指标", "| 指标 | 值 | 状态 |", "|---|---|---|"]
    for k, v in metrics["strategy"].items():
        lines.append(f"| {k} | {v['value']} | {v['status']} |")
    lines += ["", "## 系统指标", ...]
    lines += ["", "## 告警", *[f"- [{a['level']}] {a['msg']}" for a in alerts]]
    path = f"data/ops/reports/daily_{date}.md"
    Path(path).write_text("\n".join(lines), encoding="utf-8")
    return path
```

指标采样写 `data/ops/metrics.jsonl`（时点指标序列，供告警与趋势用）。

### 67.4 归因方法（§11.3）：Alpha-Beta V1

**归因模型选型表**：

| 模型 | 结论 | 理由 |
|---|---|---|
| **单因子 Alpha-Beta 回归** | **V1 默认** | 教程 §18.6 散户够用；回答"赚的是 alpha 还是 beta" |
| FF3（市场+规模+价值） | 备选（季度手动） | 数据便宜，加两个因子更细 |
| Brinson（配置+选择） | **不做** | 需要基准组合与行业配置对照——机构组合管理需求 |
| Barra 风险模型 | **不做** | §63.4 同理由 |
| 因子贡献拆解 | **做** | §63.6 factor_contribution() 复用 |

```python
from scipy.stats import linregress


def alpha_beta_attribution(
    portfolio_returns: pd.Series,
    benchmark_returns: pd.Series,
) -> dict:
    """
    单因子 Alpha-Beta 回归(教程 §18.6)。
    r_p = α + β·r_b + ε
    """
    lr = linregress(benchmark_returns.values, portfolio_returns.values)
    return {
        "alpha_monthly": round(lr.intercept, 4),
        "alpha_annual": round(lr.intercept * 12, 4),   # 月频×12
        "beta": round(lr.slope, 3),
        "r_squared": round(lr.rvalue ** 2, 3),
        "t_alpha": round(lr.intercept / lr.stderr, 2) if lr.stderr else None,
    }
```

**归因频率表**：

| 归因 | 频率 | 方式 |
|---|---|---|
| Alpha-Beta | 月频 | 自动（daily_pipeline 月末 Job） |
| 因子贡献拆解 | 月频 | 自动 |
| FF3 | 季度 | 手动跑 notebook |
| 专项归因 | 按需 | 手动（大回撤事件复盘） |

**归因报告解读三线**（教程 §18.6）：

```text
Alpha 年化 3-5%：优秀散户水平
R² > 0.7：beta 解释力强，alpha 可信；R² < 0.5：超额波动大，样本不足
Alpha > 8%：查回测问题（未来函数/费用遗漏）——太高不真实
```

归因报告落盘 `data/ops/reports/attribution_{alpha_type_id}_{quarter}.md`。

### 67.5 告警（§11.4）：3 类 8 条 + 通道分层

**告警规则 3 类**：阈值（指标超线）、突变（单日剧变）、趋势（持续恶化）。

**8 条具体条件表**（对标驾驶舱 4 类异常分类 page 342919374）：

| # | 条件 | 级别 | 类型 |
|---|---|---|---|
| 1 | Job 失败（重试后仍 FAILED） | HIGH | 阈值 |
| 2 | 数据延迟 > 1 天 | HIGH | 阈值 |
| 3 | MDD 突破回测×1.5 | **CRITICAL** | 突变 |
| 4 | rolling 6 月夏普转负 | **CRITICAL** | 趋势 |
| 5 | 超额持续为负 6 月 | HIGH | 趋势 |
| 6 | NAV 单日 < -3% | MEDIUM | 突变 |
| 7 | 换手 > 50% 超设计 | MEDIUM | 阈值 |
| 8 | 风控拦截单日 > 3 笔 | MEDIUM | 阈值 |

**通道表**：

| 通道 | 级别 | V1/V2 |
|---|---|---|
| 终端 print/log | 全部 | **V1** |
| 桌面通知（plyer / notify-send） | CRITICAL | **V1** |
| 邮件 | HIGH+ | V2 |
| 飞书/钉钉 | — | 不做 |
| Grafana | — | 不做（V2 Streamlit 已覆盖） |

```python
class AlertLevel(Enum):
    CRITICAL = "CRITICAL"   # 桌面通知
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"


def send_alert(level: str, msg: str, alert_type: str = "generic",
               alpha_type_id: str = "") -> None:
    """告警发送+落盘。去重 key = {date}_{alert_type}_{alpha_type_id}。"""
    dedup_key = f"{date.today():%Y%m%d}_{alert_type}_{alpha_type_id}"
    if dedup_key in _alerted_today:
        return                      # 同日同类型不重复告警
    _alerted_today.add(dedup_key)
    log_alert({"level": level, "msg": msg, "key": dedup_key})
    print(f"[{level}] {msg}")
    if level == "CRITICAL":
        desktop_notify(msg)         # plyer / notify-send
```

**告警治理原则**（驾驶舱三条借鉴）：severity **只升不降**（同一天同一问题级别单调升）；去重 key 防轰炸；白名单机制（维护期 Job 静默）。**不做动态升级**（MEDIUM 自动升 HIGH——误报放大器）。

### 67.6 全链路与决策汇总

**daily_pipeline 全链路树**：

```text
16:45 daily_pipeline
├── 16:45 fetch_stock_daily     (retries=2, 失败→跳过下游)
├── 16:50 compute_all           (retries=1, 失败→跳过下游)
│   └── FactorDAG 层内并行(§59)
├── 16:55 run_daily_signal      (retries=1, 失败→hold 上日 Portfolio)
│   └── per alphaType: build_alpha_signal → build_portfolio → generate_orders
├── 17:00 health_check          (retries=1, 异常→告警)
└── 17:05 write_daily_report    (Markdown 日报)
月末追加: alpha_beta_attribution + factor_contribution (自动)
```

**决策汇总（18 行）**：

| # | 决策 | 结论 | Confluence 参考 |
|---|---|---|---|
| 1 | Job 分类 | 5 类（数据/因子/信号/回测手动/巡检） | dw_asset_job 对标 |
| 2 | 调度器 | **APScheduler + 薄封装 V1** | 不用 Prefect/Celery/Airflow |
| 3 | 触发 | CronTrigger 16:45 周中 + 手动 | — |
| 4 | DAG | 不用（Job<10 线性）；V2>20 迁 Prefect | — |
| 5 | 运行记录 | job_runs.jsonl 四态 SUCCESS/FAILED/RETRY/SKIPPED | dw_asset_job_instance |
| 6 | 重试 | 默认 1 次 300s；数据 2 次 | — |
| 7 | 失败传导 | 数据→跳过下游；信号→hold 不清仓 | §65.6 铁律 |
| 8 | 策略指标 | 8 指标表（三档健康/警告/退出） | §18.3/§16.4 |
| 9 | 系统指标 | 4 指标（Job 成功率/耗时/新鲜度/条数） | 驾驶舱 |
| 10 | 展示 | Markdown 日报不做 Web | V2 Streamlit |
| 11 | 指标存储 | metrics.jsonl | — |
| 12 | 归因模型 | Alpha-Beta V1 / FF3 备选 / Brinson+Barra 不做 | §18.6 |
| 13 | 因子贡献 | factor_contribution() 复用 §63.6 | — |
| 14 | 归因频率 | Alpha-Beta+贡献月自动 / FF3 季手动 / 专项按需 | — |
| 15 | 归因解读 | Alpha 3-5% 优秀 / R²>0.7 可信 / Alpha>8% 查问题 | §18.6 |
| 16 | 告警条件 | 3 类 8 条（CRITICAL 2 / HIGH 3 / MEDIUM 3） | 驾驶舱 4 类异常 |
| 17 | 告警通道 | 终端全级别 V1 + 桌面通知 CRITICAL；邮件 V2 | — |
| 18 | 告警治理 | 去重 key + severity 只升不降 + 白名单；不做动态升级 | 驾驶舱三原则 |

**验证 5 条**：

| 验证 | 方法 | 标准 |
|---|---|---|
| 调度 | 手动触发 daily_pipeline | 5 Job 顺序执行、job_runs.jsonl 记录完整 |
| 失败传导 | 人为制造数据 Job 失败 | 下游 SKIPPED 且告警 HIGH |
| 归因 | 构造 r_p = 0.01 + 0.8·r_b + noise | α≈0.12 年化 / β≈0.8（容差 0.02） |
| 告警去重 | 同日同类型触发两次 | 仅一条落盘 |
| 日报 | 跑完 pipeline 检查 daily_{date}.md | 指标表/告警列表/链接齐全 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§11.1** | **已定 → §67**（APScheduler + 5 类 Job + job_runs.jsonl） |
| **§11.2** | **已定 → §67**（8+4 指标 + Markdown 日报） |
| **§11.3** | **已定 → §67**（Alpha-Beta V1 + 归因报告解读三线） |
| **§11.4** | **已定 → §67**（3 类 8 条 + 通道分层 + 告警治理三原则） |
| **§65.6 run_daily_signal** | 是 daily_pipeline 的一个 Job（16:45），失败 hold 复用 |
| **§62.4/§64.7 衰减监控** | 策略级指标表与之同一阈值体系（rolling IC 70/50/30%、夏普 0.5/0.3/0） |
| **§63.6 factor_contribution** | 归因直接复用 |
| **data/ops/** | reports/daily_{date}.md + metrics.jsonl + alerts.jsonl 落盘位置定稿 |

**本节仍未解决**：APScheduler 与手动触发的互斥（跑一半手动再跑会不会双写 job_runs）；metrics.jsonl 的 schema 字段定稿；归因基准的选择（沪深 300 vs 中证 500 vs 组合基准配置在哪）；desktop_notify 对 macOS 的具体实现（osascript 还是 plyer）。

---

## 68. 受转响影响需重审的既有决策（§12 全解）

> §12 的六条旧决策逐条重审：ARCHITECTURE.md 的"重引擎"设计（四层时间模型 / EBGraph 事件图 / 五域 kernel 引擎 / Signal 三层契约 / ClickHouse 存储 / 期货黄金样本）vs §3–§11 实际收敛出的"薄壳 + pandas 优先"。结论：**4 条废弃、3 条简化保留、3 条原样保留**；对 §3–§11 的落定内容**零影响**（重审是让 ARCHITECTURE.md 对齐现实，不是推翻重来）。本节同时关闭 P0 尾项 §12.4。**新增代码 ≈40 行**（黄金样本脚本骨架）。

### 68.1 Context 与 Confluence 参考

§3–§11 从需求出发收敛出了一套**薄壳架构**（pandas 自研、JSON 注册表、for-loop 调度、Parquet+DuckDB）。而项目早期的 ARCHITECTURE.md 与 baseline_spec.py 记录的是另一套**重引擎**设想（C++ EventBrick 移植、四层时间模型、ClickHouse、事件驱动）。两套并存会造成实施时无所适从——本节逐条裁决。

**Confluence 参考**：

```text
- 公司 EventBrick / starfleet / EBReplay / EBClock / EBTimer:
  C++ 事件驱动回放引擎的四层时间模型——tick 级实盘/回测对齐需求
- 公司 EBGraph / EventBrick 图: 事件订阅图,节点动态 push 触发
- 公司 Udata ClickHouse (page 288524848): Alpha 入库供实盘多策略并发查询
- 量化学习教程 §2 (page 301832392): 双重动量 ETF 策略——黄金样本出处
- 量化学习教程 §16.4: 回测合理区间(年化 10-15%/夏普 0.8-1.2/MDD 15-25%)
- 量化学习教程 §25.2: "能用 pandas 别上框架"
- strategies/futures/alstm/baseline_spec.py: 旧黄金样本
  (sharpe 16.001135 / total_return 0.860638 / trade_count 12686)
```

### 68.2 §12.1 四层时间模型：**废弃** → for-loop 时间推进

**旧设计**：starfleet → EBReplay → EBClock → EBTimer 四层时间模型（事件驱动回放，时间推进由事件触发）。

**裁决：废弃**。个人平台月频/日频，for-loop 逐再平衡日推进即可。

| 四层模型要解决的 | 个人平台是否需要 | 替代 |
|---|---|---|
| tick 级时间对齐 | 否（月频） | — |
| 事件触发时间推进 | 否 | `for date in rebalance_dates()` |
| 回放/实盘同构 | 部分 | SimBroker/QmtBroker 同一代码路径（§66.5） |
| 计算量与数据量解耦 | 否 | 月频全 A <100ms，天然消失 |
| 时钟可暂停/单步调试 | 否 | print/断点足够 |

"计算量与数据量解耦"在月频下**天然消失**——数据量（5000 股 × 250 天）本身就是一次性批量算完的规模。backtrader/vnpy 事件驱动框架同样不采用（§66.2 同结论）。

### 68.3 §12.2 EBGraph / EventBrick：**废弃** → Factor ABC + FactorDAG

| 维度 | 事件订阅图（EBGraph） | 拓扑 DAG（FactorDAG） |
|---|---|---|
| 触发方式 | 动态 push（事件到达触发下游） | 静态 pull（拓扑序批量算） |
| 依赖表达 | 运行时订阅关系 | depends_on() 声明式 |
| 批量性 | 逐事件 | DataFrame 天然批量 |
| 调试 | 事件流回放 | 直接看中间 DataFrame |

**三理由**：月频无实时事件流；因子依赖静态（动量必然依赖 close，不运行时变）；DataFrame 向量化天然批量。因子组织链已定（§57–§63）：

```text
Factor(compute) → FactorDAG(拓扑) → PostProcessor → per-factor-one-table
  → FactorCombiner → AlphaSignal   =  "DataFrame 流水线"
```

事件驱动思想**不照搬抽象**（§20.3/§20.4 已定"思想保留"）——保留的是"依赖图"思想，丢掉的是"事件订阅"机制。

### 68.4 §12.3 五域骨架：**修正保留** kernel/ → core/

**目录职责重分布**：

| 旧目录 | 新目录 | 旧定位 | 新定位 |
|---|---|---|---|
| kernel/ | **core/** | "引擎"（事件循环/时间模型/调度） | **共享工具**（config.py / registry.py / logging.py / types.py） |
| data_platform/ | data/ | 数据平台 | 数据接入与存储 |
| research/ | research/ | 研究 | 因子研究与评估 |
| execution/ | execution/ | 执行 | 订单/风控/账本/柜台（§66） |
| ops/ | ops/ | 运维 | 调度/监控/归因/告警（§67） |

**边界不重画**——五域划分仍成立，只是 kernel 的"引擎"内涵被抽空：

| 旧 kernel 职责 | 去向 |
|---|---|
| 时间推进 | 回测脚本 for-loop（§68.2） |
| 事件分发 | 不需要（§68.3） |
| Signal 类 | AlphaSignal（§63.5） |
| 数据接口 | DataAPI / load_* 函数（§61） |

core/ 里剩下的是**真正的共享工具**：config.py（§13.1）、registry.py（FactorRegistry/RuleRegistry）、logging.py（§13.2）、types.py（公共 dataclass）、asset_rules.py（§13.4）。**无引擎、无事件循环、无时间模型**。

### 68.5 §12.4 产品契约：**简化** 五层叙事 → 三步（P0 尾项关闭）

**旧契约**：Signal 三层（BlueShift / USignal / Tick）+ Portfolio + Utrade + Themis + TB 五层产品叙事。

**裁决**：

| 旧契约 | 裁决 | 替代 |
|---|---|---|
| Signal 三层 | **坍缩为一层** | AlphaSignal DataFrame（§63.5） |
| — BlueShift 层 | 不需要 | 月频无跨周期信号聚合 |
| — USignal 层 | 不需要 | 截面分数直接是 AlphaSignal |
| — Tick 层 | 不需要 | 盘中实时是公司需求 |
| Portfolio 层 | **自研** | build_portfolio（§65.7） |
| Utrade（订单） | **坍缩** | generate_orders + broker.submit_orders() 内部（§66.2） |
| Themis（风控） | **坍缩** | pre_trade_risk_check 在 submit_orders 内（§66.3） |
| TB（模拟账本） | **坍缩** | SimAccount 在 SimBroker 内（§66.4） |

**五层叙事 → 三步**：

```text
旧: Signal → Portfolio → Utrade → Themis → TB
新: Signal → Portfolio → Execution
                (broker.submit_orders() 内部封装 风控+撮合+记账)
```

Utrade/Themis/TB 不再是三个产品层，而是 `submit_orders()` 的内部实现细节——**接口边界从"产品"移到"函数"**。这就是 P0 问题"Utrade/Themis/TB 对应物"的答案：**对应物是 SimBroker 的三个内部步骤，不是三个模块**。

### 68.6 §12.5 存储选型：**修正** 砍掉 ClickHouse

| 旧选型 | 裁决 | 理由 |
|---|---|---|
| Parquet | **保留** | §5/§61 已定 |
| DuckDB | **保留** | 查询引擎（§61.8） |
| ClickHouse | **砍掉** | 单机单用户 <10GB；独立部署违反零外部依赖；公司 CK 是实盘多策略并发查询需求（page 288524848），个人消费方=本地脚本 |

**框架存储偏好表**：

| 框架 | 存储偏好 | 个人平台 |
|---|---|---|
| alphalens | DataFrame / Parquet | **直接兼容**（§27.8） |
| VectorBT | DataFrame | 兼容（V2 备选引擎） |
| backtrader / vnpy | 自有数据格式 | 不用（§66.2） |
| Qlib | 二进制 | 不用（§4） |

### 68.7 §12.6 黄金样本基线：**废弃重建**

**旧基线不适用**（strategies/futures/alstm/baseline_spec.py）：

| 旧基线 | 问题 |
|---|---|
| sharpe 16.001135 | **不可信**——过拟合或口径问题（年化夏普 16 不可能） |
| total_return 0.860638 | 样本期/口径不明 |
| trade_count 12686 | 暗示高频——与 V1 月频目标不符 |
| 标的 | 期货 ALSTM vs V1 主战场是 A 股 |
| 引擎 | VectorBT/ZVT vs 新基线用 pandas 自研 |

**新基线 = 等权 top-5 ETF 动量策略**（教程 §2 双重动量变体，≤30 行，akshare 可重现）：

```python
def golden_baseline(start="20200101", end="20241231") -> dict:
    """
    黄金样本基线: 等权 top-5 ETF 动量(教程 §2 变体)。
    ≤30 行,akshare 可完整重现——任何环境跑出同一组数字。
    """
    prices = load_etf_prices(["510300", "510500", "518880", "511010", "159915", ...],
                             start, end)                     # 教程 §2 标的池
    momentum = prices.pct_change(20)                          # 20 日动量
    for month_end in month_ends(start, end):
        top5 = momentum.loc[month_end].nlargest(5).index
        weights = pd.Series(1 / 5, index=top5)               # 等权
        execute_rebalance(month_end, weights)                  # SimBroker
    return summarize(nav_history)
```

**五步验证法**：

| 步骤 | 内容 | 容差 |
|---|---|---|
| 1 | 手算 Excel 对拍（月末调仓 3 个月） | 4 位小数一致 |
| 2 | pandas 实现 vs Excel | < 0.01% |
| 3 | SimBroker 全链路 vs pandas | < 0.01% |
| 4 | 扣费前后对比 | 费用与 FeeModel 手算一致 |
| 5 | 2020–2024 五年完整基线 | 落盘 metrics 做回归基准 |

**预期范围**（教程 §16.4）：年化 10–15% / 夏普 0.8–1.2 / MDD 15–25%。**超范围排查表**：

| 症状 | 排查方向 |
|---|---|
| 年化 > 20% | 查未来函数（shift(1)/成交价基准） |
| 夏普 > 2 | 查前视（权重用了当期信息） |
| MDD < 10% | 查是否漏了极端行情段（2022/2024） |
| 费用 < 1% | 查 FeeModel 是否漏计（印花税卖出/最低 5 元） |

旧 baseline_spec.py **保留不删**（期货 ALSTM 策略自身的回归基准仍用），但**不再是平台级黄金样本**——两者不混用。

### 68.8 决策汇总（10 行）与验证

| # | 旧决策 | 裁决 | 替代 |
|---|---|---|---|
| 1 | 四层时间模型 | **废弃** | for-loop 时间推进 |
| 2 | EBGraph/EventBrick | **废弃** | Factor ABC + FactorDAG |
| 3 | 五域骨架 | **修正保留** | kernel→core（共享工具） |
| 4 | Signal 三层契约 | **简化** | 一层 AlphaSignal |
| 5 | Utrade/Themis/TB 三层 | **简化** | broker.submit_orders() 内部三步 |
| 6 | ClickHouse | **废弃** | Parquet + DuckDB 双件套 |
| 7 | 期货黄金样本 | **废弃重建** | 等权 top-5 ETF 动量 |
| 8 | Portfolio 自研 | **保留** | build_portfolio |
| 9 | 目录五域划分 | **保留** | 边界不重画 |
| 10 | 对拍验证方法 | **保留** | 五步验证法 |

**对 §3–§11 的影响：零**——本节是让 ARCHITECTURE.md 对齐 §3–§11 已落定的现实，所有实现级决策（§57–§67）原样有效。唯一需要同步的是文档与目录名（kernel→core）。

**验证 3 条**：

| 验证 | 方法 | 标准 |
|---|---|---|
| ARCHITECTURE.md 核对 | 逐条对照 §68.8 裁决表 | 无"四层时间模型/EBGraph/ClickHouse/三层 Signal"残留表述 |
| 五步基线 | 跑通 1–5 步 | 数字落在 §16.4 区间且容差达标 |
| 目录容纳 | core/data/research/execution/ops 五目录 | §57–§67 全部模块有归属 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§12.1** | **已定 → §20.2 / §68**（两层时间模型之上再明确：四层时间模型**废弃**，for-loop 推进；五项不需要表落定） |
| **§12.2** | **已定 → §20.3 / §20.4 / §68**（EBGraph **废弃**→FactorDAG；三理由 + DataFrame 流水线组织链落定） |
| **§12.3** | **已定 → §68**（kernel→core 修正保留；职责分布表；边界不重画） |
| **§12.4** | **已定 → §32 / §34 / §68**（**P0 关闭**——Signal 三层坍缩为 AlphaSignal 一层；Utrade/Themis/TB 坍缩为 broker.submit_orders() 内部三步；五层叙事→三步） |
| **§12.5** | **升级为全解 → §20.1 / §68**（ClickHouse 砍掉二次确认 + 框架存储偏好表） |
| **§12.6** | **已定 → §20.5 / §68**（黄金样本**废弃重建**为等权 top-5 ETF 动量 + 五步验证 + 超范围排查表） |
| **ARCHITECTURE.md** | 需同步改写（本节是其新蓝本） |
| **baseline_spec.py** | 保留作期货 ALSTM 回归基准，不再是平台级黄金样本 |

**本节仍未解决**：黄金样本 ETF 标的池定稿（教程 §2 具体清单 vs 510300/510500/518880/511010/159915 自选池）；core/ 各文件的最小内容边界（registry.py 放几个 Registry）；ARCHITECTURE.md 改写的执行时机（本节落定后手动改还是后续批一起）。

---

## 69. 横切问题：配置 / 日志 / 测试 / 多资产（§13 全解）

> §13.1（配置管理）+ §13.2（日志与可观测性）+ §13.3（测试策略）+ §13.4（多资产区分）四问合并回答。核心结论：**配置 = TOML 三层 + .env + git 版本化**（修正 §35.6 的"YAML 一个文件"）、**日志 = Python logging + JSON Lines + JSONL 运行记录**（不需要分布式追踪）、**测试 = pytest 三层 + hypothesis 性质测试 + 黄金样本 fixture**（V1 无 CI）、**多资产 = 配置区分 + ASSET_RULES 规则表 + execution 层处理差异**（不学 C++ Traits 编译期多态）。对标 hubble_utrade DataTraits/ParseConfig、Datamall trigger_history/PipelineConfigWatchTask、教程 §15 A 股特殊机制。**新增代码 ≈120 行**。

### 69.1 Context 与 Confluence 参考

四类横跨问题（配置 / 日志可观测性 / 测试 / 多资产）散落在各域里，不统一定会风格不一——配置在 §35.6 已有一版、日志散在 §67 告警、测试散在 §68 黄金样本、多资产散在 §66 费用。本节是它们的**统一收口**，并与 §3–§12 已有设计对齐。

**Confluence 参考**：

```text
- hubble_utrade (page 301596677): C++ Traits 编译期多态
  DataTraits<Category,Market,Exchange> + ParseConfig XML + 四类 IStrategy 别名——
  公司多资产区分与配置解析的出处
- Datamall (page 316050560): trigger_history / Prometheus / PipelineConfigWatchTask——
  公司配置监听与可观测性设施（多人多服务需求）
- Datamall NFS 日志 → MinIO 降级: 公司日志存储演进，个人本地 JSONL 零运维
- 量化学习教程 §15 (page 301832392): A 股特殊机制 T+1 / 涨跌停
  主板±10% 创业板±20% 科创板±20% 北交所±30% ST±5% / 印花税 ETF 免 / ST 删除
- 量化学习教程 §11.2 (page 301832392): 期权不推荐散户——V4+ 才做
```

### 69.2 配置管理（§13.1）：TOML 三层 + .env + git

**格式选型表**（回应"YAML / TOML / XML / Python dataclass"）：

| 候选 | 结论 | 理由 |
|---|---|---|
| YAML | **不选** | 缩进敏感；`on`/`yes`/`no` 被解析成布尔的陷阱（`mode: on` → `True`）——配置里全是这类词 |
| **TOML** | **选** | 类型严格无陷阱；注释友好；`tomllib` 标准库零依赖（Python 3.11+，3.10 用 `tomli`） |
| XML | 不选 | 公司用 XML 是 C++ 生态惯例（ParseConfig XML）；Python 平台啰嗦 |
| Python dataclass | 不选做载体 | 代码即配置 → 改参数要改代码、进 git diff 噪音大 |

**修正 §35.6**：此前定"YAML + dataclass、一个文件、不分层"。本节升级为 **TOML + 三层**——理由是敏感信息与策略级参数混在一个 YAML 里既不安全也不清晰；`on→True` 陷阱是实打实的坑。dataclass 保留为**加载后的类型载体**（`tomllib` 解析 → dataclass 校验），只是不再用 Python 文件当配置源。

**三层结构**：

```text
config/
├── config.toml                    # 第 1 层 全局: paths / data / execution / ops
├── strategies/
│   └── {alpha_type}.toml          # 第 2 层 策略级: meta / signal / portfolio / risk
└── .env                           # 第 3 层 敏感: API key / 代理 / 密码
```

**第 1 层 `config/config.toml`**（全局，全平台共用）：

```toml
[paths]
data_dir = "data_lake"
logs_dir = "logs"

[data]
source = "akshare"
adjust = "qfq"

[execution]
mode = "sim"          # "sim" | "qmt"——create_broker 工厂读这里（§66.5）

[ops]
schedule = "45 16 * * 1-5"   # APScheduler CronTrigger（§67.2）
```

**第 2 层 `config/strategies/{alpha_type}.toml`**（每策略一份，对齐 §65 alphaType）：

```toml
[meta]
id = "mom_top5_etf_v1"
tags = { asset = "etf", frequency = "monthly", signal_type = "momentum" }

[signal]
lookback = 20
top_k = 5
combiner_method = "equal_weight"   # §63 FactorCombiner

[portfolio]
max_weight = 0.25
max_total_weight = 0.95

[risk]
max_turnover = 1.5
min_trade_value = 5000
```

**第 3 层 `config/.env`**（敏感信息，**不入 git、不打印**——先例 `.my.prompt.cnf`）：

```text
AKSHARE_PROXY=http://user:pass@host:port
# BROKER_ACCOUNT=...（V2 QMT 接入时）
```

`.gitignore` 必须含 `config/.env`；日志/报错里**禁止**打印其内容（`load_secrets()` 返回的 dict 不进 `config_snapshot`）。

**加载三函数**：

```python
def load_config(path: str = "config/config.toml") -> dict:
    """第 1 层全局配置。tomllib 零依赖。"""
    import tomllib
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_strategy_config(alpha_type_id: str, global_cfg: dict | None = None) -> dict:
    """第 2 层策略配置，策略级覆盖全局: {**global, **strategy}。"""
    import tomllib
    path = f"config/strategies/{alpha_type_id}.toml"
    with open(path, "rb") as f:
        strategy = tomllib.load(f)
    return {**(global_cfg or load_config()), **strategy}


def load_secrets(path: str = "config/.env") -> dict:
    """第 3 层敏感信息。不入 git、不打印、不进 config_snapshot。"""
    secrets = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            secrets[k.strip()] = v.strip()
    return secrets
```

**不做配置中心**：单机 TOML + git 就是配置中心——公司 PipelineConfigWatchTask（配置热更新监听）是多团队多人需求。**审计 = `git log config/`**（改了什么、谁改的、何时改的，全在提交历史里）；运行时留 `config_snapshot`（见 §69.3）保证可复现。**不热更新**沿用 §35.6：改配置 → 重启进程。

### 69.3 日志与可观测性（§13.2）：JSON Lines + 运行记录

**格式 = Python logging + JSON Lines**（人读走 console，机器查走 jsonl）：

```python
import json, logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """日志一行一 JSON: {ts UTC, level, module, func, msg} + extra + exception。"""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "func": record.funcName,
            "msg": record.getMessage(),
        }
        for key, value in getattr(record, "__dict__", {}).items():
            if key not in ("args", "msg", "exc_info", "exc_text"):
                payload.setdefault(key, value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    """console 人读 + logs/app_{YYYYMMDD}.jsonl 机器查。"""
    root = logging.getLogger()
    root.setLevel(level)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    fileh = logging.FileHandler(f"logs/app_{datetime.now():%Y%m%d}.jsonl", encoding="utf-8")
    fileh.setFormatter(JsonFormatter())
    root.addHandler(console)
    root.addHandler(fileh)
```

**级别规范表**：

| 级别 | 何时用 | 例 |
|---|---|---|
| ERROR | 任务失败、数据不可用 | Job 重试后仍 FAILED（§67.2） |
| WARNING | 可继续但需关注 | 风控告警、条数偏差 ±30%（§67.3） |
| INFO | 正常流程节点 | Job 开始/结束、回测进度 |
| DEBUG | 排查用，默认关 | 单标的订单明细 |

**字段规范表**：`ts`(UTC ISO) / `level` / `module` / `func` / `msg` 固定五字段 + extra 按需（`job_name`/`alpha_type_id`/`duration_ms`…）+ `exception`（仅 ERROR 带堆栈）。

**分布式追踪对照表**（结论：全不需要——单进程）：

| 公司设施 | 个人是否需要 | 替代 |
|---|---|---|
| instance 时间戳对齐 | 否（单进程） | — |
| Prometheus 指标 | 否 | `metrics.jsonl`（§67.3） |
| trigger_history 触发链 | 否 | `job_runs.jsonl` 一条一跑（§67.2） |
| 跨服务 trace_id | 否（无跨服务） | `duration_ms` + grep/jq 按 run_id 查 |

**运行记录 `logs/runs/`** 三类 JSONL，各一套 schema：

| 文件 | 记什么 | 关键字段 |
|---|---|---|
| `jobs.jsonl` | Job 运行元数据 | run_id / job_name / started_at / finished_at / status / duration_s / error / retry_count / output_summary（§67.2 定稿） |
| `backtests.jsonl` | 回测运行元数据 | run_id / alpha_type_id / period / metrics(sharpe/mdd/…) / **config_snapshot "path@hash"** |
| `live.jsonl` | 实盘/paper 运行元数据 | run_id / mode("sim"\|"qmt") / orders_count / fills_count / nav |

`config_snapshot = f"{path}@{file_hash}"`——把"这跑用的哪版配置"钉进运行记录，配合 git log 即可完整复现（呼应 §63.5 eval_report.json 的 config_snapshot）。

对标 Datamall 的 NFS 日志 → MinIO 降级：公司日志是多机汇聚需求；**个人本地 JSONL + grep/jq 就是零运维方案**，不上 ELK/Loki。

### 69.4 测试策略（§13.3）：pytest 三层 + 黄金样本

**目录三层**：

```text
tests/
├── unit/         # 单测: 函数级，快
├── integration/  # 集成: 管道级
└── golden/       # 黄金样本: 端到端对拍（衔接 §68.7 五步验证）
```

**单测覆盖表（9 模块）+ hypothesis 性质测试**：

| 模块 | 测什么 | 性质（hypothesis） |
|---|---|---|
| FeeModel | 四件套费率手算对拍 | `fee ≥ 0`；卖出费用 ≥ 买入费用（印花税） |
| winsorize/zscore | 极值/标准化 | `zscore 后 mean≈0, std≈1`（逐截面） |
| build_portfolio | 权重合法 | `weights.sum() ≤ 1 + 1e-9`；无负权重（禁做空） |
| SimAccount | 账本恒等式 | `cash + market_value == nav`（每笔成交后） |
| compute_order_diff | 手数取整 | 结果是 lot_size 整数倍；不超预算 |
| FactorDAG | 拓扑序 | 环检测抛错；依赖先算 |
| load_config | 覆盖合并 | 策略级覆盖全局同名键 |
| run_job | 重试 | 失败 n 次后 status=FAILED |
| ASSET_RULES | 查表 | 未知 asset_type 抛 KeyError |

```python
from hypothesis import given, strategies as st
from engine.execution.fee_model import FeeModel


@given(price=st.floats(1, 1000), shares=st.integers(100, 10000))
def test_fee_non_negative(price: float, shares: int) -> None:
    fee = FeeModel().buy_fee(price, shares)
    assert fee >= 0
    assert fee >= 5.0          # 最低 5 元（§66.4）
```

**集成 3 场景**：①数据管道（fetch→DQC→parquet 落盘）②因子管道（FactorDAG→PostProcessor→per-factor 表）③回测主循环（build_portfolio→generate_orders→SimBroker→nav）。

**测试替身表**：

| 外部依赖 | 替身 |
|---|---|
| akshare 网络 | fixture Parquet（预存 5 ETF × 3 月） |
| DuckDB/磁盘 | 内存 DataFrame / tmp_path |
| QMT 柜台 | SimBroker（本就是 V1 唯一实现） |
| 时钟 | 固定日期参数注入 |

**黄金样本 test**（衔接 §68.7 五步验证的第 2–4 步自动化）：

```python
TOLERANCE = 1e-4

def test_pandas_matches_hand_calc() -> None:
    """pandas 实现 vs golden_expected.json 手算（Excel 4 位小数）。"""
    result = golden_baseline()
    expected = json.loads(Path("tests/golden/golden_expected.json").read_text())
    for key in ("total_return", "sharpe", "mdd"):
        assert abs(result[key] - expected[key]) < TOLERANCE

def test_simbroker_matches_pandas() -> None:
    """SimBroker 全链路 vs 纯 pandas 简化账本。"""
    ...
```

fixtures：`tests/golden/golden_prices.parquet`（5 ETF × 3 月）+ `golden_expected.json`（手算期望值）。

**CI = 无（V1）**：本地 `pytest` 三档耗时预算——unit < 10s / integration < 60s / golden < 5min。V2 再上 GitHub Actions（个人仓库免费额度够用，但 V1 本地跑通先于 CI）。

### 69.5 多资产的区分（§13.4）：ASSET_RULES + execution 层

**公司做法**：hubble_utrade 用 C++ Traits 编译期多态 `DataTraits<Category, Market, Exchange>` + 四类 IStrategy 别名，把资产差异在**编译期**展开。

**个人平台决策**：**配置区分 + 规则表 + execution 层处理差异**——不学 Traits。理由：月频一次调仓 dispatch 开销可忽略，编译期多态是为 tick 级性能服务的；Python 里一个 `ASSET_RULES` dict 查表 + `get_asset_rules()` 即可。

**差异在哪层处理**：

| 差异 | 处理层 | 落点 |
|---|---|---|
| T+1 可卖 | execution | generate_orders 当日可卖判定（§66.1） |
| 涨跌停 / 禁做空 | execution | pre_trade_risk_check（§66.3 风控七规则） |
| 印花税 / 手数 / 佣金 | execution | FeeModel / compute_order_diff（§66.1/§66.4） |
| ST / 停牌 | research | universe 过滤（§57） |
| 板块涨跌停差异 | data | ticker 编码前缀（60/00/30/68/8） |

**关键洞察**：A 股内部的资产差异（股票 vs ETF）几乎全落在 execution/ + data/；research/（因子计算）对资产**透明**——因子只看 DataFrame，不关心标的类型。期货/期权的保证金、逐日盯市是**另一套账本**，所以排 V3/V4 而不是在 V1 预抽象。

**分阶段支持**：

| 阶段 | 资产 | 备注 |
|---|---|---|
| V1 | 股票 + ETF | 主战场；§66 FeeModel._is_etf 已处理印花税差异 |
| V2 | 可转债 | T+0、无印花税——规则表加一行 |
| V3 | 期货 | 保证金 + 逐日盯市——新账本（不复用 SimAccount） |
| V4+ | 期权 | 教程 §11.2 不推荐散户，最后做 |

**规则表 `core/asset_rules.py`**（与 §68.4 core/ 共享工具对齐）：

```python
ASSET_RULES = {
    "stock": {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": True,                      # 卖出征收
        "price_limits": {                       # 涨跌停幅度（教程 §15）
            "main": 0.10,                       # 主板 60/00
            "gem": 0.20,                        # 创业板 30
            "star": 0.20,                       # 科创板 68
            "bse": 0.30,                        # 北交所 8
        },
        "st_limit": 0.05,                       # ST ±5%
        "short": False,                         # 禁做空
    },
    "etf": {
        "t_plus": 1,
        "lot_size": 100,
        "stamp_tax": False,                     # ETF 免印花税
        "price_limits": {"main": 0.10},
        "st_limit": None,
        "short": False,
    },
    # "bond": V2 预留    "futures": V3 预留    "option": V4 预留
}


def get_asset_rules(asset_type: str) -> dict:
    """查表入口。未知类型直接 KeyError——不静默回落。"""
    return ASSET_RULES[asset_type]
```

FeeModel._is_etf（51/15/16 前缀免印花税，§66.4）与 `ASSET_RULES["etf"]["stamp_tax"] = False` **同源一致**——前者按代码前缀自动判，后者按资产类型显式配，两处必须对得上（验证 4 条之一）。

### 69.6 决策汇总与修订

**决策汇总（4 行，一问一行）**：

| # | 问题 | 结论 | 关键点 |
|---|---|---|---|
| 1 | 配置管理 | **TOML 三层 + .env + git** | 修正 §35.6；tomllib 零依赖；不做配置中心；审计=git log；不热更新 |
| 2 | 日志可观测 | **logging + JSONL + runs 三 schema** | JsonFormatter 五字段；不需要分布式追踪；config_snapshot "path@hash" |
| 3 | 测试策略 | **pytest 三层 + hypothesis + golden** | TOLERANCE=1e-4；替身表；V1 无 CI |
| 4 | 多资产 | **配置区分 + ASSET_RULES + execution 层** | 不学 Traits；V1 股票+ETF→V2 可转债→V3 期货→V4 期权 |

**对已有设计的修订/细化**：

| 对象 | 内容 |
|---|---|
| **§35.6 配置** | **修正**：YAML 一个文件 → **TOML 三层**（YAML `on→True` 陷阱 + 敏感信息分离）；"不分层" → 全局/策略/敏感三层；此前开放的敏感信息（=.env 不入 git）与版本化审计（=git log + config_snapshot）**已定**；"不热更新"保留 |
| **§66.4 FeeModel._is_etf** | 与 ASSET_RULES["etf"]["stamp_tax"]=False 对齐（51/15/16 前缀同源一致） |
| **§66.3 风控七规则** | 涨跌停/做空规则的参数来源 = ASSET_RULES（不再散落硬编码） |
| **§67.2 job_runs.jsonl** | 字段与 logs/runs/jobs.jsonl 统一；回测另记 backtests.jsonl（含 config_snapshot） |
| **§68.4 core/** | 增三个文件：config.py（load_* 三函数）/ logging.py（JsonFormatter+setup_logging）/ asset_rules.py（ASSET_RULES） |
| **§68.7 黄金样本** | 五步验证的 2–4 步落成 tests/golden/ 自动化（TOLERANCE=1e-4） |

**验证 4 条**：

| 验证 | 方法 | 标准 |
|---|---|---|
| 配置加载 | load_strategy_config 合并测试 | 策略级同名键覆盖全局；.env 不出现在任何日志/config_snapshot |
| 日志格式 | JsonFormatter 输出样例 | 每行合法 JSON；五字段齐全；console 可读 |
| 黄金样本 | pytest tests/golden/ | 两个 test 全过，容差 1e-4 |
| 多资产一致 | get_asset_rules("etf") vs FeeModel._is_etf("510300") | 印花税判定一致；stock 前缀 60/00/30/68/8 的 price_limits 映射正确 |

**本节仍未解决**：`.env` 具体字段清单定稿（V2 QMT 接入时补）；`backtests.jsonl` 的 metrics 字段全集；V2 可转债规则表预填（T+0/无印花税/20% 涨跌幅?）；GitHub Actions workflow 具体写法（V2）。

---

# 第四部分 讨论顺序与统计

## 70. 讨论建议的顺序

按依赖关系，建议这样推进：

**第一批：定语境（决定后面所有问题的前提）**
1. §12.1 / §12.2 / §12.3 —— 薄壳的定位（时间模型、EB 机制、五域骨架还建不建）
2. §1.3 / §1.4 —— 薄壳边界与截面策略路径（决定自研范围）

**第二批：定契约（决定模块间怎么对话）**
3. §2.4 / §2.5 —— Signal / Portfolio schema
4. §1.1 / §1.2 —— backtrader-vnpy 差异与统一账本
5. §5.6 / §5.1 / §5.2 —— udata 的必要性与 schema

**第三批：定数据（地基）**
6. §3.1 / §3.2 —— DQC 规则集与硬软边界
7. §4.1 / §4.2 / §4.8 —— 存储职责、表 schema、三件套对接
8. §6.1 / §6.4 —— 因子接口与因子表 schema

**第四批：定算法层**
9. §7.1 / §7.4 —— 多因子组合方法与合成位置
10. §8.1 / §8.2 / §8.3 —— ML 信号映射、特征管理、模型生命周期

**第五批：定元数据与运维**
11. §9.1 —— alphaType 注册表 schema
12. §10 / §11 / §13 —— 执行、运维、横切

## 71. 问题统计

| 章节 | P0 | P1 | P2 | 小计 |
|---|---|---|---|---|
| 1. 三件套集成 | 4 | 3 | 0 | 7 |
| 2. 接口契约 | 5 | 1 | 0 | 6 |
| 3. 数据输入与 DQC | 2 | 8 | 1 | 11 |
| 4. 数据存储 | 3 | 3 | 2 | 8 |
| 5. Udata 与容器层 | 4 | 2 | 0 | 6 |
| 6. 因子 | 2 | 3 | 2 | 7 |
| 7. 多因子组合 | 2 | 3 | 0 | 5 |
| 8. ML/DL 信号管理 | 3 | 4 | 1 | 8 |
| 9. 投研域 | 2 | 5 | 0 | 7 |
| 10. 执行域 | 0 | 3 | 1 | 4 |
| 11. 运维域 | 0 | 1 | 3 | 4 |
| 12. 重审决策 | 6 | 0 | 0 | 6 |
| 13. 横切 | 0 | 4 | 0 | 4 |
| **合计** | **33** | **40** | **10** | **83** |

> 注（2026-10-10 回改）：按 Part 2 标题内（P0/P1/P2）逐条实点重算——总数 83（含 §9.7），P0 33 / P1 40 / P2 10。P0 已全部关闭，见第三部分。

**第一批已解决**：§6.3 §6.6 §7.1–7.4 §9.6(部分) §9.7 §10.3(部分)，详见 §16–§18。

**第二批已解决**：§1.1 §1.3 §2.1 §2.2 §2.3 §3.2 §4.1 §12.1 §12.2 §12.5；部分：§1.2 §1.5 §1.6 §3.7 §3.8 §12.6，详见 §19–§21。

**第三批已解决**：§1.2（账本字段级 schema）、§1.1 §1.3（路线选型与 Adapter 实现）；部分：§1.5 §1.6 §4.8 §10.1 §10.2 §10.3 §10.4，详见 §22–§25。薄壳完整落地设计已定：A+B+C 组合、D 不做；账本用自定义 5 dataclass 而非 vnpy/backtrader 模型；Order:Trade = 1:N；两个 Adapter 各约 200 行 + 公共逻辑 300 行 ≈ 1750 行。

**第四批已解决**：§1.4（截面矩阵策略路径）；部分：§1.3 §1.6，详见 §26。结论：A+C 混合分两层——截面策略走自研截面组合模拟器（`cs_portfolio_sim.py`，薄壳一部分），时序/CTA 走 backtrader，实盘走 vnpy，三条路写同一个 Journal；策略基类拆成 `BaseStrategy`(on_bar) 与 `BaseCrossStrategy`(on_rebalance)；新增 ≈480 行，薄壳合计 ≈2230 行。

**第五批已解决**：§1.5（alphalens 接入）；部分：§3.10 §4.8 §6.4 §6.5，详见 §27。结论：alphalens 是回测前的顺序门控（非并行验证）；因子两阶段来源（draft 现算 / active 读表）；pricing 用日频 qfq（修正 §25.2 的 hfq）；产出落三层（YAML 标量 / Parquet 序列 / HTML tearsheet）；门控 IC>0.04、IR>0.5、t>max(2,√(2lnN))，N 由 attempt_log 按 factor-family 计；月度重评估 IC<0.03 标 under_review；薄壳内只 +120 行，另 ~530 行在 research/。

**第六批已解决**：§1.6 §1.7 §2.1（含四个子问题：表示形态/负载结构/顺序保证/生命周期）；再确认 §2.2 §2.3 不建 EventBrick/DataBus，详见 §28–§29。结论：六套引擎收敛为 4 保留（backtrader 1.9.78.123 / vnpy 4.x 精简 Gateway+EventEngine / alphalens-reloaded 0.4.6 / 自研 cs_portfolio_sim）+ 3 归档（VectorBT/ZVT/Qlib 移 archive/，**不做对照组**——修正 §20.7/§26.8）；事件系统取封闭 12 事件 Enum + frozen Event dataclass + emit/journal/optional listeners，只映射公司 DI→DAY_START、BarData→BAR，不引入 TI/EB/EBGraph；分钟级数据不改事件模型（BAR 频率无关，不需要 StartTI/EndTI）；薄壳 +60 行 ≈2290 行。

**第七批已解决**：§2.2（EventBrick 六个子问题的薄壳等价物，补上批遗漏）、§2.3（DataBus 不需要）、§2.4（Signal 三层 Schema）；部分：§2.5 §4.2，详见 §30–§33。结论：EB 四元素逐个给等价物（`emit()` / 硬编码主循环+可选 `add_listener()` / `engine.run()` / session 生命周期 + 策略 checkpoint），仅在多策略通信、tick 级路由、插件化时才重新引入 EB；DataBus 全套新增 **0 行**（数据流是直线不是总线；实盘 pub-sub 用 vnpy EventEngine；背压不存在；Replay=回测本身）；Signal 取"一层半"——`CrossSignal`（截面全量快照，**修正 §26.9** 的 `on_rebalance` 返回类型）+ `SignalData`（时序增量，补 `factor_snapshot`），Tick 层不做；**新增 SIGNAL 事件，EventType 12→13**（信号含决策原因必须进 Journal）；信号归档 `data_platform/signals/{cross,timeseries}/{strategy_id}/{date}.parquet`，DAY_END 落盘；L2 走"离线因子原料"路径（80% 场景）不进事件流，tick 级驱动是独立的另一个系统。薄壳 +150 行 ≈**2440 行**。

**第八批已解决**：§2.5（Portfolio schema 五问全解）、§9.6（约束求解器）、§9.7（手数取整落地）；部分：§12.4，详见 §34。结论：信号链拆清 Signal → Portfolio → Order 三层，补上此前缺失的约束求解步；`PortfolioConstraints`（单票上限+行业上限+换手上限+总权重 0.95 留 5% 现金+lot_size=100+min_trade_value=5000）→ `PortfolioOptimizer.solve()` **纯 numpy 裁剪不用 cvxpy/scipy**（策略给权重、优化器只做约束满足；cvxpy 留给 risk parity/BL）；`TargetPosition`/`TargetPortfolio` frozen dataclass，单位三者共存权重为主，`pre_opt_weights` 留取整前权重分析偏离；T 日收盘产出（`trade_date`）T+1 开盘执行（`exec_date`），与 §18.2 一致；与 Signal **1:1**，时序策略无此层直接 SignalData→Order；**新增 PORTFOLIO 事件 EventType 13→14**；归档 `data_platform/portfolios/{strategy_id}/{date}.parquet`。修订：§26.5 手数取整移出 cs_portfolio_sim、§32.2 CrossSignal 的 max_weight/turnover_limit 字段废弃（权威=PortfolioConstraints）。薄壳 +190 行 ≈**2630 行**。

**第九批已解决**：§2.6（Service 契约三问：注册与发现 / 启动顺序依赖 / 配置注入方式）；部分：§13.1（配置格式与分层已定，敏感信息与版本化审计仍开放），详见 §35。结论：Service 层不需要——公司 Pysim 的 6 个 Service 在薄壳里就是 6 行代码（`for trade_date in calendar` / `data_api.get_bars()` / `strategy.on_bar()` 返回值 / `optimizer.solve()` / `common.pre_check()`，TimeControl 不需要）；注册与发现=构造函数注入（`CrossSectionalBacktest.__init__` 六协作者），启动顺序=代码书写顺序（`warmup → on_init → journal.open` 三行同步）；配置=一个 YAML 文件 + 一个 `BacktestConfig` dataclass + `load_config()`（放在 `kernel/config.py`），不分层、不配置中心、不热更新（改配置重启引擎）；YAML 的 `constraints:` 块即 `PortfolioConstraints` 字段镜像。新增 **~50 行**，薄壳合计 ≈**2680 行**。

**第十批已解决**：§3.1（DQC 四维规则集五问全解），详见 §36。结论：44 条规则逐条定稿（完整性 C01–C10 / 合法性 L01–L14 / 一致性 K01–K11 / 时序性 T01–T09），每条带 ID/严重度/阈值/阈值来源/适用资产；HARD 27 条阻断入库、SOFT 17 条告警+标记（= §3.2 规则级落地）；阈值三类（固定 ~60% 不可调 / 统计 ~35% 年度复核 / 自适应 ~5% phase-2 不做）；存放=YAML（5 套按资产）+ Python custom_checks 混合，pandera 验证；semver 版本化、**历史数据不自动重跑**、CLI `revalidate` 按需重验、结果带 `rule_version`；粒度按资产为主 + per-check columns + 板块参数 + 合约 reference 查表，按 ticker 不支持；执行两时点（入库时 HARD+SOFT / 每周批量全量 SOFT + 跨资产 K06-K08 + T09）。部分：§3.3（执行时机已定，耗时/并行度开放）、§3.4（结果落盘已定，保留期/看板/告警通道开放）、§3.11（T09 修正检测已定）、§3.2（确认+细化）。新增 **~790 行**在 `dqc/`（数据平台侧，不计入薄壳）。注：来源标题写"40 条"、四表实际 44 行，已在 §36.1 标注口径差异。

**第十一批已解决**：§3.2 硬错误 vs 软警告全解 → §37：候选场景 7 项判定（主键重复=去重后入库、时间倒流=拒收分区、空值 5% 软/30% 硬拒整批、价格异常软+flag、跨源不一致软+告警、关键字段全空拒收该行）；硬拒三级粒度（行级 drop / 分区级 / 批次级）；三级阈值体系（0%/5%/20%）+ `DQCSeverityConfig`（`row_action` + `escalation_threshold`）；44 条规则完整判定表（`flag` 全部 `escalation=None`，drop 类阈值 1%/5%/20%、T03 未来日期 1 行即整批拒收）；`quality_flag` int64 位掩码（四维分段 15 个命名 bit，`compute_quality_flag()`）+ 下游默认不过滤 + `get_bars(quality_filter)`；批次级安全阀 `BatchRejectConfig`（HARD drop >20% 整批拒收 / SOFT flag >50% 告警）+ `ingest_with_dqc` 七步双保险。新增 **~80 行**（`dqc/` 合计 ~870 行）。注：bit↔规则映射（15 bit vs 17 条 SOFT）与 `quality_filter` 语义两处小不一致已在 §37.7 标注。

**第十二批已解决**：§3.3 + §3.4 全解 → §38。§3.3：执行时机=**pre-load 一遍**（post-load 三宗罪：坏数据被下游消费/事后重写 Parquet 更贵/单机流水线无第二遍场景；跨资产 K06-K08 是跨表检查单独跑）；耗时预算四资产表（日频 <1s、分钟频 2-5s、fundamental/factor <0.5s，**DQC 不是瓶颈**，瓶颈是下载 IO；L2 届时再优化）；抽样=**增量不抽样** + 每周全量（T09+K06-K08）；失败重试=**修复后重跑整批**（`reingest` CLI，与 `revalidate` 分工）；并行度=日常不需要、回填按日期 joblib n_jobs=4。§3.4：双表落盘（行级明细按月分区 `results/{asset}/YYYY-MM.parquet` 15 列 schema + 批次汇总单文件 `batch_summary.parquet` 13 列 schema，`action_taken`/`batch_status` 对接 §37 处置动作）；保留期=明细 1 年/汇总永久+月度清理脚本；看板=`python -m dqc.report dashboard` CLI+DuckDB 不建 Web；告警=logging 三级（ERROR 拒收 raise / WARNING flag>50% / INFO）不接飞书邮件 Slack（未来 cron 无人值守再加 webhook）。新增 **~100 行**（`dqc/report.py` 扩展，`dqc/` 合计 ~970 行）。注：§3.5 隔离区仅被暗示（pre-load 拒收=不写盘），整节仍开放。

**第十三批已解决**：§3.5 + §3.6 全解 → §39。隔离区=独立目录 `quarantine/{asset}/{date}.parquet` + `.rejection.json`，T+30 自动清理（7 天内应处理），修复三路径（A 重下载重跑 / B 调规则 `recheck --from-quarantine` / C 放弃），看板与 §3.4 共用（`batch_status='rejected'` 行即索引 + `python -m dqc.report quarantine` CLI）；raw 层“原样”=Parquet+zstd+数据源原始列名值类型（不清洗），分区=数据源→资产→日期（不按合约），保留 1 年（~6.5 GB/年可接受；L2 届时缩短到 3 个月或只留 asset）；Normalizer 每源一个（~100 行/源）只做格式转换不做 DQC；`daily_ingest_pipeline` 五步串起三层（下载→save_raw→Normalize→DQC→asset/quarantine）。**修正 §38.11**：HARD 拒收≠“不写盘”，而是不进 asset 层、写 quarantine 层留 30 天（raw 证据始终保留）。新增 **≈310 行**（数据平台侧：quarantine 读写 ~60 + Normalizer 基类 ~50 + 每源 Normalizer ~100×2 源）。§3.5/§3.6 从无标记升级为已定。

**第十四批已解决**：§3.7 五子问题全解 → §40。接口=`fetch()`+`list_asset_types()`+`health_check()`+`close()` 四方法（**无 `normalize()`**——Normalizer 独立模块；**无 `schedule()`**——调度是流水线的事）；`FetchResult` dataclass 定稿（df 原样/source/asset_type/trade_date/row_count/watermark）；增量=watermark 参数（格式由子类定义，调用方只存取到 `raw/{source}/.watermarks.json`）；限流/重试/超时三层（调用方只见成败 ← 基类装饰器 3 次指数退避/120s/QPS ← 子类 fetch 内分页与 API 限制）；AmazingData 约束（~100 条分页、local_path 字符串、连接数/`logout()`）全部屏蔽在 `AmazingDataAdapter` 内；新增数据源 **~180-220 行**（Adapter+Normalizer+注册 1 行，DQC 0 行——按 asset_type 不按 source）；`daily_ingest` 更新 §39.10（fetch 具象 + step5 save_watermark + finally close()）。新增 **≈420 行**（`data_platform/adapters/`：基类 ~100 + 3 子类 ~300 + 注册表 ~20）。§3.7 从部分已定升级为已定。

**第十五批已解决**：§3.8 多源冲突处理——主源指定表 `source_priority.yaml`（优先级列表 + 字段级覆盖）、跨源对比容差（价格绝对 0.01 / 量额相对 0.1% / adjfactor 1e-6 / PE/PB 2%）与时点对齐（两源入库后比）、四类校验频率（日频每天 / 分钟频每周抽 1 天）、四级冲突响应（<0.1% 静默 / 0.1-1% WARNING / >1% ERROR / adjfactor 始终告警）、备源补数（`source_tag={source}:backfill` 写 Parquet metadata，不自动回填）。§3.8 从部分已定升级为已定。

**第十六批已解决**：§3.9 交易日历/时区/合约换月/夜盘——日历来源 Tushare trade_cal 主源+AKShare 备源（不自维护）、`asset/calendar/{exchange}.parquet` 月更 + `TradingCalendar` 工具类（~200KB 常驻内存）；时区全部交易所本地时间 naive datetime（不转 UTC，pandas 快 ~30%）；主力合约=每日成交量最大（不用持仓量），连续合约前复权+换月收盘价比价、每日重建全序列；夜盘算次日（交易所官方规则），`cal.next()` 处理周末/节假日跨越。§3.10 子问题 1 落定：asset 层存 raw+adjfactor 不存复权价（零回刷）、分钟线无 adjfactor 列。§3.9 从无标记升级为已定，§3.10 保持部分已定（+§43）。

> **第十七批已解决（2026-10-04）**：§3.10 四子问题全解 → §44。adjfactor 来源：Tushare `adj_factor` 主源 + AKShare 反推校验；3 条 DQC（C06/L06/K05）+ 跨源容差 1e-6 + `corporate_actions.parquet` 公司行动检测；研究层 alphalens 前复权 close（`get_pricing_for_alphalens`）+ `PriceAccessor`（raw/forward_adj/returns）；回测/实盘统一"截至当日前复权"（收益率公式基准日消掉，策略禁用价格绝对值判断）。§3.10 从部分已定升级为**已定**（+§44）。

> **第十八批已解决（2026-10-04）**：§3.11 数据回溯修正四子问题全解 → §45。检测：每周日 02:00 全量重拉近 30 交易日+逐行比对（季度全量兜底），不依赖数据源修正标记；应用：直接覆盖 asset 层+`corrections/{asset}/{date}.jsonl` 修正日志（不版本化），走完整入库流水线；下游：影响分级 LOW/MEDIUM/HIGH（adjfactor→HIGH）+ `factor_registry.yaml` stale 标记，不自动重跑；回测：按 severity 人工决定（LOW 忽略 / MEDIUM 查 universe / HIGH 重算+重跑确认）。§3.11 从部分已定升级为**已定**（+§45）。

> **第十九批已解决（2026-10-04）**：§4.1 三套存储职责边界补全 → §46。砍掉 ClickHouse（数据量不需要/部署成本不值/不预留抽象层）；两层存储 Parquet（持久化，raw+asset+research+辅助四层）+ DuckDB（纯视图查询，无 .duckdb 文件）；DataAPI ~110 行（`duckdb.connect()` 内存模式 + `read_parquet` 视图 + `get_daily_bars`/`get_factor`）；原子写入（tmp+rename）；通用 schema 约定定稿（时间戳/编码/合约代码/可空/quality_flag/分区）为 §4.2 表 schema 前置。§4.1 标记扩展引用 §46。

> **第二十批已解决（2026-10-04）**：§4.2 表 schema 定稿 → §47。11 张核心表（bars_daily / bars_minute / bars_daily_futures / tick_futures_l2 / fundamental / factor / calendar / contract_specs / industry / corporate_actions / continuous）字段级 schema；5min/60min 不单独存（DuckDB `time_bucket` 视图从 1min 聚合）；L2 tick Phase 2 预留（唯一可能需要 ClickHouse 的表）；主键/分区/排序双轨规则（行情/基本面按日一天一文件，reference/因子全量一个文件，ticker ASC 利用 row group statistics）；时间戳 date 列用 date、datetime 列用 datetime64[ns]（naive 北京时间），不用 bigint；合约代码 `{代码}.{交易所}` 全局唯一 + L13 regex；可空默认 NOT NULL（估值比率/因子值/公司行动可选/turnover/margin_rate/industry_l3 六类例外）；quality_flag 行级 int64 bitmask（bit0-4=C02/C05/L04/L07/K01），因子表不带；pandera schema（`strict=True`）。§4.2 从无标记升级为**已定**（+§47）。

> **第二十一批已解决（2026-10-04）**：§4.3 分区与文件组织 + §4.4 压缩与编码 → §48；§4.5 事务性与并发 + §4.6 数据生命周期与容量 → §49。分区用扁平日期文件 `asset/{table}/{YYYY-MM-DD}.parquet`，不用 Hive 三层目录（DuckDB 走 row group statistics 而非 partition pruning；10 年 7500 子目录是管理噩梦；一个 ls 看清所有数据）；DataAPI 文件级过滤（按 TradingCalendar 枚举文件列表，不 glob 全量文件头）；文件大小目标 1MB–256MB（日频 ~200KB 偏小接受、按天查询最常见合并反而 30x 无用 IO；分钟频 ~25MB 理想；L2 ~500MB Phase 2 按月合并）；小文件合并不需要（全年 ~10,000 文件）；写入全部覆盖写（Parquet 不支持原生追加；版本化已被 §3.11 否决），`write_parquet_atomic`（tmp+rename）+ `cleanup_tmp_files` 清残留 .tmp；压缩 zstd level 3（压缩比接近 gzip、解压接近 snappy；比 snappy 多压 30% 解压只慢 10%；实测 bars_daily 400KB→200KB）；列编码 pyarrow 自动选择不手动指定；Row Group 64K 行（DuckDB 2048 行 batch + 公司 UdataViewer PUSH_ARROW_MICRO_BATCH_SIZE 对齐）；冷热分层不需要（10 年 ~67 GB 全放 SSD，512GB <¥300）；全表统一 `PARQUET_WRITE_CONFIG`。并发写正常不发生、FileLock（flock LOCK_NB）兜底；并发读靠 tmp+rename+Linux fd/inode 语义天然安全；一天一文件=一天一事务，跨表不需要事务；失败不回滚、所有操作幂等重跑即恢复；DuckDB 内存模式无共享状态。保留策略 asset 永久、raw/辅助 1 年、quarantine 30 天；`lifecycle cleanup` CLI 每月手动或 cron；10 年容量 ~78 GB（含 L2 ~1.3 TB）；磁盘告警 <50GB WARNING、<10GB CRITICAL 拒绝入库（`daily_ingest` 起始检查）；只备份 research+config（asset 可重建：78GB 备份 10+ 分钟 vs 重建 2-4 小时），rsync 到外部硬盘每月一次，RPO 1 个月 / RTO 4 小时。§4.3/§4.4/§4.5/§4.6 从无标记升级为**已定**（+§48/+§49）。

> **第二十二批已解决（2026-10-04）**：§4.5 事务性与并发详版 → §50（§49.2–§49.5 骨架的完整讨论稿）。Confluence 四页参考（UdataSQL 事务模型 page 288527084 / 三阶段分布式写入 page 301596866 / UdataViewer page 288527133 / Replay 一致性 page 209868537）；UdataSQL 三阶段映射（Phase 1 Initializer→创建 .lock、Phase 2 Workers→`write_parquet_atomic` 每天一文件天然按日隔离、Phase 3 Committer→`os.rename` 原子提交；版本化不做；"不保证并发写写"→flock 直接拒绝，比公司更激进）；FileLock 完整版（`LOCK_EX|LOCK_NB`，拿不到锁抛 `ConcurrentWriteError`，`__exit__` 释放并删 .lock）；`write_parquet_atomic` 升级为三阶段内嵌 FileLock（lock→write tmp→rename）；并发读靠 Linux inode 语义天然 snapshot isolation（T0–T4 可见性时序：查询看到开始时的快照，非 MVCC；UdataViewer Arrow 生成器无锁读同源）；事务边界=一天一文件=一天一事务（跨天/跨表原子性都不需要，DQC 跨粒度检查 K06–K08 异步跑；对照 UdataSQL 显式 BEGIN/COMMIT vs 个人平台隐式）；锁机制只需 flock（不用 DuckDB WAL/single-writer——DuckDB 不写数据）；失败回滚不需要——幂等重跑比 ROLLBACK 更强（失败点四行表 + `daily_ingest` 幂等代码，覆盖写 4 步全幂等）；`cleanup_stale_tmp`/`cleanup_stale_locks`（1 小时阈值；flock 进程退出自动释放）；版本化当前不做、将来最小成本 `.prev` 备份。代码 ≈40 行（FileLock ~25 + cleanup ~15，净增 ≈20 行）。

> **第二十三批已解决（2026-10-04）**：§4.6 数据生命周期与容量详版 → §51（§49.6–§49.8 骨架的完整讨论稿）。Confluence 六页参考（数据等级体系 page 269215557 / UdataSQL 版本管理 page 288527084 / Udata 压缩 page 270638954 / OT 行情存储规模 page 288526822 / 备份中心 page 245317822 / 分布式存储层 page 260838113）；数据等级 Class 1-4 映射（Class 2→raw 1 年 / Class 3-4→asset 永久 / quarantine 30 天 / 辅助日志 1 年 / factor 研究产出永久）+ `lifecycle.yaml`（policy permanent/rolling + keep_days）；`cleanup_expired(root, dry_run=True)`（rolling 层按文件名日期滚动删除 + .jsonl 日志同清；默认 dry_run 先报告；跳过 permanent 层；对标 UdataSQL 版本管理——公司软删除可 set_version 回切，个人平台物理删除靠 correction.jsonl 留痕）；**容量重估：10 年 ~45GB（不含 L2）/~84GB（含 L2），修订 §49.7 的 78GB/1.3TB**——逐表推算 bars_minute 39GB 占大头，L2 按期货 50 万行/天 3.9GB/年（此前误按股票 tick 5000 万行高估一个数量级）；对比公司规模差 5000 倍（Nasdaq OT 77.4GB/天 vs 个人 16MB/天），单块 SSD 256GB+ 满足 10 年；告警三档落 `capacity_alert.yaml`（<50GB WARNING / <10GB CRITICAL 暂停入库）+ **新增 data_growth 单日 >100MB 异常告警**（正常日增 ~16MB，防重复入库）；备份对标备份中心（rsync 执行模块一致）：全量备份 data_platform ~45GB 到外置硬盘（修订 §49.8 只备份 research+config）月度手动、文件级粒度天然解决公司'备份粒度不足'痛点、`backup.sh` 含 .tmp/.lock 排除 + parquet 文件数比对验证；RPO 1 月 / RTO 4 小时（rsync 回拷 8min + 环境重建；重建耗时修订 2-4h→1-2 天）；灾备升级路径 Level 0-3（外置硬盘→cron 周备→rclone OSS 0.7 元/月→两地）。脚本 ≈90 行（cleanup_expired ~40 + check_capacity ~20 + backup.sh ~30，净增 ≈30 行）。

> **第二十四批已解决（2026-10-04）**：§4.7 查询与访问层 → §52；§4.8 与三件套的数据对接 → §53。Confluence 十一页参考（UdataViewer Python API page 288526910 / 架构原理 page 288527133 / 性能基准 page 328170767 / Nightly Benchmark page 315824290 / 压缩性能 page 270638954 / MarketData Python API page 288526845 / Pylib DataPool page 222727558 / Usignal page 244048834 / NewPysim page 222727276 等）；查询层：**SQL-only DataAPI** ~80 行（`query`/`query_arrow`/`profile=True`/`statement_timeout`/context manager + `register_views` 视图注册 `read_parquet` glob + `union_by_name`）——公司三层出口（SQL/Python API/C++ API）只取 SQL 层，DataFrame API 不提供（read_parquet 已一样快、SQL 最通用、不值得维护两套）；sqlglot 预过滤/Arrow 批式生成器/`query_df_batch` 都不需要（UdataViewer 三层复杂度源于 Udata 私有格式；个人标准 Parquet + DuckDB 原生已含列投影/row group pruning/predicate pushdown）；**不做应用层缓存**（全数据 45GB NVMe 15s 冷读、OS page cache 自动命中；DataPool 懒加载是大数据场景）；慢查询 `[COST]` 日志 + >5s WARNING + >30s CRITICAL + timeout 60s 落 `query_config.yaml`（公司 5 条结构化日志简化为 1 行）；多进程约定四规则（每进程独立 DataAPI / 不跨进程共享连接 / 同进程多线程安全 / 不手动设 threads）；写读完全解耦（写 flock+tmp+rename、读 read_parquet，inode 语义 snapshot isolation）；研究层性能目标：alphalens 日频 128 万行 <1s、分钟 3 亿行 <5s、单股 10 年 <100ms、全历史 30 亿行 <30s（公司基准按规模差 ~500 倍折算）+ 四步排查机制（profile→EXPLAIN pruning→列投影→polars 对照）；对接层：`load_bars(symbol, start, end, freq, fields, adjust, asset_type)` ~50 行（表映射 + SQL + 复权：前复权按 latest_adjfactor 缩放/后复权按 first/不复权零转换）+ 三个适配函数各 ~20 行——backtrader 用自带 `bt.feeds.PandasData` 不自写、alphalens prices pivot 宽表 + factor MultiIndex Series、vnpy 未复权 + amount→turnover + symbol/exchange 拆分；**口径归一总表**（存储层中性未复权+统一 schema、适配层按消费端转换——复权/时间戳/字段命名/标的格式/缺失值/排序七维）；内存分块 `load_bars_chunked`（对标 query_df_batch 按行数→按时间）：日频全量、分钟 >3 月按月 chunk（单月 1.6GB 安全、一年 20GB OOM）；设计哲学'搭配但不绑定'（Usignal）+'空洞框架'（NewPysim）。代码 ≈190 行（DataAPI ~80 + load_bars ~50 + 三个 to_xxx ~60）。§4.7/§4.8 均升级为**已定**（+§52/+§53）——**§4.x 存储章节至此全部关闭**。

> **第二十五批已解决（2026-10-04）**：§5.1 udata 形态/粒度/DataBus/k-slice 四问四答 + §5.6 引入必要性 → §54。Confluence 十页参考（Udata 架构 page 244048875 / 构成 page 199033106 / Schema page 244048881 / API page 198085912 / BarData page 199033099 / BarData Python page 257678105 / 时间维度分段 page 240741315 / UdataSQL 概览 page 288526808 / DataBus page 156420118+335938903 / Usignal page 244048834）；Q1 形态：udata = Schema（schema.info 文本文件：TypeInfo+维度+生产状态+血缘）+ 容器层（默认 Ucube C++ tensor file-backed mmap，可插拔纯内存/bin/分布式内存）——带自描述 schema 的数据抽象层，公司选 mmap 为 C++/Python 零拷贝+6.52 亿行+多进程共享内存；Q2 粒度：一个 BarData = 多日×多时段×多合约三维矩阵 [di,ti,ii]（vir_ii_num 按 2^n 对齐内存，A 股 242×240×8192≈476M 数据单元→O(1) 坐标访问必须 mmap tensor）；Q3 DataBus=实时消息总线（ubiqmessage pub/sub）与 udata 落盘产品上下游关系（实时链路 DataBus→usignal 切 bar→Udata_BarData→utrade；离线链路 Udata→usignal 回测）；个人平台无 DataBus，akshare/tushare API 直接落 Parquet 对应'落盘'之后；Q4 k-slice=Slice(di,ti,ii) NumPy 风格多维切片操作（FullSliceIndex/SliceIndex/SingleIndex 三种类型）≠udata 容器；usignal'切 bar'是 tick→bar 聚合（resample）另一概念；**核心决策：不引入 udata 容器层（候选方向 B）**——公司需要 udata 的 9 大原因逐条 ✗（零拷贝/坐标访问/超大规模/共享内存/自描述 schema/多 Layout/View/时间分段/实时切 bar 个人平台全不需要）；两张总表落盘：udata 功能 9 项→Parquet+DuckDB 等价物（Bar()→SQL WHERE、Slice()→WHERE+BETWEEN、GetArry→DataAPI.query、GetPandasDataFrame→load_bars）+ 公司概念 17 项映射总表（Udata_BarData→bars_daily/bars_minute、Universe/TimeAxis→每行 ticker/date 字段不独立维护、Udata_OrderTrade→不引入）；§5.2/§5.3/§5.4 升级为部分已定（概念映射关闭，字段精度/版本化/命名细节仍开放）、§5.5 已定（对接函数即 §53 的 to_xxx）、§5.1/§5.6 完全关闭；新增代码 **0 行**——不引入新抽象层。

> **第二十六批已解决（2026-10-04）**：§5.2 udata schema 字段精度 + 可扩展性 → §55（§54.9 两项尾巴关闭）。Confluence 参考：精度事故 page 263692149（usignal_dlc float→double 导致 Compatible 返回 Incompatible、历史数据无法复用全量重刷——根因 Schema 与数据紧耦合，tensor 固定 shape + TypeInfo 强绑定）；精度决策 8 字段：价格 OHLC **DOUBLE**（A 股价格 6 位有效数字 1234.56 逼近 float32 极限，一开始就用 double 不踩升级坑）、volume **INT64**、amount/adjfactor **DOUBLE**、日期/时间/代码 **VARCHAR**（人类可读 DuckDB 自动识别）、quality_flag **INT8**；**不用 Decimal**（Parquet+DuckDB 定点运算性能差；证监会最小变动价 0.01 元，DOUBLE 15-16 位有效数字足够；公司也只用 float+double）；schema 可扩展性：Parquet evolution 三规则（新增列→NULL ✓ / 删列→忽略 ✓ / 改类型→重写 ✗）+ DuckDB `union_by_name=True` 自动合并不同 schema 填 NULL；扩展策略四行矩阵（新增列直接写不破坏 / 新增表独立表+新 view 不破坏 / 改类型一次性迁移但一开始选 DOUBLE/INT64 上限避免 / 改列名应避免——列名是行业标准）；Compatible 判断不需要轻量版（schema 只增不改，union_by_name 即兼容层）。§5.2 升级为**已定**（§47/§54/§55）。新增代码 **0 行**。

> **第二十七批已解决（2026-10-04）**：§5.2.6 schema 版本化 + §5.3 容器层组织 + 第 5 章决策汇总 → §56（udata 章收口）。Confluence 参考：Udata API page 198085912（Compatible 四级：Equal→直接使用 / EqualDiffLayout→Transform 后使用 / Superset→GetView 提取子集 / Incompatible→重新生产全量重刷）+ 精度事故 page 263692149（DynamicData FixSchema——DataLoader 在 PrepareUdata 阶段检查并修复 schema）；**schema 版本化：不需要显式机制**——Parquet footer 自带完整 schema 元数据、DuckDB `union_by_name` 自动合并不同 schema 文件、真要大改写迁移脚本全量重写（<100GB 几分钟）；版本历史用 Git——代码即 schema 定义，git log 即版本历史（Compatible/FixSchema 是 tensor 与 schema 紧耦合的补丁，Parquet 侧不需要）；**容器层组织**（§5.3 完结）：公司 6 类 Udata（BarData(di,ti,ii)/FutBarData(di,ti,ci,mi)/ProductBarData(di,ti,ci)/DailyData(di,ii)/CrossSection(di,ti)/OrderTrade 变长流式，page 288526808）逐一映射到 §4.2 的 11 张表——一张 Parquet 表=一个容器、按日文件=时间分段；合约元数据独立 instruments 表不放容器里（page 199033096）；依赖血缘=correction_log.jsonl 简化版不做 DAG 状态传播（page 223886928/223894024）；**§5.6 修正为方向 C：薄规范（§4.2 SCHEMA 字典）不做容器运行时**——§54.6 曾误记 B；B"不做二次抽象"太松散被否，A 统一契约/D 分场景亦被否；不需要 Schema 类/TypeInfo 解析/Compatible 判断/Layout 选择/ProductionStatus 状态机/DependencyLineage DAG（公司需要它们的 4 大原因：数百类型依赖 DAG、C++/Python 共享内存、超大规模 Layout 优化、多团队兼容检查——个人全不适用）；第 5 章决策汇总 10 行落盘（形态/schema/容器/生产消费/对接/必要性/精度/扩展/元信息/新增代码）。§5.2 补完版本化、**§5.3 升级为已定**、§5.6 标记修正——**第 5 章（udata）§5.1–§5.6 至此全部关闭**。新增代码 **0 行**。

> **第二十八批已解决（2026-10-04）**：§6.1 因子的定义与接口 → §57；§6.2 因子的计算调度（DAG/缓存/增量/并行）→ §58。Confluence 十六页参考（Usignal 框架 page 244048834 / 2024 项目总结 page 215591289 / uformula 计算 feature page 247377333 / feature→signal page 253447090 / UFormula 接入 page 222730288 / 算子列表 page 228460124 / Alpha 入库 page 288524848 / DataPool page 222727558 / 并行生产 page 244034279 / 只刷无效 udata page 256109564 / DPP page 288529199 / 生产状态 page 223886928 / DependencyLineage page 223894024 / 事件驱动调度 page 324637543 / 级联回刷 page 381602916 / DAG 任务 page 335950681）；公司因子链路还原：DataPlugin 注册 Udata → UFormula XML 公式（如 tsrank{tsmean{close,20},500}）/ C++ EB_Indicator / PyIndicator 三路径 → EB 订阅 di/ti 事件写 feature_data Udata → EBS_Signal → AlphaDumper 入库；核心简化（事件驱动调度 / C++ GPU / k8s 千级并行 / UFormula DSL 四大复杂度全不需要——纯 Python、百万级行、单机单进程、离线计算）；**Factor ABC 保留微调**：元数据类属性（name/description/category/frequency/asset_types/version，不引入 JSON——代码即元数据）+ `depends_on()` 显式声明（`bars_daily` 无前缀=数据表、`factor:` 前缀=因子依赖，对标 RequestDynamicUdata+DependencyLineage）+ `compute(data)→DataFrame[date,ticker,{factor_name}]` 统一输出（直接对接 alphalens MultiIndex Series / FactorDAG 下游 / Parquet 存储；不输出矩阵）；因子分类 4 类 FACTOR_CATEGORIES（time_series/cross_section 对标 ts*/cs* 算子、event/alternative 为个人扩展）；factor_library 改动建议 7 行（Factor ABC 微调 / Registry 类属性注册 / FactorDAG 保留 / PostProcessor=cs* 算子等价物 / EventBrick、UFormula、DataPlugin 不引入）+ 三示例（Momentum20D 时序、SizeRank 截面、VolAdjMomentum 组合）；调度：公司三层（usignal 单进程 EBGraph 事件驱动 / usignal-parallel+DPP 按 ceil(天数/maxThread) 切 pod / Datamall Kafka asset_ready+级联回刷+stop_nodes）→ 个人 FactorDAG 大幅简化（无 Kafka/k8s/多进程）；**DAG=声明式 depends_on() + Kahn 拓扑**（环检测=排序长度≠节点数；不做 AST 静态分析——成本高脆弱不可靠）；**缓存=按因子×日期范围** `data/factors/{name}/v{version}/{range}.parquet+metadata.json`，key=(name,version,range,depends_hash)，depends_hash=行数+max(date) 快速指纹；失效三条件（上游更新→hash 变 / 代码变更→version 变 / 手动→force=True）；不做单日/单 ticker 细粒度（全量 <1s）；**增量=append/full 两模式**（version 变或上游修正一律 full 保守策略；不做 per-di 精细增量——全量 128 万行 <5 秒，状态跟踪/断点续传复杂度远超收益）；§6.1 升级为**已定**、§6.2 **部分已定**（失败处理/长耗时因子调度仍开放）。新增代码 ≈**95 行**（FactorDAG ~40 + compute_depends_hash ~15 + compute_factor ~40）。

> **第二十九批已解决（2026-10-04）**：§6.2 计算调度收口（并行/失败/ML）→ §59；§6.3 后处理流水线全解 → §60。Confluence 参考（page 244034279 usignal-parallel mt/maxThread + --retry / page 223886928 ProductionStatus BLACK→AMBER / page 324637543 Datamall 超时兜底 / page 301602916 级联回刷 stop_nodes + DAG 层序 / page 288529199 DPP 分阶段 serial/parallel / page 228460124 UFormula cs* 截面算子 / page 301832392 量化学习教程 §8.7 三步预处理 + §26.1 中性化实战 + §7.3 截面 z-score + §7.7 ETF 不中性）；调度收口：**并行=层内 ProcessPoolExecutor**（topological_layers() Kahn 分层，同层独立因子并行 max_workers=4；不做按日期切分——A 股日频单天 ~5000 行 pandas 向量化更优）；**失败处理=fail-fast+隔离**（单因子 FAILED → 下游跳过 → 同层继续 → 返回 results+errors；compute_all_safe；**不引入 5 色灯**——离线批量修复后全量重算 <5 秒）；**ML 因子=MLFactor 子类**（train/predict 分离 + joblib 模型持久化 + retrain_freq；调度层 ML 串行不进线程池带 timeout；**不引入 Celery/Airflow/Prefect**）；整体调度流程图落盘（compute_all → 分层 → Layer0 并行/缓存 → Layer1 depends_hash 判定 append/full → Layer2 ML 串行）；后处理全解：**公司后处理内嵌 UFormula 公式链**（cszscore{csindneutral{tsrank{tsmean{close,20},500}, ind}} 编译时确定）vs **个人独立 PostProcessor Pipeline**（运行时可配置）；四步流水线 **去极值→填充→标准化→中性化(可选)**（顺序理由 4 条：极值先于 z-score、填充先于标准化/中性化、标准化先于中性化便于诊断、中性化残差=纯 alpha）；各步选型：**MAD winsorize n=3.0**（1.4826 换算，优于分位数/3σ）→ 截面中位数填充（ffill 时序/0 二元/dropna 可选）→ **z-score 默认 rank 备选**（偏态用 rank）→ **OLS 取残差中性化**（factor~行业哑变量+log 市值；个股池必须/ETF 池与行业指数池不做）；**正交化不独立实现**=中性化回归加其他因子列（csfacneutral 思路，呼应 §16.5）；**关键决策：存原始值、消费时做后处理**（灵活性/可复现/多套参数/调试 四赢；公司存后处理值是 UFormula GPU 约束；与 §58 缓存一致——缓存不进后处理 key）；**防前视：所有步骤 groupby(date) 截面独立计算**（各步前视风险表 + 正误代码对比，与公司 cs* 算子同语义）；PostProcessor 实现落盘（PostProcessStep dataclass + Pipeline.transform + 4 预设 RESEARCH/STOCK_EVAL/ETF/RANK）。§6.2 **收口为已定**（六个子问题全关）、§6.3 **升级为全解**。新增代码 ≈**250 行**（调度 140 + 后处理 110）。

> **第三十批已解决（2026-10-04）**：§6.4 因子的存储布局 → §61。Confluence 参考（page 199033106 SchemaBase / page 205515828 DynamicData Unpacked 每字段独立 ucube / page 223886928 version=random_string_timestamp / page 288529199 DPP 增量只拉新日期 / page 288524848 Alpha 入库 ClickHouse 是实盘需求 / page 263692149 float→double 精度事故 / page 301623630 存储空间报表）；公司存储还原：feature_data Udata 的 **DynamicData Unpacked** 物理形态=每字段独立 `field_xxx.ucube`（本质就是每因子一表）+ SchemaBase 元数据 + random_string_timestamp 版本目录；宽表只是 GetViewer 的查询抽象，不是物理布局；四种存储模型决策：**Direction A 每因子一表**（独立目录，增量简单/版本清晰/删除无影响）→ 不用 B 长表 EAV（6.25 亿行爆炸、列裁剪失效）/ 不用 C 物理宽表（新增因子重写文件、版本化困难）/ 不用 D 三维张量（HDF5/zarr 违反 §4）；**物理隔离 + 逻辑合并**：`load_factor_matrix()` 多因子 outer join 拼宽表（对标 GetViewer），因子迭代快/版本独立/列裁剪生效，合并成本 <2 秒不值得优化；物理布局：`data/factors/{name}/v{version}/{year}.parquet + metadata.json`（ML 因子另有 `model/model.joblib`）；Parquet schema=**date VARCHAR YYYYMMDD / ticker `000001.SZ` / {factor_name} DOUBLE**（原始值未经后处理，NaN 保留=停牌缺失；DOUBLE=§5.2 float→double 事故教训，精度丢失在中性化 OLS 放大）；**metadata.json** 定稿（身份+血缘 depends_on/depends_hash+覆盖范围 date_range/row_count/file_count+compression），对标 SchemaBase 简化映射（datafile_path→目录隐含 / TypeInfo→Parquet 自描述 / ProductionStatus→不需要由 depends_hash 替代 / DependencyLineage→depends_on+depends_hash）；**分区按年**（~125 万行/年 → zstd 5-10MB；5-10 个文件与计算粒度对齐）——不按天（2500 小文件）/不按月（1MB 太小）/不按资产（只有 A 股）；**版本化=语义化 1.0/2.0**（不用公司 random_string_timestamp——公司是流水线自动打版本场景）；升版触发=compute/depends_on/参数变更，数据 append/上游修正/元数据修改不升版；多版本共存（对比 IC/回测复现/灰度切换），不自动删旧版（~5GB 可忽略）；**历史值重算=版本变更→全量重算写新版本目录**（保证同一版本内全历史定义一致）；**增量 vs 全量**：日常 append 当年文件（concat+drop_duplicates(date,ticker)+zstd level 3）；version 变→新版本目录全量 / force=True→覆盖 / depends_hash 变→覆盖 / 日常→append 四行触发表；月度/季度全量重算 diff 校验防增量 bug 累积；**Parquet + DuckDB 不用 ClickHouse**（公司 CK 是 Alpha 入库实盘多策略并发需求 page 288524848；个人消费方=本地回测脚本）；DuckDB `read_parquet`+glob 查询 + JOIN 等价宽表；data/raw + data/factors + data/backtest 三目录；宽表等价性确认：公司 Unpacked `field_xxx.ucube` ≈ 个人 per-factor Parquet——本质都是每因子独立物理存储，差异只在宽表抽象放 Schema 层（GetViewer）还是消费端（load_factor_matrix）；决策汇总 16 行落盘（含 §58 缓存目录细化为 `{year}.parquet`、§5.2 DOUBLE 贯彻到因子表、§59 ML 模型目录对齐）。§6.4 **升级为全解**（表 schema 从"仍开放"变为已定）。新增代码 ≈**65 行**（append/full 写入 ~40 + load_factor_matrix ~15 + metadata ~10）。

> **第三十一批已解决（2026-10-04）**：§6.5 管理与治理 + §6.6 去重合并 + §6.7 调试排查 → §62（三问合并，共享 Confluence 参考与一张决策汇总表）。Confluence 参考（page 301832392 量化学习教程 §8.9 入库八判据 / §7.6 衰减 3-5 年 / §18.3 三种衰减模式+四监控指标 / §7.8 僵尸因子 / §10.6 换手；page 223886928 Udata 5 色灯；page 223894024 DependencyLineage；page 199033106 SchemaBase；page 256109564 公司 CI）；**注册表=代码即注册表**——FactorRegistry + metadata.json（factor_id/version/description/category/frequency/asset_types/depends_on/status/created_at + factor_eval 写入 ic_mean/ic_ir/ic_t/turnover/coverage），不建独立库（因子 <100 遍历足够，独立注册表引入代码-注册表不一致问题）；**质量指标 8 行表**（Rank IC>0.04 / IC IR>0.5 / t>2 保守 3 / 分组单调 / 换手 / 半衰期>1 年 / 覆盖>80% / 相关<0.5）+ evaluate_factor()（截面 Spearman → ic_t=ic_ir√n）；**入库线 vs 退出线双标尺**（入库 IC>0.04/IR>0.5，退出 IC<0.03/IR<0.3，中间灰色带观察——教程 §8.9"不要拿退出线当入库线用"）；**多重检验校正 t=√(2·ln N)**（试 100 个才找到 1 个 t=2.5 不可信，与 §27.9 attempt_log 对齐）；**生命周期三态** draft→production→deprecated（check_factor_health() rolling 6 月 IC vs 历史均值 70%/50%/30% 三档）——不引入公司 5 色灯（用 depends_hash 变化触发重算替代 BLACK→AMBER 传导）；**血缘三件套** depends_on()+FactorDAG+depends_hash（"知道依赖了谁"即可，per-di 版本粒度对个人过度——全量重算 <5 秒）；**命名** `{类型}_{核心语义}_{参数}` snake_case 禁 mom20/factor_001（公司 UFormula tag 短标识是公式热路径场景）；文档=docstring+description 不建独立文档文件；**去重**：detect_redundant_factors() 按日截面 Spearman 取均值 threshold=0.7，4 档处理（>0.9 留 IC 高者 deprecated 另一个 / 0.7-0.9 经济逻辑不同则保留 / 0.5-0.7 警告 / <0.5 独立）；**冗余处理三原则 保留>合并>废弃**——不做等权合并（引入新自由度，合成在策略层 §7.3）；deprecated 不物理删除；**容量**：production 目标 20-50 个（经典 5-8 + 行业风格 3-5 + 另类 3-5 + ML 2-3 ≈ 15-20 production），"3-5 个就够"是单策略层面（教程 §7.8），>50 个说明没做好去重；**调试**：五分支排查表（全部标的→数据 / 特定标的→停牌 ST / 特定日期→数据源 depends_hash / 数值范围→除零溢出 / IC 突变→参数或衰减）+ diagnose_factor()（nan_ratio/inf_count/coverage/cs_std_cv 变异系数）；**单测四范式**（TestMomentum20D：test_basic_computation 精确对拍 / test_nan_handling / test_output_schema / **test_no_lookahead** 删最后一天历史值不变 np.allclose）+ 五范式表（含幂等性）；**可视化四图** plot_factor_report() 2×2（IC 时序柱状+均值红线 / 累计 IC / 截面分布直方 / 分组累计收益 n=5）→ research/reports/{name}_tearsheet.png 对齐 §27.8；决策汇总 14 行落盘（含 §27.11 status 四态与三态对齐——rejected 并入 deprecated）。§6.5/§6.6 **升级为全解**、§6.7 **已定**。新增代码 ≈**180 行**（evaluate_factor ~30 + check_factor_health ~15 + detect_redundant ~20 + diagnose ~25 + plot ~35 + TestMomentum20D ~30 + list_factor_meta ~10 + 杂项）。

> **第三十二批已解决（2026-10-04）**：§7 多因子组合五问 → §63。Confluence 参考（page 301832392 量化学习教程 §7.4 五法合成 / §7.3 截面 z-score / §7.7 散户建议 / §16.3 组合评估 / §16.4 回测实盘 gap / §26.1 中性化实战；page 288524848 Alpha 入库；page 282827800 OnAlphaUpdate）；**组合方法**：主路径**等权 z-score**（教程 §7.4"永远先跑等权做 baseline，跑不赢等权的复杂度就是过拟合"），ICIR 加权可选；**不做** IC 加权（ICIR 更稳）/ 回归 max ICIR（Σ⁻¹ 不稳定样本外常不如等权）/ ML 合成（归 MLFactor §59 不在组合层）；**FactorCombiner 类**落盘（method equal_weight|ic_weight|ic_ir_weight；combine(factor_scores, ic_history)→DataFrame[date,ticker,combined_score]；_ic_weights/_ic_ir_weights 负 IC 截断 0 归一化）；**权重窗口三决策**：扩张窗口（因子少历史短，滚动窗 IC 估计噪声大）+ 月频更新（与再平衡对齐）+ **IC 只用 t-1**（当期 IC 定当期权重=前视）；IC 历史读 factor_eval/{name}/ic_series.parquet；**正交化不做**（YAGNI——3-5 因子入库已控相关<0.5；Gram-Schmidt 顺序敏感/PCA 解释性差；未来 >10 因子再考虑 csfacneutral）；**中性化两层落定**：因子层 neutralize_factor() 逐日截面 OLS 残差（申万一级 31 行业哑变量 + log_market_cap，自由度检查 <32 跳过）；组合层**不做 Barra**（机构多组合风控需求，个人因子<5 个）；ETF 池不做（教程 §7.7）；只用 industry+size 两维度（§26.1 实证 80% 伪 alpha 已消，更多维度样本不足反过拟合）；**信号形态**：combined_score 连续 float + 合成后再截面 z-score 一次（跨日可比）= **AlphaSignal**（from_multi_factor/from_ml_factor/to_weights 三方法，两条路径汇聚——下游 build_portfolio 对信号来源无感知，对应 §65 signal_source 三态）；**signal_to_weights()**：top_k 等权默认（教程 §16.6"Top N 等权就够"）/ proportional 可选（score 负值 clip 0——A 股不能做空）；**评估 8 指标表**（IC>0.04 / IR>0.5 / 年化 10-15% / 夏普 0.8-1.2 **>2.0 怀疑过拟合** / MDD<20% / Calmar>1 / 换手月频 30-100% >150% 警戒 / 因子贡献最强<50% >70%=伪分散）+ factor_contribution()（Σ weight×return 按因子分解）；**验证纪律**（教程 §16.4 回测实盘 gap）：Walk-Forward 训练/测试时间不重叠**禁 KFold**（时序交叉验证泄漏）+ 测试集只看一次 + eval_report.json → data/signals/{name}/（含 metrics/config_snapshot path@hash/walk_forward_folds）；决策汇总 14 行落盘（含 §62.3 单因子指标与组合指标双层对齐、§65.1 signal_source 对应）。§7.1–7.4 **升级为全解**、§7.5 **已定**。新增代码 ≈**130 行**（FactorCombiner ~55 + neutralize_factor ~25 + AlphaSignal ~30 + signal_to_weights ~20）。

> **第三十三批已解决（2026-10-04）**：§8 ML/DL 信号管理八问 → §64。Confluence 参考（page 301832392 教程 §13 ML 合成多重检验 / §16.4 回测实盘 gap / §18.3-18.4 衰减退出 / §7.8 模型级泄漏 / §25 单机原则；page 293740062 UModel 平台；page 288524848 Alpha 入库 ML 信号同入库）；**输出映射**：截面分数默认直接 z-score / 概率 logit 后 z-score / 期望收益同分数；分位数分组不支持（属 to_weights §63.5）、二值不推荐（丢强度）；MLSignalAdapter.from_scores/from_probabilities + smooth_signal() EWM halflife=5（日频可选月频不开——换手瓶颈在再平衡频率；平滑引入滞后，alpha 衰减快的因子不平滑）；**特征管理**：特征=因子值+原始量价，**复用因子表不建特征表**（Feature Store 不做违反 §4；因子⊂特征——入库因子必然可作特征，ML 特征集可更大含量价原始变换）；build_feature_matrix(feature_specs) 拼装；**features_hash=hash(feature_specs+depends_hash) 进 model_id**（复用 §58/§62 版本化）；特征筛选**人工优先 5-15 个**，PCA/Lasso 不做（解释性差+样本不足选择不稳，教程 §13 警告超参自由度=多重检验）；**模型生命周期**：扩张窗口训练（起点固定终点滚动——A 股历史短滚动 24 月样本不足）+ **年度重训默认** + KS 检验 p<0.05 分布漂移触发提前重训；时间切分 train/val/test + **Walk-Forward 禁 KFold**；部署 `data/models/{name}/v{major}_{minor}/`（model.pkl+config.json+metadata.json）；**model_id=name+major(结构/特征变更)+minor(训练日期)+train_end+features_hash**；退役复用 §18.4/§62.4 退出判据不另起一套；A/B 手动 t-test 不做自动影子流量；**实验管理**：experiments/{name}/ 四件套（config/metrics/notes/model）**不用 MLflow/W&B**（外部服务依赖，单人 35 实验 JSON 目录足够 grep/git 友好）；**晋升线** test_ic>0.04+test_ir>0.5+test_sharpe>1.0+**优于等权 baseline**（§7.4 铁律）；**可复现五要素** random_seed/依赖版本 uv.lock hash/特征列表/日期范围/超参缺一不可复现；**ensemble**：ensemble_models() **信号层加权平均**复用 FactorCombiner 逻辑默认等权（防权重优化过拟合）；**不做模型层平均**（logits/参数对齐深水区）；对应 §65.1 signal_source.type="ensemble"；alstm/ hybrid.py 与 low_volatility_model_a_mixer.py 的收口方向；**衰减监控**：**复用 §62.4 三态 healthy/warning/exit 同一阈值体系**（rolling IC<50% 告警 <30% 退出 / rolling 6 月夏普<0 退出 / 回测实盘偏差<70% 查执行滑点与数据差异）；warning=重训+ensemble 降权，exit=停用回退等权主路径；**基础设施**：本地 CPU/单 GPU（LSTM/LightGBM 分钟级）——不建分布式训练/任务队列/Web 看板；超参搜索手动网格+记 attempt_log N（自动调参=多重检验放大器）；**过拟合防控铁律四条**（禁 KFold / 测试集只看一次 / 必须跑赢等权 / 记 attempt_log 抬 t=√(2lnN)）+ 过拟合检测表（test_ic<0.5×train_ic SUSPECT / 夏普>2.0 SUSPECT 查前视 / 未超等权 10% REJECT / 单特征重要性>50% SUSPECT 查目标泄漏 / 换手>200% SUSPECT）+ sanity_check_ml_model() 三检函数；决策汇总 22 行落盘（含 §59 MLFactor 衔接、§63.5 from_ml_factor 出口、alstm/ 35 脚本收口方向、§27.9 超参搜索计 N）。§8.1–8.8 **全部已定**（八问全关）。新增代码 ≈**160 行**（MLSignalAdapter ~25 + smooth_signal ~15 + build_feature_matrix ~25 + ensemble_models ~20 + sanity_check ~15 + model_id/实验目录约定文档化 ~60）。

> **第三十四批已解决（2026-10-04）**：§9.1–§9.6 alphaType 六问 → §65。Confluence 参考（page 335950197 PMS ms_calculator CalculatorRegistry+YAML+BaseCalculator / page 316060789 风控指令 Decision Engine control_key / page 301832392 教程 §9 仓位三维+§17.5 拆单 / page 288524848 Alpha 入库生命周期 / page 324637543 事件驱动调度不适用）；**alphaType=策略配方**（signal_source{type multi_factor|ml_factor|ensemble, factors, combiner_method, post_process} + rules[{rule_id,params}] + params）——不是代码本身，代码在 strategies/ 配方引用它；**注册表 JSON 双表** dim-alphaType-meta-info（alpha_type_id/name/description/version/signal_source/rules/asset_pool/frequency/author/created_at/updated_at/tags 15 字段）+ dim-alphaType-publish（publish_id/config_snapshot `path@hash`/status/published_at/adopted_at/retired_at）存 `data/registry/alpha_types.json + alpha_type_publishes.json`——**不用 SQLite**（schema 迁移问题无并发需求）**不用 YAML**（§13.1 TOML 分工：注册表=数据、config=参数）；版本化=git history 追溯配置变更；**标签 7 维全枚举禁自由文本**（asset_type/frequency/holding_period/style/capacity/turnover/status）防"momentum/Mom/动量"碎片化；打标时机表（创建必填/发布校验完整性/晋升可修正 capacity/turnover/退役自动跟随）；list_alpha_types(filters) 过滤查询；**生命周期五态** draft→published→production→deprecated→archived + 转换条件表（published→production 需 **Walk-Forward 通过+§64.5 晋升线+优于等权**；deprecated→archived 确认无回测配置引用；任意→draft 回炉 version+1）；**无审批流** promote_alpha_type() 自动检查指标 + log_audit → `data/registry/audit_log.jsonl` **append-only 不可改**（对标公司发布流水线审计）；**规则库**：4 分类 signal_transform/filter/position/risk_gate（对标 Decision Engine 控制指令三态放行/约束/拦截）+ Rule ABC（rule_id/category/param_schema + apply(data,params)→DataFrame）+ **param_schema 启动时校验**（配置错误即刻暴露）+ RulePipeline **串联不支持并联/嵌套**（YAGNI）；**规则 vs 算法变体判据表**：改它只影响精细度（k=10→20）/可独立开关 A/B/通用件复用→规则；策略逻辑本质变化（动量→反转）/需独立回测评估→新 alphaType；**Signal 调度**：日频 15:30 后（收盘数据就绪）/月频月末/盘中不做（盘中实时是公司需求）；**cron+手动不事件驱动**（schedule_config cron "30 16 * * 1-5"，教程 §25.2 个人 Job<10）；**失败=重试 1 次→hold 上一交易日 Portfolio→告警，不清仓**（数据故障不是平仓理由，与 §10.2 风控精神一致）；run_daily_signal() 是 §11.1 daily_pipeline 的一个 Job；**约束求解**：约束集 6 行表（集中度等权天然满足检查告警不修正/换手检查告警不硬裁/行业暴露不做/资金简单乘法/杠杆不做/市场中性不做）；**求解器=解析解等权默认**（约束集里无真正优化问题——scipy 不要、cvxpy V2 备选给 risk parity/BL）；build_portfolio()（signal_to_weights→总仓位缩放→集中度检查→换手检查）+ compute_turnover=Σ|Δw|/2（教程 §10.6）；求解失败 V1 不发生 V2 放松→回退等权不空仓；性能全 A <100ms 非瓶颈；决策汇总 22 行落盘（含 §63.5 signal_source 三态、§13.1 config 分工、§17.2/§34 权重四法对齐）。§9.1–9.5 **已定**、§9.6 **升级为全解**。新增代码 ≈**200 行**（Rule ABC+TopKFilter+RulePipeline ~70 + promote/log_audit ~35 + list_alpha_types ~15 + build_portfolio/compute_turnover ~50 + run_daily_signal ~30）。

> **第三十五批已解决（2026-10-04）**：§10 执行域四问 → §66。Confluence 参考（page 301596677 hubble_utrade ITraderLib+TTPolicy 链+RiskManager / page 282827800 交易系统 OnAlphaUpdate→OrderSchedule+sid/vasid / page 316060789 风控指令 Decision Engine 三态 / page 301832392 教程 §17 执行算法+§3.2 成本+§9 仓位三维+§25.2 框架观）；**订单生成**：公司链路 OnAlphaUpdate→OrderSchedule→TTPolicy 链→TraderLib.NewOrder→DataBus→hubble_tb→柜台 的个人简化=**目标权重 diff→订单列表**；compute_order_diff()（floor(w×nav/price/lot_size)×lot_size，lot_size=100；diff=target−current；BUY/SELL）；挂单 V1 不处理；**拆单不做** check_split_needed() 仅告警（教程 §17.5 >1% 日成交量才拆——月频 ETF/大盘股远不到）；执行优先级**先卖后买**+卖出按市值 desc+买入按信号 desc；LIMIT 单 limit_price=close；**框架选型：pandas 自研 <300 行**（教程 §25.2"能用 pandas 别上框架"——backtrader/vnpy 学习成本>收益）；**风控**：公司两层 RiskManager（ArbiWrapper/RiskForceManager 硬拦截）+ Themis（Decision Engine 放行/约束/拦截三态）→ 个人**防策略失控/操作错误**（非防市场风险）；**七规则表**（单标的≤25% BLOCK / 总仓位≤95% BLOCK / 单日换手≤150% 告警不拦 / 单笔≥5000 跳过 / 禁做空 BLOCK / 禁 ST BLOCK / 流动性>1% 日成交量告警）；**只 pre-trade 不 intra-trade**（月频无盘中需求）；**只 PASS/BLOCK 两级**不做"约束"态（三态是公司多策略冲突仲裁需求）；RiskCheckResult + pre_trade_risk_check() 逐规则一 BLOCK 即拒；换手/流动性**告警不拦截**（硬拦破坏信号）；risk_audit.jsonl append-only 审计；**撮合账本**：**收盘价成交 V1**（无价 REJECTED=停牌；部分成交不模拟；保守版 T+1 开盘见 §18.2——黄金样本对拍用手算口径一致）；**费用四件套**（佣金万 2.5 双边最低 5 元 / 印花税万 5 卖出 ETF 免 / 过户费万 0.1 / 滑点 0.1% 方向不利买+卖−）；FeeModel._is_etf()（51/15/16 前缀）与 §13.4 ASSET_RULES 对齐；成本量级 ETF 单边 0.15-0.2% vs 股票 0.35-0.4%，月频年成本 2-5% alpha 必须覆盖；SimAccount（cash/holdings/cost_basis/trade_log + execute_fill/get_nav/get_snapshot）对标 hubble_tb；多品种不预先抽象 V1 仅 A 股（期货保证金逐日盯市另一套账本）；**柜台抽象**：Broker ABC **4 方法封顶**（submit_orders/get_holdings/get_cash/get_nav）不做 cancel/modify/行情/事件回调；SimBroker 封装风控+撮合+费用+记账（回测与 paper trading 共用）；**V2 路径 QMT>easytrader>券商 API**（QMT mini-qmt 本地 Python API 完善；easytrader 逆向不稳；自研柜台协议不做）；**不做 vnpy Gateway**（引入整个 vnpy 生态不值）——**修正 §22.5/§24.4 早期结论，详见 §68**；create_broker(mode,config) 工厂 sim/qmt 切换=回测与实盘同一代码路径；**全链路回测主循环**（rebalance_dates → build_alpha_signal → build_portfolio → generate_orders → submit_orders → record_nav）+ **代码量预算 <500 行**（订单 100+风控 80+费用账本 100+Broker 50+主循环 70）；**验证 4 条**：黄金样本等权 top5 ETF 手算对拍 <0.01% / 费用模型手算对拍到分 / 风控 7 条逐条触发 / 订单差分手算 shares 对拍；决策汇总 18 行落盘（含 §17.3/§34.4 手数取整落地、§13.4 ASSET_RULES 对齐、§65.7 主循环衔接、data/execution/ 落盘位置）。§10.1–10.4 **全部升级为全解**。新增代码 ≈**280 行**（compute_order_diff+generate_orders ~100 + pre_trade_risk_check+RiskCheckResult ~80 + FeeModel+SimAccount ~100 + Broker ABC+SimBroker+create_broker ~50 + run_backtest 主循环 ~70，部分与 §17/§23 已有设计重叠）。

> **第三十六批已解决（2026-10-04）**：§11 运维域四问 → §67。Confluence 参考（page 316050560 Datamall Dolphin→Kafka→TriggerRouter→Celery→Ray 全家桶 / page 342919374 驾驶舱 4 类异常+severity 只升不降+白名单 / page 223886928 5 色灯 / page 301832392 教程 §18.6 归因+§18.3-18.4 衰减+§16.4 gap / dw_asset_job_instance 作业实例表）；**调度**：5 类 Job 表（数据采集 16:00 retries=2 / 因子计算 16:30 / 信号产出 16:45 / 回测手动 / 监控巡检 17:00）；**APScheduler + 薄封装 V1**（进程内零部署 CronTrigger 直观）——不用 crontab（太原始无重试无状态）不用 Prefect/Celery/Airflow（服务化部署违反零运维，Job<10 用不上 DAG）；**不用 DAG**（依赖线性 for 顺序执行，V2 Job>20 再迁 Prefect）；**job_runs.jsonl**（run_id/job_name/timestamps/status SUCCESS|FAILED|RETRY|SKIPPED/duration/error/retry_count/output_summary）对标 dw_asset_job_instance；run_job() 重试封装（默认 1 次 300s 超时）；**失败传导两规则**：数据失败→跳过下游（无数据算不了），因子/信号失败→**hold 上日 Portfolio 不清仓**（§65.6 铁律）；**监控**：策略级 8 指标表三档（累计收益 / rolling 6 月夏普 0.5/0.3/0 / MDD 回测×1.2/×1.5 / 超额 6 月/1 年 / rolling IC 70%/50%/30% / 持仓数 / 换手 / 单笔成本 0.4%/0.5%）+ 系统级 4 指标（Job 成功率 rolling7d<80% / Pipeline 耗时>30min / 数据新鲜度>1 天 / 条数偏差>30%）；metrics.jsonl 时点序列；**Markdown 日报** data/ops/reports/daily_{date}.md（指标表+告警+nav 曲线）**不做 Web 看板**（V2 Streamlit 需要时再加）；**归因**：**Alpha-Beta 单因子回归 V1 默认**（教程 §18.6 散户够用，回答"赚的是 alpha 还是 beta"）/ FF3 季度手动备选 / **Brinson+Barra 不做**（基准组合与行业配置对照是机构需求）/ 因子贡献拆解做（复用 §63.6）；alpha_beta_attribution()（linregress → alpha_annual=intercept×12 + beta + r² + t_alpha）；归因频率表（Alpha-Beta+贡献月自动 / FF3 季手动 / 专项按需复盘）；**解读三线**（Alpha 年化 3-5% 优秀散户 / R²>0.7 alpha 可信 R²<0.5 样本不足 / **Alpha>8% 查回测问题**——太高不真实）；报告 attribution_{id}_{quarter}.md；**告警**：3 类（阈值/突变/趋势）+ **8 具体条件表**（Job 失败 HIGH / 数据延迟 HIGH / **MDD 突破×1.5 CRITICAL** / **夏普转负 CRITICAL** / 超额持续负 6 月 HIGH / NAV 单日<-3% MEDIUM / 换手>50% MEDIUM / 风控拦截>3 笔 MEDIUM）；通道分层（终端全级别 V1 + **桌面通知 CRITICAL** plyer|notify-send + 邮件 V2 + 飞书/Grafana 不做）；AlertLevel+send_alert+log_alert → alerts.jsonl；**告警治理三原则**（去重 key `{date}_{alert_type}_{alpha_type_id}` 防轰炸 / severity **只升不降** / 白名单维护期静默）——**不做动态升级**（误报放大器）；**daily_pipeline 全链路树**落盘（16:45 数据→因子→信号→巡检→日报 + 月末归因自动）；**验证 5 条**：调度 5 Job 顺序执行 / 失败传导 SKIPPED+告警 / **归因手算测试 α≈0.12 年化 β≈0.8 容差 0.02**（构造 r_p=0.01+0.8·r_b）/ 告警去重同日两条仅一落盘 / 日报齐全性；决策汇总 18 行落盘（含 §65.6 衔接、§62.4/§64.7 同一阈值体系、§63.6 复用、data/ops/ 落盘位置定稿）。§11.1–11.4 **全部已定**。新增代码 ≈**220 行**（run_job+daily_pipeline ~70 + write_daily_report ~35 + alpha_beta_attribution ~25 + send_alert/log_alert ~30 + 指标采集 ~40 + 杂项）。

> **第三十七批已解决（2026-10-04）**：§12 六条旧决策重审 → §68。Confluence 参考（公司 starfleet/EBReplay/EBClock/EBTimer 四层时间模型 / EBGraph EventBrick 事件订阅图 / page 288524848 Udata ClickHouse / page 301832392 教程 §2 双重动量+§16.4 合理区间+§25.2 框架观；strategies/futures/alstm/baseline_spec.py 旧基线）；背景：ARCHITECTURE.md 旧"重引擎"设计 vs §3-§11 实际收敛的"薄壳+pandas 优先"——两套并存实施无所适从，逐条裁决；**裁决 4 废弃**：①四层时间模型→for-loop 逐再平衡日推进（"计算量与数据量解耦"月频天然消失——全 A <100ms；tick 对齐/事件推进/时钟单步五项不需要表）②EBGraph/EventBrick→Factor ABC+FactorDAG（事件订阅动态 push vs 拓扑静态 pull 对比表；三理由：月频无实时事件/依赖静态/DataFrame 天然批量；组织链 Factor→DAG→PostProcessor→per-factor-one-table→Combiner→AlphaSignal="DataFrame 流水线"）③ClickHouse→Parquet+DuckDB 双件套（单机单用户 <10GB、独立部署违反零外部依赖；公司 CK 是实盘多策略并发查询需求；框架存储偏好表 alphalens 直接兼容 backtrader/vnpy 不用）④期货黄金样本→**废弃重建**；**裁决 3 简化保留**：⑤五域骨架 kernel/→**core/**（从"引擎"变"共享工具" config.py/registry.py/logging.py/types.py/asset_rules.py——无引擎/无事件循环/无时间模型；职责分布表；边界不重画：时间推进→回测脚本 for-loop、事件分发→不需要、Signal→AlphaSignal、数据接口→DataAPI）⑥Signal 三层（BlueShift/USignal/Tick）**坍缩为一层 AlphaSignal**（USignal/Tick 是盘中实时需求月频不涉及）⑦**五层叙事→三步** Signal→Portfolio→Execution——**Utrade/Themis/TB 对应物=broker.submit_orders() 内部三步（风控+撮合+记账）不是三个模块**（接口边界从"产品"移到"函数"）——**P0 尾项 §12.4 正式关闭**；**裁决 3 保留**：⑧Portfolio 自研 build_portfolio ⑨五域划分边界 ⑩对拍验证方法；**黄金样本重建**：旧 baseline_spec.py（sharpe 16.001135 不可信——过拟合或口径问题 / total_return 0.860638 口径不明 / trade_count 12686 暗示高频 / 期货 ALSTM vs V1 A 股 / VectorBT/ZVT vs pandas）**保留作期货 ALSTM 策略自身回归基准但不再是平台级黄金样本**；新基线=**等权 top-5 ETF 动量**（教程 §2 双重动量变体，≤30 行 akshare 可重现）；**五步验证法**（手算 Excel 4 位小数→pandas <0.01%→SimBroker 全链路 <0.01%→扣费与 FeeModel 手算一致→2020-2024 五年基线落盘做回归基准）；预期范围教程 §16.4 年化 10-15%/夏普 0.8-1.2/MDD 15-25%；**超范围排查表**（年化>20% 查未来函数 / 夏普>2 查前视 / MDD<10% 查漏极端行情段 2022/2024 / 费用<1% 查 FeeModel 漏计）；**对 §3-§11 影响=零**（重审是让 ARCHITECTURE.md 对齐现实不是推翻重来——唯一同步项=文档改写+目录名 kernel→core）；**验证 3 条**：ARCHITECTURE.md 逐条核对无四层时间模型/EBGraph/ClickHouse/三层 Signal 残留 / 五步基线跑通数字落区间 / core/data/research/execution/ops 五目录容纳 §57-§67 全模块；决策汇总 10 行落盘（4 废弃/3 简化保留/3 保留）。§12.1–12.6 **全部已定**。新增代码 ≈**40 行**（golden_baseline() 骨架 ≤30 行 + 超范围排查约定）。

> **第三十八批已解决（2026-10-04）**：§13 横切四问 → §69。Confluence 参考（page 301596677 hubble_utrade DataTraits<Category,Market,Exchange> C++ Traits 编译期多态+ParseConfig XML / page 316050560 Datamall trigger_history+Prometheus+PipelineConfigWatchTask / page 301832392 教程 §15 A股特殊机制+§11.2 期权不推荐散户）；**配置**：格式选型表——YAML **不选**（缩进敏感+on→True 陷阱）/ **TOML 选**（类型严格+tomllib 零依赖）/ XML 不选（C++ 生态惯例）/ dataclass 仅作加载后类型载体；**修正 §35.6**（YAML 一个文件不分层 → **TOML 三层**）：config/config.toml（paths/data/execution mode=sim|qmt/ops schedule）+ config/strategies/{alpha_type}.toml（meta/signal/portfolio/risk 对齐 §65）+ config/.env（AKSHARE_PROXY 等**不入 git 不打印**，先例 .my.prompt.cnf）；load_config/load_strategy_config（策略级覆盖全局 {**global,**strategy}）/load_secrets 三函数落盘；**不做配置中心**（单机 TOML+git 即是；公司 PipelineConfigWatchTask 是多人需求）；**审计=git log + config_snapshot**；不热更新沿用；**日志**：Python logging+**JsonFormatter**（ts UTC/level/module/func/msg 五字段+extra+exception）→ console 人读+logs/app_{YYYYMMDD}.jsonl 机器查；级别规范 ERROR/WARNING/INFO/DEBUG 表；**分布式追踪全不需要**（对照表：instance 时间戳/Prometheus/trigger_history/trace_id → 单进程用 duration_ms+grep+jq+job_runs.jsonl 替代）；**logs/runs/ 三 schema**（jobs.jsonl 对齐 §67.2 / backtests.jsonl 含 metrics+**config_snapshot path@hash** / live.jsonl）；对标 Datamall NFS→MinIO 降级——个人本地 JSONL 零运维不上 ELK；**测试**：**pytest 三层** tests/unit|integration|golden；**9 模块单测覆盖表+hypothesis 性质测试**（FeeModel fee≥0 且卖出≥买入/zscore mean≈0 std≈1/weights.sum()≤1+1e-9/cash+mkt_value==nav/order_diff lot 整数倍/DAG 环检测/配置覆盖/重试耗尽/未知 asset KeyError）含 test_fee_non_negative @given 代码；集成 3 场景（数据管道/因子管道/回测主循环）；**测试替身表**（akshare→fixture Parquet/DuckDB→内存/SimBroker 直接用/时钟→固定日期注入）；**golden test** 衔接 §68.7 五步验证 2–4 步自动化（TOLERANCE=1e-4、test_pandas_matches_hand_calc/test_simbroker_matches_pandas、fixtures golden_prices.parquet 5 ETF×3 月+golden_expected.json 手算）；**V1 无 CI** 本地三档耗时预算 unit<10s/integration<60s/golden<5min，V2 GitHub Actions；**多资产**：公司 C++ Traits 编译期多态 **不学**（月频 dispatch 开销可忽略，Traits 是 tick 级性能需求）；**配置区分+ASSET_RULES 规则表+execution 层处理差异**；差异落层表（T+1→generate_orders/涨跌停禁做空→risk_check/印花税手数→FeeModel+diff/ST→research filter/板块→data ticker 编码）；**关键洞察**：A股差异几乎全落 execution/+data/，research/ 因子计算对资产透明；期货期权保证金逐日盯市是另一套账本不复用 SimAccount；分阶段 V1 股票+ETF→V2 可转债→V3 期货→V4+ 期权（教程 §11.2 不推荐散户）；core/asset_rules.py **ASSET_RULES**（stock: T+1/lot 100/stamp_tax True/price_limits main0.10 gem0.20 star0.20 bse0.30/st_limit 0.05/short False；etf: stamp_tax False；futures/option V2 预留）+ get_asset_rules() 未知类型不静默回落；**对 §3–§12 影响**：§35.6 配置**修正**（敏感信息与版本化审计两项原开放项就此关闭）/ §66.4 FeeModel._is_etf 与 ASSET_RULES 同源一致 / §66.3 风控七规则参数来源=ASSET_RULES / §67.2 job_runs.jsonl 与 logs/runs/jobs.jsonl 统一 / §68.4 core/ 增 config.py+logging.py+asset_rules.py / §68.7 黄金样本 2–4 步自动化；**验证 4 条**：配置合并覆盖+.env 不泄漏 / JsonFormatter 行合法 JSON 五字段齐全 / golden 两 test 过容差 1e-4 / get_asset_rules(etf) 与 FeeModel._is_etf(510300) 印花税判定一致且 60/00/30/68/8 板块映射正确；决策汇总 4 行落盘（一问一行）+ 修订表 6 行。§13.1–13.4 **全部已定**。新增代码 ≈**120 行**（load_* 三函数 ~35 + JsonFormatter/setup_logging ~40 + ASSET_RULES/get_asset_rules ~30 + hypothesis 示例 ~15）。

**仍未解决的重点 P0**：无——P0 全部关闭（§8.1–8.3 / §9.1 / §12.4 已于第三十三/三十四/三十七批落定）。

讨论完一条就把结论回填到 ARCHITECTURE.md，并在本文件标注 `状态：已定 → 见 ARCHITECTURE.md §x.y`。
