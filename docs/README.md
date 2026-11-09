# 项目文档

项目文档统一使用小写 `docs/`，不再创建 `Doc/`、`doc/` 或其他大小写变体。

## 一、项目结构

- [项目结构与边界](PROJECT_STRUCTURE.md) —— 现行代码、研究实验、运行产物的放置规则

## 二、平台设计（design/）

- [架构总纲](design/ARCHITECTURE.md) —— 唯一权威架构文档：五域骨架、三步主链、选型定稿、跨域契约、迁移阶段

```
design/
├── ARCHITECTURE.md          —— 唯一权威架构总纲，五域骨架与主链契约的裁定依据
├── core/
│   ├── asset_rules.md       —— 多资产交易规则查表：T+1、手数、印花税、涨跌停
│   ├── config.md            —— 三层 TOML + .env 配置加载，含 config_snapshot 快照
│   ├── logging.md           —— Python logging + JSONL 运行记录，logs/runs/ 分类落盘
│   ├── registry.md          —— FactorRegistry 注册与状态管理（draft → production → deprecated）
│   ├── testing.md           —— pytest 三层测试（unit / integration / golden）与容差标准
│   └── types.md             —— 跨域共享数据类索引，只指向归属文档不定义 schema
├── data/
│   ├── calendar_adjustment.md   —— 交易日历、时区、复权因子计算与连续合约换月
│   ├── data_api.md              —— DuckDB read_parquet 查询层 + load_bars() 统一数据接口
│   ├── data_revision.md         —— 数据修订检测、应用与影响传播追踪
│   ├── dqc_rules.md             —— 44 条数据质量规则（27 HARD / 17 SOFT），按资产 YAML 配置
│   ├── dqc_runtime.md           —— DQC 执行时机、报告表结构、性能预算
│   ├── dqc_severity.md          —— HARD/SOFT 分级处置、QualityFlag 位掩码、批量拒收
│   ├── ingestion.md             —— 多源数据仲裁与优先级管理（source_priority.yaml）
│   ├── l2_data.md               —— Level-2 / tick 数据存储因子与数据接口
│   ├── QDCData_ingestion_contract.md —— QDCData → Data 落盘布局与字段映射契约（M1 binding）
│   ├── quarantine_raw.md        —— 隔离区与原始层管理、Normalizer 标准化
│   ├── source_adapter.md        —— 每数据源一个子类，fetch() 返回原始 DataFrame
│   ├── storage_ops.md           —— 原子写入、FileLock、生命周期管理、备份策略
│   └── storage_schema.md        —— Parquet + DuckDB 存储 schema，11 张核心表与演进规则
├── research/
│   ├── alpha_signal.md          —— 统一信号层 AlphaSignal，score → 权重转换
│   ├── alpha_type.md            —— 策略配方治理：注册、七维标签、生命周期
│   ├── cross_sectional_backtest.md —— 横截面组合模拟器 cs_portfolio_sim（pandas 自建）
│   ├── factor_combiner.md       —— 多因子合成（等权 / IC 加权 / IC-IR 加权）
│   ├── factor_evaluation.md     —— alphalens-reloaded 0.4.6 评价流水线与筛选门控
│   ├── factor_governance.md     —— 因子准入/退出标准、血缘追踪、去重机制
│   ├── factor_interface.md      —— Factor ABC 接口定义：depends_on() + compute()
│   ├── factor_runtime.md        —— FactorDAG 拓扑排序、分层并行执行、增量缓存
│   ├── factor_storage.md        —— 按因子名/版本/年份分目录存储，Parquet + metadata.json
│   ├── ml_signal.md             —— MLSignalAdapter 模型适配、模型生命周期、实验管理
│   └── post_processor.md        —— 四步后处理：去极值(MAD) → 填充 → 标准化 → 中性化
├── execution/
│   ├── broker.md                —— Broker ABC 四方法接口，SimBroker/QmtBroker 实现
│   ├── fee_model.md             —— 佣金、印花税、过户费、滑点模型
│   ├── ledger.md                —— 5 个数据类 + 3 个枚举 + Journal 事件流记账
│   ├── order_generation.md      —— 目标持仓 vs 当前持仓 diff → 订单列表
│   ├── portfolio_construction.md —— 约束满足式组合构建（不做优化求解）
│   └── risk_control.md          —— 交易前风控规则，PASS/BLOCK 二值判定
└── ops/
    ├── alerting.md              —— 3 类告警规则、8 种触发条件、分级通知渠道
    ├── monitoring.md            —— 8 项策略指标 + 4 项系统指标监控
    ├── reporting.md             —— 每日 Markdown 报告生成 + alpha-beta 归因
    └── scheduler.md             —— APScheduler 调度、5 种任务类型、daily_pipeline
```

## 三、数据源（data_source/）

### 1. **AmazingData**
  - [SDK API Reference](data_source/AmazingData/AmazingData.md)
  - [开发指南摘要](data_source/AmazingData/AmazingData开发指南摘要.md)
  - [技术指标整理](data_source/AmazingData/AmazingData技术指标.md)

### 2. **QDCData** — 待补充

### 3. **StraightFlush** — 待补充

### 4. **Tushare** — 待补充
