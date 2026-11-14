# 平台设计文档总览（`docs/design/`）

> 唯一权威架构总纲：[ARCHITECTURE.md](ARCHITECTURE.md) · 仓库文档总入口：[docs/README.md](../README.md) · 代码放置边界：[PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md)

本目录存放量化平台的五域设计契约（40 份模块文档，含 1 份契约软链共 41 项）、`asset/` 资产表分域字段字典（8 份），以及 `.diagrams/` 下的 14 份 Archify 交互式架构与流程示意图（含 `.json` 源定义、`.html` 交互图与 `.png` 静态预览）。

## 整体结构

```text
docs/design/
├── ARCHITECTURE.md                  # 唯一权威架构总纲：五域骨架、三步主链、选型定稿、红线
├── README.md                        # 本文件：design/ 目录结构、各域文档索引与示意图目录
├── .diagrams/                       # 14 份 Archify 交互式架构/数据流/状态机示意图（.json/.html/.png）
├── core/                            # 共享基础设施（6 份：无跨域名依赖）
│   ├── asset_rules.md               # 多资产规则查表 & ticker 前缀/正则校验（92/52/53/55、指数、基金）
│   ├── config.md                    # 三层 TOML + .env 配置加载与 config_snapshot
│   ├── logging.md                   # Python logging + JSONL 运行记录
│   ├── registry.md                  # FactorRegistry 注册与生命周期状态管理
│   ├── testing.md                   # pytest 三层测试（unit / integration / golden）与容差标准
│   └── types.md                     # 跨域共享数据类与枚举索引
├── data/                            # 数据接入、存储、DQC 与查询（13 份 + asset/ 8 份）
│   ├── storage_schema.md            # 52 张规范表总览：asset/（51 表）与 factors/（1 表）平级，无独立 ext/
│   ├── asset/                       # asset/ 51 张规范表分域字段字典（47 A 股 + 4 期货）
│   │   ├── README.md                # 分域总索引、通用类型/可空整型/quality_flag 约定
│   │   ├── 01-stock-core-and-status.md      # 日历 + 股票核心行情、估值与 cn_stock_daily_status（7 张）
│   │   ├── 02-stock-index-and-board.md      # 指数、行业分类与成分权重（4 张）
│   │   ├── 03-stock-fundamental-pit.md      # cn_stock_fundamental_* PIT 财报/业绩/一致预期（7 张）
│   │   ├── 04-stock-flows-and-events.md     # 资金流/情绪/筹码 + #29 股东资本事件拆表（17 张）
│   │   ├── 05-stock-tick-l2.md              # 股票逐笔成交与十档 L2 盘口（2 张）
│   │   ├── 06-etf-and-fund.md               # 独立 ETF 域（5 张，不含 pcf）+ 场外/LOF 公募基金（5 张）
│   │   └── 07-futures.md                    # 期货日线、五档 L2、合约规格与主连（4 张）
│   ├── data_api.md                  # DuckDB read_parquet 查询层与 load_bars() 统一接口
│   ├── storage_ops.md               # 原子写入、FileLock、按日/月/年分区合并写入
│   ├── ingestion.md                 # 多源数据仲裁（config/source_priority.yaml，4 个激活源）
│   ├── source_adapter.md            # 源适配器四方法契约（tushare / qdc / amazingdata / straightflush）
│   ├── quarantine_raw.md            # 原始层与隔离区管理、Normalizer 标准化
│   ├── calendar_adjustment.md       # 交易日历、复权因子与期货连续合约换月
│   ├── data_revision.md             # 数据修订检测、应用与影响传播
│   ├── dqc_rules.md / dqc_runtime.md / dqc_severity.md   # DQC 规则、执行预算与质量位掩码
│   ├── level2_data.md               # Level-2 微观结构与高维快照规范
│   └── QDC_ingestion_contract.md    # QDCData 落盘布局与字段映射契约（qdc_migration_contract.md 为其软链）
├── research/                        # 投研与因子流水线（11 份）
│   ├── factor_interface.md / factor_runtime.md / factor_storage.md
│   ├── post_processor.md / factor_combiner.md / factor_evaluation.md / factor_governance.md
│   ├── alpha_signal.md / alpha_type.md / ml_signal.md / cross_sectional_backtest.md
├── execution/                       # 组合构建、风控、费用、账本与执行（6 份）
│   ├── portfolio_construction.md / order_generation.md / risk_control.md
│   └── fee_model.md / ledger.md / broker.md
└── ops/                             # 调度、监控、日报与告警（4 份）
    └── scheduler.md / monitoring.md / reporting.md / alerting.md
```

## 交互式架构与流程示意图（`.diagrams/`）

| 编号 | 主题 | 类型 | 关联文档 | 预览 / 交互图 |
|---|---|---|---|---|
| 01 | 五域骨架与单向依赖边界 | `architecture` | [ARCHITECTURE.md](ARCHITECTURE.md) | [PNG](.diagrams/01-five-domain-architecture.png) · [HTML](.diagrams/01-five-domain-architecture.html) |
| 02 | 核心投研与执行主链数据流 | `dataflow` | [ARCHITECTURE.md](ARCHITECTURE.md) | [PNG](.diagrams/02-main-pipeline-dataflow.png) · [HTML](.diagrams/02-main-pipeline-dataflow.html) |
| 03 | `$QUANT_DATA_ROOT` 五层存储拓扑 | `architecture` | [data/storage_schema.md](data/storage_schema.md) | [PNG](.diagrams/03-storage-topology.png) · [HTML](.diagrams/03-storage-topology.html) |
| 04 | Raw 原始层与 Quarantine 隔离闭环 | `dataflow` | [data/quarantine_raw.md](data/quarantine_raw.md) | [PNG](.diagrams/04-quarantine-raw-dataflow.png) · [HTML](.diagrams/04-quarantine-raw-dataflow.html) |
| 05 | Level-2 / Tick 离线因子与 LOB 路径 | `dataflow` | [data/level2_data.md](data/level2_data.md) | [PNG](.diagrams/05-level2-dataflow.png) · [HTML](.diagrams/05-level2-dataflow.html) |
| 06 | 多源入库仲裁、补缺与交叉校验 | `workflow` | [data/ingestion.md](data/ingestion.md) | [PNG](.diagrams/06-multi-source-ingestion.png) · [HTML](.diagrams/06-multi-source-ingestion.html) |
| 07 | `ingest_with_dqc` 七步入库与三级拒收闸门 | `workflow` | [data/dqc_severity.md](data/dqc_severity.md) | [PNG](.diagrams/07-dqc-seven-step-gate.png) · [HTML](.diagrams/07-dqc-seven-step-gate.html) |
| 08 | 数据回溯修正与因子过期重算闭环 | `workflow` | [data/data_revision.md](data/data_revision.md) | [PNG](.diagrams/08-data-revision-loop.png) · [HTML](.diagrams/08-data-revision-loop.html) |
| 09 | `FactorDAG` 拓扑分层执行与增量缓存 | `workflow` | [research/factor_runtime.md](research/factor_runtime.md) | [PNG](.diagrams/09-factor-dag-runtime.png) · [HTML](.diagrams/09-factor-dag-runtime.html) |
| 10 | 因子三态生命周期与健康监控 | `lifecycle` | [research/factor_governance.md](research/factor_governance.md) | [PNG](.diagrams/10-factor-lifecycle.png) · [HTML](.diagrams/10-factor-lifecycle.html) |
| 11 | 信号 → 目标组合 → 交易订单三层换算流 | `dataflow` | [execution/portfolio_construction.md](execution/portfolio_construction.md) | [PNG](.diagrams/11-portfolio-order-dataflow.png) · [HTML](.diagrams/11-portfolio-order-dataflow.html) |
| 12 | 订单六态状态机与冻结资金流转 | `lifecycle` | [execution/ledger.md](execution/ledger.md) | [PNG](.diagrams/12-order-lifecycle.png) · [HTML](.diagrams/12-order-lifecycle.html) |
| 13 | `broker.submit_orders()` 风控撮合记账时序 | `sequence` | [execution/broker.md](execution/broker.md) | [PNG](.diagrams/13-broker-submit-sequence.png) · [HTML](.diagrams/13-broker-submit-sequence.html) |
| 14 | `daily_pipeline` 日频运维编排与失败传导链 | `workflow` | [ops/scheduler.md](ops/scheduler.md) | [PNG](.diagrams/14-ops-daily-pipeline.png) · [HTML](.diagrams/14-ops-daily-pipeline.html) |

## 数据存储与命名约定要点

1. **顶层平级目录**：`$QUANT_DATA_ROOT/` 下 `asset/`（51 张原始与标准资产表）与 `factors/`（因子存储）同级；不再设独立 `ext/` 顶层，原扩展数据全部统一按 `{nation}_{asset_type}_{data_type}` 归入 `asset/`。
2. **单表独立目录**：取消 `cn_reference/` 等混放目录，避免 DuckDB `read_parquet` 跨表 Schema 冲突；详细字段以 [data/storage_schema.py](../../data/storage_schema.py) 为代码权威，人类可读字段字典见 [data/asset/README.md](data/asset/README.md)。
3. **小截面按月合并**：`cn_stock_fundamental`、`cn_stock_daily_status`、`cn_etf_fundamental`、`cn_fund_nav` 等日频小截面表统一采用 `{month}.parquet` 按月分区，避免 `<1MB` 碎文件开销。
