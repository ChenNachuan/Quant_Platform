# 平台设计文档总览（`docs/design/`）

> 唯一权威架构总纲：[ARCHITECTURE.md](ARCHITECTURE.md) · 仓库文档总入口：[docs/README.md](../README.md) · 代码放置边界：[PROJECT_STRUCTURE.md](../PROJECT_STRUCTURE.md)

本目录存放量化平台的五域设计契约（40 份模块文档，含 1 份契约软链共 41 项）以及 `asset/` 资产表分域字段字典（8 份）。

## 整体结构

```text
docs/design/
├── ARCHITECTURE.md                  # 唯一权威架构总纲：五域骨架、三步主链、选型定稿、红线
├── README.md                        # 本文件：design/ 目录结构与各域文档索引
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

## 数据存储与命名约定要点

1. **顶层平级目录**：`$QUANT_DATA_ROOT/` 下 `asset/`（51 张原始与标准资产表）与 `factors/`（因子存储）同级；不再设独立 `ext/` 顶层，原扩展数据全部统一按 `{nation}_{asset_type}_{data_type}` 归入 `asset/`。
2. **单表独立目录**：取消 `cn_reference/` 等混放目录，避免 DuckDB `read_parquet` 跨表 Schema 冲突；详细字段以 [data/storage_schema.py](../../data/storage_schema.py) 为代码权威，人类可读字段字典见 [data/asset/README.md](data/asset/README.md)。
3. **小截面按月合并**：`cn_stock_fundamental`、`cn_stock_daily_status`、`cn_etf_fundamental`、`cn_fund_nav` 等日频小截面表统一采用 `{month}.parquet` 按月分区，避免 `<1MB` 碎文件开销。
