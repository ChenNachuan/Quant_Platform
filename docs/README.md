# 项目文档

项目文档统一使用小写 `docs/`，不再创建 `Doc/`、`doc/` 或其他大小写变体。

## 平台设计（design/）

[架构总纲](design/ARCHITECTURE.md) —— 唯一权威架构文档：五域骨架、三步主链、选型定稿、跨域契约、迁移阶段

```
design/
├── ARCHITECTURE.md
├── core/
│   ├── asset_rules.md
│   ├── config.md
│   ├── logging.md
│   ├── registry.md
│   ├── testing.md
│   └── types.md
├── data/
│   ├── calendar_adjustment.md
│   ├── data_api.md
│   ├── data_revision.md
│   ├── dqc_rules.md
│   ├── dqc_runtime.md
│   ├── dqc_severity.md
│   ├── ingestion.md
│   ├── l2_data.md
│   ├── qdc_migration_contract.md
│   ├── quarantine_raw.md
│   ├── source_adapter.md
│   ├── storage_ops.md
│   └── storage_schema.md
├── research/
│   ├── alpha_signal.md
│   ├── alpha_type.md
│   ├── cross_sectional_backtest.md
│   ├── factor_combiner.md
│   ├── factor_evaluation.md
│   ├── factor_governance.md
│   ├── factor_interface.md
│   ├── factor_runtime.md
│   ├── factor_storage.md
│   ├── ml_signal.md
│   └── post_processor.md
├── execution/
│   ├── broker.md
│   ├── fee_model.md
│   ├── ledger.md
│   ├── order_generation.md
│   ├── portfolio_construction.md
│   └── risk_control.md
└── ops/
    ├── alerting.md
    ├── monitoring.md
    ├── reporting.md
    └── scheduler.md
```

## 项目结构

- [项目结构与边界](PROJECT_STRUCTURE.md) —— 现行代码、研究实验、运行产物的放置规则

## 数据源（data_source/）

- **AmazingData**（银河证券星耀数智）
  - [SDK API Reference](data_source/AmazingData/AmazingData.md)
  - [开发指南摘要](data_source/AmazingData/AmazingData开发指南摘要.md)
  - [技术指标整理](data_source/AmazingData/AmazingData技术指标.md)
  - `AmazingData开发手册.pdf`：本地官方手册，PDF 按仓库规则不进入 Git
- **QDCData** — 待补充
- **StraightFlush** — 待补充
- **Tushare** — 待补充

策略相关研究报告跟随所属策略保存，例如 ALSTM 报告位于 `strategies/futures/alstm/reports/`；机器生成的报告和图表位于 `artifacts/`。
