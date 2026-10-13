# 项目文档

项目文档统一使用小写 `docs/`，不再创建 `Doc/`、`doc/` 或其他大小写变体。

## 平台设计（design/）

- [架构总纲](design/ARCHITECTURE.md) —— 唯一权威架构文档：五域骨架、三步主链、选型定稿、跨域契约、迁移阶段
- [实施路线图](ROADMAP.md) —— 施工顺序：阶段序列与 DoD、黄金样本五步验证、迁移路径、流程约定、设计缺口清单
- 五域模块文档（文件名与未来代码模块同名）：
  - [core/](design/core/) —— config · logging · testing · asset_rules · registry · types
  - [data/](design/data/) —— source_adapter · ingestion · dqc_* · quarantine_raw · calendar_adjustment · data_revision · storage_* · data_api · l2_data
  - [research/](design/research/) —— factor_* · post_processor · alpha_signal · ml_signal · alpha_type · cross_sectional_backtest
  - [execution/](design/execution/) —— portfolio_construction · order_generation · risk_control · fee_model · ledger · broker
  - [ops/](design/ops/) —— scheduler · monitoring · reporting · alerting

## 项目结构

- [项目结构与边界](PROJECT_STRUCTURE.md) —— 现行代码、研究实验、运行产物的放置规则

## AmazingData

- [SDK API Reference](AmazingData.md)
- [开发指南摘要](AmazingData开发指南摘要.md)
- [技术指标整理](AmazingData技术指标.md)
- `AmazingData开发手册.pdf`：本地官方手册，PDF 按仓库规则不进入 Git

## 数据处理

- [期货 Level-2 五档数据处理](data/future_level2_data_processing.md)

## 归档（archive/，不维护）

- [OPEN_QUESTIONS.md](archive/OPEN_QUESTIONS.md) —— 83 问 + §16–§69 讨论结果全史（含被否决选项与理由）；结论已蒸馏进 design/ 各模块 md
- [ARCHITECTURE.md](archive/ARCHITECTURE.md) —— 旧版总纲（四层时间模型 / EBGraph / ClickHouse / 三层 Signal，已被 §68 重审修正）

策略相关研究报告跟随所属策略保存，例如 ALSTM 报告位于 `strategies/futures/alstm/reports/`；机器生成的报告和图表位于 `artifacts/`。
