# 实施路线图（ROADMAP）

> 唯一权威架构见 [design/ARCHITECTURE.md](design/ARCHITECTURE.md) 与 `design/<域>/` 模块文档；代码目录边界见 [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)。
> 最后更新：2026-11-14（V1 主链与 M1–M5 归档已落地，本文仅保留阶段状态与未完成推进项）。

## 一、完成状态表（V1 · S1–S13）

| 阶段 | 核心交付 | 状态 |
|---|---|---|
| S1 · core 底座 | `core/{types,config,logging,registry,asset_rules}.py` | 已完成 |
| S2 · 测试骨架 | `tests/{unit,integration,golden}/` 三层体系与黄金样本夹具 | 已完成 |
| S3 · 存储与取数 | `storage_schema`（11 表）/ `storage_ops`（月+日分区）/ `DataAPI` | 已完成 |
| S4–S6 · 因子与信号 | `Factor` / `FactorDAG` / `factor_storage` / `post_processor` / `FactorCombiner` / `AlphaSignal` | 已完成 |
| S7 · 执行与回测 | `ledger` / `fee_model` / `risk_control` / `portfolio_construction` / `order_generation` / `SimBroker` / `cross_sectional_backtest` | 已完成 |
| S8 · 黄金样本闸门 | 五步验证法全绿 + `golden_baseline_2020_2024.json` 落盘 | 已完成 |
| S9–S10 · 数据接入与仲裁 | 五源 Adapters & Normalizers / DQC（44 规则）/ 多源仲裁 / 日历复权 / 数据修订 | 已完成 |
| S11 · 评价与治理 | `factor_evaluation` / `factor_governance` / `alpha_type` / `ml_signal` | 已完成 |
| S12 · 运维闭环 | `ops/{scheduler,monitoring,reporting,alerting}.py` + `daily_pipeline` | 已完成 |
| S13 · 迁移与归档 | M1–M5 旧桥接/ZVT/`examples` 清理、`PROJECT_STRUCTURE` 定稿、L2 路径登记 | 已完成 |

## 二、未完成项与下一步推进方向

### 1. V1 收尾与生产数据落地（近期优先）
- **QDCData 全量入库与跨源对拍**：依据 [QDC_ingestion_contract.md](design/data/QDC_ingestion_contract.md) 完成生产数据补齐，并推进历史 `data_lake/` 迁移清理（批次 M7）。
- **遗留设计缺口收口**：统一涨跌停拒单归属（[risk_control.py](../execution/risk_control.py) vs `fee_model`）、补齐 [monitoring.md](design/ops/monitoring.md) 数据偏差告警规则，并确认期货回归基线 `baseline_spec.py` 替代/补位方案。
- **入口文档对齐**：同步更新根 [README.md](../README.md) 中仍指向已归档 `infra/`、`engine/` 的过时描述。

### 2. V2 · 实盘接入与工程扩展
- **实盘柜台与校准**：基于 [router/xtquant_connector.py](../execution/router/xtquant_connector.py) 实现 `QmtBroker`（`mode: live`），完成回测-实盘对拍与模拟盘校准。
- **可转债与交易增强**：`ASSET_RULES` 扩展可转债（T+0/免印花税）、拆单（TWAP/VWAP）、跨日挂单/市价单，按需引入 `cvxpy` 与 VectorBT 备选。
- **自动化运维**：GitHub Actions CI、Streamlit 看板与邮件告警通道。

### 3. V3+ / Phase 2 · 期货、期权与 L2
- **V3 期货**：独立保证金与逐日盯市账本、连续合约与夜盘归属接入，迁入 [factor_library/futures/](../factor_library/futures/) 与 [strategies/futures/](../strategies/futures/)。
- **V4+ 期权 & Phase 2 L2**：期权支持、[level2_data.md](design/data/level2_data.md) 离线微观结构因子、自适应 DQC 阈值（K09）。
