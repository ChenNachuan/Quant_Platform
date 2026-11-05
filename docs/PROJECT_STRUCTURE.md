# 项目结构与边界

这份文档定义代码、研究实验、运行产物和原始数据的放置规则。新内容按此规则放置，旧内容在回归验证后逐步迁移。

## 目录分层

```text
Quant/
├── config/                 # 可版本化的配置（M1 迁移前为 settings.toml / strategies.yaml）
├── core/                   # types·config·logging·registry·asset_rules 共享工具
├── data/                   # 采集、存储、查询、DQC 数据域
│   ├── adapters/           # 多源适配（akshare / amazingdata / tushare）
│   ├── normalizers/        # 源归一化
│   ├── dqc/                # DQC 规则、检查、引擎、严重度
│   ├── execution/          # 风险审计产物（risk_audit.jsonl，不入 Git）
│   └── ops/                # 运行指标/告警产物（不入 Git）
├── dqc/                    # 命名空间垫片（__path__ → data/dqc/）
├── data_platform/          # 扁平数据湖（L2 表 Phase 2 预留）
│   ├── asset/              # bars_daily、calendar
│   ├── raw/                # amazingdata
│   └── config/
├── research/               # 因子接口/运行时/存储、信号、评价、治理、ML 信号
│   └── reports/
├── engine/                 # 策略流水线与回测
│   └── backtest/           # 期货回测（V3 登记不迁）
├── factor_library/         # 多资产因子（登记不迁）
│   ├── futures/            # 期货因子（V3）
│   └── option/             # 期权因子（V4）
├── strategies/             # 策略组装，不重复实现底层算子
│   └── futures/            # alstm/、tl_intraday/（V3 登记不迁）
├── execution/              # 组合、订单、风控、账本、通道
│   └── router/             # 通道连接器（V2 起点，登记不迁）
├── infra/                  # 期货连续合约、跨品种（V3 登记不迁）
├── ops/                    # 调度、监控、日报、告警
├── scripts/                # 面向用户/调度器的可执行入口
│   ├── bootstrap/
│   └── data_processing/
├── examples/               # 最小示例与 notebook
├── tests/                  # 自动化测试
│   ├── unit/
│   ├── integration/
│   └── golden/
├── docs/                   # 项目文档
│   ├── archive/            # 冻结只读
│   ├── data/
│   └── design/
├── logs/                   # 运行日志（不入 Git）
├── data_lake/              # 行情、因子、tick 数据（不入 Git）
└── artifacts/              # 模型、图表、报告（不入 Git）
```

> 目录树与 `find . -type d -not -path './.git*' -not -path './.venv*'` 逐条对照（M7 / S13-T3 定稿）：以上条目均在仓库实际目录中。不入树的目录：工具与缓存（`.idea/`、`.mypy_cache/`、`.pytest_cache/`、`.ruff_cache/`、`.hypothesis/`、`__pycache__/`、`cmake-build-debug/`）、工作区运行时产物（`task/`、`.multica/`、`.claude/`、`tmp/`、`third_party/`）、M3 归档残留（`libs/zvt/` Git 已无此树，磁盘仅剩 `zvt.egg-info` 构建产物）。

## 模块放置决策

| 内容 | 位置 | 判断标准 |
|---|---|---|
| 数据读写、数据集转换 | `infra/` | 不包含交易观点，可被多个策略复用 |
| 因子或特征 | `factor_library/<asset>/` | 输入市场数据，输出可注册因子值 |
| 模型、回测器、组合器 | `engine/` | 是可复用的运行机制，而非单一策略 |
| 超参数和策略组装 | `strategies/<asset>/` | 表达“交易什么、如何组合” |
| 单次任务入口 | `scripts/<domain>/` | 包含 CLI、日期范围或调度逻辑 |
| 探索性代码 | `examples/` | 用于说明、复现或试验，不被生产模块 import |
| 训练权重、预测、图表 | `artifacts/` | 可通过程序重新生成 |
| 原始或派生数据 | `data_lake/` | 体积大、机器本地化，不进入 Git |

## 当前迁移区

早期高频实验已按职责拆分到主干，ALSTM 策略工作流归入 `strategies/futures/alstm/`：

- 因子：`factor_library/futures/`
- 模型：`engine/models/`
- 回测：`engine/backtest/`
- 集成：`engine/ensemble/`
- 数据集处理：`infra/`
- 策略参数与运行入口：`strategies/futures/alstm/`
- 策略 notebook 与报告：`strategies/futures/alstm/notebooks/`、`reports/`
- 运行产物：`artifacts/futures_alstm/`

迁移期的约束：

1. 新功能只加到主干目录；策略目录不复制通用模型、因子和工具。
2. 一个实现完成回归后，再删除对应的旧副本。
3. 迁移前后保留相同的时间对齐、交易成本和信号延迟假设。

## 数据与产物边界

- `data_lake/` 是平台标准数据根目录，脚本在 Docker 内使用 `/data/` 访问。
- 根目录 `期货level2五档数据/` 是本地原始数据，不应被 Python 包直接依赖；代码应通过配置获得路径。
- `artifacts/` 仅保存可再生成的训练和回测输出。
- 要长期保留的结论应整理到 `docs/`，不要只留在 notebook 输出中。

## 建议的后续收口

以下四项由迁移批次逐项落定，各自标注完成批次（M7 / S13-T3 核对现状并标注，不执行删除）。经核：四项均已标注完成批次，无未标注项。

1. 确认 `indicator_py/` 的因子结果与 `factor_library/futures/` 一致后归档旧目录。—— **完成批次 M7 核销**：`indicator_py/` 从未入 Git（`git rev-list --all --objects` 无此路径），当前检出无此目录（`find . -name indicator_py` 无结果），归档对象已无此件；因子迁移注记现存 `factor_library/futures/`（`__init__.py`、`functions.py`「从 indicator_py 迁移」）。
2. 确认根目录 `indicator_py/` 的旧因子均已有主干实现后移除重复代码。—— **完成批次 M7 核销**：同第 1 项，根目录已无 `indicator_py/` 重复代码可移除。
3. 确认精简后的 `libs/zvt/` 通过回测后删除本地备份。—— **完成批次 M3**：QUANT-51（PR #79）已删除 `libs/zvt/`，删除前全量测试绿；磁盘仅剩 `zvt.egg-info` 构建残留，不入 Git。
4. 项目文档统一维护在小写 `docs/`；策略研究报告跟随所属策略维护。—— **完成批次 M7 核销（现状成立）**：文档统一在 `docs/`；策略报告在 `strategies/futures/alstm/reports|notebooks/`、`strategies/futures/tl_intraday/reports/`。

---

## §5.2 旧→新映射表逐行处置登记

> 本节由 QUANT-52 Iter 1（2026-11-05）写入。对 `docs/ROADMAP.md` §5.2 映射表的每一行给出明确处置结论：**归档**（Iter 2 删除或移入 `docs/archive/`）、**登记不迁**（登记保留，不迁移）、**已无此件**（文件在当前检出中不存在，无需归档）。
>
> 审计基准：develop `644b78c` + QUANT-51（M3 归档批）`ac6e4a7`。

### 处置表

| # | 现行位置（§5.2） | 批次 | 当前状态 | 处置 | 说明 |
|---|---|---|---|---|---|
| 1 | `config/settings.toml`、`config/strategies.yaml` | M1 | 存在 | **归档** | 后续批次（M1）归档；新配置已在 `config/config.toml` + `config/strategies/` |
| 2 | `tests/`（3 个文件） | M1 | 存在 | **归档** | 后续批次（M1）重组为 `tests/{unit,integration,golden}/`；`test_tl_intraday_reproduction.py` **保留**（红线 6） |
| 3 | `factor_library/base.py`、`registry.py`、`dag.py`、`preprocessor.py` | M2 | 存在 | **归档** | 已由 `research/factor_interface.py`、`core/registry.py`、`research/factor_runtime.py`、`research/post_processor.py` 取代 |
| 4 | `factor_library/operators/` | M2 | 存在 | **归档** | 已由 `research/` 共享算子取代 |
| 5 | `factor_library/stock/` | M2 | 存在 | **归档** | 已由 `research/` 因子实现取代（V1 范围） |
| 6 | `factor_library/futures/`、`factor_library/option/` | M-V3/M-V4 | 存在 | **登记不迁** | V3/V4 范围，见下方登记清单 |
| 7 | `infra/storage.py`、`query.py`、`writer.py`、`connection.py` | M2 | 存在 | **归档** | 已由 `data/storage_ops.py`、`data/data_api.py` 取代 |
| 8 | `infra/scheduler.py` | M5 | 存在 | **归档** | 已由 `ops/scheduler.py` 取代（M5）；**注意**：`ops/scheduler.py:263` 有 subprocess 调用 `/scripts/data_fetcher/fetch_stock_daily.py`，grep 验证不覆盖此模式，Iter 2 须处理或登记 |
| 9 | `infra/futures_continuous.py`、`cross_instrument.py` | M-V3 | 存在 | **登记不迁** | V3 范围，见下方登记清单 |
| 10 | `infra/qlib_processors.py`、`infra/spread_dataset.py` | M3 | 已无此件 | **已无此件** | QUANT-51 `ac6e4a7` 已删除 |
| 11 | `engine/pipeline.py` | M2 | 已无此件 | **已无此件** | QUANT-51 `ac6e4a7` 已删除（import vectorbt，grep 强制） |
| 12 | `engine/factor/backtest_runner.py`、`incremental_updater.py` | M2 | 混合 | **拆分处置** | `backtest_runner.py` 已无此件（QUANT-51 删除，import zvt_bridge）；`incremental_updater.py` 存在 → **归档**（已由 `research/factor_runtime.py` 取代） |
| 13 | `engine/simulation/models.py` | M2 | 存在 | **归档** | 已由 `execution/ledger.py` 取代 |
| 14 | `engine/models/`（alstm / lgbm / distribution_nll / model_diagnostic） | M4 | 存在 | **归档** | 已由 `research/ml_signal.py` + `research/factor_runtime.MLFactor` 取代；期货 ALSTM 自身回归基准**保留**（红线 6，见 §5.3） |
| 15 | `engine/ensemble/` | M4 | 存在 | **归档** | 已由 `research/ml_signal.ensemble_models` + `research/factor_combiner` 取代 |
| 16 | `engine/backtest/`（futures_backtest / intraday_futures / regime_filter / continuous_backtest_utils） | M-V3 | 存在 | **登记不迁** | V3 期货回测，见下方登记清单 |
| 17 | `engine/qlib_bridge/`、`engine/vectorbt_engine/`、`engine/zvt_bridge/` | M3 | 已无此件 | **已无此件** | QUANT-51 `ac6e4a7` 已删除（`qlib_bridge/` 在 `644b78c` 时已不存在） |
| 18 | `libs/zvt/` | M3 | 已无此件 | **已无此件** | QUANT-51 `ac6e4a7` 已删除；磁盘残留 `egg-info` 构建产物，不入 Git |
| 19 | `execution/position_manager.py` | M2 | 存在 | **归档** | 已由 `execution/portfolio_construction.py` + `execution/ledger.py` 取代 |
| 20 | `execution/risk_control.py` | M2 | 已无此件 | **已无此件** | 原地重写（七规则表 + `ASSET_RULES`），旧代码已被新实现覆盖，无独立旧件可归档 |
| 21 | `execution/router/vnpy_connector.py` | M3 | 已无此件 | **已无此件** | QUANT-51 或更早已删除（`644b78c` 时已不存在） |
| 22 | `execution/router/xtquant_connector.py`、`router/base.py` | M6 | 存在 | **登记不迁** | V2 起点，见下方登记清单 |
| 23 | `scripts/data_fetcher/*` | M2 | 存在 | **归档** | 已由 `data/source_adapter` + `data/ingestion` 取代；AmazingData Docker 用法保留 |
| 24 | `strategies/stock/*` | M4 | 存在 | **归档** | 已由 `strategies/` + alphaType 配方取代 |
| 25 | `strategies/futures/*`（alstm / tl_intraday） | M-V3 | 存在 | **登记不迁** | V3 范围；ALSTM 回归基准与报告**保留**（红线 6） |
| 26 | `knowledge-base/` | — | 存在 | **保留** | 补充笔记，非权威，不归档 |
| 27 | `docs/PROJECT_STRUCTURE.md`「建议的后续收口」四项 | M7 | 存在 | **后续批次** | 本计划不执行，保留原文 |

**处置统计**：归档 13 行 · 登记不迁 5 行 · 已无此件 6 行 · 保留 1 行 · 后续批次 1 行 · 拆分处置 1 行。无「待定」。

### 行外发现（§5.2 表未覆盖）

| 项目 | 状态 | 处置 |
|---|---|---|
| `factor_library/universe.py` | 存在，`factor_library/__init__.py` 导入 `UniverseFilter` / `UniverseConfig` | **不在 §5.2 任何行中**。Iter 2 归档 `factor_library/` 时须清理 `__init__.py` 导入（保留 `futures/` 可导入以维持 `test_tl_intraday_reproduction.py`） |
| `ops/scheduler.py:263` subprocess 调用 `scripts/data_fetcher/fetch_stock_daily.py` | 功能性依赖 | grep 验证（`ac-2`）不覆盖 subprocess 路径；Iter 2 归档 `scripts/data_fetcher/` 前须处理此调用或在 PR 中登记为已知缺口 |

---

## V2+ 登记清单（登记不迁）

> 以下条目**只登记、不动代码**。它们属于 V2/V3/V4 与 L2 预留范围，当前阶段不迁移、不归档、不重写。每项写明**现存路径 + 起用阶段**；起用阶段口径见 `docs/ROADMAP.md` §1「V1 明确不做」表。原 M6 / M-V3 / M-V4 登记按归档批次并入下表（V2=M6、V3=M-V3、V4=M-V4，L2 预留为新增类）。

### V2 — 执行通道（M6 登记不迁；起用阶段 V2：QmtBroker 实盘接入）

| 现存路径 | 起用阶段 | 登记说明 |
|---|---|---|
| `execution/router/xtquant_connector.py` | V2 | QMT 通道连接器。**登记不迁**，V2 执行通道起点（目标：`execution/broker.py` 的 `QmtBroker`） |
| `execution/router/base.py` | V2 | 通道抽象基类。**登记不迁**，V2 执行通道起点 |

### V3 — 期货（M-V3 登记不迁；起用阶段 V3：期货，含连续合约、夜盘归属、保证金账本）

| 现存路径 | 起用阶段 | 登记说明 |
|---|---|---|
| `factor_library/futures/` | V3 | 期货因子库。**登记不迁**，V3 因子范围 |
| `infra/futures_continuous.py` | V3 | 期货连续合约处理。**登记不迁**，V3 数据范围 |
| `infra/cross_instrument.py` | V3 | 跨品种处理。**登记不迁**，V3 数据范围 |
| `engine/backtest/` | V3 | 期货回测（futures_backtest / intraday_futures / regime_filter / continuous_backtest_utils）。**登记不迁**，V3 回测范围 |
| `strategies/futures/` | V3 | 期货策略（alstm / tl_intraday）。**登记不迁**，V3 策略范围 |
| `tests/test_tl_intraday_reproduction.py` | V3 | 期货 ALSTM 回归基准测试。**随期货 ALSTM 回归基准保留**（红线 6），不归档 |

**期货 ALSTM 回归基准保留**（红线 6）：
- `strategies/futures/alstm/`（含 `train.py` / `reporting.py` / `reports/` / `notebooks/`）—— 一律不动
- `tests/test_tl_intraday_reproduction.py` —— 一律不动
- 平台黄金样本（`tests/golden/`，等权 top-5 ETF 动量）与期货 ALSTM 回归基准（策略自身）是两套独立基线，**不混用、不互为对拍**（红线 6）

**缺口 #15 登记**：`find . -name baseline_spec.py` **无结果**——设计要求「保留 `baseline_spec.py` 作期货 ALSTM 自身回归基准」，但对象在仓库中缺失。登记为「对象缺失，V3 回归基准待补」。不删相关表述，不新写 `baseline_spec`。裁决权归 Orchestrator。

### V4 — 期权（M-V4 登记不迁；起用阶段 V4+：期权）

| 现存路径 | 起用阶段 | 登记说明 |
|---|---|---|
| `factor_library/option/` | V4+ | 期权因子库。**登记不迁**，V4 因子范围 |

### L2 预留（起用阶段 Phase 2：`tick_futures_l2` schema 已预留，列数口径差异未裁决，见 l2_data.md「使用路径登记」）

| 现存路径 | 起用阶段 | 登记说明 |
|---|---|---|
| `docs/design/data/l2_data.md` 所述五档/逐笔链路 | Phase 2 预留 | 使用路径登记见该文件「使用路径登记」一节：采集 `data/source_adapter.py` + `data/ingestion.py`，存储 `data/storage_ops.py` + `data/storage_schema.py`（`tick_futures_l2` 表），消费 `data/data_api.py`。L2 采集/存储/消费代码本阶段一律不实现 |
| `docs/data/future_level2_data_processing.md` | Phase 2 预留 | 五档正式数据集口径（`data_lake/future/market_data/tick_l2/`，DuckDB 视图名 `future_market_data_tick_l2`）；数据集不入 Git |
