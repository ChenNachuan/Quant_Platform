# Quant_Platform — AI 编辑日志

按事件顺序记录每次 AI 编辑操作。

---

## 2026-03-22

### 1. 项目审计

- **文件**: 全项目
- **操作**: 阅读全项目代码，提出审计意见
- **产出**: `doc/Feedback.md` — 审计报告（已存在，由 AI 生成）
- **要点**: 识别出 11 项问题，分为 P0-P4 五个优先级。核心问题：Token 泄露风险、硬编码路径、SQL 注入、StorageManager 职责过重、依赖管理混乱

### 2. 编写 Project.md

- **文件**: `doc/Project.md`
- **操作**: 从零编写（文件原为空）
- **内容**: 项目定位、技术栈表、5 层流水线架构图、数据流图、因子系统架构图、目录结构、核心模块说明、关键设计决策、快速开始指南

### 3. 编写 Walkthrough.md

- **文件**: `doc/Walkthrough.md`
- **操作**: 从零编写（文件原为空）
- **内容**: 逐模块代码走查 — ConfigLoader、StorageManager、Factor 基类/注册/DAG/时序算子示例/复合因子示例、UniverseFilter、5 层 Pipeline、A 股交易模型、update_daily.py、alpha_001 策略、测试用例表、已知问题速查表

### 4. 编写 Plan.md

- **文件**: `doc/Plan.md`
- **操作**: 从零编写（文件原为空）
- **内容**: 初始版本按时间线分为 Phase 0-5（工程基础→因子增强→性能优化→实盘接入→Qlib 集成→高级功能）

### 5. 重构 Plan.md

- **文件**: `doc/Plan.md`
- **操作**: 全文重写，按架构模块分类
- **变更**: 移除时间线和里程碑，改为 8 个结构分类（安全与合规、工程规范、架构重构、数据层、引擎层、执行层、调度与运维、性能与扩展）。因子系统增强（Phase 1）内容未纳入，按要求暂不考虑

### 6. 创建 Log.md

- **文件**: `doc/Log.md`
- **操作**: 创建本文件，记录上述所有 AI 编辑操作

---

## 2026-03-23

### 7. 修复 API Token 泄露风险（P0）

- **文件**: `.env.example`（新建）、`.gitignore`
- **操作**:
  - 新建 `.env.example`，内容为模板，Token 替换为占位符 `your_tushare_token_here`
  - `.gitignore` 在 `.env.*` 规则后追加 `!.env.example`，确保模板文件可提交
- **要点**: `git log --all -- .env` 确认 `.env` 从未入库，Token 未泄露历史；`.env` 本体保留本地使用，不入库

### 8. 修复硬编码绝对路径（P0）

- **文件**: `pyproject.toml`、`tests/test_factor_basic.py`、`tests/test_factor_library.py`、`tests/test_incremental_calculation.py`、`tests/test_storage_refactor.py`、`tests/test_zvt_adapter_integration.py`
- **操作**:
  - `pyproject.toml` 新增 `[tool.pytest.ini_options]`，设置 `pythonpath = ["."]` 和 `testpaths = ["tests"]`
  - 上述 5 个测试文件删除 `sys.path.insert(0, '/Users/nachuanchen/Documents/Quant')` 硬编码块，改由 pytest 配置统一处理
- **要点**: `test_adj_factor_recorder.py` 和 `test_kdata_recorder.py` 使用 `Path(__file__).parent` 相对路径，无需修改

### 9. 消除 SQL 注入漏洞（P0）

- **文件**: `infra/storage.py`、`scripts/update_daily.py`、`engine/factor/incremental_updater.py`、`engine/zvt_bridge/backtest/factor_adapter.py`、`scripts/test_pipeline_vbt.py`、`tests/test_zvt_adapter_integration.py`
- **操作**:
  - `infra/storage.py:query()` 新增 `params: list | None = None` 参数，透传给 `conn.execute()`
  - `infra/storage.py:get_factor_matrix` — `WHERE timestamp >= $1 AND timestamp <= $2`，绑定 `[start_date, end_date]`
  - `scripts/update_daily.py` — 幂等检查改为 `WHERE timestamp = $1`，绑定 `[date_dash]`
  - `engine/factor/incremental_updater.py` — timestamp 范围 + symbol IN 动态占位符，绑定参数列表
  - `engine/zvt_bridge/backtest/factor_adapter.py` — entity_id IN 动态占位符 + 条件追加 timestamp 参数
  - `scripts/test_pipeline_vbt.py` — entity_id IN + timestamp 范围参数化
  - `tests/test_zvt_adapter_integration.py` — symbol 参数化
- **要点**: `table_name` 来自固定字符串字面量（非用户输入），保留 f-string 拼接；所有用户/外部数据来源的值均改为 `$N` 占位符

### 10. 工程规范整改（二、工程规范全节）

**2.1 依赖管理**
- **文件**: `pyproject.toml`、`uv.lock`
- **操作**: 移除 `polars`、`clickhouse-driver`、`setuptools` 直接依赖；将 `ruff`、`pandas-stubs` 移至 `[dependency-groups] dev`；放宽 Python 版本为 `>=3.10,<3.12`；`torch` 加上限 `<3.0.0`；运行 `uv lock` 更新 lock 文件
- **要点**: `setuptools` 仍出现在 lock 文件中，因其为其他包的间接依赖，属正常现象

**2.2 代码风格**
- **文件**: `.pre-commit-config.yaml`（新建）、`engine/zvt_bridge/data_syncer.py`、`infra/storage.py`
- **操作**: 新建 `.pre-commit-config.yaml`，配置 ruff + ruff-format hooks；删除 `data_syncer.py:36` 重复的 `from zvt.contract.api import get_data`；删除 `storage.py` 的 `logging.basicConfig()`，改由项目入口统一配置

**2.3 测试基础设施**
- **文件**: `tests/test_preprocessor.py`（新建）、`tests/test_pipeline.py`（新建）
- **操作**: 新增 `PostProcessor` 的 25 个 pytest 测试（winsorize/standardize/check_validity/fillna）；新增 `pipeline.py` 纯逻辑组件测试（EqualWeightPortfolio/QuantilePortfolio/DefaultRiskFilter/SimulationExecutionHandler），用 `unittest.mock` 在 import 时 patch 掉 vectorbt 等重量级依赖
- **要点**: 现有 `tests/test_factor_*.py` 等文件为脚本风格（模块级代码 + `sys.exit`），不适合 pytest 收集，保持原样；新增测试全部通过（25 passed）

### 11. 架构重构（三、架构重构全节）

**3.1 ConfigLoader.reset()**
- **文件**: `infra/storage.py`、`tests/conftest.py`（新建）
- **操作**: `ConfigLoader` 新增 `reset()` classmethod；`conftest.py` 添加 `autouse=True` fixture，每个测试前后自动重置单例

**3.2 StorageManager 拆分**
- **文件**: `infra/connection.py`（新建）、`infra/writer.py`（新建）、`infra/query.py`（新建）、`infra/storage.py`（重写）
- **操作**: 将 `StorageManager` 按职责拆分为 `ConnectionManager`（DuckDB 连接 + 视图注册）、`DataWriter`（Parquet 写入）、`QueryEngine`（SQL 查询）；`StorageManager` 改为 Facade，保持公开 API 完全兼容，16 个调用方无需修改
- **要点**: `self.conn` 和 `self.data_lake_dir` 作为透传属性保留，兼容直接访问这两个属性的测试文件

**3.3 增量更新器修复**
- **文件**: `factor_library/dag.py`、`engine/factor/incremental_updater.py`、`factor_library/technical/volatility.py`、`tests/test_risk_adjusted_momentum_incremental.py`（新建）
- **操作**: `FactorDAG` 新增 `topological_sort_for_update()`；`IncrementalUpdater.update_all()` 引入 `incremental_cache: Dict[(factor_id, entity_id), Series]`，在拓扑顺序中传递 deps；修复 `Volatility.update()` 历史窗口 off-by-one bug（`window-1` → `window`）；新增 6 个测试全部通过
- **要点**: 缓存键使用 `factor.get_id()`（含参数），避免多参数变体冲突；`dependency_para_map` 支持跨参数依赖解析

**3.4 Qlib Bridge 重构**
- **文件**: `pyproject.toml`、`uv.lock`、`engine/qlib_bridge/dumper.py`（重写）
- **操作**: `pyproject.toml` 将 `"qlib"`（石油行业库）改为 `"pyqlib"`（微软量化库）；`dumper.py` 删除 subprocess，改用 `DumpDataAll`/`DumpDataUpdate` Python API；按 symbol 分文件写 CSV（符合 Qlib 格式要求）；`.last_dump_date` 文件追踪增量起点；`qlib.init()` 在构造时调用；修复 `get_market_data()` → `get_data()`
- **要点**: `qlib.init()` 在数据目录为空时会抛异常，已用 try/except 降级为 warning；`pyqlib` 的 import 名仍为 `import qlib`
