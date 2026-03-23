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
