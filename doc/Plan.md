# Quant_Platform — 开发计划

本文档定义项目分阶段开发路线图，按优先级从高到低排列。每个阶段列出具体任务、验收标准和前置依赖。

---

## Phase 0: 工程基础加固 (1-2 周)

> 目标: 消除技术债务，建立规范的开发流程，为后续迭代打基础。

### 0.1 安全修复

- [ ] 检查 `.env` 是否曾提交到 git 历史 (`git log --all -- .env`)
- [ ] 如有泄露，轮换 Tushare Token 并用 BFG Repo-Cleaner 清除历史
- [ ] 在 `README.md` 中加入安全说明（密钥管理指引）

### 0.2 消除硬编码路径

- [ ] 所有 `tests/test_*.py` 中的 `sys.path.insert('/Users/nachuanchen/...')` 替换为相对路径
- [ ] 在 `pyproject.toml` 添加 pytest 配置:
  ```toml
  [tool.pytest.ini_options]
  pythonpath = ["."]
  testpaths = ["tests"]
  ```
- [ ] 验证 `pytest tests/` 全部通过

### 0.3 依赖清理

- [ ] 从 `dependencies` 移除未使用的: `polars`, `clickhouse-driver`, `setuptools`
- [ ] 将 `ruff`, `pandas-stubs` 移至 `[dependency-groups] dev`
- [ ] 放宽 Python 版本: `requires-python = ">=3.10,<3.12"`
- [ ] 为 `torch` 添加上限: `torch>=2.0.0,<3.0.0`
- [ ] 运行 `uv lock` 更新 lock 文件

### 0.4 代码规范

- [ ] 添加 `.pre-commit-config.yaml` (ruff + ruff-format)
- [ ] 消除 `data_syncer.py` 的重复 import
- [ ] 统一 SQL 参数绑定: `infra/storage.py:398` 和 `scripts/update_daily.py:149` 的字符串拼接改为参数化查询
- [ ] 统一日志配置: `infra/storage.py:15` 的 `basicConfig` 移至项目入口，避免多模块重复调用

### 0.5 ConfigLoader 测试隔离

- [ ] 添加 `ConfigLoader.reset()` 类方法
- [ ] 测试 fixture 中自动 reset

**验收标准**: `pytest tests/` 全绿，`ruff check .` 零警告，无硬编码路径，无未使用依赖。

---

## Phase 1: 因子系统增强 (2-3 周)

> 目标: 扩充因子库，完善 DAG 增量更新，建立自动化因子评价体系。

### 1.1 补充基本面因子

在 `factor_library/fundamental/` 下实现:

- [ ] **EP (Earnings-to-Price)**: `1 / PE_TTM`
- [ ] **BP (Book-to-Price)**: `PB_TTM 的倒数`
- [ ] **ROE**: `净利润 / 净资产`，支持 TTM 和单季
- [ ] **营收增长率**: `revenue_ttm / revenue_ttm_1y_ago - 1`

数据源: Tushare `fina_indicator` / `income` 接口，存储为 `data_lake/financial/` 下的 Parquet。

### 1.2 补充另类因子

在 `factor_library/alternative/` 下实现:

- [ ] **换手率因子**: `ts_mean(turnover_rate, window)`
- [ ] **资金流因子**: `net_inflow / market_cap`
- [ ] **波动率偏度**: `ts_skew(pct_change(close), window)`

### 1.3 完善 DAG 增量更新

- [ ] 修改 `incremental_updater.py`，将上游因子的增量结果作为 `deps` 传入 `factor.update()`
- [ ] 在 `FactorDAG` 中增加 `topological_sort_for_update()` 方法，区分全量计算和增量更新的依赖顺序
- [ ] 添加测试: `RiskAdjustedMomentum` 增量更新 vs 全量计算一致性

### 1.4 因子 IC/IR 自动评价

- [ ] 创建 `engine/factor/evaluator.py`，封装 Alphalens 的 IC/IR 计算
- [ ] 在 `Factor.compute_with_postprocess()` 后自动计算:
  - 截面 IC (Rank IC / Normal IC)
  - IC 均值 / IC 标准差 / ICIR
  - 分组 (Quantile) 累计收益
  - IC 衰减 (lag 1d ~ 20d)
- [ ] 输出 JSON 报告: `results/factor_eval/{factor_id}.json`

### 1.5 因子中性化增强

- [ ] `cs_neutralize` 增加市值中性化: 对 `log(market_cap)` 回归取残差
- [ ] 支持行业中性 + 市值中性双重中性化

**验收标准**: 因子库覆盖 3 大类 (技术/基本面/另类) 共 10+ 因子，IC 评价自动生成，DAG 增量更新一致性通过。

---

## Phase 2: Pipeline 性能优化 (2 周)

> 目标: 消除性能瓶颈，提升因子计算和回测吞吐量。

### 2.1 FactorAlphaModel 向量化

- [ ] 优先路径: 使用 DuckDB 直接在 SQL 层完成因子计算（适用于简单算术因子）
- [ ] Fallback 路径: 保留逐股票循环，但使用 `joblib.Parallel` 并行化
- [ ] 增加 `vectorize=True/False` 参数，允许用户选择

### 2.2 StorageManager 拆分

将 `StorageManager` 拆分为三个职责清晰的类:

```
ConnectionManager  → DuckDB 连接生命周期管理
DataWriter         → Parquet 写入 + 因子存储
QueryEngine        → 视图注册 + SQL 查询 + 因子矩阵
```

- [ ] 创建 `infra/connection.py` → `ConnectionManager`
- [ ] 创建 `infra/writer.py` → `DataWriter`
- [ ] 创建 `infra/query.py` → `QueryEngine`
- [ ] `StorageManager` 保留为 Facade，委托给上述三个类
- [ ] 更新所有引用处

### 2.3 数据质量监控

- [ ] 将 `scripts/audit_data.py` 集成为 Prefect Task
- [ ] 每日更新后自动执行:
  - 缺失率检查 (每只股票的 NaN 比例)
  - 异常值检测 (价格变动 > 30% 告警)
  - 数据完整性 (日期覆盖范围检查)
- [ ] 输出告警到日志 + 可选的邮件/企微通知

### 2.4 缓存优化

- [ ] DuckDB 查询结果缓存 (LRU, 基于 SQL 哈希)
- [ ] 因子计算结果磁盘缓存: 已有 `data_lake/factors/`，增加自动过期检查
- [ ] `FactorDAG.computed_cache` 支持持久化 (可选, Pickle/Parquet)

**验收标准**: `FactorAlphaModel` 向量化路径速度提升 3x+，`StorageManager` 拆分完成且测试通过。

---

## Phase 3: 实盘接入 (3-4 周)

> 目标: 至少打通一个券商通道，实现从信号到下单的完整链路。

### 3.1 PositionManager 实现

- [ ] 维护本地仓位状态 (目标仓位 vs 实际仓位)
- [ ] 对接 broker 成交回报，更新实际持仓
- [ ] 支持 T+1 限制检查
- [ ] 持久化到本地 JSON/SQLite

### 3.2 XtQuant 交易通道

- [ ] 实现 `XtQuantConnector.connect()` — 登录券商账户
- [ ] 实现 `send_order()` — 下单 (限价/市价)
- [ ] 实现 `cancel_order()` — 撤单
- [ ] 实现成交回报回调 → 更新 PositionManager
- [ ] 增加 `config/settings.toml` 中 XtQuant 配置项

### 3.3 Prefect 调度完善

补全 `infra/scheduler.py` 中的 4 个 stub Task:

```python
@task
def fetch_and_store_data():     # 调用 TushareUpdater.update_market_data()

@task
def refresh_data_lake_views():  # 调用 StorageManager.refresh_views()

@task
def compute_factors():          # 按 DAG 顺序计算所有因子

@task
def generate_signals():         # 运行 pipeline，输出交易信号

@task
def execute_trades():           # 信号 → PositionManager → XtQuant 下单
```

- [ ] 日级 Flow: `daily_flow()` 串联上述 Task
- [ ] 异常处理: Task 失败重试 (3次, 指数退避)
- [ ] 告警: 关键 Task 失败发送通知

### 3.4 订单管理系统 (OMS)

- [ ] 订单状态机: `PENDING → SUBMITTED → PARTIALLY_FILLED → FILLED / CANCELLED`
- [ ] 部分成交处理: 未成交部分超时自动撤单 + 重新委托
- [ ] 下单限流: 避免短时间大量委托触发交易所风控
- [ ] 订单日志: 记录每笔委托的完整生命周期

### 3.5 实盘风控增强

- [ ] `RiskControl` 扩展:
  - 最大回撤检查: 当日回撤 > 阈值 → 暂停开仓
  - 最大杠杆检查: `总持仓市值 / 总资产 <= max_leverage`
  - 涨跌停板预判: 对即将涨停的标的不再追买
- [ ] 熔断机制: 连续 N 笔亏损 → 暂停策略

**验收标准**: 在模拟盘环境中，信号 → 下单 → 成交回报全链路跑通，OMS 状态跟踪正确。

---

## Phase 4: Qlib 深度集成 (2-3 周)

> 目标: 利用 Qlib 的 ML 框架进行 Alpha 挖掘。

### 4.1 Qlib Bridge 重构

- [ ] 消除 subprocess 调用，改用 Qlib Python API:
  ```python
  from qlib.data.dataset_dump import dump_bin
  dump_bin(csv_path, qlib_dir, region='cn')
  ```
- [ ] 增量更新支持: 只导出新增日期的数据
- [ ] 自动注册 Qlib 数据源: `qlib.init(provider_uri=...)`

### 4.2 ML 因子挖掘

- [ ] 使用 Qlib 的 `Model` 接口训练 Alpha 预测模型
- [ ] 支持的模型: LightGBM (默认), LSTM, Transformer
- [ ] 模型输出作为因子值，写入 `data_lake/factors/ml_alpha/`
- [ ] 与传统因子一起参与 Pipeline 回测

### 4.3 特征工程

- [ ] 自动从已有因子库构建特征矩阵: `StorageManager.get_factor_matrix()`
- [ ] 标签生成: 未来 N 日收益率 (可配置)
- [ ] 特征筛选: IC / MI (互信息) 排序

**验收标准**: Qlib 模型训练 → 预测 → 因子值存储 → Pipeline 回测，端到端可执行。

---

## Phase 5: 高级功能 (持续迭代)

### 5.1 Polars 迁移

- [ ] 因子计算层逐步迁移至 Polars (替代 Pandas)
- [ ] 优先迁移: 截面算子 (`cs_rank`, `cs_zscore`)
- [ ] 预期收益: 3-10x 速度提升

### 5.2 分布式计算

- [ ] Prefect Dask executor 支持
- [ ] 多因子并行计算: 每个因子独立 Task
- [ ] 参数搜索并行化: `para_space` 批量提交

### 5.3 ClickHouse 接入

- [ ] 当数据量超百万行/表时，引入 ClickHouse 作为 OLAP 后端
- [ ] DuckDB → ClickHouse 数据迁移工具
- [ ] 查询路由: 小数据量走 DuckDB，大数据量走 ClickHouse

### 5.4 Web Dashboard

- [ ] 基于 Streamlit 或 Dash 的可视化面板
- [ ] 功能: 资金曲线、持仓分布、因子 IC 热力图、数据质量仪表盘
- [ ] 实时监控: 接入 Prefect 任务状态

### 5.5 多市场支持

- [ ] 扩展至港股/美股
- [ ] UniverseFilter 适配不同市场的上市/退市规则
- [ ] 费率模型适配 (港股佣金模式、美股 PDT 规则)

---

## 里程碑时间线

```
Phase 0  ████░░░░░░░░░░░░░░░░  工程基础     Week 1-2
Phase 1  ░░░░████████░░░░░░░░  因子系统     Week 3-5
Phase 2  ░░░░░░░░░░░░████░░░░  性能优化     Week 6-7
Phase 3  ░░░░░░░░░░░░░░░░████████  实盘接入  Week 8-11
Phase 4  ░░░░░░░░░░░░░░░░░░░░████████  Qlib  Week 12-14
Phase 5  ░░░░░░░░░░░░░░░░░░░░░░░░██████████  持续迭代
```

## 依赖关系

```
Phase 0 (基础加固) ──→ Phase 1 (因子增强) ──→ Phase 2 (性能优化)
                                               │
                                               ▼
                                         Phase 3 (实盘接入) ──→ Phase 5 (高级功能)
                                               │
                                               ▼
                                         Phase 4 (Qlib 集成)
```

- Phase 0 是所有后续阶段的前置
- Phase 1 和 Phase 2 可部分并行
- Phase 3 依赖 Phase 1 的因子库和 Phase 2 的性能优化
- Phase 4 独立于 Phase 3，但需要 Phase 1 的因子矩阵
- Phase 5 是持续迭代，各子项独立推进