# Quant_Platform — 开发计划

本文档按架构模块分类，列出待完善项。每个分类内按优先级排列，不依赖时间线。

---

## 一、安全与合规

> 消除密钥泄露风险和注入漏洞。

### 1.1 Token 安全

- [ ] 检查 `.env` 是否曾提交到 git 历史 (`git log --all -- .env`)
- [ ] 如有泄露，轮换 Tushare Token 并用 BFG Repo-Cleaner 清除历史
- [ ] 在 `README.md` 中加入安全说明（密钥管理指引）

### 1.2 SQL 注入消除

- [ ] `infra/storage.py:398` — `get_factor_matrix` SQL 字符串拼接改为参数化查询
- [ ] `scripts/update_daily.py:149` — 幂等检查 SQL 字符串拼接改为参数化查询
- [ ] 全局搜索其他 `f"...WHERE.*'{date}'"` 模式，统一消除

---

## 二、工程规范

> 依赖管理、代码风格、测试基础设施。

### 2.1 依赖管理

- [ ] 从 `dependencies` 移除未使用的: `polars`, `clickhouse-driver`, `setuptools`
- [ ] 将 `ruff`, `pandas-stubs` 移至 `[dependency-groups] dev`
- [ ] 放宽 Python 版本: `requires-python = ">=3.10,<3.12"`
- [ ] 为 `torch` 添加上限: `torch>=2.0.0,<3.0.0`
- [ ] 运行 `uv lock` 更新 lock 文件

### 2.2 代码风格

- [ ] 添加 `.pre-commit-config.yaml` (ruff + ruff-format)
- [ ] 消除 `engine/zvt_bridge/data_syncer.py:36-37` 的重复 import
- [ ] 统一日志配置: `infra/storage.py:15` 的 `basicConfig` 移至项目入口，避免多模块重复调用

### 2.3 测试基础设施

- [ ] 所有 `tests/test_*.py` 中的 `sys.path.insert('/Users/nachuanchen/...')` 替换为相对路径
- [ ] 在 `pyproject.toml` 添加 pytest 配置:
  ```toml
  [tool.pytest.ini_options]
  pythonpath = ["."]
  testpaths = ["tests"]
  ```
- [ ] 验证 `pytest tests/` 全部通过
- [ ] 补充 `engine/pipeline.py` 测试（当前无覆盖）
- [ ] 补充 `factor_library/preprocessor.py` 测试（当前无覆盖）

---

## 三、架构重构

> 消除设计缺陷，拆分过重模块，修复已知 bug。

### 3.1 ConfigLoader

- [ ] 添加 `ConfigLoader.reset()` 类方法，支持测试间隔离
- [ ] 测试 fixture 中自动 reset

### 3.2 StorageManager 拆分

将 `StorageManager` 拆分为三个职责清晰的类:

```
infra/
├── connection.py  → ConnectionManager  (DuckDB 连接生命周期)
├── writer.py      → DataWriter         (Parquet 写入 + 因子存储)
├── query.py       → QueryEngine        (视图注册 + SQL 查询 + 因子矩阵)
└── storage.py     → StorageManager     (Facade，委托给上述三类)
```

- [ ] 创建 `infra/connection.py` → `ConnectionManager`
- [ ] 创建 `infra/writer.py` → `DataWriter`
- [ ] 创建 `infra/query.py` → `QueryEngine`
- [ ] `StorageManager` 保留为 Facade，委托给上述三个类
- [ ] 更新所有引用处

### 3.3 增量更新器修复

- [ ] 修改 `engine/factor/incremental_updater.py`，将上游因子的增量结果作为 `deps` 传入 `factor.update()`
- [ ] 在 `FactorDAG` 中增加 `topological_sort_for_update()` 方法，区分全量计算和增量更新的依赖顺序
- [ ] 添加测试: `RiskAdjustedMomentum` 增量更新 vs 全量计算一致性

### 3.4 Qlib Bridge 重构

- [ ] 消除 subprocess 调用，改用 Qlib Python API:
  ```python
  from qlib.data.dataset_dump import dump_bin
  dump_bin(csv_path, qlib_dir, region='cn')
  ```
- [ ] 增量更新支持: 只导出新增日期的数据
- [ ] 自动注册 Qlib 数据源: `qlib.init(provider_uri=...)`

---

## 四、数据层

> 数据质量、缓存、存储优化。

### 4.1 数据质量监控

- [ ] 将 `scripts/audit_data.py` 集成为 Prefect Task
- [ ] 每日更新后自动执行:
  - 缺失率检查 (每只股票的 NaN 比例)
  - 异常值检测 (价格变动 > 30% 告警)
  - 数据完整性 (日期覆盖范围检查)
- [ ] 输出告警到日志 + 可选的邮件/企微通知

### 4.2 缓存优化

- [ ] DuckDB 查询结果缓存 (LRU, 基于 SQL 哈希)
- [ ] 因子计算结果磁盘缓存: 已有 `data_lake/factors/`，增加自动过期检查
- [ ] `FactorDAG.computed_cache` 支持持久化 (可选, Pickle/Parquet)

---

## 五、引擎层

> 回测性能、Pipeline 优化、ML 集成。

### 5.1 FactorAlphaModel 向量化

- [ ] 优先路径: 使用 DuckDB 直接在 SQL 层完成因子计算（适用于简单算术因子）
- [ ] Fallback 路径: 保留逐股票循环，但使用 `joblib.Parallel` 并行化
- [ ] 增加 `vectorize=True/False` 参数，允许用户选择

### 5.2 Qlib ML 集成

- [ ] 使用 Qlib 的 `Model` 接口训练 Alpha 预测模型
- [ ] 支持的模型: LightGBM (默认), LSTM, Transformer
- [ ] 模型输出作为因子值，写入 `data_lake/factors/ml_alpha/`
- [ ] 自动从已有因子库构建特征矩阵: `StorageManager.get_factor_matrix()`
- [ ] 标签生成: 未来 N 日收益率 (可配置)
- [ ] 特征筛选: IC / MI (互信息) 排序

---

## 六、执行层

> 实盘通道、仓位管理、风控。

### 6.1 PositionManager

- [ ] 维护本地仓位状态 (目标仓位 vs 实际仓位)
- [ ] 对接 broker 成交回报，更新实际持仓
- [ ] 支持 T+1 限制检查
- [ ] 持久化到本地 JSON/SQLite

### 6.2 XtQuant 交易通道

- [ ] 实现 `XtQuantConnector.connect()` — 登录券商账户
- [ ] 实现 `send_order()` — 下单 (限价/市价)
- [ ] 实现 `cancel_order()` — 撤单
- [ ] 实现成交回报回调 → 更新 PositionManager
- [ ] 增加 `config/settings.toml` 中 XtQuant 配置项

### 6.3 订单管理系统 (OMS)

- [ ] 订单状态机: `PENDING → SUBMITTED → PARTIALLY_FILLED → FILLED / CANCELLED`
- [ ] 部分成交处理: 未成交部分超时自动撤单 + 重新委托
- [ ] 下单限流: 避免短时间大量委托触发交易所风控
- [ ] 订单日志: 记录每笔委托的完整生命周期

### 6.4 实盘风控增强

- [ ] `RiskControl` 扩展:
  - 最大回撤检查: 当日回撤 > 阈值 → 暂停开仓
  - 最大杠杆检查: `总持仓市值 / 总资产 <= max_leverage`
  - 涨跌停板预判: 对即将涨停的标的不再追买
- [ ] 熔断机制: 连续 N 笔亏损 → 暂停策略

---

## 七、调度与运维

> Prefect 日级流水线、告警、可视化。

### 7.1 Prefect 日级调度

补全 `infra/scheduler.py` 中的 stub Task:

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

### 7.2 Web Dashboard

- [ ] 基于 Streamlit 或 Dash 的可视化面板
- [ ] 功能: 资金曲线、持仓分布、因子 IC 热力图、数据质量仪表盘
- [ ] 实时监控: 接入 Prefect 任务状态

---

## 八、性能与扩展

> 长期迭代目标，各子项独立推进。

### 8.1 Polars 迁移

- [ ] 因子计算层逐步迁移至 Polars (替代 Pandas)
- [ ] 优先迁移: 截面算子 (`cs_rank`, `cs_zscore`)
- [ ] 预期收益: 3-10x 速度提升

### 8.2 分布式计算

- [ ] Prefect Dask executor 支持
- [ ] 多因子并行计算: 每个因子独立 Task
- [ ] 参数搜索并行化: `para_space` 批量提交

### 8.3 ClickHouse 接入

- [ ] 当数据量超百万行/表时，引入 ClickHouse 作为 OLAP 后端
- [ ] DuckDB → ClickHouse 数据迁移工具
- [ ] 查询路由: 小数据量走 DuckDB，大数据量走 ClickHouse

### 8.4 多市场支持

- [ ] 扩展至港股/美股
- [ ] UniverseFilter 适配不同市场的上市/退市规则
- [ ] 费率模型适配 (港股佣金模式、美股 PDT 规则)