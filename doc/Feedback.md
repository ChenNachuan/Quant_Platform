 Quant_Platform 项目审计报告

  ---
  一、总体评价

  项目架构设计优秀，借鉴 QuantConnect Lean 的 5
  层流水线模型清晰合理，因子库设计（注册/DAG/增量更新/后处理）达到了生产级水准。但在工程规范、安全、可维护性方面存在若干需要立即修复的问题。

  ---
  二、必须立即修复的问题

  1. API Token 泄露（严重）

  .env 文件包含明文 Tushare Token，虽然 .gitignore 已排除 .env，但如果曾提交过历史版本，Token 已永久留在 git 历史中。

  建议：
  - git log --all -- .env 检查历史
  - 如有泄露记录，立即轮换 Token
  - 使用 git filter-branch 或 BFG Repo-Cleaner 清除历史

  2. 硬编码绝对路径（高）

  多个测试文件中硬编码了 /Users/nachuanchen/Documents/Quant，例如 tests/test_factor_basic.py:9 等，导致代码不可移植，换机器或换用户即失效。

  建议： 全部替换为 sys.path.insert(0, str(Path(__file__).parent.parent)) 或改为 pytest 的 pyproject.toml 配置（pythonpath = ["."]），彻底消除 sys.path hack。

  3. SQL 字符串拼接注入风险（中）

  infra/storage.py:398 的 get_factor_matrix 和 scripts/update_daily.py 中存在 f"... WHERE timestamp = '{date_dash}'"
  的字符串拼接。虽然当前场景风险可控（非用户输入），但应统一使用参数绑定。

  ---
  三、架构与设计审计

  优点

  ┌──────────────┬───────────────────────────────────────────────────────────────────────┐
  │     维度     │                                 评价                                  │
  ├──────────────┼───────────────────────────────────────────────────────────────────────┤
  │ 分层清晰     │ 5 层流水线：Universe → Alpha → Portfolio → Risk → Execution，职责明确 │
  ├──────────────┼───────────────────────────────────────────────────────────────────────┤
  │ 因子系统     │ @register_factor + DAG + 增量 update() + PostProcessor，专业级        │
  ├──────────────┼───────────────────────────────────────────────────────────────────────┤
  │ A 股特性     │ 佣金万三、印花税卖出单边、最低 5 元、涨跌停/停牌检查，细节到位        │
  ├──────────────┼───────────────────────────────────────────────────────────────────────┤
  │ 前瞻偏差防护 │ FactorAdapter 截断最后一行，UniverseFilter 过滤 ST/停牌/退市          │
  ├──────────────┼───────────────────────────────────────────────────────────────────────┤
  │ 数据验证     │ Tushare + AKShare 双源交叉校验                                        │
  ├──────────────┼───────────────────────────────────────────────────────────────────────┤
  │ 存储设计     │ DuckDB + Parquet + Hive 分区，兼顾查询性能和存储效率                  │
  └──────────────┴───────────────────────────────────────────────────────────────────────┘

  需改进的架构问题

  1. ConfigLoader 单例模式有隐患

  infra/storage.py:28 使用类变量 _config 作为单例缓存，但无法 reset。测试间共享状态会导致不可预测行为。建议增加 ConfigLoader.reset() 方法，或改用 functools.lru_cache。

  2. StorageManager 职责过重

  一个类同时承担了：DuckDB 连接管理、视图注册、数据写入、因子存储、SQL 查询、复权视图创建。建议拆分为 ConnectionManager、DataWriter、QueryEngine 三个职责。

  3. FactorAlphaModel.get_signals 逐股票循环

  engine/pipeline.py:86 中 for entity_id, group in kdata.groupby('entity_id') 逐股票循环计算因子，与项目"向量化"的设计目标矛盾。应优先走向量化路径（DuckDB
  直接计算），逐股票循环仅作 fallback。

  4. 增量更新器的依赖传递缺陷

  engine/factor/incremental_updater.py 中 factor.update() 调用时 deps=None，导致复合因子（如 RiskAdjustedMomentum）无法真正增量更新。需在 DAG 调度中传递上游因子的增量结果。

  5. Qlib Bridge 用 subprocess 而非 Python API

  engine/qlib_bridge/dumper.py 先导出 CSV 再 subprocess 调用 qlib.run.dump_bin，增加了不必要的 IO 和进程开销。应直接调用 Qlib 的 D.dump_bin() Python API。

  ---
  四、依赖管理审计

  ┌────────────────────────────────────────────┬───────────────────────────────────────────────────────────────┐
  │                    问题                    │                             说明                              │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ polars 声明但未使用                        │ pyproject.toml 依赖但源码零引用，增加安装体积（含 Rust 编译） │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ clickhouse-driver 声明但未使用             │ 同上                                                          │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ torch>=2.0.0 版本约束过宽                  │ Qlib 需要 torch 但 >=2.0.0 无上限可能导致兼容性问题           │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ Python 版本锁定 ==3.10.*                   │ 过于严格，3.10 已接近 EOL，建议放宽至 >=3.10,<3.12            │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ ruff 放在 dependencies 而非 dev 组         │ 应移至 [dependency-groups] dev                                │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ setuptools==82.0.1 不应出现在 dependencies │ 这是构建工具，非运行时依赖                                    │
  ├────────────────────────────────────────────┼───────────────────────────────────────────────────────────────┤
  │ pandas-stubs 放在 dependencies             │ 应移至 dev 组                                                 │
  └────────────────────────────────────────────┴───────────────────────────────────────────────────────────────┘

  ---
  五、未完成模块清单

  ┌───────────────────────────────────────┬────────┬────────────────────────────────┐
  │                 模块                  │  状态  │              说明              │
  ├───────────────────────────────────────┼────────┼────────────────────────────────┤
  │ execution/router/vnpy_connector.py    │ stub   │ 硬编码返回值，无实际连接逻辑   │
  ├───────────────────────────────────────┼────────┼────────────────────────────────┤
  │ execution/router/xtquant_connector.py │ stub   │ 同上                           │
  ├───────────────────────────────────────┼────────┼────────────────────────────────┤
  │ execution/position_manager.py         │ stub   │ 仅 logger.info，无仓位跟踪     │
  ├───────────────────────────────────────┼────────┼────────────────────────────────┤
  │ infra/scheduler.py                    │ stub   │ Prefect flow/task 均为占位实现 │
  ├───────────────────────────────────────┼────────┼────────────────────────────────┤
  │ factor_library/fundamental/           │ 空目录 │ 无任何因子                     │
  ├───────────────────────────────────────┼────────┼────────────────────────────────┤
  │ factor_library/alternative/           │ 空目录 │ 无任何因子                     │
  └───────────────────────────────────────┴────────┴────────────────────────────────┘

  ---
  六、未来修改完善方向

  P0 — 工程基础加固

  1. 修复硬编码路径 — 全部改为相对路径 + pytest 配置
  2. 清理未用依赖 — 移除 polars、clickhouse-driver、setuptools；将 ruff/pandas-stubs 移入 dev 组
  3. 统一 SQL 参数绑定 — 消除所有字符串拼接 SQL
  4. ConfigLoader 增加 reset — 支持测试隔离
  5. 增加 pre-commit hook — ruff + mypy（至少类型检查因子库核心模块）

  P1 — 因子系统增强

  1. 补充基础因子 — fundamental/ 下添加 EP/BP/ROE 等基本面因子；alternative/ 下添加换手率/资金流等另类因子
  2. 完善 DAG 增量更新 — 实现 deps 传递，让复合因子支持真正的增量计算
  3. 因子 IC/IR 跑批框架 — 将 Alphalens 集成到 pipeline 中自动输出因子评价报告
  4. 因子中性化 — cs_neutralize 目前仅实现行业中性，需补充市值中性

  P2 — 实盘接入

  1. PositionManager 实现 — 维护本地仓位状态，对接 broker 回报
  2. XtQuant/VnPy Connector — 至少完成一个通道的实盘连接
  3. Prefect 调度完善 — 实现日级数据更新 → 因子计算 → 信号生成 → 下单的完整 flow
  4. 订单管理系统 (OMS) — 订单状态跟踪、部分成交处理、异常重试

  P3 — 性能与扩展

  1. Polars 替换 Pandas — 因子计算和数据读取层迁移至 Polars，可获得 3-10x 加速
  2. Qlib Bridge 改用 Python API — 消除 CSV 中间文件和 subprocess 开销
  3. ClickHouse 接入 — 当数据量超过单机 Parquet 承载时，引入 ClickHouse 作为 OLAP 引擎
  4. 分布式因子计算 — 利用 Prefect 的 Dask/Ray executor 并行化多因子计算

  P4 — 测试与监控

  1. 提高测试覆盖率 — 当前 7 个测试文件，核心 pipeline.py 和 preprocessor.py 无测试
  2. 回测基准对比 — 增加与沪深 300/中证 500 指数的基准对比输出
  3. 资金曲线监控 — 增加最大回撤告警、夏普比率实时计算
  4. 数据质量巡检自动化 — 将 audit_data.py 集成到 Prefect 日级 flow 中

  ---
  七、总结

  这是一个设计思路清晰、因子系统专业的量化平台原型。核心架构已经成型，主要瓶颈在于：

  - 工程规范：硬编码路径、依赖管理混乱、缺少 CI/CD
  - 未完成模块：实盘通道和调度系统仍是空壳
  - 性能天花板：逐股票循环与 Pandas 数据类型限制了吞吐量

  建议按 P0→P1→P2 的优先级推进，先把工程基础打牢，再逐步补全因子库和实盘能力。

  ---
  八、Gemini 审查补充（2026-03-23 核实）

  以下问题经代码核实属实，已纳入 Plan.md 二点五节。

  **已确认的真实缺陷：**

  1. ST 过滤前视偏差（严重）
     `universe.py:144` 用 `.last()` 取整个回测期最后一条 name，对全历史生效。
     若某股 2024 年被 ST，则 2015 年起就被错误剔除；若 2015 年是 ST 后来摘帽，则历史上被错误放入池。
     → 已记录至 Plan 2.5.1

  2. 停牌缺失数据误判（中）
     `universe.py:123` 数据源不下发停牌日行情时，pivot 后为 NaN，`.fillna(False)` 误判为可交易。
     → 已记录至 Plan 2.5.2

  3. 次新股/退市 Python for 循环（低，性能）
     `universe.py:160,176` 两处 `for date in dates` 循环，3000+ 交易日时性能差。
     → 已记录至 Plan 2.5.3

  4. T 日信号用 T 日收盘价成交（设计决策）
     VectorBT 执行层用 T 日收盘价成交 T 日信号，A 股日线场景下无法实现。
     属已知简化假设，需文档说明或改为 shift(1) + T+1 日成交。
     → 已记录至 Plan 2.5.4

  **Gemini 描述不准确的部分：**

  - "缺陷一：严重违反强制向量化"：`FactorAlphaModel.get_signals` 的 for 循环是 `Factor.compute()` 接口约束（逐标的设计）的必然结果，不是随意写的循环。Feedback.md 三、3 已记录此问题，修复需重新设计 Factor 接口，不是简单替换。
  - "缺陷五"的严重程度被夸大：T 日收盘成交是量化回测中常见的简化假设，不是"灾难性"缺陷，是需要文档说明的设计决策。

✻ Baked for 5m 34s

---
九、Code Review 结果（2026-03-23，claude-sonnet-4-6）

本次 review 覆盖 8 个源文件 + 2 个测试文件，约 981 行新增 / 520 行删除。

**P1 — 必须修复**

1. SQL 注入回归（`infra/connection.py:74-96`）
   `create_adjusted_views()` 将 `factor_col` 直接插值进 SQL，与 commit `cd63e03` 的修复方向相悖。
   当前值虽为硬编码，但模式危险。修复：在插值前断言 `factor_col in {'qfq_factor', 'hfq_factor'}`。

2. 增量缓存仅在非空时写入（`engine/factor/incremental_updater.py:222`）
   `incremental_cache` 写入被包裹在 `if not res_series.empty:` 内。叶子因子数据不足时不写缓存，
   下游复合因子以 `deps={}` 静默计算错误值，而非抛出异常。
   修复：无论结果是否为空均写入缓存，让复合因子显式报错。

**P2 — 应修复**

3. `ImportError` 静默返回（`engine/qlib_bridge/dumper.py:204`）
   qlib 未安装时 `dump()` 返回 `None`，调用方无法区分"无数据"与"依赖缺失"。
   修复：改为 `raise RuntimeError`。

4. `end_date` 类型检查脆弱（`engine/qlib_bridge/dumper.py:301`）
   `hasattr(end_date, 'strftime')` 在 `df['date']` 为字符串列时静默跳过格式化。
   修复：统一用 `pd.Timestamp(end_date).strftime('%Y-%m-%d')`。

5. `project_root` 从 `data_lake_dir.parent` 反推（`infra/storage.py:440`）
   若 `data_lake_dir` 配置为多级路径（如 `data/lake`），`.parent` 结果错误。
   修复：在 `ConnectionManager` 中直接暴露 `project_root` 属性。

6. 因子名未校验直接拼入 SQL 路径（`infra/query.py:35`）
   `factor_names` 来自调用方，含单引号或路径分隔符时会破坏 SQL。
   修复：插值前断言每个名称匹配 `[A-Za-z0-9_]+`。

**P3 — 可选**

7. `generate_factor_id` 在热路径内 import（`engine/factor/incremental_updater.py:74`）
   移至文件顶部。

8. `topological_sort_for_update` 是纯别名（`factor_library/dag.py:328`）
   无实际逻辑，属投机性抽象。建议直接调用 `topological_sort`，待路径真正分化时再拆分。