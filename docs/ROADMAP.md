# 实施路线图（ROADMAP）

> 状态：施工排序稿，待 Test / Review
> 权威来源：[design/ARCHITECTURE.md](design/ARCHITECTURE.md) + `design/<域>/` 39 份模块文档
> 本文只做**排序、DoD、迁移与流程**，不重述架构结论、不推翻选型、不裁决设计缺口
> 最后更新：2026-10-13

`docs/design/` 是唯一权威。本文件是**施工顺序**，不是设计文档。任何与 `design/ARCHITECTURE.md` 的冲突，以总纲为准，并把冲突登记进 §7「设计缺口」，不要在实施里自行抹平。

---

## 1. V1 边界（股票 + ETF）

**V1 做完的判据**：§3 的 S1–S12 全部通过 + S8 黄金样本五年基线落盘 + §5 迁移批次 M1–M2 完成。

**V1 明确不做**（只登记，见 §8）：

| 项 | 归属 | 一句话 |
|---|---|---|
| 可转债 | V2 | T+0、无印花税——`ASSET_RULES` 加一行 |
| 期货（含连续合约、夜盘归属、保证金账本） | V3 | 新账本，不复用 SimAccount |
| 期权 | V4+ | 不推荐散户，最后做 |
| L2 / tick 表 `tick_futures_l2` | Phase 2 | schema 已预留 27 列，使用路径已定（离线因子原料） |
| QmtBroker 实盘接入 | V2 | `mode: live` 的 Broker 实现 |
| GitHub Actions CI | V2 | V1 先本地三档耗时预算 |
| Streamlit 看板 / 邮件告警 | V2 | V1 终端 + Markdown 日报 |
| 拆单（TWAP/VWAP）、部分成交、跨日挂单 | V2 | V1 全成或不成 |
| cvxpy 求解器 | V2 备选 | V1 纯 numpy 裁剪 + 解析解 |

**V1 资产范围**：A 股股票 + ETF。A 股特殊机制（T+1 / 涨跌停 / 印花税 ETF 免）在 `core/asset_rules.md` 一张表内闭合。

---

## 2. 排序原则

1. **依赖方向优先**：`core/` 最先（红线 3），`data/` 读路径早于研究链，研究链早于执行链，`ops/` 最后（只观测不被依赖）。
2. **黄金样本是 V1 闸门**：S8 之前的所有阶段只为让五步验证法跑得起来；S9 之后是 V1 的广度补齐。
3. **一个不可再分的编码改动整体交给一个执行者**，不按函数切片。下文「任务切分」里的每个 `T*` 就是一个派单单位。
4. **红线 7（一次只动一边）**：每阶段只落新实现，不同时改旧实现；归档旧件走 §5 的独立批次。
5. **每个阶段的 DoD 是可复跑的命令 + 可判定的输出**，不是「代码写完了」。

---

## 3. 实施序列

### S1 · core 共享工具底座

| 项 | 内容 |
|---|---|
| **目标** | 落地 `core/` 五件共享工具，定死跨域类型与配置口径；后续各域只依赖 core，core 不依赖任何域。 |
| **模块** | `design/core/types.md` · `config.md` · `logging.md` · `registry.md` · `asset_rules.md` |
| **前置** | 无 |
| **产出物** | `core/{types,config,logging,registry,asset_rules}.py` + `tests/unit/test_core_*.py` |

**任务切分**（每项一个 owner，整体交付）

- **T1 `core/types.py` + `core/asset_rules.py`** —— 跨域 dataclass 归属索引本体 + `ASSET_RULES` 查表。两者都是纯定义，无 IO。
- **T2 `core/config.py` + `core/logging.py`** —— 三层配置加载（TOML 全局 / 策略级 / `.env`）+ JSONL 日志与 `logs/runs/` 三 schema。
- **T3 `core/registry.py`** —— `FactorRegistry` + `@register_factor` + `list_factor_meta()`（代码即注册表）。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_core_*.py -q` | 全绿；至少覆盖 `load_config` 策略级覆盖全局同名键、`get_asset_rules("unknown")` 抛 `KeyError`、`load_secrets` 不进 `config_snapshot` |
| `uv run ruff check .` | 退出码 0 |
| `python -c "from core import types, config, logging, registry, asset_rules"` | 可导入、无副作用 |
| `grep -rnE "^import (data\|research\|execution\|ops)\|^from (data\|research\|execution\|ops)" core/` | **无输出**（红线 3） |
| 一致性断言 | `get_asset_rules("etf")["stamp_tax"] is False` 且与 `FeeModel._is_etf` 前缀表 `("51","56","58","15","16")` 一致；FeeModel 未落地前先做前缀表单向断言，落地后补双向对拍（`core/testing.md` 一致性校验点） |
| `.gitignore` | 含 `config/.env`（红线 8） |

---

### S2 · 测试骨架与黄金样本夹具

| 项 | 内容 |
|---|---|
| **目标** | 按三层 pytest 目录搭好测试骨架，产出黄金样本手算 Excel 与 fixtures，把五步验证法的容差 / 预期区间 / 排查表写成可执行断言。 |
| **模块** | `design/core/testing.md` |
| **前置** | S1（fixtures 加载要用 config/types） |
| **产出物** | `tests/{unit,integration,golden}/`、`tests/golden/golden_prices.parquet`、`tests/golden/golden_expected.json`、`TOLERANCE = 1e-4`、hypothesis 性质用例骨架 |

**任务切分**

- **T1 测试目录与三层耗时预算** —— `unit < 10s` / `integration < 60s` / `golden < 5min`；测试替身表落成 fixtures（akshare→预存 Parquet、DuckDB→tmp_path、QMT→SimBroker、时钟→固定日期注入）。
- **T2 黄金样本夹具与手算基线** —— 5 ETF × 3 个月价格夹具 + 手算 Excel + `golden_expected.json`（4 位小数）。

**前置阻塞（必须先解）**

- 黄金样本 ETF 标的池未定稿（`core/testing.md` 实施时定稿项：教程 §2 清单 vs `510300/510500/518880/511010/159915` 自选池）。见 §7 缺口 #4。**池子未定，T2 无法开工。**

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/ -q --collect-only` | 三层目录均有收集，用例数 > 0 |
| `uv run pytest tests/golden/ -q` | fixtures 形状断言通过（5 标的 × 约 60 交易日）；`golden_expected.json` 与手算 Excel 逐字段 4 位小数一致 |
| `uv run ruff check .` | 退出码 0 |

---

### S3 · 存储与取数底座

| 项 | 内容 |
|---|---|
| **目标** | 11 张表 schema 契约 + Parquet 原子写/生命周期 + 研究侧统一取数入口，data 域读写闭环。 |
| **模块** | `design/data/storage_schema.md` · `storage_ops.md` · `data_api.md` |
| **前置** | S1 |
| **产出物** | `data/storage_schema.py`（`SCHEMA` + 11 pandera）、`data/storage_ops.py`（`write_parquet_atomic` / `FileLock` / `cleanup_*` / `check_capacity`）、`data/data_api.py`（`DataAPI` / `load_bars` / `load_bars_chunked` / `to_alphalens_*`） |

**任务切分**

- **T1 `storage_schema.py`** —— 11 张表 pandera schema、通用列约定、`quality_flag` 位定义、双轨分区约定。
- **T2 `storage_ops.py`** —— 原子写三阶段（lock → tmp → rename）、`FileLock`、`lifecycle.yaml` 清理、容量三档告警、`scripts/backup.sh`。
- **T3 `data_api.py`** —— `DataAPI`（内存 DuckDB + `read_parquet` 视图）、`load_bars`（表映射 + 复权分流 + 5min/60min `time_bucket` 聚合）、alphalens 适配出口。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_storage_schema.py -q` | 11 张表 schema 校验通过；非法列名/类型被拒；`quality_flag` int64 ≥ 0 |
| `uv run pytest tests/unit/test_storage_ops.py -q` | 原子写中途失败不留半截文件；`FileLock` 同天并发写抛 `ConcurrentWriteError`；`cleanup_expired(dry_run=True)` 不删任何文件 |
| `uv run pytest tests/integration/test_data_api.py -q` | `load_bars` 返回列 = `date,ticker,open,high,low,close,volume,amount,[adjfactor]` 且按 `(date,ticker)` 排序；`adjust="forward"` 与手算 `qfq = raw×adj/adj_latest` 一致（容差 1e-6）；`to_alphalens_prices` 形状 = date × ticker |
| 性能 | 5 ETF 日频 `load_bars` < 1s（`data_api.md` 验收线） |
| `uv run ruff check .` | 退出码 0 |

---

### S4 · 因子接口与运行时

| 项 | 内容 |
|---|---|
| **目标** | Factor ABC + FactorDAG 落地：因子声明依赖、拓扑排序、缓存、增量、层内并行、失败传导。 |
| **模块** | `design/research/factor_interface.md` · `factor_runtime.md` |
| **前置** | S1（types/registry）、S3（`load_bars` 供 `depends_on()` 装配） |
| **产出物** | `research/factor_interface.py`、`research/factor_runtime.py` |

**任务切分**

- **T1 `factor_interface.py`** —— `Factor` ABC（`depends_on()` + `compute()`）、元数据类属性、依赖前缀约定（`"bars_daily"` / `"factor:xxx"`）、四类 `FACTOR_CATEGORIES`。
- **T2 `factor_runtime.py`** —— `FactorDAG`（Kahn 拓扑 + `topological_layers`）、`compute_depends_hash`、`compute_factor`（缓存命中/append/full）、`compute_all` / `compute_all_safe`、`MLFactor` 骨架。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_factor_interface.py -q` | `depends_on()` 解析；输出 schema = `DataFrame[date, ticker, {factor_name}]`；元数据类属性可被 `list_factor_meta()` 提取 |
| `uv run pytest tests/unit/test_factor_runtime.py -q` | 环检测抛 `ValueError`；依赖先算；缓存 key = `(name, version, date_range, depends_hash)`，`force=True` 覆盖；version 变 → full；上游失败 → 下游跳过、同层独立因子继续 |
| `uv run pytest tests/unit/test_factor_runtime.py -q -k lookahead` | `test_no_lookahead`：删最后一天后历史值不变（`np.allclose(equal_nan=True)`） |
| `uv run ruff check .` | 退出码 0 |

---

### S5 · 因子存储与后处理

| 项 | 内容 |
|---|---|
| **目标** | per-factor-one-table 落盘（原始值）+ 四步后处理流水线；存原始值、消费时后处理。 |
| **模块** | `design/research/factor_storage.md` · `post_processor.md` |
| **前置** | S4 |
| **产出物** | `research/factor_storage.py`、`research/post_processor.py` |

**任务切分**

- **T1 `factor_storage.py`** —— `save_factor_append` / `save_factor_full` / `load_factor` / `load_factor_matrix` / `write_metadata`；目录 `factors/{name}/v{version}/{year}.parquet + metadata.json`。
- **T2 `post_processor.py`** —— `winsorize_mad` / `fillna_cross_section` / `standardize_zscore|rank` / `neutralize` + `PostProcessor` Pipeline（预设 `PIPELINE_RESEARCH` / `PIPELINE_STOCK_EVAL` / `PIPELINE_ETF` / `PIPELINE_RANK`）。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_factor_storage.py -q` | append 年文件 concat + `drop_duplicates([date,ticker])`；full 覆盖当前版本目录；版本目录共存不覆盖；`metadata.json` 含 `depends_on` / `depends_hash` / `date_range` |
| `uv run pytest tests/unit/test_post_processor.py -q` | 四步顺序 = 去极值→填充→标准化→中性化；每步 `groupby(date)` 截面独立（全样本统计量用例必须失败） |
| `uv run pytest tests/unit/test_post_processor.py -q -k hypothesis` | hypothesis：`zscore 后 mean≈0, std≈1`（逐截面） |
| `uv run ruff check .` | 退出码 0 |

---

### S6 · 合成与统一信号

| 项 | 内容 |
|---|---|
| **目标** | 多因子合成 + `AlphaSignal` + score→weights；三步主链第一步的产出物定型。 |
| **模块** | `design/research/factor_combiner.md` · `alpha_signal.md` |
| **前置** | S5 |
| **产出物** | `research/factor_combiner.py`、`research/alpha_signal.py` |

**任务切分**

- **T1 `factor_combiner.py`** —— `FactorCombiner`（`equal_weight` 主路径 / `ic_ir_weight` 可选 / 负 IC 截 0 / 扩张窗口 + 月频 + IC 只用 t-1）+ 8 指标评估 + `eval_report.json`。
- **T2 `alpha_signal.py`** —— `AlphaSignal`（frozen dataclass：`frame` / `meta` / `reason` / `factor_snapshot`）、`from_multi_factor` / `from_ml_factor`、`signal_to_weights`（`top_k` 默认 / `proportional` 负分 clip 0）、日终落盘 cross/timeseries 两目录。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_factor_combiner.py -q` | 等权 baseline 必跑；ICIR 加权负 IC 截 0 且权重和 1；权重只用 t-1 及之前 IC（当期 IC 用例必须失败） |
| `uv run pytest tests/unit/test_alpha_signal.py -q` | `to_weights(top_k, k)` = 1/k 等权；`proportional` 负 score clip 0 |
| `uv run pytest tests/unit/test_alpha_signal.py -q -k hypothesis` | `weights.sum() ≤ 1 + 1e-9`；无负权重（禁做空） |
| `uv run ruff check .` | 退出码 0 |

**前置阻塞**：`proportional` 对负 score 的 clip 0 在 `factor_combiner.md` / `alpha_signal.md` 同源标注「待定稿」，见 §7 缺口 #14。T2 可先按文档正文的 clip 0 实现，Review 时确认。

---

### S7 · 执行域主链与截面回测

| 项 | 内容 |
|---|---|
| **目标** | 三步主链的 Portfolio 与 Execution 落地：`TargetPortfolio` → 订单 → SimBroker（风控+撮合+记账）。回测与实盘同一代码路径，差异只注入在 Broker 实现（红线 4）。 |
| **模块** | `design/execution/portfolio_construction.md` · `order_generation.md` · `risk_control.md` · `fee_model.md` · `ledger.md` · `broker.md` · `design/research/cross_sectional_backtest.md` |
| **前置** | S1（types/asset_rules）、S3（价格/成交量取数）、S6（`AlphaSignal`） |
| **产出物** | 七个模块的实现 + `BaseCrossStrategy` 策略接口 + SimBroker 全链路 |

**任务切分**（七个模块互有调用，按「一个不可再分的改动整体交给一个执行者」切四个任务）

- **T1 `ledger.py` + `types` 落地** —— 5 dataclass + 3 枚举 + 6 态状态机 + `Journal`（append-only 按日 Parquet）+ `settle_fill` / `release_frozen`（资金流转修正版：每笔 fill「冻结→支出」，终态退剩余）。
- **T2 `fee_model.py` + `risk_control.py`** —— 费用四件套（佣金万 2.5 最低 5 元 / 印花税万 5 卖出 / 过户费万 0.1 / 滑点 0.1%）+ 风控七规则 + `risk_audit.jsonl`。两者都吃 `ASSET_RULES`。
- **T3 `portfolio_construction.py` + `order_generation.py`** —— `PortfolioConstraints` 纯 numpy 裁剪 + 手数取整 → `TargetPortfolio`；`compute_order_diff` / `generate_orders` / `check_split_needed`（>1% 日成交量只告警）。
- **T4 `broker.py` + `cross_sectional_backtest.py`** —— `Broker` ABC 四方法、`SimBroker`（`submit_orders` = 风控+撮合+记账）、`create_broker` 工厂、`BaseCrossStrategy.on_rebalance` + 回测主循环 for-loop。

**前置阻塞（必须先解，否则 T4 无法切分）**

- `research/cross_sectional_backtest.md` 的 `simulate_single_trade` 与 `execution/broker.md` 的 `submit_orders()` 内部三步职责重叠，且两者都自称薄壳的一部分；`research/ ↔ execution/` 依赖方向与总纲 §2.1 硬规则冲突。见 §7 缺口 #1 / #2。**Orchestrator 裁决前，T4 只做 `Broker` + SimBroker，`cs_portfolio_sim` 挂起。**
- 涨跌停拒单归属（风控七规则 vs `pre_check` vs 六道关）未定，见 §7 缺口 #3。T2 可先实现七规则，涨跌停检查位留接口。
- `get_latest_price` / `get_prices` / `get_daily_volume` 签名未定，见 §7 缺口 #10。三处调用方共用，T3/T4 开工前定一份。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_ledger.py -q` | 6 态状态机合法转换；非法转换抛错；`cash + Σ market_value == nav`（每笔成交后，hypothesis）；冻结资金终态退剩余，永不扣穿现金 |
| `uv run pytest tests/unit/test_fee_model.py -q` | 四件套手算对拍（含最低 5 元）；hypothesis：`fee ≥ 0`、卖出费用 ≥ 买入费用（印花税）；ETF 免印花税与 `ASSET_RULES` 一致 |
| `uv run pytest tests/unit/test_risk_control.py -q` | 七规则逐条构造触发；BLOCK 整批拒；单笔金额过小 SKIP 不拒整批；`risk_audit.jsonl` append-only 有记录 |
| `uv run pytest tests/unit/test_portfolio_construction.py -q` | `weights.sum() ≤ 1+1e-9`、无负权重（hypothesis）；`floor_to_lot` 结果是 lot_size 整数倍且不超预算 |
| `uv run pytest tests/unit/test_broker.py -q` | `submit_orders` 三步顺序；无价格 → REJECTED（停牌）；回测/实盘走同一 `submit_orders` 路径，仅 Broker 实现不同（红线 4） |
| `uv run pytest tests/integration/test_pandas_matches_hand_calc.py -q` | pandas 实现 vs `golden_expected.json` 手算，`total_return/sharpe/mdd` 差 < `TOLERANCE`（1e-4） |
| `uv run pytest tests/integration/test_simbroker_matches_pandas.py -q` | SimBroker 全链路 vs 纯 pandas，< 0.01% |
| `uv run ruff check .` | 退出码 0 |

---

### S8 · 黄金样本五步验证（V1 出口闸门）

| 项 | 内容 |
|---|---|
| **目标** | 跑通总纲 §6 五步验证法，产出 2020–2024 五年基线落盘。**这是 V1 主链的验收闸门，不过闸不算完。** |
| **模块** | 无新模块；引用 `design/core/testing.md` 的五步验证表与超范围排查表 |
| **前置** | S2（夹具）、S3（取数）、S6（信号）、S7（执行） |
| **产出物** | `tests/golden/` 五步测试全绿 + `tests/golden/golden_baseline_2020_2024.json`（metrics 落盘做回归基准） |

**五步验证法（逐条可判定，引用 `design/core/testing.md`）**

| 步 | 内容 | 命令 | 通过判据（容差） |
|---|---|---|---|
| 1 | 手算 Excel 对拍（月末调仓 3 个月） | `uv run pytest tests/golden/test_step1_handcalc.py -q` | 4 位小数一致 |
| 2 | pandas 实现 vs Excel | `uv run pytest tests/golden/test_step2_pandas.py -q` | < 0.01% |
| 3 | SimBroker 全链路 vs pandas | `uv run pytest tests/golden/test_step3_simbroker.py -q` | < 0.01% |
| 4 | 扣费前后对比 | `uv run pytest tests/golden/test_step4_fees.py -q` | 费用与 FeeModel 手算一致 |
| 5 | 2020–2024 五年完整基线 | `uv run pytest tests/golden/test_step5_baseline.py -q` | metrics 落盘，且落在预期区间 |

**预期区间**（`design/ARCHITECTURE.md` §6 / `core/testing.md`）：年化 **10–15%** / 夏普 **0.8–1.2** / MDD **15–25%**。

**超范围排查表**（`core/testing.md`，命中任一条 → 该步骤判 FAIL，按表定位后再跑）：

| 症状 | 排查方向 |
|---|---|
| 年化 > 20% | 查未来函数（`shift(1)` / 成交价基准） |
| 夏普 > 2 | 查前视（权重用了当期信息） |
| MDD < 10% | 查是否漏了极端行情段（2022 / 2024） |
| 费用 < 1% | 查 FeeModel 是否漏计（印花税卖出 / 最低 5 元） |

**黄金样本定义（红线 6）**：平台级 = **等权 top-5 ETF 动量**（教程 §2 双重动量变体，≤30 行 akshare 可重现）。旧 `baseline_spec.py` 是期货 ALSTM 自身回归基准，**永不与本样本混用**。

**前置阻塞**：缺口 #4（ETF 标的池）、#5（成交价基准双口径未选定）必须在本阶段开工前定稿。

---

### S9 · 数据接入与 DQC

| 项 | 内容 |
|---|---|
| **目标** | 单源入库链路闭环：`raw → normalize → DQC → asset/`；HARD 拒收进 quarantine，SOFT 打 `quality_flag`，watermark 增量。 |
| **模块** | `design/data/source_adapter.md` · `quarantine_raw.md` · `dqc_rules.md` · `dqc_severity.md` · `dqc_runtime.md` |
| **前置** | S3（storage）、S1（logging/config） |
| **产出物** | `data/source_adapter.py` + `data/adapters/`（akshare / tushare / amazingdata 子类）、`data/normalizers/`、`data/quarantine.py`、`data/dqc/`（rules 5 YAML + checks + engine + severity + report） |

**任务切分**

- **T1 `source_adapter.py` + adapters + normalizers + `quarantine.py`** —— Adapter ABC 四方法 + watermark + 限流/重试/超时装饰器 + 三源子类 + 每源一个 Normalizer + raw 落盘（zstd）+ quarantine 三路径修复。
- **T2 `dqc/rules/*.yaml` + `dqc/checks/`** —— 44 条四维规则 YAML（HARD 27 / SOFT 17）+ `custom_checks.py`（~15 个复杂检查）+ `cross_checks.py`（K06–K08）。
- **T3 `dqc/engine.py` + `dqc/severity.py` + `dqc/report.py`** —— `build_schema` + `ingest_with_dqc` 七步入库 + `DQCSeverityConfig` / `BatchRejectConfig` / `QualityFlag` 位掩码 + 报告双表（`_dqc_results.parquet` 月分区 / `_dqc_batch_summary.parquet`）+ CLI（`reingest` / `revalidate` / `quarantine` / `dashboard`）。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/integration/test_ingest_pipeline.py -q` | 干净小样本 44 条 0 failure；每条 HARD 构造恰好触发的脏数据 → 阻断入库；K06–K08 构造不一致 case 被拦；拒收批次落 `quarantine/{asset}/{date}.parquet` + `.rejection.json` |
| `uv run pytest tests/unit/test_dqc_severity.py -q` | `QualityFlag` 位运算：`quality_flag & 1` 过滤生效；`max_hard_drop_rate > 20%` → 整批拒收；`soft_alarm_rate > 50%` → 告警不拒收 |
| `python -m dqc.report quarantine` | 可列出隔离区批次，字段齐全 |
| `uv run pytest tests/integration/test_ingest_pipeline.py -q -k watermark` | watermark 幂等重跑安全；`cleanup_raw` 只删 `.parquet`，不误删 `.watermarks.json` |
| 每个 SourceAdapter 的字段映射 | 停牌 / 缺值 / CA 事件三个边界各有测试（`ingestion.md` §21.3 停牌复权 bug 必须规避） |
| `uv run ruff check .` | 退出码 0 |

---

### S10 · 多源仲裁、日历复权与数据修订

| 项 | 内容 |
|---|---|
| **目标** | 主备源协作口径落地（单源写 asset、多源只校验补缺）；交易日历 / 复权 / 连续合约全平台统一坐标系；数据回溯修正闭环。 |
| **模块** | `design/data/ingestion.md` · `calendar_adjustment.md` · `data_revision.md` |
| **前置** | S9 |
| **产出物** | `data_platform/config/source_priority.yaml`、`data/ingestion.py`、`data/calendar_adjustment.py`、`data/data_revision.py` |

**任务切分**

- **T1 `ingestion.py` + `source_priority.yaml`** —— 主源指定表、`cross_source_check`（容差五档）、分级响应（静默 / WARNING / ERROR + adjfactor 始终告警）、fallback 补数带 `source_tag="{backup}:backfill"`。
- **T2 `calendar_adjustment.py`** —— `TradingCalendar`（bisect）、`assign_trade_date_futures`（夜盘归属）、`build_continuous`（换月累积 adjfactor）、`PriceAccessor`（raw / qfq / returns 三口径）。
- **T3 `data_revision.py`** —— `detect_corrections`（周扫近 30 交易日）/ `apply_corrections`（先记日志再覆盖写）/ `assess_correction_impact`（LOW/MEDIUM/HIGH）/ `mark_factors_stale`。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/unit/test_calendar.py -q` | `is_trading_day` / `prev` / `next` / `range` / `count` 正确；越界抛 `IndexError`；夜盘 `hour≥21` 归次日、凌晨段归当日；`hfq = raw×adj`、`qfq = raw×adj/adj_latest` 与手算一致 |
| `uv run pytest tests/unit/test_ingestion.py -q` | 容差五档（price 0.01 绝对 / volume,amount 0.001 相对 / adjfactor 1e-6 / pe_pb 0.02 / default 0.001）；adjfactor 不一致**始终告警**；fallback 带 `source_tag` 且写 Parquet **metadata 不写数据列** |
| `uv run pytest tests/unit/test_data_revision.py -q` | `CORRECTION_TOLERANCES` 四字段；HIGH = adjfactor 变动 / MEDIUM = `max_rel_diff > 0.01` / LOW = 其余；只 MEDIUM+ 告警；修正走完整 `daily_ingest` 流水线 |
| `uv run ruff check .` | 退出码 0 |

---

### S11 · 评价、治理与策略配方

| 项 | 内容 |
|---|---|
| **目标** | 因子准入/退出门控、alphaType 配方治理、ML 信号治理落地；attempt_log 多重检验记账。 |
| **模块** | `design/research/factor_evaluation.md` · `factor_governance.md` · `alpha_type.md` · `ml_signal.md` |
| **前置** | S5（因子值）、S6（IC 序列 / AlphaSignal） |
| **产出物** | `research/factor_evaluation.py`、`research/factor_governance.py`、`research/alpha_type.py`、`research/ml_signal.py`、`data/registry/alpha_types.json` + `alpha_type_publishes.json` + `audit_log.jsonl` |

**任务切分**

- **T1 `factor_evaluation.py`** —— `AlphalensAdapter`（四静态方法）、`FactorPipeline`（compute → preprocess → evaluate → register）、`EvalResult`、门控 `t > max(2, √(2·ln N))` per-factor-family、产出三层落地（标量→registry / 序列→`factor_eval/` / tearsheet→HTML）。
- **T2 `factor_governance.py`** —— 8 指标评估（`evaluate_factor`）、健康三档（`check_factor_health`）、冗余去重（`detect_redundant_factors`，Spearman >0.7）、`diagnose_factor` 五分支、`plot_factor_report` 2×2 四图、单测四范式。
- **T3 `alpha_type.py`** —— 双表 JSON 注册、7 维标签全枚举、五态生命周期、`Rule` ABC + `RulePipeline`、`run_daily_signal`（失败 hold 不清仓）、`build_portfolio` 解析解 + 约束检查。
- **T4 `ml_signal.py`** —— `MLSignalAdapter`（scores / probabilities→logit）、`smooth_signal`、`build_feature_matrix` + `features_hash`、模型部署目录、`experiments/` JSON 目录、`ensemble_models`（信号层默认等权）、`sanity_check_ml_model` 过拟合三检。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/integration/test_factor_eval.py -q` | Adapter 四方法单测；3–5 只票集成；与直接调 alphalens API 对拍 IC/IR 一致；门控公式 `t > max(2, sqrt(2*ln(N)))` 生效（N=1 时退化为 t>2） |
| `uv run pytest tests/unit/test_factor_governance.py -q` | 入库线 IC>0.04 / IR>0.5 / t>2 / 相关<0.5；退出线 IC<0.03 或 rolling 36 月 t<1.5；生命周期 `draft→production→deprecated` 转换合法 |
| `uv run pytest tests/unit/test_alpha_type.py -q` | 7 维标签禁自由文本；五态转换条件校验；`run_daily_signal` 失败 → 重试 1 次 → hold 上日 Portfolio（**不清仓**）；`audit_log.jsonl` append-only |
| `uv run pytest tests/unit/test_ml_signal.py -q` | `sanity_check_ml_model` 三检判定（夏普 >2 → SUSPECT；夏普未超等权 10% → REJECT）；`features_hash` 变更使 model_id 失效 |
| `uv run ruff check .` | 退出码 0 |

**前置阻塞**：ensemble 权重口径两处同源未定（缺口 #14）；`data_revision` 的 `depends_on` 取值枚举未定（`data_revision.md`）。T4 可先按默认等权实现。

---

### S12 · 运维闭环

| 项 | 内容 |
|---|---|
| **目标** | 日频流水线端到端可跑，指标 / 告警 / 日报闭环。 |
| **模块** | `design/ops/scheduler.md` · `monitoring.md` · `reporting.md` · `alerting.md` |
| **前置** | S9（数据 Job）、S11（信号 Job） |
| **产出物** | `ops/{scheduler,monitoring,reporting,alerting}.py` + `logs/runs/jobs.jsonl` + `data/ops/{metrics.jsonl,alerts.jsonl,reports/}` |

**任务切分**

- **T1 `scheduler.py`** —— APScheduler `BlockingScheduler` + `run_job`（重试 / 超时 / `on_failure: skip|hold`）+ 5 类 Job + `daily_pipeline`（数据→因子→信号→巡检→日报）+ `jobs.jsonl`。
- **T2 `monitoring.py` + `alerting.py`** —— 策略级 8 指标三档 + 系统级 4 指标 → `metrics.jsonl`；告警 3 类规则 + 8 条条件 + 通道分层（终端全级别 / 桌面通知 CRITICAL）+ 去重 key + severity 只升不降。
- **T3 `reporting.py`** —— `write_daily_report`（Markdown 指标表 + 告警列表 + nav png）、`alpha_beta_attribution`（linregress）、因子贡献拆解复用 `factor_contribution()`、月末归因 Job。

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| 手动触发 `daily_pipeline` | 5 Job 顺序执行；`logs/runs/jobs.jsonl` 每条含 `run_id/job_name/started_at/finished_at/status/duration_s/error/retry_count/output_summary` |
| 人为制造数据 Job 失败 | 下游 Job `status=SKIPPED`，且收到一条 HIGH 告警；信号 Job 失败 → **hold** 上日 Portfolio（不清仓） |
| `uv run pytest tests/unit/test_alerting.py -q` | 同日同类型触发两次仅一条落盘；severity 只升不降；CRITICAL 走桌面通知 |
| `uv run pytest tests/unit/test_reporting.py -q` | 构造 `r_p = 0.01 + 0.8·r_b + noise` → α≈0.12 年化 / β≈0.8（容差 0.02）；`daily_{date}.md` 指标表/告警列表/链接齐全 |
| `uv run pytest tests/integration/test_daily_pipeline.py -q` | 跑完后 `data/ops/metrics.jsonl` 有当日快照 |
| `uv run ruff check .` | 退出码 0 |

---

### S13 · 迁移收口、归档与 V2+ 登记

| 项 | 内容 |
|---|---|
| **目标** | 执行 §5 的归档 / 迁移批次 M3–M7；登记 V2/V3/V4 与 L2 预留项。**本阶段本身不新增业务实现。** |
| **模块** | `design/data/l2_data.md`（使用路径登记，不展开）+ §5 迁移映射表 |
| **前置** | S1–S12 全部通过 |
| **产出物** | 归档 PR（批量删除 / 路径迁移，**分批回归**）、`docs/PROJECT_STRUCTURE.md` 更新、V2+ 登记清单 |

**DoD 与验证**

| 命令 | 通过判据 |
|---|---|
| `uv run pytest tests/ -q` | 迁移批次合入后全量测试仍全绿（每批一次） |
| `uv run ruff check .` | 退出码 0 |
| `grep -rn "vectorbt\|zvt_bridge\|qlib_bridge\|vnpy" --include="*.py" engine/ execution/ research/ data/ core/` | 归档批次完成后**无输出**（`libs/zvt/` 与 `pyproject.toml` 依赖清理归 M3/M7） |
| `docs/PROJECT_STRUCTURE.md` | 目录树与实际一致，旧「建议的后续收口」四项标注完成批次 |

**注意（红线 7）**：本阶段每一项都是「删除或移动旧件」，必须在对应新实现已跑通并完成回归之后单独成批执行，**绝不与新实现同批**。

---

## 4. 阶段序列总表

| # | 一句话目标 | 模块（份数） | 前置 | DoD 一行 |
|---|---|---|---|---|
| S1 | core 五件共享工具，core 不依赖任何域 | core: types·config·logging·registry·asset_rules（5） | — | `pytest tests/unit/test_core_*` 全绿 + core 无跨域 import |
| S2 | 三层测试骨架 + 黄金样本手算夹具 | core: testing（1） | S1 | fixtures 形状断言 + `golden_expected.json` 与 Excel 4 位一致 |
| S3 | 11 表 schema + 原子写 + 统一取数 | data: storage_schema·storage_ops·data_api（3） | S1 | schema 校验 + 原子写 + `load_bars` 复权与手算 1e-6 |
| S4 | Factor ABC + FactorDAG 缓存并行 | research: factor_interface·factor_runtime（2） | S1,S3 | 环检测 / 缓存失效 / `test_no_lookahead` |
| S5 | per-factor-one-table + 四步后处理 | research: factor_storage·post_processor（2） | S4 | 版本目录共存 + 后处理逐步 `groupby(date)` |
| S6 | 合成 + AlphaSignal + score→weights | research: factor_combiner·alpha_signal（2） | S5 | 等权 baseline + `weights.sum()≤1` + 无负权重 |
| S7 | TargetPortfolio→订单→SimBroker 全链 | execution: portfolio_construction·order_generation·risk_control·fee_model·ledger·broker（6）+ research: cross_sectional_backtest（1） | S1,S3,S6 | FeeModel 手算 + 账本恒等式 + `simbroker_matches_pandas` <0.01% |
| S8 | **黄金样本五步验证（V1 闸门）** | —（集成验收） | S2,S3,S6,S7 | 五步全绿 + 五年基线落盘且落在 年化 10–15% / 夏普 0.8–1.2 / MDD 15–25% |
| S9 | raw→normalize→DQC→asset 闭环 | data: source_adapter·quarantine_raw·dqc_rules·dqc_severity·dqc_runtime（5） | S1,S3 | 44 条 0 failure + 每条 HARD 恰好阻断 + quarantine 可查 |
| S10 | 多源仲裁 + 日历复权 + 修订闭环 | data: ingestion·calendar_adjustment·data_revision（3） | S9 | 容差五档 + 夜盘归属 + qfq 公式 + 修正走完整流水线 |
| S11 | 评价门控 + 治理 + 配方 + ML 信号 | research: factor_evaluation·factor_governance·alpha_type·ml_signal（4） | S5,S6 | 门控公式 + 五态生命周期 + 失败 hold 不清仓 |
| S12 | 日频流水线 + 指标/告警/日报闭环 | ops: scheduler·monitoring·reporting·alerting（4） | S9,S11 | 5 Job 顺序 + 失败传导 + 归因 α/β 容差 0.02 |
| S13 | 迁移收口、归档、V2+ 登记 | l2_data 登记（1）+ §5 批次 | S1–S12 | 全量测试仍绿 + 归档 grep 无残留 |

**模块计数**：5+1+3+2+2+2+7+0+5+3+4+4+1 = **39**。

---

## 5. 迁移路径

### 5.1 红线 7 的操作口径

> **不要在迁移期同时改新旧两套的同一实现。一次只动一边。**

每迁一个模块，固定三步，**每步单独成批 / 单独 PR**：

1. **新建**：在五域骨架下落地新实现 + 新测试，旧实现**完全不动**。
2. **切换**：把消费方（黄金样本、run 脚本、测试）指向新实现，跑通回归。
3. **归档**：单独一批删除或移动旧实现。删除前跑全量测试。

反例（禁止）：同一个 PR 里既改 `factor_library/base.py` 又新建 `research/factor_interface.py`。

### 5.2 旧 → 新映射

| 现行位置 | 去向 | 处置 | 批次 |
|---|---|---|---|
| `config/settings.toml`、`config/strategies.yaml` | `config/config.toml`、`config/strategies/{alpha_type}.toml` | 迁入。YAML→TOML（§4 选型定稿：YAML `on→True` 陷阱）；`.env` 进 `.gitignore` | M1 |
| `tests/`（3 个文件） | `tests/{unit,integration,golden}/` | 重组；`test_tl_intraday_reproduction.py` 随期货 ALSTM 回归基准**保留** | M1 |
| `factor_library/base.py`、`registry.py`、`dag.py`、`preprocessor.py` | `research/factor_interface.py`、`core/registry.py`、`research/factor_runtime.py`、`research/post_processor.py` | 迁入。Factor ABC 加 `depends_on()`、统一长表输出；Kahn 拓扑保留；后处理顺序改「去极值→填充→标准化→中性化」 | M2 |
| `factor_library/operators/` | `research/` 共享算子 | 迁入（共享逻辑不进策略目录） | M2 |
| `factor_library/stock/` | `research/` 因子实现 | 迁入（V1 范围） | M2 |
| `factor_library/futures/`、`factor_library/option/` | — | **登记不迁**（V3 / V4） | M-V3/M-V4 |
| `infra/storage.py`、`query.py`、`writer.py`、`connection.py` | `data/storage_ops.py`、`data/data_api.py` | 迁入。先落 data/ 新实现并让黄金样本读路径切过去，再归档 infra/ | M2 |
| `infra/scheduler.py` | `ops/scheduler.py` | 迁入 | M5 |
| `infra/futures_continuous.py`、`cross_instrument.py` | `data/calendar_adjustment.build_continuous` | **登记不迁**（V3） | M-V3 |
| `infra/qlib_processors.py`、`infra/spread_dataset.py` | — | **归档**（随 Qlib 桥接） | M3 |
| `engine/pipeline.py` | `research/cross_sectional_backtest.py` + `execution/*` | **拆分**。5 层叙事（Universe→Alpha→Portfolio→Risk→Execution）坍缩为三步主链 `AlphaSignal → Portfolio → Execution` | M2 |
| `engine/factor/backtest_runner.py`、`incremental_updater.py` | `research/factor_runtime.py` | 迁入 | M2 |
| `engine/simulation/models.py` | `execution/ledger.py` | 迁入 | M2 |
| `engine/models/`（alstm / lgbm / distribution_nll / model_diagnostic） | `research/ml_signal.py` + `research/factor_runtime.MLFactor` | 迁入。**期货 ALSTM 自身回归基准保留**（见 5.3） | M4 |
| `engine/ensemble/` | `research/ml_signal.ensemble_models` + `research/factor_combiner` | 迁入 | M4 |
| `engine/backtest/`（futures_backtest / intraday_futures / regime_filter / continuous_backtest_utils） | — | **登记不迁**（V3 期货回测） | M-V3 |
| `engine/qlib_bridge/`、`engine/vectorbt_engine/`、`engine/zvt_bridge/` | — | **归档，不做对照组**（§28.5：三套直接归档） | M3 |
| `libs/zvt/`（本地可编辑依赖） | — | **归档**（随 zvt_bridge 同批）；`pyproject.toml` 的 `zvt` / `vectorbt` / `pyqlib` / `prefect` 依赖同批清理 | M3 |
| `execution/position_manager.py` | `execution/portfolio_construction.py` + `execution/ledger.py` | 迁入 | M2 |
| `execution/risk_control.py` | `execution/risk_control.py` | 重写对齐七规则表 + `ASSET_RULES` 取参 | M2 |
| `execution/router/vnpy_connector.py` | — | **归档**（§66.2 不用 vnpy） | M3 |
| `execution/router/xtquant_connector.py`、`router/base.py` | `execution/broker.py`（`QmtBroker`） | **登记不迁**，V2 起点 | M6 |
| `scripts/data_fetcher/*` | `data/source_adapter` + `data/ingestion` 的 CLI 入口 | 迁入；AmazingData Docker 用法保留 | M2 |
| `strategies/stock/*` | `strategies/` + alphaType 配方 | 迁入 | M4 |
| `strategies/futures/*`（alstm / tl_intraday） | — | **登记不迁**（V3）；ALSTM 回归基准与报告保留 | M-V3 |
| `knowledge-base/` | — | 保留。**补充笔记，非权威** | — |
| `docs/PROJECT_STRUCTURE.md`「建议的后续收口」四项 | — | 列为后续批次，**本计划不执行** | M7 |

### 5.3 归档 vs 保留（逐条）

**归档（M3，三套桥接 + vnpy 通道 + 本地 ZVT fork）**

- `engine/qlib_bridge/` —— Qlib 二进制导出桥
- `engine/vectorbt_engine/` —— VectorBT 编译器/累加器
- `engine/zvt_bridge/` —— ZVT 记录器/同步器/适配器
- `execution/router/vnpy_connector.py` —— vnpy 通道（§66.2 不用 vnpy）
- `libs/zvt/` —— 本地可编辑 ZVT fork
- `infra/qlib_processors.py`、`infra/spread_dataset.py` —— 随桥接同批

依据：`ARCHITECTURE.md` §4「三套（VectorBT/ZVT/Qlib）**直接归档、不做对照组**」。VectorBT 仅在 V2 备选引擎清单里留名，不保留代码。

**保留（不删、不并）**

- **期货 ALSTM 自身回归基准** —— `ARCHITECTURE.md` §6：「旧 `baseline_spec.py`（sharpe 16 不可信）**保留作期货 ALSTM 策略自身回归基准，不再是平台级黄金样本**——两者不混用」。连带保留 `strategies/futures/alstm/`（含 `train.py` / `reporting.py` / `reports/` / `notebooks/`）与 `tests/test_tl_intraday_reproduction.py`。
  > **注意**：`316261b` 树中**不存在** `strategies/futures/alstm/baseline_spec.py`（`find . -name baseline_spec.py` 无结果）。设计要求「保留」的对象在仓库里找不到——见 §7 缺口 #15。
- **AmazingData Docker 用法与 API 限制**（`AGENTS.md` 现有段落仍有效）
- **`examples/`、`knowledge-base/`** —— 探索性与笔记，不被生产模块 import
- **`docs/archive/`** —— 冻结只读

**不混用（红线 6）**：平台级黄金样本 = 等权 top-5 ETF 动量；期货 ALSTM 回归基准 = 策略自身。两套基线各自独立，任何阶段不得互相当对拍参照。

---

## 6. 流程约定

### 6.1 Dev → Test → Review

```text
派单 → Dev 实现 + 自测 → Test 独立复跑 + 补边界 → Review 对照设计逐条核
                                                        │
                                          不通过 ───────┴──→ Dev-Debug ──→ 回 Test
                                          通过 ──→ 合入，进入下一个任务
```

| 角色 | 做什么 | 交付 |
|---|---|---|
| **Dev** | 领一个任务（`T*`），在分支上实现 + 自测；贴 DoD 命令的**实际输出** | 代码 + 测试 + 命令输出 |
| **Test** | 独立复跑该任务 DoD 命令；按 `core/testing.md` 单测覆盖表 + hypothesis 性质补边界用例；不改业务代码 | 通过 / 失败清单 + 新增用例 |
| **Review** | 对照 `design/<域>/<模块>.md` 逐条核接口契约、关键决策、8 条红线；不看「赶进度」 | 通过 / 打回（附出处） |

### 6.2 每步完成定义（DoD）

一个任务算完，必须同时满足：

1. DoD 表里的命令**都跑过**，输出贴在任务回执里；
2. 通过判据逐条勾对，失败项如实上报（失败的检查如实报告是有用的，只报通过不给输出不算）；
3. `uv run ruff check .` 退出码 0；
4. 不新增本任务未要求的依赖、不做顺手重构；
5. 红线 1–8 逐条自检通过。

### 6.3 Dev-Debug 回退路径

- Test 或 Review 打回 → **原 Dev** 在同一分支修 → 重跑 DoD → 回到 Test。
- **同一问题打回 2 次以上** → 升级给 Orchestrator：要么任务切分有问题，要么是设计缺口（转 §7 登记）。
- Review 打回必须给**设计文档出处**（文件 + 小节），不给出处的打回 Dev 可要求补。

### 6.4 Review 是必经关卡

**Review 不因赶进度跳过。** 没有 Review 通过记录的任务不得合入主干。阶段关闭的条件是该阶段**全部任务**都有 Review 通过记录。

---

## 7. 设计缺口清单

> 以下均为**梳理中发现的自相矛盾 / 缺失契约 / 不可验证之处**。本计划**不裁决**，逐条登记待 Orchestrator 定夺。每条标出处。

| # | 缺口 | 出处 | 影响阶段 |
|---|---|---|---|
| 1 | **`research/` 与 `execution/` 依赖方向自相矛盾。** 总纲 §2.1 硬规则写「`research/` 与 `execution/` 互不依赖」；但 `alpha_signal.to_weights()` 产出直接作 `portfolio_construction.PortfolioOptimizer.solve()` 入参；`cross_sectional_backtest` 写「模拟器直接用账本四件套 + 预检/佣金/手数/持仓更新公共逻辑」（账本与 FeeModel 在 execution 域）；`alpha_type.build_portfolio()` 产出 `TargetPortfolio`。`core/types.md` 可承载 dataclass 本体，但 `FeeModel` / `PortfolioOptimizer` 是类不是类型。 | `design/ARCHITECTURE.md` §2.1；`design/research/alpha_signal.md`；`design/research/cross_sectional_backtest.md`；`design/research/alpha_type.md`；`design/core/types.md` | S7 |
| 2 | **`cs_portfolio_sim` 与 `order_generation`/`broker` 职责重叠。** `cross_sectional_backtest.simulate_single_trade`（预检→资金约束→成交量约束→滑点→Order+Trade→更新持仓→Journal）与 `broker.submit_orders()` 内部三步（风控+撮合+记账）以及 `order_generation.compute_order_diff` 描述的是同一段执行逻辑；两份文档都自称「薄壳的一部分」。权威实现归属未定。 | `design/research/cross_sectional_backtest.md`；`design/execution/broker.md`；`design/execution/order_generation.md` | S7 |
| 3 | **涨跌停拒单归属未定。** §69.5 差异落层表把「涨跌停/禁做空」落 `pre_trade_risk_check`；§18.3 六道关② 涨跌停封板拒；§24.6/§24.7 `pre_check`（`strict_limit` 配置）也含涨跌停拒单；但 §66.3 风控七规则表未列该条（七条 = 单标的仓位/总仓位/单日换手/单笔金额/禁做空/禁 ST/流动性）。三处口径不一致，文档已自行照录待定。 | `design/execution/risk_control.md`「实施时定稿项」；`design/core/asset_rules.md` 差异落层表；`design/execution/fee_model.md` 六道关 | S7 |
| 4 | **黄金样本 ETF 标的池未定稿。** 教程 §2 具体清单 vs `510300/510500/518880/511010/159915` 自选池，二者未选。阻塞手算 Excel 与五年基线。 | `design/core/testing.md`「实施时定稿项」（§68.8） | S2、S8 |
| 5 | **成交价基准双口径未选定。** V1 = 决策日收盘价（简化）；保守版 = T+1 开盘 ± 滑点；「只取其一当唯一口径」被否决。黄金样本手算与 SimBroker 必须同口径，但文档未指明用哪个。 | `design/execution/fee_model.md`「成交价」关键决策；`design/execution/broker.md`「V1 收盘价撮合」 | S7、S8 |
| 6 | **`quality_flag` 精度口径不一致。** §47.15 定 int64 bitmask（5 位），§55.2 精度表记 INT8（0/1/2）。文档自己标注「实施时定稿其一」。 | `design/data/storage_schema.md`「实施时定稿项」 | S3、S9 |
| 7 | **`quality_filter` 参数语义与 `CLEAN=0` 自相矛盾。** 注释写「传 `QualityFlag.CLEAN` 只取干净」，但 `CLEAN=0` 时位运算恒不过滤。文档自己标注「应是『传要排除的问题位』，注释与代码不一致」。 | `design/data/dqc_severity.md`「实施时定稿项」 | S9 |
| 8 | **DQC 规则条数口径 40 vs 44。** 来源标题写 40 条（阈值拆分 24+14+2），四维表实为 10+14+11+9=44 行。文档照录未裁决。阈值三类来源统计也与逐条表口径不一致（约 24/14/2 vs 32/12）。 | `design/data/dqc_rules.md`「条数口径注记」与「阈值三类来源」 | S9 |
| 9 | **L13 ticker 正则与交易所覆盖不全。** L13 `^[036]\d{5}\.(SH\|SZ)$` 不含北交所 `8xxxxx`；期货 regex 含 INE，但日历只列 6 交易所（sse/szse/cffex/shfe/dce/czce）无 INE 文件。V1 股票+ETF 尚可，V3 期货会撞。 | `design/data/dqc_rules.md` L13「实施时定稿项」；`design/data/storage_schema.md` L13 regex 与日历 6 文件；`design/data/calendar_adjustment.md` INE 未出现 | S3、S9、S10 |
| 10 | **`get_latest_price` / `get_prices` / `get_daily_volume` 数据接口签名未定。** 三个执行域模块共用的取数入口没有权威契约，两份文档同时列为「与 order_generation 同源待定」。 | `design/execution/order_generation.md`「实施时定稿项」；`design/execution/broker.md`「实施时定稿项」 | S7 |
| 11 | **`NUMERIC_FIELDS[asset_type]` 多处引用、无处定义。** 跨源对比字段清单与修正比对字段清单都需要它，三份文档均标注「多处提及未统一定义」。 | `design/data/ingestion.md`；`design/data/data_revision.md`；`design/data/calendar_adjustment.md` | S10 |
| 12 | **`asset_type` 跨域命名不一致。** `data_api.load_bars(..., asset_type="equity")` 与 `TABLE_MAP` 键用 `equity`；`ASSET_RULES` 键用 `stock`/`etf`；`storage_schema` 表名用 `bars_daily`。总纲 §5「跨域契约一览」未给统一枚举名。 | `design/data/data_api.md`；`design/core/asset_rules.md`；`design/data/storage_schema.md`；`design/ARCHITECTURE.md` §5 | S1、S3、S7 |
| 13 | **monitoring 系统指标「数据条数偏差 ±30% HIGH」无对应告警条件。** §67.3 系统表与 §67.5 的 8 条告警条件表之间有缺口，文档自行照录未缝合。 | `design/ops/monitoring.md`「实施时定稿项」 | S12 |
| 14 | **ensemble 权重口径两处同源未定。** `factor_combiner`「ensemble 的权重如何进 FactorCombiner」与 `ml_signal`「ensemble 权重公式（validation ICIR 归一化细节）」是同一个问题的两个入口，均未定稿。 | `design/research/factor_combiner.md`；`design/research/ml_signal.md` | S11 |
| 15 | **`baseline_spec.py` 在仓库中不存在。** 总纲 §6 与 `core/testing.md` 都要求「保留作期货 ALSTM 策略自身回归基准」，但 `316261b` 树中 `find . -name baseline_spec.py` 无结果（`strategies/futures/alstm/` 下只有 `config.py / data.py / train.py / reporting.py / README.md`）。保留对象缺失，V3 回归基准无从谈起。 | `design/ARCHITECTURE.md` §6；`design/core/testing.md`；仓库实况 | S13、V3 |
| 16 | **`pyproject.toml` 依赖与选型定稿表不一致。** 描述仍写 "based on ZVT, VectorBT, and Qlib"，依赖含 `zvt` / `vectorbt` / `pyqlib` / `prefect==3.6.22`；§4 定稿为 pandas 自研 + SimBroker/QmtBroker、调度 = APScheduler、三套桥接归档。属迁移收口项，本计划只登记不改。 | `pyproject.toml`；`design/ARCHITECTURE.md` §4 | S13 / M3 |

---

## 8. V2 / V3 / V4 / Phase-2 登记（不展开）

| 项 | 阶段 | 触发条件 / 一句话 |
|---|---|---|
| 可转债 | V2 | `ASSET_RULES` 加一行（T+0 / 无印花税 / 涨跌幅待定）；`FeeModel` 去掉印花税分支 |
| QmtBroker 实盘 | V2 | `execution/router/xtquant_connector.py` 为起点；`mode: live` 切 Broker 实现（红线 4：不改主链） |
| 回测-实盘对拍（B）+ 模拟盘校准期（C） | V2 上线流程 | `broker.md` §22.3 四项指标：signal 一致率 / 成交价偏差 / 成交率偏差 / 累计净值跟踪误差 |
| VectorBT 备选引擎 | V2 | §4「留 V2 备选」；代码不保留，届时按需引入 |
| GitHub Actions CI | V2 | `core/testing.md`：V1 先本地三档耗时预算（unit <10s / integration <60s / golden <5min） |
| 邮件告警 / Streamlit 看板 | V2 | `ops/alerting.md` 通道表、`ops/monitoring.md` |
| 拆单 TWAP/VWAP、部分成交、跨日挂单、市价单 | V2 | `order_generation.md`：>1% 日成交量才拆，月频 ETF/大盘股远不到 |
| cvxpy 约束求解 | V2 备选 | `portfolio_construction.md`：V1 约束全是 box+线性 |
| 期货（连续合约 / 夜盘归属 / 保证金账本 / 逐日盯市） | V3 | 新账本，**不复用 SimAccount**；`infra/futures_continuous.py` 届时迁 `data/calendar_adjustment.build_continuous`；`engine/backtest/` 期货回测届时迁 |
| 期货 ALSTM 策略 | V3 | `strategies/futures/alstm/` 保留回归基准，届时迁 research/ml_signal + factor_runtime.MLFactor |
| 期权 | V4+ | 不推荐散户，最后做 |
| L2 / tick 表 `tick_futures_l2`（27 列） | Phase 2 | schema 已预留；使用路径已定 = **离线因子原料**（`l2_data.md` 路径 A），不进事件流；DQC/存储优化届时再做 |
| L2 微观结构因子脚本 | Phase 2 | `l2_data.md`：L2 表 → 离线因子脚本 → 日频/分钟频因子值 → FactorPipeline。**不加 TICK / DEPTH_UPDATE 事件**（红线 1/2） |
| 自适应 DQC 阈值（K09 滚动算法） | phase-2 | `dqc_rules.md`：先固定 + 统计阈值跑通 |
| 小文件 compaction / L2 按月合并 | Phase 2 | `storage_ops.md` |
| 数据湖迁移（`data_lake/` Hive → `data_platform/` 扁平） | 批次 M2/M7 | `storage_schema.md` / `storage_ops.md` 迁移脚本待写 |
| 配置中心 / 分布式追踪 / ClickHouse / 事件驱动内核 | **永不做** | §1 裁决原则：为 tick 级、多策略并发、多人协作而生的设施一律不引入 |

---

## 9. 建议的第一个派单

**任务 S1-T1 —— `core/types.py` + `core/asset_rules.py`**

理由：无上游依赖；是后续每个阶段 DoD 都要 import 的地基；红线 3（core 不 import 任何域）在本任务就能用一条 grep 断言钉死；体量小、判据硬，适合把 Dev → Test → Review 流程先跑通一遍。

**完成判据**（Test 复跑这五条）：

1. `uv run pytest tests/unit/test_core_types.py tests/unit/test_core_asset_rules.py -q` 全绿，至少覆盖：`get_asset_rules("unknown")` 抛 `KeyError`、`get_asset_rules("etf")["stamp_tax"] is False`、stock 前缀 `60/00/30/68/8` → `price_limits` 映射正确、Order/Trade/Position/Account/JournalEntry 五 dataclass + 三枚举可构造。
2. `uv run ruff check .` 退出码 0。
3. `python -c "from core import types, asset_rules"` 可导入、无副作用、无 IO。
4. `grep -rnE "^(import|from) (data|research|execution|ops)" core/` **无输出**（红线 3）。
5. 一致性：`ASSET_RULES["etf"]["stamp_tax"] is False` 与 `FeeModel._is_etf` 前缀表 `("51","56","58","15","16")` 同源一致（FeeModel 未落地前做前缀表单向断言，落地后补双向对拍）。
