# AGENTS.md

> **`docs/design/` 是唯一权威。本文件只是入口索引。**
>
> 架构范式、跨域契约、选型定稿、实施红线全部以 [`docs/design/ARCHITECTURE.md`](docs/design/ARCHITECTURE.md) 为准，
> 细节在 `docs/design/{core,data,research,execution,ops}/` 39 份模块文档；施工顺序在 [`docs/ROADMAP.md`](docs/ROADMAP.md)。
> 本文件不重复设计结论。若本文件与 `docs/design/` 有冲突，**以 `docs/design/` 为准**，并把冲突登记进 `docs/ROADMAP.md` §7。
> `knowledge-base/Quant/Quant_Platform/Quant_Platform 说明文档.md` 是补充笔记，同样不是权威。

## Project Overview

Quant_Platform is a thin-shell quantitative platform for China A-share (stock + ETF in V1). The rebuild's goal is
**architecture-paradigm alignment** with how top institutions layer their stack — not a port of their heavy engines.
Research iteration speed, engineering evolvability and live-trading readiness are consequences of the paradigm,
not separate targets.

## Common Commands

```bash
# Install dependencies
uv sync

# Start AmazingData MCP server
cd ~/AmazingData/docker && docker-compose up -d

# Run scripts in AmazingData container (在项目根目录下运行，docker-compose.yml 已配置 volume 挂载)
# ./scripts → /scripts, ./data_lake → /data
docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py

# Run tests (three tiers: unit / integration / golden)
pytest tests/
pytest tests/unit/
pytest tests/golden/

# Run a single test
pytest tests/unit/test_core_asset_rules.py
pytest tests/unit/test_core_asset_rules.py::test_unknown_asset_type_raises

# Lint and format
ruff check --fix .
ruff format .
```

## Architecture

Full detail lives in [`docs/design/ARCHITECTURE.md`](docs/design/ARCHITECTURE.md). The four load-bearing ideas:

### 1. Five-domain skeleton, split by operational domain

Not by pipeline stage.

```text
core/         共享工具（config / registry / logging / types / asset_rules）
data/         数据接入与存储（SourceAdapter / DQC / 存储 schema / DataAPI）
research/     投研（Factor / DAG / 后处理 / 合成 / 评价 / alphaType）
execution/    执行（组合构建 / 订单 / 风控 / 费用 / 账本 / 柜台）
ops/          运维（调度 / 监控 / 归因 / 告警）
```

Dependency directions (hard rule — see red line 3):

```text
research/ ──┐
            ├──→ core/ ←──┐
execution/ ─┘             │
data/     ──→（提供数据访问，不反向依赖）
ops/      ──→（观测所有域，不被依赖）
```

- `core/` imports **no** domain. No engine, no event loop, no time model — shared tools only.
- `research/` and `execution/` do not depend on each other.
- `ops/` is read-only observation; nothing depends on it.

The old `kernel/` (event loop / time model / scheduler) is **gone**. Its three jobs went to: the backtest script's
for-loop (time advance), nothing (event dispatch — no realtime event stream at monthly frequency), and `AlphaSignal`
(the old Signal class).

### 2. Three-step main chain — the old five-layer narrative is collapsed

```text
AlphaSignal → Portfolio → Execution
                            └── broker.submit_orders() 内部封装：风控 + 撮合 + 记账
```

| Old contract | Ruling | Replacement |
|---|---|---|
| Signal 三层（Blueshift / USignal / Tick） | collapsed to one layer | `research/alpha_signal.md` |
| Portfolio 层 | self-built | `execution/portfolio_construction.md` |
| Utrade / Themis / TB | collapsed | the three internal steps of `submit_orders()` — **not three modules** |

The interface boundary moved from *product* to *function*. Execution only knows the target book
(`TargetPortfolio`); it derives orders itself. The shell is a **translator, not a manager** — it converts framework
dialects into thin-shell dataclasses and lets each framework do what it is good at.

> **There is no "5-Layer Pipeline" (Universe → Alpha → Portfolio → Risk → Execution) and no
> `engine/pipeline.py` → `AlphaPipeline.run(kdata)` entry point.** That narrative predates §3.2 of the
> architecture baseline and has been removed.

### 3. Time model is a for-loop

```python
for date in rebalance_dates():
    ...
```

The four-layer time model (starfleet → EBReplay → EBClock → EBTimer) is **deprecated**, and so are backtrader / vnpy
event-driven frameworks. At monthly / daily frequency there is no tick alignment, no event-triggered advance, no
clock stepping — "decoupling compute from data volume" simply disappears when a full A-share cross-section runs in
<100 ms. The six company Services are six sequential calls inside `run()`; registration and discovery is constructor
injection; startup order is code order.

### 4. Factors: Factor ABC + FactorDAG

```text
Factor(compute) → FactorDAG(拓扑) → PostProcessor → per-factor-one-table
  → FactorCombiner → AlphaSignal   =  "DataFrame 流水线"
```

- `Factor` ABC: `depends_on() -> list[str]` + `compute(data) -> DataFrame[date, ticker, {factor_name}]`.
  Dependencies are declared, not inferred — data tables bare (`"bars_daily"`), upstream factors prefixed
  (`"factor:momentum_20d"`).
- `FactorDAG`: Kahn topological sort + layered parallel execution + cache keyed on
  `(factor_name, version, date_range, depends_hash)`.
- EBGraph / EventBrick event-subscription graphs are **deprecated**. The dependency graph survives; event
  subscription does not. Do not invent an event subscription mechanism for factors (red line 2).
- Evaluation layering: alphalens is the **pre-backtest screening gate** (fast IC / quantiles, no cost or
  limit-up modelling). When it contradicts the real backtest, **the backtest wins**.

### Finalized technology choices (§4)

| Dimension | Choice | Rejected (why) |
|---|---|---|
| Backtest engine | pandas self-built <300 lines + SimBroker/QmtBroker | backtrader / vnpy (heavy, event-driven not needed); VectorBT only as a V2 option |
| Storage | Parquet + DuckDB | ClickHouse (single machine <10 GB; standalone deploy violates zero-external-dependency) |
| Data ingestion | SourceAdapter four methods + watermark | company DataBus / udata container layer |
| Factor evaluation | alphalens-reloaded 0.4.6 | original alphalens |
| Config | TOML three layers + `.env` + git | YAML (`on`→True trap); XML; config center |
| Logging | Python logging + JSONL | ELK / Loki; distributed tracing |
| Testing | pytest three tiers + hypothesis + golden sample | CI in V1 (local time budgets first) |
| Multi-asset | config + `ASSET_RULES` + execution layer | C++ Traits compile-time polymorphism |
| Scheduling | APScheduler + `job_runs.jsonl` | distributed schedulers; crontab (no retry / no state) |
| Time advance | for-loop | event-driven kernel |
| Factor organisation | Factor ABC + FactorDAG | EBGraph / EventBrick |

**Framework storage preference**: alphalens (DataFrame/Parquet) is directly compatible; VectorBT is compatible but
V2-only; backtrader / vnpy (own data formats) and Qlib (binary) are not used. The three bridges
(**VectorBT / ZVT / Qlib**) are **archived outright — not kept as comparison baselines**.

## Data Flow

Target flow (see `docs/design/data/`):

```text
SourceAdapter.fetch() → save_raw → Normalizer → DQC → asset/     通过
                                           └→ quarantine/        拒收（30 天，≠ 不写盘）
                                                    ↓
                                          DataAPI / load_bars()（DuckDB read_parquet 视图）
                                                    ↓
                        ├──→ Factor → FactorDAG → PostProcessor → factors/{name}/v{ver}/{year}.parquet
                        ├──→ FactorCombiner → AlphaSignal
                        └──→ Portfolio (TargetPortfolio) → Execution (SimBroker/QmtBroker) → Journal
```

Current code placement is still the pre-rebuild layout (`infra/`, `factor_library/`, `engine/`) and will be migrated
per [`docs/ROADMAP.md`](docs/ROADMAP.md) §5. That migration honours red line 7: never touch both the old and the new
implementation of the same thing in one step.

### Storage layout (target)

```text
data_platform/
├── asset/bars_daily/{YYYY-MM-DD}.parquet      # 一天一文件，未复权 OHLCV + adjfactor + quality_flag
├── asset/bars_minute/{YYYY-MM-DD}.parquet     # 无 adjfactor 列；5min/60min 用 time_bucket 视图聚合
├── asset/calendar/{exchange}.parquet          # 6 交易所，单列 trade_date
├── asset/reference/{contract_specs,industry,corporate_actions}.parquet
├── asset/continuous/{symbol}.parquet          # 期货连续合约（V3）
├── factors/{factor_name}/v{version}/{year}.parquet + metadata.json
├── raw/{source}/{asset_type}/{date}.parquet    # 原样，保留 1 年
├── quarantine/{asset_type}/{date}.parquet     # 拒收批次，T+30 清理
└── logs/runs/{jobs,backtests,live}.jsonl
```

Two layers only: **Parquet is the single source of truth; DuckDB is a pure view engine** (`read_parquet`,
no `.duckdb` file). No ClickHouse, no storage abstraction layer, no udata container runtime. Prices are stored raw
plus a cumulative `adjfactor`; adjusted prices are derived (`hfq = raw × adj`, `qfq = raw × adj / adj_latest`), never
persisted.

## Factor System

- **Factor (ABC)** — `depends_on()` declares inputs, `compute()` returns
  `DataFrame[date, ticker, {factor_name}]` of **raw** values. Metadata lives in class attributes
  (`name / version / description / category / frequency / asset_types / …`).
- **FactorRegistry** — `@register_factor` decorator. Code *is* the registry; there is no separate JSON/SQLite
  registry. Status is three-state: `draft → production → deprecated`.
- **FactorDAG** — Kahn topological sort, layered parallel execution, cache keyed on
  `(name, version, date_range, depends_hash)`. Circular dependencies raise.
- **PostProcessor** — four steps in order: **winsorize (MAD) → fillna → standardize → neutralize (optional)**.
  Every step is `groupby(date)` cross-sectional; full-sample statistics are look-ahead.
  **Store raw values; post-process at consumption time.**
- **Operators** — time-series (`ts_mean`, `ts_rank`, `ts_decay_linear`, …) + cross-section
  (`cs_rank`, `cs_zscore`, `cs_neutralize`, …).

Universe filtering (ST / suspended / delisted) happens in `research/`, because tradability is a research-input
concern — not in execution.

## Tech Stack

| Component | Choice |
|---|---|
| Language | Python 3.10 |
| Package manager | uv + uv.lock |
| Storage | Parquet + DuckDB (one file per day: `{YYYY-MM-DD}.parquet`) |
| Data sources | AmazingData (Docker MCP), akshare, tushare |
| Backtest | pandas self-built <300 lines + SimBroker / QmtBroker |
| Factor analysis | alphalens-reloaded 0.4.6 |
| Scheduling | APScheduler |
| Config | TOML (three layers) + `.env` |
| Logging | Python logging + JSON Lines |
| Testing | pytest (unit / integration / golden) + hypothesis |
| JIT acceleration | Numba |
| Linting | Ruff |

## Implementation Red Lines (§7 — all eight, always in force)

1. **Do not write an event-driven kernel.** The for-loop is the time model.
2. **Do not invent an event subscription mechanism for factors.** Factor ABC + FactorDAG is enough.
3. **Do not let `core/` import any domain.** It depends on nothing.
4. **Do not let backtest and live trading take different code paths.** The difference is injected only in the
   Broker implementation (Sim / Qmt).
5. **Do not put one-off experiments in the registry.** The archive area never enters.
6. **Do not mix two golden samples.** The platform-level golden sample is **equal-weight top-5 ETF momentum**.
   The old `baseline_spec.py` figures are the futures-ALSTM strategy's own regression baseline and are never used
   as the platform baseline.
7. **Do not change both the old and the new implementation of the same thing during migration.** One side at a time.
8. **`.env` never enters git and is never printed.**

## Golden Sample (§6)

Platform-level golden sample = **equal-weight top-5 ETF momentum** (a variant of the double-momentum tutorial §2,
reproducible in ≤30 lines of akshare). Five-step validation:

| Step | Content | Tolerance |
|---|---|---|
| 1 | hand-calculated Excel (month-end rebalance, 3 months) | 4 decimal places |
| 2 | pandas implementation vs Excel | < 0.01% |
| 3 | SimBroker full chain vs pandas | < 0.01% |
| 4 | before/after fee comparison | fees match FeeModel hand-calc |
| 5 | 2020–2024 five-year baseline | metrics written to disk as the regression baseline |

Expected ranges: **annualized 10–15% / Sharpe 0.8–1.2 / MDD 15–25%**. Out-of-range triage:

| Symptom | Where to look |
|---|---|
| annualized > 20% | future function (`shift(1)` / fill-price basis) |
| Sharpe > 2 | look-ahead (weights using current-period information) |
| MDD < 10% | missing extreme market stretch (2022 / 2024) |
| fees < 1% | FeeModel under-counting (stamp tax on sell / 5-yuan minimum) |

## Implementation Order

Build order, per-stage definition of done, migration batches and the Dev → Test → Review process are in
[`docs/ROADMAP.md`](docs/ROADMAP.md). Design gaps found while sequencing are collected in its §7 — **do not
adjudicate them in code**.

## Important Rules

### AmazingData API Limitations

- **`get_option_basic_info` 返回上限**: 每次调用最多返回约 100-104 条记录，无论输入多少代码
- **解决方案**: 分批调用，每批 10 个代码，循环处理后合并结果
- **示例**:
  ```python
  # 错误方式 - 只会返回约100条
  result = info_data.get_option_basic_info(all_codes, is_local=False)

  # 正确方式 - 分批获取
  all_results = []
  for i in range(0, len(all_codes), 10):
      batch = all_codes[i:i+10]
      result = info_data.get_option_basic_info(batch, is_local=False)
      all_results.append(result)
  final_df = pd.concat(all_results, ignore_index=True)
  ```
- **`local_path` 必须传字符串**: SDK 内部做字符串拼接（`+`），传 `Path` 对象会报 `TypeError: unsupported operand type(s) for +: 'PosixPath' and 'str'`
  ```python
  # 错误
  base_data.get_hist_code_list(..., local_path=Path("/data/stock/"))
  # 正确
  base_data.get_hist_code_list(..., local_path="/data/stock/")
  ```
- **`is_local` 参数含义**: `True`（默认）先读本地缓存，读不到再联网下载；`False` 以本地数据为基础增量从服务器拉取
  ```python
  # 默认行为：先本地后服务器
  info_data.get_history_stock_status(code_list, local_path, is_local=True, begin_date=..., end_date=...)
  # 增量更新：基于本地数据从服务器增量拉取
  info_data.get_history_stock_status(code_list, local_path, is_local=False, begin_date=..., end_date=...)
  ```
- **连接数限制**: 同一用户同时登录连接数有限，未登出会占用连接槽位
- **最佳实践**: 脚本结束时务必调用 `ad.logout()` 释放连接

These source-side constraints are absorbed inside each `SourceAdapter` subclass (`fetch()` / `close()`), so callers
never see pagination, pooling or `local_path` typing.

### Docker Container Execution

- **必须在项目根目录下运行** docker-compose 命令，项目的 `docker-compose.yml` 已配置好 volume 挂载：
  - `./scripts` → `/scripts`（脚本目录）
  - `./data_lake` → `/data`（数据目录）
- **NEVER rebuild the Docker image** - the image `docker-amazingdata` is pre-built and must not be modified
- **不要修改** `~/AmazingData/docker/docker-compose.yml`，那是 MCP 服务器的配置
- 正确运行方式：
  ```bash
  # 在项目根目录下运行
  docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py
  ```
- 脚本内数据路径应使用 `/data/` 前缀（映射到宿主机 `data_lake/`）

### Sensitive data

`.env` (API keys, proxy credentials, passwords) is layer 3 of the config stack. It never enters git, never gets
printed, and never lands in `config_snapshot`. `load_secrets()` returns a dict that is excluded from the run record.

## Repo layout (current) vs five-domain target

Current placement rules are in [`docs/PROJECT_STRUCTURE.md`](docs/PROJECT_STRUCTURE.md). The five-domain skeleton
above is the target; migration batches are in [`docs/ROADMAP.md`](docs/ROADMAP.md) §5. During migration:

- New code goes into the five-domain skeleton only. Strategies never grow private `utils/`, `models/` or `factors/`
  copies.
- `engine/qlib_bridge/`, `engine/vectorbt_engine/`, `engine/zvt_bridge/`, `libs/zvt/` and
  `execution/router/vnpy_connector.py` are slated for archive — do not build on them.
- `strategies/futures/alstm/` and its regression baseline stay (futures is V3); they are **not** the platform
  golden sample.
