# AGENTS.md

> **`docs/design/` 是唯一权威，本文件仅作开发与协作入口指引。**
>
> - 架构总纲：[docs/design/ARCHITECTURE.md](docs/design/ARCHITECTURE.md)
> - 设计总索引：[docs/design/README.md](docs/design/README.md) · 仓库文档目录：[docs/README.md](docs/README.md)
> - 资产表字段字典（51 张 `asset/` 规范表）：[docs/design/data/asset/README.md](docs/design/data/asset/README.md)
> - 代码放置边界：[docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) · 阶段状态与后续计划：[docs/ROADMAP.md](docs/ROADMAP.md)
>
> 若本文件与 `docs/design/` 存在冲突，**一律以 `docs/design/` 为准**。

---

## 项目概览

Quant Platform 是面向中国市场（V1 以 A 股股票 + ETF 为主，兼顾公募基金与期货扩展）的量化平台，采用 **“薄壳 + pandas 优先”** 架构：保持研究迭代速度、工程可演进性与实盘接入能力，避免引入重型事件驱动内核或冗余中间件。

---

## 常用命令

```bash
# 安装 / 同步依赖
uv sync

# 运行单元与集成测试（三层体系：unit / integration / golden）
uv run pytest tests/unit tests/integration -q
uv run pytest tests/unit/
uv run pytest tests/golden/   # 需先准备黄金样本落盘基线

# 运行单个测试
uv run pytest tests/unit/test_core_asset_rules.py
uv run pytest tests/unit/test_core_asset_rules.py::test_unknown_asset_type_raises

# 代码检查与格式化（提交前必须通过）
uv run ruff check .
uv run ruff format .

# AmazingData 容器运行（需在项目根目录执行）
cd ~/AmazingData/docker && docker-compose up -d
docker-compose run --rm amazingdata python3 /scripts/bootstrap/amazing_pull.py --help
```

---

## 核心架构骨架

### 1. 五域划分与依赖方向

代码按**运营域**划分为五个核心包（`core/`、`data/`、`research/`、`execution/`、`ops/`），辅以多资产因子库 `factor_library/`、策略实例 `strategies/` 与脚本入口 `scripts/`（详见 [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md)）：

```text
core/         共享工具（config / registry / logging / types / asset_rules）
data/         数据接入与存储（SourceAdapter / Normalizer / DQC / storage_schema / storage_ops / DataAPI）
research/     投研链路（Factor / FactorDAG / PostProcessor / FactorCombiner / AlphaSignal / ML）
execution/    执行与风控（PortfolioConstruction / OrderGeneration / RiskControl / FeeModel / Ledger / Broker）
ops/          运维闭环（Scheduler / Monitoring / Reporting / Alerting）
```

**依赖方向硬规则**：

```text
research/ ──┐
            ├──→ core/ ←──┐
execution/ ─┘             │
data/     ──→（提供数据访问，不反向依赖）
ops/      ──→（只读观测所有域，不被业务域依赖）
```

- `core/` **不 import 任何业务域**：无引擎、无事件循环、无时间模型。
- `research/` 与 `execution/` 互不依赖，通过 `AlphaSignal` / `TargetPortfolio` 解耦。
- `ops/` 只读观测，不被任何域反向依赖。

### 2. 三步主链与 `for-loop` 时间模型

```text
AlphaSignal → Portfolio → Execution
                            └── broker.submit_orders() 内部封装：风控 + 撮合 + 记账
```

- 回测与实盘时间推进统一使用 `for date in rebalance_dates(): ...` 顺序调用，**不引入事件驱动内核**。
- 实盘与回测共用同一主链逻辑，差异仅注入在 `Broker` 实现（`SimBroker` / `QmtBroker`）。
- 接口边界在函数与薄壳 dataclass，不绕回已归档的 ZVT / Qlib / VectorBT 桥接层。

### 3. 因子体系（`Factor ABC` + `FactorDAG`）

```text
Factor(compute) → FactorDAG(拓扑并行) → PostProcessor → per-factor-one-table
  → FactorCombiner → AlphaSignal
```

- **`Factor` ABC**：`depends_on() -> list[str]` + `compute(data) -> DataFrame[date, ticker, {factor_name}]`（仅存原始因子值，后处理在消费时执行）。上游数据表可用规范名或兼容别名（如 `cn_stock_bars_daily` / `bars_daily`），上游因子使用 `"factor:<name>"` 前缀。
- **`FactorRegistry`**：`@register_factor` 装饰器即注册表（`draft → production → deprecated`）。
- **`FactorDAG`**：Kahn 拓扑排序 + 分层并行 + 缓存键 `(factor_name, version, date_range, depends_hash)`；出现环依赖直接抛错。
- **`PostProcessor`**：严格按 `winsorize (MAD) → fillna → standardize → neutralize（可选）` 顺序执行，且每步必须按 `date` 做横截面分组（严禁全样本前瞻统计）。
- **可交易性过滤**：ST / 停牌 / 退市过滤属于 `research/` 的 Universe 问题，不放在 `execution/`。
- **评价分层**：`alphalens-reloaded` 仅作回测前快速筛选门控（不建模手续费与涨跌停），与正式回测结论冲突时**以正式回测为准**。

---

## 数据接入与存储架构（52 张规范表）

```text
SourceAdapter.fetch() → save_raw → Normalizer → DQC → asset/     （通过）
                                           └→ quarantine/        （拒收：保留 30 天）
                                                    ↓
                                DataAPI / load_bars()（DuckDB read_parquet + catalog.duckdb）
                                                    ↓
                        ├──→ Factor → FactorDAG → factors/cn_stock_factors/{name}_v{ver}/{year}.parquet
                        ├──→ FactorCombiner → AlphaSignal
                        └──→ Portfolio (TargetPortfolio) → Execution (SimBroker/QmtBroker) → Journal
```

### 存储布局（`$QUANT_DATA_ROOT`，默认 `/Users/nachuan/Documents/Quant_Data`）

```text
$QUANT_DATA_ROOT/
├── catalog.duckdb                                   # 持久化 DuckDB 视图定义与统计缓存
├── asset/                                           # 51 张规范资产表（47 A 股 + 4 期货，无独立 ext/ 顶层）
│   ├── cn_calendar/{exchange}.parquet               # 9 个交易所日历（sse/szse/bse/cffex/shfe/dce/czce/ine/gfex）
│   ├── cn_stock_{bars_daily,bars_minute,tick,tick_l2}/{date}.parquet
│   ├── cn_stock_{fundamental,daily_status,index_bars_daily,fundamental_consensus,auction,moneyflow,hsgt_hold,margin,lhb,lhb_seats,limit_pool,chips_perf}/{month}.parquet
│   ├── cn_stock_fundamental_{income,balance,cashflow,indicator,segment,earnings}/{year}.parquet
│   ├── cn_stock_{index_weights,top_holders,restricted_release,holder_trade,pledge,research_visits}/{year}.parquet
│   ├── cn_stock_{basic,corporate_actions,industry,index_basic,hsgt_market_flow,holder_num,block_trade,repurchase}/*.parquet
│   ├── cn_etf_{basic,bars_daily,bars_minute,fundamental,tick}/...   # 独立 ETF 域（暂不落 cn_etf_pcf）
│   ├── cn_fund_{basic,managers,nav,dividend,portfolio}/...          # 场外 .OF 与场内 LOF 公募基金
│   └── cn_futures_{bars_daily,tick_l2,contract_specs,continuous}/...
├── factors/cn_stock_factors/{name}_v{version}/{year}.parquet + metadata.json   # 与 asset/ 同级
├── raw/{source}/{asset_type}/{date}.parquet         # 原始落盘（保留 1 年）
├── quarantine/{asset_type}/{date}.parquet           # 隔离区（T+30 清理）
├── dqc/ · cross_check/ · manifest/                  # DQC 报告、交叉校验、落盘清单
└── logs/runs/{jobs,backtests,live}.jsonl            # 运行记录（仓库根目录 logs/ 亦不入 Git）
```

- **两层契约**：Parquet 为唯一持久化真相；DuckDB 持久化 `catalog.duckdb` 视图与统计缓存，不引入 ClickHouse。
- **单目录单 Schema**：取消 `cn_reference/` 等混放目录及 `ext/` 顶层，所有资产表平铺于 `asset/{nation}_{asset_type}_{data_type}/`；旧短名（`bars_daily` 等 10 个）通过 `data/storage_schema.py::LEGACY_TO_CANONICAL` 透明兼容。
- **价格与复权**：表内仅存未复权价格与累计 `adjfactor`；前复权 `qfq = raw × adj / adj_latest`、后复权 `hfq = raw × adj` 均在读时派生，绝不落盘；分钟线与 Tick 表不冗余存 `adjfactor`。
- **多源仲裁**：当前 4 个激活源为 `tushare`、`qdc`（QDCData）、`amazingdata`、`straightflush`（同花顺扶摇）；`akshare` 已移出默认优先级列表（仅作历史兼容/测试）。`config/source_priority.yaml` 指定每张表唯一主写源，备源仅交叉校验与补缺（`source_tag` 写入 Parquet metadata）。

---

## 技术选型定稿

| 维度 | 当前选型 | 否决项 | 说明文档 |
|---|---|---|---|
| 语言与包管理 | Python 3.10 + `uv` + `uv.lock` | 手工 `pip` | [pyproject.toml](pyproject.toml) |
| 存储与查询 | Parquet + DuckDB 持久化 `catalog.duckdb` | ClickHouse、DuckDB 纯内存 | [docs/design/data/storage_schema.md](docs/design/data/storage_schema.md) |
| 数据接入 | `SourceAdapter` 四方法 + watermark + Normalizer | 混合直写 asset 层 | [docs/design/data/ingestion.md](docs/design/data/ingestion.md) |
| 多资产规则 | `core/asset_rules.py` 单表驱动 | 编译期 Traits / 散落硬编码 | [docs/design/core/asset_rules.md](docs/design/core/asset_rules.md) |
| 因子组织与评价 | `Factor ABC` + `FactorDAG` + `alphalens-reloaded` | EBGraph / EventBrick | [docs/design/research/factor_interface.md](docs/design/research/factor_interface.md) |
| 回测与执行 | pandas 自研 `SimBroker` / `QmtBroker` | backtrader / vnpy 事件内核 | [docs/design/execution/broker.md](docs/design/execution/broker.md) |
| 配置与密钥 | 三层 TOML + `.env` + `config_snapshot` | YAML 主配置（`on`→`True` 陷阱） | [docs/design/core/config.md](docs/design/core/config.md) |
| 日志与调度 | Python logging + JSONL + APScheduler | ELK、crontab | [docs/design/ops/scheduler.md](docs/design/ops/scheduler.md) |
| 测试与 Lint | pytest 三层（unit / integration / golden）+ Hypothesis + Ruff | 跳过回归直接改代码 | [docs/design/core/testing.md](docs/design/core/testing.md) |

---

## 八条实施红线（始终有效）

1. **不要写事件驱动内核**：`for-loop` 就是时间模型。
2. **不要给因子发明事件订阅机制**：`Factor ABC` + `FactorDAG` 即可。
3. **不要让 `core/` import 任何业务域**：它不依赖任何上层模块。
4. **不要让回测和实盘走不同代码路径**：差异只注入在 `Broker` 实现（`Sim` / `Qmt`）。
5. **不要把一次性实验放进注册表**：临时验证不进入 `FactorRegistry`。
6. **不要混用两套黄金样本**：平台级黄金样本是 **等权 Top-5 ETF 动量**；`strategies/futures/alstm/` 的回归基线仅服务期货 ALSTM 策略自身。
7. **禁止改动代码注释/重构时悄悄改变运行逻辑**：无明确需求时保持测试行为完全等价。
8. **`.env` 与敏感凭证绝不入 Git、绝不打印、绝不进入 `config_snapshot`**。

---

## 平台黄金样本与自检门禁

**平台级黄金样本 = 等权 Top-5 ETF 动量**（5 步验证法，详见 [docs/design/core/testing.md](docs/design/core/testing.md)）：

| 步骤 | 内容 | 容差要求 |
|---|---|---|
| 1 | Excel 手算（月末调仓，3 个月窗口） | 保留 4 位小数 |
| 2 | pandas 实现对比 Excel | `< 0.01%` |
| 3 | `SimBroker` 全链路对比 pandas | `< 0.01%` |
| 4 | 扣费前后对拍 | 费用精确匹配 `FeeModel` 手算（卖出印花税豁免对 ETF 生效、最低 5 元佣金） |
| 5 | 2020–2024 五年基线落盘 | 指标写入基线文件作为回归基准 |

预期合理区间：**年化 10–15% / Sharpe 0.8–1.2 / 最大回撤 15–25%**。异常排查方向：
- 年化 `> 20%`：检查未来函数（`shift(1)` / 撮合价基准 `t1_open`）。
- Sharpe `> 2`：检查权重计算是否误用当期信息。
- 最大回撤 `< 10%`：检查是否漏掉 2022 / 2024 极端行情时段。
- 费用过低（`< 1%`）：检查 `FeeModel` 卖出印花税与最低 5 元佣金逻辑。

---

## 数据源与环境约束

### 1. AmazingData SDK 限制（已封装于 `data/adapters/amazingdata_adapter.py`）

- **`get_option_basic_info` 单次上限**：每次最多返回约 100–104 条，必须按每批 10 个代码分批查询后 `pd.concat`。
- **`local_path` 必须传 `str`**：SDK 内部使用 `+` 拼接字符串，传 `Path` 会触发 `TypeError`。
- **`is_local` 参数语义**：`True`（默认）优先读本地缓存，缺失再联网；`False` 基于本地增量向服务器拉取。
- **连接槽位限制**：同一账号并发登录连接数有限，脚本结束前务必调用 `ad.logout()` 释放连接。

### 2. Docker 容器执行约定

- **必须在项目根目录运行 `docker-compose`**，使 `docker-compose.yml` 中的挂载生效：
  - `.:/opt/project`
  - `./scripts:/scripts`
  - `/Users/nachuan/Documents/Quant_Data:/data`
- **禁止重建或修改 `docker-amazingdata` 镜像**，也**禁止修改 `~/AmazingData/docker/docker-compose.yml`**。
- 容器内脚本数据输出路径统一写 `/data/...`（对应宿主机 `Quant_Data/`）。

### 3. 敏感配置

- `config/.env` 承载 `TUSHARE_TOKEN`、`STRIGHT_FLUSH_TOKEN`、`QDC_API_KEY`、`QDC_DEVICE`、`QUANT_DATA_ROOT` 等配置，已被 `.gitignore` 屏蔽。
- `core.config.load_secrets()` 读取的敏感字段不得写入 `logs/`、`manifest/` 或 `config_snapshot()`。
