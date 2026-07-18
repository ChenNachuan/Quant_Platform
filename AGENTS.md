# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

Quant_Platform is a high-performance quantitative trading platform for China A-share market. It covers the full pipeline from data collection to factor mining, backtesting, and live execution.

## Common Commands

```bash
# Install dependencies
uv sync

# Start AmazingData MCP server
cd ~/AmazingData/docker && docker-compose up -d

# Run scripts in AmazingData container (在项目根目录下运行，docker-compose.yml 已配置 volume 挂载)
# ./scripts → /scripts, ./data_lake → /data
docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py

# Run tests
pytest tests/

# Run a single test file
pytest tests/test_factor_basic.py

# Run a specific test function
pytest tests/test_factor_basic.py::test_factor_name

# Lint and format
ruff check --fix .
ruff format .

# Run momentum strategy backtest
python scripts/run_momentum_strategy.py
```

## Architecture

### 5-Layer Pipeline

The system follows a QuantConnect Lean-inspired 5-layer pipeline:

1. **Universe Selection** → Boolean matrix (Date × Symbol) filtering suspended/ST/delisted stocks
2. **Alpha Model** → Signal strength matrix from factors
3. **Portfolio Construction** → Target weight matrix (EqualWeight or Quantile)
4. **Risk Management** → Position clipping per risk rules
5. **Execution** → VectorBT Portfolio or order records

Core entry point: `engine/pipeline.py` → `AlphaPipeline.run(kdata)`

### Data Flow

```
AmazingData Docker MCP → MCP 协议调用 → data_lake/market_data/cn_stock/1d/ (Parquet, Hive partition)
                                                     ↓
                                              infra/storage.py (DuckDB views)
                                                     ↓
                              ├──→ factor_library/ → data_lake/factors/{factor_name}/1d/
                              ├──→ engine/pipeline.py (backtest)
                              └──→ engine/qlib_bridge/ (Qlib binary export)
```

### Factor System

- **Factor (ABC)** in `factor_library/base.py`: abstract base with `compute()` / `update()` / `compute_with_postprocess()`
- **FactorRegistry**: `@register_factor` decorator for auto-registration, `create_instance()` factory
- **FactorDAG**: Kahn's algorithm topological sort for composite factor dependencies with caching
- **PostProcessor**: winsorize → standardize → fillna pipeline
- **Operators**: time-series (`ts_mean`, `ts_rank`, `ts_decay_linear`...) + cross-section (`cs_rank`, `cs_zscore`, `cs_neutralize`...)

### Tech Stack

| Component | Choice |
|---|---|
| Language | Python 3.10 |
| Package manager | uv + uv.lock |
| Storage | Parquet + DuckDB (Hive partitions: `year=YYYY`) |
| Data sources | AmazingData (Docker MCP) |
| Backtest engines | VectorBT (vectorized), ZVT (event-driven), Qlib (ML) |
| Factor analysis | Alphalens-reloaded |
| Task orchestration | Prefect 3.x |
| JIT acceleration | Numba |
| Linting | Ruff |

### Key Design Decisions

- **DuckDB + Parquet**: Columnar storage ideal for financial time-series; zero-dependency, zero-ops; native PIVOT for factor matrices
- **Dual backtest engines**: VectorBT for fast cross-sectional testing; ZVT for event-driven validation closer to live trading; cross-validation detects framework bias
- **Factor DAG**: Composite factors (e.g., RiskAdjustedMomentum = Momentum / Volatility) form dependency graphs; DAG ensures correct order, caching, and cross-parameter dependencies
- **AmazingData MCP**: Docker-based data server providing full historical and real-time market data via standardized MCP protocol

### Storage Layout

```
data_lake/
├── market_data/cn_stock/1d/year=YYYY/data.parquet   # Raw OHLCV
├── factors/{factor_name}/1d/year=YYYY/data.parquet   # Factor values
└── quant.duckdb                                       # DuckDB file
```

DuckDB auto-registers views: `market_cn_stock_1d`, `cn_stock_1d_qfq`, `cn_stock_1d_hfq`

### ZVT Local Fork

ZVT is vendored as an editable local dependency at `libs/zvt/` (configured in `pyproject.toml` `[tool.uv.sources]`).

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

### Docker Container Execution

- **必须在项目根目录下运行** docker-compose 命令，项目的 `docker-compose.yml` 已配置好 volume 挂载：
  - `./scripts` → `/scripts`（脚本目录）
  - `./data_lake` → `/data`（数据目录）
- **NEVER rebuild the Docker image** - the image `docker-amazingdata` is pre-built and must not be modified
- **不要修改** `~/AmazingData/docker/docker-compose.yml`，那是 MCP 服务器的配置
- 正确运行方式：
  ```bash
  # 在项目根目录 /Users/nachuan/Documents/Quant 下运行
  docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py
  ```
- 脚本内数据路径应使用 `/data/` 前缀（映射到宿主机 `data_lake/`）