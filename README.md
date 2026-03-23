# Quant_Platform — 项目总览

## 1. 项目定位

Quant_Platform 是一个面向中国 A 股市场的高性能量化投研与回测平台。目标是建立一套**从数据采集到因子挖掘、回测评估、实盘执行**的完整流水线，覆盖个人量化研究者到小型私募的核心需求。

核心设计原则：
- **向量化优先**：以 DataFrame 矩阵运算为主线，利用 VectorBT / DuckDB 获得极致吞吐
- **因子即代码**：任何 Alpha 策略都可抽象为因子，通过装饰器注册、DAG 调度、后处理管线统一管理
- **可插拔架构**：5 层流水线的每一层均可独立替换，方便实验不同算法

## 2. 技术栈

| 层次 | 技术选型 | 说明 |
|---|---|---|
| 语言 | Python 3.10 | 严格版本锁定，兼容 Numba / VectorBT 生态 |
| 包管理 | uv + uv.lock | 确定性依赖解析，可复现构建 |
| 数据存储 | Parquet + DuckDB | 列式存储 + 列式分析引擎，Hive 分区 (`year=YYYY`) |
| 数据源 | Tushare (主) + AKShare (辅) | 双源交叉校验，确保数据质量 |
| 回测引擎 | VectorBT / ZVT / Qlib | 三套引擎覆盖向量化、事件驱动、ML 三类场景 |
| 因子分析 | Alphalens-reloaded | IC/IR/Quantile 收益评估 |
| 任务编排 | Prefect 3.x | 日级数据更新 & 策略调度 |
| JIT 加速 | Numba | 策略核心循环 JIT 编译 |
| 可视化 | Plotly | 回测报告与资金曲线 |
| Linting | Ruff | 代码风格与静态检查 |

## 3. 系统架构

### 3.1 5 层流水线

项目借鉴 QuantConnect Lean 的算法框架，将交易流程严格拆分为 5 层：

```
┌─────────────────────────────────────────────────────┐
│  [1] Universe Selection                              │
│      输入: 全市场 K 线                                │
│      输出: Boolean 矩阵 (Date × Symbol)              │
│      实现: UniverseFilter — 停牌/ST/退市/次新股过滤   │
├─────────────────────────────────────────────────────┤
│  [2] Alpha Model                                     │
│      输入: K 线 + Universe Mask                       │
│      输出: 信号强度矩阵 (Date × Symbol)               │
│      实现: FactorAlphaModel — 将 Factor 适配为 Alpha  │
├─────────────────────────────────────────────────────┤
│  [3] Portfolio Construction                          │
│      输入: 信号矩阵                                   │
│      输出: 目标权重矩阵 (Date × Symbol)               │
│      实现: EqualWeightPortfolio / QuantilePortfolio   │
├─────────────────────────────────────────────────────┤
│  [4] Risk Management                                 │
│      输入: 目标权重                                   │
│      输出: 安全权重矩阵                               │
│      实现: DefaultRiskFilter — 单票上限裁剪           │
├─────────────────────────────────────────────────────┤
│  [5] Execution                                       │
│      输入: 安全权重                                   │
│      输出: VectorBT Portfolio / 订单记录              │
│      实现: VectorBTExecutionHandler                  │
└─────────────────────────────────────────────────────┘
```

核心入口: `engine/pipeline.py` → `AlphaPipeline.run(kdata)`

### 3.2 数据流

```
Tushare / AKShare  (远程 API)
        │
        ▼
  scripts/update_daily.py   (数据采集 + 双源校验)
        │
        ▼
  data_lake/market_data/cn_stock/1d/  (Parquet, Hive 分区)
        │
        ▼
  infra/storage.py  → DuckDB 虚拟视图 (cn_stock_1d, cn_stock_1d_qfq, cn_stock_1d_hfq)
        │
        ├──→ factor_library/  (因子计算)
        │         │
        │         ▼
        │    data_lake/factors/{factor_name}/1d/  (因子值 Parquet)
        │
        ├──→ engine/pipeline.py  (回测流水线)
        │
        └──→ engine/qlib_bridge/  (导出 Qlib 二进制格式)
```

### 3.3 因子系统

```
┌──────────────┐    @register_factor    ┌───────────────────┐
│ Factor (ABC) │ ─────────────────────→ │ FactorRegistry    │
│              │                        │ (全局注册表)       │
│ compute()    │                        │                   │
│ update()     │    ┌──────────────┐    │ create_instance() │
│ postprocess()│    │ FactorDAG    │    │ list_factors()    │
└──────────────┘    │ (依赖调度)   │    └───────────────────┘
                    │              │
                    │ 拓扑排序     │
                    │ 缓存计算     │
                    │ 跨参数映射   │
                    └──────────────┘
```

- **Factor (ABC)**: 抽象基类，定义 `compute()` / `update()` / `compute_with_postprocess()` 三个核心方法
- **FactorRegistry**: `@register_factor` 装饰器自动注册，工厂方法 `create_instance()` 按名称创建实例
- **FactorDAG**: Kahn 算法拓扑排序，管理复合因子的依赖计算顺序，内置缓存避免重复计算
- **PostProcessor**: winsorize → standardize → fillna 后处理流水线
- **Operators**: 时序算子 (`ts_mean`, `ts_rank`, `ts_decay_linear`...) + 截面算子 (`cs_rank`, `cs_zscore`, `cs_neutralize`...)

### 3.4 目录结构

```
Quant/
├── config/                    # 配置文件
│   ├── settings.toml          # 全局配置（数据路径、回测成本、网关）
│   └── strategies.yaml        # 策略定义（参数、风控阈值）
├── infra/                     # 基础设施层
│   ├── storage.py             # ConfigLoader + StorageManager (DuckDB/Parquet)
│   └── scheduler.py           # Prefect 日级调度 (TODO)
├── factor_library/            # 因子库
│   ├── base.py                # Factor 抽象基类
│   ├── dag.py                 # FactorDAG 依赖调度
│   ├── registry.py            # @register_factor 注册机制
│   ├── preprocessor.py        # PostProcessor 后处理
│   ├── universe.py            # UniverseFilter 股票池过滤
│   ├── operators/             # 通用算子库 (时序 + 截面)
│   ├── technical/             # 技术因子 (momentum, volatility, risk_adjusted)
│   ├── fundamental/           # 基本面因子 (TODO)
│   └── alternative/           # 另类因子 (TODO)
├── engine/                    # 引擎层
│   ├── pipeline.py            # 5 层流水线总装
│   ├── factor/                # 因子回测 & 增量更新
│   ├── qlib_bridge/           # Qlib 格式导出
│   ├── simulation/            # A 股费率/滑点/成交模型
│   ├── vectorbt_engine/       # Numba JIT 策略编译
│   └── zvt_bridge/            # ZVT 数据同步 & 回测适配
├── execution/                 # 执行层
│   ├── position_manager.py    # 仓位管理 (stub)
│   ├── risk_control.py        # 风控规则
│   └── router/                # 交易通道 (vnpy/xtquant, stub)
├── strategies/                # 策略实现
│   └── alpha_001.py           # 双均线策略 (Numba JIT + VectorBT)
├── scripts/                   # 运维脚本
│   ├── update_daily.py        # 日级数据更新 (Tushare + AKShare 校验)
│   ├── audit_data.py          # 数据质量审计
│   ├── migrate_schema.py      # Parquet Schema 迁移
│   └── run_momentum_strategy.py  # 动量策略回测
├── tests/                     # 测试用例
├── examples/                  # 使用示例
├── doc/                       # 项目文档
├── data_lake/                 # 数据湖 (gitignored)
└── libs/zvt/                  # ZVT 本地 fork (editable)
```

## 4. 核心模块说明

### 4.1 数据层 (`infra/`)

**ConfigLoader** — 单例配置加载器
- 从 `config/settings.toml` 读取基础配置
- 从 `.env` / 环境变量加载敏感信息（Tushare Token）
- 注入 `ZVT_HOME` 等运行时环境变量

**StorageManager** — DuckDB + Parquet 存储管理器
- Hive 分区写入: `data_lake/{category}/{market}/{frequency}/year=YYYY/data.parquet`
- 自动注册 DuckDB 虚拟视图: `market_cn_stock_1d`, `cn_stock_1d_qfq`, `cn_stock_1d_hfq`
- 支持因子值存储: `data_lake/factors/{factor_name}/1d/`
- 因子矩阵查询: `get_factor_matrix()` 支持 DuckDB PIVOT 语法

### 4.2 因子库 (`factor_library/`)

**已实现因子**:

| 因子名 | 文件 | 说明 |
|---|---|---|
| `momentum_return` | `technical/momentum.py` | 动量收益率: `ts_mean(pct_change(close), window)` |
| `simple_momentum` | `technical/momentum.py` | 简单动量: `close / close_N - 1` |
| `volatility` | `technical/volatility.py` | 波动率因子 |
| `risk_adjusted_momentum` | `technical/risk_adjusted_momentum.py` | 复合因子: 动量/波动率 (DAG 依赖) |

**算子库**:
- 时序: `ts_mean`, `ts_sum`, `ts_std`, `ts_rank`, `ts_decay_linear`, `ts_delta`, `ts_corr`
- 截面: `cs_rank`, `cs_zscore`, `cs_norm`, `cs_winsorize`, `cs_neutralize`

### 4.3 回测引擎 (`engine/`)

**VectorBT 执行器** — 两种模式:
- `VectorBTExecutionHandler`: 单组合向量化回测
- `MultiVectorBTExecutionHandler`: N 分组 (Quantile) 并发回测，适用于截面因子测试

**A 股交易模型** (`simulation/models.py`):
- `AShareFeeModel`: 佣金万三（最低 5 元）+ 印花税卖出单边 0.05% + 过户费
- `VolumeShareSlippageModel`: 成交量占比滑点，超限惩罚
- `AShareFillModel`: 涨停/跌停一字板检查、停牌过滤、T+0 规则

**ZVT Bridge** (`zvt_bridge/`):
- `ZvtDataSyncer`: ZVT 数据 → Parquet，含复权因子合并
- `FactorAdapter`: 自定义 Factor → ZVT StockTrader 适配器
- AKShare Recorders: 插入 ZVT 框架的数据录制器

### 4.4 执行层 (`execution/`)

- `RiskControl`: 单票持仓上限检查
- `PositionManager`: 仓位跟踪 (stub)
- `router/vnpy_connector.py`: vn.py CTP 通道 (stub)
- `router/xtquant_connector.py`: XtQuant 券商接口 (stub)

## 5. 关键设计决策

### 5.1 为什么选择 DuckDB + Parquet 而非传统数据库？
- 列式存储天然适合金融时序数据的列裁剪查询
- Hive 分区按年/月切分，避免单文件过大
- DuckDB 零依赖、零运维，适合个人研究环境
- PIVOT 语法原生支持因子矩阵构建

### 5.2 为什么同时维护 VectorBT 和 ZVT 两套回测引擎？
- **VectorBT**: 向量化回测，速度快，适合截面分组测试和因子 IC 分析
- **ZVT**: 事件驱动回测，逻辑更接近实盘，适合单策略深度验证
- 两套引擎的结果交叉验证，可检测回测框架本身的偏差

### 5.3 为什么因子需要 DAG 调度？
复合因子（如 `RiskAdjustedMomentum = Momentum / Volatility`）天然形成依赖图。DAG 调度确保：
- 计算顺序正确（先算依赖，再算目标）
- 避免重复计算（缓存已计算的中间结果）
- 支持跨参数依赖（因子 A(window=20) 可依赖因子 B(window=5)）

### 5.4 为什么用 Tushare + AKShare 双源校验？
单一数据源的风险：API 限流导致数据缺失、数据口径差异导致复权因子错误。双源抽样比对（误差率 > 0.5% 则告警）可显著降低数据质量风险。

## 6. 快速开始

```bash
# 1. 安装依赖
uv sync

# 2. 配置 Tushare Token
echo "TUSHARE_TOKEN=your_token" > .env

# 3. 拉取历史数据
python scripts/update_daily.py --start-date 20240101

# 4. 运行动量策略回测
python scripts/run_momentum_strategy.py

# 5. 运行测试
pytest tests/
```