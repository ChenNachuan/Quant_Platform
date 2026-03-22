# Quant_Platform — 代码走查

本文档按模块逐层走查核心代码，帮助开发者快速理解各组件的实现细节与协作关系。

---

## 1. 配置加载 (`infra/storage.py` → `ConfigLoader`)

**职责**: 单例模式加载全局配置，管理密钥注入。

```
ConfigLoader.load()
  ├── 读取 .env → os.environ (不覆盖已有变量)
  ├── 读取 config/settings.toml → Dict
  ├── 注入 TUSHARE_TOKEN (环境变量 > toml)
  ├── 注入 PROJECT_ROOT
  └── 设置 ZVT_HOME 环境变量
```

**关键细节**:
- 使用 `tomli` 二进制读取 TOML，避免 `tomllib` (Python 3.11+ 才有)
- 单例缓存 `_config`，首次调用后不再重新加载
- `.env` 使用 `override=False`，保证环境变量优先级最高

**使用示例**:
```python
from infra.storage import ConfigLoader
config = ConfigLoader.load()
token = config.get('tushare', {}).get('token')
```

---

## 2. 存储管理 (`infra/storage.py` → `StorageManager`)

**职责**: DuckDB 连接管理、Parquet 读写、视图注册、因子存储。

### 2.1 初始化流程

```
StorageManager.__init__()
  ├── ConfigLoader.load()           # 加载配置
  ├── 创建 data_lake/ 目录
  ├── duckdb.connect(db_path)       # 连接 DuckDB
  ├── refresh_views()               # 扫描 Parquet 注册视图
  └── create_adjusted_views()       # 创建 QFQ/HFQ 复权视图
```

### 2.2 视图注册机制

`refresh_views()` 递归扫描 `data_lake/` 下的 Parquet 文件，按目录结构自动命名视图：

```
data_lake/market_data/cn_stock/1d/*.parquet  →  VIEW market_cn_stock_1d
data_lake/factors/momentum_return/1d/*.parquet  →  VIEW factors_momentum_return_1d
```

SQL 生成核心：
```sql
CREATE OR REPLACE VIEW {view_name} AS
SELECT * FROM read_parquet('{path}/**/*.parquet', union_by_name=true, hive_partitioning=1)
```

### 2.3 复权视图

基于 `market_cn_stock_1d` 创建两个视图：
- `cn_stock_1d_qfq`: 前复权价格 = `close * qfq_factor`
- `cn_stock_1d_hfq`: 后复权价格 = `close * hfq_factor`

### 2.4 数据写入

`write_data()` 将 DataFrame 写入 Parquet，自动处理：
- 时间戳标准化 (tz-naive, ns 精度)
- Hive 分区列注入 (`year`, `month`)
- UUID 唯一文件名避免冲突

`write_factor()` 是因子存储专用接口，要求 DataFrame 包含 `entity_id`, `timestamp`, `value` 列。

### 2.5 因子矩阵查询

`get_factor_matrix()` 使用 DuckDB PIVOT 语法，将长格式因子值转为宽表：

```sql
PIVOT (
    SELECT timestamp, entity_id, factor_name, value
    FROM read_parquet([...])
) ON factor_name USING first(value) GROUP BY timestamp, entity_id
```

返回 `MultiIndex(timestamp, entity_id)` 的 DataFrame，列为各因子名。

### 2.6 快捷查询

`get_data()` 自动路由到视图 `market_{market}_{frequency}`，支持参数绑定：
```python
df = storage.get_data(market='cn_stock', frequency='1d',
                       codes=['000001.SZ'], start_date='2024-01-01')
```

---

## 3. 因子系统

### 3.1 Factor 基类 (`factor_library/base.py`)

所有因子必须继承 `Factor`，实现以下元数据和方法：

**必需元数据**:
```python
name: str              # 唯一名称，全小写
store_time: str        # 创建日期 YYYYMMDD
para_group: Dict       # 参数空间，按时序分组
```

**必需方法**:
```python
generate_para_space() -> List[Dict]    # 参数组合列表
compute(data, deps=None) -> pd.Series  # 全量计算
```

**可选方法**:
```python
update(new_data, history, deps) -> pd.Series  # 增量更新
```

**自动处理**:
```python
compute_with_postprocess(data, deps) -> pd.Series
  ├── compute()                    # 核心计算
  ├── PostProcessor.check_validity()  # 有效性检查
  ├── PostProcessor.fillna()       # NaN 填充 (可选)
  ├── PostProcessor.winsorize()    # 缩尾 (可选)
  └── PostProcessor.standardize()  # 标准化 (可选)
```

### 3.2 因子注册 (`factor_library/registry.py`)

装饰器注册 + 工厂模式：

```python
@register_factor
class MyFactor(Factor):
    name = "my_factor"
    ...

# 按名称创建实例
factor = FactorRegistry.create_instance('my_factor', '1d', {'window': 20})
```

`FactorRegistry._registry` 是全局类变量字典，存储 `{name: FactorClass}` 映射。

### 3.3 DAG 调度 (`factor_library/dag.py`)

**核心数据结构**:
- `graph`: `{factor_name: {dependencies}}` 正向依赖图
- `reverse_graph`: `{dep_name: {dependents}}` 反向依赖图
- `computed_cache`: `{cache_key: pd.Series}` 计算结果缓存

**拓扑排序** (`topological_sort`):
1. 递归收集目标因子的所有依赖节点
2. 构建子图入度表
3. Kahn 算法：零入度节点入队，逐个处理并减少邻居入度
4. 检测循环依赖（结果长度 != 节点数）

**带缓存计算** (`compute_with_cache`):
```
compute_with_cache(factor_name, data, registry, timeframe, para)
  ├── 检查缓存 cache_key = "{name}_{timeframe}_{para}"
  ├── 递归计算依赖 deps = {dep_name: compute_with_cache(dep_name, ...)}
  │   └── 跨参数依赖: dependency_para_map.get(dep_name, para)
  ├── factor.compute(data, deps=deps)
  └── 缓存结果
```

### 3.4 示例：动量因子 (`factor_library/technical/momentum.py`)

```python
@register_factor
class MomentumReturn(Factor):
    name = "momentum_return"
    para_group = {"1d": {"window": [5, 10, 20, 60, 120, 250]}}
    dependencies = []  # 基础因子，无依赖
    post_process_steps = ['winsorize', 'standardize']

    def compute(self, data, deps=None):
        window = self.para.get("window")
        return ts_mean(data['close'].pct_change(fill_method=None), window)

    def update(self, new_data, history, deps=None):
        # 只取最近 window 条历史，减少计算量
        recent = history.iloc[-self.para['window']:]
        combined = pd.concat([recent, new_data])
        result = ts_mean(combined['close'].pct_change(fill_method=None), self.para['window'])
        return result.iloc[-len(new_data):]
```

### 3.5 示例：复合因子 (`factor_library/technical/risk_adjusted_momentum.py`)

```python
@register_factor
class RiskAdjustedMomentum(Factor):
    name = "risk_adjusted_momentum"
    dependencies = ['momentum_return', 'volatility']
    dependency_para_map = {'volatility': {'window': 20}}

    def compute(self, data, deps=None):
        momentum = deps['momentum_return']
        vol = deps['volatility']
        return momentum / vol.replace(0, np.nan)
```

DAG 调度自动确保：先计算 `momentum_return` 和 `volatility`，再计算 `risk_adjusted_momentum`。

---

## 4. Universe 过滤 (`factor_library/universe.py`)

**UniverseConfig 参数**:
| 参数 | 默认值 | 说明 |
|---|---|---|
| `listing_days` | 60 | 上市不满 N 天视为次新股 |
| `filter_st` | True | 排除 ST/*ST |
| `filter_suspended` | True | 排除停牌 (volume=0) |
| `filter_delisted` | True | 排除已退市 |
| `min_price` | 1.0 | 排除仙股 |
| `max_price_pct_up` | 0.09 | 涨停阈值 |

**过滤流程**:
```
filter(kdata)
  ├── 构建宽表骨架: mask = True (全部可交易)
  ├── 规则 1: volume == 0 → 停牌 → mask &= False
  ├── 规则 2: close < min_price → 仙股 → mask &= False
  ├── 规则 3: name 含 "ST" → ST 股 → mask &= False
  ├── 规则 4: list_date 近 N 日 → 次新股 → mask &= False
  └── 规则 5: delist_date <= T → 退市股 → mask &= False
```

输出: `pd.DataFrame`, `index=dates, columns=entity_id, dtype=bool`

---

## 5. 回测流水线 (`engine/pipeline.py`)

### 5.1 5 层接口

```python
class AlphaModel(ABC):
    def get_signals(self, kdata, universe_mask) -> pd.DataFrame

class PortfolioConstructor(ABC):
    def create_portfolio(self, signals, kdata) -> pd.DataFrame

class RiskFilter(ABC):
    def apply_risk_rules(self, target_weights) -> pd.DataFrame

class ExecutionHandler(ABC):
    def execute(self, safe_weights, kdata) -> Any
```

### 5.2 具体实现

**FactorAlphaModel** (`get_signals`):
```
for entity_id, group in kdata.groupby('entity_id'):
    vals = factor.compute(group)           # 逐股票计算因子
    results.append(timestamp, entity_id, value)

signal_df = pd.concat(results).pivot(index='timestamp', columns='entity_id', values='value')
signal_df = signal_df.where(universe_mask)  # 应用 Universe 过滤
```

**QuantilePortfolio** (`create_portfolio`):
```
quantile_labels = signals.apply(pd.qcut, q=n_quantiles, axis=1)
for q in 1..n_quantiles:
    weights[q] = mask_q / daily_count  # 等权分配
```

**VectorBTExecutionHandler** (`execute`):
```python
pf = vbt.Portfolio.from_orders(
    close=close_prices,
    size=weights,
    size_type='target_percent',    # 目标仓位模式
    group_by=True,                 # 跨标的资金共享
    cash_sharing=True,
    init_cash=1_000_000,
    fees=0.001, slippage=0.002,
    freq='1D'
)
```

### 5.3 流水线组装

```python
pipeline = AlphaPipeline(
    universe=UniverseFilter(filter_st=True, filter_suspended=True),
    alpha=FactorAlphaModel(factor),
    portfolio=QuantilePortfolio(n_quantiles=5),
    risk=DefaultRiskFilter(max_weight=0.1),
    execution=MultiVectorBTExecutionHandler(init_cash=1_000_000),
)
result = pipeline.run(kdata)
```

---

## 6. 交易成本模型 (`engine/simulation/models.py`)

### 6.1 费率模型

**AShareFeeModel**:
```
commission = max(trade_value × 0.0003, 5.0)     # 万三，最低 5 元
stamp_duty = trade_value × 0.0005 if SELL else 0  # 卖出单边
transfer_fee = trade_value × 0.00002              # 过户费
total = commission + stamp_duty + transfer_fee
```

### 6.2 滑点模型

**VolumeShareSlippageModel**:
```
vol_share = order_volume / bar_volume
if vol_share > volume_limit (10%):
    effective_rate = slippage_rate × (1 + excess × impact_factor)
else:
    effective_rate = slippage_rate
slippage = price × effective_rate × order_volume
```

### 6.3 成交模型

**AShareFillModel.can_fill()**:
- 停牌 (volume=0) → 不成交
- 涨停一字板 (open==high==limit_up) → 买入不成交
- 跌停一字板 (open==low==limit_down) → 卖出不成交

---

## 7. 数据采集 (`scripts/update_daily.py`)

### 7.1 TushareUpdater 工作流

```
update_market_data(start_date, end_date)
  ├── 获取交易日历 (排除周末/节假日)
  ├── for date in trade_dates:
  │   ├── 幂等检查: SELECT count(*) FROM cn_stock_1d_hfq WHERE timestamp = ?
  │   ├── 已存在 → skip
  │   ├── fetch_daily_hfq(date)  # Tushare API
  │   │   ├── pro.daily(trade_date)     # 基础行情
  │   │   ├── pro.adj_factor(trade_date)  # 复权因子
  │   │   └── merge + 重命名 + 类型转换
  │   ├── validate_with_akshare(df, sample=3)  # 交叉验证
  │   └── storage.write_data(df)  # 写入 Parquet
  ├── sleep(0.4)  # 限流保护
  ├── storage.refresh_views()
  └── storage.create_adjusted_views()
```

### 7.2 AKShare 交叉验证

随机抽取 3 只股票，对比 Tushare 后复权收盘价与 AKShare 东方财富接口的差异：
- `diff = abs(ts_hfq_close - ak_close) / ak_close`
- `diff > 0.5%` → 告警
- 全部失败 → 返回 False

---

## 8. 策略示例 (`strategies/alpha_001.py`)

双均线交叉策略，使用 Numba JIT 加速核心循环：

```python
@njit
def check_crossover(fast_ma, slow_ma):
    # 返回 1 (金叉买入), -1 (死叉卖出), 0 (不动)
    ...
```

通过 `compile_strategy` 装饰器编译为 VectorBT 可执行的策略函数，由 `run_strategy_numba` 驱动回测。

---

## 9. 测试用例

| 测试文件 | 覆盖内容 |
|---|---|
| `test_factor_basic.py` | Factor 创建、元数据验证、参数空间 (mock 数据) |
| `test_factor_library.py` | 算子库 (ts_mean, cs_rank)、后处理 (winsorize, standardize) |
| `test_incremental_calculation.py` | 增量 `update()` vs 全量 `compute()` 一致性验证 |
| `test_storage_refactor.py` | Parquet 写入 + DuckDB 视图注册 |
| `test_zvt_adapter_integration.py` | FactorAdapter → ZVT StockTrader 集成 |
| `test_kdata_recorder.py` | AKShareStock1dKdataRecorder 数据录制 |
| `test_adj_factor_recorder.py` | AKShareStockAdjFactorRecorder 复权因子录制 |

---

## 10. 已知代码问题速查

| 位置 | 问题 | 影响 |
|---|---|---|
| `tests/test_*.py` 多处 | `sys.path.insert('/Users/nachuanchen/...')` 硬编码 | 不可移植 |
| `infra/storage.py:398` | `get_factor_matrix` SQL 字符串拼接 | 注入风险 |
| `scripts/update_daily.py:149` | 幂等检查 SQL 字符串拼接 | 注入风险 |
| `infra/storage.py:28` | ConfigLoader 单例无 reset | 测试间污染 |
| `engine/pipeline.py:86` | `FactorAlphaModel` 逐股票循环 | 性能瓶颈 |
| `engine/factor/incremental_updater.py` | `deps=None` 硬编码 | 复合因子无法增量更新 |
| `engine/zvt_bridge/data_syncer.py:36-37` | 重复 import | 代码规范 |
| `pyproject.toml` | `polars`/`clickhouse-driver` 未使用 | 安装体积 |