# data_api

> 域：data · 对应代码 [data/data_api.py](../../../data/data_api.py)

## 职责与边界

研究/回测侧的**统一取数入口**：DuckDB 查询层 + 加载适配层。DuckDB `read_parquet` 原生已含列裁剪/row group pruning/predicate pushdown，不需要中间层。

- **DataAPI**：`duckdb.connect(catalog.duckdb)` 持久化模式 + `register_views` + `query`/`query_arrow`。
- **load_bars / load_bars_result**：表映射 + SQL 组装 + 复权 + 5min/60min 聚合，返回长表，并附带口径与血缘元数据（`df.attrs["meta"]` / `BarsResult`）。
- **适配层**：`to_alphalens_prices` / `to_alphalens_factor` 按消费端口径整形。

边界：入库流水线**不经过 DataAPI**（写入走 `write_parquet_atomic`，写读完全解耦）；不做应用层缓存；不提供 DataFrame API/对象 API（SQL 最通用）。

## 接口签名

```python
BarFreq = Literal["daily", "minute", "5min", "60min"]
AdjustMode = Literal["none", "forward", "backward"]

@dataclass(frozen=True)
class BarsMeta:
    table: str
    table_family: str
    freq: str
    adjust: str
    start: str
    end: str
    as_of: str | None
    rows: int
    tickers: int
    source: str | None = None

@dataclass(frozen=True)
class BarsResult:
    data: pd.DataFrame
    meta: BarsMeta

class DataAPI:
    def __init__(self, data_root=None, catalog_path=None,
                 statement_timeout=60.0, read_only: bool = False): ...
    def query(self, sql: str, profile: bool = False) -> pd.DataFrame
    def query_arrow(self, sql: str) -> pa.Table
    def register_views(self): ...    # CREATE OR REPLACE VIEW {table} AS read_parquet(..., union_by_name=true)
    def read_source_tags(self, table: str) -> str | None
    def close(self): ...
    # with DataAPI() as api: __enter__ 注册视图（惰性、毫秒级）

def load_bars(symbol: str | list[str] | None = None,
              start: str = "2020-01-01", end: str = "2024-12-31",
              freq: BarFreq = "daily",
              fields: list[str] | None = None,
              adjust: AdjustMode = "none",
              table_family: TableFamily | str = "equity") -> pd.DataFrame
    # 返回列：date, ticker, open, high, low, close, volume, amount, [adjfactor]
    # 按 (date, ticker) 排序；同时将 BarsMeta 挂载于 df.attrs["meta"]

def load_bars_result(...) -> BarsResult          # 显式返回 (data, meta) 容器
def load_bars_chunked(..., chunk_by: str = "month") -> Iterator[pd.DataFrame]
def to_alphalens_prices(start, end, symbols=None) -> pd.DataFrame   # pivot: date × ticker
def to_alphalens_factor(factor_df) -> pd.Series                     # MultiIndex (date, ticker)
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| SQL-only DataAPI | DataFrame API/对象 API | read_parquet 一样快；SQL 最通用 |
| `duckdb.connect(catalog.duckdb)` 持久化链接 | 纯内存模式 | 视图和统计缓存免重建 |
| `register_views` + `read_parquet` glob | sqlglot 预过滤 | DuckDB 原生优化已含 |
| `union_by_name=true`、`hive_partitioning=false` | 默认 | schema 演进按列名对齐；扁平文件显式关 Hive |
| 不做应用层缓存 | DataPool 懒加载 | OS page cache 零代码；缓存失效是坑 |
| 每进程独立 DataAPI，不跨进程共享连接 | 共享连接 | DuckDB 连接非进程安全 |
| 复权在加载层 `adjust` 参数实现 | 存储层存复权价 | 与零回刷策略一致 |
| `BarFreq`/`AdjustMode` + 不可变 `BarsMeta` | 裸字符串 + 裸 DataFrame | 静态约束常用枚举并把复权/截止日/血缘钉在返回值中 |
| `df.attrs["meta"]` + `load_bars_result` 双入口 | 直接改 `load_bars` 返回 tuple | 对现有 DataFrame 调用方零破坏，新代码可强类型解包 |
| 日频全量；分钟 >3 月按月 `load_bars_chunked` | 一律全量 | 一年分钟 ~20GB，32GB RAM 会 OOM |
| 5min/60min 用 `_build_aggregated_sql` 聚合 | 存多份 | 同源不同粒，不落盘 |

## 实现要点

详见 [data/data_api.py](../../../data/data_api.py)

核心约定：
- `catalog.duckdb` 存放于 `$QUANT_DATA_ROOT/`，持久化存储视图与统计缓存
- `register_views` 为每个 asset 子目录创建 `CREATE OR REPLACE VIEW ... AS read_parquet(glob)`
- `load_bars` 返回列：`date/ticker/OHLCV/amount/[adjfactor]`，按 `(date, ticker)` 排序，并在所有排序/裁剪完成后挂载 `df.attrs["meta"] = BarsMeta(...)`
- `BarsMeta.source` 读取底层 Parquet 文件键值元数据 `source_tag`（多源去重后按逗号拼接，无记录则为 `None`）
- 复权基准必须用全历史 `arg_max(adjfactor, trade_date)`/`arg_min(...)`，不能用窗口内 transform——截断窗口会导致复权尺度错误
- 写读完全解耦：入库走 `write_parquet_atomic`，查询走 `DataAPI`，两者互不感知
