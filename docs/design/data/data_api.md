# data_api

> 域：data · 对应代码 data/data_api.py · 来源 §52–§53（archive/OPEN_QUESTIONS.md）

## 职责与边界

研究/回测侧的**统一取数入口**：SQL-only 查询层 + 加载适配层。对标公司 UdataViewer/GetPandasDataFrame，但只保留最薄一层——DuckDB `read_parquet` 原生已含列裁剪/row group pruning/predicate pushdown，不需要 sqlglot 预过滤等中间层。

- **DataAPI**（~80 行）：`duckdb.connect()` 内存模式 + `register_views` + `query`/`query_arrow`。
- **load_bars**（~50 行）：表映射 + SQL 组装 + 复权 + 5min/60min 聚合，返回长表。
- **适配层**：`to_alphalens_prices` / `to_alphalens_factor`（各 ~20 行）按消费端出口径。

边界：入库流水线**不经过 DataAPI**（写入走 `write_parquet_atomic`，写读完全解耦）；不做应用层缓存；不提供 DataFrame API/对象 API（SQL 最通用，极致场景可直接 `pd.read_parquet()` 绕过）。

## 接口签名

```python
class DataAPI:
    def __init__(self, data_root="data_platform/asset", threads=None,
                 memory_limit=None, statement_timeout=60.0): ...
    def query(self, sql: str, profile: bool = False) -> pd.DataFrame
    def query_arrow(self, sql: str) -> pa.Table
    def register_views(self): ...    # CREATE OR REPLACE VIEW {table} AS read_parquet(..., union_by_name=true)
    def close(self): ...
    # with DataAPI() as api: __enter__ 注册视图（惰性、毫秒级）

def load_bars(symbol: str | list[str] | None = None,
              start: str = "2020-01-01", end: str = "2024-12-31",
              freq: str = "daily",           # daily | minute | 5min | 60min
              fields: list[str] | None = None,
              adjust: str = "none",          # none | forward | backward
              asset_type: str = "equity") -> pd.DataFrame
    # 返回列：date, ticker, open, high, low, close, volume, amount, [adjfactor]
    # 按 (date, ticker) 排序

def load_bars_chunked(..., chunk_by: str = "month") -> Iterator[pd.DataFrame]
def to_alphalens_prices(start, end, symbols=None) -> pd.DataFrame   # pivot: date × ticker
def to_alphalens_factor(factor_df) -> pd.Series                     # MultiIndex (date, ticker)
# ~~to_backtrader_feed / to_vnpy_bars~~ 不再需要——§66.2 不用 backtrader/vnpy
```

**表映射**：`(equity, daily)→bars_daily`、`(equity, minute)→bars_minute`、`(futures, daily)→bars_daily_futures`、`(continuous, daily)→continuous`。

**复权分流**（§43/§53.5）：存储层中性（未复权 + adjfactor），适配层按消费端转换——alphalens **前复权**；实盘/下单 **未复权**（=前复权口径一致，§44.5）；`returns` 由 PriceAccessor 出。

**口径归一总表**：适配只有两件事——**复权 + 时间戳**（字段命名在 §47 schema 定稿时已刻意对齐，ohlcv 零映射；仅历史 vnpy 适配有 amount→turnover、ticker 拆 symbol+exchange）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| SQL-only DataAPI ~80 行 | DataFrame API/对象 API | read_parquet 一样快；SQL 最通用；不维护两套 |
| `duckdb.connect()` 内存模式，无 .duckdb | 持久化连接 | 无共享可写状态，无锁（§46） |
| `register_views` + `read_parquet` glob | sqlglot 预过滤 | DuckDB 原生优化已含；预过滤为 Udata 私有格式而生 |
| `union_by_name=true`、`hive_partitioning=false` | 默认 | schema 演进按列名对齐；扁平文件显式关 Hive |
| 不做应用层缓存 | DataPool 懒加载 | 全量 ~45GB NVMe 15s；OS page cache 零代码；缓存失效是坑 |
| `[COST]` 一行日志 + >5s WARNING + >30s CRITICAL + timeout 60s | 5 条分布式追踪 | 单机单进程 grep/jq 足够 |
| `profile=True` 一行开关 | 无 profiling | DuckDB `enable_profiling` 一行 |
| 每进程独立 DataAPI，不跨进程共享连接 | 共享连接 | DuckDB 连接非进程安全 |
| 同进程多线程共享 DataAPI；不手动设 threads | 手动线程数 | DuckDB 线程安全、自动用满 CPU |
| query→DataFrame / query_arrow→Arrow 双出口 | batch 接口 | 单日最大 127 万行，一次 fetch 足够 |
| `load_bars` 统一加载（对标 GetPandasDataFrame） | 各框架自取 | 口径归一收在一处 |
| 复权在加载层 `adjust` 参数实现 | 存储层存复权价 | 与 §43 零回刷一致 |
| 日频全量；分钟 >3 月按月 `load_bars_chunked` | 一律全量 | 一年分钟 ~20GB，32GB RAM 会 OOM |
| 5min/60min 用 `_aggregate_bars_sql` 聚合 | 存多份 | §47 视图聚合定稿 |
| 性能目标：alphalens 日频 <1s / 分钟 <5s / 全历史 <30s | 无验收线 | 四步排查保证（profile→pruning→列投影→polars 对照） |

## 实现要点

```python
class DataAPI:
    def __init__(self, data_root="data_platform/asset", statement_timeout=60.0, ...):
        self._conn = duckdb.connect()                 # 内存模式
        self._conn.execute(f"SET statement_timeout = '{int(statement_timeout*1000)}ms'")

    def register_views(self):
        for table_dir in self._root.iterdir():
            if table_dir.is_dir():
                self._conn.execute(f"""CREATE OR REPLACE VIEW {table_dir.name} AS
                    SELECT * FROM read_parquet('{table_dir}/*.parquet',
                        union_by_name=true, hive_partitioning=false)""")

    def query(self, sql, profile=False):
        t0 = time.perf_counter()
        result = self._conn.execute(sql).fetchdf()
        logger.info(f"[COST] time={time.perf_counter()-t0:.3f}s rows={len(result)} sql={sql[:200]}")
        return result

def load_bars(...):
    table = TABLE_MAP[(asset_type, _base_freq(freq))]
    sql = f"SELECT {cols} FROM {table} WHERE date BETWEEN ... AND ticker ... ORDER BY date, ticker"
    if freq in ("5min", "60min"): sql = _aggregate_bars_sql(...)   # time_bucket
    with DataAPI() as api:
        df = api.query(sql)
        if adjust != "none":
            # 分母用全历史基准（§43），不能用窗口内 transform("last"/"first")——截断窗口复权尺度会错
            base = api.query(f"SELECT ticker, arg_max(adjfactor, trade_date) AS adj_latest, "
                             f"arg_min(adjfactor, trade_date) AS adj_first FROM {table} GROUP BY ticker")
            m = df.merge(base, on="ticker", how="left")
            if adjust == "forward":   ratio = m.adjfactor / m.adj_latest   # qfq = raw×adj/adj_latest
            elif adjust == "backward": ratio = m.adjfactor / m.adj_first   # 上市 adj=1 时等价 raw×adj
            # price_cols × ratio
```

新增代码 ≈80 行（DataAPI）+ ≈110 行（load_bars ~50 + 适配各 ~20）。

## 实施时定稿项

- `_resolve_table_refs` 简单实现依赖预注册视图；CTE/子查询内表名解析（视图全局可见应可以）（§52）
- `query_config.yaml` 阈值是否按查询类型细分（全表扫 vs 点查）（§52）
- profile 输出落盘方式（stdout vs jsonl）（§52）
- Jupyter 长 session 中 DataAPI 连接失效的重连语义（§52）
- `query_arrow` → polars 零拷贝路径是否封装（§52）
- ~~DataAPI 返回长表列全集与 dtype~~ 已定 → §53.3 load_bars 签名；~~alphalens periods 默认值~~ 已定 → §27 periods=(1,5,10,20)（2026-10-10 回改）
- `_aggregate_bars_sql` 的 5min/60min 聚合边界（跨日 bar、午休 11:30–13:00）（§53）
- alphalens `get_clean_factor_and_forward_returns` 默认 quantiles/periods 取值（§53）
- `load_bars_chunked` 的 `_date_chunks` 是否跳过非交易日（自然月 vs 交易月）（§53）
- 期货 continuous 表的复权语义衔接（主力连续 vs 指数连续，与换月逻辑）（§53）
- ~~vnpy exchange 枚举映射~~、~~backtrader 分钟 time 列格式~~ 不再需要——§66.2 不用 backtrader/vnpy（2026-10-10 回改）
