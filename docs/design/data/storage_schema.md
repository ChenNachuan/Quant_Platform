# storage_schema

> 域：data · 对应代码 [data/storage_schema.py](../../../data/storage_schema.py)

## 职责与边界

存储的**结构契约**：两层职责划分、11 张核心表 schema、通用列约定、字段精度、schema 演进规则。

- **两层**：Parquet = 持久化唯一真相；DuckDB = 持久化 `catalog.duckdb` 视图与统计缓存引擎（基于 `read_parquet`，否决纯内存模式）。
- **11 张核心表**：bars_daily / bars_minute / bars_daily_futures / tick_futures_l2 / fundamental / factor / calendar / contract_specs / industry / corporate_actions / continuous。
- **精度与演进**：一开始选上限类型（DOUBLE/INT64），schema 只增列不改类型/列名。

边界：不引入 ClickHouse 且**不预留接口**；写入与查询行为不在本模块（→ [storage_ops.md](storage_ops.md)、[data_api.md](data_api.md)）。

## 目录布局

扁平一层,资产类型编码在文件夹名前缀:`{nation}_{asset_type}_{data_type}`
```text
$QUANT_DATA_ROOT/
├──catalog.duckdb                              #DuckDB视图定义+统计缓存
├──asset/        
│   ├──cn_stock_bars_daily/{date}.parquet
│   ├──cn_stock_bars_minute/{date}.parquet
│   ├──cn_stock_fundamental/{month}.parquet
│   │
│   ├──cn_etf_bars_daily/{date}.parquet
│   ├──cn_etf_fundamental/{month}.parquet
│   │
│   ├──cn_calendar/{exchange}.parquet           #全量一文件
│   └──cn_reference/
│       ├──industry.parquet
│       └──corporate_actions.parquet
│
├──factors/{nation}_{asset_type}_{data_type}/{name}_v{version}/{year}.parquet
├──ext/{dataset}/{subtable}/{volume}.parquet
├──raw/{source}/{asset_type}/{date}.parquet
├──quarantine/{asset_type}/{date}.parquet
├──dqc/
├──cross_check/
└── manifest/
```
分区策略:
- 行情:按日一文件{YYYY-MM-DD}.parquet,资产类型在目录名区分
- 小文件每日截面表（`cn_stock_fundamental` / `cn_etf_fundamental`）:按月一文件{YYYY-MM}.parquet（按交易日原子合并写入,避免 <1MB 碎文件）
- reference:全量一文件
- 因子:v{version}/{year}.parquet 年分区+metadata.json
- V3+扩展:futures_bars_daily/、futures_tick_l2/等直接加目录,不改现有结构

## 核心层

以[data/storage_schema.py](../../../data/storage_schema.py) 的pandera契约为准。

**11 张表**（文件路径相对 `$QUANT_DATA_ROOT/`；PK = 主键）：

| 表 | 文件 | 关键列 / 约束 |
|---|---|---|
| bars_daily | `asset/cn_stock_bars_daily/{date}.parquet` | 14 列：ticker/trade_date/OHLCV（float64 **未复权**）/volume（int64 股）/amount/adjfactor/turnover(Y)/suspend/is_limit_up/is_limit_down/quality_flag；PK(ticker, trade_date)；**不含** prev_close/pct_change/close_adj |
| fundamental | `asset/cn_stock_fundamental/{month}.parquet` | total/float_market_cap，pe_ttm/pe_lyr/pb/ps_ttm（**可空**），total/float_shares |
| calendar | `asset/cn_calendar/{exchange}.parquet` | 单列 trade_date；9 交易所文件（sse/szse/bse/cffex/shfe/dce/czce/ine/gfex） |
| industry | `asset/cn_reference/industry.parquet` | industry_l1/l2/l3(Y)/classification(citics/sw)/effective_date；初版只存最新 |
| corporate_actions | `asset/cn_reference/corporate_actions.parquet` | 10 字段；PK(ticker, ex_date, event_type)；event_type 4 类：dividend/split/rights/bonus |
| bars_minute | `asset/cn_stock_bars_minute/{date}.parquet` | 10 列**无 adjfactor**；PK(ticker, trade_date, datetime)；datetime = bar 结束时间 |
|-|-|-|
| factor | `factors/cn_stock_factors/{name}_v{version}/{year}.parquet` | 3 列 ticker/date/{name}(Y)；版本目录+metadata.json |
|-|-|-|
| bars_daily_futures | `asset/cn_futures_bars_daily/{date}.parquet` | 同日线 + settle/open_interest；volume 单位手；无 adjfactor |
| tick_futures_l2 | `asset/cn_futures_tick_l2/{date}.parquet` | Phase 2 预留：27 列 = bid/ask 各 5 档 20 列 + 7 基础 |
| contract_specs | `asset/cn_reference/contract_specs.parquet` | 12 列：multiplier/tick/list_date/expire_date/delivery_month/price_limit_pct/margin_rate(Y)/trading_hours/is_active… |
| continuous | `asset/cn_futures_continuous/{symbol}.parquet` | **不存 close_adj**（§47.13），+settle/open_interest；adjfactor = 换月累积 |

## 通用约定

- **ticker 格式**:A股 ^[036]\d{5}\.(SH|SZ)$`;期货`^[A-Z]{1,2}\d{4}\. (CFFEX|SHFE|DCE|CZCE|INE)$
- **可空例外**:估值比率/因子value/turnover/margin_rate/industry_l3(其余 NOT NULL)
- **quality_flag**: int64位掩码,5 位公司行动可选字段(bit0=C02覆盖率/bit1=C05空值率/bit2=L04超涨跌停/bit3=L07 adjfactor 范围/bit4=K01隔夜跳空)。行级不批级,因子表不带
- **不含派生列**:prev_close/pct_change/close_adj全部读时计上算
- **分钟线无adjfactor**:日级因子存 240遍浪费,读时从, bars_daily join



## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Parquet + DuckDB catalog | ClickHouse | 单机 <10GB/零外部依赖 |
| 持久化catalog.duckdb 文件 | 纯内存模式 | 视图和统计缓存避免重建 |
| 不预留存储抽象层 | 多 backend 接口 | 只有一个 backend，抽象是纯开销 |
| 扁平化命名`{nation}_{asset_type}_{data_type}/` | 多层目录嵌套、混合存储 | 路径短，避免扫描无关文件，加新资产加目录即可 |
| factor 每因子一长表，版本目录+年分区 | 单文件/ 宽表矩阵 | 版本共存不覆盖 |
| 价格 DOUBLE | float32 | 避免精度不足全量重刷事故 |
| volume INT64；amount/adjfactor DOUBLE | Decimal | 0.01 元最小变动价，DOUBLE 绰绰有余 |
| schema 只增列，不改类型/列名 | 显式版本号 | union_by_name 就是兼容层；列名是行业标准 |
| quality_flag 行级 int64 bitmask，因子表不带 | 批级标记 | 因子质量由计算流程保证，追溯到输入表 |



## 实现要点

详见 [data/storage_schema.py](../../../data/storage_schema.py)

核心约定:
- SCHEMA 字典是pandera DataFrameSchema的集合,一表一条目
- PARTITION_PATHS字典定义每张表的落盘路径模板
- schema验证在写入前(ingest_with_dqc step7)和读回后双重校验
- 写入配置与原子写→storage_ops.PARQUET_WRITE_CONFIG/ write_parquet_atomic
- catalog.duckdb存放于$QUANT_DATA_ROOT/下,跟数据走不跟项目走
