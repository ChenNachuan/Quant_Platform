# storage_schema

> 域：data · 对应代码 data/storage_schema.py · 来源 §46–§47 / §54–§56（archive/OPEN_QUESTIONS.md）

## 职责与边界

存储的**结构契约**：两层职责划分、11 张核心表 schema、通用列约定、字段精度、schema 演进规则。它是方向 C「薄规范」的本体——`SCHEMA` 字典 + 目录约定，其上不建容器运行时（§56）。

- **两层**：Parquet = 持久化唯一真相；DuckDB = 纯视图查询引擎（`read_parquet`，**无 .duckdb 文件**）。
- **11 张核心表**：bars_daily / bars_minute / bars_daily_futures / tick_futures_l2 / fundamental / factor / calendar / contract_specs / industry / corporate_actions / continuous。
- **精度与演进**：一开始选上限类型（DOUBLE/INT64），schema 只增列不改类型/列名。

边界：不引入 ClickHouse 且**不预留接口**（单 backend 抽象层是纯开销）；不引入 udata 容器层（6 类 Udata = 11 张表的概念映射，无 Container 类）；写入与查询行为不在本模块（→ [storage_ops.md](storage_ops.md)、[data_api.md](data_api.md)）。

**udata 四答**（§54.2–54.5，公司概念还原，平台侧结论 = 不引入容器运行时）：① 形态 = Schema（自描述文本）+ 容器抽象（底层 backend 可插拔：Ucube/内存/文件）；② 粒度 = 一个 BarData = 三维矩阵 `[di, ti, ii]`（多日 × 多时段 × 多合约，不是一合约一条）；③ DataBus = 实时消息总线，与 udata 是**上下游**关系（DataBus 传行情 → 切 bar → 写 udata），不是包含关系；④ k-slice = 对容器的 `Slice(di, ti, ii)` 多维切片操作，不是容器本身。反向产出（回测结果/订单）不组织成 udata/行情 schema——落 `outputs/` 报告即可（§54.9/§55）。

## 接口签名

```python
# 通用列约定（§46.11）
# date → date 类型；datetime → datetime64[ns] naive 北京时间；UTF-8
# ticker → "{代码}.{交易所}"；默认 NOT NULL（L08，可空例外 6 类）
# quality_flag → 行级 int64 bitmask（因子表不带）；一天一文件不用 Hive

SCHEMA = {...}   # 方向 C 薄规范本体（§56）：列名 + 类型 + 目录约定
```

**11 张表**（文件路径相对 `data_platform/`；PK = 主键）：

| 表 | 文件 | 关键列 / 约束 |
|---|---|---|
| bars_daily | `asset/bars_daily/{date}.parquet` | 14 列：ticker/trade_date/OHLCV（float64 **未复权**）/volume（int64 股）/amount/adjfactor/turnover(Y)/suspend/is_limit_up/is_limit_down/quality_flag；PK(ticker, trade_date)；**不含** prev_close/pct_change/close_adj；~5000 行/天 |
| bars_minute | `asset/bars_minute/{date}.parquet` | 10 列**无 adjfactor**；PK(ticker, trade_date, datetime)；datetime = bar 结束时间；~1.2M 行/天；**5min/60min 不单独存**——DuckDB `time_bucket` 视图聚合 |
| bars_daily_futures | `asset/bars_daily_futures/{date}.parquet` | 同日线 + settle/open_interest；volume 单位手；无 adjfactor |
| tick_futures_l2 | Phase 2 预留 | 27 列 = bid/ask 各 5 档 20 列 + 7 基础；唯一可能需要 ClickHouse 的表 |
| fundamental | `asset/fundamental/{date}.parquet` | total/float_market_cap，pe_ttm/pe_lyr/pb/ps_ttm（**可空**），total/float_shares |
| factor | `factors/{name}/v{version}/{year}.parquet` | 3 列 ticker/date/{name}(Y)；版本目录+metadata.json（§61）；否决单文件 values.parquet（§47 旧案）与宽表 |
| calendar | `asset/calendar/{exchange}.parquet` | 单列 trade_date；6 交易所文件（sse/szse/cffex/shfe/dce/czce） |
| contract_specs | `asset/reference/contract_specs.parquet` | 12 列：multiplier/tick/list_date/expire_date/delivery_month/price_limit_pct/margin_rate(Y)/trading_hours/is_active… |
| industry | `asset/reference/industry.parquet` | industry_l1/l2/l3(Y)/classification(citics/sw)/effective_date；初版只存最新 |
| corporate_actions | `asset/reference/corporate_actions.parquet` | 10 字段；PK(ticker, ex_date, event_type)；event_type 4 类：dividend/split/rights/bonus |
| continuous | `asset/continuous/{symbol}.parquet` | **不存 close_adj**（§47.13），+settle/open_interest；adjfactor = 换月累积 |

**双轨分区**：行情/基本面按日一文件 `{YYYY-MM-DD}.parquet`；reference/连续合约全量（或按 symbol）一文件；因子按 `v{version}/{year}.parquet` 年分区（§61）。

**可空例外 6 类**（其余 NOT NULL）：估值比率 / 因子 value / 公司行动可选字段 / turnover / margin_rate / industry_l3。

**quality_flag 位定义**（§47.15）：bit0=C02 覆盖率不足、bit1=C05 空值率偏高、bit2=L04 超涨跌停、bit3=L07 adjfactor 范围异常、bit4=K01 隔夜跳空过大。行级不批级。

**L13 ticker regex**：A 股 `^[036]\d{5}\.(SH|SZ)$`；期货 `^[A-Z]{1,2}\d{4}\.(CFFEX|SHFE|DCE|CZCE|INE)$`。

**udata 概念映射**（§54.8/§56.3）——6 类 Udata → 11 张表，无 Container 类：

| 公司概念 | 个人平台等价物 |
|---|---|
| BarData (di,ii) 日频 / (di,ti,ii) 分钟 | bars_daily / bars_minute（不同频率=不同表） |
| FutBarData (di,ti,ci,mi) | bars_daily_futures（ticker 列区分合约，品种×月份在主数据） |
| ProductBarData (di,ti,ci) | continuous（按品种聚合的衍生表） |
| DailyData (di,ii) | 日频辅助数据（fundamental；adjfactor 本设计为 bars_daily 列（§47），源 §56.3 示意独立 adjfactor/ 目录） |
| CrossSection (di,ti) | 不需要独立容器——SQL `WHERE date=...` 的查询结果，不是存储单位 |
| OrderTrade 变长流式 | 不引入（不做逐笔） |
| Schema / TypeInfo | Parquet footer + `SCHEMA` 字典 |
| ProductionStatus | DQC 日志 |
| DependencyLineage | `correction_log` 简化版（谁改了谁记一条，不做状态传播 DAG） |
| Universe / TimeAxis | 不独立维护——每行自带 ticker / date / time 字段 |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Parquet 持久化唯一真相 + DuckDB 纯视图 | ClickHouse | 单机 <10GB/零外部依赖；公司 CK 是多策略并发需求 |
| 无 .duckdb 文件，每次 `duckdb.connect()` 内存 | DuckDB 持久化副本 | 一份数据无同步问题（§46 方案 A/B） |
| 不预留存储抽象层 | 多 backend 接口 | 只有一个 backend，抽象是纯开销 |
| 迁移触发条件：L2 单日 >1 亿行/多人/实时写入（迁移 ~200 行，Parquet 仍持久化真相） | 现在预留 | 触发前是死代码 |
| 一天一文件扁平命名 | Hive 分区 | DuckDB 不需 partition discovery；7500 子目录噩梦 |
| 11 张核心表分表 | 宽表/多合一 | 访问形态与生命周期不同 |
| factor 每因子一长表 ticker+date+{name}，版本目录+年分区 | 单文件 values.parquet（§47 旧案）/ 宽表矩阵 | 版本共存不覆盖；年分区与 load_factor 对齐；元数据 metadata.json（§61） |
| bars_daily 不含 prev_close/pct_change/close_adj | 冗余派生列 | 全部可派生 |
| 分钟线无 adjfactor 列 | 每 bar 带 | 存 240 遍浪费（→ calendar_adjustment） |
| 5min/60min 不落盘，视图聚合 | 存三份频率 | 同源不同粒，聚合 SQL 已定稿 |
| 主数据（contract_specs 等）独立 reference 表 | 放行情表 | 主数据与行情分离 |
| 价格 DOUBLE（一开始用上限） | float32 | 公司 float→double 全量重刷事故（page 263692149） |
| volume INT64；amount/adjfactor DOUBLE | Decimal | 0.01 元最小变动价，DOUBLE 绰绰有余 |
| 日期/时间/代码 VARCHAR | bigint 时间戳 | 人类可读，DuckDB 自动识别 |
| schema 只增列，不改类型/列名 | 显式版本号 | union_by_name 就是兼容层；列名是行业标准 |
| 不引入 Compatible 四级 / FixSchema | 公司兼容性机制 | 根因是 tensor 紧耦合；Parquet footer 自描述 |
| 版本历史 = git log（代码即 schema 定义） | 版本表/版本字段 | 不需要显式版本化机制（§56） |
| 不引入 udata 容器运行时，方向 C 薄规范 | Container 类 | 11 张表=6 类 Udata 容器的存储等价物（概念映射） |
| quality_flag 行级 int64 bitmask，因子表不带 | 批级标记 | 因子质量由计算流程保证，追溯到输入表 |

## 实现要点

```python
# bars_daily pandera 示例（其余 10 表同构，§47 共 ≈260 行）
import pandera.pandas as pa
BARS_DAILY_SCHEMA = pa.DataFrameSchema({
    "ticker":      pa.Column(str, pa.Check.str_matches(L13_REGEX), nullable=False),
    "trade_date":  pa.Column(pa.Date, nullable=False),
    "open":        pa.Column(float, nullable=False),   # 未复权
    "high":        pa.Column(float, nullable=False),
    "low":         pa.Column(float, nullable=False),
    "close":       pa.Column(float, nullable=False),
    "volume":      pa.Column(int, pa.Check.ge(0), nullable=False),   # 股
    "amount":      pa.Column(float, nullable=False),
    "adjfactor":   pa.Column(float, pa.Check.gt(0), nullable=False),
    "turnover":    pa.Column(float, nullable=True),
    "suspend":     pa.Column(bool, nullable=False),
    "is_limit_up": pa.Column(bool, nullable=False),
    "is_limit_down": pa.Column(bool, nullable=False),
    "quality_flag": pa.Column(int, pa.Check.ge(0), nullable=False),  # int64 bitmask
}, coerce=True, strict=True)

# 时间戳精度：date 列 date；分钟 datetime64[ns] 秒；L2 毫秒；不用 bigint
# 5min/60min 聚合视图 SQL 已定稿（§47，time_bucket）
# 写入配置与原子写 → storage_ops.PARQUET_WRITE_CONFIG / write_parquet_atomic
```

## 实施时定稿项

- 其余 10 张表的 pandera 显式代码（§47 只给了 bars_daily 示例 + 各表列定义）
- tick_futures_l2 的 27 列 pandera 展开写法（bid_price_1~5 是 5 个独立列）（§47）
- trade_date 在 pandera 用 datetime64[ns] 再转 date 的转换时机（验证前还是验证后）（§47）
- INE 交易所日历文件缺失（日历 6 文件列表无 INE，但 L13 regex 出现 INE）（§47/§42）
- factor 表按因子分目录后 `factor_*` 视图命名冲突（因子名含特殊字符时）（§47）
- corporate_actions 的 event_type 是否有第五类（当前 4 类）（§47）
- `strict=True` 对加列演进的兼容策略（与 union_by_name 问题合并）（§47）
- `union_by_name=true` 兼容性细节（schema 演进新增列）（§46）
- DataAPI 是否需要 `get_minute_bars`/`get_fundamental` 便捷方法（当前 get_daily_bars/get_factor）（§46）
- 视图注册是否处理因子目录动态增删（当前启动时一次性注册）（§46）
- `data_lake/`（Hive 分区 year=YYYY）→ `data_platform/`（一天一文件）迁移脚本（§46/§48）
- 删除列的语义（旧文件忽略被删列，新代码引用会怎样——CI 加 schema 引用检查）（§55）
- quality_flag 是否扩展（如加 3=stale）；§55.2 精度表记 INT8（0/1/2）与 §47.15 int64 bitmask 表述不一致，实施时定稿其一（§55/§47）
- VARCHAR 日期是否换 DuckDB DATE 类型（当前 'YYYY-MM-DD' 字符串比较=日期比较，无精度问题）（§55）
- schema 变更的记录位置（correction_log 还是独立 schema_changelog.jsonl）（§55）
- ~~字段精度~~ 已定 → §55.2（OHLC/amount/adjfactor DOUBLE、volume INT64、不用 Decimal）；~~schema 版本化~~ 已定 → §55.3/§56（只增列 + union_by_name + git log）；~~Compatible 轻量版~~ 不需要 → §55.3 末段
