# 02. 存储格式、时区与防踩坑约定

[← 返回目录](README.md)

> 核心来源：官网格式规范 [`format.html`](https://qdc-data.com/format.html)（[`format.md?v=2`](https://api.qdc-data.com:8443/samples/format.md?v=2)）与全库数据字典 `data_dict.json`（`2026-11-12` 版）各表 `须知`。本篇集中沉淀所有极易导致静默错误（**程序不报错但结果全错**）的数据工程红线。

---

## 1. 存储布局与分卷结构

- **底层格式**：`Parquet + zstd` 压缩，DuckDB / PyArrow / pandas 可直接读取。
- **目录与表名规则**：
  - 单表数据集存放在 `<dir>/QDCData/<sku>/full/` 下（例如 `cn_daily_full/full/`）。
  - 多表数据集按子表名分文件夹存放，如 `cn_fin_statements/income/`、`cn_lhb/seat_detail/`。
  - 例外记录：`us_stocks_1d` 下载目录为 `us_stocks_1d/full/all.parquet`，在 API / 数据字典中逻辑表名为 `raw_bars`。
- **分卷粒度**：
  - 大体量时序数据集按年切分（`2001.parquet … 2025.parquet`），当年或近月按月/日分卷（`2026-09.parquet`、`2026-10-08.parquet`）。
  - 小数据集与基准表整表合为一卷 `all.parquet`。
  - 本地目录若出现不在最新 `data_dict.json` 中的文件夹，说明该数据集已下架退役，可自行清理。

---

## 2. 时间列 `ts`、DuckDB 陷阱与双时区口径

### 2.1 `ts` 列与 DuckDB `TRY_CAST` 静默归零坑

- 行情与部分高密度数据集的时间列 `ts` 为 64 位整数（`BIGINT`），存储 **Unix 毫秒时间戳**。
- **红线禁止**：在 DuckDB 中对 `ts` 执行 **`TRY_CAST(ts AS DATE)` 对任意 `BIGINT` 恒返回 `NULL` 且不报错**，按它做 `WHERE` 过滤会**静默返回 0 行**！
- 正确过滤方式：用北京时间边界转成毫秒时间戳做范围比较，或用 `to_timestamp(ts / 1000)` 还原：

```sql
-- 正确：用 epoch_ms 边界过滤
SELECT symbol, to_timestamp(ts / 1000) AS dt, close
FROM read_parquet('cn_daily_full/full/*.parquet')
WHERE ts >= epoch_ms(TIMESTAMPTZ '2024-01-01 00:00:00+08')
  AND ts <  epoch_ms(TIMESTAMPTZ '2025-01-01 00:00:00+08');

-- 反例（绝对禁止）：TRY_CAST(ts AS DATE) 恒为 NULL，静默返回 0 行！
-- WHERE TRY_CAST(ts AS DATE) >= DATE '2024-01-01'
```

### 2.2 切勿硬依赖 `trade_day` 列

- 全库 111 个在售数据集中，**仅 28 个数据集包含 `trade_day` 列**（A 股/ETF/指数日周月与各档分钟线、可转债日线与分钟线、`cn_float_shares`、`crypto` 六库；另 `cn_etf_pcf` / `cn_sw_index` 带 `trade_day`）。
- 官网明确说明：部分历史早年行情分卷不一定携带 `trade_day`（或文件名直读不含此列），日期换算一律优先基于 `ts`；其余非 K 线表多使用 `trade_date` / `rank_date` / `d` / `date` 等真日期列。

### 2.3 两套故意区分的时区口径（严禁强行统一）

| 口径 | 时区定义 | 适用数据集 |
|---|---|---|
| **北京自然日** | `Asia/Shanghai` (`UTC+8`) | A 股、ETF、指数、可转债的日/周/月及分钟线、`cn_float_shares` 等境内市场 |
| **UTC 日** | `UTC` | **`crypto` 数字货币六库**（现货/永续 × 1d/1h/1m），按币安交易所发包与 UTC 零点切分 |

> [!CAUTION]
> 切勿把数字货币和 A 股/夜盘的切日时区强行拉平为同一套逻辑，否则与日线 Join 时会整体错位一天，而总行数和成交量守恒校验却完全通过——属于极其隐蔽的回测事故。

### 2.4 分钟 K 线时间戳与柱数差异对照

不同品种的 1 分钟 K 线在**开盘竞价归属**与**时间戳定义（结束时刻 vs 开始时刻）**上存在根本区别：

| 数据集 | 每日 K 线数 | 时间戳含义 | 集合竞价与特殊说明 |
|---|---:|---|---|
| `stocks_1m`（2001~至今） | **241 根** | **结束时刻**（`09:30` + `09:31–11:30` + `13:01–15:00`） | `09:30` 单独为一根开盘集合竞价；`15:00` 含收盘集合竞价（沪市自 `2018-08-20` 起）；无 `13:00`；**停牌日有平盘占位行**（`OHLC=昨收, vol=0`），滚算均线/波动率前须按日成交量合计 `> 0` 过滤 |
| `stocks_1m`（1999-07-26~2000-12-29） | **240 根** | **结束时刻**（`09:31–11:30`, `13:01–15:00`） | 仅 844 只股票部分通过核验的交易日，**未拆 `09:30` 竞价，未补造停牌占位行** |
| `stocks_5m / 15m / 30m / 60m` | 48 / 16 / 8 / 4 | **开始时刻**（第一根 `09:30`） | 开盘/收盘竞价分别并入首尾两根，注意时间标签定义与 `stocks_1m` 刚好相反 |
| `cn_index_1m`（2005~至今；2000–2004 部分 16 只） | **240 根**（债券指数 270 根到 `15:30`） | **结束时刻**（`09:31–15:00`） | 无单独 `09:30` 竞价根；5m/15m/30m/60m 为开始时刻 |
| `cn_etf_1m` / `cn_cb_1m` | **240 根** | **开始时刻**（`09:30–11:29`, `13:00–14:59`） | 无单独 `09:30` 竞价根，与 `stocks_1m` 相反 |
| `hk_1m` | **330 根**（2012-03-05 前 300 根） | **结束时刻**（`09:31–12:00`, `13:01–16:00`） | 首尾 `09:31` / `16:00` 并入竞价；全天无成交不出记录，有成交日的空分钟出平盘 0 量行 |
| `cn_futures_1m` | 视品种夜盘/日盘 | **开始时刻**（`09:00` 表示 09:00 起一分钟） | 夜盘归属于**实际发生的自然日**，无成交分钟不出 K 线 |

---

## 3. 证券代码（`symbol` / `code` / `ts_code`）规范

- **主流 A 股/ETF/指数/转债格式**：`六位代码.交易所`（聚宽/米筐写法）：
  - 上交所：`.XSHG`（`600000.XSHG`）
  - 深交所：`.XSHE`（`000001.XSHE`）
  - 北交所：`.XBSE`（`920xxx.XBSE`，部分表历史上混用 `83/87/43` 旧码）
  - **接入本平台映射**：`XSHG → SH`、`XSHE → SZ`、`XBSE → BJ`。
- **代码格式例外数据集（Join 前必须检查）**：
  - **纯 6 位裸码 `code`**：`cn_tick`、`cn_etf_tick`、`cn_cb_tick`、`cn_etf_nav`、`imai_etf`、`cn_tick_l2`（`证券代码`）。
  - **整数丢前导零警告**：`cn_theme_heat.ths_concept` 与部分表中的 `code` 为 `BIGINT`（`000001` 会变成 `1`），关联前务必 `zfill(6)`。
  - **Tushare 后缀 `.SH / .SZ / .BJ`（`ts_code`）**：`cn_factor_daily`、`cn_board`（`ths_*` / `dc_*` / `tdx_*`）、`cn_hsgt`、`cn_limit_sentiment`、`cn_governance`（多数表）、`cn_chips`、`cn_st`、`cn_suspend`、`cn_equity_events`（`repurchase` / `stk_holdertrade`）、`cn_restricted_release`（`share_float`）。
  - **港股与美股**：`hk_daily` / `hk_1m` 混用 `00001.HK` 或 5 位裸码（如退市股 `03333`），且港股存在退市代码重用（同码间隔 > 180 天时涨跌幅留空）；美股 `us_fundamentals` 的 `facts/sub` 使用 SEC `cik` + `adsh`，13F 持仓表 `us_13f_holding` 使用 `cusip + issuer`（`cik` 是机构编号而非上市公司编号）。
  - **外汇/金属与数字货币**：外汇为货币对（`EURUSD`、`XAUUSD`）；数字货币为交易对（`BTCUSDT`）。

---

## 4. 复权因子与 `cn_daily_hfq` 退役规范

- **架构调整**：QDC 已于 **`2026-08-09` 正式退役纯派生表 `cn_daily_hfq`**，全部统一为**「不复权行情 + 稀疏复权因子表」**本地自行计算，完整保留退市股消除幸存者偏差。
- 涉及复权因子表：A 股 `cn_adj_factor`（5,499 只）、ETF `cn_etf_adj`（1,723 只）、可转债 `cn_cb_adj`（1,057 只）、美股 `us_stocks_1d.adj_factor`（直接列附在日线表上，前复权 `= price × adj_factor / latest_adj_factor`）。

### 还原复权价格的 3 条铁律

1. **稀疏存储需前向填充**：`cn_adj_factor` 只在除权除息日及初始基线日出现一条记录，必须按 `symbol` 排序并关联日历/日线执行前向填充（`ffill`）。
2. **严禁先按回测起止日过滤因子表**：每只标的几乎都包含一条 **`d = 1900-01-01, hfq_factor = 1.0`** 的**基线行**（例如 `cn_adj_factor` 含 5,499 行 `1900-01-01` 基线行），它不是脏数据而是 `ffill` 起点！先按回测开始日过滤会直接删掉基线行，导致首次除权前的区间因子变 `NULL` 且不报错。
   - **唯一正确顺序：`整表读入 → 按 symbol 前向填充 → 截取所需回测区间`**。
3. **缺省值按 `1.0` 处理**：`cn_daily_full` 有近 6,000 只股票，而 `cn_adj_factor` 约 5,500 只，未出现在因子表或填充后仍空缺的标的说明上市以来从未除权，统一填 `1.0`。

---

## 5. 高频踩坑与单位/口径冲突总表

| 数据集 | 致命陷阱 / 口径冲突 | 正确处理方式 |
|---|---|---|
| `cn_tick` | ① `volume` 单位为**整数手**（1 手 = 100 股），与日/分钟线（股）混算会差 100 倍；② `bs=8` 为竞价撮合参考价、`volume=0` 非成交；③ `seq=0, t=09:25, bs=2` 为开盘竞价；`bs=0` 主动买、`bs=1` 主动卖、`bs=5` 盘后固定价 | 成交额按 `price × volume × 100` 估算；统计真实成交时必须加 `volume > 0 AND bs != 8` |
| `cn_tick_l2` | ① **价格列全部为整数，单位万分之一元**；② 2026-06 起沪市 ETF 的 `IOPV` 改为 0；③ 深市撤单在逐笔表中标为 `成交代码 = C` | 全部价格字段必须 **`/ 10000.0`** 转为元 |
| `cn_etf_tick` | `volume` 单位为**股（份）**，不是手！且含 `15:05–15:30` `bs=5` 盘后固定价，故日累计量略高于日线 | 直接 `price × volume` 得成交额；注意 `2010-08-06` 沪市 8 只 ETF 源头量为 0 |
| `cn_cb_*` | `cn_cb_tick` / `cn_cb_1d` / `cn_cb_1m` 的 `volume` 均按**手（1 手 = 10 张，面值千元）** | 换算为张需 `× 10`，成交额验证：`amount / volume / close ≈ 10` |
| `cn_sw_index` | 沿用申万官网原始单位：**`volume` 为亿股、`amount` 为亿元** | 跨表比较成交量/额时务必乘 `1e8` 换算 |
| `cn_mainflow_1d` | 净流入金额单位为**万元**，占比 `1` 代表 `1%` | 换算元需 `× 1e4` |
| `cn_futures_tick` | ① 是 **500 ms 快照**而非逐笔成交明细；② **`volume` 与 `amount` 为当日开盘起的累计值** | 区间成交量/额必须对相邻两条做差（`diff`），**绝不能直接 `SUM()`** |
| `cn_hsgt` | ① 北向 `hgt/sgt/north_money` 自 **`2024-08-19` 起改为成交活跃度（恒正且大十几倍）**，不再是净流入；② 南向自 `2023-11-24` 起为累计量级；③ `hk_hold` 北向自 `2024-08-16` 后仅剩季末；④ 少数日期含 `NaN` 非 `NULL` | 做北向净流入信号严格截断在 `<= 2024-08-16`；聚合前先过滤 `NOT isnan(...)`；代码 `.SH/.SZ` 转 `.XSHG/.XSHE` |
| `imai_etf` | 深市混有 LOF、分级基金、封闭式基金等，整表汇总不能当 ETF 净申赎 | 必须按 `exchange = 'SSE' OR (exchange = 'SZSE' AND etf_type = 'ETF' AND code NOT IN ('159002','159004','159006'))` 过滤 |
| `cn_estimates` | `consensus` 为全历史事后汇总表，回测有严重前视偏差 | 回测强制使用 PIT 面板 `consensus_pit_daily`（`visible_from` 过滤 + 180 天滚动） |
| `cn_board` | 东财板块历史少数日为相邻交易日推定 | 严格 PIT 回测需参考官网推定记录清单剔除推定行 |
| `cn_fin_statements` / `cn_financial_indicator` | 四套行业会计模板（工商业/银行/保险/证券）并成单宽表 | 大量行业专有列对普通企业恒 `NULL`，属正常宽表形态而非缺失 |

---

## 6. DuckDB 与 pandas 快速查询范例

```python
import duckdb
import pandas as pd

# 1. DuckDB 按北京时间边界查询 A 股日线，并关联后复权因子（遵循先整表 ffill 后截取）
sql = """
WITH adj_raw AS (
    SELECT symbol, d, hfq_factor
    FROM read_parquet('cn_adj_factor/full/*.parquet')
),
bars AS (
    SELECT symbol,
           CAST(to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Shanghai' AS DATE) AS trade_date,
           open, high, low, close, volume, amount
    FROM read_parquet('cn_daily_full/full/*.parquet')
    WHERE symbol = '600519.XSHG'
      AND ts >= epoch_ms(TIMESTAMPTZ '2024-01-01 00:00:00+08')
      AND ts <  epoch_ms(TIMESTAMPTZ '2025-01-01 00:00:00+08')
)
SELECT b.*,
       COALESCE((SELECT a.hfq_factor FROM adj_raw a
                 WHERE a.symbol = b.symbol AND a.d <= b.trade_date
                 ORDER BY a.d DESC LIMIT 1), 1.0) AS hfq_factor
FROM bars b
ORDER BY b.trade_date;
"""
df = duckdb.query(sql).df()
```

```python
# 2. pandas 直接处理时间戳与复权
bars_df = duckdb.query("SELECT * FROM read_parquet('cn_daily_full/full/2024.parquet')").df()
bars_df["trade_date"] = (
    pd.to_datetime(bars_df["ts"], unit="ms", utc=True)
    .dt.tz_convert("Asia/Shanghai")
    .dt.date
)
```
