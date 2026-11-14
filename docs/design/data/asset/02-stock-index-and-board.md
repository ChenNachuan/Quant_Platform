# 02. 股票指数、行业与概念板块（4 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)
>
> 说明：明确归属股票域，全部使用 `cn_stock_*` 前缀；支持交易所/中证/国证/申万/中信/同花顺/东财/通达信等板块代码（`validate_index_ticker`）。

---

## 1. `cn_stock_index_basic`（指数 / 行业 / 概念板块档案）

- **路径**：`asset/cn_stock_index_basic/index_basic.parquet`（全量单文件）
- **family**：`stock`
- **主键**：`[ticker]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush` → `amazingdata`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_index_ticker`（后缀 `SH`/`SZ`/`BJ`/`CSI`/`CNI`/`SI`/`CI`/`TI`/`DC`/`TS`） |
| `name` | `str` | N | 指数或行业/概念板块名称 |
| `category` | `str` | N | 分类（宽基/规模/申万行业/中信行业/同花顺概念等） |
| `publisher` | `str` | Y | 发布机构 |
| `base_date` | `pa.Date` | Y | 基期日期 |
| `base_point` | `float` | Y | `> 0`（基点） |
| `list_date` | `pa.Date` | Y | 发布/上市日期 |

---

## 2. `cn_stock_index_bars_daily`（指数与板块日线行情）

- **路径**：`asset/cn_stock_index_bars_daily/{month}.parquet`（按自然月）
- **family**：`stock`
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`, `pe`, `pb`, `turnover`, `total_market_cap`, `float_market_cap`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_index_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `pe` | `float` | Y | 指数市盈率 |
| `pb` | `float` | Y | 指数市净率 |
| `turnover` | `float` | Y | `>= 0`（换手率） |
| `total_market_cap` | `float` | Y | `>= 0`（总市值） |
| `float_market_cap` | `float` | Y | `>= 0`（流通市值） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 3. `cn_stock_industry`（股票行业分类映射）

- **路径**：`asset/cn_stock_industry/industry.parquet`（全量单文件，独立目录不再共用 `cn_reference/`）
- **family**：`stock`（旧别名：`industry`）
- **主键**：`[ticker, classification]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_equity_ticker` |
| `industry_l1` | `str` | N | 一级行业名称/代码 |
| `industry_l2` | `str` | N | 二级行业名称/代码 |
| `industry_l3` | `str` | Y | 三级行业名称/代码 |
| `classification` | `str` | N | `isin(["citics", "sw", "ths", "tdx"])` |
| `effective_date` | `pa.Date` | N | 行业归属生效日 |

---

## 4. `cn_stock_index_weights`（指数 / 板块成分与权重）

- **路径**：`asset/cn_stock_index_weights/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[index_code, ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`weight`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `index_code` | `str` | N | `validate_index_ticker` |
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 权重截面日期 |
| `weight` | `float` | Y | `>= 0`（成分权重，百分比或小数按源约定统一） |
| `in_date` | `pa.Date` | Y | 纳入日期 |
| `out_date` | `pa.Date` | Y | 剔除日期 |
