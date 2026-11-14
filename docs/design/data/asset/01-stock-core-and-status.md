# 01. 日历与 A 股股票核心及风控状态（7 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)

---

## 1. `cn_calendar`（交易日历）

- **路径**：`asset/cn_calendar/{exchange}.parquet`（按交易所：`sse`, `szse`, `bse`, `cffex`, `shfe`, `dce`, `czce`, `ine`, `gfex`）
- **family**：`reference`（旧别名：`calendar`）
- **主键**：`[trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush` → `amazingdata`（不参与多源数值交叉比对）

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `trade_date` | `pa.Date` | N | 交易日（naive 北京日期） |

---

## 2. `cn_stock_basic`（股票基础档案）

- **路径**：`asset/cn_stock_basic/stock_basic.parquet`（全量单文件）
- **family**：`stock`
- **主键**：`[ticker]`
- **数据源优先级**：`tushare` → `amazingdata` → `straightflush`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker`（含 `60/68.SH`, `00/30.SZ`, `43/83/87/92.BJ`） |
| `name` | `str` | N | 股票简称 |
| `exchange` | `str` | N | 交易所代码（`EXCHANGE_MAP` 键：`SH`/`SZ`/`BJ` 等） |
| `market` | `str` | N | 市场板块（主板/创业板/科创板/北交所） |
| `list_status` | `str` | N | 上市状态（`L` 上市 / `D` 退市 / `P` 暂停） |
| `list_date` | `pa.Date` | N | 上市日期 |
| `delist_date` | `pa.Date` | Y | 退市日期 |
| `is_hs` | `str` | Y | 沪深港通标的标志（`N`/`H`/`S`） |
| `old_code` | `str` | Y | 北交所旧三板/平移前代码映射 |

---

## 3. `cn_stock_bars_daily`（股票日线行情）

- **路径**：`asset/cn_stock_bars_daily/{date}.parquet`（按日）
- **family**：`stock`（旧别名：`bars_daily`）
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`（`field_override: adjfactor=tushare`）
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`, `adjfactor`, `turnover`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_equity_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `open` | `float` | N | `> 0`（未复权开盘价） |
| `high` | `float` | N | `> 0`（未复权最高价） |
| `low` | `float` | N | `> 0`（未复权最低价） |
| `close` | `float` | N | `> 0`（未复权收盘价） |
| `volume` | `int` | N | `>= 0`（成交股数） |
| `amount` | `float` | N | `>= 0`（成交额，元） |
| `adjfactor` | `float` | N | `> 0`（累计复权因子，读时衍生前/后复权） |
| `turnover` | `float` | Y | `>= 0`（换手率） |
| `suspend` | `bool` | N | 是否停牌 |
| `is_limit_up` | `bool` | N | 收盘是否涨停 |
| `is_limit_down` | `bool` | N | 收盘是否跌停 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 4. `cn_stock_bars_minute`（股票 1 分钟行情）

- **路径**：`asset/cn_stock_bars_minute/{date}.parquet`（按日）
- **family**：`stock`（旧别名：`bars_minute`）
- **主键**：`[ticker, trade_date, datetime]`
- **数据源优先级**：`qdc` → `tushare` → `amazingdata`
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_equity_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `datetime` | `pa.DateTime` | N | 分钟柱时间戳（无时区北京时间，无 `adjfactor`） |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 5. `cn_stock_fundamental`（股票日频估值与股本）

- **路径**：`asset/cn_stock_fundamental/{month}.parquet`（按自然月 `YYYY-MM`）
- **family**：`stock`（旧别名：`fundamental`）
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`（`field_override: pe_ttm=tushare`）
- **交叉比对数值列**：`total_market_cap`, `float_market_cap`, `pe_ttm`, `pe_lyr`, `pb`, `ps_ttm`, `total_shares`, `float_shares`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_equity_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `total_market_cap` | `float` | N | `>= 0`（总市值） |
| `float_market_cap` | `float` | N | `>= 0`（流通市值） |
| `pe_ttm` | `float` | Y | 滚动市盈率（亏损可空） |
| `pe_lyr` | `float` | Y | 静态市盈率 |
| `pb` | `float` | Y | 市净率 |
| `ps_ttm` | `float` | Y | 滚动市销率 |
| `total_shares` | `int` | N | `>= 0`（总股本） |
| `float_shares` | `int` | N | `>= 0`（流通股本） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 6. `cn_stock_daily_status`（股票日频风控状态集中表）

- **路径**：`asset/cn_stock_daily_status/{month}.parquet`（按自然月）
- **family**：`stock`
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`pre_close`, `up_limit`, `down_limit`, `limit_pct`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `pre_close` | `float` | Y | `> 0`（前收盘/除权参考价） |
| `up_limit` | `float` | Y | `> 0`（涨停价） |
| `down_limit` | `float` | Y | `> 0`（跌停价） |
| `limit_pct` | `float` | Y | `>= 0`（涨跌停幅度，如 0.10/0.20/0.30/0.05） |
| `is_st` | `bool` | N | 当日是否 ST / \*ST |
| `is_suspend` | `bool` | N | 当日是否停牌 |
| `is_limit_up` | `bool` | N | 收盘封涨停 |
| `is_limit_down` | `bool` | N | 收盘封跌停 |
| `touched_limit_up` | `bool` | N | 盘中是否曾触及涨停 |
| `touched_limit_down` | `bool` | N | 盘中是否曾触及跌停 |
| `is_xr` | `bool` | N | 当日是否除权除息 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 7. `cn_stock_corporate_actions`（公司行动 / 分红送转配股）

- **路径**：`asset/cn_stock_corporate_actions/corporate_actions.parquet`（全量单文件）
- **family**：`stock`（旧别名：`corporate_actions`）
- **主键**：`[ticker, ex_date, event_type]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`cash_dividend`, `stock_dividend`, `rights_price`, `rights_ratio`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_equity_ticker` |
| `ex_date` | `pa.Date` | N | 除权除息日 |
| `event_type` | `str` | N | `isin(["dividend", "split", "rights", "bonus"])` |
| `announce_date` | `pa.Date` | Y | 公告日 |
| `record_date` | `pa.Date` | Y | 股权登记日 |
| `detail` | `str` | N | 方案原文描述 |
| `cash_dividend` | `float` | Y | `>= 0`（每股派现，税前） |
| `stock_dividend` | `float` | Y | `>= 0`（每股送/转股比例） |
| `rights_price` | `float` | Y | `>= 0`（配股价） |
| `rights_ratio` | `float` | Y | `>= 0`（配股比例） |
