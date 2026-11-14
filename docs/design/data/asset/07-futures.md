# 07. 期货域（4 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)

---

## 1. `cn_futures_bars_daily`（期货单合约日线行情）

- **路径**：`asset/cn_futures_bars_daily/{date}.parquet`（按日）
- **family**：`futures`（旧别名：`bars_daily_futures`）
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `settle`, `volume`, `amount`, `open_interest`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_futures_ticker`（如 `IF2406.CFFEX`） |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `settle` | `float` | N | `> 0`（结算价） |
| `volume` | `int` | N | `>= 0`（成交手数） |
| `amount` | `float` | N | `>= 0`（成交额，元） |
| `open_interest` | `int` | N | `>= 0`（持仓量，手） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 2. `cn_futures_tick_l2`（期货五档 Level-2 快照，28 列）

- **路径**：`asset/cn_futures_tick_l2/{date}.parquet`（按日）
- **family**：`futures`（旧别名：`tick_futures_l2`）
- **主键**：`[ticker, trade_date, datetime]`
- **数据源优先级**：`qdc` → `amazingdata`
- **交叉比对数值列**：`last_price`, `volume`, `amount`, `open_interest`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_futures_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `datetime` | `pa.DateTime` | N | 快照时间戳（无时区北京时间） |
| `last_price` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `open_interest` | `int` | N | `>= 0` |
| `bid_price_1` .. `bid_price_5` | `float` (×5) | N | `>= 0`（买一到买五价） |
| `bid_volume_1` .. `bid_volume_5` | `int` (×5) | N | `>= 0`（买一到买五量） |
| `ask_price_1` .. `ask_price_5` | `float` (×5) | N | `>= 0`（卖一到卖五价） |
| `ask_volume_1` .. `ask_volume_5` | `int` (×5) | N | `>= 0`（卖一到卖五量） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 3. `cn_futures_contract_specs`（期货合约静态规格）

- **路径**：`asset/cn_futures_contract_specs/contract_specs.parquet`（全量单文件，独立目录不再共用 `cn_reference/`）
- **family**：`futures`（旧别名：`contract_specs`）
- **主键**：`[ticker]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`（无交叉比对数值列）

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_futures_ticker` |
| `symbol` | `str` | N | 品种代码（如 `IF`） |
| `exchange` | `str` | N | `EXCHANGE_MAP` 键 |
| `contract_multiplier` | `float` | N | `> 0`（合约乘数） |
| `price_tick` | `float` | N | `> 0`（最小变动价位） |
| `list_date` | `pa.Date` | N | 上市日 |
| `expire_date` | `pa.Date` | N | 最后交易/到期日 |
| `delivery_month` | `str` | N | 交割月份 |
| `price_limit_pct` | `float` | N | `> 0`（涨跌停板幅度） |
| `margin_rate` | `float` | Y | `> 0`（交易所保证金率） |
| `trading_hours` | `str` | N | 交易时段字符串 |
| `is_active` | `bool` | N | 当前是否活跃可交易 |

---

## 4. `cn_futures_continuous`（期货主力连续合约）

- **路径**：`asset/cn_futures_continuous/{symbol}.parquet`（按品种）
- **family**：`continuous`（旧别名：`continuous`）
- **主键**：`[trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`, `open_interest`, `settle`, `adjfactor`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `trade_date` | `pa.Date` | N | 交易日期 |
| `dominant_contract` | `str` | N | `validate_futures_ticker`（当日对应主力合约） |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `open_interest` | `int` | N | `>= 0` |
| `settle` | `float` | N | `> 0` |
| `adjfactor` | `float` | N | `> 0`（换月累积复权因子，不落盘 `close_adj`） |
