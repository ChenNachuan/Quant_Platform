# 06. 独立 ETF 域与公募基金域（10 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)
>
> 说明：ETF 独立成域（5 张，**暂不落 `cn_etf_pcf`**）；公募基金涵盖场外 `.OF` 与场内 LOF `50/16`（5 张）。

---

## 第一部分：独立 ETF 域（5 张）

### 1. `cn_etf_basic`（ETF 基础档案）

- **路径**：`asset/cn_etf_basic/etf_basic.parquet`（全量单文件）
- **family**：`etf` · **主键**：`[ticker]`
- **数据源优先级**：`tushare` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`m_fee`, `c_fee`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_etf_ticker`（`51/52/53/55/56/58.SH`, `15.SZ`） |
| `name` | `str` | N | ETF 简称 |
| `exchange` | `str` | N | `EXCHANGE_MAP` 键 |
| `management` | `str` | Y | 基金管理人 |
| `index_code` | `str` | Y | 跟踪指数代码 |
| `index_name` | `str` | Y | 跟踪指数名称 |
| `etf_type` | `str` | Y | ETF 类型（股票/债券/商品/跨境/货币） |
| `setup_date` | `pa.Date` | Y | 成立日期 |
| `list_date` | `pa.Date` | Y | 上市日期 |
| `delist_date` | `pa.Date` | Y | 退市日期 |
| `m_fee` | `float` | Y | `>= 0`（管理费率） |
| `c_fee` | `float` | Y | `>= 0`（托管费率） |

---

### 2. `cn_etf_bars_daily`（ETF 独立日线行情）

- **路径**：`asset/cn_etf_bars_daily/{date}.parquet`（按日）
- **family**：`etf` · **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`（`field_override: adjfactor=tushare`）
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`, `adjfactor`, `turnover`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_etf_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0`（成交份数） |
| `amount` | `float` | N | `>= 0`（成交额，元） |
| `adjfactor` | `float` | N | `> 0`（累计复权因子） |
| `turnover` | `float` | Y | `>= 0`（换手率） |
| `suspend` | `bool` | N | 是否停牌 |
| `is_limit_up` | `bool` | N | 是否涨停 |
| `is_limit_down` | `bool` | N | 是否跌停 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 3. `cn_etf_bars_minute`（ETF 1 分钟行情）

- **路径**：`asset/cn_etf_bars_minute/{date}.parquet`（按日）
- **family**：`etf` · **主键**：`[ticker, trade_date, datetime]`
- **数据源优先级**：`qdc` → `tushare` → `amazingdata`
- **交叉比对数值列**：`open`, `high`, `low`, `close`, `volume`, `amount`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_etf_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `datetime` | `pa.DateTime` | N | 分钟柱时间戳（无 `adjfactor`） |
| `open` | `float` | N | `> 0` |
| `high` | `float` | N | `> 0` |
| `low` | `float` | N | `> 0` |
| `close` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 4. `cn_etf_fundamental`（ETF 净值、IOPV 与份额规模）

- **路径**：`asset/cn_etf_fundamental/{month}.parquet`（按自然月）
- **family**：`etf` · **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`unit_nav`, `accum_nav`, `nav_adj_factor`, `iopv`, `total_shares`, `total_nav`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_etf_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `unit_nav` | `float` | Y | `> 0`（单位净值） |
| `accum_nav` | `float` | Y | `> 0`（累计单位净值） |
| `nav_adj_factor` | `float` | Y | `> 0`（净值复权因子） |
| `iopv` | `float` | Y | `> 0`（参考净值 IOPV） |
| `total_shares` | `float` | Y | `>= 0`（场内流通总份额） |
| `total_nav` | `float` | Y | `>= 0`（基金资产净值规模） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 5. `cn_etf_tick`（ETF 逐笔成交）

- **路径**：`asset/cn_etf_tick/{date}.parquet`（按日）
- **family**：`etf` · **主键**：`[ticker, trade_date, seq]`
- **数据源优先级**：`qdc` → `amazingdata`
- **交叉比对数值列**：`price`, `volume`, `amount`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_etf_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `datetime` | `pa.DateTime` | N | 逐笔时间戳 |
| `seq` | `int` | N | `>= 0`（成交序号） |
| `price` | `float` | N | `> 0` |
| `volume` | `int` | N | `>= 0` |
| `amount` | `float` | N | `>= 0` |
| `bs_flag` | `str` | N | `isin(["B", "S", "N", "A"])` |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 第二部分：场外与 LOF 公募基金（5 张）

### 6. `cn_fund_basic`（公募基金档案）

- **路径**：`asset/cn_fund_basic/fund_basic.parquet`（全量单文件）
- **family**：`fund` · **主键**：`[ticker]`
- **数据源优先级**：`tushare` → `straightflush`
- **交叉比对数值列**：`m_fee`, `c_fee`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_fund_ticker`（`.OF` 或 `50.SH`/`16.SZ`） |
| `name` | `str` | N | 基金简称 |
| `market` | `str` | N | `isin(["O", "E"])`（`O`=场外，`E`=场内） |
| `management` | `str` | Y | 基金管理人 |
| `custodian` | `str` | Y | 基金托管人 |
| `fund_type` | `str` | Y | 基金类型（契约型开放式/封闭式等） |
| `invest_type` | `str` | Y | 投资风格（股票/混合/债券/货币/QDII/FOF） |
| `found_date` | `pa.Date` | Y | 成立日期 |
| `list_date` | `pa.Date` | Y | 上市日期（场内适用） |
| `delist_date` | `pa.Date` | Y | 到期/摘牌日期 |
| `m_fee` | `float` | Y | `>= 0`（管理费率） |
| `c_fee` | `float` | Y | `>= 0`（托管费率） |
| `benchmark` | `str` | Y | 业绩比较基准 |

---

### 7. `cn_fund_managers`（基金经理任职履历）

- **路径**：`asset/cn_fund_managers/managers.parquet`（全量单文件）
- **family**：`fund` · **主键**：`[ticker, name, begin_date]`
- **数据源优先级**：`tushare` → `straightflush`（无交叉比对数值列）

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_fund_ticker` |
| `ann_date` | `pa.Date` | Y | 公告日期 |
| `name` | `str` | N | 基金经理姓名 |
| `gender` | `str` | Y | 性别 |
| `edu` | `str` | Y | 学历 |
| `begin_date` | `pa.Date` | N | 任职起始日期 |
| `end_date` | `pa.Date` | Y | 离任日期 |

---

### 8. `cn_fund_nav`（公募基金每日净值与份额规模）

- **路径**：`asset/cn_fund_nav/{month}.parquet`（按自然月）
- **family**：`fund` · **主键**：`[ticker, nav_date]`
- **数据源优先级**：`tushare` → `straightflush`
- **交叉比对数值列**：`unit_nav`, `accum_nav`, `adj_nav`, `nav_adj_factor`, `total_shares`, `total_netasset`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_fund_ticker` |
| `nav_date` | `pa.Date` | N | 净值日期 |
| `ann_date` | `pa.Date` | Y | 公告日期 |
| `unit_nav` | `float` | N | `> 0`（单位净值） |
| `accum_nav` | `float` | Y | `> 0`（累计单位净值） |
| `adj_nav` | `float` | Y | `> 0`（复权单位净值） |
| `nav_adj_factor` | `float` | Y | `> 0`（净值复权因子） |
| `total_shares` | `float` | Y | `>= 0`（基金总份额） |
| `total_netasset` | `float` | Y | `>= 0`（基金资产净值） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 9. `cn_fund_dividend`（公募基金分红送配）

- **路径**：`asset/cn_fund_dividend/dividend.parquet`（全量单文件）
- **family**：`fund` · **主键**：`[ticker, ex_date, ann_date]`
- **数据源优先级**：`tushare` → `straightflush`
- **交叉比对数值列**：`div_cash`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_fund_ticker` |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `record_date` | `pa.Date` | Y | 权益登记日 |
| `ex_date` | `pa.Date` | N | 除息日 |
| `pay_date` | `pa.Date` | Y | 现金红利发放日 |
| `div_cash` | `float` | Y | `>= 0`（每份派现金，元） |
| `div_proc` | `str` | Y | 分红进度/方式 |

---

### 10. `cn_fund_portfolio`（公募基金季报持仓明细）

- **路径**：`asset/cn_fund_portfolio/{year}.parquet`（按年）
- **family**：`fund` · **主键**：`[ticker, end_date, symbol]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`
- **交叉比对数值列**：`mkv`, `amount`, `stk_mkv_ratio`, `stk_float_ratio`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_fund_ticker` |
| `ann_date` | `pa.Date` | N | 季报/年报公告日期 |
| `end_date` | `pa.Date` | N | 报告期末日 |
| `symbol` | `str` | N | 持仓标的代码（股票/债券/基金代码） |
| `asset_class` | `str` | N | `isin(["stock", "bond", "fund", "other"])` |
| `mkv` | `float` | Y | `>= 0`（持仓市值） |
| `amount` | `float` | Y | `>= 0`（持有数量） |
| `stk_mkv_ratio` | `float` | Y | `>= 0`（占基金净值比例） |
| `stk_float_ratio` | `float` | Y | `>= 0`（占标的流通股比例） |
| `rank` | `float` | Y | `>= 1`（持仓排名，可空整型约定 `float`） |
