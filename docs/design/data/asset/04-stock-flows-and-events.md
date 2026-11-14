# 04. 资金流、情绪筹码与 #29 股东资本事件拆表（17 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)
>
> 说明：包含 9 张资金流/情绪/筹码日频小表（原 `ext/` 合并至 `asset/`）以及 #29 拆分为 8 张独立股东/资本事件表。

---

## 第一部分：资金流 / 情绪 / 筹码（9 张）

### 1. `cn_stock_auction`（开盘 / 收盘集合竞价）

- **路径**：`asset/cn_stock_auction/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date, auction_session]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`
- **交叉比对数值列**：`price`, `pre_close`, `pct_chg`, `volume`, `amount`, `turnover_rate`, `volume_ratio`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `auction_session` | `str` | N | `isin(["open", "close"])` |
| `price` | `float` | N | `> 0`（集合竞价成交价） |
| `pre_close` | `float` | Y | `> 0`（昨收价） |
| `pct_chg` | `float` | Y | 竞价涨跌幅 |
| `volume` | `int` | N | `>= 0`（竞价成交量，股） |
| `amount` | `float` | N | `>= 0`（竞价成交额，元） |
| `turnover_rate` | `float` | Y | `>= 0`（竞价换手率） |
| `volume_ratio` | `float` | Y | `>= 0`（量比） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 2. `cn_stock_moneyflow`（个股资金流向）

- **路径**：`asset/cn_stock_moneyflow/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc`
- **交叉比对数值列**：`buy_elg_amount`, `sell_elg_amount`, `buy_lg_amount`, `sell_lg_amount`, `buy_md_amount`, `sell_md_amount`, `buy_sm_amount`, `sell_sm_amount`, `net_mf_amount`, `net_mf_pct`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `buy_elg_amount` | `float` | Y | `>= 0`（特大单买入额） |
| `sell_elg_amount` | `float` | Y | `>= 0`（特大单卖出额） |
| `buy_lg_amount` | `float` | Y | `>= 0`（大单买入额） |
| `sell_lg_amount` | `float` | Y | `>= 0`（大单卖出额） |
| `buy_md_amount` | `float` | Y | `>= 0`（中单买入额） |
| `sell_md_amount` | `float` | Y | `>= 0`（中单卖出额） |
| `buy_sm_amount` | `float` | Y | `>= 0`（小单买入额） |
| `sell_sm_amount` | `float` | Y | `>= 0`（小单卖出额） |
| `net_mf_amount` | `float` | Y | 净流入金额 |
| `net_mf_pct` | `float` | Y | 净流入占比 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 3. `cn_stock_hsgt_market_flow`（沪深港通南北向市场资金流水）

- **路径**：`asset/cn_stock_hsgt_market_flow/market_flow.parquet`（全量单文件）
- **family**：`stock` · **主键**：`[trade_date]`
- **数据源优先级**：`tushare` → `qdc`
- **交叉比对数值列**：`ggt_ss`, `ggt_sz`, `hgt`, `sgt`, `north_money`, `south_money`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `trade_date` | `pa.Date` | N | 交易日期 |
| `ggt_ss` | `float` | Y | 港股通（沪）资金流 |
| `ggt_sz` | `float` | Y | 港股通（深）资金流 |
| `hgt` | `float` | Y | 沪股通资金流 |
| `sgt` | `float` | Y | 深股通资金流 |
| `north_money` | `float` | Y | 北向资金合计 |
| `south_money` | `float` | Y | 南向资金合计 |
| `is_turnover_regime` | `bool` | N | `2024-08-19` 披露口径切换（由净买入额转为总成交额）标志 |

---

### 4. `cn_stock_hsgt_hold`（陆股通个股持股明细）

- **路径**：`asset/cn_stock_hsgt_hold/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc`
- **交叉比对数值列**：`vol`, `ratio`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `vol` | `int` | N | `>= 0`（北向持股股数） |
| `ratio` | `float` | Y | `>= 0`（占总股本或流通股比例） |
| `exchange` | `str` | N | `EXCHANGE_MAP` 键 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 5. `cn_stock_margin`（个股融资融券明细）

- **路径**：`asset/cn_stock_margin/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`rzye`, `rzmre`, `rzche`, `rqyl`, `rqmcl`, `rqchl`, `rqye`, `rzrqye`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `rzye` | `float` | Y | `>= 0`（融资余额） |
| `rzmre` | `float` | Y | `>= 0`（融资买入额） |
| `rzche` | `float` | Y | `>= 0`（融资偿还额） |
| `rqyl` | `float` | Y | `>= 0`（融券余量） |
| `rqmcl` | `float` | Y | `>= 0`（融券卖出量） |
| `rqchl` | `float` | Y | `>= 0`（融券偿还量） |
| `rqye` | `float` | Y | `>= 0`（融券余额） |
| `rzrqye` | `float` | Y | `>= 0`（融资融券余额合计） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 6. `cn_stock_lhb`（龙虎榜每日上榜个股）

- **路径**：`asset/cn_stock_lhb/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date, reason]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`close`, `pct_chg`, `turnover_rate`, `l_buy`, `l_sell`, `net_amount`, `net_rate`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `reason` | `str` | N | 上榜原因 |
| `close` | `float` | Y | `> 0`（收盘价） |
| `pct_chg` | `float` | Y | 涨跌幅 |
| `turnover_rate` | `float` | Y | `>= 0`（换手率） |
| `l_buy` | `float` | Y | `>= 0`（龙虎榜买入额） |
| `l_sell` | `float` | Y | `>= 0`（龙虎榜卖出额） |
| `net_amount` | `float` | Y | 龙虎榜净买入额 |
| `net_rate` | `float` | Y | 净买额占总成交比 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 7. `cn_stock_lhb_seats`（龙虎榜营业部 / 机构席位明细）

- **路径**：`asset/cn_stock_lhb_seats/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date, side, rank, seat_name]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`buy`, `sell`, `net_buy`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `side` | `str` | N | `isin(["buy", "sell"])` |
| `rank` | `int` | N | `>= 1`（席位排名） |
| `seat_name` | `str` | N | 营业部/专用席位名称 |
| `buy` | `float` | Y | `>= 0`（买入金额） |
| `sell` | `float` | Y | `>= 0`（卖出金额） |
| `net_buy` | `float` | Y | 净买入额 |
| `hm_name` | `str` | Y | 关联游资标签 |
| `is_institution` | `bool` | N | 是否机构专用/深沪股通专用席位 |

---

### 8. `cn_stock_limit_pool`（涨停 / 跌停 / 炸板池）

- **路径**：`asset/cn_stock_limit_pool/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date, pool_type]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`
- **交叉比对数值列**：`open_times`, `streak_days`, `seal_amount`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `pool_type` | `str` | N | `isin(["up", "down", "break"])` |
| `first_time` | `str` | Y | 首次封板时间 |
| `last_time` | `str` | Y | 最后封板时间 |
| `open_times` | `float` | Y | `>= 0`（炸板/开板次数，可空整型约定 `float`） |
| `streak_days` | `float` | Y | `>= 0`（连板天数，可空整型约定 `float`） |
| `seal_amount` | `float` | Y | `>= 0`（封单金额） |
| `theme` | `str` | Y | 涨停题材标签 |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

### 9. `cn_stock_chips_perf`（每日筹码分布与胜率）

- **路径**：`asset/cn_stock_chips_perf/{month}.parquet`（按自然月）
- **family**：`stock` · **主键**：`[ticker, trade_date]`
- **数据源优先级**：`tushare` → `qdc`
- **交叉比对数值列**：`his_low`, `his_high`, `cost_5pct`, `cost_15pct`, `cost_50pct`, `cost_85pct`, `cost_95pct`, `weight_avg`, `winner_rate`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `his_low` | `float` | Y | `> 0`（历史最低价） |
| `his_high` | `float` | Y | `> 0`（历史最高价） |
| `cost_5pct` | `float` | Y | `> 0`（5% 分位成本） |
| `cost_15pct` | `float` | Y | `> 0`（15% 分位成本） |
| `cost_50pct` | `float` | Y | `> 0`（50% 分位成本） |
| `cost_85pct` | `float` | Y | `> 0`（85% 分位成本） |
| `cost_95pct` | `float` | Y | `> 0`（95% 分位成本） |
| `weight_avg` | `float` | Y | `> 0`（加权平均成本） |
| `winner_rate` | `float` | Y | `>= 0`（胜率） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 第二部分：股东与资本事件 — #29 拆表（8 张）

### 10. `cn_stock_holder_num`（股东户数）

- **路径**：`asset/cn_stock_holder_num/holder_num.parquet`（全量单文件）
- **family**：`stock` · **主键**：`[ticker, end_date, ann_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`holder_num`, `holder_num_chg_pct`, `avg_hold_shares`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `end_date` | `pa.Date` | N | 统计截止日 |
| `holder_num` | `int` | N | `>= 1`（股东户数） |
| `holder_num_chg_pct` | `float` | Y | 较上期变化百分比 |
| `avg_hold_shares` | `float` | Y | `>= 0`（户均持股数） |

---

### 11. `cn_stock_top_holders`（前十大股东 / 前十大流通股东）

- **路径**：`asset/cn_stock_top_holders/{year}.parquet`（按年）
- **family**：`stock` · **主键**：`[ticker, end_date, holder_scope, rank, holder_name]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`hold_amount`, `hold_ratio`, `hold_change`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `end_date` | `pa.Date` | N | 报告期末日 |
| `holder_scope` | `str` | N | `isin(["top10", "free_top10"])` |
| `rank` | `int` | N | `>= 1`（名次） |
| `holder_name` | `str` | N | 股东名称 |
| `holder_type` | `str` | Y | 股东性质 |
| `hold_amount` | `float` | Y | `>= 0`（持股数量） |
| `hold_ratio` | `float` | Y | `>= 0`（持股比例） |
| `hold_change` | `float` | Y | 持股变动 |

---

### 12. `cn_stock_restricted_release`（限售股解禁）

- **路径**：`asset/cn_stock_restricted_release/{year}.parquet`（按年）
- **family**：`stock` · **主键**：`[ticker, float_date, holder_name, share_type]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`float_share`, `float_ratio`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `ann_date` | `pa.Date` | Y | 公告日期 |
| `float_date` | `pa.Date` | N | 解禁日期 |
| `holder_name` | `str` | N | 股东/批次名称 |
| `float_share` | `float` | N | `>= 0`（解禁股数） |
| `float_ratio` | `float` | Y | `>= 0`（占总股本比例） |
| `share_type` | `str` | N | 限售股份类型 |
| `is_announced` | `bool` | N | 是否已发布正式上市流通公告 |

---

### 13. `cn_stock_block_trade`（大宗交易）

- **路径**：`asset/cn_stock_block_trade/block_trade.parquet`（全量单文件）
- **family**：`stock` · **主键**：`[ticker, trade_date, seq]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`price`, `vol`, `amount`, `discount_rate`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `seq` | `int` | N | `>= 0`（同日同股笔序号） |
| `price` | `float` | N | `> 0`（成交价） |
| `vol` | `int` | N | `>= 0`（成交量，股） |
| `amount` | `float` | N | `>= 0`（成交额，元） |
| `discount_rate` | `float` | Y | 折/溢价率 |
| `buyer` | `str` | Y | 买方营业部 |
| `seller` | `str` | Y | 卖方营业部 |

---

### 14. `cn_stock_repurchase`（股份回购）

- **路径**：`asset/cn_stock_repurchase/repurchase.parquet`（全量单文件）
- **family**：`stock` · **主键**：`[ticker, ann_date, proc]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`vol`, `amount`, `high_limit`, `low_limit`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `end_date` | `pa.Date` | Y | 截止日期 |
| `proc` | `str` | N | 回购进度（预案/实施/完成等） |
| `exp_date` | `pa.Date` | Y | 过期/到期日 |
| `vol` | `float` | Y | `>= 0`（回购股数） |
| `amount` | `float` | Y | `>= 0`（回购金额） |
| `high_limit` | `float` | Y | `>= 0`（回购价格上限） |
| `low_limit` | `float` | Y | `>= 0`（回购价格下限） |

---

### 15. `cn_stock_holder_trade`（重要股东增减持）

- **路径**：`asset/cn_stock_holder_trade/{year}.parquet`（按年）
- **family**：`stock` · **主键**：`[ticker, ann_date, holder_name, in_de]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`change_vol`, `change_ratio`, `after_share`, `avg_price`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `holder_name` | `str` | N | 股东名称 |
| `holder_type` | `str` | Y | 股东身份（高管/个人/公司等） |
| `in_de` | `str` | N | `isin(["IN", "DE"])`（增持/减持） |
| `change_vol` | `float` | Y | `>= 0`（变动股数） |
| `change_ratio` | `float` | Y | `>= 0`（占总股本变动比） |
| `after_share` | `float` | Y | `>= 0`（变动后持股数） |
| `avg_price` | `float` | Y | `>= 0`（交易均价） |
| `begin_date` | `pa.Date` | Y | 增减持起始日 |
| `close_date` | `pa.Date` | Y | 增减持截止日 |

---

### 16. `cn_stock_pledge`（股权质押与解押明细）

- **路径**：`asset/cn_stock_pledge/{year}.parquet`（按年）
- **family**：`stock` · **主键**：`[ticker, ann_date, holder_name, start_date]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`pledge_amount`, `pledge_ratio`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `holder_name` | `str` | N | 出质股东名称 |
| `pledge_amount` | `float` | Y | `>= 0`（质押股数） |
| `start_date` | `pa.Date` | N | 质押开始日期 |
| `end_date` | `pa.Date` | Y | 质押截止/解押日期 |
| `is_release` | `bool` | N | 是否已解押 |
| `pledge_ratio` | `float` | Y | `>= 0`（质押比例） |
| `freeze_type` | `str` | Y | 质押/冻结类型 |

---

### 17. `cn_stock_research_visits`（机构调研记录）

- **路径**：`asset/cn_stock_research_visits/{year}.parquet`（按年）
- **family**：`stock` · **主键**：`[ticker, surv_date, org_name]`
- **数据源优先级**：`tushare` → `qdc`（无交叉比对数值列）

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `surv_date` | `pa.Date` | N | 调研日期 |
| `rece_place` | `str` | Y | 调研地点 |
| `rece_mode` | `str` | Y | 调研方式（现场/网络/电话等） |
| `org_name` | `str` | N | 参与机构名称 |
| `org_type` | `str` | Y | 机构类型 |
| `fund_visitors` | `str` | Y | 接待人员/调研摘要 |
