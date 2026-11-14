# 08. 债券可转债、外汇、港股与美股

[← 返回目录](README.md) · 对应官网 `doc_id=184 / 177 / 519 / 190 / 251`

---

## 1. 债券专题与全球财经事件（17 项，`doc_id=184`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分/权限 & 上限 |
|---|---|---|---|---|---|
| `185` | 可转债基础信息 | `cb_basic` | `ts_code`, `list_date`, `exchange` | 38 列：`bond_full_name`, `cb_code`, `stk_code`, `maturity`, `par`, `issue_price`, `conv_start_date`, `conv_price` 等 | 2,000 分起（5,000+ 高频）；2,000 行/次 |
| `186` | 可转债发行 | `cb_issue` | `ts_code`, `ann_date`, `start_date`, `end_date` | 35 列：`plan_issue_size`, `issue_size`, `issue_price`, `issue_type`, `onl_code` 等 | 2,000 分起；2,000 行/次 |
| `269` | 可转债赎回信息 | `cb_call` | `ts_code`, `ann_date`, `start_date`, `end_date` | 11 列：`call_type`, `is_call`, `call_date`, `call_price`, `call_price_tax`, `payment_date` | 5,000 分；2,000 行/次 |
| `305` | 可转债票面利率 | `cb_rate` | `ts_code` | 5 列：`ts_code`, `rate_freq`, `rate_start_date`, `rate_end_date`, `coupon_rate` | 5,000 分；2,000 行/次 |
| `187` | 可转债日线行情 | `cb_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 15 列：`pre_close`, OHLC, `change`, `pct_chg`, `vol`, `amount`, `bond_value`, `bond_over_rate`, `cb_value`, `cb_over_rate` | 2,000 分起；2,000 行/次 |
| `459` | 可转债十大持有人 | `top10_cb_holders` | `ts_code`, `period`, `start_date`, `end_date` | 6 列：`ts_code`, `end_date`, `holder_rank`, `holder_name`, `hold_amount`, `hold_ratio` | 5,000 分；3,000 行/次 |
| `392` | 可转债技术面因子（专业版） | `cb_factor_pro` | `ts_code`, `start_date`, `end_date`, `trade_date` | 89 列：行情 + 静态技术因子 | 8,000+ 分（500 次/分）；10,000 行/次 |
| `246` | 可转债转股价变动 | `cb_price_chg` | `ts_code` | 7 列：`publish_date`, `change_date`, `convert_price_initial`, `convertprice_bef`, `convertprice_aft` | **独立权限**（“可转债价格变动”，500/年）；2,000 行/次 |
| `247` | 可转债转股结果 | `cb_share` | `ts_code`, `ann_date`, `start_date`, `end_date` | 15 列：`convert_price`, `convert_val`, `convert_vol`, `convert_ratio` 等 | 2,000 分起；2,000 行/次 |
| `458` | 可转债债券评级 | `cb_rating` | `ts_code` | 8 列：`ann_date`, `rating_date`, `rating_com_name`, `rating`, `rating_outlook` | 2,000 分；3,000 行/次 |
| `256` | 债券回购日行情 | `repo_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 12 列：`repo_maturity`, `pre_close`, OHLC, `weight`, `weight_r` 等 | 2,000 分；2,000 行/次 |
| `322` | 柜台流通式债券报价 | `bc_otcqt` | `trade_date`, `start_date`, `end_date`, `ts_code`, `bank` | 13 列：`qt_time`, `bank`, `remain_maturity`, `bond_type`, `coupon_rate`, `buy_price` 等 | 500 试用 / 2,000+ 正式；2,000 行/次 |
| `323` | 柜台债券最优报价 | `bc_bestotcqt` | `trade_date`, `start_date`, `end_date`, `ts_code` | 11 列：`best_buy_bank`, `best_buy_yield`, `best_buy_price`, `best_sell_*` | 500 试用 / 2,000+ 正式；2,000 行/次 |
| `271` | 债券大宗交易 | `bond_blk` | `ts_code`, `trade_date`, `start_date`, `end_date` | 6 列：`trade_date`, `ts_code`, `name`, `price`, `vol`, `amount` | 5,000 分；1,000 行/次 |
| `272` | 债券大宗交易明细 | `bond_blk_detail` | `ts_code`, `trade_date`, `start_date`, `end_date` | 8 列（含买卖营业部 `buy_dp`, `sell_dp`） | 5,000 分；1,000 行/次 |
| `201` | 国债收益率曲线 | `yc_cb` | `ts_code`, `curve_type`, `trade_date`, `start_date`, `end_date`, `curve_term` | 6 列：`trade_date`, `ts_code`, `curve_name`, `curve_type`, `curve_term`, `yield` | **独立权限**（联系管理员）；2,000 行/次 |
| `233` | 全球财经事件 | `eco_cal` | `date`, `start_date`, `end_date`, `currency`, `country`, `event` | 8 列：`date`, `time`, `currency`, `country`, `event`, `value`, `pre_value`, `fore_value` | 2,000 分；**100 行/次** |

---

## 2. 外汇与官方储备资产（7 项，`doc_id=177 / 519`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分 & 上限 |
|---|---|---|---|---|---|
| `517` | 人民币汇率中间价 | `fx_midrate` | `trade_date`, `ts_code`, `start_date`, `end_date` | 3 列：`trade_date`, `ts_code`, `mid_rate` | 2,000 分（5,000+ 高频）；3,000 行/次 |
| `178` | 外汇基础信息（海外 FXCM） | `fx_obasic` | `exchange`, `classify`（`FX`/`INDEX`/`COMMODITY`/`METAL`/`BUND`/`CRYPTO`/`FX_BASKET`）, `ts_code` | 12 列：`ts_code`, `name`, `classify`, `min_unit`, `pip`, `pip_cost`, `trading_hours` 等 | 2,000 分；一次全量 |
| `179` | 外汇日线行情 | `fx_daily` | `ts_code`, `trade_date`, `start_date`, `end_date`, `exchange` | 12 列：`bid_open/close/high/low`, `ask_open/close/high/low` 等 | 2,000 分起；1,000 行/次 |
| `520` | 官方外汇储备 | `fx_reserves` | `month`, `start_date`, `end_date` | 5 列：`month`, `ann_date`, `fc_res_usd`, `fc_res_sdr`, `usd_sdr` | 5,000 分；3,000 行/次 |
| `521` | 官方黄金储备 | `gold_reserves` | `month`, `start_date`, `end_date` | 6 列：`month`, `ann_date`, `gold_usd`, `gold_sdr`, `usd_sdr`, `gold_oz` | 5,000 分；3,000 行/次 |
| `522` | 官方特别提款权 | `sdr_reserves` | `month`, `start_date`, `end_date` | 5 列：`month`, `ann_date`, `sdr_usd`, `sdr_sdr`, `usd_sdr` | 5,000 分；3,000 行/次 |

---

## 3. 港股数据（11 项，`doc_id=190`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分 / 独立权限 |
|---|---|---|---|---|---|
| `191` | 港股基础信息 | `hk_basic` | `ts_code`, `list_status` | 12 列：`ts_code`, `name`, `enname`, `market`, `list_date`, `delist_date`, `trade_unit` 等 | 2,000 分 |
| `250` | 港股交易日历 | `hk_tradecal` | `start_date`, `end_date`, `is_open` | 3 列：`cal_date`, `is_open`, `pretrade_date` | 2,000 分；2,000 行/次 |
| `192` | 港股日线行情 | `hk_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列（未复权 OHLCV + `amount`） | **独立权限**（“港股历史日线”）；5,000 行/次 |
| `339` | 港股复权行情 | `hk_daily_adj` | `ts_code`, `trade_date`, `start_date`, `end_date` | 18 列（含复权价） | **独立权限**（“港股历史日线”）；6,000 行/次 |
| `401` | 港股复权因子 | `hk_adjfactor` | `ts_code`, `trade_date`, `start_date`, `end_date` | 4 列：`ts_code`, `trade_date`, `cum_adjfactor`, `close_price` | **独立权限**（“港股历史日线”）；6,000 行/次 |
| `304` | 港股分钟行情 | `hk_mins` | `ts_code`, `freq`, `start_date`, `end_date` | 8 列（`1/5/15/30/60min`） | **独立权限**（“港股历史分钟”）；8,000 行/次 |
| `383` | 港股实时日线 | `rt_hk_k` | `ts_code` | 8 列：`ts_code`, `pre_close`, `close`, `high`, `open`, `low`, `vol`, `amount` | **独立权限**（“港股实时日线”）；5,000 行/次 |
| `389`/`390`/`391` | 港股利润表/资产负债/现金流 | `hk_income`, `hk_balancesheet`, `hk_cashflow` | `ts_code`, `period`, `ind_name`, `start_date`, `end_date` | 5 列 EAV 结构：`ts_code`, `end_date`, `name`, `ind_name`, `ind_value` | **独立权限**（“港股财报”，500/年） |
| `388` | 港股财务指标 | `hk_fina_indicator` | `ts_code`, `period`, `report_type`, `start_date`, `end_date` | 87 列：`basic_eps`, `bps`, `roe`, `roa` 等宽表 | **独立权限**（“港股财报”） |

---

## 4. 美股数据（11 项，`doc_id=251`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分 / 独立权限 |
|---|---|---|---|---|---|
| `252` | 美股基础信息 | `us_basic` | `ts_code`, `classify`, `list_stauts`, `exchange`, `offset`, `limit` | 8 列：`ts_code`, `name`, `enname`, `classify`, `list_status`, `list_date`, `delist_date`, `exchange` | 120 试用 / 5,000 正式；6,000 行/次 |
| `253` | 美股交易日历 | `us_tradecal` | `start_date`, `end_date`, `is_open` | 3 列：`cal_date`, `is_open`, `pretrade_date` | 5,000 正式；6,000 行/次 |
| `254` | 美股日线行情 | `us_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 16 列（含估值与换手） | **独立权限**（“美股历史日线”，2,000/年）；6,000 行/次 |
| `338` | 美股复权行情 | `us_daily_adj` | `ts_code`, `trade_date`, `start_date`, `end_date`, `exchange`, `offset`, `limit` | 19 列（复权 OHLC + 换手/估值） | **独立权限**（“美股历史日线”）；8,000 行/次 |
| `402` | 美股复权因子 | `us_adjfactor` | `ts_code`, `trade_date`, `start_date`, `end_date` | 5 列：`ts_code`, `trade_date`, `exchange`, `cum_adjfactor`, `close_price` | **独立权限**（“美股历史日线”）；15,000 行/次 |
| `504` | 美股 ETF 列表 | `us_etf_basic` | `ts_code`, `list_date`, `list_status`, `exchange`, `mgr_name` | 27 列：`ts_code`, `name`, `exchange`, `type`, `setup_country`, `mgr_name` 等 | 5,000 分；6,000 行/次 |
| `506` | 美股 ETF 日行情 | `us_etf_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 14 列（OHLC + 涨跌/成交） | **独立权限**（“美股历史日线”）；6,000 行/次 |
| `394`/`395`/`396` | 美股利润表/资产负债/现金流 | `us_income`, `us_balancesheet`, `us_cashflow` | `ts_code`, `period`, `ind_name`, `report_type`, `start_date`, `end_date` | 7 列 EAV 结构：`ts_code`, `end_date`, `ind_type`, `name`, `ind_name`, `ind_value`, `report_type` | **独立权限**（“美股财报”，500/年） |
| `393` | 美股财务指标 | `us_fina_indicator` | `ts_code`, `period`, `report_type`, `start_date`, `end_date` | 69 列：`basic_eps`, `currency`, `accounting_standards` 等宽表 | **独立权限**（“美股财报”） |
