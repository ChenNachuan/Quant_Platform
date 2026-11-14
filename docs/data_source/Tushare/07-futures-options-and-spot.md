# 07. 期货、现货黄金与期权

[← 返回目录](README.md) · 对应官网 `doc_id=134 / 283 / 157`

---

## 1. 期货数据（22 API + Tick 交付，`doc_id=134`）

覆盖六大期货交易所后缀：`.SHF`（上期所）、`.DCE`（大商所）、`.ZCE`（郑商所）、`.CFX`（中金所）、`.INE`（上海国际能源）、`.GFE`（广期所）。

### 1.1 基础、日历、行情与复权

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分/权限 & 上限 |
|---|---|---|---|---|---|
| `135` | 合约信息 | `fut_basic` | `exchange`, `fut_type`, `fut_code`, `list_date` | 16 列：`ts_code`, `symbol`, `exchange`, `name`, `fut_code`, `multiplier`, `trade_unit`, `per_unit`, `quote_unit` 等 | 2,000 分；10,000 行/次 |
| `467` | 期货交易日历 | `fut_trade_cal` | `exchange`, `start_date`, `end_date`, `is_open` | 4 列：`exchange`, `cal_date`, `is_open`, `pretrade_date` | 2,000 分（1996 年起） |
| `138` | 期货日线行情 | `fut_daily` | `trade_date`, `ts_code`, `exchange`, `start_date`, `end_date` | 16 列：`pre_close`, `pre_settle`, `open`, `high`, `low`, `close`, `settle`, `change1`, `oi`, `oi_chg`, `vol`, `amount` 等 | 2,000 分；2,000 行/次 |
| `492` | 期货复权行情 | `fut_daily_adj` | `trade_date`, `ts_code`, `exchange`, `start_date`, `end_date` | 17 列（复权 OHLC + 结算价等） | 5,000 分；3,000 行/次 |
| `499` | 期货复权因子 | `fut_adjfactor` | `trade_date`, `ts_code`, `exchange`, `start_date`, `end_date` | 4 列：`ts_code`, `exchange`, `trade_date`, `adj_factor` | 5,000 分；3,000 行/次 |
| `337` | 期货周/月线（每日更新） | `fut_weekly_monthly` | `ts_code`, `trade_date`, `start_date`, `end_date`, `freq`, `exchange` | 18 列（含 `settle`） | 基础积分；6,000 行/次 |
| `313` | 历史分钟行情 | `ft_mins` | `ts_code`, `freq`（`1..60min`）, `start_date`, `end_date` | 8 列（OHLCVA + 持仓 `oi`） | **独立权限**（“期货历史分钟”，2,000/年）；8,000 行/次（2010 起） |
| `340` | 实时分钟行情 | `rt_fut_min` | `ts_code`, `freq` | 实时日/夜盘分钟行情 | **独立权限**（“期货实时分钟”，1,000/月）；500 次/分，支持 WebSocket |
| `314` | 历史 Tick 行情 | *无 API* | 仅网盘 CSV 交付（一档盘口 `BidPrice1`/`AskPrice1` 等 17 列） | `InstrumentID`, `TradingDay`, `UpdateTime`, `LastPrice`, `Volume`, `OpenInterest` 等 | **独立定制服务**（近 10 年，按日增量） |

### 1.2 主力映射、持仓、仓单、库存与交易所交易参数

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分 & 上限 |
|---|---|---|---|---|---|
| `189` | 期货主力与连续合约 | `fut_mapping` | `ts_code`, `trade_date`, `start_date`, `end_date` | 3 列：`ts_code`（连续代码如 `CU.SHF`）, `trade_date`, `mapping_ts_code` | 2,000 分；2,000 行/次 |
| `139` | 每日持仓排名 | `fut_holding` | `trade_date`, `symbol`, `start_date`, `end_date`, `exchange` | 10 列：`broker`, `vol`, `vol_chg`, `long_hld`, `long_chg`, `short_hld`, `short_chg` | 2,000 分；2,000 行/次 |
| `140` | 仓单日报 | `fut_wsr` | `trade_date`, `symbol`, `start_date`, `end_date`, `exchange` | 18 列：`warehouse`, `wh_id`, `pre_vol`, `vol`, `vol_chg`, `area`, `year` 等 | 2,000 分；1,000 行/次 |
| `141` | 每日结算参数 | `fut_settle` | `trade_date`, `ts_code`, `start_date`, `end_date`, `exchange` | 12 列：`settle`, `trading_fee_rate`, `delivery_fee`, `long_margin_rate`, `short_margin_rate` 等 | 2,000 分；1,600 行/次 |
| `468` | 南华期货指数日线 | `fut_index_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列（标准指数 OHLCV） | 2,000 分起 |
| `216` | 期货主要品种交易周报 | `fut_weekly_detail` | `week`, `prd`, `start_week`, `end_week`, `exchange` | 17 列：`vol`, `vol_yoy`, `amount`, `cumvol` 等 | 600 分起（5,000+ 无限制）；4,000 行/次 |
| `368` | 期货合约涨跌停价格 | `ft_limit` | `ts_code`, `trade_date`, `start_date`, `end_date`, `cont`, `exchange` | 8 列：`up_limit`, `down_limit`, `m_ratio`, `cont`, `exchange` | 5,000 分；4,000 行/次 |
| `495` | 交易所库存周报 | `fut_inv_weekly` | `trade_date`, `ts_code`, `start_date`, `end_date` | 17 列：`warehouse`, `grade`, `pre_total`, `pre_futures`, `cur_total` 等 | 5,000 分；3,000 行/次 |
| `496` | 标准仓单到期数量 | `fut_rcpt_mat` | `trade_date`, `ts_code`, `start_date`, `end_date` | 8 列：`fut_code`, `unit`, `cur_month`, `next_month`, `next_two_month` | 5,000 分；3,000 行/次 |
| `497` | 郑商所交易参数 | `fut_trade_param` | `ts_code`, `trade_date`, `start_date`, `end_date` | 12 列：`trade_unit`, `tick_size`, `price_limit`, `position_limit` 等 | 5,000 分；3,000 行/次 |
| `500` | 中金所交易参数 | `fut_trade_param_cfx` | `ts_code`, `trade_date`, `start_date`, `end_date` | 14 列：`trade_type`, `listing_date`, `upper_limit`, `lower_limit`, `end_trade_date` 等 | 5,000 分；3,000 行/次 |
| `501` | 广期所交易参数 | `fut_trade_param_gfe` | `ts_code`, `trade_date`, `start_date`, `end_date` | 17 列：`spec_buy_rate`, `hedge_buy_rate`, `rise_limit_rate` 等 | 5,000 分；3,000 行/次 |
| `502` | 大商所交易参数 | `fut_trade_param_dce` | `ts_code`, `trade_date`, `start_date`, `end_date` | 18 列（投机/套保保证金及涨跌停参数） | 5,000 分；3,000 行/次 |

---

## 2. 现货黄金数据（2 项，`doc_id=283`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分 & 上限 |
|---|---|---|---|---|---|
| `284` | 上海黄金基础信息 | `sge_basic` | `ts_code` | 14 列：`ts_code`, `ts_name`, `trade_type`, `t_unit`, `p_unit`, `min_change`, `price_limit`, `min_vol`, `max_vol` | 5,000 分；100 行/次（<20 个品种） |
| `285` | 上海黄金现货日行情 | `sge_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 14 列：`close`, `open`, `high`, `low`, `price_avg`, `change`, `pct_change`, `vol` 等 | 2,000 分；2,000 行/次 |

---

## 3. 期权数据（3 项，`doc_id=157`）

覆盖 `.ZCE`、`.SHF`、`.DCE`、`.SSE`（上交所）、`.SZSE`（深交所）、`.CFFEX`（中金所）：

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分/权限 & 上限 |
|---|---|---|---|---|---|
| `158` | 期权合约信息 | `opt_basic` | `ts_code`, `exchange`, `list_date`, `opt_code`, `call_put` | 20 列：`ts_code`, `symbol`, `exchange`, `name`, `per_unit`, `opt_code`, `opt_type`, `call_put`, `exercise_type`, `exercise_price`, `list_date`, `delist_date` 等 | 5,000 分（权限表 2,000 起） |
| `159` | 期权日线行情 | `opt_daily` | `ts_code`, `trade_date`, `start_date`, `end_date`, `exchange` | 13 列：`ts_code`, `trade_date`, `exchange`, `pre_settle`, `pre_close`, `open`, `high`, `low`, `close`, `settle`, `vol`, `amount`, `oi` | 2,000 分起（权限表 5,000 起）；15,000 行/次 |
| `341` | 期权分钟行情 | `opt_mins` | `ts_code`, `freq`（`1..60min`）, `start_date`, `end_date` | 8 列（含股指期权与商品期权） | **独立权限**（“期权历史分钟”，2,000/年）；8,000 行/次（2010 起） |

> [!NOTE]
> 在本平台 [`config/source_priority.yaml`](../../../config/source_priority.yaml) 中，`option_basic` 当前固定由 `amazingdata` 作为唯一源（`priority: [amazingdata]`），Tushare 可作为期权日线/跨期权历史复核参考。
