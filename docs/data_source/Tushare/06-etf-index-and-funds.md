# 06. ETF、指数行业与公募基金

[← 返回目录](README.md) · 对应官网 `doc_id=384 / 93 / 18`

---

## 1. ETF 专题（15 项，`doc_id=384`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分/权限 & 上限 |
|---|---|---|---|---|---|
| `385` | ETF 基本信息 | `etf_basic` | `ts_code`, `index_code`, `list_date`, `list_status`, `exchange`, `mgr` | 14 列：`ts_code`, `csname`, `extname`, `cname`, `index_code`, `index_name`, `setup_date`, `list_date` 等 | 5,000 分（8,000+ 500 次/分）；5,000 行/次 |
| `386` | ETF 跟踪指数 | `etf_index` | `ts_code`, `pub_date`, `base_date` | 8 列：`ts_code`, `indx_name`, `pub_party_name`, `base_date`, `bp`, `adj_circle` | 8,000 分；5,000 行/次 |
| `127` | ETF 日线行情 | `fund_daily` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列：`ts_code`, `trade_date`, `open`, `high`, `low`, `close`, `pre_close`, `change`, `pct_chg`, `vol`, `amount` | 5,000 分（8,000+ 高频）；5,000 行/次 |
| `387` | ETF 历史分钟 | `etf_mins` | `ts_code`, `freq`（`1..60min`）, `start_date`, `end_date` | 8 列：`ts_code`, `trade_time`, `open`, `close`, `high`, `low`, `vol`, `amount` | **独立权限**（“ETF历史分钟”）；8,000 行/次（>10 年） |
| `400` | ETF 实时日线 | `rt_etf_k` | `ts_code`, `topic` | 13 列：`ts_code`, `name`, `pre_close`, OHLC, `vol`, `amount`, `num` 等 | **独立权限**（“ETF日线RT”，200/月） |
| `416` | ETF 实时分钟 | `rt_etf_min` | `freq`（`1..60MIN`）, `ts_code`（支持多代码） | 实时分钟 OHLCVA | **独立权限**（“ETF分钟RT”）；1,000 行/次 |
| `470` | ETF 实时分钟-日累计 | `rt_etf_min_daily` | `freq`, `ts_code`（仅单代码） | 当日开盘起全部历史分钟 | **独立权限**（“ETF分钟RT”）；1,000 行/次 |
| `493` | ETF 当日集合竞价 | `etf_auction` | `ts_code`, `trade_date`, `start_date`, `end_date` | 9 列：`vol`, `price`, `amount`, `pre_close`, `turnover_rate`, `volume_ratio`, `float_share` | **独立权限**（“集合竞价成交”）；3,000 行/次 |
| `199` | ETF/基金复权因子 | `fund_adj` | `ts_code`, `trade_date`, `start_date`, `end_date`, `offset`, `limit` | 3 列：`ts_code`, `trade_date`, `adj_factor` | 2,000 分起（5,000+ 高频）；2,000 行/次 |
| `491` | ETF 每日涨跌停价格 | `etf_limit` | `ts_code`, `trade_date`, `start_date`, `end_date` | 7 列：`trade_date`, `ts_code`, `pre_close`, `up_limit`, `down_limit`, `asset_type`, `exchange` | 2,000 分；3,000 行/次 |
| `359` | ETF 技术面因子（专业版） | `fund_factor_pro` | `ts_code`, `start_date`, `end_date`, `trade_date` | 90 列：行情 + 全套静态技术指标 | 5,000 分（30 次/分）；8,000+（500 次/分）；8,000 行/次 |
| `408` | ETF 份额规模 | `etf_share_size` | `ts_code`, `trade_date`, `start_date`, `end_date`, `exchange` | 8 列：`trade_date`, `ts_code`, `etf_name`, `total_share`, `total_size`, `nav`, `close`, `exchange` | 8,000 分；5,000 行/次 |
| `471` | 沪市每日篮子组合（PCF） | `etf_sh_cons` | `ts_code`, `trade_date`, `con_code`, `start_date`, `end_date` | 10 列：`con_code`, `con_name`, `qty`, `sub_flag`, `cpr`, `rdr`, `sca`, `exchange` | 8,000 分；3,000 行/次 |
| `472` | 深市每日篮子组合（PCF） | `etf_sz_cons` | `ts_code`, `trade_date`, `con_code`, `start_date`, `end_date` | 11 列：`con_code`, `con_name`, `qty`, `sub_flag`, `cpr`, `rdr`, `sub_cc`, `red_cc` | 8,000 分；3,000 行/次 |
| `454` | 深市 ETF 实时参考 IOPV | `rt_etf_sz_iopv` | `ts_code` | 12 列：`trade_time`, `ts_code`, `price`, `iopv`, `pre_iopv`, `buy_num`, `buy_vol` 等 | **独立权限**（“深市IOPV和申赎”，300/月）；5,000 行/次 |

---

## 2. 指数与申万 / 中信行业专题（23 项，`doc_id=93`）

常用后缀：`.SH`、`.SZ`、`.CSI`（中证）、`.CNI`（国证）、`.SI`（申万）、`.CI`（中信）、`.TI`（THS）、`.DC`（东财）、`.TS`（Tushare 自编）。

| 分类 | 接口 (`doc_id`) | 说明与积分/权限 |
|---|---|---|
| **综合指数** | `index_basic` (`94`), `index_daily` (`95`, 2,000 分), `index_weekly` (`171`, 600 分), `index_monthly` (`172`, 600 分), `index_weight` (`96`, 2,000 分), `index_dailybasic` (`128`, 2,000 分起，总市值/PE/PB/换手), `idx_factor_pro` (`358`, 89 列技术因子) | 覆盖上证/深证/沪深300/中证500/1000 等宽基与指数权重 |
| **指数实时与分钟** | `rt_idx_k` (`403`, 实时日线), `rt_idx_min` (`420`, 实时分钟), `idx_mins` (`419`, 历史分钟) | **独立权限**（“指数日线RT” / “指数分钟RT” / “指数历史分钟”） |
| **申万行业** | `index_classify` (`181`, 默认 SW2014 / `src='SW2021'`), `index_member_all` (`335`, 一二三级分级成分), `sw_daily` (`327`, 5,000 分, 18:30 更新), `rt_sw_k` (`417`, 独立权限), `sw_mins` (`469`, 独立权限 2,000/年) | 在 [`source_priority.yaml`](../../../config/source_priority.yaml) 中作为 `reference.industry` 唯一主源 |
| **中信行业** | `ci_index_member` (`373`, 5,000 分), `ci_daily` (`308`, 5,000 分), `ci_index_weight_d` (`518`, 10,000 分, 8,000 行/次) | 中信一/二/三级成分、日线与日度权重 |
| **国际与市场统计** | `index_global` (`211`, 6,000 分, `HSI`/`DJI`/`SPX`/`IXIC` 等), `vix_index` (`498`, 5,000 分, 恐慌指数), `daily_info` (`215`, 600 分, 沪深市场板块统计), `sz_daily_info` (`268`, 2,000 分, 深市概况), `idx_anns` (`460`, 6,000 分, 指数公告) | 市场广度、全球主要指数与指数调整公告 |

---

## 3. 公募基金（8 项，`doc_id=18`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分 & 限制 |
|---|---|---|---|---|---|
| `19` | 基金列表 | `fund_basic` | `ts_code`, `market`（`E` 场内/`O` 场外）, `status` | 25 列：`ts_code`, `name`, `management`, `custodian`, `fund_type`, `found_date`, `issue_amount`, `m_fee`, `c_fee`, `benchmark` 等 | 2,000 分 |
| `118` | 基金管理人 | `fund_company` | 无（一次提取全部） | 18 列：`name`, `shortname`, `chairman`, `manager`, `reg_capital`, `setup_date`, `employees`, `credit_code` 等 | 1,500 分 |
| `208` | 基金经理 | `fund_manager` | `ts_code`, `ann_date`, `name`, `offset`, `limit` | 10 列：`ts_code`, `ann_date`, `name`, `gender`, `birth_year`, `edu`, `begin_date`, `end_date`, `resume` | 500 分起；5,000 行/次 |
| `462` | 基金业绩基准 | `mkt_idx_bmk` | `ts_code`, `bmk_type`, `bmk_level` | 8 列：`ts_code`, `symbol`, `name`, `bmk_level`, `bmk_type`, `bmk_src`, `idx_type` | 5,000 分；500 行/次 |
| `207` | 基金规模 | `fund_share` | `ts_code`, `trade_date`, `start_date`, `end_date`, `market` | 3 列：`ts_code`, `trade_date`, `fd_share` | 2,000 分起；2,000 行/次 |
| `119` | 基金净值 | `fund_nav` | `ts_code`, `nav_date`, `market`, `start_date`, `end_date` | 9 列：`unit_nav`, `accum_nav`, `accum_div`, `net_asset`, `total_netasset`, `adj_nav` 等 | 2,000 分起 |
| `120` | 基金分红 | `fund_div` | `ann_date`, `ex_date`, `pay_date`, `ts_code` | 16 列：`ann_date`, `div_proc`, `record_date`, `ex_date`, `pay_date`, `net_ex_date` 等 | 400 分起 |
| `121` | 基金持仓 | `fund_portfolio` | `ts_code`, `symbol`, `ann_date`, `period`, `start_date`, `end_date` | 8 列：`ts_code`, `ann_date`, `end_date`, `symbol`, `mkv`, `amount`, `stk_mkv_ratio`, `stk_float_ratio` | 5,000 分（200 次/分）；8,000+（500 次/分） |
