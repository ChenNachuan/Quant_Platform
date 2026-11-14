# 05. A 股财务、预期、资金流、事件与参考数据（25 项）

[← 返回目录](README.md)

> 本篇涵盖 `data_dict.json`「A 股」分组中的财务三表、指标估值、卖方评级与一致预期、资金流向/两融/龙虎/打板/筹码、公司事件、股东治理及交易日历共 **25 个数据集**。

---

## 1. 财务三表、指标、估值与业绩（5 项）

| SKU | 中文名 | 历史 | 子表 / 字段与关键坑点 |
|---|---|---|---|
| `cn_fin_statements` | 财务三表（含退市） | 2001~至今 | **7 表**（136.1 万行，2,402 列）：在市 `income`（281 列）、`balance`（431 列）、`cashflow`（473 列）、`income_rev`（282 列单季/修订版），以及中文列名退市版 `income_delisted`（285 列）、`balance_delisted`（373 列）、`cashflow_delisted`（277 列）。**PIT 关键列**：`report_date`、`notice_date`。列名沿用东财 F10 英文大写（`_YOY` 同比、`_ADD/_REDUCE` 增减）；一般/银行/保险/证券四套科目合并成宽表，211 个行业字段过半为空属正常 |
| `cn_financial_indicator` | A 股财务指标 | 1990~至今 | **141 列**（54.5 万行）：东财拼音缩写列名（如 `EPSJB, BPS, ROEJQ, ROIC, ZCFZL, XSMLL, DJD_TOI_YOY`），含 `REPORT_DATE`、`NOTICE_DATE`、`UPDATE_DATE`。同理合并四行业模板，46 个金融专有列在全表为空属正常 |
| `cn_valuation_1d` | 日级估值 | 2000~至今 | **45 列**：`trade_date, symbol, close, market_cap, circulating_market_cap, total_shares, circulating_shares, pe_ttm, pe_static, pb, ps_ttm, PEG, pcf` 及各期年化/扣非估值、`dividend_yield_ttm` / `dividend_yield_lfy`（`1` 表 `1%`；`0` 表确认无派息、`NULL` 表缺失/停牌）与 `*_status` 状态说明 |
| `cn_earnings` | 业绩快报·预告·披露预约 | 1990~至今 | **3 表**（49.6 万行）：`express`（17 列快报含 `announce_date`）、`preann`（12 列预告含 `forecast_type/forecast_value/performance_change_range`）、`schedule`（9 列披露预约变更及未来预约日期） |
| `cn_revenue_segment` | 主营构成 / 营收分部 | 2000~至今 | 11 列（115.2 万行）：按产品/行业/地区拆分收入、成本、利润、占比与毛利率（年报/半年报） |

---

## 2. 卖方评级与 PIT 一致性预期（2 项）

| SKU | 中文名 | 历史 | 子表与回测规范 |
|---|---|---|---|
| `cn_analyst_rating` | 券商评级与目标价 | 2005~至今 | **2 表**（328.9 万行）：`cninfo_rating_obs`（14 列巨潮资讯逐份报告，含 `declare_date, is_first, rating_change, tp_low, tp_high`）、`report_rc`（21 列含盈利预测与 `max_price/min_price`）。注：`tp_high/tp_low` 约 78% 为空，评级与日期完整 |
| `cn_estimates` | ★一致性预期 | 2005~至今 | **5 表**（版本 `v1.35`）：**回测严禁用 `consensus`（事后全历史汇总表），必须使用 PIT 时点版 4 表** —— `consensus_pit_daily`（25 列日频面板，含 `fy1/eps_fy1/eps_fy1_n/eps_fy1_std/eps_fy2/eps_fwd12m/eps_fy1_up_3m/down_3m/rev_1m/rev_3m/rating_score/tp_median/eps_fy0/eps_fy1_growth`）、`eps_pit_events`（19 列报告级事件，含 `visible_from`）、`rating_pit_events`（13 列）、`pit_share_basis`（5 列送转股本调整系数）。口径：仅持牌券商/外资投行明确数值；来源日期后首个共同交易日起可见（`09:15` 前计当日）；面板滚取 180 天内最新一份报告；建议使用 `2010+` 区间；北交所用 `920` 新码 |

---

## 3. 资金流向、两融、龙虎榜、打板、大宗与筹码（7 项）

| SKU | 中文名 | 历史 | 子表 / 字段与关键提示 |
|---|---|---|---|
| `cn_mainflow_1d` | 主力资金流（五档） | 2012~至今 | 15 列：`trade_date, symbol, name, pct_chg, last_price` 及主力/超大单/大单/中单/小单净流入（**单位万元**）与占比（**`1` = `1%`**）。含沪深 A 股及部分北交所，不含 B 股 |
| `cn_hsgt` | 沪深港通资金流向 | 2014~至今 | **2 表**（573 万行，`ts_code` 为 `.SH/.SZ/.HK`）：`moneyflow_hsgt`（7 列）、`hk_hold`（6 列，2016-06 起）。**三大红线**：① 北向资金 `2024-08-19` 起变为成交活跃度（恒正且放大十余倍），**做净流入信号严格截断至 `2024-08-16`**；② 南向资金 `2023-11-24` 起变为累计量级；③ `hk_hold` 北向 `2024-08-16` 后仅剩季末日期（南向港股仍逐日）；部分日期含 `NaN` 非 `NULL`，求和前须 `isnan()` 过滤 |
| `cn_margin` | 融资融券明细 | 2010~至今 | **2 表**（623.1 万行）：`margin`（9 列分市场汇总，2010 起）、`margin_detail`（12 列逐股明细，2016 起）。注：`short_sell_balance` 与 `margin_total_balance` 约半数为空，融资字段完整 |
| `cn_lhb` | 龙虎榜 | 2016~至今 | **2 表**（73.8 万行）：`detail`（13 列上榜股、涨跌幅、净买/买卖额、换手与上榜原因）、`seat_detail`（12 列买卖前五营业部明细，`2024+`） |
| `cn_limit_sentiment` | 打板情绪（开盘啦·游资） | 2020~至今 | **5 表**（`.SH/.SZ/.BJ` 后缀，2026-07-28 前北交所含旧码）：`kpl_list`（15 列打板清单，2020 起，已去掉 9 个全空列）、`kpl_concept`（5 列，2024-04 起）、`kpl_concept_cons`（7 列题材成分，补齐行 `hot_num` 为 `NULL`；**T 日回测仅可用 `T-1` 日及以前名单**）、`hm_list`（3 列游资名录）、`hm_detail`（8 列游资每日买卖，2022-08 起） |
| `cn_block_trade` | 大宗交易 | 2016~至今 | 12 列（14.8 万行）：`trade_date, symbol, discount_rate, trade_price, volume, amount, turnover_to_circulating_market_cap`；仅包含大宗交易不可对等当日全部成交 |
| `cn_chips` | 筹码分布与胜率 | 2018~至今 | **2 表**（`.SH/.SZ` 后缀，8.4 亿行；**不含已退市股票**）：`cyq_perf`（11 列，951 万行，胜率 `winner_rate`、加权均价及 5/15/50/85/95 分位成本）、`cyq_chips`（4 列逐价位占比，8.3 亿行） |

---

## 4. 分红、股本事件、股东治理、状态与交易日历（11 项）

| SKU | 中文名 | 历史 | 子表与核心字段要点 |
|---|---|---|---|
| `cn_dividend` | 分红送配 | 1991~至今 | 13 列（5.7 万行）：`ex_date, record_date, plan_date, disclosure_date, cash, bonus_share, convert_share, div_yield, progress`（纯现金分红居多，送转列 64%~75% 为空） |
| `cn_equity_events` | 回购/增减持/质押 | 1994~至今 | **5 表**（922.3 万行）：东财口径 `buyback`、`holder_change`、`pledge`（周频）与 Tushare 口径 `repurchase`、`stk_holdertrade` |
| `cn_restricted_release` | 限售解禁 | 2010~至今 | **2 表**（549.1 万行，含未来一年日程）：`restricted`（12 列含解禁前后 20 日涨跌）与 `share_float`（7 列按股东拆解） |
| `cn_float_shares` | 股本结构（流通/总股本） | 2001~至今 | 7 列（含退市 A/B 股）：`symbol, ts, trade_date, close, total_shares, float_shares, free_float_shares`；按事件生效日还原，不以最新股本倒填 |
| `cn_holder_num` | 股东户数 | 1992~至今 | 15 列（29.5 万行）：各报告期股东户数、较上期增减比例与户均持股/市值（变动原因字段多空，户数完整） |
| `cn_top_holders` | 十大股东 | 2016~至今 | **2 表**（378.7 万行）：`top10`（14 列）与 `free_top10`（13 列十大流通股东） |
| `cn_fund_holdings` | 基金重仓持仓 | 2010~至今 | **2 表**（908.3 万行）：`stock_fund_summary`（11 列个股被基金持有家数/股数变动）与 `fund_portfolio`（8 列基金季报重仓明细） |
| `cn_governance` | 公司治理与机构调研 | 1989~至今 | **6 表**：`stk_managers`（1989 起）、`stk_rewards`（1992 起周更）、`new_share`（2008 起网上发行）、`stk_surv`（2021-08 起机构调研）、`broker_recommend`（2020-03 起月度金股）、`listing_identity`（14 列换股/分立/重新上市/转板及北交所原代码关联表） |
| `cn_st` | ST 戴帽摘帽区间 | 2005~至今 | 4 列（1,431 行）：`ts_code, st_start, st_end, source`（事件区间表，做逐日过滤须自行按日期展开） |
| `cn_suspend` | 停复牌记录 | 2005~至今 | 4 列（6.0 万行）：`ts_code, suspend_date, resume_date, source`（2010 起含 `tushare` 补录日频停牌；不含盘中临停；`resume_date` 为复牌首日） |
| `trade_calendar` | 交易日历（10 市场） | 1990~次年末 | 7 列（10.7 万行）：`exchange, exchange_name, cal_date, is_open, pretrade_date, nexttrade_date, confirmed`。覆盖沪深（北交所同沪深）、6 家国内期货所、港股、美股；**已修正休市日 `pretrade_date` 口径，未来日程先按规则预排（`confirmed=false`），国务院通知发布后置 `true`** |
