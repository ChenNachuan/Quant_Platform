# 04. A 股行情、指数、板块、ETF、可转债与期权（48 项）

[← 返回目录](README.md)

> 本篇涵盖 `data_dict.json`「A 股」分组中的交易行情、复权、集合竞价、技术因子、指数、板块、ETF、可转债与期权共 **48 个数据集**。通用时间戳 `ts`、时区及 `.XSHG/.XSHE/.XBSE` 代码约定见 [02 篇](02-format-timezone-and-pit-conventions.md)。

---

## 1. A 股股票行情（10 项，均含退市股 · 不复权）

| SKU | 中文名 | 周期 | 历史起止 | 交付 / 子表 | 核心字段与口径要点 |
|---|---|---|---|---|---|
| `cn_daily_full` | A 股日线（不复权） | 日 | 2001~至今 | 下载器 `full` | `ts, symbol, open, high, low, close, volume, amount, source`（`volume` 为**股**；约 1,727.7 万行；停牌日不出记录；`cn_daily_hfq` 已于 2026-08-09 退役） |
| `cn_weekly` / `cn_monthly` | A 股周线 / 月线 | 周 / 月 | 2001~至今 | 下载器 `full` | 同上 9 列；周线 366 万行，月线 87.4 万行 |
| `stocks_1m` | A 股分钟（含退市） | 1 分钟 | 1999~至今 | 下载器 `full` | 10 列（含 `ingested_at`）；**2001 起每日 241 根结束时刻**（`09:30` 单列开盘竞价 + `09:31–11:30`/`13:01–15:00`），停牌日出平盘 0 量占位行；**1999-07-26~2000-12-29 为 844 只核验日，每日 240 根无停牌占位** |
| `stocks_5m` / `stocks_15m` / `stocks_30m` / `stocks_60m` | A 股 5/15/30/60 分钟线 | 5m~60m | 1999~至今 | 下载器 `full` | 9 列；每日 48/16/8/4 根，**`ts` 为开始时刻**（`09:30` 起），开收盘竞价并入首尾两根 |
| `cn_tick` | A 股逐笔成交（Tick） | 逐笔 | 2000~至今 | 下载器 `full` | 7 列：`code`（6 位裸码）, `trade_date`, `seq`, `t`（`HH:MM`）, `price`, `volume`（**整数手=100 股**）, `bs`（`0` 主买/`1` 主卖/`2` `09:25` 竞价或回补未定/`5` 盘后固定价/`8` 竞价撮合参考价 `vol=0`）；约 305.9 亿行、76.0 GiB；**无盘口挂单** |
| `cn_tick_l2` | 10 档逐笔 Tick L2（网盘 5T） | 逐笔 | 2017~至今 | **网盘**（7z/CSV） | 66 列含十档买卖价量、成交标志、BS 标志（深市含撤单 `C`）；`GB18030` 编码；**价格全为整数（万分之一元，必须 `÷10000`）**；2026-06 起沪市 ETF `IOPV` 列为 0；2026 年约 5.19 GB/日（压缩）→ 53 GB CSV → 12 GB Parquet |

---

## 2. 复权因子、集合竞价、涨跌停与技术因子（6 项）

| SKU | 中文名 | 历史 | 核心字段 / 说明 |
|---|---|---|---|
| `cn_adj_factor` | 复权因子 | 1990~至今 | 3 列：`symbol, d, hfq_factor`（稀疏表，仅除权日有记录，**含 5,499 条 `d=1900-01-01, hfq_factor=1` 基线行，务必先整表 `ffill` 再按日期截取**） |
| `cn_auction_1d` | 集合竞价 | 2010~至今 | 15 列：`trade_date, symbol, name, auction_change, auction_price, auction_volume, auction_amount, auction_turnover_rate, free_float_shares_wan, call_auction_vol_ratio` 等；约 1,450.5 万行 |
| `cn_limit_flag` | 涨跌停标记 | 2010~至今 | **3 表**：`limits`（11 列含 `up_limit, down_limit, raw_up_limit, raw_down_limit, limit_status`）、`status`（9 列含 `close_limit_up/down`, `touched_up/down`）、`flag`（5 列：`date, symbol, pct_chg, limit_up, limit_down`） |
| `cn_limitupdown_1d` | 涨停板（封板/炸板） | 2018~至今 | 24 列：`symbol, name, trade_date, limit_up_time, limit_down_time, board_start_time, last_limit_up_time, tag, sector, main_net_amount, auction_amount, limit_up_streak, open_times, actual_circulation`（开盘啦口径元）等；空值不等于 0 |
| `cn_tech_calc` | ★技术指标（不复权） | 2001~至今 | 23 列：`symbol, d(TIMESTAMP), close`, `MA_5/10/20/30/60`, `MACD_DIF/DEA/MACD`, `KDJ_K/D/J`, `RSI_6/12/24`, `BOLL_MID/UP/DN`, `pct_chg, amplitude, vol_ratio`；1,727.7 万行 |
| `cn_factor_daily` | ★每日技术因子（不复权+后复权） | 2018~至今 | **183 列**（`ts_code` `.SH/.SZ` 后缀）：行情、估值（`pe/pe_ttm/pb/ps/ps_ttm/dv_ratio/dv_ttm`）、股本市值、`adj_factor`，及 **70+ 种技术指标的 `_bfq` 与 `_hfq` 双版本**（无前复权防未来函数；涵盖 `asi/atr/bbi/bias/boll/brar/cci/cr/dfma/dmi/dpo/ema/emv/expma/kdj/ktn/ma/macd/mass/mfi/mtm/obv/psy/roc/rsi/taq/trix/vr/wr/xsii` 等）；约 6.8 GB |

---

## 3. 指数行情（8 项）

覆盖沪深主要指数（上证、深证成指、沪深 300、中证 500、创业板指等约 556 只）：

| SKU | 周期 | 历史 | 关键字段与特殊口径 |
|---|---|---|---|
| `cn_index_1m` | 1 分钟 | 2000~至今 | 9 列；主体 2005~至今，**2000-01-07~2004-12-31 另有 16 只指数核验日**；常规每日 **240 根结束时刻**（`09:31–15:00`，无 `09:30` 独立根），12 只债券指数交易至 `15:30`（270 根）；全表 4.45 亿行 |
| `cn_index_5m` / `cn_index_15m` / `cn_index_30m` / `cn_index_60m` | 5m~60m | 2000~至今 | 9 列；每日 48/16/8/4 根（债券指数 54/18/9/5 根），**`ts` 为开始时刻** |
| `cn_index_1d` / `cn_index_weekly` / `cn_index_monthly` | 日 / 周 / 月 | 2005~至今 | 9 列；日线 185.4 万行（2005-01-04 起），周线 39.1 万行，月线 9.2 万行 |

---

## 4. 板块、申万行业、成分与题材热度（5 项）

| SKU | 中文名 | 历史 | 子表清单与注意要点 |
|---|---|---|---|
| `cn_board` | 板块（行业/概念/地域） | 1996~至今 | **18 张表**（约 8,252.6 万行），汇集通达信（`cn_board_index_tdx` 1996-09-10 起 / `tdx_index` / `tdx_member`）、同花顺（`cn_board_index_ths` / `ths_index` / `ths_daily` / `ths_member` / `ths_moneyflow_ind` / `ths_moneyflow_cnt` / `ths_hot`）与东财（`cn_board_list` / `cn_board_member` / `cn_board_moneyflow` / `dc_index` / `dc_daily` / `dc_member` / `dc_hot` / `moneyflow_mkt_dc`）。**注意**：各子表起始年份各异（2010–2025），且东财少数历史成分日为相邻日推定，官网提供推定清单 CSV |
| `cn_sw_index` | 申万行业指数（行情+估值+成分） | 1999~至今 | **5 表**（381.1 万行，524 个指数：一级 31 / 二级 131 / 三级 336 / 市场表征 9 / 风格 17）：`index_daily`（12 列）、`index_valuation`（15 列含 PE/PB/股息率/换手）、`index_member`、`index_info`、`index_member_history`（带 `in_date/out_date` 无幸存者偏差）。**单位坑：`volume` 为亿股、`amount` 为亿元** |
| `cn_industry_tdx` | 通达信行业分类（当期） | 当期 | 12 列（5,623 行）：`symbol, code6, market, tdx_code, l1/l2/l3_code & name, sw_code, snapshot_date` |
| `cn_index_cons` | 指数成分（月末快照） | 当期 | 10 列（2.2 万行）：`trade_date, index_code, component_code, weight` 等；**月末一档快照，不可直接当作逐日成分变动** |
| `cn_theme_heat` | 题材热度多源 | 2026~至今 | **6 表**（2026-06 起）：`em_hot_rank`、`em_hot_up`、`em_keyword`、`em_hot_detail`、`em_concept_board`、`ths_concept`。**注意：`ths_concept.code` 为 `BIGINT` 会丢前导零；行内行情列 88% 为空** |

---

## 5. ETF 全周期、复权、净值、PCF 与份额（13 项）

| SKU | 周期 / 类型 | 历史 | 关键字段与口径要点 |
|---|---|---|---|
| `cn_etf_tick` | ETF 逐笔（3 秒聚合） | 2005~至今 | 7 列（`code` 裸码）：`volume` 为**股（份）**非手，含 `15:05–15:30` `bs=5` 盘后固定价；`2010-08-06` 沪市 8 只 ETF 源头量为 0 |
| `cn_etf_1m` / `cn_etf_5m` / `cn_etf_15m` / `cn_etf_30m` / `cn_etf_60m` | ETF 1m~60m K 线 | 2005~至今 | 9 列（含已清盘 ETF）；`volume` 为**股**；`1m` 为**每日 240 根开始时刻**（`09:30–14:59`，无独立 `09:30` 竞价根） |
| `cn_etf_1d` / `cn_etf_weekly` / `cn_etf_monthly` | ETF 日/周/月线 | 2005~至今 | 9 列（1,661 只，含清盘）；`volume` 为**股**，日线 150.6 万行 |
| `cn_etf_adj` | ETF 复权因子与折算 | 2006~至今 | **2 表**：`adj_factor`（3 列，含 1,723 行 `1900-01-01` 基线；份额拆并按真实折算率还原）、`split_events`（13 列，295 次拆并含折算前后份额与真实净申赎 `net_creation`） |
| `cn_etf_nav` | ETF 基金净值 | 2004~当期 | **2 表**：`nav`（7 列：1,647 只 ETF 的 `unit_nav/accum_nav/daily_return`）、`money_yield`（6 列：26 只场内货币基金的万份收益与 7 日年化，2012 起） |
| `cn_etf_pcf` | ETF 申赎清单（PCF） | 2005~至今 | 23 列（1.9 亿行，1,753 只 ETF）：当日每份 CU 的成分券数量、现金替代标志与溢价、现金差额；按 `etf_code + trade_day` 联合筛选 |
| `imai_etf` | 沪深基金每日份额（含 ETF） | 2012~当期 | 7 列：`date, code, name, exchange, etf_type, shares, source`。**严重提示：深市含 LOF/分级/封闭式，不可整表汇总**，标准 ETF 筛选条件：`exchange = 'SSE' OR (exchange = 'SZSE' AND etf_type = 'ETF' AND code NOT IN ('159002','159004','159006'))` |

---

## 6. 可转债（4 项）与 A 股期权（2 项）

| SKU | 中文名 | 历史 | 核心字段与特别注意 |
|---|---|---|---|
| `cn_cb_tick` | 可转债逐笔成交 | 2018~至今 | 7 列（`code` 裸码）：**`volume` 单位手（1 手 = 10 张）**，成交额 `price × volume × 10`；`bs=8` 为 `vol=0` 竞价参考价须排除 |
| `cn_cb_1d` / `cn_cb_1m` | 可转债日线 / 1 分钟 | 2010~至今 | 9 列（含已到期与强赎退市转债，约 1,045 只）；**`volume` 单位为手（1 手 = 10 张）**；`1m` 为**每日 240 根开始时刻** |
| `cn_cb_adj` | 可转债复权因子 | 2005~至今 | **2 表**：`adj_factor`（含 1,057 条 `1900-01-01` 基线行，纠正全价交易除息扣减票息造成的假下跌）、`coupon_events`（8 列：2,672 次付息记录） |
| `cn_opt_etf_1d` | ETF 期权日线（含希腊字母） | 2015~至今 | 24 列（2015-02-09 起，85.3 万行）：上交所/深交所合约、行权价、`cp`、OHLC/结算价/成交持仓及 `delta/gamma/vega/theta/rho/iv` |
| `cn_opt_index_1d` | 股指期权日线 | 2019~至今 | 22 列（2019-12-23 起，84.2 万行）：中金所沪深 300/上证 50/中证 1000 期权逐合约日线（含补算完整希腊字母与 IV，实测与官方差 < 1%） |
