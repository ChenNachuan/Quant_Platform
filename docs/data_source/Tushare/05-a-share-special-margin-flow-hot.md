# 05. 特色数据、两融、资金流向与打板专题

[← 返回目录](README.md) · 对应官网 `doc_id=291 / 330 / 342 / 346`

---

## 1. 特色数据（11 项，`doc_id=291`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分/权限 & 单次上限 |
|---|---|---|---|---|---|
| `292` | 券商盈利预测数据 | `report_rc` | `ts_code`, `report_date`, `start_date`, `end_date` | 23 列：`ts_code`, `name`, `report_date`, `report_title`, `org_name`, `author_name`, `quarter`, `op_rt`, `np`, `eps`, `pe` 等 | 2,000 试用（10 次/天）；8,000 正式（10 万次/天）；10,000+ 无限；3,000 行/次 |
| `293` | 每日筹码及胜率 | `cyq_perf` | `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列：`his_low`, `his_high`, `cost_5pct`..`cost_95pct`, `weight_avg`, `winner_rate` | 5,000 分（2 万/天）；10,000 分（20 万/天）；15,000 无限；6,000 行/次 |
| `294` | 每日筹码分布 | `cyq_chips` | `ts_code`, `trade_date`, `start_date`, `end_date` | 4 列：`ts_code`, `trade_date`, `price`, `percent` | 同 `cyq_perf`；6,000 行/次 |
| `296` | 股票技术面因子 | `stk_factor` | `ts_code`, `trade_date`, `start_date`, `end_date` | 35 列：基础行情 + MACD/KDJ/RSI/BOLL/CCI 等常用因子 | 5,000 分（100 次/分）；8,000+（500 次/分）；10,000 行/次 |
| `328` | 股票技术面因子（专业版） | `stk_factor_pro` | `ts_code`, `trade_date`, `start_date`, `end_date` | **261 列**：未/前/后复权行情 + 全套静态技术面因子（支持 HTTP） | 5,000 分（30 次/分）；8,000+（500 次/分）；10,000 行/次 |
| `353` | 股票开盘集合竞价 | `stk_auction_o` | `ts_code`, `trade_date`, `start_date`, `end_date` | 9 列：`ts_code`, `trade_date`, `close`, `open`, `high`, `low`, `vol`, `amount`, `vwap` | **独立权限**（“集合竞价成交”）；10,000 行/次（2010 起，20:00 更新） |
| `354` | 股票收盘集合竞价 | `stk_auction_c` | `ts_code`, `trade_date`, `start_date`, `end_date` | 9 列（同 `stk_auction_o`） | **独立权限**（“集合竞价成交”）；10,000 行/次 |
| `364` | 神奇九转指标 | `stk_nineturn` | `ts_code`, `trade_date`, `freq`, `start_date`, `end_date` | 13 列：`ts_code`, `trade_date`, `freq`, OHLCVA, `up_count`, `down_count` 等 | 6,000 分；10,000 行/次 |
| `399` | AH 股比价 | `stk_ah_comparison` | `hk_code`, `ts_code`, `trade_date`, `start_date`, `end_date` | 11 列：`hk_code`, `ts_code`, `trade_date`, `hk_pct_chg`, `hk_close`, `close`, `ah_comparison`, `ah_premium` 等 | 5,000 分起 |
| `275` | 机构调研数据 | `stk_surv` | `ts_code`, `trade_date`, `start_date`, `end_date` | 10 列：`ts_code`, `name`, `surv_date`, `fund_visitors`, `rece_place`, `rece_mode`, `rece_org`, `org_type`, `comp_rece`, `content` | 5,000 分；400 行/次 |
| `267` | 券商月度金股 | `broker_recommend` | `month` | 4 列：`month`, `broker`, `ts_code`, `name` | 6,000 分；1,000 行/次 |

---

## 2. 两融及转融通（7 项，`doc_id=330`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分 & 限制 |
|---|---|---|---|---|---|
| `58` | 融资融券交易汇总 | `margin` | `trade_date`, `start_date`, `end_date`, `exchange_id` | 9 列：`trade_date`, `exchange_id`, `rzye`, `rzmre`, `rzche`, `rqye`, `rqmcl`, `rzrqye`, `rqyl` | 2,000 分起；4,000 行/次 |
| `59` | 融资融券交易明细 | `margin_detail` | `trade_date`, `ts_code`, `start_date`, `end_date` | 11 列：`trade_date`, `ts_code`, `name`, `rzye`, `rqye`, `rzmre`, `rqyl`, `rzche`, `rqchl`, `rqmcl` | 2,000 分起；6,000 行/次 |
| `326` | 融资融券标的（盘前） | `margin_secs` | `ts_code`, `trade_date`, `exchange`, `start_date`, `end_date` | 4 列：`trade_date`, `ts_code`, `name`, `exchange` | 2,000 分（5,000+ 无上限）；6,000 行/次 |
| `331` | 转融资交易汇总 | `slb_len` | `trade_date`, `start_date`, `end_date` | 6 列：`trade_date`, `ob`, `auc_amount`, `repo_amount`, `repay_amount`, `cb` | 2,000 分（200 次/分）；5,000 行/次 |
| `332` | 转融券交易汇总（停） | `slb_sec` | `trade_date`, `ts_code`, `start_date`, `end_date` | 7 列：`trade_date`, `ts_code`, `name`, `ope_inv`, `lent_qnt`, `cls_inv`, `end_bal` | 2,000 分（已随政策停更，保留历史） |
| `333` | 转融券交易明细（停） | `slb_sec_detail` | `trade_date`, `ts_code`, `start_date`, `end_date` | 6 列：`trade_date`, `ts_code`, `name`, `tenor`, `fee_rate`, `lent_qnt` | 2,000 分（历史） |
| `334` | 做市借券交易汇总（停） | `slb_len_mm` | `trade_date`, `ts_code`, `start_date`, `end_date` | 7 列（同 `slb_sec`） | 2,000 分（历史） |

---

## 3. 资金流向数据（7 项，`doc_id=342`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分 & 上限 |
|---|---|---|---|---|---|
| `170` | 个股资金流向（标准） | `moneyflow` | `ts_code`, `trade_date`, `start_date`, `end_date` | 20 列：小/中/大/特大单买卖量额 `buy_sm_*`..`sell_elg_*`, `net_mf_vol`, `net_mf_amount` | 2,000 分起；6,000 行/次 |
| `348` | 个股资金流向（THS） | `moneyflow_ths` | `ts_code`, `trade_date`, `start_date`, `end_date` | 13 列：`net_amount`, `net_d5_amount`, `buy_lg_amount` 等（2019-07-26 起） | 6,000 分；6,000 行/次 |
| `349` | 个股资金流向（DC） | `moneyflow_dc` | `ts_code`, `trade_date`, `start_date`, `end_date` | 15 列：`net_amount`, `net_amount_rate`, `buy_elg_amount`.. | 5,000 分起；6,000 行/次 |
| `371` | 板块资金流向（THS） | `moneyflow_cnt_ths` | `ts_code`, `trade_date`, `start_date`, `end_date` | 12 列：`lead_stock`, `industry_index`, `company_num`, `net_buy_amount` 等 | 6,000 分；5,000 行/次 |
| `343` | 行业资金流向（THS） | `moneyflow_ind_ths` | `ts_code`, `trade_date`, `start_date`, `end_date` | 12 列：`industry`, `lead_stock`, `net_buy_amount` 等（2024-09-10 起） | 6,000 分；5,000 行/次 |
| `344` | 板块资金流向（DC） | `moneyflow_ind_dc` | `ts_code`, `trade_date`, `start_date`, `end_date`, `content_type` | 18 列：`content_type`, `net_amount`, `buy_elg_amount` 等 | 6,000 分；5,000 行/次 |
| `345` | 大盘资金流向（DC） | `moneyflow_mkt_dc` | `trade_date`, `start_date`, `end_date` | 15 列：`close_sh`, `pct_change_sh`, `close_sz`, `net_amount` 等 | 120 试用 / 6,000 正式；3,000 行/次 |

---

## 4. 打板专题数据（22 项，`doc_id=346`）

| 细分主题 | 接口 (`doc_id`) | 说明与门槛摘要 |
|---|---|---|
| **龙虎榜** | `top_list` (`106`), `top_inst` (`107`) | 每日龙虎榜统计（2,000 分）与营业部/机构明细（5,000 分），单次 10,000 行 |
| **涨跌停 / 连板** | `limit_list_ths` (`355`), `limit_list_d` (`298`), `limit_step` (`356`), `limit_cpt_list` (`357`) | THS 涨跌停（8,000 分）、全市场涨跌停/炸板（5,000/8,000 分）、连板天梯 `nums`（8,000 分）、最强板块（8,000 分） |
| **THS 概念板块** | `ths_index` (`259`), `ths_daily` (`260`), `ths_member` (`261`) | 分类/日线行情/最新成分，均 6,000 分 |
| **DC 概念板块** | `dc_index` (`362`), `dc_member` (`363`), `dc_daily` (`382`) | 东财板块分类/成分/行情，均 6,000 分 |
| **TDX 概念板块** | `tdx_index` (`376`), `tdx_member` (`377`, 2025-03-28 起), `tdx_daily` (`378`, 38 列) | 通达信板块分类/成分/行情，均 6,000 分 |
| **KP 榜单与题材** | `kpl_list` (`347`), `kpl_concept_cons` (`351`) | 涨停/炸板/跌停/自然涨停时间与原因、题材成分（5,000/8,000 分） |
| **盘前竞价成交** | `stk_auction` (`369`) | 9:25 当日开盘竞价成交额/量比/换手；**独立权限**（“集合竞价成交”），8,000 行/次 |
| **市场游资与热榜** | `hm_list` (`311`, 5,000 分), `hm_detail` (`312`, 10,000 分), `ths_hot` (`320`, 6,000 分), `dc_hot` (`321`, 8,000 分) | 游资名录（<500 条）与每日买卖明细、THS/DC 实时/历史热股榜 |
