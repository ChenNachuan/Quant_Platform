# 09. 宏观经济（国内、美国与韩国市场）

[← 返回目录](README.md) · 对应官网「宏观经济」（`doc_id=147`，共 28 个 API）

---

## 1. 国内宏观（14 项，`doc_id=224`）

| 子类 | `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 | 积分 & 单次上限 |
|---|---|---|---|---|---|---|
| 日程 | `461` | 中国经济数据发布日程 | `cn_schedule` | `m`, `title` | `month`, `publish_date`, `title`, `issuing_org`, `data_api` | 2,000 分；3,000 行/次 |
| 利率 | `149` | Shibor 利率 | `shibor` | `date`, `start_date`, `end_date` | `date`, `on`, `1w`, `2w`, `1m`, `3m`, `6m`, `9m`, `1y` | 120 分（权限表 2,000）；2,000 行/次 |
| 利率 | `150` | Shibor 报价数据 | `shibor_quote` | `date`, `start_date`, `end_date`, `bank` | 18 列：各报价行买卖报价 `on_b`/`on_a`.. | 120 分（权限表 2,000）；4,000 行/次 |
| 利率 | `151` | LPR 贷款基础利率 | `shibor_lpr` | `date`, `start_date`, `end_date` | `date`, `1y`, `5y`（2013-10-25 起） | 120 试用 / 5,000 正式（500 次/分） |
| 利率 | `152` | Libor 利率 | `libor` | `date`, `start_date`, `end_date`, `curr_type` | `date`, `curr_type`, `on`, `1w`..`12m` | 120 分；4,000 行/次 |
| 利率 | `153` | Hibor 利率 | `hibor` | `date`, `start_date`, `end_date` | `date`, `on`, `1w`..`12m` | 120 分；4,000 行/次 |
| 利率 | `173` | 温州民间借贷利率 | `wz_index` | `date`, `start_date`, `end_date` | 13 列：`comp_rate`, `center_rate`, `micro_rate` 等 | 2,000 分；不限量一次全取 |
| 利率 | `174` | 广州民间借贷利率 | `gz_index` | `date`, `start_date`, `end_date` | 7 列：`d10_rate`, `m1_rate`..`long_rate` | 2,000 分；不限量一次全取 |
| 国民经济 | `227` | 国内生产总值（GDP） | `cn_gdp` | `q`, `start_q`, `end_q`, `fields` | `quarter`, `gdp`, `gdp_yoy`, `pi`, `si`, `ti` 及同比 | 600 分；一次全取（10,000 行） |
| 价格 | `228` | 居民消费价格指数（CPI） | `cn_cpi` | `m`, `start_m`, `end_m` | 13 列：全国/城市/农村同比、环比与累计 `nt_*`/`town_*`/`cnt_*` | 600 分；5,000 行/次 |
| 价格 | `245` | 工业生产者出厂价格（PPI） | `cn_ppi` | `m`, `start_m`, `end_m` | 31 列：生产资料/生活资料分项同比环比与累计 | 600 分；5,000 行/次 |
| 金融 | `242` | 货币供应量（月） | `cn_m` | `m`, `start_m`, `end_m`, `fields` | `month`, `m0`/`m1`/`m2` 及 `*_yoy`, `*_mom` | 600 分；5,000 行/次 |
| 金融 | `310` | 社融增量（月度） | `sf_month` | `m`, `start_m`, `end_m` | `month`, `inc_month`, `inc_cumval`, `stk_endval` | 2,000 分；2,000 行/次 |
| 景气度 | `325` | 采购经理指数（PMI） | `cn_pmi` | `m`, `start_m`, `end_m` | 60 列：制造业/非制造业/综合 PMI 全分项 `pmi010000`.. | 5,000 分；2,000 行/次 |

---

## 2. 国际宏观（美国指标，13 项，`doc_id=217`）

### 2.1 美国利率数据（5 项，`120` 分起，单次 2,000 行）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 |
|---|---|---|---|---|
| `219` | 美国国债收益率曲线 | `us_tycr` | `date`, `start_date`, `end_date`, `fields` | 14 列：`m1`–`m6`, `y1`–`y30` |
| `220` | 美国实际国债收益率曲线 | `us_trycr` | 同上 | 6 列：`y5`, `y7`, `y10`, `y20`, `y30` |
| `221` | 美国短期国债利率 | `us_tbr` | 同上 | 13 列：`w4_bd/ce`..`w52_bd/ce` |
| `222` | 美国国债长期利率 | `us_tltr` | 同上 | 4 列：`date`, `ltc`, `cmt`, `e_factor` |
| `223` | 美国国债长期利率平均值 | `us_trltr` | 同上 | 2 列：`date`, `ltr_avg` |

### 2.2 美国景气度、国民经济与价格指数（8 项，5,000 分起，300 行/次）

| `doc_id` | 名称 | `api_name` | 核心入参 | 核心输出字段 |
|---|---|---|---|---|
| `524` | 美国采购经理人指数（PMI） | `us_pmi` | `trade_date`, `start_date`, `end_date` | `trade_date`, `category`, `unit`, `value` |
| `526` | 美国劳工统计局 BLS 就业 | `us_unemployment` | 同上 | `trade_date`, `category`, `frequency`, `unit`, `value` |
| `527` | 美国零售销售月度变化 | `us_rs` | 同上 | `trade_date`, `category`, `unit`, `value` |
| `528` | 美国新房开工数量 | `us_hs` | 同上 | 同上 |
| `531` | 美国新发建筑许可 | `us_bp` | 同上 | 同上 |
| `533` | 美国职位空缺与劳动力流动（JOLTS） | `us_job_offers` | 同上 | `ts_date`, `category`, `unit`, `value` |
| `530` | 美国核心 PCE 物价指数 | `us_pce` | 同上 | `trade_date`, `category`, `unit`, `value` |
| `532` | 美国生产者价格指数（PPI） | `us_ppi` | 同上 | `trade_date`, `category`, `unit`, `value` |

---

## 3. 韩国市场（8 项，均需 5,000 积分，`doc_id=508`）

均使用 `trade_date`, `start_date`, `end_date` 入参，源于韩国金融投资协会（KOFIA）：

| `doc_id` | 名称 | `api_name` | 核心说明与单次上限 |
|---|---|---|---|
| `509` | 韩国股票市场趋势 | `stk_trend_kofia` | KOSPI / KOSDAQ / K-OTC 趋势指标（3,000 行/次） |
| `510` | 韩国股票市场资金详情 | `stk_funding_kofia` | 投资者存款、衍生品保证金、融资应收等（3,000 行/次） |
| `511` | 韩国资管公司 AUM | `fund_aum_amc_kofia` | 22 列按公司与投向拆分 AUM（9,000 行/次） |
| `512` | 韩国按资产类型划分基金 AUM | `fund_aum_type_kofia` | 股票/混合/债券/FOF/衍生品/房地产等 12 列（300 行/次） |
| `513` | 韩国基金资金流 | `fund_flow_kofia` | 14 列申购/赎回/净变更（3,000 行/次） |
| `514` | 韩国货币市场基金 MMF | `fund_mmf_kofia` | 个人/机构 MMF 规模及环比变化（3,000 行/次） |
| `516` | 韩国海外投资基金规模 | `fund_overseas_kofia` | 海外基金数量、AUM、NAV（3,000 行/次） |
| `515` | 韩国分销渠道销售余额 | `fund_sales_kofia` | 证券/银行等分销渠道占比与余额（300 行/次） |
