# 07. 新闻事件、宏观经济与另类稀缺数据（17 项）

[← 返回目录](README.md)

> 本篇涵盖 `data_dict.json` 中的新闻与信息层（5 项）以及宏观/机构持仓/能源农业航运/卖方研报/全球关税管制等另类稀缺数据（12 项），合计 **17 个数据集**。多数 `imai_*` 库自带严格避免前视偏差的 `available_date` / `available_time` / `available_rule` 时点标注。

---

## 1. 新闻、信息层与公告事件流（5 项）

| SKU | 中文名 | 历史 | 子表清单与 PIT / 检索提示 |
|---|---|---|---|
| `imai_news` | ★新闻统一库（四源合并去重） | 2007~至今 | 13 列（525.3 万行）：四源合并按首发时间去重，`is_repost` / `origin_news_id` 标明转载关系，**回测直接按 `available_time` / `available_date` 过滤避免未来泄露** |
| `imai_infolayer` | ★新闻→个股信息层 | 2005~至今 | **5 表**（587.7 万行）：`news_security_link`（6 列）、`news_security_link_scored`（16 列含相关性 `relevance`、`rel_tier` 与 **角色 `role`**：注意区分事件主体、`opinion_source` 研报作者、`compare` 被对比提及）、`panel_news_symbol_daily`（8 列日频信息脉冲）、`news_anchor_leadlag`（11 列新闻领先公告分钟差）、`policy_enforcement_link`（5 列政策落地关联） |
| `imai_events` | ★财报预告·公司行动事件 | 2001~至今 | **6 表**（165.2 万行）：`event`（26 列，含 9 类公司行动与业绩事件及 `available_time`）、`event_wave2`（25 列）、`forecast_value`（11 列抽取净利润/同比区间）、`fulfillment`（15 列业绩兑现差异）、`revision`（10 列业绩更正链）、`disclosure_rt`（12 列巨潮实时公告流，澄清/异动/风险提示附 `pdf_url`） |
| `news_flash` | 新闻快讯包（三源） | 2018~至今 | **3 表**合并结构（513.8 万行，`source, ts, title, content`）：汇集新浪财经 7×24、东财快讯与同花顺快讯。**注意：新浪源 293 万行标题几乎为空（仅正文），按标题检索仅适用于东财与同花顺两源** |
| `news_cctv` | 新闻联播文字稿 | 2007~至今 | 5 列（9.7 万行，2007-01-01 起）：`date, seq, title, content, source` |

---

## 2. 宏观经济、利率与跨市场机构持仓（6 项）

| SKU | 中文名 | 历史 | 子表与关键使用注意事项 |
|---|---|---|---|
| `cn_macro_monthly` | 中国宏观与产业价格指标 | 2001~至今 | **2 表**（18.2 万行，含生产/投资/消费/地产与大宗/建材/菜篮子 32 条月/旬/周/日频序列）：`industry_indicator`（6 列）、`econ_indicator`（12 列含 `publish_date/publish_time` PIT 字段与 `value_abs/value_yoy`） |
| `cn_rate_cn` | 利率大全（中债/美债/Shibor/LPR） | 2001~至今 | **10 表**（33.8 万行）：`bond_yield`（2006 起中债曲线）、`shibor` / `shibor_quote` / `shibor_quote_bidask`（2006/2015 起）、`lpr`（2002 起月更）、`us_treasury_yield`（2001 起）、`us_real_yield`（2003 起）、`us_short_yield`、`us_long_rate`、`us_tbill_discount`（2002 起）。**注意各子表起始年份各异，且 LPR 月更新非断更** |
| `imai_macro` | 全球宏观（6 表） | 1919~至今 | **6 表**（27.8 万行）：`macro_intl_daily`（美债/VIX/原油/汇率，含 `available_date`）、`macro_global_index_1d`（对应次日 A 股开盘）、`macro_us_indicators`（美国 FRED）、`macro_commodities`、`policy_events`（国内政策发布）、`market_policy_climate_daily`（日频政策气候汇总） |
| `imai_insthold` | ★中港美机构持仓（1.36 亿行） | 1987~至今 | **7 表**：`us_13f_holding`（13 列）、`us_13f_filing`（7 列申报台账）、`cn_inst_hold`（13 列 A 股机构持仓）、`cn_inst_top`（14 列机构十大流通）、`cn_fund_hold`（8 列公募全持仓）、`hk_sdi`（15 列港股大股东权益披露）、`inst_holding_panel`（17 列跨市场增减持面板）。**注意**：① `us_13f_holding.putcall` 94% 为空（普通股占主）；② **美股 13F 用 `cusip + issuer` 标识持仓标的，`cik` 是申报管理机构编号而非上市公司编号** |
| `imai_research` | 券商研报（结构化目录） | 2017~至今 | 17 列（15.0 万行）：东财研报评级、评级变动、`eps_y1/y2/y3` 与 `aim_price_high/low`（含 `available_date`）。**注：`eps_y1/y2/y3` 72%–95% 为空、`aim_price_*` 85% 为空**，推荐结合 `cn_estimates` 使用 |
| `imai_research_sina` | 卖方研报 · 正文全文 | 2005~至今 | 11 列（112.1 万行，篇均约 1,900 字）：`rptid, kind, pub_date, title, org, researcher, stock_code, stock_name, body, url`。**注：约 51% 为行业/策略研报，`stock_code` 与 `stock_name` 为空** |

---

## 3. 能源、农业、商品、航运、关税与贸易制裁（6 项）

| SKU | 中文名 | 历史 | 子表与内容概览 |
|---|---|---|---|
| `imai_eia` | 美国能源署 EIA 全量 | 1859~至今 | **2 表**（1,583 万行）：`eia_obs`（3 列 `series_id, period, val`）、`eia_series`（9 列含指标名、单位、频率、地区、分类；覆盖原油、成品油、天然气、电力、煤炭） |
| `imai_usda` | USDA 全球农产品供需（PSD） | 1960~至今 | 12 列（209.3 万行）：按市场年度记录各国主要农产品产量、消费、进出口、期末库存（`Commodity_Code, Country_Code, Market_Year, Attribute_ID, Value`） |
| `cmo_commodity` | 世界银行大宗商品价格 | 1960~至今 | **2 表**（5.4 万行月度）：`prices`（4 列分品种月价与单位）、`indices`（3 列分类商品指数） |
| `imai_shipping` | 航运指数（BDI 等） | 2001~至今 | 7 列（3.7 万行）：`index_code, index_name, ref_date, value, freq, source, fetched_at` |
| `intl_tariff` | 全球关税与贸易壁垒 | 2020~2026 | **5 表**（36.3 万行）：`tariff_hs6`（7 经济体 HS6 MFN 基准）、`tariff_line_official`（美欧税则行级当期税率）、`tariff_hs6_current`（美欧中 HS6 汇总对比）、`tariff_preferential_cn`（中国自贸协定优惠税率）、`trade_barrier_hs6`（17 列产品级贸易壁垒与 GTA 措施统计） |
| `intl_trade_measures` | 全球贸易干预与管制清单 | 1984~至今 | **4 表**（16.2 万行）：`sanctions`（美国 SDN/实体清单等 15 列，多字段空值率 51%–100%，实体名与来源完整）、`sanction_entity`（15 列）、`gta_interventions`（24 列 GTA 贸易干预措施，含 `STRUCT` 数组与 `date_announced/published/implemented`）、`gta_ip_measures`（14 列工业政策措施） |
