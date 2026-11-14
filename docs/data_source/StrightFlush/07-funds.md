# 07. 公募基金（ETF / LOF / OTC）

[← 返回目录](README.md)

基金标的使用完整 `thscode`（场外 `.OF`，场内 `.SH` / `.SZ`）。基金公司与基金经理接口分别以 `company_id`、`manager_id` 查询（可通过基本资料接口获取）。通用越界与类型不匹配分别返回 `1003` / `3004`。

---

## 1. 场内基金行情与 ETF 历史日线

| 接口 | REST 端点 / MCP | 关键参数与约束 |
|---|---|---|
| **场内基金快照** | `GET /api/fund/market/snapshot`<br>`get_fund_market_snapshot` | 支持 ETF 与 LOF。`thscode` 必填（单数参数名，逗号分隔 **1–100 个**代码）。返回 `last_price`、`open/high/low/prev`、`price_change`、`price_change_ratio_pct`、`price_amplitude_ratio_pct`、`volume`、`turnover`、`turnover_ratio_pct` |
| **ETF 历史日线** | `GET /api/fund/market/historical`<br>`get_fund_market_historical` | **仅支持 ETF**。`thscode`（单个）、`interval`（仅 `1d`）、`start`/`end`（毫秒戳，窗口 **<= 5 个自然年**）。前复权口径（响应中 `adjust` 固定为 `null`） |
| **ETF 批量历史日线** | `GET /api/fund/market/historical-batch`<br>`get_fund_market_historical_batch` | `thscodes`（逗号分隔 **最多 10 个**）、`interval=1d`、`start`/`end`（<= 5 年）。遵循原生批量语义 |

---

## 2. 净值、业绩与回撤

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **基金净值** | `GET /api/fund/performance/nav`<br>`get_fund_performance_nav` | `thscode`（必填）、`range`（可选：`week`/`month`/`tmonth`/`hyear`/`year`/`twoyear`/`tyear`/`fyear`，省略仅返最新 1 条）、`nav_type`（`unit`/`adj`/`unit,adj` 默认）。返回 `nav_date`、`unit_nav`、`adj_nav`（复权净值，**不等同**于累计净值） |
| **基金区间收益** | `GET /api/fund/performance/returns`<br>`get_fund_performance_returns` | `thscode`（必填）。返回近 1 周/1 月/3 月/半年/1 年/2 年/3 年/5 年/今年以来/成立以来收益率（`return_*`）、同类平均（`peer_average_*`）、同类排名（`rank_*`）与总基金数（`rank_total_*`） |
| **历史业绩指标** | `GET /api/fund/performance/indicators-historical`<br>`get_fund_performance_indicators_historical` | `thscode`、`start`、`end`（三者均必填）。返回 `date_ms`、`rsi_pct`（净值波动）、`donchian_channel`（趋势强弱）、`track_index_pe_ttm_five_year_percentile`（跟踪指数 PE TTM 5 年分位） |
| **基金最大回撤** | `GET /api/fund/performance/drawdowns`<br>`get_fund_performance_drawdowns` | `thscode`（必填）。返回 `week`/`month`/`tmonth`/`hyear`/`year`/`twoyear`/`tyear`/`fyear`/`nowyear`/`now` 区间最大回撤 |

---

## 3. 重仓、历史持仓与资产/行业配置

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **重仓持仓** | `GET /api/fund/portfolio/holdings`<br>`get_fund_portfolio_holdings` | `thscode`（必填）。汇总字段 `total_stock_ratio_pct`、`total_bond_ratio_pct`、`total_fund_ratio_pct`、`turnover_rate_pct`、`main_industry`、`concentration_ratio`；明细 `item[]` 含股票/债券/基金持仓与占比 |
| **历史股票持仓** | `GET /api/fund/portfolio/stock-history`<br>`get_fund_portfolio_stock_history` | `thscode`、`report_type`（如 `quarter`）、`end_date` (`yyyy-MM-dd`)。返回 `hold_ratio`、`market_value`、`period_increase_pct`、`rank`（仅前 10 返回 `1..10`，其余 `null`） |
| **历史债券持仓** | `GET /api/fund/portfolio/bond-history`<br>`get_fund_portfolio_bond_history` | 参数与结构同 `stock-history`（`asset_type` 为 `bond`） |
| **持仓报告日期** | `GET /api/fund/portfolio/stock-report-dates`<br>`GET /api/fund/portfolio/bond-report-dates` | `thscode`（必填）、`report_type`（可选）。返回可用报告期起止时间与名称；MCP 为 `get_fund_portfolio_{stock,bond}_report_dates` |
| **资产配置** | `GET /api/fund/portfolio/asset-allocation`<br>`get_fund_portfolio_asset_allocation` | `thscode`（必填）。返回 `report_date_ms`、`stock_ratio_pct`、`bond_ratio_pct`、`deposit_ratio_pct`、`other_ratio_pct` |
| **行业配置** | `GET /api/fund/portfolio/industry-allocation`<br>`get_fund_portfolio_industry_allocation` | `thscode`（必填）。返回 `report_period`、`industry_name`、`ratio_pct` |

---

## 4. 基本资料、公司、分红、持有人与财务

| 接口 | REST 端点 / MCP | 参数与核心返回 |
|---|---|---|
| **基金基本资料** | `GET /api/fund/profile/detail`<br>`get_fund_profile_detail` | `thscode`。返回名称、成立日、`company_id`、`mgmt_name`、`fund_scale`、`unit_nav`、`manager_info[]`（含 `manager_id` 任期业绩）、`trade_rule[]`、`rate_info[]`（费率折扣） |
| **基金公司详情** | `GET /api/fund/companies/detail`<br>`get_fund_companies_detail` | `company_id`。返回公司名称、类型、成立日、`fund_count`、`scale` |
| **基金分红记录** | `GET /api/fund/corporate-actions/dividends`<br>`get_fund_corporate_actions_dividends` | `thscode`。返回 `dividend_count`、`dividend_total` 及每次每 10 份税前/税后分红与登记/除息/派息日 |
| **持有人结构** | `GET /api/fund/holders/detail`<br>`get_fund_holders_detail` | `thscode`、`merge_scope` (`all`/`merged`/`separate`)。返回机构/个人/员工持有占比及户数、户均份额 |
| **前十大持有人** | `GET /api/fund/holders/top`<br>`get_fund_holders_top` | `thscode`、`limit`（<= `10`）。返回 `holder_name`、`holder_type`、`rank`、`hold_share`、`hold_rate_pct` |
| **基金财务指标** | `GET /api/fund/financials/indicators`<br>`get_fund_financials_indicators` | `thscode`。返回可分配利润、本期利润/收入、单位净值、累计净值增长率等 |
| **基金利润表** | `GET /api/fund/financials/income-statements`<br>`get_fund_financials_income_statements` | `thscode`。返回股/债/基投资收益、公允价值变动、管理费/托管费/交易成本、净利润 |
| **基金资产负债表** | `GET /api/fund/financials/balance-sheets`<br>`get_fund_financials_balance_sheets` | `thscode`。返回银行存款、股/债/基投资、交易性金融资产、总资产/总负债/所有者权益 |

---

## 5. 基金经理、诊断、募集、资讯、QDII 与回测

| 接口 | REST 端点 / MCP | 参数与核心返回 |
|---|---|---|
| **基金经理 (4 端点)** | `GET /api/fund/managers/{investment-style,performance,experience,detail}`<br>`get_fund_managers_*` | 均传 `manager_id`；`performance` 另必传 `range` (`month`/`tmonth`/`year`/`nowyear`/`now`) |
| **基金诊断详情** | `GET /api/fund/diagnostics/detail`<br>`get_fund_diagnostics_detail` | `thscode`。返回 `dimensions`、`peer_dimensions`、`probabilities`、`ranges`、`resilience`、`peer_resilience` |
| **基金募集列表** | `GET /api/fund/offerings/list`<br>`get_fund_offerings_list` | `subscribe`（必填 `active`/`upcoming`）。返回 `thscode`、`subscription_start_ms`、`subscription_end_ms` |
| **基金资讯列表** | `GET /api/fund/news/article-list`<br>`get_fund_news_article_list` | `thscode`、`limit`、`offset`（游标分页，下一页回传 `data.offset`，以 `has_more` 判尾） |
| **QDII 额度汇总/列表** | `GET /api/fund/quota/summary`<br>`GET /api/fund/quota/list` | `tab`（URL 编码 JSON 数组字符串：`summary` 用 `nazhi100`/`meigukeji`/`biaopu`/`yatai`/`ouzhou`；`list` 用 `remen`/`meiguo`/`yatai`/`ouzhou`，可选 `buy` 布尔过滤）。MCP：`get_fund_quota_{summary,list}` |
| **在线回测与指标** | `GET /api/fund/backtest/result`<br>`GET /api/fund/backtest/indicators` | `result` 必传 `thscode`、`buy_conditions`/`sell_conditions`（JSON 字符串）、`buy_frequency_type`、`max_buy_times`、`per_buy_amount`；`indicators` 查询支持指标编码。MCP：`get_fund_backtest_{result,indicators}` |

> 注：宽基 ETF 申购净流入 `/api/fund/etf-flow/net-inflow-historical` 当前仅限同花顺 AI 客户端内访问，详见 [11-ai-client-only.md](11-ai-client-only.md)。
