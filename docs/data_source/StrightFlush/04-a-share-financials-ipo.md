# 04. A 股财务、估值与新股 IPO

[← 返回目录](README.md)

---

## 1. 三大合并财务报表（单标 & 批量）

| 报表 | 单标的端点 / MCP | 批量端点 / MCP（最多 10 标） |
|---|---|---|
| **合并利润表** | `GET /api/a-share/financials/income-statements`<br>`get_a_share_financials_income_statements` | `GET /api/a-share/financials/income-statements-batch`<br>`get_a_share_financials_income_statements_batch` |
| **资产负债表** | `GET /api/a-share/financials/balance-sheets`<br>`get_a_share_financials_balance_sheets` | `GET /api/a-share/financials/balance-sheets-batch`<br>`get_a_share_financials_balance_sheets_batch` |
| **现金流量表** | `GET /api/a-share/financials/cash-flow-statements`<br>`get_a_share_financials_cash_flow_statements` | `GET /api/a-share/financials/cash-flow-statements-batch`<br>`get_a_share_financials_cash_flow_statements_batch` |

### 共有参数与互斥规则

- `thscode`（单标）/ `thscodes`（批量，逗号分隔，去重前 <= 10）。
- `period`（必填）：`annual`（仅 Q4 年报，默认）或 `quarterly`（季末全报告期）。
- **取数模式二选一（混用 `limit` 与 `start/end` 或传半开区间均返回 `code=1004`）**：
  1. **最近 N 期**：`limit`（默认 `4`，范围 `[1, 20]`），按 `period_end` 降序返回。
  2. **时间区间**：成对传入毫秒戳 `start` 与 `end`（跨度 **<= 10 个自然年**），按 `period_end` 降序返回。

### 报表返回字段

- **共有元数据**：`thscode`、`ticker`、`period`、`fiscal_year`、`fiscal_period`（`FY`/`Q1`/`Q2`/`Q3`/`Q4`）、`report_date_ms`（披露日）、`period_end_ms`（报告期末）、`currency`（`CNY`）。
- **利润表核心字段**：`operating_income`、`operating_costs`、`operating_expenses`、`sales_fee`、`manage_fee`、`research_and_development_expenses`、`operating_profit`、`interest_expenses`、`profit_total`、`income_tax_expense`、`net_profit`、`parent_holder_net_profit`、`basic_eps`（元/股）。
- **资产负债表核心字段**：`assets_total`、`total_current_assets`、`non_current_nets_total`、`cash`、`accounts_receivable`、`total_debt`、`holder_equity_total`。
- **现金流量表核心字段**：`act_cash_flow_net`、`invest_cash_flow_net`、`financing_cash_flow_net`、`pay_fixed_assets_etc_cash`、`pay_dividends_profits_interest_cash`、`cash_equivalents_net_addition`。

---

## 2. 财务指标数据

`GET /api/a-share/financials/indicators` · MCP: `get_a_share_financials_indicators`

一次返回单只股票指定报告期的五类财务指标（`thscode` 必填，`report` 必填且格式为 `yyyy-1` / `yyyy-2` / `yyyy-3` / `yyyy-4`，分别对应一季报、中报、三季报、年报）。

- **注意数值类型**：`abilities[].indicators[].value` 返回**原始数值字符串或 `null`**（保留数据源原始小数精度），百分比类以百分数表达（如 `"89.12000000"` 表示 `89.12%`）。

| 能力类别 (`ability`) | 包含指标 (`index_id`) |
|---|---|
| **成长能力 `growth`** | `total_assets_growth_ratio`（总资产增长率）、`net_profit_yoy_growth_ratio`（净利润同比）、`operating_income_yoy_growth_ratio`（营收同比）、`operating_profit_yoy_growth_ratio`（营业利润同比） |
| **盈利能力 `profitability`** | `sale_gross_margin`（毛利率）、`sale_net_interest_ratio`（净利率）、`total_assets_net_ratio`（总资产收益率）、`index_deduct_weighted_avg_roe`（扣非加权 ROE）、`index_weighted_avg_roe`（加权 ROE） |
| **偿债能力 `solvency`** | `current_ratio`（流动比率）、`quick_ratio`（速动比率）、`assets_debt_ratio`（资产负债率）、`cash_ratio`（现金比率）、`earned_interest_multiple`（已获利息倍数） |
| **营运能力 `operation`** | `long_term_debt_equity_ratio`（长期负债权益比）、`total_assets_turnover_ratio`（总资产周转率）、`inventory_turnover_ratio`（存货周转率）、`current_assets_turnover_ratio`（流动资产周转率）、`receive_account_turnover_ratio`（应收账款周转率） |
| **现金流 `cash-flow`** | `cash_operating_index`（现金营运指数）、`operating_cash_flow_net_divide_income`（销售现金比率）、`net_profit_cash_content`（净利润现金含量）、`operating_cash_net_yoy_growth_ratio`（现金流净额同比）、`cash_meet_invest_ratio`（现金满足投资比率） |

---

## 3. A 股最新估值快照

`GET /api/a-share/valuations/snapshot` · MCP: `get_a_share_valuations_snapshot`

- **参数**：`thscodes`（必填，逗号分隔最多 **100 个** A 股 `.SH`/`.SZ`/`.BJ` 代码）。
- **返回字段 (`data.item[]`)**：`thscode`、`ticker`、`name`、`pe_ttm`（市盈率 TTM）、`pe_mrq`（市盈率 MRQ）、`pb_mrq`（市净率 MRQ）、`ps_ttm`（市销率 TTM）、`pcf_ttm`（市现率 TTM）。
- **语义说明**：固定仅返回当前最新快照（无历史估值接口）；负值保留，上游缺失返回 `null` 不补零；`data.timestamp` 为所用指标元数据中的最大有效时间。

---

## 4. 新股 IPO（A 股 & 港股）

### A 股打新日历
- **端点**：`GET /api/a-share/ipo/calendar-list` · MCP: `get_a_share_ipo_calendar_list`
- **必填参数**：`market` (`sh_sz` / `bj`)、`status` (`upcoming` / `subscribable` / `awaiting_listing`)
- **返回字段**：`thscode`、`ticker`、`name`、`subscription_code`、`subscription_date`、`listing_date`、`issue_price`（元，未定为 `null`）

### 港股打新（3 个接口）

| 接口 | REST 端点 / MCP | 参数 | 核心返回字段 |
|---|---|---|---|
| **递表记录** | `GET /api/hk-share/ipo/application-list`<br>`get_hk_share_ipo_application_list` | `page`（默认 `1`） | `company_name`、`application_date` + `pagination`（保留重复递表） |
| **认购与待上市日历** | `GET /api/hk-share/ipo/calendar-list`<br>`get_hk_share_ipo_calendar_list` | `status`（`subscribable` / `awaiting_listing`） | `issue_code`、`thscode`、`name`、`listing_date`、`issue_price`、`currency` (`HKD`)、`subscription_end_date`、`entry_fee`、`issue_price_text` |
| **新股发行详情** | `GET /api/hk-share/ipo/issue-detail`<br>`get_hk_share_ipo_issue_detail` | `issue_code`（必填，<= 32 字符，如 `HK2636`） | `issue_code`、`currency` (`HKD`)、`issue_price_range` (`{lower, upper}`)、`lot_size`（每手股数）、`entry_fee` |
