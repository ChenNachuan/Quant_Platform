# 03. PIT 财务报表、业绩事件与一致预期（7 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)
>
> 说明：统一采用 `cn_stock_fundamental_*` 前缀。财务/事件类表以 `ann_date`（或 `visible_from`）作为 PIT（Point-In-Time）可见边界，防止回测未来函数。

---

## 1. `cn_stock_fundamental_income`（PIT 利润表）

- **路径**：`asset/cn_stock_fundamental_income/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[ticker, report_date, ann_date, statement_type]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`basic_eps`, `diluted_eps`, `total_revenue`, `revenue`, `total_cogs`, `operate_profit`, `total_profit`, `net_profit`, `net_profit_attr_p`, `net_profit_dedt`, `rd_exp`, `ebit`, `ebitda`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `report_date` | `pa.Date` | N | 会计报告期期末日 |
| `ann_date` | `pa.Date` | N | 实际公告日（PIT 可见起点） |
| `f_ann_date` | `pa.Date` | Y | 首次公告日 |
| `statement_type` | `str` | N | 报表类型（如合并/单季/调整等） |
| `comp_type` | `str` | Y | 公司类型（一般工商业/银行/保险/证券） |
| `basic_eps` | `float` | Y | 基本每股收益 |
| `diluted_eps` | `float` | Y | 稀释每股收益 |
| `total_revenue` | `float` | Y | 营业总收入 |
| `revenue` | `float` | Y | 营业收入 |
| `total_cogs` | `float` | Y | 营业总成本 |
| `operate_profit` | `float` | Y | 营业利润 |
| `total_profit` | `float` | Y | 利润总额 |
| `net_profit` | `float` | Y | 净利润 |
| `net_profit_attr_p` | `float` | Y | 归母净利润 |
| `net_profit_dedt` | `float` | Y | 扣非归母净利润 |
| `rd_exp` | `float` | Y | 研发费用 |
| `ebit` | `float` | Y | 息税前利润 |
| `ebitda` | `float` | Y | 息税折旧摊销前利润 |

---

## 2. `cn_stock_fundamental_balance`（PIT 资产负债表）

- **路径**：`asset/cn_stock_fundamental_balance/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[ticker, report_date, ann_date, statement_type]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`total_assets`, `total_cur_assets`, `total_nca`, `money_cap`, `accounts_receiv`, `inventories`, `total_liab`, `total_cur_liab`, `total_ncl`, `total_hldr_eqy_exc_min_int`, `total_hldr_eqy_inc_min_int`, `total_share`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `report_date` | `pa.Date` | N | 会计报告期期末日 |
| `ann_date` | `pa.Date` | N | 实际公告日 |
| `f_ann_date` | `pa.Date` | Y | 首次公告日 |
| `statement_type` | `str` | N | 报表类型 |
| `comp_type` | `str` | Y | 公司类型 |
| `total_assets` | `float` | Y | 资产总计 |
| `total_cur_assets` | `float` | Y | 流动资产合计 |
| `total_nca` | `float` | Y | 非流动资产合计 |
| `money_cap` | `float` | Y | 货币资金 |
| `accounts_receiv` | `float` | Y | 应收账款 |
| `inventories` | `float` | Y | 存货 |
| `total_liab` | `float` | Y | 负债合计 |
| `total_cur_liab` | `float` | Y | 流动负债合计 |
| `total_ncl` | `float` | Y | 非流动负债合计 |
| `total_hldr_eqy_exc_min_int` | `float` | Y | 归母所有者权益合计 |
| `total_hldr_eqy_inc_min_int` | `float` | Y | 所有者权益合计（含少数股东） |
| `total_share` | `float` | Y | 期末总股本 |

---

## 3. `cn_stock_fundamental_cashflow`（PIT 现金流量表）

- **路径**：`asset/cn_stock_fundamental_cashflow/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[ticker, report_date, ann_date, statement_type]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata` → `straightflush`
- **交叉比对数值列**：`n_cashflow_act`, `n_cashflow_inv_act`, `n_cash_flows_fnc_act`, `c_fr_sale_sg`, `c_paid_to_for_empl`, `c_pay_acq_const_fiolta`, `free_cashflow`, `n_incr_cash_cash_equ`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `report_date` | `pa.Date` | N | 会计报告期期末日 |
| `ann_date` | `pa.Date` | N | 实际公告日 |
| `f_ann_date` | `pa.Date` | Y | 首次公告日 |
| `statement_type` | `str` | N | 报表类型 |
| `comp_type` | `str` | Y | 公司类型 |
| `n_cashflow_act` | `float` | Y | 经营活动现金流净额 |
| `n_cashflow_inv_act` | `float` | Y | 投资活动现金流净额 |
| `n_cash_flows_fnc_act` | `float` | Y | 筹资活动现金流净额 |
| `c_fr_sale_sg` | `float` | Y | 销售商品提供劳务收到现金 |
| `c_paid_to_for_empl` | `float` | Y | 支付给职工以及为职工支付的现金 |
| `c_pay_acq_const_fiolta` | `float` | Y | 购建固定资产无形资产支付现金（CapEx） |
| `free_cashflow` | `float` | Y | 企业自由现金流 |
| `n_incr_cash_cash_equ` | `float` | Y | 现金及现金等价物净增加额 |

---

## 4. `cn_stock_fundamental_indicator`（PIT 财务衍生指标与审计意见）

- **路径**：`asset/cn_stock_fundamental_indicator/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[ticker, report_date, ann_date]`
- **数据源优先级**：`tushare` → `qdc` → `straightflush`
- **交叉比对数值列**：`roe`, `roe_waa`, `roe_dt`, `roa`, `roic`, `grossprofit_margin`, `netprofit_margin`, `debt_to_assets`, `current_ratio`, `quick_ratio`, `assets_turn`, `netprofit_yoy`, `or_yoy`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `report_date` | `pa.Date` | N | 会计报告期期末日 |
| `ann_date` | `pa.Date` | N | 公告日 |
| `roe` | `float` | Y | 净资产收益率 |
| `roe_waa` | `float` | Y | 加权平均净资产收益率 |
| `roe_dt` | `float` | Y | 扣非后净资产收益率 |
| `roa` | `float` | Y | 总资产报酬率 |
| `roic` | `float` | Y | 投入资本回报率 |
| `grossprofit_margin` | `float` | Y | 销售毛利率 |
| `netprofit_margin` | `float` | Y | 销售净利率 |
| `debt_to_assets` | `float` | Y | 资产负债率 |
| `current_ratio` | `float` | Y | 流动比率 |
| `quick_ratio` | `float` | Y | 速动比率 |
| `assets_turn` | `float` | Y | 总资产周转率 |
| `netprofit_yoy` | `float` | Y | 净利润同比增长率 |
| `or_yoy` | `float` | Y | 营业收入同比增长率 |
| `audit_result` | `str` | Y | 财务审计意见 |

---

## 5. `cn_stock_fundamental_segment`（主营业务分部构成）

- **路径**：`asset/cn_stock_fundamental_segment/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[ticker, report_date, segment_type, segment_name]`
- **数据源优先级**：`tushare` → `qdc`
- **交叉比对数值列**：`bz_sales`, `bz_profit`, `bz_cost`, `sales_ratio`, `gross_margin`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `report_date` | `pa.Date` | N | 报告期 |
| `ann_date` | `pa.Date` | Y | 公告日 |
| `segment_type` | `str` | N | 划分维度（按行业/产品/地区） |
| `segment_name` | `str` | N | 分部名称 |
| `bz_sales` | `float` | Y | 主营业务收入 |
| `bz_profit` | `float` | Y | 主营业务利润 |
| `bz_cost` | `float` | Y | 主营业务成本 |
| `sales_ratio` | `float` | Y | 占总营收比重 |
| `gross_margin` | `float` | Y | 分部毛利率 |

---

## 6. `cn_stock_fundamental_earnings`（业绩预告、快报与披露日程）

- **路径**：`asset/cn_stock_fundamental_earnings/{year}.parquet`（按年）
- **family**：`stock`
- **主键**：`[ticker, report_date, ann_date, event_kind]`
- **数据源优先级**：`tushare` → `qdc` → `amazingdata`
- **交叉比对数值列**：`p_change_min`, `p_change_max`, `net_profit_min`, `net_profit_max`, `revenue`, `net_profit`, `eps`, `roe`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `report_date` | `pa.Date` | N | 目标报告期 |
| `ann_date` | `pa.Date` | N | 公告日期 |
| `event_kind` | `str` | N | `isin(["forecast", "express", "schedule"])` |
| `forecast_type` | `str` | Y | 预告类型（预增/略增/扭亏/续亏/首亏等） |
| `p_change_min` | `float` | Y | 预计净利润变动下限（%） |
| `p_change_max` | `float` | Y | 预计净利润变动上限（%） |
| `net_profit_min` | `float` | Y | 预计净利润下限 |
| `net_profit_max` | `float` | Y | 预计净利润上限 |
| `revenue` | `float` | Y | 快报营业收入 |
| `net_profit` | `float` | Y | 快报归母净利润 |
| `eps` | `float` | Y | 快报每股收益 |
| `roe` | `float` | Y | 快报净资产收益率 |
| `pre_date` | `pa.Date` | Y | 预约披露日期 |
| `actual_date` | `pa.Date` | Y | 实际披露日期 |

---

## 7. `cn_stock_fundamental_consensus`（PIT 卖方一致预期）

- **路径**：`asset/cn_stock_fundamental_consensus/{month}.parquet`（按自然月）
- **family**：`stock`
- **主键**：`[ticker, trade_date]`
- **数据源优先级**：`qdc` → `tushare`（QDC `cn_estimates` 为主源）
- **交叉比对数值列**：`eps_fy0`, `eps_fy1`, `eps_fy2`, `eps_fwd12m`, `eps_fy1_n`, `eps_fy1_std`, `rating_score`, `tp_median`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 截面日期 |
| `visible_from` | `pa.Date` | Y | PIT 最早可见日期 |
| `eps_fy0` | `float` | Y | 当年一致预期 EPS |
| `eps_fy1` | `float` | Y | 次年一致预期 EPS |
| `eps_fy2` | `float` | Y | 后年一致预期 EPS |
| `eps_fwd12m` | `float` | Y | 未来 12 个月一致预期 EPS |
| `eps_fy1_n` | `float` | Y | FY1 覆盖预测机构家数（可空整型约定 `float`） |
| `eps_fy1_std` | `float` | Y | FY1 EPS 预测标准差（分歧度） |
| `eps_fy1_up_3m` | `float` | Y | 近 3 个月上调家数 |
| `eps_fy1_down_3m` | `float` | Y | 近 3 个月下调家数 |
| `rating_score` | `float` | Y | 综合评级得分 |
| `tp_median` | `float` | Y | 一致目标价中位数 |
| `quality_flag` | `int` | N | `0 <= x < 32` |
