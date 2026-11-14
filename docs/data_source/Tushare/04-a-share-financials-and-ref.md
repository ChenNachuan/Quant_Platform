# 04. A 股财务报表与参考数据

[← 返回目录](README.md) · 对应官网「股票数据 → 财务数据（`doc_id=16`）& 参考数据（`doc_id=17`）」

---

## 1. 财务数据接口（10 项，`doc_id=16`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分 & 限制 |
|---|---|---|---|---|---|
| `33` | 利润表 | `income` | `ts_code`, `ann_date`, `f_ann_date`, `start_date`, `end_date`, `period`, `report_type`, `comp_type` | 94 列：`ts_code`, `ann_date`, `f_ann_date`, `end_date`, `report_type`, `comp_type`, `end_type`, `basic_eps`, `diluted_eps`, `total_revenue` 等 | 2,000 分起 |
| `36` | 资产负债表 | `balancesheet` | `ts_code`, `ann_date`, `start_date`, `end_date`, `period`, `report_type`, `comp_type` | 158 列：`ts_code`, `ann_date`, `f_ann_date`, `end_date`, `report_type`, `comp_type`, `total_share`, `cap_rese`, `undistr_porfit` 等 | 2,000 分起 |
| `44` | 现金流量表 | `cashflow` | `ts_code`, `ann_date`, `f_ann_date`, `start_date`, `end_date`, `period`, `report_type`, `comp_type`, `is_calc` | 97 列：`ts_code`, `ann_date`, `f_ann_date`, `end_date`, `comp_type`, `report_type`, `net_profit`, `finan_exp`, `c_fr_sale_sg` 等 | 2,000 分起 |
| `45` | 业绩预告 | `forecast` | `ts_code`, `ann_date`, `start_date`, `end_date`, `period`, `type` | 12 列：`ts_code`, `ann_date`, `end_date`, `type`, `p_change_min`, `p_change_max`, `net_profit_min`, `net_profit_max`, `last_parent_net`, `first_ann_date` | 2,000 分；3,500 行/次；每日 20–21 点 |
| `46` | 业绩快报 | `express` | `ts_code`, `ann_date`, `start_date`, `end_date`, `period` | 32 列：`ts_code`, `ann_date`, `end_date`, `revenue`, `operate_profit`, `total_profit`, `n_income`, `total_assets`, `diluted_eps` 等 | 2,000 分起 |
| `103` | 分红送股数据 | `dividend` | `ts_code`, `ann_date`, `record_date`, `ex_date`, `imp_ann_date` | 16 列：`ts_code`, `end_date`, `ann_date`, `div_proc`, `stk_div`, `stk_bo_rate`, `stk_co_rate`, `cash_div`, `cash_div_tax`, `record_date`, `ex_date` 等 | 2,000 分；2,000 行/次（2000-01-01 起） |
| `79` | 财务指标数据 | `fina_indicator` | `ts_code`, `ann_date`, `start_date`, `end_date`, `period` | 167 列：`eps`, `dt_eps`, `roe`, `roa`, `grossprofit_margin`, `debt_to_assets`, `q_eps` 等 | 2,000 分起 |
| `80` | 财务审计意见 | `fina_audit` | `ts_code`, `ann_date`, `start_date`, `end_date`, `period` | 7 列：`ts_code`, `ann_date`, `end_date`, `audit_result`, `audit_fees`, `audit_agency`, `audit_sign` | 2,000 分起 |
| `81` | 主营业务构成 | `fina_mainbz` | `ts_code`, `period`, `type`（`P`/`D`/`I`）, `start_date`, `end_date` | 9 列：`ts_code`, `end_date`, `bz_item`, `bz_code`, `bz_sales`, `bz_profit`, `bz_cost`, `curr_type`, `update_flag` | 2,000 分；**单次上限 100 行**（需循环） |
| `162` | 财报披露日期表 | `disclosure_date` | `ts_code`, `end_date`, `pre_date`, `ann_date`, `actual_date` | 6 列：`ts_code`, `ann_date`, `end_date`, `pre_date`, `actual_date`, `modify_date` | 2,000 分起；6,000 行/次 |

> [!TIP]
> **防未来函数与重复行**：三大表需使用实际公告日 `f_ann_date` / `ann_date` 对齐 PIT 时点（而非 `end_date`）；若同一 `end_date` + `report_type` 出现重复行，在 `fields` 中显式请求 `update_flag` 并优先保留 `update_flag=1`（修正后数据）。按季度全市场批量抽取可使用 VIP 接口（`income_vip` 等，需 5,000 积分）。

---

## 2. 参考数据接口（15 项，`doc_id=17`）

| `doc_id` | 名称 | `api_name` | 核心入参 | 输出字段数 / 核心字段 | 积分 & 单次上限 |
|---|---|---|---|---|---|
| `451` | 个股异常波动 | `stk_shock` | `ts_code`, `trade_date`, `start_date`, `end_date` | 6 列：`ts_code`, `trade_date`, `name`, `trade_market`, `reason`, `period` | 6,000 分；1,000 行/次 |
| `452` | 个股严重异常波动 | `stk_high_shock` | `ts_code`, `trade_date`, `start_date`, `end_date` | 6 列：`ts_code`, `trade_date`, `name`, `trade_market`, `reason`, `period` | 6,000 分；1,000 行/次 |
| `453` | 交易所重点提示证券 | `stk_alert` | `ts_code`, `trade_date`, `start_date`, `end_date` | 5 列：`ts_code`, `name`, `start_date`, `end_date`, `type` | 6,000 分；1,000 行/次 |
| `61` | 前十大股东 | `top10_holders` | `ts_code`, `period`, `ann_date`, `start_date`, `end_date` | 9 列：`ts_code`, `ann_date`, `end_date`, `holder_name`, `hold_amount`, `hold_ratio`, `hold_float_ratio`, `hold_change`, `holder_type` | 2,000 分起（5,000+ 高频） |
| `62` | 前十大流通股东 | `top10_floatholders` | `ts_code`, `period`, `ann_date`, `start_date`, `end_date` | 9 列（同 `top10_holders`） | 2,000 分起（5,000+ 高频） |
| `110` | 股权质押统计数据 | `pledge_stat` | `ts_code`, `end_date` | 7 列：`ts_code`, `end_date`, `pledge_count`, `unrest_pledge`, `rest_pledge`, `total_share`, `pledge_ratio` | 2,000 分起；1,000 行/次 |
| `111` | 股权质押明细数据 | `pledge_detail` | `ts_code`, `ann_date`, `start_date`, `end_date` | 14 列：`ts_code`, `ann_date`, `holder_name`, `pledge_amount`, `start_date`, `end_date`, `is_release`, `release_date`, `pledgor`, `holding_amount` 等 | 2,000 分起；1,000 行/次 |
| `124` | 股票回购 | `repurchase` | `ann_date`, `start_date`, `end_date` | 9 列：`ts_code`, `ann_date`, `end_date`, `proc`, `exp_date`, `vol`, `amount`, `high_limit`, `low_limit` | 2,000 分起 |
| `160` | 限售股解禁 | `share_float` | `ts_code`, `ann_date`, `float_date`, `start_date`, `end_date` | 7 列：`ts_code`, `ann_date`, `float_date`, `float_share`, `float_ratio`, `holder_name`, `share_type` | 120 分起（权限表 3,000 分）；6,000 行/次 |
| `161` | 大宗交易 | `block_trade` | `ts_code`, `trade_date`, `start_date`, `end_date` | 7 列：`ts_code`, `trade_date`, `price`, `vol`, `amount`, `buyer`, `seller` | 2,000 分起；1,000 行/次 |
| `164` | 股票开户数据（停） | `stk_account` | `date`, `start_date`, `end_date` | 5 列：`date`, `weekly_new`, `total`, `weekly_hold`, `weekly_trade` | 600 分（历史停更） |
| `165` | 股票开户数据（旧） | `stk_account_old` | `start_date`, `end_date` | 9 列：`date`, `new_sh`, `new_sz`, `active_sh`, `active_sz`, `total_sh`, `total_sz`, `trade_sh`, `trade_sz` | 600 分（历史停更） |
| `166` | 股东人数 | `stk_holdernumber` | `ts_code`, `ann_date`, `enddate`, `start_date`, `end_date` | 4 列：`ts_code`, `ann_date`, `end_date`, `holder_num` | 2,000 分（200 次/分）；3,000 行/次 |
| `175` | 股东增减持 | `stk_holdertrade` | `ts_code`, `ann_date`, `start_date`, `end_date`, `trade_type`, `holder_type` | 14 列：`ts_code`, `ann_date`, `holder_name`, `holder_type`, `in_de`, `change_vol`, `change_ratio`, `after_share`, `after_ratio`, `avg_price` 等 | 2,000 分起（5,000+ 无明显限制）；3,000 行/次 |
| `494` | 股票增发 | `stk_seasoned` | `ts_code`, `ann_date`, `start_date`, `end_date` | 69 列：`ts_code`, `ann_date`, `first_ann_date`, `board_approval_dt`, `fo_type`, `fo_stock_type` 等 | 2,000 分；3,000 行/次，1991 年起 |
