# 04. A 股基本面、股本与交易异动（`InfoData`）

`info = ad.InfoData()`。除少数无参接口外，所有方法均接受 `code_list: list[str]`、`local_path: str`（必填绝对路径）与 `is_local: bool = True`；多数历史序列接口额外支持可选 `begin_date: int` 与 `end_date: int`（本地缓存增量方案）。

> **返回类型与官网口径差异提示**：
> - `https://amazing.ptradeapi.com/` 官网将**三大财报（`get_balance_sheet` / `get_income` / `get_cash_flow`）、`get_margin_detail`、限售解禁与股权质押**标注为返回 `dict[code -> DataFrame]`；其余返回单个 `pd.DataFrame`。
> - 部分早先 SDK 版本/摘要中财报与两融明细亦可能直接返回合并后的 `DataFrame`（以 `MARKET_CODE` 区分标的），调用侧建议兼容 `isinstance(res, dict)`。
> - **官网原文排版勘误**：官网 `3.5.5.5` 业绩预告页面下方误贴了 `equity_restricted` 字段表，且正文跳过了 `3.5.6` 节（股本结构与股东数据）；本篇已整合早先详细分篇予以补齐。

---

## 1. 证券基础资料与历史状态

| 方法 | 入参 | 返回值 | 核心字段说明 |
|---|---|---|---|
| `get_stock_basic` | `code_list`（沪深北含退市；无需 `local_path`） | `DataFrame` | `MARKET_CODE`, `SECURITY_NAME`, `COMP_NAME`, `PINYIN`, `COMP_NAME_ENG`, `COMP_SNAME_ENG`, `LISTDATE`, `DELISTDATE`, `LISTPLATE_NAME`, `IS_LISTED`（`1` 上市 / `3` 终止上市） |
| `get_history_stock_status` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date`（**交易日**） | `DataFrame` | 日频状态：`MARKET_CODE`, `TRADE_DATE`, `PRECLOSE`, `HIGH_LIMITED`, `LOW_LIMITED`, `PRICE_HIGH_LMT_RATE`, `PRICE_LOW_LMT_RATE`, `IS_ST_SEC`, `IS_SUSP_SEC`, `IS_WD_SEC`, `IS_XR_SEC`（`1`/`0`） |
| `get_bj_code_mapping` | `local_path`, `is_local`（无需 `code_list`） | `DataFrame` | 北交所新旧代码对照：`OLD_CODE`, `NEW_CODE`, `SECURITY_NAME`, `LISTING_DATE` |

---

## 2. 财务报表与业绩披露

### 2.1 三大财报（支持可选 `begin_date` / `end_date`，按**报告期**过滤）

| 方法 | 入参 | 返回值 | 公共头字段与关键标识 |
|---|---|---|---|
| `get_balance_sheet` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date` | `dict[code -> DataFrame]`\* | `MARKET_CODE`, `SECURITY_NAME`, `STATEMENT_TYPE`, `REPORT_TYPE`, `REPORTING_PERIOD`, `ANN_DATE`, `ACTUAL_ANN_DATE`, `COMP_TYPE_CODE`（`1` 非金融 / `2` 银行 / `3` 保险 / `4` 证券）及资产负债科目 |
| `get_income` | 同上 | `dict[code -> DataFrame]`\* | 同上公共头 + `BASIC_EPS`, `DILUTED_EPS`, `EBIT`, `EBITDA`, `TOT_OPERA_REV`, `TOT_OPERA_COST`, `TOTAL_PROFIT`, `NET_PRO_EXCL_MIN_INT_INC`, `NET_PRO_AFTER_DED_NR_GL`, `CONTINUED_NET_OPERA_PRO` 等 |
| `get_cash_flow` | 同上 | `dict[code -> DataFrame]`\* | 同上公共头 + 经营 / 投资 / 筹资活动现金流及 `FREE_CASH_FLOW` 等附注科目 |

**`STATEMENT_TYPE` 报表类型代码**：`1` 合并报表、`2` 合并单季、`3` 合并单季调整、`4` 合并调整、`5` 合并更正前、`6` 母公司、`7` 母公司单季、`8` 母公司单季调整、`9` 母公司调整、`10` 母公司更正前、`11` 合并（未公开）、`12` 合并调整（未公开）。

### 2.2 业绩快报与预告（支持可选 `begin_date` / `end_date`，按**报告期**过滤）

- **`get_profit_express(code_list, local_path, is_local=True, begin_date=None, end_date=None)`** → `DataFrame`：含 `REPORTING_PERIOD`, `ANN_DATE`, `ACTUAL_ANN_DATE`, `TOTAL_ASSETS`, `NET_PRO_EXCL_MIN_INT_INC`, `TOT_OPERA_REV`, `TOTAL_PROFIT`, `OPERA_PROFIT`, `EPS_BASIC`, `ROE_WEIGHTED`, `NET_ASSET_PS`, `IS_AUDIT` 及同比增长率（`YOY_GR_*`）。
- **`get_profit_notice(code_list, local_path, is_local=True, begin_date=None, end_date=None)`** → `DataFrame`：含 `P_TYPECODE`（`1` 不确定 ~ `11` 持平，含 `4` 扭亏 / `6` 首亏 / `9` 预减 / `10` 预增）、`REPORT_TYPE`（`1`–`4`）、`P_CHANGE_MAX`/`MIN`（%）、`NET_PROFIT_MAX`/`MIN`（万元）、`P_REASON`、`P_SUMMARY`、`FIRST_ANN_DATE`。

---

## 3. 股本结构与股东数据

| 方法 | 返回类型 | 核心字段 |
|---|---|---|
| `get_equity_structure` | `DataFrame` | `MARKET_CODE`, `ANN_DATE`, `CHANGE_DATE`, `EX_CHANGE_DATE`, `SHARE_CHANGE_REASON_STR`, `CURRENT_SIGN`, `IS_VALID`, `TOT_SHARE`, `FLOAT_SHARE`, `FLOAT_A_SHARE`, `FLOAT_B_SHARE`, `FLOAT_HK_SHARE`（万股） |
| `get_holder_num` | `DataFrame` | `MARKET_CODE`, `ANN_DT`, `HOLDER_ENDDATE`, `HOLDER_TOTAL_NUM`（全市场合计）, `HOLDER_NUM`（A 股户数） |
| `get_share_holder` | `DataFrame` / `dict` | 十大股东持股明细 |
| `get_equity_restricted` | `dict[code -> DataFrame]` | `MARKET_CODE`, `LIST_DATE`, `SHARE_RATIO`（%）, `SHARE_LST_TYPE_NAME`, `SHARE_LST`（股）, `SHARE_LST_IS_ANN`（`0` 预测 / `1` 公布）, `CLOSE_PRICE`, `SHARE_LST_MARKET_VALUE` |
| `get_equity_pledge_freeze` | `dict[code -> DataFrame]` | `ANN_DATE`, `HOLDER_NAME`, `HOLDER_TYPE_CODE`（`2` 公司 / `3` 个人）, `TOTAL_HOLDING_SHR`, `FRO_SHARES`, `TOTAL_PLEDGE_SHR`, `IS_EQUITY_PLEDGE_REPO`, `BEGIN_DATE`, `END_DATE`, `IS_DISFROZEN`, `FROZEN_INSTITUTION`, `FREEZE_TYPE`（`1` 质押 / `2` 司法 / `3` 质押回购） |

---

## 4. 公司行为：分红与配股（可选 `begin_date` / `end_date` 按**公告日期**）

- **`get_dividend(code_list, local_path, is_local=True, begin_date=None, end_date=None)`** → `DataFrame`：含 `DIV_PROGRESS`（`1` 董事会预案、`2` 股东大会通过、`3` 实施、`4` 未通过、`12` 停止实施、`17` 股东提议、`19` 预披露）、`DVD_PER_SHARE_STK`, `DVD_PER_SHARE_PRE_TAX_CASH`, `DVD_PER_SHARE_AFTER_TAX_CASH`, `DATE_EQY_RECORD`, `DATE_EX`, `DATE_DVD_PAYOUT`, `LISTINGDATE_OF_DVD_SHR`, `DIV_PRELANDATE`, `DIV_SMTGDATE`, `DATE_DVD_ANN`, `DIV_BASEDATE`, `DIV_BASESHARE`（万股）, `IS_CHANGED`, `REPORT_PERIOD`, `DIV_CHANGE`, `DIV_BONUSRATE`, `DIV_CONVERSEDRATE`, `DIV_PREANN_DATE`, `DIV_TARGET`。
- **`get_right_issue(code_list, local_path, is_local=True, begin_date=None, end_date=None)`** → `DataFrame`：含 `PROGRESS`, `PRICE`（元）, `RATIO`, `RATIO_DENOMINATOR`, `RATIO_MOLECULAR`, `AMT_PLAN` / `AMT_REAL`（万股）, `COLLECTION_FUND` / `EXPECTED_FUND_RAISING`（元）, `RIGHTSISSUE_CODE`, `RIGHTSISSUE_NAME`, `RIGHTSISSUE_YEAR`, `SHAREB_REG_DATE`, `EX_DIVIDEND_DATE`, `LISTED_DATE`, `PAY_START_DATE`, `PAY_END_DATE`, `PREPLAN_DATE`, `SMTG_ANN_DATE`, `PASS_DATE`, `APPROVED_DATE`, `EXECUTE_DATE`, `RESULT_DATE`, `LIST_ANN_DATE`, `SUBS_METHOD`。

---

## 5. 两融、龙虎榜与大宗交易（可选 `begin_date` / `end_date` 按**交易日**）

| 方法 | 入参 | 返回类型 | 核心字段 |
|---|---|---|---|
| `get_margin_summary` | `local_path`, `is_local`, 可选 `begin_date`/`end_date`（全市场） | `DataFrame` | `TRADE_DATE`, `SUM_BORROW_MONEY_BALANCE`, `SUM_PURCH_WITH_BORROW_MONEY`, `SUM_REPAYMENT_OF_BORROW_MONEY`, `SUM_SEC_LENDING_BALANCE`, `SUM_SALES_OF_BORROWED_SEC`, `SUM_MARGIN_TRADE_BALANCE` |
| `get_margin_detail` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date` | `dict[code -> DataFrame]`\* | `MARKET_CODE`, `SECURITY_NAME`, `TRADE_DATE`, `BORROW_MONEY_BALANCE`, `PURCH_WITH_BORROW_MONEY`, `REPAYMENT_OF_BORROW_MONEY`, `SEC_LENDING_BALANCE`, `SALES_OF_BORROWED_SEC`, `REPAYMENT_OF_BORROW_SEC`, `SEC_LENDING_BALANCE_VOL`, `MARGIN_TRADE_BALANCE` |
| `get_long_hu_bang` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date` | `DataFrame` | `MARKET_CODE`, `TRADE_DATE`, `SECURITY_NAME`, `REASON_TYPE`, `REASON_TYPE_NAME`, `CHANGE_RANGE`, `TRADER_NAME`, `BUY_AMOUNT`, `SELL_AMOUNT`, `FLOW_MARK`（`1` 买 / `2` 卖）, `TOTAL_AMOUNT`（元）, `TOTAL_VOLUME`（万股） |
| `get_block_trading` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date` | `DataFrame` | `TRADE_DATE`, `MARKET_CODE`, `B_SHARE_PRICE`（元）, `B_SHARE_VOLUME`（万股）, `B_FREQUENCY`（笔数）, `B_SHARE_AMOUNT`（万元）, `BLOCK_AVG_VOLUME`, `B_BUYER_NAME`, `B_SELLER_NAME` |
