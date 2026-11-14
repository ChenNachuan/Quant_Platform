# 05. ETF、指数、可转债、国债与期权

除 `BaseData().get_etf_pcf` 外，本篇其余接口均挂载于 `info = ad.InfoData()`，缓存方法支持 `local_path: str` 与 `is_local: bool = True`（涉及历史剔除日期动态更新的接口，首次同步需设 `is_local=False`）。

---

## 1. ETF 数据

### 1.1 `BaseData().get_etf_pcf(code_list)`（每日最新申赎与成分股）

- **返回二元组** `(etf_pcf_info: DataFrame, etf_pcf_constituent: dict[code -> DataFrame])`：
  - **`etf_pcf_info`**（index 为 ETF 代码，27 字段）：`creation_redemption_unit`（每篮份数）、`max_cash_ratio`、`publish`（是否发 IOPV: `Y`/`N`）、`creation` / `redemption`（仅深）、`creation_redemption_switch`（仅沪：`0` 禁申赎 / `1` 皆可 / `2` 仅申 / `3` 仅赎）、`record_num`、`total_record_num`、`estimate_cash_component`、`trading_day` / `pre_trading_day`、`cash_component`、`nav_per_cu`、`nav`、`symbol`、`fund_management_company`、`underlying_security_id` / `_source`、`dividend_per_cu`、`creation_limit*` / `redemption_limit*` / `net_creation_limit*` / `net_redemption_limit*`（8 个限额字段仅深圳有效，`0` 表示不限）。
  - **`etf_pcf_constituent`**（8 字段）：`underlying_symbol`、`component_share`、`substitute_flag`、`discount_ratio`、`creation_cash_substitute` / `redemption_cash_substitute`（仅深）、`substitution_cash_amount`（仅沪）、`underlying_security_id`（跨市场债券 ETF 市场 ID）。
  - `substitute_flag`：`0` 沪市禁止替代、`1` 沪市允许、`2` 沪市必须、`3` 深市退补、`4` 深市必须、`5/6` 非沪深退补/必须、`7/8` 港市退补/必须（仅跨沪深港 ETF）。

### 1.2 `InfoData` ETF 历史数据（均支持可选 `begin_date` / `end_date`）

| 方法 | 返回类型 | 核心字段 |
|---|---|---|
| `get_fund_share` | `dict[code -> DataFrame]` | `FUND_SHARE`, `TOTAL_SHARE`, `FLOAT_SHARE`（万份）, `CHANGE_DATE`, `ANN_DATE`, `CHANGE_REASON`, `IS_CONSOLIDATED_DATA`（`0`/`1`/`2`）, `MARKET_CODE` |
| `get_fund_nav` | `dict[code -> DataFrame]` | `MARKET_CODE`, `ANN_DATE`, `PRICE_DATE`, `UNIT_NAV`, `ACCUM_NAV`, `ACCUM_DIV`, `NAV_ADJ_FACTOR`, `IS_EX_DIVIDEND_DATE`, `ACCUM_UNIT_DIST`, `TOTAL_NET_ASSET_VALUE`, `ADJ_UNIT_NAV`, `NET_ASSET_VALUE`, `IS_MERGED_DATA`, `INNER_CODE`, `OUTER_CODE`, `ACCUM_UNIT_NAV` |
| `get_fund_iopv` | `dict[code -> DataFrame]` | `MARKET_CODE`, `PRICE_DATE`, `IOPV_NAV` |

- **实时折溢价率参考公式**：结合 `SubscribeData` 实时盘口 `last` 与 `IOPV`，折溢价率 $= (\text{last} - \text{IOPV}) / \text{IOPV}$。
- **平台映射提示**：[`amazingdata_normalizer`](../../../data/normalizers/amazingdata_normalizer.py) 优先将 `NAV_ADJ_FACTOR` 映射为权威 `adjfactor`。

---

## 2. 指数与行业数据

| 方法 | 入参 / 支持范围 | 返回类型 | 核心字段 |
|---|---|---|---|
| `get_index_constituent` | `code_list`（约 600 只常用沪深指数）；首次需 `is_local=False` | `dict[code -> DataFrame]` | `INDEX_CODE`, `CON_CODE`, `INDATE`, `OUTDATE`, `INDEX_NAME` |
| `get_index_weight` | `code_list` + 可选 `begin_date`/`end_date`；支持 `000016.SH` / `000300.SH` / `000905.SH` / `000906.SH` / `000852.SH` | `dict[code -> DataFrame]` | `INDEX_CODE`, `CON_CODE`, `TRADE_DATE`, `TOTAL_SHARE`, `FREE_SHARE_RATIO`, `CALC_SHARE`, `WEIGHT_FACTOR`, `WEIGHT`, `CLOSE` |
| `get_industry_base_info` | `local_path`, `is_local`（首次需 `False`；从中取 `INDEX_CODE` 列表） | `dict / DataFrame` | `INDEX_CODE`, `INDUSTRY_CODE`, `LEVEL_TYPE`（`1`/`2`/`3`）, `LEVEL1_NAME`, `LEVEL2_NAME`, `LEVEL3_NAME`, `IS_PUB`, `CHANGE_REASON` |
| `get_industry_constituent` | `code_list`, `local_path`, `is_local`（首次需 `False`） | `dict[code -> DataFrame]` | `INDEX_CODE`, `CON_CODE`, `INDATE`, `OUTDATE`, `INDEX_NAME` |
| `get_industry_weight` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date` | `dict[code -> DataFrame]` | `WEIGHT`, `CON_CODE`, `TRADE_DATE`, `INDEX_CODE` |
| `get_industry_daily` | `code_list`, `local_path`, `is_local`, 可选 `begin_date`/`end_date` | `dict[code -> DataFrame]` | `OPEN`, `HIGH`, `CLOSE`, `LOW`, `AMOUNT`, `VOLUME`, `PB`, `PE`, `TOTAL_CAP`, `A_FLOAT_CAP`, `INDEX_CODE`, `PRE_CLOSE`, `TRADE_DATE` |

---

## 3. 可转债数据（全 11 项接口）

`code_list = base.get_code_list('EXTRA_KZZ')`；**全部 11 个方法均返回 `dict[code -> DataFrame]`**，支持 `local_path` 与 `is_local=True`：

| 方法 | 用途 | 核心字段 / 枚举说明 |
|---|---|---|
| `get_kzz_issuance` | 发行与初始条款 | `MARKET_CODE`, `STOCK_CODE`, `CRNCY_CODE`, `ANN_DT`, `LISTED_DATE`, `PLAN_SCHEDULE`（`1`–`13`）, `CLAUSE_*`（初始转股价/调整/转换期/回售/赎回/下修/配售）, `LIST_*`（网上下网发行中签率与规模）, `TERM_YEAR`, `COUPON_RATE` 等（70+ 字段） |
| `get_kzz_share` | 转债份额变动 | `CHANGE_DATE`, `ANN_DATE`, `MARKET_CODE`, `BOND_SHARE`（万元）, `CONV_SHARE`, `CHANGE_REASON`（`ZZG` 转股 / `SH` 赎回 / `KZZS` 上市 / `HS` 回售 / `DQ` 到期 / `QLXQ` 行权 / `TQDF` 提前兑付 / `GH` 购回 / `HSZG` 回售转股 / `HGZG` 回购转股） |
| `get_kzz_conv` | 转股条款与强转 | `MARKET_CODE`, `ANN_DATE`, `CONV_CODE`, `CONV_NAME`, `CONV_PRICE`, `CONV_START_DATE`, `CONV_END_DATE`, `TRADE_DATE_LAST`, `FORCED_CONV_DATE`, `FORCED_CONV_PRICE`, `REL_CONV_MONTH`, `IS_FORCED`, `FORCED_CONV_REASON` |
| `get_kzz_conv_change` | 转股价历次调整 | `MARKET_CODE`, `CHANGE_DATE`, `ANN_DATE`, `CONV_PRICE`, `CHANGE_REASON`（`1` 发行 ~ `26` 除息，含 `3` 派息 / `4` 配股 / `9` 修正 / `10` 增发 / `24` 调整等） |
| `get_kzz_corr` | 特别向下修正条款 | `MARKET_CODE`, `START_DATE`, `END_DATE`, `CORR_TRIG_CALC_MAX_PERIOD`, `CORR_TRIG_CALC_PERIOD`, `SPEC_CORR_TRIG_RATIO`, `CORR_CONV_PRICE_FLOOR_DESC`, `REF_PRICE_IS_AVG_PRICE`, `CORR_TIMES_LIMIT`, `IS_TIMEPOINT_CORR_CLAUSE_FLAG`, `IS_SPEC_DOWN_CORR_CLAUSE_FLAG` |
| `get_kzz_call` | 赎回参数 | `MARKET_CODE`, `CALL_PRICE`, `BEGIN_DATE`, `END_DATE`, `TRI_RATIO`（%） |
| `get_kzz_put` | 回售参数 | `MARKET_CODE`, `PUT_PRICE`, `BEGIN_DATE`, `END_DATE`, `TRI_RATIO`（%） |
| `get_kzz_put_call_item` | 回售/赎回条款明细 | `MAND_PUT_*`（无条件回售）、`CON_PUT_*` / `IS_*_PUT_ITEM`（有条件/到期/时点回售）、`CON_CALL_*` / `CALL_TRI_CON_INS` / `IS_*_CALL_ITEM`（有条件/时点赎回）、`EXPIRED_REDEMPTION_PRICE`（37 字段） |
| `get_kzz_put_explanation` | 回售执行公告与结果 | `MARKET_CODE`, `PUT_FUND_ARRIVAL_DATE`, `PUT_PRICE`, `PUT_ANNOUNCEMENT_DATE`, `PUT_EX_DATE`, `PUT_AMOUNT`, `PUT_OUTSTANDING`, `REPURCHASE_START_DATE`, `REPURCHASE_END_DATE`, `RESALE_*`, `REPURCHASE_CODE` |
| `get_kzz_call_explanation` | 赎回执行公告与结果 | `MARKET_CODE`, `CALL_DATE`, `CALL_PRICE`, `CALL_ANNOUNCEMENT_DATE`, `CALL_FUL_RES_ANN_DATE`, `CALL_AMOUNT`, `CALL_OUTSTANDING_AMOUNT`, `CALL_DATE_PUB`, `CALL_FUND_ARRIVAL_DATE`, `CALL_RECORD_DAY`, `CALL_REASON` |
| `get_kzz_suspend` | 可转债停复牌 | `MARKET_CODE`, `SUSPEND_DATE`, `SUSPEND_TYPE`（`001` 上午 / `002` 下午 / `003` 今起 / `004` 盘中 / `007` 1小时 / `016` 1天）, `RESUMP_DATE`, `CHANGE_REASON`, `CHANGE_REASON_CODE`, `RESUMP_TIME` |

---

## 4. 国债收益率数据

- **`info.get_treasury_yield(term_list, local_path, is_local=True, begin_date=None, end_date=None)`**：
  - `term_list: list[str]`：支持 `'m3'`, `'m6'`, `'y1'`, `'y2'`, `'y3'`, `'y5'`, `'y7'`, `'y10'`, `'y30'`。
  - 返回 `dict[term -> DataFrame]`：index 为日期，column 为 `YIELD`。

---

## 5. ETF 期权静态与合约变动（重要批处理限制）

> **强制限制**：`get_option_basic_info` 每次调用服务端上限约 **100–104 条**。必须每批 **10 个合约代码** 循环调用并 `pd.concat`（已内置在 [`AmazingdataAdapter._fetch_option`](../../../data/adapters/amazingdata_adapter.py)）。

| 方法 | 入参 | 返回类型 | 核心字段 |
|---|---|---|---|
| `get_option_std_ctr_specs` | `code_list`（支持 `159901.SZ`, `159915.SZ`, `159919.SZ`, `159922.SZ`, `510050.SH`, `510300.SH`, `510500.SH`, `588000.SH`, `588080.SH`）；`local_path`, `is_local` | `DataFrame` | `EXERCISE_DATE`, `CONTRACT_UNIT`, `POSITION_DECLARE_MIN`, `QUOTE_CURRENCY_UNIT`, `LAST_TRADING_DATE`, `POSITION_LIMIT`, `DELIST_DATE`, `NOTIONAL_VALUE`, `EXERCISE_METHOD`, `DELIVERY_METHOD`, `SETTLEMENT_MONTH`, `TRADING_FEE`, `EXCHANGE_NAME`, `OPTION_EN_NAME`, `CONTRACT_VALUE`, `IS_SIMULATION`, `CONTRACT_UNIT_DIMENSION`, `OPTION_STRIKE_PRICE`, `IS_SIMULATION_TRADE`, `LISTED_DATE`, `OPTION_NAME`, `PREMIUM`, `OPTION_TYPE`, `TRADING_HOURS_DESC`, `FINAL_SETTLEMENT_DATE`, `FINAL_SETTLEMENT_PRICE`, `MIN_PRICE_UNIT`, `MARKET_CODE`, `CONTRACT_MULTIPLIER` |
| `get_option_mon_ctr_specs` | `code_list`（`EXTRA_ETF_OP`）, `local_path`, `is_local` | `DataFrame`\* | `MARKET_CODE`, `CHANGE_DATE`, `CODE_OLD`, `CODE_NEW`, `NAME_OLD`, `NAME_NEW`, `EXERCISE_PRICE_OLD`, `EXERCISE_PRICE_NEW`, `UNIT_OLD`, `UNIT_NEW`, `CHANGE_REASON` |
| `get_option_basic_info` | `code_list`（每批 `<= 10`）, `local_path`, `is_local` | `DataFrame` | `MARKET_CODE`, `CONTRACT_FULL_NAME`, `CONTRACT_TYPE`（`C` 认购 / `P` 认沽）, `DELIVERY_MONTH`, `EXPIRY_DATE`, `EXERCISE_PRICE`, `EXERCISE_END_DATE`, `START_TRADE_DATE`, `LAST_TRADE_DATE`, `DELIVERY_DATE`, `LISTING_REF_PRICE`, `EXCHANGE_CODE`, `EXCHANGE_SHORT_NAME`, `CONTRACT_UNIT`, `IS_TRADE`, `CONTRACT_ADJUST_FLAG` |

> \* 官网 `3.5.10.3` 输出参数表将返回值名误写为 `block_trading`，实际对应 `option_mon_ctr_specs`。
