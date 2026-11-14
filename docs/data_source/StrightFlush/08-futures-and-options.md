# 08. 期货与期权

[← 返回目录](README.md)

期货合约使用完整 `thscode`（如 `CU2601.SHF`），品种代码使用大写（如 `CU`），商品指数使用 `.TI`（如 `850002.TI`）；期权合约使用完整 `thscode`（如 `IO2601-C-4000.CFE`）。

> [!IMPORTANT]
> **端外能力限制与边界**：
> 1. **期货与期权行情**在端外调用时，`intraday` 仅允许 `trade_date=0`（当日），`daily` 仅允许 `time_period=day_1`（日 K）；历史分时与分钟/周/月等全周期 K 线仅限同花顺 AI 客户端内开放。
> 2. **期权模块当前仅有 4 组 6 个端点**，**不提供**隐含波动率（IV）、Greeks（Delta/Gamma/Vega/Theta）或 T 型报价接口。

---

## 1. 期货数据（Futures）

### 1.1 品种、板块与合约基础资料

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **期货品种资料** | `GET /api/futures/varieties/list`<br>`get_futures_varieties_list` | 无参数。返回 `variety_code`、`quote_code`、`exchange_code`、`has_night_session`、`margin_rate`、`trade_amount`、`tick_size`、`contract_multiplier`、`main_contract_thscode`、`transaction_fee[_rate]`、`trade_sessions[]` |
| **期货品种板块** | `GET /api/futures/variety-plates/list`<br>`get_futures_variety_plates_list` | 无参数。返回 `variety_code`、`name`、`plate_level`、`plate_name` |
| **合约详情** | `GET /api/futures/contracts/detail`<br>`get_futures_contracts_detail` | `thscode`（必填）。返回上市/结束/最后交易/最后交割日及所属品种与交易所 |
| **合约目录列表** | `GET /api/futures/contracts/list`<br>`get_futures_contracts_list` | `limit`（1–1000，默认 100）、`offset`（>=0）分页获取完整合约快照 |
| **主连资料** | `GET /api/futures/contracts/main-continuous-list`<br>`get_futures_contracts_main_continuous_list` | 无参数。返回主连代码与统计信息 |
| **主力合约列表** | `GET /api/futures/contracts/main-list`<br>`get_futures_contracts_main_list` | 无参数。返回当前主力合约列表及 `margin_rate`、`transaction_fee[_rate]` |
| **次主力合约列表** | `GET /api/futures/contracts/secondary-main-list`<br>`get_futures_contracts_secondary_main_list` | 无参数。字段同主力合约列表 |
| **商品指数列表** | `GET /api/futures/contracts/commodity-index-list`<br>`get_futures_contracts_commodity_index_list` | 无参数。返回商品指数 `.TI` 代码与上市日期 |

### 1.2 期货行情与交易日历

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **期货分时** | `GET /api/futures/prices/intraday`<br>`get_futures_prices_intraday` | `thscode`（合约或商品指数）、`session` (`pre_market`/`intraday`/`post_market`)、`trade_date`（端外仅 `0`）。返回 `{timestamp, price, volume, turnover}` |
| **期货 K 线** | `GET /api/futures/prices/daily`<br>`get_futures_prices_daily` | `thscode`、`start`/`end`（成对毫秒戳；同省略默认最近 100 根）、`time_period`（端外仅 `day_1`）。返回 OHLC、`volume`、`turnover`、**`open_interest`（持仓量）** |
| **期货交易日程** | `GET /api/futures/calendar/trading-schedule`<br>`get_futures_calendar_trading_schedule` | `thscode`、`start_date`、`end_date` (`yyyy-MM-dd`)。返回 `trade_dates[]`、`regular_schedules[]`、`special_schedules[]`、`daylight_saving_periods[]` |
| **期货会话时间轴** | `GET /api/futures/calendar/session-timeline`<br>`get_futures_calendar_session_timeline` | `thscode`。返回最近交易日的 `trade_stages[]` 与 `exchange_sessions[]` |

### 1.3 龙虎持仓、仓单与基差

| 接口 | REST 端点 / MCP | 参数与说明 |
|---|---|---|
| **期货公司列表** | `GET /api/futures/positions/company-list`<br>`get_futures_positions_company_list` | 无参数。返回 `company_id`、`company_name` |
| **品种日持仓** | `GET /api/futures/positions/variety-daily`<br>`get_futures_positions_variety_daily` | `date` (`yyyy-MM-dd`)。返回各品种成交/多空持仓及变化、20 日均价、资金流 |
| **公司品种日持仓** | `GET /api/futures/positions/company-variety-daily`<br>`get_futures_positions_company_variety_daily` | `date`、`varieties`（1–5 个逗号分隔大写品种代码）。返回多空持仓、日/年收益、情绪值等 |
| **公司合约日持仓** | `GET /api/futures/positions/contract-daily`<br>`get_futures_positions_contract_daily` | `thscode`、`variety`、`date`。返回 `position_item[]`（席位多空持仓）与独立 `average_item[]`（`company_long_avg`/`company_short_avg`，两者不按索引对齐） |
| **公司合约历史持仓** | `GET /api/futures/positions/contract-historical`<br>`get_futures_positions_contract_historical` | `thscode`、`variety`、`company`（公司名）、`start_date`（须在调用日前 1 年内，结束日固定为调用日） |
| **历史注册仓单** | `GET /api/futures/warehouse-receipts/historical`<br>`get_futures_warehouse_receipts_historical` | `thscode`、`start_date`、`end_date` (`yyyy-MM-dd`)。返回 `amount`、`amount_change`、`equivalent_lots` |
| **主连最新基差** | `GET /api/futures/basis/main-continuous-latest`<br>`get_futures_basis_main_continuous_latest` | 无参数。返回现货价、折算现货价、收盘/结算基差与基差率、平均基差 |
| **合约历史基差** | `GET /api/futures/basis/historical`<br>`get_futures_basis_historical` | `thscode`（必填）、`spot_indicator_id`（可选） |

> 期货 F10 基本面指标历史数据 `/api/futures/fundamentals/indicators-historical` 当前不开放端外接入，详见 [11-ai-client-only.md](11-ai-client-only.md)。

---

## 2. 期权数据（Options）

| 接口 | REST 端点 / MCP | 参数与核心字段 |
|---|---|---|
| **期权品种资料** | `GET /api/options/varieties/list`<br>`get_options_varieties_list` | 无参数。返回 `variety_code`、`exchange_code`、`has_night_session`、`trade_amount`、`tick_size`、`contract_multiplier`、`transaction_fee`、`exercise_fee`、`settlement_type` (`physical`/`cash`)、`trade_sessions[]` |
| **期权合约详情** | `GET /api/options/contracts/detail`<br>`get_options_contracts_detail` | `thscode`。返回 `underlying_code`（非规范 code）、`strike_price`、`exercise_style` (`bermudan`/`american`/`european`)、`option_type` (`call`/`put`)、`trade_amount`、`last_trade_date`、`last_delivery_date` |
| **期权合约列表** | `GET /api/options/contracts/list`<br>`get_options_contracts_list` | `limit`（1–1000，默认 100）、`offset`（>=0）分页获取期权目录 |
| **期权交易时间轴** | `GET /api/options/calendar/session-timeline`<br>`get_options_calendar_session_timeline` | `thscode`。返回最近交易日 `trade_stages[]` 与 `exchange_sessions[]` |
| **期权分时** | `GET /api/options/prices/intraday`<br>`get_options_prices_intraday` | `thscode`、`session` (`pre_market`/`intraday`/`post_market`)、`trade_date`（端外仅允许 `0`） |
| **期权 K 线** | `GET /api/options/prices/daily`<br>`get_options_prices_daily` | `thscode`、`start`/`end`（成对毫秒戳；省略默认最近 100 根）、`time_period`（端外仅允许 `day_1`）。返回 OHLC、`volume`、`turnover`（无 `open_interest`） |
