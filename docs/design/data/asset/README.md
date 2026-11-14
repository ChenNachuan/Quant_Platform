# asset/ 资产域表结构与字段字典

> 上级索引：[storage_schema.md](../storage_schema.md) · 代码权威：[data/storage_schema.py](../../../../data/storage_schema.py) · 源优先级：[config/source_priority.yaml](../../../../config/source_priority.yaml)

本目录记录 `$QUANT_DATA_ROOT/asset/` 下 **51 张规范表**（47 张 A 股域表 + 4 张期货表）的完整字段 Schema、主键约束、分区策略、数据源仲裁与交叉校验说明（`factors/` 另见 [factor_storage.md](../../research/factor_storage.md)）。

## 文档导航

| 文档 | 覆盖域 | 表数 | 规范表清单 |
|---|---|---:|---|
| [01-stock-core-and-status.md](01-stock-core-and-status.md) | 日历 + 股票核心与日频风控状态 | 7 | `cn_calendar`, `cn_stock_basic`, `cn_stock_bars_daily`, `cn_stock_bars_minute`, `cn_stock_fundamental`, `cn_stock_daily_status`, `cn_stock_corporate_actions` |
| [02-stock-index-and-board.md](02-stock-index-and-board.md) | 指数 / 行业 / 概念与成分权重 | 4 | `cn_stock_index_basic`, `cn_stock_index_bars_daily`, `cn_stock_industry`, `cn_stock_index_weights` |
| [03-stock-fundamental-pit.md](03-stock-fundamental-pit.md) | PIT 财务报表、业绩事件与一致预期 | 7 | `cn_stock_fundamental_{income,balance,cashflow,indicator,segment,earnings,consensus}` |
| [04-stock-flows-and-events.md](04-stock-flows-and-events.md) | 资金流/情绪/筹码 + #29 股东资本事件拆表 | 17 | `cn_stock_{auction,moneyflow,hsgt_market_flow,hsgt_hold,margin,lhb,lhb_seats,limit_pool,chips_perf}` + `cn_stock_{holder_num,top_holders,restricted_release,block_trade,repurchase,holder_trade,pledge,research_visits}` |
| [05-stock-tick-l2.md](05-stock-tick-l2.md) | 股票逐笔成交与十档 L2 盘口 | 2 | `cn_stock_tick`, `cn_stock_tick_l2` |
| [06-etf-and-fund.md](06-etf-and-fund.md) | 独立 ETF 域 + 场外/LOF 公募基金 | 10 | `cn_etf_{basic,bars_daily,bars_minute,fundamental,tick}`（不含 `cn_etf_pcf`）+ `cn_fund_{basic,managers,nav,dividend,portfolio}` |
| [07-futures.md](07-futures.md) | 期货行情、五档 L2、合约规格与主连 | 4 | `cn_futures_{bars_daily,tick_l2,contract_specs,continuous}` |

## 通用类型与校验约定

- **严格模式**：所有表均在 `DataFrameSchema(..., coerce=False, strict=True, unique=PK)` 下校验；禁止未声明列写入，只允许通过**新增可空非必填列**（`nullable=True, required=False`）演进。
- **日期 / 时间**：
  - `pa.Date` 由 `_safe_date_parser` 解析为 `datetime.date`，拒绝整数（如 `20240102`）与带时区输入，空值必须为 `NaT/None`（拒绝 `"nan"`/`"NA"` 等字符串）。
  - `pa.DateTime` 由 `_safe_datetime_parser` 解析为无时区北京时间 `datetime64[ns]`。
- **数值精度与可空整数**：
  - 价格/金额/比例/财务数值统一 `float`（Parquet `DOUBLE`）；非空计数与成交量统一 `int`（Parquet `INT64`，`coerce=False` 防静默截断）。
  - 具可空语义的整数型统计列（如 `open_times`、`streak_days`、`rank`、`num_trades`）在 Pandera 中声明为 `float` + `nullable=True`，避免 pandas 缺失值转 `float64` 后触发强整型校验失败。
- **质量位掩码 `quality_flag`**：`int64` 非空（`0 <= quality_flag < 32`），bit0=`C02_COVERAGE`、bit1=`C05_NULL_RATE`、bit2=`L04_BEYOND_LIMIT`、bit3=`L07_ADJFACTOR_RANGE`、bit4=`K01_OVERNIGHT_GAP`。
- **历史短别名**：[data/storage_schema.py](../../../../data/storage_schema.py) 通过 `LEGACY_TO_CANONICAL` 透明解析 `bars_daily`、`bars_minute`、`fundamental`、`calendar`、`industry`、`corporate_actions`、`bars_daily_futures`、`tick_futures_l2`、`contract_specs`、`continuous`。
