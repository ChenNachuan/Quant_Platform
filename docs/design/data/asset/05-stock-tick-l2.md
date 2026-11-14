# 05. 股票逐笔成交与十档 L2 盘口（2 张）

> 所属目录：[docs/design/data/asset/](README.md) · Schema 源码：[data/storage_schema.py](../../../../data/storage_schema.py)
>
> 说明：分钟、逐笔与快照表均不冗余存储日级 `adjfactor`（读时从 `cn_stock_bars_daily` join）。

---

## 1. `cn_stock_tick`（股票逐笔成交）

- **路径**：`asset/cn_stock_tick/{date}.parquet`（按日）
- **family**：`stock` · **主键**：`[ticker, trade_date, seq]`
- **数据源优先级**：`qdc` → `amazingdata`
- **交叉比对数值列**：`price`, `volume`, `amount`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `datetime` | `pa.DateTime` | N | 逐笔时间戳（无时区北京时间） |
| `seq` | `int` | N | `>= 0`（交易所逐笔序号） |
| `price` | `float` | N | `> 0`（成交价） |
| `volume` | `int` | N | `>= 0`（成交股数，已按股落盘） |
| `amount` | `float` | N | `>= 0`（成交额，元） |
| `bs_flag` | `str` | N | `isin(["B", "S", "N", "A"])`（主动买/卖/中性/竞价） |
| `quality_flag` | `int` | N | `0 <= x < 32` |

---

## 2. `cn_stock_tick_l2`（股票十档 Level-2 快照，49 列）

- **路径**：`asset/cn_stock_tick_l2/{date}.parquet`（按日）
- **family**：`stock` · **主键**：`[ticker, trade_date, seq]`
- **数据源优先级**：`qdc` → `amazingdata`
- **交叉比对数值列**：`last_price`, `volume`, `amount`

| 列名 | 类型 | 可空 | 约束 / 说明 |
|---|---|---:|---|
| `ticker` | `str` | N | `validate_stock_ticker` |
| `trade_date` | `pa.Date` | N | 交易日期 |
| `datetime` | `pa.DateTime` | N | 快照时间戳（无时区北京时间） |
| `seq` | `int` | N | `>= 0`（快照序号） |
| `last_price` | `float` | N | `> 0`（最新成交价） |
| `volume` | `int` | N | `>= 0`（当日累计成交股数） |
| `amount` | `float` | N | `>= 0`（当日累计成交额，元） |
| `num_trades` | `float` | Y | `>= 0`（累计成交笔数，可空整型约定 `float`） |
| `bid_price_1` .. `bid_price_10` | `float` (×10) | N | `>= 0`（买一到买十价） |
| `bid_volume_1` .. `bid_volume_10` | `int` (×10) | N | `>= 0`（买一到买十量，股） |
| `ask_price_1` .. `ask_price_10` | `float` (×10) | N | `>= 0`（卖一到卖十价） |
| `ask_volume_1` .. `ask_volume_10` | `int` (×10) | N | `>= 0`（卖一到卖十量，股） |
| `quality_flag` | `int` | N | `0 <= x < 32` |
