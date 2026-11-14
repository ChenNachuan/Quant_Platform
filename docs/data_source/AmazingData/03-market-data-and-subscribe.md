# 03. 行情查询（`MarketData`）与实时订阅（`SubscribeData`）

## 1. 数据起点与覆盖范围

| 品种 | 历史行情覆盖 | 市场说明 | 实时订阅 |
|---|---|---|---|
| 股票 | Level-1 快照、K 线（2013 年至今） | 上交所、深交所、北交所 | 支持 |
| 指数 | Level-1 快照、K 线 | 上交所、深交所、北交所 | 支持 |
| 债券（含可转债）、场内基金（ETF） | Level-1 快照、K 线 | 上交所、深交所 | 支持 |
| ETF 期权 | Level-1 快照、K 线（2015 年至今） | 上交所 ETF 期权、深交所 ETF 期权 | 支持 |
| 港股通 | 港股通行情快照（2023 年至今） | 上交所、深交所 | 支持 |
| 期货 | Level-1 快照、K 线（2010 年 4 月至今） | 官网总表标注中金所（五所枚举见 [02 篇](02-base-data-and-enums.md)，以账号权限为准） | 支持 |

---

## 2. 历史行情查询：`MarketData`

需先通过 `BaseData().get_calendar()` 获取交易日历初始化；**注意快照与 K 线的时间入参格式不同**：

```python
calendar = ad.BaseData().get_calendar(market="SH")
md = ad.MarketData(calendar)

# 1) 历史 K 线：begin_time / end_time 为 3 或 4 位整型 HHMM（如 900、1725）
kline_dict = md.query_kline(
    code_list=["600519.SH"],
    begin_date=20240101,                 # 必填 8 位 int (YYYYMMDD)
    end_date=20241231,                   # 必填 8 位 int (YYYYMMDD)
    period=ad.constant.Period.day.value, # Period 枚举（除 snapshot* 外全周期）
    begin_time=900,                      # 可选 int (HHMM，3~4 位)
    end_time=1725,                       # 可选 int (HHMM，3~4 位)
)

# 2) 历史快照：begin_time / end_time 为 8 或 9 位整型 HHMMSSmmm（时 1~2 位 + 分 2 + 秒 2 + 毫秒 3）
snap_dict = md.query_snapshot(
    code_list=["600519.SH"],
    begin_date=20240530,
    end_date=20240530,
    begin_time=90000000,                 # 可选 int，如 09:00:00.000 = 90000000，17:25:00.000 = 172500000
    end_time=150500000,
)
# 均返回：dict[code -> pd.DataFrame]，index 为 datetime
```

- **覆盖差异**：`query_snapshot` 额外支持**港股通**（`SnapshotHKT`）；`query_kline` 支持股票、指数、债券/可转债、ETF、ETF 期权及期货。
- **K 线合成与集合竞价约定**：分钟 K 线采用前推时间窗合成，1 分钟 K 线包含开盘集合竞价合并逻辑。

---

## 3. 实时行情订阅：`SubscribeData`

```python
sub = ad.SubscribeData()

@sub.register(code_list=code_list, period=ad.constant.Period.snapshot.value)
def onSnapshot(data, period):
    print(period, data.code, data.last)

sub.run()
```

官网全部 **9 个实时回调接口**：

| 回调函数名 | `security_type` 示例 | `period` 枚举 | 回调参数 `data` 类型 | 适用标的 |
|---|---|---|---|---|
| `onSnapshotindex` | `EXTRA_INDEX_A` | `Period.snapshot.value` | `SnapshotIndex` | 沪深北指数 |
| `onSnapshot` | `EXTRA_STOCK_A` | `Period.snapshot.value` | `Snapshot` | 沪深北股票 |
| `onSnapshotglra` | `EXTRA_GLRA` | `Period.snapshot.value` | `Snapshot` | 沪深逆回购 |
| `onSnapshotfuture` | `ZJ_FUTURE` / `EXTRA_FUTURE` | `Period.snapshotfuture.value` | `SnapshotFuture` | 期货（官网注明中金所） |
| `onSnapshotetf` | `EXTRA_ETF` | `Period.snapshot.value` | `Snapshot` | 沪深 ETF |
| `onSnapshotkzz` | `EXTRA_KZZ` | `Period.snapshot.value` | `Snapshot` | 沪深可转债 |
| `onSnapshothkt` | `EXTRA_HKT` | `Period.snapshotHKT.value` \* | `SnapshotHKT` | 沪深港股通 |
| `onSnapshotoption` | `EXTRA_ETF_OP` | `Period.snapshotoption.value` | `SnapshotOption` | 沪深 ETF 期权 |
| `OnKLine` | `EXTRA_STOCK_A` 等 | `Period.min1` ~ `Period.day` | `Kline` | 沪深北股/指/ETF/转债、期权、期货 |

> \* **官网口径差异提示**：3.5.3.7 参数表标注港股通 `period` 为 `Period.snapshotHKT.value`，但同节示例代码写为 `Period.snapshot.value`（且描述文字误写为“可转债”）；3.5.3.8 亦有同样笔误。实盘订阅港股通前建议在容器内检查 `ad.constant.Period` 枚举成员。

---

## 4. 行情数据结构字段

### 4.1 K 线 `Kline`

| 字段 | 类型 | 说明 |
|---|---|---|
| `code` | `str` | 证券代码 + 市场后缀 |
| `kline_time` / `trade_time` | `datetime` | 行情时间戳（`query_kline` DataFrame 中为 `kline_time`，部分订阅文档标为 `trade_time`） |
| `open` / `high` / `low` / `close` | `float` | 开 / 高 / 低 / 收 |
| `volume` | `int` | 成交总量 |
| `amount` | `float` | 成交总金额 |

### 4.2 指数快照 `SnapshotIndex`

含 `code` (`str`)、`trade_time` (`datetime`)、`last`、`pre_close`、`open`、`high`、`low`、`close`（仅上海有效）、`volume`（`int`，上交所手 / 深交所张）、`amount`（无五档盘口）。

### 4.3 股票 / ETF / 可转债 / 逆回购快照 `Snapshot`

在基础 OHLCV、`pre_close`、`last` 之外包含：
- `num_trades`（成交笔数）、`high_limited`（涨停价）、`low_limited`（跌停价）
- 五档盘口：`ask_price1`–`5` (`float`)、`ask_volume1`–`5` (`int`)、`bid_price1`–`5` (`float`)、`bid_volume1`–`5` (`int`)

### 4.4 期货快照 `SnapshotFuture`

含 `code`、`trade_time`、`action_day`（业务日期）、`trading_day`（交易日期）、`pre_close`、`pre_settle`（上次结算价）、`pre_open_interest`、`open_interest`（持仓量）、`last`、`open`、`high`、`low`、`close`、`volume`、`amount`、`high_limited`、`low_limited`、`average_price`（当日均价）、`settle`（本次结算价）及五档盘口 `ask_price1`–`5` / `ask_volume1`–`5` / `bid_price1`–`5` / `bid_volume1`–`5`。

### 4.5 ETF 期权快照 `SnapshotOption` 与港股通快照 `SnapshotHKT`

- **`SnapshotOption`**：含基础 OHLCV、`trading_phase_code`、`total_long_position`（总持仓量）、`pre_close`、`pre_settle`、`auction_price` & `auction_volume`（动态参考价与虚拟匹配量，仅上海有效）、`settle`、`high_limited`、`low_limited`、五档盘口，以及期权专属四字段：`contract_type` (`str`)、`expire_date` (`int`)、`underlying_security_code` (`str`)、`exercise_price` (`float`)。
- **`SnapshotHKT`**：港股通快照对象（官网暂未单独展开字段明细表，可在订阅/查询后通过 `dir(data)` / `df.columns` 核查）。
