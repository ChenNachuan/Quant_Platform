# calendar_adjustment

> 域：data · 对应代码 [data/calendar_adjustment.py](../../../data/calendar_adjustment.py)

## 职责与边界

交易日历、时区、期货换月与夜盘归属、复权口径——四件时间/价格坐标系的基础约定，供全平台引用。

- **TradingCalendar**：交易日判定与前后推移（bisect，常驻内存，全平台一个实例）。
- **时区约定**：全部交易所本地时间（Asia/Shanghai）naive datetime64[ns]，不转 UTC。
- **连续合约**：主力识别 → 换月比价 → 累积 adjfactor（`build_continuous`）。
- **夜盘归属**：期货夜盘交易日算次日（`assign_trade_date_futures`）。
- **复权**：asset 层只存**原始价 + 累积 adjfactor**，复权价是派生量；`PriceAccessor`按需出 raw/前复权/收益率三口径。

边界两个「不做」：

- **不自维护日历**——临时调休/节假日调整官方才准，自维护必漏。
- **不存复权价**——qfq/hfq 都不落盘，零回刷。

## 接口签名

```python
class TradingCalendar:
    def __init__(self, exchange: str): ...          # 读 asset/cn_calendar/{exchange}.parquet
    def is_trading_day(self, d: date) -> bool
    def prev(self, d: date, n: int = 1) -> date
    def next(self, d: date, n: int = 1) -> date
    def range(self, start: date, end: date) -> list[date]
    def count(self, start: date, end: date) -> int  # bisect 实现

def assign_trade_date_futures(ts: pd.Timestamp, cal: TradingCalendar) -> date:
    """hour≥21 → cal.next(d,1)；hour<3 凌晨段=前晚夜盘延续→归当日交易日；数据源已标 trade_date 则信任"""

def build_continuous(symbol: str, ...) -> pd.DataFrame:
    """每日主力 → 换月日新/旧合约收盘价比价 → 累积 adjfactor → 前复权；每日全量重建"""

def get_minute_adj(ticker: str, trade_date: date) -> float:
    """从日线 adjfactor 取（~20 行）；分钟表无 adjfactor 列"""

class PriceAccessor:
    def raw(self) -> pd.DataFrame
    def forward_adj(self) -> pd.DataFrame        # qfq = raw × adj / adj_latest
    def returns(self, n: int = 1) -> pd.DataFrame  # close_T×adj_T/(close_{T-1}×adj_{T-1})-1
```

**复权公式**：`hfq = raw × adj`；`qfq = raw × adj / adj_latest`。returns 公式里基准日消掉，回测/实盘零差异。

**日历存储**：`asset/cn_calendar/{exchange}.parquet` 单列 `trade_date: date`（对齐 `{nation}_{asset_type}_{data_type}`）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 日历从数据源拉取 + 双源交叉校验 | 自维护 / 爬交易所官网 | 调休易漏；官网格式不统一 |
| 时区全部交易所本地时间 naive | 转 UTC | A 股 14:30→06:30 心算负担 |
| 主力合约 = 每日**成交量**最大 | 持仓量最大 | 持仓量有延迟；回测要「能交易」的合约 |
| 连续合约**前复权** + 换月日新/旧收盘价比价累积 | 后复权 / 不复权拼接 | 与 A 股 qfq 口径一致 |
| 连续合约每日全量重建 | 增量维护 | 重建比增量便宜 |
| asset 存 raw + 累积 adjfactor | 存 qfq/hfq 价 | 复权价是派生量，零回刷 |
| 分钟线无 adjfactor 列 | 每根 bar 带 | `get_minute_adj` 从日线取 |
| 回测/实盘统一「截至当日的前复权」 | 两套口径 | 唯一陷阱=策略禁用价格绝对值判断 |
| DQC 三条：adjfactor_present / positive / K05 非行动日不变 | 无校验 | 来源本身错也能发现 |


## 实现要点

详见 [data/calendar_adjustment.py](../../../data/calendar_adjustment.py)

核心约定:
- TradingCalendar用bisect实现0(Logn)推移,is_trading_day 0(1) set 查找
- 期货夜盘hour≥21归次日,凌晨hour<3归当日(前晚延续续),数据源已标trade_date则信任
- 连续合约每日全量重建:主力识别→换月日新旧合约收盘价比价>累积adjfactor
- 复权基准必须用全历史adj_latest/adj_first,不能用窗口内值
- 因子/alphalens/回测=前复权;实盘下单=未复权(=前复权口径,returns消掉基准日)
