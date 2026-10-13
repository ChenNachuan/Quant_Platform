# calendar_adjustment

> 域：data · 对应代码 data/calendar_adjustment.py · 来源 §42–§44（archive/OPEN_QUESTIONS.md）

## 职责与边界

交易日历、时区、期货换月与夜盘归属、复权口径——四件时间/价格坐标系的基础约定，供全平台引用。

- **TradingCalendar**：交易日判定与前后推移（bisect，~60 行，常驻内存 ~200KB，全平台一个实例）。
- **时区约定**：全部交易所本地时间（Asia/Shanghai）naive datetime64[ns]，不转 UTC。
- **连续合约**：主力识别 → 换月比价 → 累积 adjfactor（`build_continuous` ~80 行）。
- **夜盘归属**：期货夜盘交易日算次日（`assign_trade_date_futures` ~40 行）。
- **复权**：asset 层只存**原始价 + 累积 adjfactor**，复权价是派生量；`PriceAccessor`（~50 行）按需出 raw/前复权/收益率三口径。

边界两个「不做」：

- **不自维护日历**——临时调休/节假日调整官方才准，自维护必漏。
- **不存复权价**——qfq/hfq 都不落盘，零回刷（adjfactor 变了不用重写价格列）。

## 接口签名

```python
class TradingCalendar:
    def __init__(self, exchange: str): ...          # 读 asset/calendar/{exchange}.parquet
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

**复权公式**（§43）：`hfq = raw × adj`；`qfq = raw × adj / adj_latest`。returns 公式里基准日消掉，回测/实盘零差异。

**日历存储**：`asset/calendar/{exchange}.parquet` 单列 `trade_date: date`（非交易日不存），来源 Tushare `trade_cal` 主源（SSE/SZSE/CFFEX/SHFE/DCE/CZCE）+ AKShare `tool_trade_date_hist_sina` 备源交叉校验，每月更新（满足 C07 未来 30 交易日）。

**连续合约存储**：`asset/continuous/{symbol}.parquet`——不存 close_adj（§47.13），含 settle/open_interest，adjfactor = 换月累积。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 日历从数据源拉取 + 双源交叉校验 | 自维护 / 爬交易所官网 | 调休易漏；官网格式不统一 |
| 按交易所一文件，非交易日不存 | 合并单文件 / 存非交易日 | 查询按交易所；日期连续性即约定 |
| 时区全部交易所本地时间 naive | 转 UTC | A 股 14:30→06:30 心算负担（§42 否决） |
| 期货夜盘 trade_date 算次日（21:00 起）；凌晨段归当日 | 凌晨段也算次日 | 交易所规则；周五夜盘→下周一自动跳；凌晨是前晚夜盘延续 |
| 主力合约 = 每日**成交量**最大 | 持仓量最大 | 持仓量有延迟；回测要「能交易」的合约 |
| 连续合约**前复权** + 换月日新/旧收盘价比价累积 | 后复权 / 不复权拼接 | 与 A 股 qfq 口径一致 |
| 连续合约每日全量重建 | 增量维护 | ~80 行量级，重建比增量便宜 |
| asset 存 raw + 累积 adjfactor | 存 qfq/hfq 价 | 复权价是派生量，零回刷 |
| 分钟线无 adjfactor 列 | 每根 bar 带 | 存 240 遍浪费；`get_minute_adj` 从日线取 |
| 复权主源 Tushare adj_factor + AKShare qfq/raw 反推校验 | 单源 | 容差 1e-6，任何不一致始终告警 |
| 公司行动入 `corporate_actions.parquet`，K05 排除 (ticker, ex_date) | 不识别行动日 | K05 稳定性检查依赖 |
| 研究层 alphalens prices 用**前复权** | raw | §27.3 已选 qfq |
| 回测/实盘统一「截至当日的前复权」 | 两套口径 | 六场景口径表一致；唯一陷阱=策略禁用价格绝对值判断 |
| DQC 三条：C06 adjfactor_present / L06 positive / K05 非行动日不变 | 无校验 | 来源本身错也能发现 |

## 实现要点

```python
# TradingCalendar（~60 行）
class TradingCalendar:
    def __init__(self, exchange):
        self._days = pd.read_parquet(f"asset/calendar/{exchange}.parquet")["trade_date"].tolist()
        self._set = set(self._days)                  # is_trading_day O(1)
    def is_trading_day(self, d): return d in self._set
    def prev(self, d, n=1):
        i = bisect_left(self._days, d)
        if i - n < 0: raise IndexError("prev() 越界：早于日历起点")
        return self._days[i - n]
    def next(self, d, n=1): i = bisect_right(self._days, d); return self._days[i + n - 1]

# 夜盘归属（~40 行）
def assign_trade_date_futures(ts, cal):
    d = ts.date()
    if ts.hour >= 21: return cal.next(d, 1)          # 当晚夜盘 → 次日交易日
    # hour<3 凌晨段=前晚夜盘延续 → 归当日交易日（源 §42 伪代码误同夜盘起始段取次日，已修正）
    return d if cal.is_trading_day(d) else cal.next(d, 1)

# 连续合约（~80 行）：每日主力 → 换月日比价 → 累积 adjfactor → 前复权
# 复权（§43）：adjfactor 累积基准日=上市首日（Tushare 口径）
# 因子口径表：动量/波动率 → 前复权；换手率/均价 → 未复权
# 六场景口径（§44.5）：因子/alphalens/回测=前复权截至末日；实盘未复权=前复权
```

新增代码 ≈120 行（PriceAccessor ~50 + corporate_actions 入库 ~40 + 校验 ~30，日历/连续/夜盘见 §42）。

## 实施时定稿项

- `get_active_contracts(symbol, d)` 数据来源（futures 基础表从哪个源、何时入库）（§42）
- 连续合约 OHLC 是否全部从主力合约取（而非拼接各段）（§42）
- 换月日是否用结算价而非收盘价比价（§42）
- INE（能源中心）日历是否单列文件（§42/§47 均列 6 交易所，INE 未出现）
- `TradingCalendar` 多交易所实例缓存管理（每交易所一实例 vs 合并）（§42）
- 夜盘 `hour < 3` 对 CFFEX 国债期货（无夜盘）的兼容（§42）
- 期货交易所日历无备源校验时的降级策略（AKShare 仅覆盖 A 股）（§42）
- ~~复权因子来源本身错了如何发现~~ 已定 → §44.2（三条 DQC + 公司行动日检测）
- ~~alphalens pricing 用 qfq 还是 raw~~ 已定 → §44.4（前复权，`get_pricing_for_alphalens` pivot）
- ~~回测/实盘口径一致性~~ 已定 → §44.5（统一「截至当日的前复权」）
- `adjfactor_latest` 取值时点（每次读数据现算 vs 缓存）（§43）
- 现金分红的收益率调整层（adjfactor 只管送转/配股，现金分红在 return 层还是价格层）（§43/§44）
- `UNIFIED_DAILY_COLUMNS` 完整清单（§43；表 schema 已由 §47 落定，清单名待归并）
- `corporate_actions.parquet` 更新频率与历史深度（Tushare dividend 分页/批量约束）（§44）
- AKShare 反推容差：送转日 qfq/raw 比值是否精确等于 Tushare adjfactor（§44）
- ~~`AdjustedDataFeed` 对 backtrader 适配~~ 不再需要——§66.2 不用 backtrader/vnpy（2026-10-10 回改）
- `PriceAccessor.returns(n)` 上市不足 n 日的 NaN 处理（§44）
- 策略层「禁用价格绝对值判断」lint 能否自动化检查（§44）
