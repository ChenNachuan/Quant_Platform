# data_revision

> 域：data · 对应代码 data/data_revision.py · 来源 §45（archive/OPEN_QUESTIONS.md）

## 职责与边界

数据回溯修正的全流程：**检测 → 应用 → 影响传播**。数据源会静默覆盖历史值且不给修正标记，平台必须自己发现订正、安全地写回，并让下游知道哪些结果已过期。

- **检测**：每周日 02:00 全量重拉近 30 交易日，与本地逐行比对（`detect_corrections` ~80 行）；季度全量近 1 年兜底。
- **应用**：覆盖写入 + 修正日志审计（`apply_corrections` ~40 行），走完整 `daily_ingest` 流水线。
- **影响传播**：severity 分级 + 因子 stale 标记（`assess_correction_impact` ~60 行 + `mark_factors_stale` ~30 行），**不自动重跑下游**，人工按 severity 决定。

边界：不做数据版本化（历史版本不保留，只留修正日志）；不自动级联重算因子/回测。

## 接口签名

```python
CORRECTION_TOLERANCES = {close: 0.01, volume: 100, amount: 1000, adjfactor: 1e-6}

@dataclass
class FieldChange:
    ticker: str; field: str
    old_value: float; new_value: float
    abs_diff: float; rel_diff: float

@dataclass
class CorrectionRecord:   # 写入 corrections/{asset}/{date}.jsonl
    asset_type: str; trade_date: date
    detected_at: datetime
    changes: list[FieldChange]

def detect_corrections(asset_type: str, lookback: int = 30) -> list[FieldChange]
def apply_corrections(changes: list[FieldChange]) -> list[CorrectionRecord]
def assess_correction_impact(record: CorrectionRecord) -> str   # LOW | MEDIUM | HIGH
def mark_factors_stale(record: CorrectionRecord) -> list[str]   # 受影响因子名
```

**severity 判定**（§45.4）：HIGH = adjfactor 变动；MEDIUM = `max_rel_diff > 0.01`（价格相对差 >1%）；LOW = 其余轻微修正。只 MEDIUM+ 告警。

**CLI**（数据平台侧）：

```bash
corrections report            # 修正汇总报告
factor list --stale           # 列 stale 因子（metadata.json stale_since，§62.3）
recompute --stale             # 重算 stale 因子
clear-stale                   # 人工确认后清除标记
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 覆盖写入，不版本化 | 多版本共存 | 存储膨胀；平台永远用最新数据 |
| 修正日志 `corrections/{asset}/{date}.jsonl` append | 只覆盖不记录 | 审计与影响追溯需要 |
| 每周全量重拉近 30 交易日逐行比对 | 信源的修正标记 | 数据源静默覆盖，无修正通知 |
| 季度全量近 1 年兜底 | 只做周扫 | 覆盖长周期回溯修正 |
| 比对容差 `CORRECTION_TOLERANCES` 四字段 | 精确相等 | 源侧浮点/口径微差会噪音告警 |
| 修正走完整 daily_ingest 流水线 | 只改 Parquet 文件 | DQC/raw/watermark 都要同步更新 |
| 下游**不自动重跑**，标 stale 人工决定 | 自动级联重算 | 慢、影响小、级联深、需人工判断 |
| severity LOW/MEDIUM/HIGH，只 MEDIUM+ 告警 | 全部告警 | 噪音淹没真问题 |
| `mark_factors_stale`（stale_since + depends_on） | 直接重算 | 重算时机由人定 |
| impact 记 `impact_log.jsonl` | 不记 | 回测重跑决策依据 |
| 回测重跑按 severity 表人工决定 | 一律重跑 / 一律不跑 | 修正影响差异大 |

## 实现要点

```python
# 检测（~80 行）：重拉 → 逐行比对 → 超容差入 changes
def detect_corrections(asset_type, lookback=30):
    dates = cal.range(today - lookback, today)
    for d in dates:
        fresh = adapter.fetch(asset_type, d)           # 重拉
        local = load_asset(asset_type, d)
        for field, tol in CORRECTION_TOLERANCES.items():
            diff = (fresh[field] - local[field]).abs()
            for i in diff[diff > tol].index:
                yield FieldChange(...)

# 应用（~40 行）：先记日志再覆盖写
def apply_corrections(changes):
    records = group_by_date(changes)
    for rec in records:
        append_jsonl(f"corrections/{asset}/{rec.trade_date}.jsonl", rec)  # "a" 追加
        daily_ingest(source, asset_type, rec.trade_date)                   # 完整流水线

# 影响传播（~60 行）
def assess_correction_impact(record):
    impact = {"trade_date": str(record.trade_date), "asset_type": record.asset_type,
              "num_changes": len(record.changes),
              "max_rel_diff": max(c.rel_diff for c in record.changes),
              "affected_tickers": list(set(c.ticker for c in record.changes))}
    if any(c.field == "adjfactor" for c in record.changes):
        impact["severity"] = "HIGH"          # 所有用复权价的因子都受影响
    elif impact["max_rel_diff"] > 0.01:
        impact["severity"] = "MEDIUM"        # 价格相对差 >1%
    else:
        impact["severity"] = "LOW"

# stale 标记（~30 行）：metadata.json.stale_since + depends_on 反查（源 §45 写 factor_registry.yaml，§62.3 已定 metadata.json，已改）
def mark_factors_stale(record):
    affected = [f for f in registry if set(f.depends_on) & touched_tables(record)]
    # 只 MEDIUM+ 告警；impact_log.jsonl 追加一条
```

新增代码 ≈210 行（detect ~80 + apply ~40 + impact ~60 + stale ~30）。

## 实施时定稿项

- `NUMERIC_FIELDS[asset_type]` 字段清单（与 §39.9/§41 联动，多处提及未统一定义）（§45）
- 季度全量扫描（近 1 年）的具体实现（是否复用 `detect_corrections(lookback=250)`）（§45）
- `corrections/{asset}/{date}.jsonl` 追加写 vs 覆盖写（apply 用 `"a"` 追加，同一天多次修正日志累积）（§45）
- `metadata.json` 的 `depends_on` 取值枚举（bars_daily/bars_minute/fundamental/continuous）待定（§45；源写 factor_registry.yaml → §62.3 metadata.json）
- `recompute --stale` 是全量重算还是增量（修正影响的日期范围）（§45）
- `impact_log.jsonl` 保留期与查询接口（CLI 只有 `report`，无 `impact` 子命令）（§45）
- 修正后 `source_tag` 是否标注 `correction` 标记（当前 `daily_ingest` 无此参数）（§45）
