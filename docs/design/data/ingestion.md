# ingestion

> 域：data · 对应代码 data/ingestion.py · 来源 §21 + §41（archive/OPEN_QUESTIONS.md）

## 职责与边界

多源入库的**仲裁与编排**：谁是主源、备源何时上场、两源数据怎么比、缺数怎么补。单条链路的拉取在 [source_adapter.md](source_adapter.md)，DQC 在 `dqc_*`，落盘在 [quarantine_raw.md](quarantine_raw.md) / `storage_*`；本模块回答「**多个源之间怎么协作**」。

核心原则一句话：**单源写入 asset 层、多源交叉校验**——备源只校验补缺，不混合写入。

## 接口签名

```yaml
# source_priority.yaml — 主源指定表
bars_daily:    {priority: [tushare, akshare], field_override: {adjfactor: tushare}}
bars_minute:   {priority: [tushare]}
fundamental:   {priority: [akshare, tushare], field_override: {pe_ttm: tushare}}
option_basic:  {priority: [amazingdata]}
reference:     {calendar: ..., industry: ...}   # 按表指定
```

```python
@dataclass
class MismatchDetail:   # 按字段聚合（非逐 ticker 明细）
    field: str
    mismatch_count: int; mismatch_rate: float; max_abs_diff: float
    sample_tickers: list[str]   # 前 5 个不一致的 ticker

@dataclass
class CrossCheckResult:
    asset_type: str; trade_date: str
    primary_source: str; secondary_source: str
    total_rows: int; matched_rows: int
    mismatched_fields: dict[str, MismatchDetail]
    missing_in_primary: int; missing_in_secondary: int

def cross_source_check(df_a, df_b, asset_type) -> CrossCheckResult: ...
def should_cross_check(...) -> bool:   # hook：主源入库完成 + 备源 raw 已到达
```

容差（按字段类）：price 0.01 绝对 / volume,amount 0.001 相对 / adjfactor 1e-6 / pe_pb 0.02 / default 0.001。

对比时机：**Normalizer 之后、两源都入库后**才比（比的是标准化值，不是 raw）。

### 跨源校验节奏与响应

| 项 | 定稿 |
|---|---|
| 频率 | 日频每天；分钟频每周抽 1 天；reference 每次更新；factor 不做 |
| 结果落盘 | `cross_check/{asset_type}/{date}.json`（明细）+ `summary.parquet`（汇总，明细 1 年 / 汇总永久） |
| <0.1% 不一致 | 静默记录 |
| 0.1–1% | WARNING |
| >1% | ERROR |
| adjfactor 不一致 | **始终告警**（无论比例） |

### 备源补数

允许，但严格标记：`source_tag = "{backup}:backfill"` 写入 Parquet **metadata**（不是数据列），含 `ingest_time`。主源恢复后**次日覆盖**；不自动回填，手动 `--force`；`audit backfill` CLI 审计补数记录。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 单源写 asset，备源只校验/补缺 | 多源混合写入 | 血缘清晰，问题可追到源 |
| **不做自动主源选举** | 主源挂了自动切备源为主 | 静默换人是事故温床 |
| `source_priority.yaml` 按 asset_type 定 priority + field_override | 硬编码优先级 | 字段级覆盖（如 adjfactor 只认 tushare） |
| 主源强规则失败 → **自动 fallback 备源入库** | 空缺等主源 | 必须带 backfill 标记（§21.2/§41.6） |
| 跨源对比在 Normalizer 后、两源都入库后 | raw 对比 / 入库前对比 | raw 列名口径不同；入库后才有完整日切面 |
| 对比容差按字段类给 | 全局单一容差 | 价格/成交量/比率的误差量纲不同 |
| 分级响应（静默/WARNING/ERROR + adjfactor 永远告警） | 不一致即拒收 | 跨源合理误差存在，硬拒频繁误杀（§37 场景表） |
| 备源补数写 metadata 不写数据列 | 加 source 列污染 schema | asset 层 schema 稳定，血缘走 Parquet metadata |
| 补数不自动回填，手动 `--force` | 自动回补历史 | 源只定手动触发，不自动回补（§41） |

**必须规避的坑**（§21.3，公司停牌复权 bug 244038313）：`DLS_Basedata_US` 在 `suspend==1` 时把 `adjfactor` 写死为 1。**每个 SourceAdapter 的字段映射逻辑必须有测试覆盖**，重点边界：停牌、缺值、CA 事件。

## 实现要点

```python
def daily_ingest(asset_type, trade_date):
    primary = source_priority[asset_type].priority[0]
    df = get_adapter(primary).fetch(asset_type, trade_date)      # 主源入库（经 DQC）
    ingest_with_dqc(...)
    for backup in source_priority[asset_type].priority[1:]:
        if raw_exists(backup, asset_type, trade_date) and should_cross_check(...):
            result = cross_source_check(df, normalize(raw(backup)), asset_type)
            save_cross_check(result)                              # json + summary.parquet
            respond(result.mismatch_rate)                         # 静默/WARNING/ERROR
    if primary_failed_hard and backup_ok:                         # fallback 补数（§21.2）
        ingest_with_dqc(backup_df, ...)                           # metadata: source_tag="{backup}:backfill"
        log_degraded(...)                                         # 降级告警
```

字段级 override：`field_override` 里的字段从指定源取值替换（如 `adjfactor` 只信 tushare；替换阶段见定稿项）。新增数据源 = Adapter + Normalizer + 本文件 priority 表加一行。

## 实施时定稿项

- ~~跨源对比容差~~ 已定 → §41（price/volume/adjfactor/pe_pb/default 五档）
- ~~fallback 优先级规则与降级告警~~ 已定 → §41（priority 列表 + `log_degraded` + source_tag 标记）
- ~~主源指定表（`source_priority.yaml`）的存放位置~~ 已定 → §41.2：`data_platform/config/source_priority.yaml`（git 追踪）
- `NUMERIC_FIELDS[asset_type]` 参与对比的字段清单（§41）
- `raw_exists(backup, asset_type, date)` 的实现（查 raw 分区目录 vs watermark）（§41）
- 「主源当日缺失 → 等 2 小时 → 仍缺才备源补数」的调度实现（§41.6；2 小时是补数等待窗，不是跨源对比前置等待——跨源对比两源齐了就比）（→ ops/scheduler.md）（§41）
- 分钟频每周抽样日的选择规则（固定周一？随机？）（§41）
- `field_override` 与 priority 列表的合并细节（字段源不在 priority 里怎么办）（§41）
- `cross_check/summary.parquet` 与 `_dqc_batch_summary.parquet` 是否合并（§41）
- 多备源时逐个对主源比 vs 备源互比（§41）
- `AllSourcesFailed`（主备全挂）是否也进 quarantine（§41）
