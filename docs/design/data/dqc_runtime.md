# dqc_runtime

> 域：data · 对应代码 data/dqc/report.py · 来源 §38（archive/OPEN_QUESTIONS.md）

## 职责与边界

DQC 的**运行时**：何时跑、跑多快、失败了怎么办、结果怎么落盘与呈现。规则定义在 [dqc_rules.md](dqc_rules.md)，处置判定在 [dqc_severity.md](dqc_severity.md)，本模块收口执行时机、性能预算、失败重跑、并行策略、报告双表、看板与告警通道。

定稿入口签名：`ingest_with_dqc(df, date, asset_type)`；`batch_rejected` / `reject_reason` / `flag_rate` 收敛进 results（不再单独字段）。

## 接口签名

### 报告双表

**行级明细 `_dqc_results.parquet`**（按月分区，append-only）：

| 字段 | 含义 |
|---|---|
| run_date / run_time | 执行日 / 执行时刻 |
| data_date | 被检数据的业务日 |
| asset_type | bars_daily 等 5 类 |
| rule_id / rule_name / dimension / severity | 规则标识（dimension=四维之一） |
| action_taken | pass / drop / flag / reject_batch |
| total_rows / failing_rows / failing_rate | 规模与失败率 |
| sample_tickers | 失败样本前 5 个 |
| sample_values | 失败样本值 |
| rule_version | 规则版本（semver），可审计 |

**批次汇总 `_dqc_batch_summary.parquet`**（单文件追加）：

| 字段 | 含义 |
|---|---|
| run_time / data_date / asset_type | 执行时刻 / 业务日 / 资产 |
| total_rows / rows_dropped / rows_flagged / rows_clean | 行数账目 |
| drop_rate / flag_rate | 失败率 |
| batch_status | accepted / rejected / accepted_with_warnings |
| hard_rules_failed / soft_rules_failed | 触发 HARD/SOFT 的规则 ID 列表（JSON，如 `[C03,L01]`） |
| elapsed_seconds | 本批耗时 |

保留期：明细 1 年 / 汇总永久；cleanup 月度跑。

### CLI

```text
python -m dqc.report dashboard          # CLI + DuckDB 看板，不建 Web
python -m dqc.report quarantine         # 隔离区查询（见 quarantine_raw.md）
python -m dqc.engine reingest --asset X --date D    # 重下载+全重跑+覆盖写
python -m dqc.engine revalidate --asset X --start S --end E  # 只重验不重下载
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| **Pre-load 一遍**（写 Parquet 前跑 DQC） | post-load 两遍 | 坏数据入库即被消费；事后修改贵；Parquet immutable |
| 周批量不做第二遍全量 | 两遍模型（入库+全量重验） | 跨资产检查是周批量拼批，天天全量浪费 |
| 耗时预算：日频 <1s、分钟频 2–5s | 专项优化 | 瓶颈是网络 IO 不是 DQC；L2 届时再优化 |
| **不抽样**：增量全量 + 每周全量 | 抽样检查 | 抽样漏检 |
| 失败 = **重跑整批**（`reingest`） | 只重跑失败部分 | 局部重跑难保证批内一致性 |
| reingest / revalidate 分工 | 一个命令 | 重下载+重跑 vs 只重验，成本差一个数量级 |
| 日常单线程，回填按日期并行 joblib n_jobs=4 | 日常也并行 | 日常一天一批无并行收益；1200 天回填 ≈25 分钟 |
| 看板 = CLI + DuckDB | 自建 Web 看板 | 单人单机 grep/jq/SQL 足够 |
| 告警 = logging ERROR/WARNING/INFO + `dqc_alerts.log` | 接飞书/邮件/Slack | 先本地闭环，通道后接（ops/alerting 管） |
| 明细保留 1 年、汇总永久 | 都永久 | 明细是排查用，汇总够审计 |

性能结论：**DQC 不是瓶颈**（网络 IO 是）。L2 数据量级上来后再优化。

## 实现要点

```python
def ingest_with_dqc(df, date, asset_type):
    results = run_all_checks(df, date, asset_type)      # HARD+SOFT 一遍跑完
    # 行级 drop → 批次安全阀 → quality_flag → 软告警 → 入库（七步见 dqc_severity.md）
    write_parquet(df_clean, date)
    append_detail(results)                              # _dqc_results 按月分区
    append_summary(batch_row)                           # _dqc_batch_summary 单文件
    if batch_rejected:
        save_quarantine(df, asset_type, date, e.detail) # 拒收也落盘（§39 修正）
        log_error(...)                                  # 进 dqc_alerts.log
```

```text
# 失败重跑分工
reingest   --asset --date       # 重下载 + 全重跑 + 覆盖写（含源侧缺数场景）
revalidate --asset --start --end # 只重验本地已有数据（规则改了阈值时用）

# 回填并行
joblib.Parallel(n_jobs=4)(delayed(ingest_day)(d) for d in dates)   # 按日期切分
```

报告双表职责：明细答「哪条规则、哪些票、什么值」；汇总答「这批干净不干净、丢了多少」。批次拒收/告警事件也进报告（batch_status + action_taken='reject_batch'）。

## 实施时定稿项

- 月分区并发写入的安全性（回填 joblib 并行时 `_dqc_results` 同月分区多写者）（§38）
- `run_all_checks` 返回 results 的完整 schema（CheckResult 字段清单）（§38）
- `dqc_alerts.log` 的轮转策略（§38）
- K05（adjfactor 跨日稳定性）在按日期并行重跑时的竞态窗口（§38）
- 飞书 webhook 等外部告警通道的接通阈值与开关（现只落本地 log；归 ops/alerting）（§38）
- L2 数据量级下的 DQC 优化（现不做）（§38）
- ~~`_dqc_results.parquet` 是否记录 batch reject/alarm 事件~~ 已定 → 是（本节双表）
