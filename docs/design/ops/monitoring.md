# monitoring

> 域：ops · 对应代码 ops/monitoring.py · 来源 §67.3 / §69.3（archive/OPEN_QUESTIONS.md）

## 职责与边界

- 跑得怎么样：策略级 8 指标 + 系统级 4 指标的采集、分档（健康/警告/退出）、时点落盘。
- 指标是 [alerting.md](alerting.md) 的触发源（阈值条件由本模块算出的状态驱动），也是 [reporting.md](reporting.md) 日报的输入。
- 不做：Web 看板（V2 Streamlit 需要时再加）、Prometheus 指标体系（单进程不需要，§69.3 对照表）。

## 接口签名

```python
def collect_metrics(date: str) -> dict:
    """采集策略级 + 系统级指标，返回 {"strategy": {...}, "system": {...}}。
    每项含 value 与 status（健康/警告/退出）。"""

def append_metrics_snapshot(date: str, metrics: dict) -> None:
    """时点指标序列追加写 data/ops/metrics.jsonl。"""
```

阈值分档：策略级指标三档（健康 / 警告 / 退出），系统级指标两档（正常 / 超阈值带级别）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 策略级 8 指标三档阈值 | 只看收益 | 衰减退出要靠 rolling IC/夏普/回撤对照，不是单指标 |
| 系统级 4 指标 | 全量系统监控 | 单机 4 项已覆盖"跑没跑成、数据到没到" |
| 指标阈值与 §62.4/§64.7 衰减退出同一套 | 两套阈值 | 同一问题（衰减）不应有两套判据 |
| 指标时点序列落 `data/ops/metrics.jsonl` | 只进日报 | 告警与趋势要连续序列，日报是快照 |
| 不做 Web 看板 | Grafana / Streamlit（V1） | Markdown 日报够用；V2 再加 |

## 实现要点

**策略级 8 指标**：

| 指标 | 健康 | 警告 | 退出 | 来源 |
|---|---|---|---|---|
| 累计收益 | > 0 | < 0 持续 6 月 | — | nav 历史 |
| rolling 6 月夏普 | > 0.5 | < 0.3 | < 0 | §62.4 |
| 最大回撤 | < 回测 MDD | > 回测×1.2 | > 回测×1.5 | 对比 eval_report |
| 超额收益 | > 0 | < 0 持续 6 月 | < 0 持续 1 年 | vs 基准 |
| rolling IC | > 70% 历史均值 | < 50% | < 30% | §64.7 |
| 持仓数 | 与设计一致 | 偏差 > 20% | — | snapshot |
| 换手率 | 30–100% | > 50% 超设计 | — | compute_turnover |
| 单笔成本 | < 0.4% | > 0.5% | — | FeeModel 统计 |

**系统级 4 指标**：

| 指标 | 阈值 | 级别 |
|---|---|---|
| Job 成功率（rolling 7 天） | < 80% | HIGH |
| Pipeline 总耗时 | > 30 min | MEDIUM |
| 数据新鲜度 | 最新交易日 > 1 天前 | HIGH |
| 数据条数偏差 | vs 历史均值 ±30% | HIGH |

指标采样写 `data/ops/metrics.jsonl`（时点指标序列，供告警与趋势用）；日报展示见 [reporting.md](reporting.md)。

验证：跑完 `daily_pipeline` 后 `metrics.jsonl` 有当日快照（端到端验证 5 条在 §67.6，分落 scheduler/reporting）。

## 实施时定稿项

- metrics.jsonl 的 schema 字段定稿（§67）
- 系统指标「数据条数偏差 ±30% HIGH」在 [alerting.md](alerting.md) 8 条告警条件中无对应条目（源 §67.3 系统表与 §67.5 条件表的缺口，照录不缝合）——实施时定稿是补一条告警条件还是仅指标上报（§67）
- `collect_metrics` / `append_metrics_snapshot` 函数名为接口脚手架（源 §67.3 只给出 `write_daily_report`，归 reporting.md），实施时可按需改名（§67.3）
