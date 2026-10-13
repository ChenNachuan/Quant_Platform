# scheduler

> 域：ops · 对应代码 ops/scheduler.py · 来源 §67.2 / §69.3（archive/OPEN_QUESTIONS.md）

## 职责与边界

- 何时跑什么：APScheduler + 薄封装，5 类 Job 的编排、触发、重试、失败传导、运行记录。
- 日频全链路 `daily_pipeline`：数据 → 因子 → 信号 → 巡检 → 日报（月末追加归因，见 [reporting.md](reporting.md)）。
- 不做：分布式调度、DAG 引擎、手动触发的 Web 界面（手动 = 直接调 `run_job`）。
- 与 [logging.md](../core/logging.md) 的边界：日志格式由 `core/logging.py` 负责；本模块写运行记录 `logs/runs/jobs.jsonl`（字段已与 §67.2 的 `job_runs.jsonl` 统一）。

## 接口签名

```python
def run_job(name: str, fn=None, retries: int = 1, timeout_s: int = 300,
            on_failure: str = "skip") -> dict:
    """带重试的 Job 执行。默认重试 1 次超时 300s。on_failure: "skip" | "hold"。"""

def daily_pipeline() -> None:
    """日频流水线: 数据→因子→信号→巡检→日报。"""
```

**运行记录 schema**（`logs/runs/jobs.jsonl`，一条一跑，§69.3 与 §67.2 字段统一）：

```json
{"run_id": "20261004_compute_all_001", "job_name": "compute_all",
 "started_at": "...", "finished_at": "...", "status": "SUCCESS",
 "duration_s": 42.3, "error": null, "retry_count": 0,
 "output_summary": "45 factors, 2 failed"}
```

`status` 四态：`SUCCESS | FAILED | RETRY | SKIPPED`。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| APScheduler + 薄封装（V1） | crontab | 太原始——无失败重试、无状态记录 |
| 进程内 BlockingScheduler | Prefect / Celery / Airflow | 服务化部署违反零运维，Job<10 用不上 DAG |
| 不用 DAG，for 顺序执行 | 依赖图编排 | Job<10 且依赖线性（数据→因子→信号→巡检）；V2 Job>20 再迁 Prefect |
| 触发 = CronTrigger 周中 + 手动 | 事件驱动触发 | 月频/日频无实时事件（§68.2 同理） |
| 重试默认 1 次、数据 2 次 | 无限重试 | 数据源抖动多一次机会即可，其余重试无意义 |
| 失败传导：数据失败→跳过下游 | 一律继续 | 无数据算不了下游 |
| 失败传导：信号失败→hold 上日 Portfolio | 清仓 | §65.6 不清仓铁律 |
| 运行记录 JSONL 落 `logs/runs/jobs.jsonl` | 数据库表 | 对标 dw_asset_job_instance；单机 grep/jq 零运维 |

## 实现要点

**5 类 Job**：

| Job | 频率 | 时机 | 失败处理 |
|---|---|---|---|
| 数据采集（fetch_stock_daily 等） | 日频 | 16:00 | 重试 2 次→跳过下游 |
| 因子计算（compute_all） | 日频 | 16:30 | 重试 1 次→跳过下游 |
| 信号产出（run_daily_signal） | 日频/月频 | 16:45 | 重试 1 次→**hold**（§65.6） |
| 回测 | 手动 | — | — |
| 监控巡检（health_check） | 日频 | 17:00 | 重试 1 次→告警 |

```python
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


def daily_pipeline():
    run_job("fetch_stock_daily", retries=2)
    run_job("compute_all", retries=1)
    run_job("run_daily_signal", retries=1, on_failure="hold")   # §65.6
    run_job("health_check", retries=1)
    # 17:05 write_daily_report（见 reporting.md）；月末追加归因 Job


scheduler = BlockingScheduler()
scheduler.add_job(daily_pipeline,
                  CronTrigger(hour=16, minute=45, day_of_week="mon-fri"),
                  id="daily_pipeline")
```

```python
def run_job(name, fn=None, retries=1, timeout_s=300, on_failure="skip"):
    for attempt in range(retries + 1):
        try:
            result = run_with_timeout(fn or JOB_REGISTRY[name], timeout_s)
            return log_job_run(name, "SUCCESS", result, attempt)
        except Exception as e:
            if attempt < retries:
                continue
            log_job_run(name, "FAILED", error=str(e), retry_count=attempt)
            if on_failure == "hold":
                hold_previous_portfolio(name)   # §65.6 不清仓
            else:
                mark_downstream_skipped(name)   # 数据失败→跳过后续
            send_alert("HIGH", f"job {name} failed: {e}")   # alerting.md
            return {"status": "FAILED"}
```

验证：手动触发 `daily_pipeline`，5 Job 顺序执行、`jobs.jsonl` 记录完整；人为制造数据 Job 失败 → 下游 `SKIPPED` 且告警 HIGH。

## 实施时定稿项

- APScheduler 与手动触发的互斥（跑一半手动再跑会不会双写 job_runs）（§67）
