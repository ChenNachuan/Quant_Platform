# scheduler

> 域：ops · 对应代码 [ops/scheduler.py](../../../ops/scheduler.py)
## 职责与边界

- 何时跑什么：APScheduler + 薄封装，5 类 Job 的编排、触发、重试、失败传导、运行记录。
- 日频全链路 `daily_pipeline`：数据 → 因子 → 信号 → 巡检 → 日报（月末追加归因，见 [reporting.md](reporting.md)）。
- 不做：分布式调度、DAG 引擎、手动触发的 Web 界面（手动 = 直接调 `run_job`）。
- 与 [logging.md](../core/logging.md) 的边界：日志格式由 `core/logging.py` 负责；本模块写运行记录 `logs/runs/jobs.jsonl`。

## 接口签名

```python
def run_job(name: str, fn=None, retries: int = 1, timeout_s: int = 300,
            on_failure: str = "skip") -> dict:
    """带重试的 Job 执行。默认重试 1 次超时 300s。on_failure: "skip" | "hold"。"""

def daily_pipeline() -> None:
    """日频流水线: 数据→因子→信号→巡检→日报。"""
```

**运行记录 schema**（`logs/runs/jobs.jsonl`，一条一跑）：

```json
{"run_id": "20261004_compute_all_001", "job_name": "compute_all",
 "started_at": "...", "finished_at": "...", "status": "SUCCESS",
 "duration_s": 42.3, "error": null, "retry_count": 0,
 "output_summary": "45 factors, 2 failed"}
```

`status` 四态：`SUCCESS | FAILED | RETRY | SKIPPED`。

**5 类 Job**：

| Job | 频率 | 时机 | 失败处理 |
|---|---|---|---|
| 数据采集（fetch_stock_daily 等） | 日频 | 16:00 | 重试 2 次→跳过下游 |
| 因子计算（compute_all） | 日频 | 16:30 | 重试 1 次→跳过下游 |
| 信号产出（run_daily_signal） | 日频/月频 | 16:45 | 重试 1 次→**hold** |
| 回测 | 手动 | — | — |
| 监控巡检（health_check） | 日频 | 17:00 | 重试 1 次→告警 |


## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| APScheduler + 薄封装（V1） | crontab | 太原始——无失败重试、无状态记录 |
| 进程内 BlockingScheduler | Prefect / Celery / Airflow | 服务化部署违反零运维，Job<10 用不上 DAG |
| 不用 DAG，for 顺序执行 | 依赖图编排 | Job<10 且依赖线性（数据→因子→信号→巡检）；V2 Job>20 再迁 Prefect |
| 触发 = CronTrigger 周中 + 手动 | 事件驱动触发 | 月频/日频无实时事件 |
| 重试默认 1 次、数据 2 次 | 无限重试 | 数据源抖动多一次机会即可，其余重试无意义 |
| 失败传导：数据失败→跳过下游 | 一律继续 | 无数据算不了下游 |
| 失败传导：信号失败→hold 上日 Portfolio | 清仓 |  不清仓铁律 |


## 实现要点

详见 [ops/scheduler.py](../../../ops/scheduler.py)

核心约定:
- daily_pipeline顺序执行5个Job,CronTrigger周一至周五16:45触发
- run_job带重试+超时,失败后按 on_failure分流(skip跳下游/hold保持仓位)
- 失败传导:数据 Job失败→下游标SKIPPED;信号Job失败>hold不清仓+告警
- 运行记录一条一跑追加写 logs/runs/jobs.jsonl
- 月末 daily_pipeline 追加归因 Job(见reporting.md)
