# logging

> 域：core · 对应代码 [core/logging.py](../../../core/logging.py)
## 职责与边界

平台日志与运行记录：Python logging + JSON Lines（console 人读、jsonl 机器查），以及 `logs/runs/` 三类运行元数据的落盘约定。只做**格式与落盘**，不做日志采集/汇聚/检索服务。

边界：Job 的调度重试语义归 ../ops/scheduler.md（其 `job_runs.jsonl` 与本文 `logs/runs/jobs.jsonl` 字段统一，§69 修订）；`config_snapshot` 的生成归 config.md，本文只规定它钉进 backtests 记录。

## 接口签名

```python
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str: ...

def setup_logging(level: str = "INFO") -> None: ...
```

字段规范：`ts`(UTC ISO) / `level` / `module` / `func` / `msg` 固定五字段 + extra 按需 + `exception`（仅 ERROR 带堆栈）。

级别规范：

| 级别 | 何时用 | 例 |
|---|---|---|
| ERROR | 任务失败、数据不可用 | Job 重试后仍 FAILED |
| WARNING | 可继续但需关注 | 风控告警、条数偏差 |
| INFO | 正常流程节点 | Job 开始/结束、回测进度 |
| DEBUG | 排查用，默认关 | 单标的订单明细 |

`logs/runs/` 三类 JSONL：

| 文件 | 记什么 | 关键字段 |
|---|---|---|
| `jobs.jsonl` | Job 运行元数据 | run_id / job_name / started_at / finished_at / status / duration_s / error / retry_count / output_summary |
| `backtests.jsonl` | 回测运行元数据 | run_id / alpha_type_id / period / metrics(sharpe/mdd/…) / **config_snapshot "path@hash"** |
| `live.jsonl` | 实盘/paper 运行元数据 | run_id / mode/ orders_count / fills_count / nav |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Python logging + JSON Lines | ELK/Loki 等日志栈 | 个人本地 JSONL + grep/jq 零运维 |
| console 人读 + jsonl 机器查双输出 | 单一格式 | 两种消费场景并存 |
| 不需要分布式追踪 | Prometheus / trace_id | 单进程：`duration_ms` + grep/jq 足够 |
| `config_snapshot = "path@hash"` | 只留配置路径 | 把"这跑用的哪版配置"钉进运行记录，配合 git log 可复现 |
| 运行记录独立 JSONL（logs/runs/） | 混进应用日志 | 运行元数据是结构化查询对象，应用日志是排查流 |

## 实现要点

详见[core/logging.py](../../../core/logging.py)

核心约定：

- 日志一行一JSON：固定五字段+extra+exception
- console人读格式+Logs/app_{YYYYMMDD}.jsonl机器查格式，双输出
- Logs/runs/三类 JSONL的字段与 ops/scheduler.md的job_runs.jsonl保持一致
- config_snapshot 钉进 backtests记录，load_secrets()返回值永不出现在任何日志中
