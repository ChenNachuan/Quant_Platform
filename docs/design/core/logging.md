# logging

> 域：core · 对应代码 core/logging.py · 来源 §69.3 / §67.2（archive/OPEN_QUESTIONS.md）

## 职责与边界

平台日志与运行记录：Python logging + JSON Lines（console 人读、jsonl 机器查），以及 `logs/runs/` 三类运行元数据的落盘约定。只做**格式与落盘**，不做日志采集/汇聚/检索服务。

边界：Job 的调度重试语义归 ../ops/scheduler.md（其 `job_runs.jsonl` 与本文 `logs/runs/jobs.jsonl` 字段统一，§69 修订）；`config_snapshot` 的生成归 config.md，本文只规定它钉进 backtests 记录。

## 接口签名

```python
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str: ...

def setup_logging(level: str = "INFO") -> None: ...
```

字段规范：`ts`(UTC ISO) / `level` / `module` / `func` / `msg` 固定五字段 + extra 按需（`job_name` / `alpha_type_id` / `duration_ms` …）+ `exception`（仅 ERROR 带堆栈）。

级别规范（§69.3）：

| 级别 | 何时用 | 例 |
|---|---|---|
| ERROR | 任务失败、数据不可用 | Job 重试后仍 FAILED |
| WARNING | 可继续但需关注 | 风控告警、条数偏差 ±30% |
| INFO | 正常流程节点 | Job 开始/结束、回测进度 |
| DEBUG | 排查用，默认关 | 单标的订单明细 |

`logs/runs/` 三类 JSONL（§69.3）：

| 文件 | 记什么 | 关键字段 |
|---|---|---|
| `jobs.jsonl` | Job 运行元数据 | run_id / job_name / started_at / finished_at / status / duration_s / error / retry_count / output_summary（§67.2 定稿，与 ops 调度器字段统一） |
| `backtests.jsonl` | 回测运行元数据 | run_id / alpha_type_id / period / metrics(sharpe/mdd/…) / **config_snapshot "path@hash"** |
| `live.jsonl` | 实盘/paper 运行元数据 | run_id / mode("sim"\|"qmt") / orders_count / fills_count / nav |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Python logging + JSON Lines | ELK/Loki 等日志栈 | 个人本地 JSONL + grep/jq 零运维（对标 Datamall NFS→MinIO 降级：公司是多机汇聚需求） |
| console 人读 + jsonl 机器查双输出 | 单一格式 | 两种消费场景并存 |
| 不需要分布式追踪 | instance 时间戳对齐 / Prometheus / trigger_history / trace_id | 单进程：`duration_ms` + grep/jq + `job_runs.jsonl` 替代 |
| `config_snapshot = "path@hash"` 钉进 backtests 记录 | 只留配置路径 | 把"这跑用的哪版配置"钉进运行记录，配合 git log 可复现 |
| 运行记录独立 JSONL（logs/runs/） | 混进应用日志 | 运行元数据是结构化查询对象，应用日志是排查流 |

## 实现要点

```python
class JsonFormatter(logging.Formatter):
    """日志一行一 JSON: {ts UTC, level, module, func, msg} + extra + exception。"""
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "module": record.module,
            "func": record.funcName,
            "msg": record.getMessage(),
        }
        for key, value in getattr(record, "__dict__", {}).items():
            if key not in ("args", "msg", "exc_info", "exc_text"):
                payload.setdefault(key, value)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> None:
    """console 人读 + logs/app_{YYYYMMDD}.jsonl 机器查。"""
    root = logging.getLogger()
    root.setLevel(level)
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    fileh = logging.FileHandler(f"logs/app_{datetime.now():%Y%m%d}.jsonl", encoding="utf-8")
    fileh.setFormatter(JsonFormatter())
    root.addHandler(console)
    root.addHandler(fileh)
```

## 实施时定稿项

- `backtests.jsonl` 的 metrics 字段全集（§69.6）
- `logs/runs/` 三 schema 与 ../ops/scheduler.md 的 `job_runs.jsonl` 落点统一实施时核对一遍字段名（§69 修订已定方向）
