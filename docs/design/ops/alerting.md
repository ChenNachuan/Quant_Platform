# alerting

> 域：ops · 对应代码 [ops/alerting.py](ops/alerting.py)

## 职责与边界

- 出事怎么知道：告警规则 3 类（阈值/突变/趋势）、8 条具体条件、通道分层、告警治理（去重/只升不降/白名单）。
- 触发源是 [monitoring.md](monitoring.md) 的指标状态与 [scheduler.md](scheduler.md) 的 Job 失败事件（`run_job` 失败后直接 `send_alert`）。
- 告警落盘 `logs/ops/alerts.jsonl`，并在 [reporting.md](reporting.md) 日报的告警列表中呈现。
- 不做：飞书/钉钉机器人、Grafana 告警、动态级别自动升级。

## 接口签名

```python
class AlertLevel(Enum):
    CRITICAL = "CRITICAL"   # 桌面通知
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"


def send_alert(level: str, msg: str, alert_type: str = "generic",
               alpha_type_id: str = "") -> None:
    """告警发送+落盘。去重 key = {date}_{alert_type}_{alpha_type_id}。"""


def log_alert(payload: dict) -> None:
    """告警追加写 logs/ops/alerts.jsonl。"""
```



**8 条具体条件**：

| # | 条件 | 级别 | 类型 |
|---|---|---|---|
| 1 | Job 失败（重试后仍 FAILED） | HIGH | 阈值 |
| 2 | 数据延迟 > 1 天 | HIGH | 阈值 |
| 3 | MDD 突破回测×1.5 | **CRITICAL** | 突变 |
| 4 | rolling 6 月夏普转负 | **CRITICAL** | 趋势 |
| 5 | 超额持续为负 6 月 | HIGH | 趋势 |
| 6 | NAV 单日 < -3% | MEDIUM | 突变 |
| 7 | 换手 > 50% 超设计 | MEDIUM | 阈值 |
| 8 | 风控拦截单日 > 3 笔 | MEDIUM | 阈值 |


## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 3 类规则：阈值 / 突变 / 趋势 | 只做阈值 | 衰减退出（夏普转负）是趋势问题，静态阈值抓不到 |
| 通道：终端全级别 V1 + 桌面通知 CRITICAL | 飞书/钉钉 | 单人看终端即可；IM 机器人是团队需求 ||
| 去重 key 同日同类型只发一条 | 每次触发都发 | 防轰炸 |
| severity 只升不降 | 允许降级 | 驾驶舱治理原则（同日同问题级别单调升） |

## 实现要点

详见 [ops/alerting.py](ops/alerting.py)

核心约定:
- 去重 key = {date}_{alert_type}_{alpha_type_id},同日同类型不重复
- severity只升不降,白名单机制支持维护期 Job静默
- CRITICAL级别触发桌面通知(plyer/notify-send)
- 告警落盘 logs/ops/alerts.jsonl,append-only
