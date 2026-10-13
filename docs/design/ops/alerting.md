# alerting

> 域：ops · 对应代码 ops/alerting.py · 来源 §67.5（archive/OPEN_QUESTIONS.md）

## 职责与边界

- 出事怎么知道：告警规则 3 类（阈值/突变/趋势）、8 条具体条件、通道分层、告警治理（去重/只升不降/白名单）。
- 触发源是 [monitoring.md](monitoring.md) 的指标状态与 [scheduler.md](scheduler.md) 的 Job 失败事件（`run_job` 失败后直接 `send_alert`）。
- 告警落盘 `data/ops/alerts.jsonl`，并在 [reporting.md](reporting.md) 日报的告警列表中呈现。
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
    """告警追加写 data/ops/alerts.jsonl。"""
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 3 类规则：阈值 / 突变 / 趋势 | 只做阈值 | 衰减退出（夏普转负）是趋势问题，静态阈值抓不到 |
| 通道：终端全级别 V1 + 桌面通知 CRITICAL | 飞书/钉钉 | 单人看终端即可；IM 机器人是团队需求 |
| 邮件 V2 | Grafana | V2 Streamlit 已覆盖展示，不做独立告警栈 |
| 去重 key 同日同类型只发一条 | 每次触发都发 | 防轰炸 |
| severity 只升不降 | 允许降级 | 驾驶舱治理原则（同日同问题级别单调升） |
| 不做动态升级 | MEDIUM 自动升 HIGH | 误报放大器 |

## 实现要点

**告警规则 3 类**：阈值（指标超线）、突变（单日剧变）、趋势（持续恶化）。

**8 条具体条件**（对标驾驶舱 4 类异常分类）：

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

**通道表**：

| 通道 | 级别 | V1/V2 |
|---|---|---|
| 终端 print/log | 全部 | **V1** |
| 桌面通知（plyer / notify-send） | CRITICAL | **V1** |
| 邮件 | HIGH+ | V2 |
| 飞书/钉钉 | — | 不做 |
| Grafana | — | 不做（V2 Streamlit 已覆盖） |

```python
def send_alert(level, msg, alert_type="generic", alpha_type_id=""):
    dedup_key = f"{date.today():%Y%m%d}_{alert_type}_{alpha_type_id}"
    if dedup_key in _alerted_today:
        return                      # 同日同类型不重复告警
    _alerted_today.add(dedup_key)
    log_alert({"level": level, "msg": msg, "key": dedup_key})
    print(f"[{level}] {msg}")
    if level == "CRITICAL":
        desktop_notify(msg)         # plyer / notify-send
```

**告警治理三原则**（驾驶舱借鉴）：severity **只升不降**；去重 key 防轰炸；白名单机制（维护期 Job 静默）。

验证：同日同类型触发两次仅一条落盘；数据 Job 失败后收到 HIGH 告警（对接 [scheduler.md](scheduler.md) 失败传导）。

## 实施时定稿项

- desktop_notify 对 macOS 的具体实现（osascript 还是 plyer）（§67）
