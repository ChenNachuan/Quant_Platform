"""告警治理模块：去重键、级别只升不降、8 项条件表与分级通知通道。

核心红线：
- 每日每键 ``{date}_{alert_type}_{alpha_type_id}`` 在 ``logs/ops/alerts.jsonl`` 仅保留一行；
  同级或更低级别的重复触发完全静默。
- 告警级别只升不降：升级时原地覆写当日记录并按新级别重新推送，降级则忽略。
- 通道分级：终端输出覆盖所有级别，桌面通知（macOS ``osascript``）仅在 CRITICAL 触发。

``evaluate_alerts`` 为纯函数，``None``、NaN 或缺失字段视为“无可用数据源”而不触发；
传入非数值类型属于调用方契约错误并直接抛出异常。
"""

from __future__ import annotations

import json
import os
import subprocess
import threading
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

#: 告警记录持久化路径
ALERTS_JSONL = Path("logs/ops/alerts.jsonl")


class AlertLevel(Enum):
    """告警级别枚举；仅 CRITICAL 触发桌面通知。"""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"


#: 级别严重度顺序（同键告警级别只升不降）
_LEVEL_RANK: dict[AlertLevel, int] = {
    AlertLevel.MEDIUM: 0,
    AlertLevel.HIGH: 1,
    AlertLevel.CRITICAL: 2,
}

#: 三类告警规则分类
RULE_THRESHOLD = "threshold"
RULE_MUTATION = "mutation"
RULE_TREND = "trend"


def _num(facts: Mapping[str, Any], key: str) -> float | None:
    """读取 ``facts[key]`` 数值；``None`` 与 NaN 均视为无可用数据源。

    NaN 在阈值比较中恒为 False，必须提前转为 ``None`` 防止误入触发分支。
    """
    raw = facts.get(key)
    if raw is None:
        return None
    val = float(raw)  # 非数值属于调用方契约错误
    return None if val != val else val  # NaN 是唯一不等于自身的值


def _scope(facts: Mapping[str, Any]) -> str:
    return str(facts.get("alpha_type_id") or "")


# --- 条件谓词：返回实际触发的 (msg, scope) 列表 ---


def _breach_job_failure(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for job in facts.get("failed_jobs") or ():
        name = str(job.get("job_name") or "")
        error = job.get("error") or "unknown error"
        out.append((f"job {name} failed: {error}", name))
    return out


def _breach_data_delay(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    days = _num(facts, "data_delay_days")
    if days is None or days <= 1:
        return []
    return [(f"data delay {days:g} days (> 1)", "")]


def _breach_mdd(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    mdd = _num(facts, "mdd")
    base = _num(facts, "backtest_mdd")
    if mdd is None or base is None or mdd <= base * 1.5:
        return []
    return [(f"MDD {mdd:.2%} breaches backtest {base:.2%} x 1.5", _scope(facts))]


def _breach_sharpe(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    sharpe = _num(facts, "sharpe_6m")
    if sharpe is None or sharpe >= 0:
        return []
    return [(f"rolling 6m sharpe turned negative ({sharpe:g})", _scope(facts))]


def _breach_excess(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    months = _num(facts, "excess_negative_months")
    if months is None or months < 6:
        return []
    return [(f"excess return negative {months:g} months running (>= 6)", _scope(facts))]


def _breach_nav_drop(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    ret = _num(facts, "nav_daily_return")
    if ret is None or ret >= -0.03:
        return []
    return [(f"NAV daily return {ret:.2%} below -3%", _scope(facts))]


def _breach_turnover(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    turnover = _num(facts, "turnover")
    if turnover is None or turnover <= 1.5:
        return []
    return [(f"turnover {turnover:g} is over design (ceiling 1.0)", _scope(facts))]


def _breach_risk_reject(facts: Mapping[str, Any]) -> list[tuple[str, str]]:
    count = _num(facts, "risk_reject_count")
    if count is None or count <= 3:
        return []
    return [(f"risk rejections {count:g} in one day (> 3)", "")]


@dataclass(frozen=True)
class AlertCondition:
    """8 项告警条件表中的单条规则定义。"""

    id: int
    alert_type: str
    level: AlertLevel
    rule: str
    desc: str
    breach: Callable[[Mapping[str, Any]], Sequence[tuple[str, str]]]


#: 8 项告警条件配置表
ALERT_CONDITIONS: tuple[AlertCondition, ...] = (
    AlertCondition(
        1,
        "job_failure",
        AlertLevel.HIGH,
        RULE_THRESHOLD,
        "job failed after retries",
        _breach_job_failure,
    ),
    AlertCondition(
        2,
        "data_delay",
        AlertLevel.HIGH,
        RULE_THRESHOLD,
        "data delay > 1 day",
        _breach_data_delay,
    ),
    AlertCondition(
        3,
        "mdd_breach",
        AlertLevel.CRITICAL,
        RULE_MUTATION,
        "MDD breaches backtest x 1.5",
        _breach_mdd,
    ),
    AlertCondition(
        4,
        "sharpe_negative",
        AlertLevel.CRITICAL,
        RULE_TREND,
        "rolling 6m sharpe turned negative",
        _breach_sharpe,
    ),
    AlertCondition(
        5,
        "excess_negative",
        AlertLevel.HIGH,
        RULE_TREND,
        "excess negative 6 months running",
        _breach_excess,
    ),
    AlertCondition(
        6,
        "nav_drop",
        AlertLevel.MEDIUM,
        RULE_MUTATION,
        "NAV single-day return < -3%",
        _breach_nav_drop,
    ),
    AlertCondition(
        7,
        "turnover_excess",
        AlertLevel.MEDIUM,
        RULE_THRESHOLD,
        "turnover over design by > 50%",
        _breach_turnover,
    ),
    AlertCondition(
        8,
        "risk_reject",
        AlertLevel.MEDIUM,
        RULE_THRESHOLD,
        "risk rejects > 3 in a day",
        _breach_risk_reject,
    ),
)


def evaluate_alerts(facts: Mapping[str, Any]) -> list[dict[str, Any]]:
    """根据 8 项条件对 ``facts`` 进行纯函数求值，返回待发送的告警条目。"""
    if not isinstance(facts, Mapping):
        raise TypeError(f"facts must be a mapping, got {type(facts).__name__}")
    firings: list[dict[str, Any]] = []
    for cond in ALERT_CONDITIONS:
        for msg, scope in cond.breach(facts):
            firings.append(
                {
                    "alert_type": cond.alert_type,
                    "level": cond.level,
                    "msg": msg,
                    "alpha_type_id": scope,
                    "rule": cond.rule,
                }
            )
    return firings


# --- 通知通道 ---


def _osa_str(text: str) -> str:
    """转义 AppleScript 双引号字符串字面量。"""
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def desktop_notify(msg: str) -> None:
    """通过 macOS ``osascript`` 发送桌面通知（异常时降级至终端输出，不阻断主流程）。"""
    script = f'display notification {_osa_str(msg)} with title "Quant Alert"'
    try:
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True)
    except OSError:
        print(f"[desktop] {msg}")


def _fire_channels(level: AlertLevel, msg: str) -> None:
    print(f"[{level.value}] {msg}")
    if level is AlertLevel.CRITICAL:
        desktop_notify(msg)


# --- 有状态接收器：每日每键单行，级别只升不降 ---

_write_lock = threading.RLock()
_alert_state: dict[str, AlertLevel] = {}
_loaded_dates: set[str] = set()


def reset_state() -> None:
    """清空内存去重状态，供进程重启或测试时重新从文件加载。"""
    with _write_lock:
        _alert_state.clear()
        _loaded_dates.clear()


def _seed_state(iso_day: str) -> None:
    """从当日已落盘的告警文件中恢复去重级别状态。"""
    if iso_day in _loaded_dates:
        return
    _loaded_dates.add(iso_day)
    path = ALERTS_JSONL
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("date") != iso_day:
            continue
        key = row.get("key")
        if not key:
            continue
        try:
            level = AlertLevel(str(row.get("level")))
        except ValueError:
            continue
        prev = _alert_state.get(str(key))
        if prev is None or _LEVEL_RANK[level] > _LEVEL_RANK[prev]:
            _alert_state[str(key)] = level


def log_alert(payload: dict) -> None:
    """向 ``logs/ops/alerts.jsonl`` 追加一行告警（写入前检查末尾换行符防粘连）。"""
    with _write_lock:
        path = ALERTS_JSONL
        path.parent.mkdir(parents=True, exist_ok=True)
        needs_break = False
        if path.exists() and path.stat().st_size > 0:
            with path.open("rb") as fh:
                fh.seek(-1, os.SEEK_END)
                needs_break = fh.read(1) != b"\n"
        with path.open("a", encoding="utf-8") as fh:
            if needs_break:
                fh.write("\n")
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _replace_row(key: str, payload: dict) -> None:
    """级别升级时原地替换对应键的当日记录（使用 ``os.replace`` 原子写入）。"""
    path = ALERTS_JSONL
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    out: list[dict] = []
    replaced = False
    for row in rows:
        if not replaced and row.get("key") == key:
            out.append(payload)
            replaced = True
        else:
            out.append(row)
    if not replaced:
        out.append(payload)
    text = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in out)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _normalize_level(level: str | AlertLevel) -> AlertLevel:
    if isinstance(level, AlertLevel):
        return level
    try:
        return AlertLevel(str(level).upper())
    except ValueError:
        raise ValueError(f"unknown alert level: {level!r}") from None


def send_alert(
    level: str | AlertLevel,
    msg: str,
    alert_type: str = "generic",
    alpha_type_id: str = "",
) -> None:
    """发送并持久化告警；去重键为 ``{date}_{alert_type}_{alpha_type_id}``。

    同级或更低级别重复调用直接静默忽略；更高级别调用将覆写当日记录并重新推送各通道。
    """
    new_level = _normalize_level(level)
    today = date.today()
    dedup_key = f"{today:%Y%m%d}_{alert_type}_{alpha_type_id}"
    iso_day = today.isoformat()
    with _write_lock:
        _seed_state(iso_day)
        seen = _alert_state.get(dedup_key)
        if seen is not None and _LEVEL_RANK[seen] >= _LEVEL_RANK[new_level]:
            return
        row = {
            "date": iso_day,
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": new_level.value,
            "msg": msg,
            "alert_type": alert_type,
            "alpha_type_id": alpha_type_id,
            "key": dedup_key,
        }
        if seen is None:
            log_alert(row)
        else:
            _replace_row(dedup_key, row)
        _alert_state[dedup_key] = new_level
    # 锁外执行通道推送，避免通知 I/O 阻塞去重状态更新
    _fire_channels(new_level, msg)
