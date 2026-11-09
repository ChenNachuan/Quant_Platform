"""Alert governance: dedup key, severity only-up, 8-condition table, channel tiering.

``docs/design/ops/alerting.md`` is the contract. Three rule classes
(threshold / mutation / trend), eight concrete conditions, and the two
governance red lines:

* one row per ``{date}_{alert_type}_{alpha_type_id}`` per day in
  ``logs/ops/alerts.jsonl`` — a repeat at equal or lower level fires nothing
  at all;
* severity only ever rises — an upgrade rewrites the day's single row upward
  and re-fires the channels at the new level (so an escalation reaches the
  desktop); a downgrade is a no-op.

Channels: the terminal at every level, the desktop (macOS ``osascript``) at
CRITICAL only. ``evaluate_alerts`` is pure — it maps a facts dict to firings;
``send_alert`` is the stateful sink. The ``health_check`` body (monitoring)
evaluates and then sends each firing; the scheduler's direct job-failure
alert converges on the same dedup key via ``alert_type="job_failure"`` and
``alpha_type_id=<job name>``.

``evaluate_alerts`` facts contract — ``None``, NaN, or missing means "no
computable source" and never fires (a non-numeric value is a caller bug and
raises)::

    alpha_type_id           strategy scope for conditions 3-7
    failed_jobs             [{"job_name": str, "error": str | None}] -> cond 1
    data_delay_days         float, days since latest data     -> cond 2 (> 1)
    mdd, backtest_mdd       floats, current vs backtest MDD   -> cond 3 (> 1.5x)
    sharpe_6m               float, rolling 6-month sharpe     -> cond 4 (< 0)
    excess_negative_months  float, months running negative    -> cond 5 (>= 6)
    nav_daily_return        float, decimal single-day return  -> cond 6 (< -0.03)
    turnover                float, ratio                      -> cond 7 (> 1.5)
    risk_reject_count       float, day's BLOCK count          -> cond 8 (> 3)

Alert row schema (pinned here per the issue's open items): ``date`` (ISO day),
``ts`` (UTC ISO), ``level``, ``msg``, ``alert_type``, ``alpha_type_id``,
``key`` — the dedup key whose day part is ``%Y%m%d`` per the design formula.
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

#: Runtime landing spot for alert rows (``data/`` is gitignored runtime data).
ALERTS_JSONL = Path("logs/ops/alerts.jsonl")


class AlertLevel(Enum):
    """The design's three levels; CRITICAL is the one that reaches the desktop."""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"


#: Severity order. The only direction a key's level may move is up.
_LEVEL_RANK: dict[AlertLevel, int] = {
    AlertLevel.MEDIUM: 0,
    AlertLevel.HIGH: 1,
    AlertLevel.CRITICAL: 2,
}

#: The design's 3 rule classes, one per row of the condition table.
RULE_THRESHOLD = "threshold"
RULE_MUTATION = "mutation"
RULE_TREND = "trend"


def _num(facts: Mapping[str, Any], key: str) -> float | None:
    """``facts[key]`` as a float; ``None`` means "no computable source".

    NaN — pandas' missing marker, which ``float()`` accepts — is a missing
    value, not a number, and maps to ``None`` too: every comparison against
    NaN is False, so an unguarded NaN would fall through each threshold check
    into its breach branch and fire the condition.
    """
    raw = facts.get(key)
    if raw is None:
        return None
    val = float(raw)  # a non-numeric value is a caller contract violation
    return None if val != val else val  # NaN is the only value not equal to itself


def _scope(facts: Mapping[str, Any]) -> str:
    return str(facts.get("alpha_type_id") or "")


# ---------------------------------------------------------------------------
# Condition predicates — one per alert_type. Each returns (msg, scope) pairs
# for concrete breaches only; an empty list means the condition did not fire.
# ---------------------------------------------------------------------------


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
    """One row of the design's 8-condition table (id / type / level / rule / facts predicate)."""

    id: int
    alert_type: str
    level: AlertLevel
    rule: str
    desc: str
    breach: Callable[[Mapping[str, Any]], Sequence[tuple[str, str]]]


#: The 8-condition table (docs/design/ops/alerting.md), verbatim level + type.
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
    """Evaluate the 8-condition table against ``facts``; return the firings.

    Pure: no I/O, no state. Each firing is ``{"alert_type", "level", "msg",
    "alpha_type_id", "rule"}`` with ``level`` an :class:`AlertLevel`; the
    caller feeds them to :func:`send_alert`.
    """
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


# ---------------------------------------------------------------------------
# Channels
# ---------------------------------------------------------------------------


def _osa_str(text: str) -> str:
    """AppleScript double-quoted string literal."""
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def desktop_notify(msg: str) -> None:
    """Desktop notification via macOS ``osascript`` (the issue's pinned default).

    Best-effort: a missing or broken notifier degrades to a terminal line —
    the terminal channel has already fired either way and an alert must not
    take the pipeline down with it.
    """
    script = f'display notification {_osa_str(msg)} with title "Quant Alert"'
    try:
        subprocess.run(["osascript", "-e", script], check=False, capture_output=True)
    except OSError:
        print(f"[desktop] {msg}")


def _fire_channels(level: AlertLevel, msg: str) -> None:
    print(f"[{level.value}] {msg}")
    if level is AlertLevel.CRITICAL:
        desktop_notify(msg)


# ---------------------------------------------------------------------------
# Stateful sink — one row per key per day, severity only up
# ---------------------------------------------------------------------------

_write_lock = threading.RLock()
_alert_state: dict[str, AlertLevel] = {}
_loaded_dates: set[str] = set()


def reset_state() -> None:
    """Drop the in-memory dedup state; a fresh process reseeds from the file."""
    with _write_lock:
        _alert_state.clear()
        _loaded_dates.clear()


def _seed_state(iso_day: str) -> None:
    """Load the day's rows into the dedup state (a restart keeps its place)."""
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
    """Append one row to ``logs/ops/alerts.jsonl`` (design signature).

    A tail left without its trailing newline by an interrupted append would
    glue the new row onto the fragment and lose both rows to one unparseable
    line — so the tail is checked and a break line goes in first.
    """
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
    """Rewrite the key's single row in place (the severity-upgrade path).

    Corrupt lines are skipped the way ``_seed_state`` skips them — a partial
    append must not kill the escalation — and the rewrite lands through
    ``os.replace`` so an interrupted write can never truncate the day's
    history.
    """
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
    """Alert send + persist. Dedup key = ``{date}_{alert_type}_{alpha_type_id}``.

    A repeat at equal or lower level is a complete no-op. A repeat at a
    HIGHER level rewrites the day's row upward and re-fires the channels at
    the new level. A lower level never rewrites (severity only rises).
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
    # Channels fire outside the lock: notify I/O must not block dedup state.
    _fire_channels(new_level, msg)
