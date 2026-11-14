"""监控采集与分级模块：12 项指标快照与健康巡检主逻辑。

包含 8 项策略级指标（``healthy`` / ``warning`` / ``exit``）与 4 项系统级指标
（``ok`` / ``HIGH`` / ``MEDIUM``）。分级优先判定 ``exit`` 区间，其次 ``warning``，
其余区间记为 ``healthy``；无可用数据源时数值落为 ``null``，状态记为 ``warning``
（策略）或 ``MEDIUM``（系统）且不触发告警。

所有数据源读取保持只读且空值安全，损坏文件或不可达数据源降级为无可用数据源，
绝不因读取异常中断巡检。数据源包括 ``Journal`` 每日 Parquet（``DAY_END`` /
``PORTFOLIO`` / ``SIGNAL`` / ``TRADE``）、最新 ``eval_report.json``、
``logs/runs/jobs.jsonl``、``logs/audit/risk_audit.jsonl`` 及 ``DataAPI``（``bars_daily``）。
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from data.data_api import DataAPI
from ops import alerting
from research.alpha_type import compute_turnover
from core.config import DATA_ROOT

#: 指标时点快照追加写路径
METRICS_JSONL = Path("logs/ops/metrics.jsonl")

#: 各数据源路径（缺失时降级为无可用数据源）
JOURNAL_DIR = Path("journal")
SIGNALS_DIR = DATA_ROOT / "signals"
JOBS_JSONL = Path("logs/runs/jobs.jsonl")
RISK_AUDIT_JSONL = Path("logs/audit/risk_audit.jsonl")
DATA_ROOT = str(DATA_ROOT / "asset")

#: 8 项策略级指标（按指标表顺序）
STRATEGY_METRICS = (
    "cum_return",
    "sharpe_6m",
    "mdd",
    "excess_return",
    "rolling_ic",
    "position_count",
    "turnover",
    "trade_cost",
)

#: 4 项系统级指标（按指标表顺序）
SYSTEM_METRICS = (
    "job_success_rate_7d",
    "pipeline_duration_min",
    "data_delay_days",
    "data_count_deviation",
)

# 状态枚举常量（系统级告警状态直接复用 alerting 级别名）
STATUS_HEALTHY = "healthy"
STATUS_WARNING = "warning"
STATUS_EXIT = "exit"
STATUS_OK = "ok"
STATUS_HIGH = alerting.AlertLevel.HIGH.value
STATUS_MEDIUM = alerting.AlertLevel.MEDIUM.value

#: rolling IC 统计窗口长度（观测期数）
IC_WINDOW = 20

_EVENT_DAY_END = "DAY_END"
_EVENT_PORTFOLIO = "PORTFOLIO"
_EVENT_SIGNAL = "SIGNAL"
_EVENT_TRADE = "TRADE"

_write_lock = threading.RLock()


# --- 指标分级纯函数（None 表示无可用数据源，按降级状态返回） ---


def tier_cum_return(value: float | None, neg_streak_months: int | None) -> str:
    """累计收益：连续 6 个月为负触发 warning，无 exit 档。"""
    if value is None or neg_streak_months is None:
        return STATUS_WARNING
    return STATUS_WARNING if neg_streak_months >= 6 else STATUS_HEALTHY


def tier_sharpe_6m(value: float | None) -> str:
    """滚动 6 个月夏普：< 0 触发 exit，< 0.3 触发 warning，[0.3, 0.5] 视为 healthy。"""
    if value is None:
        return STATUS_WARNING
    if value < 0:
        return STATUS_EXIT
    if value < 0.3:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_mdd(mdd: float | None, backtest_mdd: float | None) -> str:
    """最大回撤：> 1.5 倍回测值触发 exit，> 1.2 倍触发 warning，[1.0, 1.2] 倍视为 healthy。"""
    if mdd is None or backtest_mdd is None or backtest_mdd <= 0:
        return STATUS_WARNING
    ratio = mdd / backtest_mdd
    if ratio > 1.5:
        return STATUS_EXIT
    if ratio > 1.2:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_excess_return(value: float | None, neg_streak_months: int | None) -> str:
    """超额收益：连续 >= 12 个月为负触发 exit，>= 6 个月触发 warning。"""
    if value is None or neg_streak_months is None:
        return STATUS_WARNING
    if neg_streak_months >= 12:
        return STATUS_EXIT
    if neg_streak_months >= 6:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_rolling_ic(value: float | None, baseline: float | None) -> str:
    """滚动 IC：< 历史均值 30% 触发 exit，< 50% 触发 warning。"""
    if value is None or baseline is None or baseline <= 0:
        return STATUS_WARNING
    ratio = value / baseline
    if ratio < 0.3:
        return STATUS_EXIT
    if ratio < 0.5:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_position_count(actual: int | None, design: int | None) -> str:
    """持仓数：相对设计持仓数偏离 > 20% 触发 warning，无 exit 档。"""
    if actual is None or design is None or design <= 0:
        return STATUS_WARNING
    deviation = abs(actual - design) / design
    return STATUS_WARNING if deviation > 0.2 else STATUS_HEALTHY


def tier_turnover(value: float | None) -> str:
    """换手率：> 1.5（超出设计上限 1.0 的 50%）触发 warning。"""
    if value is None:
        return STATUS_WARNING
    return STATUS_WARNING if value > 1.5 else STATUS_HEALTHY


def tier_trade_cost(value: float | None) -> str:
    """单笔成本：> 0.5% 触发 warning，无 exit 档。"""
    if value is None:
        return STATUS_WARNING
    return STATUS_WARNING if value > 0.005 else STATUS_HEALTHY


def tier_job_success_rate_7d(value: float | None) -> str:
    """近 7 天任务成功率：< 80% 触发 HIGH。"""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_HIGH if value < 0.8 else STATUS_OK


def tier_pipeline_duration_min(value: float | None) -> str:
    """流水线总耗时：> 30 分钟触发 MEDIUM。"""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_MEDIUM if value > 30 else STATUS_OK


def tier_data_delay_days(value: float | None) -> str:
    """数据延迟天数：最新交易日落后超过 1 天触发 HIGH。"""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_HIGH if value > 1 else STATUS_OK


def tier_data_count_deviation(value: float | None) -> str:
    """数据条数偏差：偏离历史均值超过 ±30% 触发 HIGH。"""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_HIGH if abs(value) > 0.3 else STATUS_OK


# --- 数据源读取器（只读且异常安全，不可用时返回空值） ---


def _float_or_none(value: Any) -> float | None:
    """转换 JSON 数值为 float；缺失、布尔、非数值或 NaN 返回 ``None``。"""
    if value is None or isinstance(value, bool):
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return None if val != val else val


def _int_or_none(value: Any) -> int | None:
    """转换为整数；非整数或无效输入返回 ``None``。"""
    val = _float_or_none(value)
    if val is None or not val.is_integer():
        return None
    return int(val)


def _journal_rows(end_day: str) -> list[tuple[str, str, dict]]:
    """只读读取截止 ``end_day`` 的日志行 ``(day, event_type, payload)``（按时间正序）。

    不实例化 ``Journal`` 以避免创建目录的副作用，损坏文件或行直接跳过。
    """
    if not JOURNAL_DIR.is_dir():
        return []
    out: list[tuple[str, str, dict]] = []
    for path in sorted(JOURNAL_DIR.glob("*.parquet")):
        day = path.stem
        if len(day) != 10 or day > end_day:
            continue
        try:
            frame = pd.read_parquet(path, columns=["event_type", "payload"])
        except Exception:  # noqa: BLE001 - 单日损坏文件不得中断巡检
            continue
        for etype, payload_text in frame.itertuples(index=False):
            if not isinstance(payload_text, str):
                continue
            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                out.append((day, str(etype), payload))
    return out


def _day_end_book(rows: list[tuple[str, str, dict]]) -> dict[str, dict]:
    """提取每日 ``DAY_END`` 快照字段（同日多条取最后一条）。"""
    book: dict[str, dict] = {}
    for day, etype, payload in rows:
        if etype != _EVENT_DAY_END:
            continue
        slot = book.setdefault(day, {})
        for key in ("nav", "benchmark_nav", "position_count", "design_position_count"):
            if key in payload:
                slot[key] = payload[key]
    return book


def _nav_metrics(book: Mapping[str, Mapping[str, Any]], end_day: str) -> dict[str, Any]:
    """基于日终净值快照计算累计收益、滚动 6 个月夏普、最大回撤（正值）与当日收益率。"""
    navs: dict[str, float] = {}
    benchmarks: dict[str, float] = {}
    for day in sorted(book):
        slot = book[day]
        nav = _float_or_none(slot.get("nav"))
        if nav is not None:
            navs[day] = nav
        bm = _float_or_none(slot.get("benchmark_nav"))
        if bm is not None:
            benchmarks[day] = bm

    days = sorted(navs)
    out: dict[str, Any] = {
        "cum_return": None,
        "cum_return_neg_streak": None,
        "sharpe_6m": None,
        "mdd": None,
        "nav_daily_return": None,
    }
    if len(days) < 2 or navs[days[0]] == 0:
        return out
    values = [navs[d] for d in days]
    out["cum_return"] = values[-1] / values[0] - 1.0

    # 相邻观测日收益率序列
    rets: list[tuple[str, float]] = []
    for prev, cur in zip(days, days[1:]):
        if navs[prev] != 0:
            rets.append((cur, navs[cur] / navs[prev] - 1.0))

    cutoff = (pd.Timestamp(end_day) - pd.DateOffset(months=6)).date().isoformat()
    window = [r for d, r in rets if d >= cutoff]
    if len(window) >= 2:
        series = pd.Series(window)
        std = series.std()  # 样本标准差 ddof=1
        if std and std == std and std > 0:
            out["sharpe_6m"] = float(series.mean() / std * (252.0**0.5))

    running_max = 0.0
    worst = 0.0
    for v in values:
        running_max = max(running_max, v)
        if running_max > 0:
            worst = min(worst, v / running_max - 1.0)
    out["mdd"] = -worst

    if days[-1] == end_day:
        out["nav_daily_return"] = (
            rets[-1][1] if rets and rets[-1][0] == end_day else None
        )

    # 统计月末累计收益连续为负的月数
    month_end: dict[str, float] = {}
    for d in days:
        month_end[d[:7]] = navs[d]
    if values[0] != 0:
        streak = 0
        for month in sorted(month_end, reverse=True):
            if month_end[month] / values[0] - 1.0 < 0:
                streak += 1
            else:
                break
        out["cum_return_neg_streak"] = streak
    return out


def _excess_metrics(book: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """基于净值与基准配对序列计算累计超额收益及连续负超额月数。"""
    pairs: list[tuple[str, float, float]] = []
    for day in sorted(book):
        slot = book[day]
        nav = _float_or_none(slot.get("nav"))
        bm = _float_or_none(slot.get("benchmark_nav"))
        if nav is not None and bm is not None:
            pairs.append((day, nav, bm))
    out: dict[str, Any] = {"excess_return": None, "excess_neg_streak": None}
    if len(pairs) < 2 or pairs[0][1] == 0 or pairs[0][2] == 0:
        return out
    out["excess_return"] = (pairs[-1][1] / pairs[0][1]) / (
        pairs[-1][2] / pairs[0][2]
    ) - 1.0

    # 按日相对超额复合为月度超额收益，统计末尾连续负月数
    monthly: dict[str, float] = {}
    for prev, next_ in zip(pairs, pairs[1:]):
        if prev[1] == 0 or prev[2] == 0:
            continue
        r_strat = next_[1] / prev[1] - 1.0
        r_bench = next_[2] / prev[2] - 1.0
        r_excess = (1.0 + r_strat) / (1.0 + r_bench) - 1.0
        month = next_[0][:7]
        monthly[month] = (1.0 + monthly.get(month, 0.0)) * (1.0 + r_excess) - 1.0
    if monthly:
        streak = 0
        for month in sorted(monthly, reverse=True):
            if monthly[month] < 0:
                streak += 1
            else:
                break
        out["excess_neg_streak"] = streak
    return out


def _position_counts(
    rows: list[tuple[str, str, dict]],
) -> tuple[int | None, int | None]:
    """从最近一条含持仓数字段的 ``DAY_END`` 快照提取 ``(实际持仓数, 设计持仓数)``。"""
    actual = design = None
    for day, etype, payload in rows:
        if etype != _EVENT_DAY_END:
            continue
        if "position_count" not in payload and "design_position_count" not in payload:
            continue
        actual = _int_or_none(payload.get("position_count"))
        design = _int_or_none(payload.get("design_position_count"))
    return actual, design


def _turnover(rows: list[tuple[str, str, dict]]) -> float | None:
    """基于最近两期 ``PORTFOLIO`` 目标权重调用 ``compute_turnover`` 计算换手率。"""
    books: dict[str, dict[str, float]] = {}
    for day, etype, payload in rows:
        if etype != _EVENT_PORTFOLIO:
            continue
        raw = payload.get("target_weights")
        if not isinstance(raw, dict) or not raw:
            continue
        clean = {}
        for ticker, weight in raw.items():
            w = _float_or_none(weight)
            if w is not None:
                clean[str(ticker)] = w
        if clean:
            books[day] = clean
    days = sorted(books)
    if len(days) < 2:
        return None

    def frame(weights: Mapping[str, float]) -> pd.DataFrame:
        return pd.DataFrame(
            sorted(weights.items()), columns=["ticker", "target_weight"]
        )

    try:
        return float(compute_turnover(frame(books[days[-1]]), frame(books[days[-2]])))
    except Exception:  # noqa: BLE001 - 畸形持仓视为无可用数据源
        return None


def _trade_cost(rows: list[tuple[str, str, dict]], end_day: str) -> float | None:
    """计算 ``end_day`` 当日成交的平均单笔成本率 ``(commission + slippage) / notional``。"""
    rates: list[float] = []
    for day, etype, payload in rows:
        if day != end_day or etype != _EVENT_TRADE:
            continue
        price = _float_or_none(payload.get("fill_price"))
        qty = _float_or_none(payload.get("filled_qty"))
        commission = _float_or_none(payload.get("commission"))
        if price is None or qty is None or commission is None:
            continue
        notional = price * qty
        if notional <= 0:
            continue
        slippage = _float_or_none(payload.get("slippage")) or 0.0
        rates.append((commission + slippage) / notional)
    return sum(rates) / len(rates) if rates else None


def _rolling_ic(rows: list[tuple[str, str, dict]]) -> float | None:
    """计算最近 ``IC_WINDOW`` 个 ``SIGNAL`` 事件 IC 值的均值。"""
    ics: dict[str, float] = {}
    for day, etype, payload in rows:
        if etype != _EVENT_SIGNAL:
            continue
        val = _float_or_none(payload.get("ic"))
        if val is not None:
            ics[day] = val
    if not ics:
        return None
    window = [ics[d] for d in sorted(ics)[-IC_WINDOW:]]
    return sum(window) / len(window)


def _eval_report() -> tuple[str, float | None, float | None]:
    """从最新修改的 ``eval_report.json`` 提取 ``(alpha_name, 回测最大回撤绝对值, ic_mean)``。"""
    fallback: tuple[str, float | None, float | None] = ("", None, None)
    if not SIGNALS_DIR.is_dir():
        return fallback
    try:
        reports = sorted(
            SIGNALS_DIR.glob("*/eval_report.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return fallback
    for path in reports:
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(report, dict):
            continue
        metrics = report.get("metrics") or {}
        mdd = _float_or_none(metrics.get("max_drawdown"))
        return (
            str(report.get("alpha_name") or ""),
            abs(mdd) if mdd is not None else None,
            _float_or_none(metrics.get("ic_mean")),
        )
    return fallback


def _job_records() -> list[dict]:
    """读取并解析 ``logs/runs/jobs.jsonl``（跳过损坏行）。"""
    if not JOBS_JSONL.exists():
        return []
    try:
        text = JOBS_JSONL.read_text(encoding="utf-8")
    except OSError:
        return []
    rows: list[dict] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _run_day(stamp: Any) -> str | None:
    """将带时区时间戳转换为本地报告日历日期（``YYYY-MM-DD``）。

    调度与风控日志使用 UTC 时间戳，而 ``end_day`` 为本地日期；若直接截取字符串日期，
    在 UTC+8 凌晨 00:00–08:00 会与本地日期错位并漏算当日记录。
    """
    text = str(stamp or "")
    if not text:
        return None
    try:
        return datetime.fromisoformat(text).astimezone().date().isoformat()
    except ValueError:
        return text[:10] or None


def _job_metrics(records: list[dict], end_day: str) -> dict[str, Any]:
    """汇总近 7 天任务成功率（仅统计终态 SUCCESS/FAILED）、当日总耗时与失败任务列表。"""
    window_start = (date.fromisoformat(end_day) - timedelta(days=6)).isoformat()
    outcomes = successes = 0
    day_rows: list[dict] = []
    failed: list[tuple[str, str | None]] = []
    for row in records:
        started = _run_day(row.get("started_at"))
        if not started:
            continue
        status = str(row.get("status") or "")
        if window_start <= started <= end_day and status in ("SUCCESS", "FAILED"):
            outcomes += 1
            successes += status == "SUCCESS"
        if started != end_day:
            continue
        day_rows.append(row)
        if status == "FAILED":
            error = row.get("error")
            failed.append(
                (
                    str(row.get("job_name") or ""),
                    str(error) if error is not None else None,
                )
            )
    durations = [
        d
        for d in (_float_or_none(r.get("duration_s")) for r in day_rows)
        if d is not None
    ]
    return {
        "job_success_rate_7d": successes / outcomes if outcomes else None,
        "pipeline_duration_min": sum(durations) / 60.0 if durations else None,
        "failed_jobs": tuple(failed),
    }


def _risk_reject_count(end_day: str) -> int | None:
    """统计 ``end_day`` 当日 ``logs/audit/risk_audit.jsonl`` 中的 BLOCK 次数。"""
    if not RISK_AUDIT_JSONL.exists():
        return None
    try:
        text = RISK_AUDIT_JSONL.read_text(encoding="utf-8")
    except OSError:
        return None
    count = 0
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict) or str(row.get("action")) != "BLOCK":
            continue
        if _run_day(row.get("timestamp")) == end_day:
            count += 1
    return count


def _data_counts() -> dict[str, int]:
    """通过 DataAPI 统计数据湖 ``bars_daily`` 每日记录条数。"""
    try:
        with DataAPI(data_root=DATA_ROOT) as api:
            frame = api.query(
                "SELECT trade_date, count(*) AS n FROM bars_daily GROUP BY trade_date"
            )
    except Exception:  # noqa: BLE001 - 数据湖不可达视为无可用数据源
        return {}
    out: dict[str, int] = {}
    for raw_day, n in frame.itertuples(index=False):
        try:
            out[pd.Timestamp(raw_day).date().isoformat()] = int(n)
        except (TypeError, ValueError):
            continue
    return out


def _freshness(
    counts: Mapping[str, int], end_day: str
) -> tuple[float | None, float | None]:
    """计算 ``(数据延迟天数, 最新日条数相对历史均值偏差)``。"""
    if not counts:
        return None, None
    days = sorted(counts)
    latest = days[-1]
    delay = float((date.fromisoformat(end_day) - date.fromisoformat(latest)).days)
    prior = [counts[d] for d in days[:-1]]
    if not prior:
        return delay, None
    mean = sum(prior) / len(prior)
    if mean <= 0:
        return delay, None
    return delay, (counts[latest] - mean) / mean


# --- 单日采集汇总：生成指标字典与告警事实字典 ---


@dataclass(frozen=True)
class _Read:
    """单次采集读取的全部原始事实；``None`` 表示无可用数据源。"""

    cum_return: float | None
    cum_return_neg_streak: int | None
    sharpe_6m: float | None
    mdd: float | None
    nav_daily_return: float | None
    excess_return: float | None
    excess_neg_streak: int | None
    rolling_ic: float | None
    position_count: int | None
    design_position_count: int | None
    turnover: float | None
    trade_cost: float | None
    job_success_rate_7d: float | None
    pipeline_duration_min: float | None
    data_delay_days: float | None
    data_count_deviation: float | None
    backtest_mdd: float | None
    ic_baseline: float | None
    risk_reject_count: int | None
    failed_jobs: tuple[tuple[str, str | None], ...]
    alpha_type_id: str


def _read_day(end_day: str) -> _Read:
    rows = _journal_rows(end_day)
    book = _day_end_book(rows)
    nav = _nav_metrics(book, end_day)
    excess = _excess_metrics(book)
    actual, design = _position_counts(rows)
    jobs = _job_metrics(_job_records(), end_day)
    delay, deviation = _freshness(_data_counts(), end_day)
    alpha_name, backtest_mdd, ic_baseline = _eval_report()
    return _Read(
        cum_return=nav["cum_return"],
        cum_return_neg_streak=nav["cum_return_neg_streak"],
        sharpe_6m=nav["sharpe_6m"],
        mdd=nav["mdd"],
        nav_daily_return=nav["nav_daily_return"],
        excess_return=excess["excess_return"],
        excess_neg_streak=excess["excess_neg_streak"],
        rolling_ic=_rolling_ic(rows),
        position_count=actual,
        design_position_count=design,
        turnover=_turnover(rows),
        trade_cost=_trade_cost(rows, end_day),
        job_success_rate_7d=jobs["job_success_rate_7d"],
        pipeline_duration_min=jobs["pipeline_duration_min"],
        data_delay_days=delay,
        data_count_deviation=deviation,
        backtest_mdd=backtest_mdd,
        ic_baseline=ic_baseline,
        risk_reject_count=_risk_reject_count(end_day),
        failed_jobs=jobs["failed_jobs"],
        alpha_type_id=alpha_name,
    )


def _collect(day: str) -> tuple[dict, dict]:
    read = _read_day(day)
    metrics = {
        "strategy": {
            "cum_return": {
                "value": read.cum_return,
                "status": tier_cum_return(read.cum_return, read.cum_return_neg_streak),
            },
            "sharpe_6m": {
                "value": read.sharpe_6m,
                "status": tier_sharpe_6m(read.sharpe_6m),
            },
            "mdd": {
                "value": read.mdd,
                "status": tier_mdd(read.mdd, read.backtest_mdd),
            },
            "excess_return": {
                "value": read.excess_return,
                "status": tier_excess_return(
                    read.excess_return, read.excess_neg_streak
                ),
            },
            "rolling_ic": {
                "value": read.rolling_ic,
                "status": tier_rolling_ic(read.rolling_ic, read.ic_baseline),
            },
            "position_count": {
                "value": read.position_count,
                "status": tier_position_count(
                    read.position_count, read.design_position_count
                ),
            },
            "turnover": {
                "value": read.turnover,
                "status": tier_turnover(read.turnover),
            },
            "trade_cost": {
                "value": read.trade_cost,
                "status": tier_trade_cost(read.trade_cost),
            },
        },
        "system": {
            "job_success_rate_7d": {
                "value": read.job_success_rate_7d,
                "status": tier_job_success_rate_7d(read.job_success_rate_7d),
            },
            "pipeline_duration_min": {
                "value": read.pipeline_duration_min,
                "status": tier_pipeline_duration_min(read.pipeline_duration_min),
            },
            "data_delay_days": {
                "value": read.data_delay_days,
                "status": tier_data_delay_days(read.data_delay_days),
            },
            "data_count_deviation": {
                "value": read.data_count_deviation,
                "status": tier_data_count_deviation(read.data_count_deviation),
            },
        },
    }
    facts = {
        "alpha_type_id": read.alpha_type_id,
        "failed_jobs": [
            {"job_name": name, "error": error} for name, error in read.failed_jobs
        ],
        "data_delay_days": read.data_delay_days,
        "mdd": read.mdd,
        "backtest_mdd": read.backtest_mdd,
        "sharpe_6m": read.sharpe_6m,
        "excess_negative_months": read.excess_neg_streak,
        "nav_daily_return": read.nav_daily_return,
        "turnover": read.turnover,
        "risk_reject_count": read.risk_reject_count,
    }
    return metrics, facts


def collect_metrics(date: str) -> dict:
    """采集策略级与系统级 12 项指标，返回 ``{"strategy": {...}, "system": {...}}``。"""
    return _collect(date)[0]


def collect_day(day: str) -> tuple[dict, dict]:
    """单次遍历数据源返回 ``(metrics, facts)``，避免重复读取日志 I/O。"""
    return _collect(day)


def active_alpha_type_id() -> str:
    """返回最新评估报告中的策略标识，不存在时返回空字符串。"""
    return _eval_report()[0]


def nav_series(end_day: str) -> tuple[pd.Series, pd.Series]:
    """从 ``DAY_END`` 快照提取 ``(nav, benchmark_nav)`` 时序（仅读取，不负责基准选择）。"""
    book = _day_end_book(_journal_rows(end_day))
    days = sorted(book)
    nav = {d: _float_or_none(book[d].get("nav")) for d in days}
    keep = [d for d in days if nav[d] is not None]
    index = pd.to_datetime(keep)
    return (
        pd.Series([nav[d] for d in keep], index=index, dtype="float64"),
        pd.Series(
            [_float_or_none(book[d].get("benchmark_nav")) for d in keep],
            index=index,
            dtype="float64",
        ),
    )


def append_metrics_snapshot(date: str, metrics: dict) -> None:
    """将 12 项指标时点快照追加写入 ``logs/ops/metrics.jsonl``（缺失值以 null 落盘）。"""
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows: list[dict] = []
    for scope, names in (
        ("strategy", STRATEGY_METRICS),
        ("system", SYSTEM_METRICS),
    ):
        cells = metrics.get(scope) or {}
        for name in names:
            cell = cells.get(name) or {}
            status = cell.get("status")
            if status is None:
                status = STATUS_WARNING if scope == "strategy" else STATUS_MEDIUM
            rows.append(
                {
                    "date": date,
                    "ts": ts,
                    "scope": scope,
                    "name": name,
                    "value": cell.get("value"),
                    "status": status,
                }
            )
    with _write_lock:
        path = METRICS_JSONL
        path.parent.mkdir(parents=True, exist_ok=True)
        needs_break = False
        if path.exists() and path.stat().st_size > 0:
            with path.open("rb") as fh:
                fh.seek(-1, os.SEEK_END)
                needs_break = fh.read(1) != b"\n"
        with path.open("a", encoding="utf-8") as fh:
            if needs_break:
                fh.write("\n")
            for row in rows:
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def health_check() -> str:
    """巡检任务主函数：采集指标 -> 快照落盘 -> 告警求值 -> 推送落盘。"""
    day = date.today().isoformat()
    metrics, facts = _collect(day)
    append_metrics_snapshot(day, metrics)
    firings = alerting.evaluate_alerts(facts)
    for firing in firings:
        alerting.send_alert(
            firing["level"],
            firing["msg"],
            alert_type=firing["alert_type"],
            alpha_type_id=firing["alpha_type_id"],
        )
    return f"metrics snapshot {day}: 12 rows, {len(firings)} alert(s)"
