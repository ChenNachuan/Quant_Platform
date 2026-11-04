"""Metrics collection + tiering: the 12-metric snapshot and the health_check body.

``docs/design/ops/monitoring.md`` is the contract — 8 strategy metrics judged
in three tiers (``healthy`` / ``warning`` / ``exit``) and 4 system metrics
judged in two ( ``ok`` / the breach level in ``status`` : ``HIGH`` / ``MEDIUM`` ).
``collect_metrics(date)`` returns the design's ``{"strategy": {...}, "system":
{...}}`` shape (each entry ``{"value", "status"}``); ``append_metrics_snapshot``
appends the 12-row snapshot to ``data/ops/metrics.jsonl`` (row schema pinned by
the PR: ``date / ts / scope / name / value / status``); ``health_check`` is the
scheduler job body (seam 3) — collect → snapshot → facts →
``ops.alerting.evaluate_alerts`` → ``send_alert`` per firing.

Tier gaps resolve by severity priority — exit band first, then warning, else
healthy — so an unlisted value reads healthy (seam 5: sharpe ∈ [0.3, 0.5] and
MDD ∈ [1.0, 1.2]× backtest are healthy). A metric with no computable source
lands ``value=null`` + status ``warning`` (strategy) / ``MEDIUM`` (system) and
never alerts (seam 6). The snapshot always lands, sources or not.

Sources (seam 4). Every reader is null-safe and a broken source degrades to
"no computable source" rather than raising — health_check must survive the
book it monitors. The journal readers bind to :class:`execution.ledger.Journal`
's on-disk format (daily ``{YYYY-MM-DD}.parquet``, ``event_type`` + JSON-text
``payload``) and consume these payload contracts:

* ``DAY_END`` — the day-end book snapshot:
  ``{"nav": float, "benchmark_nav": float, "position_count": int,
  "design_position_count": int}`` — the nav series (累计收益 / rolling sharpe /
  MDD / 超额 / single-day NAV) and 持仓数 (snapshot vs its design size).
* ``PORTFOLIO`` — ``{"target_weights": {ticker: weight}}``; the latest two
  books feed :func:`research.alpha_type.compute_turnover` (换手).
* ``SIGNAL`` — ``{"ic": float}``; the trailing 20 observations are rolling IC
  (window pinned here, §64.7 states none), judged against the eval report's
  ``ic_mean`` baseline.
* ``TRADE`` — ``{"fill_price", "filled_qty", "commission", "slippage"}`` per
  fill (``slippage`` optional, default 0) — the realized
  :mod:`execution.fee_model` costs behind 单笔成本.

Backtest MDD + the IC baseline come from ``research.factor_combiner``'s
``data/signals/{name}/eval_report.json`` (``metrics.max_drawdown`` is negative
there and normalised to a magnitude here; the most recently modified report is
the monitored strategy and its ``alpha_name`` scopes the alert facts). The
system side reads ``logs/runs/jobs.jsonl`` run records directly — monitoring
never imports the scheduler — and the data lake through
:class:`data.data_api.DataAPI` (``bars_daily``: latest ``trade_date`` and
per-day row counts vs the historical mean).
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

#: Snapshot landing spot (``data/`` is gitignored runtime data).
METRICS_JSONL = Path("data/ops/metrics.jsonl")

#: Source locations (seam 4). Every one degrades to None when absent.
JOURNAL_DIR = Path("journal")
SIGNALS_DIR = Path("data/signals")
JOBS_JSONL = Path("logs/runs/jobs.jsonl")
RISK_AUDIT_JSONL = Path("data/execution/risk_audit.jsonl")
DATA_ROOT = "data_platform/asset"

#: The 8 strategy metrics, monitoring.md table order.
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

#: The 4 system metrics, monitoring.md table order.
SYSTEM_METRICS = (
    "job_success_rate_7d",
    "pipeline_duration_min",
    "data_delay_days",
    "data_count_deviation",
)

# Status vocab (seam 5) — the system breach levels ride in ``status`` and
# reuse the alerting level strings verbatim.
STATUS_HEALTHY = "healthy"
STATUS_WARNING = "warning"
STATUS_EXIT = "exit"
STATUS_OK = "ok"
STATUS_HIGH = alerting.AlertLevel.HIGH.value
STATUS_MEDIUM = alerting.AlertLevel.MEDIUM.value

#: rolling-IC window (observations), pinned here — §64.7 states no window.
IC_WINDOW = 20

_EVENT_DAY_END = "DAY_END"
_EVENT_PORTFOLIO = "PORTFOLIO"
_EVENT_SIGNAL = "SIGNAL"
_EVENT_TRADE = "TRADE"

_write_lock = threading.RLock()


# ---------------------------------------------------------------------------
# Tier helpers — the monitoring.md band tables, pure. ``None`` inputs mean "no
# computable source" and fall back to the seam-6 status.
# ---------------------------------------------------------------------------


def tier_cum_return(value: float | None, neg_streak_months: int | None) -> str:
    """累计收益: warning iff negative 6 months running; no exit band."""
    if value is None or neg_streak_months is None:
        return STATUS_WARNING
    return STATUS_WARNING if neg_streak_months >= 6 else STATUS_HEALTHY


def tier_sharpe_6m(value: float | None) -> str:
    """rolling 6月夏普: exit < 0, warning < 0.3; [0.3, 0.5] reads healthy."""
    if value is None:
        return STATUS_WARNING
    if value < 0:
        return STATUS_EXIT
    if value < 0.3:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_mdd(mdd: float | None, backtest_mdd: float | None) -> str:
    """最大回撤: exit > 1.5x backtest, warning > 1.2x; [1.0, 1.2]x healthy."""
    if mdd is None or backtest_mdd is None or backtest_mdd <= 0:
        return STATUS_WARNING
    ratio = mdd / backtest_mdd
    if ratio > 1.5:
        return STATUS_EXIT
    if ratio > 1.2:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_excess_return(value: float | None, neg_streak_months: int | None) -> str:
    """超额收益: exit >= 12 months running negative, warning >= 6."""
    if value is None or neg_streak_months is None:
        return STATUS_WARNING
    if neg_streak_months >= 12:
        return STATUS_EXIT
    if neg_streak_months >= 6:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_rolling_ic(value: float | None, baseline: float | None) -> str:
    """rolling IC: exit < 30% of the historical mean, warning < 50%."""
    if value is None or baseline is None or baseline <= 0:
        return STATUS_WARNING
    ratio = value / baseline
    if ratio < 0.3:
        return STATUS_EXIT
    if ratio < 0.5:
        return STATUS_WARNING
    return STATUS_HEALTHY


def tier_position_count(actual: int | None, design: int | None) -> str:
    """持仓数: warning on > 20% deviation from the design size; no exit band."""
    if actual is None or design is None or design <= 0:
        return STATUS_WARNING
    deviation = abs(actual - design) / design
    return STATUS_WARNING if deviation > 0.2 else STATUS_HEALTHY


def tier_turnover(value: float | None) -> str:
    """换手率: warning > 1.5 (seam 5: 50% over the design ceiling 1.0)."""
    if value is None:
        return STATUS_WARNING
    return STATUS_WARNING if value > 1.5 else STATUS_HEALTHY


def tier_trade_cost(value: float | None) -> str:
    """单笔成本: warning > 0.5%; no exit band."""
    if value is None:
        return STATUS_WARNING
    return STATUS_WARNING if value > 0.005 else STATUS_HEALTHY


def tier_job_success_rate_7d(value: float | None) -> str:
    """Job 成功率 rolling 7 天: HIGH below 80%."""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_HIGH if value < 0.8 else STATUS_OK


def tier_pipeline_duration_min(value: float | None) -> str:
    """Pipeline 总耗时: MEDIUM over 30 minutes."""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_MEDIUM if value > 30 else STATUS_OK


def tier_data_delay_days(value: float | None) -> str:
    """数据新鲜度: HIGH when the latest trading day is more than 1 day old."""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_HIGH if value > 1 else STATUS_OK


def tier_data_count_deviation(value: float | None) -> str:
    """数据条数偏差: HIGH beyond ±30% of the historical mean."""
    if value is None:
        return STATUS_MEDIUM
    return STATUS_HIGH if abs(value) > 0.3 else STATUS_OK


# ---------------------------------------------------------------------------
# Source readers — every one returns "no computable source" (None / empty)
# instead of raising.
# ---------------------------------------------------------------------------


def _float_or_none(value: Any) -> float | None:
    """A JSON number as float; ``None`` for missing / non-numeric / NaN."""
    if value is None or isinstance(value, bool):
        return None
    try:
        val = float(value)
    except (TypeError, ValueError):
        return None
    return None if val != val else val


def _int_or_none(value: Any) -> int | None:
    """An integral count as int; ``None`` for anything else."""
    val = _float_or_none(value)
    if val is None or not val.is_integer():
        return None
    return int(val)


def _journal_rows(end_day: str) -> list[tuple[str, str, dict]]:
    """(day, event_type, payload) for journal rows up to *end_day*, oldest first.

    Read-only over ``Journal``'s file format — the Journal constructor mkdirs,
    a side effect a monitor must not take. Corrupt files and lines are
    skipped, never fatal.
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
        except Exception:  # noqa: BLE001 - one bad day file must not kill health
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
    """Per-day DAY_END snapshot keys (``nav`` / ``benchmark_nav`` / counts), last wins."""
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
    """The nav-derived metrics from the day-end book snapshots.

    cum_return is inception-to-date over the series; sharpe_6m is the trailing
    6 calendar months of daily returns annualised (√252); mdd is the deepest
    peak-to-trough (positive magnitude); nav_daily_return is *end_day*'s return
    (None when the series has no observation that day).
    """
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

    # Daily returns: (day, return) over consecutive observations.
    rets: list[tuple[str, float]] = []
    for prev, cur in zip(days, days[1:]):
        if navs[prev] != 0:
            rets.append((cur, navs[cur] / navs[prev] - 1.0))

    cutoff = (pd.Timestamp(end_day) - pd.DateOffset(months=6)).date().isoformat()
    window = [r for d, r in rets if d >= cutoff]
    if len(window) >= 2:
        series = pd.Series(window)
        std = series.std()  # ddof=1; NaN for n<2 (guarded above)
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

    # 累计收益 warning streak: trailing month-end cum returns below zero.
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
    """超额收益 over the paired nav/benchmark series: cumulative + negative streak."""
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

    # Monthly excess returns from daily relative returns; streak = trailing
    # months running negative (the alerting fact's "months running").
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
    """(actual, design) from the latest DAY_END snapshot carrying either key."""
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
    """换手 over the latest two PORTFOLIO books via compute_turnover (D9)."""
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
    except Exception:  # noqa: BLE001 - a malformed book is "no computable source"
        return None


def _trade_cost(rows: list[tuple[str, str, dict]], end_day: str) -> float | None:
    """单笔成本: mean realised (commission + slippage) / notional over the day's fills."""
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
    """Mean of the trailing IC_WINDOW daily IC observations."""
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
    """(alpha_name, backtest MDD magnitude, ic_mean) from the latest eval_report.json."""
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
    """Parsed ``logs/runs/jobs.jsonl`` rows (corrupt lines skipped)."""
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


def _job_metrics(records: list[dict], end_day: str) -> dict[str, Any]:
    """{success rate 7d, pipeline minutes, failed jobs} from the run records.

    Success rate: terminal outcomes (SUCCESS/FAILED) in the trailing 7 calendar
    days — RETRY rows are attempts and SKIPPED rows never ran, so neither is an
    outcome. Pipeline minutes: the sum of the day's attempt durations.
    """
    window_start = (date.fromisoformat(end_day) - timedelta(days=6)).isoformat()
    outcomes = successes = 0
    day_rows: list[dict] = []
    failed: list[tuple[str, str | None]] = []
    for row in records:
        started = str(row.get("started_at") or "")[:10]
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
    """The day's BLOCK count in ``data/execution/risk_audit.jsonl`` (cond 8 fact)."""
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
        if str(row.get("timestamp") or "")[:10] == end_day:
            count += 1
    return count


def _data_counts() -> dict[str, int]:
    """{trade_date: row count} over the data lake's ``bars_daily`` (via DataAPI)."""
    try:
        with DataAPI(data_root=DATA_ROOT) as api:
            frame = api.query(
                "SELECT trade_date, count(*) AS n FROM bars_daily GROUP BY trade_date"
            )
    except Exception:  # noqa: BLE001 - an unreachable lake is "no computable source"
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
    """(data delay in days, row-count deviation vs prior-day mean)."""
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


# ---------------------------------------------------------------------------
# Collection: one day's read -> the design's metrics dict + the alerting facts
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Read:
    """Everything one collection pass reads; ``None`` = no computable source."""

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
    """采集策略级 + 系统级指标 (design signature): ``{"strategy": {...}, "system": {...}}``.

    Every entry carries ``{"value", "status"}``; the 12 names are
    :data:`STRATEGY_METRICS` then :data:`SYSTEM_METRICS`, in table order.
    """
    return _collect(date)[0]


def append_metrics_snapshot(date: str, metrics: dict) -> None:
    """时点指标序列追加写 ``data/ops/metrics.jsonl`` (design signature).

    One row per metric (12 per snapshot) with the PR-pinned schema
    ``date / ts / scope / name / value / status``. Append-only: repeated calls
    stack snapshots — the file is the time series. A missing cell still lands
    its row (value null, seam-6 status) — the snapshot always lands.
    """
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
    """The ``health_check`` job body (seam 3): collect → snapshot → facts → alerts.

    Facts feed :func:`ops.alerting.evaluate_alerts`; each firing goes through
    :func:`ops.alerting.send_alert`. The scheduler's direct job-failure alert
    converges on the same dedup key (``alert_type="job_failure"`` with the job
    name as ``alpha_type_id``), which is why the facts carry the day's failed
    jobs too. Returns a one-line summary for the run record.
    """
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
