"""任务调度引擎：``run_job`` 执行器、9 字段运行日志、跨进程互斥锁与日频流水线。

调度不变量与契约：
1. 若 ``run_daily_signal`` 内部重试后返回 ``{"status": "HOLD", ...}``，直接记为
   ``FAILED`` 并执行保留旧组合（hold）动作，不再重复重试。
2. 任一任务终态 ``FAILED`` 时，无论 ``on_failure`` 为 ``skip`` 或 ``hold``，均须为
   当日任务链（:func:`job_chain`）中的所有下游任务写入 ``SKIPPED`` 记录并终止流水线。
3. 若信号任务因上游失败被跳过（``SKIPPED``），同样触发保留旧组合逻辑。
4. ``fetch_stock_daily`` 通过子进程调用 Docker 容器执行。
5. ``run_id`` 格式为 ``{YYYYMMDD}_{job_name}_{seq:03d}``，按 ``(day, job)`` 递增；
   每次尝试（含 ``RETRY``）均向 ``logs/runs/jobs.jsonl`` 追加一条记录。
"""

from __future__ import annotations

import json
import subprocess
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

import pandas as pd

from data.storage_ops import FileLock
from ops.alerting import evaluate_alerts, send_alert
from ops.monitoring import (
    active_alpha_type_id,
    collect_day,
    health_check,
    nav_series,
)
from ops.reporting import write_attribution_report, write_daily_report

REPO_ROOT = Path(__file__).resolve().parents[1]

#: 运行记录持久化路径
JOBS_JSONL = Path("logs/runs/jobs.jsonl")

#: ``jobs.jsonl`` 每行包含的 9 个固定字段
RUN_RECORD_FIELDS = (
    "run_id",
    "job_name",
    "started_at",
    "finished_at",
    "status",
    "duration_s",
    "error",
    "retry_count",
    "output_summary",
)

#: 运行状态四态枚举
RUN_STATUSES = frozenset({"SUCCESS", "FAILED", "RETRY", "SKIPPED"})

#: 日频 5 步任务链顺序（数据→因子→信号→巡检→日报）
JOB_ORDER = (
    "fetch_stock_daily",
    "compute_all",
    "run_daily_signal",
    "health_check",
    "write_daily_report",
)

#: 月末追加归因任务名（仅月末运行日由 :func:`job_chain` 追加至链尾）
MONTH_END_JOB = "month_end_attribution"

#: 月末归因所需的因子表目录（缺失时按无数据失败处理并告警，不伪造数据）
ATTRIBUTION_DIR = Path("logs/ops/attribution")

#: 各任务重试次数、超时时间及失败处理策略配置
JOB_CONFIG = {
    "fetch_stock_daily": {"retries": 2, "timeout_s": 300, "on_failure": "skip"},
    "compute_all": {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
    "run_daily_signal": {"retries": 1, "timeout_s": 300, "on_failure": "hold"},
    "health_check": {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
    "write_daily_report": {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
    MONTH_END_JOB: {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
}


def reporting_day() -> str:
    """返回本次运行对应的报告日期（``YYYY-MM-DD``），确保选链与落盘日期一致。"""
    return date.today().isoformat()


def is_month_end(day: Any) -> bool:
    """判断 ``day`` 是否为当月最后一个工作日（周一至周五）。

    流水线按工作日调度，周末从不触发；待节假日表接入后可替换为最后交易日判断。
    """
    try:
        if isinstance(day, datetime):
            d = day.date()
        elif isinstance(day, date):
            d = day
        else:
            d = date.fromisoformat(str(day)[:10])
    except (TypeError, ValueError) as exc:
        raise ValueError(f"not a date: {day!r}") from exc
    if d.weekday() >= 5:
        return False
    nxt = d + timedelta(days=1)
    while nxt.month == d.month:
        if nxt.weekday() < 5:
            return False
        nxt += timedelta(days=1)
    return True


def job_chain(day: Any = None) -> tuple[str, ...]:
    """返回指定日期应执行的任务链（日常 5 步，月末追加归因任务）。"""
    if day is None:
        day = reporting_day()
    return JOB_ORDER + ((MONTH_END_JOB,) if is_month_end(day) else ())


class JobTimeoutError(TimeoutError):
    """任务单次尝试超过 ``timeout_s`` 时抛出，触发失败记录或重试。"""


class RunMutex:
    """线程可重入的跨进程运行互斥锁。

    基于 :class:`data.storage_ops.FileLock` 防止手工触发与定时调度并发写 ``jobs.jsonl``；
    同线程内支持重入，以便 ``daily_pipeline`` 在整条流水线期间持有锁。
    """

    def __init__(self) -> None:
        self._state = threading.Lock()
        self._lock: FileLock | None = None
        self._owner: int | None = None
        self._depth = 0

    def __enter__(self) -> RunMutex:
        tid = threading.get_ident()
        with self._state:
            if self._depth > 0 and self._owner == tid:
                self._depth += 1
                return self
            lock = FileLock(JOBS_JSONL)
            lock.__enter__()  # 锁冲突时抛出 ConcurrentWriteError
            self._lock = lock
            self._owner = tid
            self._depth = 1
            return self

    def __exit__(self, *exc: object) -> None:
        with self._state:
            self._depth -= 1
            if self._depth > 0:
                return
            lock, self._lock = self._lock, None
            self._owner = None
        if lock is not None:
            lock.__exit__(*exc)


_RUN_MUTEX = RunMutex()


def run_mutex() -> RunMutex:
    """返回进程级运行互斥锁单例。"""
    return _RUN_MUTEX


def _pending(name: str, owner: str) -> Callable[[], None]:
    def _placeholder() -> None:
        raise NotImplementedError(
            f"{name}: body not wired yet ({owner}) — tests inject fakes via "
            "run_job(fn=...) or monkeypatch JOB_REGISTRY"
        )

    _placeholder.__name__ = name
    return _placeholder


def fetch_stock_daily() -> str:
    """通过子进程在项目根目录运行 Docker 容器内的日行情拉取脚本。"""
    proc = subprocess.run(
        [
            "docker-compose",
            "run",
            "--rm",
            "amazingdata",
            "python3",
            "/scripts/data_fetcher/fetch_stock_daily.py",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"fetch_stock_daily exited {proc.returncode}: {(proc.stderr or '')[-300:]}"
        )
    return (proc.stdout or "").strip()[-200:] or "fetch_stock_daily exited 0"


# --- 日报与月末归因任务及其输入准备 ---


def _quarter(month: str) -> str:
    """将 ``YYYY-MM`` 转换为季度标识 ``YYYYQn``。"""
    year, mon = month.split("-")
    return f"{year}Q{(int(mon) - 1) // 3 + 1}"


def monthly_returns(end_day: str) -> tuple[pd.Series, pd.Series]:
    """计算截止 ``end_day`` 的月末对月末组合及基准收益率序列（用于月频×12 年化回归）。"""
    nav, bench = nav_series(end_day)
    if len(nav) < 2:
        raise ValueError(
            f"need >= 2 nav observations for monthly returns, got {len(nav)}"
        )
    frame = pd.DataFrame(
        {"nav": nav.to_numpy(), "b": bench.to_numpy()}, index=nav.index
    )
    months = frame.index.to_period("M")
    monthly = frame.loc[~months.duplicated(keep="last")]
    if len(monthly) < 2:
        raise ValueError(
            f"need >= 2 calendar months of nav for monthly returns, got {len(monthly)}"
        )
    for col in ("nav", "b"):
        base = monthly[col].shift(1).iloc[1:]
        if bool((base == 0).any()):
            raise ValueError(f"{col} has a zero month — monthly returns undefined")
    r_p = (monthly["nav"] / monthly["nav"].shift(1) - 1.0).iloc[1:]
    r_b = (monthly["b"] / monthly["b"].shift(1) - 1.0).iloc[1:]
    return r_p, r_b


def attribution_frames(
    month: str,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """读取月末归因所需的 ``weights``、``factor_returns`` 与 ``factor_exposures``。

    文件缺失时直接抛出 ``FileNotFoundError`` 触发失败与告警，绝不自动补全。
    """
    base = Path(ATTRIBUTION_DIR) / month
    frames: list[pd.DataFrame] = []
    for stem in ("weights", "factor_returns", "factor_exposures"):
        path = base / f"{stem}.parquet"
        if not path.is_file():
            raise FileNotFoundError(
                f"no {stem} for {month} at {path} — the signal/factor jobs write "
                f"it into {ATTRIBUTION_DIR}/{month}/"
            )
        frames.append(pd.read_parquet(path))
    return frames[0], frames[1], frames[2]


def write_daily_report_job() -> str:
    """日报任务体：采集当日指标与净值序列 -> 求值告警 -> 生成日报。"""
    day = reporting_day()
    metrics, facts = collect_day(day)
    nav, _ = nav_series(day)
    path = write_daily_report(day, metrics, evaluate_alerts(facts), nav=nav)
    return f"daily report {day}: {path}"


def month_end_attribution() -> str:
    """月末归因任务体：准备月度收益率与因子表，调用 :func:`write_attribution_report` 输出报告。"""
    day = reporting_day()
    month = day[:7]
    r_p, r_b = monthly_returns(day)
    weights, factor_returns, factor_exposures = attribution_frames(month)
    path = write_attribution_report(
        active_alpha_type_id(),
        _quarter(month),
        r_p,
        r_b,
        weights=weights,
        factor_returns=factor_returns,
        factor_exposures=factor_exposures,
    )
    return f"attribution {month}: {path}"


#: 任务注册表，``run_job`` 未显式传入 ``fn`` 时由此查找可执行体
JOB_REGISTRY: dict[str, Callable[[], Any]] = {
    "fetch_stock_daily": fetch_stock_daily,
    "compute_all": _pending("compute_all", "S12 wires the FactorDAG + window"),
    "run_daily_signal": _pending(
        "run_daily_signal", "S12 injects the signal_fn producer"
    ),
    "health_check": health_check,
    "write_daily_report": write_daily_report_job,
    MONTH_END_JOB: month_end_attribution,
}


def run_with_timeout(fn: Callable[[], Any], timeout_s: float) -> Any:
    """在守护线程中执行 ``fn``，超时抛出 :class:`JobTimeoutError` 并放弃该线程。"""
    box: dict[str, Any] = {}
    done = threading.Event()

    def _call() -> None:
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 - 在调用方重新抛出
            box["error"] = exc
        finally:
            done.set()

    worker = threading.Thread(target=_call, name="run_job", daemon=True)
    worker.start()
    if not done.wait(timeout_s):
        raise JobTimeoutError(f"job timed out after {timeout_s}s")
    if "error" in box:
        raise box["error"]
    return box.get("result")


def _next_run_id(name: str, started: datetime) -> str:
    """生成 ``{YYYYMMDD}_{job_name}_{seq:03d}``，按 ``(day, job)`` 递增序号。"""
    day = started.strftime("%Y%m%d")
    prefix = f"{day}_{name}_"
    seq = 0
    if JOBS_JSONL.exists():
        for line in JOBS_JSONL.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            run_id = str(row.get("run_id") or "")
            if row.get("job_name") != name or not run_id.startswith(prefix):
                continue
            tail = run_id[len(prefix) :]
            if tail.isdigit():
                seq = max(seq, int(tail))
    return f"{prefix}{seq + 1:03d}"


def _summarize(result: Any) -> str | None:
    if result is None:
        return None
    return str(result)[:200]


def log_job_run(
    name: str,
    status: str,
    result: Any = None,
    retry_count: int = 0,
    error: str | None = None,
    started: datetime | None = None,
    finished: datetime | None = None,
) -> dict:
    """向 ``logs/runs/jobs.jsonl`` 追加一条 9 字段运行记录（每次尝试各写一行）。"""
    if status not in RUN_STATUSES:
        raise ValueError(
            f"status must be one of {sorted(RUN_STATUSES)}, got {status!r}"
        )
    started = started or datetime.now(timezone.utc)
    finished = finished or started
    with run_mutex():
        record = {
            "run_id": _next_run_id(name, started),
            "job_name": name,
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "status": status,
            "duration_s": round((finished - started).total_seconds(), 3),
            "error": error,
            "retry_count": retry_count,
            "output_summary": _summarize(result),
        }
        JOBS_JSONL.parent.mkdir(parents=True, exist_ok=True)
        with JOBS_JSONL.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def mark_downstream_skipped(name: str, chain: Sequence[str] = JOB_ORDER) -> list[dict]:
    """为 ``chain`` 中位于 ``name`` 之后的所有下游任务写入 ``SKIPPED`` 记录。"""
    records: list[dict] = []
    if name not in chain:
        return records
    for later in chain[chain.index(name) + 1 :]:
        records.append(
            log_job_run(later, "SKIPPED", error=f"skipped: upstream {name} failed")
        )
    return records


def hold_previous_portfolio(name: str) -> dict:
    """失败保护动作：保持上一交易日持仓不变，绝不触发清仓。"""
    return {"action": "hold_previous_portfolio", "job": name, "portfolio": "previous"}


def run_job(
    name: str,
    fn: Callable[[], Any] | None = None,
    retries: int = 1,
    timeout_s: int = 300,
    on_failure: str = "skip",
    *,
    chain: Sequence[str] = JOB_ORDER,
) -> dict:
    """执行单个任务并处理重试、超时与失败策略。

    ``retries`` 为首次失败后的最大重试次数。耗尽重试或返回 ``{"status": "HOLD"}`` 时，
    记录终态 ``FAILED``，按需执行 ``hold_previous_portfolio``，为下游任务写入
    ``SKIPPED`` 记录，并发送一条 HIGH 级别告警。
    """
    if on_failure not in ("skip", "hold"):
        raise ValueError(f'on_failure must be "skip" or "hold", got {on_failure!r}')
    if retries < 0:
        raise ValueError(f"retries must be >= 0, got {retries!r}")
    target = fn if fn is not None else JOB_REGISTRY[name]
    with run_mutex():
        for attempt in range(retries + 1):
            started = datetime.now(timezone.utc)
            result: Any = None
            error: str | None = None
            held = False
            try:
                result = run_with_timeout(target, timeout_s)
                if (
                    isinstance(result, dict)
                    and str(result.get("status", "")).upper() == "HOLD"
                ):
                    # 内部已完成重试，直接按 HOLD 失败处理，不再重复重试
                    held = True
                    error = result.get("error") or "returned status=HOLD"
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
            finished = datetime.now(timezone.utc)
            if error is None:
                return log_job_run(
                    name, "SUCCESS", result, attempt, started=started, finished=finished
                )
            if held or attempt >= retries:
                record = log_job_run(
                    name,
                    "FAILED",
                    result if held else None,
                    attempt,
                    error=error,
                    started=started,
                    finished=finished,
                )
                if on_failure == "hold" or held:
                    hold_previous_portfolio(name)  # 保持上一期持仓
                mark_downstream_skipped(name, chain)  # 无论 skip/hold 均标记下游跳过
                send_alert(
                    "HIGH",
                    f"job {name} failed: {error}",
                    alert_type="job_failure",
                    alpha_type_id=name,
                )
                return record
            log_job_run(
                name,
                "RETRY",
                None,
                attempt,
                error=error,
                started=started,
                finished=finished,
            )
    raise AssertionError(
        "unreachable: the last attempt always returns"
    )  # pragma: no cover


def daily_pipeline() -> None:
    """日频流水线：在互斥锁下顺序执行当日任务链（月末追加归因任务）。

    任一任务非 ``SUCCESS`` 即中断（下游已由 ``run_job`` 标记 ``SKIPPED``）；
    若信号任务未执行，则主动调用 ``hold_previous_portfolio`` 保留旧组合。
    """
    chain = job_chain()
    with run_mutex():
        signal_ran = False
        for name in chain:
            record = run_job(name, chain=chain, **JOB_CONFIG[name])
            if name == "run_daily_signal":
                signal_ran = True
            if record["status"] != "SUCCESS":
                break
        if not signal_ran:
            hold_previous_portfolio("run_daily_signal")  # 信号未执行时保留旧持仓
