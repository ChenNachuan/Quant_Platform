"""Job execution engine: ``run_job``, run records, and the run mutex.

QUANT-48 T1 (ac-5) scope: the ``run_job`` engine (retry / timeout /
``on_failure`` ``skip|hold``), the nine-field run-record writer
``log_job_run`` over ``logs/runs/jobs.jsonl``, ``JOB_REGISTRY`` for the five
job classes, and the cross-process run mutex reusing
:class:`data.storage_ops.FileLock`. ``daily_pipeline`` (T2) runs the 5-job
chain over that engine; ``health_check`` runs the S12-T2 body
(``ops.monitoring``) and ``write_daily_report`` stays an S12-T3 placeholder.

Seams pinned in ``task/plan_quant48.md``:

1. ``research.alpha_type.run_daily_signal`` retries internally and returns
   ``{"status": "HOLD", ...}`` **without raising**. A returned ``HOLD`` is the
   hold outcome: record ``status=FAILED`` (the enum has no HOLD), ``error``
   carries the inner reason, take the hold path, and do **not** re-retry.
2. A terminal ``FAILED`` job writes a ``SKIPPED`` record for *every* later job
   in ``JOB_ORDER``, on the skip **and** the hold path (review gap: hold is a
   portfolio action, skip-marks are record bookkeeping — not exclusive).
   ``daily_pipeline`` must not execute those jobs afterwards — the records
   already exist.
3. A ``SKIPPED`` signal job also holds: no new signal, no new book.
   ``daily_pipeline`` holds the previous portfolio when ``run_daily_signal``
   never ran.
4. ``fetch_stock_daily`` is a Docker-container script, not importable
   in-process; ``JOB_REGISTRY`` wraps it as a subprocess (tests inject fakes
   via ``run_job(..., fn=...)`` and never touch Docker).
8. ``run_id`` is ``{YYYYMMDD}_{job_name}_{seq:03d}``, the sequence restarting
   per (day, job).

Record ruling: one JSONL row **per attempt**. ``status=RETRY`` marks an
attempt that failed with retries remaining, so all four statuses are reachable
from the engine; ``retry_count`` is the attempt index (0 for a first-try run).
"""

from __future__ import annotations

import json
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from data.storage_ops import FileLock
from ops.alerting import send_alert
from ops.monitoring import health_check

REPO_ROOT = Path(__file__).resolve().parents[1]

#: Run-record landing spot (``logs/`` is a gitignored runtime directory).
JOBS_JSONL = Path("logs/runs/jobs.jsonl")

#: The nine fields every ``jobs.jsonl`` row carries, in schema order.
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

#: The four-state run-record enum.
RUN_STATUSES = frozenset({"SUCCESS", "FAILED", "RETRY", "SKIPPED"})

#: The 5-job daily chain, in execution order (数据→因子→信号→巡检→日报).
JOB_ORDER = (
    "fetch_stock_daily",
    "compute_all",
    "run_daily_signal",
    "health_check",
    "write_daily_report",
)

#: The design job table (docs/design/ops/scheduler.md). Call sites pass these
#: to ``run_job`` (its signature defaults stay the doc's verbatim ones).
JOB_CONFIG = {
    "fetch_stock_daily": {"retries": 2, "timeout_s": 300, "on_failure": "skip"},
    "compute_all": {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
    "run_daily_signal": {"retries": 1, "timeout_s": 300, "on_failure": "hold"},
    "health_check": {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
    "write_daily_report": {"retries": 1, "timeout_s": 300, "on_failure": "skip"},
}


class JobTimeoutError(TimeoutError):
    """An attempt exceeded ``timeout_s``; it fails and is retried."""


class RunMutex:
    """Thread-reentrant exclusive guard over the run-record file.

    Wraps :class:`data.storage_ops.FileLock` (``flock LOCK_EX|LOCK_NB``,
    :class:`~data.storage_ops.ConcurrentWriteError` on contention, lock inode
    never unlinked) so a manual trigger cannot double-write ``jobs.jsonl``
    while a scheduled run is in flight — cross-process, which a threading lock
    cannot be. Re-entrant per thread so a pipeline can hold it across the
    ``run_job`` calls nested inside it.
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
            lock.__enter__()  # raises ConcurrentWriteError on contention
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
    """The process-wide run mutex (seam 6).

    ``run_job`` takes it around each run; ``daily_pipeline`` takes it around
    the whole pipeline so a manual ``run_job`` in between is refused with
    :class:`~data.storage_ops.ConcurrentWriteError`.
    """
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
    """Seam 4: subprocess wrapper around the Docker-container fetch script.

    ``scripts/data_fetcher/fetch_stock_daily.py`` has a ``main()`` but runs
    inside the AmazingData container, so it is not importable in-process.
    AGENTS.md requires the compose command to run from the project root.
    """
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


#: The five job classes (5 类 Job) and their callables. ``run_job(name)``
#: without ``fn`` looks the callable up here. Tests monkeypatch entries.
JOB_REGISTRY: dict[str, Callable[[], Any]] = {
    "fetch_stock_daily": fetch_stock_daily,
    "compute_all": _pending("compute_all", "S12 wires the FactorDAG + window"),
    "run_daily_signal": _pending(
        "run_daily_signal", "S12 injects the signal_fn producer"
    ),
    "health_check": health_check,
    "write_daily_report": _pending("write_daily_report", "S12-T3 body"),
}


def run_with_timeout(fn: Callable[[], Any], timeout_s: float) -> Any:
    """Run ``fn`` in a daemon thread, failing the attempt after ``timeout_s``.

    A timed-out ``fn`` is not killed (Python threads cannot be); the daemon
    thread is abandoned and the attempt is marked failed.
    """
    box: dict[str, Any] = {}
    done = threading.Event()

    def _call() -> None:
        try:
            box["result"] = fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised in the caller
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
    """Seam 8: ``{YYYYMMDD}_{job_name}_{seq:03d}``, seq per (day, job)."""
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
    """Append one nine-field run record to ``logs/runs/jobs.jsonl``.

    One row per attempt: ``RETRY`` for a failed attempt with retries left,
    ``SUCCESS``/``FAILED`` for the outcome row, ``SKIPPED`` for a job that did
    not run because an upstream job failed.
    """
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


def mark_downstream_skipped(name: str) -> list[dict]:
    """Seam 2: ``SKIPPED`` records for every job after *name* in ``JOB_ORDER``."""
    records: list[dict] = []
    if name not in JOB_ORDER:
        return records
    for later in JOB_ORDER[JOB_ORDER.index(name) + 1 :]:
        records.append(
            log_job_run(later, "SKIPPED", error=f"skipped: upstream {name} failed")
        )
    return records


def hold_previous_portfolio(name: str) -> dict:
    """Hold path (§65.6 red line): keep the previous portfolio.

    Hold is the absence of any portfolio action — this module never touches
    portfolio state and never liquidates on any failure path. The returned
    marker is what callers assert on; S12 wires the real book hand-off.
    """
    return {"action": "hold_previous_portfolio", "job": name, "portfolio": "previous"}


def run_job(
    name: str,
    fn: Callable[[], Any] | None = None,
    retries: int = 1,
    timeout_s: int = 300,
    on_failure: str = "skip",
) -> dict:
    """Run one job with retry / timeout / failure handling.

    ``retries`` counts re-attempts after the first (default 1 = 2 attempts
    total; the data job takes 2 per ``JOB_CONFIG``). Each attempt appends one
    run record and returns the outcome record.

    Failure handling after retries are exhausted: ``on_failure="hold"`` (or a
    returned ``{"status": "HOLD"}``, seam 1) holds the previous portfolio and
    never liquidates. Either way every later job gets a ``SKIPPED`` record
    (seam 2 — the hold path records the skips too) and exactly one HIGH alert
    is sent.
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
                    # Seam 1: the inner retry already happened — no re-retry.
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
                    hold_previous_portfolio(name)  # §65.6: keep previous book
                mark_downstream_skipped(name)  # review gap: both paths record
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
    """日频流水线: 数据→因子→信号→巡检→日报 (design-doc signature).

    Runs the five jobs in ``JOB_ORDER`` under the run mutex (seam 6), each
    with its ``JOB_CONFIG`` row, and stops at the first non-SUCCESS —
    ``run_job`` has already written the downstream ``SKIPPED`` records.
    Seam 3: if the signal job never ran, hold the previous portfolio (a
    failed signal job already held inside ``run_job``).
    """
    with run_mutex():
        signal_ran = False
        for name in JOB_ORDER:
            record = run_job(name, **JOB_CONFIG[name])
            if name == "run_daily_signal":
                signal_ran = True
            if record["status"] != "SUCCESS":
                break
        if not signal_ran:
            hold_previous_portfolio("run_daily_signal")  # seam 3: no new book
