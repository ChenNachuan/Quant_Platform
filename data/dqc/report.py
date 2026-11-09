"""DQC report — dual tables + the ``python -m dqc.report`` CLI (S9-T3).

Field authority: ``docs/design/data/dqc_runtime.md`` §38 (report dual tables,
CLI) and ``docs/design/data/quarantine_raw.md`` (quarantine query).  What to
check lives in ``data/dqc/rules/`` (S9-T2), disposition in
:mod:`data.dqc.severity`, execution in :mod:`data.dqc.engine`.

Dual tables, both under ``{data_root}/dqc/``:

- ``_dqc_results/month=YYYY-MM/data.parquet`` — one row per rule per run,
  append-only.  **Partition key**: ``month=YYYY-MM`` taken from ``data_date``
  (the business date under check, not the run date) — queries and the 1-year
  retention key off business time.
- ``_dqc_batch_summary.parquet`` — one row per ingest batch, single file.

Aggregation contract for ``_dqc_batch_summary``:

- ``total_rows = rows_dropped + rows_flagged + rows_clean``.  A refused
  batch's ``rows_dropped`` is ``rows_would_drop`` (the rows that triggered
  the refusal); the batch itself went to quarantine whole.
- ``drop_rate`` / ``flag_rate`` are shares of ``total_rows``.
- ``hard_rules_failed`` / ``soft_rules_failed`` are JSON lists of failing
  rule ids — the same encoding as ``sample_tickers`` / ``sample_values`` on
  the detail table (JSON strings, stable across appends).
- ``batch_status``: ``rejected`` (refused into quarantine),
  ``accepted_with_warnings`` (a flag-rule escalation alarm or the soft alarm
  fired), else ``accepted``.  Plain row drops record in ``rows_dropped`` and
  stay ``accepted`` — dropping is the routine remediation (§37.6 step 4).
- ``action_taken='reject_batch'`` on the detail table marks every failing
  rule whose disposition would have dropped rows (or gated the batch) when
  the batch was refused: the batch moved to quarantine whole, so nothing was
  dropped individually.  Failing ``flag`` rules keep ``'flag'``.

``reject_reason`` lives in the quarantine ``rejection.json`` (the §38 summary
header has no such column) and is surfaced by the ``quarantine`` subcommand.

CLI exit codes (all subcommands): 0 — completed normally (list commands
return 0 even with zero rows); 1 — the data condition failed (``reingest`` /
``revalidate`` met a batch that would be rejected); 2 — usage or operational
error.  ``run_time`` is naive UTC.  Retention (detail 1y / summary forever)
is a lifecycle concern (``data/storage_ops.py`` S4) and is not wired here.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from data.dqc.severity import severity_config_for
from data.quarantine import QUARANTINE_ROOT
from data.storage_ops import PARQUET_WRITE_CONFIG, FileLock
from data.storage_schema import PARTITION_PATHS
from core.config import DATA_ROOT

__all__ = [
    "DQC_REPORT_ROOT",
    "DETAIL_TABLE",
    "SUMMARY_TABLE",
    "detail_path",
    "summary_path",
    "build_detail_rows",
    "build_summary_row",
    "append_detail",
    "append_summary",
    "load_detail",
    "load_summary",
    "load_quarantine_batches",
    "build_reference_overrides",
    "run_dashboard",
    "reingest",
    "revalidate",
    "main",
]

logger = logging.getLogger(__name__)

DQC_REPORT_ROOT = "dqc"
DETAIL_TABLE = "_dqc_results"
SUMMARY_TABLE = "_dqc_batch_summary"

DETAIL_COLUMNS = [
    "run_date",
    "run_time",
    "data_date",
    "asset_type",
    "rule_id",
    "rule_name",
    "dimension",
    "severity",
    "action_taken",
    "total_rows",
    "failing_rows",
    "failing_rate",
    "sample_tickers",
    "sample_values",
    "rule_version",
]
SUMMARY_COLUMNS = [
    "run_time",
    "data_date",
    "asset_type",
    "total_rows",
    "rows_dropped",
    "rows_flagged",
    "rows_clean",
    "drop_rate",
    "flag_rate",
    "batch_status",
    "hard_rules_failed",
    "soft_rules_failed",
    "elapsed_seconds",
]
QUARANTINE_COLUMNS = [
    "data_date",
    "asset_type",
    "reject_time",
    "batch_status",
    "total_rows",
    "rows_would_drop",
    "drop_rate",
    "hard_rules_triggered",
    "reject_reason",
]
BATCH_STATUSES = ("accepted", "accepted_with_warnings", "rejected")
_DATETIME_COLUMNS = ("run_date", "run_time", "data_date")


def _now() -> datetime:
    """Naive UTC timestamp — the one clock the report tables share."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# ── table locations ───────────────────────────────────────────────────


def detail_path(
    data_date: date, data_root: str | os.PathLike[str] = str(DATA_ROOT)
) -> Path:
    """Month-partitioned detail file for *data_date*'s month."""
    month = data_date.strftime("%Y-%m")
    return (
        Path(data_root)
        / DQC_REPORT_ROOT
        / DETAIL_TABLE
        / f"month={month}"
        / "data.parquet"
    )


def summary_path(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> Path:
    """Single-file batch summary table."""
    return Path(data_root) / DQC_REPORT_ROOT / f"{SUMMARY_TABLE}.parquet"


# ── row builders ──────────────────────────────────────────────────────


def build_detail_rows(
    hard_results: Sequence[Any],
    soft_results: Sequence[Any],
    *,
    data_date: date,
    asset_type: str,
    batch_rejected: bool = False,
    run_time: datetime | None = None,
) -> list[dict]:
    """One ``_dqc_results`` row per rule per run (§38)."""
    when = run_time or _now()
    rows: list[dict] = []
    for result in list(hard_results) + list(soft_results):
        failing = result.failing_count
        if failing == 0:
            action = "pass"
        elif batch_rejected and severity_config_for(result.rule_id).row_action in (
            "drop",
            "pass",
        ):
            action = "reject_batch"
        else:
            action = result.action_taken
        rows.append(
            {
                "run_date": when.date(),
                "run_time": when,
                "data_date": data_date,
                "asset_type": asset_type,
                "rule_id": result.rule_id,
                "rule_name": result.rule_name,
                "dimension": result.dimension,
                "severity": result.severity,
                "action_taken": action,
                "total_rows": int(result.total_rows),
                "failing_rows": int(failing),
                "failing_rate": float(result.failing_rate),
                "sample_tickers": json.dumps(
                    list(result.sample_tickers), ensure_ascii=False, default=str
                ),
                "sample_values": json.dumps(
                    list(result.sample_values), ensure_ascii=False, default=str
                ),
                "rule_version": result.rule_version,
            }
        )
    return rows


def build_summary_row(
    data_date: date,
    asset_type: str,
    *,
    total_rows: int,
    rows_dropped: int,
    rows_flagged: int,
    batch_status: str,
    hard_failed: Sequence[str],
    soft_failed: Sequence[str],
    elapsed_seconds: float,
    run_time: datetime | None = None,
) -> dict:
    """One ``_dqc_batch_summary`` row (§38); see the module docstring."""
    if batch_status not in BATCH_STATUSES:
        raise ValueError(
            f"bad batch_status {batch_status!r}; known: {list(BATCH_STATUSES)}"
        )
    total = int(total_rows)
    dropped = int(rows_dropped)
    flagged = int(rows_flagged)
    return {
        "run_time": run_time or _now(),
        "data_date": data_date,
        "asset_type": asset_type,
        "total_rows": total,
        "rows_dropped": dropped,
        "rows_flagged": flagged,
        "rows_clean": total - dropped - flagged,
        "drop_rate": (dropped / total) if total else 0.0,
        "flag_rate": (flagged / total) if total else 0.0,
        "batch_status": batch_status,
        "hard_rules_failed": json.dumps(list(hard_failed), ensure_ascii=False),
        "soft_rules_failed": json.dumps(list(soft_failed), ensure_ascii=False),
        "elapsed_seconds": float(elapsed_seconds),
    }


# ── append + load ─────────────────────────────────────────────────────


def _append_parquet(path: Path, rows: list[dict], columns: list[str]) -> Path:
    """Append *rows* to *path*: read-modify-write under ``FileLock``.

    Reuses the ``data.raw`` pattern — ``FileLock`` + ``PARQUET_WRITE_CONFIG``
    + tmp→fsync→os.replace — because these tables are unregistered (no
    ``SCHEMA`` entry), so ``write_parquet_atomic`` refuses them.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows, columns=columns)
    for col in _DATETIME_COLUMNS:
        if col in new_df.columns:
            new_df[col] = pd.to_datetime(new_df[col])
    tmp = path.with_name(path.name + ".tmp")
    with FileLock(path):
        if path.exists():
            combined = pd.concat([pd.read_parquet(path), new_df], ignore_index=True)
        else:
            combined = new_df
        try:
            combined.to_parquet(tmp, **PARQUET_WRITE_CONFIG)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, path)
        except BaseException:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise
        try:
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning("wrote %s but could not fsync %s", path, path.parent)
    return path


def append_detail(
    rows: list[dict], data_root: str | os.PathLike[str] = str(DATA_ROOT)
) -> list[Path]:
    """Append detail rows to their ``month=YYYY-MM`` partition files."""
    if not rows:
        return []
    by_month: dict[str, list[dict]] = {}
    for row in rows:
        month = pd.Timestamp(row["data_date"]).strftime("%Y-%m")
        by_month.setdefault(month, []).append(row)
    written: list[Path] = []
    for month_rows in by_month.values():
        first = pd.Timestamp(month_rows[0]["data_date"]).date()
        written.append(
            _append_parquet(detail_path(first, data_root), month_rows, DETAIL_COLUMNS)
        )
    return written


def append_summary(
    row: dict, data_root: str | os.PathLike[str] = str(DATA_ROOT)
) -> Path:
    """Append one batch row to the summary table."""
    return _append_parquet(summary_path(data_root), [row], SUMMARY_COLUMNS)


def load_detail(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> pd.DataFrame:
    """Read every detail partition (empty frame when nothing was written)."""
    root = Path(data_root) / DQC_REPORT_ROOT / DETAIL_TABLE
    files = sorted(root.glob("month=*/data.parquet"))
    if not files:
        return pd.DataFrame(columns=DETAIL_COLUMNS)
    frames = [pd.read_parquet(f) for f in files]
    return pd.concat(frames, ignore_index=True)[DETAIL_COLUMNS]


def load_summary(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> pd.DataFrame:
    """Read the batch summary table (empty frame when nothing was written)."""
    path = summary_path(data_root)
    if not path.exists():
        return pd.DataFrame(columns=SUMMARY_COLUMNS)
    return pd.read_parquet(path)[SUMMARY_COLUMNS]


def load_quarantine_batches(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> list[dict]:
    """Quarantined batches: ``rejection.json`` records ∪ summary ``rejected``
    rows (the two views can differ after a T+30 quarantine cleanup)."""
    root = Path(data_root)
    batches: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for meta_file in sorted(root.glob(f"{QUARANTINE_ROOT}/*/*.rejection.json")):
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("unreadable rejection metadata %s: %s", meta_file, exc)
            continue
        row = {col: meta.get(col) for col in QUARANTINE_COLUMNS}
        row["hard_rules_triggered"] = list(row.get("hard_rules_triggered") or [])
        row["data_date"] = str(
            row.get("data_date") or meta_file.name[: -len(".rejection.json")]
        )
        row["asset_type"] = str(row.get("asset_type") or meta_file.parent.name)
        batches.append(row)
        seen.add((row["data_date"], row["asset_type"]))
    for summary in load_summary(data_root).to_dict("records"):
        if summary.get("batch_status") != "rejected":
            continue
        key = (
            pd.Timestamp(summary["data_date"]).strftime("%Y-%m-%d"),
            str(summary["asset_type"]),
        )
        if key in seen:
            continue
        seen.add(key)
        batches.append(
            {
                "data_date": key[0],
                "asset_type": key[1],
                "reject_time": None,
                "batch_status": "rejected",
                "total_rows": summary["total_rows"],
                "rows_would_drop": summary["rows_dropped"],
                "drop_rate": summary["drop_rate"],
                "hard_rules_triggered": json.loads(
                    summary["hard_rules_failed"] or "[]"
                ),
                "reject_reason": "(no quarantine record)",
            }
        )
    return sorted(batches, key=lambda r: (r["data_date"], r["asset_type"]))


# ── reference injection (CLI backfill of the check overrides) ─────────


def build_reference_overrides(
    data_root: str | os.PathLike[str], asset_type: str, data_date: date
) -> dict:
    """Assemble the check ``overrides`` from the local reference stores.

    ``calendar_dates`` ← ``asset/calendar/sse.parquet``; ``ca_dates`` ←
    ``asset/reference/corporate_actions.parquet`` ex_date; ``universe`` ←
    ``asset/reference/industry.parquet`` tickers; ``daily_tickers`` ← the
    latest ``asset/bars_daily`` partition at or before *data_date*;
    ``previous_adjfactor`` ← the currently stored landing frame of *asset_type*
    (T06 re-ingest idempotence); ``today`` is the **batch** date — a
    historical re-run judges against its own as-of date, not wall clock.

    A store that is absent stays absent: the checks fail closed on missing
    dependencies (C01/C02/C09/C10/K10/K11), which is the documented
    behaviour, not a silent pass.
    """
    root = Path(data_root)
    overrides: dict[str, Any] = {"today": data_date}

    cal = root / "asset" / "calendar" / "sse.parquet"
    if cal.exists():
        cal_df = pd.read_parquet(cal)
        if "trade_date" in cal_df.columns:
            overrides["calendar_dates"] = sorted(
                pd.to_datetime(cal_df["trade_date"]).dt.strftime("%Y-%m-%d").unique()
            )

    ca = root / "asset" / "reference" / "corporate_actions.parquet"
    if ca.exists():
        ca_df = pd.read_parquet(ca)
        if "ex_date" in ca_df.columns:
            overrides["ca_dates"] = sorted(
                pd.to_datetime(ca_df["ex_date"]).dt.strftime("%Y-%m-%d").unique()
            )

    ind = root / "asset" / "reference" / "industry.parquet"
    if ind.exists():
        ind_df = pd.read_parquet(ind)
        if "ticker" in ind_df.columns:
            overrides["universe"] = sorted(set(ind_df["ticker"].astype(str)))

    bars_dir = root / "asset" / "bars_daily"
    if bars_dir.exists():
        prior = [
            f
            for f in bars_dir.glob("*.parquet")
            if f.stem <= data_date.strftime("%Y-%m-%d")
        ]
        if prior:
            prev = pd.read_parquet(max(prior))
            if "ticker" in prev.columns:
                overrides["daily_tickers"] = sorted(set(prev["ticker"].astype(str)))

    template = PARTITION_PATHS.get(asset_type)
    if template and "{date}" in template:
        target = root / template.format(date=data_date.strftime("%Y-%m-%d"))
        if target.exists():
            stored = pd.read_parquet(target)
            if {"ticker", "adjfactor"} <= set(stored.columns):
                overrides["previous_adjfactor"] = stored
    return overrides


# ── dashboard (CLI + DuckDB) ──────────────────────────────────────────


def run_dashboard(
    data_root: str | os.PathLike[str] = str(DATA_ROOT), last: int = 10
) -> str:
    """Status counts, recent batches and top failing rules (DuckDB reads)."""
    import duckdb

    summary = summary_path(data_root)
    if not summary.exists():
        return f"no DQC batches under {Path(data_root) / DQC_REPORT_ROOT}/"
    detail_glob = (
        Path(data_root) / DQC_REPORT_ROOT / DETAIL_TABLE / "month=*/data.parquet"
    )
    con = duckdb.connect()
    lines: list[str] = []
    by_status = con.execute(
        "SELECT batch_status, count(*) AS batches, "
        "round(avg(drop_rate), 4) AS avg_drop_rate, "
        "round(avg(flag_rate), 4) AS avg_flag_rate "
        "FROM read_parquet(?) GROUP BY 1 ORDER BY 1",
        [str(summary)],
    ).fetchdf()
    lines.append("-- batch_status --")
    lines.append(by_status.to_string(index=False))
    recent = con.execute(
        "SELECT data_date, asset_type, batch_status, total_rows, rows_dropped, "
        "rows_flagged, drop_rate, flag_rate, elapsed_seconds "
        "FROM read_parquet(?) ORDER BY data_date DESC, run_time DESC LIMIT ?",
        [str(summary), int(last)],
    ).fetchdf()
    lines.append(f"-- last {int(last)} batches --")
    lines.append(recent.to_string(index=False))
    if list(detail_glob.parent.glob("month=*/data.parquet")):
        top = con.execute(
            "SELECT rule_id, rule_name, severity, count(*) AS failing_batches, "
            "round(max(failing_rate), 4) AS max_failing_rate "
            "FROM read_parquet(?) WHERE failing_rows > 0 "
            "GROUP BY 1, 2, 3 ORDER BY failing_batches DESC, rule_id LIMIT 10",
            [str(detail_glob)],
        ).fetchdf()
        lines.append("-- top failing rules --")
        lines.append(top.to_string(index=False))
    con.close()
    return "\n".join(lines)


# ── CLI operations ────────────────────────────────────────────────────


def reingest(
    source: str,
    asset_type: str,
    trade_date: date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    overrides: dict | None = None,
) -> int:
    """Re-download one date and run the full ingest (repair path A).

    Saves the raw fetch first (evidence chain), then normalizes and hands the
    batch to :func:`data.dqc.engine.ingest_with_dqc` — report tables and the
    overwrite write are its side.  Exit 1 when the batch is rejected (or the
    source returned nothing).
    """
    from data.adapters import get_adapter
    from data.dqc.engine import ingest_with_dqc
    from data.dqc.severity import DQCBatchReject
    from data.normalizers import get_normalizer
    from data.raw import save_raw

    adapter = get_adapter(source)
    try:
        result = adapter.fetch(asset_type, trade_date)
    finally:
        adapter.close()
    raw_df = result.df
    if raw_df is None or raw_df.empty:
        print(
            f"source {source} returned no rows for {asset_type} "
            f"{trade_date.strftime('%Y-%m-%d')}",
            file=sys.stderr,
        )
        return 1
    save_raw(raw_df, source, asset_type, trade_date, data_root)
    df = get_normalizer(source)(asset_type, raw_df)
    ref = build_reference_overrides(data_root, asset_type, trade_date)
    ref.update(overrides or {})
    try:
        ingest_with_dqc(df, trade_date, asset_type, overrides=ref, data_root=data_root)
    except DQCBatchReject as exc:
        print(f"batch rejected: {exc}", file=sys.stderr)
        return 1
    return 0


def revalidate(
    source: str,
    asset_type: str,
    start: date,
    end: date,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    overrides: dict | None = None,
) -> int:
    """Re-check local raw under the current rules — report rows only.

    Writes ``_dqc_results`` rows (``rule_version`` stamps the ruleset that
    judged them) and moves no data: no landing write, no summary row, no
    quarantine.  Exit 1 when any batch would be rejected.
    """
    from data.dqc.engine import evaluate_batch
    from data.normalizers import get_normalizer
    from data.raw import load_raw

    normalize = get_normalizer(source)
    rows: list[dict] = []
    checked = rejected = skipped = 0
    day = start
    while day <= end:
        raw = load_raw(source, asset_type, day, data_root)
        if raw is None:
            skipped += 1
        else:
            df = normalize(asset_type, raw)
            ref = build_reference_overrides(data_root, asset_type, day)
            ref.update(overrides or {})
            evaluation = evaluate_batch(df, day, asset_type, overrides=ref)
            bad = evaluation.batch_status == "rejected"
            checked += 1
            rejected += int(bad)
            rows.extend(
                build_detail_rows(
                    evaluation.hard_results,
                    evaluation.soft_results,
                    data_date=day,
                    asset_type=asset_type,
                    batch_rejected=bad,
                )
            )
        day += timedelta(days=1)
    written = append_detail(rows, data_root) if rows else []
    print(
        f"revalidated {checked} batch(es): {rejected} would reject, "
        f"{skipped} missing raw, {len(written)} report file(s) written"
    )
    return 1 if rejected else 0


# ── argparse entry point ──────────────────────────────────────────────


def _parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def _print_quarantine(batches: list[dict]) -> None:
    if not batches:
        print("no quarantined batches")
        return
    for row in batches:
        triggered = ",".join(row.get("hard_rules_triggered") or []) or "-"
        print(
            f"{row['data_date']} {row['asset_type']}: {row['batch_status']} "
            f"total={row['total_rows']} would_drop={row['rows_would_drop']} "
            f"drop_rate={row['drop_rate']} rules=[{triggered}] "
            f"reason={row['reject_reason']}"
        )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dqc.report",
        description=(
            "DQC dual-table report: quarantine / dashboard / reingest / revalidate."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser(
        "quarantine", help="list quarantined batches with reject reasons"
    )
    p.add_argument("--data-root", default=str(DATA_ROOT))
    p.add_argument("--json", action="store_true", help="print machine-readable JSON")

    p = sub.add_parser(
        "dashboard", help="status counts, recent batches, top failing rules"
    )
    p.add_argument("--data-root", default=str(DATA_ROOT))
    p.add_argument("--last", type=int, default=10)

    p = sub.add_parser(
        "reingest", help="re-download one date and re-run the full ingest"
    )
    p.add_argument("--source", required=True, help="raw source name (akshare/...)")
    p.add_argument("--asset", required=True, help="asset_type (bars_daily/...)")
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--data-root", default=str(DATA_ROOT))

    p = sub.add_parser(
        "revalidate", help="re-check local raw under current rules (report only)"
    )
    p.add_argument("--source", required=True, help="raw source name (akshare/...)")
    p.add_argument("--asset", required=True, help="asset_type (bars_daily/...)")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--data-root", default=str(DATA_ROOT))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry (``python -m dqc.report``).  Returns the exit code."""
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "quarantine":
            batches = load_quarantine_batches(args.data_root)
            if args.json:
                print(json.dumps(batches, ensure_ascii=False, indent=2, default=str))
            else:
                _print_quarantine(batches)
            return 0
        if args.command == "dashboard":
            print(run_dashboard(args.data_root, last=args.last))
            return 0
        if args.command == "reingest":
            return reingest(
                args.source,
                args.asset,
                _parse_date(args.date),
                data_root=args.data_root,
            )
        if args.command == "revalidate":
            start, end = _parse_date(args.start), _parse_date(args.end)
            if end < start:
                raise ValueError(f"end {end} precedes start {start}")
            return revalidate(
                args.source,
                args.asset,
                start,
                end,
                data_root=args.data_root,
            )
    except Exception as exc:  # noqa: BLE001 — operational errors exit 2
        print(f"error: {exc}", file=sys.stderr)
        return 2
    raise AssertionError(f"unhandled command {args.command!r}")


if __name__ == "__main__":
    sys.exit(main())
