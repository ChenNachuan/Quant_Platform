"""DQC engine — rule execution + the seven-step ingest core (S9-T3).

Field authority: ``docs/design/data/dqc_runtime.md`` (entry, report contract)
and ``docs/design/data/dqc_severity.md`` §37.6 (step order).  *What to check*
is ``data/dqc/rules/*.yaml`` + ``data/dqc/checks/`` (S9-T2); *what to do about
a finding* is :mod:`data.dqc.severity` (the §37.3 disposition table).

Entry point: :func:`ingest_with_dqc` — steps of §37.6, with the rule-level
escalation of §37.3 checked before the global valve (stricter first):

1. :func:`run_all_checks` (HARD + SOFT)
2. collect row-level HARD ``drop`` rows (partition granularity expands to
   whole ticker×date groups)
3. reject the batch on escalation: (a) per-rule ``escalation_threshold``,
   (b) global ``max_hard_drop_rate`` valve
4. drop the problem rows
5. compute ``quality_flag``
6. soft alarm above ``soft_alarm_rate`` (never rejects)
7. validate against the storage schema and write parquet

Every disposition lands on the report dual tables under ``{data_root}/dqc/``
(:mod:`data.dqc.report`), a refused batch included — the report is written
before quarantine so the refusal is on record even though no data lands.

Check evaluation matches the S9-T2 test harness semantics exactly (failure
mask ``True = failure``; a missing column fails every row; the rule filter
narrows failures to its scope).  Aggregate verdicts (``__aggregate__``) judge
the whole batch and are broadcast to every row before disposition.
"""

from __future__ import annotations

import importlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, NoReturn

import numpy as np
import pandas as pd
import yaml
from pandera.pandas import DataFrameSchema

from core.config import DATA_ROOT
from data.dqc import report
from data.dqc.severity import (
    BatchRejectConfig,
    DQCBatchReject,
    compute_quality_flag,
    severity_config_for,
)
from data.quarantine import save_quarantine
from data.storage_ops import normalize_missing_floats, write_parquet_atomic
from data.storage_schema import PARTITION_PATHS, SCHEMA

__all__ = [
    "RULES_DIR",
    "RULE_VERSION",
    "CheckResult",
    "BatchEvaluation",
    "run_all_checks",
    "evaluate_batch",
    "build_schema",
    "ingest_with_dqc",
]

logger = logging.getLogger(__name__)

RULES_DIR: Path = Path(__file__).resolve().parent / "rules"

# asset_type -> rules file.  asset_type is the table family the DQC runtime
# knows (dqc_runtime.md: "asset_type | bars_daily 等 5 类").
ASSET_RULE_FILES: dict[str, str] = {
    "bars_daily": "bars_daily.yaml",
    "bars_minute": "bars_minute.yaml",
    "fundamental": "fundamental.yaml",
    "factor": "factor.yaml",
    "reference": "reference.yaml",
}

# Ruleset version stamped on every CheckResult.  The YAMLs carry no version
# key, so the engine owns the number: patch = thresholds, minor = rules
# added, major = semantics changed (docs/design/data/dqc_rules.md).
RULE_VERSION = "1.0.0"

AGGREGATE_INDEX = "__aggregate__"
CROSS_FUNCTION_PREFIX = "checks.cross_checks."

DIMENSION_BY_PREFIX = {
    "C": "completeness",
    "L": "legality",
    "K": "consistency",
    "T": "timeliness",
}


@dataclass
class CheckResult:
    """One rule's verdict over one batch (dqc_runtime.md report contract).

    ``failing_rows`` is always row-aligned to the input frame — aggregate
    verdicts are broadcast.  ``action_taken`` is the disposition applied to
    the rows: ``drop`` / ``flag`` / ``pass`` (batch gates touch no row; the
    batch outcome is recorded on the summary table); ``reject_batch`` is
    written by the report writer when the batch was refused.
    """

    rule_id: str
    rule_name: str
    dimension: str  # completeness | legality | consistency | timeliness
    severity: str  # HARD | SOFT (the rule's YAML class)
    action_taken: str  # pass | drop | flag | reject_batch
    total_rows: int
    failing_rows: pd.Series  # bool, indexed like the input frame
    failing_rate: float
    sample_tickers: list[str]
    sample_values: list[Any]
    rule_version: str = RULE_VERSION
    data_date: date | None = None  # batch business date stamped by run_all_checks

    @property
    def failing_count(self) -> int:
        return int(self.failing_rows.sum())


@dataclass
class BatchEvaluation:
    """Steps 2-3 and 5-6 of §37.6 over one batch — the fate before any write.

    ``batch_status`` / ``reject_reason`` decide what ingest does next:
    ``rejected`` quarantines the batch whole (nothing lands), anything else
    drops ``rows_to_drop`` and writes.  ``quality_flag`` is computed once on
    the full input frame (row-aligned); ingest slices it for the clean frame
    and the summary accounting reads ``flagged_count`` (kept rows with a bit
    set).  ``warnings`` records the alarms that fired (escalated flag-rule
    alarm, soft alarm) — they never reject.
    """

    hard_results: list[CheckResult]
    soft_results: list[CheckResult]
    rows_to_drop: pd.Series  # bool, indexed like the input frame
    quality_flag: pd.Series  # int64 bitmask, indexed like the input frame
    warnings: list[str]
    batch_status: str  # accepted | accepted_with_warnings | rejected
    reject_reason: str | None
    flagged_count: int

    @property
    def rows_would_drop(self) -> int:
        return int(self.rows_to_drop.sum())


# ── rule loading + evaluation (S9-T2 harness semantics) ────────────────


def _load_rules(asset_type: str) -> list[dict]:
    try:
        fname = ASSET_RULE_FILES[asset_type]
    except KeyError:
        raise KeyError(
            f"unknown asset_type {asset_type!r}; known: {sorted(ASSET_RULE_FILES)}"
        ) from None
    with open(RULES_DIR / fname, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return list(data["rules"])


def _resolve_function(func_path: str):
    """Resolve ``checks.custom_checks.foo`` -> callable under ``data.dqc``."""
    full = "data.dqc." + func_path
    mod_name, fn_name = full.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, fn_name)


def _apply_condition(series: pd.Series, condition: str, value) -> pd.Series:
    """Return the boolean failure mask of one column under one condition."""
    if condition == "gt":
        return series <= value
    if condition == "ge":
        return series < value
    if condition == "lt":
        return series >= value
    if condition == "le":
        return series > value
    if condition == "eq":
        return series != value
    if condition == "ne":
        return series == value
    if condition == "range":
        return (series <= value[0]) | (series >= value[1])
    if condition == "regex":
        return ~series.astype(str).str.match(value, na=False)
    if condition == "notnull":
        return series.isna()
    if condition == "isbool_notnull":
        is_bool = series.map(
            lambda x: isinstance(x, (bool, np.bool_)) if pd.notna(x) else False
        )
        return series.isna() | ~is_bool
    if condition == "notinf":
        if series.dtype.kind in "fc":
            return series.abs() == float("inf")
        return pd.Series(False, index=series.index)
    if condition == "date_format":
        return pd.to_datetime(series, errors="coerce", format="ISO8601").isna()
    raise ValueError(f"Unknown condition: {condition}")


def _evaluate_column_check(df: pd.DataFrame, rule: dict) -> pd.Series:
    """Evaluate a declarative column_check.  Returns the failure mask."""
    failures = pd.Series(False, index=df.index)
    value = rule["value"]
    for col in rule["columns"]:
        if col not in df.columns:
            failures = failures | pd.Series(True, index=df.index)
            continue
        col_value = value[col] if isinstance(value, dict) else value
        failures = failures | _apply_condition(df[col], rule["condition"], col_value)
    if rule.get("filter"):
        in_scope = df.eval(rule["filter"]).astype(bool)
        failures = failures & in_scope
    return failures


def _eval_params(rule: dict, overrides: dict) -> dict:
    params = dict(rule.get("params") or {})
    params.update(overrides)
    return params


def _evaluate_custom_check(df: pd.DataFrame, rule: dict, overrides: dict) -> pd.Series:
    """Call the rule's custom check; *overrides* are injected at call time."""
    fn = _resolve_function(rule["function"])
    return fn(df, **_eval_params(rule, overrides))


def _evaluate_cross_check(
    rule: dict, daily_df: pd.DataFrame, minute_df: pd.DataFrame, overrides: dict
) -> pd.Series:
    """Call a cross-table check (K06-K08): ``fn(daily, minute, **params)``."""
    fn = _resolve_function(rule["function"])
    return fn(daily_df, minute_df, **_eval_params(rule, overrides))


def _normalize_mask(raw: pd.Series, df: pd.DataFrame) -> pd.Series:
    """Return *raw* as a bool mask aligned to every row of *df*.

    Three shapes arrive from checks (S9-T2 contract): row-aligned, an
    aggregate verdict of length 1 indexed ``__aggregate__`` (broadcast — the
    rule judges the batch), and a ``(ticker, trade_date)`` MultiIndex for
    cross checks (mapped onto the rows of the frame under test).
    """
    raw = raw.fillna(False).astype(bool)
    if list(raw.index) == [AGGREGATE_INDEX]:
        return pd.Series(bool(raw.iloc[0]), index=df.index, dtype=bool)
    if isinstance(raw.index, pd.MultiIndex):
        date_col = "trade_date" if "trade_date" in df.columns else "date"
        keys = pd.MultiIndex.from_frame(df[["ticker", date_col]])
        return pd.Series(raw.reindex(keys, fill_value=False).to_numpy(), index=df.index)
    return raw.reindex(df.index, fill_value=False)


def _samples(df: pd.DataFrame, mask: pd.Series, rule: dict) -> tuple[list, list]:
    """Failing tickers (<= 5) and failing values of the rule's first column."""
    failing = df.loc[mask]
    tickers: list[str] = []
    if "ticker" in df.columns:
        tickers = failing["ticker"].astype(str).drop_duplicates().head(5).tolist()
    cols = rule.get("columns") or []
    col = cols[0] if cols and cols[0] in df.columns else None
    if col is None:
        for c in ("close", "value", "adjfactor"):
            if c in df.columns:
                col = c
                break
    values: list = []
    if col is not None:
        values = failing[col].head(5).tolist()
    return tickers, values


def _build_result(
    rule: dict, df: pd.DataFrame, mask: pd.Series, data_date: date | None = None
) -> CheckResult:
    cfg = severity_config_for(rule["id"])
    total = len(df)
    failing = int(mask.sum())
    if cfg.granularity == "batch":
        rate = 1.0 if failing else 0.0
    else:
        rate = (failing / total) if total else 0.0
    tickers, values = _samples(df, mask, rule)
    return CheckResult(
        rule_id=rule["id"],
        rule_name=rule.get("name", rule["id"]),
        dimension=DIMENSION_BY_PREFIX[rule["id"][0]],
        severity=rule["severity"],
        action_taken=("pass" if failing == 0 else cfg.row_action),
        total_rows=total,
        failing_rows=mask,
        failing_rate=rate,
        sample_tickers=tickers,
        sample_values=values,
        data_date=data_date,
    )


# ── public API ─────────────────────────────────────────────────────────


def run_all_checks(
    df: pd.DataFrame,
    data_date: date,
    asset_type: str,
    *,
    overrides: dict | None = None,
    cross_df: pd.DataFrame | None = None,
) -> tuple[list[CheckResult], list[CheckResult]]:
    """Run the asset's rules; return ``(hard_results, soft_results)``.

    Reference data (``calendar_dates``, ``universe``, ``today``,
    ``ca_dates``, ``daily_tickers``, ...) is injected through *overrides*
    and merged into every custom check's params — checks never read it from
    disk.  A check whose dependency is missing fails closed (C01/C02/K10/
    K11) or falls back to its documented default (``today`` -> wall clock).

    Cross rules (K06-K08) need two frames and run only when *cross_df* is
    given — they belong to the weekly cross-granularity batch (§36.10),
    not to every single-date ingest.  *data_date* is the batch's business
    date; it is recorded on the results, not read from the frame.
    """
    overrides = dict(overrides or {})
    hard: list[CheckResult] = []
    soft: list[CheckResult] = []
    for rule in _load_rules(asset_type):
        fn_path = rule.get("function") or ""
        if fn_path.startswith(CROSS_FUNCTION_PREFIX):
            if cross_df is None:
                continue
            if asset_type == "bars_daily":
                daily_df, minute_df = df, cross_df
            elif asset_type == "bars_minute":
                daily_df, minute_df = cross_df, df
            else:
                raise ValueError(
                    f"cross rules need bars_daily/bars_minute frames, "
                    f"got asset_type={asset_type!r}"
                )
            raw = _evaluate_cross_check(rule, daily_df, minute_df, overrides)
        elif rule["type"] == "column_check":
            raw = _evaluate_column_check(df, rule)
        else:
            raw = _evaluate_custom_check(df, rule, overrides)
        result = _build_result(rule, df, _normalize_mask(raw, df), data_date)
        (hard if rule["severity"] == "HARD" else soft).append(result)
    return hard, soft


def build_schema(table: str) -> DataFrameSchema:
    """Return the storage schema of *table* — the pipeline's pre-write gate.

    The DQC YAMLs declare check rules, not dtypes; frame structure is owned
    by ``data/storage_schema.py`` (T1 authority).  Step 7 validates the clean
    frame against this schema before writing, and ``write_parquet_atomic``
    enforces the same schema at the storage layer.

    (``docs/design/data/dqc_rules.md`` once sketched
    ``build_schema(yaml_path) -> pandera.DataFrameSchema``; the delivered
    rule YAMLs carry no dtype blocks, so schema-from-rules is inexpressible
    and that sketch is superseded by this table-scoped form.)
    """
    if table not in SCHEMA:
        raise KeyError(
            f"no storage schema for table {table!r}; known: {sorted(SCHEMA)}"
        )
    return SCHEMA[table]


def _landing_path(table: str, data_date: date, data_root: str | os.PathLike) -> Path:
    template = PARTITION_PATHS.get(table)
    if template is None or "{date}" not in template:
        raise NotImplementedError(
            f"landing layout for table {table!r} is not a date partition; "
            f"pass target_path explicitly (layouts: {PARTITION_PATHS})"
        )
    return Path(data_root) / template.format(date=data_date.strftime("%Y-%m-%d"))


def _expand_to_partitions(df: pd.DataFrame, mask: pd.Series) -> pd.Series:
    """Expand a failure mask to whole ``(ticker, trade_date)`` groups."""
    keys = [c for c in ("ticker", "trade_date") if c in df.columns]
    if len(keys) < 2 or not mask.any():
        return mask
    idx = pd.MultiIndex.from_frame(df[keys])
    hit = pd.MultiIndex.from_frame(df.loc[mask, keys]).unique()
    return pd.Series(idx.isin(hit), index=df.index, dtype=bool)


def evaluate_batch(
    df: pd.DataFrame,
    data_date: date,
    asset_type: str,
    *,
    overrides: dict | None = None,
    cross_df: pd.DataFrame | None = None,
    batch_config: BatchRejectConfig | None = None,
) -> BatchEvaluation:
    """Run the asset's rules and judge the batch (§37.6 steps 2-3, 5-6).

    Never raises: the caller reads ``batch_status`` / ``reject_reason`` and
    decides.  Rule-level escalation is checked before the global valve
    (stricter first) and the first rejecting escalation wins.  Flag-rule
    escalations (K06-K08) and the soft alarm raise warnings only — SOFT
    findings never reject (issue red line).
    """
    config = batch_config or BatchRejectConfig()
    hard_results, soft_results = run_all_checks(
        df, data_date, asset_type, overrides=overrides, cross_df=cross_df
    )

    # step 2 — collect row-level HARD drops (§37.3 granularity)
    rows_to_drop = pd.Series(False, index=df.index)
    for r in hard_results:
        cfg = severity_config_for(r.rule_id)
        if cfg.row_action != "drop":
            continue
        mask = r.failing_rows
        if cfg.granularity == "partition":
            mask = _expand_to_partitions(df, mask)
        rows_to_drop = rows_to_drop | mask

    # step 3a — rule-level escalation (double insurance with the valve).
    # drop/pass rules above threshold reject the batch; flag rules (K06-K08)
    # raise an escalated alarm and keep their rows.
    reject_reason: str | None = None
    warnings: list[str] = []
    for r in hard_results + soft_results:
        cfg = severity_config_for(r.rule_id)
        thr = cfg.escalation_threshold
        if thr is None or not r.failing_count or r.failing_rate <= thr:
            continue
        if cfg.row_action == "flag":
            logger.error(
                "DQC escalated alarm: %s failing rate %.4f > %.4f (%s %s); rows kept",
                r.rule_id,
                r.failing_rate,
                thr,
                data_date,
                asset_type,
            )
            warnings.append(f"escalated_alarm:{r.rule_id}")
            continue
        reject_reason = f"rule_escalation:{r.rule_id}"
        break

    # step 3b — global batch safety valve (a rule reason is stricter)
    drop_rate = (float(rows_to_drop.sum()) / len(df)) if len(df) else 0.0
    if reject_reason is None and drop_rate > config.max_hard_drop_rate:
        reject_reason = (
            f"global_valve:hard_drop_rate {drop_rate:.4f} > {config.max_hard_drop_rate}"
        )

    # step 5 — quality_flag bitmask (SOFT findings only), computed once
    quality_flag = compute_quality_flag(df, soft_results)

    # step 6 — soft alarm: never rejects
    kept = ~rows_to_drop
    kept_rows = int(kept.sum())
    flagged_count = int((quality_flag[kept] != 0).sum()) if kept_rows else 0
    flagged_rate = (flagged_count / kept_rows) if kept_rows else 0.0
    if flagged_rate > config.soft_alarm_rate:
        logger.warning(
            "DQC soft alarm: flagged rate %.4f > %.4f (%s %s); batch accepted "
            "(SOFT never rejects)",
            flagged_rate,
            config.soft_alarm_rate,
            data_date,
            asset_type,
        )
        warnings.append("soft_alarm")

    if reject_reason is not None:
        status = "rejected"
    elif warnings:
        status = "accepted_with_warnings"
    else:
        status = "accepted"
    return BatchEvaluation(
        hard_results=hard_results,
        soft_results=soft_results,
        rows_to_drop=rows_to_drop,
        quality_flag=quality_flag,
        warnings=warnings,
        batch_status=status,
        reject_reason=reject_reason,
        flagged_count=flagged_count,
    )


def _reject_batch(
    df: pd.DataFrame,
    data_date: date,
    asset_type: str,
    rows_to_drop: pd.Series,
    hard_results: list[CheckResult],
    *,
    reason: str,
    data_root: str | os.PathLike,
) -> NoReturn:
    """Quarantine the raw batch and raise DQCBatchReject (§37.6)."""
    drop_rate = (float(rows_to_drop.sum()) / len(df)) if len(df) else 0.0
    triggered = [r.rule_id for r in hard_results if r.failing_count]
    detail = {
        "data_date": data_date.strftime("%Y-%m-%d"),
        "asset_type": asset_type,
        "reject_time": datetime.now().isoformat(timespec="seconds"),
        "batch_status": "rejected",
        "total_rows": int(len(df)),
        "rows_would_drop": int(rows_to_drop.sum()),
        "drop_rate": round(drop_rate, 6),
        "hard_rules_triggered": triggered,
        "reject_reason": reason,
    }
    save_quarantine(df, asset_type, data_date, detail, data_root)
    logger.error("DQC batch rejected (%s): %s", reason, detail)
    raise DQCBatchReject(
        f"batch {detail['data_date']} {asset_type} rejected: {reason} "
        f"(drop_rate={drop_rate:.4f}, rows_would_drop={detail['rows_would_drop']}"
        f"/{detail['total_rows']})",
        detail=detail,
    )


def ingest_with_dqc(
    df: pd.DataFrame,
    data_date: date,
    asset_type: str,
    *,
    table: str | None = None,
    overrides: dict | None = None,
    cross_df: pd.DataFrame | None = None,
    batch_config: BatchRejectConfig | None = None,
    data_root: str | os.PathLike = str(DATA_ROOT),
    target_path: str | os.PathLike | None = None,
) -> pd.DataFrame:
    """Seven-step pre-load ingest (dqc_severity.md §37.6).  Returns df_clean.

    HARD rows that must leave are dropped (or the batch is refused into
    quarantine); SOFT findings only mark ``quality_flag``.  The returned
    frame is the post-drop batch with ``quality_flag`` attached (helper
    columns such as ``prev_close`` kept); the stored parquet is its
    projection onto the storage schema of *table* (``asset_type`` when the
    table name matches — ``bars_daily`` etc.  Reference sub-tables pass
    ``table=`` explicitly.)

    *batch_config* is the global valve (defaults
    ``max_hard_drop_rate=0.20`` / ``soft_alarm_rate=0.50``).  *target_path*
    overrides the ``PARTITION_PATHS`` landing spot for tables whose layout
    is not a plain date partition (factor / reference writers).

    Every disposition also lands on the report dual tables under
    ``{data_root}/dqc/`` (:mod:`data.dqc.report`): a refused batch is
    recorded (``batch_status='rejected'`` / ``action_taken='reject_batch'``)
    *before* it goes to quarantine, an accepted one after the parquet write.
    """
    config = batch_config or BatchRejectConfig()
    table = table or asset_type
    started = time.monotonic()
    run_time = datetime.now(timezone.utc).replace(tzinfo=None)
    evaluation = evaluate_batch(
        df,
        data_date,
        asset_type,
        overrides=overrides,
        cross_df=cross_df,
        batch_config=config,
    )
    hard_results, soft_results = evaluation.hard_results, evaluation.soft_results
    rows_to_drop = evaluation.rows_to_drop
    drop_rate = (float(rows_to_drop.sum()) / len(df)) if len(df) else 0.0
    rejected = evaluation.batch_status == "rejected"
    detail_rows = report.build_detail_rows(
        hard_results,
        soft_results,
        data_date=data_date,
        asset_type=asset_type,
        batch_rejected=rejected,
        run_time=run_time,
    )
    summary_row = report.build_summary_row(
        data_date,
        asset_type,
        total_rows=len(df),
        rows_dropped=evaluation.rows_would_drop,
        rows_flagged=evaluation.flagged_count,
        batch_status=evaluation.batch_status,
        hard_failed=[r.rule_id for r in hard_results if r.failing_count],
        soft_failed=[r.rule_id for r in soft_results if r.failing_count],
        elapsed_seconds=time.monotonic() - started,
        run_time=run_time,
    )

    if rejected:
        # the refusal is a fact even though no data lands: report first,
        # then quarantine + raise
        report.append_detail(detail_rows, data_root=data_root)
        report.append_summary(summary_row, data_root=data_root)
        _reject_batch(
            df,
            data_date,
            asset_type,
            rows_to_drop,
            hard_results,
            reason=evaluation.reject_reason,
            data_root=data_root,
        )

    # step 4 — drop problem rows
    df_clean = df.loc[~rows_to_drop].copy()

    # step 5 — quality_flag bitmask (SOFT findings only)
    df_clean["quality_flag"] = evaluation.quality_flag[~rows_to_drop]

    # step 6 (soft alarm) was judged in evaluate_batch — it never rejects

    # step 7 — schema gate + atomic write
    schema = build_schema(table)
    projected = df_clean[[c for c in schema.columns if c in df_clean.columns]]
    validated = schema.validate(normalize_missing_floats(projected, table))
    path = (
        Path(target_path)
        if target_path is not None
        else _landing_path(table, data_date, data_root)
    )
    write_parquet_atomic(validated, path, table=table)

    flagged_rate = (
        float((df_clean["quality_flag"] != 0).mean()) if len(df_clean) else 0.0
    )
    report.append_detail(detail_rows, data_root=data_root)
    report.append_summary(summary_row, data_root=data_root)
    logger.info(
        "DQC ingest ok: %s %s rows %d -> %d, drop_rate %.4f, flagged_rate %.4f",
        asset_type,
        data_date,
        len(df),
        len(df_clean),
        drop_rate,
        flagged_rate,
    )
    return df_clean
