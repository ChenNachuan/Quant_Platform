"""DQC 引擎：规则执行与七步入库流程。

入口 :func:`ingest_with_dqc` 按顺序执行（规则级升级优先于全局安全阀）：
1. :func:`run_all_checks`（HARD + SOFT 规则）
2. 收集行级 HARD ``drop`` 行（``partition`` 粒度扩展至整组 ``(ticker, trade_date)``）
3. 判定拒收：(a) 单规则 ``escalation_threshold``；(b) 全局 ``max_hard_drop_rate`` 安全阀
4. 剔除问题行
5. 计算 ``quality_flag`` 位掩码
6. 触发软告警（超过 ``soft_alarm_rate`` 时告警，永不拒收）
7. 校验存储 Schema 并原子写入 Parquet

无论接收或拒收，均写入 ``{data_root}/dqc/`` 下的双报告表（拒收先写报告再进隔离区）。
评估语义约定 ``True`` 表示校验失败；缺列时全行判错；聚合结果（``__aggregate__``）广播至全批。
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
from data.storage_ops import (
    is_monthly_partition,
    normalize_missing_floats,
    partition_path,
    upsert_parquet_atomic,
    write_parquet_atomic,
)
from data.storage_schema import SCHEMA

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

# asset_type -> 规则文件名映射
ASSET_RULE_FILES: dict[str, str] = {
    "bars_daily": "bars_daily.yaml",
    "bars_minute": "bars_minute.yaml",
    "fundamental": "fundamental.yaml",
    "factor": "factor.yaml",
    "reference": "reference.yaml",
}

# 规则集版本号（YAML 中不维护版本字段，由引擎统一标记：patch 改阈值，minor 增规则，major 改语义）
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
    """单条规则对单批次的检查结果。

    ``failing_rows`` 始终对齐输入行（聚合结论会广播到每行）；``action_taken`` 为
    ``pass`` / ``drop`` / ``flag`` / ``reject_batch``。
    """

    rule_id: str
    rule_name: str
    dimension: str  # completeness | legality | consistency | timeliness
    severity: str  # HARD | SOFT
    action_taken: str  # pass | drop | flag | reject_batch
    total_rows: int
    failing_rows: pd.Series  # 布尔掩码，索引对齐输入 DataFrame
    failing_rate: float
    sample_tickers: list[str]
    sample_values: list[Any]
    rule_version: str = RULE_VERSION
    data_date: date | None = None

    @property
    def failing_count(self) -> int:
        return int(self.failing_rows.sum())


@dataclass
class BatchEvaluation:
    """批次写入前的综合评估结论（对应七步流程第 2–3、5–6 步）。

    ``batch_status == "rejected"`` 时整批进隔离区不落盘；否则剔除 ``rows_to_drop``
    并保留 ``quality_flag``。``warnings`` 仅记录升级告警与软告警，不触发拒收。
    """

    hard_results: list[CheckResult]
    soft_results: list[CheckResult]
    rows_to_drop: pd.Series  # 布尔掩码，索引对齐输入 DataFrame
    quality_flag: pd.Series  # int64 位掩码，索引对齐输入 DataFrame
    warnings: list[str]
    batch_status: str  # accepted | accepted_with_warnings | rejected
    reject_reason: str | None
    flagged_count: int

    @property
    def rows_would_drop(self) -> int:
        return int(self.rows_to_drop.sum())


# ── 规则加载与执行 ───────────────────────────────────────────────────────


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
    """将 ``checks.custom_checks.foo`` 解析为 ``data.dqc`` 下的可调用对象。"""
    full = "data.dqc." + func_path
    mod_name, fn_name = full.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    return getattr(mod, fn_name)


def _apply_condition(series: pd.Series, condition: str, value) -> pd.Series:
    """计算单列在指定条件下的失败掩码。"""
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
    """执行声明式列检查，返回失败掩码。"""
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
    """调用自定义检查函数并注入 *overrides* 参数。"""
    fn = _resolve_function(rule["function"])
    return fn(df, **_eval_params(rule, overrides))


def _evaluate_cross_check(
    rule: dict, daily_df: pd.DataFrame, minute_df: pd.DataFrame, overrides: dict
) -> pd.Series:
    """调用跨表检查函数（K06–K08）。"""
    fn = _resolve_function(rule["function"])
    return fn(daily_df, minute_df, **_eval_params(rule, overrides))


def _normalize_mask(raw: pd.Series, df: pd.DataFrame) -> pd.Series:
    """将行级、``__aggregate__`` 聚合或 ``(ticker, trade_date)`` 跨表结果统一对齐到 *df* 行索引。"""
    raw = raw.fillna(False).astype(bool)
    if list(raw.index) == [AGGREGATE_INDEX]:
        return pd.Series(bool(raw.iloc[0]), index=df.index, dtype=bool)
    if isinstance(raw.index, pd.MultiIndex):
        date_col = "trade_date" if "trade_date" in df.columns else "date"
        keys = pd.MultiIndex.from_frame(df[["ticker", date_col]])
        return pd.Series(raw.reindex(keys, fill_value=False).to_numpy(), index=df.index)
    return raw.reindex(df.index, fill_value=False)


def _samples(df: pd.DataFrame, mask: pd.Series, rule: dict) -> tuple[list, list]:
    """抽取前 5 个失败标的与首列样例值。"""
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


# ── 公共 API ─────────────────────────────────────────────────────────────


def run_all_checks(
    df: pd.DataFrame,
    data_date: date,
    asset_type: str,
    *,
    overrides: dict | None = None,
    cross_df: pd.DataFrame | None = None,
) -> tuple[list[CheckResult], list[CheckResult]]:
    """执行指定 *asset_type* 的全部规则，返回 ``(hard_results, soft_results)``。

    外部参考数据通过 *overrides* 注入；跨表规则（K06–K08）仅在传入 *cross_df* 时运行。
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
    """返回 *table* 对应的存储校验 Schema（由 ``data/storage_schema.py`` 统一定义）。"""
    if table not in SCHEMA:
        raise KeyError(
            f"no storage schema for table {table!r}; known: {sorted(SCHEMA)}"
        )
    return SCHEMA[table]


def _landing_path(table: str, data_date: date, data_root: str | os.PathLike) -> Path:
    return partition_path(table, data_date, data_root)


def _expand_to_partitions(df: pd.DataFrame, mask: pd.Series) -> pd.Series:
    """将失败行掩码扩展至完整的 ``(ticker, trade_date)`` 分组。"""
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
    """运行全部规则并判定批次去留（不抛异常）。

    规则级升级阈值先于全局安全阀判定；``flag`` 规则（如 K06–K08）升级时仅记录告警，
    SOFT 发现永不拒收批次。
    """
    config = batch_config or BatchRejectConfig()
    hard_results, soft_results = run_all_checks(
        df, data_date, asset_type, overrides=overrides, cross_df=cross_df
    )

    # 第 2 步：收集行级 HARD drop 行（partition 粒度扩展至整组）
    rows_to_drop = pd.Series(False, index=df.index)
    for r in hard_results:
        cfg = severity_config_for(r.rule_id)
        if cfg.row_action != "drop":
            continue
        mask = r.failing_rows
        if cfg.granularity == "partition":
            mask = _expand_to_partitions(df, mask)
        rows_to_drop = rows_to_drop | mask

    # 第 3a 步：单规则升级判定；partition 粒度按扩展后的实际丢弃比例计算
    reject_reason: str | None = None
    warnings: list[str] = []
    for r in hard_results + soft_results:
        cfg = severity_config_for(r.rule_id)
        thr = cfg.escalation_threshold
        if thr is None or not r.failing_count:
            continue
        if cfg.granularity == "partition":
            expanded = _expand_to_partitions(df, r.failing_rows)
            rate = float(expanded.sum()) / len(df) if len(df) else 0.0
        else:
            rate = r.failing_rate
        if rate <= thr:
            continue
        if cfg.row_action == "flag":
            logger.error(
                "DQC escalated alarm: %s failing rate %.4f > %.4f (%s %s); rows kept",
                r.rule_id,
                rate,
                thr,
                data_date,
                asset_type,
            )
            warnings.append(f"escalated_alarm:{r.rule_id}")
            continue
        reject_reason = f"rule_escalation:{r.rule_id}"
        break

    # 第 3b 步：全局安全阀判定
    drop_rate = (float(rows_to_drop.sum()) / len(df)) if len(df) else 0.0
    if reject_reason is None and drop_rate > config.max_hard_drop_rate:
        reject_reason = (
            f"global_valve:hard_drop_rate {drop_rate:.4f} > {config.max_hard_drop_rate}"
        )

    # 第 5 步：计算 SOFT 质量位掩码
    quality_flag = compute_quality_flag(df, soft_results)

    # 第 6 步：软告警判定（永不拒收）
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
    """将原始批次写入隔离区并抛出 :class:`DQCBatchReject`。"""
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
    """执行七步 DQC 入库流程并返回清洗后的 ``df_clean``。

    拒收时先写双报告表再移入隔离区并抛异常；接收时先按存储 Schema 投影校验
    并写入 Parquet（普通按日期覆盖，月度分区表执行 upsert），随后记录 DQC 报告。
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
        # 拒收前先写入报告，确保无数据落盘时仍有审计记录
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

    # 第 4–5 步：剔除问题行并挂载质量位掩码
    df_clean = df.loc[~rows_to_drop].copy()
    df_clean["quality_flag"] = evaluation.quality_flag[~rows_to_drop]

    # 第 7 步：Schema 校验并原子落盘
    schema = build_schema(table)
    projected = df_clean[[c for c in schema.columns if c in df_clean.columns]]
    validated = schema.validate(normalize_missing_floats(projected, table))
    path = (
        Path(target_path)
        if target_path is not None
        else _landing_path(table, data_date, data_root)
    )
    if target_path is None and is_monthly_partition(table):
        upsert_parquet_atomic(validated, path, table=table, partition_dates=[data_date])
    else:
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
