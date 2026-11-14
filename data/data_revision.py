"""数据修订检测、回写与下游影响传播。

由于上游源站会静默覆盖历史且不标记修订，本模块按三阶段处理：
- **检测**（:func:`detect_corrections`）：重拉近 *lookback* 个交易日并与资产层逐行比对；
- **回写**（:func:`apply_corrections`）：先追加审计日志，再通过完整 ``daily_ingest`` 流程覆盖写入（不保留历史版本）；
- **传播**（:func:`assess_correction_impact`、:func:`mark_factors_stale`）：分级记录影响并标记关联因子过期，绝不自动重算因子或回测。

``data.ingestion.daily_ingest`` 在调用时延迟导入，确保本模块独立可测；缺失时抛出明确 ``RuntimeError``。
"""

from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence
from core.config import DATA_ROOT

__all__ = [
    "CORRECTION_TOLERANCES",
    "CORRECTIONS_ROOT",
    "IMPACT_LOG_NAME",
    "DEFAULT_LOOKBACK",
    "QUARTERLY_LOOKBACK",
    "MEDIUM_REL_DIFF",
    "TOTAL_CHANGE_REL_DIFF",
    "ALERT_SEVERITIES",
    "SEVERITY_LOW",
    "SEVERITY_MEDIUM",
    "SEVERITY_HIGH",
    "STORAGE_TO_SOURCE_ASSET",
    "FieldChange",
    "CorrectionRecord",
    "detect_corrections",
    "compare_frames",
    "apply_corrections",
    "assess_correction_impact",
    "mark_factors_stale",
    "correction_log_path",
    "impact_log_path",
]

logger = logging.getLogger(__name__)

# 各字段对比容差：不超过阈值视为上游浮点/单位微调，不触发修订记录
CORRECTION_TOLERANCES: dict[str, float] = {
    "close": 0.01,
    "volume": 100,
    "amount": 1000,
    "adjfactor": 1e-6,
}

CORRECTIONS_ROOT = "corrections"
IMPACT_LOG_NAME = "impact_log.jsonl"

# 周度常规扫描回溯交易日数
DEFAULT_LOOKBACK = 30
# 季度全量扫描回溯交易日数（兜底长尾历史修订）
QUARTERLY_LOOKBACK = 250

# 单边缺失（新增或被移除）记录的相对变化率，确保为有限 JSON 数值且触发 MEDIUM 以上级别
TOTAL_CHANGE_REL_DIFF = 1.0

# 判定为 MEDIUM 级别的相对差异阈值
MEDIUM_REL_DIFF = 0.01

SEVERITY_LOW = "LOW"
SEVERITY_MEDIUM = "MEDIUM"
SEVERITY_HIGH = "HIGH"

# 触发告警日志的最低严重级别集合（过滤 LOW 级噪音）
ALERT_SEVERITIES = frozenset({SEVERITY_MEDIUM, SEVERITY_HIGH})

# 存储表名到适配器/标准化器源域资产类型的映射（仅覆盖已接入的统一日线表）
STORAGE_TO_SOURCE_ASSET: dict[str, str] = {
    "bars_daily": "stock",
}


# --- 修订记录数据结构 ---


@dataclass(frozen=True)
class FieldChange:
    """单个标的单个字段超出容差的变更记录（含定位用的 ``asset_type`` 与 ``trade_date``）。"""

    ticker: str
    field: str
    old_value: float
    new_value: float
    abs_diff: float
    rel_diff: float
    asset_type: str = ""
    trade_date: date | None = None


@dataclass(frozen=True)
class CorrectionRecord:
    """单个 ``(asset_type, trade_date)`` 的聚合修订记录，按行追加写入 ``corrections/{asset_type}/{date}.jsonl``。"""

    asset_type: str
    trade_date: date
    detected_at: datetime
    changes: tuple[FieldChange, ...] = ()


# --- 路径辅助 ---


def correction_log_path(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """返回修订审计日志路径：``{data_root}/corrections/{asset_type}/{date}.jsonl``。"""
    date_str = trade_date.strftime("%Y-%m-%d")
    return Path(data_root) / CORRECTIONS_ROOT / asset_type / f"{date_str}.jsonl"


def impact_log_path(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """返回影响日志路径：``{data_root}/corrections/impact_log.jsonl``。"""
    return Path(data_root) / CORRECTIONS_ROOT / IMPACT_LOG_NAME


# --- 修订检测 ---


def _now() -> datetime:
    """时钟接缝，便于测试注入固定时间。"""
    return datetime.now()


def _source_asset_type(asset_type: str) -> str:
    """将存储表名转换到适配器/标准化器认可的源域资产类型。"""
    try:
        return STORAGE_TO_SOURCE_ASSET[asset_type]
    except KeyError:
        raise ValueError(
            f"no source asset type for storage table {asset_type!r} "
            f"(mapped: {sorted(STORAGE_TO_SOURCE_ASSET)}); add its entry to "
            "STORAGE_TO_SOURCE_ASSET before scanning it"
        ) from None


def _fetch_normalized(asset_type: str, trade_date: date) -> Any:
    """从主数据源重拉单日数据并经过标准化器处理（I/O 测试接缝）。"""
    from data.adapters import get_adapter
    from data.ingestion import get_primary_source
    from data.normalizers import get_normalizer

    source = get_primary_source(asset_type)  # 存储域主数据源
    source_asset_type = _source_asset_type(asset_type)  # 适配器域资产类型
    adapter = get_adapter(source)
    try:
        result = adapter.fetch(source_asset_type, trade_date)
        # get_normalizer() 直接返回 normalize 可调用对象
        return get_normalizer(source)(source_asset_type, result.df)
    finally:
        adapter.close()


def _load_local(
    asset_type: str,
    trade_date: date,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Any:
    """读取资产层指定交易日的既有本地批次。"""
    import pandas as pd

    from data.storage_ops import is_monthly_partition, partition_path

    try:
        path = partition_path(asset_type, trade_date, data_root)
    except NotImplementedError as exc:
        raise NotImplementedError(
            f"no date-partitioned layout for {asset_type!r}"
        ) from exc

    if not path.exists():
        legacy_path = (
            Path(data_root)
            / f"asset/{asset_type}/{trade_date.strftime('%Y-%m-%d')}.parquet"
        )
        if legacy_path.exists():
            path = legacy_path
        else:
            return None
    df = pd.read_parquet(path)
    if is_monthly_partition(asset_type) and "trade_date" in df.columns:
        td = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df = df[td == trade_date].reset_index(drop=True)
        if df.empty:
            return None
    return df


def _no_rows(frame: Any) -> bool:
    """判断单边结果是否为空（``None`` 或 0 行），空拉取视为无数据而非全量删除。"""
    return frame is None or len(frame) == 0


def _post_dqc_baseline(
    df: Any, asset_type: str, trade_date: date, data_root: str | os.PathLike[str]
) -> Any:
    """对新拉取数据运行只读 DQC 评估并剔除会被拦截的行，使新旧基线口径一致（不写入数据湖）。"""
    from data.dqc.engine import evaluate_batch
    from data.dqc.report import build_reference_overrides

    evaluation = evaluate_batch(
        df,
        trade_date,
        asset_type,
        overrides=build_reference_overrides(data_root, asset_type, trade_date),
    )
    if evaluation.batch_status == "rejected":
        return None
    return df.loc[~evaluation.rows_to_drop]


def _foreign_fields(asset_type: str) -> set[str]:
    """获取由其他数据源覆盖接管的字段集合，比对前剔除以避免误报。"""
    from data.ingestion import get_field_overrides, get_primary_source

    primary = get_primary_source(asset_type)
    return {
        field
        for field, source in get_field_overrides(asset_type).items()
        if source != primary
    }


def _rel_diff(old: float, new: float) -> float:
    """计算相对差异 ``abs_diff / |old|``，分母设下限防零除。"""
    return abs(new - old) / max(abs(old), 1e-9)


def _is_missing(value: Any) -> bool:
    """判断值是否为 NaN / None / NaT。"""
    import pandas as pd

    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _jsonable(value: float) -> float | None:
    """将 NaN / Inf 转为 ``None`` 以保证 JSONL 严格合法。"""
    return value if math.isfinite(value) else None


def compare_frames(
    df_old: Any,
    df_new: Any,
    asset_type: str,
    trade_date: date,
) -> list[FieldChange]:
    """按 :data:`CORRECTION_TOLERANCES` 逐行比对两份标准化 DataFrame。

    采用外连接（outer merge）捕获单边新增/缺失与 ``NaN <-> 数值`` 填充/清空（记为 :data:`TOTAL_CHANGE_REL_DIFF`）；
    仅比对容差表内共有字段，且合并前拒绝重复 ``(ticker, trade_date)`` 主键。
    """
    import pandas as pd

    old = pd.DataFrame(df_old)
    new = pd.DataFrame(df_new)
    if old.empty and new.empty:
        return []

    key = ["ticker", "trade_date"]
    if any(c not in old.columns or c not in new.columns for c in key):
        raise ValueError(f"frames must carry {key} to be compared")
    # 拒绝重复主键，防止外连接产生笛卡尔积并重复记录 FieldChange
    if old.duplicated(key).any() or new.duplicated(key).any():
        raise ValueError(f"duplicate join keys {key} in input frames")

    # 显式命名 indicator 列为 merge_state（默认 _merge 会被 itertuples 重命名）
    merged = old.merge(
        new, on=key, suffixes=("_old", "_new"), how="outer", indicator="merge_state"
    )
    changes: list[FieldChange] = []

    for field_name, tol in CORRECTION_TOLERANCES.items():
        old_col, new_col = f"{field_name}_old", f"{field_name}_new"
        if old_col not in merged.columns or new_col not in merged.columns:
            continue
        for row in merged.itertuples(index=False):
            old_raw = getattr(row, old_col)
            new_raw = getattr(row, new_col)
            old_missing = _is_missing(old_raw)
            new_missing = _is_missing(new_raw)
            if old_missing and new_missing:
                continue

            if old_missing or new_missing:
                # 单边缺失：回填、删除或空值填充/清空
                present = new_raw if old_missing else old_raw
                changes.append(
                    FieldChange(
                        ticker=str(row.ticker),
                        field=field_name,
                        old_value=float("nan") if old_missing else float(old_raw),
                        new_value=float("nan") if new_missing else float(new_raw),
                        abs_diff=abs(float(present)),
                        rel_diff=TOTAL_CHANGE_REL_DIFF,
                        asset_type=asset_type,
                        trade_date=trade_date,
                    )
                )
                continue

            old_value = float(old_raw)
            new_value = float(new_raw)
            abs_diff = abs(new_value - old_value)
            if abs_diff <= tol:
                continue
            changes.append(
                FieldChange(
                    ticker=str(row.ticker),
                    field=field_name,
                    old_value=old_value,
                    new_value=new_value,
                    abs_diff=abs_diff,
                    rel_diff=_rel_diff(old_value, new_value),
                    asset_type=asset_type,
                    trade_date=trade_date,
                )
            )
    return changes


def detect_corrections(
    asset_type: str,
    lookback: int = 30,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> list[FieldChange]:
    """重拉最近 *lookback* 个交易日并与本地资产层（post-DQC 同口径）比对，跳过任一侧无数据的日期。"""
    from data.calendar_adjustment import TradingCalendar

    cal = TradingCalendar("sse", data_root=data_root)
    end = cal.prev(date.today())
    start = cal.prev(end, lookback)
    drop_fields = _foreign_fields(asset_type)
    changes: list[FieldChange] = []
    for trade_date in cal.range(start, end):
        fresh = _fetch_normalized(asset_type, trade_date)
        local = _load_local(asset_type, trade_date, data_root)
        if _no_rows(fresh) or _no_rows(local):
            continue
        fresh = _post_dqc_baseline(fresh, asset_type, trade_date, data_root)
        if _no_rows(fresh):
            continue
        changes.extend(
            compare_frames(
                local.drop(columns=drop_fields, errors="ignore"),
                fresh.drop(columns=drop_fields, errors="ignore"),
                asset_type,
                trade_date,
            )
        )
    return changes


# --- 修订回写（先写日志，再走完整入库覆盖） ---


def _group_changes(
    changes: Sequence[FieldChange], detected_at: datetime
) -> list[CorrectionRecord]:
    """按 ``(asset_type, trade_date)`` 分组聚合变更记录并保持原有顺序。"""
    buckets: dict[tuple[str, date], list[FieldChange]] = {}
    for change in changes:
        if not change.asset_type or change.trade_date is None:
            raise ValueError(
                f"FieldChange needs asset_type and trade_date to be applied: {change!r}"
            )
        buckets.setdefault((change.asset_type, change.trade_date), []).append(change)
    return [
        CorrectionRecord(
            asset_type=key[0],
            trade_date=key[1],
            detected_at=detected_at,
            changes=tuple(items),
        )
        for key, items in buckets.items()
    ]


def _append_correction_log(
    record: CorrectionRecord, data_root: str | os.PathLike[str]
) -> Path:
    """逐条追加写入修订 JSONL 日志（目录不存在时自动创建）。"""
    path = correction_log_path(record.asset_type, record.trade_date, data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for change in record.changes:
            fh.write(
                json.dumps(
                    {
                        "detected_at": record.detected_at.isoformat(),
                        "asset_type": record.asset_type,
                        "trade_date": str(record.trade_date),
                        "ticker": change.ticker,
                        "field": change.field,
                        "old": _jsonable(change.old_value),
                        "new": _jsonable(change.new_value),
                        "abs_diff": _jsonable(change.abs_diff),
                        "rel_diff": _jsonable(change.rel_diff),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    return path


def _daily_ingest(source: str, asset_type: str, trade_date: date) -> None:
    """延迟导入并调用完整入库流水线覆盖写入；不可用时抛 ``RuntimeError``（禁止绕过 DQC 直接写 Parquet）。"""
    try:
        from data.ingestion import daily_ingest
    except ImportError as exc:
        raise RuntimeError(
            "data.ingestion.daily_ingest is not available on this tree; "
            "apply_corrections must overwrite through the full ingest pipeline "
            "and will not write the asset layer directly. Land the ingest "
            "pipeline (QUANT-40 daily_ingest) before applying corrections."
        ) from exc

    daily_ingest(source=source, asset_type=asset_type, trade_date=trade_date)


def apply_corrections(
    changes: Sequence[FieldChange],
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> list[CorrectionRecord]:
    """分组记录修订并先写审计日志、再调用 ``daily_ingest`` 覆盖资产层（保证崩溃时日志先于落盘）。"""
    from data.ingestion import get_primary_source

    records = _group_changes(list(changes), _now())
    applied: list[CorrectionRecord] = []
    for record in records:
        _append_correction_log(record, data_root)  # 1. 先记录审计日志
        _daily_ingest(
            source=get_primary_source(record.asset_type),
            asset_type=record.asset_type,
            trade_date=record.trade_date,
        )  # 2. 再覆盖写入
        logger.warning(
            "Correction applied: %s %s, %d field(s) changed",
            record.asset_type,
            record.trade_date,
            len(record.changes),
        )
        applied.append(record)
    return applied


# --- 影响传播（分级、记账、标脏，严禁自动重算） ---


def _classify(record: CorrectionRecord) -> tuple[str, str, dict[str, Any]]:
    """计算修订严重级别、原因说明及影响负载字典。"""
    changes = list(record.changes)
    max_rel_diff = max((c.rel_diff for c in changes), default=0.0)
    impact: dict[str, Any] = {
        "trade_date": str(record.trade_date),
        "asset_type": record.asset_type,
        "num_changes": len(changes),
        "max_rel_diff": max_rel_diff,
        "affected_tickers": sorted({c.ticker for c in changes}),
    }

    if any(c.field == "adjfactor" for c in changes):
        return (
            SEVERITY_HIGH,
            "adjfactor changed, all price-based factors affected",
            impact,
        )
    if max_rel_diff > MEDIUM_REL_DIFF:
        return (
            SEVERITY_MEDIUM,
            f"price change > 1% for {len(changes)} field(s)",
            impact,
        )
    return SEVERITY_LOW, "minor correction, likely negligible", impact


def assess_correction_impact(
    record: CorrectionRecord,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> str:
    """对修订记录分级并追加 ``impact_log.jsonl``（``adjfactor`` 变更=HIGH，相对差异>1%=MEDIUM，其余=LOW），MEDIUM 及以上发告警。"""
    severity, reason, impact = _classify(record)
    impact["severity"] = severity
    impact["reason"] = reason

    path = impact_log_path(data_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(impact, ensure_ascii=False) + "\n")

    if severity in ALERT_SEVERITIES:
        logger.warning("Correction impact %s: %s", severity, reason)
    else:
        logger.info("Correction impact %s: %s", severity, reason)
    return severity


def mark_factors_stale(
    record: CorrectionRecord,
    *,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> list[str]:
    """将依赖受影响表的因子在其 ``metadata.json`` 中写入最早未重算 ``stale_since`` 日期（只标脏，不触发重算）。"""
    factors_dir = Path(data_root) / "factors"
    if not factors_dir.is_dir():
        return []

    touched = {record.asset_type}
    affected: list[str] = []
    for meta_path in sorted(factors_dir.glob("*/v*/metadata.json")):
        try:
            meta: dict[str, Any] = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            logger.warning("unreadable factor metadata: %s", meta_path)
            continue

        depends_on = set(meta.get("depends_on") or [])
        if not (depends_on & touched):
            continue

        name = str(meta.get("factor_name") or meta_path.parent.parent.name)
        stale = str(record.trade_date)
        prior = meta.get("stale_since")
        # 保留最早的待重算过期日期，防止后续修订覆盖更早的脏起点
        meta["stale_since"] = (
            min(prior, stale) if isinstance(prior, str) and prior else stale
        )
        meta_path.write_text(
            json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        affected.append(name)
        logger.info("Factor %r marked stale since %s", name, record.trade_date)
    return affected
