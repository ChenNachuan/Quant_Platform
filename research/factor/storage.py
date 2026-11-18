"""因子物理存储层：按频率、因子名与版本号分目录，按年/月分文件持久化原始因子值。

磁盘布局::

    data_platform/factors/{freq}/{factor_name}/v{version}/{partition}.parquet
    data_platform/factors/{freq}/{factor_name}/v{version}/metadata.json

契约约定：
- 仅读写 ``Factor.compute()`` 的原始输出（保留停牌/缺失 ``NaN``），不做后处理。
- 列结构由 ``data.storage_schema.factor_schema`` 唯一定义：固定前导键列 ``date``
  （``daily`` 为 ``DATE``/``date32``、``minute`` 为 ``timestamp[us]``，``tick`` 仅占位）与
  ``ticker``，后接 ``output_columns``（单列默认 ``{factor_name}``，类型为 ``DOUBLE``）。
- 分区粒度：``daily`` 按年（``{year}.parquet``），``minute`` 按月（``{YYYY-MM}.parquet``）。
- 语义版本目录独立共存，全量写入仅覆盖当前版本目录。
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence

import pandas as pd

from core.config import DATA_ROOT, get_data_root
from data.storage_ops import write_parquet_atomic
from data.storage_schema import FACTOR_FREQUENCY_CHOICES, PARTITION_PATHS, factor_schema
from research.factor.interface import resolved_output_columns

__all__ = [
    "load_factor",
    "load_factor_matrix",
    "save_factor_append",
    "save_factor_full",
    "write_metadata",
]

#: 默认因子存储根目录。
DEFAULT_BASE_DIR: str = str(DATA_ROOT / "factors")

#: Parquet 压缩编解码参数（通过 write_parquet_atomic 原子写入）。
COMPRESSION: str = "zstd"
COMPRESSION_LEVEL: int = 3

#: metadata.json 核心标识字段（按写入顺序；eval/attempts/attempt_log 等评估块在重写时保留）。
METADATA_FIELDS: tuple[str, ...] = (
    "factor_name",
    "version",
    "description",
    "category",
    "frequency",
    "output_columns",
    "asset_types",
    "created_at",
    "updated_at",
    "depends_on",
    "depends_hash",
    "data_sources",
    "data_hash",
    "date_range",
    "row_count",
    "file_count",
    "compression",
)

_KEY_COLS: tuple[str, ...] = ("date", "ticker")

_FACTOR_REL_PARENT = PurePosixPath(PARTITION_PATHS["factor"]).parent
if _FACTOR_REL_PARENT.parts[:1] != ("factors",):
    raise RuntimeError(
        f"unexpected PARTITION_PATHS['factor']: {PARTITION_PATHS['factor']!r}"
    )
# 派生自 storage_schema.PARTITION_PATHS["factor"] 的版本目录模板（"{freq}/{name}/v{version}"）
_FACTOR_DIR_TEMPLATE: str = str(PurePosixPath(*_FACTOR_REL_PARENT.parts[1:]))


# ---------------------------------------------------------------------------
# 路径与标量辅助函数
# ---------------------------------------------------------------------------


def _safe_component(value: str, what: str) -> str:
    """校验因子名或版本号，拒绝空值与路径分隔符/穿越字符。"""
    s = str(value)
    if not s or s in {".", ".."} or any(sep in s for sep in ("/", "\\")):
        raise ValueError(f"invalid {what} {value!r}")
    return s


def _resolve_base_dir(base_dir: str | Path | None) -> str:
    """解析因子根目录；未显式传入时使用当前上下文 ``get_data_root() / 'factors'``。"""
    if base_dir is not None:
        return str(base_dir)
    return str(get_data_root() / "factors")


def _validate_storage_frequency(frequency: str) -> str:
    """校验因子存储频率；``tick`` 仅作契约占位并抛出 ``NotImplementedError``。"""
    if frequency == "tick":
        raise NotImplementedError(
            "tick-frequency factor storage is reserved as a placeholder only"
        )
    if frequency not in FACTOR_FREQUENCY_CHOICES:
        raise ValueError(
            f"unsupported factor frequency {frequency!r}; expected one of {FACTOR_FREQUENCY_CHOICES}"
        )
    return frequency


def _version_dir(
    base_dir: str | Path | None,
    name: str,
    version: str,
    frequency: str = "daily",
) -> Path:
    """返回因子版本目录路径 ``{base_dir}/{freq}/{name}/v{version}``。"""
    freq = _validate_storage_frequency(frequency)
    rel = _FACTOR_DIR_TEMPLATE.format(
        freq=_safe_component(freq, "frequency"),
        name=_safe_component(name, "factor name"),
        version=_safe_component(version, "version"),
    )
    return Path(_resolve_base_dir(base_dir)) / rel


def _to_bound(value: Any, *, is_end: bool, frequency: str) -> date | pd.Timestamp:
    """将输入日期/时间标量规范化；``minute`` 模式下纯日期终点扩为当日 ``23:59:59.999999``。"""
    ts = pd.to_datetime(
        pd.Series([value], dtype=object), format="mixed", errors="raise"
    ).iloc[0]
    if pd.isna(ts):
        raise ValueError(f"date bound must not be missing: {value!r}")
    if frequency == "daily":
        return ts.date()
    if is_end and (
        (isinstance(value, str) and len(value.strip()) <= 10)
        or (isinstance(value, date) and not isinstance(value, datetime))
    ):
        return ts.normalize() + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return ts


def _depends_hash(depends_on: Iterable[str]) -> str:
    """计算 ``depends_on()`` 声明列表的 12 位 MD5 指纹（用于 metadata 标识，不同于运行时数据内容指纹）。"""
    raw = "|".join(sorted(str(d) for d in depends_on))
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()[:12]


def _compute_data_hash(df: pd.DataFrame, *, frequency: str = "daily") -> str:
    """计算因子数据内容的确定性 SHA-256 摘要（按 ``(date, ticker)`` 排序）。"""
    if df.empty:
        return hashlib.sha256(b"").hexdigest()
    work = df.copy()
    if "date" in work.columns:
        parsed_dt = pd.to_datetime(work["date"], format="mixed")
        if frequency == "minute":
            fmt = (
                "%Y-%m-%d %H:%M:%S.%f"
                if (parsed_dt.dt.microsecond != 0).any()
                else "%Y-%m-%d %H:%M:%S"
            )
        else:
            fmt = "%Y-%m-%d"
        work["date"] = parsed_dt.dt.strftime(fmt)
    sort_cols = [c for c in _KEY_COLS if c in work.columns]
    if sort_cols:
        work = work.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    payload = work.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _resolve_data_sources(
    factor: Any,
    result: pd.DataFrame,
    depends_on: list[str],
    explicit: Iterable[str] | None,
) -> list[str]:
    """解析写入因子元数据的数据源血缘列表。"""
    if explicit is not None:
        return [str(s) for s in explicit]
    attrs = getattr(result, "attrs", None)
    if isinstance(attrs, dict) and attrs.get("data_sources") is not None:
        return [str(s) for s in attrs["data_sources"]]
    factor_sources = getattr(factor, "data_sources", None)
    if factor_sources is not None:
        return [str(s) for s in factor_sources]
    return [str(d) for d in depends_on if not str(d).startswith("factor:")]


def _partition_keys(dates: pd.Series, frequency: str) -> pd.Series:
    """按频率生成分区文件 stem（``daily`` 按年 ``YYYY``，``minute`` 按月 ``YYYY-MM``）。"""
    if frequency == "daily":
        return dates.dt.year.astype(str)
    if frequency == "minute":
        return dates.dt.strftime("%Y-%m")
    raise NotImplementedError(f"partitioning not implemented for {frequency!r}")


def _write_partition(
    chunk: pd.DataFrame,
    path: Path,
    value_columns: Sequence[str],
    frequency: str,
) -> None:
    """通过 ``data.storage_ops.write_parquet_atomic`` 原子写入单分区 Parquet 文件。"""
    parquet_kwargs: dict[str, Any] = {
        "compression": COMPRESSION,
        "compression_level": COMPRESSION_LEVEL,
    }
    if frequency == "minute":
        parquet_kwargs["coerce_timestamps"] = "us"
        parquet_kwargs["allow_truncated_timestamps"] = True
    write_parquet_atomic(
        chunk,
        path,
        table="factor",
        value_column=tuple(value_columns),
        frequency=frequency,
        **parquet_kwargs,
    )


# ---------------------------------------------------------------------------
# write_metadata
# ---------------------------------------------------------------------------


def write_metadata(
    out_dir,
    factor,
    result,
    *,
    data_sources: Iterable[str] | None = None,
    data_hash: str | None = None,
) -> None:
    """写入描述因子版本目录的 ``metadata.json``。

    ``date_range`` / ``row_count`` / ``file_count`` / ``data_hash`` 优先统计目录内现存 Parquet 文件，
    尚无文件时回退至 ``result``；跨次重写自动保留 ``created_at`` 与 ``eval`` /
    ``attempts`` / ``attempt_log`` 等评估块。
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    meta_path = out_path / "metadata.json"

    previous: dict[str, Any] = {}
    if meta_path.exists():
        try:
            previous = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            previous = {}

    freq = str(getattr(factor, "frequency", "daily") or "daily")
    output_cols = list(resolved_output_columns(factor))

    files = sorted(out_path.glob("*.parquet"))
    if files:
        stored = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
        dates = stored["date"]
        row_count = int(len(dates))
        date_range: list[str] | None = [str(dates.min()), str(dates.max())]
        hash_source = stored
    else:
        row_count = int(len(result))
        if row_count:
            rdates = pd.to_datetime(result["date"], format="mixed")
            if freq == "minute":
                date_range = [
                    rdates.min().isoformat(sep=" "),
                    rdates.max().isoformat(sep=" "),
                ]
            else:
                date_range = [str(rdates.min().date()), str(rdates.max().date())]
        else:
            date_range = None
        hash_source = result

    depends_on = list(factor.depends_on())
    resolved_sources = _resolve_data_sources(factor, result, depends_on, data_sources)
    if data_hash is not None:
        resolved_hash = str(data_hash)
    else:
        attrs = getattr(result, "attrs", None)
        attr_hash = attrs.get("data_hash") if isinstance(attrs, dict) else None
        resolved_hash = (
            str(attr_hash)
            if attr_hash is not None
            else str(
                getattr(factor, "data_hash", None)
                or _compute_data_hash(hash_source, frequency=freq)
            )
        )

    now = datetime.now(timezone.utc).isoformat()

    meta: dict[str, Any] = {
        "factor_name": factor.name,
        "version": factor.version,
        "description": getattr(factor, "description", ""),
        "category": getattr(factor, "category", ""),
        "frequency": freq,
        "output_columns": output_cols,
        "asset_types": list(getattr(factor, "asset_types", []) or []),
        "created_at": previous.get("created_at", now),
        "updated_at": now,
        "depends_on": depends_on,
        "depends_hash": _depends_hash(depends_on),
        "data_sources": resolved_sources,
        "data_hash": resolved_hash,
        "date_range": date_range,
        "row_count": row_count,
        "file_count": len(files),
        "compression": COMPRESSION,
    }
    # 保留既有的评估记录块，避免日常追加写入覆盖注册评估信息
    for extra in ("eval", "attempts", "attempt_log"):
        if extra in previous:
            meta[extra] = previous[extra]
    meta_path.write_text(
        json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# 保存写入
# ---------------------------------------------------------------------------


def _normalise(
    result: pd.DataFrame,
    value_columns: Sequence[str],
    frequency: str = "daily",
) -> pd.DataFrame:
    """校验并规范化输出表：检查列名、解析时间列为 ``datetime64``、按 ``(date, ticker)`` 去重排序并校验 schema（不修改原 DataFrame）。"""
    cols = tuple(value_columns)
    schema = factor_schema(cols, frequency=frequency)
    expected = list(schema.columns)
    if set(result.columns) != set(expected):
        raise ValueError(
            f"compute() must return columns {expected}, got {list(result.columns)}"
        )

    out = result.copy()
    out["date"] = pd.to_datetime(out["date"], format="mixed", errors="raise")
    out["ticker"] = out["ticker"].astype(str)
    for col in cols:
        out[col] = out[col].astype("float64")
    out = (
        out.drop_duplicates(["date", "ticker"])
        .sort_values(["date", "ticker"], kind="mergesort")
        .reset_index(drop=True)
    )

    schema.validate(out)  # 仅做准入校验，返回帧保留 datetime64 以供按分区拆分
    return out


def save_factor_append(
    factor,
    result,
    base_dir: str | Path | None = None,
    *,
    data_sources: Iterable[str] | None = None,
    data_hash: str | None = None,
) -> None:
    """增量追加写入因子当前版本目录。

    先校验 ``result`` 再建目录；按频率分区拆分并与盘上既有文件合并，按 ``(date, ticker)``
    去重并保留已存旧值（数据修正须走 ``save_factor_full``），最后更新 ``metadata.json``。
    """
    freq = _validate_storage_frequency(getattr(factor, "frequency", "daily"))
    cols = resolved_output_columns(factor)
    data = _normalise(result, cols, frequency=freq)
    out_dir = _version_dir(base_dir, factor.name, factor.version, frequency=freq)
    out_dir.mkdir(parents=True, exist_ok=True)

    if len(data):
        data = data.assign(_part=_partition_keys(data["date"], freq))
        for part, chunk in data.groupby("_part", sort=True):
            path = out_dir / f"{part}.parquet"
            chunk = chunk.drop(columns="_part")
            if path.exists():
                stored = pd.read_parquet(path)
                stored["date"] = pd.to_datetime(stored["date"], format="mixed")
                chunk = pd.concat([stored, chunk], ignore_index=True)
            chunk = (
                chunk.drop_duplicates(["date", "ticker"])
                .sort_values(["date", "ticker"], kind="mergesort")
                .reset_index(drop=True)
            )
            _write_partition(chunk, path, cols, freq)
        data = data.drop(columns=["_part"])

    write_metadata(
        out_dir,
        factor,
        data,
        data_sources=data_sources,
        data_hash=data_hash,
    )


def save_factor_full(
    factor,
    result,
    base_dir: str | Path | None = None,
    *,
    data_sources: Iterable[str] | None = None,
    data_hash: str | None = None,
) -> None:
    """全量覆盖写入当前版本目录（不影响其他版本目录）。

    先校验 ``result`` 再建目录，全部新分区文件原子写入完成后再删除过期分区文件，
    防止中途异常导致目录残留不完整历史。
    """
    freq = _validate_storage_frequency(getattr(factor, "frequency", "daily"))
    cols = resolved_output_columns(factor)
    data = _normalise(result, cols, frequency=freq)
    out_dir = _version_dir(base_dir, factor.name, factor.version, frequency=freq)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: set[Path] = set()
    if len(data):
        data = data.assign(_part=_partition_keys(data["date"], freq))
        for part, chunk in data.groupby("_part", sort=True):
            chunk = chunk.drop(columns="_part")
            path = out_dir / f"{part}.parquet"
            _write_partition(chunk, path, cols, freq)
            written.add(path)
        data = data.drop(columns=["_part"])

    # 所有新文件落盘后再清理旧分区文件，确保异常中断不破坏完整历史
    for stale in sorted(out_dir.glob("*.parquet")):
        if stale not in written:
            stale.unlink()

    write_metadata(
        out_dir,
        factor,
        data,
        data_sources=data_sources,
        data_hash=data_hash,
    )


# ---------------------------------------------------------------------------
# 读取加载
# ---------------------------------------------------------------------------


def _iter_months(start_d: date, end_d: date) -> Iterable[str]:
    y, m = start_d.year, start_d.month
    end_ym = (end_d.year, end_d.month)
    while (y, m) <= end_ym:
        yield f"{y:04d}-{m:02d}"
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def _empty_factor_columns(
    out_dir: Path, name: str, explicit: list[str] | None
) -> list[str]:
    if explicit is not None:
        return ["date", "ticker", *explicit]
    meta_path = out_dir / "metadata.json"
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            cols = meta.get("output_columns")
            if isinstance(cols, list) and cols:
                return ["date", "ticker", *[str(c) for c in cols]]
        except (OSError, json.JSONDecodeError):
            pass
    return ["date", "ticker", name]


def load_factor(
    name,
    start,
    end,
    version="1.0",
    base_dir: str | Path | None = None,
    frequency: str = "daily",
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    """按 ``[start, end]``（含端点）读取单因子原始值。

    ``daily`` 读取年分区并返回 ``datetime.date``；``minute`` 读取月分区并返回时间戳；
    ``tick`` 抛出 ``NotImplementedError``；版本目录不存在时抛出 ``FileNotFoundError``。
    """
    freq = _validate_storage_frequency(frequency)
    explicit_cols: list[str] | None = None
    if columns is not None:
        if isinstance(columns, str):
            explicit_cols = [columns]
        else:
            explicit_cols = list(columns)
        if not explicit_cols or any(not c or c in _KEY_COLS for c in explicit_cols):
            raise ValueError(f"invalid requested columns {columns!r}")
        if len(set(explicit_cols)) != len(explicit_cols):
            raise ValueError(f"duplicate requested columns {columns!r}")

    out_dir = _version_dir(base_dir, name, version, frequency=freq)
    if not out_dir.is_dir():
        raise FileNotFoundError(f"factor version directory not found: {out_dir}")

    start_b = _to_bound(start, is_end=False, frequency=freq)
    end_b = _to_bound(end, is_end=True, frequency=freq)

    if freq == "daily":
        part_names = (
            [str(y) for y in range(start_b.year, end_b.year + 1)]
            if start_b <= end_b
            else []
        )
    else:
        part_names = (
            list(_iter_months(start_b.date(), end_b.date())) if start_b <= end_b else []
        )

    frames: list[pd.DataFrame] = []
    for part in part_names:
        path = out_dir / f"{part}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))

    if not frames:
        return pd.DataFrame(columns=_empty_factor_columns(out_dir, name, explicit_cols))

    df = pd.concat(frames, ignore_index=True)
    if explicit_cols is not None:
        missing = [c for c in explicit_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"factor {name!r} does not contain requested column(s) {missing}"
            )
        value_cols = explicit_cols
    else:
        value_cols = [c for c in df.columns if c not in _KEY_COLS]
    target_cols = ["date", "ticker", *value_cols]

    parsed = pd.to_datetime(df["date"], format="mixed")
    df = df.loc[
        (parsed >= pd.Timestamp(start_b)) & (parsed <= pd.Timestamp(end_b)),
        target_cols,
    ]
    return df.sort_values(list(_KEY_COLS), kind="mergesort").reset_index(drop=True)


def load_factor_matrix(
    names: list[str],
    start,
    end,
    version="1.0",
    base_dir: str | Path | None = None,
    frequency: str = "daily",
) -> pd.DataFrame:
    """读取多因子并按 ``(date, ticker)`` 外连接为宽表 ``(date, ticker, f1, f2, ...)``，缺失值保持 ``NaN``。"""
    if not names:
        raise ValueError("names must be a non-empty list of factor names")

    frames = [
        load_factor(n, start, end, version, base_dir, frequency=frequency).set_index(
            list(_KEY_COLS)
        )
        for n in names
    ]
    wide = pd.concat(frames, axis=1, join="outer").reset_index()
    return wide.sort_values(list(_KEY_COLS), kind="mergesort").reset_index(drop=True)
