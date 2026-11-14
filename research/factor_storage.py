"""因子物理存储层：按因子与版本号分目录、按年份分文件持久化原始因子值。

磁盘布局::

    data_platform/factors/{factor_name}/v{version}/{year}.parquet
    data_platform/factors/{factor_name}/v{version}/metadata.json

契约约定：
- 仅读写 ``Factor.compute()`` 的原始输出（保留停牌/缺失 ``NaN``），不做后处理。
- 严格三列结构由 ``data.storage_schema.factor_schema`` 定义：``date``（Parquet
  ``DATE``/``date32``，内存为 ``datetime.date``，可直接与 ``load_bars`` 连接）、
  ``ticker``（带交易所后缀字符串）、``{factor}``（``DOUBLE``）。
- 语义版本目录独立共存，全量写入仅覆盖当前版本目录。
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from data.storage_ops import write_parquet_atomic
from data.storage_schema import factor_schema
from core.config import DATA_ROOT

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
    "asset_types",
    "created_at",
    "updated_at",
    "depends_on",
    "depends_hash",
    "date_range",
    "row_count",
    "file_count",
    "compression",
)

_KEY_COLS: tuple[str, ...] = ("date", "ticker")


# ---------------------------------------------------------------------------
# 路径与标量辅助函数
# ---------------------------------------------------------------------------


def _safe_component(value: str, what: str) -> str:
    """校验因子名或版本号，拒绝空值与路径分隔符/穿越字符。"""
    s = str(value)
    if not s or s in {".", ".."} or any(sep in s for sep in ("/", "\\")):
        raise ValueError(f"invalid {what} {value!r}")
    return s


def _version_dir(base_dir: str, name: str, version: str) -> Path:
    """返回因子版本目录路径 ``{base_dir}/{name}/v{version}``。"""
    return (
        Path(base_dir)
        / _safe_component(name, "factor name")
        / f"v{_safe_component(version, 'version')}"
    )


def _to_date(value: Any) -> date:
    """将输入日期标量规范化为 ``datetime.date``，缺失或无法解析时抛出 ``ValueError``。"""
    ts = pd.to_datetime(
        pd.Series([value], dtype=object), format="mixed", errors="raise"
    ).iloc[0]
    if pd.isna(ts):
        raise ValueError(f"date bound must not be missing: {value!r}")
    return ts.date()


def _depends_hash(depends_on: Iterable[str]) -> str:
    """计算 ``depends_on()`` 声明列表的 12 位 MD5 指纹（用于 metadata 标识，不同于运行时数据内容指纹）。"""
    raw = "|".join(sorted(str(d) for d in depends_on))
    return hashlib.md5(raw.encode(), usedforsecurity=False).hexdigest()[:12]


def _write_year(chunk: pd.DataFrame, path: Path, factor_name: str) -> None:
    """通过 ``data.storage_ops.write_parquet_atomic`` 原子写入单年 Parquet 文件。"""
    write_parquet_atomic(
        chunk,
        path,
        table="factor",
        value_column=factor_name,
        compression=COMPRESSION,
        compression_level=COMPRESSION_LEVEL,
    )


# ---------------------------------------------------------------------------
# write_metadata
# ---------------------------------------------------------------------------


def write_metadata(out_dir, factor, result) -> None:
    """写入描述因子版本目录的 ``metadata.json``。

    ``date_range`` / ``row_count`` / ``file_count`` 优先统计目录内现存 Parquet 文件，
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

    files = sorted(out_path.glob("*.parquet"))
    if files:
        dates = pd.concat(
            [pd.read_parquet(p, columns=["date"]) for p in files], ignore_index=True
        )["date"]
        row_count = int(len(dates))
        date_range: list[str] | None = [str(dates.min()), str(dates.max())]
    else:
        row_count = int(len(result))
        if row_count:
            rdates = pd.to_datetime(result["date"], format="mixed")
            date_range = [str(rdates.min().date()), str(rdates.max().date())]
        else:
            date_range = None

    depends_on = list(factor.depends_on())
    now = datetime.now(timezone.utc).isoformat()

    meta: dict[str, Any] = {
        "factor_name": factor.name,
        "version": factor.version,
        "description": getattr(factor, "description", ""),
        "category": getattr(factor, "category", ""),
        "frequency": getattr(factor, "frequency", ""),
        "asset_types": list(getattr(factor, "asset_types", []) or []),
        "created_at": previous.get("created_at", now),
        "updated_at": now,
        "depends_on": depends_on,
        "depends_hash": _depends_hash(depends_on),
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


def _normalise(result: pd.DataFrame, factor_name: str) -> pd.DataFrame:
    """校验并规范化输出表：检查列名、解析日期为 ``datetime64``、按 ``(date, ticker)`` 去重排序并校验 schema（不修改原 DataFrame）。"""
    schema = factor_schema(factor_name)
    expected = list(schema.columns)
    if set(result.columns) != set(expected):
        raise ValueError(
            f"compute() must return columns {expected}, got {list(result.columns)}"
        )

    out = result.copy()
    out["date"] = pd.to_datetime(out["date"], format="mixed", errors="raise")
    out["ticker"] = out["ticker"].astype(str)
    out[factor_name] = out[factor_name].astype("float64")
    out = (
        out.drop_duplicates(["date", "ticker"])
        .sort_values(["date", "ticker"], kind="mergesort")
        .reset_index(drop=True)
    )

    schema.validate(out)  # 仅做准入校验，返回帧保留 datetime64 以供按年拆分
    return out


def save_factor_append(factor, result, base_dir=str(DATA_ROOT / "factors")) -> None:
    """增量追加写入因子当前版本目录。

    先校验 ``result`` 再建目录；按年份拆分并与盘上既有文件合并，按 ``(date, ticker)``
    去重并保留已存旧值（数据修正须走 ``save_factor_full``），最后更新 ``metadata.json``。
    """
    data = _normalise(result, factor.name)
    out_dir = _version_dir(base_dir, factor.name, factor.version)
    out_dir.mkdir(parents=True, exist_ok=True)

    if len(data):
        data = data.assign(_year=data["date"].dt.year)
        for year, chunk in data.groupby("_year", sort=True):
            path = out_dir / f"{year}.parquet"
            chunk = chunk.drop(columns="_year")
            if path.exists():
                stored = pd.read_parquet(path)
                stored["date"] = pd.to_datetime(stored["date"], format="mixed")
                chunk = pd.concat([stored, chunk], ignore_index=True)
            chunk = (
                chunk.drop_duplicates(["date", "ticker"])
                .sort_values(["date", "ticker"], kind="mergesort")
                .reset_index(drop=True)
            )
            _write_year(chunk, path, factor.name)
        data = data.drop(columns=["_year"])

    write_metadata(out_dir, factor, data)


def save_factor_full(factor, result, base_dir=str(DATA_ROOT / "factors")) -> None:
    """全量覆盖写入当前版本目录（不影响其他版本目录）。

    先校验 ``result`` 再建目录，全部新年份文件原子写入完成后再删除过期年份文件，
    防止中途异常导致目录残留不完整历史。
    """
    data = _normalise(result, factor.name)
    out_dir = _version_dir(base_dir, factor.name, factor.version)
    out_dir.mkdir(parents=True, exist_ok=True)

    written: set[Path] = set()
    if len(data):
        data = data.assign(_year=data["date"].dt.year)
        for year, chunk in data.groupby("_year", sort=True):
            chunk = chunk.drop(columns="_year")
            path = out_dir / f"{year}.parquet"
            _write_year(chunk, path, factor.name)
            written.add(path)
        data = data.drop(columns=["_year"])

    # 所有新文件落盘后再清理旧年份文件，确保异常中断不破坏完整历史
    for stale in sorted(out_dir.glob("*.parquet")):
        if stale not in written:
            stale.unlink()

    write_metadata(out_dir, factor, data)


# ---------------------------------------------------------------------------
# 读取加载
# ---------------------------------------------------------------------------


def load_factor(
    name, start, end, version="1.0", base_dir=str(DATA_ROOT / "factors")
) -> pd.DataFrame:
    """按 ``[start, end]``（含端点）读取单因子原始值。

    仅读取覆盖该区间的年份文件，返回的 ``date`` 列为 ``datetime.date``（可直接与
    ``load_bars`` 连接）；版本目录不存在时抛出 ``FileNotFoundError``。
    """
    out_dir = _version_dir(base_dir, name, version)
    if not out_dir.is_dir():
        raise FileNotFoundError(f"factor version directory not found: {out_dir}")

    columns = ["date", "ticker", name]
    start_d, end_d = _to_date(start), _to_date(end)

    frames: list[pd.DataFrame] = []
    for year in range(start_d.year, end_d.year + 1):
        path = out_dir / f"{year}.parquet"
        if path.exists():
            frames.append(pd.read_parquet(path))

    if not frames:
        return pd.DataFrame(columns=columns)

    df = pd.concat(frames, ignore_index=True)
    parsed = pd.to_datetime(df["date"], format="mixed")
    df = df.loc[
        (parsed >= pd.Timestamp(start_d)) & (parsed <= pd.Timestamp(end_d)), columns
    ]
    return df.sort_values(list(_KEY_COLS), kind="mergesort").reset_index(drop=True)


def load_factor_matrix(
    names: list[str],
    start,
    end,
    version="1.0",
    base_dir=str(DATA_ROOT / "factors"),
) -> pd.DataFrame:
    """读取多因子并按 ``(date, ticker)`` 外连接为宽表 ``(date, ticker, f1, f2, ...)``，缺失值保持 ``NaN``。"""
    if not names:
        raise ValueError("names must be a non-empty list of factor names")

    frames = [
        load_factor(n, start, end, version, base_dir).set_index(list(_KEY_COLS))
        for n in names
    ]
    wide = pd.concat(frames, axis=1, join="outer").reset_index()
    return wide.sort_values(list(_KEY_COLS), kind="mergesort").reset_index(drop=True)
