"""存储层运行时操作：原子写、文件锁、生命周期清理与容量检查。

表结构定义统一归属 :mod:`data.storage_schema`，本模块不重复定义列、类型或校验规则；
所有写入路径均先调用 ``SCHEMA[table].validate(df)``，校验失败则拒绝落盘。

三阶段原子写（``lock -> tmp -> rename``）：
1. ``FileLock(path)`` 获取排他锁；
2. 写入 ``{path}.tmp`` 并 ``fsync``；
3. ``os.replace(tmp, path)`` 原子提交，异常路径清理残留 ``.tmp``。
"""

from __future__ import annotations

import fcntl
import logging
import os
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Iterator, Sequence

import pandas as pd
import yaml
from core.config import DATA_ROOT
from data.storage_schema import (
    CLEAN,
    PARTITION_PATHS,
    QUALITY_FLAG_BITS,
    QUALITY_FLAG_MAX,
    SCHEMA,
    TABLE_FAMILY,
    factor_schema,
    factor_view_name,
)

logger = logging.getLogger(__name__)

__all__ = [
    "CLEAN",
    "PARTITION_PATHS",
    "QUALITY_FLAG_BITS",
    "QUALITY_FLAG_MAX",
    "TABLE_FAMILY",
    "PARQUET_WRITE_CONFIG",
    "ConcurrentWriteError",
    "DiskFullError",
    "FileLock",
    "write_parquet_atomic",
    "upsert_parquet_atomic",
    "partition_path",
    "is_monthly_partition",
    "normalize_missing_floats",
    "parse_date_from_filename",
    "cleanup_stale_tmp",
    "cleanup_stale_locks",
    "cleanup_expired",
    "load_lifecycle",
    "check_capacity",
    "check_daily_growth",
    "factor_schema",
    "factor_view_name",
]

# --- Parquet 写入配置（全局统一，不按表覆盖） ---
PARQUET_WRITE_CONFIG: dict[str, Any] = dict(
    compression="zstd",
    row_group_size=65536,
    use_dictionary=True,
    write_statistics=True,
    index=False,
)

# 容量分级阈值：可用空间 > 50 GB 为 OK，10–50 GB 为 WARNING，<= 10 GB 为 CRITICAL
CAPACITY_OK_BYTES = 50 * 1024**3
CAPACITY_WARNING_BYTES = 10 * 1024**3

# 日增量异常基线：单日 > 100 MB 视为重复入库或体积异常告警
GROWTH_BASELINE_BYTES = 100 * 1024**2

# .tmp / .lock 残留过期判定阈值（小时，全局统一）
STALE_MAX_AGE_HOURS = 1

_TMP_SUFFIX = ".tmp"
_LOCK_SUFFIX = ".lock"


# --- 异常定义 ---
class ConcurrentWriteError(OSError):
    """目标文件锁已被其他写入方持有（同日同表并发写入视为配置错误，不自动重试）。"""


class DiskFullError(OSError):
    """磁盘可用空间处于 CRITICAL 级别，拒绝写入（需人工清理后重试）。"""


# --- FileLock（flock LOCK_EX | LOCK_NB） ---
class FileLock:
    """基于 ``flock(LOCK_EX | LOCK_NB)`` 的非阻塞排他文件锁。

    锁被占用时立即抛出 :class:`ConcurrentWriteError`（不重试）。
    锁文件作为永久 inode 在退出时不执行 unlink，仅通过 ``close()`` 释放 ``flock``，
    避免 unlink 间隙引发多写入方分别锁定不同 inode 的竞态问题；
    崩溃遗留的锁文件由 :func:`cleanup_stale_locks` 巡检报告。
    """

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self._target = Path(path)
        self._lock_path = self._target.with_name(self._target.name + _LOCK_SUFFIX)
        self._fd: int | None = None

    @property
    def lock_path(self) -> Path:
        return self._lock_path

    def __enter__(self) -> FileLock:
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._lock_path, os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise ConcurrentWriteError(
                f"another writer holds the lock for {self._target}"
            ) from exc
        except BaseException:
            # 其他 flock 异常时关闭 fd 防止句柄泄漏
            os.close(fd)
            raise
        self._fd = fd
        return self

    def __exit__(self, *exc: object) -> None:
        fd = self._fd
        self._fd = None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        # 退出时不删除锁文件，原因见类文档字符串


# --- 缺失浮点值标准化 ---
def _is_float_dtype(dtype: Any) -> bool:
    return str(dtype).startswith("float")


def _normalize_for_schema(df: pd.DataFrame, schema: Any) -> pd.DataFrame:
    """将 Schema 中浮点列的 ``None`` 替换为 ``NaN``（避免全空列退化为 ``object`` 类型导致校验失败）。"""
    out = df.copy()
    for name, col in schema.columns.items():
        if name not in out.columns:
            continue
        if not _is_float_dtype(col.dtype):
            continue
        series = out[name]
        if series.dtype == object:
            out[name] = series.where(series.notna(), float("nan")).astype("float64")
    return out


def normalize_missing_floats(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """按 ``SCHEMA[table]`` 标准化浮点列缺失值（指定值列名的因子表由写入入口按动态 Schema 处理）。"""
    return _normalize_for_schema(df, SCHEMA[table])


# --- Schema 解析 ---
_FIXED_FACTOR_COLUMNS = ("ticker", "date")


def _resolve_schema(table: str, value_column: str | None = None):
    """解析 *table* 对应的 Pandera Schema；factor 表支持指定 ``value_column``（禁止与保留列冲突）。"""
    if table not in SCHEMA:
        raise KeyError(f"unknown table {table!r}; known: {sorted(SCHEMA)}")
    if table == "factor" and value_column is not None:
        if value_column in _FIXED_FACTOR_COLUMNS:
            raise ValueError(
                f"factor value_column {value_column!r} collides with a fixed "
                f"column; factor_schema would silently overwrite it"
            )
        return factor_schema(value_column)
    return SCHEMA[table]


# --- 分区路径解析（支持 {date} 与 {month}） ---
def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def is_monthly_partition(table: str) -> bool:
    """若表按 ``{month}`` (YYYY-MM) 分区落地则返回 ``True``。"""
    template = PARTITION_PATHS.get(table, "")
    return "{month}" in template


def partition_path(
    table: str,
    data_date: date | datetime | str,
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
) -> Path:
    """解析 *table* 在 *data_date* 下的分区路径（仅支持 ``{date}`` 或 ``{month}`` 模板）。"""
    template = PARTITION_PATHS.get(table)
    if template is None:
        raise NotImplementedError(
            f"landing layout for table {table!r} is unknown (layouts: {PARTITION_PATHS})"
        )
    day = _coerce_date(data_date)
    if "{date}" in template:
        return Path(data_root) / template.format(date=day.strftime("%Y-%m-%d"))
    if "{month}" in template:
        return Path(data_root) / template.format(month=day.strftime("%Y-%m"))
    raise NotImplementedError(
        f"landing layout for table {table!r} is neither a date nor month partition; "
        f"pass target_path explicitly (layouts: {PARTITION_PATHS})"
    )


# --- 原子写入（lock -> tmp -> rename） ---
def write_parquet_atomic(
    df: pd.DataFrame,
    path: str | os.PathLike[str],
    *,
    table: str,
    value_column: str | None = None,
    check_disk: bool = True,
    **parquet_kwargs: Any,
) -> None:
    """经 Schema 校验后将 *df* 原子写入 *path*。

    流程：标准化浮点缺失值 -> Schema 校验（失败不写盘）-> 容量检查 ->
    ``FileLock`` 加锁写 ``{path}.tmp`` 并 ``fsync`` -> ``os.replace`` 原子提交。
    异常路径自动清理残留 ``.tmp``；factor 表可通过 ``value_column`` 指定值列名。
    """
    target = Path(path)
    schema = _resolve_schema(table, value_column=value_column)

    # 按已解析 Schema 将浮点列中的 None 转为 NaN，避免全空列变成 object 类型
    prepared = _normalize_for_schema(df, schema)
    # 校验失败直接抛出异常，不触碰磁盘
    validated = schema.validate(prepared)

    if check_disk:
        status = check_capacity(target.parent if target.parent.exists() else target)
        if status == "CRITICAL":
            raise DiskFullError(
                f"free space is at the CRITICAL tier; refusing to write {target}"
            )

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + _TMP_SUFFIX)

    with FileLock(target):
        try:
            kwargs = {**PARQUET_WRITE_CONFIG, **parquet_kwargs}
            validated.to_parquet(tmp, **kwargs)
            # 先刷盘 tmp 数据再执行 os.replace，确保断电等场景下的持久性
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, target)
        except BaseException:
            # 异常路径确保不残留半成品文件或 .tmp
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise

        # 提交后刷盘父目录确保 rename 持久性；置于 try 块外以免已提交却误报写入失败
        try:
            dir_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning(
                "wrote %s but could not fsync %s; committed but not durable",
                target,
                target.parent,
            )


def upsert_parquet_atomic(
    df: pd.DataFrame,
    path: str | os.PathLike[str],
    *,
    table: str,
    partition_dates: Sequence[date | datetime | str] | None = None,
    value_column: str | None = None,
    check_disk: bool = True,
    **parquet_kwargs: Any,
) -> None:
    """在 ``FileLock`` 保护下将 *df* 原子合并写入多日分区文件（如 ``{month}.parquet``）。

    替换匹配 *partition_dates* 或入参 ``trade_date`` 的已有行，保留同分区其余日期数据，
    按 ``(trade_date, ticker)`` 排序并经 Schema 校验后执行 ``tmp -> fsync -> os.replace`` 原子提交。
    """
    target = Path(path)
    schema = _resolve_schema(table, value_column=value_column)
    prepared = _normalize_for_schema(df, schema)
    validated = schema.validate(prepared)

    if check_disk:
        status = check_capacity(target.parent if target.parent.exists() else target)
        if status == "CRITICAL":
            raise DiskFullError(
                f"free space is at the CRITICAL tier; refusing to write {target}"
            )

    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + _TMP_SUFFIX)

    with FileLock(target):
        to_write = validated
        if target.exists() and "trade_date" in schema.columns:
            existing = pd.read_parquet(target)
            replace_dates: set[date] = {
                _coerce_date(d) for d in (partition_dates or ())
            }
            if "trade_date" in validated.columns and len(validated):
                inc_dates = pd.to_datetime(
                    validated["trade_date"], errors="coerce"
                ).dt.date.dropna()
                replace_dates.update(inc_dates.tolist())

            if replace_dates and "trade_date" in existing.columns and len(existing):
                ext_dates = pd.to_datetime(
                    existing["trade_date"], errors="coerce"
                ).dt.date
                kept = existing[~ext_dates.isin(replace_dates)]
            else:
                kept = existing

            if not kept.empty:
                combined = (
                    pd.concat([kept, validated], ignore_index=True)
                    if not validated.empty
                    else kept.reset_index(drop=True)
                )
                sort_cols = [
                    c for c in ("trade_date", "ticker") if c in combined.columns
                ]
                if sort_cols:
                    combined = combined.sort_values(
                        sort_cols, kind="mergesort"
                    ).reset_index(drop=True)
                to_write = schema.validate(_normalize_for_schema(combined, schema))

        try:
            kwargs = {**PARQUET_WRITE_CONFIG, **parquet_kwargs}
            to_write.to_parquet(tmp, **kwargs)
            fd = os.open(tmp, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(tmp, target)
        except BaseException:
            try:
                os.remove(tmp)
            except FileNotFoundError:
                pass
            raise

        try:
            dir_fd = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            logger.warning(
                "wrote %s but could not fsync %s; committed but not durable",
                target,
                target.parent,
            )


# --- 文件名日期解析（供生命周期清理使用） ---
_DATE_PATTERNS = (
    re.compile(r"(?<!\d)(\d{4}-\d{2}-\d{2})(?!\d)"),
    re.compile(r"(?<!\d)(\d{8})(?!\d)"),
    re.compile(r"(?<!\d)(\d{4})(?!\d)"),
)


def parse_date_from_filename(name: str) -> date | None:
    """解析文件名中的日期（支持 ``YYYY-MM-DD``、``YYYYMMDD`` 与 ``YYYY``）；无日期的全量参考表返回 ``None`` 且永不过期。"""
    stem = Path(name).name
    for pattern, kind in (
        (_DATE_PATTERNS[0], "iso"),
        (_DATE_PATTERNS[1], "compact"),
        (_DATE_PATTERNS[2], "year"),
    ):
        match = pattern.search(stem)
        if not match:
            continue
        token = match.group(1)
        try:
            if kind == "iso":
                return date.fromisoformat(token)
            if kind == "compact":
                return datetime.strptime(token, "%Y%m%d").date()
            return date(int(token), 1, 1)
        except ValueError:
            # 形似日期但非合法日期时直接返回 None，不回退猜测年份
            return None
    return None


# --- 残留 .tmp 与 .lock 清理 ---
def _iter_files(root: Path) -> Iterator[Path]:
    if not root.exists():
        return
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            yield Path(dirpath) / filename


def _is_stale(path: Path, max_age_hours: float, now: datetime | None) -> bool:
    reference = now if now is not None else datetime.now()
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime)
    except FileNotFoundError:
        return False
    return (reference - mtime) > timedelta(hours=max_age_hours)


def cleanup_stale_tmp(
    root: str | os.PathLike[str],
    max_age_hours: float = STALE_MAX_AGE_HOURS,
    *,
    now: datetime | None = None,
    dry_run: bool = True,
) -> list[Path]:
    """清理超过 *max_age_hours* 的残留 ``.tmp`` 文件；默认 ``dry_run=True`` 仅报告不删除。"""
    root = Path(root)
    removed: list[Path] = []
    for path in _iter_files(root):
        if not path.name.endswith(_TMP_SUFFIX):
            continue
        if not _is_stale(path, max_age_hours, now):
            continue
        removed.append(path)
        if not dry_run:
            path.unlink(missing_ok=True)
    return removed


def _try_exclusive_lock(path: Path) -> int | None:
    """尝试对 *path* 获取非阻塞排他锁，成功返回 fd（仅用于探测活跃写入方，禁止用于删除锁文件）。"""
    try:
        fd = os.open(path, os.O_RDWR)
    except OSError:
        return None
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def cleanup_stale_locks(
    root: str | os.PathLike[str],
    max_age_hours: float = STALE_MAX_AGE_HOURS,
    *,
    now: datetime | None = None,
    dry_run: bool = True,
) -> list[Path]:
    """报告超过 *max_age_hours* 且未被持有的 ``.lock`` 文件（永不删除）。

    锁文件作为永久 inode，删除会重新引入多写入方竞态；本函数仅巡检并返回候选列表。
    ``dry_run`` 仅为保持清理接口签名一致，无实际删除行为。
    """
    root = Path(root)
    found: list[Path] = []
    for path in _iter_files(root):
        if not path.name.endswith(_LOCK_SUFFIX):
            continue
        if not _is_stale(path, max_age_hours, now):
            continue
        fd = _try_exclusive_lock(path)
        if fd is None:
            # 仍被活跃写入方持有或已不存在，不属于残留
            continue
        try:
            found.append(path)
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
    return found


# --- 生命周期清理（由 data/lifecycle.yaml 驱动） ---
def load_lifecycle(config_path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """从 ``data/lifecycle.yaml`` 加载数据保留策略。"""
    if config_path is None:
        config_path = Path(__file__).with_name("lifecycle.yaml")
    with open(config_path, encoding="utf-8") as handle:
        policy = yaml.safe_load(handle)
    if not isinstance(policy, dict) or "policies" not in policy:
        raise ValueError(f"lifecycle config at {config_path} has no 'policies' key")
    return policy


def _match_policy(rel_parts: tuple[str, ...], policies: list[dict]) -> dict:
    """返回与相对路径 *rel_parts* 最长前缀匹配的保留策略（无匹配返回空字典）。"""
    best: dict | None = None
    best_len = -1
    for policy in policies:
        prefix = str(policy.get("prefix", "")).strip("/")
        if not prefix:
            continue
        prefix_parts = tuple(prefix.split("/"))
        if (
            rel_parts[: len(prefix_parts)] == prefix_parts
            and len(prefix_parts) > best_len
        ):
            best = policy
            best_len = len(prefix_parts)
    return best if best is not None else {}


def cleanup_expired(
    root: str | os.PathLike[str] = str(DATA_ROOT),
    *,
    subdir: str | None = None,
    dry_run: bool = True,
    config_path: str | os.PathLike[str] | None = None,
    today: date | None = None,
) -> list[Path]:
    """按 ``lifecycle.yaml`` 清理超过保留期限的历史文件。

    默认 ``dry_run=True`` 仅返回待删路径；跳过 ``permanent`` 规则、无日期文件名及 ``.tmp`` / ``.lock`` 残留。
    可通过 *subdir* 将扫描范围限定到 *root* 下的单个子目录。
    """
    root = Path(root)
    if subdir is not None:
        p = PurePosixPath(subdir)
        if p.is_absolute() or ".." in p.parts or len(p.parts) != 1:
            raise ValueError(
                f"subdir must be a single relative path segment, got {subdir!r}"
            )
    scan_root = root / subdir if subdir else root
    if not scan_root.exists():
        return []
    policy_doc = load_lifecycle(config_path)
    policies = list(policy_doc.get("policies", []))
    default_policy = {"retention": policy_doc.get("default", "permanent")}
    cutoff_today = today if today is not None else date.today()

    removed: list[Path] = []
    for path in _iter_files(scan_root):
        if path.name.endswith((_TMP_SUFFIX, _LOCK_SUFFIX)):
            continue
        try:
            rel = path.relative_to(root)
        except ValueError:
            continue
        policy = _match_policy(rel.parts, policies) or default_policy
        if str(policy.get("retention", "permanent")) == "permanent":
            continue
        days = policy.get("days")
        if days is None:
            continue
        file_date = parse_date_from_filename(path.name)
        if file_date is None:
            continue
        if file_date < cutoff_today - timedelta(days=int(days)):
            removed.append(path)
            if not dry_run:
                path.unlink(missing_ok=True)
    return removed


# --- 磁盘容量与日增量检查 ---
def check_capacity(data_root: str | os.PathLike[str] = str(DATA_ROOT)) -> str:
    """返回 *data_root* 所在文件系统的可用空间分级（``OK`` >50GB / ``WARNING`` 10–50GB / ``CRITICAL`` <=10GB）；目录尚未创建时向上探测最近祖先目录。"""
    target = Path(data_root)
    probe = target
    while not probe.exists():
        parent = probe.parent
        if parent == probe:
            break
        probe = parent
    free = shutil.disk_usage(probe).free
    if free > CAPACITY_OK_BYTES:
        return "OK"
    if free > CAPACITY_WARNING_BYTES:
        return "WARNING"
    return "CRITICAL"


def check_daily_growth(
    data_root: str | os.PathLike[str] = str(DATA_ROOT),
    *,
    today: date | None = None,
    baseline_bytes: int = GROWTH_BASELINE_BYTES,
) -> dict[str, Any]:
    """统计文件名日期等于 *today* 的数据文件总字节数并对比 100 MB 日增量基线（忽略 ``.tmp`` / ``.lock``，超限仅告警不拒写）。"""
    root = Path(data_root)
    day = today if today is not None else date.today()
    total = 0
    count = 0
    for path in _iter_files(root):
        if path.name.endswith((_TMP_SUFFIX, _LOCK_SUFFIX)):
            continue
        if parse_date_from_filename(path.name) != day:
            continue
        try:
            total += path.stat().st_size
        except FileNotFoundError:
            continue
        count += 1
    return {
        "date": day,
        "bytes": total,
        "files": count,
        "baseline_bytes": baseline_bytes,
        "exceeded": total > baseline_bytes,
    }
