"""FactorDAG 调度层：静态拓扑执行、内存缓存与故障隔离。

基于 ``research.factor_interface.Factor`` 契约，使用 ``parse_dependency`` 解析
``depends_on()`` 构建静态有向无环图，按拓扑顺序执行并维护以
``(name, version, date_range, depends_hash)`` 为键的内存缓存。
"""

from __future__ import annotations

import hashlib
import warnings
from collections import deque
from typing import Any, Callable

import pandas as pd

from research.factor_interface import (
    KIND_FACTOR,
    KIND_TABLE,
    Factor,
    parse_dependency,
    validate_output,
)

__all__ = [
    "FactorDAG",
    "MLFactor",
    "compute_all",
    "compute_all_safe",
    "compute_depends_hash",
    "compute_factor",
]

# ---------------------------------------------------------------------------
# 缓存键定义
# ---------------------------------------------------------------------------

#: 缓存键格式：``(factor_name, version, date_range, depends_hash)``。
CacheKey = tuple[str, str, tuple[str, str], str]

# ---------------------------------------------------------------------------
# 默认数据加载器：裸表名通过 load_bars 解析
# ---------------------------------------------------------------------------


def _table_family() -> dict[str, tuple[str, str]]:
    """运行时反转 ``data.data_api.TABLE_MAP``，映射表名到 ``(asset_type, freq)``。"""
    from data.data_api import TABLE_MAP

    return {v: k for k, v in TABLE_MAP.items()}


def _default_table_loader(table_name: str, start: str, end: str) -> pd.DataFrame:
    """通过 ``data.data_api.load_bars`` 加载原始数据表。"""
    from data.data_api import load_bars

    families = _table_family()
    if table_name not in families:
        raise ValueError(
            f"unknown data table {table_name!r}; expected one of {sorted(families)}"
        )
    asset_type, freq = families[table_name]
    return load_bars(start=start, end=end, freq=freq, asset_type=asset_type)


# ---------------------------------------------------------------------------
# FactorDAG
# ---------------------------------------------------------------------------


class FactorDAG:
    """基于已注册因子的静态依赖有向无环图。

    ``depends_on()`` 中的 ``"factor:"`` 条目构成因子依赖边，裸表名通过 ``data_loader`` 加载。
    """

    def __init__(
        self,
        data_loader: Callable[[str, str, str], pd.DataFrame] | None = None,
    ) -> None:
        self.factors: dict[str, Factor] = {}
        #: 因子名 → 上游因子名列表
        self._upstream: dict[str, list[str]] = {}
        self._data_loader = data_loader or _default_table_loader
        #: 内存结果缓存（不负责磁盘持久化）
        self._cache: dict[CacheKey, pd.DataFrame] = {}

    # -- 图构建 -------------------------------------------------------------

    def register(self, factor: Factor) -> None:
        """注册因子并解析 ``depends_on()`` 添加依赖边；全部校验通过后原子提交。"""
        name = factor.name
        if name in self.factors:
            raise ValueError(f"factor {name!r} is already registered")
        upstream: list[str] = []
        for dep in factor.depends_on():
            kind, dep_name = parse_dependency(dep)
            if kind == KIND_FACTOR:
                upstream.append(dep_name)
        self.factors[name] = factor
        self._upstream[name] = upstream

    # -- 拓扑排序 -----------------------------------------------------------

    def topological_sort(self) -> list[str]:
        """Kahn 算法拓扑排序，返回依赖顺序的因子名列表；存在环或悬空依赖时抛出 ``ValueError``。"""
        self._check_dangling()
        in_degree = {n: 0 for n in self.factors}
        for name, ups in self._upstream.items():
            for u in ups:
                if u in self.factors:
                    in_degree[name] += 1

        downstream: dict[str, list[str]] = {n: [] for n in self.factors}
        for name, ups in self._upstream.items():
            for u in ups:
                if u in self.factors:
                    downstream[u].append(name)

        queue = deque(n for n, d in in_degree.items() if d == 0)
        result: list[str] = []
        while queue:
            node = queue.popleft()
            result.append(node)
            for dep_name in downstream[node]:
                in_degree[dep_name] -= 1
                if in_degree[dep_name] == 0:
                    queue.append(dep_name)

        if len(result) != len(self.factors):
            remaining = set(self.factors) - set(result)
            raise ValueError(
                f"circular dependency detected involving {sorted(remaining)}"
            )
        return result

    def topological_layers(self) -> list[list[str]]:
        """按 Kahn 算法分层输出拓扑序，同层因子彼此独立。"""
        self._check_dangling()
        in_degree = {n: 0 for n in self.factors}
        for name, ups in self._upstream.items():
            for u in ups:
                if u in self.factors:
                    in_degree[name] += 1

        downstream: dict[str, list[str]] = {n: [] for n in self.factors}
        for name, ups in self._upstream.items():
            for u in ups:
                if u in self.factors:
                    downstream[u].append(name)

        layers: list[list[str]] = []
        current = sorted(n for n, d in in_degree.items() if d == 0)
        visited = 0
        while current:
            layers.append(current)
            visited += len(current)
            next_level: list[str] = []
            for node in current:
                for dep_name in downstream[node]:
                    in_degree[dep_name] -= 1
                    if in_degree[dep_name] == 0:
                        next_level.append(dep_name)
            current = sorted(next_level)

        if visited != len(self.factors):
            remaining = set(self.factors) - {n for layer in layers for n in layer}
            raise ValueError(
                f"circular dependency detected involving {sorted(remaining)}"
            )
        return layers

    def _check_dangling(self) -> None:
        """检查是否存在引用未注册因子的依赖，有则抛出 ``ValueError``。"""
        for name, ups in self._upstream.items():
            for u in ups:
                if u not in self.factors:
                    raise ValueError(
                        f"factor {name!r} depends on unregistered factor {u!r}"
                    )


# ---------------------------------------------------------------------------
# 依赖内容指纹
# ---------------------------------------------------------------------------


def _dataframe_fingerprint(df: pd.DataFrame) -> str:
    """计算 DataFrame 列结构与行数据的值敏感 12 位指纹。"""
    if df is None:
        return "missing"
    if df.empty:
        return "empty"
    row_hashes = pd.util.hash_pandas_object(df, index=False).to_numpy()
    schema = ",".join(f"{c}:{df[c].dtype}" for c in df.columns)
    raw = schema + "|" + hashlib.md5(row_hashes.tobytes()).hexdigest()
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def compute_depends_hash(factor: Factor, data: dict[str, pd.DataFrame]) -> str:
    """计算因子全部输入依赖内容的 12 位 MD5 指纹。"""
    parts: list[str] = []
    for dep in sorted(factor.depends_on()):
        df = data.get(dep)
        if df is None:
            parts.append(f"{dep}:missing:")
            continue
        parts.append(f"{dep}:{_dataframe_fingerprint(df)}")
    raw = "|".join(parts)
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# 规范行序
# ---------------------------------------------------------------------------

#: 规范化输出排序键。
_CANON_KEYS: tuple[str, ...] = ("date", "ticker")


def _canonicalise(df: pd.DataFrame) -> pd.DataFrame:
    """将 DataFrame 排序为规范行序（先 ``date`` 后 ``ticker``），确保结果与缓存路径无关。"""
    if df is None:
        return df
    keys = [c for c in _CANON_KEYS if c in df.columns]
    if not keys:
        return df
    # 使用稳定排序 mergesort，保证相同内容始终映射为一致的行序
    return df.sort_values(keys, kind="mergesort").reset_index(drop=True)


# ---------------------------------------------------------------------------
# 数据装配
# ---------------------------------------------------------------------------


def _assemble_data(
    dag: FactorDAG,
    factor: Factor,
    start: str,
    end: str,
    _visited: set[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """为 ``factor.compute()`` 装配依赖数据字典并统一规范行序。"""
    data: dict[str, pd.DataFrame] = {}
    for dep in factor.depends_on():
        kind, name = parse_dependency(dep)
        if kind == KIND_TABLE:
            data[dep] = _canonicalise(dag._data_loader(name, start, end))
        elif kind == KIND_FACTOR:
            data[dep] = _canonicalise(
                compute_factor(dag, name, start, end, _visited=_visited)
            )
    return data


# ---------------------------------------------------------------------------
# compute_factor：缓存命中 / 追加拼接 / 全量计算
# ---------------------------------------------------------------------------


def _make_cache_key(
    name: str,
    version: str,
    start: str,
    end: str,
    depends_hash: str,
) -> CacheKey:
    return (name, version, (str(start), str(end)), depends_hash)


def _slice_data_to_range(
    data: dict[str, pd.DataFrame],
    start: str,
    end: str,
) -> dict[str, pd.DataFrame]:
    """将各依赖表裁剪至 ``[start, end]`` 日期区间。"""
    start10, end10 = str(start)[:10], str(end)[:10]
    sliced: dict[str, pd.DataFrame] = {}
    for dep, df in data.items():
        if df is None or "date" not in getattr(df, "columns", []):
            sliced[dep] = df
            continue
        dates = df["date"].astype(str).str[:10]
        sliced[dep] = df.loc[(dates >= start10) & (dates <= end10)]
    return sliced


def _find_append_candidate(
    dag: FactorDAG,
    factor: Factor,
    data: dict[str, pd.DataFrame],
    start: str,
    end: str,
) -> tuple[CacheKey, pd.DataFrame] | None:
    """查找同 ``(name, version)``、起始日相同、覆盖 ``[start, X]``（``X < end``）且该前缀依赖内容未变的缓存项。"""
    start_s, end_s = str(start), str(end)
    for key, df in dag._cache.items():
        c_name, c_version, c_range, c_hash = key
        if c_name != factor.name or c_version != factor.version:
            continue
        c_start, c_end = c_range
        if c_start != start_s or not (c_end < end_s):
            continue
        prefix_hash = compute_depends_hash(
            factor, _slice_data_to_range(data, c_start, c_end)
        )
        if prefix_hash == c_hash:
            return key, df
    return None


def compute_factor(
    dag: FactorDAG,
    factor_name: str,
    start: str,
    end: str,
    force: bool = False,
    _visited: set[str] | None = None,
) -> pd.DataFrame:
    """计算单个因子在 ``[start, end]`` 区间的值并维护内存缓存。

    支持三种路径：精确缓存命中、前缀不变时的追加拼接（复用缓存历史前缀以保证
    溯源一致性，在全量上下文上重算以保留回溯窗口并截取尾部拼接），以及全量计算。
    所有输入输出均经规范行序处理；未注册因子或存在循环依赖时抛出 ``ValueError``。
    """
    if factor_name not in dag.factors:
        raise ValueError(f"factor {factor_name!r} is not registered")
    factor = dag.factors[factor_name]

    # 递归路径防环检查，显式抛出 ValueError 而非触发 RecursionError
    if _visited is None:
        _visited = set()
    if factor_name in _visited:
        raise ValueError(
            f"circular dependency detected: {factor_name!r} is already "
            f"being computed on this call path"
        )
    _visited.add(factor_name)
    try:
        data = _assemble_data(dag, factor, start, end, _visited=_visited)
        dep_hash = compute_depends_hash(factor, data)
        key = _make_cache_key(factor.name, factor.version, start, end, dep_hash)
    finally:
        _visited.discard(factor_name)

    # 1. 精确缓存命中
    if not force and key in dag._cache:
        return dag._cache[key]

    # 2. 追加拼接：复用历史缓存前缀以保证溯源一致性，截取尾部新行拼接
    if not force:
        candidate = _find_append_candidate(dag, factor, data, start, end)
        if candidate is not None:
            _old_key, cached_df = candidate
            _c_start, c_end = _old_key[2]
            tail_result = factor.compute(data)
            validate_output(factor.name, tail_result)
            tail_dates = tail_result["date"].astype(str).str[:10]
            tail_result = tail_result.loc[tail_dates > c_end[:10]]
            result = pd.concat([cached_df, tail_result])
            result = result.drop_duplicates(["date", "ticker"])
            result = _canonicalise(result)
            dag._cache[key] = result
            return result

    # 3. 全量计算
    result = factor.compute(data)
    validate_output(factor.name, result)
    result = _canonicalise(result)
    dag._cache[key] = result
    return result


# ---------------------------------------------------------------------------
# compute_all / compute_all_safe
# ---------------------------------------------------------------------------


def _warn_max_workers(max_workers: int | None) -> None:
    """提示当前版本下 ``max_workers`` 参数尚不生效（仍为顺序执行）。"""
    if max_workers is not None:
        warnings.warn(
            "max_workers has no effect until the layer-parallel executor "
            "is selected (pending design question); execution is sequential",
            stacklevel=3,
        )


def compute_all(
    dag: FactorDAG,
    start: str,
    end: str,
    max_workers: int | None = None,
    force: bool = False,
) -> dict[str, pd.DataFrame]:
    """按拓扑分层顺序计算全部已注册因子（``MLFactor`` 单独串行隔离执行）。"""
    _warn_max_workers(max_workers)
    results: dict[str, pd.DataFrame] = {}
    for layer in dag.topological_layers():
        ml_names = {n for n in layer if isinstance(dag.factors[n], MLFactor)}
        normal = [n for n in layer if n not in ml_names]
        for name in normal:
            results[name] = compute_factor(dag, name, start, end, force=force)
        for name in sorted(ml_names):
            results[name] = compute_factor(dag, name, start, end, force=force)
    return results


def compute_all_safe(
    dag: FactorDAG,
    start: str,
    end: str,
    max_workers: int | None = None,
    force: bool = False,
) -> tuple[dict[str, pd.DataFrame], dict[str, Exception]]:
    """容错批量计算：上游失败时跳过下游因子，同层独立因子继续执行，返回 ``(results, errors)``。"""
    _warn_max_workers(max_workers)
    results: dict[str, pd.DataFrame] = {}
    errors: dict[str, Exception] = {}
    failed: set[str] = set()

    for layer in dag.topological_layers():
        for name in layer:
            factor = dag.factors[name]
            upstream_failed = False
            for dep in factor.depends_on():
                kind, dep_name = parse_dependency(dep)
                if kind == KIND_FACTOR and dep_name in failed:
                    upstream_failed = True
                    break
            if upstream_failed:
                errors[name] = RuntimeError(
                    f"upstream dependency of {name!r} failed; skipping"
                )
                failed.add(name)
                continue
            try:
                results[name] = compute_factor(dag, name, start, end, force=force)
            except Exception as exc:
                errors[name] = exc
                failed.add(name)

    return results, errors


# ---------------------------------------------------------------------------
# MLFactor 接口骨架
# ---------------------------------------------------------------------------


class MLFactor(Factor):
    """机器学习因子抽象骨架，定义 ``train`` / ``predict`` 分离及重训与超时配置接口。"""

    #: 占位名称，具体子类必须覆盖。
    name: str = "ml_factor"
    description: str = "ML factor skeleton (training lands at S11)"

    #: 持久化模型文件路径（joblib）。
    model_path: str = ""

    #: 重训频率（monthly / quarterly / yearly）。
    retrain_freq: str = "monthly"

    #: 单次计算超时阈值（秒），超时按失败处理。
    timeout_seconds: int = 300

    def depends_on(self) -> list[str]:
        return []

    def train(self, data: dict[str, pd.DataFrame], train_end: str) -> Any:
        """使用截至 ``train_end`` 的数据训练模型（严格隔离未来数据以防前视）。"""
        raise NotImplementedError("MLFactor.train() lands at S11")

    def predict(self, model: Any, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """基于已训练模型生成预测结果。"""
        raise NotImplementedError("MLFactor.predict() lands at S11")

    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """加载或训练模型并执行预测。"""
        raise NotImplementedError("MLFactor.compute() lands at S11")
