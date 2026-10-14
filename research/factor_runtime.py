"""FactorDAG scheduling — topological execution, caching, and failure isolation.

Consumes :mod:`research.factor_interface`'s ``Factor`` contract.  Builds a
static dependency graph from ``depends_on()`` via :func:`parse_dependency`
(T1 — no second prefix parser), executes in topological order with an
in-memory cache keyed on ``(name, version, date_range, depends_hash)``.

Scope (T2 only — S5 / S11 own the rest):

- no factor storage to disk (S5 owns ``factor_storage``)
- no post-processing (S5 owns ``PostProcessor``)
- no ML training (S11) — :class:`MLFactor` is a shape skeleton
- no executor-parallel layer execution — :meth:`FactorDAG.topological_layers`
  declares the parallelism shape; the executor choice (thread vs process) is
  a pending design question

The time model is a for-loop (red line 1).  The DAG is a static topological
declaration, not an event subscription graph (red line 2).
"""

from __future__ import annotations

import hashlib
from collections import deque
from datetime import datetime, timedelta
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
# Cache key
# ---------------------------------------------------------------------------

#: Cache key = ``(factor_name, version, date_range, depends_hash)``.
CacheKey = tuple[str, str, tuple[str, str], str]

# ---------------------------------------------------------------------------
# Default data loader — bare dependency names resolve through ``load_bars``
# ---------------------------------------------------------------------------


def _default_table_loader(table_name: str, start: str, end: str) -> pd.DataFrame:
    """Load a raw data table via :func:`data.data_api.load_bars`.

    The dependency name *table_name* is matched against the known table
    families.  For T2 the only bare names exercised in tests are synthetic;
    real table loading goes through ``load_bars`` with the appropriate
    ``asset_type`` / ``freq``.
    """
    from data.data_api import load_bars

    # Table name → (asset_type, freq) — mirrors TABLE_MAP in data_api.
    _TABLE_FAMILY: dict[str, tuple[str, str]] = {
        "bars_daily": ("equity", "daily"),
        "bars_minute": ("equity", "minute"),
        "bars_daily_futures": ("futures", "daily"),
        "continuous": ("continuous", "daily"),
    }
    if table_name not in _TABLE_FAMILY:
        raise ValueError(
            f"unknown data table {table_name!r}; "
            f"expected one of {sorted(_TABLE_FAMILY)}"
        )
    asset_type, freq = _TABLE_FAMILY[table_name]
    return load_bars(start=start, end=end, freq=freq, asset_type=asset_type)


# ---------------------------------------------------------------------------
# FactorDAG
# ---------------------------------------------------------------------------


class FactorDAG:
    """Static dependency graph over registered factors.

    Edges come from ``depends_on()`` entries with the ``"factor:"`` prefix
    (parsed by T1's :func:`parse_dependency`).  Bare names are data tables
    and do not create graph edges.

    Parameters
    ----------
    data_loader
        Callable ``(table_name, start, end) -> DataFrame`` used to resolve
        bare table dependencies.  Defaults to :func:`_default_table_loader`
        (backed by ``data.data_api.load_bars``).  Tests inject synthetic
        loaders.
    """

    def __init__(
        self,
        data_loader: Callable[[str, str, str], pd.DataFrame] | None = None,
    ) -> None:
        self.factors: dict[str, Factor] = {}
        #: factor name → list of upstream factor names (from ``factor:`` deps)
        self._upstream: dict[str, list[str]] = {}
        self._data_loader = data_loader or _default_table_loader
        #: In-memory result cache.  Disk persistence is S5.
        self._cache: dict[CacheKey, pd.DataFrame] = {}

    # -- graph construction -----------------------------------------------

    def register(self, factor: Factor) -> None:
        """Register *factor* and add graph edges from ``depends_on()``.

        Uses T1's :func:`parse_dependency` — bare names are data tables
        (no edge), ``"factor:xxx"`` creates an edge to upstream factor *xxx*.
        """
        name = factor.name
        if name in self.factors:
            raise ValueError(f"factor {name!r} is already registered")
        self.factors[name] = factor
        upstream: list[str] = []
        for dep in factor.depends_on():
            kind, dep_name = parse_dependency(dep)
            if kind == KIND_FACTOR:
                upstream.append(dep_name)
        self._upstream[name] = upstream

    # -- topological ordering ---------------------------------------------

    def topological_sort(self) -> list[str]:
        """Kahn's algorithm.  Returns factor names in dependency order.

        Raises
        ------
        ValueError
            If the graph contains a cycle (sort length ≠ node count), or if
            a ``factor:`` dependency references an unregistered name.
        """
        self._check_dangling()
        in_degree = {n: 0 for n in self.factors}
        for name, ups in self._upstream.items():
            for u in ups:
                if u in self.factors:
                    in_degree[name] += 1

        # Reverse edges: upstream → [downstream, ...]
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
        """Layered topological order — factors in one layer are independent.

        Same source as :meth:`topological_sort` (Kahn's algorithm).  Layer
        *i* contains all nodes whose in-degree reaches zero after layer
        *i-1* is removed.  Same-layer factors can run in parallel.
        """
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
        """Raise if any ``factor:`` dependency names an unregistered factor."""
        for name, ups in self._upstream.items():
            for u in ups:
                if u not in self.factors:
                    raise ValueError(
                        f"factor {name!r} depends on unregistered factor {u!r}"
                    )


# ---------------------------------------------------------------------------
# Dependency fingerprint
# ---------------------------------------------------------------------------


def compute_depends_hash(factor: Factor, data: dict[str, pd.DataFrame]) -> str:
    """Compute a 12-char md5 fingerprint of the factor's dependency contents.

    For each dependency (sorted by name) the fingerprint includes the
    dependency name, its row count, and its ``max(date)``.  The parts are
    joined with ``|`` and hashed — first 12 hex chars of the md5.
    """
    parts: list[str] = []
    for dep in sorted(factor.depends_on()):
        df = data.get(dep)
        if df is None:
            parts.append(f"{dep}:missing:")
            continue
        row_count = len(df)
        max_date = str(df["date"].max()) if "date" in df.columns and not df.empty else ""
        parts.append(f"{dep}:{row_count}:{max_date}")
    raw = "|".join(parts)
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Data assembly (Dev default — see PR description)
# ---------------------------------------------------------------------------


def _assemble_data(
    dag: FactorDAG,
    factor: Factor,
    start: str,
    end: str,
) -> dict[str, pd.DataFrame]:
    """Build the ``data`` dict for ``factor.compute()``.

    Bare dependency names → data tables loaded via the DAG's ``data_loader``.
    ``"factor:xxx"`` → recursively computed upstream factor results.
    """
    data: dict[str, pd.DataFrame] = {}
    for dep in factor.depends_on():
        kind, name = parse_dependency(dep)
        if kind == KIND_TABLE:
            data[dep] = dag._data_loader(name, start, end)
        elif kind == KIND_FACTOR:
            data[dep] = compute_factor(dag, name, start, end)
    return data


# ---------------------------------------------------------------------------
# compute_factor — cache hit / append / full
# ---------------------------------------------------------------------------


def _make_cache_key(
    name: str,
    version: str,
    start: str,
    end: str,
    depends_hash: str,
) -> CacheKey:
    return (name, version, (str(start), str(end)), depends_hash)


def _find_append_candidate(
    dag: FactorDAG,
    name: str,
    version: str,
    depends_hash: str,
    start: str,
    end: str,
) -> tuple[CacheKey, pd.DataFrame] | None:
    """Find a cache entry with same (name, version, depends_hash) that covers
    ``[start, X]`` where ``X < end`` — the append candidate.
    """
    for key, df in dag._cache.items():
        c_name, c_version, c_range, c_hash = key
        if c_name == name and c_version == version and c_hash == depends_hash:
            c_start, c_end = c_range
            if c_start == start and c_end < end:
                return key, df
    return None


def compute_factor(
    dag: FactorDAG,
    factor_name: str,
    start: str,
    end: str,
    force: bool = False,
) -> pd.DataFrame:
    """Compute one factor over ``[start, end]`` with caching.

    Three modes:

    - **cache hit** — exact key match and ``force=False`` → return cached
    - **append** — same (name, version, depends_hash) cached for a shorter
      range ending before *end* → compute only the delta and append
    - **full** — no cache, ``force=True``, or version change (different key)
      → compute the full range

    The cache key is exactly ``(name, version, date_range, depends_hash)``.
    """
    if factor_name not in dag.factors:
        raise ValueError(f"factor {factor_name!r} is not registered")
    factor = dag.factors[factor_name]

    # Assemble inputs (tables + upstream factors) for depends_hash + compute.
    data = _assemble_data(dag, factor, start, end)
    dep_hash = compute_depends_hash(factor, data)
    key = _make_cache_key(factor.name, factor.version, start, end, dep_hash)

    # 1. Exact cache hit.
    if not force and key in dag._cache:
        return dag._cache[key]

    # 2. Append: same identity (name, version, hash), shorter cached range.
    if not force:
        candidate = _find_append_candidate(
            dag, factor.name, factor.version, dep_hash, start, end
        )
        if candidate is not None:
            _old_key, cached_df = candidate
            _c_start, c_end = _old_key[2]
            # Compute the missing tail (next day after cached end).
            tail_start = _next_day(c_end)
            tail_data = _assemble_data(dag, factor, tail_start, end)
            tail_result = factor.compute(tail_data)
            validate_output(factor.name, tail_result)
            result = pd.concat([cached_df, tail_result])
            result = result.drop_duplicates(["date", "ticker"])
            result = result.sort_values(["date", "ticker"]).reset_index(drop=True)
            dag._cache[key] = result
            return result

    # 3. Full compute.
    result = factor.compute(data)
    validate_output(factor.name, result)
    dag._cache[key] = result
    return result


def _next_day(date_str: str) -> str:
    """Return the calendar day after *date_str* (ISO format preserved)."""
    dt = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
    return (dt + timedelta(days=1)).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# compute_all / compute_all_safe
# ---------------------------------------------------------------------------


def compute_all(
    dag: FactorDAG,
    start: str,
    end: str,
    max_workers: int = 4,
    force: bool = False,
) -> dict[str, pd.DataFrame]:
    """Compute all registered factors in topological order.

    Iterates :meth:`FactorDAG.topological_layers` and computes each factor
    in dependency order.  ML factors are computed serially (isolated from
    any future parallel pool).  *max_workers* is reserved for when the
    layer-parallel executor is selected; execution is sequential for now.
    """
    results: dict[str, pd.DataFrame] = {}
    for layer in dag.topological_layers():
        ml_names = {n for n in layer if isinstance(dag.factors[n], MLFactor)}
        normal = [n for n in layer if n not in ml_names]
        # Sequential within layer — executor choice is a pending question.
        for name in normal:
            results[name] = compute_factor(dag, name, start, end, force=force)
        for name in sorted(ml_names):
            results[name] = compute_factor(dag, name, start, end, force=force)
    return results


def compute_all_safe(
    dag: FactorDAG,
    start: str,
    end: str,
    max_workers: int = 4,
    force: bool = False,
) -> tuple[dict[str, pd.DataFrame], dict[str, Exception]]:
    """Fault-tolerant compute: upstream failure skips downstream, same-layer
    independent factors continue.

    Returns ``(results, errors)`` where *errors* maps factor name → exception
    (or a ``RuntimeError`` explaining that an upstream dependency failed).
    """
    results: dict[str, pd.DataFrame] = {}
    errors: dict[str, Exception] = {}
    failed: set[str] = set()

    for layer in dag.topological_layers():
        for name in layer:
            factor = dag.factors[name]
            # Check if any upstream factor has already failed.
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
# MLFactor skeleton (training is S11)
# ---------------------------------------------------------------------------


class MLFactor(Factor):
    """Skeleton for ML-based factors.

    Only the interface shape lands here: ``train`` / ``predict`` separation,
    ``retrain_freq`` placeholder, and ``timeout_seconds`` placeholder.
    Training, model persistence and prediction logic land at S11.

    Subclasses must override :attr:`name` (and normally implement
    ``depends_on`` / ``train`` / ``predict`` / ``compute``).
    """

    #: Placeholder name — concrete subclasses must override.
    name: str = "ml_factor"
    description: str = "ML factor skeleton (training lands at S11)"

    #: Path to the persisted model file (joblib).  Managed by S11.
    model_path: str = ""

    #: Retrain frequency trigger.  Dev default: checked on each full compute
    #: run by comparing the model's last-train timestamp against the
    #: frequency window.  Concrete trigger mechanism is a Dev default
    #: (see PR description).
    retrain_freq: str = "monthly"  # monthly / quarterly / yearly

    #: Seconds before an ML compute is considered stalled.  Dev default: 300
    #: seconds, after which the factor is recorded as failed (compute_all_safe
    #: treats it like any other failure).  Value is a Dev default (see PR).
    timeout_seconds: int = 300

    def depends_on(self) -> list[str]:
        return []

    def train(self, data: dict[str, pd.DataFrame], train_end: str) -> Any:
        """Train a model on *data* up to *train_end*.

        *train_end* strictly separates training from prediction to prevent
        look-ahead.  Implementation lands at S11.
        """
        raise NotImplementedError("MLFactor.train() lands at S11")

    def predict(self, model: Any, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Generate predictions from a trained *model*.  Lands at S11."""
        raise NotImplementedError("MLFactor.predict() lands at S11")

    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Load-or-train then predict.  Lands at S11."""
        raise NotImplementedError("MLFactor.compute() lands at S11")
