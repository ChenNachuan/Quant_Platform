"""Factor ABC and IO contract — the definition of what a factor is.

Interface only.  Scheduling is T2 (``FactorDAG``), post-processing is S5
(``PostProcessor``), storage is S5, governance is S11.  ``depends_on()`` is a
**static declaration** of input names, not an event subscription (red lines 1–2).

Dependency prefix contract (hard — T2 assembles the DAG from it):

- bare name (no colon)  → raw data table, loaded via ``data_api.load_bars()``
- ``"factor:xxx"``      → upstream factor, loaded from factor storage or computed
- any other prefix      → **rejected** (explicit ``ValueError``, no silent
  fallback to table name)

``compute()`` returns **raw** values in a long table ``DataFrame[date, ticker,
{factor_name}]`` — exactly three columns.  Winsorising, filling, standardising
and neutralising belong to S5's ``PostProcessor``, never to ``Factor.compute()``.

Metadata lives in class attributes — code IS the registry.  No JSON/YAML
registration files (factor count stays under 100).
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any, Iterable

import pandas as pd

# ---------------------------------------------------------------------------
# Categories
# ---------------------------------------------------------------------------

#: The four factor families defined in the design.
FACTOR_CATEGORIES: tuple[str, ...] = (
    "time_series",
    "cross_section",
    "event",
    "alternative",
)

#: Catch-all for factors that do not fit the four families.
DEFAULT_CATEGORY: str = "custom"

#: Every legal ``category`` value (four families + default).
VALID_CATEGORIES: frozenset[str] = frozenset(FACTOR_CATEGORIES) | {DEFAULT_CATEGORY}

# ---------------------------------------------------------------------------
# Dependency prefix contract
# ---------------------------------------------------------------------------

#: Prefix that marks an upstream-factor dependency.
FACTOR_DEP_PREFIX: str = "factor:"

#: Returned by :func:`parse_dependency` for a raw data table.
KIND_TABLE: str = "table"
#: Returned by :func:`parse_dependency` for an upstream factor.
KIND_FACTOR: str = "factor"

# ---------------------------------------------------------------------------
# Factor naming — ``{type}_{semantic}_{params}`` snake_case
# (factor_governance.md §62.6).  Structural check: at least two
# underscore-separated segments, every segment contains a letter, whole name
# is lowercase.  Rejects ``mom20``, ``Momentum20``, ``factor_001``, etc.
# Whether a structurally valid name is semantically right is a review-time
# judgement, not a regex.
# ---------------------------------------------------------------------------

_NAME_RE: re.Pattern[str] = re.compile(
    r"^[a-z][a-z0-9]*(?:_[a-z0-9]*[a-z][a-z0-9]*)+$"
)

# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------

_META_FIELDS: tuple[str, ...] = (
    "name",
    "description",
    "category",
    "frequency",
    "asset_types",
    "version",
)


# ---------------------------------------------------------------------------
# Dependency parsing
# ---------------------------------------------------------------------------


def parse_dependency(dep: str) -> tuple[str, str]:
    """Parse one ``depends_on()`` entry into ``(kind, name)``.

    ==================  ====================  =========================
    input                kind                  name
    ==================  ====================  =========================
    ``"bars_daily"``     :data:`KIND_TABLE`    ``"bars_daily"``
    ``"factor:mom_20d"`` :data:`KIND_FACTOR`   ``"mom_20d"``
    ==================  ====================  =========================

    Anything with a colon whose prefix is not ``"factor"`` is **rejected** —
    the silent fallback of treating ``"foo:bar"`` as a table name is exactly
    how a typo turns into a wrong-data bug three stages later.

    Raises
    ------
    ValueError
        Empty / whitespace-only input, an unrecognised prefix, or a
        ``factor:`` entry with an empty name.
    """
    if not isinstance(dep, str) or not dep.strip():
        raise ValueError(f"dependency must be a non-empty string, got {dep!r}")

    dep = dep.strip()

    if ":" not in dep:
        return KIND_TABLE, dep

    prefix, _, name = dep.partition(":")
    prefix = prefix.strip()
    name = name.strip()

    if prefix == FACTOR_DEP_PREFIX.rstrip(":"):
        if not name:
            raise ValueError(f"factor dependency {dep!r} has an empty factor name")
        return KIND_FACTOR, name

    raise ValueError(
        f"unrecognised dependency prefix {prefix!r} in {dep!r}; "
        f"only 'factor:' is a recognised prefix — bare names are raw data tables"
    )


# ---------------------------------------------------------------------------
# Metadata extraction
# ---------------------------------------------------------------------------


def factor_meta(cls: type) -> dict[str, Any]:
    """Extract the metadata class attributes from a Factor subclass.

    Returns a dict with keys :data:`_META_FIELDS`.  A missing attribute
    becomes ``None``.
    """
    return {f: getattr(cls, f, None) for f in _META_FIELDS}


def extract_factor_meta(
    factors: Iterable[type] | None = None,
) -> list[dict[str, Any]]:
    """Extract metadata from Factor classes.

    Distinct from :func:`core.registry.list_factor_meta` to avoid a
    same-name / different-shape collision.  This returns the six Factor
    interface fields; the registry extractor returns the class-level
    ``META_FIELDS`` contract and deliberately omits ``depends_on``, which is a
    method here and so is not ``getattr``-able metadata.

    Parameters
    ----------
    factors
        Factor classes to read.  When *None*, walks every class in
        :class:`core.registry.FactorRegistry` (the code-is-registry store
        landed at S1-T3).

    Returns
    -------
    list of dict
        One :func:`factor_meta` dict per class.
    """
    if factors is None:
        from core.registry import FactorRegistry

        factors = (FactorRegistry.get_class(n) for n in FactorRegistry.list_all())
    return [factor_meta(f) for f in factors]


# ---------------------------------------------------------------------------
# Output schema validation
# ---------------------------------------------------------------------------

_OUTPUT_KEY_COLS: tuple[str, ...] = ("date", "ticker")


def validate_output(factor_name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Validate that *df* matches the contract ``DataFrame[date, ticker, {name}]``.

    The output is a **long** table with exactly three columns in the order
    ``date, ticker, {factor_name}``.  Returns *df* unchanged on success.

    Raises
    ------
    ValueError
        If the columns are not exactly ``["date", "ticker", factor_name]``.
    """
    expected = [*_OUTPUT_KEY_COLS, factor_name]
    actual = list(df.columns)
    if actual != expected:
        raise ValueError(
            f"compute() must return columns {expected}, got {actual}"
        )
    return df


# ---------------------------------------------------------------------------
# Factor ABC
# ---------------------------------------------------------------------------


class Factor(ABC):
    """Abstract base class for every factor.

    Metadata is declared as class attributes — code IS the registry.
    Subclasses must set :attr:`name` and implement :meth:`depends_on` and
    :meth:`compute`.

    ``depends_on()`` is a **static declaration** of input names, not a
    subscription.  ``compute()`` returns raw values; post-processing is S5.
    """

    #: Unique factor name — ``{type}_{semantic}_{params}`` snake_case.
    name: str

    #: One-line human description (the compute docstring is the authority).
    description: str = ""

    #: Factor family — one of :data:`FACTOR_CATEGORIES` or :data:`DEFAULT_CATEGORY`.
    category: str = DEFAULT_CATEGORY

    #: Computation frequency (``"daily"``, ``"minute"``, …).
    frequency: str = "daily"

    #: Table-family selectors: ``equity`` / ``futures`` / ``continuous``.
    #: This is the same vocabulary as ``load_bars(..., asset_type=)`` — **not**
    #: ``core.types.AssetType``'s six-member enum.  Bridge through
    #: ``core.types.EQUITY_TYPES`` when per-asset rules (fees, price limits)
    #: are needed.
    #:
    #: An immutable tuple on purpose: a class-level mutable default is shared
    #: by every subclass and instance, so one ``append`` would rewrite the
    #: asset-family selector for all factors at once.
    asset_types: tuple[str, ...] = ("equity",)

    #: Semantic version.  Bump when ``compute()`` logic changes (see PR notes).
    version: str = "1.0"

    # -- class-construction-time validation -------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        name = getattr(cls, "name", None)
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"{cls.__name__} must define a non-empty 'name' class attribute"
            )
        if not _NAME_RE.match(name):
            raise ValueError(
                f"factor name {name!r} does not match "
                f"{{type}}_{{semantic}}_{{params}} snake_case "
                f"(lowercase, at least two segments, every segment has a letter)"
            )

        category = getattr(cls, "category", DEFAULT_CATEGORY)
        if category not in VALID_CATEGORIES:
            raise ValueError(
                f"invalid category {category!r} on {cls.__name__}; "
                f"expected one of {sorted(VALID_CATEGORIES)}"
            )

    # -- abstract IO contract --------------------------------------------

    @abstractmethod
    def depends_on(self) -> list[str]:
        """Declare input names as a flat list of strings.

        Bare name → raw data table (loaded via ``load_bars()``).
        ``"factor:xxx"`` → upstream factor.

        This is a static declaration, **not** a subscription.
        """
        ...

    @abstractmethod
    def compute(self, data: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Compute raw factor values.

        Parameters
        ----------
        data
            Mapping from each name in :meth:`depends_on` to its DataFrame.
            Data tables arrive from ``load_bars()``; upstream factors arrive
            from FactorDAG.

        Returns
        -------
        pd.DataFrame
            Long table with exactly three columns:
            ``[date, ticker, {factor_name}]``.  Raw values — no winsorising,
            filling, standardising or neutralising (those are S5).
        """
        ...
