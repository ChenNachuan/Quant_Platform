"""Shared registry mechanism — code IS the registry.

Registration is a decorator plus class attributes; metadata is extracted
from the class (design/core/registry.md).  No independent registry store:
factors number under 100, so ``list_all()`` traversal is enough, and a
separate store only creates a code-vs-registry sync problem.

Domain-agnostic by construction (red line 3): this module imports no
domain.  ``FactorRegistry.register`` accepts any class — the ``Factor``
base class lands at S4 and is never required here.  Registration is a
decorator, not a publish/subscribe channel (red line 2).  Exactly two
registries live here: :class:`FactorRegistry` and :class:`RuleRegistry`.
alphaType registration is a separate JSON double-table (S11) and is
deliberately absent.
"""

#: Class-level metadata, read with ``getattr``.  A missing attribute becomes
#: ``None``; a callable becomes ``None`` too — the Factor contract declares
#: ``depends_on`` as a **method**, and handing a function object back as
#: metadata is both wrong and not JSON-serializable.  This module may not
#: import ``research`` to call it (red line 3), so a method-valued field is
#: reported as not-extractable rather than mis-extracted.
META_FIELDS = [
    "name",
    "version",
    "description",
    "category",
    "frequency",
    "asset_types",
    "depends_on",
    "status",
    "created_at",
]

# status three-state: draft -> production -> deprecated.  Not the company's
# 5-colour light; do not invent a fourth state.
VALID_STATUSES = ("draft", "production", "deprecated")


class _Registry:
    """Shared class-registration mechanism.

    Each concrete registry subclasses this and owns its ``_store``, so
    ``FactorRegistry`` and ``RuleRegistry`` stay independent.  Methods are
    classmethods so they reach the right store; the call shape is exactly
    ``FactorRegistry.register(cls)`` / ``.list_all()`` / ``.get_class(n)`` /
    ``.create_instance(n, **kw)``.
    """

    _store: dict[str, type] = {}

    @classmethod
    def register(cls, klass: type) -> type:
        """Register *klass* and return it (usable as ``@...register``).

        The registry key is ``klass.name`` when that attribute is set,
        else ``klass.__name__``.  A name already taken raises ``ValueError``
        — no silent overwrite.  An explicit ``status`` outside
        :data:`VALID_STATUSES` raises ``ValueError``; a missing ``status``
        is accepted and surfaces as ``None`` in metadata.
        """
        status = getattr(klass, "status", None)
        if status is not None and status not in VALID_STATUSES:
            raise ValueError(
                f"invalid status {status!r} on {klass.__name__}; "
                f"expected one of {VALID_STATUSES}"
            )
        key = getattr(klass, "name", None) or klass.__name__
        if key in cls._store:
            raise ValueError(f"{key!r} is already registered in {cls.__name__}")
        cls._store[key] = klass
        return klass

    @classmethod
    def list_all(cls) -> list[str]:
        """Return every registered key, in registration order."""
        return list(cls._store)

    @classmethod
    def get_class(cls, name: str) -> type:
        """Return the class registered under *name*; ``KeyError`` if absent."""
        return cls._store[name]

    @classmethod
    def create_instance(cls, name: str, **kwargs):
        """Instantiate the class registered under *name* with *kwargs*."""
        return cls.get_class(name)(**kwargs)


class FactorRegistry(_Registry):
    """Factor-class registry.

    Accepts any class — metadata is duck-typed from class attributes,
    never ``isinstance(cls, Factor)``.  ``Factor`` lands at S4.
    """

    _store: dict[str, type] = {}


class RuleRegistry(_Registry):
    """Rule-class registry.  Same shape as :class:`FactorRegistry`.

    ``Rule(ABC)`` lands at S11; the shared mechanism is small enough to
    land here now.
    """

    _store: dict[str, type] = {}


def register_factor(cls):
    """Decorator sugar for ``FactorRegistry.register``."""
    return FactorRegistry.register(cls)


def _meta_value(klass: type, field: str):
    """One metadata field, or ``None`` when it is not class data.

    ``getattr`` alone returns a *method* for a field like ``depends_on`` —
    the Factor contract declares ``def depends_on(self) -> list[str]`` — and a
    function object in a metadata row is both wrong and not
    JSON-serializable. A missing attribute and a method both mean "not
    extractable here"; neither is a failure.
    """
    value = getattr(klass, field, None)
    return None if callable(value) else value


def list_factor_meta() -> list[dict]:
    """Summarise every registered factor's metadata.

    Walks :class:`FactorRegistry` and pulls each field in :data:`META_FIELDS`;
    a missing attribute or a method becomes ``None`` and does not raise.
    Factors number under 100, so traversal is enough.
    """
    result = []
    for name in FactorRegistry.list_all():
        klass = FactorRegistry.get_class(name)
        result.append({f: _meta_value(klass, f) for f in META_FIELDS})
    return result
