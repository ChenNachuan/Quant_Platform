"""Data source adapters — one subclass per external source.

Register new sources in :data:`REGISTRY`; access via :func:`get_adapter`.
"""

from __future__ import annotations

from data.adapters.akshare_adapter import AkshareAdapter
from data.adapters.amazingdata_adapter import AmazingdataAdapter
from data.adapters.tushare_adapter import TushareAdapter
from data.source_adapter import SourceAdapter

__all__ = ["REGISTRY", "get_adapter"]

REGISTRY: dict[str, type[SourceAdapter]] = {
    "akshare": AkshareAdapter,
    "tushare": TushareAdapter,
    "amazingdata": AmazingdataAdapter,
}


def get_adapter(source: str, **kwargs) -> SourceAdapter:
    """Instantiate and return the adapter for *source*.

    Raises ``KeyError`` for an unregistered source name.
    """
    cls = REGISTRY[source]
    return cls(**kwargs)
