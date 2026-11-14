"""数据源适配器注册表。

在 :data:`REGISTRY` 中注册新数据源，通过 :func:`get_adapter` 获取实例。
"""

from __future__ import annotations

from data.adapters.akshare_adapter import AkshareAdapter
from data.adapters.amazingdata_adapter import AmazingdataAdapter
from data.adapters.qdc_adapter import QdcAdapter
from data.adapters.straightflush_adapter import StraightFlushAdapter
from data.adapters.tushare_adapter import TushareAdapter
from data.source_adapter import SourceAdapter

__all__ = ["REGISTRY", "get_adapter"]

REGISTRY: dict[str, type[SourceAdapter]] = {
    "akshare": AkshareAdapter,
    "tushare": TushareAdapter,
    "amazingdata": AmazingdataAdapter,
    "qdc": QdcAdapter,
    "straightflush": StraightFlushAdapter,
}


def get_adapter(source: str, **kwargs) -> SourceAdapter:
    """实例化并返回指定 *source* 的适配器；未注册时抛出 ``KeyError``。"""
    cls = REGISTRY[source]
    return cls(**kwargs)
