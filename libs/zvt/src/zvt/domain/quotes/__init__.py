# -*- coding: utf-8 -*-
from sqlalchemy import String, Column, Float, Integer, JSON, Boolean

from zvt.contract import Mixin


class KdataCommon(Mixin):
    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))
    level = Column(String(length=32))

    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Float)
    turnover = Column(Float)
    change_pct = Column(Float)
    turnover_rate = Column(Float)


class StockKdataCommon(KdataCommon):
    is_limit_up = Column(Boolean)
    is_limit_down = Column(Boolean)


# the __all__ is generated
__all__ = [
    "KdataCommon",
    "StockKdataCommon",
]

# import from submodule stock
from .stock import *
from .stock import __all__ as _stock_all

__all__ += _stock_all