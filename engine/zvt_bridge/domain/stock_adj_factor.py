# -*- coding: utf-8 -*-
from sqlalchemy import Column, String, Float
from sqlalchemy.orm import declarative_base
from zvt.contract import Mixin
from zvt.contract.register import register_schema

StockAdjFactorBase = declarative_base()

class StockAdjFactor(StockAdjFactorBase, Mixin):
    __tablename__ = 'stock_adj_factor'

    provider = Column(String(length=32))
    code = Column(String(length=32))
    name = Column(String(length=32))
    level = Column(String(length=32))

    hfq_factor = Column(Float)

register_schema(providers=['akshare'], db_name='stock_adj_factor', schema_base=StockAdjFactorBase, entity_type='stock')

__all__ = ['StockAdjFactor']
