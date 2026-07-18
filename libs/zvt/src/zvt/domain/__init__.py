# -*- coding: utf-8 -*-
import enum


class BlockCategory(enum.Enum):
    #: 行业版块
    industry = "industry"
    #: 概念版块
    concept = "concept"
    #: 区域版块
    area = "area"


class IndexCategory(enum.Enum):
    #: 规模指数
    scope = "scope"
    #: 行业指数
    industry = "industry"
    #: 风格指数
    style = "style"
    #: 基金指数
    fund = "fund"


class ReportPeriod(enum.Enum):
    season1 = "season1"
    season2 = "season2"
    season3 = "season3"
    season4 = "season4"
    half_year = "half_year"
    year = "year"


class CompanyType(enum.Enum):
    qiye = "qiye"
    baoxian = "baoxian"
    yinhang = "yinhang"
    quanshang = "quanshang"


# the __all__ is generated
__all__ = ["BlockCategory", "IndexCategory", "ReportPeriod", "CompanyType"]

# import from submodule quotes
from .quotes import *
from .quotes import __all__ as _quotes_all

__all__ += _quotes_all

# import from submodule meta
from .meta import *
from .meta import __all__ as _meta_all

__all__ += _meta_all