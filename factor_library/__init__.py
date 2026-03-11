# -*- coding: utf-8 -*-
"""
因子库
"""
from factor_library.base import Factor
from factor_library.registry import FactorRegistry, register_factor, generate_factor_id
from factor_library.dag import FactorDAG
from factor_library.preprocessor import PostProcessor
from factor_library.universe import UniverseFilter, UniverseConfig

__all__ = [
    'Factor',
    'FactorRegistry',
    'register_factor',
    'generate_factor_id',
    'FactorDAG',
    'PostProcessor',
    'UniverseFilter',
    'UniverseConfig',
]

__version__ = '0.1.0'
