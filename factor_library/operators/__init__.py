# -*- coding: utf-8 -*-
"""算子库"""
from factor_library.operators.time_series import *
from factor_library.operators.cross_section import *

__all__ = [
    # Time-series
    'ts_mean', 'ts_sum', 'ts_std', 'ts_rank',
    'ts_decay_linear', 'ts_max', 'ts_min',
    'ts_argmax', 'ts_argmin',
    # Cross-sectional
    'cs_rank', 'cs_zscore', 'cs_norm',
    'cs_winsorize', 'cs_demean', 'cs_neutralize'
]
