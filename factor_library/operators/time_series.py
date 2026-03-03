# -*- coding: utf-8 -*-
"""
时间序列算子库
"""
import pandas as pd
import numpy as np
from typing import Union


def ts_mean(series: pd.Series, window: int) -> pd.Series:
    """时间序列均值"""
    return series.rolling(window).mean()


def ts_sum(series: pd.Series, window: int) -> pd.Series:
    """时间序列求和"""
    return series.rolling(window).sum()


def ts_std(series: pd.Series, window: int) -> pd.Series:
    """时间序列标准差"""
    return series.rolling(window).std()


def ts_rank(series: pd.Series, window: int) -> pd.Series:
    """
    时间序列排名（百分位）
    
    返回当前值在过去window期中的百分位排名
    """
    def rank_pct(x):
        if len(x) < window:
            return np.nan
        return pd.Series(x).rank().iloc[-1] / len(x)
    
    return series.rolling(window).apply(rank_pct, raw=False)


def ts_decay_linear(series: pd.Series, window: int) -> pd.Series:
    """
    线性衰减加权均值
    
    权重: 1, 2, 3, ..., window
    最近的数据权重最大
    """
    weights = np.arange(1, window + 1)
    weights = weights / weights.sum()
    
    def weighted_mean(x):
        if len(x) < window:
            return np.nan
        return np.dot(x[-window:], weights)
    
    return series.rolling(window).apply(weighted_mean, raw=True)


def ts_max(series: pd.Series, window: int) -> pd.Series:
    """时间序列最大值"""
    return series.rolling(window).max()


def ts_min(series: pd.Series, window: int) -> pd.Series:
    """时间序列最小值"""
    return series.rolling(window).min()


def ts_argmax(series: pd.Series, window: int) -> pd.Series:
    """
    时间序列最大值位置
    
    返回过去window期中最大值距离当前的天数
    """
    def argmax_dist(x):
        if len(x) < window:
            return np.nan
        return len(x) - np.argmax(x) - 1
    
    return series.rolling(window).apply(argmax_dist, raw=True)


def ts_argmin(series: pd.Series, window: int) -> pd.Series:
    """
    时间序列最小值位置
    
    返回过去window期中最小值距离当前的天数
    """
    def argmin_dist(x):
        if len(x) < window:
            return np.nan
        return len(x) - np.argmin(x) - 1
    
    return series.rolling(window).apply(argmin_dist, raw=True)


__all__ = [
    'ts_mean', 'ts_sum', 'ts_std', 'ts_rank', 
    'ts_decay_linear', 'ts_max', 'ts_min',
    'ts_argmax', 'ts_argmin'
]
