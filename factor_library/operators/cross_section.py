# -*- coding: utf-8 -*-
"""
截面算子库
"""
import pandas as pd
import numpy as np


def cs_rank(series: pd.Series) -> pd.Series:
    """截面排名（百分位）"""
    return series.rank(pct=True)


def cs_zscore(series: pd.Series) -> pd.Series:
    """截面Z-Score标准化"""
    return (series - series.mean()) / series.std()


def cs_norm(series: pd.Series) -> pd.Series:
    """截面归一化 [0, 1]"""
    return (series - series.min()) / (series.max() - series.min())


def cs_winsorize(series: pd.Series, lower=0.01, upper=0.99) -> pd.Series:
    """
    截面去极值
    
    Args:
        series: 输入序列
        lower: 下分位数
        upper: 上分位数
    """
    lower_bound = series.quantile(lower)
    upper_bound = series.quantile(upper)
    return series.clip(lower_bound, upper_bound)


def cs_demean(series: pd.Series) -> pd.Series:
    """截面去均值"""
    return series - series.mean()


def cs_neutralize(series: pd.Series, market_cap: pd.Series) -> pd.Series:
    """
    市值中性化
    
    对因子值进行市值加权回归，返回残差
    """
    # 简化版：减去市值加权均值
    weights = market_cap / market_cap.sum()
    weighted_mean = (series * weights).sum()
    return series - weighted_mean


__all__ = [
    'cs_rank', 'cs_zscore', 'cs_norm',
    'cs_winsorize', 'cs_demean', 'cs_neutralize'
]
