# -*- coding: utf-8 -*-
"""
因子后处理器
"""
import pandas as pd
import numpy as np
import warnings
from typing import Literal


class PostProcessor:
    """因子后处理器"""
    
    @staticmethod
    def winsorize(series: pd.Series, lower: float = 0.01, upper: float = 0.99) -> pd.Series:
        """
        去极值（Winsorize）
        
        Args:
            series: 输入序列
            lower: 下分位数 (默认1%)
            upper: 上分位数 (默认99%)
        
        Returns:
            去极值后的序列
        """
        lower_bound = series.quantile(lower)
        upper_bound = series.quantile(upper)
        return series.clip(lower_bound, upper_bound)
    
    @staticmethod
    def standardize(series: pd.Series, method: Literal['zscore', 'minmax'] = 'zscore') -> pd.Series:
        """
        标准化
        
        Args:
            series: 输入序列
            method: 标准化方法
                - 'zscore': Z-Score标准化
                - 'minmax': Min-Max归一化
        
        Returns:
            标准化后的序列
        """
        if method == 'zscore':
            mean = series.mean()
            std = series.std()
            if std == 0:
                warnings.warn("标准差为0，无法进行Z-Score标准化")
                return series
            return (series - mean) / std
        
        elif method == 'minmax':
            min_val = series.min()
            max_val = series.max()
            if min_val == max_val:
                warnings.warn("最大值等于最小值，无法进行Min-Max归一化")
                return series
            return (series - min_val) / (max_val - min_val)
        
        else:
            raise ValueError(f"未知标准化方法: {method}")
    
    @staticmethod
    def check_validity(series: pd.Series, max_nan_ratio: float = 0.1,
                      max_inf_ratio: float = 0.01) -> dict:
        """
        有效性检查
        
        Args:
            series: 输入序列
            max_nan_ratio: 最大NaN比例阈值
            max_inf_ratio: 最大Inf比例阈值
        
        Returns:
            检查结果字典
        """
        total = len(series)
        
        # 检查 NaN
        nan_count = series.isna().sum()
        nan_ratio = nan_count / total if total > 0 else 0
        
        # 检查 Inf
        inf_count = np.isinf(series.replace([np.nan], 0)).sum()
        inf_ratio = inf_count / total if total > 0 else 0
        
        # 发出警告
        if nan_ratio > max_nan_ratio:
            warnings.warn(
                f"NaN 比例过高: {nan_ratio:.2%} (阈值: {max_nan_ratio:.2%})"
            )
        
        if inf_ratio > max_inf_ratio:
            warnings.warn(
                f"Inf 比例过高: {inf_ratio:.2%} (阈值: {max_inf_ratio:.2%})"
            )
        
        return {
            'nan_count': nan_count,
            'nan_ratio': nan_ratio,
            'inf_count': inf_count,
            'inf_ratio': inf_ratio,
            'valid': nan_ratio <= max_nan_ratio and inf_ratio <= max_inf_ratio
        }
    
    @staticmethod
    def fillna(series: pd.Series, method: Literal['ffill', 'bfill', 'mean', 'zero'] = 'ffill') -> pd.Series:
        """
        填充NaN值
        
        Args:
            series: 输入序列
            method: 填充方法
                - 'ffill': 前向填充
                - 'bfill': 后向填充
                - 'mean': 均值填充
                - 'zero': 零填充
        
        Returns:
            填充后的序列
        """
        if method == 'ffill':
            return series.ffill()
        elif method == 'bfill':
            return series.bfill()
        elif method == 'mean':
            return series.fillna(series.mean())
        elif method == 'zero':
            return series.fillna(0)
        else:
            raise ValueError(f"未知填充方法: {method}")


__all__ = ['PostProcessor']
