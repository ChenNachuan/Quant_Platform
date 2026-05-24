# -*- coding: utf-8 -*-
"""
截面函数的 numba 加速实现
"""
import numpy as np
from numba import njit, prange


@njit(cache=True, parallel=True)
def _numpy_rank_axis1(arr: np.ndarray, ascending: bool = True) -> np.ndarray:
    """对2D数组按行计算排名 - 极致优化版，使用排序算法"""
    n_rows, n_cols = arr.shape
    result = np.full((n_rows, n_cols), np.nan, dtype=np.float64)

    for i in prange(n_rows):
        row = arr[i]

        # 收集有效值和索引
        valid_count = 0
        for j in range(n_cols):
            if not np.isnan(row[j]):
                valid_count += 1

        if valid_count == 0:
            continue

        valid_indices = np.empty(valid_count, dtype=np.int64)
        valid_vals = np.empty(valid_count, dtype=np.float64)
        idx = 0
        for j in range(n_cols):
            if not np.isnan(row[j]):
                valid_indices[idx] = j
                valid_vals[idx] = row[j]
                idx += 1

        # 使用argsort排序，O(n log n)
        sorted_idx = np.argsort(valid_vals)
        if not ascending:
            sorted_idx = sorted_idx[::-1]

        # 计算排名，处理相同值
        ranks = np.empty(valid_count, dtype=np.float64)
        k = 0
        while k < valid_count:
            # 找到相同值的范围
            j = k
            while j < valid_count and valid_vals[sorted_idx[j]] == valid_vals[sorted_idx[k]]:
                j += 1
            # 相同值取平均排名
            avg_rank = (k + 1 + j) / 2.0
            for m in range(k, j):
                ranks[sorted_idx[m]] = avg_rank
            k = j

        # 填充结果
        for k in range(valid_count):
            result[i, valid_indices[k]] = ranks[k]

    return result


@njit(cache=True, parallel=True)
def _cs_cor_numba(arr_x: np.ndarray, arr_y: np.ndarray) -> np.ndarray:
    """截面协方差，使用numba加速 - 极致优化版"""
    n_rows, n_cols = arr_x.shape
    result = np.full(n_rows, np.nan)

    for i in prange(n_rows):
        # 一次遍历计算所有统计量
        sum_x = 0.0
        sum_y = 0.0
        n_valid = 0

        for j in range(n_cols):
            if not np.isnan(arr_x[i, j]) and not np.isnan(arr_y[i, j]):
                sum_x += arr_x[i, j]
                sum_y += arr_y[i, j]
                n_valid += 1

        if n_valid > 1:
            mean_x = sum_x / n_valid
            mean_y = sum_y / n_valid

            cov = 0.0
            for j in range(n_cols):
                if not np.isnan(arr_x[i, j]) and not np.isnan(arr_y[i, j]):
                    cov += (arr_x[i, j] - mean_x) * (arr_y[i, j] - mean_y)
            result[i] = cov / n_valid

    return result


@njit(cache=True, parallel=True)
def _cs_corr_numba(arr_x: np.ndarray, arr_y: np.ndarray) -> np.ndarray:
    """截面相关系数，使用numba加速 - 极致优化版"""
    n_rows, n_cols = arr_x.shape
    result = np.full(n_rows, np.nan)

    for i in prange(n_rows):
        # 一次遍历计算所有统计量
        sum_x = 0.0
        sum_y = 0.0
        sum_x2 = 0.0
        sum_y2 = 0.0
        sum_xy = 0.0
        n_valid = 0

        for j in range(n_cols):
            if not np.isnan(arr_x[i, j]) and not np.isnan(arr_y[i, j]):
                x_val = arr_x[i, j]
                y_val = arr_y[i, j]
                sum_x += x_val
                sum_y += y_val
                sum_x2 += x_val * x_val
                sum_y2 += y_val * y_val
                sum_xy += x_val * y_val
                n_valid += 1

        if n_valid > 1:
            mean_x = sum_x / n_valid
            mean_y = sum_y / n_valid
            var_x = sum_x2 / n_valid - mean_x * mean_x
            var_y = sum_y2 / n_valid - mean_y * mean_y
            cov_xy = sum_xy / n_valid - mean_x * mean_y

            if var_x > 0 and var_y > 0:
                result[i] = cov_xy / np.sqrt(var_x * var_y)

    return result


@njit(cache=True, parallel=True)
def _cs_pct_rank_numba(arr: np.ndarray) -> np.ndarray:
    """截面百分位排名，使用numba加速 - 极致优化版，使用排序算法"""
    n_rows, n_cols = arr.shape
    result = np.full((n_rows, n_cols), np.nan, dtype=np.float64)

    for i in prange(n_rows):
        row = arr[i]

        # 收集有效值和索引
        valid_count = 0
        for j in range(n_cols):
            if not np.isnan(row[j]):
                valid_count += 1

        if valid_count == 0:
            continue

        valid_indices = np.empty(valid_count, dtype=np.int64)
        valid_vals = np.empty(valid_count, dtype=np.float64)
        idx = 0
        for j in range(n_cols):
            if not np.isnan(row[j]):
                valid_indices[idx] = j
                valid_vals[idx] = row[j]
                idx += 1

        # 使用argsort排序，O(n log n)
        sorted_idx = np.argsort(valid_vals)

        # 计算排名，处理相同值
        ranks = np.empty(valid_count, dtype=np.float64)
        k = 0
        while k < valid_count:
            # 找到相同值的范围
            j = k
            while j < valid_count and valid_vals[sorted_idx[j]] == valid_vals[sorted_idx[k]]:
                j += 1
            # 相同值取平均排名
            avg_rank = (k + 1 + j) / 2.0
            for m in range(k, j):
                ranks[sorted_idx[m]] = avg_rank
            k = j

        # 填充结果，转换为百分位
        for k in range(valid_count):
            result[i, valid_indices[k]] = ranks[k] / valid_count

    return result
