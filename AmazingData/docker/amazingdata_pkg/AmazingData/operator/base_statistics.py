# -*- coding: utf-8 -*-
"""
统计函数的 numba 加速实现
"""
import numpy as np
from numba import njit, prange


@njit(cache=True)
def _rolling_std_fast(arr: np.ndarray, n: int, ddof: int) -> np.ndarray:
    """使用Welford算法的快速标准差计算 - 极致优化版，O(n)复杂度"""
    length = len(arr)
    result = np.empty(length)

    sum_x = 0.0
    sum_x2 = 0.0

    for i in range(length):
        sum_x += arr[i]
        sum_x2 += arr[i] * arr[i]

        window_size = min(i + 1, n)
        if i >= n:
            sum_x -= arr[i - n]
            sum_x2 -= arr[i - n] * arr[i - n]

        mean_val = sum_x / window_size
        var_val = sum_x2 / window_size - mean_val * mean_val

        if ddof == 1 and window_size > 1:
            var_val = var_val * window_size / (window_size - 1)

        result[i] = np.sqrt(max(0.0, var_val))

    return result


@njit(cache=True)
def _rolling_var_fast(arr: np.ndarray, n: int, ddof: int) -> np.ndarray:
    """使用滑动窗口累积和的快速方差计算 - 极致优化版，O(n)复杂度"""
    length = len(arr)
    result = np.empty(length)

    sum_x = 0.0
    sum_x2 = 0.0

    for i in range(length):
        sum_x += arr[i]
        sum_x2 += arr[i] * arr[i]

        window_size = min(i + 1, n)
        if i >= n:
            sum_x -= arr[i - n]
            sum_x2 -= arr[i - n] * arr[i - n]

        mean_val = sum_x / window_size
        var_val = sum_x2 / window_size - mean_val * mean_val

        if ddof == 1 and window_size > 1:
            var_val = var_val * window_size / (window_size - 1)

        result[i] = max(0.0, var_val)

    return result


@njit(cache=True)
def _rolling_corr_fast(arr_x: np.ndarray, arr_y: np.ndarray, n: int) -> np.ndarray:
    """使用滑动窗口累积和的快速相关系数计算 - 极致优化版，O(n)复杂度"""
    length = len(arr_x)
    result = np.empty(length)
    result[:] = np.nan

    sum_x = 0.0
    sum_y = 0.0
    sum_x2 = 0.0
    sum_y2 = 0.0
    sum_xy = 0.0

    for i in range(length):
        sum_x += arr_x[i]
        sum_y += arr_y[i]
        sum_x2 += arr_x[i] * arr_x[i]
        sum_y2 += arr_y[i] * arr_y[i]
        sum_xy += arr_x[i] * arr_y[i]

        window_size = min(i + 1, n)
        if i >= n:
            sum_x -= arr_x[i - n]
            sum_y -= arr_y[i - n]
            sum_x2 -= arr_x[i - n] * arr_x[i - n]
            sum_y2 -= arr_y[i - n] * arr_y[i - n]
            sum_xy -= arr_x[i - n] * arr_y[i - n]

        mean_x = sum_x / window_size
        mean_y = sum_y / window_size
        var_x = sum_x2 / window_size - mean_x * mean_x
        var_y = sum_y2 / window_size - mean_y * mean_y
        cov_xy = sum_xy / window_size - mean_x * mean_y

        if var_x > 0 and var_y > 0:
            result[i] = cov_xy / np.sqrt(var_x * var_y)

    return result


@njit(cache=True)
def _rolling_apply_numba(arr: np.ndarray, n: int, min_periods: int, func_type: int) -> np.ndarray:
    """
    使用numba加速的滑动窗口计算 - 极致优化版，O(n)复杂度
    func_type: 0=std(ddof=0), 1=std(ddof=1), 2=var(ddof=0), 3=var(ddof=1), 4=median
    """
    length = len(arr)
    result = np.empty(length)
    result[:] = np.nan

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        if window_size >= min_periods:
            if func_type == 4:  # median
                # 收集窗口数据并排序
                window = arr[start:i + 1].copy()
                window.sort()
                mid = window_size // 2
                if window_size % 2 == 0:
                    result[i] = (window[mid - 1] + window[mid]) / 2.0
                else:
                    result[i] = window[mid]
            else:
                # 计算均值
                sum_val = 0.0
                for j in range(start, i + 1):
                    sum_val += arr[j]
                mean_val = sum_val / window_size

                if func_type <= 3:  # std or var
                    sum_sq = 0.0
                    for j in range(start, i + 1):
                        diff = arr[j] - mean_val
                        sum_sq += diff * diff

                    if func_type == 0:  # std ddof=0
                        result[i] = np.sqrt(sum_sq / window_size)
                    elif func_type == 1:  # std ddof=1
                        if window_size > 1:
                            result[i] = np.sqrt(sum_sq / (window_size - 1))
                    elif func_type == 2:  # var ddof=0
                        result[i] = sum_sq / window_size
                    elif func_type == 3:  # var ddof=1
                        if window_size > 1:
                            result[i] = sum_sq / (window_size - 1)

    return result


@njit(cache=True)
def _rolling_avedev(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动平均绝对偏差 - O(n)优化版，使用累积和"""
    length = len(arr)
    result = np.empty(length)

    # 预计算累积和
    cumsum = np.nancumsum(arr)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        # 使用累积和计算均值，O(1)
        if start == 0:
            sum_val = cumsum[i]
        else:
            sum_val = cumsum[i] - cumsum[start - 1]
        mean_val = sum_val / window_size

        # 计算平均绝对偏差
        sum_abs_dev = 0.0
        for j in range(start, i + 1):
            sum_abs_dev += abs(arr[j] - mean_val)
        result[i] = sum_abs_dev / window_size

    return result


@njit(cache=True)
def _rolling_devsq(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动偏差平方和 - O(n)优化版，使用累积和"""
    length = len(arr)
    result = np.empty(length)

    # 预计算累积和与累积平方和
    cumsum = np.nancumsum(arr)
    cumsum_sq = np.nancumsum(arr * arr)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        # 使用累积和计算均值，O(1)
        if start == 0:
            sum_val = cumsum[i]
            sum_sq = cumsum_sq[i]
        else:
            sum_val = cumsum[i] - cumsum[start - 1]
            sum_sq = cumsum_sq[i] - cumsum_sq[start - 1]

        mean_val = sum_val / window_size
        # 计算偏差平方和：E[(X-mean)^2] = E[X^2] - mean^2
        result[i] = sum_sq - window_size * mean_val * mean_val

    return result


@njit(cache=True)
def _rolling_forcast(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动线性回归预测值 - O(n)优化版，使用累积和"""
    length = len(arr)
    result = np.full(length, np.nan)

    if length == 0:
        return result

    # 预计算累积和
    cumsum = np.nancumsum(arr)
    # 预计算加权和：j * arr[j]
    weighted_arr = np.arange(length, dtype=np.float64) * arr
    cumsum_weighted = np.nancumsum(weighted_arr)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1
        m = window_size

        if m < 2:
            result[i] = arr[i] if m > 0 else np.nan
        else:
            # 使用公式计算
            sum_y = m * (m - 1) / 2
            sum_y2 = (m - 1) * m * (2 * m - 1) / 6

            # 使用累积和计算sum_x和sum_xy，O(1)
            if start == 0:
                sum_x = cumsum[i]
                sum_xy = cumsum_weighted[i]
            else:
                sum_x = cumsum[i] - cumsum[start - 1]
                sum_xy = cumsum_weighted[i] - cumsum_weighted[start - 1]

            denom = m * sum_y2 - sum_y * sum_y
            if abs(denom) < 1e-10:
                result[i] = np.nan
            else:
                slope = (m * sum_xy - sum_y * sum_x) / denom
                intercept = (sum_x - slope * sum_y) / m
                result[i] = slope * (m - 1) + intercept

    return result


@njit(cache=True)
def _rolling_slope(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动线性回归斜率 - O(n)优化版，使用累积和"""
    length = len(arr)
    result = np.full(length, np.nan)

    if length == 0:
        return result

    # 预计算累积和
    cumsum = np.nancumsum(arr)
    weighted_arr = np.arange(length, dtype=np.float64) * arr
    cumsum_weighted = np.nancumsum(weighted_arr)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1
        m = window_size

        if m >= 2:
            sum_y = m * (m - 1) / 2
            sum_y2 = (m - 1) * m * (2 * m - 1) / 6

            # 使用累积和计算，O(1)
            if start == 0:
                sum_x = cumsum[i]
                sum_xy = cumsum_weighted[i]
            else:
                sum_x = cumsum[i] - cumsum[start - 1]
                sum_xy = cumsum_weighted[i] - cumsum_weighted[start - 1]

            denom = m * sum_y2 - sum_y * sum_y
            if abs(denom) < 1e-10:
                result[i] = np.nan
            else:
                result[i] = (m * sum_xy - sum_y * sum_x) / denom

    return result


@njit(cache=True)
def _rolling_covar(arr_x: np.ndarray, arr_y: np.ndarray, n: int) -> np.ndarray:
    """滑动协方差 - O(n)优化版，使用累积和"""
    length = len(arr_x)
    result = np.empty(length)

    if length == 0:
        return result

    # 预计算累积和
    cumsum_x = np.nancumsum(arr_x)
    cumsum_y = np.nancumsum(arr_y)
    cumsum_xy = np.nancumsum(arr_x * arr_y)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        # 使用累积和计算均值，O(1)
        if start == 0:
            sum_x = cumsum_x[i]
            sum_y = cumsum_y[i]
            sum_xy = cumsum_xy[i]
        else:
            sum_x = cumsum_x[i] - cumsum_x[start - 1]
            sum_y = cumsum_y[i] - cumsum_y[start - 1]
            sum_xy = cumsum_xy[i] - cumsum_xy[start - 1]

        mean_x = sum_x / window_size
        mean_y = sum_y / window_size

        # 协方差：E[XY] - E[X]E[Y]
        result[i] = sum_xy / window_size - mean_x * mean_y

    return result


@njit(cache=True)
def _rolling_relate(arr_x: np.ndarray, arr_y: np.ndarray, n: int) -> np.ndarray:
    """滑动相关系数 - O(n)优化版，使用累积和"""
    length = len(arr_x)
    result = np.full(length, np.nan)

    if length == 0:
        return result

    # 预计算累积和
    cumsum_x = np.nancumsum(arr_x)
    cumsum_y = np.nancumsum(arr_y)
    cumsum_xy = np.nancumsum(arr_x * arr_y)
    cumsum_x2 = np.nancumsum(arr_x * arr_x)
    cumsum_y2 = np.nancumsum(arr_y * arr_y)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        if window_size >= 2:
            # 使用累积和计算，O(1)
            if start == 0:
                sum_x = cumsum_x[i]
                sum_y = cumsum_y[i]
                sum_xy = cumsum_xy[i]
                sum_x2 = cumsum_x2[i]
                sum_y2 = cumsum_y2[i]
            else:
                sum_x = cumsum_x[i] - cumsum_x[start - 1]
                sum_y = cumsum_y[i] - cumsum_y[start - 1]
                sum_xy = cumsum_xy[i] - cumsum_xy[start - 1]
                sum_x2 = cumsum_x2[i] - cumsum_x2[start - 1]
                sum_y2 = cumsum_y2[i] - cumsum_y2[start - 1]

            mean_x = sum_x / window_size
            mean_y = sum_y / window_size

            # 相关系数：cov / (std_x * std_y)
            cov = sum_xy / window_size - mean_x * mean_y
            var_x = sum_x2 / window_size - mean_x * mean_x
            var_y = sum_y2 / window_size - mean_y * mean_y

            if var_x > 1e-10 and var_y > 1e-10:
                result[i] = cov / np.sqrt(var_x * var_y)

    return result


@njit(cache=True)
def _rolling_beta(arr_x: np.ndarray, arr_b: np.ndarray, n: int) -> np.ndarray:
    """滑动贝塔系数 - O(n)优化版，使用累积和"""
    length = len(arr_x)
    result = np.full(length, np.nan)

    if length == 0:
        return result

    # 预计算累积和
    cumsum_x = np.nancumsum(arr_x)
    cumsum_b = np.nancumsum(arr_b)
    cumsum_xb = np.nancumsum(arr_x * arr_b)
    cumsum_b2 = np.nancumsum(arr_b * arr_b)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        # 使用累积和计算均值，O(1)
        if start == 0:
            sum_x = cumsum_x[i]
            sum_b = cumsum_b[i]
            sum_xb = cumsum_xb[i]
            sum_b2 = cumsum_b2[i]
        else:
            sum_x = cumsum_x[i] - cumsum_x[start - 1]
            sum_b = cumsum_b[i] - cumsum_b[start - 1]
            sum_xb = cumsum_xb[i] - cumsum_xb[start - 1]
            sum_b2 = cumsum_b2[i] - cumsum_b2[start - 1]

        mean_x = sum_x / window_size
        mean_b = sum_b / window_size

        # 协方差和方差
        cov = sum_xb / window_size - mean_x * mean_b
        var_b = sum_b2 / window_size - mean_b * mean_b

        if var_b > 1e-10:
            result[i] = cov / var_b

    return result


@njit(cache=True)
def _rolling_kurtosis(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动峰度 - O(n)优化版，使用累积和"""
    length = len(arr)
    result = np.full(length, np.nan)

    if length == 0:
        return result

    # 预计算累积和
    cumsum = np.nancumsum(arr)
    cumsum_sq = np.nancumsum(arr * arr)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1
        m = window_size

        if m >= 4:
            # 使用累积和计算均值，O(1)
            if start == 0:
                sum_val = cumsum[i]
                sum_sq = cumsum_sq[i]
            else:
                sum_val = cumsum[i] - cumsum[start - 1]
                sum_sq = cumsum_sq[i] - cumsum_sq[start - 1]

            mean_val = sum_val / m
            # 方差：E[X^2] - E[X]^2
            var_val = sum_sq / m - mean_val * mean_val
            std_val = np.sqrt(max(0.0, var_val))

            if std_val > 1e-10:
                # 计算标准化后的四阶矩
                z4_sum = 0.0
                for j in range(start, i + 1):
                    z = (arr[j] - mean_val) / std_val
                    z4_sum += z ** 4
                kurt = (m * (m + 1) / ((m - 1) * (m - 2) * (m - 3))) * z4_sum - \
                       3 * (m - 1) ** 2 / ((m - 2) * (m - 3))
                result[i] = kurt

    return result


@njit(cache=True)
def _rolling_skew(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动偏度 - O(n)优化版，使用累积和"""
    length = len(arr)
    result = np.full(length, np.nan)

    if length == 0:
        return result

    # 预计算累积和
    cumsum = np.nancumsum(arr)
    cumsum_sq = np.nancumsum(arr * arr)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1
        m = window_size

        if m >= 3:
            # 使用累积和计算均值，O(1)
            if start == 0:
                sum_val = cumsum[i]
                sum_sq = cumsum_sq[i]
            else:
                sum_val = cumsum[i] - cumsum[start - 1]
                sum_sq = cumsum_sq[i] - cumsum_sq[start - 1]

            mean_val = sum_val / m
            var_val = sum_sq / m - mean_val * mean_val
            std_val = np.sqrt(max(0.0, var_val))

            if std_val > 1e-10:
                # 计算标准化后的三阶矩
                z3_sum = 0.0
                for j in range(start, i + 1):
                    z = (arr[j] - mean_val) / std_val
                    z3_sum += z ** 3
                skew = (m / ((m - 1) * (m - 2))) * z3_sum
                result[i] = skew

    return result


@njit(cache=True)
def _rolling_quantile(arr: np.ndarray, n: int, q: float) -> np.ndarray:
    """滑动分位数 - 优化版，减少内存分配"""
    length = len(arr)
    result = np.full(length, np.nan)

    # 预分配最大窗口大小的缓冲区（重用）
    max_window_size = min(n, length)
    window_buffer = np.empty(max_window_size)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        # 使用缓冲区切片
        window = window_buffer[:window_size]
        for j in range(window_size):
            window[j] = arr[start + j]

        sorted_win = np.sort(window)
        idx = q * (window_size - 1)
        lower = int(np.floor(idx))
        upper = int(np.ceil(idx))

        if lower == upper:
            result[i] = sorted_win[lower]
        else:
            result[i] = sorted_win[lower] * (upper - idx) + sorted_win[upper] * (idx - lower)

    return result
