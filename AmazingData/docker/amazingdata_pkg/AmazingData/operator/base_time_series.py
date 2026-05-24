# -*- coding: utf-8 -*-
"""
时序函数的 numba 加速实现
"""
import numpy as np
from numba import njit


@njit(cache=True)
def _hhv_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """N周期内最高值 - 使用单调队列优化，O(n)复杂度"""
    length = len(arr)
    result = np.empty(length)

    if n <= 0:
        result[0] = arr[0]
        for i in range(1, length):
            result[i] = max(result[i-1], arr[i])
    else:
        # 使用单调递减队列维护窗口最大值
        # 队列存储索引，队首是最大值的索引
        deque = np.empty(length, dtype=np.int64)
        head = 0
        tail = 0

        for i in range(length):
            window_size = min(i + 1, n)

            # 移除超出窗口的元素
            while head < tail and deque[head] <= i - window_size:
                head += 1

            # 移除所有小于当前元素的值（维护单调性）
            while head < tail and arr[deque[tail - 1]] <= arr[i]:
                tail -= 1

            # 添加当前元素
            deque[tail] = i
            tail += 1

            # 队首就是最大值
            result[i] = arr[deque[head]]

    return result


@njit(cache=True)
def _llv_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """N周期内最低值 - 使用单调队列优化，O(n)复杂度"""
    length = len(arr)
    result = np.empty(length)

    if n <= 0:
        result[0] = arr[0]
        for i in range(1, length):
            result[i] = min(result[i-1], arr[i])
    else:
        # 使用单调递增队列维护窗口最小值
        # 队列存储索引，队首是最小值的索引
        deque = np.empty(length, dtype=np.int64)
        head = 0
        tail = 0

        for i in range(length):
            window_size = min(i + 1, n)

            # 移除超出窗口的元素
            while head < tail and deque[head] <= i - window_size:
                head += 1

            # 移除所有大于当前元素的值（维护单调性）
            while head < tail and arr[deque[tail - 1]] >= arr[i]:
                tail -= 1

            # 添加当前元素
            deque[tail] = i
            tail += 1

            # 队首就是最小值
            result[i] = arr[deque[head]]

    return result


@njit(cache=True)
def _ma_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """简单移动平均 - 极致优化版，O(n)复杂度，支持NaN
    使用滑动窗口实现，忽略NaN值
    """
    length = len(arr)
    result = np.empty(length)

    for i in range(length):
        window_size = min(i + 1, n)
        start = i - window_size + 1

        # 计算窗口内非NaN值的和与数量
        cumsum = 0.0
        count = 0
        for j in range(start, i + 1):
            if not np.isnan(arr[j]):
                cumsum += arr[j]
                count += 1

        if count > 0:
            result[i] = cumsum / count
        else:
            result[i] = np.nan

    return result


@njit(cache=True)
def _sum_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """滑动求和 - 极致优化版，O(n)复杂度，支持NaN"""
    length = len(arr)
    result = np.empty(length)

    if n <= 0:
        # 累计求和 - 忽略NaN
        cumsum = 0.0
        for i in range(length):
            if not np.isnan(arr[i]):
                cumsum += arr[i]
            result[i] = cumsum
    else:
        # 滑动窗口求和 - 忽略NaN
        for i in range(length):
            window_size = min(i + 1, n)
            start = i - window_size + 1
            cumsum = 0.0
            for j in range(start, i + 1):
                if not np.isnan(arr[j]):
                    cumsum += arr[j]
            result[i] = cumsum

    return result


@njit(cache=True)
def _ema_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """指数移动平均 - 极致优化版，O(n)复杂度，支持NaN
    使用递推公式，避免重复计算
    """
    length = len(arr)
    result = np.full(length, np.nan)  # 使用full完全初始化

    if length == 0:
        return result

    # 找到第一个非NaN值作为初始值
    first_valid_idx = -1
    for i in range(length):
        if not np.isnan(arr[i]):
            first_valid_idx = i
            result[i] = arr[i]
            break

    if first_valid_idx == -1:
        return result  # 全为NaN，返回已初始化的数组

    alpha = 2.0 / (n + 1)

    # 递推计算
    for i in range(first_valid_idx + 1, length):
        if np.isnan(arr[i]):
            result[i] = result[i - 1]
        else:
            result[i] = alpha * arr[i] + (1 - alpha) * result[i - 1]

    return result


@njit(cache=True)
def _barslast_numba(arr: np.ndarray) -> np.ndarray:
    """上一次条件成立到当前的周期数 - 极致优化版"""
    n = len(arr)
    result = np.empty(n)
    result[:] = np.nan
    last_true = -1

    for i in range(n):
        if arr[i]:
            last_true = i
            result[i] = 0.0
        elif last_true >= 0:
            result[i] = i - last_true

    return result


@njit(cache=True)
def _barslasts_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """倒数第N次成立时距今的周期数 - 极致优化版"""
    length = len(arr)
    result = np.empty(length)
    result[:] = np.nan

    for i in range(length):
        count = 0
        for j in range(i, -1, -1):
            if arr[j]:
                count += 1
                if count == n:
                    result[i] = i - j
                    break

    return result


@njit(cache=True)
def _barsnext_numba(arr: np.ndarray) -> np.ndarray:
    """下一次条件成立到当前的周期数 - 极致优化版"""
    n = len(arr)
    result = np.empty(n)
    result[:] = np.nan

    for i in range(n - 1, -1, -1):
        for j in range(i + 1, n):
            if arr[j]:
                result[i] = j - i
                break

    return result


@njit(cache=True)
def _barssincen_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """N周期内第一个条件成立到当前的周期数 - 极致优化版"""
    length = len(arr)
    result = np.empty(length)
    result[:] = np.nan

    for i in range(length):
        start = max(0, i - n + 1)
        for j in range(start, i + 1):
            if arr[j]:
                result[i] = i - j
                break

    return result


@njit(cache=True)
def _barssince_numba(arr: np.ndarray) -> np.ndarray:
    """第一个条件成立到当前的周期数 - 极致优化版"""
    n = len(arr)
    result = np.empty(n)
    result[:] = np.nan
    first_true = -1

    for i in range(n):
        if first_true < 0 and arr[i]:
            first_true = i
        if first_true >= 0:
            result[i] = i - first_true

    return result


@njit(cache=True)
def _barslastcount_numba(arr: np.ndarray) -> np.ndarray:
    """统计连续满足条件的周期数 - 极致优化版"""
    n = len(arr)
    result = np.empty(n)
    count = 0

    for i in range(n):
        if arr[i]:
            count += 1
        else:
            count = 0
        result[i] = count

    return result


@njit(cache=True)
def _hhvbars_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """求上一高点到当前的周期数 - 优化版"""
    length = len(arr)
    result = np.zeros(length)

    if n <= 0:
        # 全局最高点
        for i in range(length):
            max_val = arr[0]
            max_idx = 0
            for j in range(1, i + 1):
                if arr[j] >= max_val:
                    max_val = arr[j]
                    max_idx = j
            result[i] = i - max_idx
    else:
        # 使用单调队列优化
        deque = np.empty(length, dtype=np.int64)
        head = 0
        tail = 0

        for i in range(length):
            window_size = min(i + 1, n)

            # 移除超出窗口的元素
            while head < tail and deque[head] <= i - window_size:
                head += 1

            # 移除所有小于等于当前元素的值
            while head < tail and arr[deque[tail - 1]] <= arr[i]:
                tail -= 1

            # 添加当前元素
            deque[tail] = i
            tail += 1

            # 队首就是最大值的索引
            result[i] = i - deque[head]

    return result


@njit(cache=True)
def _llvbars_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """求上一低点到当前的周期数 - 优化版"""
    length = len(arr)
    result = np.zeros(length)

    if n <= 0:
        # 全局最低点
        for i in range(length):
            min_val = arr[0]
            min_idx = 0
            for j in range(1, i + 1):
                if arr[j] <= min_val:
                    min_val = arr[j]
                    min_idx = j
            result[i] = i - min_idx
    else:
        # 使用单调队列优化
        deque = np.empty(length, dtype=np.int64)
        head = 0
        tail = 0

        for i in range(length):
            window_size = min(i + 1, n)

            # 移除超出窗口的元素
            while head < tail and deque[head] <= i - window_size:
                head += 1

            # 移除所有大于等于当前元素的值
            while head < tail and arr[deque[tail - 1]] >= arr[i]:
                tail -= 1

            # 添加当前元素
            deque[tail] = i
            tail += 1

            # 队首就是最小值的索引
            result[i] = i - deque[head]

    return result


@njit(cache=True)
def _hod_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """求高值名次 - 极致优化版，使用动态窗口"""
    length = len(arr)
    result = np.empty(length)

    for i in range(length):
        window_size = min(i + 1, n) if n > 0 else i + 1
        start = i - window_size + 1
        current_val = arr[i]
        rank = 1

        for j in range(start, i):
            if arr[j] > current_val:
                rank += 1
        result[i] = rank

    return result


@njit(cache=True)
def _lod_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """求低值名次 - 极致优化版，使用动态窗口"""
    length = len(arr)
    result = np.empty(length)

    for i in range(length):
        window_size = min(i + 1, n) if n > 0 else i + 1
        start = i - window_size + 1
        current_val = arr[i]
        rank = 1

        for j in range(start, i):
            if arr[j] < current_val:
                rank += 1
        result[i] = rank

    return result


@njit(cache=True)
def _sumbars_numba(arr: np.ndarray, a: float) -> np.ndarray:
    """向前累加到指定值到现在的周期数 - 极致优化版"""
    length = len(arr)
    result = np.empty(length)

    for i in range(length):
        total = 0.0
        count = 0
        for j in range(i, -1, -1):
            total += arr[j]
            count += 1
            if total >= a:
                break
        result[i] = count

    return result


@njit(cache=True)
def _sumbarsx_numba(arr: np.ndarray, a: float) -> np.ndarray:
    """向前累加到指定值到现在的周期数(未达到返回nan) - 极致优化版"""
    length = len(arr)
    result = np.empty(length)
    result[:] = np.nan

    for i in range(length):
        total = 0.0
        for j in range(i, -1, -1):
            total += arr[j]
            if total >= a:
                result[i] = i - j + 1
                break

    return result


@njit(cache=True)
def _sma_numba(arr: np.ndarray, n: int, m: int) -> np.ndarray:
    """移动平均 Y=(X*M+Y'*(N-M))/N - 极致优化版，O(n)复杂度，支持NaN"""
    length = len(arr)
    result = np.full(length, np.nan)  # 使用full完全初始化

    if length == 0:
        return result

    # 找到第一个非NaN值作为初始值
    first_valid_idx = -1
    for i in range(length):
        if not np.isnan(arr[i]):
            first_valid_idx = i
            result[i] = arr[i]
            break

    if first_valid_idx == -1:
        return result  # 全为NaN，返回已初始化的数组

    factor = m / n
    decay = (n - m) / n

    # 递推计算
    for i in range(first_valid_idx + 1, length):
        if np.isnan(arr[i]):
            result[i] = result[i - 1]  # NaN时保持上一个值
        else:
            result[i] = arr[i] * factor + result[i - 1] * decay

    return result


@njit(cache=True)
def _tma_numba(arr: np.ndarray, a: float, b: float) -> np.ndarray:
    """移动平均 Y=(A*Y'+B*X) - 极致优化版，O(n)复杂度，支持NaN"""
    length = len(arr)
    result = np.full(length, np.nan)  # 使用full完全初始化

    if length == 0:
        return result

    # 找到第一个非NaN值作为初始值
    first_valid_idx = -1
    for i in range(length):
        if not np.isnan(arr[i]):
            first_valid_idx = i
            result[i] = arr[i]
            break

    if first_valid_idx == -1:
        return result  # 全为NaN，返回已初始化的数组

    for i in range(first_valid_idx + 1, length):
        if np.isnan(arr[i]):
            result[i] = result[i - 1]
        else:
            result[i] = a * result[i - 1] + b * arr[i]

    return result


@njit(cache=True)
def _mema_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """平滑移动平均 Y=(X+Y'*(N-1))/N - 极致优化版，O(n)复杂度，支持NaN"""
    length = len(arr)
    result = np.full(length, np.nan)  # 使用full完全初始化

    if length == 0:
        return result

    # 找到第一个非NaN值作为初始值
    first_valid_idx = -1
    for i in range(length):
        if not np.isnan(arr[i]):
            first_valid_idx = i
            result[i] = arr[i]
            break

    if first_valid_idx == -1:
        return result  # 全为NaN，返回已初始化的数组

    inv_n = 1.0 / n
    decay = (n - 1) * inv_n

    for i in range(first_valid_idx + 1, length):
        if np.isnan(arr[i]):
            result[i] = result[i - 1]
        else:
            result[i] = arr[i] * inv_n + result[i - 1] * decay

    return result


@njit(cache=True)
def _dma_numba(arr_x: np.ndarray, arr_a: np.ndarray) -> np.ndarray:
    """动态移动平均 Y=A*X+(1-A)*Y' - 极致优化版，O(n)复杂度，支持NaN"""
    length = len(arr_x)
    result = np.full(length, np.nan)  # 使用full完全初始化

    if length == 0:
        return result

    # 找到第一个非NaN值作为初始值
    first_valid_idx = -1
    for i in range(length):
        if not np.isnan(arr_x[i]):
            first_valid_idx = i
            result[i] = arr_x[i]
            break

    if first_valid_idx == -1:
        return result  # 全为NaN，返回已初始化的数组

    for i in range(first_valid_idx + 1, length):
        if np.isnan(arr_x[i]):
            result[i] = result[i - 1]
        else:
            alpha = arr_a[i]
            result[i] = alpha * arr_x[i] + (1 - alpha) * result[i - 1]

    return result


@njit(cache=True)
def _ama_numba(arr_x: np.ndarray, arr_a: np.ndarray) -> np.ndarray:
    """自适应均线值 Y=Y'+A*(X-Y') - 极致优化版，O(n)复杂度，支持NaN"""
    length = len(arr_x)
    result = np.full(length, np.nan)  # 使用full完全初始化

    if length == 0:
        return result

    # 找到第一个非NaN值作为初始值
    first_valid_idx = -1
    for i in range(length):
        if not np.isnan(arr_x[i]):
            first_valid_idx = i
            result[i] = arr_x[i]
            break

    if first_valid_idx == -1:
        return result  # 全为NaN，返回已初始化的数组

    for i in range(first_valid_idx + 1, length):
        if np.isnan(arr_x[i]):
            result[i] = result[i - 1]
        else:
            result[i] = result[i - 1] + arr_a[i] * (arr_x[i] - result[i - 1])

    return result


@njit(cache=True)
def _filter_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """过滤连续出现的信号 - 极致优化版"""
    length = len(arr)
    result = np.zeros(length)
    skip_until = -1

    for i in range(length):
        if i > skip_until and arr[i]:
            result[i] = 1
            skip_until = i + n

    return result


@njit(cache=True)
def _filterx_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """反向过滤连续出现的信号 - 极致优化版"""
    length = len(arr)
    result = arr.astype(np.float64).copy()

    for i in range(length - 1, -1, -1):
        if arr[i]:
            start = max(0, i - n)
            for j in range(start, i):
                result[j] = 0

    return result


@njit(cache=True)
def _longcross_numba(arr_a: np.ndarray, arr_b: np.ndarray, n: int) -> np.ndarray:
    """两条线维持一定周期后交叉 - 优化版，使用动态窗口"""
    length = len(arr_a)
    result = np.zeros(length)

    for i in range(length):
        window_size = min(i + 1, n)
        if window_size < n:
            continue

        all_below = True
        for j in range(i - n, i):
            if arr_a[j] >= arr_b[j]:
                all_below = False
                break
        if all_below and arr_a[i] > arr_b[i]:
            result[i] = 1

    return result


@njit(cache=True)
def _upnday_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """返回周期数内是否连涨 - 优化版，使用动态窗口"""
    length = len(arr)
    result = np.zeros(length)

    for i in range(length):
        window_size = min(i + 1, n)
        if window_size < n:
            continue

        all_up = True
        for j in range(i - n + 2, i + 1):
            if arr[j] <= arr[j - 1]:
                all_up = False
                break
        if all_up:
            result[i] = 1

    return result


@njit(cache=True)
def _downnday_numba(arr: np.ndarray, n: int) -> np.ndarray:
    """返回周期数内是否连跌 - 优化版，使用动态窗口"""
    length = len(arr)
    result = np.zeros(length)

    for i in range(length):
        window_size = min(i + 1, n)
        if window_size < n:
            continue

        all_down = True
        for j in range(i - n + 2, i + 1):
            if arr[j] >= arr[j - 1]:
                all_down = False
                break
        if all_down:
            result[i] = 1

    return result


@njit(cache=True)
def _nday_numba(arr_x: np.ndarray, arr_y: np.ndarray, n: int) -> np.ndarray:
    """返回是否持续存在X>Y - 优化版，使用动态窗口"""
    length = len(arr_x)
    result = np.zeros(length)

    for i in range(length):
        window_size = min(i + 1, n)
        if window_size < n:
            continue

        all_greater = True
        for j in range(i - n + 1, i + 1):
            if arr_x[j] <= arr_y[j]:
                all_greater = False
                break
        if all_greater:
            result[i] = 1

    return result


@njit(cache=True)
def _existr_numba(arr: np.ndarray, a: int, b: int) -> np.ndarray:
    """是否存在(前几日到前几日间) - 优化版"""
    length = len(arr)
    result = np.zeros(length)

    for i in range(length):
        start = 0 if a == 0 else max(0, i - a)
        end = length if b == 0 else max(0, i - b + 1)

        if start < end:
            for j in range(start, end):
                if arr[j]:
                    result[i] = 1
                    break

    return result


@njit(cache=True)
def _last_numba(arr: np.ndarray, a: int, b: int) -> np.ndarray:
    """持续存在 - 优化版"""
    length = len(arr)
    result = np.zeros(length)

    for i in range(length):
        start = 0 if a == 0 else max(0, i - a)
        end = length if b == 0 else max(0, i - b + 1)

        if start < end:
            all_true = True
            for j in range(start, end):
                if not arr[j]:
                    all_true = False
                    break
            if all_true:
                result[i] = 1

    return result


@njit(cache=True)
def _hhvllv_numba(arr: np.ndarray, t: int, n1: int, n2: int) -> np.ndarray:
    """阶段最高最低值 - 优化版"""
    length = len(arr)
    result = np.empty(length)
    result[:] = np.nan

    for i in range(length):
        start = 0 if n1 == 0 else max(0, i - n1)
        end = length if n2 == 0 else max(0, i - n2 + 1)

        if start < end and end <= length:
            if t == 0:
                # 求最大值
                max_val = arr[start]
                for j in range(start + 1, end):
                    if arr[j] > max_val:
                        max_val = arr[j]
                result[i] = max_val
            else:
                # 求最小值
                min_val = arr[start]
                for j in range(start + 1, end):
                    if arr[j] < min_val:
                        min_val = arr[j]
                result[i] = min_val

    return result


@njit(cache=True)
def _sar_numba(high: np.ndarray, low: np.ndarray, close: np.ndarray,
               n: int, step: float, max_af: float) -> np.ndarray:
    """抛物线转向指标 SAR - 修复越界和逻辑问题"""
    length = len(close)
    result = np.full(length, np.nan)  # 使用full确保完全初始化

    # 边界检查：如果n大于等于数据长度，无法初始化
    if n >= length:
        return result

    # 动态趋势判断：使用n周期内的价格变化
    price_change = close[n] - close[0]
    # 使用n周期内的波动率作为阈值
    volatility = 0.0
    for j in range(1, n + 1):
        volatility += abs(close[j] - close[j-1])
    volatility = volatility / n if n > 0 else 1.0

    # 只有当价格变化显著大于波动率时才判断趋势
    is_long = price_change > 0.1 * volatility

    # 初始化
    af = step
    if is_long:
        ep = high[0]
        sar_val = low[0]
        for j in range(1, n + 1):
            if high[j] > ep:
                ep = high[j]
            if low[j] < sar_val:
                sar_val = low[j]
    else:
        ep = low[0]
        sar_val = high[0]
        for j in range(1, n + 1):
            if low[j] < ep:
                ep = low[j]
            if high[j] > sar_val:
                sar_val = high[j]
    result[n] = sar_val

    for i in range(n + 1, length):
        result[i] = result[i - 1] + af * (ep - result[i - 1])
        if is_long:
            if low[i] < result[i]:
                is_long = False
                result[i] = ep
                ep = low[i]
                af = step
            else:
                if high[i] > ep:
                    ep = high[i]
                    af_new = af + step
                    af = min(af_new, max_af)
        else:
            if high[i] > result[i]:
                is_long = True
                result[i] = ep
                ep = high[i]
                af = step
            else:
                if low[i] < ep:
                    ep = low[i]
                    af_new = af + step
                    af = min(af_new, max_af)
    return result
