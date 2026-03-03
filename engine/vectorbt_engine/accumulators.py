
import numba as nb
import numpy as np

@nb.njit
def update_mean(new_val, old_mean, count):
    """
    Incremental mean update.
    """
    if count == 0:
        return new_val
    return (old_mean * count + new_val) / (count + 1)

@nb.njit
def update_ema(new_val, old_ema, alpha):
    """
    Incremental EMA update.
    """
    if np.isnan(old_ema):
        return new_val
    return alpha * new_val + (1 - alpha) * old_ema

@nb.njit
def check_crossover(fast, slow, prev_fast, prev_slow):
    """
    Checks for crossover.
    Returns:
    1 if fast crosses above slow (Golden Cross)
    -1 if fast crosses below slow (Death Cross)
    0 otherwise
    """
    if np.isnan(fast) or np.isnan(slow) or np.isnan(prev_fast) or np.isnan(prev_slow):
        return 0
    
    if prev_fast <= prev_slow and fast > slow:
        return 1
    elif prev_fast >= prev_slow and fast < slow:
        return -1
    return 0
