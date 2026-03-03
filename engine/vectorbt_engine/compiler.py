
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-14

import numpy as np
import numba as nb
import vectorbt as vbt
import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

def compile_strategy(strategy_func: Callable) -> Callable:
    """
    [装饰器] 编译策略函数 (Numba JIT)

    将 Python 编写的策略逻辑编译为机器码，以供 VectorBT 的 `from_order_func` 调用。
    
    编译原理:
    - 目标函数签名必须符合 (c, *args) -> order。
    - 使用 @nb.njit (No Python Mode) 禁用 Python 解释器介入，实现 C++ 级别的执行速度。

    Args:
        strategy_func (Callable): 原始 Python 策略函数

    Returns:
        Callable: 编译后的 JIT 函数
    """
    try:
        compiled_func = nb.njit(strategy_func)
        logger.debug(f"策略函数 {strategy_func.__name__} 编译成功")
        return compiled_func
    except Exception as e:
        logger.warning(f"Numba 编译警告 (将回退至纯 Python 模式): {e}")
        return strategy_func

def run_strategy_numba(target_price: Any, strategy_func: Callable, *args, **kwargs) -> vbt.Portfolio:
    """
    运行 Numba 加速的回测引擎。

    使用 `vbt.Portfolio.from_order_func` 逐行扫描数据，并调用预编译的策略逻辑。

    Args:
        target_price (pd.DataFrame): 价格数据 (建议 Close 收盘价)
        strategy_func (Callable): 已通过 @compile_strategy 编译的函数
        *args: 传递给策略函数的可变参数
        **kwargs: 传递给 `Portfolio.from_order_func` 的关键字参数
        
    Returns:
        vbt.Portfolio: 回测结果对象
    """
    # 强制类型转换为 float32 以节省显存/内存 (Memory Optimization)
    # n_dates * n_assets * 4 bytes (float32) vs 8 bytes (float64)
    if hasattr(target_price, 'values'):
        if target_price.values.dtype != np.float32:
             # 注意：此处不就地修改，而是尽可能利用 vectorbt 的转换机制或在传入前处理
             # 为演示明确性，这里做检查提示
             logger.debug("建议输入数据为 float32 格式以优化性能")

    logger.info(f"开始执行 Numba 回测: {strategy_func.__name__}")
    
    return vbt.Portfolio.from_order_func(
        target_price,
        order_func=strategy_func,
        *args,
        **kwargs
    )
