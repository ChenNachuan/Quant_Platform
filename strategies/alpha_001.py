
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-14

import vectorbt as vbt
import numpy as np
import sys
import logging
from pathlib import Path
from typing import Any

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from engine.vectorbt_engine.compiler import compile_strategy, run_strategy_numba
import pandas as pd # 用于生成模拟数据

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 定义策略逻辑 (使用 Numba 加速)
@compile_strategy
def ma_crossover_signal(c: Any, fast_window: int, slow_window: int) -> Any:
    """
    [Numba Kernel] 双均线交叉策略信号生成函数。

    该函数由 VectorBT 引擎逐行 (Bar-by-bar) 调用。
    
    Args:
        c (Context): VectorBT 上下文对象，包含当前行索引 c.i 和数据列索引 c.col
        fast_window (int): 快速均线窗口
        slow_window (int): 慢速均线窗口

    Returns:
        vbt.Order: 生成的订单指令
    """
    current_idx = c.i
    col_idx = c.col
    
    # 获取当前收盘价
    # c.close 是一个 2D numpy 数组 (Time, Assets)
    price = c.close[current_idx, col_idx]
    
    # 预热期检查: 如果数据不足以计算慢速均线，则不操作
    if current_idx < slow_window:
        return vbt.Order.nothing()
        
    # === 因子计算 (Factor Calculation) ===
    # 采用即时计算 (On-the-fly) 模式演示 Numba 的循环性能
    # 实际生产中建议预计算指标传入以提高效率
    
    # 计算快速均线 (Fast MA)
    fast_sum = 0.0
    for i in range(fast_window):
        fast_sum += c.close[current_idx - i, col_idx]
    fast_ma = fast_sum / fast_window
    
    # 计算慢速均线 (Slow MA)
    slow_sum = 0.0
    for i in range(slow_window):
        slow_sum += c.close[current_idx - i, col_idx]
    slow_ma = slow_sum / slow_window
    
    # === 信号生成 (Signal Logic) ===
    # 金叉 (Golden Cross): Fast MA > Slow MA -> 做多 (Long)
    # 死叉 (Death Cross): Fast MA < Slow MA -> 平仓 (Close) / 做空
    
    if fast_ma > slow_ma:
        # 目标仓位模式: 持有 100% 仓位
        return vbt.Order(
            size=1.0, 
            size_type=vbt.enums.SizeType.TargetPercent, 
            price=price
        )
    else:
        # 目标仓位模式: 清空仓位 (0%)
        return vbt.Order(
            size=0.0, 
            size_type=vbt.enums.SizeType.TargetPercent, 
            price=price
        )

def run_backtest() -> vbt.Portfolio:
    """
    执行策略回测主流程。
    """
    logger.info("开始执行 Alpha 001 (双均线) 回测...")
    
    # 1. 生成模拟数据 (Mock Data)
    # 在真实环境中，应调用 StorageManager.get_market_data()
    days = 1000
    assets = 3
    np.random.seed(42) # 固定随机种子以复现结果
    price = pd.DataFrame(
        np.random.uniform(10, 20, size=(days, assets)), 
        columns=['000001.SZ', '000002.SZ', '600000.SH'],
        index=pd.date_range(start='2020-01-01', periods=days, freq='D')
    )
    
    # 2. 运行 Numba 编译策略
    pf = run_strategy_numba(
        price, 
        ma_crossover_signal, 
        10, # fast_window
        50, # slow_window
        freq='d' # 年化频率
    )
    
    # 3. 结果分析 (Result Analysis)
    total_return = pf.total_return()
    sharpe_ratio = pf.sharpe_ratio()
    
    logger.info(f"回测结束。资产总回报率:\n{total_return}")
    logger.info(f"夏普比率 (Sharpe Ratio):\n{sharpe_ratio}")
    
    return pf

if __name__ == "__main__":
    run_backtest()
