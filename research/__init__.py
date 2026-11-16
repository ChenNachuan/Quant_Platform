"""研究域：因子定义（factor）、信号构建（signal）、ML 适配（ml）、后处理与横截面回测工具。"""

from research import factor, ml, post_processor, signal
from research.cross_sectional_backtest import (
    BaseCrossStrategy,
    CrossSectionalBacktest,
)
from research.post_processor import PostProcessor

__all__ = [
    "BaseCrossStrategy",
    "CrossSectionalBacktest",
    "PostProcessor",
    "factor",
    "ml",
    "post_processor",
    "signal",
]
