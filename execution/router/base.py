
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-14

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

class BaseRouter(ABC):
    """
    [抽象基类] 交易执行路由器 (Base Execution Router)

    该类定义了所有交易网关（如 vn.py, XtQuant）必须实现的统一接口。
    """
    
    @abstractmethod
    def connect(self) -> None:
        """
        连接至交易网关。
        应当实现自动重连机制。
        """
        pass

    @abstractmethod
    def send_order(self, symbol: str, volume: float, price: float, direction: str, offset: str = 'OPEN') -> str:
        """
        发送委托订单。

        Args:
            symbol (str): 标的代码 (如 '000001.SZ')
            volume (float): 委托数量
            price (float): 委托价格
            direction (str): 买卖方向 ('BUY' / 'SELL')
            offset (str): 开平标志 ('OPEN' / 'CLOSE' / 'CLOSE_TODAY')

        Returns:
            str: 订单 ID (Order ID)
        """
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> None:
        """
        撤销委托订单。

        Args:
            order_id (str): 待撤销的订单 ID
        """
        pass

    @abstractmethod
    def query_account(self) -> Dict[str, Any]:
        """
        查询账户资金状态。

        Returns:
            Dict[str, Any]: 包含 'balance' (总资产), 'available' (可用资金) 等字段的字典
        """
        pass

    @abstractmethod
    def query_position(self) -> Dict[str, Any]:
        """
        查询当前持仓。

        Returns:
            Dict[str, Any]: 持仓字典 {symbol: volume}
        """
        pass
