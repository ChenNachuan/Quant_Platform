from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseRouter(ABC):
    """交易执行路由器抽象基类，定义各类交易网关必须实现的统一接口。"""

    @abstractmethod
    def connect(self) -> None:
        """连接至交易网关，应当支持自动重连机制。"""
        pass

    @abstractmethod
    def send_order(self, symbol: str, volume: float, price: float, direction: str, offset: str = 'OPEN') -> str:
        """发送委托订单，返回订单 ID。"""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> None:
        """撤销指定订单 ID 的委托。"""
        pass

    @abstractmethod
    def query_account(self) -> Dict[str, Any]:
        """查询账户资金状态，返回包含资产与可用资金等字段的字典。"""
        pass

    @abstractmethod
    def query_position(self) -> Dict[str, Any]:
        """查询当前持仓映射 ``{symbol: volume}``。"""
        pass
