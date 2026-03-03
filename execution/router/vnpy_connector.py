
from typing import Dict, Any
from .base import BaseRouter

class VnpyConnector(BaseRouter):
    """
    Connector for vn.py trading gateway.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connected = False

    def connect(self):
        print("Connecting to VNPY gateway...")
        # VNPY connection logic would go here
        self.connected = True
        print("Connected to VNPY.")

    def send_order(self, symbol: str, volume: float, price: float, direction: str, offset: str = 'OPEN'):
        if not self.connected:
            raise ConnectionError("Not connected to gateway")
        print(f"VNPY: Sending order {direction} {volume} of {symbol} at {price}")
        return "ORDER_ID_STUB"

    def cancel_order(self, order_id: str):
        print(f"VNPY: Cancelling order {order_id}")

    def query_account(self) -> Dict[str, Any]:
        return {"balance": 1000000.0, "available": 1000000.0}

    def query_position(self) -> Dict[str, Any]:
        return {"000001.SZ": 100}
