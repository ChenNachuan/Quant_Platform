
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-14

from typing import Dict
import logging

logger = logging.getLogger(__name__)

class RiskControl:
    """
    风控模块 (Risk Control Module)
    
    负责交易前的合规性与安全性检查。
    """
    def __init__(self, config: Dict):
        """
        初始化风控参数。

        Args:
            config (Dict): 配置字典
                - max_drawdown: 最大回撤限制
                - max_leverage: 最大杠杆限制
                - single_position_limit: 单票持仓上限 (0.0 - 1.0)
        """
        self.max_drawdown = config.get('max_drawdown', 0.2)
        self.max_leverage = config.get('max_leverage', 1.0)
        self.single_pos_limit = config.get('single_position_limit', 0.1)
        
        logger.info(f"风控模块已初始化 | 单票限制: {self.single_pos_limit:.1%}")

    def check_order(self, symbol: str, volume: float, price: float, account_equity: float) -> bool:
        """
        订单合规性检查 (Pre-trade Risk Check)。

        检查逻辑:
        1. 计算订单名义价值 (Notional Value) = Volume * Price
        2. 验证是否超过单票持仓上限: Order Value > Equity * Limit

        Args:
            symbol (str): 标的代码
            volume (float): 委托数量
            price (float): 委托价格
            account_equity (float): 账户当前总权益

        Returns:
            bool: 检查通过返回 True，否则 False
        """
        order_val = volume * price
        limit_val = account_equity * self.single_pos_limit
        
        if order_val > limit_val:
            logger.warning(f"风控阻断: 标的 {symbol} 订单价值 {order_val:,.2f} 超过单票限制 {limit_val:,.2f}")
            return False
            
        logger.debug(f"风控通过: {symbol}")
        return True
