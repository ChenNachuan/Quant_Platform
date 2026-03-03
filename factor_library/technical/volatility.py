# -*- coding: utf-8 -*-
"""
波动率因子
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
from factor_library.operators import ts_std
import pandas as pd
from typing import List, Dict, Optional


@register_factor
class Volatility(Factor):
    """
    波动率因子
    
    公式：std(pct_change(close, 1), window)
    方向：数值越大，波动越剧烈，风险越高
    
    用途：
    - 风险管理
    - 作为其他复合因子的输入
    """
    
    # 元数据
    name = "volatility"
    input_type = "bar"
    max_lookback = 250
    applicable_market = []
    store_time = "20260217"
    
    # 参数空间
    para_group = {
        "1d": {"window": [10, 20, 30, 60]},
        "1h": {"window": [20, 50, 100]}
    }
    
    # 无依赖
    dependencies = []
    
    # 后处理配置
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'
    
    def generate_para_space(self) -> List[Dict[str, int]]:
        """生成参数空间"""
        if self.timeframe not in self.para_group:
            return []
        group = self.para_group[self.timeframe]
        return [{"window": w} for w in group["window"]]
    
    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算因子
        
        Args:
            data: OHLCV数据
            deps: 无依赖
        
        Returns:
            波动率序列
        """
        # 防御性检查
        if 'close' not in data.columns:
            raise KeyError("缺少必要列: close")
        
        window = self.para.get("window")
        if window is None:
            raise ValueError("参数 'window' 缺失")
        
        # 计算收益率的标准差
        returns = data['close'].pct_change(fill_method=None)
        return ts_std(returns, window)
    
    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """增量更新"""
        window = self.para['window']
        
        # 只取最近 window-1 条历史数据
        recent = history.iloc[-(window-1):] if len(history) >= window-1 else history
        combined = pd.concat([recent, new_data])
        
        # 重算
        returns = combined['close'].pct_change(fill_method=None)
        result = ts_std(returns, window)
        
        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['Volatility']
