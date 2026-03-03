# -*- coding: utf-8 -*-
"""
风险调整动量因子（复合因子）
演示DAG依赖管理
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
from factor_library.operators import cs_rank
import pandas as pd
from typing import List, Dict, Optional


@register_factor
class RiskAdjustedMomentum(Factor):
    """
    风险调整动量因子（复合因子）
    
    公式：momentum / volatility
    思想：Sharpe-like ratio，单位风险的收益
    
    **核心特性**: 依赖其他因子，测试DAG调度
    
    依赖:
    - momentum_return: 动量因子
    - volatility: 波动率因子
    
    方向：数值越大，风险调整后收益越高
    """
    
    # 元数据
    name = "risk_adjusted_momentum"
    input_type = "bar"
    max_lookback = 250
    applicable_market = []
    store_time = "20260217"
    
    # 参数空间（继承依赖因子的参数）
    para_group = {
        "1d": {"window": [20, 60]},
        "1h": {"window": [50, 100]}
    }
    
    # **核心**: 声明依赖
    dependencies = ["momentum_return", "volatility"]
    
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
            data: OHLCV数据（此因子不直接使用原始数据）
            deps: 依赖因子的计算结果
                - deps['momentum_return']: 动量因子值
                - deps['volatility']: 波动率因子值
        
        Returns:
            风险调整动量值
        """
        # 防御性检查
        if deps is None or 'momentum_return' not in deps or 'volatility' not in deps:
            raise ValueError("缺少依赖因子: momentum_return, volatility")
        
        momentum = deps['momentum_return']
        vol = deps['volatility']
        
        # 避免除零
        # 当波动率接近0时，用小常数替代
        vol_safe = vol.replace(0, 1e-6)
        
        # Sharpe-like ratio
        return momentum / vol_safe
    
    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新
        
        复合因子的增量更新依赖于上游因子的增量更新
        由框架自动处理依赖链
        """
        # 复合因子通常直接使用 compute()
        # 因为依赖因子已经是最新的
        return self.compute(new_data, deps)


__all__ = ['RiskAdjustedMomentum']
