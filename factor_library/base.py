# -*- coding: utf-8 -*-
"""
因子基类
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Literal
import pandas as pd
from factor_library.preprocessor import PostProcessor


class Factor(ABC):
    """
    因子基类
    
    必须实现的属性:
        name: 因子名称（全小写，唯一）
        para_group: 参数空间（按时间框架）
    
    必须实现的方法:
        generate_para_space(): 生成参数组合
        compute(): 计算因子值
    
    可选实现:
        update(): 增量更新（默认使用compute）
        dependencies: 依赖的因子列表
    """
    
    # ==================== 元数据（子类必须定义） ====================
    name: str = ""
    input_type: str = "bar"  # "bar" or "tick"
    max_lookback: int = 250
    applicable_market: List[str] = []  # 空表示全市场
    store_time: str = ""  # YYYYMMDD
    para_group: Dict[str, Dict] = {}
    
    # ==================== 依赖管理 ====================
    dependencies: List[str] = []           # 依赖的因子列表
    dependency_para_map: Dict[str, Dict] = {}  # 跨参数依赖映射表
    # 示例: dependency_para_map = {'sma': {'window': 5}}
    # 表示本因子在计算 'sma' 依赖时强制使用 window=5，
    # 而不是透传自身的 para。
    
    # ==================== 后处理配置 ====================
    post_process_steps: List[str] = []  # ['winsorize', 'standardize']
    winsorize_params: Dict = {'lower': 0.01, 'upper': 0.99}
    standardize_method: Literal['zscore', 'minmax'] = 'zscore'
    fillna_method: Optional[Literal['ffill', 'bfill', 'mean', 'zero']] = None
    
    def __init__(self, timeframe: str, para: dict):
        """
        初始化因子
        
        Args:
            timeframe: 时间框架 ('1d', '1h' 等)
            para: 因子参数字典
        """
        self.timeframe = timeframe
        self.para = para
        self._validate_metadata()
    
    def _validate_metadata(self):
        """验证元数据完整性"""
        if not self.name:
            raise ValueError(f"因子 {self.__class__.__name__} 缺少 name 属性")
        
        if not self.store_time:
            raise ValueError(f"因子 {self.name} 缺少 store_time 属性")
        
        if not self.para_group:
            raise ValueError(f"因子 {self.name} 缺少 para_group 属性")
    
    @abstractmethod
    def generate_para_space(self) -> List[Dict]:
        """
        生成参数空间
        
        Returns:
            参数字典列表
        
        Example:
            >>> factor = MomentumReturn(timeframe='1d', para={})
            >>> factor.generate_para_space()
            [{'window': 5}, {'window': 10}, {'window': 20}]
        """
        pass
    
    @abstractmethod
    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算因子（全量）
        
        Args:
            data: 原始OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 上游因子结果（由框架注入）
        
        Returns:
            因子值序列
        
        注意:
            - 使用 operators 库进行计算
            - 严禁使用未来数据（Look-ahead Bias）
            - 必须进行防御性检查（列名、参数）
        """
        pass
    
    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新（实盘用）
        
        Args:
            new_data: 最新数据
            history: 历史因子值
            deps: 上游因子（如有）
        
        Returns:
            更新后的因子值（仅新数据部分）
        
        默认实现：
            拼接 max_lookback 长度的历史数据 + 新数据，重算
            子类可覆写实现高效增量逻辑
        """
        # 默认：取历史数据的最后 max_lookback 条
        lookback_data = history.iloc[-self.max_lookback:] if len(history) > 0 else pd.Series(dtype=float)
        combined = pd.concat([lookback_data, new_data])
        
        # 重算
        full_result = self.compute(combined, deps)
        
        # 只返回新数据部分
        return full_result.iloc[-len(new_data):]
    
    def compute_with_postprocess(
        self,
        data: pd.DataFrame,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        计算 + 后处理
        
        Args:
            data: 原始数据
            deps: 依赖因子
        
        Returns:
            后处理后的因子值
        """
        # 1. 核心计算
        result = self.compute(data, deps)
        
        # 2. 有效性检查
        validity = PostProcessor.check_validity(result)
        if not validity['valid']:
            import warnings
            warnings.warn(f"因子 {self.name} 有效性检查未通过")
        
        # 3. 填充 NaN（如果配置）
        if self.fillna_method:
            result = PostProcessor.fillna(result, self.fillna_method)
        
        # 4. 后处理流水线
        for step in self.post_process_steps:
            if step == 'winsorize':
                result = PostProcessor.winsorize(result, **self.winsorize_params)
            elif step == 'standardize':
                result = PostProcessor.standardize(result, self.standardize_method)
        
        return result
    
    def get_id(self) -> str:
        """
        获取唯一因子ID
        
        Returns:
            因子ID，格式: {name}_{timeframe}_{params}
        """
        # 延迟导入避免循环依赖
        from factor_library.registry import generate_factor_id
        return generate_factor_id(self.name, self.timeframe, self.para)
    
    def temporary_tf(self, timeframe: str):
        """临时设置时间框架（兼容旧模板）"""
        self.timeframe = timeframe
    
    def change_para(self, new_para: dict):
        """修改参数（兼容旧模板）"""
        self.para = new_para
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', timeframe='{self.timeframe}', para={self.para})"


__all__ = ['Factor']
