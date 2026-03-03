# -*- coding: utf-8 -*-
"""
因子注册表
实现装饰器自动注册和工厂模式
"""
from typing import Dict, Type, Optional, TYPE_CHECKING
import hashlib
import json

if TYPE_CHECKING:
    from factor_library.base import Factor


class FactorRegistry:
    """全局因子注册表"""
    
    _registry: Dict[str, Type['Factor']] = {}
    
    @classmethod
    def register(cls, factor_cls: Type['Factor']) -> Type['Factor']:
        """
        装饰器：自动注册因子类
        
        Args:
            factor_cls: 因子类
        
        Returns:
            原因子类（支持链式调用）
        
        Raises:
            ValueError: 因子名称重复
        """
        if factor_cls.name in cls._registry:
            raise ValueError(f"因子 '{factor_cls.name}' 已注册！")
        
        cls._registry[factor_cls.name] = factor_cls
        return factor_cls
    
    @classmethod
    def get(cls, name: str) -> Optional[Type['Factor']]:
        """
        获取因子类
        
        Args:
            name: 因子名称
        
        Returns:
            因子类，如果不存在返回 None
        """
        return cls._registry.get(name)
    
    @classmethod
    def get_all(cls) -> Dict[str, Type['Factor']]:
        """获取所有已注册的因子类"""
        return cls._registry.copy()
    
    @classmethod
    def list_factors(cls) -> list[str]:
        """列出所有已注册的因子名称"""
        return list(cls._registry.keys())
    
    @classmethod
    def create_instance(cls, name: str, timeframe: str, para: dict) -> 'Factor':
        """
        工厂方法：创建因子实例
        
        Args:
            name: 因子名称
            timeframe: 时间框架 ('1d', '1h' 等)
            para: 因子参数
        
        Returns:
            因子实例
        
        Raises:
            ValueError: 因子未注册
        """
        factor_cls = cls.get(name)
        if not factor_cls:
            raise ValueError(f"因子 '{name}' 未注册")
        return factor_cls(timeframe=timeframe, para=para)


def register_factor(cls: Type['Factor']) -> Type['Factor']:
    """
    装饰器快捷方式
    
    用法:
        @register_factor
        class MyFactor(Factor):
            ...
    """
    return FactorRegistry.register(cls)


def generate_factor_id(name: str, timeframe: str, para: dict) -> str:
    """
    生成标准化因子ID
    
    格式：{name}_{timeframe}_{params}
    示例：momentum_return_1d_win20
    
    Args:
        name: 因子名称
        timeframe: 时间框架
        para: 参数字典
    
    Returns:
        因子ID
    """
    # 参数直接拼接（可读性好）
    para_str = '_'.join(f"{k}{v}" for k, v in sorted(para.items()))
    factor_id = f"{name}_{timeframe}_{para_str}"
    
    return factor_id


def generate_factor_hash(name: str, timeframe: str, para: dict) -> str:
    """
    生成因子哈希ID（参数复杂时使用）
    
    格式：{name}_{timeframe}_{hash8}
    
    Args:
        name: 因子名称
        timeframe: 时间框架
        para: 参数字典
    
    Returns:
        因子哈希ID
    """
    para_hash = hashlib.md5(
        json.dumps(para, sort_keys=True).encode()
    ).hexdigest()[:8]
    
    return f"{name}_{timeframe}_{para_hash}"


__all__ = ['FactorRegistry', 'register_factor', 'generate_factor_id', 'generate_factor_hash']
