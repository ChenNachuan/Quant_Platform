# -*- coding: utf-8 -*-
"""
因子DAG调度器
管理因子依赖关系，避免重复计算
"""
from typing import List, Dict, Set, Optional, TYPE_CHECKING
import pandas as pd
from collections import defaultdict, deque

if TYPE_CHECKING:
    from factor_library.base import Factor
    from factor_library.registry import FactorRegistry


class FactorDAG:
    """
    因子依赖的有向无环图调度器
    
    功能：
    1. 构建因子依赖图
    2. 拓扑排序确定计算顺序
    3. 缓存已计算的因子值
    """
    
    def __init__(self):
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.computed_cache: Dict[str, pd.Series] = {}
        self.reverse_graph: Dict[str, Set[str]] = defaultdict(set)
    
    def add_dependency(self, factor_name: str, depends_on: List[str]):
        """
        添加依赖关系
        
        Args:
            factor_name: 因子名称
            depends_on: 依赖的因子列表
        """
        for dep in depends_on:
            # factor_name 依赖 dep
            # dep -> factor_name
            self.reverse_graph[dep].add(factor_name)
            self.graph[factor_name].add(dep)
    
    def topological_sort(self, target_factors: List[str]) -> List[str]:
        """
        拓扑排序：返回计算顺序
        
        Args:
            target_factors: 需要计算的目标因子列表
        
        Returns:
            按依赖顺序排列的因子列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 1. 收集所有相关节点（目标因子及其递归依赖）
        relevant_nodes = set()
        def collect(node):
            if node in relevant_nodes:
                return
            relevant_nodes.add(node)
            for dep in self.graph.get(node, []):
                collect(dep)
        
        for factor in target_factors:
            collect(factor)
            
        # 2. 构建子图的入度表
        # 注意：只计算子图内部的依赖关系
        in_degree = {node: 0 for node in relevant_nodes}
        subgraph = defaultdict(set)
        
        for node in relevant_nodes:
            # 节点的依赖即为其前驱节点
            # dependency -> node
            for dep in self.graph.get(node, []):
                if dep in relevant_nodes:
                    subgraph[dep].add(node)
                    in_degree[node] += 1
        
        # 3. Kahn算法
        queue = deque([node for node in relevant_nodes if in_degree[node] == 0])
        result = []
        
        while queue:
            node = queue.popleft()
            result.append(node)
            
            for neighbor in subgraph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # 4. 检查循环依赖
        if len(result) != len(relevant_nodes):
            # 找出循环的节点
            remaining = set(relevant_nodes) - set(result)
            raise ValueError(f"检测到循环依赖，涉及节点: {remaining}")
            
        return result
    
    def compute_with_cache(
        self,
        factor_name: str,
        data: pd.DataFrame,
        registry: 'FactorRegistry',
        timeframe: str,
        para: dict
    ) -> pd.Series:
        """
        带缓存的计算
        
        Args:
            factor_name: 因子名称
            data: 原始数据
            registry: 因子注册表
            timeframe: 时间框架
            para: 因子参数
        
        Returns:
            计算结果
        """
        # 生成缓存键
        cache_key = f"{factor_name}_{timeframe}_{para}"
        
        # 检查缓存
        if cache_key in self.computed_cache:
            return self.computed_cache[cache_key]
        
        # 获取因子实例
        factor = registry.create_instance(factor_name, timeframe, para)
        
        # 收集依赖
        deps = {}
        if hasattr(factor, 'dependencies') and factor.dependencies:
            for dep_name in factor.dependencies:
                # 跨参数依赖：优先查 dependency_para_map，无则透传当前 para
                # 解决因子 A(window=20) 依赖因子 B(window=5) 的参数错配问题
                dep_para = getattr(factor, 'dependency_para_map', {}).get(dep_name, para)
                deps[dep_name] = self.compute_with_cache(
                    dep_name, data, registry, timeframe, dep_para
                )
        
        # 计算
        if deps:
            result = factor.compute(data, deps=deps)
        else:
            result = factor.compute(data)
        
        # 缓存
        self.computed_cache[cache_key] = result
        return result
    
    def clear_cache(self):
        """清空计算缓存"""
        self.computed_cache.clear()
    
    def build_from_registry(self, registry: 'FactorRegistry'):
        """
        从注册表自动构建依赖图
        
        Args:
            registry: 因子注册表
        """
        for name, factor_cls in registry.get_all().items():
            if hasattr(factor_cls, 'dependencies') and factor_cls.dependencies:
                self.add_dependency(name, factor_cls.dependencies)


__all__ = ['FactorDAG']
