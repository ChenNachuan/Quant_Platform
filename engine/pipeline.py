"""
Algorithm Framework 流水线封装
Version: 1.0.0
Date: 2026-03-11

借鉴 QuantConnect Lean 的 5 层模型，将策略执行拆分为严格的流水线组件：
Universe Selection -> Alpha Model -> Portfolio Construction -> Risk Management -> Execution

针对向量化计算模式（ZVT/VectorBT）适配：
- Universe 产出：布尔矩阵 (Date x Symbol)
- Alpha 产出：信号强度矩阵 (Date x Symbol)
- Portfolio 产出：目标权重矩阵 (Date x Symbol)
- Risk 产出：风险裁剪后的安全权重矩阵 (Date x Symbol)
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import vectorbt as vbt

from factor_library.base import Factor
from factor_library.universe import UniverseFilter
from execution.risk_control import RiskControl

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 1. 核心抽象接口
# ─────────────────────────────────────────────

class AlphaModel(ABC):
    """
    Alpha 模型接口。
    用于计算各标的在每个时间点的预期收益 / 信号强度。
    """
    @abstractmethod
    def get_signals(self, kdata: pd.DataFrame, universe_mask: pd.DataFrame) -> pd.DataFrame:
        pass


class PortfolioConstructor(ABC):
    """
    组合构建器接口。
    将 Alpha 信号转换为目标权重矩阵。
    """
    @abstractmethod
    def create_portfolio(self, signals: pd.DataFrame, kdata: pd.DataFrame) -> pd.DataFrame:
        pass


class RiskFilter(ABC):
    """
    风控过滤器接口。
    负责对目标权重进行安全性裁剪。
    """
    @abstractmethod
    def apply_risk_rules(self, target_weights: pd.DataFrame) -> pd.DataFrame:
        pass


class ExecutionHandler(ABC):
    """
    执行处理器接口。
    负责将权重转换为实际订单或仓位变动记录。
    """
    @abstractmethod
    def execute(self, safe_weights: pd.DataFrame, kdata: pd.DataFrame) -> Any:
        pass


# ─────────────────────────────────────────────
# 2. 具体实现层：适配既有类库
# ─────────────────────────────────────────────

class FactorAlphaModel(AlphaModel):
    """将既有 Factor 适配为 AlphaModel"""
    def __init__(self, factor: Factor):
        self.factor = factor

    def get_signals(self, kdata: pd.DataFrame, universe_mask: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"AlphaModel: 计算因子 [{self.factor.name}]")
        
        results = []
        for entity_id, group in kdata.groupby('entity_id'):
            group = group.sort_values('timestamp')
            try:
                vals = self.factor.compute(group)
                results.append(pd.DataFrame({
                    'timestamp': group['timestamp'],
                    'entity_id': entity_id,
                    'value': vals
                }))
            except Exception as e:
                logger.debug(f"标的 {entity_id} 计算 Alpha 失败: {e}")
                
        if not results:
            return pd.DataFrame()
            
        all_res = pd.concat(results, ignore_index=True)
        signal_df = all_res.pivot(index='timestamp', columns='entity_id', values='value')
        
        # 在 Universe 层面过滤
        common_idx = signal_df.index.intersection(universe_mask.index)
        common_col = signal_df.columns.intersection(universe_mask.columns)
        
        filtered = signal_df.copy()
        filtered.loc[common_idx, common_col] = signal_df.loc[common_idx, common_col].where(
            universe_mask.loc[common_idx, common_col]
        )
        return filtered


class EqualWeightPortfolio(PortfolioConstructor):
    """简单的等权组合构建器 (默认将非 NaN 的信号等权分配)"""
    def create_portfolio(self, signals: pd.DataFrame, kdata: pd.DataFrame) -> pd.DataFrame:
        logger.info("PortfolioConstructor: 构建等权截面组合")
        valid_mask = signals.notna()
        daily_count = valid_mask.sum(axis=1)
        # 防止除 0
        weights = valid_mask.div(daily_count.where(daily_count > 0, np.nan), axis=0).fillna(0.0)
        return weights


class DefaultRiskFilter(RiskFilter):
    """默认风控：复用既有 RiskControl 的单票上限（向量化裁剪）"""
    def __init__(self, max_weight: float = 1.0):
        # 此处可以将 execution/risk_control.py 中的 single_position_limit 映射过来
        self.max_weight = max_weight

    def apply_risk_rules(self, target_weights: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"RiskFilter: 执行风控权重裁剪 (上限 {self.max_weight:.1%})")
        # 直接在横截面上裁剪权重
        weights = target_weights.clip(upper=self.max_weight)
        return weights


class SimulationExecutionHandler(ExecutionHandler):
    """
    权重透传执行器
    直接原样返回权重宽表。
    """
    def execute(self, safe_weights: pd.DataFrame, kdata: pd.DataFrame) -> pd.DataFrame:
        logger.info("ExecutionHandler: 输出模拟执行权重宽表")
        return safe_weights


class VectorBTExecutionHandler(ExecutionHandler):
    """
    VectorBT 向量化回测执行器。
    将安全权重矩阵输入 VectorBT，进行高性能组合回测。
    """
    def __init__(self, init_cash: float = 1000000.0, fees: float = 0.001, slippage: float = 0.002):
        self.init_cash = init_cash
        self.fees = fees
        self.slippage = slippage

    def execute(self, safe_weights: pd.DataFrame, kdata: pd.DataFrame) -> vbt.Portfolio:
        logger.info("ExecutionHandler: 调用 VectorBT 引擎执行回测")
        # 提取收盘价宽表 (Date x Symbol)
        close_df = kdata.pivot(index='timestamp', columns='entity_id', values='close')
        
        # 对齐索引和列
        common_idx = safe_weights.index.intersection(close_df.index)
        common_col = safe_weights.columns.intersection(close_df.columns)
        
        weights = safe_weights.loc[common_idx, common_col]
        close_prices = close_df.loc[common_idx, common_col]
        
        # 填充缺失价格以防 vbt 报错
        close_prices = close_prices.ffill()

        # 使用 from_orders 目标仓位百分比模式
        # 注意：cash_sharing=True 和 group_by=True 是跨标的共享资金池的关键
        pf = vbt.Portfolio.from_orders(
            close=close_prices,
            size=weights,
            size_type='target_percent',
            group_by=True,
            cash_sharing=True,
            init_cash=self.init_cash,
            fees=self.fees,
            slippage=self.slippage,
            freq='1D'
        )
        logger.info(f"VectorBT 回测完成。最终权益: {pf.value().iloc[-1]:,.2f}")
        return pf

# ─────────────────────────────────────────────
# 3. 5 层流水线总装
# ─────────────────────────────────────────────

class AlphaPipeline:
    """
    Algorithm Framework 5 层向量化流水线
    将量化交易全流程转化为 DataFrame 矩阵的传递
    """
    def __init__(
        self,
        universe: UniverseFilter,
        alpha: AlphaModel,
        portfolio: PortfolioConstructor,
        risk: RiskFilter,
        execution: ExecutionHandler
    ):
        self.universe = universe
        self.alpha = alpha
        self.portfolio = portfolio
        self.risk = risk
        self.execution = execution

    def run(self, kdata: pd.DataFrame) -> pd.DataFrame:
        """
        执行一条完整的策略流水线，返回最终的目标权重矩阵或执行记录。
        """
        logger.info("========== Pipeline Start ==========")
        
        # 1. Universe Selection: 获取有效池矩阵
        logger.info("[1/5] Universe Selection")
        universe_mask = self.universe.filter(kdata)
        
        # 2. Alpha Model: 提取信号强度
        logger.info("[2/5] Alpha Extraction")
        signals = self.alpha.get_signals(kdata, universe_mask)
        
        # 3. Portfolio Construction: 构建目标权重
        logger.info("[3/5] Portfolio Construction")
        target_weights = self.portfolio.create_portfolio(signals, kdata)
        
        # 4. Risk Management: 风险裁剪验证
        logger.info("[4/5] Risk Filter")
        safe_weights = self.risk.apply_risk_rules(target_weights)
        
        # 5. Execution: 执行落地
        logger.info("[5/5] Execution")
        final_result = self.execution.execute(safe_weights, kdata)
        
        logger.info("========== Pipeline End ==========")
        return final_result


__all__ = [
    'AlphaModel', 'PortfolioConstructor', 'RiskFilter', 'ExecutionHandler',
    'FactorAlphaModel', 'EqualWeightPortfolio', 'DefaultRiskFilter', 
    'SimulationExecutionHandler', 'VectorBTExecutionHandler',
    'AlphaPipeline'
]
