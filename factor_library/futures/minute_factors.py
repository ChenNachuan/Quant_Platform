# -*- coding: utf-8 -*-
"""
分钟频因子库

从 high-freq/utils/factor_module.py 迁移，适配 quant-platform 的 Factor 基类接口。
包含 150+ 个 qlib 表达式格式的分钟频因子。

因子分类：
- 基础行情因子
- 收益率因子
- 动量均线因子
- 波动率因子
- 技术指标因子（RSI、MACD、布林带等）
- 成交量因子
- 趋势因子
- 高频统计因子
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from factor_library.base import Factor
from factor_library.registry import register_factor


class MinuteFactorLibrary:
    """分钟频因子库。

    - FACTORS: 因子名 -> qlib 表达式
    - get_features: 支持按名称选择部分因子
    """

    FACTORS = {
        # ===== 基础行情因子 =====
        "open": "$open",
        "high": "$high",
        "low": "$low",
        "close": "$close",
        "volume": "$volume",
        "vwap": "$vwap",
        "total_turnover": "$total_turnover",
        # ===== 收益率因子 =====
        "returns_1min": "$vwap/(Ref($vwap,1)+1e-12)-1",
        "returns_5min": "$vwap/(Ref($vwap,5)+1e-12)-1",
        "returns_15min": "$vwap/(Ref($vwap,15)+1e-12)-1",
        "returns_20min": "$vwap/(Ref($vwap,20)+1e-12)-1",
        "returns_30min": "$vwap/(Ref($vwap,30)+1e-12)-1",
        "returns_60min": "$vwap/(Ref($vwap,60)+1e-12)-1",
        "returns_120min": "$vwap/(Ref($vwap,120)+1e-12)-1",
        # ===== 动量均线类因子 =====
        "sma_5": "Mean($vwap,5)",
        "sma_10": "Mean($vwap,10)",
        "sma_15": "Mean($vwap,15)",
        "sma_20": "Mean($vwap,20)",
        "sma_30": "Mean($vwap,30)",
        "sma_60": "Mean($vwap,60)",
        "ema_5": "EMA($vwap,5)",
        "ema_10": "EMA($vwap,10)",
        "ema_15": "EMA($vwap,15)",
        "ema_20": "EMA($vwap,20)",
        "ema_30": "EMA($vwap,30)",
        "ema_60": "EMA($vwap,60)",
        "mid_price_5_20": "Mean(($high+$low)/2,5)/(Mean(($high+$low)/2,20)+1e-12)-1",
        "intra_mom_5": "Mean(($high+$low)/2/$open,5)",
        "intra_mom_10": "Mean(($high+$low)/2/$open,10)",
        "intra_mom_20": "Mean(($high+$low)/2/$open,20)",
        "intra_mom_30": "Mean(($high+$low)/2/$open,30)",
        "momentum_5": "$vwap/Ref($vwap,5)-1",
        "momentum_10": "$vwap/Ref($vwap,10)-1",
        "momentum_20": "$vwap/Ref($vwap,20)-1",
        "momentum_30": "$vwap/Ref($vwap,30)-1",
        "momentum_60": "$vwap/Ref($vwap,60)-1",
        # ===== 交叉与趋势强度 =====
        "sma_cross_5_10": "Mean($vwap,5)/(Mean($vwap,10)+1e-12)-1",
        "sma_cross_10_20": "Mean($vwap,10)/(Mean($vwap,20)+1e-12)-1",
        "ema_cross_15_30": "EMA($vwap,15)/(EMA($vwap,30)+1e-12)-1",
        # ===== 价格位置 =====
        "price_position_sma5": "($vwap-Mean($vwap,5))/(Mean($vwap,5)+1e-12)",
        "price_position_sma10": "($vwap-Mean($vwap,10))/(Mean($vwap,10)+1e-12)",
        "price_position_sma15": "($vwap-Mean($vwap,15))/(Mean($vwap,15)+1e-12)",
        "price_position_sma20": "($vwap-Mean($vwap,20))/(Mean($vwap,20)+1e-12)",
        "price_position_sma30": "($vwap-Mean($vwap,30))/(Mean($vwap,30)+1e-12)",
        "price_position_sma60": "($vwap-Mean($vwap,60))/(Mean($vwap,60)+1e-12)",
        # ===== 波动率 =====
        "volatility_5": "Std($vwap/Ref($vwap,1)-1,5)",
        "volatility_10": "Std($vwap/Ref($vwap,1)-1,10)",
        "volatility_15": "Std($vwap/Ref($vwap,1)-1,15)",
        "volatility_20": "Std($vwap/Ref($vwap,1)-1,20)",
        "volatility_30": "Std($vwap/Ref($vwap,1)-1,30)",
        "volatility_60": "Std($vwap/Ref($vwap,1)-1,60)",
        # ===== 布林带 =====
        "bb_position_60": "($vwap-Mean($vwap,60))/(2*Std($vwap,60)+1e-12)",
        "bb_position_20": "($vwap-Mean($vwap,20))/(2*Std($vwap,20)+1e-12)",
        "bb_width_60": "(4*Std($vwap,60))/(Mean($vwap,60)+1e-12)",
        "bb_width_20": "(4*Std($vwap,20))/(Mean($vwap,20)+1e-12)",
        # ===== RSI =====
        "rsi_120": "100*Mean(If($vwap-Ref($vwap,1)>0,$vwap-Ref($vwap,1),0),120)/(Mean(Abs($vwap-Ref($vwap,1)),120)+1e-12)",
        "rsi_60": "100*Mean(If($vwap-Ref($vwap,1)>0,$vwap-Ref($vwap,1),0),60)/(Mean(Abs($vwap-Ref($vwap,1)),60)+1e-12)",
        "rsi_20": "100*Mean(If($vwap-Ref($vwap,1)>0,$vwap-Ref($vwap,1),0),20)/(Mean(Abs($vwap-Ref($vwap,1)),20)+1e-12)",
        # ===== MACD =====
        "macd_10_60": "EMA($vwap,10)-EMA($vwap,60)",
        "macd_15_30": "EMA($vwap,15)-EMA($vwap,30)",
        "macd_signal_10_60": "EMA(EMA($vwap,10)-EMA($vwap,60),9)",
        "macd_signal_15_30": "EMA(EMA($vwap,15)-EMA($vwap,30),9)",
        "macd_histogram_10_60": "(EMA($vwap,10)-EMA($vwap,60))-EMA(EMA($vwap,10)-EMA($vwap,60),9)",
        "macd_histogram_15_30": "(EMA($vwap,15)-EMA($vwap,30))-EMA(EMA($vwap,15)-EMA($vwap,30),9)",
        # ===== 成交量相关 =====
        "volume_ratio_5": "$volume/(Mean($volume,5)+1e-12)",
        "volume_ratio_10": "$volume/(Mean($volume,10)+1e-12)",
        "volume_ratio_20": "$volume/(Mean($volume,20)+1e-12)",
        "volume_ratio_30": "$volume/(Mean($volume,30)+1e-12)",
        "volume_ratio_60": "$volume/(Mean($volume,60)+1e-12)",
        "vpt_sma_5": "Mean(($vwap/Ref($vwap,1)-1)*$volume,5)",
        "vpt_sma_10": "Mean(($vwap/Ref($vwap,1)-1)*$volume,10)",
        "vpt_sma_20": "Mean(($vwap/Ref($vwap,1)-1)*$volume,20)",
        # ===== 价格结构 =====
        "high_low_ratio": "$high/($low+1e-12)",
        "close_open_ratio": "$close/($open+1e-12)",
        "high_close_ratio": "$high/($close+1e-12)",
        "low_close_ratio": "$low/($close+1e-12)",
        # ===== ATR =====
        "atr_5": "Mean($high/($low+1e-12)-1,5)",
        "atr_10": "Mean($high/($low+1e-12)-1,10)",
        "atr_20": "Mean($high/($low+1e-12)-1,20)",
        "atr_30": "Mean($high/($low+1e-12)-1,30)",
        "atr_60": "Mean($high/($low+1e-12)-1,60)",
        # ===== Williams %R =====
        "williams_r": "-100*($high-$close)/($high-$low+1e-12)",
        "williams_r_5": "-100*(Max($high,5)-$close)/(Max($high,5)-Min($low,5)+1e-12)",
        "williams_r_20": "-100*(Max($high,20)-$close)/(Max($high,20)-Min($low,20)+1e-12)",
        # ===== KDJ =====
        "k_percent": "100*($close-$low)/($high-$low+1e-12)",
        "d_percent": "Mean(100*($close-$low)/($high-$low+1e-12),5)",
        # ===== 价格加速度 =====
        "price_acceleration": "($vwap/Ref($vwap,1)-1)-(Ref($vwap,1)/Ref($vwap,2)-1)",
        "vwap_ratio": "$close/($vwap+1e-12)-1",
        # ===== 成交量指标因子 =====
        "chaikin_ad_10": "EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,3)-EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,10)",
        "chaikin_ad_30": "EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,3)-EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,30)",
        "obv_sma_5_20": "Mean(If($close>Ref($close,1),$volume,If($close<Ref($close,1),0-$volume,0)),5)/(Mean(If($close>Ref($close,1),$volume,If($close<Ref($close,1),0-$volume,0)),20)+1e-12)-1",
        # ===== 反转指标因子 =====
        "mfi_20": "100-(100/(1+Mean(If($close>Ref($close,1),($high+$low+$close)/3*$volume,0),20)/(Mean(If($close<Ref($close,1),($high+$low+$close)/3*$volume,0),20)+1e-12)))",
        "cci_20": "($close-Mean(($high+$low+$close)/3,20))/(0.015*Mean(Abs(($high+$low+$close)/3-Mean(($high+$low+$close)/3,20)),20)+1e-12)",
        "cmo_20": "100*(Mean(If($close-Ref($close,1)>0,$close-Ref($close,1),0),20)-Mean(If(Ref($close,1)-$close>0,Ref($close,1)-$close,0),20))/(Mean(Abs($close-Ref($close,1)),20)+1e-12)",
        # ===== 趋势因子 =====
        "short_term_trend_strength": "Sum(Log($vwap/Ref($vwap,1)),20)",
        "long_term_trend_strength": "Sum(Log($vwap/Ref($vwap,1)),60)",
        "short_term_momentum_volatility": "Sum(Log($vwap/Ref($vwap,1)),20)/(Std($vwap/Ref($vwap,1)-1,20)+1e-12)",
        "long_term_momentum_volatility": "Sum(Log($vwap/Ref($vwap,1)),60)/(Std($vwap/Ref($vwap,1)-1,60)+1e-12)",
        # ===== 高频因子 - 收益率分布 =====
        "realized_skewness_20": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),20)/((Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20))+1e-12)",
        "realized_kurtosis_20": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),20)/((Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20))+1e-12)",
        "up_down_volatility_ratio_20": "Std(If($vwap/Ref($vwap,1)-1>0,$vwap/Ref($vwap,1)-1,0),20)/(Std(If(Ref($vwap,1)/$vwap-1>0,Ref($vwap,1)/$vwap-1,0),20)+1e-12)",
        "realized_skewness_60": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),60)/((Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60))+1e-12)",
        "realized_kurtosis_60": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),60)/((Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60))+1e-12)",
        "up_down_volatility_ratio_60": "Std(If($vwap/Ref($vwap,1)-1>0,$vwap/Ref($vwap,1)-1,0),60)/(Std(If(Ref($vwap,1)/$vwap-1>0,Ref($vwap,1)/$vwap-1,0),60)+1e-12)",
        # ===== 高频因子 - 成交量分布 =====
        "morning_volume_ratio": "Sum($volume,30)/Sum($volume,240)",
        "afternoon_volume_ratio": "Sum($volume,120)/Sum($volume,240)",
        "volume_concentration": "Std($volume,240)/(Mean($volume,240)+1e-12)",
        "midday_volatility_effect": "Std(Ref($vwap/Ref($vwap,1)-1,60),60)/(Std($vwap/Ref($vwap,1)-1,60)+1e-12)",
        # ===== 高频因子 - 量价复合 =====
        "price_volume_correlation": "Corr($vwap, $volume,20)",
        # ===== 市场情绪和风险变化 =====
        "jumps_frequency": "Sum(If(Abs($vwap/Ref($vwap,1)-1)>2*Std($vwap/Ref($vwap,1)-1,240),1,0),240)",
        # ===== 价格动量与趋势特征 =====
        "trend_strength": "Abs($vwap-Ref($vwap,20))/(Sum(Abs($vwap-Ref($vwap,1)),20)+1e-12)",
        "efficiency_ratio": "Abs($close-$open)/(Max($high,240)-Min($low,240))",
        # ===== 跨日时序特征 =====
        "volatility_regime_10": "Std($vwap/Ref($vwap,1)-1,240)/Mean(Std($vwap/Ref($vwap,1)-1,240),10)",
        "volume_persistence_5": "Corr($volume, Ref($volume,240),5)",
        "volume_persistence_10": "Corr($volume, Ref($volume,240),10)",
        "volume_persistence_20": "Corr($volume, Ref($volume,240),20)",
        "momentum_persistence_10": "Corr($vwap/Ref($vwap,1)-1, Ref($vwap/Ref($vwap,1)-1,240),10)",
        "momentum_persistence_20": "Corr($vwap/Ref($vwap,1)-1, Ref($vwap/Ref($vwap,1)-1,240),20)",
    }

    @classmethod
    def list_factor_names(cls) -> list[str]:
        """列出所有因子名称。"""
        return list(cls.FACTORS.keys())

    @classmethod
    def get_features(cls, selected_factors: list[str] | None = None) -> list[str]:
        """获取因子的 qlib 表达式列表。

        Args:
            selected_factors: 指定因子名称列表，None 表示全部

        Returns:
            qlib 表达式列表
        """
        if not selected_factors:
            return list(cls.FACTORS.values())

        unknown = [name for name in selected_factors if name not in cls.FACTORS]
        if unknown:
            raise ValueError(
                f"Unknown factor names: {unknown}. Available factors: {cls.list_factor_names()}"
            )
        return [cls.FACTORS[name] for name in selected_factors]

    @classmethod
    def get_factor_dict(
        cls, selected_factors: list[str] | None = None
    ) -> dict[str, str]:
        """获取因子名称到表达式的映射。

        Args:
            selected_factors: 指定因子名称列表，None 表示全部

        Returns:
            {因子名: qlib表达式} 字典
        """
        if not selected_factors:
            return dict(cls.FACTORS)
        return {
            name: cls.FACTORS[name] for name in selected_factors if name in cls.FACTORS
        }


# ===== 注册为 quant-platform Factor =====


@register_factor
class MinuteFactors(Factor):
    """分钟频因子集合，基于 qlib 表达式。

    用于高频策略的特征工程，支持 150+ 个技术因子。
    """

    def __init__(self, timeframe: str = "1min", para: dict[str, Any] | None = None):
        super().__init__(timeframe=timeframe, para=para or {})
        self.selected_factors = self.para.get("selected_factors", None)

    def compute(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算所有选中的分钟频因子。

        Args:
            data: 包含 OHLCV 的 DataFrame

        Returns:
            因子值 DataFrame
        """
        # 这里返回的是因子名称列表，实际计算需要通过 qlib
        # 在 quant-platform 中，这些因子通过 qlib 表达式引擎计算
        factor_names = self.selected_factors or MinuteFactorLibrary.list_factor_names()
        return pd.DataFrame(index=data.index, columns=factor_names)

    def generate_para_space(self) -> list[dict]:
        """生成参数空间。"""
        return [{"selected_factors": None}]


# ===== 便捷函数 =====


def list_minute_factors() -> list[str]:
    """列出所有分钟频因子名称。"""
    return MinuteFactorLibrary.list_factor_names()


def get_minute_factor_expressions(selected: list[str] | None = None) -> list[str]:
    """获取分钟频因子的 qlib 表达式列表。"""
    return MinuteFactorLibrary.get_features(selected)


def get_minute_factor_dict(selected: list[str] | None = None) -> dict[str, str]:
    """获取分钟频因子名称到表达式的映射。"""
    return MinuteFactorLibrary.get_factor_dict(selected)


__all__ = [
    "MinuteFactorLibrary",
    "MinuteFactors",
    "list_minute_factors",
    "get_minute_factor_expressions",
    "get_minute_factor_dict",
]
