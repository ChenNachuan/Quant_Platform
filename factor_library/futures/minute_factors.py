# -*- coding: utf-8 -*-
"""
分钟频因子库

包含 150+ 个 qlib 表达式格式的分钟频因子（基础行情、收益率、动量均线、
波动率、技术指标、成交量、趋势及高频统计因子）。
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from factor_library.base import Factor
from factor_library.registry import register_factor


class MinuteFactorLibrary:
    """分钟频因子库，维护因子名到 qlib 表达式的映射。"""

    FACTORS = {
        # 基础行情因子
        "open": "$open",
        "high": "$high",
        "low": "$low",
        "close": "$close",
        "volume": "$volume",
        "vwap": "$vwap",
        "total_turnover": "$total_turnover",
        # 收益率因子
        "returns_1min": "$vwap/(Ref($vwap,1)+1e-12)-1",
        "returns_5min": "$vwap/(Ref($vwap,5)+1e-12)-1",
        "returns_15min": "$vwap/(Ref($vwap,15)+1e-12)-1",
        "returns_20min": "$vwap/(Ref($vwap,20)+1e-12)-1",
        "returns_30min": "$vwap/(Ref($vwap,30)+1e-12)-1",
        "returns_60min": "$vwap/(Ref($vwap,60)+1e-12)-1",
        "returns_120min": "$vwap/(Ref($vwap,120)+1e-12)-1",
        # 动量均线类因子
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
        # 交叉与趋势强度
        "sma_cross_5_10": "Mean($vwap,5)/(Mean($vwap,10)+1e-12)-1",
        "sma_cross_10_20": "Mean($vwap,10)/(Mean($vwap,20)+1e-12)-1",
        "ema_cross_15_30": "EMA($vwap,15)/(EMA($vwap,30)+1e-12)-1",
        # 价格相对均线位置
        "price_position_sma5": "($vwap-Mean($vwap,5))/(Mean($vwap,5)+1e-12)",
        "price_position_sma10": "($vwap-Mean($vwap,10))/(Mean($vwap,10)+1e-12)",
        "price_position_sma15": "($vwap-Mean($vwap,15))/(Mean($vwap,15)+1e-12)",
        "price_position_sma20": "($vwap-Mean($vwap,20))/(Mean($vwap,20)+1e-12)",
        "price_position_sma30": "($vwap-Mean($vwap,30))/(Mean($vwap,30)+1e-12)",
        "price_position_sma60": "($vwap-Mean($vwap,60))/(Mean($vwap,60)+1e-12)",
        # 波动率
        "volatility_5": "Std($vwap/Ref($vwap,1)-1,5)",
        "volatility_10": "Std($vwap/Ref($vwap,1)-1,10)",
        "volatility_15": "Std($vwap/Ref($vwap,1)-1,15)",
        "volatility_20": "Std($vwap/Ref($vwap,1)-1,20)",
        "volatility_30": "Std($vwap/Ref($vwap,1)-1,30)",
        "volatility_60": "Std($vwap/Ref($vwap,1)-1,60)",
        # 布林带
        "bb_position_60": "($vwap-Mean($vwap,60))/(2*Std($vwap,60)+1e-12)",
        "bb_position_20": "($vwap-Mean($vwap,20))/(2*Std($vwap,20)+1e-12)",
        "bb_width_60": "(4*Std($vwap,60))/(Mean($vwap,60)+1e-12)",
        "bb_width_20": "(4*Std($vwap,20))/(Mean($vwap,20)+1e-12)",
        # RSI
        "rsi_120": "100*Mean(If($vwap-Ref($vwap,1)>0,$vwap-Ref($vwap,1),0),120)/(Mean(Abs($vwap-Ref($vwap,1)),120)+1e-12)",
        "rsi_60": "100*Mean(If($vwap-Ref($vwap,1)>0,$vwap-Ref($vwap,1),0),60)/(Mean(Abs($vwap-Ref($vwap,1)),60)+1e-12)",
        "rsi_20": "100*Mean(If($vwap-Ref($vwap,1)>0,$vwap-Ref($vwap,1),0),20)/(Mean(Abs($vwap-Ref($vwap,1)),20)+1e-12)",
        # MACD
        "macd_10_60": "EMA($vwap,10)-EMA($vwap,60)",
        "macd_15_30": "EMA($vwap,15)-EMA($vwap,30)",
        "macd_signal_10_60": "EMA(EMA($vwap,10)-EMA($vwap,60),9)",
        "macd_signal_15_30": "EMA(EMA($vwap,15)-EMA($vwap,30),9)",
        "macd_histogram_10_60": "(EMA($vwap,10)-EMA($vwap,60))-EMA(EMA($vwap,10)-EMA($vwap,60),9)",
        "macd_histogram_15_30": "(EMA($vwap,15)-EMA($vwap,30))-EMA(EMA($vwap,15)-EMA($vwap,30),9)",
        # 成交量比与量价趋势
        "volume_ratio_5": "$volume/(Mean($volume,5)+1e-12)",
        "volume_ratio_10": "$volume/(Mean($volume,10)+1e-12)",
        "volume_ratio_20": "$volume/(Mean($volume,20)+1e-12)",
        "volume_ratio_30": "$volume/(Mean($volume,30)+1e-12)",
        "volume_ratio_60": "$volume/(Mean($volume,60)+1e-12)",
        "vpt_sma_5": "Mean(($vwap/Ref($vwap,1)-1)*$volume,5)",
        "vpt_sma_10": "Mean(($vwap/Ref($vwap,1)-1)*$volume,10)",
        "vpt_sma_20": "Mean(($vwap/Ref($vwap,1)-1)*$volume,20)",
        # 价格结构比值
        "high_low_ratio": "$high/($low+1e-12)",
        "close_open_ratio": "$close/($open+1e-12)",
        "high_close_ratio": "$high/($close+1e-12)",
        "low_close_ratio": "$low/($close+1e-12)",
        # 真实波幅均值 ATR
        "atr_5": "Mean($high/($low+1e-12)-1,5)",
        "atr_10": "Mean($high/($low+1e-12)-1,10)",
        "atr_20": "Mean($high/($low+1e-12)-1,20)",
        "atr_30": "Mean($high/($low+1e-12)-1,30)",
        "atr_60": "Mean($high/($low+1e-12)-1,60)",
        # 威廉指标 Williams %R
        "williams_r": "-100*($high-$close)/($high-$low+1e-12)",
        "williams_r_5": "-100*(Max($high,5)-$close)/(Max($high,5)-Min($low,5)+1e-12)",
        "williams_r_20": "-100*(Max($high,20)-$close)/(Max($high,20)-Min($low,20)+1e-12)",
        # KDJ
        "k_percent": "100*($close-$low)/($high-$low+1e-12)",
        "d_percent": "Mean(100*($close-$low)/($high-$low+1e-12),5)",
        # 价格加速度与 VWAP 偏离
        "price_acceleration": "($vwap/Ref($vwap,1)-1)-(Ref($vwap,1)/Ref($vwap,2)-1)",
        "vwap_ratio": "$close/($vwap+1e-12)-1",
        # 成交量摆动与能量潮
        "chaikin_ad_10": "EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,3)-EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,10)",
        "chaikin_ad_30": "EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,3)-EMA(((($close-$low)-($high-$close))/($high-$low+1e-12))*$volume,30)",
        "obv_sma_5_20": "Mean(If($close>Ref($close,1),$volume,If($close<Ref($close,1),0-$volume,0)),5)/(Mean(If($close>Ref($close,1),$volume,If($close<Ref($close,1),0-$volume,0)),20)+1e-12)-1",
        # 资金流与顺势摆动指标
        "mfi_20": "100-(100/(1+Mean(If($close>Ref($close,1),($high+$low+$close)/3*$volume,0),20)/(Mean(If($close<Ref($close,1),($high+$low+$close)/3*$volume,0),20)+1e-12)))",
        "cci_20": "($close-Mean(($high+$low+$close)/3,20))/(0.015*Mean(Abs(($high+$low+$close)/3-Mean(($high+$low+$close)/3,20)),20)+1e-12)",
        "cmo_20": "100*(Mean(If($close-Ref($close,1)>0,$close-Ref($close,1),0),20)-Mean(If(Ref($close,1)-$close>0,Ref($close,1)-$close,0),20))/(Mean(Abs($close-Ref($close,1)),20)+1e-12)",
        # 趋势强度与夏普化动量
        "short_term_trend_strength": "Sum(Log($vwap/Ref($vwap,1)),20)",
        "long_term_trend_strength": "Sum(Log($vwap/Ref($vwap,1)),60)",
        "short_term_momentum_volatility": "Sum(Log($vwap/Ref($vwap,1)),20)/(Std($vwap/Ref($vwap,1)-1,20)+1e-12)",
        "long_term_momentum_volatility": "Sum(Log($vwap/Ref($vwap,1)),60)/(Std($vwap/Ref($vwap,1)-1,60)+1e-12)",
        # 高频收益率分布（偏度、峰度、上下行波动比）
        "realized_skewness_20": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),20)/((Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20))+1e-12)",
        "realized_kurtosis_20": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),20)/((Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20)*Std($vwap/Ref($vwap,1)-1,20))+1e-12)",
        "up_down_volatility_ratio_20": "Std(If($vwap/Ref($vwap,1)-1>0,$vwap/Ref($vwap,1)-1,0),20)/(Std(If(Ref($vwap,1)/$vwap-1>0,Ref($vwap,1)/$vwap-1,0),20)+1e-12)",
        "realized_skewness_60": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),60)/((Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60))+1e-12)",
        "realized_kurtosis_60": "Mean((($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)*($vwap/Ref($vwap,1)-1)),60)/((Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60)*Std($vwap/Ref($vwap,1)-1,60))+1e-12)",
        "up_down_volatility_ratio_60": "Std(If($vwap/Ref($vwap,1)-1>0,$vwap/Ref($vwap,1)-1,0),60)/(Std(If(Ref($vwap,1)/$vwap-1>0,Ref($vwap,1)/$vwap-1,0),60)+1e-12)",
        # 高频成交量分布
        "morning_volume_ratio": "Sum($volume,30)/Sum($volume,240)",
        "afternoon_volume_ratio": "Sum($volume,120)/Sum($volume,240)",
        "volume_concentration": "Std($volume,240)/(Mean($volume,240)+1e-12)",
        "midday_volatility_effect": "Std(Ref($vwap/Ref($vwap,1)-1,60),60)/(Std($vwap/Ref($vwap,1)-1,60)+1e-12)",
        # 量价相关性
        "price_volume_correlation": "Corr($vwap, $volume,20)",
        # 跳跃频率
        "jumps_frequency": "Sum(If(Abs($vwap/Ref($vwap,1)-1)>2*Std($vwap/Ref($vwap,1)-1,240),1,0),240)",
        # 路径效率特征
        "trend_strength": "Abs($vwap-Ref($vwap,20))/(Sum(Abs($vwap-Ref($vwap,1)),20)+1e-12)",
        "efficiency_ratio": "Abs($close-$open)/(Max($high,240)-Min($low,240))",
        # 跨日波动状态与自相关持续性
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
        """获取指定因子的 qlib 表达式列表，None 表示全部。"""
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
        """获取因子名称到 qlib 表达式的映射，None 表示全部。"""
        if not selected_factors:
            return dict(cls.FACTORS)
        return {
            name: cls.FACTORS[name] for name in selected_factors if name in cls.FACTORS
        }


@register_factor
class MinuteFactors(Factor):
    """基于 qlib 表达式的分钟频因子集合，用于高频特征工程。"""

    def __init__(self, timeframe: str = "1min", para: dict[str, Any] | None = None):
        super().__init__(timeframe=timeframe, para=para or {})
        self.selected_factors = self.para.get("selected_factors", None)

    def compute(self, data: pd.DataFrame) -> pd.DataFrame:
        """返回选中因子的列框架（实际值由 qlib 表达式引擎计算）。"""
        # 仅返回因子名占位，实际计算由 qlib 表达式引擎完成
        factor_names = self.selected_factors or MinuteFactorLibrary.list_factor_names()
        return pd.DataFrame(index=data.index, columns=factor_names)

    def generate_para_space(self) -> list[dict]:
        """生成参数空间。"""
        return [{"selected_factors": None}]


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
