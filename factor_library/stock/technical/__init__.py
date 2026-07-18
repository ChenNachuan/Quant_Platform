# -*- coding: utf-8 -*-
"""
股票技术因子
"""
from .momentum import StockSimpleMomentum, StockMomentumReturn
from .momentum_r11 import StockMomentumR11
from .momentum_extra import StockMomentum12M, StockRankMomentum
from .volatility import StockVolatility
from .volatility_amqi import StockHistoricalVolatility, StockBeta, StockDownsideBeta, StockCVaR
from .frazzini_beta import StockFrazziniPedersenBeta
from .atr import StockATR, StockATRExpansion
from .atr_extra import StockATRPriceBreakout, StockATRPricePosition, StockATRTrend, StockATRVolumeConfirmation
from .bollinger import StockBollinger, StockBollingerSqueezeExpansion
from .bollinger_extra import StockBollingerBreakoutUpper, StockBollingerMiddleSupport, StockBollingerOversoldBounce
from .obv import StockOBV, StockOBVSlope
from .obv_extra import StockOBVBreakthrough, StockOBVChangeRate, StockOBVDivergence, StockOBVRank
from .turnover import StockTurnover, StockTurnoverVolatility
from .turnover_extra import StockDailyTurnoverRate, StockMonthlyTurnover, StockTurnoverResidual
from .risk_adjusted_momentum import StockRiskAdjustedMomentum
from .ichimoku import StockIchimoku, StockIchimokuCloudTrend, StockIchimokuTKCross
from .ichimoku_extra import StockIchimokuCloudWidthMomentum, StockIchimokuPricePosition
from .mfi import StockMFI, StockMFIChangeRate
from .mfi_extra import StockMFIDivergence
from .pvt import StockPVT, StockPVTDivergence
from .pvt_extra import StockPVTMADeviation, StockPVTMomentumReversal
from .rvi import StockRVI, StockRVIStrength
from .rvi_extra import StockRVICross, StockRVIDiff, StockRVITrend, StockRVIValue, StockRVIVolume
from .rsi import StockRSI
from .reversal import StockShortTermReversal, StockMonthlyExcessReversal
from .swma_tema import StockSWMA, StockTEMA
from .coppock import StockCoppockCurve
from .max_price_52w import StockMaxPrice52WRatio
from .volume_price import StockVolumePriceDivergence

__all__ = [
    # 动量因子
    'StockSimpleMomentum', 'StockMomentumReturn', 'StockMomentumR11', 'StockMomentum12M', 'StockRankMomentum',
    # 波动率因子
    'StockVolatility', 'StockHistoricalVolatility', 'StockBeta', 'StockDownsideBeta', 'StockCVaR',
    'StockFrazziniPedersenBeta',
    # ATR 因子
    'StockATR', 'StockATRExpansion', 'StockATRPriceBreakout', 'StockATRPricePosition',
    'StockATRTrend', 'StockATRVolumeConfirmation',
    # Bollinger 因子
    'StockBollinger', 'StockBollingerSqueezeExpansion', 'StockBollingerBreakoutUpper',
    'StockBollingerMiddleSupport', 'StockBollingerOversoldBounce',
    # OBV 因子
    'StockOBV', 'StockOBVSlope', 'StockOBVBreakthrough', 'StockOBVChangeRate',
    'StockOBVDivergence', 'StockOBVRank',
    # 换手率因子
    'StockTurnover', 'StockTurnoverVolatility', 'StockDailyTurnoverRate',
    'StockMonthlyTurnover', 'StockTurnoverResidual',
    # 复合因子
    'StockRiskAdjustedMomentum',
    # Ichimoku 因子
    'StockIchimoku', 'StockIchimokuCloudTrend', 'StockIchimokuTKCross',
    'StockIchimokuCloudWidthMomentum', 'StockIchimokuPricePosition',
    # MFI 因子
    'StockMFI', 'StockMFIChangeRate', 'StockMFIDivergence',
    # PVT 因子
    'StockPVT', 'StockPVTDivergence', 'StockPVTMADeviation', 'StockPVTMomentumReversal',
    # RVI 因子
    'StockRVI', 'StockRVIStrength', 'StockRVICross', 'StockRVIDiff',
    'StockRVITrend', 'StockRVIValue', 'StockRVIVolume',
    # RSI 因子
    'StockRSI',
    # 反转因子
    'StockShortTermReversal', 'StockMonthlyExcessReversal',
    # 移动平均因子
    'StockSWMA', 'StockTEMA',
    # 其他因子
    'StockCoppockCurve', 'StockMaxPrice52WRatio', 'StockVolumePriceDivergence',
]
