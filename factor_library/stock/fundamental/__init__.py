# -*- coding: utf-8 -*-
"""
股票基本面因子
"""
from .profitability import StockOCFtoNI, StockROEMomNAGrowth
from .profitability_extra import StockROICQoQChange, StockQuarterlyROIC, StockQuarterlyAbnormalGM
from .profitability_extra2 import StockEPSurplus, StockIntCoverage, StockInterestCoverageRatio
from .valuation import StockDividendYield
from .valuation_extra import StockEPChange60D, StockPEGDYRatio, StockRevenuePerShare
from .efficiency import StockAPTurnover, StockFATurnover, StockTotalAssetTurnover
from .efficiency_extra import StockAPDays, StockFARatio, StockNOAT, StockEquityTurnover
from .leverage import StockEquityRatio, StockDebtGrowthRate
from .leverage_extra import StockAccrualsToAssets, StockDebtYoYGrowth
from .growth import StockCapexGrowthRate, StockEarningsVolatility
from .growth_extra import StockCAGRCapex, StockIssuanceGrowthRate
from .cashflow import StockOpCashRatio
from .cashflow_extra import StockOpAssetChg, StockOpCostMargin, StockSalesExpenseRatio, StockTaxRate
from .standardized import StockStandardizedFinancialDebtChangeRatio, StockStandardizedOperatingProfit

__all__ = [
    # 盈利能力因子
    'StockOCFtoNI', 'StockROEMomNAGrowth', 'StockROICQoQChange', 'StockQuarterlyROIC',
    'StockQuarterlyAbnormalGM', 'StockEPSurplus', 'StockIntCoverage', 'StockInterestCoverageRatio',
    # 估值因子
    'StockDividendYield', 'StockEPChange60D', 'StockPEGDYRatio', 'StockRevenuePerShare',
    # 运营效率因子
    'StockAPTurnover', 'StockFATurnover', 'StockTotalAssetTurnover',
    'StockAPDays', 'StockFARatio', 'StockNOAT', 'StockEquityTurnover',
    # 资本结构因子
    'StockEquityRatio', 'StockDebtGrowthRate', 'StockAccrualsToAssets', 'StockDebtYoYGrowth',
    # 增长因子
    'StockCapexGrowthRate', 'StockEarningsVolatility', 'StockCAGRCapex', 'StockIssuanceGrowthRate',
    # 现金流因子
    'StockOpCashRatio', 'StockOpAssetChg', 'StockOpCostMargin', 'StockSalesExpenseRatio', 'StockTaxRate',
    # 标准化因子
    'StockStandardizedFinancialDebtChangeRatio', 'StockStandardizedOperatingProfit',
]