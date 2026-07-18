# -*- coding: utf-8 -*-
"""
期货回测引擎模块

从 high-freq 项目迁移的回测组件：
- futures_backtest: 期货日内回测引擎
- regime_filter: 市场状态过滤回测
"""

from engine.backtest.futures_backtest import (
    IFFuturesExchange,
    IFFuturesPosition,
    IFIntradaySignalStrategy,
    StrategyRuleConfig,
    run_backtest,
    search_optimal_thresholds,
    shift_signal_for_execution,
)

from engine.backtest.regime_filter import (
    RegimeFilterConfig,
    run_regime_filtered_state_backtest,
    run_regime_filtered_regression_backtest,
    build_regime_filtered_state_target_lots,
)

__all__ = [
    # Futures backtest
    "IFFuturesExchange",
    "IFFuturesPosition",
    "IFIntradaySignalStrategy",
    "StrategyRuleConfig",
    "run_backtest",
    "search_optimal_thresholds",
    "shift_signal_for_execution",
    # Regime filter
    "RegimeFilterConfig",
    "run_regime_filtered_state_backtest",
    "run_regime_filtered_regression_backtest",
    "build_regime_filtered_state_target_lots",
]
