# -*- coding: utf-8 -*-
"""
多专家集成策略模块

从 high-freq 项目迁移的集成组件：
- expert_ensemble: 多专家模型集成
- regime_ensemble: 市场状态集成
"""

from engine.ensemble.expert_ensemble import (
    ExpertConfig,
    backtest_ensemble_signal,
    combine_signals,
    fit_signal_standardizer,
    optimize_static_ensemble,
    train_expert_ensemble,
    train_expert_model,
)

from engine.ensemble.regime_ensemble import (
    EnsembleRegimeThresholdConfig,
    EnsembleStrictRegimeFilterConfig,
    run_dynamic_threshold_ensemble_backtest,
    run_high_vol_volume_hurst_filter_backtest,
)

__all__ = [
    # Expert ensemble
    "ExpertConfig",
    "backtest_ensemble_signal",
    "combine_signals",
    "fit_signal_standardizer",
    "optimize_static_ensemble",
    "train_expert_ensemble",
    "train_expert_model",
    # Regime ensemble
    "EnsembleRegimeThresholdConfig",
    "EnsembleStrictRegimeFilterConfig",
    "run_dynamic_threshold_ensemble_backtest",
    "run_high_vol_volume_hurst_filter_backtest",
]
