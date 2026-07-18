"""
ML Factor Strategy Module.

This module implements machine learning based stock selection strategies
using Random Forest and XGBoost models.
"""

from .config import (
    BacktestConfig,
    BarraConfig,
    DataConfig,
    PreprocessConfig,
    RandomForestConfig,
    StrategyConfig,
    XGBoostConfig,
)
from .data_preprocessor import DataPreprocessor, BenchmarkDataLoader
from .model_trainer import create_trainer, RandomForestTrainer, XGBoostTrainer
from .backtest import BacktestEngine, PortfolioConstructor, PerformanceCalculator, StatisticsCalculator
from .evaluator import ICEvaluator, BarraEvaluator, FamaMacBethEvaluator, FactorModelEvaluator, GRSEvaluator
from .visualizer import StrategyVisualizer

__all__ = [
    # Config
    'StrategyConfig',
    'DataConfig',
    'PreprocessConfig',
    'RandomForestConfig',
    'XGBoostConfig',
    'BacktestConfig',
    'BarraConfig',
    # Data
    'DataPreprocessor',
    'BenchmarkDataLoader',
    # Models
    'create_trainer',
    'RandomForestTrainer',
    'XGBoostTrainer',
    # Backtest
    'BacktestEngine',
    'PortfolioConstructor',
    'PerformanceCalculator',
    'StatisticsCalculator',
    # Evaluators
    'ICEvaluator',
    'BarraEvaluator',
    'FamaMacBethEvaluator',
    'FactorModelEvaluator',
    'GRSEvaluator',
    # Visualization
    'StrategyVisualizer',
]