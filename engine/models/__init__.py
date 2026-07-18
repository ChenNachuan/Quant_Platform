# -*- coding: utf-8 -*-
"""
深度学习模型模块

包含从 high-freq 项目迁移的模型：
- ALSTM (Attention LSTM): 主力模型，支持回归和三状态分类
- LGBM: LightGBM 模型
- Distribution NLL: 分布 NLL 模型
"""

from engine.models.alstm import (
    ALSTMStateClassifier,
    build_alstm_model,
    train_predict_alstm,
    train_predict_return_and_state_alstm,
    train_state_classifier,
    predict_state_classifier,
)

from engine.models.lgbm import (
    train_predict_return_and_state_lgbm,
)

from engine.models.distribution_nll import (
    train_predict_distribution_nll_alstm,
)

__all__ = [
    # ALSTM
    "ALSTMStateClassifier",
    "build_alstm_model",
    "train_predict_alstm",
    "train_predict_return_and_state_alstm",
    "train_state_classifier",
    "predict_state_classifier",
    # LGBM
    "train_predict_return_and_state_lgbm",
    # Distribution NLL
    "train_predict_distribution_nll_alstm",
]
