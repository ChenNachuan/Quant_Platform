"""机器学习信号子包：模型输出适配、特征工程、实验管理与过拟合检验。"""

from research.ml.signal import (
    VERDICTS,
    MLSignalAdapter,
    build_feature_matrix,
    ensemble_models,
    features_hash,
    model_id,
    sanity_check_ml_model,
    smooth_signal,
    transform_bars,
    write_experiment,
    write_model,
)

__all__ = [
    "MLSignalAdapter",
    "VERDICTS",
    "build_feature_matrix",
    "ensemble_models",
    "features_hash",
    "model_id",
    "sanity_check_ml_model",
    "smooth_signal",
    "transform_bars",
    "write_experiment",
    "write_model",
]
