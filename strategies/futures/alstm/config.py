"""Single source of truth for the futures ALSTM strategy defaults."""

from pathlib import Path
from typing import Final

STRATEGY_NAME: Final = "futures_alstm"
PROJECT_ROOT: Final = Path(__file__).resolve().parents[3]
DATA_ROOT: Final = PROJECT_ROOT / "data_lake" / "future" / "market_data"
ARTIFACT_ROOT: Final = PROJECT_ROOT / "artifacts" / STRATEGY_NAME
PLOT_ROOT: Final = ARTIFACT_ROOT / "plots"

MODEL_A_THRESHOLD: Final = 0.0005
MODEL_B_FULL_THRESHOLD: Final = 0.0008
MODEL_B_NONFLAT_THRESHOLD: Final = 0.0005

TRAIN_MONTHS: Final = 12
VALID_MONTHS: Final = 6
TEST_MONTHS: Final = 3

ALSTM_PARAMS: Final = {
    "hidden_size": 32,
    "num_layers": 2,
    "dropout": 0.3,
    "n_epochs": 30,
    "lr": 1e-4,
    "batch_size": 128,
    "early_stop": 8,
    "metric": "loss",
    "loss": "mse",
    "optimizer": "adam",
    "GPU": 0,
    "seed": 42,
}

STATE_PARAMS: Final = {
    "state_hidden_size": 32,
    "state_num_classes": 3,
    "state_class_names": {0: "down", 1: "flat", 2: "up"},
    "state_lr": 5e-4,
    "state_class_weight_mode": "sqrt_balanced",
    "state_class_weight_clip": (0.5, 3.0),
    "state_loss": "cross_entropy",
    "state_selection_metric": "valid_accuracy",
    "state_selection_min_pred_trade_ratio": 0.03,
    "state_selection_max_pred_trade_ratio": 0.45,
    "state_selection_min_trade_count": 20,
}
