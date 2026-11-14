# -*- coding: utf-8 -*-
"""
期货 ALSTM 两阶段策略训练入口

- Model A：二分类，判断行情是否值得交易（阈值 5bp）
- Model B：full（全样本三分类，阈值 8bp）/ nonflat（仅超 5bp 样本二分类）
- 滚动窗口：12 月训练，6 月验证，3 月测试
"""

import time
from pathlib import Path

import pandas as pd

from engine.models.alstm import predict_state_classifier, train_state_classifier
from strategies.futures.alstm.config import (
    ALSTM_PARAMS,
    ARTIFACT_ROOT,
    MODEL_A_THRESHOLD,
    MODEL_B_FULL_THRESHOLD,
    MODEL_B_NONFLAT_THRESHOLD,
    TEST_MONTHS,
    TRAIN_MONTHS,
    VALID_MONTHS,
)

OUTPUT_DIR = ARTIFACT_ROOT / "rolling_710"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    """输出带时间戳日志"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} {msg}")


def create_rolling_windows(test_start: str, test_end: str):
    """按 12 月训练 / 6 月验证 / 3 月测试生成滚动窗口。"""
    windows = []
    test_start_dt = pd.Timestamp(test_start)
    test_end_dt = pd.Timestamp(test_end)

    current_test_start = test_start_dt
    while current_test_start < test_end_dt:
        current_test_end = current_test_start + pd.DateOffset(months=TEST_MONTHS)
        if current_test_end > test_end_dt:
            current_test_end = test_end_dt

        valid_end = current_test_start
        valid_start = valid_end - pd.DateOffset(months=VALID_MONTHS)
        train_end = valid_start
        train_start = train_end - pd.DateOffset(months=TRAIN_MONTHS)

        windows.append(
            {
                "window_id": current_test_start.strftime("%Y-%m"),
                "train_start": train_start.strftime("%Y-%m-%d"),
                "train_end": train_end.strftime("%Y-%m-%d"),
                "valid_start": valid_start.strftime("%Y-%m-%d"),
                "valid_end": valid_end.strftime("%Y-%m-%d"),
                "test_start": current_test_start.strftime("%Y-%m-%d"),
                "test_end": current_test_end.strftime("%Y-%m-%d"),
            }
        )

        current_test_start = current_test_end

    return windows


def make_trade_label(frame: pd.DataFrame, threshold: float) -> pd.Series:
    """构建交易过滤二分类标签（|future_return| > threshold）。"""
    future_return = frame["pv3_future_return"]
    return (future_return.abs() > threshold).astype(float)


def make_dir_label(frame: pd.DataFrame, threshold: float) -> pd.Series:
    """构建三分类方向标签（0=下跌，1=震荡，2=上涨）。"""
    future_return = frame["pv3_future_return"]
    label = pd.Series(1, index=frame.index)  # 震荡
    label[future_return > threshold] = 2  # 上涨
    label[future_return < -threshold] = 0  # 下跌
    return label.astype(int)


def make_dir_binary_label(frame: pd.DataFrame) -> pd.Series:
    """构建二分类方向标签（1=上涨，0=下跌）。"""
    future_return = frame["pv3_future_return"]
    return (future_return > 0).astype(int)


def train_model_a(
    frame: pd.DataFrame, feature_cols: list, window: dict, window_dir: Path
):
    """训练 Model A（二分类，判断是否值得交易）。"""
    log(f"  训练 Model A (阈值={MODEL_A_THRESHOLD})")

    frame["label_trade"] = make_trade_label(frame, MODEL_A_THRESHOLD)

    params = dict(ALSTM_PARAMS)
    params.update(
        {
            "state_num_classes": 2,
            "state_class_names": {0: "no_trade", 1: "trade"},
            "state_class_weight_mode": "sqrt_balanced",
            "state_class_weight_clip": (0.5, 3.0),
            "seed": 6341,
        }
    )

    result = train_state_classifier(
        frame,
        d_feat=len(feature_cols),
        model_params=params,
        state_threshold=0.5,
        model_save_path=window_dir / "model_a_trade.pt",
        show_progress=True,
    )

    return result


def train_model_b_full(
    frame: pd.DataFrame, feature_cols: list, window: dict, window_dir: Path
):
    """训练 Model B Full（全样本三分类，阈值 8bp）。"""
    log(f"  训练 Model B Full (阈值={MODEL_B_FULL_THRESHOLD})")

    frame["label_dir3"] = make_dir_label(frame, MODEL_B_FULL_THRESHOLD)

    params = dict(ALSTM_PARAMS)
    params.update(
        {
            "state_num_classes": 3,
            "state_class_names": {0: "dir_down", 1: "dir_uncertain", 2: "dir_up"},
            "state_class_weight_mode": "sqrt_balanced",
            "state_class_weight_clip": (0.5, 3.0),
            "n_epochs": 18,
            "early_stop": 5,
            "seed": 6346,
        }
    )

    result = train_state_classifier(
        frame,
        d_feat=len(feature_cols),
        model_params=params,
        state_threshold=0.5,
        model_save_path=window_dir / "model_b_full.pt",
        show_progress=True,
    )

    return result


def train_model_b_nonflat(
    frame: pd.DataFrame, feature_cols: list, window: dict, window_dir: Path
):
    """训练 Model B Nonflat（仅用超 5bp 样本作二分类）。"""
    log(f"  训练 Model B Nonflat (阈值={MODEL_B_NONFLAT_THRESHOLD})")

    frame["label_dir_bin"] = make_dir_binary_label(frame)

    # 通过样本权重屏蔽绝对收益未超阈值的样本
    future_return = frame["pv3_future_return"]
    sample_weight = (future_return.abs() > MODEL_B_NONFLAT_THRESHOLD).astype(float)

    params = dict(ALSTM_PARAMS)
    params.update(
        {
            "state_num_classes": 2,
            "state_class_names": {0: "dir_down", 1: "dir_up"},
            "state_class_weight_mode": "sqrt_balanced",
            "state_class_weight_clip": (0.5, 3.0),
            "n_epochs": 18,
            "early_stop": 5,
            "seed": 6346,
        }
    )

    result = train_state_classifier(
        frame,
        d_feat=len(feature_cols),
        model_params=params,
        state_threshold=0.5,
        state_sample_weight_by_segment={"train": sample_weight, "valid": sample_weight},
        model_save_path=window_dir / "model_b_nonflat.pt",
        show_progress=True,
    )

    return result


def evaluate_model(
    model, frame: pd.DataFrame, feature_cols: list, window: dict, model_name: str
):
    """在测试集上评估模型准确率。"""
    prob_df, pred_state, true_state, raw_label = predict_state_classifier(
        model,
        frame,
        segment="test",
        batch_size=128,
        state_threshold=0.5,
    )

    accuracy = (pred_state == true_state).mean()

    return {
        "model": model_name,
        "accuracy": float(accuracy),
        "n_samples": len(pred_state),
    }


def main():
    """滚动训练主入口。"""
    log("开始 7.10 策略训练")

    windows = create_rolling_windows("2025-01-01", "2025-12-31")
    log(f"创建 {len(windows)} 个滚动窗口")

    for window in windows:
        window_id = window["window_id"]
        window_dir = OUTPUT_DIR / window_id
        window_dir.mkdir(parents=True, exist_ok=True)

        log(f"\n处理窗口: {window_id}")
        log(f"  训练: {window['train_start']} ~ {window['train_end']}")
        log(f"  验证: {window['valid_start']} ~ {window['valid_end']}")
        log(f"  测试: {window['test_start']} ~ {window['test_end']}")

        # TODO: 接入 qlib 数据加载并执行各窗口训练流程

        log(f"  窗口 {window_id} 完成")

    log("\n训练完成!")
    log(f"结果保存到: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
