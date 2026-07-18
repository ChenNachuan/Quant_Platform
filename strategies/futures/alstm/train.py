# -*- coding: utf-8 -*-
"""
期货 ALSTM 两阶段策略训练入口

根据文档配置，实现：
1. Model A：二分类，判断是否值得交易，阈值5bp
2. Model B：
   - full 全样本训练，三分类，阈值8bp
   - nonflat 只用超过5bp的样本训练，二分类

训练窗口：12月训练，6月验证，3月/6月测试
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

# 输出目录
OUTPUT_DIR = ARTIFACT_ROOT / "rolling_710"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def log(msg: str):
    """输出日志"""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} {msg}")


def create_rolling_windows(test_start: str, test_end: str):
    """创建滚动窗口"""
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
    """创建交易标签（二分类）"""
    future_return = frame["pv3_future_return"]
    return (future_return.abs() > threshold).astype(float)


def make_dir_label(frame: pd.DataFrame, threshold: float) -> pd.Series:
    """创建方向标签（三分类）"""
    future_return = frame["pv3_future_return"]
    label = pd.Series(1, index=frame.index)  # flat
    label[future_return > threshold] = 2  # up
    label[future_return < -threshold] = 0  # down
    return label.astype(int)


def make_dir_binary_label(frame: pd.DataFrame) -> pd.Series:
    """创建方向标签（二分类）"""
    future_return = frame["pv3_future_return"]
    return (future_return > 0).astype(int)


def train_model_a(
    frame: pd.DataFrame, feature_cols: list, window: dict, window_dir: Path
):
    """训练 Model A（二分类，判断是否值得交易）"""
    log(f"  训练 Model A (阈值={MODEL_A_THRESHOLD})")

    # 创建标签
    frame["label_trade"] = make_trade_label(frame, MODEL_A_THRESHOLD)

    # 训练
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
    """训练 Model B Full（三分类，阈值8bp）"""
    log(f"  训练 Model B Full (阈值={MODEL_B_FULL_THRESHOLD})")

    # 创建标签
    frame["label_dir3"] = make_dir_label(frame, MODEL_B_FULL_THRESHOLD)

    # 训练
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
    """训练 Model B Nonflat（二分类，只用超过5bp的样本）"""
    log(f"  训练 Model B Nonflat (阈值={MODEL_B_NONFLAT_THRESHOLD})")

    # 创建标签
    frame["label_dir_bin"] = make_dir_binary_label(frame)

    # 创建样本权重（只使用超过阈值的样本）
    future_return = frame["pv3_future_return"]
    sample_weight = (future_return.abs() > MODEL_B_NONFLAT_THRESHOLD).astype(float)

    # 训练
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
    """评估模型准确率"""
    # 预测
    prob_df, pred_state, true_state, raw_label = predict_state_classifier(
        model,
        frame,
        segment="test",
        batch_size=128,
        state_threshold=0.5,
    )

    # 计算准确率
    accuracy = (pred_state == true_state).mean()

    return {
        "model": model_name,
        "accuracy": float(accuracy),
        "n_samples": len(pred_state),
    }


def main():
    """主函数"""
    log("开始 7.10 策略训练")

    # 创建滚动窗口
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

        # TODO: 加载数据并训练模型
        # 这里需要实现数据加载和训练逻辑
        # 由于数据加载依赖 qlib，需要在 notebook 中实现

        log(f"  窗口 {window_id} 完成")

    log("\n训练完成!")
    log(f"结果保存到: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
