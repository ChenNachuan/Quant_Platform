"""
Model training module for ML Factor Strategy.
"""
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor

from .config import RandomForestConfig, XGBoostConfig

warnings.filterwarnings('ignore')


class ModelTrainer:
    """模型训练器基类."""

    def __init__(self, train_window_months: int = 24):
        self.train_window_months = train_window_months
        self.model = None
        self.feature_importances = []

    def train_and_predict(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        """滚动训练和预测."""
        raise NotImplementedError

    def _prepare_data(
        self, df: pd.DataFrame, feature_cols: List[str], predict_date, unique_dates: list, date_idx: int
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.Series], Optional[pd.DataFrame]]:
        """准备训练和测试数据."""
        start_date = unique_dates[date_idx - self.train_window_months]

        train_mask = (df['date'] >= start_date) & (df['date'] < predict_date)
        test_mask = (df['date'] == predict_date)

        X_train = df.loc[train_mask, feature_cols]
        y_train = df.loc[train_mask, 'rank_ret']
        X_test = df.loc[test_mask, feature_cols]

        if len(X_train) == 0 or len(X_test) == 0:
            return None, None, None

        return X_train, y_train, X_test


class RandomForestTrainer(ModelTrainer):
    """Random Forest 模型训练器."""

    def __init__(self, config: RandomForestConfig):
        super().__init__(config.train_window_months)
        self.config = config

    def train_and_predict(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        """滚动训练和预测."""
        print("=" * 50)
        print("Step 2: Random Forest 滚动预测")
        print("=" * 50)

        unique_dates = sorted(df['date'].unique())
        predictions = []
        self.feature_importances = []

        print(f"开始滚动预测... 总共有 {len(unique_dates)} 个月的数据。")

        for i in range(self.train_window_months, len(unique_dates)):
            predict_date = unique_dates[i]

            X_train, y_train, X_test = self._prepare_data(df, feature_cols, predict_date, unique_dates, i)
            if X_train is None:
                continue

            # 训练模型
            model = RandomForestRegressor(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                min_samples_leaf=self.config.min_samples_leaf,
                n_jobs=self.config.n_jobs,
                random_state=self.config.random_state
            )

            model.fit(X_train, y_train)

            # 预测
            pred_y = model.predict(X_test)

            # 存储结果
            temp_res = df.loc[df['date'] == predict_date, ['date', 'stkcd', 'next_ret', 'raw_size']].copy()
            temp_res['pred_ret'] = pred_y
            predictions.append(temp_res)

            # 记录特征重要性
            if i % 12 == 0:
                self.feature_importances.append({
                    'date': predict_date,
                    'importance': model.feature_importances_
                })

            print(f"已完成预测: {predict_date.date()}")

        all_predictions = pd.concat(predictions)
        print("第二步完成：预测数据生成完毕。")
        return all_predictions


class XGBoostTrainer(ModelTrainer):
    """XGBoost 模型训练器."""

    def __init__(self, config: XGBoostConfig):
        super().__init__(config.train_window_months)
        self.config = config

    def train_and_predict(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        """滚动训练和预测."""
        try:
            import xgboost as xgb
        except ImportError:
            raise ImportError("请安装 xgboost: pip install xgboost")

        print("=" * 50)
        print("Step 2: XGBoost 滚动预测")
        print("=" * 50)

        unique_dates = sorted(df['date'].unique())
        predictions = []
        self.feature_importances = []

        print(f"开始滚动预测... 总共有 {len(unique_dates)} 个月的数据。")

        for i in range(self.train_window_months, len(unique_dates)):
            predict_date = unique_dates[i]

            X_train, y_train, X_test = self._prepare_data(df, feature_cols, predict_date, unique_dates, i)
            if X_train is None:
                continue

            # 训练模型
            model = xgb.XGBRegressor(
                n_estimators=self.config.n_estimators,
                max_depth=self.config.max_depth,
                learning_rate=self.config.learning_rate,
                tree_method=self.config.tree_method,
                device=self.config.device,
                objective=self.config.objective,
                subsample=self.config.subsample,
                colsample_bytree=self.config.colsample_bytree,
                reg_alpha=self.config.reg_alpha,
                reg_lambda=self.config.reg_lambda,
                n_jobs=self.config.n_jobs,
                random_state=self.config.random_state
            )

            model.fit(X_train, y_train)

            # 预测
            pred_y = model.predict(X_test)

            # 存储结果
            temp_res = df.loc[df['date'] == predict_date, ['date', 'stkcd', 'next_ret', 'raw_size']].copy()
            temp_res['pred_ret'] = pred_y
            predictions.append(temp_res)

            # 记录特征重要性
            if i % 12 == 0:
                self.feature_importances.append({
                    'date': predict_date,
                    'importance': model.feature_importances_
                })

            print(f"已完成预测: {predict_date.date()}")

        all_predictions = pd.concat(predictions)
        print("第二步完成：XGBoost 预测生成完毕。")
        return all_predictions


def create_trainer(model_type: str, rf_config: RandomForestConfig = None, xgb_config: XGBoostConfig = None) -> ModelTrainer:
    """创建模型训练器工厂函数."""
    if model_type == "random_forest":
        if rf_config is None:
            rf_config = RandomForestConfig()
        return RandomForestTrainer(rf_config)
    elif model_type == "xgboost":
        if xgb_config is None:
            xgb_config = XGBoostConfig()
        return XGBoostTrainer(xgb_config)
    else:
        raise ValueError(f"不支持的模型类型: {model_type}")
