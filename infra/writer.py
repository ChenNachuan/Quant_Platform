"""
Parquet 数据写入：市场数据 + 因子数据。
支持 data_lake/{asset_type}/{category}/{sub_type} 三级层次结构。
"""

import uuid
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class DataWriter:
    """负责将 DataFrame 写入 Parquet 数据湖。"""

    def __init__(self, data_lake_dir: Path):
        self.data_lake_dir = data_lake_dir

    def write_data(
        self,
        df: pd.DataFrame,
        asset_type: str = "stock",
        category: str = "market_data",
        sub_type: str = "1d",
        partition_cols=None,
    ) -> None:
        """
        通用数据写入接口。
        Path: data_lake/{asset_type}/{category}/{sub_type}/year=YYYY/data.parquet

        Args:
            df: 要写入的 DataFrame
            asset_type: 品种 (stock, etf, future, option)
            category: 数据类型 (market_data, financials, reference)
            sub_type: 子类型 (1d, min5, balance_sheet, etc.)
            partition_cols: 分区列，默认 ['year']
        """
        if partition_cols is None:
            partition_cols = ["year"]

        if df.empty:
            logger.warning("尝试写入空 DataFrame，操作已跳过")
            return

        target_dir = self.data_lake_dir / asset_type / category / sub_type
        target_dir.mkdir(parents=True, exist_ok=True)

        df_to_write = df.copy()

        if "timestamp" not in df_to_write.columns and isinstance(
            df_to_write.index, pd.DatetimeIndex
        ):
            df_to_write["timestamp"] = df_to_write.index

        if "timestamp" in df_to_write.columns:
            df_to_write["timestamp"] = pd.to_datetime(
                df_to_write["timestamp"], utc=False
            )
            if df_to_write["timestamp"].dt.tz is not None:
                df_to_write["timestamp"] = (
                    df_to_write["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
                )
            df_to_write["timestamp"] = df_to_write["timestamp"].astype("datetime64[ns]")

            if "year" in partition_cols:
                df_to_write["year"] = df_to_write["timestamp"].dt.year
            if "month" in partition_cols:
                df_to_write["month"] = df_to_write["timestamp"].dt.month

        try:
            unique_id = uuid.uuid4().hex[:8]
            df_to_write.to_parquet(
                target_dir,
                engine="pyarrow",
                partition_cols=partition_cols,
                index=False,
                existing_data_behavior="overwrite_or_ignore",
                basename_template=f"data_{unique_id}_{{i}}.parquet",
            )
            logger.info(
                f"成功写入 {len(df_to_write)} 条数据 -> {asset_type}/{category}/{sub_type}"
            )
        except Exception as e:
            logger.error(f"数据写入失败: {e}")
            raise RuntimeError(f"数据写入失败: {e}")

    def write_factor(
        self, df: pd.DataFrame, factor_name: str, frequency: str = "1d"
    ) -> None:
        """
        统一因子存储接口。df 必须包含 entity_id, timestamp, value 列。
        路径: data_lake/factors/{factor_name}/{frequency}/year=YYYY/
        """
        required = {"entity_id", "timestamp", "value"}
        if not required.issubset(df.columns):
            raise ValueError(f"因子数据必须包含 {required} 列")

        df = df.copy()
        if "factor_name" not in df.columns:
            df["factor_name"] = factor_name

        target_dir = self.data_lake_dir / "factors" / factor_name / frequency
        target_dir.mkdir(parents=True, exist_ok=True)

        df_to_write = df.copy()
        if "timestamp" in df_to_write.columns:
            df_to_write["timestamp"] = pd.to_datetime(
                df_to_write["timestamp"], utc=False
            )
            if df_to_write["timestamp"].dt.tz is not None:
                df_to_write["timestamp"] = (
                    df_to_write["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
                )
            df_to_write["timestamp"] = df_to_write["timestamp"].astype("datetime64[ns]")
            df_to_write["year"] = df_to_write["timestamp"].dt.year
            df_to_write["month"] = df_to_write["timestamp"].dt.month

        try:
            unique_id = uuid.uuid4().hex[:8]
            df_to_write.to_parquet(
                target_dir,
                engine="pyarrow",
                partition_cols=["year", "month"],
                index=False,
                existing_data_behavior="overwrite_or_ignore",
                basename_template=f"data_{unique_id}_{{i}}.parquet",
            )
            logger.info(f"成功写入因子 {factor_name}: {len(df_to_write)} 条数据")
        except Exception as e:
            logger.error(f"因子写入失败: {e}")
            raise RuntimeError(f"因子写入失败: {e}")
