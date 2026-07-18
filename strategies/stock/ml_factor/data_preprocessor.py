"""
Data preprocessing module for ML Factor Strategy.
"""
import warnings

import numpy as np
import pandas as pd

from .config import DataConfig, PreprocessConfig

warnings.filterwarnings('ignore')


class DataPreprocessor:
    """数据预处理器."""

    def __init__(self, data_config: DataConfig, preprocess_config: PreprocessConfig):
        self.data_config = data_config
        self.preprocess_config = preprocess_config
        self.df = None
        self.feature_cols = None

    def load_and_prepare(self) -> pd.DataFrame:
        """加载并预处理数据."""
        print("=" * 50)
        print("Step 1: 数据预处理")
        print("=" * 50)

        # 1. 读取数据
        print("正在读取数据...")
        df = pd.read_parquet(self.data_config.raw_data_path, engine='fastparquet')

        selected_vars = [col for col in df.columns if col not in ['ts_code', 'year', 'month', 'next_ret']]
        column_order = ['ts_code', 'year', 'month', 'next_ret'] + selected_vars
        df = df[column_order]

        # 2. 重命名列
        df = df.rename(columns={'ts_code': 'stkcd'})

        # 3. 转换年月为整数
        df['year'] = df['year'].astype(int)
        df['month'] = df['month'].astype(int)
        df = df.drop(columns=['date', 'next_ret'], errors='ignore')

        # 4. 构造时间索引
        print("正在构造时间索引...")
        df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01')

        # 5. 严格排序
        df = df.sort_values(['stkcd', 'date']).reset_index(drop=True)

        # 6. 构造下期收益率（带日期校验）
        ret_col = 'ret'
        df['expected_next_date'] = df['date'] + pd.DateOffset(months=1)
        df['next_ret_raw'] = df.groupby('stkcd')[ret_col].shift(-1)
        df['next_date_raw'] = df.groupby('stkcd')['date'].shift(-1)

        # 校验日期是否匹配
        df['next_ret'] = np.where(
            df['next_date_raw'] == df['expected_next_date'],
            df['next_ret_raw'],
            np.nan
        )

        # 清理中间变量
        df = df.drop(columns=['expected_next_date', 'next_ret_raw', 'next_date_raw'])

        # 7. 删除缺失值
        df = df.dropna(subset=['next_ret']).copy()
        df = df.sort_values(['date', 'stkcd']).reset_index(drop=True)

        # 8. 备份原始市值
        df['raw_size'] = df['size']

        # 9. 确定特征列
        self.feature_cols = [col for col in df.columns if col not in self.preprocess_config.exclude_cols]
        print(f"特征工程完成。总特征数量: {len(self.feature_cols)}")

        # 10. 特征标准化：Rank Normalization (映射到 [-1, 1])
        print("正在进行截面标准化...")
        df[self.feature_cols] = df.groupby('date')[self.feature_cols].transform(self._rank_normalize)
        df[self.feature_cols] = df[self.feature_cols].fillna(0)

        # 11. 构造截面排名目标
        print("正在构造截面排名目标 (Target Ranking)...")
        df['rank_ret'] = df.groupby('date')['next_ret'].rank(pct=True, method='first')

        print("第一步完成：数据准备就绪。")
        print(f"数据形状: {df.shape}")
        print(f"时间范围: {df['date'].min()} ~ {df['date'].max()}")

        self.df = df
        return df

    @staticmethod
    def _rank_normalize(x):
        """Rank Normalization: 映射到 [-1, 1]."""
        return (x.rank() - 1) / (x.count() - 1) * 2 - 1

    def get_feature_cols(self) -> list:
        """获取特征列名."""
        if self.feature_cols is None:
            raise ValueError("请先调用 load_and_prepare() 方法")
        return self.feature_cols

    def get_dataframe(self) -> pd.DataFrame:
        """获取预处理后的数据."""
        if self.df is None:
            raise ValueError("请先调用 load_and_prepare() 方法")
        return self.df


class BenchmarkDataLoader:
    """基准数据加载器."""

    def __init__(self, data_config: DataConfig):
        self.data_config = data_config

    def load_index_data(self, start_date, end_date) -> pd.DataFrame:
        """加载指数基准数据."""
        try:
            bench_df = pd.read_parquet(self.data_config.index_data_path, engine='fastparquet')
        except Exception as e:
            print(f"读取 Parquet 失败: {e}")
            return pd.DataFrame()

        if bench_df.empty:
            return pd.DataFrame()

        # 构造日期索引
        bench_df['date'] = pd.to_datetime(
            bench_df['year'].astype(str) + '-' + bench_df['month'].astype(str) + '-01'
        )

        # 数据透视
        bench_pivoted = bench_df.pivot(index='date', columns='ts_code', values='pct_chg')

        # 处理收益率单位
        if bench_pivoted.max().max() > 1:
            bench_pivoted = bench_pivoted / 100.0

        # 重命名列
        index_map = {
            '000300.SH': '沪深300',
            '000905.SH': '中证500',
            '000001.SH': '上证指数'
        }
        bench_pivoted = bench_pivoted.rename(columns=index_map)

        # 只保留关心的指数
        valid_indices = [c for c in bench_pivoted.columns if c in index_map.values()]
        bench_returns = bench_pivoted[valid_indices]

        # 时间对齐
        bench_returns = bench_returns.loc[
            (bench_returns.index >= start_date) & (bench_returns.index <= end_date)
        ]

        # 计算累计净值
        cum_bench = (1 + bench_returns).cumprod()

        print("基准指数数据处理完成。包含指数:", cum_bench.columns.tolist())
        return cum_bench
