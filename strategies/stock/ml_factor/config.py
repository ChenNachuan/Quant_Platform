"""
Configuration for ML Factor Strategy.
"""
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RESEARCH_DATA_ROOT = PROJECT_ROOT / "data_lake" / "stock" / "research" / "ml_factor"
ARTIFACT_ROOT = PROJECT_ROOT / "artifacts" / "stock_ml_factor"


@dataclass
class DataConfig:
    """Data configuration."""
    # 数据文件路径
    raw_data_path: str = str(RESEARCH_DATA_ROOT / "cleaned_final_dataset2_2000_2025.parquet")
    barra_data_path: str = str(RESEARCH_DATA_ROOT / "全A股月度cne5数据_2007-2025.csv")
    industry_data_path: str = str(RESEARCH_DATA_ROOT / "industry.xlsx")
    stock_basic_path: str = str(PROJECT_ROOT / "data_lake/stock/basedata/stock_basic/stock_basic.parquet")
    index_data_path: str = str(RESEARCH_DATA_ROOT / "index_monthly_filtered.parquet")
    ch3_factors_path: str = str(RESEARCH_DATA_ROOT / "CH3_factors_monthly.xlsx")
    ch4_factors_path: str = str(RESEARCH_DATA_ROOT / "CH4_factors_monthly.xlsx")
    carhart_factors_path: str = str(RESEARCH_DATA_ROOT / "Carhart4_factors_monthly.xlsx")

    # 输出路径
    output_dir: str = str(ARTIFACT_ROOT)


@dataclass
class PreprocessConfig:
    """Preprocessing configuration."""
    # 特征列排除项
    exclude_cols: list = None

    def __post_init__(self):
        if self.exclude_cols is None:
            self.exclude_cols = ['stkcd', 'year', 'month', 'ret', 'date', 'next_ret', 'raw_size']


@dataclass
class RandomForestConfig:
    """Random Forest model configuration."""
    train_window_months: int = 24
    n_estimators: int = 100
    max_depth: int = 6
    min_samples_leaf: int = 20
    n_jobs: int = -1
    random_state: int = 42


@dataclass
class XGBoostConfig:
    """XGBoost model configuration."""
    train_window_months: int = 24
    n_estimators: int = 1000
    max_depth: int = 6
    learning_rate: float = 0.05
    tree_method: str = "hist"
    device: str = "cuda"  # 使用 GPU
    objective: str = "reg:squarederror"
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    reg_alpha: float = 0.1
    reg_lambda: float = 1.0
    n_jobs: int = -1
    random_state: int = 42


@dataclass
class BacktestConfig:
    """Backtest configuration."""
    n_groups: int = 5  # 分组数量
    min_stocks_per_group: int = 5
    weight_method: str = "both"  # "equal", "value", "both"


@dataclass
class BarraConfig:
    """Barra risk model configuration."""
    style_factors: list = None

    def __post_init__(self):
        if self.style_factors is None:
            self.style_factors = [
                'size', 'beta', 'momentum', 'residual_volatility', 'non_linear_size',
                'book_to_price_ratio', 'liquidity', 'earnings_yield', 'growth', 'leverage'
            ]


@dataclass
class StrategyConfig:
    """Main strategy configuration."""
    data: DataConfig = None
    preprocess: PreprocessConfig = None
    rf_model: RandomForestConfig = None
    xgb_model: XGBoostConfig = None
    backtest: BacktestConfig = None
    barra: BarraConfig = None

    def __post_init__(self):
        if self.data is None:
            self.data = DataConfig()
        if self.preprocess is None:
            self.preprocess = PreprocessConfig()
        if self.rf_model is None:
            self.rf_model = RandomForestConfig()
        if self.xgb_model is None:
            self.xgb_model = XGBoostConfig()
        if self.backtest is None:
            self.backtest = BacktestConfig()
        if self.barra is None:
            self.barra = BarraConfig()
