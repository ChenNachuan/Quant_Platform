"""
Environment checker for ML Factor Strategy.

检查依赖包和数据文件是否就绪。
"""
import sys
from pathlib import Path

from strategies.stock.ml_factor.config import StrategyConfig


def check_dependencies():
    """检查依赖包."""
    print("=" * 50)
    print("检查依赖包")
    print("=" * 50)

    required_packages = [
        'pandas',
        'numpy',
        'scikit-learn',
        'xgboost',
        'statsmodels',
        'scipy',
        'matplotlib',
        'seaborn',
    ]

    missing_packages = []

    for package in required_packages:
        try:
            if package == 'scikit-learn':
                __import__('sklearn')
            else:
                __import__(package)
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} - 未安装")
            missing_packages.append(package)

    if missing_packages:
        print(f"\n缺少以下依赖包: {', '.join(missing_packages)}")
        print("请运行: pip install " + ' '.join(missing_packages))
        return False
    else:
        print("\n所有依赖包已安装!")
        return True


def check_data_files():
    """检查数据文件."""
    print("\n" + "=" * 50)
    print("检查数据文件")
    print("=" * 50)

    data_config = StrategyConfig().data
    data_files = [
        data_config.raw_data_path,
        data_config.barra_data_path,
        data_config.industry_data_path,
        data_config.stock_basic_path,
        data_config.index_data_path,
        data_config.ch3_factors_path,
        data_config.ch4_factors_path,
        data_config.carhart_factors_path,
    ]

    missing_files = []

    for file_name in data_files:
        file_path = Path(file_name)
        if file_path.exists():
            print(f"✓ {file_path}")
        else:
            print(f"✗ {file_path} - 未找到")
            missing_files.append(str(file_path))

    if missing_files:
        print(f"\n缺少以下数据文件: {', '.join(missing_files)}")
        return False
    else:
        print("\n所有数据文件已就绪!")
        return True


def check_gpu():
    """检查 GPU 可用性."""
    print("\n" + "=" * 50)
    print("检查 GPU")
    print("=" * 50)

    try:
        import xgboost as xgb
        import numpy as np

        # 创建一个小的测试数据
        X = np.random.rand(100, 10)
        y = np.random.rand(100)

        # 尝试使用 GPU
        try:
            model = xgb.XGBRegressor(device='cuda', n_estimators=10)
            model.fit(X, y)
            print("✓ GPU 可用 (CUDA)")
            return True
        except Exception as e:
            print(f"✗ GPU 不可用: {e}")
            print("  将使用 CPU 模式运行 XGBoost")
            return False

    except ImportError:
        print("✗ XGBoost 未安装")
        return False


def main():
    """运行所有检查."""
    print("=" * 60)
    print("ML Factor Strategy 环境检查")
    print("=" * 60)

    dep_ok = check_dependencies()
    data_ok = check_data_files()
    gpu_ok = check_gpu()

    print("\n" + "=" * 60)
    print("检查结果总结")
    print("=" * 60)

    if dep_ok and data_ok:
        print("✓ 环境检查通过! 可以运行策略。")
        if gpu_ok:
            print("  - XGBoost 将使用 GPU 加速")
        else:
            print("  - XGBoost 将使用 CPU 模式")
    else:
        print("✗ 环境检查未通过，请解决上述问题后重试。")

    return dep_ok and data_ok


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
