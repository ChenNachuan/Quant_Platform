# -*- coding: utf-8 -*-
"""
因子库基础功能测试
不依赖数据库，使用模拟数据
"""
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, '/Users/nachuanchen/Documents/Quant')

from factor_library import Factor, FactorRegistry, register_factor
from factor_library.operators import ts_mean


# 导入示例因子（会自动注册）
from factor_library.technical.momentum import MomentumReturn


def test_registration():
    """测试自动注册"""
    print("\n" + "=" * 60)
    print("测试1: 自动注册机制")
    print("=" * 60)
    
    # 检查因子是否已注册
    registered = FactorRegistry.get('momentum_return')
    assert registered is not None, "因子未注册"
    assert registered == MomentumReturn, "注册的类不匹配"
    
    print(f"✅ 因子已注册: {FactorRegistry.list_factors()}")
    return True


def test_factor_creation():
    """测试因子创建"""
    print("\n" + "=" * 60)
    print("测试2: 因子实例化")
    print("=" * 60)
    
    # 方式1：直接创建
    factor1 = MomentumReturn(timeframe='1d', para={'window': 20})
    print(f"✅ 直接创建: {factor1}")
    print(f"   因子ID: {factor1.get_id()}")
    
    # 方式2：工厂模式
    factor2 = FactorRegistry.create_instance('momentum_return', '1d', {'window': 20})
    print(f"✅ 工厂创建: {factor2}")
    
    return True


def test_parameter_space():
    """测试参数空间生成"""
    print("\n" + "=" * 60)
    print("测试3: 参数空间生成")
    print("=" * 60)
    
    factor = MomentumReturn(timeframe='1d', para={})
    para_space = factor.generate_para_space()
    
    print(f"✅ 参数空间 (1d): {para_space}")
    assert len(para_space) == 6, "参数数量不正确"
    assert para_space[0] == {'window': 5}, "参数值不正确"
    
    return True


def test_compute():
    """测试因子计算"""
    print("\n" + "=" * 60)
    print("测试4: 因子计算（模拟数据）")
    print("=" * 60)
    
    # 生成模拟数据
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    codes = ['000001.SZ'] * 100
    
    data = pd.DataFrame({
        'timestamp': dates,
        'code': codes,
        'open': 10 + np.random.randn(100).cumsum() * 0.1,
        'high': 10.5 + np.random.randn(100).cumsum() * 0.1,
        'low': 9.5 + np.random.randn(100).cumsum() * 0.1,
        'close': 10 + np.random.randn(100).cumsum() * 0.1,
        'volume': np.random.randint(1000000, 10000000, 100)
    })
    data = data.set_index(['timestamp', 'code'])
    
    # 计算因子
    factor = MomentumReturn(timeframe='1d', para={'window': 20})
    result = factor.compute(data)
    
    print(f"✅ 计算成功")
    print(f"   结果长度: {len(result)}")
    print(f"   NaN数量: {result.isna().sum()}")
    print(f"   最近5个值:\n{result.tail()}")
    
    # 检查前19个值应该是NaN（窗口=20）
    assert result.iloc[:19].isna().all(), "前19个值应该为NaN"
    assert not result.iloc[19:].isna().all(), "后续值不应全为NaN"
    
    return True


def test_postprocessing():
    """测试后处理"""
    print("\n" + "=" * 60)
    print("测试5: 后处理（Winsorize + Standardize）")
    print("=" * 60)
    
    # 生成模拟数据
    np.random.seed(42)
    dates = pd.date_range('2023-01-01', periods=100, freq='D')
    codes = ['000001.SZ'] * 100
    
    data = pd.DataFrame({
        'timestamp': dates,
        'code': codes,
        'close': 10 + np.random.randn(100).cumsum() * 0.1
    })
    data = data.set_index(['timestamp', 'code'])
    
    factor = MomentumReturn(timeframe='1d', para={'window': 20})
    
    # 原始计算
    result_raw = factor.compute(data)
    
    # 带后处理
    result_processed = factor.compute_with_postprocess(data)
    
    # 去除NaN后统计
    raw_valid = result_raw.dropna()
    processed_valid = result_processed.dropna()
    
    print(f"✅ 原始结果:")
    print(f"   均值: {raw_valid.mean():.6f}")
    print(f"   标准差: {raw_valid.std():.6f}")
    
    print(f"✅ 后处理结果:")
    print(f"   均值: {processed_valid.mean():.6f}")
    print(f"   标准差: {processed_valid.std():.6f}")
    
    # Z-Score标准化后，均值应接近0，标准差接近1
    assert abs(processed_valid.mean()) < 0.1, "标准化后均值应接近0"
    assert abs(processed_valid.std() - 1) < 0.1, "标准化后标准差应接近1"
    
    return True


def test_operators():
    """测试算子库"""
    print("\n" + "=" * 60)
    print("测试6: 算子库")
    print("=" * 60)
    
    # 生成测试数据
    data = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    
    # 测试 ts_mean
    result = ts_mean(data, 3)
    expected = data.rolling(3).mean()
    
    print(f"✅ ts_mean 测试:")
    print(f"   输入: {data.tolist()}")
    print(f"   结果 (window=3): {result.tolist()}")
    
    assert result.equals(expected), "ts_mean 结果不正确"
    
    # 测试其他算子
    from factor_library.operators import cs_rank, cs_zscore
    
    cross_data = pd.Series([10, 20, 30, 40, 50])
    rank_result = cs_rank(cross_data)
    print(f"✅ cs_rank 测试: {rank_result.tolist()}")
    
    zscore_result = cs_zscore(cross_data)
    print(f"✅ cs_zscore 测试: {zscore_result.tolist()}")
    
    return True


if __name__ == "__main__":
    print("\n\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + "  因子库基础功能测试".center(56) + "  ║")
    print("╚" + "=" * 58 + "╝")
    
    tests = [
        ("自动注册", test_registration),
        ("因子创建", test_factor_creation),
        ("参数空间", test_parameter_space),
        ("因子计算", test_compute),
        ("后处理", test_postprocessing),
        ("算子库", test_operators),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            test_func()
            passed += 1
        except Exception as e:
            print(f"\n❌ {name} 测试失败: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print("\n\n" + "=" * 60)
    print(f"测试结果: {passed}/{len(tests)} 通过")
    if failed == 0:
        print("🎉 所有测试通过！")
    else:
        print(f"⚠️  {failed} 个测试失败")
    print("=" * 60 + "\n")
    
    sys.exit(0 if failed == 0 else 1)
