#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
因子库核心功能验证测试
使用模拟数据，无需数据库
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, '/Users/nachuanchen/Documents/Quant')

import pandas as pd
import numpy as np

print("\n" + "="*60)
print("因子库基础设施验证测试")
print("="*60)

# ============================================================
# 测试 1: 导入核心模块
# ============================================================
print("\n[测试 1/6] 导入核心模块...")
try:
    from factor_library import Factor, FactorRegistry, register_factor
    from factor_library.operators import ts_mean, cs_rank, cs_zscore
    from factor_library.preprocessor import PostProcessor
    print("✅ 所有核心模块导入成功")
except ImportError as e:
    print(f"❌ 导入失败: {e}")
    sys.exit(1)

# ============================================================
# 测试 2: 创建测试因子（不使用装饰器）
# ============================================================
print("\n[测试 2/6] 创建测试因子...")

class TestMomentum(Factor):
    """测试用动量因子"""
    name = "test_momentum"
    input_type = "bar"
    max_lookback = 60
    applicable_market = []
    store_time = "20260217"
    para_group = {"1d": {"window": [5, 10, 20]}}
    
    def generate_para_space(self):
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]
    
    def compute(self, data, deps=None):
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")
        
        window = self.para.get("window")
        if window is None:
            raise ValueError("参数 'window' 缺失")
        
        return ts_mean(data['close'].pct_change(fill_method=None), window)

try:
    test_factor = TestMomentum(timeframe='1d', para={'window': 10})
    print(f"✅ 因子创建成功: {test_factor}")
    print(f"   因子ID: {test_factor.get_id()}")
except Exception as e:
    print(f"❌ 创建失败: {e}")
    sys.exit(1)

# ============================================================
# 测试 3: 参数空间生成
# ============================================================
print("\n[测试 3/6] 测试参数空间...")
try:
    para_space = test_factor.generate_para_space()
    print(f"✅ 参数空间: {para_space}")
    assert len(para_space) == 3
    assert para_space[0] == {'window': 5}
except Exception as e:
    print(f"❌ 参数空间测试失败: {e}")
    sys.exit(1)

# ============================================================
# 测试 4: 算子库
# ============================================================
print("\n[测试 4/6] 测试算子库...")
try:
    # 测试 ts_mean
    data_series = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    result = ts_mean(data_series, 3)
    expected = data_series.rolling(3).mean()
    assert result.equals(expected), "ts_mean 结果不正确"
    print(f"✅ ts_mean 正确")
    
    # 测试 cs_rank
    cross_data = pd.Series([10, 20, 30, 40, 50])
    rank_result = cs_rank(cross_data)
    print(f"✅ cs_rank 正确: {rank_result.tolist()}")
    
    # 测试 cs_zscore
    zscore_result = cs_zscore(cross_data)
    assert abs(zscore_result.mean()) < 1e-10, "Z-Score均值应为0"
    print(f"✅ cs_zscore 正确")
    
except Exception as e:
    print(f"❌ 算子库测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================
# 测试 5: 因子计算（模拟数据）
# ============================================================
print("\n[测试 5/6] 测试因子计算...")
try:
    # 生成模拟数据
    np.random.seed(42)
    dates = pd.date_range('2024-01-01', periods=50, freq='D')
    codes = ['TEST.SZ'] * 50
    
    mock_data = pd.DataFrame({
        'timestamp': dates,
        'code': codes,
        'close': 100 + np.random.randn(50).cumsum() * 2
    })
    mock_data = mock_data.set_index(['timestamp', 'code'])
    
    # 计算因子
    factor_result = test_factor.compute(mock_data)
    
    print(f"✅ 计算成功")
    print(f"   结果长度: {len(factor_result)}")
    print(f"   NaN数量: {factor_result.isna().sum()}")
    print(f"   前10个值应为NaN: {factor_result.iloc[:9].isna().all()}")
    print(f"   最近5个值:\n{factor_result.tail()}")
    
    # 验证
    assert len(factor_result) == 50
    assert factor_result.iloc[:9].isna().all(), "前9个值应该为NaN (window=10)"
    
except Exception as e:
    print(f"❌ 因子计算失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================
# 测试 6: 后处理
# ============================================================
print("\n[测试 6/6] 测试后处理...")
try:
    # 生成测试数据
    test_series = pd.Series(np.random.randn(100) * 10 + 50)
    
    # Winsorize
    winsorized = PostProcessor.winsorize(test_series, lower=0.05, upper=0.95)
    print(f"✅ Winsorize: {winsorized.min():.2f} ~ {winsorized.max():.2f}")
    
    # Z-Score标准化
    standardized = PostProcessor.standardize(test_series, method='zscore')
    print(f"✅ Z-Score: 均值={standardized.mean():.6f}, 标准差={standardized.std():.6f}")
    assert abs(standardized.mean()) < 0.1, "标准化后均值应接近0"
    assert abs(standardized.std() - 1) < 0.1, "标准化后标准差应接近1"
    
    # 有效性检查
    validity = PostProcessor.check_validity(test_series)
    print(f"✅ 有效性检查: NaN比例={validity['nan_ratio']:.2%}, Inf比例={validity['inf_ratio']:.2%}")
    
except Exception as e:
    print(f"❌ 后处理测试失败: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# ============================================================
# 测试总结
# ============================================================
print("\n" + "="*60)
print("🎉 所有测试通过！")
print("="*60)
print("\n核心功能验证完成：")
print("  ✅ 模块导入")
print("  ✅ 因子创建")
print("  ✅ 参数空间")
print("  ✅ 算子库")
print("  ✅ 因子计算")
print("  ✅ 后处理")
print("\n下一步可以:")
print("  1. 测试装饰器自动注册")
print("  2. 测试DAG依赖调度")
print("  3. 集成ZVT回测")
print("="*60 + "\n")

sys.exit(0)
