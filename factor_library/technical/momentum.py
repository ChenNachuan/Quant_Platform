# -*- coding: utf-8 -*-
"""
动量收益率因子
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
from factor_library.operators import ts_mean
import pandas as pd
from typing import List, Dict, Optional


@register_factor  # 特性1：装饰器自动注册
class MomentumReturn(Factor):
    """
    动量收益率因子
    
    公式：mean(pct_change(close, 1), window)
    方向：数值越大，动量越强，宜做多
    
    工业级特性演示：
    1. @register_factor 自动注册
    2. 使用 operators 库（ts_mean）
    3. 后处理：winsorize + standardize
    4. 增量更新：update() 方法
    5. 规范化ID：get_id()
    6. 无依赖（基础因子）
    """
    
    # 元数据
    name = "momentum_return"
    input_type = "bar"
    max_lookback = 250
    applicable_market = []
    store_time = "20260217"
    
    # 参数空间
    para_group = {
        "1d": {"window": [5, 10, 20, 60, 120, 250]},
        "1h": {"window": [10, 20, 50, 100]}
    }
    
    # 特性2：无依赖（基础因子）
    dependencies = []
    
    # 特性3：后处理配置
    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'
    
    def generate_para_space(self) -> List[Dict[str, int]]:
        """生成参数空间"""
        if self.timeframe not in self.para_group:
            return []
        group = self.para_group[self.timeframe]
        return [{"window": w} for w in group["window"]]
    
    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算因子
        
        Args:
            data: OHLCV数据
            deps: 无依赖
        
        Returns:
            因子值
        """
        # 防御性检查
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")
        
        window = self.para.get("window")
        if window is None:
            raise ValueError("参数 'window' 缺失")
        
        # 特性2：使用算子库（向量化）
        return ts_mean(data['close'].pct_change(fill_method=None), window)
    
    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        特性4：高效增量更新
        
        只需要过去 window 天的数据，而不是 max_lookback
        """
        window = self.para['window']
        
        # 只取最近 window 条历史数据 (因为 pct_change 需要多一个点)
        # 需要 window + 1 个价格点来计算 window 个收益率
        # history 提供 window 个，new_data 提供 1 个 -> 总共 window + 1
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])
        
        # 重算
        result = ts_mean(combined['close'].pct_change(fill_method=None), window)
        
        # 只返回新数据部分
        return result.iloc[-len(new_data):]


if __name__ == "__main__":
    """
    测试代码
    
    运行方式:
        python factor_library/technical/momentum.py
    """
    import sys
    
    print("=" * 60)
    print("动量收益率因子测试")
    print("=" * 60)
    
    # 1. 测试因子创建
    factor = MomentumReturn(timeframe='1d', para={'window': 20})
    print(f"\n✅ 因子实例: {factor}")
    print(f"✅ 因子ID: {factor.get_id()}")
    
    # 2. 测试参数空间
    para_space = factor.generate_para_space()
    print(f"\n✅ 参数空间 (1d): {para_space}")
    
    # 3. 测试自动注册
    from factor_library.registry import FactorRegistry
    registered = FactorRegistry.get('momentum_return')
    print(f"\n✅ 自动注册: {registered is not None}")
    print(f"   注册表中的因子: {FactorRegistry.list_factors()}")
    
    # 4. 测试单股票计算
    try:
        from infra.storage import StorageManager
        storage = StorageManager()
        
        data = storage.conn.execute("""
            SELECT timestamp, code, open, high, low, close, volume
            FROM cn_stock_1d
            WHERE code = '000001.SZ'
              AND timestamp >= '2023-01-01'
            ORDER BY timestamp
        """).df()
        
        if data.empty:
            print("\n⚠️  数据库中无测试数据，跳过计算测试")
            sys.exit(0)
        
        data = data.set_index(['timestamp', 'code'])
        
        # 不带后处理
        result_raw = factor.compute(data)
        print(f"\n✅ 原始计算结果 (最近10条):\n{result_raw.tail(10)}")
        
        # 带后处理
        result_processed = factor.compute_with_postprocess(data)
        print(f"\n✅ 后处理结果 (最近10条):\n{result_processed.tail(10)}")
        
        # 统计信息
        print(f"\n📊 统计信息:")
        print(f"   - NaN比例: {result_raw.isna().sum() / len(result_raw):.2%}")
        print(f"   - 均值: {result_raw.mean():.6f}")
        print(f"   - 标准差: {result_raw.std():.6f}")
        print(f"   - 后处理均值: {result_processed.mean():.6f}")
        print(f"   - 后处理标准差: {result_processed.std():.6f}")
        
    except Exception as e:
        print(f"\n❌ 计算测试失败: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 60)
    print("测试完成")
    print("=" * 60)
