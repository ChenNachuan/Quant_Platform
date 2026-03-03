# -*- coding: utf-8 -*-
"""
验证增量计算逻辑
1. 生成模拟历史数据
2. 使用全量计算作为 Ground Truth
3. 模拟每日增量更新
4. 对比结果一致性
"""
import sys
import pandas as pd
import numpy as np
import shutil
from pathlib import Path

# Add project root to path
project_root = '/Users/nachuanchen/Documents/Quant'
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from factor_library.technical.momentum import MomentumReturn
from engine.factor.incremental_updater import IncrementalUpdater
from infra.storage import StorageManager

def generate_mock_data(codes=['000001'], days=100):
    """生成模拟 OHLCV 数据"""
    dates = pd.date_range('2024-01-01', periods=days, freq='D')
    dfs = []
    for code in codes:
        np.random.seed(42) # 固定种子
        close = 100 + np.random.randn(days).cumsum()
        df = pd.DataFrame({
            'timestamp': dates,
            'entity_id': f'stock_sz_{code}',
            'code': code,
            'name': 'Test',
            'open': close * 0.99,
            'high': close * 1.01,
            'low': close * 0.98,
            'close': close,
            'volume': np.random.randint(1000, 10000, days),
            'turnover': np.random.randint(10000, 100000, days),
            'qfq_factor': 1.0,
            'hfq_factor': 1.0
        })
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)

def test_incremental_logic():
    print("="*60)
    print("增量计算逻辑验证")
    print("="*60)
    
    # 0. 清理环境
    sm = StorageManager()
    
    # 清理旧数据 (raw data & factors)
    market_dir = sm.data_lake_dir / 'market_data' / 'cn_stock'
    if market_dir.exists():
        shutil.rmtree(market_dir)
        
    factor_dir = sm.data_lake_dir / 'factors'
    if factor_dir.exists():
        shutil.rmtree(factor_dir)
        
    # 1. 准备数据 (Total 100 days)
    # Day 0-89: Initial History
    # Day 90-99: Incremental Updates
    total_days = 100
    split_day = 90
    
    full_data = generate_mock_data(days=total_days)
    dates = full_data['timestamp'].unique()
    dates = np.sort(dates) # DatetimeArray has no sort method in place
    
    history_data = full_data[full_data['timestamp'] < dates[split_day]]
    incremental_data = full_data[full_data['timestamp'] >= dates[split_day]]
    
    print(f"全量数据: {len(full_data)} 条")
    print(f"初始历史: {len(history_data)} 条 (Day 0-{split_day-1})")
    print(f"增量数据: {len(incremental_data)} 条 (Day {split_day}-{total_days-1})")
    
    # 2. 写入初始历史数据到 Data Lake
    print("\n[Step 1] 写入初始历史数据...")
    sm.write_data(history_data, category='market_data', market='cn_stock', frequency='1d')
    sm.refresh_views() # 刷新视图以便查询
    
    # 3. 全量计算 (Ground Truth)
    # 我们直接用 factor.compute() 对 full_data 计算，作为标准答案
    print("\n[Step 2] 计算 Ground Truth (全量)...")
    factor = MomentumReturn(timeframe='1d', para={'window': 20})
    factor_id = factor.get_id()
    
    # 构造 MultiIndex DataFrame 符合 compute 要求
    full_data_indexed = full_data.set_index(['timestamp', 'code']) # 其实 momentum 只需要 close col
    
    # 注意: compute 可能会用到 groupby 如果传入多只股票
    # 这里简便起见，假设只有一只股票 000001
    ground_truth = factor.compute(full_data_indexed)
    # ground_truth Series index is timestamp (if single stock) or MultiIndex?
    # Momentum compute uses ts_mean(series), returning Series with original index.
    # If input had MultiIndex, output has MultiIndex.
    
    # 4. 模拟增量更新
    print("\n[Step 3] 开始增量更新模拟...")
    updater = IncrementalUpdater()
    
    # 逐日更新
    for date in dates[split_day:]:
        date_str = str(date).split('T')[0]
        # print(f"  Updating {date_str}...")
        
        # 4.1 写入当天新数据到 Data Lake
        day_data = incremental_data[incremental_data['timestamp'] == date]
        sm.write_data(day_data, category='market_data', market='cn_stock', frequency='1d')
        sm.refresh_views() # 必须刷新，否则 updater 查不到新数据
        
        # 4.2 调用 updater
        # Updater 会读取 [date-lookback, date] 的数据并在内部调用 factor.update()
        updater.update_factor('momentum_return', date_str, codes=['000001'])
    
    # 5. 验证结果
    print("\n[Step 4] 验证结果一致性...")
    
    # 读取增量计算保存的结果
    # factors/{factor_id}/1d/year=YYYY/data.parquet
    # 或者用 DuckDB 读
    
    try:
        # factor storage path: market={factor_id}, frequency=1d
        # We can construct a view or just read parquet directly
        # factor_id: momentum_return_1d_window20 (sort needed)
        # Check actual factor id
        print(f"Factor ID: {factor_id}")
        
        # Read from storage
        # Need to implement easy read for factors in StorageManager or just use SQL
        # Factor tables are not automatically registered as views yet?
        # StorageManager._register_views_for_root covers 'factors' category?
        # Let's check logic: _register_views_for_root(self.data_lake_dir / 'factors', 'factors')
        # Yes, it should register views like `factors_momentum_return_1d_window20_1d`
        # But wait, factor_id contains param string which might have special chars?
        # ID: momentum_return_1d_window20
        # View name sanitization might be needed.
        # Let's check view name.
        
        views = sm.conn.execute("SHOW TABLES").df()
        print(f"Available Views: {views['name'].tolist()}")
        
        # Find view for our factor
        # Expected: factors_{factor_id}_1d
        target_view_prefix = f"factors_{factor_id}"
        target_view = None
        for v in views['name']:
            if v.startswith(target_view_prefix):
                target_view = v
                break
                
        if not target_view:
            print(f"❌ 未找到因子视图: {target_view_prefix}")
            return

        print(f"Reading from view: {target_view}")
        result_incremental_df = sm.conn.execute(f"SELECT * FROM {target_view} ORDER BY timestamp").df()
        
        # Compare
        # Ground Truth (Series) vs Incremental Result (DataFrame)
        # Extract validation segment
        
        # Ground Truth Segment: split_day to end
        gt_segment = ground_truth.loc[(slice(None), '000001')] # If MultiIndex
        # Or if single level index (timestamp)
        # Momentum implementation: ts_mean(data['close'].pct_change())
        # If input has MultiIndex, pct_change might group by level? No, pandas default doesn't.
        # But our mock data set_index(['timestamp', 'code']).
        # We need to ensure Ground Truth is calculated correctly for MultiIndex inputs (groupby code).
        # MomentumReturn.compute doesn't have groupby logic inside! it expects `data` to be for one stock OR
        # handles it. Let's check MomentumReturn.compute again.
        # return ts_mean(data['close'].pct_change(fill_method=None), window)
        # If data has MultiIndex, pct_change spans across codes! THIS IS A BUG in simple implementation.
        # But here we only test 1 stock. So it's fine.
        
        # Align data
        result_incremental_df = result_incremental_df.set_index('timestamp').sort_index()
        # Filter GT to same range
        gt_segment = gt_segment[gt_segment.index >= dates[split_day]]
        
        # Filter Incremental to same range (it should only have incremental parts?
        # No, update() writes only new data. But we ran it for 10 days.
        # StorageManager writes are append?
        # Parquet files are per year/month.
        result_segment = result_incremental_df[result_incremental_df.index >= dates[split_day]]
        
        print(f"Ground Truth shape: {gt_segment.shape}")
        print(f"Incremental shape: {result_segment.shape}")
        
        # Value Comparison
        # Need to align perfectly
        comparison = pd.DataFrame({
            'GT': gt_segment,
            'Inc': result_segment['value']
        })
        
        print("\nContrast (Tail 5):")
        print(comparison.tail(5))
        
        diff = np.abs(comparison['GT'] - comparison['Inc'])
        max_diff = diff.max()
        
        if max_diff < 1e-6:
            print(f"\n✅ 验证成功! 最大误差: {max_diff:.9f}")
        else:
            print(f"\n❌ 验证失败! 最大误差: {max_diff:.9f}")
            print(comparison[diff > 1e-6])
            
    except Exception as e:
        print(f"❌ 验证过程出错: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_incremental_logic()
