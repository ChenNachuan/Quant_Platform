#!/usr/bin/env python3
"""测试 stock_adj_factor_recorder 的修复"""
import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from infra.storage import ConfigLoader
ConfigLoader.load()

from zvt.domain import Stock
from engine.zvt_bridge.recorders.akshare import register_akshare_factor_recorder, AKShareStockAdjFactorRecorder
from engine.zvt_bridge.domain import StockAdjFactor

# 注册 Recorder
register_akshare_factor_recorder()

print("=== 测试 AKShareStockAdjFactorRecorder ===")
recorder = AKShareStockAdjFactorRecorder(
    codes=['000004'],
    start_timestamp='2024-01-01'
)

try:
    recorder.run()
    print("✅ Recorder 运行成功")
    
    # 查询数据验证
    df = StockAdjFactor.query_data(codes=['000004'], provider='akshare', limit=5)
    print("\n=== 查询结果 ===")
    print(df)
    print(f"\n列名: {df.columns.tolist()}")
    
    # 检查关键字段
    if 'hfq_factor' in df.columns:
        print("✅ hfq_factor 字段存在")
        print(f"\nhfq_factor 示例:\n{df[['timestamp', 'hfq_factor']].head()}")
    else:
        print("❌ 缺少 hfq_factor 字段")
    
    if 'qfq_factor' in df.columns:
        print("❌ 错误: qfq_factor 不应该存在")
    else:
        print("✅ qfq_factor 已正确移除")
        
except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()
