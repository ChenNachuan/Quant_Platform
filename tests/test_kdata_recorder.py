#!/usr/bin/env python3
"""测试 stock_1d_kdata_recorder 的修复"""
import sys
from pathlib import Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from infra.storage import ConfigLoader
ConfigLoader.load()

from zvt.domain import Stock
from engine.zvt_bridge.recorders.akshare import register_akshare_recorders, AKShareStock1dKdataRecorder

# 注册 Recorder
register_akshare_recorders()

# 测试手动创建 Recorder 并抓取数据
print("=== 测试 AKShareStock1dKdataRecorder ===")
recorder = AKShareStock1dKdataRecorder(
    codes=['000004'],
    start_timestamp='2024-01-01',
    adjust_type=None  # Raw Data
)

try:
    recorder.run()
    print("✅ Recorder 运行成功")
    
    # 查询数据验证
    from zvt.domain import Stock1dKdata
    df = Stock1dKdata.query_data(codes=['000004'], provider='akshare', limit=5)
    print("\n=== 查询结果 ===")
    print(df)
    print(f"\n列名: {df.columns.tolist()}")
    
    # 检查关键字段
    if 'change_pct' in df.columns and 'turnover_rate' in df.columns:
        print("✅ change_pct 和 turnover_rate 字段存在")
        print(f"\nchange_pct 示例: {df['change_pct'].head()}")
        print(f"turnover_rate 示例: {df['turnover_rate'].head()}")
    else:
        print("❌ 缺少 change_pct 或 turnover_rate 字段")
        
except Exception as e:
    print(f"❌ 测试失败: {e}")
    import traceback
    traceback.print_exc()
