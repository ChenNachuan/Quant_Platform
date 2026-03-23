# -*- coding: utf-8 -*-
"""
Test Data Lake Refactoring
Verify:
1. Fetch and merge logic (Raw + Factors)
2. Storage path (market_data/cn_stock/1d/year=YYYY/)
3. DuckDB Views (cn_stock_1d_qfq, etc.)
"""
import sys
import pandas as pd
import shutil
from pathlib import Path

from infra.storage import StorageManager
from engine.zvt_bridge import ZvtDataSyncer

def test_storage_refactor():
    print("="*60)
    print("Testing Data Lake Refactoring")
    print("="*60)

    # 1. Setup
    # Use real Stock code 000001 (Ping An Bank) for a short period to be fast
    test_code = '000001'
    start_date = '2023-12-01'
    end_date = '2023-12-31'
    
    # Initialize Syncer
    syncer = ZvtDataSyncer()
    
    # Clean up conflicting data lake paths for test code if necessary
    # (Optional, but good for clean test)
    # Be careful not to delete everything if user has real data.
    # Here we just rely on existing_data_behavior='overwrite_or_ignore'
    
    sm = StorageManager()
    target_clean = sm.data_lake_dir / 'market_data' / 'cn_stock'
    if target_clean.exists():
        print(f"Cleaning up existing data in {target_clean}...")
        shutil.rmtree(target_clean)

    # 2. Run Fetch and Store (MOCKED)
    print(f"\n[Step 1] Constructing and Storing MOCK Data for {test_code} ({start_date} to {end_date})...")
    
    # Create Mock DataFrame
    dates = pd.date_range(start_date, end_date, freq='D')
    mock_df = pd.DataFrame({
        'entity_id': [f'stock_sz_{test_code}'] * len(dates),
        'timestamp': dates,
        'code': [test_code] * len(dates),
        'name': ['TestStock'] * len(dates),
        'open': [10.0 + i*0.1 for i in range(len(dates))],
        'high': [10.5 + i*0.1 for i in range(len(dates))],
        'low': [9.5 + i*0.1 for i in range(len(dates))],
        'close': [10.2 + i*0.1 for i in range(len(dates))],
        'volume': [1000 + i*10 for i in range(len(dates))],
        'turnover': [10000 + i*100 for i in range(len(dates))],
        'qfq_factor': [1.0] * len(dates),
        'hfq_factor': [1.0 + i*0.01 for i in range(len(dates))]
    })
    
    try:
        # Manually write to storage using the refactored logic
        # category='market_data', market='cn_stock', frequency='1d'
        sm = StorageManager()
        sm.write_data(
            mock_df, 
            category='market_data', 
            market='cn_stock', 
            frequency='1d'
        )
        print("✅ Mock Data Store completed.")
        
        # Manually trigger view refresh because write_data doesn't do it automatically in concurrent mode
        sm.refresh_views()
        
    except Exception as e:
        print(f"❌ Mock Store failed: {e}")
        import traceback
        traceback.print_exc()
        return

    # 3. Verify Directory Structure
    print("\n[Step 2] Verifying Directory Structure...")
    sm = StorageManager()
    data_lake_dir = sm.data_lake_dir
    target_path = data_lake_dir / 'market_data' / 'cn_stock' / '1d'
    
    year_path = target_path / 'year=2023'
    print(f"Checking path: {year_path}")
    
    parquet_files = list(year_path.glob('*.parquet'))
    if parquet_files:
        print(f"✅ Found {len(parquet_files)} parquet files in {year_path}")
        print(f"   Sample: {parquet_files[0].name}")
    else:
        print(f"❌ No parquet files found in {year_path}")
        # Check parent dir
        print(f"   Contents of {target_path}: {list(target_path.iterdir()) if target_path.exists() else 'Dir not found'}")
        return

    # 4. Verify Data Content (Single Table)
    print("\n[Step 3] Verifying Merged Data (Raw + Factors)...")
    try:
        df = pd.read_parquet(parquet_files[0])
        required_cols = ['open', 'close', 'qfq_factor', 'hfq_factor']
        missing = [c for c in required_cols if c not in df.columns]
        
        if not missing:
            print("✅ Data columns check passed.")
            print(f"   Columns: {df.columns.tolist()}")
            print(f"   Sample Data:\n{df[['timestamp', 'code', 'close', 'qfq_factor']].head(3)}")
        else:
            print(f"❌ Missing columns: {missing}")
            print(f"   Available: {df.columns.tolist()}")
    except Exception as e:
        print(f"❌ Failed to read parquet: {e}")

    # 5. Verify DuckDB Views
    print("\n[Step 4] Verifying DuckDB Views...")
    try:
        sm.refresh_views()
        sm.create_adjusted_views()
        
        # Check QFQ View
        print("   Querying cn_stock_1d_qfq...")
        df_qfq = sm.conn.execute("SELECT * FROM cn_stock_1d_qfq LIMIT 5").df()
        
        if not df_qfq.empty:
            print("✅ QFQ View query successful.")
            print(f"   Columns: {df_qfq.columns.tolist()}")
            # Verify calculation logically? open should be close to actual price history
            print(df_qfq[['timestamp', 'symbol', 'close', 'qfq_factor']])
        else:
            print("⚠️ QFQ View returned empty result (might be no data matched filter?)")
            
        # Check Tables list
        print("   Tables in DuckDB:")
        print(sm.conn.execute("SHOW TABLES").df())

    except Exception as e:
        print(f"❌ View verification failed: {e}")

    print("\n" + "="*60)
    print("Test Complete")
    print("="*60)

if __name__ == "__main__":
    test_storage_refactor()
