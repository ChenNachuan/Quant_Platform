# -*- coding: utf-8 -*-
"""
Test FactorAdapter Integration
Verify that FactorAdapter can correctly fetch data from the new Data Lake structure (via DuckDB Views)
"""
import sys
import pandas as pd
from pathlib import Path

# Add project root to path
project_root = '/Users/nachuanchen/Documents/Quant'
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from engine.zvt_bridge.backtest import FactorAdapter
from factor_library.base import Factor
from infra.storage import StorageManager

class MockFactor(Factor):
    def generate_para_space(self):
        return [{}]

    def compute(self, data: pd.DataFrame, deps=None) -> pd.Series:
        # Simple factor: close price
        return data['close']

def test_zvt_adapter_integration():
    print("="*60)
    print("Test FactorAdapter Integration")
    print("="*60)
    
    # 1. Setup
    # We rely on data written by test_storage_refactor.py for code '000001'
    test_code = '000001'
    start_date = '2023-12-01'
    end_date = '2023-12-05'
    
    # Check if data exists first
    sm = StorageManager()
    res = sm.conn.execute(f"SELECT count(*) FROM cn_stock_1d_hfq WHERE symbol='{test_code}'").fetchone()
    print(f"Data check for {test_code}: {res[0]} rows in cn_stock_1d_hfq")
    
    if res[0] == 0:
        print("❌ No data found in DuckDB. Run test_storage_refactor.py first.")
        return

    # 2. Initialize Adapter
    print(f"\n[Step 1] Initializing FactorAdapter for {test_code}...")
    
    # MockFactor needs name attribute, usually set in class definition
    MockFactor.name = 'test_factor'
    MockFactor.store_time = '20231201' # Add required metadata
    MockFactor.para_group = {'1d': {}} # Add required metadata
    
    factor_instance = MockFactor(timeframe='1d', para={})
    
    adapter = FactorAdapter(
        custom_factor=factor_instance,
        codes=[f'{test_code}.SZ'], # ZVT usually expects 000001.SZ, Adapter logic handles parsing?
        # Let's check adapter logic in _load_data_from_duckdb
        # args: codes: List[str]
        # In _load_data_from_duckdb: codes_list = self.codes
        # It joins them: code IN ('000001.SZ')
        # BUT our DuckDB view `cn_stock_1d_hfq` has `symbol` column = '000001' (no suffix usually, or depends on ingestion)
        # In test_storage_refactor.py we wrote code='000001'.
        # StorageManager view: code as symbol.
        # Adapter query: WHERE code IN ...
        # Wait, Adapter query SQL:
        # SELECT ... code as entity_id ... FROM {table_name} WHERE code IN ('{codes_str}')
        # "code" in view is the raw code (e.g. 000001).
        # "entity_id" in view is zvt entity_id (e.g. stock_sz_000001).
        # We need to make sure what column Adapter filters on.
        # Adapter: WHERE code IN ...
        # If view has `code` column?
        # In StorageManager view definition: `code as symbol`, `entity_id`. 
        # It does NOT have a column named `code` anymore (renamed to symbol).
        # So Adapter query will FAIL if it uses `WHERE code IN ...`
        
        # Let's re-read Adapter logic in next step if this test fails.
        # But wait, looking at my previous view of zvt_adapter.py (Step 2039)
        # query = Select ... code as entity_id ... FROM {table_name} WHERE code IN ...
        # It expects `code` column in the view.
        # But StorageManager (Step 1941) maps `code as symbol` and selects `entity_id`.
        # The view `cn_stock_1d_hfq` has: timestamp, symbol, entity_id, ...
        # so `code` column is GONE (it is `symbol`).
        
        # This confirms I need to fix FactoryAdapter query SQL as well!
        # It should filter by `entity_id` or `symbol`.
        # ZVT entity_id is like 'stock_sz_000001'.
        
        start_timestamp=start_date,
        end_timestamp=end_date
    )
    
    # 3. Compute Factor
    print("[Step 2] Computing Factor...")
    try:
        adapter.compute_factor()
        
        if adapter.factor_df is not None and not adapter.factor_df.empty:
            print("✅ Factor computation successful.")
            print(f"   Shape: {adapter.factor_df.shape}")
            print(f"   Sample:\n{adapter.factor_df.head()}")
        else:
            print("❌ Factor computation produced empty result.")
            
    except Exception as e:
        print(f"❌ functionality test failed: {e}")
        import traceback
        traceback.print_exc()

    print("\n" + "="*60)
    print("Integration Test Complete")
    print("="*60)

if __name__ == "__main__":
    test_zvt_adapter_integration()
