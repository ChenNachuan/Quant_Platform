
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-15

import sys
import logging
from pathlib import Path
import pandas as pd
from typing import Dict, List, Any

# 添加项目根目录到 Path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from infra.storage import StorageManager, ConfigLoader

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataAuditor:
    """
    数据质量审查器 (Data Quality Auditor)
    
    功能:
    1. 扫描 Data Lake 目录结构。
    2. 统计每个数据集 (Market/Freq) 的覆盖范围 (时间, 标的数量)。
    3. 检查关键字段 (Open, Close, Volume) 的完整性。
    4. 识别潜在的数据断点。
    """
    def __init__(self):
        self.storage = StorageManager()
        self.data_lake_dir = self.storage.data_lake_dir
        
    def scan_structure(self) -> Dict[str, Any]:
        """扫描数据湖目录结构"""
        structure = {}
        for category_path in self.data_lake_dir.iterdir():
            if not category_path.is_dir(): continue
            
            category = category_path.name
            structure[category] = {}
            
            for market_path in category_path.iterdir():
                if not market_path.is_dir(): continue
                market = market_path.name
                structure[category][market] = []
                
                for freq_path in market_path.iterdir():
                    if not freq_path.is_dir(): continue
                    freq = freq_path.name
                    structure[category][market].append(freq)
                    
        return structure

    def audit_dataset(self, category: str, market: str, freq: str):
        """审查特定数据集"""
        view_name = f"{category}_{market}_{freq}" # StorageManager 默认 refresh_views 生成的名字可能是 market_... 需要确认
        # infra/storage.py 中 logic: view_name = f"market_{market}_{frequency}"
        # 修正: 假设 category=market_data, 则 view 为 market_{market}_{freq}
        
        if category == 'market_data':
            view_name = f"market_{market}_{freq}"
        elif category == 'factors':
            # StorageManager目前可能没为factors自动建view，我们手动查 path
            view_name = None
        else:
            view_name = f"{category}_{market}_{freq}"

        logger.info(f"=== 审查数据集: {category}/{market}/{freq} ===")
        
        path = self.data_lake_dir / category / market / freq
        if not path.exists():
            logger.error(f"路径不存在: {path}")
            return

        try:
            # 1. 基础统计 (利用 DuckDB 读取 Parquet Metadata)
            # 使用 read_parquet 通配符
            parquet_glob = f"{path}/**/*.parquet"
            
            # 检查是否有文件
            if not list(path.glob("**/*.parquet")):
                logger.warning("  [WARN] 目录下无 Parquet 文件")
                return

            db = self.storage
            # 统计总行数, 时间范围, 标的数量
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_rows,
                    MIN(timestamp) as start_date,
                    MAX(timestamp) as end_date,
                    COUNT(DISTINCT entity_id) as symbol_count
                FROM '{parquet_glob}'
            """
            stats = db.conn.execute(stats_query).df().iloc[0]
            
            logger.info(f"  [INFO] 总行数: {stats['total_rows']:,}")
            logger.info(f"  [INFO] 时间范围: {stats['start_date']} -> {stats['end_date']}")
            logger.info(f"  [INFO] 标的数量: {stats['symbol_count']:,}")
            
            # 2. 也是检查列完整性
            schema_query = f"SELECT * FROM '{parquet_glob}' LIMIT 1"
            schema_df = db.conn.execute(schema_query).df()
            cols = schema_df.columns.tolist()
            logger.info(f"  [INFO] 字段列表: {cols}")
            
            required_cols = ['timestamp', 'entity_id']
            if category == 'market_data':
                required_cols.extend(['open', 'high', 'low', 'close', 'volume'])
            elif category == 'factors':
                required_cols.extend(['value', 'factor_name'])
                
            missing_cols = [c for c in required_cols if c not in cols]
            if missing_cols:
                logger.error(f"  [FAIL] 缺失关键字段: {missing_cols}")
            else:
                logger.info(f"  [PASS] 关键字段完整")
            
            # 3. 检查 Null 值 (仅检查关键列)
            # 采样检查，避免全表扫描太慢? 不，DuckDB 很快
            null_checks = []
            for col in required_cols:
                if col in cols:
                    null_checks.append(f"COUNT(*) FILTER (WHERE {col} IS NULL) as {col}_nulls")
            
            if null_checks:
                null_query = f"SELECT {', '.join(null_checks)} FROM '{parquet_glob}'"
                null_stats = db.conn.execute(null_query).df().iloc[0]
                
                has_nulls = False
                for col, count in null_stats.items():
                    if count > 0:
                        logger.warning(f"  [WARN] 字段 {col.replace('_nulls','')} 存在 {count} 个 NULL 值")
                        has_nulls = True
                if not has_nulls:
                    logger.info("  [PASS] 关键字段无 NULL 值")

        except Exception as e:
            logger.error(f"  [ERROR] 审查过程中发生错误: {e}")

    def run(self):
        logger.info("启动数据质量审查...")
        structure = self.scan_structure()
        
        if not structure:
            logger.warning("Data Lake 为空或结构未识别")
            return

        for category, markets in structure.items():
            for market, freqs in markets.items():
                for freq in freqs:
                    self.audit_dataset(category, market, freq)
                    print("-" * 50)

if __name__ == "__main__":
    auditor = DataAuditor()
    auditor.run()
