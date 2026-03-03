
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-14

import pandas as pd
from pathlib import Path
import sys
import shutil
import subprocess
import logging
from typing import Optional, List, Dict

# 配置日志
logger = logging.getLogger(__name__)

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from infra.storage import StorageManager

class QlibDumper:
    """
    Qlib 数据转储器 (Qlib Incremental Dumper)
    
    负责将数据湖 (Parquet/DuckDB) 中的数据转换为 Qlib 专用的 Binary 格式。
    采用“增量转储”策略，通过生成中间态 CSV 并调用 Qlib 命令行工具实现。
    """
    def __init__(self):
        self.storage = StorageManager()
        
        # 从配置中读取 Qlib 数据目录
        provider_uri = self.storage.config.get('qlib', {}).get('provider_uri')
        if not provider_uri:
            raise ValueError("配置文件中缺少 'qlib.provider_uri'")
            
        self.qlib_dir = Path(provider_uri)
        self.qlib_dir.mkdir(parents=True, exist_ok=True)
        
        # 临时 CSV 目录
        self.csv_temp_dir = self.qlib_dir / "temp_csv"

    def dump(self, start_date: Optional[str] = None) -> None:
        """
        执行数据转储。

        Args:
            start_date (Optional[str]): 开始日期。若为 None，建议实现逻辑自动判断增量起点。
        """
        # 1. 从 DuckDB 查询数据
        logger.info(f"正在查询存储数据 (Start Date: {start_date})...")
        df: pd.DataFrame = self.storage.get_market_data(start_date=start_date)
        
        if df.empty:
            logger.info("无新数据需要转储")
            return

        # 2. 格式映射 (Format Mapping)
        # Qlib 标准字段: symbol, date, open, high, low, close, volume, factor...
        col_map: Dict[str, str] = {
            'entity_id': 'symbol',
            'timestamp': 'date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        }
        
        # 重命名列
        df = df.rename(columns=col_map)
        
        # 填充缺失的标准列
        required_fields: List[str] = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        for col in required_fields:
            if col not in df.columns:
                logger.warning(f"缺失字段 '{col}'，自动填充为 0")
                df[col] = 0.0
        
        # 仅保留 Qlib 所需列
        df = df[required_fields]

        # 3. 写入临时 CSV
        logger.info("正在生成临时 CSV 文件...")
        if self.csv_temp_dir.exists():
            shutil.rmtree(self.csv_temp_dir)
        self.csv_temp_dir.mkdir()
        
        # 这里为了简单，将所有数据写入一个 CSV。
        # 生产环境建议按 symbol 分组写入多个 CSV 以并行化。
        df.to_csv(self.csv_temp_dir / "data.csv", index=False)

        # 4. 调用 Qlib dump_bin 命令
        logger.info("调用 Qlib 转换内核 (Convert to Binary)...")
        try:
            cmd = [
                sys.executable, "-m", "qlib.run.dump_bin",
                "dump_all",
                "--csv_path", str(self.csv_temp_dir),
                "--qlib_dir", str(self.qlib_dir),
                "--symbol_field_name", "symbol",
                "--date_field_name", "date",
                "--include_fields", "open,high,low,close,volume"
            ]
            
            # 使用 subprocess 执行外部命令
            result = subprocess.run(cmd, check=True, capture_output=True, text=True)
            logger.info(f"Qlib 转储成功: {result.stdout}")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Qlib 转储失败 (Exit Code {e.returncode}):\n{e.stderr}")
        except Exception as e:
            logger.error(f"转储过程发生未知错误: {e}")
        finally:
             # 清理临时文件
             if self.csv_temp_dir.exists():
                shutil.rmtree(self.csv_temp_dir)
                logger.debug("临时文件已清理")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dumper = QlibDumper()
    # dumper.dump(start_date='2023-01-01')
