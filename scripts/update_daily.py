
# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-02-15

import sys
import logging
import tushare as ts
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到 Path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from infra.storage import StorageManager, ConfigLoader

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TushareUpdater:
    """
    Tushare 数据更新器
    负责获取最新的日线数据并更新到 Data Lake
    """
    def __init__(self):
        self.config = ConfigLoader.load()
        self.storage = StorageManager()
        
        # 初始化 Tushare
        token = self.config.get('tushare', {}).get('token')
        if not token or token == "YOUR_TUSHARE_TOKEN_HERE":
            logger.warning("未配置有效的 Tushare Token，请在 config/settings.toml 中设置")
            self.pro = None
        else:
            ts.set_token(token)
            self.pro = ts.pro_api()

    def fetch_daily(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的全市场日线行情
        """
        if not self.pro:
            return pd.DataFrame()
            
        try:
            # 获取日线行情
            df = self.pro.daily(trade_date=trade_date)
            if df.empty:
                return df
                
            # 重命名列以符合标准 Schema
            # tushare: ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount
            # standard: entity_id, timestamp, open, high, low, close, volume, turnver, ...
            
            df = df.rename(columns={
                'ts_code': 'entity_id',
                'trade_date': 'timestamp',
                # 'vol': 'vol', # 保持原名
                'amount': 'turnover'
            })
            
            # 转换时间格式
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # 增加标准列
            df['provider'] = 'tushare'
            
            return df
        except Exception as e:
            logger.error(f"获取 {trade_date} 数据失败: {e}")
            return pd.DataFrame()

    def update_market_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        """
        更新市场数据
        """
        if not self.pro:
            logger.error("Tushare 未初始化，无法更新")
            return

        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        
        if not start_date:
            # 默认尝试从最近的数据开始更新？或者只更新当天？
            # 简单起见，如果未指定，默认更新过去 5 天（用于补漏）
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

        logger.info(f"开始更新数据: {start_date} -> {end_date}")
        
        # 生成日期范围
        dates = pd.date_range(start=start_date, end=end_date)
        
        for date in dates:
            date_str = date.strftime('%Y%m%d')
            date_dash = date.strftime('%Y-%m-%d')
            
            # Idempotency Check: 检查数据是否存在
            # 注意: 如果是首次运行，视图可能不存在，query 会报错，需要处理
            try:
                check_sql = f"SELECT count(*) as cnt FROM market_cn_stock_1d WHERE timestamp = '{date_dash}'"
                res = self.storage.query(check_sql)
                if not res.empty and res.iloc[0]['cnt'] > 0:
                     logger.info(f"{date_str} 数据已存在 ({res.iloc[0]['cnt']} 条), 跳过")
                     continue
            except Exception:
                # 视图不存在等情况，忽略，继续抓取
                pass

            logger.info(f"正在抓取 {date_str} ...")
            
            df = self.fetch_daily(trade_date=date_str)
            
            if not df.empty:
                self.storage.write_data(
                    df,
                    category='market_data',
                    market='cn_stock',
                    frequency='1d',
                    partition_cols=['year', 'month']
                )
            else:
                logger.info(f"{date_str} 无数据或非交易日")

if __name__ == "__main__":
    updater = TushareUpdater()
    updater.update_market_data()
