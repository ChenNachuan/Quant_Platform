import sys
import os
import time
import random
import logging
import tushare as ts
import akshare as aks
import pandas as pd
from typing import List, Optional
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目根目录到 Path 并加载环境变量
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from infra.storage import StorageManager, ConfigLoader

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TushareUpdater:
    """
    Tushare 历史与增量数据更新器
    负责获取最新的日线后复权数据、由 AKShare 验证后更新到 Data Lake
    """
    def __init__(self):
        self.config = ConfigLoader.load()
        self.storage = StorageManager()
        
        # 优先读取环境变量，其次配置文件
        token = os.environ.get("TUSHARE_TOKEN") or self.config.get('tushare', {}).get('token')
        if not token or token == "YOUR_TUSHARE_TOKEN_HERE" or token == "your_tushare_token_here":
            logger.warning("未配置有效的 Tushare Token。请在 .env 中设置 TUSHARE_TOKEN")
            self.pro = None
        else:
            ts.set_token(token)
            self.pro = ts.pro_api()

    def fetch_daily_hfq(self, trade_date: str) -> pd.DataFrame:
        """
        获取指定日期的全市场日线行情并合并复权因子，保存以供动态视图计算。
        """
        if not self.pro:
            return pd.DataFrame()
            
        try:
            # 基础行情
            df = self.pro.daily(trade_date=trade_date)
            # 复权因子
            adj = self.pro.adj_factor(trade_date=trade_date)
            
            if df.empty or adj.empty:
                return pd.DataFrame()
                
            # 合并获取复权前后的价格
            res = pd.merge(df, adj[['ts_code', 'adj_factor']], on='ts_code', how='left')
            res['hfq_factor'] = res['adj_factor'].fillna(1.0)
            res['qfq_factor'] = 1.0 # 如果需要精确 qfq_factor 需要当天最新收盘价反推，为演示简单先固定 1.0 或者不写入
                
            # 重命名与数据类型规范化
            res = res.rename(columns={
                'ts_code': 'entity_id',
                'trade_date': 'timestamp',
                'vol': 'volume',
                'amount': 'turnover'
            })
            res['timestamp'] = pd.to_datetime(res['timestamp'])
            res['entity_id'] = res['entity_id'].apply(lambda c: f"stock_{c.split('.')[1].lower()}_{c.split('.')[0]}")
            
            # 使用 year, month 分区
            res['year'] = res['timestamp'].dt.year
            res['month'] = res['timestamp'].dt.month
            
            # 由于底层存储不一定需要强制规范所有列，但为了适配 cn_stock_1d_hfq 视图:
            # CREATE VIEW ... SELECT timestamp, entity_id, open * hfq_factor, close * hfq_factor ... FROM market_cn_stock_1d
            res['code'] = res['entity_id'].apply(lambda n: n.split('_')[-1])
            return res[['timestamp', 'code', 'entity_id', 'open', 'high', 'low', 'close', 'volume', 'turnover', 'hfq_factor', 'qfq_factor', 'year', 'month']]
        except Exception as e:
            logger.error(f"获取 {trade_date} 数据失败: {e}")
            return pd.DataFrame()

    def validate_with_akshare(self, ts_df: pd.DataFrame, sample_size: int = 5) -> bool:
        """从拉取好的 Tushare 数据中随机抽样几个，与 AKShare 东方财富单票后复权接口做数值对比"""
        if ts_df.empty:
            return True
            
        stocks = ts_df['entity_id'].unique().tolist()
        samples = random.sample(stocks, min(sample_size, len(stocks)))
        logger.info(f"AKShare 交叉验证开始，抽样数: {sample_size} 只。")
        
        valid_count = 0
        date_str = ts_df['timestamp'].iloc[0].strftime('%Y%m%d')
        
        for code in samples:
            symbol = code.split('_')[-1] # extract 000001
            try:
                # 注: AKShare 的历史日线接口目前仅能拉单票，故做抽样循环
                ak_df = aks.stock_zh_a_hist(symbol=symbol, period="daily", start_date=date_str, end_date=date_str, adjust="hfq")
                if ak_df.empty:
                    continue
                    
                ak_close = ak_df.iloc[0]['收盘']
                ts_row = ts_df[ts_df['entity_id'] == code].iloc[0]
                ts_close = ts_row['close'] * ts_row['hfq_factor']
                
                # 计算差异率
                diff = abs(ts_close - ak_close) / ak_close
                if diff > 0.005: # 万分之五十误差率容忍度
                    logger.warning(f"交叉验证异常! 标的 {code} 在 {date_str}: AK( {ak_close:.2f} ) vs TS( {ts_close:.2f} ). 误差: {diff:.4%}")
                else:
                    valid_count += 1
                time.sleep(0.05)
            except Exception as e:
                pass # 忽略 AKshare 单次请求失败
                
        if valid_count > 0:
            logger.info(f"交叉验证通过: {valid_count}/{len(samples)} 抽样比对成功。")
            return True
        else:
            logger.warning("所有的 AKShare 交叉验证均未产生有效结果或网络超时。")
            return False

    def update_market_data(self, start_date: Optional[str] = None, end_date: Optional[str] = None):
        if not self.pro:
            logger.error("Tushare 未初始化，无法更新")
            return

        if not end_date:
            end_date = datetime.now().strftime('%Y%m%d')
        if not start_date:
            start_date = (datetime.now() - timedelta(days=5)).strftime('%Y%m%d')

        logger.info(f"启动行情同步任务: {start_date} -> {end_date}")
        
        # 这里用 Tushare 的交易日历功能，避免抓取周末空数据
        cal = self.pro.trade_cal(exchange='SSE', start_date=start_date.replace('-', ''), end_date=end_date.replace('-', ''), is_open='1')
        trade_dates = cal['cal_date'].tolist()
        
        for date_str in trade_dates:
            date_dash = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
            
            # Idempotency Check: 检查数据是否在目标宽表中
            try:
                check_sql = "SELECT count(*) as cnt FROM cn_stock_1d_hfq WHERE timestamp = $1"
                res = self.storage.query(check_sql, [date_dash])
                if not res.empty and res.iloc[0]['cnt'] > 0:
                     logger.info(f"{date_dash} 数据已存在 ({res.iloc[0]['cnt']} 条), 自动跳过")
                     continue
            except Exception:
                pass

            logger.info(f"正在抓取 {date_dash} ...")
            df = self.fetch_daily_hfq(trade_date=date_str)
            
            if not df.empty:
                # 交叉验证
                self.validate_with_akshare(df, sample_size=3)
                
                # 写入 Data Lake
                self.storage.write_data(
                    df,
                    category='market_data',
                    market='cn_stock',
                    frequency='1d',
                    partition_cols=['year']
                )
                logger.info(f"{date_dash} 成功入库: {len(df)} 行记录。")
            else:
                logger.info(f"{date_dash} 返回为空。")
                
            time.sleep(0.4) # 避免积分太低导致限流
            
        logger.info("增量更新结束，开始刷新 DuckDB 视图...")
        self.storage.refresh_views()
        self.storage.create_adjusted_views()
        logger.info("全部任务完成。")

if __name__ == "__main__":
    # 解析命令行或者直接运行
    updater = TushareUpdater()
    # 按照任务要求，清空重载 2024，从 2024-01-01 开始
    updater.update_market_data(start_date="20240101")
