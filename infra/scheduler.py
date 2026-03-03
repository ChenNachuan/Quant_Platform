"""
Version: 0.1.0
Date: 2026-02-17
修改内容: 初次生成后代码审计，工作流核心任务除了更新视图以外未完成
"""
from prefect import flow, task, get_run_logger
from pathlib import Path
import sys
import os

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from infra.storage import StorageManager

@task(name="数据抓取与存储")
def fetch_and_store_data():
    """
    任务: 从数据源抓取数据并存储至数据湖 (Parquet)。
    """
    logger = get_run_logger()
    logger.info("开始数据抓取任务...")
    
    # 初始化存储管理器
    storage = StorageManager()
    
    # TODO: 调用 Adapter 获取数据
    # df = zvt_adapter.fetch_data(...)
    # storage.write_market_data(df)
    
    logger.info("数据抓取任务完成")

@task(name="Qlib 数据转储")
def update_qlib_data():
    """
    任务: 将增量数据转换为 Qlib 所需的 Binary 格式。
    """
    logger = get_run_logger()
    logger.info("开始更新 Qlib 二进制数据...")
    
    # TODO: 调用 Qlib Bridge
    # dumper.dump()
    
    logger.info("Qlib 数据更新完成")

@task(name="VectorBT 回测")
def run_backtest():
    """
    任务: 运行 VectorBT (Numba 加速) 回测策略。
    """
    logger = get_run_logger()
    logger.info("开始执行 VectorBT 回测...")
    
    # TODO: 调用策略 Runner
    
    logger.info("回测任务完成")

@task(name="刷新数据湖视图")
def refresh_data_lake_views():
    """
    任务: 刷新 DuckDB 视图以感知新写入的分区或列。
    通常在所有写入任务完成后执行。
    """
    logger = get_run_logger()
    logger.info("开始刷新数据湖视图...")
    
    with StorageManager() as storage:
        storage.refresh_views()
    
    logger.info("视图刷新完成")

@flow(name="每日工作流")
def daily_flow():
    """
    每日量化全流程编排:
    1. 数据抓取 (Fetch): 从各个数据源 (Tushare/ZVT) 获取最新数据并存入 Data Lake。
    2. 全局视图刷新 (Refresh): 确保 DuckDB 能查询到最新的 Parquet 文件。
    3. Qlib 转储 (Dump): 将 Data Lake 中的增量数据转换为 Qlib Binary 格式。
    4. 策略回测 (Backtest): 运行已注册的 VectorBT 策略进行每日跟踪。
    """
    logger = get_run_logger()
    logger.info("启动每日工作流...")
    
    try:
        # 1. 数据抓取
        fetch_result = fetch_and_store_data()
        
        # 2. 视图刷新 (新增步骤)
        # 必须在抓取完成后执行，以便后续任务(Qlib/Backtest)能查到新数据
        refresh_result = refresh_data_lake_views(wait_for=[fetch_result])

        # 3. Qlib 更新 
        update_result = update_qlib_data(wait_for=[refresh_result])
        
        # 4. 回测 
        # 注意: 这里的 wait_for 仅确保执行顺序，数据传递应通过参数或共享存储
        backtest_result = run_backtest(wait_for=[update_result])
        
        logger.info("每日量化工作流执行完毕")
        
    except Exception as e:
        logger.error(f"工作流执行失败: {e}")
        raise e

if __name__ == "__main__":
    daily_flow()
