# 模型信息：Deepmind Antigravity | 70B | Chat | 2026-03-11

import os
import sys
import numpy as np
import pandas as pd
import vectorbt as vbt
import logging
from typing import Dict
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from infra.storage import StorageManager
from engine.pipeline import (
    AlphaPipeline, UniverseFilter, FactorAlphaModel, 
    QuantilePortfolio, DefaultRiskFilter, MultiVectorBTExecutionHandler
)
from factor_library.technical.momentum import SimpleMomentum

def run_momentum_backtest():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger('MomentumBacktest')
    
    logger.info("1. 初始化存储引擎，从 DuckDB 读取数据...")
    storage = StorageManager()
    
    # 获取 2024 年以来的基础数据（仅获取具有数据的子集，防止内存爆炸，这里我们获取宽基指数如中证 500 的一部分做演示，或者随机）
    # 为了回测顺利，我们筛选一些具有代表性的大盘股进行测试
    test_symbols = [
        '000001', '000002', '000333', '600000', '600030', 
        '600519', '601318', '601398', '601899', '601988',
        '000858', '002594', '300750', '600036', '600276',
        '600887', '601012', '601166', '603259', '002415'
    ]
    
    # 构建 Entity_ID 条件
    ids = [f"stock_{s[:2]}_{s}" if not s.startswith('6') else f"stock_sh_{s}" for s in test_symbols]
    ids_str = "','".join(ids)
    
    query = f"""
        SELECT timestamp, entity_id, open, high, low, close, volume, hfq_factor 
        FROM market_cn_stock_1d 
        WHERE timestamp >= '2024-01-01' 
        ORDER BY timestamp, entity_id
    """
    
    # 如果只想在 20 只股票里玩，可以加上 AND entity_id IN ('{ids_str}')
    # 此处我们尝试在库内全部获取到的数据上运行
    
    try:
        kdata = storage.query(query)
    except Exception as e:
        logger.error(f"查询失败: {e}")
        return

    if kdata.empty:
        logger.error("DuckDB 中没有拿到数据，请确认 fetch_all_a.py/update_daily.py 是否已完成填充")
        return
        
    logger.info(f"成功加载行数: {len(kdata)}")

    logger.info("2. 组装量化 5 层 Pipeline...")
    
    # A. 实例化自定义的量化策略组件
    # 策略逻辑：计算 30 日动量，然后在组合管理层做多截面排名前 20% 的股票
    
    from engine.pipeline import QuantilePortfolio, MultiVectorBTExecutionHandler
    from engine.pipeline import AlphaPipeline
    
    # 组装定制的多层管线
    pipeline = AlphaPipeline(
        universe=UniverseFilter(min_volume=1000),             # [1] 去除停牌和无流动性的票
        alpha=FactorAlphaModel(SimpleMomentum(timeframe='1d', para={'window': 30})),         # [2] 挂载 30日动量因子
        portfolio=QuantilePortfolio(n_quantiles=5),         # [3] 组合构造：5分组模型
        risk=DefaultRiskFilter(max_weight=0.1),             # [4] 单票权重不超过 10%
        execution=MultiVectorBTExecutionHandler(
            init_cash=1000000,
            fees=0.001,   # 模拟千分之一的双边摩擦
            slippage=0.002 # 模拟千分之二的滑点冲击
        )                                                     # [5] 矩阵回测引擎
    )

    logger.info("3. 注入数据开始 VectorBT 矩阵回测...")
    
    # 按照之前 `test_pipeline_vbt.py` 的用法，将 kdata 灌入执行
    portfolio = pipeline.run(kdata)
    
    # 打印一些核心分析结果
    if portfolio is not None:
        logger.info("\n========= 动量 5 分组策略核心指标 =========")
        
        # portfolio 现在同时包含 5 个 group (Group_1 到 Group_5) 的曲线结果
        print("各分组总收益 (%)：")
        returns = portfolio.total_return() * 100
        print(returns)
        print("\n各分组夏普比率：")
        sharpes = portfolio.sharpe_ratio()
        print(sharpes)
        
        try:
            import plotly.graph_objects as go
            html_path = project_root / 'results' / 'momentum_5_quantiles_report.html'
            html_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 手动画出各组以及多空对冲 (Group_5 - Group_1) 的累计净值曲线
            fig = go.Figure()
            
            # portfolio.value() 返回的是 (Timestamp, Group) 矩阵
            values_df = portfolio.value()
            
            for group_name in values_df.columns:
                # 归一化为净值 1 起步
                normalized_value = values_df[group_name] / pipeline.execution.init_cash if hasattr(pipeline, 'execution') else values_df[group_name] / 1000000.0
                fig.add_trace(go.Scatter(x=values_df.index, y=normalized_value, mode='lines', name=group_name))
            
            # 计算纯多空对冲收益曲线 (Long Top - Short Bottom)，如果支持做空的话。由于此时的权重都是 0 到 1，这相当于绝对收益的多空差距
            if 'Group_5' in values_df.columns and 'Group_1' in values_df.columns:
                 # 计算简单的多空对冲净值 (假设起始也是 1.0)
                 # 每日多空对冲收益率 = Group5_ret - Group1_ret
                 ret_g5 = values_df['Group_5'].pct_change().fillna(0)
                 ret_g1 = values_df['Group_1'].pct_change().fillna(0)
                 ls_ret = ret_g5 - ret_g1
                 ls_value = (1 + ls_ret).cumprod()
                 fig.add_trace(go.Scatter(x=values_df.index, y=ls_value, mode='lines', name='Long-Short (G5-G1)', line=dict(dash='dash', width=3)))
            
            fig.update_layout(title='30日动量因子 5 分组净值曲线', xaxis_title='Date', yaxis_title='Cumulative Value (Base=1)')
            fig.write_html(str(html_path))
            logger.info(f"5 分组回测曲线网页版已保存至: {html_path}")
        except Exception as e:
            logger.error(f"画图保存失败: {e}")

if __name__ == '__main__':
    run_momentum_backtest()
