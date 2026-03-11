import sys
from pathlib import Path
import logging

# 将项目根目录加入 sys.path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from factor_library.universe import UniverseFilter, UniverseConfig
from engine.pipeline import (
    FactorAlphaModel,
    EqualWeightPortfolio,
    DefaultRiskFilter,
    VectorBTExecutionHandler,
    AlphaPipeline
)
from factor_library.registry import FactorRegistry

# 加载因子库中真实的因子
try:
    from factor_library.technical.momentum import MomentumReturn
except ImportError:
    MomentumReturn = None

from zvt.domain import Stock1dHfqKdata

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_test():
    print("=== 初始化量化 5 层流水线 测试 ===")
    
    # 获取测试数据
    print("加载股票数据...")
    from infra.storage import StorageManager
    storage = StorageManager()
    
    # 模拟数据湖中存在的数据
    entity_ids = ['stock_sz_000001']
    ids_str = "','".join(entity_ids)
    
    query = f"""
        SELECT timestamp, entity_id, open, high, low, close, volume 
        FROM cn_stock_1d_hfq 
        WHERE entity_id IN ('{ids_str}') 
          AND timestamp >= '2020-01-01' 
          AND timestamp <= '2024-05-01'
        ORDER BY entity_id, timestamp
    """
    try:
        kdata = storage.query(query)
    except Exception as e:
        print(f"DuckDB 查询失败: {e}")
        kdata = None
        
    if kdata is None or kdata.empty:
        print("未获取到数据，请检查 Parquet 数据湖是否完整。")
        return
        
    print(f"数据加载完成: {len(kdata)} 行")
    
    # 1. 初始化 Universe (过滤停牌、ST等)
    universe = UniverseFilter(UniverseConfig(filter_st=True, filter_suspended=True))
    
    # 2. 初始化 Alpha 模型 (以动量因子为例)
    if MomentumReturn is None:
        print("未找到 MomentumReturn 因子，测试跳过。")
        return
        
    factor = MomentumReturn(timeframe='1d', para={'window': 20})
    alpha_model = FactorAlphaModel(factor)
    
    # 3. 初始化组合优化器 (等权)
    portfolio = EqualWeightPortfolio()
    
    # 4. 初始化风控 (最大单票仓位 50%)
    risk = DefaultRiskFilter(max_weight=0.5)
    
    # 5. 初始化 VectorBT 执行器 (初始资金 100万，万三佣金+千一滑点)
    execution = VectorBTExecutionHandler(init_cash=1000000, fees=0.0003, slippage=0.001)
    
    # 组装 Pipeline
    pipeline = AlphaPipeline(universe, alpha_model, portfolio, risk, execution)
    
    # 执行流水线
    print("\n>>> 开始运行 Pipeline ...")
    pf = pipeline.run(kdata=kdata)
    
    # 输出 VectorBT 回测结果
    print("\n=== VectorBT 回测报告 ===")
    print(f"总收益率: {pf.total_return()*100:.2f}%")
    print(f"年化收益: {pf.annualized_return()*100:.2f}%")
    print(f"最大回撤: {pf.max_drawdown()*100:.2f}%")
    print(f"夏普比率: {pf.sharpe_ratio():.2f}")
    
    # 如果想看权重分配细节:
    print("\n最终持仓分配权重 (最近 5 天):")
    # safe_weights = risk.apply_risk_rules(portfolio.create_portfolio(...))
    # 为验证方便，我们可以直接看 get_position_pnl
    stats = pf.stats()
    print(stats)

if __name__ == '__main__':
    run_test()
