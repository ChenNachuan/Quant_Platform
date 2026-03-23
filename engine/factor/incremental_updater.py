"""
增量更新引擎
用于实盘或每日收盘后，增量计算因子值，而无需重算历史数据

审计于 2026 年 3 月 2 日

TODO：

1. 目前为了简化逻辑，在 update_factor 中只更新了日频的因子

2.  使用 StorageManager 加载数据时，考虑根据交易日准确加载数据

3. 考虑使用数据库层面的分组查询，或者分批次（如每次处理 100 只股票）加载计算

4. 使用专门的交易日历工具来获取准确的 start_date

5. deps=None 写死了，导致无法计算复合因子
"""
import pandas as pd
from typing import List, Dict, Optional, Union
import logging

from factor_library.registry import FactorRegistry
from factor_library.dag import FactorDAG
from infra.storage import StorageManager

logger = logging.getLogger(__name__)

class IncrementalUpdater:
    """
    增量更新器
    
    负责：
    1. 确定因子计算顺序 (DAG)
    2. 准备数据 (最新1天 + 历史窗口)
    3. 调用 factor.update()
    4. 存储结果
    """
    
    def __init__(self):
        self.registry = FactorRegistry  #初始化因子注册表
        self.dag = FactorDAG()  # 初始化因子注册顺序
        self.storage = StorageManager()  # 初始化存储管理器
        
        # 构建 DAG
        '''
        遍历 FactorRegistry 中所有已注册的因子，提取它们声明的依赖关系（因子内部的 dependencies 属性），
        并基于这些关系在内存中构建出完整的计算顺序网络（DAG），
        从而确保后续 update_all()时调用拓扑排序（topological_sort）能得到正确的执行顺序
        '''
        self.dag.build_from_registry(self.registry)
        
    def update_all(self, date: str, codes: List[str] = None):
        """
        更新所有因子到指定日期

        Args:
            date: 更新日期 'YYYY-MM-DD'
            codes: 指定股票列表，None表示所有
        """
        logger.info(f"开始增量更新因子: {date}")

        # 1. 获取所有因子并按依赖顺序排序
        all_factors = self.registry.list_factors()
        sorted_factors = self.dag.topological_sort_for_update(all_factors)

        # 2. 跨因子共享的增量结果缓存: (factor_id, entity_id) -> pd.Series
        #    保证复合因子能拿到上游因子在本次更新中的计算结果
        incremental_cache: Dict[tuple, pd.Series] = {}

        # 3. 按拓扑顺序逐个更新
        for factor_name in sorted_factors:
            self.update_factor(factor_name, date, codes, incremental_cache=incremental_cache)

        logger.info(f"增量更新完成: {date}")
            
    def update_factor(self, factor_name: str, date: str, codes: List[str] = None,
                      incremental_cache: Dict[tuple, pd.Series] = None):
        """
        更新单个因子
        """
        logger.info(f"正在更新因子: {factor_name}")
        
        factor_cls = self.registry.get(factor_name)
        if not factor_cls:
            logger.warning(f"因子 {factor_name} 未找到注册信息")
            return
            
        # 实例化因子 (假设只处理 1d 频率，且使用默认参数组的第一组参数用于演示)
        # 实际生产中需要遍历 param_group
        # 这里为了简化，我们先只更新 1d, window=20 的 Momentum
        # 更好的做法是：Updater 应该遍历所有已实例化的因子配置？
        # 或者我们遍历 registry 中的 para_group
        
        if '1d' not in factor_cls.para_group:
            return
            
        para_configs = factor_cls.para_group['1d']
        # 假设 para_group 结构: {'window': [5, 10, 20]}
        # 我们需要生成笛卡尔积，这里简化处理，假设只有一个参数 key
        
        import itertools

        # 获取参数名列表
        keys = para_configs.keys()

        # 获取参数值列表
        values = para_configs.values()

        # 将参数值列表转换为笛卡尔积
        for instance_values in itertools.product(*values):

            # 将参数名和当前组合组合成列表
            para = dict(zip(keys, instance_values))
            
            # 创建实例
            factor = factor_cls(timeframe='1d', para=para)
            
            try:
                self._process_single_factor_instance(factor, date, codes,
                                                     incremental_cache=incremental_cache)
            except Exception as e:
                logger.error(f"因子 {factor.name} 参数 {para} 更新失败: {e}")
                import traceback
                traceback.print_exc()

    def _process_single_factor_instance(self, factor, date: str, codes: List[str],
                                         incremental_cache: Dict[tuple, pd.Series] = None):
        """处理单个因子实例的更新"""

        # 因子声明的最大回溯窗口
        lookback = factor.max_lookback
        
        # 1. 加载数据
        # 需要 [date - lookback, date] 的数据
        # XXX：这里简化了，直接加载范围数据。实盘中可能需要更精确的交易日推算。

        start_date = pd.Timestamp(date) - pd.Timedelta(days=lookback * 2) # 多取一点以防非交易日
        end_date = pd.Timestamp(date)

        # 使用 StorageManager 加载
        params = [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
        query = """
            SELECT timestamp, entity_id, open, high, low, close, volume
            FROM cn_stock_1d_hfq
            WHERE timestamp >= $1
              AND timestamp <= $2
        """

        if codes:
            # 这里的 codes 需要转换为 entity_ids 或者 view 支持 symbol
            # cn_stock_1d_hfq 只有 entity_id 和 symbol (000001)
            # 假设 codes 是 symbol 列表
            placeholders = ", ".join(f"${i+3}" for i in range(len(codes)))
            query += f" AND symbol IN ({placeholders})"
            params.extend(codes)

        query += " ORDER BY entity_id, timestamp"

        df = self.storage.conn.execute(query, params).df()
        
        if df.empty:
            logger.warning(f"没有数据用于更新 {factor.get_id()} (date={date})")
            return
            
        # 转换为 MultiIndex [timestamp, code] (Base Factor 要求)
        # view 中有 entity_id 和 symbol. Base Factor 文档说 index 是 [timestamp, code]
        # 但 ZVTAdapter 和 Momentum 示例中使用了 entity_id 或 whatever available.
        # Momentum compute 只需要 columns.
        # update 需要 history + new_data.
        
        # 2. 分组计算 (按股票)
        results = []

        # 先按 entity_id 分组
        for entity_id, group in df.groupby('entity_id'):
            group = group.sort_values('timestamp')
            
            # 分割为 history 和 new_data
            # new_data 是 date 当天的数据
            mask_new = group['timestamp'] == pd.Timestamp(date)
            new_data_df = group[mask_new]
            history_df = group[~mask_new]
            
            if new_data_df.empty:
                continue
                
            # 设置索引
            # data: 原始OHLCV数据，索引为 MultiIndex[timestamp, code] ... 实际上 base.py 好像没强制索引
            # 但 momentum.py 用 data['close'].pct_change()，依赖时间顺序
            new_data_df = new_data_df.set_index('timestamp')
            history_df = history_df.set_index('timestamp')
            
            # 调用 incremental update
            try:
                # 从 incremental_cache 中为复合因子构建 deps
                deps = None
                dep_names = getattr(factor, 'dependencies', None)
                if dep_names and incremental_cache is not None:
                    deps = {}
                    from factor_library.registry import generate_factor_id
                    dep_para_map = getattr(factor, 'dependency_para_map', {})
                    for dep_name in dep_names:
                        dep_para = dep_para_map.get(dep_name, getattr(factor, 'para', {}))
                        dep_factor_id = generate_factor_id(dep_name, factor.timeframe, dep_para)
                        cache_key = (dep_factor_id, entity_id)
                        if cache_key in incremental_cache:
                            deps[dep_name] = incremental_cache[cache_key]
                        else:
                            logger.warning(
                                f"依赖因子 {dep_name} 的增量结果不在缓存中，"
                                f"因子 {factor.get_id()} 可能计算失败"
                            )

                res_series = factor.update(new_data_df, history_df, deps=deps)

                # 结果处理，将结果格式化，补充元数据
                # res_series 是 Series, index 是 timestamp
                if not res_series.empty:
                    res_df = res_series.to_frame(name='value')
                    res_df['entity_id'] = entity_id
                    res_df['timestamp'] = res_df.index
                    res_df['code'] = entity_id.split('_')[-1] # 简易提取
                    results.append(res_df)

                    # 将结果存入 incremental_cache，供下游复合因子使用
                    if incremental_cache is not None:
                        incremental_cache[(factor.get_id(), entity_id)] = res_series
                    
            except Exception as e:
                logger.error(f"{entity_id} 计算出错: {e}")
                
        # 3. 存储结果
        if results:
            final_df = pd.concat(results, ignore_index=True)
            
            # 存储到因子表
            # market = factor_id
            factor_id = factor.get_id()
            
            logger.info(f"保存因子结果: {factor_id}, 条数: {len(final_df)}")
            self.storage.write_data(
                final_df,
                category='factors',
                market=factor_id,
                frequency=factor.timeframe
            )
