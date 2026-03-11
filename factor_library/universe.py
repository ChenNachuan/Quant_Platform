"""
动态股票池过滤器（Universe Selection）
Version: 1.0.0
Date: 2026-03-11

按每个交易日动态构建有效股票池，防止幸存者偏差。

"幸存者偏差" 示例：
    如果回测时只用"现在还存在的股票"去查历史数据，
    已退市的垃圾股会被忽略，回测收益会虚高。

使用示例:
    uf = UniverseFilter(listing_days=60, filter_st=True, filter_suspended=True)
    valid_mask = uf.filter(kdata_df)   # 布尔宽表 (date × entity_id)

    # 将因子值中不在池内的位置置为 NaN
    factor_df = factor_df.where(valid_mask)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional, List

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class UniverseConfig:
    """Universe 过滤器参数配置"""
    listing_days:       int  = 60     # 新股上市后锁定天数（次新股排除）
    filter_st:          bool = True   # 是否过滤 ST/*ST 股票
    filter_suspended:   bool = True   # 是否过滤停牌股（volume == 0）
    filter_delisted:    bool = True   # 是否过滤已退市股票
    min_price:          float = 1.0   # 最低价格过滤（元）：排除仙股
    max_price_pct_up:   float = 0.09  # 涨停过滤阈值（涨幅 > 9% 视为潜在涨停，当日不建仓）


class UniverseFilter:
    """
    按日动态股票池过滤器。

    每个交易日 T，对股票池应用以下过滤规则：
    1. 已退市股：T 日已触及退市日期          → 排除
    2. T 日停牌：成交量 == 0                → 排除
    3. ST / *ST：股票名称含 "ST"            → 排除（可选）
    4. 次新股：上市不满 listing_days 个交易日 → 排除
    5. 仙股：收盘价 < min_price             → 排除

    输出：布尔宽表 DataFrame (index=date, columns=entity_id)
          True  = 当天可以交易
          False = 当天排除
    """

    def __init__(self, config: Optional[UniverseConfig] = None, **kwargs):
        """
        Args:
            config: UniverseConfig 实例，不传则使用默认配置
            **kwargs: 直接传入 UniverseConfig 字段（优先级高于 config）
        """
        if config is None:
            config = UniverseConfig()
        # kwargs 覆盖
        for k, v in kwargs.items():
            if hasattr(config, k):
                setattr(config, k, v)
        self.cfg = config

    def filter(
        self,
        kdata: pd.DataFrame,
        entity_info: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        构建有效股票池布尔矩阵。

        Args:
            kdata: 行情数据，长表格式
                   必须包含列：timestamp, entity_id, volume, close
                   可选列：name（股票名称，用于 ST 过滤），
                           list_date（上市日期，用于新股过滤）
            entity_info: 股票基础信息表（可选）
                         含 entity_id、list_date、delist_date、name
                         若 kdata 中已有对应列，则此参数可省略

        Returns:
            布尔宽表 DataFrame，index=date, columns=entity_id，True 表示可交易
        """
        kdata = kdata.copy()

        # ── 确保时间戳为 datetime 类型 ──
        kdata['timestamp'] = pd.to_datetime(kdata['timestamp'])

        # ── 合并 entity_info（若提供）──
        if entity_info is not None:
            info_cols = [c for c in ['entity_id', 'name', 'list_date', 'delist_date']
                         if c in entity_info.columns]
            kdata = kdata.merge(
                entity_info[info_cols],
                on='entity_id',
                how='left',
                suffixes=('', '_info')
            )

        # ── 构建宽表骨架 ──
        dates    = sorted(kdata['timestamp'].unique())
        entities = sorted(kdata['entity_id'].unique())

        # 初始化：全部为 True（可交易）
        mask = pd.DataFrame(True, index=dates, columns=entities)

        # ── 规则 1：停牌过滤（volume == 0）──
        if self.cfg.filter_suspended and 'volume' in kdata.columns:
            suspended = (
                kdata[['timestamp', 'entity_id', 'volume']]
                .assign(suspended=lambda df: df['volume'] == 0)
                .pivot(index='timestamp', columns='entity_id', values='suspended')
                .reindex(index=dates, columns=entities)
                .fillna(False)
            )
            mask &= ~suspended
            n_suspended = suspended.sum().sum()
            if n_suspended > 0:
                logger.debug(f"停牌过滤：共 {n_suspended:.0f} 个记录被排除")

        # ── 规则 2：仙股过滤（close < min_price）──
        if 'close' in kdata.columns and self.cfg.min_price > 0:
            penny = (
                kdata[['timestamp', 'entity_id', 'close']]
                .assign(is_penny=lambda df: df['close'] < self.cfg.min_price)
                .pivot(index='timestamp', columns='entity_id', values='is_penny')
                .reindex(index=dates, columns=entities)
                .fillna(False)
            )
            mask &= ~penny

        # ── 规则 3：ST / *ST 过滤 ──
        if self.cfg.filter_st and 'name' in kdata.columns:
            # name 字段按 entity_id 聚合（取最新值）
            name_map = (
                kdata.groupby('entity_id')['name'].last()
            )
            is_st = name_map.str.contains('ST', case=False, na=False)
            st_entities = is_st[is_st].index.tolist()
            if st_entities:
                mask[st_entities] = False
                logger.debug(f"ST 过滤：{len(st_entities)} 只 ST 股票被排除")

        # ── 规则 4：次新股过滤（上市不满 listing_days 日）──
        if self.cfg.listing_days > 0 and 'list_date' in kdata.columns:
            list_date_map = (
                kdata.groupby('entity_id')['list_date']
                .first()
                .pipe(pd.to_datetime, errors='coerce')
            )
            for date in dates:
                cutoff = date - pd.Timedelta(days=self.cfg.listing_days * 1.5)
                new_stocks = list_date_map[list_date_map > cutoff].index.tolist()
                if new_stocks:
                    valid_new = [s for s in new_stocks if s in mask.columns]
                    if valid_new:
                        mask.loc[date, valid_new] = False

        # ── 规则 5：退市股过滤 ──
        if self.cfg.filter_delisted and 'delist_date' in kdata.columns:
            delist_map = (
                kdata.groupby('entity_id')['delist_date']
                .first()
                .pipe(pd.to_datetime, errors='coerce')
                .dropna()
            )
            for date in dates:
                delisted = delist_map[delist_map <= date].index.tolist()
                valid_delisted = [s for s in delisted if s in mask.columns]
                if valid_delisted:
                    mask.loc[date, valid_delisted] = False

        valid_count = mask.sum(axis=1)
        logger.info(
            f"Universe Filter 完成：日均有效标的 {valid_count.mean():.0f} 只 "
            f"（最少 {valid_count.min():.0f}，最多 {valid_count.max():.0f}）"
        )
        return mask

    def apply_to_factor(
        self,
        factor_wide: pd.DataFrame,
        kdata: pd.DataFrame,
        entity_info: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        快捷方法：直接对因子宽表应用 Universe 过滤。

        Args:
            factor_wide: 因子宽表 (index=date, columns=entity_id)
            kdata:       行情数据（传给 filter()）
            entity_info: 股票基础信息（可选）

        Returns:
            过滤后的因子宽表（无效位置置为 NaN）
        """
        mask = self.filter(kdata, entity_info)
        # 对齐 index 和 columns，确保不越界
        common_dates    = factor_wide.index.intersection(mask.index)
        common_entities = factor_wide.columns.intersection(mask.columns)
        filtered = factor_wide.copy()
        filtered.loc[common_dates, common_entities] = factor_wide.loc[
            common_dates, common_entities
        ].where(mask.loc[common_dates, common_entities])
        return filtered


__all__ = ['UniverseConfig', 'UniverseFilter']
