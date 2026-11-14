"""
动态股票池过滤器（Universe Selection）

按交易日构建有效股票池（剔除停牌、ST/*ST、次新、仙股、已退市标的），防止幸存者偏差。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class UniverseConfig:
    """股票池过滤配置参数"""
    listing_days:       int  = 60     # 上市最短天数（过滤次新股）
    filter_st:          bool = True   # 是否过滤 ST/*ST
    filter_suspended:   bool = True   # 是否过滤停牌股（volume == 0）
    filter_delisted:    bool = True   # 是否过滤已退市股
    min_price:          float = 1.0   # 最低收盘价（过滤低价股）
    max_price_pct_up:   float = 0.09  # 涨停过滤阈值


class UniverseFilter:
    """按日动态股票池过滤器，输出布尔宽表 (date × entity_id)，True 表示可交易。"""

    def __init__(self, config: Optional[UniverseConfig] = None, **kwargs):
        if config is None:
            config = UniverseConfig()
        for k, v in kwargs.items():
            if hasattr(config, k):
                setattr(config, k, v)
        self.cfg = config

    def filter(
        self,
        kdata: pd.DataFrame,
        entity_info: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """构建每日可交易标的布尔宽表。

        Args:
            kdata: 长表行情（需含 timestamp, entity_id, volume, close）
            entity_info: 可选基础信息表（含 entity_id, list_date, delist_date, name）
        """
        kdata = kdata.copy()
        kdata['timestamp'] = pd.to_datetime(kdata['timestamp'])

        if entity_info is not None:
            info_cols = [c for c in ['entity_id', 'name', 'list_date', 'delist_date']
                         if c in entity_info.columns]
            kdata = kdata.merge(
                entity_info[info_cols],
                on='entity_id',
                how='left',
                suffixes=('', '_info')
            )

        dates    = sorted(kdata['timestamp'].unique())
        entities = sorted(kdata['entity_id'].unique())
        mask = pd.DataFrame(True, index=dates, columns=entities)

        # 规则 1：停牌过滤（volume == 0）
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

        # 规则 2：低价股过滤（close < min_price）
        if 'close' in kdata.columns and self.cfg.min_price > 0:
            penny = (
                kdata[['timestamp', 'entity_id', 'close']]
                .assign(is_penny=lambda df: df['close'] < self.cfg.min_price)
                .pivot(index='timestamp', columns='entity_id', values='is_penny')
                .reindex(index=dates, columns=entities)
                .fillna(False)
            )
            mask &= ~penny

        # 规则 3：ST / *ST 过滤
        if self.cfg.filter_st and 'name' in kdata.columns:
            name_map = (
                kdata.groupby('entity_id')['name'].last()
            )
            is_st = name_map.str.contains('ST', case=False, na=False)
            st_entities = is_st[is_st].index.tolist()
            if st_entities:
                mask[st_entities] = False
                logger.debug(f"ST 过滤：{len(st_entities)} 只 ST 股票被排除")

        # 规则 4：次新股过滤（自然日按交易日 1.5 倍估算）
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

        # 规则 5：已退市过滤
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
        """对因子宽表应用股票池掩码，池外位置置为 NaN。"""
        mask = self.filter(kdata, entity_info)
        common_dates    = factor_wide.index.intersection(mask.index)
        common_entities = factor_wide.columns.intersection(mask.columns)
        filtered = factor_wide.copy()
        filtered.loc[common_dates, common_entities] = factor_wide.loc[
            common_dates, common_entities
        ].where(mask.loc[common_dates, common_entities])
        return filtered


__all__ = ['UniverseConfig', 'UniverseFilter']
