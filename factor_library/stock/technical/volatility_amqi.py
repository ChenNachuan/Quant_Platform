# -*- coding: utf-8 -*-
"""
股票波动率因子（从 AMQI 仓库迁移）
包括：Historical Volatility, Beta, Downside Beta, CVaR
"""
from factor_library.base import Factor
from factor_library.registry import register_factor
import pandas as pd
import numpy as np
from typing import List, Dict, Optional


@register_factor
class StockHistoricalVolatility(Factor):
    """
    股票历史波动率因子

    公式：std(returns, window) * sqrt(252)

    方向：数值越大，波动越剧烈
    """

    name = "stock_historical_volatility"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [20, 60]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算历史波动率

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            历史波动率值（年化）
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 20)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算日收益率
        returns = close.pct_change()

        # 计算滚动标准差并年化
        vol = returns.rolling(window=window).std() * np.sqrt(252)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            vol = vol.stack()
            vol.index.names = ['timestamp', 'code']

        return vol

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 window 条历史数据
        """
        window = self.para.get("window", 20)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockBeta(Factor):
    """
    股票 Beta 因子

    公式：Cov(stock_returns, market_returns) / Var(market_returns)

    方向：数值大于1表示波动大于市场，小于1表示波动小于市场
    """

    name = "stock_beta"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [60, 120]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 Beta 因子

        Args:
            data: OHLCV数据，需要包含 'mkt_ret' 列（市场收益率）
            deps: 无依赖

        Returns:
            Beta 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 60)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算日收益率
        returns = close.pct_change()

        # 如果没有市场收益率，使用等权平均作为市场代理
        if 'mkt_ret' in data.columns:
            if isinstance(data.index, pd.MultiIndex):
                mkt_ret = data['mkt_ret'].unstack(level='code').iloc[:, 0]
            else:
                mkt_ret = data['mkt_ret']
        else:
            # 使用所有股票的等权平均收益作为市场代理
            mkt_ret = returns.mean(axis=1)

        # 计算滚动 Beta
        def rolling_beta(stock_ret, mkt_ret, window):
            """计算滚动 Beta"""
            cov = stock_ret.rolling(window=window).cov(mkt_ret)
            var = mkt_ret.rolling(window=window).var()
            return cov / var

        if isinstance(data.index, pd.MultiIndex):
            beta = pd.DataFrame(index=returns.index, columns=returns.columns)
            for col in returns.columns:
                beta[col] = rolling_beta(returns[col], mkt_ret, window)
        else:
            beta = rolling_beta(returns, mkt_ret, window)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            beta = beta.stack()
            beta.index.names = ['timestamp', 'code']

        return beta

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 window 条历史数据
        """
        window = self.para.get("window", 60)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockDownsideBeta(Factor):
    """
    股票下行 Beta 因子

    公式：只计算市场下跌时的 Beta

    方向：数值越大，在市场下跌时波动越大
    """

    name = "stock_downside_beta"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [60, 120]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": w} for w in self.para_group[self.timeframe]["window"]]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算下行 Beta 因子

        Args:
            data: OHLCV数据，需要包含 'mkt_ret' 列（市场收益率）
            deps: 无依赖

        Returns:
            下行 Beta 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 60)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算日收益率
        returns = close.pct_change()

        # 如果没有市场收益率，使用等权平均作为市场代理
        if 'mkt_ret' in data.columns:
            if isinstance(data.index, pd.MultiIndex):
                mkt_ret = data['mkt_ret'].unstack(level='code').iloc[:, 0]
            else:
                mkt_ret = data['mkt_ret']
        else:
            # 使用所有股票的等权平均收益作为市场代理
            mkt_ret = returns.mean(axis=1)

        # 只在市场下跌时计算 Beta
        def rolling_downside_beta(stock_ret, mkt_ret, window):
            """计算滚动下行 Beta"""
            # 创建掩码：市场下跌时为 True
            mask = mkt_ret < 0

            # 只使用市场下跌时的数据
            stock_ret_down = stock_ret[mask]
            mkt_ret_down = mkt_ret[mask]

            # 计算滚动协方差和方差
            cov = stock_ret_down.rolling(window=window, min_periods=window//2).cov(mkt_ret_down)
            var = mkt_ret_down.rolling(window=window, min_periods=window//2).var()

            return cov / var

        if isinstance(data.index, pd.MultiIndex):
            beta = pd.DataFrame(index=returns.index, columns=returns.columns)
            for col in returns.columns:
                beta[col] = rolling_downside_beta(returns[col], mkt_ret, window)
        else:
            beta = rolling_downside_beta(returns, mkt_ret, window)

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            beta = beta.stack()
            beta.index.names = ['timestamp', 'code']

        return beta

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 window 条历史数据
        """
        window = self.para.get("window", 60)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


@register_factor
class StockCVaR(Factor):
    """
    股票 CVaR (Conditional Value at Risk) 因子

    公式：在 VaR 以下的平均损失

    方向：数值越大，尾部风险越高
    """

    name = "stock_cvar"
    input_type = "bar"
    max_lookback = 252
    applicable_market = ["CN_STOCK"]
    store_time = "20260703"

    para_group = {
        "1d": {"window": [60], "alpha": [0.05]},
    }

    dependencies = []

    post_process_steps = ['winsorize', 'standardize']
    winsorize_params = {'lower': 0.01, 'upper': 0.99}
    standardize_method = 'zscore'

    def generate_para_space(self) -> List[Dict[str, int]]:
        if self.timeframe not in self.para_group:
            return []
        return [{"window": 60, "alpha": 0.05}]

    def compute(self, data: pd.DataFrame, deps: Optional[Dict[str, pd.Series]] = None) -> pd.Series:
        """
        计算 CVaR 因子

        Args:
            data: OHLCV数据，索引为 MultiIndex[timestamp, code]
            deps: 无依赖

        Returns:
            CVaR 值
        """
        required_cols = ['close']
        if not set(required_cols).issubset(data.columns):
            raise KeyError(f"缺少必要列: {required_cols}")

        window = self.para.get("window", 60)
        alpha = self.para.get("alpha", 0.05)

        # 按股票分组计算
        if isinstance(data.index, pd.MultiIndex):
            close = data['close'].unstack(level='code')
        else:
            close = data['close']

        # 计算日收益率
        returns = close.pct_change()

        # 计算滚动 CVaR
        def rolling_cvar(series, window, alpha):
            """计算滚动 CVaR"""
            def cvar_func(x):
                if len(x) < window:
                    return np.nan
                var = np.percentile(x, alpha * 100)
                return x[x <= var].mean()
            return series.rolling(window=window).apply(cvar_func, raw=True)

        cvar = returns.rolling(window=window).apply(
            lambda x: x[x <= np.percentile(x, alpha * 100)].mean() if len(x) >= window else np.nan,
            raw=True
        )

        # 转回 long format
        if isinstance(data.index, pd.MultiIndex):
            cvar = cvar.stack()
            cvar.index.names = ['timestamp', 'code']

        return cvar

    def update(
        self,
        new_data: pd.DataFrame,
        history: pd.Series,
        deps: Optional[Dict[str, pd.Series]] = None
    ) -> pd.Series:
        """
        增量更新

        需要 window 条历史数据
        """
        window = self.para.get("window", 60)

        # 取最近 window 条历史数据
        recent_history = history.iloc[-window:] if len(history) >= window else history
        combined = pd.concat([recent_history, new_data])

        # 重算
        result = self.compute(combined, deps)

        # 只返回新数据部分
        return result.iloc[-len(new_data):]


__all__ = ['StockHistoricalVolatility', 'StockBeta', 'StockDownsideBeta', 'StockCVaR']
