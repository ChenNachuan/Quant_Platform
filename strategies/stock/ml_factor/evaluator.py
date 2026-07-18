"""
Evaluation module for ML Factor Strategy.

包含 IC 检验、Barra 检验、Fama-MacBeth 检验、因子模型检验等。
"""
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import statsmodels.api as sm
from scipy import stats

from .config import BarraConfig, DataConfig

warnings.filterwarnings('ignore')


class ICEvaluator:
    """IC 检验器."""

    @staticmethod
    def calculate_ic(all_predictions: pd.DataFrame) -> pd.DataFrame:
        """计算 Rank IC (Spearman Correlation)."""
        print("=" * 50)
        print("IC 检验")
        print("=" * 50)

        ic_series = all_predictions.groupby('date').apply(
            lambda x: x['pred_ret'].corr(x['next_ret'], method='spearman')
        )

        print(f"Information Coefficient (IC) Mean: {ic_series.mean():.4f}")
        print(f"IC IR (IC / IC_Std): {ic_series.mean() / ic_series.std():.4f}")

        return ic_series


class BarraEvaluator:
    """Barra 风险模型检验器."""

    def __init__(self, barra_config: BarraConfig, data_config: DataConfig):
        self.barra_config = barra_config
        self.data_config = data_config

    def run_barra_regression(
        self,
        all_predictions: pd.DataFrame,
        industry_col: str = 'industry3'
    ) -> pd.DataFrame:
        """运行 Barra 截面回归检验."""
        print("=" * 50)
        print("Barra + 行业中性化 截面回归检验")
        print("=" * 50)

        # 读取数据
        df_barra = pd.read_csv(self.data_config.barra_data_path)
        df_barra['date'] = pd.to_datetime(df_barra['date']) + pd.offsets.MonthEnd(0)
        all_predictions = all_predictions.copy()
        all_predictions['date'] = pd.to_datetime(all_predictions['date']) + pd.offsets.MonthEnd(0)

        # 读取行业数据
        if industry_col == 'industry3':
            df_industry = pd.read_excel(self.data_config.industry_data_path, engine='openpyxl')
            df_industry = df_industry.rename(columns={'ts_code': 'stkcd'})
            df_industry_clean = df_industry[['stkcd', 'industry3']]
        else:
            df_industry = pd.read_parquet(self.data_config.stock_basic_path, engine='fastparquet')
            df_industry = df_industry.rename(columns={'ts_code': 'stkcd'})
            df_industry_clean = df_industry[['stkcd', 'industry']]

        # 准备 Barra 因子
        valid_style_factors = [c for c in self.barra_config.style_factors if c in df_barra.columns]
        df_barra_clean = df_barra[['date', 'stkcd'] + valid_style_factors]

        # 合并数据
        print("正在合并数据...")
        barra_data = pd.merge(
            all_predictions[['date', 'stkcd', 'pred_ret', 'next_ret']],
            df_barra_clean,
            on=['date', 'stkcd'],
            how='inner'
        )
        barra_data = pd.merge(barra_data, df_industry_clean, on='stkcd', how='left')

        if len(barra_data) == 0:
            raise ValueError("合并后数据为空！请检查日期或代码格式。")

        # 生成行业哑变量
        print("正在生成行业哑变量...")
        ind_col = industry_col if industry_col in barra_data.columns else 'industry'
        barra_data[ind_col] = barra_data[ind_col].fillna('Unknown')
        barra_data = pd.get_dummies(barra_data, columns=[ind_col], prefix='IND', drop_first=True, dtype=int)

        # 定义回归自变量
        industry_cols = [c for c in barra_data.columns if c.startswith('IND_')]
        X_columns = ['pred_ret'] + valid_style_factors + industry_cols

        # 确保数值型
        for col in X_columns:
            barra_data[col] = pd.to_numeric(barra_data[col], errors='coerce')

        # 逐月回归
        print(f"正在逐月回归 (因子总数: {len(X_columns)})...")
        monthly_gammas = barra_data.groupby('date').apply(
            lambda g: self._cross_sectional_regression(g, X_columns)
        )

        # 统计检验
        barra_summary = pd.DataFrame({
            'Factor': monthly_gammas.columns,
            'Gamma_Mean': monthly_gammas.mean(),
            'T-Statistic': float('nan')
        })

        for col in monthly_gammas.columns:
            series = monthly_gammas[col].dropna()
            if len(series) > 10:
                t_stat, _ = stats.ttest_1samp(series, 0)
                barra_summary.loc[barra_summary['Factor'] == col, 'T-Statistic'] = t_stat

        # 展示结果
        show_list = ['const', 'pred_ret'] + valid_style_factors
        display_df = barra_summary[barra_summary['Factor'].isin(show_list)]

        print("\n>>> Barra + 行业中性化 回归结果:")
        print(display_df.to_string(index=False))

        # 结论
        if 'pred_ret' in barra_summary['Factor'].values:
            ml_t = barra_summary.loc[barra_summary['Factor'] == 'pred_ret', 'T-Statistic'].values[0]
            print(f"\n>>> 核心结论: 控制行业和Barra风格后，ML因子的 t值为 {ml_t:.2f}")
            if abs(ml_t) > 1.96:
                print("   [显著] 策略拥有纯粹的选股 Alpha。")
            else:
                print("   [不显著] 策略收益主要来源于风格暴露或行业轮动。")

        return monthly_gammas

    @staticmethod
    def _cross_sectional_regression(group, X_columns):
        """截面回归."""
        data = group.dropna(subset=['next_ret'] + X_columns)

        if len(data) < len(X_columns) + 10:
            return None

        Y = data['next_ret']
        X = data[X_columns]
        X = sm.add_constant(X)

        try:
            model = sm.OLS(Y, X).fit()
            return model.params
        except:
            return None


class FamaMacBethEvaluator:
    """Fama-MacBeth 回归检验器."""

    @staticmethod
    def run_regression(
        all_predictions: pd.DataFrame,
        df: pd.DataFrame,
        control_vars: List[str] = None
    ) -> pd.DataFrame:
        """运行 Fama-MacBeth 截面回归检验."""
        print("=" * 50)
        print("Fama-MacBeth 截面回归检验")
        print("=" * 50)

        if control_vars is None:
            control_vars = ['size', 'Bm', 'MOM12', 'ILLIQ']

        # 准备数据
        fmb_data = pd.merge(
            all_predictions[['date', 'stkcd', 'pred_ret', 'next_ret']],
            df[['date', 'stkcd'] + control_vars],
            on=['date', 'stkcd'],
            how='inner'
        )

        # 逐月回归
        lambdas = fmb_data.groupby('date').apply(
            lambda g: FamaMacBethEvaluator._cross_sectional_reg(g, control_vars)
        )

        # 计算统计量
        fmb_summary = pd.DataFrame({
            'Mean_Coeff': lambdas.mean(),
            'Std_Error': lambdas.std() / np.sqrt(len(lambdas)),
            't-stat': lambdas.mean() / (lambdas.std() / np.sqrt(len(lambdas)))
        })

        print(">>> Fama-MacBeth 回归结果 (关注 pred_ret 的 t-stat):")
        print(fmb_summary.to_string())

        pred_t = fmb_summary.loc['pred_ret', 't-stat']
        if pred_t > 1.96:
            print(f"\n[结论] ML预测因子的 t值为 {pred_t:.2f}，显著正向，因子有效！")
        else:
            print(f"\n[结论] ML预测因子的 t值为 {pred_t:.2f}，统计上不显著。")

        return fmb_summary

    @staticmethod
    def _cross_sectional_reg(group, control_vars):
        """截面回归."""
        group = group.dropna()

        if len(group) < 30:
            return pd.Series(dtype=float)

        Y = group['next_ret']
        X = group[['pred_ret'] + control_vars]
        X = sm.add_constant(X)

        try:
            model = sm.OLS(Y, X).fit()
            return model.params
        except Exception as e:
            print(f"回归出错: {e}")
            return pd.Series(dtype=float)


class FactorModelEvaluator:
    """因子模型检验器 (CH-3, CH-4, Carhart)."""

    def __init__(self, data_config: DataConfig):
        self.data_config = data_config

    def run_ch3_test(self, performance_df: pd.DataFrame) -> pd.DataFrame:
        """CH-3 因子模型回归检验."""
        print("=" * 50)
        print("CH-3 因子模型回归检验")
        print("=" * 50)

        factors_df = pd.read_excel(self.data_config.ch3_factors_path)
        factors_df = factors_df.rename(columns={
            'mnthdt': 'date',
            'mktrf': 'MKT',
            'SMB': 'SMB',
            'VMG': 'VMG'
        })
        factors_df['date'] = pd.to_datetime(factors_df['date'].astype(str))
        factors_df = factors_df.set_index('date')
        factors_df.index = factors_df.index + pd.offsets.MonthEnd(0)

        performance_df = performance_df.copy()
        performance_df.index = pd.to_datetime(performance_df.index) + pd.offsets.MonthEnd(0)

        aligned_data = pd.concat([performance_df, factors_df], axis=1, join='inner')

        print(f"合并后数据量: {len(aligned_data)} 行")

        results = pd.DataFrame({
            'EW_Strategy': self._run_factor_regression(aligned_data, 'EW_Strategy', ['MKT', 'SMB', 'VMG']),
            'VW_Strategy': self._run_factor_regression(aligned_data, 'VW_Strategy', ['MKT', 'SMB', 'VMG'])
        })

        print("\n>>> CH-3 因子模型回归结果:")
        print(results.T.to_string())

        vw_t = results.loc['t-stat (Alpha)', 'VW_Strategy']
        if vw_t > 1.96:
            print(f"\n市值加权策略的 Alpha t值 ({vw_t:.2f}) 显著大于 1.96，说明策略战胜了 CH-3 模型！")
        else:
            print(f"\n市值加权策略的 Alpha t值 ({vw_t:.2f}) 未超过 1.96，超额收益统计上不显著。")

        return results

    def run_ch4_test(self, performance_df: pd.DataFrame) -> pd.DataFrame:
        """CH-4 因子模型回归检验."""
        print("=" * 50)
        print("CH-4 四因子模型回归检验")
        print("=" * 50)

        df_ch4 = pd.read_excel(self.data_config.ch4_factors_path)
        df_ch4 = df_ch4.rename(columns={
            'mnthdt': 'date',
            'mktrf': 'MKT',
            'SMB': 'SMB',
            'VMG': 'VMG',
            'PMO': 'PMO'
        })
        df_ch4['date'] = pd.to_datetime(df_ch4['date'].astype(str))
        df_ch4 = df_ch4.set_index('date')
        df_ch4.index = df_ch4.index + pd.offsets.MonthEnd(0)

        performance_df = performance_df.copy()
        performance_df.index = pd.to_datetime(performance_df.index) + pd.offsets.MonthEnd(0)

        aligned_data = pd.concat([performance_df, df_ch4], axis=1, join='inner')

        results = pd.DataFrame({
            'EW_Strategy': self._run_factor_regression(aligned_data, 'EW_Strategy', ['MKT', 'SMB', 'VMG', 'PMO']),
            'VW_Strategy': self._run_factor_regression(aligned_data, 'VW_Strategy', ['MKT', 'SMB', 'VMG', 'PMO'])
        })

        print("\n>>> CH-4 四因子回归结果:")
        print(results.T.to_string())

        return results

    def run_carhart_test(self, performance_df: pd.DataFrame) -> pd.DataFrame:
        """Carhart 四因子模型回归检验."""
        print("=" * 50)
        print("Carhart 四因子模型回归检验")
        print("=" * 50)

        df_carhart = pd.read_excel(self.data_config.carhart_factors_path)
        df_carhart = df_carhart.rename(columns={
            'TradingMonth': 'date',
            'RiskPremium1': 'MKT',
            'SMB1': 'SMB',
            'HML1': 'HML',
            'UMD1': 'UMD'
        })
        df_carhart['date'] = pd.to_datetime(df_carhart['date'])
        df_carhart = df_carhart.set_index('date')
        df_carhart.index = df_carhart.index + pd.offsets.MonthEnd(0)

        performance_df = performance_df.copy()
        performance_df.index = pd.to_datetime(performance_df.index) + pd.offsets.MonthEnd(0)

        aligned_data = pd.concat([performance_df, df_carhart], axis=1, join='inner')

        results = pd.DataFrame({
            'EW_Strategy': self._run_factor_regression(aligned_data, 'EW_Strategy', ['MKT', 'SMB', 'HML', 'UMD']),
            'VW_Strategy': self._run_factor_regression(aligned_data, 'VW_Strategy', ['MKT', 'SMB', 'HML', 'UMD'])
        })

        print("\n>>> Carhart 四因子回归结果:")
        print(results.T.to_string())

        return results

    @staticmethod
    def _run_factor_regression(aligned_data, y_col, x_cols):
        """运行因子回归."""
        data = aligned_data.dropna(subset=[y_col] + x_cols)

        Y = data[y_col]
        X = data[x_cols]
        X = sm.add_constant(X)

        model = sm.OLS(Y, X).fit()

        alpha = model.params['const']
        t_alpha = model.tvalues['const']

        result = {
            'Alpha (Monthly)': alpha,
            'Alpha (Annualized)': alpha * 12,
            't-stat (Alpha)': t_alpha,
            'R-squared': model.rsquared
        }

        for col in x_cols:
            result[f'Beta_{col}'] = model.params[col]

        return pd.Series(result)


class GRSEvaluator:
    """GRS 联合假设检验器."""

    @staticmethod
    def run_test(
        factor_df: pd.DataFrame,
        portfolio_df: pd.DataFrame,
        factor_cols: List[str] = None
    ) -> Tuple[float, float, np.ndarray]:
        """运行 GRS 检验."""
        from scipy.stats import f as f_dist

        print("=" * 50)
        print("GRS 联合检验")
        print("=" * 50)

        if factor_cols is None:
            factor_cols = ['MKT', 'SMB', 'VMG']

        # 数据对齐
        common_index = factor_df.index.intersection(portfolio_df.index)
        f_data = factor_df.loc[common_index, factor_cols]
        r_data = portfolio_df.loc[common_index]

        T, N = r_data.shape
        K = len(factor_cols)

        # 时间序列回归
        X = sm.add_constant(f_data)
        residuals = pd.DataFrame(index=common_index, columns=r_data.columns)
        alphas = np.zeros(N)

        for i, col in enumerate(r_data.columns):
            y = r_data[col]
            model = sm.OLS(y, X).fit()
            residuals[col] = model.resid
            alphas[i] = model.params['const']

        # 计算 GRS 统计量
        Sigma = residuals.cov()
        Omega = f_data.cov()
        mu_f = f_data.mean().values

        try:
            inv_Sigma = np.linalg.inv(Sigma)
            inv_Omega = np.linalg.inv(Omega)

            term1 = (T - N - K) / N
            term2 = 1 + mu_f.T @ inv_Omega @ mu_f
            term3 = alphas.T @ inv_Sigma @ alphas

            grs_stat = term1 * (term3 / term2)
            p_value = 1 - f_dist.cdf(grs_stat, N, T - N - K)

            print(f"GRS Statistic: {grs_stat:.4f}")
            print(f"P-Value:       {p_value:.4f}")

            if p_value < 0.05:
                print("\n[结论] P值 < 0.05，拒绝原假设。")
                print("CH-3 模型无法完全解释这 5 组投资组合的超额收益。")
            else:
                print("\n[结论] P值 >= 0.05，无法拒绝原假设。")

            return grs_stat, p_value, alphas

        except Exception as e:
            print(f"GRS 计算出错: {e}")
            return None, None, None
