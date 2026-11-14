# reporting

> 域：ops · 对应代码 [ops/reporting.py](../../../ops/reporting.py)
## 职责与边界

产出两类报告：
- Markdown 日报 `logs/ops/reports/daily_{date}.md`（指标表 + 异常列表 + nav 曲线 png）；
- 归因报告 `logs/ops/reports/attribution_{alpha_type_id}_{quarter}.md`。

输入来自 [monitoring.md](monitoring.md) 的指标快照与 [alerting.md](alerting.md) 的当日告警。


## 接口签名

```python
def write_daily_report(date: str, metrics: dict, alerts: list) -> str:
    """Markdown 日报落盘 logs/ops/reports/daily_{date}.md，返回 path。"""

def alpha_beta_attribution(portfolio_returns: pd.Series,
                           benchmark_returns: pd.Series) -> dict:
    """单因子 Alpha-Beta 回归。r_p = α + β·r_b + ε。
    返回 alpha_monthly / alpha_annual / beta / r_squared / t_alpha。"""
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Markdown 日报落盘 | Web 看板 | 单人看 md 足够；V2 Streamlit 需要时再加 |
| Alpha-Beta 单因子回归 V1 | Brinson | 需要基准组合与行业配置对照——机构组合管理需求 |
| 因子贡献拆解做 | 重新实现 | factor_contribution() 已有 |
| 归因月频自动 + FF3 季手动 + 专项按需 | 全自动全频 | 月频够用，大回撤事件才做专项复盘 |

**归因频率**：

| 归因 | 频率 | 方式 |
|---|---|---|
| Alpha-Beta | 月频 | 自动（daily_pipeline 月末 Job） |
| 因子贡献拆解 | 月频 | 自动 |
| FF3 | 季度 | 手动跑 notebook |
| 专项归因 | 按需 | 手动（大回撤事件复盘） |

## 实现要点

详见  [ops/reporting.py](../../../ops/reporting.py)

核心约定:
- 日报在daily_pipeline尾部生成(scheduler.md),包含策略指标表+系统指标表+当日告警列表
- Alpha-Beta回归: r_p = α + B.r_b + ε, scipy linregress单因子回归
- 归因解读参考线:alpha年化 3-5%优秀;R2>0.7beta解释力强;alpha>8%查回测问题
- 日报权重口径0.19 (SLEEVE/5,留 5%现金),与黄金样本夹具的 0.20(满仓等权)区分
