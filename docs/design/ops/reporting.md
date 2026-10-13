# reporting

> 域：ops · 对应代码 ops/reporting.py · 来源 §67.3 / §67.4（archive/OPEN_QUESTIONS.md）

## 职责与边界

- 产出两类报告：① Markdown 日报 `data/ops/reports/daily_{date}.md`（指标表 + 异常列表 + nav 曲线 png）；② 归因报告 `data/ops/reports/attribution_{alpha_type_id}_{quarter}.md`。
- 归因回答"收益从哪来"：Alpha-Beta 单因子回归（V1）+ 因子贡献拆解（复用 §63.6 `factor_contribution()`）。
- 输入来自 [monitoring.md](monitoring.md) 的指标快照与 [alerting.md](alerting.md) 的当日告警。
- 不做：Web 看板（V2 Streamlit 需要时再加）、Brinson/Barra 机构级归因。

## 接口签名

```python
def write_daily_report(date: str, metrics: dict, alerts: list) -> str:
    """Markdown 日报落盘 data/ops/reports/daily_{date}.md，返回 path。"""

def alpha_beta_attribution(portfolio_returns: pd.Series,
                           benchmark_returns: pd.Series) -> dict:
    """单因子 Alpha-Beta 回归(教程 §18.6)。r_p = α + β·r_b + ε。
    返回 alpha_monthly / alpha_annual / beta / r_squared / t_alpha。"""
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Markdown 日报落盘 | Web 看板（V1） | 单人看 md 足够；V2 Streamlit 需要时再加 |
| Alpha-Beta 单因子回归 V1 | Brinson（配置+选择） | 需要基准组合与行业配置对照——机构组合管理需求 |
| FF3 备选（季度手动） | Barra 风险模型 | §63.4 同理由，不做 |
| 因子贡献拆解做（复用 §63.6） | 重新实现 | factor_contribution() 已有 |
| 归因月频自动 + FF3 季手动 + 专项按需 | 全自动全频 | 月频够用，大回撤事件才做专项复盘 |

## 实现要点

**daily_pipeline 尾部链路**（[scheduler.md](scheduler.md)）：17:05 `write_daily_report`；月末追加 `alpha_beta_attribution` + `factor_contribution`（自动 Job）。

```python
def write_daily_report(date, metrics, alerts):
    lines = [f"# Ops Daily Report {date}", "",
             "## 策略指标", "| 指标 | 值 | 状态 |", "|---|---|---|"]
    for k, v in metrics["strategy"].items():
        lines.append(f"| {k} | {v['value']} | {v['status']} |")
    lines += ["", "## 系统指标", ...]
    lines += ["", "## 告警", *[f"- [{a['level']}] {a['msg']}" for a in alerts]]
    path = f"data/ops/reports/daily_{date}.md"
    Path(path).write_text("\n".join(lines), encoding="utf-8")
    return path
```

```python
from scipy.stats import linregress


def alpha_beta_attribution(portfolio_returns, benchmark_returns):
    lr = linregress(benchmark_returns.values, portfolio_returns.values)
    return {
        "alpha_monthly": round(lr.intercept, 4),
        "alpha_annual": round(lr.intercept * 12, 4),   # 月频×12
        "beta": round(lr.slope, 3),
        "r_squared": round(lr.rvalue ** 2, 3),
        "t_alpha": round(lr.intercept / lr.stderr, 2) if lr.stderr else None,
    }
```

**归因频率**：

| 归因 | 频率 | 方式 |
|---|---|---|
| Alpha-Beta | 月频 | 自动（daily_pipeline 月末 Job） |
| 因子贡献拆解 | 月频 | 自动 |
| FF3 | 季度 | 手动跑 notebook |
| 专项归因 | 按需 | 手动（大回撤事件复盘） |

**归因报告解读三线**（教程 §18.6）：

```text
Alpha 年化 3-5%：优秀散户水平
R² > 0.7：beta 解释力强，alpha 可信；R² < 0.5：超额波动大，样本不足
Alpha > 8%：查回测问题（未来函数/费用遗漏）——太高不真实
```

验证：构造 `r_p = 0.01 + 0.8·r_b + noise` → α≈0.12 年化 / β≈0.8（容差 0.02）；跑完 pipeline 检查 `daily_{date}.md` 指标表/告警列表/链接齐全。

## 实施时定稿项

- 归因基准的选择（沪深 300 vs 中证 500 vs 组合基准配置在哪）（§67）
