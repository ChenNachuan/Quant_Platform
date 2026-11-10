# risk_control

> 域：execution · 对应代码 [execution/risk_control.py](execution/risk_control.py)
## 职责与边界

下单前风控：逐规则检查订单列表，返回 PASS/BLOCK 两级结果。目标是防「策略失控 / 操作错误」，**不是防市场风险**。只做 pre-trade，不做盘中 intra-trade 监控。

## 接口签名

```python
@dataclass
class RiskCheckResult:
    passed: bool
    blocked_by: list[str]   # 被哪条规则拦的（"rule:ticker"）
    warnings: list[str]
    def is_blocked(self, ticker: str) -> bool:
        # blocked_by 元素形如 "rule:ticker"，broker 判单按 ticker 精确匹配后缀
        return any(b == ticker or b.split(":", 1)[-1] == ticker for b in self.blocked_by)

def pre_trade_risk_check(
    orders: list[dict],
    account: "SimAccount",
    nav: float,
    config: dict,
) -> RiskCheckResult:
    """逐规则检查，一 BLOCK 即整批拒；金额过小单独 SKIP 不拒整批。"""

def log_risk_audit(check: RiskCheckResult) -> None:
    """append-only 写 logs/audit/risk_audit.jsonl（时间戳/规则/标的/动作/原因）。"""
```

**风控七规则表**：

| # | 规则 | 阈值 | 动作 |
|---|---|---|---|
| 1 | 单标的仓位 | 成交后权重 ≤ 25% | BLOCK |
| 2 | 总仓位 | ≤ 95%（留 5% 现金） | BLOCK |
| 3 | 单日换手 | ≤ 150% | 告警（不拦——硬拦破坏信号） |
| 4 | 单笔金额 | ≥ 5000 元 | SKIP（跳过该单） |
| 5 | 禁做空 | 卖出 ≤ 持仓量 | BLOCK |
| 6 | 禁 ST | 标的池已排除 | BLOCK |
| 7 | 流动性 | 单笔股数 > 1% 日成交量（股） | 告警（同拆单告警） |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 只 pre-trade | intra-trade 盘中监控 | 月频无盘中需求 |
| 只 PASS/BLOCK 两级 | 公司 Themis 三态（放行/约束/拦截） | 「约束」态是多策略冲突仲裁需求 |
| 换手/流动性超限告警不拦截 | 硬拦截 | 硬拦破坏信号 |
| 单笔金额过小 SKIP 该单 | 拒整批 | 不是错误，是过滤 |
| 阈值参数来自配置/ASSET_RULES | 散落硬编码 | 涨跌停/做空规则参数源 = ASSET_RULES |
| 单标的仓位按**成交后权重**（现持仓+本单）| 只查本单 value/nav | 卖出/加仓场景单笔口径失真 |
| 审计 risk_audit.jsonl append-only | 只打日志 | 拦截必须可追溯 |

## 实现要点

详见 [execution/risk_control.py](execution/risk_control.py)

核心约定:
- 逐规则检查,一BLOCK即标记该ticker;金额过小SKIP不拒整批
- 单标的仓位用成交后权重:(现持仓市值+本单市值)/nav
- 流动性检查用股数比:order_shares/daily_volume,数据从execution/market_data.py获取
- 审计落盘到 logs/audit/risk_audit.jsonl(时间戳/规则/标的/动作/原因),append-only
- broker 调用 check.is_blocked(ticker)按 ticker精确匹配,决定逐笔FILLED或REJECTED
