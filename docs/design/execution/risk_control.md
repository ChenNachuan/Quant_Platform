# risk_control

> 域：execution · 对应代码 execution/risk_control.py · 来源 §66.3（archive/OPEN_QUESTIONS.md）

## 职责与边界

下单前风控（Themis 等价物的个人简化版）：逐规则检查订单列表，返回 PASS/BLOCK 两级结果。目标是防「策略失控 / 操作错误」，**不是防市场风险**（那是策略层的事）。只做 pre-trade，不做盘中 intra-trade 监控。被拦截必须可追溯（审计落盘）。

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
    """append-only 写 data/execution/risk_audit.jsonl（时间戳/规则/标的/动作/原因）。"""
```

**风控七规则表**：

| # | 规则 | 阈值 | 动作 |
|---|---|---|---|
| 1 | 单标的仓位 | 成交后权重 ≤ 25% | BLOCK |
| 2 | 总仓位 | ≤ 95%（留 5% 现金） | BLOCK |
| 3 | 单日换手 | ≤ 150% | 告警（不拦——硬拦破坏信号） |
| 4 | 单笔金额 | ≥ 5000 元 | SKIP（低于跳过该单） |
| 5 | 禁做空 | 卖出 ≤ 持仓量 | BLOCK |
| 6 | 禁 ST | 标的池已排除（research filter） | BLOCK |
| 7 | 流动性 | 单笔股数 > 1% 日成交量（股） | 告警（同拆单告警） |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 只 pre-trade | intra-trade 盘中监控 | 月频无盘中需求 |
| 只 PASS/BLOCK 两级 | 公司 Themis 三态（放行/约束/拦截） | 「约束」态是多策略冲突仲裁需求 |
| 换手/流动性超限告警不拦截 | 硬拦截 | 硬拦破坏信号 |
| 单笔金额过小 SKIP 该单 | 拒整批 | 不是错误，是过滤 |
| 阈值参数来自配置/ASSET_RULES | 散落硬编码 | §69.5 修订：涨跌停/做空规则参数源 = ASSET_RULES |
| 单标的仓位按**成交后权重**（现持仓+本单）| 只查本单 value/nav | 卖出/加仓场景单笔口径失真 |
| 审计 risk_audit.jsonl append-only | 只打日志 | 拦截必须可追溯 |

## 实现要点

```python
def pre_trade_risk_check(orders, account, nav, config):
    blocked, warnings = [], []
    for o in orders:
        px = o["limit_price"]
        value = o["shares"] * px
        held = account.holdings.get(o["ticker"], 0) * px         # V1 用 limit_price 估值现持仓
        post_value = held + (value if o["side"] == "BUY" else -value)
        if post_value / nav > config.get("max_single_weight", 0.25):
            blocked.append(f"single_weight:{o['ticker']}")       # 成交后权重（现持仓+本单）
        if o["side"] == "SELL" and o["shares"] > account.holdings.get(o["ticker"], 0):
            blocked.append(f"short_forbidden:{o['ticker']}")     # 禁做空
        if o["side"] == "BUY" and value < config.get("min_trade_value", 5000):
            o["skip"] = True                                     # 金额过小跳过
        if o["shares"] > 0.01 * get_daily_volume(o["ticker"]):
            warnings.append(f"liquidity:{o['ticker']}")          # 流动性告警：股数比，量纲一致
    return RiskCheckResult(not blocked, blocked, warnings)
```

## 实施时定稿项

- 总仓位/单日换手两条规则的检查时机（逐单累加 vs 批级汇总）与换手率口径（§66.3 骨架未展开）
- `get_daily_volume` 数据来源（与 order_generation 的成交量来源同源，§66）
- 涨跌停拒单的归属（源内张力照录）：§69.5 差异落层表把「涨跌停/禁做空」落 pre_trade_risk_check、§18.3 六道关② 涨跌停封板拒、§24.6/§24.7 pre_check（`strict_limit` 配置：回测默认拒/实盘可提交）都含涨跌停拒单；但 §66.3 风控七规则表未列该条（七条 = 单标的仓位/总仓位/单日换手/单笔金额/禁做空/禁 ST/流动性）。实施时定稿：七规则外另置涨跌停检查，还是并入 pre_check 序列（§66.3/§69.5/§18.3/§24）
