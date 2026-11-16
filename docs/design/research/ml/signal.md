# ml_signal

> 域：research · 对应代码 [research/ml/signal.py](../../../../research/ml/signal.py)

## 职责与边界

ML/DL 信号治理层：将模型预测输出经 `MLSignalAdapter` 映射为标准截面信号（对接 [alpha_signal.md](../signal/alpha_signal.md)），并收口特征版本化、模型部署目录、实验追踪、信号层 ensemble、分布衰减监控与过拟合三检。

- 负责：输出形态映射（分数 / 概率 / 期望收益）、特征矩阵构建与 `features_hash`、语义化模型目录与 `experiments/` 记录、信号层加权集成、分布漂移监控与过拟合校验。
- 不负责：`MLFactor` 在 DAG 中的调度隔离（[runtime.md](../factor/runtime.md)）、多因子线性合成（[combiner.md](../signal/combiner.md)）、`alphaType` 配方注册与发布（[alpha_type.md](../signal/alpha_type.md)）。

## 接口签名

```python
class MLSignalAdapter:
    @staticmethod
    def from_scores(scores: pd.DataFrame) -> pd.DataFrame: ...
        # [date, ticker, score] → 截面 z-score（期望收益输出同此路径）
    @staticmethod
    def from_probabilities(probs: pd.DataFrame) -> pd.DataFrame: ...
        # [date, ticker, prob] → logit → 截面 z-score

def smooth_signal(frame: pd.DataFrame, halflife: int = 5) -> pd.DataFrame: ...
    # 逐 ticker EWM 平滑（日频可选开启；月频再平衡不启用）
def transform_bars(bars: pd.DataFrame, field: str, transform: str) -> pd.DataFrame: ...
def build_feature_matrix(specs: list[dict], ...) -> pd.DataFrame: ...
    # spec: {"source": "factor"|"raw", "name"|"field": ..., "transform": ...}
def features_hash(specs: list[dict], dep_hashes: list[str]) -> str: ...
def model_id(name: str, major: int, minor: int, train_date: str, train_end: str, feat_hash: str) -> str: ...
def write_model(name: str, major: int, minor: int, model, config: dict, metadata: dict, ...) -> Path: ...
def write_experiment(exp_id: str, metrics: dict, config: dict, ...) -> Path: ...
def ensemble_models(models: list, X, weights: list[float] | None = None) -> pd.DataFrame: ...
    # 信号层加权平均，默认等权
def sanity_check_ml_model(test_metrics: dict, train_ic: float, baseline_sharpe: float) -> str: ...
    # 过拟合三检 → "PASS" | "SUSPECT" | "REJECT"
```

模型部署与实验记录目录：

```text
data/models/{name}/v{major}_{minor}/
├── model.pkl            # joblib 持久化模型
├── config.json          # 模型超参
└── metadata.json        # model_id / train_end / features_hash / random_seed / metrics
experiments/{exp_id}/    # metrics.json + config + 时间戳（本地 JSON 追踪，不依赖 MLflow）
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 输出映射：连续分数默认；概率经 logit 再截面 z-score | 直接分位分组 / 二值化 | 分组交下游 `to_weights`，连续信号保留相对排序强度 |
| 特征复用 `factors/` 因子表 + 原始量价派生 | 独立 `feature_store` | 因子即标准化特征，维持单一数据源与血缘一致 |
| 人工精选 5–15 个强逻辑特征 | PCA / Lasso 全自动筛选 | 金融样本信噪比低，高维自动筛选极易过拟合且难归因 |
| `features_hash` 写入 `model_id` 与 `metadata.json` | 仅记录版本号 | 上游因子或特征规格变动时可精确追溯模型血缘 |
| 扩张窗口训练（起点固定、终点滚动） | 固定短滚动窗口（如 24 月） | A 股有效历史长度有限，扩张窗口减少参数方差 |
| 年度重训 + KS 检验 `p < 0.05` 触发提前重训 | 高频月度重训 | 频繁重训容易拟合近期市场噪声 |
| 严格时间切分 + Walk-Forward 验证，禁止随机 KFold | 随机 KFold 交叉验证 | 时序数据随机切分存在严重未来信息泄漏 |
| 实验全落 `experiments/`；达标模型才写入 `data/models/` | 每次实验均视为生产模型 | 隔离探索性实验与可部署模型资产 |
| ensemble 在信号层加权平均（默认等权） | 模型层平均参数 / logits | 异构模型结构差异大，信号层集成可解释且可对接 `FactorCombiner` |
| 衰减监控复用三态门槛（`healthy` / `warning` / `exit`） | 另建独立阈值体系 | 与传统因子保持统一的准入（`test_ic > 0.04, ir > 0.5, sharpe > 1.0`）与退出标准 |
| 本地 CPU / 单 GPU 训练 | 分布式训练平台 | 单机规模下模型训练非架构瓶颈 |

过拟合检查规则（`sanity_check_ml_model`）：

| 检查项 | 判定 | 处置建议 |
|---|---|---|
| `test_ic < 0.5 × train_ic` | `SUSPECT` | 排查特征泄漏、精简特征或增强正则 |
| `sharpe > 2.0` | `SUSPECT` | 复核 `shift(1)` 与成交价基准，排除前视偏差 |
| 未超越等权基准 10% | `REJECT` | 不予晋升，退回等权基线组合 |
| 单特征重要性 `> 50%` | `SUSPECT` | 检查目标变量泄漏 |
| 月均换手 `> 200%` | `SUSPECT` | 模型过度学习样本内高频抖动噪声 |

## 实现要点

详见 [research/ml/signal.py](../../../../research/ml/signal.py)。

核心约定：
- 特征矩阵通过 `build_feature_matrix` 统一从因子存储（`load_factor`）与量价变换（`transform_bars`）装配，再经 `features_hash` 绑定至 `model_id`。
- `metadata.json` 必须记录可复现五要素：`random_seed`、依赖数据版本/指纹、特征列表、训练/验证日期范围与完整超参。
- 衰减退出（`exit`）时停用该 ML 模型并自动回退等权多因子基线组合。
