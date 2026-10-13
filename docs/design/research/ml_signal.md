# ml_signal

> 域：research · 对应代码 research/ml_signal.py · 来源 §64（archive/OPEN_QUESTIONS.md）

## 职责与边界

ML/DL 信号治理：模型输出 → `MLSignalAdapter` → `AlphaSignal`（与多因子路径在 §63.5 汇聚）。八问收口：输出映射、特征管理、模型生命周期、实验管理、ensemble、衰减监控、算力、过拟合防控。

- 负责：输出形态映射、特征矩阵与 features_hash、模型版本与部署目录、experiments/ 实验记录、信号层 ensemble、衰减监控接入、过拟合三检。
- 不负责：MLFactor train/predict 调度形态（[factor_runtime.md](factor_runtime.md) §59）、合成与权重（[factor_combiner.md](factor_combiner.md) / [alpha_signal.md](alpha_signal.md)）、alphaType 的 ml_factor 注册（[alpha_type.md](alpha_type.md) §65）。
- 目标：把 alstm/ 下 35+ 实验脚本群收口成可治理体系；公司对应物 UModel 训练平台 + Alpha 入库 + Datamall 实验追踪，个人平台用 JSON 目录等价物。

## 接口签名

```python
class MLSignalAdapter:
    @staticmethod
    def from_scores(scores: pd.DataFrame) -> pd.DataFrame:
        """[date, ticker, score] → 截面 z-score → AlphaSignal.frame。期望收益输出同此路。"""
    @staticmethod
    def from_probabilities(probs: pd.DataFrame) -> pd.DataFrame:
        """probs: [date, ticker, prob] → logit → z-score → AlphaSignal。"""

def smooth_signal(frame: pd.DataFrame, halflife: int = 5) -> pd.DataFrame:
    """逐 ticker EWM 平滑。日频策略默认开 halflife=5；月频再平衡不开。"""

def build_feature_matrix(feature_specs: list[dict], start, end) -> pd.DataFrame:
    # spec: {"source": "factor"|"raw", "name"|"field": ..., "transform": "pct_change_20"|...}
    # source=factor → 读因子表；source=raw → transform_bars(field, transform, start, end)

# 特征版本化: features_hash = hash(feature_specs + 依赖数据 depends_hash)（§58/§62）
# model_id = {name}+v{major}.{minor}(训练日期)+train_end+features_hash

def ensemble_models(models: list, X, weights: list[float] | None = None) -> pd.DataFrame:
    """信号层加权平均，默认等权。signal_source.type="ensemble" 的实现（§65.1）。"""

def sanity_check_ml_model(test_metrics: dict, train_ic: float, baseline_sharpe: float) -> str:
    """过拟合三检 → "PASS" | "SUSPECT" | "REJECT"。"""
```

部署目录（§59 joblib 持久化）：

```text
data/models/{name}/v{major}_{minor}/
├── model.pkl            # joblib 序列化
├── config.json          # 超参
└── metadata.json        # model_id / train_end / features_hash / metrics
experiments/{exp_id}/    # metrics.json + config + 时间戳（不用 MLflow）
```

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 输出映射：截面分数默认 / 概率→logit→z-score / 期望收益同分数 | 分位分组、二值 | 分组是 to_weights 的事；二值丢强度 |
| 特征 = 因子值 + 原始量价，复用因子表 | 独立 feature_store | 因子 ⊂ 特征；单数据源 |
| 特征筛选人工 5–15 个 | PCA/Lasso 自动筛 | 解释性差 + 样本不足时选择不稳 |
| features_hash 进 model_id | 只用版本号 | 特征血缘可追溯（复用 depends_hash） |
| 训练窗口扩张窗口（起点固定，终点滚动） | 滚动 24 月 | A 股历史短，滚动样本不足 |
| 重训频率年度 + KS p<0.05 衰减触发 | 月频重训 | 月频重训过拟合噪声 |
| 严格时间切分 + Walk-Forward，禁 KFold | 随机 KFold | 时序泄漏（§16.4） |
| data/models/ 语义化版本 + joblib | 每次实验都算生产 | train 输出写 data/models/，predict 读 model_id |
| experiments/ JSON 目录 | MLflow / W&B | 外部服务依赖；35 个实验 JSON 足够 |
| ensemble 在信号层加权平均（复用 FactorCombiner），默认等权 | 模型层平均 logits/参数 | 模型结构对齐是深水区；信号层可解释 |
| 衰减监控复用 §62.4 三态（healthy/warning/exit） | 另起一套阈值 | ML 信号与因子同一阈值体系 |
| 本地 CPU / 单 GPU，不建训练基础设施 | 分布式训练 | 单人规模不需要 |
| 晋升线 test_ic>0.04 + test_ir>0.5 + test_sharpe>1.0 + 优于等权 | 只看 test IC | 教程 §7.4 铁律 |

过拟合三检阈值（sanity_check_ml_model）：

| 检查 | 判定 | 响应 |
|---|---|---|
| test IC < 0.5 × train IC | SUSPECT | 查泄漏 / 减特征 / 加正则 |
| 夏普 > 2.0 | SUSPECT | 查前视（shift(1)/成交价基准） |
| 夏普未超等权 10% | REJECT | 不晋升，退回等权 |
| 单特征重要性 > 50% | SUSPECT | 查目标泄漏 |
| 换手 > 200%/月 | SUSPECT | 信号噪声学了样本内抖动 |

## 实现要点

```python
# 训练侧（§59 train/predict 分离）
# train → data/models/{name}/v{x}_{y}/model.pkl + metadata.json
# predict → 读 model_id → 分数 → MLSignalAdapter → AlphaSignal

# 输出映射
frame = MLSignalAdapter.from_scores(scores)          # 或 from_probabilities(probs)
if frequency == "1d":
    frame = smooth_signal(frame, halflife=5)          # 月频不开

# 衰减（§64.7）：KS 分布漂移 p<0.05 → 提前重训
# warning → 重训 + ensemble 降权；exit → 停用回退等权组合（§63.2 主路径）

# ensemble（信号层）
combined = ensemble_models([m1, m2, m3], X, weights=None)   # 默认等权
# 权重来源: validation ICIR 或等权（默认等权，防权重优化过拟合）
# 对应 alstm/ hybrid.py / low_volatility_model_a_mixer.py 的收口方向

# 过拟合三检
verdict = sanity_check_ml_model(test_metrics, train_ic=..., baseline_sharpe=...)
# REJECT → 不进 data/models/；SUSPECT → 排查后再晋升

# 可复现五要素（metadata.json 必含）：
# random_seed / 依赖版本 / 特征列表 / 日期范围 / 超参
```

## 实施时定稿项

- transform_bars 的变换集合（pct_change_N 之外还有哪些）
- features_hash 具体算法（hash 组成与编码）
- KS 检验的参考窗口（历史多长算「分布漂移」）
- ensemble 权重公式（validation ICIR 归一化细节）；与 [factor_combiner.md](factor_combiner.md) 的 ensemble 权重进 combiner 问题同源
