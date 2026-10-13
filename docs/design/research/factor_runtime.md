# factor_runtime

> 域：research · 对应代码 research/factor_runtime.py · 来源 §58 / §59（archive/OPEN_QUESTIONS.md）

## 职责与边界

FactorDAG 的构建与执行：依赖解析、拓扑排序、缓存、增量更新、层内并行、失败处理、ML 因子调度。消费 [factor_interface.md](factor_interface.md) 的 `depends_on()`，产出的 DataFrame 交 [factor_storage.md](factor_storage.md) 落盘、交 [post_processor.md](post_processor.md) 后处理。

不做：按日期切分并行、per-di 精细增量、状态持久化（5 色灯）、外部调度框架、AST 静态分析。

## 接口签名

```python
class FactorDAG:
    def register(self, factor: Factor) -> None: ...
        # 解析 depends_on() 的 "factor:" 前缀建图
    def topological_sort(self) -> list[str]: ...
        # Kahn 算法；环 = 排序长度 ≠ 节点数 → ValueError
    def topological_layers(self) -> list[list[str]]: ...
        # 分层版（与 topological_sort 同源），每层内因子互不依赖可并行

def compute_depends_hash(factor: Factor, data: dict[str, pd.DataFrame]) -> str: ...
    # 依赖指纹：各依赖的 行数 + max(date) 拼接 md5 前 12 位
def compute_factor(dag, factor_name, start, end, force=False) -> pd.DataFrame: ...
    # 单因子：缓存命中 → append / miss 或 force → full
def compute_all(dag, start, end, max_workers=4, force=False) -> dict[str, pd.DataFrame]: ...
    # 拓扑分层 + 层内 ProcessPoolExecutor 并行；ML 因子串行隔离
def compute_all_safe(...) -> tuple[dict[str, pd.DataFrame], dict[str, Exception]]: ...
    # 容错版：失败不阻塞独立因子，返回 (results, errors)
```

缓存 key = `(factor_name, version, date_range, depends_hash)`，目录见 [factor_storage.md](factor_storage.md)。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 声明式 `depends_on()` + Kahn 拓扑 | AST 解析 compute() 静态分析 | 成本高、脆弱、不可靠；公司也是显式声明 |
| 缓存按因子 × 日期范围 | 按单日/单 ticker 细粒度缓存 | 全量计算 <1s，细粒度管理成本大于收益 |
| 增量 append / full 两模式 | per-di 精细增量 | 全量重算 128 万行 <5 秒，per-di 复杂度远超收益 |
| version 变 / 上游修正 → 一律 full | 增量与全量混写 | 保守策略，避免新旧口径不一致 |
| 层内 ProcessPoolExecutor 并行 | 按日期切分多进程/多 pod | pandas 向量化处理全部日期更高效；日期切分是 tick 级数据量驱动 |
| 失败 fail-fast + 下游跳过 + 同层继续 | 引入 5 色灯状态持久化 | 离线批量跑完即结束，失败修复后全量重算即可 |
| ML 因子 MLFactor 子类：train/predict 分离 + 模型持久化 + 串行执行 | ML 因子进并行池 | 训练耗时长、内存大、需防 look-ahead |
| concurrent.futures | Celery / Airflow / Prefect | 单机场景足够，外部调度框架是分布式需求 |
| 因子侧 version 参与缓存 key | 完全不版本化 | compute 逻辑变更必须失效缓存（数据不版本化、因子逻辑版本化） |

## 实现要点

```python
# —— 缓存与增量（compute_factor 核心）——
cached = load_cache(cache_path, start, end)
if cached is not None and not force:
    if cached["date"].max() >= end:
        return cached                                   # 完全命中
    new = _do_compute(factor, dag, next_trading_day(cached["date"].max()), end)
    result = pd.concat([cached, new]).drop_duplicates(["date", "ticker"])
else:
    result = _do_compute(factor, dag, start, end)        # full 模式
save_cache(cache_path, result)

# —— 层内并行 + ML 隔离（compute_all 核心）——
for layer in dag.topological_layers():
    ml = [n for n in layer if isinstance(dag.factors[n], MLFactor)]
    normal = [n for n in layer if n not in ml]
    with ProcessPoolExecutor(max_workers=min(max_workers, len(normal))) as pool:
        futures = {pool.submit(compute_factor, dag, n, start, end, force): n for n in normal}
        for f in as_completed(futures):
            results[futures[f]] = f.result()
    for n in ml:                                         # ML 串行，避免内存爆炸
        results[n] = compute_factor(dag, n, start, end)

# —— 失败处理（compute_all_safe）——
# 上游在 failed_set → 本节点跳过并记 RuntimeError；同层独立因子继续

# —— MLFactor（继承 Factor ABC）——
class MLFactor(Factor):
    model_path: str = ""
    retrain_freq: str = "monthly"     # monthly / quarterly / yearly
    def train(self, data, train_end) -> Any: ...   # train_end 严格切分防 look-ahead
    def predict(self, model, data) -> pd.DataFrame: ...
    def compute(self, data):
        model = self._load_or_train(data)          # joblib 持久化
        return self.predict(model, data)
```

整体流程：`compute_all` → `topological_layers()` → 逐层（缓存检查 → miss 计算 → 存缓存）→ ML 层串行 → 返回 `{factor_name: DataFrame}`。新增代码 ≈235 行（拓扑 ~40 + 指纹 ~15 + compute_factor ~40 + compute_all/分层/容错 + MLFactor）。

## 实施时定稿项

- `_do_compute` 依赖装配细节（data dict 怎么从 load_bars + 上游因子缓存拼出来，预计 <20 行）（§58/§59）
- MLFactor 的 `retrain_freq` 触发机制（每次全量跑时检查时间戳？）（§59）
- ML 因子的 timeout 具体值与超时后行为（§59）
