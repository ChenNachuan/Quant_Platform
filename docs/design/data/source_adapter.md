# source_adapter

> 域：data · 对应代码 data/source_adapter.py · 来源 §40（archive/OPEN_QUESTIONS.md）

## 职责与边界

数据入库链最上游抽象：从外部数据源拉取**原始** DataFrame。一源一子类，新增数据源 = 写一个 Adapter 子类 + 一个 Normalizer（~180–220 行），DQC 与 asset 层零改动。

边界两个「没有」：

- **没有 `normalize()`**——格式转换归独立模块 `normalizers/{source}_normalizer.py`（§39.9 由此闭合），Adapter 只管拉到 raw。
- **没有 `schedule()`**——调度是入库流水线的事，Adapter 是被调用方，不决定何时调用自己。

源特有的 API 约束（分页、连接池、`local_path` 必须字符串、登出释放连接槽）全部在子类 `fetch()` / `close()` 内部消化，调用方不感知。

## 接口签名

```python
@dataclass
class FetchResult:
    df: pd.DataFrame         # 数据源原始列名/原始值
    source: str              # "akshare" / "tushare" / "amazingdata"
    asset_type: str          # 传入值原样返回
    trade_date: date         # 业务日期（非下载日期）
    row_count: int           # len(df)，冗余但方便日志
    watermark: str | None    # 增量标记；不支持增量的源返回 None

class SourceAdapter(ABC):
    requests_per_second: float = 5.0   # 子类可覆盖

    @abstractmethod
    def fetch(self, asset_type: str, trade_date: date,
              watermark: str | None = None) -> FetchResult: ...
    @abstractmethod
    def list_asset_types(self) -> list[str]: ...
    def health_check(self) -> bool: ...   # 默认：拉一天看能否成功
    def close(self): ...                  # 有连接池的源覆盖（如 ad.logout()）
```

- `fetch()` 输入：`asset_type`（由 `list_asset_types()` 声明）+ `trade_date` + 可选 `watermark`（上次返回值）。
- 增量表达 = **watermark 模式**：格式由子类定义（时间戳/offset/页码），调用方只存取不解析，持久化在 `raw/{source}/.watermarks.json`。
- 注册：`adapters/__init__.py` 的 `REGISTRY` 字典 + `get_adapter(source)` 工厂。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 4 方法接口（fetch/list_asset_types/health_check/close） | 5+ 方法大接口 | 够用不多 |
| 不设 `normalize()` | Adapter 内做格式转换 | 职责分离，Normalizer 独立模块 |
| 不设 `schedule()` | Adapter 自调度 | 被调用方不决定调用时机 |
| watermark 增量，格式子类定义 | 平台统一增量协议 | 各源增量形态不同（时间戳/offset/页码） |
| 限流/重试/超时三层：基类装饰器 + 子类 QPS | 调用方处理 | 调用方只见成功/失败；重试 3 次指数退避、超时 120s |
| API 限制在子类内部屏蔽 | 平台层适配 | 调用方对分页/连接数零感知 |
| REGISTRY 字典注册 | entry_points 插件发现——**不作否决**，是否需要仍是 §40 未决项 | 新增源只需 REGISTRY 加一行（§40） |

## 实现要点

```python
# 基类装饰器（__init_subclass__ / mixin 注入）
RETRY_CONFIG = dict(max_retries=3, backoff_base=2.0,
                    retry_on=(ConnectionError, TimeoutError, HTTPError))
FETCH_TIMEOUT = 120

def _do_fetch(self, asset_type, trade_date, watermark):
    @retry(**RETRY_CONFIG); @timeout(FETCH_TIMEOUT); @rate_limit(self.requests_per_second)
    def _inner(): return self.fetch(asset_type, trade_date, watermark)
    return _inner()
```

入库流水线中的位置（§40.9）：`get_adapter(source)` → `fetch()` → `save_raw` → `normalize` → `ingest_with_dqc` → `save_watermark` → `finally: adapter.close()`。拒收走 quarantine（`e.detail`），连接释放在 `finally`。

AmazingData 子类示范：`requests_per_second = 2.0`（连接数有限）；按真实 API 形态分批循环（每批 10 代码 / 单次 ~100 条上限）；`local_path` 强制 `str()`；`close()` 落地为 `ad.logout()`。一次性代码量 ≈420 行（基类 ~100 + 3 子类 ~300 + 注册 ~20）。

## 实施时定稿项

- 重试/超时/限流装饰器选型：`tenacity`/`backoff` 库 vs 手写 ~30 行（§40）
- `health_check()` 默认实现里 `_latest_trade_date()` 的来源（交易日历表 vs 墙钟）（§40）
- watermark 写入失败（step 5）时是否回滚 asset 层（幂等重跑语义）（§40）
- `FetchResult` 是否加 `fetched_at`/`elapsed` 元字段（§40）
- AKShare/Tushare 子类的具体 API 映射（分页/增量形态）（§40）
- `close()` 与 joblib 并行回填下的连接池共享问题（§40）
- `cleanup_raw` 只删 `.parquet`，勿误删 `.watermarks.json`（§40 修订表注记）
