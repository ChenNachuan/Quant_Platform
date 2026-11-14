# source_adapter

> 域：data · 对应代码 [data/source_adapter.py](../../../data/source_adapter.py)
## 职责与边界

数据入库链最上游抽象：从外部数据源拉取**原始** DataFrame。一源一子类，新增数据源 = 写一个 Adapter 子类 + 一个 Normalizer，DQC 与 asset 层零改动。

边界两个「没有」：

- **没有 `normalize()`**——格式转换归独立模块 `normalizers/{source}_normalizer.py`，Adapter 只管拉到 raw。
- **没有 `schedule()`**——调度是入库流水线的事，Adapter 是被调用方。

源特有的 API 约束（分页、连接池、登出释放连接槽）全部在子类内部消化，调用方不感知。

## 接口签名

```python
@dataclass
class FetchResult:
    df: pd.DataFrame         # 数据源原始列名/原始值
    source: str              # "akshare" / "tushare" / "amazingdata" / "qdc" / "straightflush"
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
    def close(self): ...                  # 有连接池/Session 的源覆盖（如 ad.logout() / session.close()）
```

- 增量 = **watermark 模式**：格式由子类定义，调用方只存取不解析，持久化在 `raw/{source}/.watermarks.json`。\n- 注册：[`data/adapters/__init__.py`](../../../data/adapters/__init__.py) 的 `REGISTRY` 字典 + `get_adapter(source)` 工厂（已注册 `akshare`、`tushare`、`amazingdata`、`qdc`、`straightflush`）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 4 方法接口 | 5+ 方法大接口 | 够用不多 |
| 不设 `normalize()` | Adapter 内做格式转换 | 职责分离，Normalizer 独立模块 |
| 不设 `schedule()` | Adapter 自调度 | 被调用方不决定调用时机 |
| watermark 增量，格式子类定义 | 平台统一增量协议 | 各源增量形态不同 |
| 限流/重试/超时在基类 | 调用方处理 | 重试 3 次指数退避、超时 120s |
| REGISTRY 字典注册 | entry_points 插件发现 | 新增源只需 REGISTRY 加一行 |

## 实现要点

详见[data/source_adapter.py](../../../data/source_adapter.py)

入库流水线中的位置：`get_adapter(source)` → `fetch()` → `save_raw` → `normalize` → `ingest_with_dqc` → `save_watermark` → `finally: adapter.close()`。

核心约定：
- 基类提供重试（3次指数退避）、超时（120s）、限流（`request_per_second`）三层装饰
- 拒收走 quarantine，连接释放在 `finally`
- `cleanup_raw`只删`.parquet`，不删`.watermarks.json`
