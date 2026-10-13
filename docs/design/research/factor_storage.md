# factor_storage

> 域：research · 对应代码 research/factor_storage.py · 来源 §61（archive/OPEN_QUESTIONS.md）

## 职责与边界

因子值的物理落盘布局：每因子一目录、语义化版本目录、按年 Parquet、metadata.json 元数据、append/full 两种写入、DuckDB 查询与 `load_factor_matrix()` 逻辑宽表。存 [factor_interface.md](factor_interface.md) 的 `compute()` 原始输出（§60 存原始值决策，NaN 保留不填）。

- 输入：`compute()` / [factor_runtime.md](factor_runtime.md) 产出的 `DataFrame[date, ticker, {factor_name}]`
- 输出：`data_platform/factors/{name}/v{version}/{year}.parquet + metadata.json`；ML 因子另有 `model/model.joblib`（随因子 version 走）
- 不属于本模块：行情 bars（data 域）、回测产物 `outputs/`、后处理（消费时做，[post_processor.md](post_processor.md)）

## 接口签名

```python
def save_factor_append(factor, result: pd.DataFrame,
                       base_dir: str = "data_platform/factors") -> None: ...
    # 日常增量：按 date 前 4 位拆年，当年文件 concat + drop_duplicates([date,ticker]) 再写
def save_factor_full(factor, result, base_dir="data_platform/factors") -> None: ...
    # 全量：覆盖当前版本目录（或写新版本目录），按年落盘
def load_factor(name, start, end, version="1.0",
                base_dir="data_platform/factors") -> pd.DataFrame: ...
    # 读年文件并按 [start, end] 裁剪 date 列（YYYYMMDD 字符串比较即时间序）
def load_factor_matrix(names: list[str], start, end, version="1.0",
                       base_dir="data_platform/factors") -> pd.DataFrame: ...
    # 多因子 outer join 拼宽表 (date, ticker, f1, f2, ...)，对标公司 GetViewer
def write_metadata(out_dir, factor, result) -> None: ...
```

目录与 schema：

```text
data_platform/factors/{factor_name}/
├── v1.0/               ← 语义化版本目录，多版本共存不覆盖
│   ├── 2020.parquet … 2024.parquet   ← 按年分区
│   └── metadata.json
├── v2.0/ …
└── model/model.joblib  ← ML 因子专属
```

| 列名 | 类型 | 说明 |
|---|---|---|
| date | VARCHAR `YYYYMMDD` | §5.2 全库统一；字典序=时间序 |
| ticker | VARCHAR `000001.SZ` | 带交易所后缀，与 bars 可直接 JOIN |
| {factor_name} | DOUBLE | 原始值（§60）；NaN=停牌/缺失，保留不填 |

metadata.json：factor_name/version/description/category/frequency/asset_types + created_at/updated_at + depends_on/depends_hash + date_range/row_count/file_count + compression。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| Direction A 每因子一表（独立目录） | 长表 EAV / 物理宽表 / 三维张量 | 对公司 Unpacked（field_xxx.ucube）本质的显式化；增删改互不影响、版本化清晰 |
| 不用长表 EAV（date,ticker,factor_name,value） | — | 2500 天×5000 股×50 因子=6.25 亿行；value 列混杂，列裁剪失效 |
| 不用物理宽表 | — | Parquet 加列=重写；单因子升版动整表；联合查询在消费端 merge 即可（50 因子全历史 <2 秒） |
| 不用三维张量（HDF5/zarr） | — | 违反「只用 Parquet+DuckDB」；ML 训练在消费端拼 numpy |
| 按年分区（~125 万行/文件） | 按天/按月/按资产 | 与 compute() 整年粒度对齐；2500 小文件 / 1MB 太小 / 仅 A 股单市场 |
| 数值精度 DOUBLE | float32 | §5.2 精度事故——float32 在下游中性化 OLS 中被放大；存储成本差异可忽略 |
| 列名用因子名 | 固定 `value` 列 | 读入直接可读；合并宽表无需 rename |
| 语义化版本 v1.0/v2.0 | random_string_timestamp | 与 Factor.version 类属性对应、git 可追溯；因子版本少而离散 |
| 升版=compute/depends_on/参数变更；数据更新不升 | — | 一个版本目录内历史值定义一致，不允许新旧算法混写 |
| 历史重算=新版本目录全量重算，旧版保留不自动删 | 原地覆盖 | 对比 IC / 回测复现 / 灰度切换三场景；旧版总量 ~5GB 可忽略 |
| append 日常增量 + 触发式 full | 每次全量 | 对标 DPP 增量生产；version 变/force/depends_hash 变一律 full 覆盖 |
| Parquet 文件 + DuckDB 查询 | ClickHouse | 公司 CK 是实盘多策略并发查询需求；个人消费方=本地批量脚本，零运维 |
| 元数据 metadata.json | SchemaBase / 5 色灯状态 | 身份+血缘+覆盖范围足够；状态传导由 depends_hash 触发重算替代 |

## 实现要点

```python
def save_factor_append(factor, result, base_dir="data_platform/factors"):
    out_dir = Path(base_dir) / factor.name / f"v{factor.version}"
    out_dir.mkdir(parents=True, exist_ok=True)
    result = result.copy()
    result["_year"] = result["date"].astype(str).str[:4]   # YYYYMMDD 前 4 位
    for year, chunk in result.groupby("_year"):
        path = out_dir / f"{year}.parquet"
        chunk = chunk.drop(columns="_year")
        if path.exists():
            chunk = pd.concat([pd.read_parquet(path), chunk], ignore_index=True)
        chunk = (chunk.drop_duplicates(["date", "ticker"])
                      .sort_values(["date", "ticker"]).reset_index(drop=True))
        chunk.to_parquet(path, compression="zstd", compression_level=3, index=False)
    write_metadata(out_dir, factor, result)

def save_factor_full(factor, result, base_dir="data_platform/factors"):
    # 同上但不 concat 旧文件，直接覆盖各年 parquet；写 metadata.json

def load_factor_matrix(names, start, end, version="1.0", base_dir="data_platform/factors"):
    frames = [load_factor(n, start, end, version, base_dir).set_index(["date", "ticker"])
              for n in names]
    wide = pd.concat(frames, axis=1, join="outer").reset_index()
    return wide.sort_values(["date", "ticker"]).reset_index(drop=True)
```

- 写入模式触发表：version 变 → full 写新版本目录；`force=True` → full 覆盖当前版；depends_hash 变（上游修正）→ full 覆盖（保守不混写）；新交易日 → append
- 月度一致性校验：full 重算与 append 目录 diff（行数一致、值一致容差 1e-12），不一致以全量为准覆盖
- DuckDB 查询：`read_parquet('data_platform/factors/{name}/v1.0/2024.parquet')` / glob `*.parquet` / 多因子 JOIN 或直接 `load_factor_matrix()`
- 三目录划分：`data_platform/asset/`（行情）/ `data_platform/factors/`（本节）/ `outputs/`（回测产物，§54.9/§55）
- 与 §58 缓存衔接：缓存目录草图 `{range}.parquet` 细化为 `{year}.parquet`；缓存 key 的 version/depends_hash 由 metadata.json 镜像。新增代码 ≈65 行

## 实施时定稿项

- `load_factor()` 入参签名（start/end 与年文件的裁剪逻辑，预计 <15 行）（§61）
- `write_metadata()` 实现细节（~15 行，字段已在本模块 schema 定死）（§61）
- 旧版本目录手动清理规则（谁判断「无回测引用」——回测配置快照记录 version 的话可自动判断）（§61）
- 行情 bars 是否也用 metadata.json（本节只覆盖因子表）（§61）
