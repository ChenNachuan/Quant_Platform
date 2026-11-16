# factor_storage

> 域：research · 对应代码 [research/factor/storage.py](../../../../research/factor/storage.py)

## 职责与边界

因子物理存储层：按因子与语义版本号分目录、按年份分 Parquet 持久化原始因子值，维护 `metadata.json` 元数据，提供 `append` / `full` 两种写入、范围读取及 `load_factor_matrix()` 宽表拼接。仅持久化 [interface.md](interface.md) 中 `compute()` 产出的**原始值**（保留停牌/缺失 `NaN`）。

- 输入：`compute()` / [runtime.md](runtime.md) 产出的 `DataFrame[date, ticker, {factor_name}]`。
- 输出：`$QUANT_DATA_ROOT/factors/{factor_name}/v{version}/{year}.parquet` 与 `metadata.json`。
- 不属于本模块：行情 bars（data 域）、回测产物 `outputs/`、消费侧后处理（[post_processor.md](../post_processor.md)）。

## 接口签名

```python
DEFAULT_BASE_DIR: str = str(DATA_ROOT / "factors")
COMPRESSION: str = "zstd"
COMPRESSION_LEVEL: int = 3

def save_factor_append(factor, result: pd.DataFrame, base_dir: str = DEFAULT_BASE_DIR) -> None: ...
    # 日常增量：按年份拆分，与盘上同年文件合并后按 (date, ticker) 去重保留已有旧值
def save_factor_full(factor, result: pd.DataFrame, base_dir: str = DEFAULT_BASE_DIR) -> None: ...
    # 全量覆盖：原子写入全部新年份文件后再清理过期旧年份文件
def load_factor(name: str, start, end, version: str = "1.0", base_dir: str = DEFAULT_BASE_DIR) -> pd.DataFrame: ...
    # 按 [start, end]（含端点）仅读取区间覆盖的年份文件，date 列返回 datetime.date
def load_factor_matrix(names: list[str], start, end, version: str = "1.0", base_dir: str = DEFAULT_BASE_DIR) -> pd.DataFrame: ...
    # 多因子 outer join 拼接逻辑宽表 [date, ticker, f1, f2, ...]
def write_metadata(out_dir, factor, result) -> None: ...
    # 统计 date_range / row_count / file_count，保留 created_at 与 eval/attempts/attempt_log
```

目录布局与 Schema（由 [data/storage_schema.py](../../../../data/storage_schema.py) `factor_schema` 定义）：

```text
$QUANT_DATA_ROOT/factors/{factor_name}/
├── v1.0/                              # 语义化版本目录，多版本共存互不覆盖
│   ├── 2020.parquet … 2024.parquet    # 按年分区
│   └── metadata.json
└── v2.0/ …
```

| 列名 | Parquet / 内存类型 | 说明 |
|---|---|---|
| `date` | `DATE`（`pa.Date` / `datetime.date`） | 无时区日期，可直接与 `load_bars()` 连接 |
| `ticker` | `VARCHAR`（`str`） | 带交易所后缀（如 `000001.SZ`） |
| `{factor_name}` | `DOUBLE`（`float64`） | 原始未清洗因子值；`NaN` 保留不填 |

`metadata.json` 核心字段：`factor_name`、`version`、`description`、`category`、`frequency`、`asset_types`、`created_at`、`updated_at`、`depends_on`、`depends_hash`、`date_range`、`row_count`、`file_count`、`compression`（另保留 `eval` / `attempts` / `attempt_log` 评估块）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 每因子一独立目录 | 长表 EAV / 物理宽表 / 三维张量 | 增删改与升版互不干扰，避免全表重写与列裁剪失效 |
| 不存长表 EAV（`date, ticker, factor_name, value`） | EAV 单表 | 全历史数亿行导致列裁剪失效 |
| 不存物理宽表 | 合并大宽表 | Parquet 加列需重写整表；消费侧按需 `load_factor_matrix` 拼接即可 |
| 不存三维张量（HDF5/zarr） | 科学数组容器 | 统一 Parquet + DuckDB 技术栈；训练时在消费端转 numpy |
| 按年分区（`{year}.parquet`） | 按天 / 按月 / 按资产 | ~125 万行/文件大小适中，避免大量 `<1MB` 碎文件 |
| 数值精度 `DOUBLE`（`float64`） | `float32` | 防止下游截面标准化/中性化 OLS 放大浮点误差 |
| 值列以因子名命名 | 固定 `value` 列 | 读入自解释，拼接宽表无需重命名 |
| 语义化版本目录 `v{version}` | 随机串 / 时间戳目录 | 与 `Factor.version` 一一对应，升级逻辑不覆盖旧版本历史 |
| `append` 日常增量 + 触发式 `full` 覆盖 | 每次全量重写 | 数据追加走 `append`，`version`/逻辑/`depends_hash` 变化走 `full` |
| Parquet 文件 + DuckDB 查询 | ClickHouse | 单机研究零运维；`FactorViewRegistry` 提供防冲突 `factor_{name}` 视图 |

## 实现要点

详见 [research/factor/storage.py](../../../../research/factor/storage.py)。

核心约定：
- 写入前先经 `_normalise` 对 `date` 解析、`ticker` 转字符串、因子列转 `float64`，按 `("date", "ticker")` 去重排序并通过 `factor_schema(factor_name)` 严格校验后才创建版本目录。
- 所有 Parquet 文件均通过 `data.storage_ops.write_parquet_atomic`（`zstd` level 3）原子写入；`save_factor_full` 在全部新年份文件落盘后再删除过期年份文件，防止中途异常破坏历史数据。
- `_safe_component` 拒绝空值与路径分隔符/穿越字符（`.`、`..`、`/`、`\`）。
- `write_metadata` 优先扫描目录内实际 Parquet 文件统计 `date_range` / `row_count` / `file_count`，并在重写时保留既有的 `created_at` 与 `eval` / `attempts` / `attempt_log` 评估块。
