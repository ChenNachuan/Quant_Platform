# quarantine_raw

> 域：data · 对应代码 data/quarantine.py · data/normalizers/ · 来源 §39（archive/OPEN_QUESTIONS.md）

## 职责与边界

两件事收在一份文档：**隔离区**（被 DQC 拒收的数据放哪、留多久、怎么修复）与 **raw 层**（数据源原样落盘，证据链起点）+ 两侧之间的 **Normalizer**（格式转换，不做清洗）。

三层数据流：

```text
download → save_raw → normalize → DQC → asset/        通过
                                  └→ quarantine/      拒收（≠ 不写盘）
```

关键修正（对 §38.11）：**HARD 拒收 ≠ 不写盘**——拒收批次写入 quarantine 存 30 天，raw 证据始终在（raw 在 DQC **前**落盘，保证据链）。

## 接口签名

```text
quarantine/{asset_type}/{date}.parquet     # 拒收批次数据（normalize 后的 df）
quarantine/{asset_type}/{date}.rejection.json
  # data_date / asset_type / reject_time / batch_status / total_rows /
  # rows_would_drop / drop_rate / hard_rules_triggered[]
raw/{source}/{asset_type}/{date}.parquet                 # 原样，按日一文件（source → asset_type → date）
normalizers/{source}_normalizer.py         # normalize(asset_type, raw_df) -> df   （~100 行/源）
```

修复三路径：

| 路径 | 场景 | 动作 |
|---|---|---|
| A | 数据源本身坏了 | 修源后 `ingest --force` 重下载 |
| B | 规则太严误杀 | `recheck --from-quarantine` 按新规则重验 |
| C | 不修了 | 等 T+30 自动清理 |

生命周期：quarantine **T+30 自动清理**（7 天内应处理完）；raw 保留 **1 年**（~6.5GB/年）；asset 永久。L2 数据上来后 raw 缩到 3 个月。不单独建看板——`batch_status='rejected'`（批次汇总表）即索引，`python -m dqc.report quarantine` 查询。

### raw 与 asset 的职责分界

「原样」= Parquet 格式 + **原始列名 + 原始值 + 原始类型**，不做清洗/转换/计算。以下全部在 asset 侧：

| 动作 | raw | asset |
|---|---|---|
| 列名映射（统一 schema） | ✗ | ✓ |
| 类型修正 | ✗ | ✓ |
| adjfactor 挂载 | ✗ | ✓ |
| tradability flags | ✗ | ✓ |
| DQC / quality_flag | ✗ | ✓ |

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 拒收数据落 quarantine 30 天，不是直接丢弃 | 拒收即扔 | 可修复、可重验、可追责 |
| raw 在 DQC **前**落盘 | DQC 通过后才存 raw | 保证据链：坏批次的原始输入永远可复现 |
| quarantine 存 normalize 后的 df | 存 raw | 修复路径 B 的 recheck 直接可跑 |
| raw 分区 source → asset_type → date（不按合约） | 按合约分区 | 列名/类型不同不能混存；跨源对比要溯源；换源后旧 raw 完整（§39.7） |
| Parquet + zstd | snappy | 多压 30–40% |
| Normalizer 每源一个文件，只做格式转换 | 一个大 Normalizer / Adapter 内转换 | 源间差异隔离；DQC 按 asset_type 不感知源 |
| `normalize(asset_type, raw_df)` 签名 | 按 date/多参数 | 职责最小：raw 进、标准 df 出 |
| 不单独建隔离区看板 | Web 看板 | batch_status 本身就是索引 |
| raw 保留 1 年 / quarantine 30 天 / asset 永久 | 都永久 | 1 年内的质量问题都该被发现了（§39）；asset 是研究输入 |

## 实现要点

```python
# normalizers/{source}_normalizer.py  (~100 行/源)
def normalize(asset_type: str, raw_df: pd.DataFrame) -> pd.DataFrame:
    """格式转换 only：列名映射、类型修正。不做 DQC、不做清洗。"""

# 流水线（对齐 source_adapter.md 的 daily_ingest）
def daily_ingest(...):
    res = adapter.fetch(asset_type, trade_date, watermark)
    save_raw(res)                       # ① 原样落盘（zstd）
    df = normalizer.normalize(asset_type, res.df)   # ② 格式转换
    try:
        ingest_with_dqc(df, date, asset_type)        # ③ DQC → asset/ 或 raise
    except DQCBatchReject as e:
        save_quarantine(df, asset_type, date, e.detail)  # ④ 拒收落隔离区
    save_watermark(...)                 # ⑤
    finally:
        adapter.close()

# 生命周期
cleanup_quarantine(days=30)   # T+30 自动删 parquet + rejection.json
cleanup_raw(days=365)         # 只删 .parquet，勿误删 .watermarks.json（见 source_adapter.md）
```

隔离区查询：`python -m dqc.report quarantine`（扫 `rejection.json` / 批次汇总 `batch_status='rejected'`）。

## 实施时定稿项

- Normalizer 基类的分发/注册方式（签名 `normalize(asset_type, raw_df)` 已定 → §40，分发机制未定）（§39）
- `UNIFIED_DAILY_COLUMNS` 统一列清单（asset 层列名映射的目标 schema）（§39）
- `_attach_adjfactor` / `_attach_tradability_flags` 的 join 失败策略（缺失时补 NaN？拒收？）（§39）
- `e.detail` 与 `rejection.json` 字段一致性（两处 schema 对齐表）（§39）
- `cleanup_raw` 对非 `YYYY-MM-DD` 目录名的兼容（§39）
- 多源并发写入 raw 的安全（同日多源同时落盘）（§39）
- 清理任务（quarantine 30 天 / raw 365 天）是否合并进同一调度 job（→ ops/scheduler.md）（§39）
