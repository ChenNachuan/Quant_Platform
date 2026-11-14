# quarantine_raw

> 域：data · 对应代码 [data/quarantine.py](../../../data/quarantine.py) · [data/normalizers/](../../../data/normalizers/) · [data/raw.py](../../../data/raw.py)
## 职责与边界

三件事：**隔离区**（DQC 拒收的数据放哪、留多久、怎么修复）、 **raw 层**（数据源原样落盘，证据链起点）、**Normalizer**（格式转换，不做清洗）。

三层数据流：

```text
download → save_raw → normalize → DQC → asset/        通过
                                  └→ quarantine/      拒收（≠ 不写盘）
```

**HARD 拒收 ≠ 不写盘**——拒收批次写入 quarantine 存 30 天，raw 在 DQC **前**落盘。

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

生命周期：quarantine **T+30 **；raw **1 年**；asset 永久。

### raw 与 asset 的职责分界

「原样」= Parquet 格式 + **原始列名 + 原始值 + 原始类型**，不做清洗/转换/计算。以下全部在 asset 侧：

| 动作 | raw | asset |
|---|---|---|
| 列名映射 | ✗ | ✓ |
| 类型修正 | ✗ | ✓ |
| adjfactor 挂载 | ✗ | ✓ |
| tradability flags | ✗ | ✓ |
| DQC / quality_flag | ✗ | ✓ |

## 关键决策

| 决策 | 否决项 | 理由 |
|---|---|---|
| 拒收数据落 quarantine 30 天 | 拒收即扔 | 可修复、可重验、可追责 |
| raw 在 DQC **前**落盘 | DQC 通过后才存 raw | 保证据链 |
| quarantine 存 normalize 后的 df | 存 raw | recheck 直接可跑 |
| raw 分区 source → asset_type → date | 按合约分区 | 列名/类型不同不能混存；跨源对比要溯源；换源后旧 raw 完整 |
| Normalizer 每源一个文件，只做格式转换 |  Adapter 内转换 | 源间差异隔离；DQC 按 asset_type 不感知源 |
| raw 保留 1 年 / quarantine 30 天 / asset 永久 | 都永久 | 1 年内的质量问题都该被发现了、 |

## 实现要点

详见 [data/quarantine.py](../../../data/quarantine.py) · [data/normalizers/](../../../data/normalizers/) · [data/raw.py](../../../data/raw.py)

核心约定:
- save_raw 在 DQC之前,save_quarantine在DQC拒收时--两步保证无论通过还是拒收,原始数据都有落盘
- Normalizer 签名统一为 normalize(asset_type,raw_df) ->df,只做列名映射和类型修正,不做清洗
- cleanup_raw 只删.parquet,不删.watermarks.json
- cleanup_quarantine 只删 quarantine/下文件,不越界
- 隔离区查询:python -m data.dqc.report quarantine
