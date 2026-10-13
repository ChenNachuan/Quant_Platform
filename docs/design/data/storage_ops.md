# storage_ops

> 域：data · 对应代码 data/storage_ops.py · 来源 §48–§51（archive/OPEN_QUESTIONS.md）

## 职责与边界

存储的**运行时行为**：写入配置、原子写与并发控制、失败恢复、数据生命周期、容量告警、备份。schema 结构见 [storage_schema.md](storage_schema.md)。

- **写**：`PARQUET_WRITE_CONFIG` + `write_parquet_atomic`（lock → tmp → rename 三阶段，~40 行）。
- **并发**：FileLock（flock LOCK_NB，~25 行）+ 按日隔离；读侧零协调（inode 语义）。
- **生命周期**：`cleanup_expired` 按 `lifecycle.yaml` 滚动删除（~40 行）；`cleanup_stale_tmp/locks` 清写入残留（~15 行）。
- **容量与备份**：`check_capacity` 三档告警（~20 行）+ `backup.sh` rsync 月度（~30 行）。

核心原则：**个人平台没有真正的并发冲突——最便宜的机制兜底即可；所有操作幂等，失败就重跑，不搞事务回滚；可重建的数据不备份强一致，备份保最新可恢复**（§49/§51）。

## 接口签名

```python
PARQUET_WRITE_CONFIG = dict(compression="zstd", row_group_size=65536,
                            use_dictionary=True, write_statistics=True, index=False)

def write_parquet_atomic(df: pd.DataFrame, path: str, **kwargs):
    """三阶段：FileLock → 写 {path}.tmp → os.rename（对应 Initializer→Worker→Committer）"""

class FileLock:                      # flock LOCK_EX | LOCK_NB，拿不到抛 ConcurrentWriteError
    def __enter__(self): ...
    def __exit__(self, *exc): ...    # 顺手删 .lock

def cleanup_stale_tmp(root, max_age_hours=1): ...     # 清 >1h 的 .tmp
def cleanup_stale_locks(root, max_age_hours=1): ...   # flock 随进程退出自动释放，仅整洁
def cleanup_expired(root="data_platform", dry_run=True): ...  # lifecycle.yaml 滚动清理
def check_capacity(data_root="data_platform") -> str: ...     # OK | WARNING | CRITICAL

# scripts/backup.sh —— rsync 全量 data_platform → 外置盘，每月手动
```

**保留策略**（`lifecycle.yaml`）：

| 层 | 保留 | 理由 |
|---|---|---|
| asset/ | **永久** | 正式数据，回测/实盘任意日期引用 |
| research/ | **永久** | 因子/回测结果，不可重建性最高 |
| raw/ | 1 年滚动 | 溯源；数据源本身就是归档（可重取） |
| corrections/ · cross_check/ · dqc_detail | 1 年滚动 | 审计，与 raw 同寿命 |
| quarantine/ | 30 天 | 排查完即删；超期说明已放弃修复 |

**容量告警**：free > 50 GB OK；10–50 GB WARNING（提醒清理）；≤ 10 GB CRITICAL（入库前检查并**拒绝写入**）；另加单日新增 >100 MB 异常告警（正常日增 ~16 MB，防重复入库/膨胀）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 扁平 `{YYYY-MM-DD}.parquet` | Hive 分区 | DuckDB 不需 partition discovery |
| zstd level 3 | snappy/gzip | 压缩比与速度折中（对比表 §48） |
| Row Group 65536；列编码 pyarrow 自动 | 手动指定编码 | 不用调 |
| 文件大小目标 1–256MB；日频表 80–300KB 偏小但接受，不合并 | 日频合并成月文件 | 按天查询是最常见 pattern，月文件查单天 30x 无用 IO（§48.4） |
| 不做小文件合并 | 定期 compaction | 10 年 ~1 万文件，ext4/xfs 轻松；L2 若需按月合并是 Phase 2（§48.5） |
| tmp + rename 原子写 | 直接覆盖写 | 要么旧要么新，无半截文件 |
| FileLock **内嵌** write_parquet_atomic | 调用方手动加锁 | 不可能忘记加锁（修订 §48.6） |
| flock LOCK_NB 同天写直接拒绝 | 等待/重试 | 并发写是配置错误，不是需协调的正常场景 |
| 并发读零锁（inode fd 语义 = snapshot isolation） | 读锁 | 打开的 fd 指向旧 inode，读完整旧版 |
| 一天一文件 = 一天一事务 | 两阶段提交/跨文件事务 | 跨表 partial 成功无害，复杂度远超收益 |
| 幂等重跑，不写回滚 | ROLLBACK 逻辑 | 重跑 = 首次运行，比回滚更强 |
| 全部覆盖写 | Parquet 追加 | Parquet 不支持原生追加 |
| 版本化当前不做（.prev 最小成本预案 ~8 行） | list_versions/set_version | 覆盖写 + correction 日志已够 |
| asset/research 永久，raw 1 年，quarantine 30 天 | 统一保留期 | Class 分级映射：Class 3/4 永久、Class 2 可重取 |
| `cleanup_expired` 默认 `dry_run=True` | 直删 | cron 先报告后执行 |
| 不做冷热分层 | scratch/vault | <100 GB 全 SSD |
| 10 年容量 ~45 GB（不含 L2）/ ~84 GB（含 L2） | 78 GB/1.3 TB 旧估 | 逐表推算修订 §49.7，以 §51 为准 |
| 备份**全量** data_platform，rsync 月度 → 外置盘 | 只备份 research+config（§49 旧） | 45 GB 8min 可回拷；Parquet 文件级粒度天然最细（修订 §49.8） |
| RPO 1 个月 / RTO 4 小时 | 更高频 | 研究系统可重建（全量重拉 1–2 天作灾难兜底） |
| 备份排除 `*.tmp`/`*.lock` + parquet 文件数比对验证 | 无验证 | 对标备份中心完整性验证 |

## 实现要点

```python
def write_parquet_atomic(df, path, **kwargs):        # 修订 §48.6：锁内嵌
    with FileLock(path):                             # Phase 1 声明意图
        tmp = path + ".tmp"
        df.to_parquet(tmp, **PARQUET_WRITE_CONFIG, **kwargs)   # Phase 2
        os.rename(tmp, path)                         # Phase 3 原子提交；不 rename = 回滚

class FileLock:                                      # ~25 行
    def __enter__(self):
        self._fd = open(self._lock_path, "w")
        try: fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise ConcurrentWriteError(...)
    def __exit__(self, *a): fcntl.flock(self._fd, fcntl.LOCK_UN); os.remove(self._lock_path)

def daily_ingest(source, asset_type, trade_date):    # 幂等四步，全部覆盖写
    if check_capacity() == "CRITICAL": raise DiskFullError(...)
    result = adapter.fetch(...); save_raw(...); ingest_with_dqc(...)
    save_watermark(...)                              # rename 后崩溃 → 重跑安全

# cleanup_expired：lifecycle.yaml policy=permanent 跳过；rolling 按文件名日期 < cutoff 删除
# cleanup_stale_tmp：入库启动时清 >1h 的 .tmp（与 cleanup_expired 分工：残留 vs 过期）
# backup.sh：rsync -avz --delete --exclude='*.tmp' --exclude='*.lock' + 文件数比对
```

新增代码 ≈40 行（写入/锁）+ ≈80 行（cleanup/disk/backup 骨架）+ 净增 ≈30 行（§50/§51 完整版）。

## 实施时定稿项

- `cleanup_tmp` 阈值 1 小时是否按表区分（分钟频写入时间长）（§48）
- `use_dictionary=True` 对 factor 表 value 列 float64 的语义（无害但模糊）（§48）
- L2 按月合并文件与 `row_group_size=65536` 的配合（~500MB 文件 row group 数）（§48）
- `write_statistics=True` 对 27 列 L2 表 metadata 大小的影响（§48）
- data_lake Hive 命名 → 扁平命名迁移脚本细节（§48/§46）
- `lifecycle.yaml` 形态定稿（硬编码 dict vs YAML——示例已用 YAML）（§49）
- raw 层 1 年保留起算点（文件名日期滚动 vs 入库时间 mtime）（§49/§51）
- FileLock 锁文件清理时机（与 .tmp 清理合并是否足够）（§49）
- research 每周备份改频的触发条件（「代价太大」如何量化）（§49）
- rclone 云备份启用判断（灾备升级 Level 2 的量化标准）（§49/§51）
- 容量分项明细：bars 0.5+60+0.8+5≈66GB 与旧估 78GB 差额来源（已被 §51 逐表推算取代）（§49）
- `DiskFullError` 恢复路径（清理后手动重试 vs 自动重试）（§49）
- `ConcurrentWriteError` 异常层级归属模块（§50）
- 同天并发写被拒后的重试语义（当前：直接失败，调用方自定）（§50）
- `write_parquet_atomic_versioned` 的 .prev 清理与保留个数（当前不做版本化）（§50）
- K06–K08 跨粒度异步检查的调度时机（「两者都完成后」如何判定）（§50）
- `save_watermark` 与 asset 写入的顺序语义（watermark 应是最后成功步骤标记）（§50）
- `parse_date_from_filename` 对非日期命名文件（reference 全量单文件）的处理（§51）
- data_growth 基线 100MB 是否按表分开（bars_minute 单表已 ~16MB）（§51）
- factor 永久保留的清理策略（因子淘汰时 deprecated 标记 vs 物理删除）（§51）
- 备份验证是否加抽样 checksum（当前只文件数比对）（§51）
