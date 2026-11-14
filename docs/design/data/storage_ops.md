# storage_ops

> 域：data · 对应代码 [data/storage_ops.py](data/storage_ops.py)

## 职责与边界

存储的**运行时行为**：写入配置、原子写与并发控制、失败恢复、数据生命周期、容量告警、备份。schema 结构见 [storage_schema.md](storage_schema.md)。
核心原则：**所有操作幂等，失败就重跑，不搞事务回滚；最便宜的机制兜底即可**。

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
def cleanup_expired(data_root=DATA_ROOT, dry_run=True): ...  # lifecycle.yaml 滚动清理
def check_capacity(data_root=DATA_ROOT) -> str: ...     # OK | WARNING | CRITICAL

# scripts/backup.sh —— rsync 全量 $QUANT_DATA_ROOT → 外置盘，每月手动
```

**保留策略**（`lifecycle.yaml`）：

| 层 | 保留 | 理由 |
|---|---|---|
| asset/ | **永久** | 正式数据，回测/实盘任意日期引用 |
| factor/ | **永久** | 因子/回测结果，不可重建性最高 |
| raw/ | 1 年滚动 | 溯源；数据源本身就是归档（可重取） |
| corrections/ · cross_check/ · dqc_detail | 1 年滚动 | 审计，与 raw 同寿命 |
| quarantine/ | 30 天 | 排查完即删；超期说明已放弃修复 |

**容量告警**：free > 50 GB OK；10–50 GB WARNING（提醒清理）；≤ 10 GB CRITICAL（**拒绝写入**）；单日新增 >100 MB 异常告警。

## 关键决策

| 决策 | 否决项 | 理由 |
|---|---|---|
| zstd level 3 | snappy/gzip | 压缩比与速度折中 |
| Row Group 65536；列编码 pyarrow 自动 | 手动指定编码 | 不用调 |
| tmp + rename 原子写 | 直接覆盖写 | 要么旧要么新，无半截文件 |
| FileLock **内嵌** write_parquet_atomic | 调用方手动加锁 | 不可能忘记加锁 |
| flock LOCK_NB 同天写直接拒绝 | 等待/重试 | 并发写是配置错误，不是需协调的正常场景 |
| 并发读零锁 | 读锁 | 打开的 fd 指向旧 inode，读完整旧版 |
| 幂等重跑，不写回滚 | ROLLBACK 逻辑 | 重跑 = 首次运行，比回滚更强 |
| 全部覆盖写 | Parquet 追加 | Parquet 不支持原生追加 |
| `cleanup_expired` 默认 `dry_run=True` | 直删 | cron 先报告后执行 |
| asset/research 永久，raw 1 年，quarantine 30 天 | 统一保留期 | Class 分级映射|
| 备份**全量** $QUANT_DATA_ROOT，rsync 月度 → 外置盘 | 只备份 research+config | 45 GB 8min 可回拷；Parquet 文件级粒度天然最细 |
| RPO 1 个月 / RTO 4 小时 | 更高频 | 研究系统可重建 |

## 实现要点

详见[data/storage_ops.py](data/storage_ops.py)

核心约定:

- 原子写三阶段:FileLock声明意图→写.tmp→os.rename原子子提交。rename前崩溃=自动回滚(.tmp被cleanup_stale_tmp清理)
- 幂等语义:覆盖写+watermark最后更新。任何步骤崩溃,重跑等价于首次运行
- cleanup分工:cleanup_expired按 lifecycle.yaml 删过期数据;cleanup_stale_tmp清写入残留(>1h的.tmp);两者独立
- 容量检查在写入前:check_capacity =="CRITICAL"时拒绝写入,不等磁盘满了再报错
- 备份排除*.tmp、*.lock+parquet文件数比对验证
