# 01. 命令行下载器（`qdc-cli`）与本地 MCP

[← 返回目录](README.md)

> 官网下载页：<https://qdc-data.com/downloads.html> · 本机验证基线：`qdc-cli v1.3.0`（macOS arm64 Mach-O；与官网图形版下载器共用同一内核，无需 API 订阅即可对已购数据集执行全量同步、增量更新、格式转换与空间管理）。

---

## 1. 安装与安全注意

- **macOS 安装**：强烈建议按官网指引使用 `curl` 下载至本地路径（例如 `/opt/homebrew/bin/qdc-cli` 或 `~/bin/qdc-cli`）并赋予执行权限 `chmod +x`。**切勿用浏览器直接下载**，否则文件会携带 `com.apple.quarantine` 隔离标记被 Gatekeeper 拦截。
- **Windows 安全校验**：无代码签名；首次运行若弹出 SmartScreen「未知发布者」，可点「更多信息 → 仍要运行」，并用 `certutil -hashfile qdc-cli.exe SHA256` 核对官网下载页公布的 SHA256 清单。`qdc-cli` 不做静默自更新。
- **不绑定 API Key**：`qdc-cli` 与图形下载器使用官网账号邮箱/密码登录并缓存本机凭据，**下载已购历史数据与每日增量更新不消耗在线 API 每日行数额度**。

---

## 2. 鉴权与环境变量

`qdc-cli login` 刻意**不提供 `--password` 命令行参数**，防止密码明文进入 shell history 或被同机进程通过 `ps` 窥探。

支持以下两种安全登录方式：

```bash
# 方式一：交互输入（推荐）
qdc-cli login

# 方式二：通过环境变量注入（切勿把真实邮箱与密码写入 Git 或文档）
QDC_EMAIL="<your-qdc-email>" QDC_PASSWORD="<your-qdc-password>" qdc-cli login
```

其他账号命令：

- `qdc-cli whoami` —— 查看当前登录状态、账号档位、到期日及已购数据集/历史深度。
- `qdc-cli logout` —— 退出登录并清除本地凭据。

---

## 3. 全局参数与目录查看

| 命令 | 说明 |
|---|---|
| `qdc-cli catalog` | 输出官网全部在售数据集目录 JSON（含 `meta`、`offers` 套餐定义与 111 个 `skus` 元信息） |
| `qdc-cli local` | 列出本地已下载数据集、占盘大小、已有年份、当前格式与移除标记 |
| `qdc-cli version` | 查看当前 `qdc-cli` 版本并检查官网最新版 |
| `qdc-cli help` | 打印完整命令与参数说明 |
| `--json` | 全局参数：强制结构化 JSON 输出（当前版本默认即输出 JSON，保留作向后兼容） |
| `--quiet` | 全局参数：抑制向 `stderr` 打印的进度信息（适合自动化脚本调用） |

> [!NOTE]
> `qdc-cli` 不支持子命令级别的 `--help`（例如 `qdc-cli get --help` 会报 `缺少参数`）；全量用法统一见 `qdc-cli --help`。

---

## 4. 配置管理（`config`）

首次使用下载命令（`get` / `sync` / `update`）前**必须先配置下载根目录**，否则 `qdc-cli` 会直接拒绝执行，避免数十至数百 GB 数据落入未预期磁盘。

```bash
# 查看当前下载配置（默认 format=parquet, auto_convert=false）
qdc-cli config

# 设置下载根目录（程序会自动在其下创建 QDCData/ 文件夹）
qdc-cli config --dir /path/to/raw_storage

# 设置默认存储格式：parquet | duckdb | csv | xlsx
qdc-cli config --format parquet

# 开启/关闭“更新后自动转换格式”（默认 off）
# 若数据集曾执行过 convert，开启 on 后每次 update 拉下的新 Parquet 分卷会自动转为同格式
qdc-cli config --auto-convert on
```

---

## 5. 下载与增量更新

| 命令 | 用法与行为说明 |
|---|---|
| `qdc-cli get <sku> [--years 2023,2024]` | 下载指定数据集（如 `cn_lhb`）。不带 `--years` 表示下载账号授权下的完整历史包；大表建议先用 `--years` 指定年份试下或分批拉取 |
| `qdc-cli sync [--years N] [--only a,b]` | 一键下载已购全部数据集；`--years N` 仅同步最近 `N` 年（**强烈建议配置**，全量不加限制可能达数百 GB）；`--only a,b` 仅同步逗号分隔的指定 SKU |
| `qdc-cli update [--all-history]` | 一键增量更新：默认**仅按用户原先选择的年份范围**补齐最新分卷，且校验一致的旧分卷自动跳过；显式传入 `--all-history` 才会补齐历史未选年份 |

> [!IMPORTANT]
> - 本地用 `qdc-cli rm` 删除过的数据集/年份会被写入移除标记，执行 `qdc-cli update` **不会**把它们重新拉回；必须先 `qdc-cli restore <sku>` 撤销标记。
> - 相反，如果你直接在 Finder / 文件管理器手工删除文件而未执行 `qdc-cli rm`，下次 `update` **会**把缺失分卷重新下载回来。

---

## 6. 本地加工与格式转换

```bash
# 1. 本地周期合并（1min -> 5min / 15min / 30min / 60min / 1d / 1w / 1M）
qdc-cli resample <sku> --period 5min

# 2. 本地复权加工（前复权 qfq / 后复权 hfq）
qdc-cli adjust <sku> --mode qfq

# 3. 本地格式转换（duckdb / csv / xlsx）
qdc-cli convert <sku> --to duckdb
```

> [!WARNING]
> **必须牢记的三条本地转换铁律（CLI 与 MCP 内核共用）：**
> 1. **原地转换、本地只留一份**：`convert` 会把目标数据集在本地的所有分卷一次性全部转换为新格式，并在完成后**自动删除原 Parquet 分卷**，不保留双份。
> 2. **先加工，再转格式；转后不可逆**：`resample` 与 `adjust` **只读 Parquet 文件**；一旦 `convert` 成 `duckdb` / `csv` / `xlsx`，**无法再转回 Parquet**（如需回到 Parquet 只能 `rm` 删除后重下）。因此务必按 **`Parquet` 下载 → `resample`/`adjust` 加工 →（如有需要再）`convert`** 的顺序操作。
> 3. **体积与行数约束**：CSV / Excel 体积常比 Parquet+zstd 大数倍，大表转换前先确认剩余磁盘空间；`xlsx` 单卷超过 **104 万行**会被直接拒绝；首次执行 `convert` 会自动下载并缓存一个约 **30 MB** 的本地转换引擎。

---

## 7. 磁盘空间清理与恢复

| 命令 | 说明 |
|---|---|
| `qdc-cli rm <sku> [--years 2019,2020] [--dry]` | 删除指定数据集或年份分卷，并写入移除标记防止 `update` 重拉；建议始终先加 `--dry` 预览释放空间 |
| `qdc-cli trim --keep <N> [--only a,b] [--dry]` | 全局修剪，所有（或 `--only` 指定的）本地数据集仅保留最近 `N` 年，同样支持 `--dry` 预览 |
| `qdc-cli restore <sku>` | 撤销 `rm` 移除标记，使其在下一次 `update` 时恢复自动补齐 |

---

## 8. 本地 MCP Server（`qdc-cli mcp`）

`qdc-cli` 内置本地 **stdio MCP Server**，供 Claude Desktop、Cursor 等本地 AI 客户端管理磁盘上的已购数据（与在线远程 HTTP MCP 完全独立，不需要 API 订阅）。

- **启动要求**：必须先在终端成功运行过一次 `qdc-cli login` 持久化本机凭据（MCP 工具设计上不允许通过参数传密码）；日志输出到 `stderr`，JSON-RPC 协议走 `stdout`。
- **客户端配置示例（`claude_desktop_config.json` / `mcp.json`）**：

```json
{
  "mcpServers": {
    "qdc": {
      "command": "/opt/homebrew/bin/qdc-cli",
      "args": ["mcp"]
    }
  }
}
```
> Windows 路径在 JSON 中须写双反斜杠，如 `D:\\qdc\\qdc-cli.exe`。

### 内置 10 个 MCP 工具清单

| 工具名 | 参数 | 核心作用与内置防呆约束 |
|---|---|---|
| `account` | 无 | 查询账号档位、到期日与已购数量；调下载前先查权限避免 `403` |
| `list_datasets` | `keyword`（建议必填）, `limit`（默认 20） | 列出当前账号有权限下载的 SKU 与本地下载状态；**强制建议带 `keyword` 过滤**避免 90+ 条撑满上下文 |
| `local_data` | 无 | 检查本地占盘大小、已有年份、文件格式及移除标记 |
| `estimate_download` | `dataset`（必填）, `years`（可选） | 向服务器拉取清单估算目标体积与分卷数（不落盘）；下载数十 GB 前必须先估算，大表按年列出明细 |
| `download_start` | `dataset` 或 `all`（二选一）, `years`, `depth_years` | **异步启动下载作业并立即返回作业号**（可能跑数小时）；全局同时只允许 1 个作业；用 `all=true` 时强烈建议传 `depth_years` |
| `job_status` | 无 | 查询当前异步下载作业进度、跳过/失败摘要；下载期间建议每隔十几秒轮询一次 |
| `update_all` | 无 | 异步启动一键更新（仅补齐已选年份范围；已通过程序移除的不会拉回） |
| `convert_format` | `dataset`（必填）, `to`（`duckdb`/`csv`/`xlsx`） | 本地格式转换，严格遵循「删原 Parquet、只留一份、不可逆、`xlsx` 超 104 万行拒绝」铁律 |
| `cleanup` | `mode`（`delete`/`trim`/`restore`）, `dataset`, `years`, `keep`, `dry`（默认 `true`） | 清理或撤销标记；**默认 `dry=true` 仅预览释放空间**，二次确认后再传 `dry=false` 执行真删 |
| `setup` | `dir`（绝对路径，下建 `QDCData/`）, `format`（`parquet`/`duckdb`/`csv`/`xlsx`） | 查看或配置下载目录与默认格式；未设目录前下载工具一律拒绝执行 |

---

## 9. 特殊交付：`cn_tick_l2` 网盘工作流

全库唯一不走 `qdc-cli` / API、仅通过**百度网盘 / 夸克网盘**交付的数据集是 `cn_tick_l2`（10 档逐笔 Tick L2，2017~至今，5T 级归档）：

1. 下单后在官网会员中心获取分享链接与提取码，新增交易日直接追加在原分享目录内，无需换链。
2. 每日一个 `.7z` 压缩包（2017 年约 `0.69 GB/日`，2026 年约 **`5.19 GB/日`**，最近一年约 `870 GB`）。
3. 解压后为按证券分目录的 `GB18030` 编码 CSV（每只标的 3 个文件：逐笔成交、逐笔委托、盘口快照），单日解压膨胀至约 **`53 GB`**（压缩态的 8.6 倍）。
4. **标准用法**：「用哪天下哪天、算完即删」，如需长期保存务必使用网盘随附脚本合并转为日度 3 个 Parquet 文件（压回约 **`12 GB/日`**），严禁整年解压为原始 CSV。
5. **价格单位坑**：所有价格列均为整数，单位**万分之一元**，计算或落盘必须 **`÷ 10000`**（例如 `12790000` = `1279.00` 元）。
