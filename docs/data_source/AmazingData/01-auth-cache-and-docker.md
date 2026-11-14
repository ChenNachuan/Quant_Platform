# 01. 认证、本地缓存与 Docker 执行

## 1. 运行环境、安装与账号管理

### 1.1 运行环境与安装（容器外参考）

安装包托管于官方仓库 <https://github.com/Rockyzsu/AmazingData_API_Doc.git>，支持 **Python 3.8–3.13**：

```bash
pip install tgw-1.7.1-py3-none-any.whl             # 前置依赖（tgw-1.*.*-py3-none-any.whl）
pip install AmazingData-1.0.0-cp312-none-any.whl   # 按操作系统与 Python 版本选择
```

| 平台 | 最低配置 | 推荐配置 |
|---|---|---|
| **Linux（RedHat 7.2 / 7.4 / 7.6）** | 4 核 `2.10GHz` · DDR4 `4GB` · `200GB` 盘 · 普通网卡 | 8 核 `2.10GHz` · DDR4 `4GB` · `480GB` 盘 · 万兆网卡 |
| **Windows（Win10 64 位）** | 4 核 `2.60GHz` · DDR4 `4GB` · `200GB` 盘 · 普通网卡 | 8 核 `2.60GHz` · DDR4 `4GB` · `480GB` 盘 · 万兆网卡 |

### 1.2 登录、登出与更新密码

所有数据接口调用前必须先执行 `ad.login()`。虽然官网说明“正常使用无需调用 `logout`”，但同一账号同时在线连接槽位有限，**本平台强制要求脚本退出前调用 `ad.logout()`**（建议置于 `try ... finally` 块中）：

```python
import os
import AmazingData as ad

ad.login(
    username=os.environ["AMAZINGDATA_USER"],
    password=os.environ["AMAZINGDATA_PASSWORD"],
    host=os.environ["AMAZINGDATA_HOST"],          # 官网参数表误写为 ip/host，实际关键字参数为 host: str, port: int
    port=int(os.environ["AMAZINGDATA_PORT"]),     # 必须为 int
)
try:
    ...
finally:
    ad.logout(username=os.environ["AMAZINGDATA_USER"])
```

- **更新密码**：`ad.update_password(username: str, old_password: str, new_password: str)`（需在已登录状态下调用）。

---

## 2. 本地量化数据库与缓存机制（`local_path` / `is_local`）

`BaseData` 与 `InfoData` 多数历史/静态接口支持本地落盘缓存，显著降低服务端排队与传输延迟：

| 参数 | 类型 | 默认值 | 行为说明 |
|---|---|---|---|
| `local_path` | `str` | 无（多数接口必填） | 本地缓存文件夹**绝对路径**（例如 `"/data/cache/"`） |
| `is_local` | `bool` | `True` | `True`：优先读本地缓存，缺失时再回源拉取并保存；`False`：以本地为基础从服务端增量拉取并更新缓存 |

### 定时预下载：`DownloadInfoData`

可通过 `ad.DownloadInfoData` 做每日定时落盘（以历史代码表为例）：

```python
download_info = ad.DownloadInfoData(local_path="/data/cache/")
calendar = ad.BaseData().get_calendar(market="SH")
download_info.download_hist_code_list(calendar)  # 下载 20130101 以来全部支持代码表
```

---

## 3. Docker 容器执行规范（本平台）

本平台通过预构建镜像 `docker-amazingdata` 托管 SDK，根目录 [`docker-compose.yml`](../../../docker-compose.yml) 已配置卷挂载：

| 宿主机路径 | 容器内路径 | 说明 |
|---|---|---|
| `.`（项目根） | `/opt/project` | 代码目录 |
| `$QUANT_DATA_ROOT`（默认 `Quant_Data`） | `/data` | 容器内所有 `local_path` 应使用 `/data/...` 前缀 |
| `./scripts` | `/scripts` | 抓取脚本目录 |

```bash
# 必须在项目根目录执行（不要重建镜像，也不要修改 ~/AmazingData/docker/docker-compose.yml）
docker-compose run --rm amazingdata python3 /scripts/data_fetcher/fetch_stock_daily.py
```

---

## 4. 已知 SDK 坑点与平台封装边界

以下硬约束均已内置在 [`AmazingdataAdapter`](../../../data/adapters/amazingdata_adapter.py) 中，业务层不应重复造轮子：

1. **`local_path` 必须传 `str`**：SDK 内部对路径做 `+` 字符串拼接，传入 `pathlib.Path` 会触发 `TypeError: unsupported operand type(s) for +: 'PosixPath' and 'str'`。
2. **日期必须为 `YYYYMMDD` 整数（`int`）**：`query_kline` / `query_snapshot` / `get_hist_code_list` 的 `begin_date` / `end_date` / `start_date` 传字符串会失败（`get_calendar` 默认返回 `str` 列表）。
3. **`get_option_basic_info` 单次上限约 100–104 条**：无论传入多少期权代码，单次最多返回百余条；必须按 **每批 10 个代码** 循环调用并 `pd.concat`。
4. **连接数限制**：[`AmazingdataAdapter`](../../../data/adapters/amazingdata_adapter.py) 设置 `requests_per_second = 2.0`，并在 `close()` 中自动调用 `ad.logout()`。
5. **前复权仅作用于股票**：股票 K 线回测前需乘以复权因子；指数与场内 ETF/基金不需要复权。
