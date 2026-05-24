# gRPC 流量捕获指南 (Windows)

## 概述

`tgw` 底层使用 gRPC over TLS 连接 `*.dgw.com`（ArchForce 数据网关）。本指南帮助你在 Windows 上捕获并解密这些流量，以便逆向出 protobuf 定义。

## 方法一：SSLKEYLOGFILE + Wireshark（推荐）

### 原理

部分 SSL 库支持 `SSLKEYLOGFILE` 环境变量，会将 TLS session key 写入文件。Wireshark 可以用这个文件解密流量。

### 步骤

#### 1. 检查 tgw 是否支持 SSLKEYLOGFILE

```powershell
# 在 PowerShell 中设置环境变量
$env:SSLKEYLOGFILE = "C:\Users\Public\sslkeys.log"

# 删除旧文件（如有）
Remove-Item "C:\Users\Public\sslkeys.log" -ErrorAction SilentlyContinue

# 运行 AmazingData
python -c "
import AmazingData as ad
ad.login('username', 'password', 'server_ip', 'port')
base = ad.BaseData()
print(base.get_code_list('EXTRA_STOCK_A'))
"

# 检查是否有内容写入
Get-Content "C:\Users\Public\sslkeys.log"
```

如果文件有内容（类似 `CLIENT_RANDOM xxxx yyyy`），说明 tgw 的 OpenSSL 支持此功能。

#### 2. 安装 Wireshark

下载安装：https://www.wireshark.org/download.html

安装时确保勾选 **Npcap**（Windows 抓包驱动）。

#### 3. 配置 Wireshark TLS 解密

1. 打开 Wireshark
2. 菜单：`Edit` → `Preferences`
3. 展开 `Protocols` → `TLS`
4. 在 `(Pre)-Master-Secret log filename` 中填入：
   ```
   C:\Users\Public\sslkeys.log
   ```
5. 点击 `OK`

#### 4. 开始抓包

1. 选择你的网络接口（通常是 "以太网" 或 "Wi-Fi"）
2. 开始捕获
3. 设置显示过滤器：
   ```
   tcp.port == 443 && grpc
   ```
   或者如果目标端口不是 443：
   ```
   ip.addr == <server_ip> && tcp.port == <port>
   ```
4. 运行 AmazingData 登录 + 查询
5. 停止捕获

#### 5. 分析 gRPC 流量

在 Wireshark 中：
1. 右键点击一个 gRPC 包 → `Decode As` → 选择 `HTTP/2`
2. 展开 `HyperText Transfer Protocol 2` → `Stream: xxx`
3. 查看 `:path` 字段获取 RPC 方法名
4. 查看 `Data` 字段获取 protobuf 序列化的请求/响应

**关键字段：**
- `:method` = POST
- `:path` = `/galaxy.tgw.IGMDApi/QueryKline`（示例）
- `:authority` = `xxxxx.dgw.com:xxxx`
- `content-type` = `application/grpc`

---

## 方法二：Python 层拦截（简单但信息有限）

如果 SSLKEYLOGFILE 不生效，可以退回到 Python 层拦截。

### 步骤

1. 将 `capture_grpc.py` 和 `patch_tgw.py` 复制到 Windows
2. 运行：
   ```powershell
   python -m WealthManager.tools.capture_grpc
   ```
3. 在交互式 Python 中执行查询
4. 检查 `capture_output/` 目录下的日志文件

**注意：** 这种方法只能看到 Python 层的请求参数和返回的 DataFrame，看不到原始 protobuf 编码。

---

## 方法三：mitmproxy 中间人（高级）

### 原理

用 mitmproxy 作为 gRPC 代理，替换 TLS 证书，捕获明文流量。

### 步骤

#### 1. 安装 mitmproxy

```powershell
pip install mitmproxy
```

#### 2. 启动 mitmproxy

```powershell
# 启动透明代理
mitmproxy --mode transparent --listen-port 8080
```

#### 3. 导出 mitmproxy CA 证书

首次运行后，证书保存在：
```
C:\Users\<username>\.mitmproxy\mitmproxy-ca-cert.pem
```

#### 4. 替换 tgw 的 CA 证书

```powershell
# 备份原证书
Copy-Item "path\to\tgw\win_py39_x64_package\.ca.crt" "path\to\tgw\win_py39_x64_package\.ca.crt.bak"

# 用 mitmproxy 的证书替换
Copy-Item "C:\Users\<username>\.mitmproxy\mitmproxy-ca-cert.pem" "path\to\tgw\win_py39_x64_package\.ca.crt"
```

#### 5. 配置系统代理

```powershell
# 设置 HTTP 代理
$env:HTTP_PROXY = "http://127.0.0.1:8080"
$env:HTTPS_PROXY = "http://127.0.0.1:8080"
```

#### 6. 运行 AmazingData

```powershell
python -c "
import AmazingData as ad
ad.login('username', 'password', 'server_ip', 'port')
"
```

**风险：** tgw 可能做证书固定（certificate pinning），会拒绝 mitmproxy 的证书。

---

## 捕获的数据

无论用哪种方法，最终你需要收集：

### 1. RPC 方法列表

```
galaxy.tgw.IGMDApi/Login
galaxy.tgw.IGMDApi/QueryKline
galaxy.tgw.IGMDApi/QuerySnapshot
galaxy.tgw.IGMDApi/QueryCodeTable
...
```

### 2. 请求消息格式

每个 RPC 方法的请求参数结构：
```protobuf
message ReqKline {
  repeated string code = 1;
  int32 start_date = 2;
  int32 end_date = 3;
  int32 period = 4;
  // ...
}
```

### 3. 响应消息格式

每个 RPC 方法的返回数据结构：
```protobuf
message RspKline {
  repeated KLineData data = 1;
  int32 error_code = 2;
  string error_msg = 3;
}
```

### 4. 枚举值

```protobuf
enum MarketType {
  EXTRA_STOCK_A = 0;
  EXTRA_STOCK_B = 1;
  // ...
}
```

---

## 下一步

捕获完成后，将以下文件带回 Mac：

1. `capture_output/capture_log.jsonl` — Python 层调用日志
2. `capture_output/capture_structures.txt` — 数据结构摘要
3. Wireshark 保存的 `.pcapng` 文件 — 完整网络流量
4. `sslkeys.log` — TLS 解密密钥（如有）

然后使用 `analyze_protobuf.py` 工具分析这些文件，生成 `.proto` 定义。

---

## 常见问题

### Q: SSLKEYLOGFILE 文件是空的？

A: 说明 tgw 使用的 OpenSSL 版本不支持此功能。需要回退到 Python 层拦截或 mitmproxy。

### Q: Wireshark 看不到 gRPC 流量？

A: 尝试：
1. 确保过滤器正确：`tcp.port == <port>`
2. 先用 `http2` 过滤器试试
3. 检查是否选择了正确的网络接口

### Q: tgw 报证书错误？

A: 如果用 mitmproxy 方案，说明 tgw 做了证书固定。只能用 SSLKEYLOGFILE 或 Python 层拦截。

### Q: 如何找到 tgw 的 CA 证书位置？

A: 在 `tgw` 包的平台目录下：
```
tgw/win_py39_x64_package/.ca.crt
```
