# gRPC 逆向工具集

本目录包含用于逆向 `tgw` gRPC 协议的工具，目标是从银河证券 AmazingData SDK 中提取 protobuf 定义，以便在 macOS 上重建纯 Python 客户端。

## 文件说明

| 文件 | 说明 |
|------|------|
| `capture_grpc.py` | Python 层拦截脚本，记录所有 API 调用参数和返回值 |
| `analyze_protobuf.py` | 分析捕获数据，生成 .proto 文件和 API 摘要 |
| `CAPTURE_GUIDE.md` | 详细的 Wireshark 抓包指南 |

## 使用流程

### 第一步：在 Windows 上捕获流量

1. 将整个 `WealthManager/tools/` 目录复制到 Windows
2. 安装依赖：
   ```powershell
   pip install amazingdata
   ```
3. 运行捕获脚本：
   ```powershell
   python -m WealthManager.tools.capture_grpc
   ```
4. 在交互式 Python 中执行 API 调用：
   ```python
   import AmazingData as ad
   ad.login('username', 'password', 'server_ip', 'port')

   base = ad.BaseData()
   print(base.get_code_list('EXTRA_STOCK_A'))

   calendar = base.get_calendar()
   md = ad.MarketData(calendar)
   print(md.query_kline(['000001.SZ'], 20240101, 20240131, period=ad.constant.Period.day.value))
   ```
5. 捕获完成后，`capture_output/` 目录下会有：
   - `capture_log.jsonl` — 完整调用日志
   - `capture_structures.txt` — 数据结构摘要

### 第二步（可选）：Wireshark 抓取原始 gRPC 流量

参考 `CAPTURE_GUIDE.md` 中的 SSLKEYLOGFILE 方法：

```powershell
$env:SSLKEYLOGFILE = "C:\Users\Public\sslkeys.log"
# 然后运行 AmazingData 并用 Wireshark 抓包
```

### 第三步：分析并生成 .proto 文件

```bash
python analyze_protobuf.py --log capture_output/capture_log.jsonl --output proto_output
```

输出：
- `proto_output/api_summary.md` — API 调用摘要
- `proto_output/amazingdata.proto` — protobuf 定义文件
- `proto_output/structures.json` — 结构化数据

### 第四步：在 macOS 上重建 AmazingData

使用生成的 .proto 文件：

```bash
# 安装 gRPC 工具
pip install grpcio grpcio-tools

# 生成 Python 代码
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. amazingdata.proto
```

然后编写新的 AmazingData 包，实现与原版相同的 API 接口。

## 注意事项

1. **捕获脚本有限制**：Python 层拦截只能看到请求参数和返回的 DataFrame，看不到原始 protobuf 编码
2. **Wireshark 抓包更完整**：如果 SSLKEYLOGFILE 生效，可以看到完整的 gRPC 流量
3. **字段编号是猜测的**：生成的 .proto 文件中字段编号是估计值，可能需要根据实际流量调整
4. **敏感信息**：捕获的文件可能包含登录凭证，注意不要提交到版本控制

## 下一步

捕获完成后，将以下文件带回 Mac：
- `capture_output/` 目录
- Wireshark 保存的 `.pcapng` 文件（如有）
- `sslkeys.log`（如有）

然后在 Mac 上运行分析工具生成 .proto 文件。
