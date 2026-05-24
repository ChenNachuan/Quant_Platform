FROM --platform=linux/amd64 python:3.10-slim

WORKDIR /app

# 安装系统依赖（tgw 需要）
RUN apt-get update && apt-get install -y \
    libgomp1 \
    libgssapi-krb5-2 \
    libkrb5-3 \
    && rm -rf /var/lib/apt/lists/*

# 复制 tgw 包（Linux 版本，注意源目录内有嵌套的 tgw/）
COPY docker/tgw_pkg/tgw/ /app/tgw/

# 复制 AmazingData 包（注意源目录内有嵌套的 AmazingData/）
COPY docker/amazingdata_pkg/AmazingData/ /app/AmazingData/

# 复制 MCP Server
COPY mcp_server/ /app/mcp_server/

# 安装 Python 依赖
COPY docker/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# 设置环境变量（运行时通过 docker-compose 或 -e 传入）
ENV PYTHONPATH=/app:/app/tgw/linux_py310_x64_package
ENV LD_LIBRARY_PATH=/app/tgw/linux_py310_x64_package:/app/tgw/common_linux_lib64

ENTRYPOINT ["python", "/app/mcp_server/server.py"]
