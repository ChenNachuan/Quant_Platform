"""平台配置模块：三层加载、合并与校验。

无配置中心、热加载与运行时改写（见 ``design/core/config.md``）；
``load_secrets`` 返回值严禁写入 ``config_snapshot`` 或打印输出。
"""

import hashlib
import os
from pathlib import Path

#: 统一数据根目录。所有数据存储路径由此派生，不在各模块各自硬编码。
DATA_ROOT = Path(
    os.environ.get("QUANT_DATA_ROOT", "/Users/nachuan/Documents/Quant_Data")
)

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 兼容
    import tomli as tomllib


def load_config(path: str = "config/config.toml") -> dict:
    """Layer 1：加载全局 TOML 配置；文件不存在时直接抛出 ``FileNotFoundError``。"""
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_strategy_config(alpha_type_id: str, global_cfg: dict | None = None) -> dict:
    """Layer 2：加载策略级 TOML 并覆盖合并到全局配置（同名键以策略配置为准）。"""
    path = f"config/strategies/{alpha_type_id}.toml"
    with open(path, "rb") as f:
        strategy = tomllib.load(f)
    return {**(global_cfg if global_cfg is not None else load_config()), **strategy}


def load_secrets(path: str = "config/.env") -> dict:
    """Layer 3：加载 ``.env`` 敏感键值对；返回值严禁进入 ``config_snapshot`` 或被打印。"""
    secrets: dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip()
    return secrets


def config_snapshot(path: str) -> str:
    """计算配置文件的 SHA-256 摘要并生成 ``f"{path}@{file_hash}"`` 运行记录标识，绝不读取 ``.env``。"""
    with open(path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
    return f"{path}@{file_hash}"
