"""平台配置模块：三层加载、合并与校验。

无配置中心、热加载与运行时改写（见 ``design/core/config.md``）；
``load_secrets`` 返回值严禁写入 ``config_snapshot`` 或打印输出。
"""

import contextvars
import hashlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

#: 统一数据根目录。所有数据存储路径由此派生，不在各模块各自硬编码。
DATA_ROOT = Path(
    os.environ.get("QUANT_DATA_ROOT", "/Users/nachuan/Documents/Quant_Data")
)

_data_root_var: contextvars.ContextVar[Path | None] = contextvars.ContextVar(
    "data_root", default=None
)


def get_data_root() -> Path:
    """返回当前执行上下文生效的数据根目录（默认取 ``DATA_ROOT``，可由 :func:`use_data_root` 临时覆盖）。"""
    override = _data_root_var.get()
    return override if override is not None else Path(DATA_ROOT)


def set_data_root(path: str | Path) -> contextvars.Token[Path | None]:
    """设置当前上下文的数据根目录并返回用于恢复的 ``Token``。"""
    return _data_root_var.set(Path(path))


@contextmanager
def use_data_root(path: str | Path) -> Iterator[Path]:
    """在当前上下文内临时切换 :func:`get_data_root` 返回值，退出 ``with`` 块后自动恢复。"""
    resolved = Path(path)
    token = _data_root_var.set(resolved)
    try:
        yield resolved
    finally:
        _data_root_var.reset(token)


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
    """计算配置文件的 SHA-256 摘要并生成 ``f\"{path}@{file_hash}\"`` 运行记录标识，绝不读取 ``.env``。"""
    with open(path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
    return f"{path}@{file_hash}"
