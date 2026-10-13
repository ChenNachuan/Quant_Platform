"""Platform configuration — three-layer load + merge + type-check only.

No config centre, no hot reload, no runtime rewrite (design/core/config.md).
Pure loading and merging; ``load_secrets`` output never enters
``config_snapshot`` and is never printed (red line 8).
"""

import hashlib

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


def load_config(path: str = "config/config.toml") -> dict:
    """Layer 1 — global TOML config.

    Raises ``FileNotFoundError`` if *path* does not exist (no silent fallback).
    """
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_strategy_config(alpha_type_id: str, global_cfg: dict | None = None) -> dict:
    """Layer 2 — strategy-level TOML config merged over global.

    Merge semantics: strategy overrides global on same-named keys,
    i.e. ``{**global, **strategy}``.
    """
    path = f"config/strategies/{alpha_type_id}.toml"
    with open(path, "rb") as f:
        strategy = tomllib.load(f)
    return {**(global_cfg if global_cfg is not None else load_config()), **strategy}


def load_secrets(path: str = "config/.env") -> dict:
    """Layer 3 — sensitive ``.env`` key-value pairs.

    The returned dict is **never** included in ``config_snapshot`` and
    **never** printed (red line 8).
    """
    secrets: dict[str, str] = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip()
    return secrets


def config_snapshot(path: str) -> str:
    """Return ``f"{path}@{file_hash}"`` for run records.

    Hashes the file at *path* with SHA-256.  This function never reads
    ``.env`` and never includes secret values in its output (red line 8).
    """
    with open(path, "rb") as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
    return f"{path}@{file_hash}"
