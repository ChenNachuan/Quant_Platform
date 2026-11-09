# config

> 域：core · 对应代码 [core/config.py](core/config.py)

## 职责与边界

平台配置的加载与校验：三层配置（全局 TOML / 策略级 TOML / 敏感 .env），`tomllib` 解析后以 dataclass 作类型载体。只做**加载 + 合并 + 类型校验**，不做配置中心、不做热更新、不做运行时改写。

边界：策略级参数的语义定义归各模块（`constraints` → ../execution/portfolio_construction.md，`backtest` 成本默认值 → ../execution/fee_model.md），config 只负责把字典装进类型并按层合并。

## 接口签名

```python
def load_config(path: str = "config/config.toml") -> dict: ...
def load_strategy_config(alpha_type_id: str, global_cfg: dict | None = None) -> dict: ...
def load_secrets(path: str = "config/.env") -> dict: ...
```

三层结构：

```text
config/
├── config.toml                    # 第 1 层 全局: paths / data / execution / ops
├── strategies/
│   └── {alpha_type}.toml          # 第 2 层 策略级: meta / signal / portfolio / risk
└── .env                           # 第 3 层 敏感: API key / 代理 / 密码
```

## 关键决策

| 决策 | 否决项 | 理由 |
|---|---|---|
| TOML | YAML（on`→`True` 陷阱）；XML啰嗦；Python dataclass 当配置源（改参数要改代码） | `tomllib` 标准库零依赖 |
| 三层结构（全局/策略/敏感） | 一个文件不分层 | 敏感信息与策略参数混在一起既不安全也不清晰 |
| 敏感信息 = `.env`，不入 git 不打印 | 提交进配置文件| `load_secrets()` 返回的 dict 不进 `config_snapshot` |
| 不热更新 | 配置监听热加载 | 改配置 → 重启进程 |
| 手写解析 | `dacite` | 手写 load_* 三函数足够 |

## 实现要点

详见core/config.py

核心约定：
- 合并语义是**策略级覆盖全局同名键**（`{**global, **strategy}`）
- `.gitignore` 必须含 `config/.env`
- `config_snapshot = f"{path}@{file_hash}"` 钉进运行记录（见 logging.md），配合 `git log` 完整复现
- `load_secrets()`返回值不进config_snapshot、不打印、不进日志


