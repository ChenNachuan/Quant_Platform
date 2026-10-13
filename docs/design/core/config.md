# config

> 域：core · 对应代码 core/config.py · 来源 §35.6 / §69.2（archive/OPEN_QUESTIONS.md）

## 职责与边界

平台配置的加载与校验：三层配置（全局 TOML / 策略级 TOML / 敏感 .env），`tomllib` 解析后以 dataclass 作类型载体。只做**加载 + 合并 + 类型校验**，不做配置中心、不做热更新、不做运行时改写。

边界：策略级参数的语义定义归各模块（`constraints` → ../execution/portfolio_construction.md，`backtest` 成本默认值 → ../execution/fee_model.md），config 只负责把字典装进类型并按层合并。

## 接口签名

```python
def load_config(path: str = "config/config.toml") -> dict: ...
def load_strategy_config(alpha_type_id: str, global_cfg: dict | None = None) -> dict: ...
def load_secrets(path: str = "config/.env") -> dict: ...
```

三层结构（§69.2）：

```text
config/
├── config.toml                    # 第 1 层 全局: paths / data / execution / ops
├── strategies/
│   └── {alpha_type}.toml          # 第 2 层 策略级: meta / signal / portfolio / risk
└── .env                           # 第 3 层 敏感: API key / 代理 / 密码
```

配置块与既有设计的对应（§35.6）：`constraints` → `PortfolioConstraints` 全部字段（§34.2）；`data.adjust` → 复权口径（§27.3）；`backtest.commission_rate/slippage_bps` → 成本模型默认参数（§18.1，最终费率表见 ../execution/fee_model.md）；`journal.dir/flush_size` → Journal 落盘（§23/§29.8）；`mode: backtest|live` → 引擎双模式（§22）。

## 关键决策

| 决策 | 否决项 | 一句话理由 |
|---|---|---|
| 格式 = TOML | YAML（on/yes/no 被解析成布尔，`mode: on`→`True` 陷阱）；XML（C++ 生态惯例，Python 啰嗦）；Python dataclass 当配置源（改参数要改代码、git diff 噪音大） | 类型严格无陷阱；`tomllib` 标准库零依赖（Python 3.11+，**3.10 用 `tomli`**，§69.2） |
| 三层结构（全局/策略/敏感） | 一个文件不分层（§35.6 原结论） | 敏感信息与策略参数混在一起既不安全也不清晰——**§35.6 已被 §69 修正，以本节为准** |
| dataclass 仅作加载后类型载体 | — | `tomllib` 解析 → dataclass 校验，不把 Python 文件当配置源 |
| 敏感信息 = `.env`，不入 git 不打印 | 提交进配置文件；打印进日志/报错 | 先例 `.my.prompt.cnf`；`load_secrets()` 返回的 dict 不进 `config_snapshot` |
| 审计 = `git log config/` + `config_snapshot` | 配置中心/专门审计系统 | 单机 TOML + git 就是配置中心——公司 PipelineConfigWatchTask 是多人需求 |
| 不热更新 | 配置监听热加载 | 改配置 → 重启进程（沿用 §35.6） |
| 手写解析 | `dacite` | 手写 load_* 三函数足够（§35.7：load_config ~20 行 + BacktestConfig ~30 行） |

## 实现要点

```python
def load_config(path: str = "config/config.toml") -> dict:
    """第 1 层全局配置。tomllib 零依赖（Python 3.11+；本项目 3.10 用 tomli）。"""
    import tomllib  # py3.10: import tomli as tomllib
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_strategy_config(alpha_type_id: str, global_cfg: dict | None = None) -> dict:
    """第 2 层策略配置，策略级覆盖全局: {**global, **strategy}。"""
    import tomllib
    path = f"config/strategies/{alpha_type_id}.toml"
    with open(path, "rb") as f:
        strategy = tomllib.load(f)
    return {**(global_cfg or load_config()), **strategy}


def load_secrets(path: str = "config/.env") -> dict:
    """第 3 层敏感信息。不入 git、不打印、不进 config_snapshot。"""
    secrets = {}
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            secrets[k.strip()] = v.strip()
    return secrets
```

要点：合并语义是**策略级覆盖全局同名键**（`{**global, **strategy}`）；`.gitignore` 必须含 `config/.env`；`config_snapshot = f"{path}@{file_hash}"` 钉进运行记录（见 logging.md），配合 `git log` 完整复现。

## 实施时定稿项

- `StrategyConfig` / `DataConfig` / `BacktestParams` / `JournalConfig` 四个子 dataclass 的字段清单（§35.8）
- `.env` 具体字段清单定稿（V2 QMT 接入时补，§69.6）
- ~~敏感信息（API key / 数据库密码）怎么管~~ 已定 → §69（.env 不入 git 不打印）
- ~~配置的版本化与审计~~ 已定 → §69（git log + config_snapshot）
- ~~`dacite` 还是手写解析~~ 已定 → §69（手写 load_* 三函数）
