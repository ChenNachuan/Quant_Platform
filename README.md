# Quant Platform

面向中国 A 股、期货与期权研究的量化平台，覆盖数据采集、因子计算、模型训练、回测和执行。

项目以 Parquet + DuckDB 为数据底座，以统一 Factor API 管理多资产因子，并提供 VectorBT、ZVT 与 Qlib 三类研究/回测路径。

## 快速开始

```bash
# 安装依赖
uv sync

# 查看测试基线（AmazingData 测试需要 Docker 环境）
uv run pytest tests/

# 代码检查与格式化
uv run ruff check .
uv run ruff format .
```

AmazingData 容器必须从项目根目录启动；不要重建镜像，也不要修改 `~/AmazingData/docker/docker-compose.yml`：

```bash
docker-compose run --rm amazingdata \
  python3 /scripts/data_fetcher/fetch_stock_daily.py
```

## 主干结构

| 目录 | 职责 |
|---|---|
| `config/` | 全局与策略配置 |
| `infra/` | 存储、查询、调度和数据集适配 |
| `factor_library/` | 按资产组织的统一因子库 |
| `engine/` | 流水线、模型、回测与框架桥接 |
| `strategies/` | 可运行的策略实现 |
| `execution/` | 风控、仓位与交易通道 |
| `scripts/` | 数据采集和训练等命令行入口 |
| `examples/` | 教程与研究 notebook |
| `tests/` | 自动化测试 |
| `docs/` | 架构、数据源与开发文档 |
| `libs/zvt/` | 精简后的 ZVT 本地 fork |

完整的目录职责、数据边界和迁移说明见 [项目结构](docs/PROJECT_STRUCTURE.md)。文档索引见 [项目文档](docs/README.md)。因子开发规范见 [Factor Library](factor_library/README.md)。AmazingData 接口说明见 [AmazingData](docs/AmazingData.md)。

## 核心数据流

```text
AmazingData
    ↓
scripts/data_fetcher
    ↓
data_lake (Parquet) ←→ infra (DuckDB)
    ↓                       ↓
factor_library          engine / strategies
    ↓                       ↓
data_lake/factors       backtest / execution
```

`data_lake/`、`artifacts/`、原始期货 Level-2 数据和本地备份都属于工作区数据，不进入 Git。

## 核心入口

- `engine/pipeline.py`：五层 Alpha 流水线
- `factor_library/registry.py`：因子注册与实例化
- `factor_library/dag.py`：复合因子依赖调度
- `infra/storage.py`：Parquet / DuckDB 存储访问
- `scripts/data_fetcher/`：AmazingData 数据采集脚本
- `strategies/futures/alstm/`：期货 ALSTM 策略完整工作流

## 开发约定

- 新因子按 `factor_library/<asset>/<category>/` 放置，并使用 `@register_factor` 注册。
- 可复用逻辑进入 `engine/`、`infra/` 或 `factor_library/`；一次性执行入口进入 `scripts/`。
- 策略研究 notebook 跟随所属策略放置；`examples/` 只保留公共 API 的最小示例。
- 训练输出放在 `artifacts/<strategy>/`，行情与特征数据放在 `data_lake/`。
- 数据采集脚本必须在结束时调用 `ad.logout()`，AmazingData 的 `local_path` 必须传字符串。
- 策略不得创建自己的 `utils/`、`models/`、`factors/` 副本；共享能力统一进入主干模块。

## 当前状态

仓库正在从早期的单资产/实验目录迁移到多资产结构：

- 股票因子已迁入 `factor_library/stock/`。
- 期货高频因子已迁入 `factor_library/futures/`。
- 高频模型、回测和数据集工具已分别迁入 `engine/` 与 `infra/`。
- ALSTM 策略已统一归入 `strategies/futures/alstm/`，生成物归入 `artifacts/futures_alstm/`。
- 根目录 `indicator_py/` 暂作为期货因子迁移参考，待结果回归确认后再归档或删除。

在迁移完成前，避免在新旧两套目录同时修改同一实现。
