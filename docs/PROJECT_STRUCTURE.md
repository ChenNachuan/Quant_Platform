# 项目结构与边界

这份文档定义代码、研究实验、运行产物和原始数据的放置规则。新内容按此规则放置，旧内容在回归验证后逐步迁移。

## 目录分层

```text
Quant/
├── config/                 # 可版本化的配置
├── infra/                  # 存储、查询、调度、数据集适配
├── factor_library/         # 多资产因子与通用算子
│   ├── stock/
│   ├── futures/
│   ├── option/
│   └── operators/
├── engine/                 # 策略流水线、模型、回测和框架桥接
│   ├── backtest/
│   ├── ensemble/
│   ├── factor/
│   ├── models/
│   ├── qlib_bridge/
│   ├── vectorbt_engine/
│   └── zvt_bridge/
├── strategies/             # 策略组装，不重复实现底层算子
├── execution/              # 风控、仓位和交易通道
├── scripts/                # 面向用户/调度器的可执行入口
├── examples/               # 最小示例与 notebook
├── tests/                  # 自动化测试
├── docs/                   # 项目文档
├── libs/zvt/               # 本地可编辑依赖
├── data_lake/              # 行情、因子、Qlib 数据（不入 Git）
└── artifacts/              # 模型、图表、报告（不入 Git）
```

## 模块放置决策

| 内容 | 位置 | 判断标准 |
|---|---|---|
| 数据读写、数据集转换 | `infra/` | 不包含交易观点，可被多个策略复用 |
| 因子或特征 | `factor_library/<asset>/` | 输入市场数据，输出可注册因子值 |
| 模型、回测器、组合器 | `engine/` | 是可复用的运行机制，而非单一策略 |
| 超参数和策略组装 | `strategies/<asset>/` | 表达“交易什么、如何组合” |
| 单次任务入口 | `scripts/<domain>/` | 包含 CLI、日期范围或调度逻辑 |
| 探索性代码 | `examples/` | 用于说明、复现或试验，不被生产模块 import |
| 训练权重、预测、图表 | `artifacts/` | 可通过程序重新生成 |
| 原始或派生数据 | `data_lake/` | 体积大、机器本地化，不进入 Git |

## 当前迁移区

早期高频实验已按职责拆分到主干，ALSTM 策略工作流归入 `strategies/futures/alstm/`：

- 因子：`factor_library/futures/`
- 模型：`engine/models/`
- 回测：`engine/backtest/`
- 集成：`engine/ensemble/`
- 数据集处理：`infra/`
- 策略参数与运行入口：`strategies/futures/alstm/`
- 策略 notebook 与报告：`strategies/futures/alstm/notebooks/`、`reports/`
- 运行产物：`artifacts/futures_alstm/`

迁移期的约束：

1. 新功能只加到主干目录；策略目录不复制通用模型、因子和工具。
2. 一个实现完成回归后，再删除对应的旧副本。
3. 迁移前后保留相同的时间对齐、交易成本和信号延迟假设。

## 数据与产物边界

- `data_lake/` 是平台标准数据根目录，脚本在 Docker 内使用 `/data/` 访问。
- 根目录 `期货level2五档数据/` 是本地原始数据，不应被 Python 包直接依赖；代码应通过配置获得路径。
- `artifacts/` 仅保存可再生成的训练和回测输出。
- 要长期保留的结论应整理到 `docs/`，不要只留在 notebook 输出中。

## 建议的后续收口

以下操作涉及大量删除或路径变更，应分批回归，不在本次低风险整理中直接执行：

1. 确认 `indicator_py/` 的因子结果与 `factor_library/futures/` 一致后归档旧目录。
2. 确认根目录 `indicator_py/` 的旧因子均已有主干实现后移除重复代码。
3. 确认精简后的 `libs/zvt/` 通过回测后删除本地备份。
4. 项目文档统一维护在小写 `docs/`；策略研究报告跟随所属策略维护。
