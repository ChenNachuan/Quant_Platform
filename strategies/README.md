# 策略目录约定

`strategies/` 只放“如何把平台能力组合成一套可运行策略”的代码。通用因子、模型、回测器和数据访问不能复制到某个策略目录中。

## 标准结构

```text
strategies/<asset>/<strategy>/
├── README.md          # 策略假设、数据、运行顺序和结果口径
├── config.py          # 该策略唯一的默认参数与路径
├── data.py            # 薄数据适配层；通用读取能力仍放 infra/
├── train.py           # 训练入口
├── backtest.py        # 仅在需要策略特有组装时存在
├── reporting.py       # 策略报告入口
├── notebooks/         # 探索过程，不作为正式运行入口
└── reports/           # 人工整理后的结论，不放生成图表和权重
```

并非每个策略都要创建所有文件。只有出现对应职责时才增加文件，禁止创建自己的 `utils/`、`models/`、`factors/` 或 `artifacts/` 副本。

## 共享能力的位置

| 能力 | 唯一位置 |
|---|---|
| 因子与特征 | `factor_library/<asset>/` |
| 模型实现 | `engine/models/` |
| 回测与组合机制 | `engine/backtest/`、`engine/ensemble/` |
| 存储与数据集转换 | `infra/` |
| 运行产物 | `artifacts/<strategy>/` |
| 市场数据 | `data_lake/` |

## 工作流

1. 数据采集脚本写入 `data_lake/`。
2. `factor_library/` 和 `infra/` 生成可复用特征/数据集。
3. 策略目录中的 `train.py` 读取配置并调用 `engine/models/`。
4. 策略目录组装 `engine/backtest/` 完成验证。
5. 生成物写入 `artifacts/<strategy>/`，结论写入策略的 `reports/`。
6. notebook 验证成熟后，将可复用逻辑下沉到主干模块；notebook 不被其他代码 import。
