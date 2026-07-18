# Moving Average Crossover

`alpha_001` 双均线策略。当前使用模拟价格演示 `engine/vectorbt_engine` 的策略编译与回测接口。

```bash
uv run python -m strategies.stock.ma_crossover.strategy
```

该策略没有独立模型和数据工具；信号组装保留在本目录，共享执行逻辑由 `engine/vectorbt_engine/` 提供。
