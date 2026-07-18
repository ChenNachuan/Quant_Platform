# Examples

此目录只保留用于理解公共 API 的小型示例，不放完整策略、训练产物或长期研究 notebook。

- `test_dag_dependencies.py`：因子 DAG 的最小演示
- `batch_backtest_example.py`：批量因子回测调用示例
- `zvt_backtest_example.py`：ZVT Bridge 调用示例

完整策略必须放在 `strategies/<asset>/<strategy>/`；探索 notebook 跟随所属策略放置。若某个示例开始拥有独立配置、训练流程和产物目录，它已经是策略，不再属于 `examples/`。
