# 项目文档

项目文档统一使用小写 `docs/`，不再创建 `Doc/`、`doc/` 或其他大小写变体。

## 项目与架构

- [项目结构与边界](PROJECT_STRUCTURE.md)

## AmazingData

- [SDK API Reference](AmazingData.md)
- [开发指南摘要](AmazingData开发指南摘要.md)
- [技术指标整理](AmazingData技术指标.md)
- `AmazingData开发手册.pdf`：本地官方手册，PDF 按仓库规则不进入 Git

## 数据处理

- [期货 Level-2 五档数据处理](data/future_level2_data_processing.md)

策略相关研究报告跟随所属策略保存，例如 ALSTM 报告位于 `strategies/futures/alstm/reports/`；机器生成的报告和图表位于 `artifacts/`。
