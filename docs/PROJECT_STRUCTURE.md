# 项目结构与边界

这份文档定义代码、研究实验、运行产物和原始数据的放置规则。新内容按此规则放置，旧内容在回归验证后逐步迁移。

## 一、目录分层

```text
Quant/
├── config/                 # 可版本化的配置
│ 
├── core/                   # types·config·logging·registry·asset_rules 共享工具
│ 
├── data/                   # 采集、存储、查询、DQC 数据域
│   ├── adapters/           # 多源适配（QDCData / Amazingdata / StrightFlush / Tushare）
│   ├── normalizers/        # 源归一化
│   ├── dqc/                # DQC 规则、检查、引擎、严重度
│   ├── execution/          # 风险审计产物（risk_audit.jsonl）
│   └── ops/                # 运行指标/告警产物（不入 Git）
│ 
├── dqc/                    # 命名空间垫片（__path__ → data/dqc/）
│ 
├── execution/              # 组合、订单、风控、账本、通道
│   └── router/             # 通道连接器
│ 
├── ops/                    # 调度、监控、日报、告警
│ 
├── scripts/                # 面向用户/调度器的可执行入口
│   ├── bootstrap/
│   └── data_processing/
│ 
├── research/               # 因子接口/运行时/存储、信号、评价、治理、ML 信号
│   └── reports/
│ 
├── factor_library/         # 多资产因子
│   ├── futures/            # 期货因子
│   └── option/             # 期权因子
│ 
├── strategies/             # 策略组装，不重复实现底层算子
│   └── futures/            # 期货策略
│ 
├── tests/                  # 自动化测试
│   ├── unit/
│   ├── integration/
│   └── golden/
│ 
├── docs/                   # 项目文档
│   ├── design/             # 架构总纲与五域模块文档
│   └── data_source/        # 数据源参考文档
└── logs/                   # 运行日志（不入 Git）
```
