"""平台日志模块：控制台可读输出与 JSONL 机器可读输出。

仅负责格式化与落盘（见 ``design/core/logging.md``），不含日志聚合检索或 ``logs/runs/`` 写入器。
"""

import json
import logging
import os
from datetime import datetime, timezone

# 标准 LogRecord 属性（排除在 extras 之外）；含控制台 Handler 先行格式化时注入的 message/asctime
_STANDARD_RECORD_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "message",
        "asctime",
    }
)

# 契约固定字段，extras 不可覆盖
_CONTRACT_FIELDS = frozenset({"ts", "level", "module", "func", "msg", "exception"})


class JsonFormatter(logging.Formatter):
    """单行 JSON 格式化器：输出 ts / level / module / func / msg 及 extras；仅存在异常时附带 exception。"""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {}
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS and key not in _CONTRACT_FIELDS:
                payload[key] = value
        # 固定契约字段优先级高于 extras
        payload["ts"] = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).isoformat()
        payload["level"] = record.levelname
        payload["module"] = record.module
        payload["func"] = record.funcName
        payload["msg"] = record.getMessage()
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    """配置控制台输出与 ``logs/app_{YYYYMMDD}.jsonl``（自动建目录，重复调用先关闭并替换旧 Handler）。"""
    root = logging.getLogger()
    root.setLevel(level)

    # 重复调用时先关闭并移除旧 Handler，避免仅 clear() 导致 FileHandler 文件描述符泄漏
    for handler in root.handlers[:]:
        root.removeHandler(handler)
        handler.close()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(console)

    os.makedirs("logs", exist_ok=True)
    fileh = logging.FileHandler(
        f"logs/app_{datetime.now():%Y%m%d}.jsonl", encoding="utf-8"
    )
    fileh.setFormatter(JsonFormatter())
    root.addHandler(fileh)
