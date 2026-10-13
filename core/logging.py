"""Platform logging — JSONL machine-readable + console human-readable.

Python logging + JSON Lines (design/core/logging.md).  Only format and
write; no log aggregation or retrieval service.  The ``logs/runs/``
schema writers belong to ``ops/`` (S12) and are intentionally absent.
"""

import json
import logging
import os
from datetime import datetime, timezone

# Standard LogRecord attributes that are not user "extras".
# Includes ``message`` / ``asctime`` which Python's Formatter mutates onto
# the shared record when the console handler runs first (Finding 1).
_STANDARD_RECORD_ATTRS = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "message", "asctime",
    }
)

# Contract fields that extras must never overwrite (Finding 2).
_CONTRACT_FIELDS = frozenset({"ts", "level", "module", "func", "msg", "exception"})


class JsonFormatter(logging.Formatter):
    """One JSON object per line: ts / level / module / func / msg + extras.

    ``exception`` is present only when ``exc_info`` is set on the record.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {}
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS and key not in _CONTRACT_FIELDS:
                payload[key] = value
        # Fixed fields always win over extras (Finding 2).
        payload["ts"] = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        payload["level"] = record.levelname
        payload["module"] = record.module
        payload["func"] = record.funcName
        payload["msg"] = record.getMessage()
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    """Wire console (human-readable) + ``logs/app_{YYYYMMDD}.jsonl`` (JSONL).

    The ``logs/`` directory is created if it does not exist.  Repeated calls
    replace existing root handlers rather than stacking duplicates.
    """
    root = logging.getLogger()
    root.setLevel(level)

    # Replace handlers to avoid duplicates on repeated calls.
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    root.addHandler(console)

    os.makedirs("logs", exist_ok=True)
    fileh = logging.FileHandler(
        f"logs/app_{datetime.now():%Y%m%d}.jsonl", encoding="utf-8"
    )
    fileh.setFormatter(JsonFormatter())
    root.addHandler(fileh)
