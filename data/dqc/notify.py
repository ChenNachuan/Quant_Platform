"""DQC Webhook 轻量告警通知。

读取环境变量 ``DQC_WEBHOOK_URL`` 发送飞书/钉钉/Slack 文本消息；
未配置 URL 或任意网络/序列化异常均静默忽略，绝不阻塞或中断主入库流程。
"""

from __future__ import annotations

import os
from typing import Any

import requests

__all__ = ["notify_dqc_event"]


def notify_dqc_event(
    level: str,
    message: str,
    detail: dict[str, Any],
    run_id: str = "",
) -> None:
    """发送 DQC 告警事件至 ``DQC_WEBHOOK_URL``；未设置或发送失败时静默返回。"""
    webhook_url = os.environ.get("DQC_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return
    prefix = f"[DQC {level}][run_id={run_id}]" if run_id else f"[DQC {level}]"
    payload = {
        "msg_type": "text",
        "content": {"text": f"{prefix} {message}\n{detail}"},
    }
    try:
        requests.post(webhook_url, json=payload, timeout=5)
    except Exception:  # noqa: BLE001 — 通知失败绝不阻塞主流程
        pass
