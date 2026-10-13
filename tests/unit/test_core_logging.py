"""Tests for core.logging — JsonFormatter field contract and setup_logging."""

import json
import logging

from core.logging import JsonFormatter, setup_logging


def _make_record(**kwargs) -> logging.LogRecord:
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    for k, v in kwargs.items():
        setattr(record, k, v)
    return record


class TestJsonFormatter:
    def test_five_fixed_fields_present(self):
        formatter = JsonFormatter()
        line = formatter.format(_make_record())
        payload = json.loads(line)
        for field in ("ts", "level", "module", "func", "msg"):
            assert field in payload, f"missing fixed field: {field}"
        assert payload["level"] == "INFO"
        assert payload["msg"] == "hello"

    def test_every_line_is_valid_json(self):
        formatter = JsonFormatter()
        for lvl in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR):
            record = _make_record()
            record.levelno = lvl
            record.levelname = logging.getLevelName(lvl)
            line = formatter.format(record)
            json.loads(line)  # raises on invalid JSON

    def test_exception_only_with_exc_info(self):
        formatter = JsonFormatter()
        # No exc_info -> no "exception" key
        payload = json.loads(formatter.format(_make_record()))
        assert "exception" not in payload

        # With exc_info -> "exception" key present
        try:
            raise ValueError("boom")
        except ValueError:
            record = _make_record()
            record.exc_info = __import__("sys").exc_info()
        payload = json.loads(formatter.format(record))
        assert "exception" in payload
        assert "ValueError" in payload["exception"]

    def test_extras_included(self):
        formatter = JsonFormatter()
        record = _make_record()
        record.job_name = "nightly"
        record.duration_ms = 42
        payload = json.loads(formatter.format(record))
        assert payload["job_name"] == "nightly"
        assert payload["duration_ms"] == 42

    def test_ts_is_utc_iso(self):
        formatter = JsonFormatter()
        payload = json.loads(formatter.format(_make_record()))
        ts = payload["ts"]
        assert "T" in ts  # ISO 8601 separator
        assert "+" in ts or "Z" in ts  # UTC offset


class TestSetupLogging:
    def test_creates_log_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup_logging()
        log_files = list((tmp_path / "logs").glob("app_*.jsonl"))
        assert len(log_files) == 1

    def test_console_and_file_handlers(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup_logging()
        root = logging.getLogger()
        handler_types = [type(h).__name__ for h in root.handlers]
        assert "StreamHandler" in handler_types
        assert "FileHandler" in handler_types

    def test_file_handler_uses_json_formatter(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup_logging()
        root = logging.getLogger()
        file_handlers = [h for h in root.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 1
        assert isinstance(file_handlers[0].formatter, JsonFormatter)
