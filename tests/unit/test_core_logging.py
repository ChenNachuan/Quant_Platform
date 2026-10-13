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


class TestFinding1MessageAsctimeFiltered:
    """Regression: console Formatter mutates record.message / .asctime onto
    the shared LogRecord; those must not leak as extras into JSONL (Finding 1)."""

    def test_message_and_asctime_not_in_output(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        setup_logging()
        logging.info("hello world", extra={"job_name": "nightly"})
        # Flush handlers so the file is written
        for h in logging.getLogger().handlers:
            h.flush()
        log_files = list((tmp_path / "logs").glob("app_*.jsonl"))
        assert len(log_files) == 1
        lines = log_files[0].read_text().strip().splitlines()
        assert len(lines) == 1
        payload = json.loads(lines[0])
        assert "message" not in payload, "message must not leak as extra"
        assert "asctime" not in payload, "asctime must not leak as extra"
        assert payload["msg"] == "hello world"
        assert payload["job_name"] == "nightly"


class TestFinding2FixedFieldsProtected:
    """Regression: extras must not overwrite contract fields or inject
    ``exception`` without ``exc_info`` (Finding 2)."""

    def test_extra_cannot_overwrite_fixed_fields(self):
        formatter = JsonFormatter()
        record = _make_record()
        record.funcName = "real_func"
        record.ts = "FAKE_TS"
        record.level = "FAKE_LEVEL"
        record.func = "FAKE_FUNC"
        payload = json.loads(formatter.format(record))
        assert payload["ts"] != "FAKE_TS"
        assert payload["level"] != "FAKE_LEVEL"
        assert payload["func"] != "FAKE_FUNC"
        assert payload["level"] == "INFO"
        assert payload["func"] == "real_func"

    def test_extra_cannot_inject_exception_without_exc_info(self):
        formatter = JsonFormatter()
        record = _make_record()
        record.exception = "fake traceback"
        payload = json.loads(formatter.format(record))
        assert "exception" not in payload

    def test_exception_still_present_with_exc_info(self):
        formatter = JsonFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            record = _make_record()
            record.exc_info = __import__("sys").exc_info()
        record.exception = "fake traceback"  # extra tries to override
        payload = json.loads(formatter.format(record))
        assert "ValueError" in payload["exception"]
        assert payload["exception"] != "fake traceback"


class TestFinding3NonSerializableExtra:
    """Regression: non-serializable extra must not silently drop the record."""

    def test_non_serializable_extra_stringified(self):
        formatter = JsonFormatter()
        record = _make_record()
        record.custom_obj = object()  # not JSON-serializable
        line = formatter.format(record)
        payload = json.loads(line)  # must not raise
        assert payload["msg"] == "hello"
        assert "custom_obj" in payload


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
