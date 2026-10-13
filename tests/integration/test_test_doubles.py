"""Integration-layer tests: exercise the test-double fixtures end to end.

These verify the doubles that later pipeline tests (S3–S7) will rely on.
The three design-doc integration scenarios (data pipeline / factor pipeline /
backtest loop) land at S3–S7 — their subjects do not exist yet.
"""

import socket
from datetime import date, datetime

import pytest

from core.testing import TIME_BUDGET_SECONDS

pytestmark = pytest.mark.integration


class TestClockDouble:
    """Clock double returns deterministic dates — never ``datetime.now()``."""

    def test_fixed_dates_length_and_type(self, fixed_dates):
        assert len(fixed_dates) == 60
        assert all(isinstance(d, date) for d in fixed_dates)

    def test_clock_today_matches_first_fixed_date(self, clock, fixed_dates):
        assert clock.today() == fixed_dates[0]
        assert clock.today() == date(2024, 1, 2)

    def test_clock_advance_walks_the_list(self, clock, fixed_dates):
        clock.advance()
        assert clock.today() == fixed_dates[1]
        clock.advance()
        assert clock.today() == fixed_dates[2]

    def test_clock_now_is_deterministic(self, clock):
        now = clock.now()
        assert isinstance(now, datetime)
        assert now == datetime(2024, 1, 2, 12, 0, 0)

    def test_clock_advance_stays_bounded(self, clock, fixed_dates):
        for _ in range(len(fixed_dates) + 10):
            clock.advance()
        assert clock.today() == fixed_dates[-1]


class TestDiskDoubles:
    """DuckDB / disk double: tmp_path + in-memory DataFrame."""

    def test_data_lake_dir_is_isolated_tmp(self, data_lake_dir, tmp_path):
        assert data_lake_dir.exists()
        assert data_lake_dir.is_dir()
        assert data_lake_dir.parent == tmp_path

    def test_memory_frame_returns_dataframe(self, memory_frame):
        df = memory_frame({"close": [1.0, 2.0, 3.0]})
        assert list(df["close"]) == [1.0, 2.0, 3.0]
        assert len(df) == 3


class TestNetworkGuard:
    """Hard rule: the autouse guard blocks outbound socket connections."""

    def test_socket_connect_raises(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(RuntimeError, match="network access is forbidden"):
                s.connect(("127.0.0.1", 1))
        finally:
            s.close()

    def test_create_connection_raises(self):
        with pytest.raises(RuntimeError, match="network access is forbidden"):
            socket.create_connection(("127.0.0.1", 1), timeout=0.1)


class TestEnvGuard:
    """Hard rule: the real ``config/.env`` is never opened."""

    def test_real_env_open_raises(self):
        from pathlib import Path

        real_env = Path(__file__).resolve().parent.parent.parent / "config" / ".env"
        with pytest.raises(RuntimeError, match="forbidden"):
            open(real_env)

    def test_fake_env_in_tmp_path_is_fine(self, tmp_path):
        fake = tmp_path / "config" / ".env"
        fake.parent.mkdir(parents=True)
        fake.write_text("API_KEY=test123\n")
        assert fake.read_text().strip() == "API_KEY=test123"


class TestBudgetsRegistered:
    """The three layer budgets are the documented values."""

    def test_budgets_match_design(self):
        assert TIME_BUDGET_SECONDS == {
            "unit": 10,
            "integration": 60,
            "golden": 300,
        }
