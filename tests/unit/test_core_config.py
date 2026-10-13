"""Tests for core.config — three-layer loaders, merge semantics, snapshot."""

import pytest

from core.config import config_snapshot, load_config, load_secrets, load_strategy_config


class TestLoadConfig:
    def test_loads_toml(self, tmp_path):
        cfg_file = tmp_path / "config.toml"
        cfg_file.write_text('log_level = "INFO"\n[data]\ndata_lake_dir = "data_lake"\n')
        cfg = load_config(str(cfg_file))
        assert cfg["log_level"] == "INFO"
        assert cfg["data"]["data_lake_dir"] == "data_lake"

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "nonexistent.toml"))


class TestLoadStrategyConfig:
    def _write_strategy(self, tmp_path, alpha_type_id: str, content: str):
        strat_dir = tmp_path / "config" / "strategies"
        strat_dir.mkdir(parents=True)
        (strat_dir / f"{alpha_type_id}.toml").write_text(content)

    def test_strategy_overrides_global(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_strategy(tmp_path, "mom5", 'lookback = 5\nmode = "backtest"\n')
        global_cfg = {"lookback": 20, "mode": "backtest"}
        result = load_strategy_config("mom5", global_cfg=global_cfg)
        assert result["lookback"] == 5  # strategy wins

    def test_global_only_keys_preserved(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_strategy(tmp_path, "mom5", "lookback = 5\n")
        global_cfg = {"lookback": 20, "log_level": "INFO"}
        result = load_strategy_config("mom5", global_cfg=global_cfg)
        assert result["log_level"] == "INFO"  # global-only key survives

    def test_strategy_only_keys_merged_in(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        self._write_strategy(tmp_path, "mom5", "lookback = 5\nalpha_id = 42\n")
        global_cfg = {"lookback": 20}
        result = load_strategy_config("mom5", global_cfg=global_cfg)
        assert result["alpha_id"] == 42  # strategy-only key joins

    def test_no_global_cfg_loads_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        (config_dir / "config.toml").write_text('log_level = "DEBUG"\n')
        self._write_strategy(tmp_path, "mom5", "lookback = 5\n")
        result = load_strategy_config("mom5")
        assert result["log_level"] == "DEBUG"
        assert result["lookback"] == 5


class TestLoadSecrets:
    def test_parses_key_value(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("TUSHARE_TOKEN=abc123\nPROXY_URL=http://proxy:8080\n")
        secrets = load_secrets(str(env_file))
        assert secrets == {"TUSHARE_TOKEN": "abc123", "PROXY_URL": "http://proxy:8080"}

    def test_skips_comments_and_blank_lines(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text(
            "# comment line\n\nKEY_A=val_a\n   \n# another\nKEY_B=val_b\n"
        )
        secrets = load_secrets(str(env_file))
        assert secrets == {"KEY_A": "val_a", "KEY_B": "val_b"}

    def test_handles_equals_in_value(self, tmp_path):
        env_file = tmp_path / ".env"
        env_file.write_text("DSN=postgres://user:pass=word@host/db\n")
        secrets = load_secrets(str(env_file))
        assert secrets["DSN"] == "postgres://user:pass=word@host/db"


class TestConfigSnapshot:
    def test_format_path_at_hash(self, tmp_path):
        cfg_file = tmp_path / "config.toml"
        cfg_file.write_text('log_level = "INFO"\n')
        snap = config_snapshot(str(cfg_file))
        assert snap.startswith(f"{cfg_file}@")
        file_hash = snap.split("@", 1)[1]
        assert len(file_hash) == 64  # sha256 hex digest

    def test_secrets_not_in_snapshot(self, tmp_path):
        """Red line 8: load_secrets values never appear in config_snapshot."""
        env_file = tmp_path / ".env"
        env_file.write_text("API_KEY=super_secret_value_xyz\n")
        cfg_file = tmp_path / "config.toml"
        cfg_file.write_text('log_level = "INFO"\n')
        secrets = load_secrets(str(env_file))
        snap = config_snapshot(str(cfg_file))
        for value in secrets.values():
            assert value not in snap

    def test_snapshot_changes_with_content(self, tmp_path):
        cfg_file = tmp_path / "config.toml"
        cfg_file.write_text("a = 1\n")
        snap1 = config_snapshot(str(cfg_file))
        cfg_file.write_text("a = 2\n")
        snap2 = config_snapshot(str(cfg_file))
        assert snap1 != snap2
