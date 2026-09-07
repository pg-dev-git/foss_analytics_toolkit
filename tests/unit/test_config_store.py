"""Tests for the persistent config store (Phase 2 of api-version plan)."""

import json
from pathlib import Path

from asftool.core.config_store import ConfigStore, UserConfig, get_user_config


class TestUserConfig:
    def test_defaults_to_none(self):
        cfg = UserConfig()
        assert cfg.sf_api_version is None

    def test_accepts_explicit_value(self):
        cfg = UserConfig(sf_api_version="v68.0")
        assert cfg.sf_api_version == "v68.0"

    def test_extra_keys_ignored(self):
        """UserConfig should silently drop unknown keys, not raise."""
        cfg = UserConfig(sf_api_version="v68.0", some_random_field="x")
        assert cfg.sf_api_version == "v68.0"
        assert not hasattr(cfg, "some_random_field")


class TestConfigStoreLoad:
    def test_missing_file_returns_empty(self, tmp_path: Path):
        store = ConfigStore(path=tmp_path / "missing.json")
        cfg = store.load()
        assert cfg.sf_api_version is None

    def test_corrupt_file_returns_empty(self, tmp_path: Path):
        bad = tmp_path / "bad.json"
        bad.write_text("not valid json{{", encoding="utf-8")
        store = ConfigStore(path=bad)
        cfg = store.load()
        assert cfg.sf_api_version is None  # treated as empty

    def test_invalid_value_returns_empty(self, tmp_path: Path):
        """A value that fails Pydantic validation should not crash the app."""
        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"sf_api_version": "60.0"}), encoding="utf-8")
        store = ConfigStore(path=bad)
        cfg = store.load()
        assert cfg.sf_api_version is None

    def test_load_round_trip(self, tmp_path: Path):
        p = tmp_path / "config.json"
        store = ConfigStore(path=p)
        store.save(UserConfig(sf_api_version="v67.0"))
        cfg = store.load()
        assert cfg.sf_api_version == "v67.0"


class TestConfigStoreSave:
    def test_save_creates_parent(self, tmp_path: Path):
        p = tmp_path / "nested" / "deeper" / "config.json"
        store = ConfigStore(path=p)
        store.save(UserConfig(sf_api_version="v68.0"))
        assert p.exists()

    def test_save_excludes_none(self, tmp_path: Path):
        p = tmp_path / "config.json"
        store = ConfigStore(path=p)
        store.save(UserConfig(sf_api_version=None))
        # None fields are stripped — file should be empty dict.
        data = json.loads(p.read_text())
        assert data == {}

    def test_save_is_atomic_no_leftover_temp(self, tmp_path: Path):
        """After a successful save there should be no .tmp file in the dir."""
        p = tmp_path / "config.json"
        store = ConfigStore(path=p)
        store.save(UserConfig(sf_api_version="v68.0"))
        leftovers = list(tmp_path.glob(".config_*.json.tmp"))
        assert leftovers == []


class TestConfigStoreReset:
    def test_reset_deletes_file(self, tmp_path: Path):
        p = tmp_path / "config.json"
        p.write_text("{}", encoding="utf-8")
        store = ConfigStore(path=p)
        store.reset()
        assert not p.exists()

    def test_reset_missing_file_is_noop(self, tmp_path: Path):
        store = ConfigStore(path=tmp_path / "missing.json")
        store.reset()  # should not raise


class TestConfigStoreUpdate:
    def test_update_loads_then_saves(self, tmp_path: Path):
        p = tmp_path / "config.json"
        store = ConfigStore(path=p)
        result = store.update(sf_api_version="v65.0")
        assert result.sf_api_version == "v65.0"
        # Re-read from disk to confirm.
        cfg2 = store.load()
        assert cfg2.sf_api_version == "v65.0"


class TestGetUserConfigCaching:
    def test_returns_same_object(self, tmp_path: Path, monkeypatch):
        # Point the module-level default at a temp file.
        monkeypatch.setattr(ConfigStore, "DEFAULT_PATH", tmp_path / "config.json")
        (tmp_path / "config.json").write_text(
            json.dumps({"sf_api_version": "v68.0"}), encoding="utf-8"
        )
        # First call loads from disk.
        a = get_user_config()
        assert a.sf_api_version == "v68.0"
        # Mutate the file on disk; second call still returns the cached value.
        (tmp_path / "config.json").write_text(
            json.dumps({"sf_api_version": "v99.0"}), encoding="utf-8"
        )
        b = get_user_config()
        assert b.sf_api_version == "v68.0"  # cached
