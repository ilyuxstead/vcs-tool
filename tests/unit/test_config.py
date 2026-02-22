"""
tests/unit/test_config.py — TOML config read/write unit tests.

Covers: read_config, write_config, set_value, get_value, resolution order.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from vcs.repo.config import (
    get_value,
    read_config,
    set_value,
    write_config,
)
from vcs.store.exceptions import ConfigError


class TestReadConfig:
    def test_missing_file_returns_empty(self, tmp_path: Path):
        cfg = read_config(tmp_path / "nonexistent.toml")
        assert cfg == {}

    def test_read_simple(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        p.write_text('[core]\nauthor = "Alice"\n', encoding="utf-8")
        cfg = read_config(p)
        assert cfg["core"]["author"] == "Alice"

    def test_read_invalid_toml_raises(self, tmp_path: Path):
        p = tmp_path / "bad.toml"
        p.write_text("this = [not valid toml\n", encoding="utf-8")
        with pytest.raises(ConfigError):
            read_config(p)


class TestWriteConfig:
    def test_write_and_read_back(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        data = {"core": {"author": "Bob", "bare": False}}
        write_config(p, data)
        cfg = read_config(p)
        assert cfg["core"]["author"] == "Bob"
        assert cfg["core"]["bare"] is False

    def test_write_integer(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        write_config(p, {"limits": {"max_size": 100}})
        cfg = read_config(p)
        assert cfg["limits"]["max_size"] == 100

    def test_write_boolean_true(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        write_config(p, {"core": {"bare": True}})
        cfg = read_config(p)
        assert cfg["core"]["bare"] is True

    def test_write_creates_parent_dirs(self, tmp_path: Path):
        p = tmp_path / "deep" / "nested" / "config.toml"
        write_config(p, {"a": "b"})
        assert p.exists()

    def test_write_unsupported_type_raises(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        with pytest.raises(ConfigError):
            write_config(p, {"bad": [1, 2, 3]})  # lists not supported

    def test_write_atomic(self, tmp_path: Path):
        """No .tmp file should remain after a successful write."""
        p = tmp_path / "config.toml"
        write_config(p, {"key": "value"})
        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []


class TestGetValue:
    def test_simple_key(self):
        cfg = {"author": "Alice"}
        assert get_value(cfg, "author") == "Alice"

    def test_nested_key(self):
        cfg = {"core": {"author": "Bob"}}
        assert get_value(cfg, "core.author") == "Bob"

    def test_missing_key_raises(self):
        cfg = {"core": {}}
        with pytest.raises(ConfigError):
            get_value(cfg, "core.missing")

    def test_deeply_nested(self):
        cfg = {"a": {"b": {"c": 42}}}
        assert get_value(cfg, "a.b.c") == 42


class TestSetValue:
    def test_set_new_key(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        set_value(p, "core.author", "Charlie")
        cfg = read_config(p)
        assert cfg["core"]["author"] == "Charlie"

    def test_overwrite_existing(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        write_config(p, {"core": {"author": "Old"}})
        set_value(p, "core.author", "New")
        cfg = read_config(p)
        assert cfg["core"]["author"] == "New"

    def test_set_boolean(self, tmp_path: Path):
        p = tmp_path / "config.toml"
        set_value(p, "core.bare", True)
        cfg = read_config(p)
        assert cfg["core"]["bare"] is True
