"""
vcs.repo.config — TOML configuration helpers.

Reading  → Python 3.11+ stdlib ``tomllib`` (read-only).
Writing  → A purpose-built minimal serialiser covering string, integer,
           boolean, and inline table types.  No external dependencies.

Config resolution order (highest → lowest priority):
  1. CLI flag (--repo, etc.) — handled by the dispatcher
  2. Repo-level  .vcs/config.toml
  3. User-level  ~/.config/vcs/config.toml
"""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from vcs.store.exceptions import ConfigError

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_CONFIG_NAME = "config.toml"
USER_CONFIG_DIR = Path.home() / ".config" / "vcs"
USER_CONFIG_PATH = USER_CONFIG_DIR / REPO_CONFIG_NAME


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------

def read_config(path: Path) -> dict[str, Any]:
    """
    Parse a TOML config file and return its contents as a dict.

    Returns an empty dict if the file does not exist.
    Raises :py:exc:`ConfigError` on parse failures.
    """
    if not path.exists():
        return {}
    try:
        with path.open("rb") as fh:
            return tomllib.load(fh)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"Failed to parse config {path}: {exc}") from exc


def resolve_config(repo_config_path: Path) -> dict[str, Any]:
    """
    Return a merged config dict (repo overrides user).

    Merges at the top-level key level; repo values win on conflict.
    """
    user_cfg = read_config(USER_CONFIG_PATH)
    repo_cfg = read_config(repo_config_path)
    merged = {**user_cfg, **repo_cfg}
    return merged


def get_value(config: dict[str, Any], key: str) -> Any:
    """
    Retrieve a dot-separated *key* from *config*.

    Example::

        get_value(cfg, "core.author")  # → cfg["core"]["author"]

    Raises :py:exc:`ConfigError` if any intermediate key is missing.
    """
    parts = key.split(".")
    node: Any = config
    for part in parts:
        if not isinstance(node, dict) or part not in node:
            raise ConfigError(f"Config key {key!r} not found.")
        node = node[part]
    return node


# ---------------------------------------------------------------------------
# Writing — minimal TOML serialiser
# ---------------------------------------------------------------------------

def _toml_value(value: Any) -> str:
    """Serialise a scalar or inline table to a TOML value string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, str):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        return f'"{escaped}"'
    if isinstance(value, dict):
        pairs = ", ".join(f"{k} = {_toml_value(v)}" for k, v in value.items())
        return "{" + pairs + "}"
    raise ConfigError(f"Unsupported config value type: {type(value).__name__!r}")


def _serialise_toml(data: dict[str, Any], _indent: int = 0) -> str:
    """Recursively serialise a dict to TOML text."""
    lines: list[str] = []
    scalars = {k: v for k, v in data.items() if not isinstance(v, dict)}
    tables = {k: v for k, v in data.items() if isinstance(v, dict)}

    for key, value in scalars.items():
        lines.append(f"{key} = {_toml_value(value)}")

    for key, sub in tables.items():
        lines.append(f"\n[{key}]")
        for sub_key, sub_val in sub.items():
            lines.append(f"{sub_key} = {_toml_value(sub_val)}")

    return "\n".join(lines) + ("\n" if lines else "")


def write_config(path: Path, data: dict[str, Any]) -> None:
    """
    Write *data* as TOML to *path* atomically.

    Raises :py:exc:`ConfigError` on serialisation or I/O errors.
    """
    try:
        content = _serialise_toml(data)
    except ConfigError:
        raise
    except Exception as exc:
        raise ConfigError(f"Failed to serialise config: {exc}") from exc

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".toml.tmp")
    try:
        tmp.write_text(content, encoding="utf-8")
        tmp.replace(path)
    except OSError as exc:
        raise ConfigError(f"Failed to write config to {path}: {exc}") from exc


def set_value(path: Path, key: str, value: Any) -> None:
    """
    Set a dot-separated *key* to *value* in the TOML file at *path*.

    If the file does not exist it is created.  Nested keys create nested
    TOML tables automatically.
    """
    config = read_config(path)
    parts = key.split(".")
    node = config
    for part in parts[:-1]:
        if part not in node or not isinstance(node[part], dict):
            node[part] = {}
        node = node[part]
    node[parts[-1]] = value
    write_config(path, config)
