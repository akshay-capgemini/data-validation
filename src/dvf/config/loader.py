"""Settings loader — YAML base + environment overlay + DVF__* env vars + overrides."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Mapping

import yaml

from dvf.config.settings import FrameworkSettings
from dvf.core.exceptions import ConfigurationError

ENV_PREFIX = "DVF__"


def deep_merge(base: dict, overlay: Mapping) -> dict:
    """Recursively merge overlay into base (overlay wins). Returns base."""
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _read_yaml(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        raise ConfigurationError(f"Config file not found: {path}")
    except yaml.YAMLError as exc:
        raise ConfigurationError(f"Invalid YAML in {path}: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigurationError(f"Top level of {path} must be a mapping")
    return data


def _env_var_overrides(environ: Mapping[str, str]) -> dict:
    """DVF__ENGINE__MAX_PARALLEL=16 -> {'engine': {'max_parallel': 16}}.

    Values are YAML-parsed so 'true', '16', '[html, json]' coerce naturally.
    """
    result: dict = {}
    for name, raw in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        path = [p.lower() for p in name[len(ENV_PREFIX):].split("__") if p]
        if not path:
            continue
        try:
            value = yaml.safe_load(raw)
        except yaml.YAMLError:
            value = raw
        node = result
        for part in path[:-1]:
            node = node.setdefault(part, {})
        node[path[-1]] = value
    return result


def load_settings(config_dir: str | Path | None = None,
                  env: str | None = None,
                  overrides: Mapping | None = None,
                  environ: Mapping[str, str] | None = None) -> FrameworkSettings:
    """Build effective FrameworkSettings.

    config_dir: directory containing framework.yaml and environments/.
                None -> pure defaults (unit tests / quick local use).
    env:        overlay environments/{env}.yaml when present.
    """
    data: dict = {}
    if config_dir is not None:
        config_dir = Path(config_dir)
        data = _read_yaml(config_dir / "framework.yaml")
        if env:
            env_file = config_dir / "environments" / f"{env}.yaml"
            if env_file.exists():
                deep_merge(data, _read_yaml(env_file))
            else:
                raise ConfigurationError(f"Unknown environment '{env}': {env_file} missing")
    deep_merge(data, _env_var_overrides(environ if environ is not None else os.environ))
    if overrides:
        deep_merge(data, dict(overrides))
    return FrameworkSettings(**data)
