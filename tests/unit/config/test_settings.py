"""Settings loader — defaults, YAML overlay, env vars, precedence."""

from __future__ import annotations

import pytest

from dvf.config.loader import load_settings
from dvf.config.settings import FrameworkSettings
from dvf.core.exceptions import ConfigurationError


def test_pure_defaults():
    s = load_settings()
    assert s.engine.max_parallel == 8
    assert s.engine.tier_thresholds.small == 100_000
    assert s.canonical.delimiter == "|~|"
    assert s.catalog == "dvf_local"
    assert "html" in s.reporting.formats


def test_yaml_overlay_and_env_vars(tmp_path):
    (tmp_path / "environments").mkdir()
    (tmp_path / "framework.yaml").write_text(
        "engine:\n  max_parallel: 4\n  query_timeout_s: 100\n", encoding="utf-8")
    (tmp_path / "environments" / "prod.yaml").write_text(
        "environment: prod\ncatalog: dvf_prod\nengine:\n  max_parallel: 16\n",
        encoding="utf-8")

    s = load_settings(tmp_path, env="prod",
                      environ={"DVF__ENGINE__QUERY_TIMEOUT_S": "250",
                               "DVF__LOGGING__JSON": "false"})
    assert s.engine.max_parallel == 16          # env yaml beats base yaml
    assert s.engine.query_timeout_s == 250      # env var beats env yaml
    assert s.logging.json_format is False
    assert s.catalog == "dvf_prod"
    assert s.environment == "prod"


def test_overrides_win_over_everything(tmp_path):
    (tmp_path / "framework.yaml").write_text("engine:\n  max_parallel: 4\n",
                                             encoding="utf-8")
    s = load_settings(tmp_path, environ={"DVF__ENGINE__MAX_PARALLEL": "12"},
                      overrides={"engine": {"max_parallel": 2}})
    assert s.engine.max_parallel == 2


def test_unknown_environment_raises(tmp_path):
    (tmp_path / "framework.yaml").write_text("{}", encoding="utf-8")
    with pytest.raises(ConfigurationError):
        load_settings(tmp_path, env="nope")


def test_snapshot_json_round_trips():
    s = FrameworkSettings()
    snap = s.snapshot_json()
    assert FrameworkSettings.model_validate_json(snap).engine.max_parallel == \
        s.engine.max_parallel


def test_repo_config_files_load():
    """The shipped config/ directory parses for every environment."""
    from pathlib import Path
    config_dir = Path(__file__).resolve().parents[3] / "config"
    for env in ("dev", "qa", "prod"):
        s = load_settings(config_dir, env=env, environ={})
        assert s.catalog == f"dvf_{env}"
