import os
import yaml
import logging

from typing import Any

logger = logging.getLogger(__name__)


_config_read = False
_config = None


def get_config(key: str, default: str | None = None) -> Any | None:
    """Get a specific configuration value from the config file."""
    config = read_config()
    if config is None:
        return None
    return config.get(key, None)


def read_config() -> dict[Any, Any] | None:
    """Read the configuration from the YAML file."""
    global _config, _config_read
    if _config_read:
        return _config
    _config_read = True
    # Find the repository root
    repo_root = _find_repo_root()
    if not repo_root:
        return None

    # Define the path to the config file
    config_path = _config_path(repo_root)
    _config = _read_yaml(config_path)
    return _config


def _config_path(repo_root: str) -> str:
    """Construct the path to the config file. Useful to easily mock in tests."""
    return os.path.join(repo_root, ".brickopscfg", "config.yml")


def _read_yaml(config_path: str) -> Any | None:
    # Check if the config file exists
    if not os.path.exists(config_path):
        logger.info(f"Config file not found at {config_path}")
        return None
    ret = None
    # Read the YAML file
    with open(config_path, "r") as file:
        ret = yaml.safe_load(file)
    return ret


def _find_repo_root() -> str | None:
    current_dir = os.path.abspath(os.path.dirname(__file__))

    while current_dir != os.path.dirname(current_dir):  # Stop at the filesystem root
        if os.path.isdir(os.path.join(current_dir, ".git")):
            return current_dir
        current_dir = os.path.dirname(current_dir)

    logger.info("Repository not found")
    return None
