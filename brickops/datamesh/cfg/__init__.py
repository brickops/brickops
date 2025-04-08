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

    # Define the path to the config file
    config_path = _findconfig()
    if not config_path:
        return None

    _config = _read_yaml(config_path)
    return _config


def _findconfig() -> str | None:
    """
    Look for a .brickopscfg folder in the current directory and each parent
    directory until reaching the system root or encountering an error.
    We cannot use .git folder to find root of repo, since it is not available in Databricks.

    Returns:
        str: The full path to the first .brickopscfg folder found, or None if not found.
    """
    current_dir = os.path.abspath(os.getcwd())
    while True:
        config_dir = os.path.join(current_dir, ".brickopscfg")

        # Check if .brickopscfg exists and is a directory
        if os.path.isdir(config_dir):
            return os.path.join(config_dir, "config.yml")

        # Check if we've reached the root directory
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # We've reached the root
            return None

        # Move up to the parent directory
        current_dir = parent_dir

    # This line shouldn't be reached under normal circumstances
    return None


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
