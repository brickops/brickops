import yaml
import logging

from functools import cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def get_config(key: str, default: str | None = None) -> Any | None:
    """Get a specific configuration value from the config file."""
    config = read_config()
    if config is None:
        return None
    return config.get(key, None)


@cache
def read_config() -> dict[Any, Any] | None:
    """Read the configuration from the YAML file."""
    # Define the path to the config file
    config_path = _find_config()
    if not config_path:
        return None
    return _read_yaml(config_path)


def _find_config() -> Path | None:
    """
    Look for a .brickopscfg folder in the current directory and each parent
    directory until reaching the system root or encountering an error.
    We cannot use .git folder to find root of repo, since it is not available in Databricks.

    Returns:
        Path: The full path to the first .brickopscfg folder found, or None if not found.
    """
    current_dir = Path.cwd()
    while str(current_dir) != current_dir.root:
        config_dir = current_dir / ".brickopscfg"
        if config_dir.exists():
            return config_dir / "config.yml"
        current_dir = current_dir.parent
    return None


def _read_yaml(config_path: Path) -> Any | None:
    with config_path.open("r") as file:
        return yaml.safe_load(file)
