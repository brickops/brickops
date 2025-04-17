import yaml
import pytest
from typing import Any
from pathlib import Path


# Setup config files
# Get the directory of the current file
current_dir = Path(__file__).parent
# Append the path to the config file
BRICKOPS_DEFAULT_CONFIG_PATH = current_dir / "datamesh/fixtures/configs/default.yml"
# "fullmesh" means using org as well as domain and project
BRICKOPS_FULLMESH_CONFIG_PATH = current_dir / "datamesh/fixtures/configs/fullmesh.yml"


def read_config(cfg_path: Path) -> dict[str, Any] | Any:
    """Read the configuration from the YAML file."""
    with cfg_path.open("r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def brickops_fullmesh_config_path() -> Path:
    return BRICKOPS_FULLMESH_CONFIG_PATH


@pytest.fixture
def brickops_default_config() -> dict[str, Any] | Any:
    return read_config(BRICKOPS_DEFAULT_CONFIG_PATH)


@pytest.fixture
def brickops_fullmesh_config() -> dict[str, Any] | Any:
    return read_config(BRICKOPS_FULLMESH_CONFIG_PATH)


def pytest_configure(config):  # type: ignore[no-untyped-def]
    pytest.BRICKOPS_DEFAULT_CONFIG = read_config(BRICKOPS_DEFAULT_CONFIG_PATH)  # type: ignore[attr-defined]
    pytest.BRICKOPS_FULLMESH_CONFIG = read_config(BRICKOPS_FULLMESH_CONFIG_PATH)  # type: ignore[attr-defined]
