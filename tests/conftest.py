import yaml
import pytest
from typing import Any
from pathlib import Path


def read_config(cfg_path: Path) -> dict[str, Any] | Any:
    """Read the configuration from the YAML file."""
    with cfg_path.open("r") as file:
        return yaml.safe_load(file)


@pytest.fixture
def brickops_default_config() -> dict[str, Any] | Any:
    return read_config(Path(__file__).parent / "datamesh/fixtures/configs/default.yml")


@pytest.fixture
def brickops_fullmesh_config() -> dict[str, Any] | Any:
    return read_config(Path(__file__).parent / "datamesh/fixtures/configs/fullmesh.yml")
