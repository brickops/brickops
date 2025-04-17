import pytest
import pytest_mock
from typing import Any
import brickops
from brickops.datamesh.cfg import (
    get_config,
    read_config,
    find_config,
)
from pathlib import Path


@pytest.fixture
def reset_config_state() -> Any:
    """Reset the module's global state between tests."""
    read_config.cache_clear()
    yield


@pytest.fixture
def temp_repo_with_config(tmp_path: Path) -> Any:
    """Create a temporary directory structure with a .brickopscfg/config.yml file."""
    # Create the .brickopscfg directory
    config_dir = tmp_path / ".brickopscfg"
    config_dir.mkdir(parents=True, exist_ok=True)

    # Create a config.yml file with the specified content
    config_path = config_dir / "config.yml"
    config_content = """naming:
  job:
    prod: "{org}_{domain}_{project}_{env}"
    other: "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitshortref}"
  pipeline:
    prod: "{org}_{domain}_{project}_{env}_dlt"
    other: "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitshortref}_dlt"
  catalog:
    prod: "{domain}"
    other: "{domain}"
  db:
    prod: "{db}"
    other: "{env}_{username}_{gitbranch}_{gitshortref}_{db}"
"""
    config_path.write_text(config_content)

    # Return the path to the temp directory
    yield tmp_path


def test_get_config_returns_value(mocker: pytest_mock.plugin.MockerFixture) -> None:
    # Setup
    mocker.patch(
        "brickops.datamesh.cfg.read_config", return_value={"test_key": "test_value"}
    )

    # Execute
    result = get_config("test_key")

    # Verify
    assert result == "test_value"
    brickops.datamesh.cfg.read_config.assert_called_once()  # type: ignore[attr-defined]


def test_get_config_returns_none_for_missing_key(
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    # Setup
    mocker.patch(
        "brickops.datamesh.cfg.read_config",
        return_value={"other_key": "other_value"},
    )

    # Execute
    result = get_config("test_key")

    # Verify
    assert result is None
    brickops.datamesh.cfg.read_config.assert_called_once()  # type: ignore[attr-defined]


def test_get_config_returns_none_when_config_is_none(
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    # Setup
    mocker.patch("brickops.datamesh.cfg.read_config", return_value=None)

    # Execute
    result = get_config("test_key")

    # Verify
    assert result is None
    brickops.datamesh.cfg.read_config.assert_called_once()  # type: ignore[attr-defined]


def test_read_config_first_call(
    mocker: pytest_mock.MockType, reset_config_state: Any
) -> None:
    # Setup
    mocker.patch(
        "brickops.datamesh.cfg.find_config",
        return_value="/path/to/.brickopscfg/config.yml",
    )

    mock_config = {"key": "value"}
    mocker.patch("brickops.datamesh.cfg._read_yaml", return_value=mock_config)

    # Execute
    result = read_config()

    # Verify
    assert result == mock_config
    brickops.datamesh.cfg.find_config.assert_called_once()  # type: ignore[attr-defined]
    brickops.datamesh.cfg._read_yaml.assert_called_once_with(  # type: ignore[attr-defined]
        "/path/to/.brickopscfg/config.yml"
    )


def test_read_config_cache(
    reset_config_state: Any,
    monkeypatch: pytest.MonkeyPatch,
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    mock_config = {"key": "value"}
    # Setup
    mocker.patch(
        "brickops.datamesh.cfg.find_config",
        return_value="/path/to/.brickopscfg/config.yml",
    )
    mocker.patch("brickops.datamesh.cfg._read_yaml", return_value=mock_config)

    # First call to set up the cache
    read_config()

    # Execute - second call should use cache
    result = read_config()

    # Verify
    assert result == mock_config
    brickops.datamesh.cfg.find_config.assert_called_once()  # type: ignore[attr-defined]
    brickops.datamesh.cfg._read_yaml.assert_called_once()  # type: ignore[attr-defined]


def test_read_config_no_config_found(
    reset_config_state: Any,
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    mocker.patch(
        "brickops.datamesh.cfg.find_config",
        return_value=None,
    )

    # Execute
    result = read_config()

    # Verify
    assert result is None
    brickops.datamesh.cfg.find_config.assert_called_once()  # type: ignore[attr-defined]


def test_with_actual_config_file(
    temp_repo_with_config: Any,
    reset_config_state: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the config module with an actual config file."""
    # Change to the temp directory that contains .brickopscfg

    monkeypatch.chdir(temp_repo_with_config)

    # Use the actual implementation to read the config
    config = read_config()

    # Verify the config was read correctly
    assert config is not None
    assert "naming" in config
    assert "job" in config["naming"]
    assert "pipeline" in config["naming"]
    assert "catalog" in config["naming"]
    assert "db" in config["naming"]

    # Check specific format strings
    assert config["naming"]["job"]["prod"] == "{org}_{domain}_{project}_{env}"
    assert (
        config["naming"]["job"]["other"]
        == "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitshortref}"
    )
    assert config["naming"]["pipeline"]["prod"] == "{org}_{domain}_{project}_{env}_dlt"

    # Test get_config with the actual config
    naming_config = get_config("naming")
    assert naming_config is not None
    assert naming_config["catalog"]["prod"] == "{domain}"
    assert (
        naming_config["db"]["other"]
        == "{env}_{username}_{gitbranch}_{gitshortref}_{db}"
    )
    assert get_config("nonexistent_key") is None


def test_find_config_with_actual_directory(
    temp_repo_with_config: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test find_config with an actual directory structure."""
    # Change to the temp directory that contains .brickopscfg
    # Create a nested directory structure
    nested_dir = temp_repo_with_config / "level1" / "level2"
    nested_dir.mkdir(parents=True, exist_ok=True)

    # Change to the nested directory and verify find_config walks up to find config
    monkeypatch.chdir(temp_repo_with_config)
    config_path = find_config()

    # Verify we found the config file in the parent
    expected_path = temp_repo_with_config / ".brickopscfg" / "config.yml"

    assert config_path == expected_path
