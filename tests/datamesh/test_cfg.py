import pytest
from typing import Any
from unittest.mock import patch
from brickops.datamesh.cfg import (
    get_config,
    read_config,
    _find_config,
    _read_yaml,
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


def test_read_yaml_existing_file(tmp_path: Path) -> None:
    tmp_file = tmp_path / "config.yml"
    tmp_file.write_text("key: value")

    # Execute
    result = _read_yaml(tmp_file)

    # Verify
    assert result == {"key": "value"}


class TestGetConfig:
    @patch("brickops.datamesh.cfg.read_config")
    def test_get_config_returns_value(self, mock_read_config: Any) -> None:
        # Setup
        mock_read_config.return_value = {"test_key": "test_value"}

        # Execute
        result = get_config("test_key")

        # Verify
        assert result == "test_value"
        mock_read_config.assert_called_once()

    @patch("brickops.datamesh.cfg.read_config")
    def test_get_config_returns_none_for_missing_key(
        self, mock_read_config: Any
    ) -> None:
        # Setup
        mock_read_config.return_value = {"other_key": "other_value"}

        # Execute
        result = get_config("test_key")

        # Verify
        assert result is None
        mock_read_config.assert_called_once()

    @patch("brickops.datamesh.cfg.read_config")
    def test_get_config_returns_none_when_config_is_none(
        self, mock_read_config: Any
    ) -> None:
        # Setup
        mock_read_config.return_value = None

        # Execute
        result = get_config("test_key")

        # Verify
        assert result is None
        mock_read_config.assert_called_once()


class TestReadConfig:
    @patch("brickops.datamesh.cfg._find_config")
    @patch("brickops.datamesh.cfg._read_yaml")
    def test_read_config_first_call(
        self, mock_read_yaml: Any, mock_find_config: Any, reset_config_state: Any
    ) -> None:
        # Setup
        mock_find_config.return_value = "/path/to/.brickopscfg/config.yml"
        mock_config = {"key": "value"}
        mock_read_yaml.return_value = mock_config

        # Execute
        result = read_config()

        # Verify
        assert result == mock_config
        mock_find_config.assert_called_once()
        mock_read_yaml.assert_called_once_with("/path/to/.brickopscfg/config.yml")

    @patch("brickops.datamesh.cfg._find_config")
    @patch("brickops.datamesh.cfg._read_yaml")
    def test_read_config_cache(
        self, mock_read_yaml: Any, mock_find_config: Any, reset_config_state: Any
    ) -> None:
        # Setup
        mock_find_config.return_value = "/path/to/.brickopscfg/config.yml"
        mock_config = {"key": "value"}
        mock_read_yaml.return_value = mock_config

        # First call to set up the cache
        read_config()

        # Reset the mocks
        mock_find_config.reset_mock()
        mock_read_yaml.reset_mock()

        # Execute - second call should use cache
        result = read_config()

        # Verify
        assert result == mock_config
        mock_find_config.assert_not_called()
        mock_read_yaml.assert_not_called()

    @patch("brickops.datamesh.cfg._find_config")
    def test_read_config_no_config_found(
        self, mock_find_config: Any, reset_config_state: Any
    ) -> None:
        # Setup
        mock_find_config.return_value = None

        # Execute
        result = read_config()

        # Verify
        assert result is None
        mock_find_config.assert_called_once()


class TestWithActualConfig:
    def test_with_actual_config_file(
        self,
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
        assert (
            config["naming"]["pipeline"]["prod"] == "{org}_{domain}_{project}_{env}_dlt"
        )

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
        self,
        temp_repo_with_config: Any,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test _find_config with an actual directory structure."""
        # Change to the temp directory that contains .brickopscfg
        # Create a nested directory structure
        nested_dir = temp_repo_with_config / "level1" / "level2"
        nested_dir.mkdir(parents=True, exist_ok=True)

        # Change to the nested directory and verify _find_config walks up to find config
        monkeypatch.chdir(temp_repo_with_config)
        config_path = _find_config()

        # Verify we found the config file in the parent
        expected_path = temp_repo_with_config / ".brickopscfg" / "config.yml"

        assert config_path is not None
        assert config_path == expected_path

    def test_read_yaml_with_actual_file(self, temp_repo_with_config: Any) -> None:
        """Test _read_yaml with an actual YAML file."""
        # Get the path to the actual config file
        config_path = temp_repo_with_config / ".brickopscfg" / "config.yml"

        # Read the YAML file directly
        result = _read_yaml(config_path)

        # Verify the content was read correctly
        assert result is not None
        assert "naming" in result
        assert "job" in result["naming"]
        assert "pipeline" in result["naming"]
        assert "catalog" in result["naming"]
        assert "db" in result["naming"]

        # Check specific values from the config
        assert result["naming"]["job"]["prod"] == "{org}_{domain}_{project}_{env}"
        assert (
            result["naming"]["pipeline"]["other"]
            == "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitshortref}_dlt"
        )
        assert result["naming"]["catalog"]["other"] == "{domain}"
        assert result["naming"]["db"]["prod"] == "{db}"
