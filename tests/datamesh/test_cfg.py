import os
import pytest
import tempfile
import shutil
from typing import Any
from unittest.mock import patch, mock_open, MagicMock
import yaml
from brickops.datamesh import cfg
from brickops.datamesh.cfg import (
    get_config,
    read_config,
    _findconfig,
    _read_yaml,
)


@pytest.fixture
def reset_config_state() -> Any:
    """Reset the module's global state between tests."""
    import brickops.datamesh.cfg

    brickops.datamesh.cfg._config = None
    brickops.datamesh.cfg._config_read = False
    yield
    brickops.datamesh.cfg._config = None
    brickops.datamesh.cfg._config_read = False


@pytest.fixture
def temp_repo_with_config() -> Any:
    """Create a temporary directory structure with a .brickopscfg/config.yml file."""
    # Create a temporary directory to act as our repo
    temp_dir = tempfile.mkdtemp()

    try:
        # Create the .brickopscfg directory
        config_dir = os.path.join(temp_dir, ".brickopscfg")
        os.makedirs(config_dir)

        # Create a config.yml file with the specified content
        config_path = os.path.join(config_dir, "config.yml")
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
        with open(config_path, "w") as f:
            f.write(config_content)

        # Return the path to the temp directory
        yield temp_dir
    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir)


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
    @patch("brickops.datamesh.cfg._findconfig")
    @patch("brickops.datamesh.cfg._read_yaml")
    def test_read_config_first_call(
        self, mock_read_yaml: Any, mock_findconfig: Any, reset_config_state: Any
    ) -> None:
        # Setup
        mock_findconfig.return_value = "/path/to/.brickopscfg/config.yml"
        mock_config = {"key": "value"}
        mock_read_yaml.return_value = mock_config

        # Execute
        result = read_config()

        # Verify
        assert result == mock_config
        mock_findconfig.assert_called_once()
        mock_read_yaml.assert_called_once_with("/path/to/.brickopscfg/config.yml")
        assert cfg._config_read is True
        assert cfg._config == mock_config

    @patch("brickops.datamesh.cfg._findconfig")
    @patch("brickops.datamesh.cfg._read_yaml")
    def test_read_config_cache(
        self, mock_read_yaml: Any, mock_findconfig: Any, reset_config_state: Any
    ) -> None:
        # Setup
        mock_findconfig.return_value = "/path/to/.brickopscfg/config.yml"
        mock_config = {"key": "value"}
        mock_read_yaml.return_value = mock_config

        # First call to set up the cache
        read_config()

        # Reset the mocks
        mock_findconfig.reset_mock()
        mock_read_yaml.reset_mock()

        # Execute - second call should use cache
        result = read_config()

        # Verify
        assert result == mock_config
        mock_findconfig.assert_not_called()
        mock_read_yaml.assert_not_called()

    @patch("brickops.datamesh.cfg._findconfig")
    def test_read_config_no_config_found(
        self, mock_findconfig: Any, reset_config_state: Any
    ) -> None:
        # Setup
        mock_findconfig.return_value = None

        # Execute
        result = read_config()

        # Verify
        assert result is None
        mock_findconfig.assert_called_once()
        assert cfg._config_read is True
        assert cfg._config is None


class TestFindConfig:
    @patch("os.path.isdir")
    @patch("os.getcwd")
    @patch("os.path.abspath")
    def test_findconfig_current_dir(
        self, mock_abspath: Any, mock_getcwd: Any, mock_isdir: Any
    ) -> None:
        # Setup
        mock_getcwd.return_value = "/current/dir"
        mock_abspath.return_value = "/current/dir"

        # Mock that .brickopscfg exists in the current directory
        def isdir_side_effect(path: str) -> bool:
            return path == "/current/dir/.brickopscfg"

        mock_isdir.side_effect = isdir_side_effect

        # Execute
        result = _findconfig()

        # Verify
        assert result == "/current/dir/.brickopscfg/config.yml"
        mock_getcwd.assert_called_once()
        mock_abspath.assert_called_once_with("/current/dir")

    @patch("os.path.isdir")
    @patch("os.getcwd")
    @patch("os.path.abspath")
    @patch("os.path.dirname")
    def test_findconfig_parent_dir(
        self, mock_dirname: Any, mock_abspath: Any, mock_getcwd: Any, mock_isdir: Any
    ) -> None:
        # Setup
        mock_getcwd.return_value = "/parent/child/dir"
        mock_abspath.return_value = "/parent/child/dir"

        # Set up directory structure for traversal
        def dirname_side_effect(path: str) -> Any:
            if path == "/parent/child/dir":
                return "/parent/child"
            elif path == "/parent/child":
                return "/parent"
            else:
                return "/"

        mock_dirname.side_effect = dirname_side_effect

        # Mock that .brickopscfg exists in the parent directory
        def isdir_side_effect(path: str) -> bool:
            return path == "/parent/.brickopscfg"

        mock_isdir.side_effect = isdir_side_effect

        # Execute
        result = _findconfig()

        # Verify
        assert result == "/parent/.brickopscfg/config.yml"

    @patch("os.path.isdir")
    @patch("os.getcwd")
    @patch("os.path.abspath")
    @patch("os.path.dirname")
    def test_findconfig_not_found(
        self, mock_dirname: Any, mock_abspath: Any, mock_getcwd: Any, mock_isdir: Any
    ) -> None:
        # Setup
        mock_getcwd.return_value = "/some/dir"
        mock_abspath.return_value = "/some/dir"

        # Set up directory structure for traversal to root
        def dirname_side_effect(path: str) -> Any:
            if path == "/some/dir":
                return "/some"
            elif path == "/some":
                return "/"
            elif path == "/":
                return "/"  # Root directory returns itself

        mock_dirname.side_effect = dirname_side_effect

        # Mock that .brickopscfg doesn't exist anywhere
        mock_isdir.return_value = False

        # Execute
        result = _findconfig()

        # Verify
        assert result is None


class TestReadYaml:
    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data="key: value")
    def test_read_yaml_existing_file(self, mock_file: Any, mock_exists: Any) -> None:
        # Setup
        mock_exists.return_value = True

        # Execute
        result = _read_yaml("/path/to/config.yml")

        # Verify
        assert result == {"key": "value"}
        mock_exists.assert_called_once_with("/path/to/config.yml")
        mock_file.assert_called_once_with("/path/to/config.yml", "r")

    @patch("os.path.exists")
    @patch("logging.getLogger")
    def test_read_yaml_nonexistent_file(
        self, mock_getlogger: Any, mock_exists: Any
    ) -> None:
        # Setup
        mock_exists.return_value = False
        mock_logger = MagicMock()
        mock_getlogger.return_value = mock_logger

        # Execute
        result = _read_yaml("/path/to/nonexistent.yml")

        # Verify
        assert result is None
        mock_exists.assert_called_once_with("/path/to/nonexistent.yml")
        # Note: This assertion is a bit tricky since we're mocking a logger that's
        # instantiated at module level; in a real test, we'd likely need to adjust
        # the implementation to make this more testable

    @patch("os.path.exists")
    @patch("builtins.open", new_callable=mock_open, read_data="not: valid: yaml")
    def test_read_yaml_invalid_yaml(self, mock_file: Any, mock_exists: Any) -> None:
        # Setup
        mock_exists.return_value = True

        # Creating a mock for yaml.safe_load that raises an exception
        with patch("yaml.safe_load") as mock_yaml_load:
            mock_yaml_load.side_effect = yaml.YAMLError("Invalid YAML")

            # Execute and expect exception
            with pytest.raises(yaml.YAMLError):
                _read_yaml("/path/to/invalid.yml")

            # Verify
            mock_exists.assert_called_once_with("/path/to/invalid.yml")
            mock_file.assert_called_once_with("/path/to/invalid.yml", "r")


class TestWithActualConfig:
    def test_with_actual_config_file(
        self, temp_repo_with_config: Any, reset_config_state: Any
    ) -> None:
        """Test the config module with an actual config file."""
        # Change to the temp directory that contains .brickopscfg
        original_dir = os.getcwd()
        try:
            os.chdir(temp_repo_with_config)

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
                config["naming"]["pipeline"]["prod"]
                == "{org}_{domain}_{project}_{env}_dlt"
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

        finally:
            # Restore the original directory
            os.chdir(original_dir)

    def test_findconfig_with_actual_directory(self, temp_repo_with_config: Any) -> None:
        """Test _findconfig with an actual directory structure."""
        # Change to the temp directory that contains .brickopscfg
        original_dir = os.getcwd()
        try:
            os.chdir(temp_repo_with_config)

            # Create a nested directory structure
            nested_dir = os.path.join(temp_repo_with_config, "level1", "level2")
            os.makedirs(nested_dir, exist_ok=True)

            # Change to the nested directory and verify _findconfig walks up to find config
            os.chdir(nested_dir)
            config_path = _findconfig()

            # Verify we found the config file in the parent
            expected_path = os.path.join(
                temp_repo_with_config, ".brickopscfg", "config.yml"
            )
            assert config_path is not None
            assert os.path.normpath(config_path) == os.path.normpath(expected_path)

        finally:
            # Restore the original directory
            os.chdir(original_dir)

    def test_read_yaml_with_actual_file(self, temp_repo_with_config: Any) -> None:
        """Test _read_yaml with an actual YAML file."""
        # Get the path to the actual config file
        config_path = os.path.join(temp_repo_with_config, ".brickopscfg", "config.yml")

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
