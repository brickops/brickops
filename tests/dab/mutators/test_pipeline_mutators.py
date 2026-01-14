"""Tests for pipeline_mutators module."""

from unittest.mock import MagicMock, patch

import pytest

from brickops.dab.mutators.pipeline_mutators import (
    bundlepath,
    _username,
    brickops_pipeline_params,
)


class TestBundlePath:
    """Tests for _bundle_path function."""

    def test_bundle_path_returns_current_directory(self) -> None:
        """Test that _bundle_path returns the current working directory."""
        with patch("os.getcwd", return_value="/test/path"):
            result = bundlepath()
            assert result == "/test/path"


class TestUsername:
    """Tests for _username function."""

    def test_username_extracts_from_source_path(self) -> None:
        """Test that _username correctly extracts username from source path."""
        pipeline = MagicMock()
        pipeline.configuration = {
            "bundle.sourcePath": "/Workspace/Users/testuser/myproject"
        }
        result = _username(pipeline)
        assert result == "testuser"

    def test_username_handles_complex_path(self) -> None:
        """Test username extraction with complex path."""
        pipeline = MagicMock()
        pipeline.configuration = {
            "bundle.sourcePath": "/Workspace/Users/john.doe/projects/subdir/bundle"
        }
        result = _username(pipeline)
        assert result == "john.doe"


class TestBrickopsPipelineParams:
    """Tests for brickops_pipeline_params function."""

    @pytest.fixture
    def mock_bundle(self) -> MagicMock:
        """Create a mock Bundle object."""
        bundle = MagicMock()
        bundle.target = "dev"
        return bundle

    @pytest.fixture
    def mock_pipeline(self) -> MagicMock:
        """Create a mock Pipeline object."""
        pipeline = MagicMock()
        pipeline.name = "original_pipeline"
        pipeline.schema = "original_schema"
        pipeline.configuration = {
            "bundle.sourcePath": "/Workspace/Users/testuser/project"
        }
        return pipeline

    @pytest.fixture
    def mock_git_info(self) -> dict[str, str]:
        """Create mock git info."""
        return {
            "branch": "feature/test-branch",
            "commit": "abc123",
            "url": "https://github.com/test/repo.git",
        }

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_updates_pipeline_name(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
        mock_git_info: dict[str, str],
    ) -> None:
        """Test that pipeline name is updated correctly."""
        mock_getcwd.return_value = "/test/path"
        mock_get_git_info.return_value = mock_git_info
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        result = brickops_pipeline_params(mock_bundle, mock_pipeline)

        assert result.name == "new_pipeline_name"
        mock_naming.pipelinename.assert_called_once()

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_updates_schema_name(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
        mock_git_info: dict[str, str],
    ) -> None:
        """Test that schema name is updated correctly."""
        mock_getcwd.return_value = "/test/path"
        mock_get_git_info.return_value = mock_git_info
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        result = brickops_pipeline_params(mock_bundle, mock_pipeline)

        assert result.schema == "new_schema_name"
        mock_naming.dbname.assert_called_once()
        call_args = mock_naming.dbname.call_args
        assert call_args.kwargs["db"] == "original_schema"
        assert call_args.kwargs["cat"] == ""
        assert call_args.kwargs["prepend_cat"] is False
        assert call_args.kwargs["target"] == "dev"

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_adds_git_configuration(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
        mock_git_info: dict[str, str],
    ) -> None:
        """Test that git information is added to pipeline configuration."""
        mock_getcwd.return_value = "/test/path"
        mock_get_git_info.return_value = mock_git_info
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        result = brickops_pipeline_params(mock_bundle, mock_pipeline)

        # Check that configuration contains git info
        config = result.configuration
        assert "brickops.git_branch" in config
        assert config["brickops.git_branch"] == "feature/test-branch"
        assert "brickops.git_commit" in config
        assert config["brickops.git_commit"] == "abc123"
        assert "brickops.git_url" in config
        assert config["brickops.git_url"] == "https://github.com/test/repo.git"

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_adds_clean_branch(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
        mock_git_info: dict[str, str],
    ) -> None:
        """Test that cleaned branch name is added to configuration."""
        mock_getcwd.return_value = "/test/path"
        mock_get_git_info.return_value = mock_git_info
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        result = brickops_pipeline_params(mock_bundle, mock_pipeline)

        config = result.configuration
        assert "brickops.clean_git_branch" in config
        # Branch "feature/test-branch" should be cleaned to "featuretestbranch"
        assert config["brickops.clean_git_branch"] == "featuretestbranch"

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_excludes_repo_root(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
    ) -> None:
        """Test that repo_root is not added to pipeline configuration."""
        mock_getcwd.return_value = "/test/path"
        git_info_with_root = {
            "branch": "main",
            "commit": "abc123",
            "url": "https://github.com/test/repo.git",
            "repo_root": "/path/to/repo",
        }
        mock_get_git_info.return_value = git_info_with_root
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        result = brickops_pipeline_params(mock_bundle, mock_pipeline)

        config = result.configuration
        assert "brickops.git_repo_root" not in config
        assert "repo_root" not in config

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_creates_db_context_correctly(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
        mock_git_info: dict[str, str],
    ) -> None:
        """Test that db_context is created with correct parameters."""
        mock_getcwd.return_value = "/test/path"
        mock_get_git_info.return_value = mock_git_info
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        brickops_pipeline_params(mock_bundle, mock_pipeline)

        # Verify get_context_from_params was called with correct arguments
        mock_get_context.assert_called_once_with(
            "/test/path", "testuser", mock_git_info
        )

    @patch("brickops.dab.mutators.pipeline_mutators.replace")
    @patch("brickops.dab.mutators.pipeline_mutators.get_git_info")
    @patch("brickops.dab.mutators.pipeline_mutators.get_context_from_params")
    @patch("brickops.dab.mutators.pipeline_mutators.naming")
    @patch("os.getcwd")
    def test_brickops_pipeline_params_returns_modified_pipeline(
        self,
        mock_getcwd: MagicMock,
        mock_naming: MagicMock,
        mock_get_context: MagicMock,
        mock_get_git_info: MagicMock,
        mock_replace: MagicMock,
        mock_bundle: MagicMock,
        mock_pipeline: MagicMock,
        mock_git_info: dict[str, str],
    ) -> None:
        """Test that the function returns the modified pipeline."""
        mock_getcwd.return_value = "/test/path"
        mock_get_git_info.return_value = mock_git_info
        mock_naming.pipelinename.return_value = "new_pipeline_name"
        mock_naming.dbname.return_value = "new_schema_name"

        result = brickops_pipeline_params(mock_bundle, mock_pipeline)

        assert result is mock_pipeline
