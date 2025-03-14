from pathlib import Path

import pytest

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.job.buildconfig.enrichtasks import enrich_tasks
from brickops.dataops.deploy.job.buildconfig.job_config import JobConfig, defaultconfig


@pytest.fixture
def basic_config() -> JobConfig:
    job_config = defaultconfig()
    job_config.tasks = [
        {
            "task_key": "task_key",
            "job_cluster_key": "some_cluster_key",
        }
    ]
    job_config.git_source = {"git_path": "test"}
    return job_config


@pytest.fixture
def serverless_config() -> JobConfig:
    job_config = defaultconfig()
    job_config.tasks = [
        {
            "task_key": "task_key",
            "serverless": True,
        }
    ]
    job_config.git_source = {"git_path": "test"}
    return job_config


@pytest.fixture
def invalid_config() -> JobConfig:
    job_config = defaultconfig()
    job_config.tasks = [
        {
            "task_key": "task_key",
            "serverless": True,
            "job_cluster_key": "some_cluster_key",
        }
    ]
    job_config.git_source = {"git_path": "test"}
    return job_config


@pytest.fixture
def notebook_task_config() -> JobConfig:
    job_config = defaultconfig()
    job_config.tasks = [
        {
            "task_key": "task_key",
            "serverless": True,
            "notebook_task": {
                "notebook_path": "./test_notebook",
            },
        }
    ]
    job_config.git_source = {"git_path": "/git_root/folder/location_of_notebook"}
    return job_config


@pytest.fixture
def databricks_context_data() -> DbContext:
    return DbContext(
        api_url="api_url",
        api_token="dummy",  # noqa: S106
        username="username",
        notebook_path="test/notebook_path",
    )


def test_that_enriching_with_no_cluster_yields_error(
    databricks_context_data: DbContext,
    basic_config: JobConfig,
) -> None:
    basic_config.tasks = [
        {
            "task_key": "task_key",
        }
    ]
    with pytest.raises(ValueError):
        enrich_tasks(
            job_config=basic_config,
            db_context=databricks_context_data,
        )


def test_that_specifying_serverless_returns_config_without_cluster(
    serverless_config: JobConfig, databricks_context_data: DbContext
) -> None:
    result = enrich_tasks(serverless_config, db_context=databricks_context_data)
    assert result.tasks == [
        {
            "notebook_task": {
                "notebook_path": "task_key",
                "source": "GIT",
            },
            "task_key": "task_key",
        }
    ]
    assert result.git_source == {"git_path": "test"}
    assert result.job_clusters == []


def test_that_specifying_both_serverless_and_cluster_name_raises(
    invalid_config: JobConfig, databricks_context_data: DbContext
) -> None:
    with pytest.raises(ValueError):
        enrich_tasks(invalid_config, db_context=databricks_context_data)


def test_that_git_path_is_prepended_when_defining_notebook_task(
    notebook_task_config: JobConfig, databricks_context_data: DbContext
) -> None:
    databricks_context_data.notebook_path = (
        notebook_task_config.git_source["git_path"]
        + "/"
        + databricks_context_data.notebook_path
    )
    result = enrich_tasks(notebook_task_config, db_context=databricks_context_data)
    assert result.tasks == [
        {
            "task_key": "task_key",
            "serverless": True,
            "notebook_task": {
                "notebook_path": str(Path("test/test_notebook")),
                "source": "GIT",
            },
        }
    ]
