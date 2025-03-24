import pytest

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.pipeline.buildconfig.enrichtasks import enrich_tasks
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
    PipelineConfig,
    defaultconfig,
)


@pytest.fixture
def basic_config() -> PipelineConfig:
    pipeline_config = defaultconfig()
    pipeline_config.pipeline_tasks = [
        {
            "pipeline_key": "revenue",
            "db": "dltrevenue",
        }
    ]
    pipeline_config.git_source = {"git_path": "test"}
    return pipeline_config


@pytest.fixture
def databricks_context_data() -> DbContext:
    return DbContext(
        api_url="api_url",
        api_token="dummy",  # noqa: S106
        username="username",
        notebook_path="test/notebook_path",
    )


def test_default_config(
    databricks_context_data: DbContext,
    basic_config: PipelineConfig,
) -> None:
    basic_config.tasks = [
        {
            "task_key": "task_key",
        }
    ]
    result = enrich_tasks(
        pipeline_config=basic_config,
        db_context=databricks_context_data,
    )
    assert result.libraries == [{"notebook": {"path": "test/revenue"}}]
