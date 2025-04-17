import pytest
import pytest_mock
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
        }
    ]
    pipeline_config.schema = "dltrevenue"
    pipeline_config.git_source = {"git_path": "test"}
    return pipeline_config


@pytest.fixture
def databricks_context_data() -> DbContext:
    return DbContext(
        api_url="api_url",
        api_token="dummy",  # noqa: S106
        username="username",
        notebook_path="/Repos/test@vlfk.no/dp-notebooks/domains/test/projects/project/flows/prep/revenue",
    )


GIT_SOURCE = {
    "git_url": "test",
    "git_provider": "github",
    "git_branch": "testbranch",
    "git_commit": "fffffhhhh",
    "git_path": "test/notebook_path",
}


def test_default_config_prod_env(
    databricks_context_data: DbContext,
    basic_config: PipelineConfig,
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    mocker.patch("brickops.datamesh.naming.git_source", return_value=GIT_SOURCE)
    basic_config.pipeline_tasks = [
        {
            "pipeline_key": "revenue",
        }
    ]
    basic_config.schema = "dltrevenue"
    result = enrich_tasks(
        pipeline_config=basic_config,
        db_context=databricks_context_data,
        env="prod",
    )
    assert result.libraries == [
        {
            "notebook": {
                "path": "/Repos/test@vlfk.no/dp-notebooks/domains/test/projects/project/flows/prep/revenue"
            }
        }
    ]


def test_default_config_test_env(
    databricks_context_data: DbContext,
    basic_config: PipelineConfig,
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    mocker.patch("brickops.datamesh.naming.git_source", return_value=GIT_SOURCE)
    basic_config.pipeline_tasks = [
        {
            "pipeline_key": "revenue",
            "schema": "dltrevenue",
        }
    ]
    result = enrich_tasks(
        pipeline_config=basic_config,
        db_context=databricks_context_data,
        env="test",
    )
    assert result.libraries == [
        {
            "notebook": {
                "path": "/Repos/test@vlfk.no/dp-notebooks/domains/test/projects/project/flows/prep/revenue"
            }
        }
    ]
