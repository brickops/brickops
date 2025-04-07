from typing import Any
import pytest
from _pytest.monkeypatch import MonkeyPatch

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.readconfig import read_config_yaml
from brickops.dataops.deploy.pipeline.buildconfig.build import build_pipeline_config
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
    PipelineConfig,
    defaultconfig,
)


@pytest.fixture
def basic_config() -> dict[str, Any]:
    return {
        "pipeline_tasks": [
            {
                "pipeline_key": "revenue",
            }
        ],
        "schema": "dltrevenue",
        "git_source": {
            "git_url": "git_url",
            "git_branch": "git_branch",
            "git_commit": "abcdefgh123",
            "git_path": "/Repos/test@vlfk.no/dp-notebooks/",
        },
    }


@pytest.fixture
def db_context() -> DbContext:
    return DbContext(
        api_token="token",  # noqa: S106
        api_url="",
        notebook_path="/Repos/test@vlfk.no/dp-notebooks/domains/domainfoo/projects/projectfoo/flows/prep/flowfoo",
        username="TestUser@vlfk.no",
        widgets={
            "git_url": "git_url",
            "git_branch": "git_branch",
            "git_commit": "abcdefgh123",
        },
    )


DEV_EXPECTED_DEFAULT_CONFIG = {
    "name": "",
    "edition": "ADVANCED",
    "data_sampling": False,
    "pipeline_type": "WORKSPACE",
    "continuous": False,
    "channel": "CURRENT",
    "photon": True,
    "libraries": [],
    "serverless": True,
    "parameters": [],
    "tags": {},
}


def test_default_config_converts_correctly_to_dict() -> None:
    pipeline_config = defaultconfig()
    as_dict = pipeline_config.export_dict()
    assert as_dict == DEV_EXPECTED_DEFAULT_CONFIG


DEV_EXPECTED_CONFIG = {
    "name": "domainfoo_projectfoo_test_TestUser_gitbranch_abcdefgh_dlt",
    "edition": "ADVANCED",
    "catalog": "domainfoo",
    "data_sampling": False,
    "pipeline_type": "WORKSPACE",
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "photon": True,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/test@vlfk.no/dp-notebooks/domains/domainfoo/projects/projectfoo/flows/prep/revenue"
            }
        }
    ],
    "serverless": True,
    "parameters": [
        {
            "default": "test",
            "name": "pipeline_env",
        },
        {
            "default": "git_url",
            "name": "git_url",
        },
        {
            "default": "git_branch",
            "name": "git_branch",
        },
        {
            "default": "abcdefgh123",
            "name": "git_commit",
        },
    ],
    "schema": "test_TestUser_gitbranch_abcdefgh_dltrevenue",
    "tags": {
        "deployment": "test_TestUser_gitbranch_abcdefgh",
        "git_branch": "git_branch",
        "git_commit": "abcdefgh123",
        "git_url": "git_url",
        "pipeline_env": "test",
    },
}


def test_build_pipeline_config_returns_valid_result_for_valid_config(
    basic_config: dict[str, Any],
    db_context: DbContext,
) -> None:
    result = build_pipeline_config(basic_config, "test", db_context)
    assert isinstance(result, PipelineConfig)


def test_build_pipeline_config_returns_expected_result_for_valid_config(
    basic_config: dict[str, Any],
    db_context: DbContext,
) -> None:
    result = build_pipeline_config(basic_config, "test", db_context)
    assert result.export_dict() == DEV_EXPECTED_CONFIG


def test_build_pipeline_sets_correct_run_as(
    basic_config: dict[str, Any],
    db_context: DbContext,
) -> None:
    result = build_pipeline_config(basic_config, "test", db_context)
    assert result.run_as is None


def test_tags_are_set_correctly(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    result = build_pipeline_config(basic_config, "test", db_context)
    assert result.tags == {
        "git_branch": "git_branch",
        "git_commit": "abcdefgh123",
        "git_url": "git_url",
        "deployment": "test_TestUser_gitbranch_abcdefgh",
        "pipeline_env": "test",
    }


def test_service_prinical_is_set_when_running_as_sp(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    result = build_pipeline_config(basic_config, "test", db_context)
    assert result.run_as is None


def test_pipeline_name_is_correct_when_in_prod_env(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    db_context.notebook_path = "/Repos/test@vlfk.no/dp-notebooks/something/domains/domainfoo/projects/projectfoo/flows/flowfoo/task_key"
    result = build_pipeline_config(basic_config, env="prod", db_context=db_context)
    assert result.name == "domainfoo_projectfoo_prod_dlt"


def test_pipeline_name_is_correct_when_in_prod_env_w_org(
    basic_config: dict[str, Any], db_context: DbContext, monkeypatch: MonkeyPatch
) -> None:
    monkeypatch.setenv("BRICKOPS_MESH_PIPELINEPREFIX_LEVELS", "org,domain,project,flow")
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    db_context.notebook_path = "/Repos/test@vlfk.no/dp-notebooks/something/org/acme/domains/domainfoo/projects/projectfoo/flows/flowfoo/task_key"
    result = build_pipeline_config(basic_config, env="prod", db_context=db_context)
    assert result.name == "domainfoo_projectfoo_prod_dlt"


def test_values_from_yaml_is_set_correct_in_pipeline_config(
    db_context: DbContext,
) -> None:
    config_from_yaml = read_config_yaml(
        "tests/dataops/deploy/pipeline/buildconfig/deployment.yml"
    )
    config_from_yaml["git_source"] = {
        "git_url": "git_url",
        "git_branch": "git_branch",
        "git_commit": "abcdefgh123",
        "git_path": "/",
    }
    result = build_pipeline_config(config_from_yaml, env="test", db_context=db_context)
    assert result.parameters == [
        {
            "name": "days_to_keep",
            "default": 2,
        },
        {
            "name": "pipeline_env",
            "default": "test",
        },
        {
            "name": "git_url",
            "default": "git_url",
        },
        {
            "name": "git_branch",
            "default": "git_branch",
        },
        {
            "name": "git_commit",
            "default": "abcdefgh123",
        },
    ]
    assert result.schedule == {
        "quartz_cron_expression": "0 0 20 * * ?",
        "pause_status": "UNPAUSED",
        "timezone_id": "Europe/Brussels",
    }
