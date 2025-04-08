from typing import Any

import pytest

from unittest import mock
from brickops.databricks.context import DbContext
from brickops.dataops.deploy.job.buildconfig.build import build_job_config
from brickops.dataops.deploy.job.buildconfig.job_config import JobConfig, defaultconfig
from brickops.dataops.deploy.readconfig import read_config_yaml


@pytest.fixture
def basic_config() -> dict[str, Any]:
    return {
        "tasks": [
            {
                "task_key": "task_key",
                "job_cluster_key": "common-job-cluster",
            }
        ],
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
        notebook_path="/Repos/test@vlfk.no/dp-notebooks/domains/test/projects/project/flows/flow/testflow",
        username="TestUser@vlfk.no",
        widgets={
            "git_url": "git_url",
            "git_branch": "git_branch",
            "git_commit": "abcdefgh123",
        },
    )


def test_that_default_config_converts_correctly_to_dict() -> None:
    job_config = defaultconfig()
    as_dict = job_config.dict()
    assert as_dict == {
        "name": "",
        "email_notifications": {},
        "git_source": {},
        "job_clusters": [],
        "max_concurrent_runs": 1,
        "parameters": [],
        "run_as": {},
        "tags": {},
        "tasks": [],
    }


def test_that_build_job_config_returns_valid_result_for_valid_config(
    basic_config: dict[str, Any],
    db_context: DbContext,
) -> None:
    result = build_job_config(basic_config, "test", db_context)
    assert isinstance(result, JobConfig)


def test_that_build_job_sets_correct_run_as(
    basic_config: dict[str, Any],
    db_context: DbContext,
) -> None:
    result = build_job_config(basic_config, "test", db_context)
    assert result.run_as == {"user_name": "TestUser@vlfk.no"}


def test_that_tags_are_set_correctly(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    result = build_job_config(basic_config, "test", db_context)
    assert result.tags == {
        "git_branch": "git_branch",
        "git_commit": "abcdefgh123",
        "git_url": "git_url",
        "deployment": "test_TestUser_gitbranch_abcdefgh",
    }


def test_that_service_prinical_is_set__when_running_as_sp(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    result = build_job_config(basic_config, "test", db_context)
    assert result.run_as == {
        "service_principal_name": "service_principal",
    }


def test_that_job_name_is_correct_when_in_prod_env(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    result = build_job_config(basic_config, env="prod", db_context=db_context)
    assert result.name == "test_project_prod"


@mock.patch(
    "brickops.datamesh.cfg._findconfig",
    return_value=pytest.BRICKOPS_FULLMESH_CONFIG,  # type: ignore[attr-defined]
)
def test_that_job_name_is_correct_when_in_prod_env_w_org(
    basic_config: dict[str, Any], db_context: DbContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    db_context.notebook_path = "/Repos/test@vlfk.no/dp-notebooks/something/org/acme/domains/domainfoo/projects/projectfoo/flows/prep/taskfoo"
    result = build_job_config(basic_config, env="prod", db_context=db_context)
    assert result.name == "domainfoo_projectfoo_prod"


def test_that_cluster_is_set_correct_in_job_config(
    basic_config: dict[str, Any], db_context: DbContext
) -> None:
    db_context.username = "service_principal"
    db_context.is_service_principal = True
    result = build_job_config(basic_config, env="test", db_context=db_context)
    assert result.job_clusters == [
        {
            "new_cluster": {
                "num_workers": 1,
                "spark_version": "14.3.x-scala2.12",
                "spark_conf": {},
                "azure_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK_AZURE",
                    "spot_bid_max_price": -1,
                },
                "node_type_id": "Standard_D4ads_v5",
                "ssh_public_keys": [],
                "custom_tags": {},
                "spark_env_vars": {},
                "init_scripts": [],
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
            },
            "job_cluster_key": "common-job-cluster",
        }
    ]


def test_that_values_from_yaml_is_set_correct_in_job_config(
    db_context: DbContext,
) -> None:
    config_from_yaml = read_config_yaml(
        "tests/dataops/deploy/job/buildconfig/deployment.yml"
    )
    config_from_yaml["git_source"] = {
        "git_url": "git_url",
        "git_branch": "git_branch",
        "git_commit": "abcdefgh123",
        "git_path": "/",
    }
    result = build_job_config(config_from_yaml, env="test", db_context=db_context)
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
