from typing import Any

import pytest

from brickops.databricks.context import DbContext
from brickops.dataops.deploy.pipeline.buildconfig.build import build_pipeline_config
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
    PipelineConfig,
    defaultconfig,
)
from brickops.dataops.deploy.readconfig import read_config_yaml


@pytest.fixture
def basic_config() -> dict[str, Any]:
    return {
        "pipeline_tasks": [
            {
                "pipeline_key": "revenue",
                "db": "dltrevenue",
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


DEV_EXPECTED_CONFIG = {
    "name": "",
    "edition": "ADVANCED",
    "data_sampling": False,
    "pipeline_type": "WORKSPACE",
    "development": True,
    "continuous": False,
    "channel": "CURRENT",
    "photon": True,
    "libraries": [
        {
            "notebook": {
                "path": "/Repos/magnus.lawmender@noregsveldi.norig/databricks-dataops-course/orgs/acme/domains/transport/projects/taxinyc/flows/prep/dltrevenue/revenue"
            }
        }
    ],
    "serverless": True,
    "parameters": [],
    "policy_name": "dlt_default_policy",
    "tags": {
        # "deployment": "dev_magnuslawmender_featuregh345revenue_aaaabbbb",
        # "env": "dev",
        # "git_branch": "feature/gh-345-revenue",
        # "git_commit": "aaaabbbb2a48e1a5fc5b9b40746c82f81cce1111",
        # "git_url": "https://github.com/paalvibe/databricks-dataops-course",
    },
}


def test_that_default_config_converts_correctly_to_dict() -> None:
    pipeline_config = defaultconfig()
    as_dict = pipeline_config.dict()
    assert as_dict == DEV_EXPECTED_CONFIG
    # assert as_dict == {
    #     "name": "",
    #     "git_source": {},
    #     "pipeline_clusters": [],
    #     "max_concurrent_runs": 1,
    #     "parameters": [],
    #     "run_as": {},
    #     "tags": {},
    #     "tasks": [],
    # }


def test_that_build_pipeline_config_returns_valid_result_for_valid_config(
    basic_config: dict[str, Any],
    db_context: DbContext,
) -> None:
    result = build_pipeline_config(basic_config, "test", db_context)
    assert isinstance(result, PipelineConfig)


# def test_that_build_pipeline_sets_correct_run_as(
#     basic_config: dict[str, Any],
#     db_context: DbContext,
# ) -> None:
#     result = build_pipeline_config(basic_config, "test", db_context)
#     assert result.run_as == {"user_name": "TestUser@vlfk.no"}


# def test_that_tags_are_set_correctly(
#     basic_config: dict[str, Any], db_context: DbContext
# ) -> None:
#     result = build_pipeline_config(basic_config, "test", db_context)
#     assert result.tags == {
#         "git_branch": "git_branch",
#         "git_commit": "abcdefgh123",
#         "git_url": "git_url",
#         "deployment": "test_TestUser_gitbranch_abcdefgh",
#     }


# def test_that_service_prinical_is_set__when_running_as_sp(
#     basic_config: dict[str, Any], db_context: DbContext
# ) -> None:
#     db_context.username = "service_principal"
#     db_context.is_service_principal = True
#     result = build_pipeline_config(basic_config, "test", db_context)
#     assert result.run_as == {
#         "service_principal_name": "service_principal",
#     }


# def test_that_pipeline_name_is_correct_when_in_prod_env(
#     basic_config: dict[str, Any], db_context: DbContext
# ) -> None:
#     db_context.username = "service_principal"
#     db_context.is_service_principal = True
#     result = build_pipeline_config(basic_config, env="prod", db_context=db_context)
#     assert result.name == "test_project_flow_prod"


# def test_that_pipeline_name_is_correct_when_in_prod_env_w_org(
#     basic_config: dict[str, Any], db_context: DbContext, monkeypatch
# ) -> None:
#     monkeypatch.setenv("BRICKOPS_MESH_PIPELINEPREFIX_LEVELS", "org,domain,project,flow")
#     db_context.username = "service_principal"
#     db_context.is_service_principal = True
#     db_context.notebook_path = "/Repos/test@vlfk.no/dp-notebooks/something/org/acme/domains/test/projects/project/flows/flow/task_key"
#     result = build_pipeline_config(basic_config, env="prod", db_context=db_context)
#     assert result.name == "acme_test_project_flow_prod"


# def test_that_cluster_is_set_correct_in_pipeline_config(
#     basic_config: dict[str, Any], db_context: DbContext
# ) -> None:
#     db_context.username = "service_principal"
#     db_context.is_service_principal = True
#     result = build_pipeline_config(basic_config, env="test", db_context=db_context)
#     assert result.pipeline_clusters == [
#         {
#             "new_cluster": {
#                 "num_workers": 1,
#                 "spark_version": "14.3.x-scala2.12",
#                 "spark_conf": {},
#                 "azure_attributes": {
#                     "first_on_demand": 1,
#                     "availability": "SPOT_WITH_FALLBACK_AZURE",
#                     "spot_bid_max_price": -1,
#                 },
#                 "node_type_id": "Standard_D4ads_v5",
#                 "ssh_public_keys": [],
#                 "custom_tags": {},
#                 "spark_env_vars": {},
#                 "init_scripts": [],
#                 "data_security_mode": "SINGLE_USER",
#                 "runtime_engine": "STANDARD",
#             },
#             "pipeline_cluster_key": "common-pipeline-cluster",
#         }
#     ]


# def test_that_values_from_yaml_is_set_correct_in_pipeline_config(
#     db_context: DbContext,
# ) -> None:
#     config_from_yaml = read_config_yaml(
#         "tests/dataops/deploy/pipeline/buildconfig/deployment.yml"
#     )
#     config_from_yaml["git_source"] = {
#         "git_url": "git_url",
#         "git_branch": "git_branch",
#         "git_commit": "abcdefgh123",
#         "git_path": "/",
#     }
#     result = build_pipeline_config(config_from_yaml, env="test", db_context=db_context)
#     assert result.parameters == [
#         {
#             "name": "days_to_keep",
#             "default": 2,
#         },
#         {
#             "name": "pipeline_env",
#             "default": "test",
#         },
#         {
#             "name": "git_url",
#             "default": "git_url",
#         },
#         {
#             "name": "git_branch",
#             "default": "git_branch",
#         },
#         {
#             "name": "git_commit",
#             "default": "abcdefgh123",
#         },
#     ]
#     assert result.schedule == {
#         "quartz_cron_expression": "0 0 20 * * ?",
#         "pause_status": "UNPAUSED",
#         "timezone_id": "Europe/Brussels",
#     }
