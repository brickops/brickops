import pytest

from unittest import mock
from typing import Any
from brickops.databricks.context import DbContext
from brickops.datamesh.naming import (
    catname_from_path,
)

from brickops.datamesh.parsepath.parse import (
    parsepath,
    ParsedPath,
)


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


@pytest.fixture
def valid_path() -> str:
    return "something/domains/sales/projects/test_project/flows/flowfoo/notebookfoo"


@pytest.fixture
def valid_org_path() -> str:
    return "something/org/acme/domains/sales/projects/testproject/flows/flowfoo/notebookfoo"


@pytest.fixture
def explore_path() -> str:
    return (
        "something/domains/sales/projects/test_project/explore/exploration/notebookfoo"
    )


def test_starting_with_valid_path_returns_correct_catalog_name(
    valid_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = valid_path
    assert catname_from_path(db_context=db_context) == "sales"


@mock.patch(
    "brickops.datamesh.cfg.read_config",
    return_value=pytest.BRICKOPS_DEFAULT_CONFIG,  # type: ignore[attr-defined]
)
def test_starting_with_valid_path_returns_correct_catalog_name_w_conf(
    _: Any,
    valid_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = valid_path
    assert catname_from_path(db_context=db_context) == "sales"


@mock.patch(
    "brickops.datamesh.cfg.read_config",
    return_value=pytest.BRICKOPS_DEFAULT_CONFIG,  # type: ignore[attr-defined]
)
def test_starting_with_valid_path_returns_correct_catalog_name_w_org(
    _: Any,
    valid_org_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = valid_org_path
    assert catname_from_path(db_context=db_context) == "sales"


@mock.patch(
    "brickops.datamesh.cfg.read_config",
    return_value=pytest.BRICKOPS_DEFAULT_CONFIG,  # type: ignore[attr-defined]
)
def test_starting_with_valid_path_returns_correct_catalog_name_w_org_w_conf(
    _: Any,
    valid_org_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = valid_org_path
    assert catname_from_path(db_context=db_context) == "sales"


@mock.patch(
    "brickops.datamesh.cfg.read_config",
    return_value=pytest.BRICKOPS_FULLMESH_CONFIG,  # type: ignore[attr-defined]
)
def test_starting_with_valid_path_returns_correct_catalog_name_w_org_w_fullmesh_conf(
    _: Any,
    valid_org_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = valid_org_path
    assert catname_from_path(db_context=db_context) == "acme_sales_testproject_test"


def test_containing_valid_path_in_prod_returns_correct_catalog_name_without_postfix(
    valid_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = valid_path
    assert catname_from_path(db_context=db_context) == "sales"


def test_containing_path_without_domain_returns_none(
    db_context: DbContext,
) -> None:
    db_context.notebook_path = (
        "something/domains/projects/test_project/flows/test_flow/test_notebook"
    )
    assert catname_from_path(db_context=db_context) == ""


def test_env_is_correctly_post_fixed(
    valid_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = f"some_prefix/path{valid_path}"
    assert catname_from_path(db_context=db_context) == "sales"


def test_catalog_can_be_extracted_for_explore_folders(
    explore_path: str,
    db_context: DbContext,
) -> None:
    db_context.notebook_path = f"some_prefix/path{explore_path}"
    assert catname_from_path(db_context=db_context) == "sales"


def test_parsepath_supports_explore_folders() -> None:
    assert parsepath(
        "/domains/sales/projects/test_project/explore/exploration/a_notebook",
    ) == ParsedPath(
        domain="sales",
        project="test_project",
        flow="exploration",
    )


def test_parsepath_supports_explore_folders_w_org() -> None:
    assert parsepath(
        "/org/acme/domains/sales/projects/test_project/explore/exploration/a_notebook",
    ) == ParsedPath(
        org="acme",
        domain="sales",
        project="test_project",
        flow="exploration",
    )
