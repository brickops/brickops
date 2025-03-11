import pytest

from src.databricks.context import DbContext
from src.datamesh.naming import (
    tablename,
    dbname,
    extract_catname_from_path,
    parse_path,
)


@pytest.fixture
def valid_path() -> str:
    return (
        "something/domains/sanntid/projects/test_project/flows/test_flow/test_notebook"
    )


@pytest.fixture
def explore_path() -> str:
    return "something/domains/sanntid/projects/test_project/explore/exploration/test_notebook"


def test_that_starting_with_valid_path_returns_correct_catalog_name(
    valid_path: str,
) -> None:
    assert extract_catname_from_path(valid_path) == "sanntid"


def test_that_containing_valid_path_in_prod_returns_correct_catalog_name_without_postfix(
    valid_path: str,
) -> None:
    assert extract_catname_from_path(f"some_prefix/path{valid_path}") == "sanntid"


def test_that_containing_path_without_domain_returns_none() -> None:
    assert (
        extract_catname_from_path(
            "something/domains/projects/test_project/flows/test_flow/test_notebook"
        )
        == ""
    )


def test_that_env_is_correctly_post_fixed(
    valid_path: str,
) -> None:
    assert extract_catname_from_path(f"some_prefix/path{valid_path}") == "sanntid"


def test_that_catalog_can_be_extracted_for_explore_folders(
    explore_path: str,
) -> None:
    assert extract_catname_from_path(f"some_prefix/path{explore_path}") == "sanntid"


def test_parse_path_supports_explore_folders() -> None:
    assert parse_path(
        "/domains/sanntid/projects/test_project/explore/exploration/a_notebook"
    ) == (
        "sanntid",
        "test_project",
        "exploration",
    )


@pytest.fixture
def db_context() -> DbContext:
    return DbContext(
        api_token="token",  # noqa: S106
        api_url="",
        notebook_path="path",
        username="TestUser@vlfk.no",
        widgets={
            "git_url": "git_url",
            "git_branch": "git_branch",
            "git_commit": "abcdefgh123",
            "pipeline_env": "prod",
        },
    )


def test_that_tablename_in_test_contains_user_and_branch(
    db_context: DbContext,
) -> None:
    db_context.widgets["git_branch"] = "feat/new_branch"
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )

    assert result == "training.TestUser_featnewbranch_abcdefgh_test_db.test_tbl"


def test_that_tablename_in_prod_does_not_contain_user_and_branch(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipal name"  # we are implicitly in prod when username does not contains @
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )

    assert result == "training.test_db.test_tbl"


def test_that_tablename_with_norwegian_characters_in_table_results_in_backticked_name(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipal"
    result = tablename(
        tbl="test_tøbbel",
        db="test_db",
        cat="training",
        db_context=db_context,
    )

    assert result == "training.test_db.`test_tøbbel`"


def test_that_tablename_with_norwegian_characters_in_catalog_and_table_results_in_backticked_names(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipal"
    result = tablename(
        tbl="test_tøbbel",
        db="test_db",
        cat="træning",
        db_context=db_context,
    )

    assert result == "`træning`.test_db.`test_tøbbel`"


@pytest.mark.parametrize("branch_name", ["pr122", "averylongbranchname"])
def test_that_full_dbname_is_correct(branch_name: str, db_context: DbContext) -> None:
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert result == f"training.TestUser_{branch_name}_abcdefgh_test_db"


def test_that_full_branch_name_with_slash_is_stripped_correctly(
    db_context: DbContext,
) -> None:
    branch_name = "feature/branch"
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert result == "training.TestUser_featurebranch_abcdefgh_test_db"


def test_that_full_branch_name_with_spaces_is_stripped_correctly(
    db_context: DbContext,
) -> None:
    branch_name = "feature_of_something_branch"
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert result == "training.TestUser_featureofsomethingbranch_abcdefgh_test_db"


def test_that_dbname_with_norwegian_characters_in_name_results_in_backticked_name(
    db_context: DbContext,
) -> None:
    result = dbname(db_context=db_context, db="test_db", cat="en_liten_ø")
    assert result == "`en_liten_ø`.TestUser_gitbranch_abcdefgh_test_db"
