import pytest

from brickops.databricks.context import DbContext
from brickops.datamesh.naming import (
    dbname,
    tablename,
    jobname,
    pipelinename,
    name_from_path,
)


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
            "pipeline_env": "test",
        },
    )


def test_tablename_in_test_contains_user_and_branch(
    db_context: DbContext,
) -> None:
    db_context.widgets["git_branch"] = "feat/new_branch"
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )

    assert result == "training.test_TestUser_featnewbranch_abcdefgh_test_db.test_tbl"


def test_tablename_in_test_in_prod_env_from_widget_var_pipeline_env(
    db_context: DbContext,
) -> None:
    db_context.widgets["git_branch"] = "feat/new_branch"
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )

    assert result == "training.test_TestUser_featnewbranch_abcdefgh_test_db.test_tbl"


def test_tablename_in_prod_does_not_contain_user_and_branch(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipalName"  # we are implicitly in prod when username does not contains @
    del db_context.widgets[
        "pipeline_env"
    ]  # remove pipeline_env=test, since it overrides username check
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )

    assert result == "training.test_db.test_tbl"


def test_tablename_with_norwegian_characters_in_table_results_in_backticked_name(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipal"
    del db_context.widgets[
        "pipeline_env"
    ]  # remove pipeline_env=test, since it overrides username check
    result = tablename(
        tbl="test_tøbbel",
        db="test_db",
        cat="training",
        db_context=db_context,
    )

    assert result == "training.test_db.`test_tøbbel`"


def test_tablename_with_norwegian_characters_in_catalog_and_table_results_in_backticked_names(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipal"
    del db_context.widgets[
        "pipeline_env"
    ]  # remove pipeline_env=test, since it overrides username check
    result = tablename(
        tbl="test_tøbbel",
        db="test_db",
        cat="træning",
        db_context=db_context,
    )

    assert result == "`træning`.test_db.`test_tøbbel`"


@pytest.mark.parametrize("branch_name", ["pr122", "averylongbranchname"])
def test_full_dbname_is_correct(branch_name: str, db_context: DbContext) -> None:
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert result == f"training.test_TestUser_{branch_name}_abcdefgh_test_db"


def test_full_branch_name_with_slash_is_stripped_correctly(
    db_context: DbContext,
) -> None:
    branch_name = "feature/branch"
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert result == "training.test_TestUser_featurebranch_abcdefgh_test_db"


def test_full_branch_name_with_spaces_is_stripped_correctly(
    db_context: DbContext,
) -> None:
    branch_name = "feature_of_something_branch"
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert result == "training.test_TestUser_featureofsomethingbranch_abcdefgh_test_db"


def test_dbname_with_norwegian_characters_in_name_results_in_backticked_name(
    db_context: DbContext,
) -> None:
    db_context.widgets["pipeline_env"] = "test"
    result = dbname(db_context=db_context, db="test_db", cat="en_liten_ø")
    assert result == "`en_liten_ø`.test_TestUser_gitbranch_abcdefgh_test_db"


def test_name_from_path_is_correct_prod(
    db_context: DbContext,
) -> None:
    branch_name = "feature/branch"
    db_context.widgets["git_branch"] = branch_name
    # Set target catalog
    cat = name_from_path(
        resource="catalog",
        db_context=db_context,
        env="prod",
    )
    assert cat == "domainfoo"


def test_jobname(
    db_context: DbContext,
) -> None:
    result = jobname(db_context=db_context, env="test")
    assert result == "domainfoo_projectfoo_test_TestUser_gitbranch_abcdefgh"


def test_pipelinename(
    db_context: DbContext,
) -> None:
    result = pipelinename(db_context=db_context, env="test")
    assert result == "domainfoo_projectfoo_test_TestUser_gitbranch_abcdefgh_dlt"
