import pytest
import pytest_mock

from typing import Any
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
            "target": "test",
        },
    )


@pytest.fixture
def db_context_empty_widgets_short_path() -> DbContext:
    return DbContext(
        api_token="token",  # noqa: S106
        api_url="",
        notebook_path="/Users/test@vlfk.no/databricks-dataops-course/course/01-Student-Prep/01-General/1-CreateDatabaseObjects",
        username="userfoo@vlfk.no",
        widgets={},
        is_service_principal=False,
    )


def test_tablename_in_test_contains_user_and_branch(
    db_context: DbContext,
) -> None:
    db_context.widgets["git_branch"] = "feat/new_branch"
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )
    assert result == "training.test_TestUser_featnewbranch_abcdefgh_test_db.test_tbl"


def test_tablename_accepts_db_path(
    db_context: DbContext,
) -> None:
    db_context.widgets["git_branch"] = "feat/new_branch"
    result = tablename(
        tbl="test_tbl",
        db="training.test_TestUser_featnewbranch_abcdefgh_test_db",
        cat="training",
        db_context=db_context,
    )
    assert result == "training.test_TestUser_featnewbranch_abcdefgh_test_db.test_tbl"


GIT_SOURCE = {
    "git_url": "api_sourced_url",
    "git_provider": "api_sourced_provider",
    "git_branch": "apisourcedbranch",
    "git_commit": "apidefgh",
    "git_path": "api_sourced_path",
}


def test_tablename_in_test_with_empty_widgets(
    db_context_empty_widgets_short_path: DbContext,
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    mocker.patch("brickops.datamesh.naming.git_source", return_value=GIT_SOURCE)
    result = tablename(
        tbl="tblfoo",
        db="dbfoo",
        cat="training",
        db_context=db_context_empty_widgets_short_path,
    )
    assert result == "training.test_userfoo_apisourcedbranch_apidefgh_dbfoo.tblfoo"


def test_dbname_in_test_with_empty_widgets(
    db_context_empty_widgets_short_path: DbContext,
    mocker: pytest_mock.plugin.MockerFixture,
) -> None:
    mocker.patch("brickops.datamesh.naming.git_source", return_value=GIT_SOURCE)
    result = dbname(
        db="dbfoo",
        cat="training",
        db_context=db_context_empty_widgets_short_path,
    )
    assert result == "training.test_userfoo_apisourcedbranch_apidefgh_dbfoo"


def test_tablename_in_test_in_prod_env_from_widget_var_target(
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
        "target"
    ]  # remove target=test, since it overrides username check
    result = tablename(
        tbl="test_tbl", db="test_db", cat="training", db_context=db_context
    )

    assert result == "training.test_db.test_tbl"


def test_tablename_with_norwegian_characters_in_table_results_in_backticked_name(
    db_context: DbContext,
) -> None:
    db_context.username = "ServicePrincipal"
    del db_context.widgets[
        "target"
    ]  # remove target=test, since it overrides username check
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
        "target"
    ]  # remove target=test, since it overrides username check
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
    db_context.widgets["target"] = "test"
    result = dbname(db_context=db_context, db="test_db", cat="en_liten_ø")
    assert result == "`en_liten_ø`.test_TestUser_gitbranch_abcdefgh_test_db"


def test_full_branch_name_with_slash_is_stripped_correctly_w_full_mesh(
    db_context: DbContext,
    mocker: pytest_mock.plugin.MockerFixture,
    brickops_fullmeshre_config: dict[str, Any],
) -> None:
    mocker.patch(
        "brickops.datamesh.cfg.read_config", return_value=brickops_fullmeshre_config
    )
    branch_name = "feature/branch"
    db_context.widgets["git_branch"] = branch_name
    result = dbname(db_context=db_context, db="test_db", cat="training")

    assert (
        result
        == "training.test_TestUser_featurebranch_abcdefgh_flows_prep_flowfoo_test_db"
    )


def test_name_from_path_is_correct_prod(
    db_context: DbContext,
) -> None:
    branch_name = "feature/branch"
    db_context.widgets["git_branch"] = branch_name
    # Set target catalog
    cat = name_from_path(
        resource="catalog",
        db_context=db_context,
        target="prod",
    )
    assert cat == "domainfoo"


def test_jobname(
    db_context: DbContext,
) -> None:
    result = jobname(db_context=db_context, target="test")
    assert result == "domainfoo_projectfoo_test_TestUser_gitbranch_abcdefgh"


def test_pipelinename(
    db_context: DbContext,
) -> None:
    result = pipelinename(db_context=db_context, target="test")
    assert result == "domainfoo_projectfoo_test_TestUser_gitbranch_abcdefgh_dlt"


def test_tablename_in_dev_contains_username(
    db_context: DbContext,
) -> None:
    """Test that in dev target, tablename contains username prefix."""
    db_context.widgets["git_branch"] = "main"
    result = tablename(
        tbl="revenue_table",
        db="sales_db",
        cat="analytics",
        db_context=db_context,
        target="dev",
    )
    assert "TestUser" in result
    assert result == "analytics.dev_TestUser_main_abcdefgh_sales_db.revenue_table"


def test_tablename_in_dev_with_feature_branch(
    db_context: DbContext,
) -> None:
    """Test dev target with feature branch containing username in name."""
    db_context.widgets["git_branch"] = "feature/GH-123-add-metrics"
    result = tablename(
        tbl="metrics_table",
        db="reporting_db",
        cat="analytics",
        db_context=db_context,
        target="dev",
    )
    assert "TestUser" in result
    assert "featureGH123addmetrics" in result
    assert (
        result
        == "analytics.dev_TestUser_featureGH123addmetrics_abcdefgh_reporting_db.metrics_table"
    )


def test_dbname_in_dev_contains_username(
    db_context: DbContext,
) -> None:
    """Test that in dev target, dbname contains username prefix."""
    db_context.widgets["git_branch"] = "main"
    result = dbname(
        db="analytics_db",
        cat="training",
        db_context=db_context,
        target="dev",
    )
    assert "TestUser" in result
    assert result == "training.dev_TestUser_main_abcdefgh_analytics_db"


def test_dbname_in_dev_with_different_users(
    db_context: DbContext,
) -> None:
    """Test that different usernames produce different dev database names."""
    db_context.widgets["git_branch"] = "main"

    # First user
    db_context.username = "alice@company.com"
    result1 = dbname(
        db="test_db",
        cat="training",
        db_context=db_context,
        target="dev",
    )

    # Second user
    db_context.username = "bob@company.com"
    result2 = dbname(
        db="test_db",
        cat="training",
        db_context=db_context,
        target="dev",
    )

    assert "alice" in result1
    assert "bob" in result2
    assert result1 != result2


def test_jobname_in_dev_contains_username(
    db_context: DbContext,
) -> None:
    """Test that in dev target, jobname contains username."""
    result = jobname(db_context=db_context, target="dev")
    assert "TestUser" in result
    assert result == "domainfoo_projectfoo_dev_TestUser_gitbranch_abcdefgh"


def test_pipelinename_in_dev_contains_username(
    db_context: DbContext,
) -> None:
    """Test that in dev target, pipelinename contains username."""
    result = pipelinename(db_context=db_context, target="dev")
    assert "TestUser" in result
    assert result == "domainfoo_projectfoo_dev_TestUser_gitbranch_abcdefgh_dlt"


def test_name_from_path_catalog_in_dev(
    db_context: DbContext,
) -> None:
    """Test catalog name extraction in dev target."""
    db_context.widgets["git_branch"] = "develop"
    cat = name_from_path(
        resource="catalog",
        db_context=db_context,
        target="dev",
    )
    # Catalog names typically don't include username even in dev
    assert cat == "domainfoo"


def test_tablename_dev_target_with_underscore_in_username(
    db_context: DbContext,
) -> None:
    """Test dev naming with username containing underscores (preserved in output)."""
    db_context.username = "first_last@company.com"
    db_context.widgets["git_branch"] = "feature/update"
    result = tablename(
        tbl="users",
        db="main_db",
        cat="production",
        db_context=db_context,
        target="dev",
    )
    assert "first_last" in result
    assert result == "production.dev_first_last_featureupdate_abcdefgh_main_db.users"


def test_dbname_dev_target_maintains_isolation(
    db_context: DbContext,
) -> None:
    """Test that dev target with username ensures database isolation between developers."""
    db_context.widgets["git_branch"] = "main"

    # Developer 1 working on main branch
    db_context.username = "dev1@company.com"
    dev1_db = dbname(
        db="shared_db",
        cat="analytics",
        db_context=db_context,
        target="dev",
    )

    # Developer 2 working on same main branch
    db_context.username = "dev2@company.com"
    dev2_db = dbname(
        db="shared_db",
        cat="analytics",
        db_context=db_context,
        target="dev",
    )

    # Both should have username in the name for isolation
    assert "dev1" in dev1_db
    assert "dev2" in dev2_db
    assert dev1_db != dev2_db
    assert dev1_db == "analytics.dev_dev1_main_abcdefgh_shared_db"
    assert dev2_db == "analytics.dev_dev2_main_abcdefgh_shared_db"
