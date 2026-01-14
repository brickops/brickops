from __future__ import annotations

import inspect
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession

    from databricks.sdk.runtime.dbutils_stub import dbutils as dbutils_type


def current_target(db_context: DbContext | None = None) -> str:
    """Get the current target.

    If target is not specified in the widgets, we rely on the username to detect
    if we are running in the 'test' target.
    Default to what is set in the widgets if target is not available and
    finally prod if that also does not exist.
    """
    if db_context:
        if env_from_widget := db_context.widgets.get("target", ""):
            return env_from_widget
        if "@" in db_context.username:
            return "test"

    return "prod"


def get_context(dbutils: dbutils_type | None = None) -> DbContext:
    if dbutils is None:
        dbutils = get_dbutils()
    return _convert_to_data(dbutils)


def get_context_from_params(path: str, username: str, git_info: dict) -> DbContext:
    """Get context from from parameters.
    We need to fall back to parameters when running naming functions in dab mutators."""
    widgets = {
        "git_url": git_info.get("url", ""),
        "git_branch": git_info.get("branch", ""),
        "git_commit": git_info.get("commit", ""),
        "git_path": path,
    }
    # widgets = {for k, v in git_info.items()}

    logger.debug("username: %s", username)
    return DbContext(
        api_url="",
        api_token="",
        notebook_path=path,
        username=username,
        widgets=widgets,
    )


def get_dbutils() -> dbutils_type:
    """Iterate through the stack to find the dbutils object."""
    for stack in inspect.stack():
        if "dbutils" in stack[0].f_globals:
            return stack[0].f_globals["dbutils"]  # type: ignore [no-any-return]

    msg = "dbutils not found in the stack."
    raise RuntimeError(msg)


def get_spark() -> SparkSession:
    """Iterate through the stack to find the dbutils object."""
    for stack in inspect.stack():
        if "spark" in stack[0].f_globals:
            return stack[0].f_globals["spark"]  # type: ignore [no-any-return]

    msg = "spark not found in the stack."
    raise RuntimeError(msg)


@dataclass
class DbContext:
    """Dataclass to hold needed databricks context data.

    This is created from the dbutils object, and passed around to functions.
    With this approach we greatly simplify testing and de-couple the code
    from needing the databricks runtime to execute.
    """

    api_url: str
    api_token: str
    notebook_path: str
    username: str
    widgets: dict[str, str] = field(default_factory=dict)
    is_service_principal: bool = False

    def __post_init__(self: DbContext) -> None:
        """Set up calculated fields."""
        self.is_service_principal = "@" not in self.username


def _convert_to_data(dbutils: dbutils_type) -> DbContext:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()  # type: ignore [attr-defined]
    return DbContext(
        api_url=ctx.apiUrl().get(),
        api_token=ctx.apiToken().get(),
        notebook_path=ctx.notebookPath().get(),
        username=str(ctx.userName().get()),
        widgets=dbutils.widgets.getAll(),  # type: ignore [attr-defined]
    )
