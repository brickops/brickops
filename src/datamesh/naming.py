from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from src.databricks.context import current_env, get_context
from src.databricks.username import get_username
from src.dataops.deploy.repo import git_source
from src.gitutils import clean_branch, commit_shortref

if TYPE_CHECKING:
    from src.databricks.context import DbContext


def _escape_sql_name(name: str) -> str:
    parts = name.split(".")
    return ".".join(
        [_escape_norwegian_chars(part) if "`" not in part else part for part in parts]
    )


def _escape_norwegian_chars(name: str) -> str:
    norwegian_chars = ["æ", "ø", "å"]
    return f"`{name}`" if any((c in norwegian_chars) for c in name) else name


def tablename(
    tbl: str,
    db: str,
    cat: str | None = None,
    db_context: DbContext | None = None,
) -> str:
    """Cat is the Unity Catalog catalog name."""
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    if not tbl:
        msg = "tbl must be a non-empty string"
        raise ValueError(msg)
    if not db:
        msg = "db must be a non-empty string"
        raise ValueError(msg)
    if not cat:
        cat = catname_from_path()
    if not db_context:
        db_context = get_context()

    db_name = dbname(db=db, cat=cat, db_context=db_context)
    return _escape_sql_name(f"{db_name}.{tbl}")


def dbname(
    db: str,
    cat: str,
    db_context: DbContext | None = None,
) -> str:
    """Generate a database name from db, cat, env."""
    if not db:
        msg = "db must be a non-empty string"
        raise ValueError(msg)
    if not db_context:
        db_context = get_context()
    env = current_env(db_context)
    db_prefix = dbprefix(env=env, db_context=db_context)
    return _escape_sql_name(f"{cat}.{db_prefix}{db}")


def dbprefix(env: str, db_context: DbContext) -> str:
    """Compose deployment prefix from env and git config."""
    if env == "prod":
        return ""
    dep_prefix = f"{get_username(db_context)}_"
    # Get git state to build db name
    git_src = _git_src(db_context)
    branch = clean_branch(git_src["git_branch"])
    short_ref = commit_shortref(git_src["git_commit"])
    return f"{dep_prefix}{branch}_{short_ref}_"


def _git_src(db_context: DbContext) -> dict[str, Any]:
    """Get git src params from either task params or repos api.

    Widget parameters take precedence over repos api.
    """
    git_data = git_source(db_context)
    git_data_from_widgets = _git_src_from_widget_params(db_context)
    return git_data | git_data_from_widgets


def _git_src_from_widget_params(db_context: DbContext) -> dict[str, Any]:
    widget_data = {
        "git_url": db_context.widgets.get("git_url"),
        "git_branch": db_context.widgets.get("git_branch"),
        "git_commit": db_context.widgets.get("git_commit"),
        "git_path": db_context.widgets.get("git_path"),
    }
    return {k: v for k, v in widget_data.items() if v is not None}


def parse_path(path: str) -> tuple[str, str, str] | None:
    """Parse path to extract org, domain, project, and flow."""
    ret = re.search(
        r".*\/domains/([^/]+)\/projects\/([^/]+)\/(?:flows|explore)\/([^/]+)\/.+",
        path,
        re.IGNORECASE,
    )
    if ret is None:
        return None

    if len(ret.groups()) < 3:  # noqa: PLR2004
        logging.warning(
            """parse_path: unexpected number of groups.
            Is the notebook in the correct folder!?"""
        )
        return None

    domain = ret[1]
    project = ret[2]
    flow = ret[3]
    return domain, project, flow


def _extract_catname_from_path(path: str) -> str:
    """Derive catalog name from repo data mesh structure.

    We simply use domain as base catalog name.
    """
    if result := parse_path(path):
        domain, *_ = result
        return domain
    return ""


def catname_from_path() -> str:
    """Derive catalog name from repo data mesh structure.

    We simply use domain as base catalog name.

    Example path:
    .../domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    db_context = get_context()
    nb_path = db_context.notebook_path
    return _escape_sql_name(_extract_catname_from_path(nb_path))
