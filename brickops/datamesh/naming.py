from __future__ import annotations

from typing import TYPE_CHECKING, Any

from brickops.databricks.context import current_env, get_context
from brickops.databricks.username import get_username
from brickops.dataops.deploy.repo import git_source
from brickops.gitutils import clean_branch, commit_shortref
from brickops.datamesh.parsepath.extractname import (
    extract_name_from_path,
)
from brickops.datamesh.parsepath.extractname import PipelineContext


if TYPE_CHECKING:
    from brickops.databricks.context import DbContext


def tablename(
    tbl: str,
    db: str,
    cat: str | None = None,
    env: str | None = None,
    db_context: DbContext | None = None,
) -> str:
    """Return a table name prefixed with db name for table of name tbl.

    If a catalog is not provided, it is derived from the notebook path.
    If an env is not provided, it is derived from environment settings.
    If a db_context is not provided, it is derived from the current context.
    Cat is the Unity Catalog catalog name.
    
    db can either be a <catalog>.<db> path or a simply the database name.
    """
    # Get dbutils from calling module, as databricks lib not available in UC cluster
    if not tbl:
        msg = "tbl must be a non-empty string"
        raise ValueError(msg)
    if not db:
        msg = "db must be a non-empty string"
        raise ValueError(msg)
    if not db_context:
        db_context = get_context()
    if not env:
        env = current_env(db_context)
    if not cat:
        cat = catname_from_path(db_context=db_context, env=env)

    if "." not in db:
        db_name = dbname(db=db, cat=cat, db_context=db_context, env=env)
    else:
        db_name = db
    return _escape_sql_name(f"{db_name}.{tbl}")


def name_from_path(*, resource: str, db_context: DbContext, env: str) -> str:
    """Derive name from repo data mesh structure.

    The naming config defines how the name is composed
    e.g:

    naming:
      table:
        prod: "{org}_{domain}_{project}_{env}"
        other: "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitref}"

    Example path:
    .../domains/transport/projects/taxinyc/flows/prep/revenue/revenue

    With org:
    .../org/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    nb_path = db_context.notebook_path
    pipeline_context = _get_pipeline_context(db_context, env=env)
    return _escape_sql_name(
        extract_name_from_path(
            path=nb_path,
            resource=resource,
            pipeline_context=pipeline_context,
        )
    )


def dbname(
    db: str,
    cat: str,
    db_context: DbContext | None = None,
    prepend_cat: bool = True,
    env: str | None = None,
) -> str:
    """Generate a database name from db, cat, env, and possible path,
    determined by the naming config for db, either default or in
    .brickopscfg/config.yml, e.g.

    db:
      prod: {db}
      other: {env}_{username}_{gitbranch}_{gitref}_{db}.

    if prepend_cat is True (default), prepend the catalog name to the db name."""
    if not db:
        msg = "db must be a non-empty string"
        raise ValueError(msg)
    if not db_context:
        db_context = get_context()
    if not env:
        env = current_env(db_context)
    nb_path = db_context.notebook_path
    pipeline_context = _get_pipeline_context(db_context, env=env)
    db_only = extract_name_from_path(
        path=nb_path, resource="db", resource_name=db, pipeline_context=pipeline_context
    )
    name = db_only
    if prepend_cat:
        name = f"{cat}.{name}"
    return _escape_sql_name(name)


# Alias for tablename, to avoid breaking legacy code
build_table_name = tablename


def _git_src(db_context: DbContext) -> dict[str, Any]:
    """Get git src params from either task params or repos api.

    Widget parameters take precedence over repos api.
    """
    git_data = git_source(db_context)
    git_data_from_widgets = _git_src_from_widget_params(db_context)
    return git_data | git_data_from_widgets


def _git_src_from_widget_params(db_context: DbContext) -> dict[str, Any]:
    """TODO: document why this function is needed"""
    widget_data = {
        "git_url": db_context.widgets.get("git_url"),
        "git_branch": db_context.widgets.get("git_branch"),
        "git_commit": db_context.widgets.get("git_commit"),
        "git_path": db_context.widgets.get("git_path"),
    }
    return {k: v for k, v in widget_data.items() if v is not None}


def catname_from_path(
    *, db_context: DbContext | None = None, env: str | None = None
) -> str:
    """Derive catalog name from repo data mesh structure.

    By default we simply use domain as base catalog name.

    If naming config is defined, then the catalog name is composed
    according to the elements in the config, e.g. f"{org}_{domain}_{project}"

    Example path:
    .../domains/transport/projects/taxinyc/flows/prep/revenue/revenue

    With org:
    .../org/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    if not db_context:  # Can be extracted from dbutils, available in notebooks
        db_context = get_context()
    if not env:
        env = current_env(db_context)
    nb_path = db_context.notebook_path
    pipeline_context = _get_pipeline_context(db_context, env=env)
    return _escape_sql_name(
        extract_name_from_path(
            path=nb_path,
            resource="catalog",
            pipeline_context=pipeline_context,
        )
    )


def jobname(db_context: DbContext, env: str) -> str:
    """Derive job name from repo data mesh structure.

    The naming config defines how the job name is composed
    e.g:

    naming:
      job:
        prod: "{org}_{domain}_{project}_{env}"
        other: "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitref}"

    Example path:
    .../domains/transport/projects/taxinyc/flows/prep/revenue/revenue

    With org:
    .../org/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    nb_path = db_context.notebook_path
    pipeline_context = _get_pipeline_context(db_context, env=env)
    return _escape_sql_name(
        extract_name_from_path(
            path=nb_path,
            resource="job",
            pipeline_context=pipeline_context,
        )
    )


def pipelinename(db_context: DbContext, env: str) -> str:
    """Derive pipeline name from repo data mesh structure.

    The naming config defines how the pipeline name is composed
    e.g:

    naming:
      pipeline:
        prod: "{org}_{domain}_{project}_{env}"
        other: "{org}_{domain}_{project}_{env}_{username}_{gitbranch}_{gitref}"

    Example path:
    .../domains/transport/projects/taxinyc/flows/prep/revenue/revenue

    With org:
    .../org/acme/domains/transport/projects/taxinyc/flows/prep/revenue/revenue
    """
    nb_path = db_context.notebook_path
    pipeline_context = _get_pipeline_context(db_context, env=env)
    return _escape_sql_name(
        extract_name_from_path(
            path=nb_path,
            resource="pipeline",
            pipeline_context=pipeline_context,
        )
    )


def _get_pipeline_context(db_context: DbContext, env: str) -> PipelineContext:
    """Get pipeline context from databricks context and env.
    It is used to derive correct name in extract_name_from_path()."""
    git_src = _git_src(db_context)
    pipeline_context = PipelineContext(
        username=get_username(db_context),
        gitbranch=clean_branch(git_src["git_branch"]),
        gitshortref=commit_shortref(git_src["git_commit"]),
        env=env,
    )
    return pipeline_context


def _escape_sql_name(name: str) -> str:
    parts = name.split(".")
    return ".".join(
        [_escape_norwegian_chars(part) if "`" not in part else part for part in parts]
    )


def _escape_norwegian_chars(name: str) -> str:
    norwegian_chars = ["æ", "ø", "å"]
    return f"`{name}`" if any((c in norwegian_chars) for c in name) else name
