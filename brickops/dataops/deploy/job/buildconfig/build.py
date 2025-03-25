from typing import Any

from brickops.databricks.context import DbContext
from brickops.databricks.username import get_username
from brickops.datamesh.parsepath import extract_jobprefix_from_path
from brickops.dataops.deploy.job.buildconfig.enrichtasks import enrich_tasks
from brickops.dataops.deploy.job.buildconfig.job_config import JobConfig, defaultconfig
from brickops.gitutils import clean_branch, commit_shortref


def depname(*, db_context: DbContext, env: str, git_src: dict[str, Any]) -> str:
    """Compose deployment name from env and git config."""
    if env == "prod":
        return "prod"
    uname = get_username(db_context)
    branch = clean_branch(git_src["git_branch"])
    short_ref = commit_shortref(git_src["git_commit"])
    return f"{env}_{uname}_{branch}_{short_ref}"


def jobname(db_context: DbContext, depname: str) -> str:
    _nbpath = db_context.notebook_path
    jobprefix = extract_jobprefix_from_path(_nbpath)
    return f"{jobprefix}_{depname}"


def build_job_config(
    cfg: dict[str, Any],
    env: str,
    db_context: DbContext,
) -> JobConfig:
    """Combine custom parameters with default parameters, and default cluster config."""
    full_cfg = defaultconfig()
    if env != "prod":
        full_cfg.email_notifications = {}

    full_cfg.update(cfg)
    dep_name = depname(db_context=db_context, env=env, git_src=full_cfg.git_source)
    full_cfg.name = jobname(db_context, depname=dep_name)
    tags = _tags(cfg=cfg, depname=dep_name)
    full_cfg.tags = tags
    full_cfg.parameters.extend(build_context_parameters(env, tags))
    full_cfg = enrich_tasks(job_config=full_cfg, db_context=db_context)
    if db_context.is_service_principal:
        full_cfg.run_as = {"service_principal_name": db_context.username}
    else:  # if we have a service principal, we need to use the correct config
        full_cfg.run_as = {
            "user_name": db_context.username,
        }

    return full_cfg


def build_context_parameters(env: str, tags: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a list of parameters containing the environment and git info."""
    return [
        {
            "name": "pipeline_env",
            "default": env,
        },
        {
            "name": "git_url",
            "default": tags["git_url"],
        },
        {
            "name": "git_branch",
            "default": tags["git_branch"],
        },
        {
            "name": "git_commit",
            "default": tags["git_commit"],
        },
    ]


def _tags(*, cfg: dict[str, Any], depname: str) -> dict[str, Any]:
    return {
        "deployment": depname,
        "git_url": cfg["git_source"]["git_url"],
        "git_branch": cfg["git_source"]["git_branch"],
        "git_commit": cfg["git_source"]["git_commit"],
    }
