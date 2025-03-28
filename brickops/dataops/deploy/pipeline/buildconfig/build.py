from typing import Any

from brickops.databricks.context import DbContext
from brickops.databricks.username import get_username
from brickops.datamesh.parsepath import extract_jobprefix_from_path
from brickops.dataops.deploy.pipeline.buildconfig.enrichtasks import enrich_tasks
from brickops.dataops.deploy.pipeline.buildconfig.pipeline_config import (
    PipelineConfig,
    defaultconfig,
)
from brickops.gitutils import clean_branch, commit_shortref


def depname(*, db_context: DbContext, env: str, git_src: dict[str, Any]) -> str:
    """Compose deployment name from env and git config."""
    if env == "prod":
        return "prod"
    uname = get_username(db_context)
    branch = clean_branch(git_src["git_branch"])
    short_ref = commit_shortref(git_src["git_commit"])
    return f"{env}_{uname}_{branch}_{short_ref}"


def pipelinename(db_context: DbContext, depname: str) -> str:
    _nbpath = db_context.notebook_path
    pipelineprefix = extract_jobprefix_from_path(_nbpath)
    return f"{pipelineprefix}_{depname}"


def build_pipeline_config(
    cfg: dict[str, Any],
    env: str,
    db_context: DbContext,
) -> PipelineConfig:
    """Combine custom parameters with default parameters, and default cluster config."""
    full_cfg = defaultconfig()
    full_cfg.update(cfg)
    dep_name = depname(db_context=db_context, env=env, git_src=full_cfg.git_source)
    full_cfg.name = pipelinename(db_context, depname=dep_name)
    tags = _tags(cfg=cfg, depname=dep_name, pipeline_env=env)
    full_cfg.tags = tags
    full_cfg.parameters.extend(build_context_parameters(env, tags))
    full_cfg = enrich_tasks(pipeline_config=full_cfg, db_context=db_context, env=env)
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


def _tags(*, cfg: dict[str, Any], depname: str, pipeline_env: str) -> dict[str, Any]:
    return {
        "deployment": depname,
        "git_url": cfg["git_source"]["git_url"],
        "git_branch": cfg["git_source"]["git_branch"],
        "git_commit": cfg["git_source"]["git_commit"],
        "pipeline_env": pipeline_env,
    }
